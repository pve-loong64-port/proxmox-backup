//! Use a local datastore as cache for operations on a datastore attached via
//! a network layer (e.g. via the S3 backend).

use std::future::Future;
use std::sync::Arc;

use anyhow::{bail, Error};
use http_body_util::BodyExt;

use pbs_tools::async_lru_cache::{AsyncCacher, AsyncLruCache};
use proxmox_s3_client::S3Client;

use crate::ChunkStore;
use crate::DataBlob;

#[derive(Clone)]
/// Cacher to fetch chunks from the S3 object store and insert them in the local cache store.
pub struct S3Cacher {
    client: Arc<S3Client>,
    store: Arc<ChunkStore>,
}

impl AsyncCacher<[u8; 32], ()> for S3Cacher {
    fn fetch(
        &self,
        key: [u8; 32],
    ) -> Box<dyn Future<Output = Result<Option<()>, Error>> + Send + 'static> {
        let client = Arc::clone(&self.client);
        let store = Arc::clone(&self.store);
        Box::new(async move {
            let object_key = crate::s3::object_key_from_digest(&key)?;
            match client.get_object(object_key).await? {
                None => bail!("could not fetch object with key {}", hex::encode(key)),
                Some(response) => {
                    let bytes = response.content.collect().await?.to_bytes();
                    let chunk = DataBlob::from_raw(bytes.to_vec())?;
                    store.insert_chunk(&chunk, &key)?;
                    Ok(Some(()))
                }
            }
        })
    }
}

impl S3Cacher {
    pub fn new(client: Arc<S3Client>, store: Arc<ChunkStore>) -> Self {
        Self { client, store }
    }
}

/// LRU cache using local datastore for caching chunks
///
/// Uses a LRU cache, but without storing the values in-memory but rather
/// on the filesystem
pub struct LocalDatastoreLruCache {
    cache: AsyncLruCache<[u8; 32], ()>,
    store: Arc<ChunkStore>,
}

impl LocalDatastoreLruCache {
    /// Create a new cache instance storing up to given capacity chunks in the local cache store.
    pub fn new(capacity: usize, store: Arc<ChunkStore>) -> Self {
        Self {
            cache: AsyncLruCache::new(capacity),
            store,
        }
    }

    /// Insert a new chunk into the local datastore cache.
    ///
    /// Fails if the chunk cannot be inserted successfully.
    pub fn insert(&self, digest: &[u8; 32], chunk: &DataBlob) -> Result<(), Error> {
        self.store.insert_chunk(chunk, digest)?;
        self.cache
            .insert(*digest, (), |digest| self.store.clear_chunk(&digest))
    }

    /// Remove a chunk from the local datastore cache.
    ///
    /// Callers to this method must assure that:
    /// - no concurrent insert is being performed, the chunk store's mutex must be held.
    /// - the chunk to be removed is no longer referenced by an index file.
    /// - the chunk to be removed has not been inserted by an active writer (atime newer than
    ///   writer start time).
    /// - there is no active writer in an old process, which could have inserted the chunk to be
    ///   deleted.
    ///
    /// Fails if the chunk cannot be deleted successfully.
    pub(crate) unsafe fn remove(&self, digest: &[u8; 32]) -> Result<(), Error> {
        self.cache.remove(*digest);
        let (path, _digest_str) = self.store.chunk_path(digest);
        std::fs::remove_file(path).map_err(Error::from)
    }

    /// Access the locally cached chunk or fetch it from the S3 object store via the provided
    /// cacher instance.
    ///
    /// For evicted cache nodes, clear the chunk file contents but leave the empty marker file
    /// behind so garbage collection doesn't clean it while in use.
    pub async fn access(
        &self,
        digest: &[u8; 32],
        cacher: &mut S3Cacher,
    ) -> Result<Option<DataBlob>, Error> {
        if self
            .cache
            .access(*digest, cacher, |digest| self.store.clear_chunk(&digest))
            .await?
            .is_some()
        {
            let (path, _digest_str) = self.store.chunk_path(digest);
            let mut file = match std::fs::File::open(&path) {
                Ok(file) => file,
                Err(err) => {
                    // Expected chunk to be present since LRU cache has it, but it is missing
                    // locally, try to fetch again
                    if err.kind() == std::io::ErrorKind::NotFound {
                        let chunk = self.fetch_and_insert(cacher.client.clone(), digest).await?;
                        return Ok(Some(chunk));
                    } else {
                        return Err(Error::from(err));
                    }
                }
            };
            let chunk = match DataBlob::load_from_reader(&mut file) {
                Ok(chunk) => chunk,
                Err(err) => {
                    use std::io::Seek;
                    // Check if file is empty marker file, try fetching content if so
                    if file.seek(std::io::SeekFrom::End(0))? == 0 {
                        let chunk = self.fetch_and_insert(cacher.client.clone(), digest).await?;
                        return Ok(Some(chunk));
                    } else {
                        return Err(err);
                    }
                }
            };
            Ok(Some(chunk))
        } else {
            Ok(None)
        }
    }

    /// Checks if the given digest is stored in the datastores LRU cache
    pub fn contains(&self, digest: &[u8; 32]) -> bool {
        self.cache.contains(*digest)
    }

    async fn fetch_and_insert(
        &self,
        client: Arc<S3Client>,
        digest: &[u8; 32],
    ) -> Result<DataBlob, Error> {
        let object_key = crate::s3::object_key_from_digest(digest)?;
        match client.get_object(object_key).await? {
            None => {
                bail!("could not fetch object with key {}", hex::encode(digest))
            }
            Some(response) => {
                let bytes = response.content.collect().await?.to_bytes();
                let chunk = DataBlob::from_raw(bytes.to_vec())?;
                self.store.insert_chunk(&chunk, digest)?;
                Ok(chunk)
            }
        }
    }
}
