//! Use a local datastore as cache for operations on a datastore attached via
//! a network layer (e.g. via the S3 backend).

use std::sync::Arc;

use anyhow::{bail, Error};
use http_body_util::BodyExt;

use pbs_tools::async_lru_cache::AsyncLruCache;
use proxmox_s3_client::S3Client;

use crate::ChunkStore;
use crate::DataBlob;

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
        let _lock = self.store.mutex().lock().unwrap();

        // Safety: lock acquire above
        unsafe {
            self.store.insert_chunk_nolock(chunk, digest)?;
        }
        self.cache.insert(*digest, (), |digest| {
            // Safety: lock acquired above, this is executed inline!
            unsafe { self.store.replace_chunk_with_marker(&digest) }
        })
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
        self.store.remove_chunk_marker(digest)
    }

    /// Access the locally cached chunk or fetch it from the S3 object store via the provided
    /// cacher instance.
    ///
    /// For evicted cache nodes, clear the chunk file contents but leave the empty marker file
    /// behind so garbage collection doesn't clean it while in use.
    pub async fn access(
        &self,
        digest: &[u8; 32],
        client: Arc<S3Client>,
    ) -> Result<Option<DataBlob>, Error> {
        let (path, _digest_str) = self.store.chunk_path(digest);
        match std::fs::File::open(&path) {
            Ok(mut file) => match DataBlob::load_from_reader(&mut file) {
                // File was still cached with contents, load response from file
                Ok(chunk) => {
                    let _lock = self.store.mutex().lock().unwrap();
                    self.cache.insert(*digest, (), |digest| {
                        // Safety: lock acquired above, this is executed inline
                        unsafe { self.store.replace_chunk_with_marker(&digest) }
                    })?;
                    Ok(Some(chunk))
                }
                // File was empty, might have been evicted since
                Err(err) => {
                    use std::io::Seek;
                    // Check if file is empty marker file, try fetching content if so
                    if file.seek(std::io::SeekFrom::End(0))? == 0 {
                        let chunk = self.fetch_and_insert(client.clone(), digest).await?;
                        Ok(Some(chunk))
                    } else {
                        Err(err)
                    }
                }
            },
            Err(err) => {
                // Failed to open file, missing
                if err.kind() == std::io::ErrorKind::NotFound {
                    let chunk = self.fetch_and_insert(client.clone(), digest).await?;
                    Ok(Some(chunk))
                } else {
                    Err(Error::from(err))
                }
            }
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
                self.insert(digest, &chunk)?;
                Ok(chunk)
            }
        }
    }
}
