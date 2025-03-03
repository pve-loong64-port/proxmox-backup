use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{bail, Error};
use http_body_util::BodyExt;

use pbs_api_types::CryptMode;
use pbs_tools::crypt_config::CryptConfig;
use proxmox_s3_client::S3Client;

use crate::data_blob::DataBlob;
use crate::datastore::DatastoreBackend;
use crate::read_chunk::{AsyncReadChunk, ReadChunk};
use crate::DataStore;

#[derive(Clone)]
pub struct LocalChunkReader {
    store: Arc<DataStore>,
    backend: DatastoreBackend,
    crypt_config: Option<Arc<CryptConfig>>,
    crypt_mode: CryptMode,
}

impl LocalChunkReader {
    pub fn new(
        store: Arc<DataStore>,
        crypt_config: Option<Arc<CryptConfig>>,
        crypt_mode: CryptMode,
    ) -> Result<Self, Error> {
        let backend = store.backend()?;
        Ok(Self {
            store,
            backend,
            crypt_config,
            crypt_mode,
        })
    }

    fn ensure_crypt_mode(&self, chunk_mode: CryptMode) -> Result<(), Error> {
        match self.crypt_mode {
            CryptMode::Encrypt => match chunk_mode {
                CryptMode::Encrypt => Ok(()),
                CryptMode::SignOnly | CryptMode::None => {
                    bail!("Index and chunk CryptMode don't match.")
                }
            },
            CryptMode::SignOnly | CryptMode::None => match chunk_mode {
                CryptMode::Encrypt => bail!("Index and chunk CryptMode don't match."),
                CryptMode::SignOnly | CryptMode::None => Ok(()),
            },
        }
    }
}

async fn fetch(s3_client: Arc<S3Client>, digest: &[u8; 32]) -> Result<DataBlob, Error> {
    let object_key = crate::s3::object_key_from_digest(digest)?;
    if let Some(response) = s3_client.get_object(object_key).await? {
        let bytes = response.content.collect().await?.to_bytes();
        DataBlob::from_raw(bytes.to_vec())
    } else {
        bail!("no object with digest {}", hex::encode(digest));
    }
}

impl ReadChunk for LocalChunkReader {
    fn read_raw_chunk(&self, digest: &[u8; 32]) -> Result<DataBlob, Error> {
        let chunk = match &self.backend {
            DatastoreBackend::Filesystem => self.store.load_chunk(digest)?,
            DatastoreBackend::S3(s3_client) => {
                proxmox_async::runtime::block_on(fetch(Arc::clone(s3_client), digest))?
            }
        };
        self.ensure_crypt_mode(chunk.crypt_mode()?)?;

        Ok(chunk)
    }

    fn read_chunk(&self, digest: &[u8; 32]) -> Result<Vec<u8>, Error> {
        let chunk = ReadChunk::read_raw_chunk(self, digest)?;

        let raw_data = chunk.decode(self.crypt_config.as_ref().map(Arc::as_ref), Some(digest))?;

        Ok(raw_data)
    }
}

impl AsyncReadChunk for LocalChunkReader {
    fn read_raw_chunk<'a>(
        &'a self,
        digest: &'a [u8; 32],
    ) -> Pin<Box<dyn Future<Output = Result<DataBlob, Error>> + Send + 'a>> {
        Box::pin(async move {
            let chunk = match &self.backend {
                DatastoreBackend::Filesystem => {
                    let (path, _) = self.store.chunk_path(digest);
                    let raw_data = tokio::fs::read(&path).await?;
                    DataBlob::load_from_reader(&mut &raw_data[..])?
                }
                DatastoreBackend::S3(s3_client) => fetch(Arc::clone(s3_client), digest).await?,
            };
            self.ensure_crypt_mode(chunk.crypt_mode()?)?;

            Ok(chunk)
        })
    }

    fn read_chunk<'a>(
        &'a self,
        digest: &'a [u8; 32],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send + 'a>> {
        Box::pin(async move {
            let chunk = AsyncReadChunk::read_raw_chunk(self, digest).await?;

            let raw_data =
                chunk.decode(self.crypt_config.as_ref().map(Arc::as_ref), Some(digest))?;

            // fixme: verify digest?

            Ok(raw_data)
        })
    }
}
