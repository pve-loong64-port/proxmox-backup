//! Sync datastore contents from source to target, either in push or pull direction

use std::collections::HashMap;
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Error};
use http::StatusCode;
use tracing::info;

use proxmox_router::HttpError;

use pbs_api_types::{BackupDir, CryptMode};
use pbs_client::{BackupReader, RemoteChunkReader};
use pbs_datastore::data_blob::DataBlob;
use pbs_datastore::manifest::CLIENT_LOG_BLOB_NAME;
use pbs_datastore::read_chunk::AsyncReadChunk;
use pbs_datastore::{DataStore, LocalChunkReader};

#[derive(Default)]
pub(crate) struct RemovedVanishedStats {
    pub(crate) groups: usize,
    pub(crate) snapshots: usize,
    pub(crate) namespaces: usize,
}

impl RemovedVanishedStats {
    pub(crate) fn add(&mut self, rhs: RemovedVanishedStats) {
        self.groups += rhs.groups;
        self.snapshots += rhs.snapshots;
        self.namespaces += rhs.namespaces;
    }
}

#[derive(Default)]
pub(crate) struct SyncStats {
    pub(crate) chunk_count: usize,
    pub(crate) bytes: usize,
    pub(crate) elapsed: Duration,
    pub(crate) removed: Option<RemovedVanishedStats>,
}

impl From<RemovedVanishedStats> for SyncStats {
    fn from(removed: RemovedVanishedStats) -> Self {
        Self {
            removed: Some(removed),
            ..Default::default()
        }
    }
}

impl SyncStats {
    pub(crate) fn add(&mut self, rhs: SyncStats) {
        self.chunk_count += rhs.chunk_count;
        self.bytes += rhs.bytes;
        self.elapsed += rhs.elapsed;

        if let Some(rhs_removed) = rhs.removed {
            if let Some(ref mut removed) = self.removed {
                removed.add(rhs_removed);
            } else {
                self.removed = Some(rhs_removed);
            }
        }
    }
}

#[async_trait::async_trait]
/// `SyncReader` is a trait that provides an interface for reading data from a source.
/// The trait includes methods for getting a chunk reader, loading a file, downloading client log,
/// and checking whether chunk sync should be skipped.
pub(crate) trait SyncSourceReader: Send + Sync {
    /// Returns a chunk reader with the specified encryption mode.
    fn chunk_reader(&self, crypt_mode: CryptMode) -> Arc<dyn AsyncReadChunk>;

    /// Asynchronously loads a file from the source into a local file.
    /// `filename` is the name of the file to load from the source.
    /// `into` is the path of the local file to load the source file into.
    async fn load_file_into(&self, filename: &str, into: &Path) -> Result<Option<DataBlob>, Error>;

    /// Tries to download the client log from the source and save it into a local file.
    async fn try_download_client_log(&self, to_path: &Path) -> Result<(), Error>;

    fn skip_chunk_sync(&self, target_store_name: &str) -> bool;
}

pub(crate) struct RemoteSourceReader {
    pub(crate) backup_reader: Arc<BackupReader>,
    pub(crate) dir: BackupDir,
}

pub(crate) struct LocalSourceReader {
    pub(crate) _dir_lock: Arc<Mutex<proxmox_sys::fs::DirLockGuard>>,
    pub(crate) path: PathBuf,
    pub(crate) datastore: Arc<DataStore>,
}

#[async_trait::async_trait]
impl SyncSourceReader for RemoteSourceReader {
    fn chunk_reader(&self, crypt_mode: CryptMode) -> Arc<dyn AsyncReadChunk> {
        Arc::new(RemoteChunkReader::new(
            self.backup_reader.clone(),
            None,
            crypt_mode,
            HashMap::new(),
        ))
    }

    async fn load_file_into(&self, filename: &str, into: &Path) -> Result<Option<DataBlob>, Error> {
        let mut tmp_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .read(true)
            .open(into)?;
        let download_result = self.backup_reader.download(filename, &mut tmp_file).await;
        if let Err(err) = download_result {
            match err.downcast_ref::<HttpError>() {
                Some(HttpError { code, message }) => match *code {
                    StatusCode::NOT_FOUND => {
                        info!(
                            "skipping snapshot {} - vanished since start of sync",
                            &self.dir
                        );
                        return Ok(None);
                    }
                    _ => {
                        bail!("HTTP error {code} - {message}");
                    }
                },
                None => {
                    return Err(err);
                }
            };
        };
        tmp_file.rewind()?;
        Ok(DataBlob::load_from_reader(&mut tmp_file).ok())
    }

    async fn try_download_client_log(&self, to_path: &Path) -> Result<(), Error> {
        let mut tmp_path = to_path.to_owned();
        tmp_path.set_extension("tmp");

        let tmpfile = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(&tmp_path)?;

        // Note: be silent if there is no log - only log successful download
        if let Ok(()) = self
            .backup_reader
            .download(CLIENT_LOG_BLOB_NAME, tmpfile)
            .await
        {
            if let Err(err) = std::fs::rename(&tmp_path, to_path) {
                bail!("Atomic rename file {to_path:?} failed - {err}");
            }
            info!("got backup log file {CLIENT_LOG_BLOB_NAME:?}");
        }

        Ok(())
    }

    fn skip_chunk_sync(&self, _target_store_name: &str) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl SyncSourceReader for LocalSourceReader {
    fn chunk_reader(&self, crypt_mode: CryptMode) -> Arc<dyn AsyncReadChunk> {
        Arc::new(LocalChunkReader::new(
            self.datastore.clone(),
            None,
            crypt_mode,
        ))
    }

    async fn load_file_into(&self, filename: &str, into: &Path) -> Result<Option<DataBlob>, Error> {
        let mut tmp_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .read(true)
            .open(into)?;
        let mut from_path = self.path.clone();
        from_path.push(filename);
        tmp_file.write_all(std::fs::read(from_path)?.as_slice())?;
        tmp_file.rewind()?;
        Ok(DataBlob::load_from_reader(&mut tmp_file).ok())
    }

    async fn try_download_client_log(&self, _to_path: &Path) -> Result<(), Error> {
        Ok(())
    }

    fn skip_chunk_sync(&self, target_store_name: &str) -> bool {
        self.datastore.name() == target_store_name
    }
}
