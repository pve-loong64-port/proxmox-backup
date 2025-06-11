use std::fs::File;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::{bail, Context, Error};
use nix::dir::Dir;
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use pbs_config::BackupLockGuard;

use pbs_api_types::{
    print_store_and_ns, ArchiveType, BackupNamespace, Operation, CLIENT_LOG_BLOB_NAME,
    MANIFEST_BLOB_NAME,
};

use crate::backup_info::BackupDir;
use crate::dynamic_index::DynamicIndexReader;
use crate::fixed_index::FixedIndexReader;
use crate::index::IndexFile;
use crate::DataStore;

/// Helper to access the contents of a datastore backup snapshot
///
/// This make it easy to iterate over all used chunks and files.
pub struct SnapshotReader {
    snapshot: BackupDir,
    datastore_name: String,
    file_list: Vec<String>,
    locked_dir: Dir,

    // while this is never read, the lock needs to be kept until the
    // reader is dropped to ensure valid locking semantics
    _lock: BackupLockGuard,
}

impl SnapshotReader {
    /// Lock snapshot, reads the manifest and returns a new instance
    pub fn new(
        datastore: Arc<DataStore>,
        ns: BackupNamespace,
        snapshot: pbs_api_types::BackupDir,
    ) -> Result<Self, Error> {
        Self::new_do(datastore.backup_dir(ns, snapshot)?)
    }

    pub(crate) fn new_do(snapshot: BackupDir) -> Result<Self, Error> {
        let datastore = snapshot.datastore();
        let snapshot_path = snapshot.full_path();

        if !snapshot_path.exists() {
            bail!("snapshot {} does not exist!", snapshot.dir());
        }

        let lock = snapshot
            .lock_shared()
            .with_context(|| format!("while trying to read snapshot '{snapshot:?}'"))?;

        let locked_dir = Dir::open(&snapshot_path, OFlag::O_RDONLY, Mode::empty())
            .with_context(|| format!("unable to open snapshot directory {snapshot_path:?}"))?;

        let datastore_name = datastore.name().to_string();
        let manifest = match snapshot.load_manifest() {
            Ok((manifest, _)) => manifest,
            Err(err) => {
                bail!(
                    "manifest load error on {}, snapshot '{}' - {}",
                    print_store_and_ns(datastore.name(), snapshot.backup_ns()),
                    snapshot.dir(),
                    err
                );
            }
        };

        let mut client_log_path = snapshot_path;
        client_log_path.push(CLIENT_LOG_BLOB_NAME.as_ref());

        let mut file_list = vec![MANIFEST_BLOB_NAME.to_string()];
        for item in manifest.files() {
            file_list.push(item.filename.clone());
        }
        if client_log_path.exists() {
            file_list.push(CLIENT_LOG_BLOB_NAME.to_string());
        }

        Ok(Self {
            snapshot,
            datastore_name,
            file_list,
            locked_dir,
            _lock: lock,
        })
    }

    /// Return the snapshot directory
    pub fn snapshot(&self) -> &BackupDir {
        &self.snapshot
    }

    /// Return the datastore name
    pub fn datastore_name(&self) -> &str {
        &self.datastore_name
    }

    /// Returns the list of files the snapshot refers to.
    pub fn file_list(&self) -> &Vec<String> {
        &self.file_list
    }

    /// Opens a file inside the snapshot (using openat) for reading
    pub fn open_file(&self, filename: &str) -> Result<File, Error> {
        let raw_fd = nix::fcntl::openat(
            Some(self.locked_dir.as_raw_fd()),
            Path::new(filename),
            nix::fcntl::OFlag::O_RDONLY | nix::fcntl::OFlag::O_CLOEXEC,
            nix::sys::stat::Mode::empty(),
        )?;
        let file = unsafe { File::from_raw_fd(raw_fd) };
        Ok(file)
    }

    /// Returns an iterator for all chunks not skipped by `skip_fn`.
    pub fn chunk_iterator<F: Fn(&[u8; 32]) -> bool>(
        &self,
        skip_fn: F,
    ) -> Result<SnapshotChunkIterator<F>, Error> {
        SnapshotChunkIterator::new(self, skip_fn)
    }
}

/// Iterates over all chunks used by a backup snapshot
///
/// Note: The iterator returns a `Result`, and the iterator state is
/// undefined after the first error. So it make no sense to continue
/// iteration after the first error.
pub struct SnapshotChunkIterator<'a, F: Fn(&[u8; 32]) -> bool> {
    snapshot_reader: &'a SnapshotReader,
    todo_list: Vec<String>,
    skip_fn: F,
    #[allow(clippy::type_complexity)]
    current_index: Option<(Rc<Box<dyn IndexFile + Send>>, usize, Vec<(usize, u64)>)>,
}

impl<F: Fn(&[u8; 32]) -> bool> Iterator for SnapshotChunkIterator<'_, F> {
    type Item = Result<[u8; 32], Error>;

    fn next(&mut self) -> Option<Self::Item> {
        proxmox_lang::try_block!({
            loop {
                if self.current_index.is_none() {
                    if let Some(filename) = self.todo_list.pop() {
                        let file = self.snapshot_reader.open_file(&filename)?;
                        let index: Box<dyn IndexFile + Send> =
                            match ArchiveType::from_path(&filename)? {
                                ArchiveType::FixedIndex => Box::new(FixedIndexReader::new(file)?),
                                ArchiveType::DynamicIndex => {
                                    Box::new(DynamicIndexReader::new(file)?)
                                }
                                _ => bail!(
                                    "SnapshotChunkIterator: got unknown file type - internal error"
                                ),
                            };

                        let datastore = DataStore::lookup_datastore(
                            self.snapshot_reader.datastore_name(),
                            Some(Operation::Read),
                        )?;
                        let order =
                            datastore.get_chunks_in_order(&*index, &self.skip_fn, |_| Ok(()))?;

                        self.current_index = Some((Rc::new(index), 0, order));
                    } else {
                        return Ok(None);
                    }
                }
                let (index, pos, list) = self.current_index.take().unwrap();
                if pos < list.len() {
                    let (real_pos, _) = list[pos];
                    let digest = *index.index_digest(real_pos).unwrap();
                    self.current_index = Some((index, pos + 1, list));
                    return Ok(Some(digest));
                } else {
                    // pop next index
                }
            }
        })
        .transpose()
    }
}

impl<'a, F: Fn(&[u8; 32]) -> bool> SnapshotChunkIterator<'a, F> {
    pub fn new(snapshot_reader: &'a SnapshotReader, skip_fn: F) -> Result<Self, Error> {
        let mut todo_list = Vec::new();

        for filename in snapshot_reader.file_list() {
            match ArchiveType::from_path(filename)? {
                ArchiveType::FixedIndex | ArchiveType::DynamicIndex => {
                    todo_list.push(filename.to_owned());
                }
                ArchiveType::Blob => { /* no chunks, do nothing */ }
            }
        }

        Ok(Self {
            snapshot_reader,
            todo_list,
            current_index: None,
            skip_fn,
        })
    }
}
