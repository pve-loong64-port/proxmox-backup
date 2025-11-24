use pbs_config::BackupLockGuard;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{bail, Error};
use http_body_util::BodyExt;
use tracing::{error, info, warn};

use proxmox_worker_task::WorkerTaskContext;

use pbs_api_types::{
    print_ns_and_snapshot, print_store_and_ns, ArchiveType, Authid, BackupNamespace, BackupType,
    CryptMode, SnapshotVerifyState, VerifyState, PRIV_DATASTORE_BACKUP, PRIV_DATASTORE_VERIFY,
    UPID,
};
use pbs_datastore::backup_info::{BackupDir, BackupGroup, BackupInfo};
use pbs_datastore::index::{ChunkReadInfo, IndexFile};
use pbs_datastore::manifest::{BackupManifest, FileInfo};
use pbs_datastore::{DataBlob, DataStore, DatastoreBackend, StoreProgress};

use crate::tools::parallel_handler::{ParallelHandler, SendHandle};

use crate::backup::hierarchy::ListAccessibleBackupGroups;

/// A VerifyWorker encapsulates a task worker, datastore and information about which chunks have
/// already been verified or detected as corrupt.
pub struct VerifyWorker {
    worker: Arc<dyn WorkerTaskContext>,
    datastore: Arc<DataStore>,
    verified_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
    corrupt_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
    backend: DatastoreBackend,
    read_threads: usize,
    verify_threads: usize,
}

struct IndexVerifyState {
    read_bytes: AtomicU64,
    decoded_bytes: AtomicU64,
    errors: AtomicUsize,
    datastore: Arc<DataStore>,
    corrupt_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
    verified_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
    start_time: Instant,
}

impl IndexVerifyState {
    fn new(
        datastore: &Arc<DataStore>,
        corrupt_chunks: &Arc<Mutex<HashSet<[u8; 32]>>>,
        verified_chunks: &Arc<Mutex<HashSet<[u8; 32]>>>,
    ) -> Self {
        Self {
            read_bytes: AtomicU64::new(0),
            decoded_bytes: AtomicU64::new(0),
            errors: AtomicUsize::new(0),
            datastore: Arc::clone(datastore),
            corrupt_chunks: Arc::clone(corrupt_chunks),
            verified_chunks: Arc::clone(verified_chunks),
            start_time: Instant::now(),
        }
    }
}

impl VerifyWorker {
    /// Creates a new VerifyWorker for a given task worker and datastore.
    pub fn new(
        worker: Arc<dyn WorkerTaskContext>,
        datastore: Arc<DataStore>,
        read_threads: Option<usize>,
        verify_threads: Option<usize>,
    ) -> Result<Self, Error> {
        let backend = datastore.backend()?;
        let thread_settings = datastore.thread_settings();

        let read_threads = read_threads
            .or(thread_settings.verify_job_read_threads)
            .unwrap_or(1);

        let verify_threads = verify_threads
            .or(thread_settings.verify_job_verify_threads)
            .unwrap_or(4);

        Ok(Self {
            worker,
            datastore,
            // start with 16k chunks == up to 64G data
            verified_chunks: Arc::new(Mutex::new(HashSet::with_capacity(16 * 1024))),
            // start with 64 chunks since we assume there are few corrupt ones
            corrupt_chunks: Arc::new(Mutex::new(HashSet::with_capacity(64))),
            backend,
            read_threads,
            verify_threads,
        })
    }

    fn verify_blob(backup_dir: &BackupDir, info: &FileInfo) -> Result<(), Error> {
        let blob = backup_dir.load_blob(&info.filename)?;

        let raw_size = blob.raw_size();
        if raw_size != info.size {
            bail!("wrong size ({} != {})", info.size, raw_size);
        }

        let csum = openssl::sha::sha256(blob.raw_data());
        if csum != info.csum {
            bail!("wrong index checksum");
        }

        match blob.crypt_mode()? {
            CryptMode::Encrypt => Ok(()),
            CryptMode::None => {
                // digest already verified above
                blob.decode(None, None)?;
                Ok(())
            }
            CryptMode::SignOnly => bail!("Invalid CryptMode for blob"),
        }
    }

    fn verify_index_chunks(
        &self,
        index: Box<dyn IndexFile + Send>,
        crypt_mode: CryptMode,
    ) -> Result<(), Error> {
        let verify_state = Arc::new(IndexVerifyState::new(
            &self.datastore,
            &self.corrupt_chunks,
            &self.verified_chunks,
        ));

        let decoder_pool = ParallelHandler::new("verify chunk decoder", self.verify_threads, {
            let verify_state = Arc::clone(&verify_state);
            move |(chunk, digest, size): (DataBlob, [u8; 32], u64)| {
                let chunk_crypt_mode = match chunk.crypt_mode() {
                    Err(err) => {
                        verify_state.corrupt_chunks.lock().unwrap().insert(digest);
                        info!("can't verify chunk, unknown CryptMode - {err}");
                        verify_state.errors.fetch_add(1, Ordering::SeqCst);
                        return Ok(());
                    }
                    Ok(mode) => mode,
                };

                if chunk_crypt_mode != crypt_mode {
                    info!(
                    "chunk CryptMode {chunk_crypt_mode:?} does not match index CryptMode {crypt_mode:?}"
                );
                    verify_state.errors.fetch_add(1, Ordering::SeqCst);
                }

                if let Err(err) = chunk.verify_unencrypted(size as usize, &digest) {
                    verify_state.corrupt_chunks.lock().unwrap().insert(digest);
                    info!("{err}");
                    verify_state.errors.fetch_add(1, Ordering::SeqCst);
                    match verify_state.datastore.rename_corrupt_chunk(&digest) {
                        Ok(Some(new_path)) => info!("corrupt chunk renamed to {new_path:?}"),
                        Err(err) => info!("{err}"),
                        _ => (),
                    }
                } else {
                    verify_state.verified_chunks.lock().unwrap().insert(digest);
                }

                Ok(())
            }
        });

        let skip_chunk = |digest: &[u8; 32]| -> bool {
            if self.verified_chunks.lock().unwrap().contains(digest) {
                true
            } else if self.corrupt_chunks.lock().unwrap().contains(digest) {
                let digest_str = hex::encode(digest);
                info!("chunk {digest_str} was marked as corrupt");
                verify_state.errors.fetch_add(1, Ordering::SeqCst);
                true
            } else {
                false
            }
        };

        let check_abort = |pos: usize| -> Result<(), Error> {
            if pos & 1023 == 0 {
                self.worker.check_abort()?;
                self.worker.fail_on_shutdown()?;
            }
            Ok(())
        };

        let chunk_list = self
            .datastore
            .get_chunks_in_order(&*index, skip_chunk, check_abort)?;

        let reader_pool = ParallelHandler::new("read chunks", self.read_threads, {
            let decoder_pool = decoder_pool.channel();
            let verify_state = Arc::clone(&verify_state);
            let backend = self.backend.clone();

            move |info: ChunkReadInfo| {
                Self::verify_chunk_by_backend(
                    &backend,
                    Arc::clone(&verify_state),
                    &decoder_pool,
                    &info,
                )
            }
        });
        for (pos, _) in chunk_list {
            self.worker.check_abort()?;
            self.worker.fail_on_shutdown()?;

            let info = index.chunk_info(pos).unwrap();

            // we must always recheck this here, the parallel worker below alter it!
            if skip_chunk(&info.digest) {
                continue; // already verified or marked corrupt
            }

            reader_pool.send(info)?;
        }

        reader_pool.complete()?;

        let elapsed = verify_state.start_time.elapsed().as_secs_f64();

        let read_bytes = verify_state.read_bytes.load(Ordering::SeqCst);
        let decoded_bytes = verify_state.decoded_bytes.load(Ordering::SeqCst);

        let read_bytes_mib = (read_bytes as f64) / (1024.0 * 1024.0);
        let decoded_bytes_mib = (decoded_bytes as f64) / (1024.0 * 1024.0);

        let read_speed = read_bytes_mib / elapsed;
        let decode_speed = decoded_bytes_mib / elapsed;

        let error_count = verify_state.errors.load(Ordering::SeqCst);

        info!(
            "  verified {read_bytes_mib:.2}/{decoded_bytes_mib:.2} MiB in {elapsed:.2} seconds, speed {read_speed:.2}/{decode_speed:.2} MiB/s ({error_count} errors)"
        );

        if verify_state.errors.load(Ordering::SeqCst) > 0 {
            bail!("chunks could not be verified");
        }

        Ok(())
    }

    fn verify_chunk_by_backend(
        backend: &DatastoreBackend,
        verify_state: Arc<IndexVerifyState>,
        decoder_pool: &SendHandle<(DataBlob, [u8; 32], u64)>,
        info: &ChunkReadInfo,
    ) -> Result<(), Error> {
        match backend {
            DatastoreBackend::Filesystem => match verify_state.datastore.load_chunk(&info.digest) {
                Err(err) => Self::add_corrupt_chunk(
                    verify_state,
                    info.digest,
                    &format!("can't verify chunk, load failed - {err}"),
                ),
                Ok(chunk) => {
                    let size = info.size();
                    verify_state
                        .read_bytes
                        .fetch_add(chunk.raw_size(), Ordering::SeqCst);
                    decoder_pool.send((chunk, info.digest, size))?;
                    verify_state.decoded_bytes.fetch_add(size, Ordering::SeqCst);
                }
            },
            DatastoreBackend::S3(s3_client) => {
                let object_key = pbs_datastore::s3::object_key_from_digest(&info.digest)?;
                match proxmox_async::runtime::block_on(s3_client.get_object(object_key)) {
                    Ok(Some(response)) => {
                        match proxmox_async::runtime::block_on(response.content.collect()) {
                            Ok(raw_chunk) => {
                                match DataBlob::from_raw(raw_chunk.to_bytes().to_vec()) {
                                    Ok(chunk) => {
                                        let size = info.size();
                                        verify_state
                                            .read_bytes
                                            .fetch_add(chunk.raw_size(), Ordering::SeqCst);
                                        decoder_pool.send((chunk, info.digest, size))?;
                                        verify_state
                                            .decoded_bytes
                                            .fetch_add(size, Ordering::SeqCst);
                                    }
                                    Err(err) => Self::add_corrupt_chunk(
                                        verify_state,
                                        info.digest,
                                        &format!(
                                            "can't verify chunk with digest {} - {err}",
                                            hex::encode(info.digest)
                                        ),
                                    ),
                                }
                            }
                            Err(err) => {
                                verify_state.errors.fetch_add(1, Ordering::SeqCst);
                                error!("can't verify chunk, load failed - {err}");
                            }
                        }
                    }
                    Ok(None) => Self::add_corrupt_chunk(
                        verify_state,
                        info.digest,
                        &format!(
                            "can't verify missing chunk with digest {}",
                            hex::encode(info.digest)
                        ),
                    ),
                    Err(err) => {
                        verify_state.errors.fetch_add(1, Ordering::SeqCst);
                        error!("can't verify chunk, load failed - {err}");
                    }
                }
            }
        }
        Ok(())
    }

    fn add_corrupt_chunk(verify_state: Arc<IndexVerifyState>, digest: [u8; 32], message: &str) {
        // Panic on poisoned mutex
        let mut corrupt_chunks = verify_state.corrupt_chunks.lock().unwrap();
        corrupt_chunks.insert(digest);
        error!(message);
        verify_state.errors.fetch_add(1, Ordering::SeqCst);
        match verify_state.datastore.rename_corrupt_chunk(&digest) {
            Ok(Some(new_path)) => info!("corrupt chunk renamed to {new_path:?}"),
            Err(err) => info!("{err}"),
            _ => (),
        }
    }

    fn verify_fixed_index(&self, backup_dir: &BackupDir, info: &FileInfo) -> Result<(), Error> {
        let mut path = backup_dir.relative_path();
        path.push(&info.filename);

        let index = self.datastore.open_fixed_reader(&path)?;

        let (csum, size) = index.compute_csum();
        if size != info.size {
            bail!("wrong size ({} != {})", info.size, size);
        }

        if csum != info.csum {
            bail!("wrong index checksum");
        }

        self.verify_index_chunks(Box::new(index), info.chunk_crypt_mode())
    }

    fn verify_dynamic_index(&self, backup_dir: &BackupDir, info: &FileInfo) -> Result<(), Error> {
        let mut path = backup_dir.relative_path();
        path.push(&info.filename);

        let index = self.datastore.open_dynamic_reader(&path)?;

        let (csum, size) = index.compute_csum();
        if size != info.size {
            bail!("wrong size ({} != {})", info.size, size);
        }

        if csum != info.csum {
            bail!("wrong index checksum");
        }

        self.verify_index_chunks(Box::new(index), info.chunk_crypt_mode())
    }

    /// Verify a single backup snapshot
    ///
    /// This checks all archives inside a backup snapshot.
    /// Errors are logged to the worker log.
    ///
    /// Returns
    /// - Ok(true) if verify is successful
    /// - Ok(false) if there were verification errors
    /// - Err(_) if task was aborted
    pub fn verify_backup_dir(
        &self,
        backup_dir: &BackupDir,
        upid: UPID,
        filter: Option<&dyn Fn(&BackupManifest) -> bool>,
    ) -> Result<bool, Error> {
        if !backup_dir.full_path().exists() {
            info!(
                "SKIPPED: verify {}:{} - snapshot does not exist (anymore).",
                self.datastore.name(),
                backup_dir.dir(),
            );
            return Ok(true);
        }

        let snap_lock = backup_dir.lock_shared();

        match snap_lock {
            Ok(snap_lock) => self.verify_backup_dir_with_lock(backup_dir, upid, filter, snap_lock),
            Err(err) => {
                info!(
                    "SKIPPED: verify {}:{} - could not acquire snapshot lock: {}",
                    self.datastore.name(),
                    backup_dir.dir(),
                    err,
                );
                Ok(true)
            }
        }
    }

    /// See verify_backup_dir
    pub fn verify_backup_dir_with_lock(
        &self,
        backup_dir: &BackupDir,
        upid: UPID,
        filter: Option<&dyn Fn(&BackupManifest) -> bool>,
        _snap_lock: BackupLockGuard,
    ) -> Result<bool, Error> {
        let datastore_name = self.datastore.name();
        let backup_dir_name = backup_dir.dir();

        let manifest = match backup_dir.load_manifest() {
            Ok((manifest, _)) => manifest,
            Err(err) => {
                info!("verify {datastore_name}:{backup_dir_name} - manifest load error: {err}");
                return Ok(false);
            }
        };

        if let Some(filter) = filter {
            if !filter(&manifest) {
                info!("SKIPPED: verify {datastore_name}:{backup_dir_name} (recently verified)");
                return Ok(true);
            }
        }

        info!("verify {datastore_name}:{backup_dir_name}");

        let mut error_count = 0;

        let mut verify_result = VerifyState::Ok;
        for info in manifest.files() {
            let result = proxmox_lang::try_block!({
                info!("  check {}", info.filename);
                match ArchiveType::from_path(&info.filename)? {
                    ArchiveType::FixedIndex => self.verify_fixed_index(backup_dir, info),
                    ArchiveType::DynamicIndex => self.verify_dynamic_index(backup_dir, info),
                    ArchiveType::Blob => Self::verify_blob(backup_dir, info),
                }
            });

            self.worker.check_abort()?;
            self.worker.fail_on_shutdown()?;

            if let Err(err) = result {
                info!(
                    "verify {datastore_name}:{backup_dir_name}/{file_name} failed: {err}",
                    file_name = info.filename,
                );
                error_count += 1;
                verify_result = VerifyState::Failed;
            }
        }

        let verify_state = SnapshotVerifyState {
            state: verify_result,
            upid,
        };

        if let Err(err) = {
            let verify_state = serde_json::to_value(verify_state)?;
            backup_dir.update_manifest(&self.datastore.backend()?, |manifest| {
                manifest.unprotected["verify_state"] = verify_state;
            })
        } {
            info!("verify {datastore_name}:{backup_dir_name} - manifest update error: {err}");
            return Ok(false);
        }

        Ok(error_count == 0)
    }

    /// Verify all backups inside a backup group
    ///
    /// Errors are logged to the worker log.
    ///
    /// Returns
    /// - Ok((count, failed_dirs)) where failed_dirs had verification errors
    /// - Err(_) if task was aborted
    pub fn verify_backup_group(
        &self,
        group: &BackupGroup,
        progress: &mut StoreProgress,
        upid: &UPID,
        filter: Option<&dyn Fn(&BackupManifest) -> bool>,
    ) -> Result<Vec<String>, Error> {
        let mut errors = Vec::new();
        let mut list = match group.list_backups() {
            Ok(list) => list,
            Err(err) => {
                info!(
                    "verify {}, group {} - unable to list backups: {}",
                    print_store_and_ns(self.datastore.name(), group.backup_ns()),
                    group.group(),
                    err,
                );
                return Ok(errors);
            }
        };

        let snapshot_count = list.len();
        info!(
            "verify group {}:{} ({} snapshots)",
            self.datastore.name(),
            group.group(),
            snapshot_count
        );

        progress.group_snapshots = snapshot_count as u64;

        BackupInfo::sort_list(&mut list, false); // newest first
        for (pos, info) in list.into_iter().enumerate() {
            if !self.verify_backup_dir(&info.backup_dir, upid.clone(), filter)? {
                errors.push(print_ns_and_snapshot(
                    info.backup_dir.backup_ns(),
                    info.backup_dir.as_ref(),
                ));
            }
            progress.done_snapshots = pos as u64 + 1;
            info!("percentage done: {progress}");
        }
        Ok(errors)
    }

    /// Verify all (owned) backups inside a datastore
    ///
    /// Errors are logged to the worker log.
    ///
    /// Returns
    /// - Ok(failed_dirs) where failed_dirs had verification errors
    /// - Err(_) if task was aborted
    pub fn verify_all_backups(
        &self,
        upid: &UPID,
        ns: BackupNamespace,
        max_depth: Option<usize>,
        owner: Option<&Authid>,
        filter: Option<&dyn Fn(&BackupManifest) -> bool>,
    ) -> Result<Vec<String>, Error> {
        let mut errors = Vec::new();

        info!("verify datastore {}", self.datastore.name());

        let owner_filtered = if let Some(owner) = &owner {
            info!("limiting to backups owned by {owner}");
            true
        } else {
            false
        };

        // FIXME: This should probably simply enable recursion (or the call have a recursion parameter)
        let store = &self.datastore;
        let max_depth = max_depth.unwrap_or(pbs_api_types::MAX_NAMESPACE_DEPTH);

        let mut list = match ListAccessibleBackupGroups::new_with_privs(
            store,
            ns.clone(),
            max_depth,
            Some(PRIV_DATASTORE_VERIFY),
            Some(PRIV_DATASTORE_BACKUP),
            owner,
        ) {
            Ok(list) => list
                .filter_map(|group| match group {
                    Ok(group) => Some(group),
                    Err(err) if owner_filtered => {
                        // intentionally not in task log, the user might not see this group!
                        println!("error on iterating groups in ns '{ns}' - {err}");
                        None
                    }
                    Err(err) => {
                        // we don't filter by owner, but we want to log the error
                        info!("error on iterating groups in ns '{ns}' - {err}");
                        errors.push(err.to_string());
                        None
                    }
                })
                .filter(|group| {
                    !(group.backup_type() == BackupType::Host && group.backup_id() == "benchmark")
                })
                .collect::<Vec<BackupGroup>>(),
            Err(err) => {
                info!("unable to list backups: {err}");
                return Ok(errors);
            }
        };

        list.sort_unstable_by(|a, b| a.group().cmp(b.group()));

        let group_count = list.len();
        info!("found {group_count} groups");

        log::info!(
            "using {} read and {} verify thread(s)",
            self.read_threads,
            self.verify_threads,
        );

        let mut progress = StoreProgress::new(group_count as u64);

        for (pos, group) in list.into_iter().enumerate() {
            progress.done_groups = pos as u64;
            progress.done_snapshots = 0;
            progress.group_snapshots = 0;

            let mut group_errors = self.verify_backup_group(&group, &mut progress, upid, filter)?;
            errors.append(&mut group_errors);
        }

        Ok(errors)
    }

    /// Filter out any snapshot from being (re-)verified where this fn returns false.
    pub fn verify_filter(
        ignore_verified_snapshots: bool,
        outdated_after: Option<i64>,
        manifest: &BackupManifest,
    ) -> bool {
        if !ignore_verified_snapshots {
            return true;
        }

        match manifest.verify_state() {
            Err(err) => {
                warn!("error reading manifest: {err:#}");
                true
            }
            Ok(None) => true, // no last verification, always include
            Ok(Some(last_verify)) => {
                match outdated_after {
                    None => false, // never re-verify if ignored and no max age
                    Some(max_age) => {
                        let now = proxmox_time::epoch_i64();
                        let days_since_last_verify = (now - last_verify.upid.starttime) / 86400;

                        days_since_last_verify > max_age
                    }
                }
            }
        }
    }
}
