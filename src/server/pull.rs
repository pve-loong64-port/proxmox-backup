//! Sync datastore by pulling contents from remote server

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::io::Seek;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use anyhow::{bail, format_err, Context, Error};
use proxmox_human_byte::HumanByte;
use tracing::{info, warn};

use pbs_api_types::{
    print_store_and_ns, ArchiveType, Authid, BackupArchiveName, BackupDir, BackupGroup,
    BackupNamespace, GroupFilter, Operation, RateLimitConfig, Remote, SnapshotListItem,
    VerifyState, CLIENT_LOG_BLOB_NAME, MANIFEST_BLOB_NAME, MAX_NAMESPACE_DEPTH,
    PRIV_DATASTORE_AUDIT, PRIV_DATASTORE_BACKUP,
};
use pbs_client::BackupRepository;
use pbs_config::CachedUserInfo;
use pbs_datastore::data_blob::DataBlob;
use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_datastore::fixed_index::FixedIndexReader;
use pbs_datastore::index::IndexFile;
use pbs_datastore::manifest::{BackupManifest, FileInfo};
use pbs_datastore::read_chunk::AsyncReadChunk;
use pbs_datastore::{check_backup_owner, DataStore, DatastoreBackend, StoreProgress};
use pbs_tools::sha::sha256;

use super::sync::{
    check_namespace_depth_limit, exclude_not_verified_or_encrypted,
    ignore_not_verified_or_encrypted, LocalSource, RemoteSource, RemovedVanishedStats, SkipInfo,
    SkipReason, SyncSource, SyncSourceReader, SyncStats,
};
use crate::backup::{check_ns_modification_privs, check_ns_privs};
use crate::tools::parallel_handler::ParallelHandler;

pub(crate) struct PullTarget {
    store: Arc<DataStore>,
    ns: BackupNamespace,
    // Contains the active S3Client in case of S3 backend
    backend: DatastoreBackend,
}

/// Parameters for a pull operation.
pub(crate) struct PullParameters {
    /// Where data is pulled from
    source: Arc<dyn SyncSource>,
    /// Where data should be pulled into
    target: PullTarget,
    /// Owner of synced groups (needs to match local owner of pre-existing groups)
    owner: Authid,
    /// Whether to remove groups which exist locally, but not on the remote end
    remove_vanished: bool,
    /// How many levels of sub-namespaces to pull (0 == no recursion, None == maximum recursion)
    max_depth: Option<usize>,
    /// Filters for reducing the pull scope
    group_filter: Vec<GroupFilter>,
    /// How many snapshots should be transferred at most (taking the newest N snapshots)
    transfer_last: Option<usize>,
    /// Only sync encrypted backup snapshots
    encrypted_only: bool,
    /// Only sync verified backup snapshots
    verified_only: bool,
    /// Whether to re-sync corrupted snapshots
    resync_corrupt: bool,
}

impl PullParameters {
    /// Creates a new instance of `PullParameters`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: &str,
        ns: BackupNamespace,
        remote: Option<&str>,
        remote_store: &str,
        remote_ns: BackupNamespace,
        owner: Authid,
        remove_vanished: Option<bool>,
        max_depth: Option<usize>,
        group_filter: Option<Vec<GroupFilter>>,
        limit: RateLimitConfig,
        transfer_last: Option<usize>,
        encrypted_only: Option<bool>,
        verified_only: Option<bool>,
        resync_corrupt: Option<bool>,
    ) -> Result<Self, Error> {
        if let Some(max_depth) = max_depth {
            ns.check_max_depth(max_depth)?;
            remote_ns.check_max_depth(max_depth)?;
        };
        let remove_vanished = remove_vanished.unwrap_or(false);
        let resync_corrupt = resync_corrupt.unwrap_or(false);
        let encrypted_only = encrypted_only.unwrap_or(false);
        let verified_only = verified_only.unwrap_or(false);

        let source: Arc<dyn SyncSource> = if let Some(remote) = remote {
            let (remote_config, _digest) = pbs_config::remote::config()?;
            let remote: Remote = remote_config.lookup("remote", remote)?;

            let repo = BackupRepository::new(
                Some(remote.config.auth_id.clone()),
                Some(remote.config.host.clone()),
                remote.config.port,
                remote_store.to_string(),
            );
            let client = crate::api2::config::remote::remote_client_config(&remote, Some(limit))?;
            Arc::new(RemoteSource {
                repo,
                ns: remote_ns,
                client,
            })
        } else {
            Arc::new(LocalSource {
                store: DataStore::lookup_datastore(remote_store, Some(Operation::Read))?,
                ns: remote_ns,
            })
        };
        let store = DataStore::lookup_datastore(store, Some(Operation::Write))?;
        let backend = store.backend()?;
        let target = PullTarget { store, ns, backend };

        let group_filter = group_filter.unwrap_or_default();

        Ok(Self {
            source,
            target,
            owner,
            remove_vanished,
            max_depth,
            group_filter,
            transfer_last,
            encrypted_only,
            verified_only,
            resync_corrupt,
        })
    }
}

async fn pull_index_chunks<I: IndexFile>(
    chunk_reader: Arc<dyn AsyncReadChunk>,
    target: Arc<DataStore>,
    index: I,
    encountered_chunks: Arc<Mutex<EncounteredChunks>>,
    backend: &DatastoreBackend,
) -> Result<SyncStats, Error> {
    use futures::stream::{self, StreamExt, TryStreamExt};

    let start_time = SystemTime::now();

    let stream = stream::iter(
        (0..index.index_count())
            .map(|pos| index.chunk_info(pos).unwrap())
            .filter(|info| {
                let guard = encountered_chunks.lock().unwrap();
                match guard.check_reusable(&info.digest) {
                    Some(touched) => !touched, // reusable and already touched, can always skip
                    None => true,
                }
            }),
    );

    let target2 = target.clone();
    let backend = backend.clone();
    let verify_pool = ParallelHandler::new(
        "sync chunk writer",
        4,
        move |(chunk, digest, size): (DataBlob, [u8; 32], u64)| {
            // println!("verify and write {}", hex::encode(&digest));
            chunk.verify_unencrypted(size as usize, &digest)?;
            target2.insert_chunk(&chunk, &digest, &backend)?;
            Ok(())
        },
    );

    let verify_and_write_channel = verify_pool.channel();

    let bytes = Arc::new(AtomicUsize::new(0));
    let chunk_count = Arc::new(AtomicUsize::new(0));

    stream
        .map(|info| {
            let target = Arc::clone(&target);
            let chunk_reader = chunk_reader.clone();
            let bytes = Arc::clone(&bytes);
            let chunk_count = Arc::clone(&chunk_count);
            let verify_and_write_channel = verify_and_write_channel.clone();
            let encountered_chunks = Arc::clone(&encountered_chunks);

            Ok::<_, Error>(async move {
                {
                    // limit guard scope
                    let mut guard = encountered_chunks.lock().unwrap();
                    if let Some(touched) = guard.check_reusable(&info.digest) {
                        if touched {
                            return Ok::<_, Error>(());
                        }
                        let chunk_exists = proxmox_async::runtime::block_in_place(|| {
                            target.cond_touch_chunk(&info.digest, false)
                        })?;
                        if chunk_exists {
                            guard.mark_touched(&info.digest);
                            //info!("chunk {} exists {}", pos, hex::encode(digest));
                            return Ok::<_, Error>(());
                        }
                    }
                    // mark before actually downloading the chunk, so this happens only once
                    guard.mark_reusable(&info.digest);
                    guard.mark_touched(&info.digest);
                }

                //info!("sync {} chunk {}", pos, hex::encode(digest));
                let chunk = chunk_reader.read_raw_chunk(&info.digest).await?;
                let raw_size = chunk.raw_size() as usize;

                // decode, verify and write in a separate threads to maximize throughput
                proxmox_async::runtime::block_in_place(|| {
                    verify_and_write_channel.send((chunk, info.digest, info.size()))
                })?;

                bytes.fetch_add(raw_size, Ordering::SeqCst);
                chunk_count.fetch_add(1, Ordering::SeqCst);

                Ok(())
            })
        })
        .try_buffer_unordered(20)
        .try_for_each(|_res| futures::future::ok(()))
        .await?;

    drop(verify_and_write_channel);

    tokio::task::spawn_blocking(|| verify_pool.complete()).await??;

    let elapsed = start_time.elapsed()?;

    let bytes = bytes.load(Ordering::SeqCst);
    let chunk_count = chunk_count.load(Ordering::SeqCst);

    info!(
        "downloaded {} ({}/s)",
        HumanByte::from(bytes),
        HumanByte::new_binary(bytes as f64 / elapsed.as_secs_f64()),
    );

    Ok(SyncStats {
        chunk_count,
        bytes,
        elapsed,
        removed: None,
    })
}

fn verify_archive(info: &FileInfo, csum: &[u8; 32], size: u64) -> Result<(), Error> {
    if size != info.size {
        bail!(
            "wrong size for file '{}' ({} != {})",
            info.filename,
            info.size,
            size
        );
    }

    if csum != &info.csum {
        bail!("wrong checksum for file '{}'", info.filename);
    }

    Ok(())
}

/// Pulls a single file referenced by a manifest.
///
/// Pulling an archive consists of the following steps:
/// - Load archive file into tmp file
///   -- Load file into tmp file
///   -- Verify tmp file checksum
/// - if archive is an index, pull referenced chunks
/// - Rename tmp file into real path
async fn pull_single_archive<'a>(
    reader: Arc<dyn SyncSourceReader + 'a>,
    snapshot: &'a pbs_datastore::BackupDir,
    archive_info: &'a FileInfo,
    encountered_chunks: Arc<Mutex<EncounteredChunks>>,
    backend: &DatastoreBackend,
) -> Result<SyncStats, Error> {
    let archive_name = &archive_info.filename;
    let mut path = snapshot.full_path();
    path.push(archive_name);

    let mut tmp_path = path.clone();
    tmp_path.set_extension("tmp");

    let mut sync_stats = SyncStats::default();

    info!("sync archive {archive_name}");

    reader.load_file_into(archive_name, &tmp_path).await?;

    let mut tmpfile = std::fs::OpenOptions::new().read(true).open(&tmp_path)?;

    match ArchiveType::from_path(archive_name)? {
        ArchiveType::DynamicIndex => {
            let index = DynamicIndexReader::new(tmpfile).map_err(|err| {
                format_err!("unable to read dynamic index {:?} - {}", tmp_path, err)
            })?;
            let (csum, size) = index.compute_csum();
            verify_archive(archive_info, &csum, size)?;

            if reader.skip_chunk_sync(snapshot.datastore().name()) {
                info!("skipping chunk sync for same datastore");
            } else {
                let stats = pull_index_chunks(
                    reader
                        .chunk_reader(archive_info.crypt_mode)
                        .context("failed to get chunk reader")?,
                    snapshot.datastore().clone(),
                    index,
                    encountered_chunks,
                    backend,
                )
                .await?;
                sync_stats.add(stats);
            }
        }
        ArchiveType::FixedIndex => {
            let index = FixedIndexReader::new(tmpfile).map_err(|err| {
                format_err!("unable to read fixed index '{:?}' - {}", tmp_path, err)
            })?;
            let (csum, size) = index.compute_csum();
            verify_archive(archive_info, &csum, size)?;

            if reader.skip_chunk_sync(snapshot.datastore().name()) {
                info!("skipping chunk sync for same datastore");
            } else {
                let stats = pull_index_chunks(
                    reader
                        .chunk_reader(archive_info.crypt_mode)
                        .context("failed to get chunk reader")?,
                    snapshot.datastore().clone(),
                    index,
                    encountered_chunks,
                    backend,
                )
                .await?;
                sync_stats.add(stats);
            }
        }
        ArchiveType::Blob => {
            tmpfile.rewind()?;
            let (csum, size) = sha256(&mut tmpfile)?;
            verify_archive(archive_info, &csum, size)?;
        }
    }
    if let Err(err) = std::fs::rename(&tmp_path, &path) {
        bail!("Atomic rename file {:?} failed - {}", path, err);
    }

    backend
        .upload_index_to_backend(snapshot, archive_name)
        .await?;

    Ok(sync_stats)
}

/// Actual implementation of pulling a snapshot.
///
/// Pulling a snapshot consists of the following steps:
/// - (Re)download the manifest
///   -- if it matches and is not corrupt, only download log and treat snapshot as already synced
/// - Iterate over referenced files
///   -- if file already exists, verify contents
///   -- if not, pull it from the remote
/// - Download log if not already existing
async fn pull_snapshot<'a>(
    params: &PullParameters,
    reader: Arc<dyn SyncSourceReader + 'a>,
    snapshot: &'a pbs_datastore::BackupDir,
    encountered_chunks: Arc<Mutex<EncounteredChunks>>,
    corrupt: bool,
    is_new: bool,
) -> Result<SyncStats, Error> {
    if is_new {
        info!("sync snapshot {}", snapshot.dir());
    } else if corrupt {
        info!("re-sync snapshot {} due to corruption", snapshot.dir());
    } else {
        info!("re-sync snapshot {}", snapshot.dir());
    }

    let mut sync_stats = SyncStats::default();
    let mut manifest_name = snapshot.full_path();
    manifest_name.push(MANIFEST_BLOB_NAME.as_ref());

    let mut client_log_name = snapshot.full_path();
    client_log_name.push(CLIENT_LOG_BLOB_NAME.as_ref());

    let mut tmp_manifest_name = manifest_name.clone();
    tmp_manifest_name.set_extension("tmp");
    let tmp_manifest_blob;
    if let Some(data) = reader
        .load_file_into(MANIFEST_BLOB_NAME.as_ref(), &tmp_manifest_name)
        .await?
    {
        tmp_manifest_blob = data;
    } else {
        return Ok(sync_stats);
    }

    if manifest_name.exists() && !corrupt {
        let manifest_blob = proxmox_lang::try_block!({
            let mut manifest_file = std::fs::File::open(&manifest_name).map_err(|err| {
                format_err!("unable to open local manifest {manifest_name:?} - {err}")
            })?;

            let manifest_blob = DataBlob::load_from_reader(&mut manifest_file)?;
            Ok(manifest_blob)
        })
        .map_err(|err: Error| {
            format_err!("unable to read local manifest {manifest_name:?} - {err}")
        })?;

        if manifest_blob.raw_data() == tmp_manifest_blob.raw_data() {
            if !client_log_name.exists() {
                reader.try_download_client_log(&client_log_name).await?;
            };
            info!("no data changes");
            let _ = std::fs::remove_file(&tmp_manifest_name);
            return Ok(sync_stats); // nothing changed
        }
    }

    let manifest_data = tmp_manifest_blob.raw_data().to_vec();
    let manifest = BackupManifest::try_from(tmp_manifest_blob)?;

    if ignore_not_verified_or_encrypted(
        &manifest,
        snapshot.dir(),
        params.verified_only,
        params.encrypted_only,
    ) {
        if is_new {
            let path = snapshot.full_path();
            // safe to remove as locked by caller
            std::fs::remove_dir_all(&path).map_err(|err| {
                format_err!("removing temporary backup snapshot {path:?} failed - {err}")
            })?;
        }
        return Ok(sync_stats);
    }

    let backend = &params.target.backend;
    for item in manifest.files() {
        let mut path = snapshot.full_path();
        path.push(&item.filename);

        if !corrupt && path.exists() {
            let filename: BackupArchiveName = item.filename.as_str().try_into()?;
            match filename.archive_type() {
                ArchiveType::DynamicIndex => {
                    let index = DynamicIndexReader::open(&path)?;
                    let (csum, size) = index.compute_csum();
                    match manifest.verify_file(&filename, &csum, size) {
                        Ok(_) => continue,
                        Err(err) => {
                            info!("detected changed file {path:?} - {err}");
                        }
                    }
                }
                ArchiveType::FixedIndex => {
                    let index = FixedIndexReader::open(&path)?;
                    let (csum, size) = index.compute_csum();
                    match manifest.verify_file(&filename, &csum, size) {
                        Ok(_) => continue,
                        Err(err) => {
                            info!("detected changed file {path:?} - {err}");
                        }
                    }
                }
                ArchiveType::Blob => {
                    let mut tmpfile = std::fs::File::open(&path)?;
                    let (csum, size) = sha256(&mut tmpfile)?;
                    match manifest.verify_file(&filename, &csum, size) {
                        Ok(_) => continue,
                        Err(err) => {
                            info!("detected changed file {path:?} - {err}");
                        }
                    }
                }
            }
        }

        let stats = pull_single_archive(
            reader.clone(),
            snapshot,
            item,
            encountered_chunks.clone(),
            backend,
        )
        .await?;
        sync_stats.add(stats);
    }

    if let Err(err) = std::fs::rename(&tmp_manifest_name, &manifest_name) {
        bail!("Atomic rename file {:?} failed - {}", manifest_name, err);
    }
    if let DatastoreBackend::S3(s3_client) = backend {
        let object_key = pbs_datastore::s3::object_key_from_path(
            &snapshot.relative_path(),
            MANIFEST_BLOB_NAME.as_ref(),
        )
        .context("invalid manifest object key")?;

        let data = hyper::body::Bytes::from(manifest_data);
        let _is_duplicate = s3_client
            .upload_replace_with_retry(object_key, data)
            .await
            .context("failed to upload manifest to s3 backend")?;
    }

    if !client_log_name.exists() {
        reader.try_download_client_log(&client_log_name).await?;
        if client_log_name.exists() {
            if let DatastoreBackend::S3(s3_client) = backend {
                let object_key = pbs_datastore::s3::object_key_from_path(
                    &snapshot.relative_path(),
                    CLIENT_LOG_BLOB_NAME.as_ref(),
                )
                .context("invalid archive object key")?;

                let data = tokio::fs::read(&client_log_name)
                    .await
                    .context("failed to read log file contents")?;
                let contents = hyper::body::Bytes::from(data);
                let _is_duplicate = s3_client
                    .upload_replace_with_retry(object_key, contents)
                    .await
                    .context("failed to upload client log to s3 backend")?;
            }
        }
    };
    snapshot
        .cleanup_unreferenced_files(&manifest)
        .map_err(|err| format_err!("failed to cleanup unreferenced files - {err}"))?;

    Ok(sync_stats)
}

/// Pulls a `snapshot`, removing newly created ones on error, but keeping existing ones in any case.
///
/// The `reader` is configured to read from the source backup directory, while the
/// `snapshot` is pointing to the local datastore and target namespace.
async fn pull_snapshot_from<'a>(
    params: &PullParameters,
    reader: Arc<dyn SyncSourceReader + 'a>,
    snapshot: &'a pbs_datastore::BackupDir,
    encountered_chunks: Arc<Mutex<EncounteredChunks>>,
    corrupt: bool,
) -> Result<SyncStats, Error> {
    let (_path, is_new, _snap_lock) = snapshot
        .datastore()
        .create_locked_backup_dir(snapshot.backup_ns(), snapshot.as_ref())?;

    let result = pull_snapshot(
        params,
        reader,
        snapshot,
        encountered_chunks,
        corrupt,
        is_new,
    )
    .await;

    if is_new {
        // Cleanup directory on error if snapshot was not present before
        match result {
            Err(err) => {
                if let Err(cleanup_err) = snapshot.datastore().remove_backup_dir(
                    snapshot.backup_ns(),
                    snapshot.as_ref(),
                    true,
                ) {
                    info!("cleanup error - {cleanup_err}");
                }
                return Err(err);
            }
            Ok(_) => info!("sync snapshot {} done", snapshot.dir()),
        }
    }

    result
}

/// Pulls a group according to `params`.
///
/// Pulling a group consists of the following steps:
/// - Query the list of snapshots available for this group in the source namespace on the remote
/// - Sort by snapshot time
/// - Get last snapshot timestamp on local datastore
/// - Iterate over list of snapshots
///   -- pull snapshot, unless it's not finished yet or older than last local snapshot
/// - (remove_vanished) list all local snapshots, remove those that don't exist on remote
///
/// Backwards-compat: if `source_namespace` is [None], only the group type and ID will be sent to the
/// remote when querying snapshots. This allows us to interact with old remotes that don't have
/// namespace support yet.
///
/// Permission checks:
/// - remote snapshot access is checked by remote (twice: query and opening the backup reader)
/// - local group owner is already checked by pull_store
async fn pull_group(
    params: &PullParameters,
    source_namespace: &BackupNamespace,
    group: &BackupGroup,
    progress: &mut StoreProgress,
    encountered_chunks: Arc<Mutex<EncounteredChunks>>,
) -> Result<SyncStats, Error> {
    let mut already_synced_skip_info = SkipInfo::new(SkipReason::AlreadySynced);
    let mut transfer_last_skip_info = SkipInfo::new(SkipReason::TransferLast);

    let mut raw_list: Vec<SnapshotListItem> = params
        .source
        .list_backup_snapshots(source_namespace, group)
        .await?;
    raw_list.sort_unstable_by(|a, b| a.backup.time.cmp(&b.backup.time));

    let target_ns = source_namespace.map_prefix(&params.source.get_ns(), &params.target.ns)?;

    let mut source_snapshots = HashSet::new();
    let last_sync_time = params
        .target
        .store
        .last_successful_backup(&target_ns, group)?
        .unwrap_or(i64::MIN);

    // Filter remote BackupDirs to include in pull
    // Also stores if the snapshot is corrupt (verification job failed)
    let list: Vec<(BackupDir, bool)> = raw_list
        .into_iter()
        .filter_map(|item| {
            if exclude_not_verified_or_encrypted(&item, params.verified_only, params.encrypted_only)
            {
                return None;
            }

            let dir = item.backup;
            source_snapshots.insert(dir.time);
            // If resync_corrupt is set, check if the corresponding local snapshot failed to
            // verification
            if params.resync_corrupt {
                let local_dir = params
                    .target
                    .store
                    .backup_dir(target_ns.clone(), dir.clone());
                if let Ok(local_dir) = local_dir {
                    if local_dir.full_path().exists() {
                        match local_dir.verify_state() {
                            Ok(Some(state)) => {
                                if state == VerifyState::Failed {
                                    return Some((dir, true));
                                }
                            }
                            Ok(None) => {
                                // The verify_state item was not found in the manifest, this means the
                                // snapshot is new.
                            }
                            Err(_) => {
                                // There was an error loading the manifest, probably better if we
                                // resync.
                                return Some((dir, true));
                            }
                        }
                    }
                }
            }
            Some((dir, false))
        })
        .collect();

    let total_amount = list.len();
    let cutoff = params
        .transfer_last
        .map(|count| total_amount.saturating_sub(count))
        .unwrap_or_default();

    let list: Vec<(BackupDir, bool)> = list
        .into_iter()
        .enumerate()
        .filter_map(|(pos, (dir, needs_resync))| {
            if params.resync_corrupt && needs_resync {
                return Some((dir, needs_resync));
            }
            // Note: the snapshot represented by `last_sync_time` might be missing its backup log
            // or post-backup verification state if those were not yet available during the last
            // sync run, always resync it
            if last_sync_time > dir.time {
                already_synced_skip_info.update(dir.time);
                return None;
            }
            if pos < cutoff && last_sync_time != dir.time {
                transfer_last_skip_info.update(dir.time);
                return None;
            }
            Some((dir, needs_resync))
        })
        .collect();

    if already_synced_skip_info.count > 0 {
        info!("{already_synced_skip_info}");
        already_synced_skip_info.reset();
    }
    if transfer_last_skip_info.count > 0 {
        info!("{transfer_last_skip_info}");
        transfer_last_skip_info.reset();
    }

    let backup_group = params
        .target
        .store
        .backup_group(target_ns.clone(), group.clone());
    if let Some(info) = backup_group.last_backup(true).unwrap_or(None) {
        let mut reusable_chunks = encountered_chunks.lock().unwrap();
        if let Err(err) = proxmox_lang::try_block!({
            let _snapshot_guard = info
                .backup_dir
                .lock_shared()
                .with_context(|| format!("failed locking last backup of group {info:?}"))?;

            let (manifest, _) = info.backup_dir.load_manifest().with_context(|| {
                format!("failed loading manifest of last backup of group {info:?}")
            })?;

            match manifest.verify_state()? {
                Some(verify_state) if verify_state.state == VerifyState::Failed => (),
                _ => {
                    for file in manifest.files() {
                        let index: Box<dyn IndexFile> = match ArchiveType::from_path(&file.filename)
                        {
                            Ok(ArchiveType::FixedIndex) => {
                                let mut path = info.backup_dir.full_path();
                                path.push(&file.filename);
                                let index =
                                    params.target.store.open_fixed_reader(&path).with_context(
                                        || format!("failed loading fixed index {path:?}"),
                                    )?;
                                Box::new(index)
                            }
                            Ok(ArchiveType::DynamicIndex) => {
                                let mut path = info.backup_dir.full_path();
                                path.push(&file.filename);
                                let index = params
                                    .target
                                    .store
                                    .open_dynamic_reader(&path)
                                    .with_context(|| {
                                        format!("failed loading dynamic index {path:?}")
                                    })?;
                                Box::new(index)
                            }
                            _ => continue,
                        };

                        for pos in 0..index.index_count() {
                            let chunk_info = index.chunk_info(pos).unwrap();
                            reusable_chunks.mark_reusable(&chunk_info.digest);
                        }
                    }
                }
            }
            Ok::<(), Error>(())
        }) {
            warn!("Failed to collect reusable chunk from last backup: {err:#?}");
        }
    }

    progress.group_snapshots = list.len() as u64;

    let mut sync_stats = SyncStats::default();

    for (pos, (from_snapshot, corrupt)) in list.into_iter().enumerate() {
        let to_snapshot = params
            .target
            .store
            .backup_dir(target_ns.clone(), from_snapshot.clone())?;

        let reader = params
            .source
            .reader(source_namespace, &from_snapshot)
            .await?;
        let result = pull_snapshot_from(
            params,
            reader,
            &to_snapshot,
            encountered_chunks.clone(),
            corrupt,
        )
        .await;

        progress.done_snapshots = pos as u64 + 1;
        info!("percentage done: {progress}");

        let stats = result?; // stop on error
        sync_stats.add(stats);
    }

    if params.remove_vanished {
        let group = params
            .target
            .store
            .backup_group(target_ns.clone(), group.clone());
        let local_list = group.list_backups()?;
        for info in local_list {
            let snapshot = info.backup_dir;
            if source_snapshots.contains(&snapshot.backup_time()) {
                continue;
            }
            if snapshot.is_protected() {
                info!(
                    "don't delete vanished snapshot {} (protected)",
                    snapshot.dir()
                );
                continue;
            }
            info!("delete vanished snapshot {}", snapshot.dir());
            params
                .target
                .store
                .remove_backup_dir(&target_ns, snapshot.as_ref(), false)?;
            sync_stats.add(SyncStats::from(RemovedVanishedStats {
                snapshots: 1,
                groups: 0,
                namespaces: 0,
            }));
        }
    }

    Ok(sync_stats)
}

fn check_and_create_ns(params: &PullParameters, ns: &BackupNamespace) -> Result<bool, Error> {
    let mut created = false;
    let store_ns_str = print_store_and_ns(params.target.store.name(), ns);

    if !ns.is_root() && !params.target.store.namespace_path(ns).exists() {
        check_ns_modification_privs(params.target.store.name(), ns, &params.owner)
            .map_err(|err| format_err!("Creating {ns} not allowed - {err}"))?;

        let name = match ns.components().last() {
            Some(name) => name.to_owned(),
            None => {
                bail!("Failed to determine last component of namespace.");
            }
        };

        if let Err(err) = params.target.store.create_namespace(&ns.parent(), name) {
            bail!("sync into {store_ns_str} failed - namespace creation failed: {err}");
        }
        created = true;
    }

    check_ns_privs(
        params.target.store.name(),
        ns,
        &params.owner,
        PRIV_DATASTORE_BACKUP,
    )
    .map_err(|err| format_err!("sync into {store_ns_str} not allowed - {err}"))?;

    Ok(created)
}

fn check_and_remove_ns(params: &PullParameters, local_ns: &BackupNamespace) -> Result<bool, Error> {
    check_ns_modification_privs(params.target.store.name(), local_ns, &params.owner)
        .map_err(|err| format_err!("Removing {local_ns} not allowed - {err}"))?;

    let (removed_all, _delete_stats) = params
        .target
        .store
        .remove_namespace_recursive(local_ns, true)?;

    Ok(removed_all)
}

fn check_and_remove_vanished_ns(
    params: &PullParameters,
    synced_ns: HashSet<BackupNamespace>,
) -> Result<(bool, RemovedVanishedStats), Error> {
    let mut errors = false;
    let mut removed_stats = RemovedVanishedStats::default();
    let user_info = CachedUserInfo::new()?;

    // clamp like remote does so that we don't list more than we can ever have synced.
    let max_depth = params
        .max_depth
        .unwrap_or_else(|| MAX_NAMESPACE_DEPTH - params.source.get_ns().depth());

    let mut local_ns_list: Vec<BackupNamespace> = params
        .target
        .store
        .recursive_iter_backup_ns_ok(params.target.ns.clone(), Some(max_depth))?
        .filter(|ns| {
            let user_privs =
                user_info.lookup_privs(&params.owner, &ns.acl_path(params.target.store.name()));
            user_privs & (PRIV_DATASTORE_BACKUP | PRIV_DATASTORE_AUDIT) != 0
        })
        .collect();

    // children first!
    local_ns_list.sort_unstable_by_key(|b| std::cmp::Reverse(b.name_len()));

    for local_ns in local_ns_list {
        if local_ns == params.target.ns {
            continue;
        }

        if synced_ns.contains(&local_ns) {
            continue;
        }

        if local_ns.is_root() {
            continue;
        }
        match check_and_remove_ns(params, &local_ns) {
            Ok(true) => {
                info!("Removed namespace {local_ns}");
                removed_stats.namespaces += 1;
            }
            Ok(false) => info!("Did not remove namespace {local_ns} - protected snapshots remain"),
            Err(err) => {
                info!("Failed to remove namespace {local_ns} - {err}");
                errors = true;
            }
        }
    }

    Ok((errors, removed_stats))
}

/// Pulls a store according to `params`.
///
/// Pulling a store consists of the following steps:
/// - Query list of namespaces on the remote
/// - Iterate list
///   -- create sub-NS if needed (and allowed)
///   -- attempt to pull each NS in turn
/// - (remove_vanished && max_depth > 0) remove sub-NS which are not or no longer available on the remote
///
/// Backwards compat: if the remote namespace is `/` and recursion is disabled, no namespace is
/// passed to the remote at all to allow pulling from remotes which have no notion of namespaces.
///
/// Permission checks:
/// - access to local datastore, namespace anchor and remote entry need to be checked at call site
/// - remote namespaces are filtered by remote
/// - creation and removal of sub-NS checked here
/// - access to sub-NS checked here
pub(crate) async fn pull_store(mut params: PullParameters) -> Result<SyncStats, Error> {
    // explicit create shared lock to prevent GC on newly created chunks
    let _shared_store_lock = params.target.store.try_shared_chunk_store_lock()?;
    let mut errors = false;

    let old_max_depth = params.max_depth;
    let mut namespaces = if params.source.get_ns().is_root() && old_max_depth == Some(0) {
        vec![params.source.get_ns()] // backwards compat - don't query remote namespaces!
    } else {
        params
            .source
            .list_namespaces(&mut params.max_depth, Box::new(|_| true))
            .await?
    };

    check_namespace_depth_limit(&params.source.get_ns(), &params.target.ns, &namespaces)?;

    errors |= old_max_depth != params.max_depth; // fail job if we switched to backwards-compat mode
    namespaces.sort_unstable_by_key(|a| a.name_len());

    let (mut groups, mut snapshots) = (0, 0);
    let mut synced_ns = HashSet::with_capacity(namespaces.len());
    let mut sync_stats = SyncStats::default();

    // start with 65536 chunks (up to 256 GiB)
    let encountered_chunks = Arc::new(Mutex::new(EncounteredChunks::with_capacity(1024 * 64)));

    for namespace in namespaces {
        let source_store_ns_str = print_store_and_ns(params.source.get_store(), &namespace);

        let target_ns = namespace.map_prefix(&params.source.get_ns(), &params.target.ns)?;
        let target_store_ns_str = print_store_and_ns(params.target.store.name(), &target_ns);

        info!("----");
        info!("Syncing {source_store_ns_str} into {target_store_ns_str}");

        synced_ns.insert(target_ns.clone());

        match check_and_create_ns(&params, &target_ns) {
            Ok(true) => info!("Created namespace {target_ns}"),
            Ok(false) => {}
            Err(err) => {
                info!("Cannot sync {source_store_ns_str} into {target_store_ns_str} - {err}");
                errors = true;
                continue;
            }
        }

        match pull_ns(&namespace, &mut params, encountered_chunks.clone()).await {
            Ok((ns_progress, ns_sync_stats, ns_errors)) => {
                errors |= ns_errors;

                sync_stats.add(ns_sync_stats);

                if params.max_depth != Some(0) {
                    groups += ns_progress.done_groups;
                    snapshots += ns_progress.done_snapshots;

                    let ns = if namespace.is_root() {
                        "root namespace".into()
                    } else {
                        format!("namespace {namespace}")
                    };
                    info!(
                        "Finished syncing {ns}, current progress: {groups} groups, {snapshots} snapshots"
                    );
                }
            }
            Err(err) => {
                errors = true;
                info!(
                    "Encountered errors while syncing namespace {} - {err}",
                    &namespace,
                );
            }
        };
    }

    if params.remove_vanished {
        let (has_errors, stats) = check_and_remove_vanished_ns(&params, synced_ns)?;
        errors |= has_errors;
        sync_stats.add(SyncStats::from(stats));
    }

    if errors {
        bail!("sync failed with some errors.");
    }

    Ok(sync_stats)
}

/// Pulls a namespace according to `params`.
///
/// Pulling a namespace consists of the following steps:
/// - Query list of groups on the remote (in `source_ns`)
/// - Filter list according to configured group filters
/// - Iterate list and attempt to pull each group in turn
/// - (remove_vanished) remove groups with matching owner and matching the configured group filters which are
///   not or no longer available on the remote
///
/// Permission checks:
/// - remote namespaces are filtered by remote
/// - owner check for vanished groups done here
async fn pull_ns(
    namespace: &BackupNamespace,
    params: &mut PullParameters,
    encountered_chunks: Arc<Mutex<EncounteredChunks>>,
) -> Result<(StoreProgress, SyncStats, bool), Error> {
    let list: Vec<BackupGroup> = params.source.list_groups(namespace, &params.owner).await?;

    let unfiltered_count = list.len();
    let mut list: Vec<BackupGroup> = list
        .into_iter()
        .filter(|group| group.apply_filters(&params.group_filter))
        .collect();

    list.sort_unstable();

    info!(
        "found {} groups to sync (out of {unfiltered_count} total)",
        list.len()
    );

    let mut errors = false;

    let mut new_groups = HashSet::new();
    for group in list.iter() {
        new_groups.insert(group.clone());
    }

    let mut progress = StoreProgress::new(list.len() as u64);
    let mut sync_stats = SyncStats::default();

    let target_ns = namespace.map_prefix(&params.source.get_ns(), &params.target.ns)?;

    for (done, group) in list.into_iter().enumerate() {
        progress.done_groups = done as u64;
        progress.done_snapshots = 0;
        progress.group_snapshots = 0;

        let (owner, _lock_guard) =
            match params
                .target
                .store
                .create_locked_backup_group(&target_ns, &group, &params.owner)
            {
                Ok(result) => result,
                Err(err) => {
                    info!("sync group {} failed - group lock failed: {err}", &group);
                    errors = true;
                    // do not stop here, instead continue
                    info!("create_locked_backup_group failed");
                    continue;
                }
            };

        // permission check
        if params.owner != owner {
            // only the owner is allowed to create additional snapshots
            info!(
                "sync group {} failed - owner check failed ({} != {owner})",
                &group, params.owner
            );
            errors = true; // do not stop here, instead continue
        } else {
            encountered_chunks.lock().unwrap().clear();
            match pull_group(
                params,
                namespace,
                &group,
                &mut progress,
                encountered_chunks.clone(),
            )
            .await
            {
                Ok(stats) => sync_stats.add(stats),
                Err(err) => {
                    info!("sync group {} failed - {err:#}", &group);
                    errors = true; // do not stop here, instead continue
                }
            }
        }
    }

    if params.remove_vanished {
        let result: Result<(), Error> = proxmox_lang::try_block!({
            for local_group in params.target.store.iter_backup_groups(target_ns.clone())? {
                let local_group = local_group?;
                let local_group = local_group.group();
                if new_groups.contains(local_group) {
                    continue;
                }
                let owner = params.target.store.get_owner(&target_ns, local_group)?;
                if check_backup_owner(&owner, &params.owner).is_err() {
                    continue;
                }
                if !local_group.apply_filters(&params.group_filter) {
                    continue;
                }
                info!("delete vanished group '{local_group}'");
                let delete_stats_result = params
                    .target
                    .store
                    .remove_backup_group(&target_ns, local_group);

                match delete_stats_result {
                    Ok(stats) => {
                        if !stats.all_removed() {
                            info!("kept some protected snapshots of group '{local_group}'");
                            sync_stats.add(SyncStats::from(RemovedVanishedStats {
                                snapshots: stats.removed_snapshots(),
                                groups: 0,
                                namespaces: 0,
                            }));
                        } else {
                            sync_stats.add(SyncStats::from(RemovedVanishedStats {
                                snapshots: stats.removed_snapshots(),
                                groups: 1,
                                namespaces: 0,
                            }));
                        }
                    }
                    Err(err) => {
                        info!("{err}");
                        errors = true;
                    }
                }
            }
            Ok(())
        });
        if let Err(err) = result {
            info!("error during cleanup: {err}");
            errors = true;
        };
    }

    Ok((progress, sync_stats, errors))
}

/// Store the state of encountered chunks, tracking if they can be reused for the
/// index file currently being pulled and if the chunk has already been touched
/// during this sync.
struct EncounteredChunks {
    // key: digest, value: (reusable, touched)
    chunk_set: HashMap<[u8; 32], (bool, bool)>,
}

impl EncounteredChunks {
    /// Create a new instance with preallocated capacity
    fn with_capacity(capacity: usize) -> Self {
        Self {
            chunk_set: HashMap::with_capacity(capacity),
        }
    }

    /// Check if the current state allows to reuse this chunk and if so,
    /// if the chunk has already been touched.
    fn check_reusable(&self, digest: &[u8; 32]) -> Option<bool> {
        if let Some((reusable, touched)) = self.chunk_set.get(digest) {
            if !reusable {
                None
            } else {
                Some(*touched)
            }
        } else {
            None
        }
    }

    /// Mark chunk as reusable, inserting it as un-touched if not present
    fn mark_reusable(&mut self, digest: &[u8; 32]) {
        match self.chunk_set.entry(*digest) {
            Entry::Occupied(mut occupied) => {
                let (reusable, _touched) = occupied.get_mut();
                *reusable = true;
            }
            Entry::Vacant(vacant) => {
                vacant.insert((true, false));
            }
        }
    }

    /// Mark chunk as touched during this sync, inserting it as not reusable
    /// but touched if not present.
    fn mark_touched(&mut self, digest: &[u8; 32]) {
        match self.chunk_set.entry(*digest) {
            Entry::Occupied(mut occupied) => {
                let (_reusable, touched) = occupied.get_mut();
                *touched = true;
            }
            Entry::Vacant(vacant) => {
                vacant.insert((false, true));
            }
        }
    }

    /// Clear all entries
    fn clear(&mut self) {
        self.chunk_set.clear();
    }
}
