//! Sync datastore by pulling contents from remote server

use std::collections::HashSet;
use std::io::Seek;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use anyhow::{bail, format_err, Error};
use proxmox_human_byte::HumanByte;
use tracing::info;

use pbs_api_types::{
    print_store_and_ns, Authid, BackupDir, BackupGroup, BackupNamespace, GroupFilter, Operation,
    RateLimitConfig, Remote, MAX_NAMESPACE_DEPTH, PRIV_DATASTORE_AUDIT, PRIV_DATASTORE_BACKUP,
};
use pbs_client::BackupRepository;
use pbs_config::CachedUserInfo;
use pbs_datastore::data_blob::DataBlob;
use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_datastore::fixed_index::FixedIndexReader;
use pbs_datastore::index::IndexFile;
use pbs_datastore::manifest::{
    ArchiveType, BackupManifest, FileInfo, CLIENT_LOG_BLOB_NAME, MANIFEST_BLOB_NAME,
};
use pbs_datastore::read_chunk::AsyncReadChunk;
use pbs_datastore::{check_backup_owner, DataStore, StoreProgress};
use pbs_tools::sha::sha256;

use super::sync::{
    check_namespace_depth_limit, LocalSource, RemoteSource, RemovedVanishedStats, SkipInfo,
    SkipReason, SyncSource, SyncSourceReader, SyncStats,
};
use crate::backup::{check_ns_modification_privs, check_ns_privs};
use crate::tools::parallel_handler::ParallelHandler;

pub(crate) struct PullTarget {
    store: Arc<DataStore>,
    ns: BackupNamespace,
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
    ) -> Result<Self, Error> {
        if let Some(max_depth) = max_depth {
            ns.check_max_depth(max_depth)?;
            remote_ns.check_max_depth(max_depth)?;
        };
        let remove_vanished = remove_vanished.unwrap_or(false);

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
        let target = PullTarget {
            store: DataStore::lookup_datastore(store, Some(Operation::Write))?,
            ns,
        };

        let group_filter = group_filter.unwrap_or_default();

        Ok(Self {
            source,
            target,
            owner,
            remove_vanished,
            max_depth,
            group_filter,
            transfer_last,
        })
    }
}

async fn pull_index_chunks<I: IndexFile>(
    chunk_reader: Arc<dyn AsyncReadChunk>,
    target: Arc<DataStore>,
    index: I,
    downloaded_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
) -> Result<SyncStats, Error> {
    use futures::stream::{self, StreamExt, TryStreamExt};

    let start_time = SystemTime::now();

    let stream = stream::iter(
        (0..index.index_count())
            .map(|pos| index.chunk_info(pos).unwrap())
            .filter(|info| {
                let mut guard = downloaded_chunks.lock().unwrap();
                let done = guard.contains(&info.digest);
                if !done {
                    // Note: We mark a chunk as downloaded before its actually downloaded
                    // to avoid duplicate downloads.
                    guard.insert(info.digest);
                }
                !done
            }),
    );

    let target2 = target.clone();
    let verify_pool = ParallelHandler::new(
        "sync chunk writer",
        4,
        move |(chunk, digest, size): (DataBlob, [u8; 32], u64)| {
            // println!("verify and write {}", hex::encode(&digest));
            chunk.verify_unencrypted(size as usize, &digest)?;
            target2.insert_chunk(&chunk, &digest)?;
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

            Ok::<_, Error>(async move {
                let chunk_exists = proxmox_async::runtime::block_in_place(|| {
                    target.cond_touch_chunk(&info.digest, false)
                })?;
                if chunk_exists {
                    //info!("chunk {} exists {}", pos, hex::encode(digest));
                    return Ok::<_, Error>(());
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

    verify_pool.complete()?;

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
/// -- Load file into tmp file
/// -- Verify tmp file checksum
/// - if archive is an index, pull referenced chunks
/// - Rename tmp file into real path
async fn pull_single_archive<'a>(
    reader: Arc<dyn SyncSourceReader + 'a>,
    snapshot: &'a pbs_datastore::BackupDir,
    archive_info: &'a FileInfo,
    downloaded_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
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
                    reader.chunk_reader(archive_info.crypt_mode),
                    snapshot.datastore().clone(),
                    index,
                    downloaded_chunks,
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
                    reader.chunk_reader(archive_info.crypt_mode),
                    snapshot.datastore().clone(),
                    index,
                    downloaded_chunks,
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
    Ok(sync_stats)
}

/// Actual implementation of pulling a snapshot.
///
/// Pulling a snapshot consists of the following steps:
/// - (Re)download the manifest
/// -- if it matches, only download log and treat snapshot as already synced
/// - Iterate over referenced files
/// -- if file already exists, verify contents
/// -- if not, pull it from the remote
/// - Download log if not already existing
async fn pull_snapshot<'a>(
    reader: Arc<dyn SyncSourceReader + 'a>,
    snapshot: &'a pbs_datastore::BackupDir,
    downloaded_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
) -> Result<SyncStats, Error> {
    let mut sync_stats = SyncStats::default();
    let mut manifest_name = snapshot.full_path();
    manifest_name.push(MANIFEST_BLOB_NAME);

    let mut client_log_name = snapshot.full_path();
    client_log_name.push(CLIENT_LOG_BLOB_NAME);

    let mut tmp_manifest_name = manifest_name.clone();
    tmp_manifest_name.set_extension("tmp");
    let tmp_manifest_blob;
    if let Some(data) = reader
        .load_file_into(MANIFEST_BLOB_NAME, &tmp_manifest_name)
        .await?
    {
        tmp_manifest_blob = data;
    } else {
        return Ok(sync_stats);
    }

    if manifest_name.exists() {
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

    let manifest = BackupManifest::try_from(tmp_manifest_blob)?;

    for item in manifest.files() {
        let mut path = snapshot.full_path();
        path.push(&item.filename);

        if path.exists() {
            match ArchiveType::from_path(&item.filename)? {
                ArchiveType::DynamicIndex => {
                    let index = DynamicIndexReader::open(&path)?;
                    let (csum, size) = index.compute_csum();
                    match manifest.verify_file(&item.filename, &csum, size) {
                        Ok(_) => continue,
                        Err(err) => {
                            info!("detected changed file {path:?} - {err}");
                        }
                    }
                }
                ArchiveType::FixedIndex => {
                    let index = FixedIndexReader::open(&path)?;
                    let (csum, size) = index.compute_csum();
                    match manifest.verify_file(&item.filename, &csum, size) {
                        Ok(_) => continue,
                        Err(err) => {
                            info!("detected changed file {path:?} - {err}");
                        }
                    }
                }
                ArchiveType::Blob => {
                    let mut tmpfile = std::fs::File::open(&path)?;
                    let (csum, size) = sha256(&mut tmpfile)?;
                    match manifest.verify_file(&item.filename, &csum, size) {
                        Ok(_) => continue,
                        Err(err) => {
                            info!("detected changed file {path:?} - {err}");
                        }
                    }
                }
            }
        }

        let stats =
            pull_single_archive(reader.clone(), snapshot, item, downloaded_chunks.clone()).await?;
        sync_stats.add(stats);
    }

    if let Err(err) = std::fs::rename(&tmp_manifest_name, &manifest_name) {
        bail!("Atomic rename file {:?} failed - {}", manifest_name, err);
    }

    if !client_log_name.exists() {
        reader.try_download_client_log(&client_log_name).await?;
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
    reader: Arc<dyn SyncSourceReader + 'a>,
    snapshot: &'a pbs_datastore::BackupDir,
    downloaded_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
) -> Result<SyncStats, Error> {
    let (_path, is_new, _snap_lock) = snapshot
        .datastore()
        .create_locked_backup_dir(snapshot.backup_ns(), snapshot.as_ref())?;

    let sync_stats = if is_new {
        info!("sync snapshot {}", snapshot.dir());

        match pull_snapshot(reader, snapshot, downloaded_chunks).await {
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
            Ok(sync_stats) => {
                info!("sync snapshot {} done", snapshot.dir());
                sync_stats
            }
        }
    } else {
        info!("re-sync snapshot {}", snapshot.dir());
        pull_snapshot(reader, snapshot, downloaded_chunks).await?
    };

    Ok(sync_stats)
}

/// Pulls a group according to `params`.
///
/// Pulling a group consists of the following steps:
/// - Query the list of snapshots available for this group in the source namespace on the remote
/// - Sort by snapshot time
/// - Get last snapshot timestamp on local datastore
/// - Iterate over list of snapshots
/// -- pull snapshot, unless it's not finished yet or older than last local snapshot
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
) -> Result<SyncStats, Error> {
    let mut already_synced_skip_info = SkipInfo::new(SkipReason::AlreadySynced);
    let mut transfer_last_skip_info = SkipInfo::new(SkipReason::TransferLast);

    let mut raw_list: Vec<BackupDir> = params
        .source
        .list_backup_dirs(source_namespace, group)
        .await?;
    raw_list.sort_unstable_by(|a, b| a.time.cmp(&b.time));

    let total_amount = raw_list.len();

    let cutoff = params
        .transfer_last
        .map(|count| total_amount.saturating_sub(count))
        .unwrap_or_default();

    let target_ns = source_namespace.map_prefix(&params.source.get_ns(), &params.target.ns)?;

    let mut source_snapshots = HashSet::new();
    let last_sync_time = params
        .target
        .store
        .last_successful_backup(&target_ns, group)?
        .unwrap_or(i64::MIN);

    let list: Vec<BackupDir> = raw_list
        .into_iter()
        .enumerate()
        .filter(|&(pos, ref dir)| {
            source_snapshots.insert(dir.time);
            if last_sync_time > dir.time {
                already_synced_skip_info.update(dir.time);
                return false;
            } else if already_synced_skip_info.count > 0 {
                info!("{already_synced_skip_info}");
                already_synced_skip_info.reset();
            }

            if pos < cutoff && last_sync_time != dir.time {
                transfer_last_skip_info.update(dir.time);
                return false;
            } else if transfer_last_skip_info.count > 0 {
                info!("{transfer_last_skip_info}");
                transfer_last_skip_info.reset();
            }
            true
        })
        .map(|(_, dir)| dir)
        .collect();

    // start with 65536 chunks (up to 256 GiB)
    let downloaded_chunks = Arc::new(Mutex::new(HashSet::with_capacity(1024 * 64)));

    progress.group_snapshots = list.len() as u64;

    let mut sync_stats = SyncStats::default();

    for (pos, from_snapshot) in list.into_iter().enumerate() {
        let to_snapshot = params
            .target
            .store
            .backup_dir(target_ns.clone(), from_snapshot.clone())?;

        let reader = params
            .source
            .reader(source_namespace, &from_snapshot)
            .await?;
        let result = pull_snapshot_from(reader, &to_snapshot, downloaded_chunks.clone()).await;

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

    params
        .target
        .store
        .remove_namespace_recursive(local_ns, true)
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
/// -- create sub-NS if needed (and allowed)
/// -- attempt to pull each NS in turn
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
        params.source.list_namespaces(&mut params.max_depth).await?
    };

    check_namespace_depth_limit(&params.source.get_ns(), &params.target.ns, &namespaces)?;

    errors |= old_max_depth != params.max_depth; // fail job if we switched to backwards-compat mode
    namespaces.sort_unstable_by_key(|a| a.name_len());

    let (mut groups, mut snapshots) = (0, 0);
    let mut synced_ns = HashSet::with_capacity(namespaces.len());
    let mut sync_stats = SyncStats::default();

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

        match pull_ns(&namespace, &mut params).await {
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
pub(crate) async fn pull_ns(
    namespace: &BackupNamespace,
    params: &mut PullParameters,
) -> Result<(StoreProgress, SyncStats, bool), Error> {
    let mut list: Vec<BackupGroup> = params.source.list_groups(namespace, &params.owner).await?;

    list.sort_unstable_by(|a, b| {
        let type_order = a.ty.cmp(&b.ty);
        if type_order == std::cmp::Ordering::Equal {
            a.id.cmp(&b.id)
        } else {
            type_order
        }
    });

    let unfiltered_count = list.len();
    let list: Vec<BackupGroup> = list
        .into_iter()
        .filter(|group| group.apply_filters(&params.group_filter))
        .collect();
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
            match pull_group(params, namespace, &group, &mut progress).await {
                Ok(stats) => sync_stats.add(stats),
                Err(err) => {
                    info!("sync group {} failed - {err}", &group);
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
