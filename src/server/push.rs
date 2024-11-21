//! Sync datastore by pushing contents to remote server

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use anyhow::{bail, format_err, Error};
use futures::stream::{self, StreamExt, TryStreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

use pbs_api_types::{
    print_store_and_ns, ApiVersion, ApiVersionInfo, Authid, BackupDir, BackupGroup,
    BackupGroupDeleteStats, BackupNamespace, GroupFilter, GroupListItem, NamespaceListItem,
    Operation, RateLimitConfig, Remote, SnapshotListItem, PRIV_DATASTORE_BACKUP,
    PRIV_DATASTORE_READ, PRIV_REMOTE_DATASTORE_BACKUP, PRIV_REMOTE_DATASTORE_MODIFY,
    PRIV_REMOTE_DATASTORE_PRUNE,
};
use pbs_client::{BackupRepository, BackupWriter, HttpClient, MergedChunkInfo, UploadOptions};
use pbs_config::CachedUserInfo;
use pbs_datastore::data_blob::ChunkInfo;
use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_datastore::fixed_index::FixedIndexReader;
use pbs_datastore::index::IndexFile;
use pbs_datastore::manifest::{ArchiveType, CLIENT_LOG_BLOB_NAME, MANIFEST_BLOB_NAME};
use pbs_datastore::read_chunk::AsyncReadChunk;
use pbs_datastore::{DataStore, StoreProgress};

use super::sync::{
    check_namespace_depth_limit, LocalSource, RemovedVanishedStats, SkipInfo, SkipReason,
    SyncSource, SyncStats,
};
use crate::api2::config::remote;

/// Target for backups to be pushed to
pub(crate) struct PushTarget {
    // Remote as found in remote.cfg
    remote: Remote,
    // Target repository on remote
    repo: BackupRepository,
    // Target namespace on remote
    ns: BackupNamespace,
    // Http client to connect to remote
    client: HttpClient,
    // Remote target api supports prune delete stats
    supports_prune_delete_stats: bool,
}

impl PushTarget {
    fn remote_user(&self) -> Authid {
        self.remote.config.auth_id.clone()
    }

    fn datastore_api_path(&self, endpoint: &str) -> String {
        format!(
            "api2/json/admin/datastore/{store}/{endpoint}",
            store = self.repo.store()
        )
    }
}

/// Parameters for a push operation
pub(crate) struct PushParameters {
    /// Source of backups to be pushed to remote
    source: Arc<LocalSource>,
    /// Target for backups to be pushed to
    target: PushTarget,
    /// User used for permission checks on the source side, including potentially filtering visible
    /// namespaces and backup groups.
    local_user: Authid,
    /// Whether to remove groups and namespaces which exist locally, but not on the remote end
    remove_vanished: bool,
    /// How many levels of sub-namespaces to push (0 == no recursion, None == maximum recursion)
    max_depth: Option<usize>,
    /// Filters for reducing the push scope
    group_filter: Vec<GroupFilter>,
    /// How many snapshots should be transferred at most (taking the newest N snapshots)
    transfer_last: Option<usize>,
}

impl PushParameters {
    /// Creates a new instance of `PushParameters`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        store: &str,
        ns: BackupNamespace,
        remote_id: &str,
        remote_store: &str,
        remote_ns: BackupNamespace,
        local_user: Authid,
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
        let store = DataStore::lookup_datastore(store, Some(Operation::Read))?;

        if !store.namespace_exists(&ns) {
            bail!(
                "Source namespace '{ns}' doesn't exist in datastore '{store}'!",
                store = store.name()
            );
        }

        let source = Arc::new(LocalSource { store, ns });

        let (remote_config, _digest) = pbs_config::remote::config()?;
        let remote: Remote = remote_config.lookup("remote", remote_id)?;

        let repo = BackupRepository::new(
            Some(remote.config.auth_id.clone()),
            Some(remote.config.host.clone()),
            remote.config.port,
            remote_store.to_string(),
        );

        let client = remote::remote_client_config(&remote, Some(limit))?;

        let mut result = client.get("api2/json/version", None).await?;
        let data = result["data"].take();
        let version_info: ApiVersionInfo = serde_json::from_value(data)?;
        let api_version = ApiVersion::try_from(version_info)?;

        // push assumes namespace support on the remote side, fail early if missing
        if api_version.major < 2 || (api_version.major == 2 && api_version.minor < 2) {
            bail!("unsupported remote api version, minimum v2.2 required");
        }

        let supports_prune_delete_stats = api_version.major > 3
            || (api_version.major == 3 && api_version.minor >= 2 && api_version.release >= 11);

        let target = PushTarget {
            remote,
            repo,
            ns: remote_ns,
            client,
            supports_prune_delete_stats,
        };
        let group_filter = group_filter.unwrap_or_default();

        Ok(Self {
            source,
            target,
            local_user,
            remove_vanished,
            max_depth,
            group_filter,
            transfer_last,
        })
    }

    // Map the given namespace from source to target by adapting the prefix
    fn map_to_target(&self, namespace: &BackupNamespace) -> Result<BackupNamespace, Error> {
        namespace.map_prefix(&self.source.ns, &self.target.ns)
    }
}

// Check if the job user given in the push parameters has the provided privs on the remote
// datastore namespace
fn check_ns_remote_datastore_privs(
    params: &PushParameters,
    target_namespace: &BackupNamespace,
    privs: u64,
) -> Result<(), Error> {
    let user_info = CachedUserInfo::new()?;
    let acl_path =
        target_namespace.remote_acl_path(&params.target.remote.name, params.target.repo.store());

    user_info.check_privs(&params.local_user, &acl_path, privs, false)?;

    Ok(())
}

// Fetch the list of namespaces found on target
async fn fetch_target_namespaces(params: &PushParameters) -> Result<Vec<BackupNamespace>, Error> {
    let api_path = params.target.datastore_api_path("namespace");
    let mut result = params.target.client.get(&api_path, None).await?;
    let namespaces: Vec<NamespaceListItem> = serde_json::from_value(result["data"].take())?;
    let mut namespaces: Vec<BackupNamespace> = namespaces
        .into_iter()
        .map(|namespace| namespace.ns)
        .collect();
    namespaces.sort_unstable_by_key(|a| a.name_len());

    Ok(namespaces)
}

// Remove the provided namespace from the target
async fn remove_target_namespace(
    params: &PushParameters,
    target_namespace: &BackupNamespace,
) -> Result<BackupGroupDeleteStats, Error> {
    if target_namespace.is_root() {
        bail!("cannot remove root namespace from target");
    }

    check_ns_remote_datastore_privs(params, target_namespace, PRIV_REMOTE_DATASTORE_MODIFY)
        .map_err(|err| format_err!("Pruning remote datastore namespaces not allowed - {err}"))?;

    let api_path = params.target.datastore_api_path("namespace");

    let mut args = serde_json::json!({
        "ns": target_namespace.name(),
        "delete-groups": true,
    });

    if params.target.supports_prune_delete_stats {
        args["error-on-protected"] = serde_json::to_value(false)?;
    }

    let mut result = params.target.client.delete(&api_path, Some(args)).await?;

    if params.target.supports_prune_delete_stats {
        let data = result["data"].take();
        serde_json::from_value(data).map_err(|err| {
            format_err!("removing target namespace {target_namespace} failed - {err}")
        })
    } else {
        Ok(BackupGroupDeleteStats::default())
    }
}

// Fetch the list of groups found on target in given namespace
// Returns sorted list of owned groups and a hashset containing not owned backup groups on target.
async fn fetch_target_groups(
    params: &PushParameters,
    target_namespace: &BackupNamespace,
) -> Result<(Vec<BackupGroup>, HashSet<BackupGroup>), Error> {
    let api_path = params.target.datastore_api_path("groups");
    let args = Some(serde_json::json!({ "ns": target_namespace.name() }));

    let mut result = params.target.client.get(&api_path, args).await?;
    let groups: Vec<GroupListItem> = serde_json::from_value(result["data"].take())?;

    let (mut owned, not_owned) = groups.into_iter().fold(
        (Vec::new(), HashSet::new()),
        |(mut owned, mut not_owned), group| {
            if Some(params.target.remote_user()) == group.owner {
                owned.push(group.backup);
            } else {
                not_owned.insert(group.backup);
            }
            (owned, not_owned)
        },
    );

    owned.sort_unstable();

    Ok((owned, not_owned))
}

// Remove the provided backup group in given namespace from the target
async fn remove_target_group(
    params: &PushParameters,
    target_namespace: &BackupNamespace,
    backup_group: &BackupGroup,
) -> Result<BackupGroupDeleteStats, Error> {
    check_ns_remote_datastore_privs(params, target_namespace, PRIV_REMOTE_DATASTORE_PRUNE)
        .map_err(|err| format_err!("Pruning remote datastore contents not allowed - {err}"))?;

    let api_path = params.target.datastore_api_path("groups");

    let mut args = serde_json::json!(backup_group);
    args["ns"] = serde_json::to_value(target_namespace.name())?;

    if params.target.supports_prune_delete_stats {
        args["error-on-protected"] = serde_json::to_value(false)?;
    }

    let mut result = params.target.client.delete(&api_path, Some(args)).await?;

    if params.target.supports_prune_delete_stats {
        let data = result["data"].take();
        serde_json::from_value(data)
            .map_err(|err| format_err!("removing target group {backup_group} failed - {err}"))
    } else {
        Ok(BackupGroupDeleteStats::default())
    }
}

// Check if the namespace is already present on the target, create it otherwise
async fn check_or_create_target_namespace(
    params: &PushParameters,
    existing_target_namespaces: &mut Vec<BackupNamespace>,
    target_namespace: &BackupNamespace,
) -> Result<(), Error> {
    if !target_namespace.is_root() && !existing_target_namespaces.contains(target_namespace) {
        // Namespace not present on target, create namespace.
        // Sub-namespaces have to be created by creating parent components first.

        check_ns_remote_datastore_privs(params, target_namespace, PRIV_REMOTE_DATASTORE_MODIFY)
            .map_err(|err| format_err!("Creating namespace not allowed - {err}"))?;

        let mut parent = BackupNamespace::root();
        for component in target_namespace.components() {
            let current = BackupNamespace::from_parent_ns(&parent, component.to_string())?;
            // Skip over pre-existing parent namespaces on target
            if existing_target_namespaces.contains(&current) {
                parent = current;
                continue;
            }
            let api_path = params.target.datastore_api_path("namespace");
            let mut args = serde_json::json!({ "name": component.to_string() });
            if !parent.is_root() {
                args["parent"] = serde_json::to_value(parent.clone())?;
            }
            match params.target.client.post(&api_path, Some(args)).await {
                Ok(_) => info!("Created new namespace on target: {current}"),
                Err(err) => {
                    bail!("Remote creation of namespace {current} failed, remote returned: {err}")
                }
            }
            existing_target_namespaces.push(current.clone());
            parent = current;
        }
    }

    Ok(())
}

/// Push contents of source datastore matched by given push parameters to target.
pub(crate) async fn push_store(mut params: PushParameters) -> Result<SyncStats, Error> {
    let mut errors = false;

    let user_info = CachedUserInfo::new()?;
    let store = params.source.get_store().to_owned();
    let auth_id = params.local_user.clone();
    // Generate list of source namespaces to push to target, limited by max-depth and filtered
    // by local user access privs.
    let ns_access_filter = Box::new(move |namespace: &BackupNamespace| {
        let acl_path = namespace.acl_path(&store);
        let privs = PRIV_DATASTORE_READ | PRIV_DATASTORE_BACKUP;
        user_info
            .check_privs(&auth_id, &acl_path, privs, true)
            .is_ok()
    });
    let mut source_namespaces = params
        .source
        .list_namespaces(&mut params.max_depth, ns_access_filter)
        .await?;

    check_namespace_depth_limit(
        &params.source.get_ns(),
        &params.target.ns,
        &source_namespaces,
    )?;

    source_namespaces.sort_unstable_by_key(|a| a.name_len());

    // Fetch all accessible namespaces already present on the target
    let mut existing_target_namespaces = fetch_target_namespaces(&params).await?;
    // Remember synced namespaces, removing non-synced ones when remove vanished flag is set
    let mut synced_namespaces = HashSet::with_capacity(source_namespaces.len());

    let (mut groups, mut snapshots) = (0, 0);
    let mut stats = SyncStats::default();
    for source_namespace in &source_namespaces {
        let source_store_and_ns = print_store_and_ns(params.source.store.name(), source_namespace);
        let target_namespace = params.map_to_target(source_namespace)?;
        let target_store_and_ns = print_store_and_ns(params.target.repo.store(), &target_namespace);

        info!("----");
        info!("Syncing {source_store_and_ns} into {target_store_and_ns}");

        synced_namespaces.insert(target_namespace.clone());

        if let Err(err) = check_or_create_target_namespace(
            &params,
            &mut existing_target_namespaces,
            &target_namespace,
        )
        .await
        {
            warn!("Encountered error: {err}");
            warn!("Failed to sync {source_store_and_ns} into {target_store_and_ns}!");
            errors = true;
            continue;
        }

        match push_namespace(source_namespace, &params).await {
            Ok((sync_progress, sync_stats, sync_errors)) => {
                errors |= sync_errors;
                stats.add(sync_stats);

                if params.max_depth != Some(0) {
                    groups += sync_progress.done_groups;
                    snapshots += sync_progress.done_snapshots;

                    let ns = if source_namespace.is_root() {
                        "root namespace".into()
                    } else {
                        format!("namespace {source_namespace}")
                    };
                    info!(
                        "Finished syncing {ns}, current progress: {groups} groups, {snapshots} snapshots"
                    );
                }
            }
            Err(err) => {
                errors = true;
                info!("Encountered errors while syncing namespace {source_namespace} - {err}");
            }
        }
    }

    if params.remove_vanished {
        // Attention: Filter out all namespaces which are not sub-namespaces of the sync target
        // namespace, or not included in the sync because of the depth limit.
        // Without this pre-filtering, all namespaces unrelated to the sync would be removed!
        let max_depth = params
            .max_depth
            .unwrap_or_else(|| pbs_api_types::MAX_NAMESPACE_DEPTH);
        let mut target_sub_namespaces: Vec<BackupNamespace> = existing_target_namespaces
            .into_iter()
            .filter(|target_namespace| {
                params
                    .target
                    .ns
                    .contains(&target_namespace)
                    .map(|sub_depth| sub_depth <= max_depth)
                    .unwrap_or(false)
            })
            .collect();

        // Sort by namespace length and revert for sub-namespaces to be removed before parents
        target_sub_namespaces.sort_unstable_by_key(|a| a.name_len());
        target_sub_namespaces.reverse();

        for target_namespace in target_sub_namespaces {
            if synced_namespaces.contains(&target_namespace) {
                continue;
            }
            match remove_target_namespace(&params, &target_namespace).await {
                Ok(delete_stats) => {
                    stats.add(SyncStats::from(RemovedVanishedStats {
                        snapshots: delete_stats.removed_snapshots(),
                        groups: delete_stats.removed_groups(),
                        namespaces: 1,
                    }));
                    if delete_stats.protected_snapshots() > 0 {
                        warn!(
                            "kept {protected_count} protected snapshots of namespace '{target_namespace}'",
                            protected_count = delete_stats.protected_snapshots(),
                        );
                        continue;
                    }
                }
                Err(err) => {
                    warn!("failed to remove vanished namespace {target_namespace} - {err}");
                    continue;
                }
            }
            info!("removed vanished namespace {target_namespace}");
        }

        if !params.target.supports_prune_delete_stats {
            info!("Older api version on remote detected, delete stats might be incomplete");
        }
    }

    if errors {
        bail!("sync failed with some errors.");
    }

    Ok(stats)
}

/// Push namespace including all backup groups to target
///
/// Iterate over all backup groups in the namespace and push them to the target.
pub(crate) async fn push_namespace(
    namespace: &BackupNamespace,
    params: &PushParameters,
) -> Result<(StoreProgress, SyncStats, bool), Error> {
    let target_namespace = params.map_to_target(namespace)?;
    // Check if user is allowed to perform backups on remote datastore
    check_ns_remote_datastore_privs(params, &target_namespace, PRIV_REMOTE_DATASTORE_BACKUP)
        .map_err(|err| format_err!("Pushing to remote not allowed - {err}"))?;

    let mut list: Vec<BackupGroup> = params
        .source
        .list_groups(namespace, &params.local_user)
        .await?;

    list.sort_unstable();

    let total = list.len();
    let list: Vec<BackupGroup> = list
        .into_iter()
        .filter(|group| group.apply_filters(&params.group_filter))
        .collect();

    info!(
        "found {filtered} groups to sync (out of {total} total)",
        filtered = list.len()
    );

    let mut errors = false;
    // Remember synced groups, remove others when the remove vanished flag is set
    let mut synced_groups = HashSet::new();
    let mut progress = StoreProgress::new(list.len() as u64);
    let mut stats = SyncStats::default();

    let (owned_target_groups, not_owned_target_groups) =
        fetch_target_groups(params, &target_namespace).await?;

    for (done, group) in list.into_iter().enumerate() {
        progress.done_groups = done as u64;
        progress.done_snapshots = 0;
        progress.group_snapshots = 0;

        if not_owned_target_groups.contains(&group) {
            warn!(
                "group '{group}' not owned by remote user '{}' on target, skip",
                params.target.remote_user(),
            );
            continue;
        }
        synced_groups.insert(group.clone());

        match push_group(params, namespace, &group, &mut progress).await {
            Ok(sync_stats) => stats.add(sync_stats),
            Err(err) => {
                warn!("sync group '{group}' failed  - {err}");
                errors = true;
            }
        }
    }

    if params.remove_vanished {
        // only ever allow to prune owned groups on target
        for target_group in owned_target_groups {
            if synced_groups.contains(&target_group) {
                continue;
            }
            if !target_group.apply_filters(&params.group_filter) {
                continue;
            }

            info!("delete vanished group '{target_group}'");

            match remove_target_group(params, &target_namespace, &target_group).await {
                Ok(delete_stats) => {
                    if delete_stats.protected_snapshots() > 0 {
                        warn!(
                            "kept {protected_count} protected snapshots of group '{target_group}'",
                            protected_count = delete_stats.protected_snapshots(),
                        );
                    }
                    stats.add(SyncStats::from(RemovedVanishedStats {
                        snapshots: delete_stats.removed_snapshots(),
                        groups: delete_stats.removed_groups(),
                        namespaces: 0,
                    }));
                }
                Err(err) => {
                    warn!("failed to delete vanished group - {err}");
                    errors = true;
                    continue;
                }
            }
        }
    }

    Ok((progress, stats, errors))
}

async fn fetch_target_snapshots(
    params: &PushParameters,
    target_namespace: &BackupNamespace,
    group: &BackupGroup,
) -> Result<Vec<SnapshotListItem>, Error> {
    let api_path = params.target.datastore_api_path("snapshots");
    let mut args = serde_json::to_value(group)?;
    if !target_namespace.is_root() {
        args["ns"] = serde_json::to_value(target_namespace)?;
    }
    let mut result = params.target.client.get(&api_path, Some(args)).await?;
    let snapshots: Vec<SnapshotListItem> = serde_json::from_value(result["data"].take())?;

    Ok(snapshots)
}

async fn forget_target_snapshot(
    params: &PushParameters,
    target_namespace: &BackupNamespace,
    snapshot: &BackupDir,
) -> Result<(), Error> {
    check_ns_remote_datastore_privs(params, target_namespace, PRIV_REMOTE_DATASTORE_PRUNE)
        .map_err(|err| format_err!("Pruning remote datastore contents not allowed - {err}"))?;

    let api_path = params.target.datastore_api_path("snapshots");
    let mut args = serde_json::to_value(snapshot)?;
    if !target_namespace.is_root() {
        args["ns"] = serde_json::to_value(target_namespace)?;
    }
    params.target.client.delete(&api_path, Some(args)).await?;

    Ok(())
}

/// Push group including all snaphshots to target
///
/// Iterate over all snapshots in the group and push them to the target.
/// The group sync operation consists of the following steps:
/// - Query snapshots of given group from the source
/// - Sort snapshots by time
/// - Apply transfer last cutoff and filters to list
/// - Iterate the snapshot list and push each snapshot individually
/// - (Optional): Remove vanished groups if `remove_vanished` flag is set
pub(crate) async fn push_group(
    params: &PushParameters,
    namespace: &BackupNamespace,
    group: &BackupGroup,
    progress: &mut StoreProgress,
) -> Result<SyncStats, Error> {
    let mut already_synced_skip_info = SkipInfo::new(SkipReason::AlreadySynced);
    let mut transfer_last_skip_info = SkipInfo::new(SkipReason::TransferLast);

    let mut snapshots: Vec<BackupDir> = params.source.list_backup_dirs(namespace, group).await?;
    snapshots.sort_unstable_by(|a, b| a.time.cmp(&b.time));

    let total_snapshots = snapshots.len();
    let cutoff = params
        .transfer_last
        .map(|count| total_snapshots.saturating_sub(count))
        .unwrap_or_default();

    let target_namespace = params.map_to_target(namespace)?;
    let mut target_snapshots = fetch_target_snapshots(params, &target_namespace, group).await?;
    target_snapshots.sort_unstable_by_key(|a| a.backup.time);

    let last_snapshot_time = target_snapshots
        .last()
        .map(|snapshot| snapshot.backup.time)
        .unwrap_or(i64::MIN);

    let mut source_snapshots = HashSet::new();
    let snapshots: Vec<BackupDir> = snapshots
        .into_iter()
        .enumerate()
        .filter(|&(pos, ref snapshot)| {
            source_snapshots.insert(snapshot.time);
            if last_snapshot_time >= snapshot.time {
                already_synced_skip_info.update(snapshot.time);
                return false;
            }
            if pos < cutoff {
                transfer_last_skip_info.update(snapshot.time);
                return false;
            }
            true
        })
        .map(|(_, dir)| dir)
        .collect();

    if already_synced_skip_info.count > 0 {
        info!("{already_synced_skip_info}");
        already_synced_skip_info.reset();
    }
    if transfer_last_skip_info.count > 0 {
        info!("{transfer_last_skip_info}");
        transfer_last_skip_info.reset();
    }

    progress.group_snapshots = snapshots.len() as u64;

    let mut stats = SyncStats::default();
    let mut fetch_previous_manifest = !target_snapshots.is_empty();
    for (pos, source_snapshot) in snapshots.into_iter().enumerate() {
        let result =
            push_snapshot(params, namespace, &source_snapshot, fetch_previous_manifest).await;
        fetch_previous_manifest = true;

        progress.done_snapshots = pos as u64 + 1;
        info!("percentage done: {progress}");

        // stop on error
        let sync_stats = result?;
        stats.add(sync_stats);
    }

    if params.remove_vanished {
        for snapshot in target_snapshots {
            if source_snapshots.contains(&snapshot.backup.time) {
                continue;
            }
            if snapshot.protected {
                info!(
                    "don't delete vanished snapshot {name} (protected)",
                    name = snapshot.backup
                );
                continue;
            }
            if let Err(err) =
                forget_target_snapshot(params, &target_namespace, &snapshot.backup).await
            {
                info!(
                    "could not delete vanished snapshot {name} - {err}",
                    name = snapshot.backup
                );
            }
            info!("delete vanished snapshot {name}", name = snapshot.backup);
            stats.add(SyncStats::from(RemovedVanishedStats {
                snapshots: 1,
                groups: 0,
                namespaces: 0,
            }));
        }
    }

    Ok(stats)
}

/// Push snapshot to target
///
/// Creates a new snapshot on the target and pushes the content of the source snapshot to the
/// target by creating a new manifest file and connecting to the remote as backup writer client.
/// Chunks are written by recreating the index by uploading the chunk stream as read from the
/// source. Data blobs are uploaded as such.
pub(crate) async fn push_snapshot(
    params: &PushParameters,
    namespace: &BackupNamespace,
    snapshot: &BackupDir,
    fetch_previous_manifest: bool,
) -> Result<SyncStats, Error> {
    let mut stats = SyncStats::default();
    let target_ns = params.map_to_target(namespace)?;
    let backup_dir = params
        .source
        .store
        .backup_dir(namespace.clone(), snapshot.clone())?;

    // Reader locks the snapshot
    let reader = params.source.reader(namespace, snapshot).await?;

    // Does not lock the manifest, but the reader already assures a locked snapshot
    let source_manifest = match backup_dir.load_manifest() {
        Ok((manifest, _raw_size)) => manifest,
        Err(err) => {
            // No manifest in snapshot or failed to read, warn and skip
            log::warn!("failed to load manifest - {err}");
            return Ok(stats);
        }
    };

    // Writer instance locks the snapshot on the remote side
    let backup_writer = BackupWriter::start(
        &params.target.client,
        None,
        params.target.repo.store(),
        &target_ns,
        snapshot,
        false,
        false,
    )
    .await?;

    let mut previous_manifest = None;
    // Use manifest of previous snapshots in group on target for chunk upload deduplication
    if fetch_previous_manifest {
        match backup_writer.download_previous_manifest().await {
            Ok(manifest) => previous_manifest = Some(Arc::new(manifest)),
            Err(err) => log::info!("Could not download previous manifest - {err}"),
        }
    };

    // Dummy upload options: the actual compression and/or encryption already happened while
    // the chunks were generated during creation of the backup snapshot, therefore pre-existing
    // chunks (already compressed and/or encrypted) can be pushed to the target.
    // Further, these steps are skipped in the backup writer upload stream.
    //
    // Therefore, these values do not need to fit the values given in the manifest.
    // The original manifest is uploaded in the end anyways.
    //
    // Compression is set to true so that the uploaded manifest will be compressed.
    // Encrypt is set to assure that above files are not encrypted.
    let upload_options = UploadOptions {
        compress: true,
        encrypt: false,
        previous_manifest,
        ..UploadOptions::default()
    };

    // Avoid double upload penalty by remembering already seen chunks
    let known_chunks = Arc::new(Mutex::new(HashSet::with_capacity(64 * 1024)));

    for entry in source_manifest.files() {
        let mut path = backup_dir.full_path();
        path.push(&entry.filename);
        if path.try_exists()? {
            match ArchiveType::from_path(&entry.filename)? {
                ArchiveType::Blob => {
                    let file = std::fs::File::open(path.clone())?;
                    let backup_stats = backup_writer.upload_blob(file, &entry.filename).await?;
                    stats.add(SyncStats {
                        chunk_count: backup_stats.chunk_count as usize,
                        bytes: backup_stats.size as usize,
                        elapsed: backup_stats.duration,
                        removed: None,
                    });
                }
                ArchiveType::DynamicIndex => {
                    if let Some(manifest) = upload_options.previous_manifest.as_ref() {
                        // Add known chunks, ignore errors since archive might not be present
                        let _res = backup_writer
                            .download_previous_dynamic_index(
                                &entry.filename,
                                manifest,
                                known_chunks.clone(),
                            )
                            .await;
                    }
                    let index = DynamicIndexReader::open(&path)?;
                    let chunk_reader = reader.chunk_reader(entry.chunk_crypt_mode());
                    let sync_stats = push_index(
                        &entry.filename,
                        index,
                        chunk_reader,
                        &backup_writer,
                        None,
                        known_chunks.clone(),
                    )
                    .await?;
                    stats.add(sync_stats);
                }
                ArchiveType::FixedIndex => {
                    if let Some(manifest) = upload_options.previous_manifest.as_ref() {
                        // Add known chunks, ignore errors since archive might not be present
                        let _res = backup_writer
                            .download_previous_fixed_index(
                                &entry.filename,
                                manifest,
                                known_chunks.clone(),
                            )
                            .await;
                    }
                    let index = FixedIndexReader::open(&path)?;
                    let chunk_reader = reader.chunk_reader(entry.chunk_crypt_mode());
                    let size = index.index_bytes();
                    let sync_stats = push_index(
                        &entry.filename,
                        index,
                        chunk_reader,
                        &backup_writer,
                        Some(size),
                        known_chunks.clone(),
                    )
                    .await?;
                    stats.add(sync_stats);
                }
            }
        } else {
            bail!("{path:?} does not exist, aborting upload.");
        }
    }

    // Fetch client log from source and push to target
    // this has to be handled individually since the log is never part of the manifest
    let mut client_log_path = backup_dir.full_path();
    client_log_path.push(CLIENT_LOG_BLOB_NAME);
    if client_log_path.is_file() {
        backup_writer
            .upload_blob_from_file(
                &client_log_path,
                CLIENT_LOG_BLOB_NAME,
                upload_options.clone(),
            )
            .await?;
    }

    // Rewrite manifest for pushed snapshot, recreating manifest from source on target
    let manifest_json = serde_json::to_value(source_manifest)?;
    let manifest_string = serde_json::to_string_pretty(&manifest_json)?;
    let backup_stats = backup_writer
        .upload_blob_from_data(
            manifest_string.into_bytes(),
            MANIFEST_BLOB_NAME,
            upload_options,
        )
        .await?;
    backup_writer.finish().await?;

    stats.add(SyncStats {
        chunk_count: backup_stats.chunk_count as usize,
        bytes: backup_stats.size as usize,
        elapsed: backup_stats.duration,
        removed: None,
    });

    Ok(stats)
}

// Read fixed or dynamic index and push to target by uploading via the backup writer instance
//
// For fixed indexes, the size must be provided as given by the index reader.
#[allow(clippy::too_many_arguments)]
async fn push_index<'a>(
    filename: &'a str,
    index: impl IndexFile + Send + 'static,
    chunk_reader: Arc<dyn AsyncReadChunk>,
    backup_writer: &BackupWriter,
    size: Option<u64>,
    known_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
) -> Result<SyncStats, Error> {
    let (upload_channel_tx, upload_channel_rx) = mpsc::channel(20);
    let mut chunk_infos =
        stream::iter(0..index.index_count()).map(move |pos| index.chunk_info(pos).unwrap());

    tokio::spawn(async move {
        while let Some(chunk_info) = chunk_infos.next().await {
            // Avoid reading known chunks, as they are not uploaded by the backup writer anyways
            let needs_upload = {
                // Need to limit the scope of the lock, otherwise the async block is not `Send`
                let mut known_chunks = known_chunks.lock().unwrap();
                // Check if present and insert, chunk will be read and uploaded below if not present
                known_chunks.insert(chunk_info.digest)
            };

            let merged_chunk_info = if needs_upload {
                chunk_reader
                    .read_raw_chunk(&chunk_info.digest)
                    .await
                    .map(|chunk| {
                        MergedChunkInfo::New(ChunkInfo {
                            chunk,
                            digest: chunk_info.digest,
                            chunk_len: chunk_info.size(),
                            offset: chunk_info.range.start,
                        })
                    })
            } else {
                Ok(MergedChunkInfo::Known(vec![(
                    // Pass size instead of offset, will be replaced with offset by the backup
                    // writer
                    chunk_info.size(),
                    chunk_info.digest,
                )]))
            };
            let _ = upload_channel_tx.send(merged_chunk_info).await;
        }
    });

    let merged_chunk_info_stream = ReceiverStream::new(upload_channel_rx).map_err(Error::from);

    let upload_options = UploadOptions {
        compress: true,
        encrypt: false,
        fixed_size: size,
        ..UploadOptions::default()
    };

    let upload_stats = backup_writer
        .upload_index_chunk_info(filename, merged_chunk_info_stream, upload_options)
        .await?;

    Ok(SyncStats {
        chunk_count: upload_stats.chunk_count as usize,
        bytes: upload_stats.size as usize,
        elapsed: upload_stats.duration,
        removed: None,
    })
}
