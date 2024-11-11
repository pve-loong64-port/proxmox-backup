//! Sync datastore contents from source to target, either in push or pull direction

use std::collections::HashMap;
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, format_err, Context, Error};
use futures::{future::FutureExt, select};
use http::StatusCode;
use serde_json::json;
use tracing::{info, warn};

use proxmox_human_byte::HumanByte;
use proxmox_rest_server::WorkerTask;
use proxmox_router::HttpError;

use pbs_api_types::{
    Authid, BackupDir, BackupGroup, BackupNamespace, CryptMode, GroupListItem, SnapshotListItem,
    SyncDirection, SyncJobConfig, MAX_NAMESPACE_DEPTH, PRIV_DATASTORE_BACKUP, PRIV_DATASTORE_READ,
};
use pbs_client::{BackupReader, BackupRepository, HttpClient, RemoteChunkReader};
use pbs_datastore::data_blob::DataBlob;
use pbs_datastore::manifest::CLIENT_LOG_BLOB_NAME;
use pbs_datastore::read_chunk::AsyncReadChunk;
use pbs_datastore::{DataStore, ListNamespacesRecursive, LocalChunkReader};

use crate::backup::ListAccessibleBackupGroups;
use crate::server::jobstate::Job;
use crate::server::pull::{pull_store, PullParameters};
use crate::server::push::{push_store, PushParameters};

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

pub type NamespaceFilter = Box<dyn Fn(&BackupNamespace) -> bool + Send>;

#[async_trait::async_trait]
/// `SyncSource` is a trait that provides an interface for synchronizing data/information from a
/// source.
/// The trait includes methods for listing namespaces, groups, and backup directories,
/// as well as retrieving a reader for reading data from the source.
pub(crate) trait SyncSource: Send + Sync {
    /// Lists namespaces from the source.
    async fn list_namespaces(
        &self,
        max_depth: &mut Option<usize>,
        filter_callback: NamespaceFilter,
    ) -> Result<Vec<BackupNamespace>, Error>;

    /// Lists groups within a specific namespace from the source.
    async fn list_groups(
        &self,
        namespace: &BackupNamespace,
        owner: &Authid,
    ) -> Result<Vec<BackupGroup>, Error>;

    /// Lists backup directories for a specific group within a specific namespace from the source.
    async fn list_backup_dirs(
        &self,
        namespace: &BackupNamespace,
        group: &BackupGroup,
    ) -> Result<Vec<BackupDir>, Error>;
    fn get_ns(&self) -> BackupNamespace;
    fn get_store(&self) -> &str;

    /// Returns a reader for reading data from a specific backup directory.
    async fn reader(
        &self,
        ns: &BackupNamespace,
        dir: &BackupDir,
    ) -> Result<Arc<dyn SyncSourceReader>, Error>;
}

pub(crate) struct RemoteSource {
    pub(crate) repo: BackupRepository,
    pub(crate) ns: BackupNamespace,
    pub(crate) client: HttpClient,
}

pub(crate) struct LocalSource {
    pub(crate) store: Arc<DataStore>,
    pub(crate) ns: BackupNamespace,
}

#[async_trait::async_trait]
impl SyncSource for RemoteSource {
    async fn list_namespaces(
        &self,
        max_depth: &mut Option<usize>,
        filter_callback: NamespaceFilter,
    ) -> Result<Vec<BackupNamespace>, Error> {
        if self.ns.is_root() && max_depth.map_or(false, |depth| depth == 0) {
            return Ok(vec![self.ns.clone()]);
        }

        let path = format!("api2/json/admin/datastore/{}/namespace", self.repo.store());
        let mut data = json!({});
        if let Some(max_depth) = max_depth {
            data["max-depth"] = json!(max_depth);
        }

        if !self.ns.is_root() {
            data["parent"] = json!(self.ns);
        }
        self.client.login().await?;

        let mut result = match self.client.get(&path, Some(data)).await {
            Ok(res) => res,
            Err(err) => match err.downcast_ref::<HttpError>() {
                Some(HttpError { code, message }) => match code {
                    &StatusCode::NOT_FOUND => {
                        if self.ns.is_root() && max_depth.is_none() {
                            warn!("Could not query remote for namespaces (404) -> temporarily switching to backwards-compat mode");
                            warn!("Either make backwards-compat mode explicit (max-depth == 0) or upgrade remote system.");
                            max_depth.replace(0);
                        } else {
                            bail!("Remote namespace set/recursive sync requested, but remote does not support namespaces.")
                        }

                        return Ok(vec![self.ns.clone()]);
                    }
                    _ => {
                        bail!("Querying namespaces failed - HTTP error {code} - {message}");
                    }
                },
                None => {
                    bail!("Querying namespaces failed - {err}");
                }
            },
        };

        let list: Vec<BackupNamespace> =
            serde_json::from_value::<Vec<pbs_api_types::NamespaceListItem>>(result["data"].take())?
                .into_iter()
                .map(|list_item| list_item.ns)
                .collect();

        let list = list.into_iter().filter(filter_callback).collect();

        Ok(list)
    }

    async fn list_groups(
        &self,
        namespace: &BackupNamespace,
        _owner: &Authid,
    ) -> Result<Vec<BackupGroup>, Error> {
        let path = format!("api2/json/admin/datastore/{}/groups", self.repo.store());

        let args = if !namespace.is_root() {
            Some(json!({ "ns": namespace.clone() }))
        } else {
            None
        };

        self.client.login().await?;
        let mut result =
            self.client.get(&path, args).await.map_err(|err| {
                format_err!("Failed to retrieve backup groups from remote - {}", err)
            })?;

        Ok(
            serde_json::from_value::<Vec<GroupListItem>>(result["data"].take())
                .map_err(Error::from)?
                .into_iter()
                .map(|item| item.backup)
                .collect::<Vec<BackupGroup>>(),
        )
    }

    async fn list_backup_dirs(
        &self,
        namespace: &BackupNamespace,
        group: &BackupGroup,
    ) -> Result<Vec<BackupDir>, Error> {
        let path = format!("api2/json/admin/datastore/{}/snapshots", self.repo.store());

        let mut args = json!({
            "backup-type": group.ty,
            "backup-id": group.id,
        });

        if !namespace.is_root() {
            args["ns"] = serde_json::to_value(namespace)?;
        }

        self.client.login().await?;

        let mut result = self.client.get(&path, Some(args)).await?;
        let snapshot_list: Vec<SnapshotListItem> = serde_json::from_value(result["data"].take())?;
        Ok(snapshot_list
            .into_iter()
            .filter_map(|item: SnapshotListItem| {
                let snapshot = item.backup;
                // in-progress backups can't be synced
                if item.size.is_none() {
                    info!("skipping snapshot {snapshot} - in-progress backup");
                    return None;
                }

                Some(snapshot)
            })
            .collect::<Vec<BackupDir>>())
    }

    fn get_ns(&self) -> BackupNamespace {
        self.ns.clone()
    }

    fn get_store(&self) -> &str {
        self.repo.store()
    }

    async fn reader(
        &self,
        ns: &BackupNamespace,
        dir: &BackupDir,
    ) -> Result<Arc<dyn SyncSourceReader>, Error> {
        let backup_reader =
            BackupReader::start(&self.client, None, self.repo.store(), ns, dir, true).await?;
        Ok(Arc::new(RemoteSourceReader {
            backup_reader,
            dir: dir.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl SyncSource for LocalSource {
    async fn list_namespaces(
        &self,
        max_depth: &mut Option<usize>,
        filter_callback: NamespaceFilter,
    ) -> Result<Vec<BackupNamespace>, Error> {
        let list: Result<Vec<BackupNamespace>, Error> = ListNamespacesRecursive::new_max_depth(
            self.store.clone(),
            self.ns.clone(),
            max_depth.unwrap_or(MAX_NAMESPACE_DEPTH),
        )?
        .collect();

        let list = list?.into_iter().filter(filter_callback).collect();

        Ok(list)
    }

    async fn list_groups(
        &self,
        namespace: &BackupNamespace,
        owner: &Authid,
    ) -> Result<Vec<BackupGroup>, Error> {
        Ok(ListAccessibleBackupGroups::new_with_privs(
            &self.store,
            namespace.clone(),
            0,
            Some(PRIV_DATASTORE_READ),
            Some(PRIV_DATASTORE_BACKUP),
            Some(owner),
        )?
        .filter_map(Result::ok)
        .map(|backup_group| backup_group.group().clone())
        .collect::<Vec<pbs_api_types::BackupGroup>>())
    }

    async fn list_backup_dirs(
        &self,
        namespace: &BackupNamespace,
        group: &BackupGroup,
    ) -> Result<Vec<BackupDir>, Error> {
        Ok(self
            .store
            .backup_group(namespace.clone(), group.clone())
            .iter_snapshots()?
            .filter_map(Result::ok)
            .map(|snapshot| snapshot.dir().to_owned())
            .collect::<Vec<BackupDir>>())
    }

    fn get_ns(&self) -> BackupNamespace {
        self.ns.clone()
    }

    fn get_store(&self) -> &str {
        self.store.name()
    }

    async fn reader(
        &self,
        ns: &BackupNamespace,
        dir: &BackupDir,
    ) -> Result<Arc<dyn SyncSourceReader>, Error> {
        let dir = self.store.backup_dir(ns.clone(), dir.clone())?;
        let dir_lock = proxmox_sys::fs::lock_dir_noblock_shared(
            &dir.full_path(),
            "snapshot",
            "locked by another operation",
        )?;
        Ok(Arc::new(LocalSourceReader {
            _dir_lock: Arc::new(Mutex::new(dir_lock)),
            path: dir.full_path(),
            datastore: dir.datastore().clone(),
        }))
    }
}

#[derive(PartialEq, Eq)]
pub(crate) enum SkipReason {
    AlreadySynced,
    TransferLast,
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SkipReason::AlreadySynced =>
                    "older than the newest snapshot present on sync target",
                SkipReason::TransferLast => "due to transfer-last",
            }
        )
    }
}

pub(crate) struct SkipInfo {
    oldest: i64,
    newest: i64,
    pub(crate) count: u64,
    skip_reason: SkipReason,
}

impl SkipInfo {
    pub(crate) fn new(skip_reason: SkipReason) -> Self {
        SkipInfo {
            oldest: i64::MAX,
            newest: i64::MIN,
            count: 0,
            skip_reason,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.count = 0;
        self.oldest = i64::MAX;
        self.newest = i64::MIN;
    }

    pub(crate) fn update(&mut self, backup_time: i64) {
        self.count += 1;

        if backup_time < self.oldest {
            self.oldest = backup_time;
        }

        if backup_time > self.newest {
            self.newest = backup_time;
        }
    }

    fn affected(&self) -> Result<String, Error> {
        match self.count {
            0 => Ok(String::new()),
            1 => Ok(proxmox_time::epoch_to_rfc3339_utc(self.oldest)?),
            _ => Ok(format!(
                "{} .. {}",
                proxmox_time::epoch_to_rfc3339_utc(self.oldest)?,
                proxmox_time::epoch_to_rfc3339_utc(self.newest)?,
            )),
        }
    }
}

impl std::fmt::Display for SkipInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "skipped: {} snapshot(s) ({}) - {}",
            self.count,
            self.affected().map_err(|_| std::fmt::Error)?,
            self.skip_reason,
        )
    }
}

/// Check if a sync from source to target of given namespaces exceeds the global namespace depth limit
pub(crate) fn check_namespace_depth_limit(
    source_namespace: &BackupNamespace,
    target_namespace: &BackupNamespace,
    namespaces: &[BackupNamespace],
) -> Result<(), Error> {
    let target_ns_depth = target_namespace.depth();
    let sync_ns_depth = namespaces
        .iter()
        .map(BackupNamespace::depth)
        .max()
        .map_or(0, |v| v - source_namespace.depth());

    if sync_ns_depth + target_ns_depth > MAX_NAMESPACE_DEPTH {
        bail!(
            "Syncing would exceed max allowed namespace depth. ({sync_ns_depth}+{target_ns_depth} > {MAX_NAMESPACE_DEPTH})",
        );
    }
    Ok(())
}

/// Run a sync job in given direction
pub fn do_sync_job(
    mut job: Job,
    sync_job: SyncJobConfig,
    auth_id: &Authid,
    schedule: Option<String>,
    sync_direction: SyncDirection,
    to_stdout: bool,
) -> Result<String, Error> {
    let job_id = format!(
        "{}:{}:{}:{}:{}",
        sync_job.remote.as_deref().unwrap_or("-"),
        sync_job.remote_store,
        sync_job.store,
        sync_job.ns.clone().unwrap_or_default(),
        job.jobname(),
    );
    let worker_type = job.jobtype().to_string();

    if sync_job.remote.is_none() && sync_job.store == sync_job.remote_store {
        bail!("can't sync to same datastore");
    }

    let upid_str = WorkerTask::spawn(
        &worker_type,
        Some(job_id.clone()),
        auth_id.to_string(),
        to_stdout,
        move |worker| async move {
            job.start(&worker.upid().to_string())?;

            let worker2 = worker.clone();
            let sync_job2 = sync_job.clone();

            let worker_future = async move {
                info!("Starting datastore sync job '{job_id}'");
                if let Some(event_str) = schedule {
                    info!("task triggered by schedule '{event_str}'");
                }
                let sync_stats = match sync_direction {
                    SyncDirection::Pull => {
                        info!(
                            "sync datastore '{}' from '{}{}'",
                            sync_job.store,
                            sync_job
                                .remote
                                .as_deref()
                                .map_or(String::new(), |remote| format!("{remote}/")),
                            sync_job.remote_store,
                        );
                        let pull_params = PullParameters::try_from(&sync_job)?;
                        pull_store(pull_params).await?
                    }
                    SyncDirection::Push => {
                        info!(
                            "sync datastore '{}' to '{}{}'",
                            sync_job.store,
                            sync_job
                                .remote
                                .as_deref()
                                .map_or(String::new(), |remote| format!("{remote}/")),
                            sync_job.remote_store,
                        );
                        let push_params = PushParameters::new(
                            &sync_job.store,
                            sync_job.ns.clone().unwrap_or_default(),
                            sync_job
                                .remote
                                .as_deref()
                                .context("missing required remote")?,
                            &sync_job.remote_store,
                            sync_job.remote_ns.clone().unwrap_or_default(),
                            sync_job
                                .owner
                                .as_ref()
                                .unwrap_or_else(|| Authid::root_auth_id())
                                .clone(),
                            sync_job.remove_vanished,
                            sync_job.max_depth,
                            sync_job.group_filter.clone(),
                            sync_job.limit.clone(),
                            sync_job.transfer_last,
                        )
                        .await?;
                        push_store(push_params).await?
                    }
                };

                if sync_stats.bytes != 0 {
                    let amount = HumanByte::from(sync_stats.bytes);
                    let rate = HumanByte::new_binary(
                        sync_stats.bytes as f64 / sync_stats.elapsed.as_secs_f64(),
                    );
                    info!(
                        "Summary: sync job {sync_direction}ed {amount} in {} chunks (average rate: {rate}/s)",
                        sync_stats.chunk_count,
                    );
                } else {
                    info!("Summary: sync job found no new data to {sync_direction}");
                }

                if let Some(removed) = sync_stats.removed {
                    info!(
                        "Summary: removed vanished: snapshots: {}, groups: {}, namespaces: {}",
                        removed.snapshots, removed.groups, removed.namespaces,
                    );
                }

                info!("sync job '{job_id}' end");

                Ok(())
            };

            let mut abort_future = worker2
                .abort_future()
                .map(|_| Err(format_err!("sync aborted")));

            let result = select! {
                worker = worker_future.fuse() => worker,
                abort = abort_future => abort,
            };

            let status = worker2.create_state(&result);

            match job.finish(status) {
                Ok(_) => {}
                Err(err) => eprintln!("could not finish job state: {err}"),
            }

            if let Err(err) = crate::server::send_sync_status(&sync_job2, &result) {
                eprintln!("send sync notification failed: {err}");
            }

            result
        },
    )?;

    Ok(upid_str)
}
