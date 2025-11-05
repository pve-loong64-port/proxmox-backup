//! Datastore Management

use std::ffi::OsStr;
use std::ops::Deref;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, format_err, Context, Error};
use futures::*;
use http_body_util::BodyExt;
use hyper::http::request::Parts;
use hyper::{body::Incoming, header, Response, StatusCode};
use proxmox_http::Body;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

use proxmox_async::blocking::WrappedReaderStream;
use proxmox_async::{io::AsyncChannelWriter, stream::AsyncReaderStream};
use proxmox_compression::zstd::ZstdEncoder;
use proxmox_log::LogContext;
use proxmox_router::{
    http_err, list_subdirs_api_method, ApiHandler, ApiMethod, ApiResponseFuture, Permission,
    Router, RpcEnvironment, RpcEnvironmentType, SubdirMap,
};
use proxmox_rrd_api_types::{RrdMode, RrdTimeframe};
use proxmox_schema::*;
use proxmox_sortable_macro::sortable;
use proxmox_sys::fs::{
    file_read_firstline, file_read_optional_string, replace_file, CreateOptions,
};
use proxmox_time::CalendarEvent;
use proxmox_worker_task::WorkerTaskContext;

use pxar::accessor::aio::Accessor;
use pxar::EntryKind;

use pbs_api_types::{
    print_ns_and_snapshot, print_store_and_ns, ArchiveType, Authid, BackupArchiveName,
    BackupContent, BackupGroupDeleteStats, BackupNamespace, BackupType, Counts, CryptMode,
    DataStoreConfig, DataStoreListItem, DataStoreMountStatus, DataStoreStatus,
    GarbageCollectionJobStatus, GroupListItem, JobScheduleStatus, KeepOptions, MaintenanceMode,
    MaintenanceType, Operation, PruneJobOptions, SnapshotListItem, SyncJobConfig,
    BACKUP_ARCHIVE_NAME_SCHEMA, BACKUP_ID_SCHEMA, BACKUP_NAMESPACE_SCHEMA, BACKUP_TIME_SCHEMA,
    BACKUP_TYPE_SCHEMA, CATALOG_NAME, CLIENT_LOG_BLOB_NAME, DATASTORE_SCHEMA,
    IGNORE_VERIFIED_BACKUPS_SCHEMA, MAX_NAMESPACE_DEPTH, NS_MAX_DEPTH_SCHEMA, PRIV_DATASTORE_AUDIT,
    PRIV_DATASTORE_BACKUP, PRIV_DATASTORE_MODIFY, PRIV_DATASTORE_PRUNE, PRIV_DATASTORE_READ,
    PRIV_DATASTORE_VERIFY, PRIV_SYS_MODIFY, UPID, UPID_SCHEMA, VERIFICATION_OUTDATED_AFTER_SCHEMA,
};
use pbs_client::pxar::{create_tar, create_zip};
use pbs_config::CachedUserInfo;
use pbs_datastore::backup_info::BackupInfo;
use pbs_datastore::cached_chunk_reader::CachedChunkReader;
use pbs_datastore::catalog::{ArchiveEntry, CatalogReader};
use pbs_datastore::data_blob::DataBlob;
use pbs_datastore::data_blob_reader::DataBlobReader;
use pbs_datastore::dynamic_index::{BufferedDynamicReader, DynamicIndexReader, LocalDynamicReadAt};
use pbs_datastore::fixed_index::FixedIndexReader;
use pbs_datastore::index::IndexFile;
use pbs_datastore::manifest::BackupManifest;
use pbs_datastore::prune::compute_prune_info;
use pbs_datastore::{
    check_backup_owner, ensure_datastore_is_mounted, task_tracking, BackupDir, DataStore,
    DatastoreBackend, LocalChunkReader, StoreProgress,
};
use pbs_tools::json::required_string_param;
use proxmox_rest_server::{formatter, worker_is_active, WorkerTask};

use crate::api2::backup::optional_ns_param;
use crate::api2::node::rrd::create_value_from_rrd;
use crate::backup::{check_ns_privs_full, ListAccessibleBackupGroups, VerifyWorker, NS_PRIVS_OK};
use crate::server::jobstate::{compute_schedule_status, Job, JobState};
use crate::tools::{backup_info_to_snapshot_list_item, get_all_snapshot_files, read_backup_index};

// helper to unify common sequence of checks:
// 1. check privs on NS (full or limited access)
// 2. load datastore
// 3. if needed (only limited access), check owner of group
fn check_privs_and_load_store(
    store: &str,
    ns: &BackupNamespace,
    auth_id: &Authid,
    full_access_privs: u64,
    partial_access_privs: u64,
    operation: Option<Operation>,
    backup_group: &pbs_api_types::BackupGroup,
) -> Result<Arc<DataStore>, Error> {
    let limited = check_ns_privs_full(store, ns, auth_id, full_access_privs, partial_access_privs)?;

    let datastore = DataStore::lookup_datastore(store, operation)?;

    if limited {
        let owner = datastore.get_owner(ns, backup_group)?;
        check_backup_owner(&owner, auth_id)?;
    }

    Ok(datastore)
}

#[api(
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
            },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
        },
    },
    returns: pbs_api_types::ADMIN_DATASTORE_LIST_GROUPS_RETURN_TYPE,
    access: {
        permission: &Permission::Anybody,
        description: "Requires DATASTORE_AUDIT for all or DATASTORE_BACKUP for owned groups on \
            /datastore/{store}[/{namespace}]",
    },
)]
/// List backup groups.
pub fn list_groups(
    store: String,
    ns: Option<BackupNamespace>,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<GroupListItem>, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let ns = ns.unwrap_or_default();

    let list_all = !check_ns_privs_full(
        &store,
        &ns,
        &auth_id,
        PRIV_DATASTORE_AUDIT,
        PRIV_DATASTORE_BACKUP,
    )?;

    let datastore = DataStore::lookup_datastore(&store, Some(Operation::Read))?;

    datastore
        .iter_backup_groups(ns.clone())? // FIXME: Namespaces and recursion parameters!
        .try_fold(Vec::new(), |mut group_info, group| {
            let group = group?;

            let item =
                backup_group_to_group_list_item(datastore.clone(), group, &ns, &auth_id, list_all);

            if let Some(item) = item {
                group_info.push(item);
            }

            Ok(group_info)
        })
}

fn backup_group_to_group_list_item(
    datastore: Arc<DataStore>,
    group: pbs_datastore::BackupGroup,
    ns: &BackupNamespace,
    auth_id: &Authid,
    list_all: bool,
) -> Option<GroupListItem> {
    let owner = get_group_owner(datastore.name(), ns, &group)?;

    if !list_all && check_backup_owner(&owner, auth_id).is_err() {
        return None;
    }

    let mut snapshots: Vec<_> = match group.iter_snapshots() {
        Ok(snapshots) => snapshots.collect::<Result<Vec<_>, Error>>().ok()?,
        Err(_) => return None,
    };

    let backup_count: u64 = snapshots.len() as u64;
    let last = if backup_count == 1 {
        // we may have only one unfinished snapshot
        snapshots.pop().and_then(|dir| BackupInfo::new(dir).ok())
    } else {
        // we either have no snapshots, or at least one finished one, since we cannot have
        // multiple unfinished ones
        snapshots.sort_by_key(|b| std::cmp::Reverse(b.backup_time()));
        snapshots
            .iter()
            .filter_map(|backup| BackupInfo::new(backup.clone()).ok())
            .find(|info| info.is_finished())
    };

    let (last_backup, files) = last
        .map(|info| (info.backup_dir.backup_time(), info.files))
        .unwrap_or((0, Vec::new()));

    let notes_path = datastore.group_notes_path(ns, group.as_ref());
    let comment = file_read_firstline(notes_path).ok();

    let item = GroupListItem {
        backup: group.into(),
        last_backup,
        owner: Some(owner),
        backup_count,
        files,
        comment,
    };

    Some(item)
}

#[api(
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            group: {
                type: pbs_api_types::BackupGroup,
                flatten: true,
            },
            "error-on-protected": {
                type: bool,
                optional: true,
                default: true,
                description: "Return error when group cannot be deleted because of protected snapshots",
            }
        },
    },
    returns: {
        type: BackupGroupDeleteStats,
    },
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_MODIFY for any \
            or DATASTORE_PRUNE and being the owner of the group",
    },
)]
/// Delete backup group including all snapshots.
pub async fn delete_group(
    store: String,
    ns: Option<BackupNamespace>,
    error_on_protected: bool,
    group: pbs_api_types::BackupGroup,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<BackupGroupDeleteStats, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;

    tokio::task::spawn_blocking(move || {
        let ns = ns.unwrap_or_default();

        let datastore = check_privs_and_load_store(
            &store,
            &ns,
            &auth_id,
            PRIV_DATASTORE_MODIFY,
            PRIV_DATASTORE_PRUNE,
            Some(Operation::Write),
            &group,
        )?;

        let delete_stats = datastore.remove_backup_group(&ns, &group)?;

        let error_msg = if datastore.old_locking() {
            "could not remove empty groups directories due to old locking mechanism.\n\
            If you are an admin, please reboot PBS or ensure no old backup job is running anymore, \
            then remove the file '/run/proxmox-backup/old-locking', and reload all PBS daemons"
        } else if !delete_stats.all_removed() {
            "group only partially deleted due to protected snapshots"
        } else {
            return Ok(delete_stats);
        };

        if error_on_protected {
            bail!(error_msg);
        } else {
            warn!(error_msg);
        }

        Ok(delete_stats)
    })
    .await?
}

#[api(
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            backup_dir: {
                type: pbs_api_types::BackupDir,
                flatten: true,
            },
        },
    },
    returns: pbs_api_types::ADMIN_DATASTORE_LIST_SNAPSHOT_FILES_RETURN_TYPE,
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_AUDIT or \
            DATASTORE_READ for any or DATASTORE_BACKUP and being the owner of the group",
    },
)]
/// List snapshot files.
pub async fn list_snapshot_files(
    store: String,
    ns: Option<BackupNamespace>,
    backup_dir: pbs_api_types::BackupDir,
    _info: &ApiMethod,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<BackupContent>, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;

    tokio::task::spawn_blocking(move || {
        let ns = ns.unwrap_or_default();

        let datastore = check_privs_and_load_store(
            &store,
            &ns,
            &auth_id,
            PRIV_DATASTORE_AUDIT | PRIV_DATASTORE_READ,
            PRIV_DATASTORE_BACKUP,
            Some(Operation::Read),
            &backup_dir.group,
        )?;

        let snapshot = datastore.backup_dir(ns, backup_dir)?;

        let info = BackupInfo::new(snapshot)?;

        let (_manifest, files) = get_all_snapshot_files(&info)?;

        Ok(files)
    })
    .await?
}

#[api(
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            backup_dir: {
                type: pbs_api_types::BackupDir,
                flatten: true,
            },
        },
    },
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_MODIFY for any \
            or DATASTORE_PRUNE and being the owner of the group",
    },
)]
/// Delete backup snapshot.
pub async fn delete_snapshot(
    store: String,
    ns: Option<BackupNamespace>,
    backup_dir: pbs_api_types::BackupDir,
    _info: &ApiMethod,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Value, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;

    tokio::task::spawn_blocking(move || {
        let ns = ns.unwrap_or_default();

        let datastore = check_privs_and_load_store(
            &store,
            &ns,
            &auth_id,
            PRIV_DATASTORE_MODIFY,
            PRIV_DATASTORE_PRUNE,
            Some(Operation::Write),
            &backup_dir.group,
        )?;

        let snapshot = datastore.backup_dir(ns, backup_dir)?;

        snapshot.destroy(false, &datastore.backend()?)?;

        Ok(Value::Null)
    })
    .await?
}

#[api(
    serializing: true,
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            "backup-type": {
                optional: true,
                type: BackupType,
            },
            "backup-id": {
                optional: true,
                schema: BACKUP_ID_SCHEMA,
            },
        },
    },
    returns: pbs_api_types::ADMIN_DATASTORE_LIST_SNAPSHOTS_RETURN_TYPE,
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_AUDIT for any \
            or DATASTORE_BACKUP and being the owner of the group",
    },
)]
/// List backup snapshots.
pub async fn list_snapshots(
    store: String,
    ns: Option<BackupNamespace>,
    backup_type: Option<BackupType>,
    backup_id: Option<String>,
    _param: Value,
    _info: &ApiMethod,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<SnapshotListItem>, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;

    tokio::task::spawn_blocking(move || unsafe {
        list_snapshots_blocking(store, ns, backup_type, backup_id, auth_id)
    })
    .await
    .map_err(|err| format_err!("failed to await blocking task: {err}"))?
}

fn get_group_owner(
    store: &str,
    ns: &BackupNamespace,
    group: &pbs_datastore::BackupGroup,
) -> Option<Authid> {
    match group.get_owner() {
        Ok(auth_id) => Some(auth_id),
        Err(err) => {
            log::warn!(
                "Failed to get owner of group '{}' in {} - {}",
                group.group(),
                print_store_and_ns(store, ns),
                err
            );
            None
        }
    }
}

/// This must not run in a main worker thread as it potentially does tons of I/O.
unsafe fn list_snapshots_blocking(
    store: String,
    ns: Option<BackupNamespace>,
    backup_type: Option<BackupType>,
    backup_id: Option<String>,
    auth_id: Authid,
) -> Result<Vec<SnapshotListItem>, Error> {
    let ns = ns.unwrap_or_default();

    let list_all = !check_ns_privs_full(
        &store,
        &ns,
        &auth_id,
        PRIV_DATASTORE_AUDIT,
        PRIV_DATASTORE_BACKUP,
    )?;

    let datastore = DataStore::lookup_datastore(&store, Some(Operation::Read))?;

    // FIXME: filter also owner before collecting, for doing that nicely the owner should move into
    // backup group and provide an error free (Err -> None) accessor
    let groups = match (backup_type, backup_id) {
        (Some(backup_type), Some(backup_id)) => {
            vec![datastore.backup_group_from_parts(ns.clone(), backup_type, backup_id)]
        }
        // FIXME: Recursion
        (Some(backup_type), None) => datastore
            .iter_backup_type_ok(ns.clone(), backup_type)?
            .collect(),
        // FIXME: Recursion
        (None, Some(backup_id)) => BackupType::iter()
            .filter_map(|backup_type| {
                let group =
                    datastore.backup_group_from_parts(ns.clone(), backup_type, backup_id.clone());
                group.exists().then_some(group)
            })
            .collect(),
        // FIXME: Recursion
        (None, None) => datastore.list_backup_groups(ns.clone())?,
    };

    groups.iter().try_fold(Vec::new(), |mut snapshots, group| {
        let owner = match get_group_owner(&store, &ns, group) {
            Some(auth_id) => auth_id,
            None => return Ok(snapshots),
        };

        if !list_all && check_backup_owner(&owner, &auth_id).is_err() {
            return Ok(snapshots);
        }

        let group_backups = group.list_backups()?;

        snapshots.extend(
            group_backups
                .into_iter()
                .map(|info| backup_info_to_snapshot_list_item(&info, &owner)),
        );

        Ok(snapshots)
    })
}

async fn get_snapshots_count(
    store: &Arc<DataStore>,
    owner: Option<&Authid>,
) -> Result<Counts, Error> {
    let store = Arc::clone(store);
    let owner = owner.cloned();
    tokio::task::spawn_blocking(move || {
        let root_ns = Default::default();
        ListAccessibleBackupGroups::new_with_privs(
            &store,
            root_ns,
            MAX_NAMESPACE_DEPTH,
            Some(PRIV_DATASTORE_AUDIT | PRIV_DATASTORE_READ),
            None,
            owner.as_ref(),
        )?
        .try_fold(Counts::default(), |mut counts, group| {
            let group = match group {
                Ok(group) => group,
                Err(_) => return Ok(counts), // TODO: add this as error counts?
            };
            let snapshot_count = group.list_backups()?.len() as u64;

            // only include groups with snapshots, counting/displaying empty groups can confuse
            if snapshot_count > 0 {
                let type_count = match group.backup_type() {
                    BackupType::Ct => counts.ct.get_or_insert(Default::default()),
                    BackupType::Vm => counts.vm.get_or_insert(Default::default()),
                    BackupType::Host => counts.host.get_or_insert(Default::default()),
                };

                type_count.groups += 1;
                type_count.snapshots += snapshot_count;
            }

            Ok(counts)
        })
    })
    .await?
}

#[api(
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
            },
            verbose: {
                type: bool,
                default: false,
                optional: true,
                description: "Include additional information like snapshot counts and GC status.",
            },
        },

    },
    returns: {
        type: DataStoreStatus,
    },
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store} either DATASTORE_AUDIT or DATASTORE_BACKUP for \
            the full statistics. Counts of accessible groups are always returned, if any",
    },
)]
/// Get datastore status.
pub async fn status(
    store: String,
    verbose: bool,
    _info: &ApiMethod,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<DataStoreStatus, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let user_info = CachedUserInfo::new()?;
    let store_privs = user_info.lookup_privs(&auth_id, &["datastore", &store]);

    let store_stats = if store_privs & (PRIV_DATASTORE_AUDIT | PRIV_DATASTORE_BACKUP) != 0 {
        true
    } else if store_privs & PRIV_DATASTORE_READ != 0 {
        false // allow at least counts, user can read groups anyway..
    } else {
        match user_info.any_privs_below(&auth_id, &["datastore", &store], NS_PRIVS_OK) {
            // avoid leaking existence info if users hasn't at least any priv. below
            Ok(false) | Err(_) => return Err(http_err!(FORBIDDEN, "permission check failed")),
            _ => false,
        }
    };

    let datastore = DataStore::lookup_datastore(&store, Some(Operation::Read))?;

    let (counts, gc_status) = if verbose {
        let filter_owner = if store_privs & PRIV_DATASTORE_AUDIT != 0 {
            None
        } else {
            Some(&auth_id)
        };

        let counts = Some(get_snapshots_count(&datastore, filter_owner).await?);
        let gc_status = if store_stats {
            Some(datastore.last_gc_status())
        } else {
            None
        };

        (counts, gc_status)
    } else {
        (None, None)
    };

    Ok(if store_stats {
        let storage = crate::tools::fs::fs_info(datastore.base_path()).await?;
        DataStoreStatus {
            total: storage.total,
            used: storage.used,
            avail: storage.available,
            gc_status,
            counts,
        }
    } else {
        DataStoreStatus {
            total: 0,
            used: 0,
            avail: 0,
            gc_status,
            counts,
        }
    })
}

#[api(
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
            },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            "backup-type": {
                type: BackupType,
                optional: true,
            },
            "backup-id": {
                schema: BACKUP_ID_SCHEMA,
                optional: true,
            },
            "ignore-verified": {
                schema: IGNORE_VERIFIED_BACKUPS_SCHEMA,
                optional: true,
            },
            "outdated-after": {
                schema: VERIFICATION_OUTDATED_AFTER_SCHEMA,
                optional: true,
            },
            "backup-time": {
                schema: BACKUP_TIME_SCHEMA,
                optional: true,
            },
            "max-depth": {
                schema: NS_MAX_DEPTH_SCHEMA,
                optional: true,
            },
        },
    },
    returns: {
        schema: UPID_SCHEMA,
    },
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_VERIFY for any \
            or DATASTORE_BACKUP and being the owner of the group",
    },
)]
/// Verify backups.
///
/// This function can verify a single backup snapshot, all backup from a backup group,
/// or all backups in the datastore.
#[allow(clippy::too_many_arguments)]
pub fn verify(
    store: String,
    ns: Option<BackupNamespace>,
    backup_type: Option<BackupType>,
    backup_id: Option<String>,
    backup_time: Option<i64>,
    ignore_verified: Option<bool>,
    outdated_after: Option<i64>,
    max_depth: Option<usize>,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Value, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let ns = ns.unwrap_or_default();

    let owner_check_required = check_ns_privs_full(
        &store,
        &ns,
        &auth_id,
        PRIV_DATASTORE_VERIFY,
        PRIV_DATASTORE_BACKUP,
    )?;

    let datastore = DataStore::lookup_datastore(&store, Some(Operation::Read))?;
    let ignore_verified = ignore_verified.unwrap_or(true);

    let worker_id;

    let mut backup_dir = None;
    let mut backup_group = None;
    let mut worker_type = "verify";

    match (backup_type, backup_id, backup_time) {
        (Some(backup_type), Some(backup_id), Some(backup_time)) => {
            worker_id = format!(
                "{}:{}/{}/{}/{:08X}",
                store,
                ns.display_as_path(),
                backup_type,
                backup_id,
                backup_time
            );
            let dir =
                datastore.backup_dir_from_parts(ns.clone(), backup_type, backup_id, backup_time)?;

            if owner_check_required {
                let owner = datastore.get_owner(dir.backup_ns(), dir.as_ref())?;
                check_backup_owner(&owner, &auth_id)?;
            }

            backup_dir = Some(dir);
            worker_type = "verify_snapshot";
        }
        (Some(backup_type), Some(backup_id), None) => {
            worker_id = format!(
                "{}:{}/{}/{}",
                store,
                ns.display_as_path(),
                backup_type,
                backup_id
            );
            let group = pbs_api_types::BackupGroup::from((backup_type, backup_id));

            if owner_check_required {
                let owner = datastore.get_owner(&ns, &group)?;
                check_backup_owner(&owner, &auth_id)?;
            }

            backup_group = Some(datastore.backup_group(ns.clone(), group));
            worker_type = "verify_group";
        }
        (None, None, None) => {
            worker_id = if ns.is_root() {
                store
            } else {
                format!("{}:{}", store, ns.display_as_path())
            };
        }
        _ => bail!("parameters do not specify a backup group or snapshot"),
    }

    let to_stdout = rpcenv.env_type() == RpcEnvironmentType::CLI;

    let upid_str = WorkerTask::new_thread(
        worker_type,
        Some(worker_id),
        auth_id.to_string(),
        to_stdout,
        move |worker| {
            let verify_worker = VerifyWorker::new(worker.clone(), datastore)?;
            let failed_dirs = if let Some(backup_dir) = backup_dir {
                let mut res = Vec::new();
                if !verify_worker.verify_backup_dir(
                    &backup_dir,
                    worker.upid().clone(),
                    Some(&move |manifest| {
                        VerifyWorker::verify_filter(ignore_verified, outdated_after, manifest)
                    }),
                )? {
                    res.push(print_ns_and_snapshot(
                        backup_dir.backup_ns(),
                        backup_dir.as_ref(),
                    ));
                }
                res
            } else if let Some(backup_group) = backup_group {
                verify_worker.verify_backup_group(
                    &backup_group,
                    &mut StoreProgress::new(1),
                    worker.upid(),
                    Some(&move |manifest| {
                        VerifyWorker::verify_filter(ignore_verified, outdated_after, manifest)
                    }),
                )?
            } else {
                let owner = if owner_check_required {
                    Some(&auth_id)
                } else {
                    None
                };

                verify_worker.verify_all_backups(
                    worker.upid(),
                    ns,
                    max_depth,
                    owner,
                    Some(&move |manifest| {
                        VerifyWorker::verify_filter(ignore_verified, outdated_after, manifest)
                    }),
                )?
            };
            if !failed_dirs.is_empty() {
                info!("Failed to verify the following snapshots/groups:");
                for dir in failed_dirs {
                    info!("\t{dir}");
                }
                bail!("verification failed - please check the log for details");
            }
            Ok(())
        },
    )?;

    Ok(json!(upid_str))
}

#[api(
    input: {
        properties: {
            group: {
                type: pbs_api_types::BackupGroup,
                flatten: true,
            },
            "dry-run": {
                optional: true,
                type: bool,
                default: false,
                description: "Just show what prune would do, but do not delete anything.",
            },
            "keep-options": {
                type: KeepOptions,
                flatten: true,
            },
            store: {
                schema: DATASTORE_SCHEMA,
            },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            "use-task": {
                type: bool,
                default: false,
                optional: true,
                description: "Spins up an asynchronous task that does the work.",
            },
        },
    },
    returns: pbs_api_types::ADMIN_DATASTORE_PRUNE_RETURN_TYPE,
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_MODIFY for any \
            or DATASTORE_PRUNE and being the owner of the group",
    },
)]
/// Prune a group on the datastore
pub fn prune(
    group: pbs_api_types::BackupGroup,
    dry_run: bool,
    keep_options: KeepOptions,
    store: String,
    ns: Option<BackupNamespace>,
    param: Value,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Value, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let ns = ns.unwrap_or_default();
    let datastore = check_privs_and_load_store(
        &store,
        &ns,
        &auth_id,
        PRIV_DATASTORE_MODIFY,
        PRIV_DATASTORE_PRUNE,
        Some(Operation::Write),
        &group,
    )?;

    let worker_id = format!("{store}:{ns}:{group}");
    let group = datastore.backup_group(ns.clone(), group);

    #[derive(Debug, serde::Serialize)]
    struct PruneResult {
        #[serde(rename = "backup-type")]
        backup_type: BackupType,
        #[serde(rename = "backup-id")]
        backup_id: String,
        #[serde(rename = "backup-time")]
        backup_time: i64,
        keep: bool,
        protected: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        ns: Option<BackupNamespace>,
    }
    let mut prune_result: Vec<PruneResult> = Vec::new();

    let list = group.list_backups()?;

    let mut prune_info = compute_prune_info(list, &keep_options)?;

    prune_info.reverse(); // delete older snapshots first

    let keep_all = !keep_options.keeps_something();

    if dry_run {
        for (info, mark) in prune_info {
            let keep = keep_all || mark.keep();
            let backup_dir = &info.backup_dir;

            let mut result = PruneResult {
                backup_type: backup_dir.backup_type(),
                backup_id: backup_dir.backup_id().to_owned(),
                backup_time: backup_dir.backup_time(),
                keep,
                protected: mark.protected(),
                ns: None,
            };
            let prune_ns = backup_dir.backup_ns();
            if !prune_ns.is_root() {
                result.ns = Some(prune_ns.to_owned());
            }
            prune_result.push(result);
        }
        return Ok(json!(prune_result));
    }

    let prune_group = move |_worker: Arc<WorkerTask>| {
        if keep_all {
            info!("No prune selection - keeping all files.");
        } else {
            let mut opts = Vec::new();
            if !ns.is_root() {
                opts.push(format!("--ns {ns}"));
            }
            crate::server::cli_keep_options(&mut opts, &keep_options);

            info!("retention options: {}", opts.join(" "));
            info!(
                "Starting prune on {} group \"{}\"",
                print_store_and_ns(&store, &ns),
                group.group(),
            );
        }

        for (info, mark) in prune_info {
            let keep = keep_all || mark.keep();
            let backup_dir = &info.backup_dir;

            let backup_time = backup_dir.backup_time();
            let timestamp = backup_dir.backup_time_string();
            let group: &pbs_api_types::BackupGroup = backup_dir.as_ref();

            let msg = format!("{}/{}/{timestamp} {mark}", group.ty, group.id);

            info!("{msg}");

            prune_result.push(PruneResult {
                backup_type: group.ty,
                backup_id: group.id.clone(),
                backup_time,
                keep,
                protected: mark.protected(),
                ns: None,
            });

            if !keep {
                match datastore.backend() {
                    Ok(backend) => {
                        if let Err(err) = backup_dir.destroy(false, &backend) {
                            warn!(
                                "failed to remove dir {:?}: {}",
                                backup_dir.relative_path(),
                                err,
                            );
                        }
                    }
                    Err(err) => warn!(
                        "failed to remove dir {:?}: {err}",
                        backup_dir.relative_path()
                    ),
                };
            }
        }
        prune_result
    };

    if param["use-task"].as_bool().unwrap_or(false) {
        let upid = WorkerTask::spawn(
            "prune",
            Some(worker_id),
            auth_id.to_string(),
            true,
            move |worker| async move {
                let _ = prune_group(worker.clone());
                Ok(())
            },
        )?;
        Ok(json!(upid))
    } else {
        let (worker, logger) =
            WorkerTask::new("prune", Some(worker_id), auth_id.to_string(), true)?;
        let result = LogContext::new(logger).sync_scope(|| {
            let result = prune_group(worker.clone());
            worker.log_result(&Ok(()));
            result
        });
        Ok(json!(result))
    }
}

#[api(
    input: {
        properties: {
            "dry-run": {
                optional: true,
                type: bool,
                default: false,
                description: "Just show what prune would do, but do not delete anything.",
            },
            "prune-options": {
                type: PruneJobOptions,
                flatten: true,
            },
            store: {
                schema: DATASTORE_SCHEMA,
            },
        },
    },
    returns: {
        schema: UPID_SCHEMA,
    },
    access: {
        permission: &Permission::Anybody,
        description: "Requires Datastore.Modify or Datastore.Prune on the datastore/namespace.",
    },
)]
/// Prune the datastore
pub fn prune_datastore(
    dry_run: bool,
    prune_options: PruneJobOptions,
    store: String,
    _param: Value,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<String, Error> {
    let user_info = CachedUserInfo::new()?;

    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;

    user_info.check_privs(
        &auth_id,
        &prune_options.acl_path(&store),
        PRIV_DATASTORE_MODIFY | PRIV_DATASTORE_PRUNE,
        true,
    )?;

    let datastore = DataStore::lookup_datastore(&store, Some(Operation::Write))?;
    let ns = prune_options.ns.clone().unwrap_or_default();
    let worker_id = format!("{store}:{ns}");

    let to_stdout = rpcenv.env_type() == RpcEnvironmentType::CLI;

    let upid_str = WorkerTask::new_thread(
        "prune",
        Some(worker_id),
        auth_id.to_string(),
        to_stdout,
        move |_worker| crate::server::prune_datastore(auth_id, prune_options, datastore, dry_run),
    )?;

    Ok(upid_str)
}

#[api(
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
            },
        },
    },
    returns: {
        schema: UPID_SCHEMA,
    },
    access: {
        permission: &Permission::Privilege(&["datastore", "{store}"], PRIV_DATASTORE_MODIFY, false),
    },
)]
/// Start garbage collection.
pub fn start_garbage_collection(
    store: String,
    _info: &ApiMethod,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Value, Error> {
    let datastore = DataStore::lookup_datastore(&store, Some(Operation::Write))?;
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;

    let job = Job::new("garbage_collection", &store)
        .map_err(|_| format_err!("garbage collection already running"))?;

    let to_stdout = rpcenv.env_type() == RpcEnvironmentType::CLI;

    let upid_str =
        crate::server::do_garbage_collection_job(job, datastore, &auth_id, None, to_stdout)
            .map_err(|err| {
                format_err!("unable to start garbage collection job on datastore {store} - {err:#}")
            })?;

    Ok(json!(upid_str))
}

#[api(
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
            },
        },
    },
    returns: {
        type: GarbageCollectionJobStatus,
    },
    access: {
        permission: &Permission::Privilege(&["datastore", "{store}"], PRIV_DATASTORE_AUDIT, false),
    },
)]
/// Garbage collection status.
pub fn garbage_collection_status(
    store: String,
    _info: &ApiMethod,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<GarbageCollectionJobStatus, Error> {
    let (config, _) = pbs_config::datastore::config()?;
    let store_config: DataStoreConfig = config.lookup("datastore", &store)?;

    let mut info = GarbageCollectionJobStatus {
        store: store.clone(),
        schedule: store_config.gc_schedule,
        ..Default::default()
    };

    let datastore = DataStore::lookup_datastore(&store, Some(Operation::Read))?;
    let status_in_memory = datastore.last_gc_status();
    let state_file = JobState::load("garbage_collection", &store)
        .map_err(|err| log::error!("could not open GC statefile for {store}: {err}"))
        .ok();

    let mut last = proxmox_time::epoch_i64();

    if let Some(ref upid) = status_in_memory.upid {
        let mut computed_schedule: JobScheduleStatus = JobScheduleStatus::default();
        if let Some(state) = state_file {
            if let Ok(cs) = compute_schedule_status(&state, Some(upid)) {
                computed_schedule = cs;
            }
        }

        if let Some(endtime) = computed_schedule.last_run_endtime {
            last = endtime;
            if let Ok(parsed_upid) = upid.parse::<UPID>() {
                info.duration = Some(endtime - parsed_upid.starttime);
            }
        }

        info.next_run = computed_schedule.next_run;
        info.last_run_endtime = computed_schedule.last_run_endtime;
        info.last_run_state = computed_schedule.last_run_state;
    }

    info.next_run = info
        .schedule
        .as_ref()
        .and_then(|s| {
            s.parse::<CalendarEvent>()
                .map_err(|err| log::error!("{err}"))
                .ok()
        })
        .and_then(|e| {
            e.compute_next_event(last)
                .map_err(|err| log::error!("{err}"))
                .ok()
        })
        .and_then(|ne| ne);

    info.status = status_in_memory;

    Ok(info)
}

#[api(
    returns: {
        description: "List the accessible datastores.",
        type: Array,
        items: { type: DataStoreListItem },
    },
    access: {
        permission: &Permission::Anybody,
    },
)]
/// Datastore list
pub fn get_datastore_list(
    _param: Value,
    _info: &ApiMethod,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<DataStoreListItem>, Error> {
    let (config, _digest) = pbs_config::datastore::config()?;

    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let user_info = CachedUserInfo::new()?;

    let mut list = Vec::new();

    for (store, (_, data)) in config.sections {
        let acl_path = &["datastore", &store];
        let user_privs = user_info.lookup_privs(&auth_id, acl_path);
        let allowed = (user_privs & (PRIV_DATASTORE_AUDIT | PRIV_DATASTORE_BACKUP)) != 0;

        let mut allow_id = false;
        if !allowed {
            if let Ok(any_privs) = user_info.any_privs_below(&auth_id, acl_path, NS_PRIVS_OK) {
                allow_id = any_privs;
            }
        }

        if allowed || allow_id {
            let store_config: DataStoreConfig = serde_json::from_value(data)?;

            let mount_status = match pbs_datastore::get_datastore_mount_status(&store_config) {
                Some(true) => DataStoreMountStatus::Mounted,
                Some(false) => DataStoreMountStatus::NotMounted,
                None => DataStoreMountStatus::NonRemovable,
            };

            list.push(DataStoreListItem {
                store: store.clone(),
                comment: store_config.comment.filter(|_| allowed),
                mount_status,
                maintenance: store_config.maintenance_mode,
            });
        }
    }

    Ok(list)
}

#[sortable]
pub const API_METHOD_DOWNLOAD_FILE: ApiMethod = ApiMethod::new(
    &ApiHandler::AsyncHttp(&download_file),
    &ObjectSchema::new(
        "Download single raw file from backup snapshot.",
        &sorted!([
            ("store", false, &DATASTORE_SCHEMA),
            ("ns", true, &BACKUP_NAMESPACE_SCHEMA),
            ("backup-type", false, &BACKUP_TYPE_SCHEMA),
            ("backup-id", false, &BACKUP_ID_SCHEMA),
            ("backup-time", false, &BACKUP_TIME_SCHEMA),
            ("file-name", false, &BACKUP_ARCHIVE_NAME_SCHEMA),
        ]),
    ),
)
.access(
    Some(
        "Requires on /datastore/{store}[/{namespace}] either DATASTORE_READ for any or \
        DATASTORE_BACKUP and being the owner of the group",
    ),
    &Permission::Anybody,
);

pub fn download_file(
    _parts: Parts,
    _req_body: Incoming,
    param: Value,
    _info: &ApiMethod,
    rpcenv: Box<dyn RpcEnvironment>,
) -> ApiResponseFuture {
    async move {
        let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
        let store = required_string_param(&param, "store")?;
        let backup_ns = optional_ns_param(&param)?;

        let backup_dir: pbs_api_types::BackupDir = Deserialize::deserialize(&param)?;
        let datastore = check_privs_and_load_store(
            store,
            &backup_ns,
            &auth_id,
            PRIV_DATASTORE_READ,
            PRIV_DATASTORE_BACKUP,
            Some(Operation::Read),
            &backup_dir.group,
        )?;

        let file_name = required_string_param(&param, "file-name")?.to_owned();

        println!(
            "Download {} from {} ({}/{})",
            file_name,
            print_store_and_ns(store, &backup_ns),
            backup_dir,
            file_name
        );

        let backup_dir = datastore.backup_dir(backup_ns, backup_dir)?;

        let mut path = datastore.base_path();
        path.push(backup_dir.relative_path());
        path.push(&file_name);

        let file = tokio::fs::File::open(&path)
            .await
            .map_err(|err| http_err!(BAD_REQUEST, "File open failed: {}", err))?;

        let payload =
            tokio_util::codec::FramedRead::new(file, tokio_util::codec::BytesCodec::new())
                .map_ok(|bytes| bytes.freeze())
                .map_err(move |err| {
                    eprintln!("error during streaming of '{:?}' - {}", &path, err);
                    err
                });
        let body = Body::wrap_stream(payload);

        // fixme: set other headers ?
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .unwrap())
    }
    .boxed()
}

#[sortable]
pub const API_METHOD_DOWNLOAD_FILE_DECODED: ApiMethod = ApiMethod::new(
    &ApiHandler::AsyncHttp(&download_file_decoded),
    &ObjectSchema::new(
        "Download single decoded file from backup snapshot. Only works if it's not encrypted.",
        &sorted!([
            ("store", false, &DATASTORE_SCHEMA),
            ("ns", true, &BACKUP_NAMESPACE_SCHEMA),
            ("backup-type", false, &BACKUP_TYPE_SCHEMA),
            ("backup-id", false, &BACKUP_ID_SCHEMA),
            ("backup-time", false, &BACKUP_TIME_SCHEMA),
            ("file-name", false, &BACKUP_ARCHIVE_NAME_SCHEMA),
        ]),
    ),
)
.access(
    Some(
        "Requires on /datastore/{store}[/{namespace}] either DATASTORE_READ for any or \
        DATASTORE_BACKUP and being the owner of the group",
    ),
    &Permission::Anybody,
);

pub fn download_file_decoded(
    _parts: Parts,
    _req_body: Incoming,
    param: Value,
    _info: &ApiMethod,
    rpcenv: Box<dyn RpcEnvironment>,
) -> ApiResponseFuture {
    async move {
        let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
        let store = required_string_param(&param, "store")?;
        let backup_ns = optional_ns_param(&param)?;

        let backup_dir_api: pbs_api_types::BackupDir = Deserialize::deserialize(&param)?;
        let datastore = check_privs_and_load_store(
            store,
            &backup_ns,
            &auth_id,
            PRIV_DATASTORE_READ,
            PRIV_DATASTORE_BACKUP,
            Some(Operation::Read),
            &backup_dir_api.group,
        )?;

        let file_name: BackupArchiveName =
            required_string_param(&param, "file-name")?.try_into()?;
        let backup_dir = datastore.backup_dir(backup_ns.clone(), backup_dir_api.clone())?;

        let (manifest, files) = read_backup_index(&backup_dir)?;
        for file in files {
            if file.filename == file_name.as_ref() && file.crypt_mode == Some(CryptMode::Encrypt) {
                bail!("cannot decode '{}' - is encrypted", file_name);
            }
        }

        println!(
            "Download {} from {} ({}/{})",
            file_name,
            print_store_and_ns(store, &backup_ns),
            backup_dir_api,
            file_name
        );

        let mut path = datastore.base_path();
        path.push(backup_dir.relative_path());
        path.push(file_name.as_ref());

        let body = match file_name.archive_type() {
            ArchiveType::DynamicIndex => {
                let index = DynamicIndexReader::open(&path).map_err(|err| {
                    format_err!("unable to read dynamic index '{:?}' - {}", &path, err)
                })?;
                let (csum, size) = index.compute_csum();
                manifest.verify_file(&file_name, &csum, size)?;

                let chunk_reader = LocalChunkReader::new(datastore, None, CryptMode::None)
                    .context("creating local chunk reader failed")?;
                let reader = CachedChunkReader::new(chunk_reader, index, 1).seekable();
                Body::wrap_stream(AsyncReaderStream::new(reader).map_err(move |err| {
                    eprintln!("error during streaming of '{path:?}' - {err}");
                    err
                }))
            }
            ArchiveType::FixedIndex => {
                let index = FixedIndexReader::open(&path).map_err(|err| {
                    format_err!("unable to read fixed index '{:?}' - {}", &path, err)
                })?;

                let (csum, size) = index.compute_csum();
                manifest.verify_file(&file_name, &csum, size)?;

                let chunk_reader = LocalChunkReader::new(datastore, None, CryptMode::None)
                    .context("creating local chunk reader failed")?;
                let reader = CachedChunkReader::new(chunk_reader, index, 1).seekable();
                Body::wrap_stream(
                    AsyncReaderStream::with_buffer_size(reader, 4 * 1024 * 1024).map_err(
                        move |err| {
                            eprintln!("error during streaming of '{path:?}' - {err}");
                            err
                        },
                    ),
                )
            }
            ArchiveType::Blob => {
                let file = std::fs::File::open(&path)
                    .map_err(|err| http_err!(BAD_REQUEST, "File open failed: {}", err))?;

                // FIXME: load full blob to verify index checksum?

                Body::wrap_stream(
                    WrappedReaderStream::new(DataBlobReader::new(file, None)?).map_err(
                        move |err| {
                            eprintln!("error during streaming of '{path:?}' - {err}");
                            err
                        },
                    ),
                )
            }
        };

        // fixme: set other headers ?
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .unwrap())
    }
    .boxed()
}

#[sortable]
pub const API_METHOD_UPLOAD_BACKUP_LOG: ApiMethod = ApiMethod::new(
    &ApiHandler::AsyncHttp(&upload_backup_log),
    &ObjectSchema::new(
        "Upload the client backup log file into a backup snapshot ('client.log.blob').",
        &sorted!([
            ("store", false, &DATASTORE_SCHEMA),
            ("ns", true, &BACKUP_NAMESPACE_SCHEMA),
            ("backup-type", false, &BACKUP_TYPE_SCHEMA),
            ("backup-id", false, &BACKUP_ID_SCHEMA),
            ("backup-time", false, &BACKUP_TIME_SCHEMA),
        ]),
    ),
)
.access(
    Some("Only the backup creator/owner is allowed to do this."),
    &Permission::Anybody,
);

pub fn upload_backup_log(
    _parts: Parts,
    req_body: Incoming,
    param: Value,
    _info: &ApiMethod,
    rpcenv: Box<dyn RpcEnvironment>,
) -> ApiResponseFuture {
    async move {
        let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
        let store = required_string_param(&param, "store")?;
        let backup_ns = optional_ns_param(&param)?;

        let backup_dir_api: pbs_api_types::BackupDir = Deserialize::deserialize(&param)?;

        let datastore = check_privs_and_load_store(
            store,
            &backup_ns,
            &auth_id,
            0,
            PRIV_DATASTORE_BACKUP,
            Some(Operation::Write),
            &backup_dir_api.group,
        )?;
        let backup_dir = datastore.backup_dir(backup_ns.clone(), backup_dir_api.clone())?;

        let file_name = &CLIENT_LOG_BLOB_NAME;

        let mut path = backup_dir.full_path();
        path.push(file_name.as_ref());

        if path.exists() {
            bail!("backup already contains a log.");
        }

        println!(
            "Upload backup log to {} {backup_dir_api}/{file_name}",
            print_store_and_ns(store, &backup_ns),
            file_name = file_name.deref(),
        );

        let data = req_body.collect().await.map_err(Error::from)?.to_bytes();

        // always verify blob/CRC at server side
        let blob = DataBlob::load_from_reader(&mut &data[..])?;

        if let DatastoreBackend::S3(s3_client) = datastore.backend()? {
            let object_key = pbs_datastore::s3::object_key_from_path(
                &backup_dir.relative_path(),
                file_name.as_ref(),
            )
            .context("invalid client log object key")?;
            let data = hyper::body::Bytes::copy_from_slice(blob.raw_data());
            s3_client
                .upload_replace_with_retry(object_key, data)
                .await
                .context("failed to upload client log to s3 backend")?;
        };

        replace_file(&path, blob.raw_data(), CreateOptions::new(), false)?;

        // fixme: use correct formatter
        Ok(formatter::JSON_FORMATTER.format_data(Value::Null, &*rpcenv))
    }
    .boxed()
}

fn decode_path(path: &str) -> Result<Vec<u8>, Error> {
    if path != "root" && path != "/" {
        proxmox_base64::decode(path)
            .map_err(|err| format_err!("base64 decoding of path failed - {err}"))
    } else {
        Ok(vec![b'/'])
    }
}

#[api(
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            backup_dir: {
                type: pbs_api_types::BackupDir,
                flatten: true,
            },
            "filepath": {
                description: "Base64 encoded path.",
                type: String,
            },
            "archive-name": {
                type: BackupArchiveName,
                optional: true,
            },
        },
    },
    access: {
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_READ for any or \
            DATASTORE_BACKUP and being the owner of the group",
        permission: &Permission::Anybody,
    },
)]
/// Get the entries of the given path of the catalog
pub async fn catalog(
    store: String,
    ns: Option<BackupNamespace>,
    backup_dir: pbs_api_types::BackupDir,
    filepath: String,
    archive_name: Option<BackupArchiveName>,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<ArchiveEntry>, Error> {
    let file_name = archive_name.clone().unwrap_or_else(|| CATALOG_NAME.clone());

    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;

    let ns = ns.unwrap_or_default();

    let datastore = check_privs_and_load_store(
        &store,
        &ns,
        &auth_id,
        PRIV_DATASTORE_READ,
        PRIV_DATASTORE_BACKUP,
        Some(Operation::Read),
        &backup_dir.group,
    )?;

    let backup_dir = datastore.backup_dir(ns, backup_dir)?;

    let (manifest, files) = read_backup_index(&backup_dir)?;
    for file in files {
        if file.filename == file_name.as_ref() && file.crypt_mode == Some(CryptMode::Encrypt) {
            bail!("cannot decode '{file_name}' - is encrypted");
        }
    }

    if archive_name.is_none() {
        tokio::task::spawn_blocking(move || {
            let mut path = datastore.base_path();
            path.push(backup_dir.relative_path());
            path.push(file_name.as_ref());

            let index = DynamicIndexReader::open(&path)
                .map_err(|err| format_err!("unable to read dynamic index '{path:?}' - {err}"))?;

            let (csum, size) = index.compute_csum();
            manifest.verify_file(&file_name, &csum, size)?;

            let chunk_reader = LocalChunkReader::new(datastore, None, CryptMode::None)
                .context("creating local chunk reader failed")?;
            let reader = BufferedDynamicReader::new(index, chunk_reader);

            let mut catalog_reader = CatalogReader::new(reader);

            let path = decode_path(&filepath)?;
            catalog_reader.list_dir_contents(&path)
        })
        .await?
    } else {
        let (archive_name, _payload_archive_name) =
            pbs_client::tools::get_pxar_archive_names(&file_name, &manifest)?;
        let (reader, archive_size) =
            get_local_pxar_reader(datastore.clone(), &manifest, &backup_dir, &archive_name)?;

        // only care about the metadata, don't attach a payload reader
        let reader = pxar::PxarVariant::Unified(reader);
        let accessor = Accessor::new(reader, archive_size).await?;

        let file_path = decode_path(&filepath)?;
        pbs_client::pxar::tools::pxar_metadata_catalog_lookup(
            accessor,
            OsStr::from_bytes(&file_path),
            None,
        )
        .await
    }
}

#[sortable]
pub const API_METHOD_PXAR_FILE_DOWNLOAD: ApiMethod = ApiMethod::new(
    &ApiHandler::AsyncHttp(&pxar_file_download),
    &ObjectSchema::new(
        "Download single file from pxar file of a backup snapshot. Only works if it's not encrypted.",
        &sorted!([
            ("store", false, &DATASTORE_SCHEMA),
            ("ns", true, &BACKUP_NAMESPACE_SCHEMA),
            ("backup-type", false, &BACKUP_TYPE_SCHEMA),
            ("backup-id", false,  &BACKUP_ID_SCHEMA),
            ("backup-time", false, &BACKUP_TIME_SCHEMA),
            ("filepath", false, &StringSchema::new("Base64 encoded path").schema()),
            ("tar", true, &BooleanSchema::new("Download as .tar.zst").schema()),
            ("archive-name", true, &BackupArchiveName::API_SCHEMA),
        ]),
    )
).access(
    Some(
        "Requires on /datastore/{store}[/{namespace}] either DATASTORE_READ for any or \
        DATASTORE_BACKUP and being the owner of the group",
    ),
    &Permission::Anybody,
);

fn get_local_pxar_reader(
    datastore: Arc<DataStore>,
    manifest: &BackupManifest,
    backup_dir: &BackupDir,
    pxar_name: &BackupArchiveName,
) -> Result<(LocalDynamicReadAt<LocalChunkReader>, u64), Error> {
    let mut path = datastore.base_path();
    path.push(backup_dir.relative_path());
    path.push(pxar_name.as_ref());

    let index = DynamicIndexReader::open(&path)
        .map_err(|err| format_err!("unable to read dynamic index '{:?}' - {}", &path, err))?;

    let (csum, size) = index.compute_csum();
    manifest.verify_file(pxar_name, &csum, size)?;

    let chunk_reader = LocalChunkReader::new(datastore, None, CryptMode::None)
        .context("creating local chunk reader failed")?;
    let reader = BufferedDynamicReader::new(index, chunk_reader);
    let archive_size = reader.archive_size();

    Ok((LocalDynamicReadAt::new(reader), archive_size))
}

pub fn pxar_file_download(
    _parts: Parts,
    _req_body: Incoming,
    param: Value,
    _info: &ApiMethod,
    rpcenv: Box<dyn RpcEnvironment>,
) -> ApiResponseFuture {
    async move {
        let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
        let store = required_string_param(&param, "store")?;
        let ns = optional_ns_param(&param)?;

        let backup_dir: pbs_api_types::BackupDir = Deserialize::deserialize(&param)?;
        let datastore = check_privs_and_load_store(
            store,
            &ns,
            &auth_id,
            PRIV_DATASTORE_READ,
            PRIV_DATASTORE_BACKUP,
            Some(Operation::Read),
            &backup_dir.group,
        )?;

        let backup_dir = datastore.backup_dir(ns, backup_dir)?;

        let filepath = required_string_param(&param, "filepath")?.to_owned();

        let tar = param["tar"].as_bool().unwrap_or(false);

        let mut components = proxmox_base64::decode(&filepath)?;
        if !components.is_empty() && components[0] == b'/' {
            components.remove(0);
        }

        let (pxar_name, file_path) = if let Some(archive_name) = param["archive-name"].as_str() {
            let archive_name = archive_name.as_bytes().to_owned();
            (archive_name, proxmox_base64::decode(&filepath)?)
        } else {
            let mut split = components.splitn(2, |c| *c == b'/');
            let pxar_name = split.next().unwrap();
            let file_path = split.next().unwrap_or(b"/");
            (pxar_name.to_owned(), file_path.to_owned())
        };
        let pxar_name: BackupArchiveName = std::str::from_utf8(&pxar_name)?.try_into()?;
        let (manifest, files) = read_backup_index(&backup_dir)?;
        for file in files {
            if file.filename == pxar_name.as_ref() && file.crypt_mode == Some(CryptMode::Encrypt) {
                bail!("cannot decode '{}' - is encrypted", pxar_name);
            }
        }

        let (pxar_name, payload_archive_name) =
            pbs_client::tools::get_pxar_archive_names(&pxar_name, &manifest)?;
        let (reader, archive_size) =
            get_local_pxar_reader(datastore.clone(), &manifest, &backup_dir, &pxar_name)?;

        let reader = if let Some(payload_archive_name) = payload_archive_name {
            let payload_input =
                get_local_pxar_reader(datastore, &manifest, &backup_dir, &payload_archive_name)?;
            pxar::PxarVariant::Split(reader, payload_input)
        } else {
            pxar::PxarVariant::Unified(reader)
        };
        let decoder = Accessor::new(reader, archive_size).await?;

        let root = decoder.open_root().await?;
        let path = OsStr::from_bytes(&file_path).to_os_string();
        let file = root
            .lookup(&path)
            .await?
            .ok_or_else(|| format_err!("error opening '{:?}'", path))?;

        let body = match file.kind() {
            EntryKind::File { .. } => Body::wrap_stream(
                AsyncReaderStream::new(file.contents().await?).map_err(move |err| {
                    eprintln!("error during streaming of file '{filepath:?}' - {err}");
                    err
                }),
            ),
            EntryKind::Hardlink(_) => Body::wrap_stream(
                AsyncReaderStream::new(decoder.follow_hardlink(&file).await?.contents().await?)
                    .map_err(move |err| {
                        eprintln!("error during streaming of hardlink '{path:?}' - {err}");
                        err
                    }),
            ),
            EntryKind::Directory => {
                let (sender, receiver) = tokio::sync::mpsc::channel::<Result<_, Error>>(100);
                let channelwriter = AsyncChannelWriter::new(sender, 1024 * 1024);
                if tar {
                    proxmox_rest_server::spawn_internal_task(create_tar(
                        channelwriter,
                        decoder,
                        path.clone(),
                    ));
                    let zstdstream = ZstdEncoder::new(ReceiverStream::new(receiver))?;
                    Body::wrap_stream(zstdstream.map_err(move |err| {
                        log::error!("error during streaming of tar.zst '{:?}' - {}", path, err);
                        err
                    }))
                } else {
                    proxmox_rest_server::spawn_internal_task(create_zip(
                        channelwriter,
                        decoder,
                        path.clone(),
                    ));
                    Body::wrap_stream(ReceiverStream::new(receiver).map_err(move |err| {
                        log::error!("error during streaming of zip '{:?}' - {}", path, err);
                        err
                    }))
                }
            }
            other => bail!("cannot download file of type {:?}", other),
        };

        // fixme: set other headers ?
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .unwrap())
    }
    .boxed()
}

#[api(
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
            },
            timeframe: {
                type: RrdTimeframe,
            },
            cf: {
                type: RrdMode,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(
            &["datastore", "{store}"], PRIV_DATASTORE_AUDIT | PRIV_DATASTORE_BACKUP, true),
    },
)]
/// Read datastore stats
pub fn get_rrd_stats(
    store: String,
    timeframe: RrdTimeframe,
    cf: RrdMode,
    _param: Value,
) -> Result<Value, Error> {
    let datastore = DataStore::lookup_datastore(&store, Some(Operation::Read))?;
    let disk_manager = crate::tools::disks::DiskManage::new();

    let mut rrd_fields = vec![
        "total",
        "available",
        "used",
        "read_ios",
        "read_bytes",
        "write_ios",
        "write_bytes",
    ];

    // we do not have io_ticks for zpools, so don't include them
    match disk_manager.find_mounted_device(&datastore.base_path()) {
        Ok(Some((fs_type, _, _))) if fs_type.as_str() == "zfs" => {}
        _ => rrd_fields.push("io_ticks"),
    };

    create_value_from_rrd(&format!("datastore/{store}"), &rrd_fields, timeframe, cf)
}

#[api(
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["datastore", "{store}"], PRIV_DATASTORE_AUDIT, true),
    },
)]
/// Read datastore stats
pub fn get_active_operations(store: String, _param: Value) -> Result<Value, Error> {
    let active_operations = task_tracking::get_active_operations(&store)?;
    Ok(json!({
        "read": active_operations.read,
        "write": active_operations.write,
    }))
}

#[api(
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            backup_group: {
                type: pbs_api_types::BackupGroup,
                flatten: true,
            },
        },
    },
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_AUDIT for any \
            or DATASTORE_BACKUP and being the owner of the group",
    },
)]
/// Get "notes" for a backup group
pub fn get_group_notes(
    store: String,
    ns: Option<BackupNamespace>,
    backup_group: pbs_api_types::BackupGroup,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<String, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let ns = ns.unwrap_or_default();

    let datastore = check_privs_and_load_store(
        &store,
        &ns,
        &auth_id,
        PRIV_DATASTORE_AUDIT,
        PRIV_DATASTORE_BACKUP,
        Some(Operation::Read),
        &backup_group,
    )?;

    let notes_path = datastore.group_notes_path(&ns, &backup_group);
    Ok(file_read_optional_string(notes_path)?.unwrap_or_else(|| "".to_owned()))
}

#[api(
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            backup_group: {
                type: pbs_api_types::BackupGroup,
                flatten: true,
            },
            notes: {
                description: "A multiline text.",
            },
        },
    },
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_MODIFY for any \
            or DATASTORE_BACKUP and being the owner of the group",
    },
)]
/// Set "notes" for a backup group
pub fn set_group_notes(
    store: String,
    ns: Option<BackupNamespace>,
    backup_group: pbs_api_types::BackupGroup,
    notes: String,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let ns = ns.unwrap_or_default();

    let datastore = check_privs_and_load_store(
        &store,
        &ns,
        &auth_id,
        PRIV_DATASTORE_MODIFY,
        PRIV_DATASTORE_BACKUP,
        Some(Operation::Write),
        &backup_group,
    )?;

    let backup_group = datastore.backup_group(ns, backup_group);
    datastore
        .set_group_notes(notes, backup_group)
        .map_err(|err| format_err!("failed to set group notes - {err:#?}"))?;
    Ok(())
}

#[api(
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            backup_dir: {
                type: pbs_api_types::BackupDir,
                flatten: true,
            },
        },
    },
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_AUDIT for any \
            or DATASTORE_BACKUP and being the owner of the group",
    },
)]
/// Get "notes" for a specific backup
pub fn get_notes(
    store: String,
    ns: Option<BackupNamespace>,
    backup_dir: pbs_api_types::BackupDir,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<String, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let ns = ns.unwrap_or_default();

    let datastore = check_privs_and_load_store(
        &store,
        &ns,
        &auth_id,
        PRIV_DATASTORE_AUDIT,
        PRIV_DATASTORE_BACKUP,
        Some(Operation::Read),
        &backup_dir.group,
    )?;

    let backup_dir = datastore.backup_dir(ns, backup_dir)?;

    let (manifest, _) = backup_dir.load_manifest()?;

    let notes = manifest.unprotected["notes"].as_str().unwrap_or("");

    Ok(String::from(notes))
}

#[api(
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            backup_dir: {
                type: pbs_api_types::BackupDir,
                flatten: true,
            },
            notes: {
                description: "A multiline text.",
            },
        },
    },
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_MODIFY for any \
            or DATASTORE_BACKUP and being the owner of the group",
    },
)]
/// Set "notes" for a specific backup
pub fn set_notes(
    store: String,
    ns: Option<BackupNamespace>,
    backup_dir: pbs_api_types::BackupDir,
    notes: String,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let ns = ns.unwrap_or_default();

    let datastore = check_privs_and_load_store(
        &store,
        &ns,
        &auth_id,
        PRIV_DATASTORE_MODIFY,
        PRIV_DATASTORE_BACKUP,
        Some(Operation::Write),
        &backup_dir.group,
    )?;

    let backup_dir = datastore.backup_dir(ns, backup_dir)?;

    backup_dir
        .update_manifest(&datastore.backend()?, |manifest| {
            manifest.unprotected["notes"] = notes.into();
        })
        .map_err(|err| format_err!("unable to update manifest blob - {}", err))?;

    Ok(())
}

#[api(
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            backup_dir: {
                type: pbs_api_types::BackupDir,
                flatten: true,
            },
        },
    },
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_AUDIT for any \
            or DATASTORE_BACKUP and being the owner of the group",
    },
)]
/// Query protection for a specific backup
pub fn get_protection(
    store: String,
    ns: Option<BackupNamespace>,
    backup_dir: pbs_api_types::BackupDir,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<bool, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let ns = ns.unwrap_or_default();
    let datastore = check_privs_and_load_store(
        &store,
        &ns,
        &auth_id,
        PRIV_DATASTORE_AUDIT,
        PRIV_DATASTORE_BACKUP,
        Some(Operation::Read),
        &backup_dir.group,
    )?;

    let backup_dir = datastore.backup_dir(ns, backup_dir)?;

    Ok(backup_dir.is_protected())
}

#[api(
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            backup_dir: {
                type: pbs_api_types::BackupDir,
                flatten: true,
            },
            protected: {
                description: "Enable/disable protection.",
            },
        },
    },
    access: {
        permission: &Permission::Anybody,
        description: "Requires on /datastore/{store}[/{namespace}] either DATASTORE_MODIFY for any \
            or DATASTORE_BACKUP and being the owner of the group",
    },
)]
/// En- or disable protection for a specific backup
pub async fn set_protection(
    store: String,
    ns: Option<BackupNamespace>,
    backup_dir: pbs_api_types::BackupDir,
    protected: bool,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;

    tokio::task::spawn_blocking(move || {
        let ns = ns.unwrap_or_default();
        let datastore = check_privs_and_load_store(
            &store,
            &ns,
            &auth_id,
            PRIV_DATASTORE_MODIFY,
            PRIV_DATASTORE_BACKUP,
            Some(Operation::Write),
            &backup_dir.group,
        )?;

        let backup_dir = datastore.backup_dir(ns, backup_dir)?;

        datastore.update_protection(&backup_dir, protected)
    })
    .await?
}

#[api(
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            backup_group: {
                type: pbs_api_types::BackupGroup,
                flatten: true,
            },
            "new-owner": {
                type: Authid,
            },
        },
    },
    access: {
        permission: &Permission::Anybody,
        description: "Datastore.Modify on whole datastore, or changing ownership between user and \
            a user's token for owned backups with Datastore.Backup"
    },
)]
/// Change owner of a backup group
pub async fn set_backup_owner(
    store: String,
    ns: Option<BackupNamespace>,
    backup_group: pbs_api_types::BackupGroup,
    new_owner: Authid,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;

    tokio::task::spawn_blocking(move || {
        let ns = ns.unwrap_or_default();
        let owner_check_required = check_ns_privs_full(
            &store,
            &ns,
            &auth_id,
            PRIV_DATASTORE_MODIFY,
            PRIV_DATASTORE_BACKUP,
        )?;

        let datastore = DataStore::lookup_datastore(&store, Some(Operation::Write))?;

        let backup_group = datastore.backup_group(ns, backup_group);
        let owner = backup_group.get_owner()?;

        if owner_check_required {
            let allowed = match (owner.is_token(), new_owner.is_token()) {
                (true, true) => {
                    // API token to API token, owned by same user
                    let owner = owner.user();
                    let new_owner = new_owner.user();
                    owner == new_owner && Authid::from(owner.clone()) == auth_id
                }
                (true, false) => {
                    // API token to API token owner
                    Authid::from(owner.user().clone()) == auth_id && new_owner == auth_id
                }
                (false, true) => {
                    // API token owner to API token
                    owner == auth_id && Authid::from(new_owner.user().clone()) == auth_id
                }
                (false, false) => {
                    // User to User, not allowed for unprivileged users
                    false
                }
            };

            if !allowed {
                return Err(http_err!(
                    UNAUTHORIZED,
                    "{} does not have permission to change owner of backup group '{}' to {}",
                    auth_id,
                    backup_group.group(),
                    new_owner,
                ));
            }
        }

        let user_info = CachedUserInfo::new()?;

        if !user_info.is_active_auth_id(&new_owner) {
            bail!(
                "{} '{}' is inactive or non-existent",
                if new_owner.is_token() {
                    "API token".to_string()
                } else {
                    "user".to_string()
                },
                new_owner
            );
        }

        let _guard = backup_group
            .lock()
            .with_context(|| format!("while setting the owner of group '{backup_group:?}'"))?;

        if owner != backup_group.get_owner()? {
            bail!("{owner} does not own this group anymore");
        }

        backup_group.set_owner(&new_owner, true)?;

        Ok(())
    })
    .await?
}

fn setup_mounted_device(datastore: &DataStoreConfig, tmp_mount_path: &str) -> Result<(), Error> {
    let default_options = proxmox_sys::fs::CreateOptions::new();
    let mount_point = datastore.absolute_path();
    let full_store_path = format!(
        "{tmp_mount_path}/{}",
        datastore.path.trim_start_matches('/')
    );
    let backup_user = pbs_config::backup_user()?;
    let options = CreateOptions::new()
        .owner(backup_user.uid)
        .group(backup_user.gid);

    proxmox_sys::fs::create_path(&mount_point, Some(default_options), Some(options))
        .map_err(|e| format_err!("creating mountpoint '{mount_point}' failed: {e}"))?;

    // can't be created before it is mounted, so we have to do it here
    proxmox_sys::fs::create_path(&full_store_path, Some(default_options), Some(options))
        .map_err(|e| format_err!("creating datastore path '{full_store_path}' failed: {e}"))?;

    info!(
        "bind mount '{}'({}) to '{}'",
        datastore.name, datastore.path, mount_point
    );

    crate::tools::disks::bind_mount(Path::new(&full_store_path), Path::new(&mount_point))
}

/// Here we
///
/// 1. mount the removable device to `<PBS_RUN_DIR>/mount/<RANDOM_UUID>`
/// 2. bind mount `<PBS_RUN_DIR>/mount/<RANDOM_UUID>/<datastore.path>` to `/mnt/datastore/<datastore.name>`
/// 3. unmount `<PBS_RUN_DIR>/mount/<RANDOM_UUID>`
///
/// leaving us with the datastore being mounted directly with its name under /mnt/datastore/...
///
/// The reason for the randomized device mounting paths is to avoid two tasks trying to mount to
/// the same path, this is *very* unlikely since the device is only mounted really shortly, but
/// technically possible.
pub fn do_mount_device(datastore: DataStoreConfig) -> Result<bool, Error> {
    if let Some(uuid) = datastore.backing_device.as_ref() {
        if pbs_datastore::get_datastore_mount_status(&datastore) == Some(true) {
            info!(
                "device is already mounted at '{}'",
                datastore.absolute_path()
            );
            return Ok(false);
        }
        let tmp_mount_path = format!(
            "{}/{:x}",
            pbs_buildcfg::rundir!("/mount"),
            proxmox_uuid::Uuid::generate()
        );

        let default_options = proxmox_sys::fs::CreateOptions::new();
        proxmox_sys::fs::create_path(
            &tmp_mount_path,
            Some(default_options),
            Some(default_options),
        )?;

        info!("temporarily mounting '{uuid}' to '{}'", tmp_mount_path);
        crate::tools::disks::mount_by_uuid(uuid, Path::new(&tmp_mount_path))
            .map_err(|e| format_err!("mounting to tmp path failed: {e}"))?;

        let setup_result = setup_mounted_device(&datastore, &tmp_mount_path);

        let mut unmounted = true;
        if let Err(e) = crate::tools::disks::unmount_by_mountpoint(Path::new(&tmp_mount_path)) {
            unmounted = false;
            warn!("unmounting from tmp path '{tmp_mount_path} failed: {e}'");
        }
        if unmounted {
            if let Err(e) = std::fs::remove_dir(std::path::Path::new(&tmp_mount_path)) {
                warn!("removing tmp path '{tmp_mount_path} failed: {e}'");
            }
        }

        setup_result.map_err(|e| {
            format_err!(
                "Datastore '{}' could not be created: {}.",
                datastore.name,
                e
            )
        })?;
    } else {
        bail!(
            "Datastore '{}' cannot be mounted because it is not removable.",
            datastore.name
        )
    }
    Ok(true)
}

async fn do_sync_jobs(
    jobs_to_run: Vec<SyncJobConfig>,
    worker: Arc<WorkerTask>,
) -> Result<(), Error> {
    let count = jobs_to_run.len();
    info!(
        "will run {count} sync jobs: {}",
        jobs_to_run
            .iter()
            .map(|job| job.id.as_str())
            .collect::<Vec<&str>>()
            .join(", ")
    );

    let client = crate::client_helpers::connect_to_localhost()
        .context("Failed to connect to localhost for starting sync jobs")?;
    for (i, job_config) in jobs_to_run.into_iter().enumerate() {
        if worker.abort_requested() {
            bail!("aborted due to user request");
        }
        let job_id = &job_config.id;
        info!("[{}/{count}] starting '{job_id}'...", i + 1);
        let result = match client
            .post(format!("api2/json/admin/sync/{job_id}/run").as_str(), None)
            .await
        {
            Ok(result) => result,
            Err(err) => {
                warn!("unable to start sync job {job_id}: {err}");
                continue;
            }
        };
        let Some(upid_str) = result["data"].as_str() else {
            warn!(
                "could not receive UPID of started job (may be running, just can't track it here)"
            );
            continue;
        };
        let upid: UPID = upid_str.parse()?;

        let sleep_duration = core::time::Duration::from_secs(1);
        let mut status_retries = 1;
        loop {
            if worker.abort_requested() {
                bail!("aborted due to user request, already started job will finish");
            }
            match worker_is_active(&upid).await {
                Ok(true) => tokio::time::sleep(sleep_duration).await,
                Ok(false) => break,
                Err(_) if status_retries > 3 => break,
                Err(err) => {
                    warn!("could not get job status: {err} ({status_retries}/3)");
                    status_retries += 1;
                }
            }
        }
    }
    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
            },
        }
    },
    returns: {
        schema: UPID_SCHEMA,
    },
    access: {
        permission: &Permission::And(&[
            &Permission::Privilege(&["datastore", "{store}"], PRIV_DATASTORE_MODIFY, false),
            &Permission::Privilege(&["system", "disks"], PRIV_SYS_MODIFY, false)
        ]),
    },
)]
/// Mount removable datastore.
pub fn mount(store: String, rpcenv: &mut dyn RpcEnvironment) -> Result<Value, Error> {
    let (section_config, _digest) = pbs_config::datastore::config()?;
    let datastore: DataStoreConfig = section_config.lookup("datastore", &store)?;

    if datastore.backing_device.is_none() {
        bail!("datastore '{store}' is not removable");
    }

    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let to_stdout = rpcenv.env_type() == RpcEnvironmentType::CLI;

    let upid = WorkerTask::spawn(
        "mount-device",
        Some(store.clone()),
        auth_id.to_string(),
        to_stdout,
        move |_worker| async move {
            let name = datastore.name.clone();
            let log_context = LogContext::current();
            if !tokio::task::spawn_blocking(|| {
                if let Some(log_context) = log_context {
                    log_context.sync_scope(|| do_mount_device(datastore))
                } else {
                    do_mount_device(datastore)
                }
            })
            .await??
            {
                return Ok(());
            }
            let Ok((sync_config, _digest)) = pbs_config::sync::config() else {
                warn!("unable to read sync job config, won't run any sync jobs");
                return Ok(());
            };
            let Ok(list) = sync_config.convert_to_typed_array("sync") else {
                warn!("unable to parse sync job config, won't run any sync jobs");
                return Ok(());
            };
            let mut jobs_to_run: Vec<SyncJobConfig> = list
                .into_iter()
                .filter(|job: &SyncJobConfig| {
                    // add job iff (running on mount is enabled and) any of these apply
                    //   - the jobs is local and we are source or target
                    //   - we are the source of a push to a remote
                    //   - we are the target of a pull from a remote
                    //
                    // `job.store == datastore.name` iff we are the target for pull from remote or we
                    // are the source for push to remote, therefore we don't have to check for the
                    // direction of the job.
                    job.run_on_mount.unwrap_or(false)
                        && (job.remote.is_none() && job.remote_store == name || job.store == name)
                })
                .collect();
            jobs_to_run.sort_by(|j1, j2| j1.id.cmp(&j2.id));
            if !jobs_to_run.is_empty() {
                info!("starting {} sync jobs", jobs_to_run.len());
                let _ = WorkerTask::spawn(
                    "mount-sync-jobs",
                    Some(store),
                    auth_id.to_string(),
                    false,
                    move |worker| async move { do_sync_jobs(jobs_to_run, worker).await },
                );
            }
            Ok(())
        },
    )?;

    Ok(json!(upid))
}

fn expect_maintanance_unmounting(
    store: &str,
) -> Result<(pbs_config::BackupLockGuard, DataStoreConfig), Error> {
    let lock = pbs_config::datastore::lock_config()?;
    let (section_config, _digest) = pbs_config::datastore::config()?;
    let store_config: DataStoreConfig = section_config.lookup("datastore", store)?;

    if store_config
        .get_maintenance_mode()
        .is_none_or(|m| m.ty != MaintenanceType::Unmount)
    {
        bail!("maintenance mode is not 'Unmount'");
    }

    Ok((lock, store_config))
}

fn unset_maintenance(
    _lock: pbs_config::BackupLockGuard,
    mut config: DataStoreConfig,
) -> Result<(), Error> {
    let (mut section_config, _digest) = pbs_config::datastore::config()?;
    config.maintenance_mode = None;
    section_config.set_data(&config.name, "datastore", &config)?;
    pbs_config::datastore::save_config(&section_config)?;
    Ok(())
}

fn do_unmount_device(
    datastore: DataStoreConfig,
    worker: Option<&dyn WorkerTaskContext>,
) -> Result<(), Error> {
    if datastore.backing_device.is_none() {
        bail!("can't unmount non-removable datastore");
    }
    let mount_point = datastore.absolute_path();

    let mut active_operations = task_tracking::get_active_operations(&datastore.name)?;
    let mut old_status = String::new();
    let mut aborted = false;
    while active_operations.read + active_operations.write > 0 {
        if let Some(worker) = worker {
            if worker.abort_requested() || expect_maintanance_unmounting(&datastore.name).is_err() {
                aborted = true;
                break;
            }
            let status = format!(
                "cannot unmount yet, still {} read and {} write operations active",
                active_operations.read, active_operations.write
            );
            if status != old_status {
                info!("{status}");
                old_status = status;
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
        active_operations = task_tracking::get_active_operations(&datastore.name)?;
    }

    if aborted || worker.is_some_and(|w| w.abort_requested()) {
        let _ = expect_maintanance_unmounting(&datastore.name)
            .inspect_err(|e| warn!("maintenance mode was not as expected: {e}"))
            .and_then(|(lock, config)| {
                unset_maintenance(lock, config)
                    .inspect_err(|e| warn!("could not reset maintenance mode: {e}"))
            });
        bail!("aborted, due to user request");
    } else {
        let (lock, config) = expect_maintanance_unmounting(&datastore.name)?;
        crate::tools::disks::unmount_by_mountpoint(Path::new(&mount_point))?;
        unset_maintenance(lock, config)
            .map_err(|e| format_err!("could not reset maintenance mode: {e}"))?;
    }
    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            store: { schema: DATASTORE_SCHEMA },
        },
    },
    returns: {
        schema: UPID_SCHEMA,
    },
    access: {
        permission: &Permission::And(&[
            &Permission::Privilege(&["datastore", "{store}"], PRIV_DATASTORE_MODIFY, true),
            &Permission::Privilege(&["system", "disks"], PRIV_SYS_MODIFY, false)
        ]),
    }
)]
/// Unmount a removable device that is associated with the datastore
pub async fn unmount(store: String, rpcenv: &mut dyn RpcEnvironment) -> Result<Value, Error> {
    let _lock = pbs_config::datastore::lock_config()?;
    let (mut section_config, _digest) = pbs_config::datastore::config()?;
    let mut datastore: DataStoreConfig = section_config.lookup("datastore", &store)?;

    if datastore.backing_device.is_none() {
        bail!("datastore '{store}' is not removable");
    }

    ensure_datastore_is_mounted(&datastore)?;

    datastore.set_maintenance_mode(Some(MaintenanceMode {
        ty: MaintenanceType::Unmount,
        message: None,
    }))?;
    section_config.set_data(&store, "datastore", &datastore)?;
    pbs_config::datastore::save_config(&section_config)?;

    drop(_lock);

    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let to_stdout = rpcenv.env_type() == RpcEnvironmentType::CLI;

    if let Ok(proxy_pid) = proxmox_rest_server::read_pid(pbs_buildcfg::PROXMOX_BACKUP_PROXY_PID_FN)
    {
        let sock = proxmox_daemon::command_socket::path_from_pid(proxy_pid);
        let _ = proxmox_daemon::command_socket::send_raw(
            sock,
            &format!(
                "{{\"command\":\"update-datastore-cache\",\"args\":\"{}\"}}\n",
                &store
            ),
        )
        .await;
    }

    let upid = WorkerTask::new_thread(
        "unmount-device",
        Some(store),
        auth_id.to_string(),
        to_stdout,
        move |worker| do_unmount_device(datastore, Some(&worker)),
    )?;

    Ok(json!(upid))
}

#[api(
    protected: true,
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
            },
        }
    },
    returns: {
        schema: UPID_SCHEMA,
    },
    access: {
        permission: &Permission::Privilege(&["datastore", "{store}"], PRIV_DATASTORE_MODIFY, false),
    },
)]
/// Refresh datastore contents from S3 to local cache store.
pub async fn s3_refresh(store: String, rpcenv: &mut dyn RpcEnvironment) -> Result<Value, Error> {
    let datastore = DataStore::lookup_datastore(&store, Some(Operation::Lookup))?;
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let to_stdout = rpcenv.env_type() == RpcEnvironmentType::CLI;

    let upid = WorkerTask::spawn(
        "s3-refresh",
        Some(store),
        auth_id.to_string(),
        to_stdout,
        move |_worker| async move { datastore.s3_refresh().await },
    )?;

    Ok(json!(upid))
}

#[sortable]
const DATASTORE_INFO_SUBDIRS: SubdirMap = &[
    (
        "active-operations",
        &Router::new().get(&API_METHOD_GET_ACTIVE_OPERATIONS),
    ),
    ("catalog", &Router::new().get(&API_METHOD_CATALOG)),
    (
        "change-owner",
        &Router::new().post(&API_METHOD_SET_BACKUP_OWNER),
    ),
    (
        "download",
        &Router::new().download(&API_METHOD_DOWNLOAD_FILE),
    ),
    (
        "download-decoded",
        &Router::new().download(&API_METHOD_DOWNLOAD_FILE_DECODED),
    ),
    ("files", &Router::new().get(&API_METHOD_LIST_SNAPSHOT_FILES)),
    (
        "gc",
        &Router::new()
            .get(&API_METHOD_GARBAGE_COLLECTION_STATUS)
            .post(&API_METHOD_START_GARBAGE_COLLECTION),
    ),
    (
        "group-notes",
        &Router::new()
            .get(&API_METHOD_GET_GROUP_NOTES)
            .put(&API_METHOD_SET_GROUP_NOTES),
    ),
    (
        "groups",
        &Router::new()
            .get(&API_METHOD_LIST_GROUPS)
            .delete(&API_METHOD_DELETE_GROUP),
    ),
    ("mount", &Router::new().post(&API_METHOD_MOUNT)),
    (
        "namespace",
        // FIXME: move into datastore:: sub-module?!
        &crate::api2::admin::namespace::ROUTER,
    ),
    (
        "notes",
        &Router::new()
            .get(&API_METHOD_GET_NOTES)
            .put(&API_METHOD_SET_NOTES),
    ),
    (
        "protected",
        &Router::new()
            .get(&API_METHOD_GET_PROTECTION)
            .put(&API_METHOD_SET_PROTECTION),
    ),
    ("prune", &Router::new().post(&API_METHOD_PRUNE)),
    (
        "prune-datastore",
        &Router::new().post(&API_METHOD_PRUNE_DATASTORE),
    ),
    (
        "pxar-file-download",
        &Router::new().download(&API_METHOD_PXAR_FILE_DOWNLOAD),
    ),
    ("rrd", &Router::new().get(&API_METHOD_GET_RRD_STATS)),
    ("s3-refresh", &Router::new().put(&API_METHOD_S3_REFRESH)),
    (
        "snapshots",
        &Router::new()
            .get(&API_METHOD_LIST_SNAPSHOTS)
            .delete(&API_METHOD_DELETE_SNAPSHOT),
    ),
    ("status", &Router::new().get(&API_METHOD_STATUS)),
    ("unmount", &Router::new().post(&API_METHOD_UNMOUNT)),
    (
        "upload-backup-log",
        &Router::new().upload(&API_METHOD_UPLOAD_BACKUP_LOG),
    ),
    ("verify", &Router::new().post(&API_METHOD_VERIFY)),
];

const DATASTORE_INFO_ROUTER: Router = Router::new()
    .get(&list_subdirs_api_method!(DATASTORE_INFO_SUBDIRS))
    .subdirs(DATASTORE_INFO_SUBDIRS);

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_GET_DATASTORE_LIST)
    .match_all("store", &DATASTORE_INFO_ROUTER);
