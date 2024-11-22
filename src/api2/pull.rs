//! Sync datastore from remote server
use anyhow::{bail, format_err, Error};
use futures::{future::FutureExt, select};
use tracing::info;

use proxmox_router::{Permission, Router, RpcEnvironment};
use proxmox_schema::api;

use pbs_api_types::{
    Authid, BackupNamespace, GroupFilter, RateLimitConfig, SyncJobConfig, DATASTORE_SCHEMA,
    GROUP_FILTER_LIST_SCHEMA, NS_MAX_DEPTH_REDUCED_SCHEMA, PRIV_DATASTORE_BACKUP,
    PRIV_DATASTORE_PRUNE, PRIV_REMOTE_READ, REMOTE_ID_SCHEMA, REMOVE_VANISHED_BACKUPS_SCHEMA,
    RESYNC_CORRUPT_SCHEMA, TRANSFER_LAST_SCHEMA,
};
use pbs_config::CachedUserInfo;
use proxmox_rest_server::WorkerTask;

use crate::server::pull::{pull_store, PullParameters};

pub fn check_pull_privs(
    auth_id: &Authid,
    store: &str,
    ns: Option<&str>,
    remote: Option<&str>,
    remote_store: &str,
    delete: bool,
) -> Result<(), Error> {
    let user_info = CachedUserInfo::new()?;

    let local_store_ns_acl_path = match ns {
        Some(ns) => vec!["datastore", store, ns],
        None => vec!["datastore", store],
    };

    user_info.check_privs(
        auth_id,
        &local_store_ns_acl_path,
        PRIV_DATASTORE_BACKUP,
        false,
    )?;

    if let Some(remote) = remote {
        user_info.check_privs(
            auth_id,
            &["remote", remote, remote_store],
            PRIV_REMOTE_READ,
            false,
        )?;
    } else {
        user_info.check_privs(
            auth_id,
            &["datastore", remote_store],
            PRIV_DATASTORE_BACKUP,
            false,
        )?;
    }

    if delete {
        user_info.check_privs(
            auth_id,
            &local_store_ns_acl_path,
            PRIV_DATASTORE_PRUNE,
            false,
        )?;
    }

    Ok(())
}

impl TryFrom<&SyncJobConfig> for PullParameters {
    type Error = Error;

    fn try_from(sync_job: &SyncJobConfig) -> Result<Self, Self::Error> {
        PullParameters::new(
            &sync_job.store,
            sync_job.ns.clone().unwrap_or_default(),
            sync_job.remote.as_deref(),
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
            sync_job.resync_corrupt,
        )
    }
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
            remote: {
                schema: REMOTE_ID_SCHEMA,
                optional: true,
            },
            "remote-store": {
                schema: DATASTORE_SCHEMA,
            },
            "remote-ns": {
                type: BackupNamespace,
                optional: true,
            },
            "remove-vanished": {
                schema: REMOVE_VANISHED_BACKUPS_SCHEMA,
                optional: true,
            },
            "max-depth": {
                schema: NS_MAX_DEPTH_REDUCED_SCHEMA,
                optional: true,
            },
            "group-filter": {
                schema: GROUP_FILTER_LIST_SCHEMA,
                optional: true,
            },
            limit: {
                type: RateLimitConfig,
                flatten: true,
            },
            "transfer-last": {
                schema: TRANSFER_LAST_SCHEMA,
                optional: true,
            },
            "resync-corrupt": {
                schema: RESYNC_CORRUPT_SCHEMA,
                optional: true,
            },
        },
    },
    access: {
        // Note: used parameters are no uri parameters, so we need to test inside function body
        description: r###"The user needs Datastore.Backup privilege on '/datastore/{store}',
and needs to own the backup group. Remote.Read is required on '/remote/{remote}/{remote-store}'.
The delete flag additionally requires the Datastore.Prune privilege on '/datastore/{store}'.
"###,
        permission: &Permission::Anybody,
    },
)]
/// Sync store from other repository
#[allow(clippy::too_many_arguments)]
async fn pull(
    store: String,
    ns: Option<BackupNamespace>,
    remote: Option<String>,
    remote_store: String,
    remote_ns: Option<BackupNamespace>,
    remove_vanished: Option<bool>,
    max_depth: Option<usize>,
    group_filter: Option<Vec<GroupFilter>>,
    limit: RateLimitConfig,
    transfer_last: Option<usize>,
    resync_corrupt: Option<bool>,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<String, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let delete = remove_vanished.unwrap_or(false);

    if remote.is_none() && store == remote_store {
        bail!("can't sync to same datastore");
    }

    let ns = ns.unwrap_or_default();
    let ns_str = if ns.is_root() {
        None
    } else {
        Some(ns.to_string())
    };

    check_pull_privs(
        &auth_id,
        &store,
        ns_str.as_deref(),
        remote.as_deref(),
        &remote_store,
        delete,
    )?;

    let pull_params = PullParameters::new(
        &store,
        ns,
        remote.as_deref(),
        &remote_store,
        remote_ns.unwrap_or_default(),
        auth_id.clone(),
        remove_vanished,
        max_depth,
        group_filter,
        limit,
        transfer_last,
        resync_corrupt,
    )?;

    // fixme: set to_stdout to false?
    // FIXME: add namespace to worker id?
    let upid_str = WorkerTask::spawn(
        "sync",
        Some(store.clone()),
        auth_id.to_string(),
        true,
        move |worker| async move {
            info!(
                "pull datastore '{store}' from '{}/{remote_store}'",
                remote.as_deref().unwrap_or("-"),
            );

            let pull_future = pull_store(pull_params);
            (select! {
                success = pull_future.fuse() => success,
                abort = worker.abort_future().map(|_| Err(format_err!("pull aborted"))) => abort,
            })?;

            info!("pull datastore '{store}' end");

            Ok(())
        },
    )?;

    Ok(upid_str)
}

pub const ROUTER: Router = Router::new().post(&API_METHOD_PULL);
