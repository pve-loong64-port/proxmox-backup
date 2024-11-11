use anyhow::{format_err, Error};
use futures::{future::FutureExt, select};

use pbs_api_types::{
    Authid, BackupNamespace, GroupFilter, RateLimitConfig, DATASTORE_SCHEMA,
    GROUP_FILTER_LIST_SCHEMA, NS_MAX_DEPTH_REDUCED_SCHEMA, PRIV_DATASTORE_BACKUP,
    PRIV_DATASTORE_READ, PRIV_REMOTE_DATASTORE_BACKUP, PRIV_REMOTE_DATASTORE_PRUNE,
    REMOTE_ID_SCHEMA, REMOVE_VANISHED_BACKUPS_SCHEMA, TRANSFER_LAST_SCHEMA,
};
use proxmox_rest_server::WorkerTask;
use proxmox_router::{Permission, Router, RpcEnvironment};
use proxmox_schema::api;

use pbs_config::CachedUserInfo;

use crate::server::push::{push_store, PushParameters};

/// Check if the provided user is allowed to read from the local source and act on the remote
/// target for pushing content
fn check_push_privs(
    auth_id: &Authid,
    store: &str,
    namespace: &BackupNamespace,
    remote: &str,
    remote_store: &str,
    remote_ns: &BackupNamespace,
    delete: bool,
) -> Result<(), Error> {
    let user_info = CachedUserInfo::new()?;

    let target_acl_path = remote_ns.remote_acl_path(remote, remote_store);

    // Check user is allowed to backup to remote/<remote>/<datastore>/<namespace>
    user_info.check_privs(
        auth_id,
        &target_acl_path,
        PRIV_REMOTE_DATASTORE_BACKUP,
        false,
    )?;

    if delete {
        // Check user is allowed to prune remote datastore
        user_info.check_privs(
            auth_id,
            &target_acl_path,
            PRIV_REMOTE_DATASTORE_PRUNE,
            false,
        )?;
    }

    // Check user is allowed to read source datastore
    user_info.check_privs(
        auth_id,
        &namespace.acl_path(store),
        PRIV_DATASTORE_READ | PRIV_DATASTORE_BACKUP,
        true,
    )?;

    Ok(())
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
        },
    },
    access: {
        description: r###"The user needs (at least) Remote.DatastoreBackup on ".
            "'/remote/{remote}/{remote-store}[/{remote-ns}]', and either Datastore.Backup or ".
            "Datastore.Read on '/datastore/{store}[/{ns}]'. The 'remove-vanished' parameter might ".
            "require additional privileges."###,
        permission: &Permission::Anybody,
    },
)]
/// Push store to other repository
#[allow(clippy::too_many_arguments)]
async fn push(
    store: String,
    ns: Option<BackupNamespace>,
    remote: String,
    remote_store: String,
    remote_ns: Option<BackupNamespace>,
    remove_vanished: Option<bool>,
    max_depth: Option<usize>,
    group_filter: Option<Vec<GroupFilter>>,
    limit: RateLimitConfig,
    transfer_last: Option<usize>,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<String, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let delete = remove_vanished.unwrap_or(false);
    let ns = ns.unwrap_or_default();
    let remote_ns = remote_ns.unwrap_or_default();

    check_push_privs(
        &auth_id,
        &store,
        &ns,
        &remote,
        &remote_store,
        &remote_ns,
        delete,
    )?;

    let push_params = PushParameters::new(
        &store,
        ns,
        &remote,
        &remote_store,
        remote_ns,
        auth_id.clone(),
        remove_vanished,
        max_depth,
        group_filter,
        limit,
        transfer_last,
    )
    .await?;

    let upid_str = WorkerTask::spawn(
        "sync",
        Some(store.clone()),
        auth_id.to_string(),
        true,
        move |worker| async move {
            let push_future = push_store(push_params);
            (select! {
                success = push_future.fuse() => success,
                abort = worker.abort_future().map(|_| Err(format_err!("push aborted"))) => abort,
            })?;
            Ok(())
        },
    )?;

    Ok(upid_str)
}

pub const ROUTER: Router = Router::new().post(&API_METHOD_PUSH);
