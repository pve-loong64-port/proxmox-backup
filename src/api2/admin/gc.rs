use anyhow::Error;
use pbs_api_types::{Authid, GarbageCollectionJobStatus};

use proxmox_router::{ApiMethod, Permission, Router, RpcEnvironment};
use proxmox_schema::api;

use pbs_api_types::DATASTORE_SCHEMA;

use serde_json::Value;

use crate::api2::admin::datastore::{garbage_collection_status, list_datastores_checked};

#[api(
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
                optional: true,
            },
        },
    },
    returns: {
        description: "List configured gc jobs and their status",
        type: Array,
        items: { type: GarbageCollectionJobStatus },
    },
    access: {
        permission: &Permission::Anybody,
        description: "Requires Datastore.Audit or Datastore.Modify on datastore.",
    },
)]
/// List all GC jobs (max one per datastore)
pub fn list_all_gc_jobs(
    store: Option<String>,
    _param: Value,
    _info: &ApiMethod,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<GarbageCollectionJobStatus>, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;

    let gc_info = match store {
        Some(store) => garbage_collection_status(store, _info, rpcenv).map(|info| vec![info])?,
        None => list_datastores_checked(&auth_id, false)?
            .into_iter()
            .map(|store_list_item| store_list_item.store)
            .filter_map(|store| garbage_collection_status(store, _info, rpcenv).ok())
            .collect::<Vec<_>>(),
    };

    Ok(gc_info)
}

const GC_ROUTER: Router = Router::new().get(&API_METHOD_LIST_ALL_GC_JOBS);

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_LIST_ALL_GC_JOBS)
    .match_all("store", &GC_ROUTER);
