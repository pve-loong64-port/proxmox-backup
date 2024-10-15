use anyhow::Error;
use pbs_api_types::{Authid, MetricDataPoint, Metrics, PRIV_DATASTORE_AUDIT, PRIV_SYS_AUDIT};
use pbs_config::CachedUserInfo;
use proxmox_router::{Permission, Router, RpcEnvironment};
use proxmox_schema::api;

use crate::server::metric_collection::pull_metrics;

pub const ROUTER: Router = Router::new().get(&API_METHOD_GET_METRICS);

#[api(
    input: {
        properties: {
            "start-time": {
                optional: true,
                default: 0,
                description: "Only return values with a timestamp > start-time. Only has an effect if 'history' is also set",
            },
            "history": {
                optional: true,
                default: false,
                description: "Include historic values (last 30 minutes)",
            }
        },
    },
    access: {
        description: "Users need Sys.Audit on /system/status for host metrics and Datastore.Audit on /datastore/{store} for datastore metrics",
        permission: &Permission::Anybody,
    },
)]
/// Return backup server metrics.
pub fn get_metrics(
    start_time: i64,
    history: bool,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Metrics, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let user_info = CachedUserInfo::new()?;

    let has_any_datastore_audit_privs =
        user_info.any_privs_below(&auth_id, &["datastore"], PRIV_DATASTORE_AUDIT)?;

    let has_host_audit_privs =
        (CachedUserInfo::lookup_privs(&user_info, &auth_id, &["system", "status"])
            & PRIV_SYS_AUDIT)
            != 0;

    if !has_any_datastore_audit_privs && !has_host_audit_privs {
        // The `pull_metrics::get_*` calls are expensive, so
        // check early if the current user has sufficient privileges to read *any*
        // metric data.
        // For datastores, we do not yet know for which individual datastores
        // we have metrics in the cache, so we just check if we have
        // audit permissions for *any* datastore and filter after
        // reading the data.
        return Ok(Metrics { data: Vec::new() });
    }

    let metrics = if history {
        pull_metrics::get_all_metrics(start_time)?
    } else {
        pull_metrics::get_most_recent_metrics()?
    };

    let filter_by_privs = |point: &MetricDataPoint| {
        let id = point.id.as_str();
        if id == "host" {
            return has_host_audit_privs;
        } else if let Some(datastore_id) = id.strip_prefix("datastore/") {
            if !datastore_id.contains('/') {
                // Now, check whether we have permissions for the individual datastore
                let user_privs = CachedUserInfo::lookup_privs(
                    &user_info,
                    &auth_id,
                    &["datastore", datastore_id],
                );
                return (user_privs & PRIV_DATASTORE_AUDIT) != 0;
            }
        }
        log::error!("invalid metric object id: {id:?}");
        false
    };

    Ok(Metrics {
        data: metrics.into_iter().filter(filter_by_privs).collect(),
    })
}
