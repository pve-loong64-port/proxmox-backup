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
    let metrics = if history {
        pull_metrics::get_all_metrics(start_time)?
    } else {
        pull_metrics::get_most_recent_metrics()?
    };

    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let user_info = CachedUserInfo::new()?;

    let filter_by_privs = |point: &MetricDataPoint| {
        let elements: Vec<&str> = point.id.as_str().split('/').collect();

        match elements.as_slice() {
            ["host"] => {
                let user_privs =
                    CachedUserInfo::lookup_privs(&user_info, &auth_id, &["system", "status"]);
                (user_privs & PRIV_SYS_AUDIT) != 0
            }
            ["datastore", datastore_id] => {
                let user_privs = CachedUserInfo::lookup_privs(
                    &user_info,
                    &auth_id,
                    &["datastore", datastore_id],
                );
                (user_privs & PRIV_DATASTORE_AUDIT) != 0
            }
            _ => {
                log::error!("invalid metric object id: {}", point.id);
                false
            }
        }
    };

    Ok(Metrics {
        data: metrics.into_iter().filter(filter_by_privs).collect(),
    })
}
