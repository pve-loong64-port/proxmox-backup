//! Version information

use anyhow::Error;
use serde_json::Value;

use proxmox_router::{ApiMethod, Permission, Router, RpcEnvironment};
use proxmox_schema::api;

use pbs_api_types::ApiVersionInfo;

const FEATURES: &[&str] = &["prune-delete-stats"];

#[api(
    returns: {
        type: ApiVersionInfo,
    },
    access: {
        permission: &Permission::Anybody,
    }
)]
///Proxmox Backup Server API version.
fn version(
    _param: Value,
    _info: &ApiMethod,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<ApiVersionInfo, Error> {
    Ok(ApiVersionInfo {
        version: pbs_buildcfg::PROXMOX_PKG_VERSION.to_string(),
        release: pbs_buildcfg::PROXMOX_PKG_RELEASE.to_string(),
        repoid: pbs_buildcfg::PROXMOX_PKG_REPOID.to_string(),
        features: FEATURES.iter().map(|feature| feature.to_string()).collect(),
    })
}

pub const ROUTER: Router = Router::new().get(&API_METHOD_VERSION);
