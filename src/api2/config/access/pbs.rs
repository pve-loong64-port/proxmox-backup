use ::serde::{Deserialize, Serialize};
use anyhow::Error;
use hex::FromHex;

use proxmox_router::{Permission, Router, RpcEnvironment};
use proxmox_schema::api;

use pbs_api_types::{
    PbsRealmConfig, PbsRealmConfigUpdater, PRIV_REALM_ALLOCATE, PRIV_SYS_AUDIT,
    PROXMOX_CONFIG_DIGEST_SCHEMA,
};

use pbs_config::domains;

#[api(
    returns: {
        type: PbsRealmConfig,
    },
    access: {
        permission: &Permission::Privilege(&["access", "domains"], PRIV_SYS_AUDIT, false),
    },
)]
/// Read the Proxmox Backup authentication server realm configuration
pub fn read_pbs_realm(rpcenv: &mut dyn RpcEnvironment) -> Result<PbsRealmConfig, Error> {
    let (domains, digest) = domains::config()?;

    let config = domains.lookup("pbs", "pbs")?;

    rpcenv["digest"] = hex::encode(digest).into();

    Ok(config)
}

#[api]
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Deletable property name
pub enum DeletableProperty {
    /// Delete the comment property.
    Comment,
    /// Delete the default property.
    Default,
}

#[api(
    protected: true,
    input: {
        properties: {
            update: {
                type: PbsRealmConfigUpdater,
                flatten: true,
            },
            delete: {
                description: "List of properties to delete.",
                type: Array,
                optional: true,
                items: {
                    type: DeletableProperty,
                }
            },
            digest: {
                optional: true,
                schema: PROXMOX_CONFIG_DIGEST_SCHEMA,
            },
        },
    },
    returns: {
        type: PbsRealmConfig,
    },
    access: {
        permission: &Permission::Privilege(&["access", "domains"], PRIV_REALM_ALLOCATE, false),
    },
)]
/// Update the Proxmox Backup authentication server realm configuration
pub fn update_pbs_realm(
    update: PbsRealmConfigUpdater,
    delete: Option<Vec<DeletableProperty>>,
    digest: Option<String>,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let _lock = domains::lock_config()?;

    let (mut domains, expected_digest) = domains::config()?;

    if let Some(ref digest) = digest {
        let digest = <[u8; 32]>::from_hex(digest)?;
        crate::tools::detect_modified_configuration_file(&digest, &expected_digest)?;
    }

    let mut config: PbsRealmConfig = domains.lookup("pbs", "pbs")?;

    if let Some(delete) = delete {
        for delete_prop in delete {
            match delete_prop {
                DeletableProperty::Comment => {
                    config.comment = None;
                }
                DeletableProperty::Default => {
                    config.default = None;
                }
            }
        }
    }

    if let Some(comment) = update.comment {
        let comment = comment.trim().to_string();
        if comment.is_empty() {
            config.comment = None;
        } else {
            config.comment = Some(comment);
        }
    }

    if let Some(true) = update.default {
        pbs_config::domains::unset_default_realm(&mut domains)?;
        config.default = Some(true);
    } else {
        config.default = None;
    }

    domains.set_data("pbs", "pbs", &config)?;

    domains::save_config(&domains)?;

    Ok(())
}

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_READ_PBS_REALM)
    .put(&API_METHOD_UPDATE_PBS_REALM);
