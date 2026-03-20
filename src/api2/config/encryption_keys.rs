use anyhow::{bail, format_err, Error};
use serde_json::Value;

use proxmox_router::{Permission, Router, RpcEnvironment};
use proxmox_schema::api;

use pbs_api_types::{
    Authid, CryptKey, SyncJobConfig, CRYPT_KEY_ID_SCHEMA, PRIV_SYS_AUDIT, PRIV_SYS_MODIFY,
    PROXMOX_CONFIG_DIGEST_SCHEMA,
};

use pbs_config::encryption_keys::{self, ENCRYPTION_KEYS_CFG_TYPE_ID};
use pbs_config::CachedUserInfo;

use pbs_key_config::KeyConfig;

#[api(
    input: {
        properties: {
            "include-archived": {
                type: bool,
                description: "List also keys which have been archived.",
                optional: true,
                default: false,
            },
        },
    },
    returns: {
        description: "List of configured encryption keys.",
        type: Array,
        items: { type: CryptKey },
    },
    access: {
        permission: &Permission::Anybody,
        description: "List configured encryption keys filtered by Sys.Audit privileges",
    },
)]
/// List configured encryption keys.
pub fn list_keys(
    include_archived: bool,
    _param: Value,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<CryptKey>, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let user_info = CachedUserInfo::new()?;

    let (config, digest) = encryption_keys::config()?;

    let list: Vec<CryptKey> = config.convert_to_typed_array(ENCRYPTION_KEYS_CFG_TYPE_ID)?;
    let list = list
        .into_iter()
        .filter(|key| {
            if !include_archived && key.archived_at.is_some() {
                return false;
            }
            let privs = user_info.lookup_privs(&auth_id, &["system", "encryption-keys", &key.id]);
            privs & PRIV_SYS_AUDIT != 0
        })
        .collect();

    rpcenv["digest"] = hex::encode(digest).into();

    Ok(list)
}

#[api(
    protected: true,
    input: {
        properties: {
            id: {
                schema: CRYPT_KEY_ID_SCHEMA,
            },
            key: {
                description: "Use provided key instead of creating new one.",
                type: String,
                optional: true,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "encryption-keys"], PRIV_SYS_MODIFY, false),
    },
)]
/// Create new encryption key instance or use the provided one.
pub fn create_key(
    id: String,
    key: Option<String>,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<KeyConfig, Error> {
    let key_config = if let Some(key) = &key {
        let key_config: KeyConfig = serde_json::from_str(key)
            .map_err(|err| format_err!("failed to parse provided key: {err}"))?;
        // early detect unusable keys
        if key_config.kdf.is_some() {
            bail!("protected keys not supported");
        }
        let _ = key_config
            .decrypt(&|| Ok(Vec::new()))
            .map_err(|err| format_err!("failed to load provided key: {err}"))?;
        key_config
    } else {
        let mut raw_key = [0u8; 32];
        proxmox_sys::linux::fill_with_random_data(&mut raw_key)?;
        KeyConfig::without_password(raw_key)?
    };

    encryption_keys::store_key(&id, &key_config)?;

    Ok(key_config)
}

#[api(
    protected: true,
    input: {
        properties: {
            id: {
                schema: CRYPT_KEY_ID_SCHEMA,
            },
            digest: {
                optional: true,
                schema: PROXMOX_CONFIG_DIGEST_SCHEMA,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "encryption-keys", "{id}"], PRIV_SYS_MODIFY, false),
    },
)]
/// Mark the key by given id as archived, no longer usable to encrypt contents.
pub fn archive_key(
    id: String,
    digest: Option<String>,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let _lock = encryption_keys::lock_config()?;
    let (mut config, expected_digest) = encryption_keys::config()?;

    pbs_config::detect_modified_configuration_file(digest, &expected_digest)?;

    let mut key: CryptKey = config.lookup(ENCRYPTION_KEYS_CFG_TYPE_ID, &id)?;

    if key.archived_at.is_some() {
        bail!("key already marked as archived");
    } else {
        check_encryption_key_in_use(&id, false)?;
    }

    key.archived_at = Some(proxmox_time::epoch_i64());

    config.set_data(&id, ENCRYPTION_KEYS_CFG_TYPE_ID, &key)?;
    // drops config lock
    encryption_keys::save_config(&config)?;

    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            id: {
                schema: CRYPT_KEY_ID_SCHEMA,
            },
            digest: {
                optional: true,
                schema: PROXMOX_CONFIG_DIGEST_SCHEMA,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "encryption-keys", "{id}"], PRIV_SYS_MODIFY, false),
    },
)]
/// Remove encryption key.
pub fn delete_key(
    id: String,
    digest: Option<String>,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let _lock = encryption_keys::lock_config()?;
    let (config, expected_digest) = encryption_keys::config()?;

    pbs_config::detect_modified_configuration_file(digest, &expected_digest)?;

    check_encryption_key_in_use(&id, true)?;

    encryption_keys::delete_key(&id, config)?;

    Ok(())
}

// Check if sync jobs hold given key as active encryption key and if flag set, if sync jobs have it
// as associated key.
fn check_encryption_key_in_use(id: &str, include_associated: bool) -> Result<(), Error> {
    let (config, _digest) = pbs_config::sync::config()?;

    let mut used_by_jobs = Vec::new();

    let job_list: Vec<SyncJobConfig> = config.convert_to_typed_array("sync")?;
    for job in job_list {
        if job.active_encryption_key.as_deref() == Some(id) {
            used_by_jobs.push(job.id.clone());
        } else if include_associated
            && job
                .associated_key
                .as_deref()
                .unwrap_or(&[])
                .contains(&id.to_string())
        {
            used_by_jobs.push(job.id.clone());
        }
    }

    if !used_by_jobs.is_empty() {
        let plural = if used_by_jobs.len() > 1 { "s" } else { "" };
        let ids = used_by_jobs.join(", ");
        bail!("encryption key in use by sync job{plural}: '{ids}'");
    }

    Ok(())
}

const ITEM_ROUTER: Router = Router::new()
    .post(&API_METHOD_ARCHIVE_KEY)
    .delete(&API_METHOD_DELETE_KEY);

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_LIST_KEYS)
    .post(&API_METHOD_CREATE_KEY)
    .match_all("id", &ITEM_ROUTER);
