use ::serde::{Deserialize, Serialize};
use anyhow::{bail, Context, Error};
use hex::FromHex;
use serde_json::Value;

use proxmox_router::{http_bail, Permission, Router, RpcEnvironment};
use proxmox_s3_client::{
    S3BucketListItem, S3Client, S3ClientConf, S3ClientConfig, S3ClientConfigUpdater,
    S3ClientConfigWithoutSecret, S3ClientOptions, S3_CLIENT_ID_SCHEMA,
};
use proxmox_schema::{api, param_bail, ApiType};

use pbs_api_types::{
    Authid, DataStoreConfig, DatastoreBackendConfig, DatastoreBackendType, JOB_ID_SCHEMA,
    PRIV_SYS_AUDIT, PRIV_SYS_MODIFY, PROXMOX_CONFIG_DIGEST_SCHEMA,
};
use pbs_config::s3::{self, S3_CFG_TYPE_ID};
use pbs_config::CachedUserInfo;

#[api(
    input: {
        properties: {},
    },
    returns: {
        description: "List configured s3 clients.",
        type: Array,
        items: { type: S3ClientConfigWithoutSecret },
    },
    access: {
        permission: &Permission::Anybody,
        description: "List configured s3 endpoints filtered by Sys.Audit privileges",
    },
)]
/// List all s3 client configurations.
pub fn list_s3_client_config(
    _param: Value,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<S3ClientConfigWithoutSecret>, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let user_info = CachedUserInfo::new()?;

    let (config, digest) = s3::config()?;
    let list: Vec<S3ClientConfigWithoutSecret> = config.convert_to_typed_array(S3_CFG_TYPE_ID)?;

    let list = list
        .into_iter()
        .filter(|endpoint| {
            let privs = user_info.lookup_privs(&auth_id, &["system", "s3-endpoint", &endpoint.id]);
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
                schema: S3_CLIENT_ID_SCHEMA,
            },
            config: {
                type: S3ClientConfig,
                flatten: true,
            },
            "secret-key": {
                type: String,
                description: "S3 secret key",
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "s3-endpoint"], PRIV_SYS_MODIFY, false),
    },
)]
/// Create a new s3 client configuration.
pub fn create_s3_client_config(
    id: String,
    config: S3ClientConfig,
    secret_key: String,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let _lock = s3::lock_config()?;
    let (mut section_config, _digest) = s3::config()?;
    if section_config.sections.contains_key(&id) {
        param_bail!("id", "s3 client config '{}' already exists.", &id);
    }

    let config = S3ClientConf {
        id: id.clone(),
        config,
        secret_key,
    };

    section_config.set_data(&config.id, S3_CFG_TYPE_ID, &config)?;
    s3::save_config(&section_config)?;

    Ok(())
}

#[api(
    input: {
        properties: {
            id: {
                schema: JOB_ID_SCHEMA,
            },
        },
    },
    returns: { type: S3ClientConfigWithoutSecret },
    access: {
        permission: &Permission::Privilege(&["system", "s3-endpoint", "{id}"], PRIV_SYS_AUDIT, false),
    },
)]
/// Read an s3 client configuration.
pub fn read_s3_client_config(
    id: String,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<S3ClientConfigWithoutSecret, Error> {
    let (config, digest) = s3::config()?;
    let s3_client_config: S3ClientConfigWithoutSecret = config.lookup(S3_CFG_TYPE_ID, &id)?;
    rpcenv["digest"] = hex::encode(digest).into();

    Ok(s3_client_config)
}

#[api()]
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Deletable property name
pub enum DeletableProperty {
    /// Delete the port property.
    Port,
    /// Delete the region property.
    Region,
    /// Delete the fingerprint property.
    Fingerprint,
    /// Delete the path-style property.
    PathStyle,
    /// Delete the rate-in property.
    RateIn,
    /// Delete the burst-in property.
    BurstIn,
    /// Delete the rate-out property.
    RateOut,
    /// Delete the burst-out property.
    BurstOut,
    /// Delete the provider quirks property.
    ProviderQuirks,
}

#[api(
    protected: true,
    input: {
        properties: {
            id: {
                schema: JOB_ID_SCHEMA,
            },
            update: {
                type: S3ClientConfigUpdater,
                flatten: true,
            },
            "secret-key": {
                type: String,
                description: "S3 client secret key.",
                optional: true,
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
    access: {
        permission: &Permission::Privilege(&["system", "s3-endpoint", "{id}"], PRIV_SYS_MODIFY, false),
    },
)]
/// Update an s3 client configuration.
#[allow(clippy::too_many_arguments)]
pub fn update_s3_client_config(
    id: String,
    update: S3ClientConfigUpdater,
    secret_key: Option<String>,
    delete: Option<Vec<DeletableProperty>>,
    digest: Option<String>,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let _lock = s3::lock_config()?;
    let (mut config, expected_digest) = s3::config()?;

    // Secrets are not included in digest concurrent changes therefore not detected.
    if let Some(ref digest) = digest {
        let digest = <[u8; 32]>::from_hex(digest)?;
        crate::tools::detect_modified_configuration_file(&digest, &expected_digest)?;
    }

    let mut data: S3ClientConf = config.lookup(S3_CFG_TYPE_ID, &id)?;

    if let Some(delete) = delete {
        for delete_prop in delete {
            match delete_prop {
                DeletableProperty::Port => {
                    data.config.port = None;
                }
                DeletableProperty::Region => {
                    data.config.region = None;
                }
                DeletableProperty::Fingerprint => {
                    data.config.fingerprint = None;
                }
                DeletableProperty::PathStyle => {
                    data.config.path_style = None;
                }
                DeletableProperty::RateIn => {
                    data.config.rate_in = None;
                }
                DeletableProperty::BurstIn => {
                    data.config.burst_in = None;
                }
                DeletableProperty::RateOut => {
                    data.config.rate_out = None;
                }
                DeletableProperty::BurstOut => {
                    data.config.burst_out = None;
                }
                DeletableProperty::ProviderQuirks => {
                    data.config.provider_quirks = None;
                }
            }
        }
    }

    if let Some(endpoint) = update.endpoint {
        data.config.endpoint = endpoint;
    }
    if let Some(port) = update.port {
        data.config.port = Some(port);
    }
    if let Some(region) = update.region {
        data.config.region = Some(region);
    }
    if let Some(access_key) = update.access_key {
        data.config.access_key = access_key;
    }
    if let Some(fingerprint) = update.fingerprint {
        data.config.fingerprint = Some(fingerprint);
    }
    if let Some(path_style) = update.path_style {
        data.config.path_style = Some(path_style);
    }
    if let Some(rate_in) = update.rate_in {
        data.config.rate_in = Some(rate_in);
    }
    if let Some(burst_in) = update.burst_in {
        data.config.burst_in = Some(burst_in);
    }
    if let Some(rate_out) = update.rate_out {
        data.config.rate_out = Some(rate_out);
    }
    if let Some(burst_out) = update.burst_out {
        data.config.burst_out = Some(burst_out);
    }
    if let Some(provider_quirks) = update.provider_quirks {
        data.config.provider_quirks = Some(provider_quirks);
    }

    if let Some(secret_key) = secret_key {
        data.secret_key = secret_key;
    }

    config.set_data(&id, S3_CFG_TYPE_ID, &data)?;
    s3::save_config(&config)?;

    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            id: {
                schema: JOB_ID_SCHEMA,
            },
            digest: {
                optional: true,
                schema: PROXMOX_CONFIG_DIGEST_SCHEMA,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["system", "s3-endpoint", "{id}"], PRIV_SYS_MODIFY, false),
    },
)]
/// Remove an s3 client configuration.
pub fn delete_s3_client_config(
    id: String,
    digest: Option<String>,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let _lock = s3::lock_config()?;
    let (mut config, expected_digest) = s3::config()?;

    if let Some(ref digest) = digest {
        let digest = <[u8; 32]>::from_hex(digest)?;
        crate::tools::detect_modified_configuration_file(&digest, &expected_digest)?;
    }

    if let Some(datastore) =
        s3_client_in_use(&id).context("failed to check if s3 client is in-use")?
    {
        bail!("in-use by datastore {datastore}");
    }

    if config.sections.remove(&id).is_none() {
        http_bail!(NOT_FOUND, "s3 client config '{id}' do not exist.")
    }
    s3::save_config(&config)
}

#[api(
    input: {
        properties: {
            id: {
                schema: S3_CLIENT_ID_SCHEMA,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&[], PRIV_SYS_AUDIT, false),
    },
)]
/// List buckets accessible by given s3 client configuration
pub async fn list_buckets(
    id: String,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<S3BucketListItem>, Error> {
    let (config, _digest) = pbs_config::s3::config()?;
    let config: S3ClientConf = config
        .lookup(S3_CFG_TYPE_ID, &id)
        .context("config lookup failed")?;

    let empty_prefix = String::new();
    let options =
        S3ClientOptions::from_config(config.config, config.secret_key, None, empty_prefix, None);
    let client = S3Client::new(options).context("client creation failed")?;
    let list_buckets_response = client
        .list_buckets()
        .await
        .context("failed to list buckets")?;
    let buckets = list_buckets_response
        .buckets
        .into_iter()
        .map(|bucket| S3BucketListItem { name: bucket.name })
        .collect();

    Ok(buckets)
}

// Check if the configured s3 client is still in-use by a datastore backend.
//
// If so, return the first datastore name with the configured client.
fn s3_client_in_use(id: &str) -> Result<Option<String>, Error> {
    let (config, _digest) = pbs_config::datastore::config()?;
    let list: Vec<DataStoreConfig> = config.convert_to_typed_array("datastore")?;
    for datastore in list {
        let backend_config: DatastoreBackendConfig = serde_json::from_value(
            DatastoreBackendConfig::API_SCHEMA
                .parse_property_string(datastore.backend.as_deref().unwrap_or(""))?,
        )?;
        match (backend_config.ty, backend_config.client) {
            (Some(DatastoreBackendType::S3), Some(client)) if client == id => {
                return Ok(Some(datastore.name.to_owned()))
            }
            _ => (),
        }
    }
    Ok(None)
}

const LIST_BUCKETS_ROUTER: Router = Router::new().get(&API_METHOD_LIST_BUCKETS);

const ITEM_ROUTER: Router = Router::new()
    .get(&API_METHOD_READ_S3_CLIENT_CONFIG)
    .put(&API_METHOD_UPDATE_S3_CLIENT_CONFIG)
    .delete(&API_METHOD_DELETE_S3_CLIENT_CONFIG)
    .subdirs(&[("list-buckets", &LIST_BUCKETS_ROUTER)]);

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_LIST_S3_CLIENT_CONFIG)
    .post(&API_METHOD_CREATE_S3_CLIENT_CONFIG)
    .match_all("id", &ITEM_ROUTER);
