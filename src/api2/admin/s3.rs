//! S3 bucket operations

use std::path::Path;
use std::sync::atomic::Ordering;

use anyhow::{bail, Context, Error};
use serde_json::Value;

use proxmox_http::Body;
use proxmox_router::{list_subdirs_api_method, Permission, Router, RpcEnvironment, SubdirMap};
use proxmox_s3_client::{
    S3Client, S3ClientConf, S3ClientOptions, S3ObjectKey, S3RequestCounterConfig,
    SharedRequestCounters, S3_BUCKET_NAME_SCHEMA, S3_CLIENT_ID_SCHEMA, S3_HTTP_REQUEST_TIMEOUT,
};
use proxmox_schema::*;
use proxmox_sortable_macro::sortable;

use pbs_api_types::PRIV_SYS_MODIFY;

use pbs_config::s3::S3_CFG_TYPE_ID;
use pbs_datastore::S3_CLIENT_REQUEST_COUNTER_BASE_PATH;

#[api(
    input: {
        properties: {
            "s3-client-id": {
                schema: S3_CLIENT_ID_SCHEMA,
            },
            bucket: {
                schema: S3_BUCKET_NAME_SCHEMA,
            },
            "store-prefix": {
                type: String,
                description: "Store prefix within bucket for S3 object keys (commonly datastore name)",
                optional: true,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&[], PRIV_SYS_MODIFY, false),
    },
)]
/// Perform basic sanity check for given s3 client configuration
pub async fn check(
    s3_client_id: String,
    bucket: String,
    store_prefix: Option<String>,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<Value, Error> {
    let (config, _digest) = pbs_config::s3::config()?;
    let config: S3ClientConf = config
        .lookup(S3_CFG_TYPE_ID, &s3_client_id)
        .context("config lookup failed")?;

    let request_counter_id = if let Some(store) = &store_prefix {
        format!("{s3_client_id}-{bucket}-{store}")
    } else {
        format!("{s3_client_id}-{bucket}")
    };
    let request_counter_config = S3RequestCounterConfig {
        id: request_counter_id,
        user: pbs_config::backup_user()?,
        base_path: S3_CLIENT_REQUEST_COUNTER_BASE_PATH.into(),
    };

    let store_prefix = store_prefix.unwrap_or_default();
    let options = S3ClientOptions::from_config(
        config.config,
        config.secret_key,
        Some(bucket),
        store_prefix,
        None,
        pbs_config::node::node_http_proxy_config()?,
        Some(request_counter_config),
    );

    let test_object_key =
        S3ObjectKey::try_from(".s3-client-test").context("failed to generate s3 object key")?;
    let client = S3Client::new(options).context("client creation failed")?;
    client.head_bucket().await.context("head object failed")?;
    client
        .put_object(
            test_object_key.clone(),
            Body::empty(),
            Some(S3_HTTP_REQUEST_TIMEOUT),
            true,
        )
        .await
        .context("put object failed")?;
    client
        .get_object(test_object_key.clone())
        .await
        .context("get object failed")?;
    client
        .delete_object(test_object_key.clone())
        .await
        .context("delete object failed")?;

    Ok(Value::Null)
}

#[api(
    input: {
        properties: {
            "s3-client-id": {
                schema: S3_CLIENT_ID_SCHEMA,
            },
            bucket: {
                schema: S3_BUCKET_NAME_SCHEMA,
            },
            "store-prefix": {
                type: String,
                description: "Store prefix within bucket for S3 object keys (commonly datastore name)",
                optional: true,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&[], PRIV_SYS_MODIFY, false),
    },
)]
/// Reset the S3 request counters for matching endpoint, bucket or datastore (if prefix is given).
pub async fn reset_counters(
    s3_client_id: String,
    bucket: String,
    store_prefix: Option<String>,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<(), Error> {
    let (config, _digest) = pbs_config::s3::config()?;
    // only check if the provided endpoint id exists
    let _config: S3ClientConf = config
        .lookup(S3_CFG_TYPE_ID, &s3_client_id)
        .context("config lookup failed")?;

    let request_counter_id = if let Some(store) = &store_prefix {
        format!("{s3_client_id}-{bucket}-{store}")
    } else {
        format!("{s3_client_id}-{bucket}")
    };

    let path = format!("{S3_CLIENT_REQUEST_COUNTER_BASE_PATH}/{request_counter_id}.shmem");
    let path = Path::new(&path);
    // Fail early to not create the file when opening shared memory map below. Accept that
    // this can race, with a new counter file being created in the mean time, but that is
    // not an issue.
    if !path.is_file() {
        bail!("Cannot find s3 counters file '{path:?}'");
    }

    let user = pbs_config::backup_user()?;
    let request_counters = SharedRequestCounters::open_shared_memory_mapped(path, user)
        .context("failed to open shared request counters")?;
    request_counters.reset(Ordering::Release);

    Ok(())
}

#[sortable]
const S3_OPERATION_SUBDIRS: SubdirMap = &[
    ("check", &Router::new().put(&API_METHOD_CHECK)),
    (
        "reset-counters",
        &Router::new().put(&API_METHOD_RESET_COUNTERS),
    ),
];

const S3_OPERATION_ROUTER: Router = Router::new()
    .get(&list_subdirs_api_method!(S3_OPERATION_SUBDIRS))
    .subdirs(S3_OPERATION_SUBDIRS);

pub const ROUTER: Router = Router::new().match_all("s3-client-id", &S3_OPERATION_ROUTER);
