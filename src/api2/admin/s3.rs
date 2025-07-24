//! S3 bucket operations

use anyhow::{Context, Error};
use serde_json::Value;

use proxmox_http::Body;
use proxmox_router::{list_subdirs_api_method, Permission, Router, RpcEnvironment, SubdirMap};
use proxmox_s3_client::{
    S3Client, S3ClientConf, S3ClientOptions, S3ObjectKey, S3_BUCKET_NAME_SCHEMA,
    S3_CLIENT_ID_SCHEMA,
};
use proxmox_schema::*;
use proxmox_sortable_macro::sortable;

use pbs_api_types::PRIV_SYS_MODIFY;

use pbs_config::s3::S3_CFG_TYPE_ID;

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
    store_prefix: String,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<Value, Error> {
    let (config, _digest) = pbs_config::s3::config()?;
    let config: S3ClientConf = config
        .lookup(S3_CFG_TYPE_ID, &s3_client_id)
        .context("config lookup failed")?;

    let options =
        S3ClientOptions::from_config(config.config, config.secret_key, bucket, store_prefix);

    let test_object_key =
        S3ObjectKey::try_from(".s3-client-test").context("failed to generate s3 object key")?;
    let client = S3Client::new(options).context("client creation failed")?;
    client.head_bucket().await.context("head object failed")?;
    client
        .put_object(test_object_key.clone(), Body::empty(), true)
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

#[sortable]
const S3_OPERATION_SUBDIRS: SubdirMap = &[("check", &Router::new().put(&API_METHOD_CHECK))];

const S3_OPERATION_ROUTER: Router = Router::new()
    .get(&list_subdirs_api_method!(S3_OPERATION_SUBDIRS))
    .subdirs(S3_OPERATION_SUBDIRS);

pub const ROUTER: Router = Router::new().match_all("s3-client-id", &S3_OPERATION_ROUTER);
