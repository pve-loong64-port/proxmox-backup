use proxmox_router::{cli::*, RpcEnvironment};
use proxmox_s3_client::{S3_BUCKET_NAME_SCHEMA, S3_CLIENT_ID_SCHEMA};
use proxmox_schema::api;

use proxmox_backup::api2;

use anyhow::Error;
use serde_json::Value;

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
)]
/// Perform basic sanity checks for given S3 client configuration
async fn check(
    s3_client_id: String,
    bucket: String,
    store_prefix: String,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Value, Error> {
    api2::admin::s3::check(s3_client_id, bucket, store_prefix, rpcenv).await?;
    Ok(Value::Null)
}

pub fn s3_commands() -> CommandLineInterface {
    let cmd_def = CliCommandMap::new().insert(
        "check",
        CliCommand::new(&API_METHOD_CHECK)
            .arg_param(&["s3-client-id", "bucket"])
            .completion_cb("s3-client-id", pbs_config::s3::complete_s3_client_id),
    );

    cmd_def.into()
}
