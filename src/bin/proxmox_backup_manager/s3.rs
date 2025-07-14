use proxmox_router::{cli::*, ApiHandler, RpcEnvironment};
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

#[api(
    input: {
        properties: {
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        }
    }
)]
/// List configured s3 clients.
fn list_s3_clients(param: Value, rpcenv: &mut dyn RpcEnvironment) -> Result<Value, Error> {
    let output_format = get_output_format(&param);

    let info = &api2::config::s3::API_METHOD_LIST_S3_CLIENT_CONFIG;
    let mut data = match info.handler {
        ApiHandler::Sync(handler) => (handler)(param, info, rpcenv)?,
        _ => unreachable!(),
    };

    let options = default_table_format_options()
        .column(ColumnConfig::new("id"))
        .column(ColumnConfig::new("endpoint"))
        .column(ColumnConfig::new("port"))
        .column(ColumnConfig::new("region"))
        .column(ColumnConfig::new("access-key"))
        .column(ColumnConfig::new("fingerprint"))
        .column(ColumnConfig::new("path-style"));

    format_and_print_result_full(&mut data, &info.returns, &output_format, &options);

    Ok(Value::Null)
}

pub fn s3_commands() -> CommandLineInterface {
    let client_cmd_def = CliCommandMap::new()
        .insert("list", CliCommand::new(&API_METHOD_LIST_S3_CLIENTS))
        .insert(
            "create",
            CliCommand::new(&api2::config::s3::API_METHOD_CREATE_S3_CLIENT_CONFIG)
                .arg_param(&["id"]),
        )
        .insert(
            "update",
            CliCommand::new(&api2::config::s3::API_METHOD_UPDATE_S3_CLIENT_CONFIG)
                .arg_param(&["id"])
                .completion_cb("id", pbs_config::s3::complete_s3_client_id),
        )
        .insert(
            "remove",
            CliCommand::new(&api2::config::s3::API_METHOD_DELETE_S3_CLIENT_CONFIG)
                .arg_param(&["id"])
                .completion_cb("id", pbs_config::s3::complete_s3_client_id),
        );

    let cmd_def = CliCommandMap::new()
        .insert(
            "check",
            CliCommand::new(&API_METHOD_CHECK)
                .arg_param(&["s3-client-id", "bucket"])
                .completion_cb("s3-client-id", pbs_config::s3::complete_s3_client_id),
        )
        .insert("client", client_cmd_def);

    cmd_def.into()
}
