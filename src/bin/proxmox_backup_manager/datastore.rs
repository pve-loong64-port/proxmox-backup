use pbs_api_types::{
    DataStoreConfig, DataStoreConfigUpdater, DATASTORE_SCHEMA, PROXMOX_CONFIG_DIGEST_SCHEMA,
};
use pbs_client::view_task_result;
use proxmox_router::{cli::*, ApiHandler, RpcEnvironment};
use proxmox_schema::api;

use proxmox_backup::api2;
use proxmox_backup::api2::config::datastore::DeletableProperty;
use proxmox_backup::client_helpers::connect_to_localhost;

use anyhow::{format_err, Error};
use serde_json::Value;

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
/// Datastore list.
fn list_datastores(param: Value, rpcenv: &mut dyn RpcEnvironment) -> Result<Value, Error> {
    let output_format = get_output_format(&param);

    let info = &api2::config::datastore::API_METHOD_LIST_DATASTORES;
    let mut data = match info.handler {
        ApiHandler::Sync(handler) => (handler)(param, info, rpcenv)?,
        _ => unreachable!(),
    };

    let options = default_table_format_options()
        .column(ColumnConfig::new("name"))
        .column(ColumnConfig::new("path"))
        .column(ColumnConfig::new("comment"));

    format_and_print_result_full(&mut data, &info.returns, &output_format, &options);

    Ok(Value::Null)
}

#[api(
    protected: true,
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
            },
            digest: {
                optional: true,
                schema: PROXMOX_CONFIG_DIGEST_SCHEMA,
            },
        },
    },
)]
/// Mount a removable datastore.
async fn mount_datastore(mut param: Value, rpcenv: &mut dyn RpcEnvironment) -> Result<(), Error> {
    param["node"] = "localhost".into();

    let info = &api2::admin::datastore::API_METHOD_MOUNT;
    let result = match info.handler {
        ApiHandler::Sync(handler) => (handler)(param, info, rpcenv)?,
        _ => unreachable!(),
    };

    crate::wait_for_local_worker(result.as_str().unwrap()).await?;
    Ok(())
}

#[api(
    input: {
        properties: {
            name: {
                schema: DATASTORE_SCHEMA,
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        }
    }
)]
/// Show datastore configuration
fn show_datastore(param: Value, rpcenv: &mut dyn RpcEnvironment) -> Result<Value, Error> {
    let output_format = get_output_format(&param);

    let info = &api2::config::datastore::API_METHOD_READ_DATASTORE;
    let mut data = match info.handler {
        ApiHandler::Sync(handler) => (handler)(param, info, rpcenv)?,
        _ => unreachable!(),
    };

    let options = default_table_format_options();
    format_and_print_result_full(&mut data, &info.returns, &output_format, &options);

    Ok(Value::Null)
}

#[api(
    protected: true,
    input: {
        properties: {
            config: {
                type: DataStoreConfig,
                flatten: true,
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        },
    },
)]
/// Create new datastore config.
async fn create_datastore(mut param: Value) -> Result<Value, Error> {
    let output_format = extract_output_format(&mut param);

    let client = connect_to_localhost()?;

    let result = client
        .post("api2/json/config/datastore", Some(param))
        .await?;

    view_task_result(&client, result, &output_format).await?;

    Ok(Value::Null)
}

#[api(
    protected: true,
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
            },
            digest: {
                optional: true,
                schema: PROXMOX_CONFIG_DIGEST_SCHEMA,
            },
        },
    },
)]
/// Unmount a removable datastore.
async fn unmount_datastore(mut param: Value, rpcenv: &mut dyn RpcEnvironment) -> Result<(), Error> {
    param["node"] = "localhost".into();

    let info = &api2::admin::datastore::API_METHOD_UNMOUNT;
    let result = match info.handler {
        ApiHandler::Async(handler) => (handler)(param, info, rpcenv).await?,
        _ => unreachable!(),
    };

    crate::wait_for_local_worker(result.as_str().unwrap()).await?;
    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            name: {
                schema: DATASTORE_SCHEMA,
            },
            "keep-job-configs": {
                description: "If enabled, the job configurations related to this datastore will be kept.",
                type: bool,
                optional: true,
                default: false,
            },
            "destroy-data": {
                description: "Delete the datastore's underlying contents",
                optional: true,
                type: bool,
                default: false,
            },
            digest: {
                optional: true,
                schema: PROXMOX_CONFIG_DIGEST_SCHEMA,
            },
        },
    },
)]
/// Remove a datastore configuration.
async fn delete_datastore(mut param: Value, rpcenv: &mut dyn RpcEnvironment) -> Result<(), Error> {
    param["node"] = "localhost".into();

    let info = &api2::config::datastore::API_METHOD_DELETE_DATASTORE;
    let result = match info.handler {
        ApiHandler::Async(handler) => (handler)(param, info, rpcenv).await?,
        _ => unreachable!(),
    };

    crate::wait_for_local_worker(result.as_str().unwrap()).await?;
    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            name: {
                schema: DATASTORE_SCHEMA,
            },
            update: {
                type: DataStoreConfigUpdater,
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
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        },
    },
)]
/// Update datastore configuration.
async fn update_datastore(name: String, mut param: Value) -> Result<(), Error> {
    let output_format = extract_output_format(&mut param);
    let client = connect_to_localhost()?;

    let result = client
        .put(
            format!("api2/json/config/datastore/{name}").as_str(),
            Some(param),
        )
        .await?;

    view_task_result(&client, result, &output_format).await?;

    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            uuid: {
                type: String,
                description: "The UUID of the device that should be mounted",
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        },
    },
)]
/// Try mounting a removable datastore given the UUID.
async fn uuid_mount(param: Value, _rpcenv: &mut dyn RpcEnvironment) -> Result<Value, Error> {
    let uuid = param["uuid"]
        .as_str()
        .ok_or_else(|| format_err!("uuid has to be specified"))?;

    let (config, _digest) = pbs_config::datastore::config()?;
    let list: Vec<DataStoreConfig> = config.convert_to_typed_array("datastore")?;
    let matching_stores: Vec<DataStoreConfig> = list
        .into_iter()
        .filter(|store: &DataStoreConfig| {
            store
                .backing_device
                .clone()
                .map_or(false, |device| device.eq(&uuid))
        })
        .collect();

    if matching_stores.len() != 1 {
        return Ok(Value::Null);
    }

    if let Some(store) = matching_stores.first() {
        api2::admin::datastore::do_mount_device(store.clone())?;
    }

    // we don't want to fail for UUIDs that are not associated with datastores, as that produces
    // quite some noise in the logs, given this is check for every device that is plugged in.
    Ok(Value::Null)
}

pub fn datastore_commands() -> CommandLineInterface {
    let cmd_def = CliCommandMap::new()
        .insert("list", CliCommand::new(&API_METHOD_LIST_DATASTORES))
        .insert(
            "mount",
            CliCommand::new(&API_METHOD_MOUNT_DATASTORE)
                .arg_param(&["store"])
                .completion_cb(
                    "store",
                    pbs_config::datastore::complete_removable_datastore_name,
                ),
        )
        .insert(
            "show",
            CliCommand::new(&API_METHOD_SHOW_DATASTORE)
                .arg_param(&["name"])
                .completion_cb("name", pbs_config::datastore::complete_datastore_name),
        )
        .insert(
            "create",
            CliCommand::new(&API_METHOD_CREATE_DATASTORE).arg_param(&["name", "path"]),
        )
        .insert(
            "unmount",
            CliCommand::new(&API_METHOD_UNMOUNT_DATASTORE)
                .arg_param(&["store"])
                .completion_cb(
                    "store",
                    pbs_config::datastore::complete_removable_datastore_name,
                ),
        )
        .insert(
            "update",
            CliCommand::new(&API_METHOD_UPDATE_DATASTORE)
                .arg_param(&["name"])
                .completion_cb("name", pbs_config::datastore::complete_datastore_name)
                .completion_cb(
                    "gc-schedule",
                    pbs_config::datastore::complete_calendar_event,
                )
                .completion_cb(
                    "prune-schedule",
                    pbs_config::datastore::complete_calendar_event,
                ),
        )
        .insert(
            "uuid-mount",
            CliCommand::new(&API_METHOD_UUID_MOUNT).arg_param(&["uuid"]),
        )
        .insert(
            "remove",
            CliCommand::new(&API_METHOD_DELETE_DATASTORE)
                .arg_param(&["name"])
                .completion_cb("name", pbs_config::datastore::complete_datastore_name),
        );

    cmd_def.into()
}
