use anyhow::{Error, bail};
use serde_json::{Value, json};

use pbs_api_types::{BackupNamespace, NS_MAX_DEPTH_SCHEMA};
use pbs_client::{BackupTargetArgs, view_task_result};

use proxmox_router::cli::{
    CliCommand, CliCommandMap, OUTPUT_FORMAT, extract_output_format, format_and_print_result,
    get_output_format,
};
use proxmox_schema::api;

use crate::{
    complete_namespace, connect, extract_repository_from_value, optional_ns_param,
    record_repository, remove_repository_from_value,
};

#[api(
    input: {
        properties: {
            repo: {
                type: BackupTargetArgs,
                flatten: true,
            },
            "max-depth": {
                description: "maximum recursion depth",
                optional: true,
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        }
    },
)]
/// List namespaces in a repository.
async fn list_namespaces(param: Value, max_depth: Option<usize>) -> Result<(), Error> {
    let output_format = get_output_format(&param);
    let repo = extract_repository_from_value(&param)?;
    let backup_ns = optional_ns_param(&param)?;

    let path = format!("api2/json/admin/datastore/{}/namespace", repo.store());

    let mut param = json!({});

    if let Some(max_depth) = max_depth {
        param["max-depth"] = max_depth.into();
    }

    if !backup_ns.is_root() {
        param["parent"] = serde_json::to_value(backup_ns)?;
    }

    let client = connect(&repo)?;

    let mut result = client.get(&path, Some(param)).await?;

    record_repository(&repo);

    if output_format == "text" {
        let data: Vec<pbs_api_types::NamespaceListItem> =
            serde_json::from_value(result["data"].take())?;
        for entry in data {
            if entry.ns.is_root() {
                continue;
            }

            if let Some(comment) = entry.comment {
                println!("{} ({comment})", entry.ns);
            } else {
                println!("{}", entry.ns);
            }
        }
    } else {
        format_and_print_result(&result, &output_format);
    }

    Ok(())
}

#[api(
    input: {
        properties: {
            repo: {
                type: BackupTargetArgs,
                flatten: true,
            },
        }
    },
)]
/// Create a new namespace.
async fn create_namespace(param: Value) -> Result<(), Error> {
    let repo = extract_repository_from_value(&param)?;
    let mut backup_ns = optional_ns_param(&param)?;

    let path = format!("api2/json/admin/datastore/{}/namespace", repo.store());

    let name = match backup_ns.pop() {
        Some(name) => name,
        None => bail!("root namespace is always present"),
    };

    let param = json!({
        "parent": backup_ns,
        "name": name,
    });

    let client = connect(&repo)?;

    let _result = client.post(&path, Some(param)).await?;

    record_repository(&repo);

    Ok(())
}

#[api(
    input: {
        properties: {
            repo: {
                type: BackupTargetArgs,
                flatten: true,
            },
            "delete-groups": {
                description: "Destroys all groups in the hierarchy.",
                optional: true,
            },
        }
    },
)]
/// Delete an existing namespace.
async fn delete_namespace(param: Value, delete_groups: Option<bool>) -> Result<(), Error> {
    let repo = extract_repository_from_value(&param)?;
    let backup_ns = optional_ns_param(&param)?;

    if backup_ns.is_root() {
        bail!("root namespace cannot be deleted");
    }

    let path = format!("api2/json/admin/datastore/{}/namespace", repo.store());
    let mut param = json!({ "ns": backup_ns });

    if let Some(value) = delete_groups {
        param["delete-groups"] = serde_json::to_value(value)?;
    }

    let client = connect(&repo)?;

    let _result = client.delete(&path, Some(param)).await?;

    record_repository(&repo);

    Ok(())
}

#[api(
    input: {
        properties: {
            repo: {
                type: BackupTargetArgs,
                flatten: true,
            },
            "target-ns": {
                type: BackupNamespace,
            },
            "max-depth": {
                schema: NS_MAX_DEPTH_SCHEMA,
                optional: true,
            },
            "delete-source": {
                type: bool,
                optional: true,
                default: true,
                description: "Remove the source namespace after moving all contents. \
                    Defaults to true.",
            },
            "merge-groups": {
                type: bool,
                optional: true,
                default: true,
                description: "If a group with the same name already exists in the target \
                    namespace, merge snapshots into it. Requires matching ownership and \
                    non-overlapping snapshot times.",
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        }
    },
)]
/// Move a backup namespace to a new location within the same datastore.
async fn move_namespace(mut param: Value) -> Result<(), Error> {
    let output_format = extract_output_format(&mut param);
    let source_ns = optional_ns_param(&param)?;
    if source_ns.is_root() {
        bail!("root namespace cannot be moved");
    }
    let repo = remove_repository_from_value(&mut param)?;
    // Forward the source ns even if it only came from PBS_NAMESPACE.
    param["ns"] = serde_json::to_value(&source_ns)?;

    let client = connect(&repo)?;
    let path = format!("api2/json/admin/datastore/{}/move-namespace", repo.store());
    let result = client.post(&path, Some(param)).await?;

    record_repository(&repo);

    view_task_result(&client, result, &output_format).await?;

    Ok(())
}

pub fn cli_map() -> CliCommandMap {
    CliCommandMap::new()
        .insert(
            "list",
            CliCommand::new(&API_METHOD_LIST_NAMESPACES)
                .arg_param(&["ns"])
                .completion_cb("ns", complete_namespace),
        )
        .insert(
            "create",
            CliCommand::new(&API_METHOD_CREATE_NAMESPACE)
                .arg_param(&["ns"])
                .completion_cb("ns", complete_namespace),
        )
        .insert(
            "delete",
            CliCommand::new(&API_METHOD_DELETE_NAMESPACE)
                .arg_param(&["ns"])
                .completion_cb("ns", complete_namespace),
        )
        .insert(
            "move",
            CliCommand::new(&API_METHOD_MOVE_NAMESPACE)
                .arg_param(&["ns"])
                .completion_cb("ns", complete_namespace)
                .completion_cb("target-ns", complete_namespace),
        )
}
