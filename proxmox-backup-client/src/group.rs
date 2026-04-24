use anyhow::{bail, Error};
use serde_json::Value;

use proxmox_router::cli::{
    extract_output_format, CliCommand, CliCommandMap, Confirmation, OUTPUT_FORMAT,
};
use proxmox_schema::api;

use crate::{
    complete_backup_group, complete_namespace, complete_repository, merge_group_into,
    optional_ns_param, record_repository, BackupTargetArgs,
};
use pbs_api_types::{BackupGroup, BackupNamespace};
use pbs_client::tools::{connect, remove_repository_from_value};
use pbs_client::view_task_result;

pub fn group_mgmt_cli() -> CliCommandMap {
    CliCommandMap::new()
        .insert(
            "forget",
            CliCommand::new(&API_METHOD_FORGET_GROUP)
                .arg_param(&["group"])
                .completion_cb("ns", complete_namespace)
                .completion_cb("repository", complete_repository)
                .completion_cb("group", complete_backup_group),
        )
        .insert(
            "move",
            CliCommand::new(&API_METHOD_MOVE_GROUP)
                .arg_param(&["group"])
                .completion_cb("ns", complete_namespace)
                .completion_cb("target-ns", complete_namespace)
                .completion_cb("repository", complete_repository)
                .completion_cb("group", complete_backup_group),
        )
}

#[api(
    input: {
        properties: {
            group: {
                type: String,
                description: "Backup group",
            },
            repo: {
                type: BackupTargetArgs,
                flatten: true,
            },
        }
    }
)]
/// Forget (remove) backup snapshots.
async fn forget_group(group: String, mut param: Value) -> Result<(), Error> {
    let backup_group: BackupGroup = group.parse()?;
    let repo = remove_repository_from_value(&mut param)?;
    let client = connect(&repo)?;

    let mut api_param = param;
    merge_group_into(api_param.as_object_mut().unwrap(), backup_group.clone());

    let path = format!("api2/json/admin/datastore/{}/snapshots", repo.store());
    let result = client.get(&path, Some(api_param.clone())).await?;
    let snapshots = result["data"].as_array().unwrap().len();

    let confirmation = Confirmation::query_with_default(
        format!("Delete group \"{backup_group}\" with {snapshots} snapshot(s)?").as_str(),
        Confirmation::No,
    )?;
    if confirmation.is_yes() {
        let path = format!("api2/json/admin/datastore/{}/groups", repo.store());
        if let Err(err) = client.delete(&path, Some(api_param)).await {
            // "ENOENT: No such file or directory" is part of the error returned when the group
            // has not been found. The full error contains the full datastore path and we would
            // like to avoid printing that to the console. Checking if it exists before deleting
            // the group doesn't work because we currently do not differentiate between an empty
            // and a nonexistent group. This would make it impossible to remove empty groups.
            if err
                .root_cause()
                .to_string()
                .contains("ENOENT: No such file or directory")
            {
                bail!("Unable to find backup group!");
            } else {
                bail!(err);
            }
        }
        println!("Successfully deleted group!");
    } else {
        println!("Abort.");
    }

    Ok(())
}

#[api(
    input: {
        properties: {
            group: {
                type: String,
                description: "Backup group.",
            },
            repo: {
                type: BackupTargetArgs,
                flatten: true,
            },
            "target-ns": {
                type: BackupNamespace,
            },
            "merge-group": {
                type: bool,
                optional: true,
                default: true,
                description: "If the group already exists in the target namespace, merge \
                    snapshots into it. Requires matching ownership and non-overlapping \
                    snapshot times.",
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        }
    }
)]
/// Move a backup group to a different namespace within the same datastore.
async fn move_group(group: String, mut param: Value) -> Result<(), Error> {
    let output_format = extract_output_format(&mut param);
    let backup_group: BackupGroup = group.parse()?;
    let repo = remove_repository_from_value(&mut param)?;
    let ns = optional_ns_param(&param)?;
    if !ns.is_root() {
        param["ns"] = serde_json::to_value(&ns)?;
    }
    merge_group_into(param.as_object_mut().unwrap(), backup_group);

    let client = connect(&repo)?;
    let path = format!("api2/json/admin/datastore/{}/move-group", repo.store());
    let result = client.post(&path, Some(param)).await?;

    record_repository(&repo);

    view_task_result(&client, result, &output_format).await?;

    Ok(())
}
