use anyhow::{bail, Error};
use serde_json::Value;

use proxmox_router::cli::{CliCommand, CliCommandMap, Confirmation};
use proxmox_schema::api;

use crate::{
    complete_backup_group, complete_namespace, complete_repository, merge_group_into,
    REPO_URL_SCHEMA,
};
use pbs_api_types::{BackupGroup, BackupNamespace};
use pbs_client::tools::{connect, remove_repository_from_value};

pub fn group_mgmt_cli() -> CliCommandMap {
    CliCommandMap::new().insert(
        "forget",
        CliCommand::new(&API_METHOD_FORGET_GROUP)
            .arg_param(&["group"])
            .completion_cb("ns", complete_namespace)
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
            repository: {
                schema: REPO_URL_SCHEMA,
                optional: true,
            },
            ns: {
                type: BackupNamespace,
                optional: true,
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
        format!(
            "Delete group \"{}\" with {} snapshot(s)?",
            backup_group, snapshots
        )
        .as_str(),
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
