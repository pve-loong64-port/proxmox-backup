use ::serde::{Deserialize, Serialize};
use anyhow::{Error, format_err};
use serde_json::Value;

use proxmox_router::{Permission, Router, RpcEnvironment, http_bail};
use proxmox_schema::{api, param_bail};
use proxmox_section_config::SectionConfigData;

use pbs_api_types::{
    Authid, DRIVE_NAME_SCHEMA, LtoTapeDrive, LtoTapeDriveUpdater, PRIV_TAPE_AUDIT,
    PRIV_TAPE_MODIFY, PROXMOX_CONFIG_DIGEST_SCHEMA, ScsiTapeChanger,
};
use pbs_config::CachedUserInfo;

use pbs_tape::linux_list_drives::{check_drive_path, lto_tape_device_list};

fn check_drive_in_use(
    new_drive: &LtoTapeDrive,
    section_config: &SectionConfigData,
) -> Result<(), Error> {
    let existing: Vec<LtoTapeDrive> = section_config.convert_to_typed_array("lto")?;

    for drive in existing {
        if drive.name == new_drive.name {
            continue;
        }
        if drive.path == new_drive.path {
            param_bail!(
                "path",
                "Path '{}' already used in drive '{}'",
                new_drive.path,
                drive.name
            );
        }
    }

    Ok(())
}

fn check_unique_drive_changer_assignment(
    new_drive: &LtoTapeDrive,
    section_config: &SectionConfigData,
) -> Result<(), Error> {
    let Some(new_changer) = new_drive.changer.as_ref() else {
        return Ok(());
    };

    let new_drive_num = new_drive.changer_drivenum.unwrap_or(0);

    let existing: Vec<LtoTapeDrive> = section_config.convert_to_typed_array("lto")?;

    for drive in existing {
        if drive.name == new_drive.name {
            continue;
        }
        let Some(changer) = drive.changer.as_ref() else {
            continue;
        };
        let drive_num = drive.changer_drivenum.unwrap_or(0);
        if changer == new_changer && drive_num == new_drive_num {
            param_bail!(
                "changer_drivenum",
                "Changer drive number '{drive_num}' already used in drive '{}'",
                drive.name
            );
        }
    }

    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            config: {
                type: LtoTapeDrive,
                flatten: true,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["tape", "device"], PRIV_TAPE_MODIFY, false),
    },
)]
/// Create a new drive
pub fn create_drive(config: LtoTapeDrive) -> Result<(), Error> {
    let _lock = pbs_config::drive::lock()?;

    let (mut section_config, _digest) = pbs_config::drive::config()?;

    if section_config.sections.contains_key(&config.name) {
        param_bail!("name", "Entry '{}' already exists", config.name);
    }

    let lto_drives = lto_tape_device_list();

    check_drive_path(&lto_drives, &config.path)?;

    check_drive_in_use(&config, &section_config)?;

    check_unique_drive_changer_assignment(&config, &section_config)?;

    section_config.set_data(&config.name, "lto", &config)?;

    pbs_config::drive::save_config(&section_config)?;

    Ok(())
}

#[api(
    input: {
        properties: {
            name: {
                schema: DRIVE_NAME_SCHEMA,
            },
        },
    },
    returns: {
        type: LtoTapeDrive,
    },
    access: {
        permission: &Permission::Privilege(&["tape", "device", "{name}"], PRIV_TAPE_AUDIT, false),
    },
)]
/// Get drive configuration
pub fn get_config(
    name: String,
    _param: Value,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<LtoTapeDrive, Error> {
    let (config, digest) = pbs_config::drive::config()?;

    let data: LtoTapeDrive = config.lookup("lto", &name)?;

    rpcenv["digest"] = hex::encode(digest).into();

    Ok(data)
}

#[api(
    input: {
        properties: {},
    },
    returns: {
        description: "The list of configured drives (with config digest).",
        type: Array,
        items: {
            type: LtoTapeDrive,
        },
    },
    access: {
        description: "List configured tape drives filtered by Tape.Audit privileges",
        permission: &Permission::Anybody,
    },
)]
/// List drives
pub fn list_drives(
    _param: Value,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<LtoTapeDrive>, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let user_info = CachedUserInfo::new()?;

    let (config, digest) = pbs_config::drive::config()?;

    let drive_list: Vec<LtoTapeDrive> = config.convert_to_typed_array("lto")?;

    let drive_list = drive_list
        .into_iter()
        .filter(|drive| {
            let privs = user_info.lookup_privs(&auth_id, &["tape", "device", &drive.name]);
            privs & PRIV_TAPE_AUDIT != 0
        })
        .collect();

    rpcenv["digest"] = hex::encode(digest).into();

    Ok(drive_list)
}

#[api()]
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Deletable property name
pub enum DeletableProperty {
    /// Delete the changer property.
    Changer,
    /// Delete the changer-drivenum property.
    ChangerDrivenum,
}

#[api(
    protected: true,
    input: {
        properties: {
            name: {
                schema: DRIVE_NAME_SCHEMA,
            },
            update: {
                type: LtoTapeDriveUpdater,
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
                schema: PROXMOX_CONFIG_DIGEST_SCHEMA,
                optional: true,
            },
       },
    },
    access: {
        permission: &Permission::Privilege(&["tape", "device", "{name}"], PRIV_TAPE_MODIFY, false),
    },
)]
/// Update a drive configuration
pub fn update_drive(
    name: String,
    update: LtoTapeDriveUpdater,
    delete: Option<Vec<DeletableProperty>>,
    digest: Option<String>,
    _param: Value,
) -> Result<(), Error> {
    let _lock = pbs_config::drive::lock()?;

    let (mut config, expected_digest) = pbs_config::drive::config()?;

    pbs_config::detect_modified_configuration_file(digest, &expected_digest)?;

    let mut data: LtoTapeDrive = config.lookup("lto", &name)?;

    if let Some(delete) = delete {
        for delete_prop in delete {
            match delete_prop {
                DeletableProperty::Changer => {
                    data.changer = None;
                    data.changer_drivenum = None;
                }
                DeletableProperty::ChangerDrivenum => {
                    data.changer_drivenum = None;
                }
            }
        }
    }

    if let Some(path) = update.path {
        let lto_drives = lto_tape_device_list();
        check_drive_path(&lto_drives, &path)?;
        data.path = path;
    }
    check_drive_in_use(&data, &config)?;

    if let Some(changer) = update.changer {
        let _: ScsiTapeChanger = config.lookup("changer", &changer)?;
        data.changer = Some(changer);
    }

    if let Some(changer_drivenum) = update.changer_drivenum {
        if changer_drivenum == 0 {
            data.changer_drivenum = None;
        } else {
            if data.changer.is_none() {
                param_bail!(
                    "changer",
                    format_err!("Option 'changer-drivenum' requires option 'changer'.")
                );
            }
            data.changer_drivenum = Some(changer_drivenum);
        }
    }

    check_unique_drive_changer_assignment(&data, &config)?;

    config.set_data(&name, "lto", &data)?;

    pbs_config::drive::save_config(&config)?;

    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            name: {
                schema: DRIVE_NAME_SCHEMA,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["tape", "device", "{name}"], PRIV_TAPE_MODIFY, false),
    },
)]
/// Delete a drive configuration
pub fn delete_drive(name: String, _param: Value) -> Result<(), Error> {
    let _lock = pbs_config::drive::lock()?;

    let (mut config, _digest) = pbs_config::drive::config()?;

    match config.sections.get(&name) {
        Some((section_type, _)) => {
            if section_type != "lto" {
                param_bail!(
                    "name",
                    "Entry '{}' exists, but is not a lto tape drive",
                    name
                );
            }
            config.sections.remove(&name);
        }
        None => http_bail!(NOT_FOUND, "Delete drive '{}' failed - no such drive", name),
    }

    pbs_config::drive::save_config(&config)?;

    Ok(())
}

const ITEM_ROUTER: Router = Router::new()
    .get(&API_METHOD_GET_CONFIG)
    .put(&API_METHOD_UPDATE_DRIVE)
    .delete(&API_METHOD_DELETE_DRIVE);

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_LIST_DRIVES)
    .post(&API_METHOD_CREATE_DRIVE)
    .match_all("name", &ITEM_ROUTER);
