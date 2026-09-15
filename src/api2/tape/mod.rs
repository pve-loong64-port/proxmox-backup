//! Tape Backup Management

use anyhow::Error;
use serde_json::Value;

use proxmox_router::{Router, SubdirMap, list_subdirs_api_method};
use proxmox_schema::api;

use pbs_api_types::{
    Authid, PRIV_DATASTORE_READ, PRIV_TAPE_WRITE, TapeBackupJobSetup, TapeDeviceInfo,
};
use pbs_config::CachedUserInfo;
use pbs_tape::linux_list_drives::{linux_tape_changer_list, lto_tape_device_list};

pub mod backup;
pub mod changer;
pub mod drive;
pub mod media;
pub mod restore;

pub(crate) fn check_tape_backup_permission(
    auth_id: &Authid,
    setup: &TapeBackupJobSetup,
) -> Result<(), Error> {
    let user_info = CachedUserInfo::new()?;

    user_info.check_privs(
        auth_id,
        &["datastore", &setup.store],
        PRIV_DATASTORE_READ,
        false,
    )?;

    user_info.check_privs(
        auth_id,
        &["tape", "device", &setup.drive],
        PRIV_TAPE_WRITE,
        false,
    )?;

    user_info.check_privs(
        auth_id,
        &["tape", "pool", &setup.pool],
        PRIV_TAPE_WRITE,
        false,
    )?;

    Ok(())
}

#[api(
    input: {
        properties: {},
    },
    returns: {
        description: "The list of autodetected tape drives.",
        type: Array,
        items: {
            type: TapeDeviceInfo,
        },
    },
)]
/// Scan tape drives
pub fn scan_drives(_param: Value) -> Result<Vec<TapeDeviceInfo>, Error> {
    let list = lto_tape_device_list();

    Ok(list)
}

#[api(
    input: {
        properties: {},
    },
    returns: {
        description: "The list of autodetected tape changers.",
        type: Array,
        items: {
            type: TapeDeviceInfo,
        },
    },
)]
/// Scan for SCSI tape changers
pub fn scan_changers(_param: Value) -> Result<Vec<TapeDeviceInfo>, Error> {
    let list = linux_tape_changer_list();

    Ok(list)
}

const SUBDIRS: SubdirMap = &[
    ("backup", &backup::ROUTER),
    ("changer", &changer::ROUTER),
    ("drive", &drive::ROUTER),
    ("media", &media::ROUTER),
    ("restore", &restore::ROUTER),
    (
        "scan-changers",
        &Router::new().get(&API_METHOD_SCAN_CHANGERS),
    ),
    ("scan-drives", &Router::new().get(&API_METHOD_SCAN_DRIVES)),
];

pub const ROUTER: Router = Router::new()
    .get(&list_subdirs_api_method!(SUBDIRS))
    .subdirs(SUBDIRS);
