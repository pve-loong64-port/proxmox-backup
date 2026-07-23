use std::os::unix::prelude::AsRawFd;

use anyhow::{Error, bail, format_err};
use hex::FromHex;
use nix::unistd::{Uid, User};

use proxmox_product_config::lockfile_create_options;
use proxmox_sys::fs::DirLockGuard;

pub use pbs_buildcfg::BACKUP_USER_NAME;

pub mod acl;
mod cached_user_info;
pub use cached_user_info::CachedUserInfo;
pub mod datastore;
pub mod domains;
pub mod drive;
pub mod encryption_keys;
pub mod key_value;
pub mod media_pool;
pub mod metrics;
pub mod node;
pub mod notifications;
pub mod prune;
pub mod remote;
pub mod s3;
pub mod sync;
pub mod tape_job;
pub mod token_shadow;
pub mod traffic_control;
pub mod user;
pub mod verify;

mod config_version_cache;
pub use config_version_cache::ConfigVersionCache;

/// Return User info for the 'backup' user (``getpwnam_r(3)``)
pub fn backup_user() -> Result<nix::unistd::User, Error> {
    if cfg!(test) {
        Ok(User::from_uid(Uid::current())?.expect("current user does not exist"))
    } else {
        User::from_name(BACKUP_USER_NAME)?
            .ok_or_else(|| format_err!("Unable to lookup '{}' user.", BACKUP_USER_NAME))
    }
}

/// Return User info for root
pub fn priv_user() -> Result<nix::unistd::User, Error> {
    if cfg!(test) {
        Ok(User::from_uid(Uid::current())?.expect("current user does not exist"))
    } else {
        User::from_name("root")?.ok_or_else(|| format_err!("Unable to lookup superuser."))
    }
}

#[must_use = "lock guard must be used to keep file locked"]
pub struct BackupLockGuard {
    file: Option<std::fs::File>,
    // TODO: Remove `_legacy_dir` with PBS 5
    _legacy_dir: Option<DirLockGuard>,
}

impl AsRawFd for BackupLockGuard {
    fn as_raw_fd(&self) -> i32 {
        self.file.as_ref().map_or(-1, |f| f.as_raw_fd())
    }
}

// TODO: Remove with PBS 5
impl From<DirLockGuard> for BackupLockGuard {
    fn from(value: DirLockGuard) -> Self {
        Self {
            file: None,
            _legacy_dir: Some(value),
        }
    }
}

#[doc(hidden)]
/// Note: do not use for production code, this is only intended for tests
pub unsafe fn create_mocked_lock() -> BackupLockGuard {
    BackupLockGuard {
        file: None,
        _legacy_dir: None,
    }
}

/// Open or create a lock file owned by user "backup" and lock it.
///
/// Owner/Group of the file is set to backup/backup.
/// File mode is 0660.
/// Default timeout is 10 seconds.
///
/// Note: This method needs to be called by user "root" or "backup".
pub fn open_backup_lockfile<P: AsRef<std::path::Path>>(
    path: P,
    timeout: Option<std::time::Duration>,
    exclusive: bool,
) -> Result<BackupLockGuard, Error> {
    // TODO: Replace whole helper with proxmox_product_config::open_api_lockfile() when
    // dropping _legacy_dir in in PBS5.
    let options = lockfile_create_options();

    let timeout = timeout.unwrap_or(std::time::Duration::new(10, 0));

    let file = proxmox_sys::fs::open_file_locked(&path, timeout, exclusive, options)?;
    Ok(BackupLockGuard {
        file: Some(file),
        _legacy_dir: None,
    })
}

/// Detect modified configuration files
///
/// This function fails with a reasonable error message if checksums do not match.
pub fn detect_modified_configuration_file<T: AsRef<str>>(
    digest_str: Option<T>,
    expected_digest: &[u8; 32],
) -> Result<(), Error> {
    if let Some(digest_str) = digest_str {
        let digest = <[u8; 32]>::from_hex(digest_str.as_ref())?;
        if &digest != expected_digest {
            bail!("detected modified configuration - file changed by other user? Try again.");
        }
    }
    Ok(())
}
