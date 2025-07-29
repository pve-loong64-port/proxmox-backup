//! Tools and utilities
//!
//! This is a collection of small and useful tools.

use anyhow::{bail, Error};
use std::collections::HashSet;

use pbs_api_types::{
    Authid, BackupContent, CryptMode, SnapshotListItem, SnapshotVerifyState, MANIFEST_BLOB_NAME,
};
use proxmox_http::{client::Client, HttpOptions, ProxyConfig};

use pbs_datastore::backup_info::{BackupDir, BackupInfo};
use pbs_datastore::manifest::BackupManifest;

pub mod config;
pub mod disks;
pub mod fs;

mod shared_rate_limiter;
pub use shared_rate_limiter::SharedRateLimiter;

pub mod statistics;
pub mod systemd;
pub mod ticket;

pub mod parallel_handler;

pub fn assert_if_modified(digest1: &str, digest2: &str) -> Result<(), Error> {
    if digest1 != digest2 {
        bail!("detected modified configuration - file changed by other user? Try again.");
    }
    Ok(())
}

/// Detect modified configuration files
///
/// This function fails with a reasonable error message if checksums do not match.
pub fn detect_modified_configuration_file(
    digest1: &[u8; 32],
    digest2: &[u8; 32],
) -> Result<(), Error> {
    if digest1 != digest2 {
        bail!("detected modified configuration - file changed by other user? Try again.");
    }
    Ok(())
}

/// The default 2 hours are far too long for PBS
pub const PROXMOX_BACKUP_TCP_KEEPALIVE_TIME: u32 = 120;
pub const DEFAULT_USER_AGENT_STRING: &str = "proxmox-backup-client/1.0";

/// Returns a new instance of [`Client`] configured for PBS usage.
pub fn pbs_simple_http(proxy_config: Option<ProxyConfig>) -> Client {
    let options = HttpOptions {
        proxy_config,
        user_agent: Some(DEFAULT_USER_AGENT_STRING.to_string()),
        tcp_keepalive: Some(PROXMOX_BACKUP_TCP_KEEPALIVE_TIME),
    };

    Client::with_options(options)
}

pub fn setup_safe_path_env() {
    std::env::set_var("PATH", "/sbin:/bin:/usr/sbin:/usr/bin");
    // Make %ENV safer - as suggested by https://perldoc.perl.org/perlsec.html
    for name in &["IFS", "CDPATH", "ENV", "BASH_ENV"] {
        std::env::remove_var(name);
    }
}

pub(crate) fn read_backup_index(
    backup_dir: &BackupDir,
) -> Result<(BackupManifest, Vec<BackupContent>), Error> {
    let (manifest, index_size) = backup_dir.load_manifest()?;

    let mut result = Vec::new();
    for item in manifest.files() {
        result.push(BackupContent {
            filename: item.filename.clone(),
            crypt_mode: Some(item.crypt_mode),
            size: Some(item.size),
        });
    }

    result.push(BackupContent {
        filename: MANIFEST_BLOB_NAME.to_string(),
        crypt_mode: match manifest.signature {
            Some(_) => Some(CryptMode::SignOnly),
            None => Some(CryptMode::None),
        },
        size: Some(index_size),
    });

    Ok((manifest, result))
}

pub(crate) fn get_all_snapshot_files(
    info: &BackupInfo,
) -> Result<(BackupManifest, Vec<BackupContent>), Error> {
    let (manifest, mut files) = read_backup_index(&info.backup_dir)?;

    let file_set = files.iter().fold(HashSet::new(), |mut acc, item| {
        acc.insert(item.filename.clone());
        acc
    });

    for file in &info.files {
        if file_set.contains(file) {
            continue;
        }
        files.push(BackupContent {
            filename: file.to_string(),
            size: None,
            crypt_mode: None,
        });
    }

    Ok((manifest, files))
}

/// Helper to transform `BackupInfo` to `SnapshotListItem` with given owner.
pub(crate) fn backup_info_to_snapshot_list_item(
    info: &BackupInfo,
    owner: &Authid,
) -> SnapshotListItem {
    let backup = info.backup_dir.dir().to_owned();
    let protected = info.protected;
    let owner = Some(owner.to_owned());

    match get_all_snapshot_files(info) {
        Ok((manifest, files)) => {
            // extract the first line from notes
            let comment: Option<String> = manifest.unprotected["notes"]
                .as_str()
                .and_then(|notes| notes.lines().next())
                .map(String::from);

            let fingerprint = match manifest.fingerprint() {
                Ok(fp) => fp,
                Err(err) => {
                    eprintln!("error parsing fingerprint: '{}'", err);
                    None
                }
            };

            let verification: Option<SnapshotVerifyState> = match manifest.verify_state() {
                Ok(verify) => verify,
                Err(err) => {
                    eprintln!("error parsing verification state : '{err}'");
                    None
                }
            };

            let size = Some(files.iter().map(|x| x.size.unwrap_or(0)).sum());

            SnapshotListItem {
                backup,
                comment,
                verification,
                fingerprint,
                files,
                size,
                owner,
                protected,
            }
        }
        Err(err) => {
            eprintln!("error during snapshot file listing: '{err}'");
            let files = info
                .files
                .iter()
                .map(|filename| BackupContent {
                    filename: filename.to_owned(),
                    size: None,
                    crypt_mode: None,
                })
                .collect();

            SnapshotListItem {
                backup,
                comment: None,
                verification: None,
                fingerprint: None,
                files,
                size: None,
                owner,
                protected,
            }
        }
    }
}
