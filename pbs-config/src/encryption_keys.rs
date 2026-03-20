use std::collections::HashMap;
use std::sync::LazyLock;

use anyhow::{bail, format_err, Error};
use nix::{sys::stat::Mode, unistd::Uid};
use serde::Deserialize;

use pbs_api_types::{CryptKey, KeyInfo, CRYPT_KEY_ID_SCHEMA};
use proxmox_schema::ApiType;
use proxmox_section_config::{SectionConfig, SectionConfigData, SectionConfigPlugin};
use proxmox_sys::fs::CreateOptions;

use pbs_buildcfg::configdir;
use pbs_key_config::KeyConfig;

use crate::{open_backup_lockfile, replace_backup_config, BackupLockGuard};

pub static CONFIG: LazyLock<SectionConfig> = LazyLock::new(init);

fn init() -> SectionConfig {
    let obj_schema = CryptKey::API_SCHEMA.unwrap_all_of_schema();
    let plugin = SectionConfigPlugin::new(
        ENCRYPTION_KEYS_CFG_TYPE_ID.to_string(),
        Some(String::from("id")),
        obj_schema,
    );
    let mut config = SectionConfig::new(&CRYPT_KEY_ID_SCHEMA);
    config.register_plugin(plugin);

    config
}

/// Configuration file location for encryption keys.
pub const ENCRYPTION_KEYS_CFG_FILENAME: &str = configdir!("/encryption-keys.cfg");
/// Configuration lock file used to prevent concurrent configuration update operations.
pub const ENCRYPTION_KEYS_CFG_LOCKFILE: &str = configdir!("/.encryption-keys.lck");
/// Directory where to store the actual encryption keys
pub const ENCRYPTION_KEYS_DIR: &str = configdir!("/encryption-keys/");

/// Config type for encryption key config entries
pub const ENCRYPTION_KEYS_CFG_TYPE_ID: &str = "sync-key";

/// Get exclusive lock for encryption key configuration update.
pub fn lock_config() -> Result<BackupLockGuard, Error> {
    open_backup_lockfile(ENCRYPTION_KEYS_CFG_LOCKFILE, None, true)
}

/// Load encryption key configuration from file.
pub fn config() -> Result<(SectionConfigData, [u8; 32]), Error> {
    let content = proxmox_sys::fs::file_read_optional_string(ENCRYPTION_KEYS_CFG_FILENAME)?;
    let content = content.unwrap_or_default();
    let digest = openssl::sha::sha256(content.as_bytes());
    let data = CONFIG.parse(ENCRYPTION_KEYS_CFG_FILENAME, &content)?;
    Ok((data, digest))
}

/// Save given key configuration to file.
pub fn save_config(config: &SectionConfigData) -> Result<(), Error> {
    let raw = CONFIG.write(ENCRYPTION_KEYS_CFG_FILENAME, config)?;
    replace_backup_config(ENCRYPTION_KEYS_CFG_FILENAME, raw.as_bytes())
}

/// Shell completion helper to complete encryption key id's as found in the config.
pub fn complete_encryption_key_id(_arg: &str, _param: &HashMap<String, String>) -> Vec<String> {
    match config() {
        Ok((data, _digest)) => data.sections.keys().map(|id| id.to_string()).collect(),
        Err(_) => Vec::new(),
    }
}

/// Load the encryption key from file.
///
/// Looks up the key in the config and tries to load it from the given file.
/// Upon loading, the config key fingerprint is compared to the one stored in the key
/// file. Fail to load archived keys if flag is set.
pub fn load_key_config(id: &str, fail_on_archived: bool) -> Result<KeyConfig, Error> {
    let _lock = lock_config()?;
    let (config, _digest) = config()?;

    let key: CryptKey = config.lookup(ENCRYPTION_KEYS_CFG_TYPE_ID, id)?;
    if fail_on_archived && key.archived_at.is_some() {
        bail!("cannot load archived encryption key {id}");
    }
    let key_config = match &key.info.path {
        Some(path) => KeyConfig::load(path)?,
        None => bail!("missing path for encryption key {id}"),
    };

    let stored_key_info = KeyInfo::from(&key_config);

    if key.info.fingerprint != stored_key_info.fingerprint {
        bail!("loaded key does not match the config for key {id}");
    }

    Ok(key_config)
}

/// Store the encryption key to file.
///
/// Inserts the key in the config and stores it to the given file.
pub fn store_key(id: &str, key: &KeyConfig) -> Result<(), Error> {
    let _lock = lock_config()?;
    let (mut config, _digest) = config()?;

    if config.sections.contains_key(id) {
        bail!("key with id '{id}' already exists.");
    }

    let backup_user = crate::backup_user()?;
    let dir_options = CreateOptions::new()
        .perm(Mode::from_bits_truncate(0o0750))
        .owner(Uid::from_raw(0))
        .group(backup_user.gid);

    proxmox_sys::fs::ensure_dir_exists(ENCRYPTION_KEYS_DIR, &dir_options, true)?;

    let key_path = format!("{ENCRYPTION_KEYS_DIR}{id}.enc");
    let key_lock_path = format!("{key_path}.lck");

    // lock to avoid race with key deletion
    let _lock = open_backup_lockfile(&key_lock_path, None, true)?;

    // assert the key file is empty or does not exist
    match std::fs::metadata(&key_path) {
        Ok(metadata) => {
            if metadata.len() > 0 {
                bail!("detected pre-existing key file, refusing to overwrite.");
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => (),
        Err(err) => return Err(err.into()),
    }

    let keyfile_mode = nix::sys::stat::Mode::from_bits_truncate(0o0640);

    key.store_with(
        &key_path,
        true,
        Some(keyfile_mode),
        Some(Uid::from_raw(0)),
        Some(backup_user.gid),
    )?;

    let mut info = KeyInfo::from(key);
    info.path = Some(key_path.clone());

    let crypt_key = CryptKey {
        id: id.to_string(),
        info,
        archived_at: None,
    };

    let result = proxmox_lang::try_block!({
        config.set_data(id, ENCRYPTION_KEYS_CFG_TYPE_ID, crypt_key)?;
        save_config(&config)
    });

    if result.is_err() {
        let _ = std::fs::remove_file(key_path);
    }

    result
}

/// Delete the encryption key from config.
///
/// Returns true if the key was removed successfully, false if there was no matching key.
/// Safety: caller must acquire and hold config lock.
pub fn delete_key(id: &str, mut config: SectionConfigData) -> Result<bool, Error> {
    if let Some((_, key)) = config.sections.remove(id) {
        let key =
            CryptKey::deserialize(key).map_err(|_err| format_err!("failed to parse key config"))?;

        if key.archived_at.is_none() {
            bail!("key still active, deleting is only possible for archived keys");
        }

        if let Some(key_path) = &key.info.path {
            let key_lock_path = format!("{key_path}.lck");
            // Avoid races with key insertion
            let _lock = open_backup_lockfile(key_lock_path, None, true)?;

            let key_config = KeyConfig::load(key_path)?;
            let stored_key_info = KeyInfo::from(&key_config);
            // Check the key is the expected one
            if key.info.fingerprint != stored_key_info.fingerprint {
                bail!("unexpected key detected in key file, refuse to delete");
            }

            let raw = CONFIG.write(ENCRYPTION_KEYS_CFG_FILENAME, &config)?;
            // drops config lock
            replace_backup_config(ENCRYPTION_KEYS_CFG_FILENAME, raw.as_bytes())?;

            std::fs::remove_file(key_path)?;
            return Ok(true);
        }

        bail!("missing key file path for key '{id}'");
    }
    Ok(false)
}
