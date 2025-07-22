use std::collections::HashMap;
use std::sync::LazyLock;

use anyhow::Error;

use proxmox_s3_client::{S3ClientConf, S3_CLIENT_ID_SCHEMA};
use proxmox_schema::*;
use proxmox_section_config::{SectionConfig, SectionConfigData, SectionConfigPlugin};


use pbs_buildcfg::configdir;

use crate::{open_backup_lockfile, replace_backup_config, BackupLockGuard};

pub static CONFIG: LazyLock<SectionConfig> = LazyLock::new(init);

fn init() -> SectionConfig {
    let obj_schema = S3ClientConf::API_SCHEMA.unwrap_all_of_schema();
    let plugin =
        SectionConfigPlugin::new("s3client".to_string(), Some(String::from("id")), obj_schema);
    let mut config = SectionConfig::new(&S3_CLIENT_ID_SCHEMA);
    config.register_plugin(plugin);

    config
}

/// Configuration file location for S3 client.
pub const S3_CFG_FILENAME: &str = configdir!("/s3.cfg");
/// Configuration lock file used to prevent concurrent configuration update operations.
pub const S3_CFG_LOCKFILE: &str = configdir!("/.s3.lck");

/// Config type for s3 client config entries
pub const S3_CFG_TYPE_ID: &str = "s3client";

/// Get exclusive lock for S3 client configuration update.
pub fn lock_config() -> Result<BackupLockGuard, Error> {
    open_backup_lockfile(S3_CFG_LOCKFILE, None, true)
}

/// Load s3 client configuration from file.
pub fn config() -> Result<(SectionConfigData, [u8; 32]), Error> {
    parse_config(S3_CFG_FILENAME)
}

/// Save given s3 client configuration to file.
pub fn save_config(config: &SectionConfigData) -> Result<(), Error> {
    let raw = CONFIG.write(S3_CFG_FILENAME, config)?;
    replace_backup_config(S3_CFG_FILENAME, raw.as_bytes())?;
    Ok(())
}

/// Shell completion helper to complete s3 client id's as found in the config.
pub fn complete_s3_client_id(_arg: &str, _param: &HashMap<String, String>) -> Vec<String> {
    match config() {
        Ok((data, _digest)) => data.sections.keys().map(|id| id.to_string()).collect(),
        Err(_) => Vec::new(),
    }
}

fn parse_config(path: &str) -> Result<(SectionConfigData, [u8; 32]), Error> {
    let content = proxmox_sys::fs::file_read_optional_string(path)?;
    let content = content.unwrap_or_default();
    let digest = openssl::sha::sha256(content.as_bytes());
    let data = CONFIG.parse(path, &content)?;
    Ok((data, digest))
}
