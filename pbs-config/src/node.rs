use std::collections::HashSet;

use anyhow::{Error, bail};
use openssl::ssl::{SslAcceptor, SslMethod};

use pbs_api_types::NodeConfig;
use proxmox_http::ProxyConfig;
use proxmox_schema::ApiType;

use pbs_buildcfg::configdir;

use crate::{BackupLockGuard, open_backup_lockfile};

const CONF_FILE: &str = configdir!("/node.cfg");
const LOCK_FILE: &str = configdir!("/.node.lck");

pub fn lock() -> Result<BackupLockGuard, Error> {
    open_backup_lockfile(LOCK_FILE, None, true)
}

/// Read the Node Config.
pub fn config() -> Result<(NodeConfig, [u8; 32]), Error> {
    let content = proxmox_sys::fs::file_read_optional_string(CONF_FILE)?.unwrap_or_default();

    let digest = openssl::sha::sha256(content.as_bytes());
    let data: NodeConfig = crate::key_value::from_str(&content, &NodeConfig::API_SCHEMA)?;

    Ok((data, digest))
}

/// Write the Node Config, requires the write lock to be held.
pub fn save_config(config: &NodeConfig) -> Result<(), Error> {
    let mut domains = HashSet::new();
    for domain in config.acme_domains() {
        let domain = domain?;
        if !domains.insert(domain.domain.to_lowercase()) {
            bail!("duplicate domain '{}' in ACME config", domain.domain);
        }
    }
    let mut dummy_acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
    if let Some(ciphers) = config.ciphers_tls_1_3.as_deref() {
        dummy_acceptor.set_ciphersuites(ciphers)?;
    }
    if let Some(ciphers) = config.ciphers_tls_1_2.as_deref() {
        dummy_acceptor.set_cipher_list(ciphers)?;
    }

    let raw = crate::key_value::to_bytes(config, &NodeConfig::API_SCHEMA)?;
    crate::replace_backup_config(CONF_FILE, &raw)
}

pub fn node_http_proxy_config() -> Result<Option<ProxyConfig>, Error> {
    let (node_config, _digest) = self::config()?;
    Ok(node_config.http_proxy())
}
