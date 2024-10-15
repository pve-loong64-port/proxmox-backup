use std::{path::Path, sync::OnceLock, time::Duration};

use anyhow::{format_err, Error};

use nix::sys::stat::Mode;
use pbs_buildcfg::PROXMOX_BACKUP_RUN_DIR;
use proxmox_shared_cache::SharedCache;
use proxmox_sys::fs::CreateOptions;

use super::METRIC_COLLECTION_INTERVAL;

const METRIC_CACHE_TIME: Duration = Duration::from_secs(30 * 60);
const STORED_METRIC_GENERATIONS: u64 =
    METRIC_CACHE_TIME.as_secs() / METRIC_COLLECTION_INTERVAL.as_secs();

static METRIC_CACHE: OnceLock<SharedCache> = OnceLock::new();

/// Initialize the metric cache.
pub(super) fn init() -> Result<(), Error> {
    let backup_user = pbs_config::backup_user()?;
    let file_opts = CreateOptions::new()
        .owner(backup_user.uid)
        .group(backup_user.gid)
        .perm(Mode::from_bits_truncate(0o660));

    let cache_location = Path::new(PROXMOX_BACKUP_RUN_DIR).join("metrics");

    let cache = SharedCache::new(cache_location, file_opts, STORED_METRIC_GENERATIONS as u32)?;

    METRIC_CACHE
        .set(cache)
        .map_err(|_e| format_err!("metric cache already initialized"))?;

    Ok(())
}
