//! Round Robin Database cache
//!
//! RRD files are stored under `/var/lib/proxmox-backup/rrdb/`. Only a
//! single process may access and update those files, so we initialize
//! and update RRD data inside `proxmox-backup-proxy`.

use std::path::Path;

use anyhow::{format_err, Error};
use once_cell::sync::OnceCell;

use proxmox_rrd::rrd::{AggregationFn, Archive, DataSourceType, Database};
use proxmox_rrd::Cache;
use proxmox_sys::fs::CreateOptions;

use pbs_buildcfg::PROXMOX_BACKUP_STATE_DIR_M;
use proxmox_rrd_api_types::{RrdMode, RrdTimeframe};

const RRD_CACHE_BASEDIR: &str = concat!(PROXMOX_BACKUP_STATE_DIR_M!(), "/rrdb");

static RRD_CACHE: OnceCell<Cache> = OnceCell::new();

/// Get the RRD cache instance
pub fn get_rrd_cache() -> Result<&'static Cache, Error> {
    RRD_CACHE
        .get()
        .ok_or_else(|| format_err!("RRD cache not initialized!"))
}

/// Initialize the RRD cache instance
///
/// Note: Only a single process must do this (proxmox-backup-proxy)
pub fn initialize_rrd_cache() -> Result<&'static Cache, Error> {
    let backup_user = pbs_config::backup_user()?;

    let file_options = CreateOptions::new()
        .owner(backup_user.uid)
        .group(backup_user.gid);

    let dir_options = CreateOptions::new()
        .owner(backup_user.uid)
        .group(backup_user.gid);

    let apply_interval = 30.0 * 60.0; // 30 minutes

    let cache = Cache::new(
        RRD_CACHE_BASEDIR,
        Some(file_options),
        Some(dir_options),
        apply_interval,
        load_callback,
        create_callback,
    )?;

    RRD_CACHE
        .set(cache)
        .map_err(|_| format_err!("RRD cache already initialized!"))?;

    Ok(RRD_CACHE.get().unwrap())
}

fn load_callback(path: &Path, _rel_path: &str) -> Option<Database> {
    match Database::load(path, true) {
        Ok(rrd) => Some(rrd),
        Err(err) => {
            if err.kind() != std::io::ErrorKind::NotFound {
                log::warn!("overwriting RRD file {path:?}, because of load error: {err}",);
            }
            None
        }
    }
}

fn create_callback(dst: DataSourceType) -> Database {
    let rra_list = vec![
        // 1 min * 1440 => 1 day
        Archive::new(AggregationFn::Average, 60, 1440),
        Archive::new(AggregationFn::Maximum, 60, 1440),
        // 30 min * 1440 => 30 days ~ 1 month
        Archive::new(AggregationFn::Average, 30 * 60, 1440),
        Archive::new(AggregationFn::Maximum, 30 * 60, 1440),
        // 6 h * 1440 => 360 days ~ 1 year
        Archive::new(AggregationFn::Average, 6 * 3600, 1440),
        Archive::new(AggregationFn::Maximum, 6 * 3600, 1440),
        // 1 week * 570 => 10 years
        Archive::new(AggregationFn::Average, 7 * 86400, 570),
        Archive::new(AggregationFn::Maximum, 7 * 86400, 570),
    ];

    Database::new(dst, rra_list)
}

/// Extracts data for the specified time frame from from RRD cache
pub fn extract_rrd_data(
    basedir: &str,
    name: &str,
    timeframe: RrdTimeframe,
    mode: RrdMode,
) -> Result<Option<proxmox_rrd::Entry>, Error> {
    let end = proxmox_time::epoch_f64() as u64;

    let (start, resolution) = match timeframe {
        RrdTimeframe::Hour => (end - 3600, 60),
        RrdTimeframe::Day => (end - 3600 * 24, 60),
        RrdTimeframe::Week => (end - 3600 * 24 * 7, 30 * 60),
        RrdTimeframe::Month => (end - 3600 * 24 * 30, 30 * 60),
        RrdTimeframe::Year => (end - 3600 * 24 * 365, 6 * 60 * 60),
        RrdTimeframe::Decade => (end - 10 * 3600 * 24 * 366, 7 * 86400),
    };

    let cf = match mode {
        RrdMode::Max => AggregationFn::Maximum,
        RrdMode::Average => AggregationFn::Average,
    };

    let rrd_cache = get_rrd_cache()?;

    rrd_cache.extract_cached_data(basedir, name, cf, resolution, Some(start), Some(end))
}

/// Sync/Flush the RRD journal
pub fn rrd_sync_journal() {
    if let Ok(rrd_cache) = get_rrd_cache() {
        if let Err(err) = rrd_cache.sync_journal() {
            log::error!("rrd_sync_journal failed - {}", err);
        }
    }
}
/// Update RRD Gauge values
pub fn rrd_update_gauge(name: &str, value: f64) {
    if let Ok(rrd_cache) = get_rrd_cache() {
        let now = proxmox_time::epoch_f64();
        if let Err(err) = rrd_cache.update_value(name, now, value, DataSourceType::Gauge) {
            log::error!("rrd::update_value '{}' failed - {}", name, err);
        }
    }
}

/// Update RRD Derive values
pub fn rrd_update_derive(name: &str, value: f64) {
    if let Ok(rrd_cache) = get_rrd_cache() {
        let now = proxmox_time::epoch_f64();
        if let Err(err) = rrd_cache.update_value(name, now, value, DataSourceType::Derive) {
            log::error!("rrd::update_value '{}' failed - {}", name, err);
        }
    }
}
