use std::{path::Path, sync::OnceLock, time::Duration};

use anyhow::{format_err, Error};

use nix::sys::stat::Mode;
use pbs_api_types::{
    MetricDataPoint,
    MetricDataType::{self, Derive, Gauge},
};
use pbs_buildcfg::PROXMOX_BACKUP_RUN_DIR;
use proxmox_shared_cache::SharedCache;
use proxmox_sys::fs::CreateOptions;
use serde::{Deserialize, Serialize};

use super::{DiskStat, HostStats, METRIC_COLLECTION_INTERVAL};

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

/// Return most recent metrics
///
/// If the metric collection loop has no produced any metrics yet, an empty
/// `Vec` is returned. Returns an error if the cache could not be accessed.
pub fn get_most_recent_metrics() -> Result<Vec<MetricDataPoint>, Error> {
    let cached_datapoints: Option<MetricDataPoints> = get_cache()?.get()?;
    let mut points = cached_datapoints.map(|r| r.datapoints).unwrap_or_default();

    points.sort_unstable_by_key(|p| p.timestamp);

    Ok(points)
}

/// Return all cached metrics with a `timestamp > start_time`
///
/// If the metric collection loop has no produced any metrics yet, an empty
/// `Vec` is returned. Returns an error if the cache could not be accessed.
pub fn get_all_metrics(start_time: i64) -> Result<Vec<MetricDataPoint>, Error> {
    let now = proxmox_time::epoch_i64();

    let delta = now - start_time;

    if delta < 0 {
        // start-time in the future, no metrics for you
        return Ok(Vec::new());
    }

    let generations = delta / (METRIC_COLLECTION_INTERVAL.as_secs() as i64);
    let generations = generations.clamp(0, STORED_METRIC_GENERATIONS as i64);

    let cached_datapoints: Vec<MetricDataPoints> = get_cache()?.get_last(generations as u32)?;

    let mut points = Vec::new();

    for gen in cached_datapoints {
        if gen.timestamp > start_time {
            points.extend(gen.datapoints);
        }
    }

    points.sort_unstable_by_key(|p| p.timestamp);

    Ok(points)
}

/// Convert `DiskStat` `HostStat` into a universal metric data point and cache
/// them for a later retrieval.
pub(super) fn update_metrics(
    host: &HostStats,
    hostdisk: &DiskStat,
    datastores: &[DiskStat],
) -> Result<(), Error> {
    let mut points = MetricDataPoints::new(proxmox_time::epoch_i64());

    // Using the same metric names as in PVE's new /cluster/metrics/export endpoint
    if let Some(stat) = &host.proc {
        points.add(Gauge, "host", "cpu_current", stat.cpu);
        points.add(Gauge, "host", "cpu_iowait", stat.iowait_percent);
    }

    if let Some(loadavg) = &host.load {
        points.add(Gauge, "host", "cpu_avg1", loadavg.0);
        points.add(Gauge, "host", "cpu_avg5", loadavg.1);
        points.add(Gauge, "host", "cpu_avg15", loadavg.2);
    }

    if let Some(meminfo) = &host.meminfo {
        points.add(Gauge, "host", "mem_total", meminfo.memtotal as f64);
        points.add(Gauge, "host", "mem_used", meminfo.memused as f64);
        points.add(Gauge, "host", "swap_total", meminfo.swaptotal as f64);
        points.add(Gauge, "host", "swap_used", meminfo.swapused as f64);
    }

    if let Some(netdev) = &host.net {
        use pbs_config::network::is_physical_nic;
        let mut netin = 0;
        let mut netout = 0;
        for item in netdev {
            if !is_physical_nic(&item.device) {
                continue;
            }
            netin += item.receive;
            netout += item.send;
        }
        points.add(Derive, "host", "net_in", netin as f64);
        points.add(Derive, "host", "net_out", netout as f64);
    }

    update_disk_metrics(&mut points, hostdisk, "host");

    for stat in datastores {
        let id = format!("datastore/{}", stat.name);
        update_disk_metrics(&mut points, stat, &id);
    }

    get_cache()?.set(&points, Duration::from_secs(2))?;

    Ok(())
}

fn get_cache() -> Result<&'static SharedCache, Error> {
    // Not using get_or_init here since initialization can fail.
    METRIC_CACHE
        .get()
        .ok_or_else(|| format_err!("metric cache not initialized"))
}

fn update_disk_metrics(points: &mut MetricDataPoints, disk: &DiskStat, id: &str) {
    if let Some(status) = &disk.usage {
        points.add(Gauge, id, "disk_total", status.total as f64);
        points.add(Gauge, id, "disk_used", status.used as f64);
        points.add(Gauge, id, "disk_available", status.available as f64);
    }

    if let Some(stat) = &disk.dev {
        points.add(Derive, id, "disk_read", (stat.read_sectors * 512) as f64);
        points.add(Derive, id, "disk_write", (stat.write_sectors * 512) as f64);
    }
}

#[derive(Serialize, Deserialize)]
struct MetricDataPoints {
    timestamp: i64,
    datapoints: Vec<MetricDataPoint>,
}

impl MetricDataPoints {
    fn new(timestamp: i64) -> Self {
        Self {
            datapoints: Vec::new(),
            timestamp,
        }
    }

    fn add(&mut self, ty: MetricDataType, id: &str, metric: &str, value: f64) {
        self.datapoints.push(MetricDataPoint {
            id: id.into(),
            metric: metric.into(),
            timestamp: self.timestamp,
            ty,
            value,
        })
    }
}
