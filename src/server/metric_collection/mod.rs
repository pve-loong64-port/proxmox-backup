use std::{
    path::Path,
    pin::pin,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Error;
use pbs_api_types::{DataStoreConfig, Operation};
use serde_json::{json, Value};
use tokio::join;

use proxmox_metrics::MetricsData;
use proxmox_sys::{
    fs::FileSystemInformation,
    linux::procfs::{Loadavg, ProcFsMemInfo, ProcFsNetDev, ProcFsStat},
};

use crate::tools::disks::{zfs_dataset_stats, BlockDevStat, DiskManage};

use rrd::{initialize_rrd_cache, rrd_sync_journal, rrd_update_derive, rrd_update_gauge};

pub mod rrd;

/// Initialize the metric collection subsystem.
///
/// Any datapoints in the RRD journal will be committed.
pub fn init() -> Result<(), Error> {
    let rrd_cache = initialize_rrd_cache()?;
    rrd_cache.apply_journal()?;
    Ok(())
}

/// Spawns a tokio task for regular metric collection.
///
/// Every 10 seconds, host and disk stats will be collected and
///   - stored in the RRD
///   - sent to any configured metric servers
pub fn start_collection_task() {
    tokio::spawn(async {
        let abort_future = pin!(proxmox_daemon::shutdown_future());
        let future = pin!(run_stat_generator());
        futures::future::select(future, abort_future).await;
    });
}

async fn run_stat_generator() {
    loop {
        let delay_target = Instant::now() + Duration::from_secs(10);

        let stats_future = tokio::task::spawn_blocking(|| {
            let hoststats = collect_host_stats_sync();
            let (hostdisk, datastores) = collect_disk_stats_sync();
            Arc::new((hoststats, hostdisk, datastores))
        });
        let stats = match stats_future.await {
            Ok(res) => res,
            Err(err) => {
                log::error!("collecting host stats panicked: {err}");
                tokio::time::sleep_until(tokio::time::Instant::from_std(delay_target)).await;
                continue;
            }
        };

        let rrd_future = tokio::task::spawn_blocking({
            let stats = Arc::clone(&stats);
            move || {
                rrd_update_host_stats_sync(&stats.0, &stats.1, &stats.2);
                rrd_sync_journal();
            }
        });

        let metrics_future = send_data_to_metric_servers(stats);

        let (rrd_res, metrics_res) = join!(rrd_future, metrics_future);
        if let Err(err) = rrd_res {
            log::error!("rrd update panicked: {err}");
        }
        if let Err(err) = metrics_res {
            log::error!("error during metrics sending: {err}");
        }

        tokio::time::sleep_until(tokio::time::Instant::from_std(delay_target)).await;
    }
}

async fn send_data_to_metric_servers(
    stats: Arc<(HostStats, DiskStat, Vec<DiskStat>)>,
) -> Result<(), Error> {
    let (config, _digest) = pbs_config::metrics::config()?;
    let channel_list = get_metric_server_connections(config)?;

    if channel_list.is_empty() {
        return Ok(());
    }

    let ctime = proxmox_time::epoch_i64();
    let nodename = proxmox_sys::nodename();

    let mut values = Vec::new();

    let mut cpuvalue = match &stats.0.proc {
        Some(stat) => serde_json::to_value(stat)?,
        None => json!({}),
    };

    if let Some(loadavg) = &stats.0.load {
        cpuvalue["avg1"] = Value::from(loadavg.0);
        cpuvalue["avg5"] = Value::from(loadavg.1);
        cpuvalue["avg15"] = Value::from(loadavg.2);
    }

    values.push(Arc::new(
        MetricsData::new("cpustat", ctime, cpuvalue)?
            .tag("object", "host")
            .tag("host", nodename),
    ));

    if let Some(stat) = &stats.0.meminfo {
        values.push(Arc::new(
            MetricsData::new("memory", ctime, stat)?
                .tag("object", "host")
                .tag("host", nodename),
        ));
    }

    if let Some(netdev) = &stats.0.net {
        for item in netdev {
            values.push(Arc::new(
                MetricsData::new("nics", ctime, item)?
                    .tag("object", "host")
                    .tag("host", nodename)
                    .tag("instance", item.device.clone()),
            ));
        }
    }

    values.push(Arc::new(
        MetricsData::new("blockstat", ctime, stats.1.to_value())?
            .tag("object", "host")
            .tag("host", nodename),
    ));

    for datastore in stats.2.iter() {
        values.push(Arc::new(
            MetricsData::new("blockstat", ctime, datastore.to_value())?
                .tag("object", "host")
                .tag("host", nodename)
                .tag("datastore", datastore.name.clone()),
        ));
    }

    // we must have a concrete functions, because the inferred lifetime from a
    // closure is not general enough for the tokio::spawn call we are in here...
    fn map_fn(item: &(proxmox_metrics::Metrics, String)) -> &proxmox_metrics::Metrics {
        &item.0
    }

    let results =
        proxmox_metrics::send_data_to_channels(&values, channel_list.iter().map(map_fn)).await;
    for (res, name) in results
        .into_iter()
        .zip(channel_list.iter().map(|(_, name)| name))
    {
        if let Err(err) = res {
            log::error!("error sending into channel of {name}: {err}");
        }
    }

    futures::future::join_all(channel_list.into_iter().map(|(channel, name)| async move {
        if let Err(err) = channel.join().await {
            log::error!("error sending to metric server {name}: {err}");
        }
    }))
    .await;

    Ok(())
}

/// Get the metric server connections from a config
fn get_metric_server_connections(
    metric_config: proxmox_section_config::SectionConfigData,
) -> Result<Vec<(proxmox_metrics::Metrics, String)>, Error> {
    let mut res = Vec::new();

    for config in
        metric_config.convert_to_typed_array::<pbs_api_types::InfluxDbUdp>("influxdb-udp")?
    {
        if !config.enable {
            continue;
        }
        let future = proxmox_metrics::influxdb_udp(&config.host, config.mtu);
        res.push((future, config.name));
    }

    for config in
        metric_config.convert_to_typed_array::<pbs_api_types::InfluxDbHttp>("influxdb-http")?
    {
        if !config.enable {
            continue;
        }
        let future = proxmox_metrics::influxdb_http(
            &config.url,
            config.organization.as_deref().unwrap_or("proxmox"),
            config.bucket.as_deref().unwrap_or("proxmox"),
            config.token.as_deref(),
            config.verify_tls.unwrap_or(true),
            config.max_body_size.unwrap_or(25_000_000),
        )?;
        res.push((future, config.name));
    }
    Ok(res)
}

struct HostStats {
    proc: Option<ProcFsStat>,
    meminfo: Option<ProcFsMemInfo>,
    net: Option<Vec<ProcFsNetDev>>,
    load: Option<Loadavg>,
}

struct DiskStat {
    name: String,
    usage: Option<FileSystemInformation>,
    dev: Option<BlockDevStat>,
}

impl DiskStat {
    fn to_value(&self) -> Value {
        let mut value = json!({});
        if let Some(usage) = &self.usage {
            value["total"] = Value::from(usage.total);
            value["used"] = Value::from(usage.used);
            value["avail"] = Value::from(usage.available);
        }

        if let Some(dev) = &self.dev {
            value["read_ios"] = Value::from(dev.read_ios);
            value["read_bytes"] = Value::from(dev.read_sectors * 512);
            value["write_ios"] = Value::from(dev.write_ios);
            value["write_bytes"] = Value::from(dev.write_sectors * 512);
            value["io_ticks"] = Value::from(dev.io_ticks / 1000);
        }
        value
    }
}

fn collect_host_stats_sync() -> HostStats {
    use proxmox_sys::linux::procfs::{
        read_loadavg, read_meminfo, read_proc_net_dev, read_proc_stat,
    };

    let proc = match read_proc_stat() {
        Ok(stat) => Some(stat),
        Err(err) => {
            eprintln!("read_proc_stat failed - {err}");
            None
        }
    };

    let meminfo = match read_meminfo() {
        Ok(stat) => Some(stat),
        Err(err) => {
            eprintln!("read_meminfo failed - {err}");
            None
        }
    };

    let net = match read_proc_net_dev() {
        Ok(netdev) => Some(netdev),
        Err(err) => {
            eprintln!("read_prox_net_dev failed - {err}");
            None
        }
    };

    let load = match read_loadavg() {
        Ok(loadavg) => Some(loadavg),
        Err(err) => {
            eprintln!("read_loadavg failed - {err}");
            None
        }
    };

    HostStats {
        proc,
        meminfo,
        net,
        load,
    }
}

fn collect_disk_stats_sync() -> (DiskStat, Vec<DiskStat>) {
    let disk_manager = DiskManage::new();

    let root = gather_disk_stats(disk_manager.clone(), Path::new("/"), "host");

    let mut datastores = Vec::new();
    match pbs_config::datastore::config() {
        Ok((config, _)) => {
            let datastore_list: Vec<DataStoreConfig> = config
                .convert_to_typed_array("datastore")
                .unwrap_or_default();

            for config in datastore_list {
                if config
                    .get_maintenance_mode()
                    .map_or(false, |mode| mode.check(Some(Operation::Read)).is_err())
                {
                    continue;
                }
                let path = std::path::Path::new(&config.path);
                datastores.push(gather_disk_stats(disk_manager.clone(), path, &config.name));
            }
        }
        Err(err) => {
            eprintln!("read datastore config failed - {err}");
        }
    }

    (root, datastores)
}

fn rrd_update_host_stats_sync(host: &HostStats, hostdisk: &DiskStat, datastores: &[DiskStat]) {
    if let Some(stat) = &host.proc {
        rrd_update_gauge("host/cpu", stat.cpu);
        rrd_update_gauge("host/iowait", stat.iowait_percent);
    }

    if let Some(meminfo) = &host.meminfo {
        rrd_update_gauge("host/memtotal", meminfo.memtotal as f64);
        rrd_update_gauge("host/memused", meminfo.memused as f64);
        rrd_update_gauge("host/swaptotal", meminfo.swaptotal as f64);
        rrd_update_gauge("host/swapused", meminfo.swapused as f64);
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
        rrd_update_derive("host/netin", netin as f64);
        rrd_update_derive("host/netout", netout as f64);
    }

    if let Some(loadavg) = &host.load {
        rrd_update_gauge("host/loadavg", loadavg.0);
    }

    rrd_update_disk_stat(hostdisk, "host");

    for stat in datastores {
        let rrd_prefix = format!("datastore/{}", stat.name);
        rrd_update_disk_stat(stat, &rrd_prefix);
    }
}

fn rrd_update_disk_stat(disk: &DiskStat, rrd_prefix: &str) {
    if let Some(status) = &disk.usage {
        let rrd_key = format!("{}/total", rrd_prefix);
        rrd_update_gauge(&rrd_key, status.total as f64);
        let rrd_key = format!("{}/used", rrd_prefix);
        rrd_update_gauge(&rrd_key, status.used as f64);
        let rrd_key = format!("{}/available", rrd_prefix);
        rrd_update_gauge(&rrd_key, status.available as f64);
    }

    if let Some(stat) = &disk.dev {
        let rrd_key = format!("{}/read_ios", rrd_prefix);
        rrd_update_derive(&rrd_key, stat.read_ios as f64);
        let rrd_key = format!("{}/read_bytes", rrd_prefix);
        rrd_update_derive(&rrd_key, (stat.read_sectors * 512) as f64);

        let rrd_key = format!("{}/write_ios", rrd_prefix);
        rrd_update_derive(&rrd_key, stat.write_ios as f64);
        let rrd_key = format!("{}/write_bytes", rrd_prefix);
        rrd_update_derive(&rrd_key, (stat.write_sectors * 512) as f64);

        let rrd_key = format!("{}/io_ticks", rrd_prefix);
        rrd_update_derive(&rrd_key, (stat.io_ticks as f64) / 1000.0);
    }
}

fn gather_disk_stats(disk_manager: Arc<DiskManage>, path: &Path, name: &str) -> DiskStat {
    let usage = match proxmox_sys::fs::fs_info(path) {
        Ok(status) => Some(status),
        Err(err) => {
            eprintln!("read fs info on {path:?} failed - {err}");
            None
        }
    };

    let dev = match disk_manager.find_mounted_device(path) {
        Ok(None) => None,
        Ok(Some((fs_type, device, source))) => {
            let mut device_stat = None;
            match (fs_type.as_str(), source) {
                ("zfs", Some(source)) => match source.into_string() {
                    Ok(dataset) => match zfs_dataset_stats(&dataset) {
                        Ok(stat) => device_stat = Some(stat),
                        Err(err) => eprintln!("zfs_dataset_stats({dataset:?}) failed - {err}"),
                    },
                    Err(source) => {
                        eprintln!("zfs_pool_stats({source:?}) failed - invalid characters")
                    }
                },
                _ => {
                    if let Ok(disk) = disk_manager.clone().disk_by_dev_num(device.into_dev_t()) {
                        match disk.read_stat() {
                            Ok(stat) => device_stat = stat,
                            Err(err) => eprintln!("disk.read_stat {path:?} failed - {err}"),
                        }
                    }
                }
            }
            device_stat
        }
        Err(err) => {
            eprintln!("find_mounted_device failed - {err}");
            None
        }
    };

    DiskStat {
        name: name.to_string(),
        usage,
        dev,
    }
}
