use std::{
    collections::HashMap,
    path::Path,
    pin::pin,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use anyhow::Error;
use tokio::join;

use pbs_api_types::{DataStoreConfig, Operation};
use proxmox_network_api::{get_network_interfaces, IpLink};
use proxmox_sys::{
    fs::FileSystemInformation,
    linux::procfs::{Loadavg, ProcFsMemInfo, ProcFsNetDev, ProcFsStat},
};

use crate::tools::disks::{zfs_dataset_stats, BlockDevStat, DiskManage};

mod metric_server;
pub(crate) mod pull_metrics;
pub(crate) mod rrd;

const METRIC_COLLECTION_INTERVAL: Duration = Duration::from_secs(10);

/// Initialize the metric collection subsystem.
///
/// Any datapoints in the RRD journal will be committed.
pub fn init() -> Result<(), Error> {
    let rrd_cache = rrd::init()?;
    rrd_cache.apply_journal()?;

    pull_metrics::init()?;

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
        let delay_target = Instant::now() + METRIC_COLLECTION_INTERVAL;

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
                rrd::update_metrics(&stats.0, &stats.1, &stats.2);
                rrd::sync_journal();
            }
        });
        let pull_metric_future = tokio::task::spawn_blocking({
            let stats = Arc::clone(&stats);
            move || {
                pull_metrics::update_metrics(&stats.0, &stats.1, &stats.2)?;
                Ok::<(), Error>(())
            }
        });

        let metrics_future = metric_server::send_data_to_metric_servers(stats);

        let (rrd_res, metrics_res, pull_metrics_res) =
            join!(rrd_future, metrics_future, pull_metric_future);
        if let Err(err) = rrd_res {
            log::error!("rrd update panicked: {err}");
        }
        if let Err(err) = metrics_res {
            log::error!("error during metrics sending: {err}");
        }
        if let Err(err) = pull_metrics_res {
            log::error!("error caching pull-style metrics: {err}");
        }

        tokio::time::sleep_until(tokio::time::Instant::from_std(delay_target)).await;
    }
}

struct HostStats {
    proc: Option<ProcFsStat>,
    meminfo: Option<ProcFsMemInfo>,
    net: Option<Vec<NetdevStat>>,
    load: Option<Loadavg>,
}

struct DiskStat {
    name: String,
    usage: Option<FileSystemInformation>,
    dev: Option<BlockDevStat>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
enum NetdevType {
    Physical,
    Virtual,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
struct NetdevStat {
    pub device: String,
    pub receive: u64,
    pub send: u64,
    pub ty: NetdevType,
}

impl NetdevStat {
    fn from_fs_net_dev(net_dev: ProcFsNetDev, ty: NetdevType) -> Self {
        Self {
            device: net_dev.device,
            receive: net_dev.receive,
            send: net_dev.send,
            ty,
        }
    }
}

static NETWORK_INTERFACE_CACHE: OnceLock<HashMap<String, IpLink>> = OnceLock::new();

fn collect_netdev_stats() -> Option<Vec<NetdevStat>> {
    use proxmox_sys::linux::procfs::read_proc_net_dev;

    let net_devs = match read_proc_net_dev() {
        Ok(net_devs) => net_devs,
        Err(err) => {
            eprintln!("read_prox_net_dev failed - {err}");
            return None;
        }
    };

    let ip_links = match NETWORK_INTERFACE_CACHE.get() {
        Some(ip_links) => ip_links,
        None => match get_network_interfaces() {
            Ok(network_interfaces) => {
                let _ = NETWORK_INTERFACE_CACHE.set(network_interfaces);
                NETWORK_INTERFACE_CACHE.get().unwrap()
            }
            Err(err) => {
                eprintln!("get_network_interfaces failed - {err}");
                return None;
            }
        },
    };

    let mut stat_devs = Vec::with_capacity(net_devs.len());

    for net_dev in net_devs {
        if let Some(ip_link) = ip_links.get(&net_dev.device) {
            let ty = if ip_link.is_physical() {
                NetdevType::Physical
            } else {
                NetdevType::Virtual
            };

            stat_devs.push(NetdevStat::from_fs_net_dev(net_dev, ty));
        }
    }

    Some(stat_devs)
}

fn collect_host_stats_sync() -> HostStats {
    use proxmox_sys::linux::procfs::{read_loadavg, read_meminfo, read_proc_stat};

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

    let net = collect_netdev_stats();

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
                    .is_some_and(|mode| mode.check(Some(Operation::Read)).is_err())
                {
                    continue;
                }

                if pbs_datastore::get_datastore_mount_status(&config) == Some(false) {
                    continue;
                }

                datastores.push(gather_disk_stats(
                    disk_manager.clone(),
                    Path::new(&config.absolute_path()),
                    &config.name,
                ));
            }
        }
        Err(err) => {
            eprintln!("read datastore config failed - {err}");
        }
    }

    (root, datastores)
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
