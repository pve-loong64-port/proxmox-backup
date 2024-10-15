use std::{
    path::Path,
    pin::pin,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Error;
use pbs_api_types::{DataStoreConfig, Operation};
use tokio::join;

use proxmox_sys::{
    fs::FileSystemInformation,
    linux::procfs::{Loadavg, ProcFsMemInfo, ProcFsNetDev, ProcFsStat},
};

use crate::tools::disks::{zfs_dataset_stats, BlockDevStat, DiskManage};

mod metric_server;
pub mod rrd;

/// Initialize the metric collection subsystem.
///
/// Any datapoints in the RRD journal will be committed.
pub fn init() -> Result<(), Error> {
    let rrd_cache = rrd::init()?;
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
                rrd::update_metrics(&stats.0, &stats.1, &stats.2);
                rrd::sync_journal();
            }
        });

        let metrics_future = metric_server::send_data_to_metric_servers(stats);

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
                let path = Path::new(&config.path);
                datastores.push(gather_disk_stats(disk_manager.clone(), path, &config.name));
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
