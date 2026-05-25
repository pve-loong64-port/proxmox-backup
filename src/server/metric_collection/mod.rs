use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::Path;
use std::pin::pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Error, format_err};
use hyper::Method;
use tokio::join;

use pbs_api_types::{
    DataStoreConfig, DatastoreBackendConfig, DatastoreBackendType, Operation, S3Statistics,
};
use proxmox_lang::try_block;
use proxmox_network_api::{IpLink, get_network_interfaces};
use proxmox_s3_client::SharedRequestCounters;
use proxmox_sys::fs::FileSystemInformation;
use proxmox_sys::linux::procfs::{Loadavg, ProcFsMemInfo, ProcFsNetDev, ProcFsStat};

use crate::tools::disks::{BlockDevStat, DiskManage, zfs_dataset_stats};
use pbs_datastore::S3_CLIENT_REQUEST_COUNTER_BASE_PATH;

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

struct DatastoreStats {
    disk: DiskStat,
    s3_stats: Option<S3Statistics>,
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

static S3_REQUEST_COUNTERS_MAP: LazyLock<Mutex<HashMap<String, SharedRequestCounters>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn collect_s3_stats(
    store: &str,
    backend_config: &DatastoreBackendConfig,
) -> Result<Option<S3Statistics>, Error> {
    let endpoint_id = backend_config
        .client
        .as_ref()
        .ok_or(format_err!("missing s3 endpoint id"))?;
    let bucket = backend_config
        .bucket
        .as_ref()
        .ok_or(format_err!("missing s3 bucket name"))?;
    let path =
        format!("{S3_CLIENT_REQUEST_COUNTER_BASE_PATH}/{endpoint_id}-{bucket}-{store}.shmem");

    let mut counters = S3_REQUEST_COUNTERS_MAP.lock().unwrap();
    let s3_stats = match counters.entry(path.clone()) {
        Entry::Occupied(o) => load_s3_statistics(o.get()),
        Entry::Vacant(v) => {
            let user = pbs_config::backup_user()?;
            let counters = SharedRequestCounters::open_shared_memory_mapped(path, user)?;
            let s3_stats = load_s3_statistics(&counters);
            v.insert(counters);
            s3_stats
        }
    };

    Ok(Some(s3_stats))
}

fn load_s3_statistics(counters: &SharedRequestCounters) -> S3Statistics {
    S3Statistics {
        get: counters.load(Method::GET, Ordering::Acquire),
        put: counters.load(Method::PUT, Ordering::Acquire),
        post: counters.load(Method::POST, Ordering::Acquire),
        delete: counters.load(Method::DELETE, Ordering::Acquire),
        head: counters.load(Method::HEAD, Ordering::Acquire),
        uploaded: counters.get_upload_traffic(Ordering::Acquire),
        downloaded: counters.get_download_traffic(Ordering::Acquire),
    }
}

fn collect_disk_stats_sync() -> (DiskStat, Vec<DatastoreStats>) {
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
                    .is_some_and(|mode| mode.check(Operation::Read).is_err())
                {
                    continue;
                }

                if pbs_datastore::get_datastore_mount_status(&config) == Some(false) {
                    continue;
                }

                let s3_stats: Option<S3Statistics> = try_block!({
                    let backend_config = pbs_config::datastore::parse_backend_config(&config)?;

                    if backend_config.ty.unwrap_or_default() == DatastoreBackendType::S3 {
                        collect_s3_stats(&config.name, &backend_config)
                    } else {
                        Ok(None)
                    }
                })
                .unwrap_or_else(|err: Error| {
                    eprintln!("parsing datastore backend config failed - {err}");
                    None
                });

                let disk = gather_disk_stats(
                    disk_manager.clone(),
                    Path::new(&config.absolute_path()),
                    &config.name,
                );

                datastores.push(DatastoreStats { disk, s3_stats });
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
