use std::sync::Arc;

use anyhow::Error;
use serde_json::{json, Value};

use proxmox_metrics::MetricsData;

use super::{DiskStat, HostStats};

pub async fn send_data_to_metric_servers(
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
