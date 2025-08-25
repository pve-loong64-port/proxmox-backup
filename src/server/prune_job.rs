use std::sync::Arc;

use anyhow::Error;
use tracing::{info, warn};

use pbs_api_types::{
    print_store_and_ns, Authid, KeepOptions, Operation, PruneJobOptions, MAX_NAMESPACE_DEPTH,
    PRIV_DATASTORE_MODIFY, PRIV_DATASTORE_PRUNE,
};
use pbs_datastore::prune::{compute_prune_info, PruneMark};
use pbs_datastore::DataStore;
use proxmox_rest_server::WorkerTask;

use crate::backup::ListAccessibleBackupGroups;
use crate::server::jobstate::Job;

pub fn prune_datastore(
    auth_id: Authid,
    prune_options: PruneJobOptions,
    datastore: Arc<DataStore>,
    dry_run: bool,
) -> Result<(), Error> {
    let store = &datastore.name();
    let max_depth = prune_options.max_depth.unwrap_or(MAX_NAMESPACE_DEPTH);
    let depth = match max_depth {
        MAX_NAMESPACE_DEPTH => "down to full depth".to_string(),
        max_depth if max_depth > 0 => format!("to depth {max_depth}"),
        _ => "non-recursive".to_string(),
    };
    let ns = prune_options.ns.clone().unwrap_or_default();
    let store_ns = print_store_and_ns(store, &ns);
    info!("Starting datastore prune on {store_ns}, {depth}");

    if dry_run {
        info!("(dry test run)");
    }

    let keep_all = !prune_options.keeps_something();

    if keep_all {
        info!("No prune selection - keeping all files.");
    } else {
        let rendered_options = cli_prune_options_string(&prune_options);
        info!("retention options: {rendered_options}");
    }

    for group in ListAccessibleBackupGroups::new_with_privs(
        &datastore,
        ns,
        max_depth,
        Some(PRIV_DATASTORE_MODIFY), // overrides the owner check
        Some(PRIV_DATASTORE_PRUNE),  // additionally required if owner
        Some(&auth_id),
    )? {
        let group = group?;
        let ns = group.backup_ns();
        let list = group.list_backups()?;

        let mut prune_info = compute_prune_info(list, &prune_options.keep)?;
        prune_info.reverse(); // delete older snapshots first

        info!(
            "Pruning group {ns}:\"{}/{}\"",
            group.backup_type(),
            group.backup_id()
        );

        for (info, mark) in prune_info {
            let keep = keep_all || mark.keep();
            info!(
                "{}{mark} {}/{}/{}",
                if dry_run { "would " } else { "" },
                group.backup_type(),
                group.backup_id(),
                info.backup_dir.backup_time_string(),
                mark = match mark {
                    PruneMark::Protected => "keep protected",
                    PruneMark::Keep => "keep",
                    PruneMark::KeepPartial => "keep partial",
                    PruneMark::Remove => "remove",
                }
            );
            if !keep && !dry_run {
                if let Err(err) = datastore.remove_backup_dir(ns, info.backup_dir.as_ref(), false) {
                    let path = info.backup_dir.relative_path();
                    warn!("failed to remove dir {path:?}: {err}");
                }
            }
        }
    }

    Ok(())
}

pub(crate) fn cli_prune_options_string(options: &PruneJobOptions) -> String {
    let mut opts = Vec::new();

    if let Some(ns) = &options.ns {
        if !ns.is_root() {
            opts.push(format!("--ns {ns}"));
        }
    }
    if let Some(max_depth) = options.max_depth {
        // FIXME: don't add if it's the default?
        opts.push(format!("--max-depth {max_depth}"));
    }

    cli_keep_options(&mut opts, &options.keep);

    opts.join(" ")
}

pub(crate) fn cli_keep_options(opts: &mut Vec<String>, options: &KeepOptions) {
    for (key, keep) in [
        ("last", options.keep_last),
        ("hourly", options.keep_hourly),
        ("daily", options.keep_daily),
        ("weekly", options.keep_weekly),
        ("monthly", options.keep_monthly),
        ("yearly", options.keep_yearly),
    ] {
        match keep {
            Some(count) if count > 0 => opts.push(format!("--keep-{key} {count}")),
            _ => {}
        };
    }
}

pub fn do_prune_job(
    mut job: Job,
    prune_options: PruneJobOptions,
    store: String,
    auth_id: &Authid,
    schedule: Option<String>,
) -> Result<String, Error> {
    let datastore = DataStore::lookup_datastore(&store, Some(Operation::Write))?;

    let worker_type = job.jobtype().to_string();
    let auth_id = auth_id.clone();
    let worker_id = match &prune_options.ns {
        Some(ns) if ns.is_root() => store.clone(),
        Some(ns) => format!("{store}:{ns}"),
        None => store.clone(),
    };

    let upid_str = WorkerTask::new_thread(
        &worker_type,
        Some(worker_id),
        auth_id.to_string(),
        false,
        move |worker| {
            job.start(&worker.upid().to_string())?;

            info!("prune job '{}'", job.jobname());

            if let Some(event_str) = schedule {
                info!("task triggered by schedule '{event_str}'");
            }

            let result = prune_datastore(auth_id, prune_options, datastore, false);

            let status = worker.create_state(&result);

            if let Err(err) = job.finish(status) {
                eprintln!("could not finish job state for {}: {err}", job.jobtype());
            }

            if let Err(err) = crate::server::send_prune_status(&store, job.jobname(), &result) {
                log::error!("send prune notification failed: {err}");
            }
            result
        },
    )?;
    Ok(upid_str)
}
