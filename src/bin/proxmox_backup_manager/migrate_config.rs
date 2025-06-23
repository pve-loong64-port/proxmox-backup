use anyhow::{bail, Error};
use serde::Deserialize;

use pbs_api_types::{DataStoreConfig, PruneJobConfig, PruneJobOptions};
use pbs_config::prune;

/// Handle a 'migrate-config' command.
pub fn handle_command(command: &str) -> Result<(), Error> {
    match command {
        "update-to-prune-jobs-config" => return update_to_prune_jobs_config(),
        _ => bail!("invalid fixup command: {command}"),
    }
}

/// Migrate a datastore's prune setting to a prune job.
pub(crate) fn update_to_prune_jobs_config() -> Result<(), Error> {
    use pbs_config::datastore;

    let _prune_lock = prune::lock_config()?;
    let _datastore_lock = datastore::lock_config()?;

    let (mut data, _digest) = prune::config()?;
    let (mut storeconfig, _digest) = datastore::config()?;

    for (store, entry) in storeconfig.sections.iter_mut() {
        let ty = &entry.0;

        if ty != "datastore" {
            continue;
        }

        let mut config = match DataStoreConfig::deserialize(&entry.1) {
            Ok(c) => c,
            Err(err) => {
                eprintln!("failed to parse config of store {store}: {err}");
                continue;
            }
        };

        let options = PruneJobOptions {
            keep: std::mem::take(&mut config.keep),
            ..Default::default()
        };

        let schedule = config.prune_schedule.take();

        entry.1 = serde_json::to_value(config)?;

        let schedule = match schedule {
            Some(s) => s,
            None => {
                if options.keeps_something() {
                    eprintln!(
                        "dropping prune job without schedule from datastore '{store}' in datastore.cfg"
                    );
                } else {
                    eprintln!("ignoring empty prune job of datastore '{store}' in datastore.cfg");
                }
                continue;
            }
        };

        let mut id = format!("storeconfig-{store}");
        id.truncate(32);
        if data.sections.contains_key(&id) {
            eprintln!("skipping existing converted prune job for datastore '{store}': {id}");
            continue;
        }

        if !options.keeps_something() {
            eprintln!("dropping empty prune job of datastore '{store}' in datastore.cfg");
            continue;
        }

        let prune_config = PruneJobConfig {
            id: id.clone(),
            store: store.clone(),
            disable: false,
            comment: None,
            schedule,
            options,
        };

        let prune_config = serde_json::to_value(prune_config)?;

        data.sections
            .insert(id, ("prune".to_string(), prune_config));

        eprintln!(
            "migrating prune job of datastore '{store}' from datastore.cfg to prune.cfg jobs"
        );
    }

    prune::save_config(&data)?;
    datastore::save_config(&storeconfig)?;

    Ok(())
}
