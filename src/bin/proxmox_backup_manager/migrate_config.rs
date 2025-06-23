use anyhow::{bail, Error};
use serde::Deserialize;

use pbs_api_types::{
    DataStoreConfig, NotificationMode, PruneJobConfig, PruneJobOptions, TapeBackupJobConfig,
};
use pbs_config::prune;

/// Handle a 'migrate-config' command.
pub fn handle_command(command: &str) -> Result<(), Error> {
    match command {
        "update-to-prune-jobs-config" => update_to_prune_jobs_config(),
        "default-notification-mode" => {
            migrate_tape_job_notification_mode()?;
            migrate_datastore_notification_mode_default()?;
            Ok(())
        }
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

/// Explicitly set 'notification-mode' to 'legacy-sendmail' for any datastore which
/// relied on the previous default of 'legacy-sendmail'.
///
/// This allows us to change the default to 'notification-system' without any noticeable
/// change in behavior.
fn migrate_datastore_notification_mode_default() -> Result<(), Error> {
    let _lock = pbs_config::datastore::lock_config()?;

    let (mut datastore_config, _digest) = pbs_config::datastore::config()?;

    for (store, entry) in datastore_config.sections.iter_mut() {
        let mut config = match DataStoreConfig::deserialize(&entry.1) {
            Ok(c) => c,
            Err(err) => {
                eprintln!("failed to parse config of store {store}: {err}");
                continue;
            }
        };

        if config.notification_mode.is_none() {
            config.notification_mode = Some(NotificationMode::LegacySendmail);
            eprintln!("setting notification-mode of datastore '{store}' to 'legacy-sendmail' to presere previous default behavior.");
        }

        entry.1 = serde_json::to_value(config)?;
    }

    pbs_config::datastore::save_config(&datastore_config)?;

    Ok(())
}

/// Explicitly set 'notification-mode' to 'legacy-sendmail' for any tape backup job which
/// relied on the previous default of 'legacy-sendmail'.
///
/// This allows us to change the default to 'notification-system' without any noticeable
/// change in behavior.
fn migrate_tape_job_notification_mode() -> Result<(), Error> {
    let _lock = pbs_config::tape_job::lock()?;

    let (mut tapejob_config, _digest) = pbs_config::tape_job::config()?;

    for (job_id, entry) in tapejob_config.sections.iter_mut() {
        let mut config = match TapeBackupJobConfig::deserialize(&entry.1) {
            Ok(c) => c,
            Err(err) => {
                eprintln!("failed to parse config of tape-backup job {job_id}: {err}");
                continue;
            }
        };

        if config.setup.notification_mode.is_none() {
            config.setup.notification_mode = Some(NotificationMode::LegacySendmail);
            eprintln!(
                "setting notification-mode of tape backup job '{job_id}' to 'legacy-sendmail' to preserve previous default behavior."
            );
        }

        entry.1 = serde_json::to_value(config)?;
    }

    pbs_config::tape_job::save_config(&tapejob_config)?;

    Ok(())
}
