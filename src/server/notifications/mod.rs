use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::Error;
use const_format::concatcp;
use nix::unistd::Uid;

use proxmox_notify::context::pbs::PBS_CONTEXT;
use proxmox_schema::ApiType;
use proxmox_sys::fs::{create_path, CreateOptions};

use crate::tape::TapeNotificationMode;
use pbs_api_types::{
    APTUpdateInfo, DataStoreConfig, DatastoreNotify, GarbageCollectionStatus, NotificationMode,
    Notify, SyncJobConfig, TapeBackupJobSetup, User, Userid, VerificationJobConfig,
};
use proxmox_notify::endpoints::sendmail::{SendmailConfig, SendmailEndpoint};
use proxmox_notify::{Endpoint, Notification, Severity};

const SPOOL_DIR: &str = concatcp!(pbs_buildcfg::PROXMOX_BACKUP_STATE_DIR, "/notifications");

mod template_data;

use template_data::{
    AcmeErrTemplateData, CommonData, GcErrTemplateData, GcOkTemplateData,
    PackageUpdatesTemplateData, PruneErrTemplateData, PruneOkTemplateData, SyncErrTemplateData,
    SyncOkTemplateData, TapeBackupErrTemplateData, TapeBackupOkTemplateData, TapeLoadTemplateData,
    VerifyErrTemplateData, VerifyOkTemplateData,
};

/// Initialize the notification system by setting context in proxmox_notify
pub fn init() -> Result<(), Error> {
    proxmox_notify::context::set_context(&PBS_CONTEXT);
    Ok(())
}

/// Create the directory which will be used to temporarily store notifications
/// which were sent from an unprivileged process.
pub fn create_spool_dir() -> Result<(), Error> {
    let backup_user = pbs_config::backup_user()?;
    let opts = CreateOptions::new()
        .owner(backup_user.uid)
        .group(backup_user.gid);

    create_path(SPOOL_DIR, None, Some(opts))?;
    Ok(())
}

async fn send_queued_notifications() -> Result<(), Error> {
    let mut read_dir = tokio::fs::read_dir(SPOOL_DIR).await?;

    let mut notifications = Vec::new();

    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();

        if let Some(ext) = path.extension() {
            if ext == "json" {
                let p = path.clone();

                let bytes = tokio::fs::read(p).await?;
                let notification: Notification = serde_json::from_slice(&bytes)?;
                notifications.push(notification);

                // Currently, there is no retry-mechanism in case of failure...
                // For retries, we'd have to keep track of which targets succeeded/failed
                // to send, so we do not retry notifying a target which succeeded before.
                tokio::fs::remove_file(path).await?;
            }
        }
    }

    // Make sure that we send the oldest notification first
    notifications.sort_unstable_by_key(|n| n.timestamp());

    let res = tokio::task::spawn_blocking(move || {
        let config = pbs_config::notifications::config()?;
        for notification in notifications {
            if let Err(err) = proxmox_notify::api::common::send(&config, &notification) {
                log::error!("failed to send notification: {err}");
            }
        }

        Ok::<(), Error>(())
    })
    .await?;

    if let Err(e) = res {
        log::error!("could not read notification config: {e}");
    }

    Ok::<(), Error>(())
}

/// Worker task to periodically send any queued notifications.
pub async fn notification_worker() {
    loop {
        let delay_target = Instant::now() + Duration::from_secs(5);

        if let Err(err) = send_queued_notifications().await {
            log::error!("notification worker task error: {err}");
        }

        tokio::time::sleep_until(tokio::time::Instant::from_std(delay_target)).await;
    }
}

fn send_notification(notification: Notification) -> Result<(), Error> {
    if nix::unistd::ROOT == Uid::current() {
        let config = pbs_config::notifications::config()?;
        proxmox_notify::api::common::send(&config, &notification)?;
    } else {
        let ser = serde_json::to_vec(&notification)?;
        let path = Path::new(SPOOL_DIR).join(format!("{id}.json", id = notification.id()));

        let backup_user = pbs_config::backup_user()?;
        let opts = CreateOptions::new()
            .owner(backup_user.uid)
            .group(backup_user.gid);
        proxmox_sys::fs::replace_file(path, &ser, opts, true)?;
        log::info!("queued notification (id={id})", id = notification.id())
    }

    Ok(())
}

fn send_sendmail_legacy_notification(notification: Notification, email: &str) -> Result<(), Error> {
    let endpoint = SendmailEndpoint {
        config: SendmailConfig {
            mailto: vec![email.into()],
            ..Default::default()
        },
    };

    endpoint.send(&notification)?;

    Ok(())
}

/// Summary of a successful Tape Job
#[derive(Default)]
pub struct TapeBackupJobSummary {
    /// The list of snaphots backed up
    pub snapshot_list: Vec<String>,
    /// The total time of the backup job
    pub duration: std::time::Duration,
    /// The labels of the used tapes of the backup job
    pub used_tapes: Option<Vec<String>>,
}

pub fn send_gc_status(
    datastore: &str,
    status: &GarbageCollectionStatus,
    result: &Result<(), Error>,
) -> Result<(), Error> {
    let metadata = HashMap::from([
        ("datastore".into(), datastore.into()),
        ("hostname".into(), proxmox_sys::nodename().into()),
        ("type".into(), "gc".into()),
    ]);

    let notification = match result {
        Ok(()) => {
            let template_data = GcOkTemplateData::new(datastore.to_string(), status);
            Notification::from_template(
                Severity::Info,
                "gc-ok",
                serde_json::to_value(template_data)?,
                metadata,
            )
        }
        Err(err) => {
            let template_data = GcErrTemplateData::new(datastore.to_string(), format!("{err:#}"));
            Notification::from_template(
                Severity::Error,
                "gc-err",
                serde_json::to_value(template_data)?,
                metadata,
            )
        }
    };

    let (email, notify, mode) = lookup_datastore_notify_settings(datastore);
    match mode {
        NotificationMode::LegacySendmail => {
            let notify = notify.gc.unwrap_or(Notify::Always);

            if notify == Notify::Never || (result.is_ok() && notify == Notify::Error) {
                return Ok(());
            }

            if let Some(email) = email {
                send_sendmail_legacy_notification(notification, &email)?;
            }
        }
        NotificationMode::NotificationSystem => {
            send_notification(notification)?;
        }
    }

    Ok(())
}

pub fn send_verify_status(
    job: VerificationJobConfig,
    result: &Result<Vec<String>, Error>,
) -> Result<(), Error> {
    let metadata = HashMap::from([
        ("job-id".into(), job.id.clone()),
        ("datastore".into(), job.store.clone()),
        ("hostname".into(), proxmox_sys::nodename().into()),
        ("type".into(), "verify".into()),
    ]);

    let notification = match result {
        Err(_) => {
            // aborted job - do not send any notification
            return Ok(());
        }
        Ok(errors) if errors.is_empty() => {
            let template_data = VerifyOkTemplateData {
                common: CommonData::new(),
                datastore: job.store.clone(),
                job_id: job.id.clone(),
            };
            Notification::from_template(
                Severity::Info,
                "verify-ok",
                serde_json::to_value(template_data)?,
                metadata,
            )
        }
        Ok(errors) => {
            let template_data = VerifyErrTemplateData {
                common: CommonData::new(),
                datastore: job.store.clone(),
                job_id: job.id.clone(),
                failed_snapshot_list: errors.clone(),
            };
            Notification::from_template(
                Severity::Error,
                "verify-err",
                serde_json::to_value(template_data)?,
                metadata,
            )
        }
    };

    let (email, notify, mode) = lookup_datastore_notify_settings(&job.store);
    match mode {
        NotificationMode::LegacySendmail => {
            let notify = notify.verify.unwrap_or(Notify::Always);

            if notify == Notify::Never || (result.is_ok() && notify == Notify::Error) {
                return Ok(());
            }

            if let Some(email) = email {
                send_sendmail_legacy_notification(notification, &email)?;
            }
        }
        NotificationMode::NotificationSystem => {
            send_notification(notification)?;
        }
    }

    Ok(())
}

pub fn send_prune_status(
    store: &str,
    jobname: &str,
    result: &Result<(), Error>,
) -> Result<(), Error> {
    let metadata = HashMap::from([
        ("job-id".into(), jobname.to_string()),
        ("datastore".into(), store.into()),
        ("hostname".into(), proxmox_sys::nodename().into()),
        ("type".into(), "prune".into()),
    ]);

    let notification = match result {
        Ok(()) => {
            let template_data = PruneOkTemplateData {
                common: CommonData::new(),
                datastore: store.to_string(),
                job_id: jobname.to_string(),
            };

            Notification::from_template(
                Severity::Info,
                "prune-ok",
                serde_json::to_value(template_data)?,
                metadata,
            )
        }
        Err(err) => {
            let template_data = PruneErrTemplateData {
                common: CommonData::new(),
                datastore: store.to_string(),
                job_id: jobname.to_string(),
                error: format!("{err:#}"),
            };

            Notification::from_template(
                Severity::Error,
                "prune-err",
                serde_json::to_value(template_data)?,
                metadata,
            )
        }
    };

    let (email, notify, mode) = lookup_datastore_notify_settings(store);
    match mode {
        NotificationMode::LegacySendmail => {
            let notify = notify.prune.unwrap_or(Notify::Error);

            if notify == Notify::Never || (result.is_ok() && notify == Notify::Error) {
                return Ok(());
            }

            if let Some(email) = email {
                send_sendmail_legacy_notification(notification, &email)?;
            }
        }
        NotificationMode::NotificationSystem => {
            send_notification(notification)?;
        }
    }

    Ok(())
}

pub fn send_sync_status(job: &SyncJobConfig, result: &Result<(), Error>) -> Result<(), Error> {
    let metadata = HashMap::from([
        ("job-id".into(), job.id.clone()),
        ("datastore".into(), job.store.clone()),
        ("hostname".into(), proxmox_sys::nodename().into()),
        ("type".into(), "sync".into()),
    ]);

    let notification = match result {
        Ok(()) => {
            let template_data = SyncOkTemplateData {
                common: CommonData::new(),
                datastore: job.store.clone(),
                job_id: job.id.clone(),
                remote: job.remote.clone(),
                remote_datastore: job.remote_store.clone(),
            };
            Notification::from_template(
                Severity::Info,
                "sync-ok",
                serde_json::to_value(template_data)?,
                metadata,
            )
        }
        Err(err) => {
            let template_data = SyncErrTemplateData {
                common: CommonData::new(),
                datastore: job.store.clone(),
                job_id: job.id.clone(),
                remote: job.remote.clone(),
                remote_datastore: job.remote_store.clone(),
                error: format!("{err:#}"),
            };
            Notification::from_template(
                Severity::Error,
                "sync-err",
                serde_json::to_value(template_data)?,
                metadata,
            )
        }
    };

    let (email, notify, mode) = lookup_datastore_notify_settings(&job.store);
    match mode {
        NotificationMode::LegacySendmail => {
            let notify = notify.sync.unwrap_or(Notify::Always);

            if notify == Notify::Never || (result.is_ok() && notify == Notify::Error) {
                return Ok(());
            }

            if let Some(email) = email {
                send_sendmail_legacy_notification(notification, &email)?;
            }
        }
        NotificationMode::NotificationSystem => {
            send_notification(notification)?;
        }
    }

    Ok(())
}

pub fn send_tape_backup_status(
    id: Option<&str>,
    job: &TapeBackupJobSetup,
    result: &Result<(), Error>,
    summary: TapeBackupJobSummary,
) -> Result<(), Error> {
    let mut metadata = HashMap::from([
        ("datastore".into(), job.store.clone()),
        ("media-pool".into(), job.pool.clone()),
        ("hostname".into(), proxmox_sys::nodename().into()),
        ("type".into(), "tape-backup".into()),
    ]);

    if let Some(id) = id {
        metadata.insert("job-id".into(), id.into());
    }

    let duration = summary.duration.as_secs();

    let notification = match result {
        Ok(()) => {
            let template_data = TapeBackupOkTemplateData {
                common: CommonData::new(),
                datastore: job.store.clone(),
                job_id: id.map(|id| id.into()),
                job_duration: duration,
                tape_pool: job.pool.clone(),
                tape_drive: job.drive.clone(),
                used_tapes_list: summary.used_tapes.unwrap_or_default(),
                snapshot_list: summary.snapshot_list,
            };

            Notification::from_template(
                Severity::Info,
                "tape-backup-ok",
                serde_json::to_value(template_data)?,
                metadata,
            )
        }
        Err(err) => {
            let template_data = TapeBackupErrTemplateData {
                common: CommonData::new(),
                datastore: job.store.clone(),
                job_id: id.map(|id| id.into()),
                job_duration: duration,
                tape_pool: job.pool.clone(),
                tape_drive: job.drive.clone(),
                used_tapes_list: summary.used_tapes.unwrap_or_default(),
                snapshot_list: summary.snapshot_list,
                error: format!("{err:#}"),
            };

            Notification::from_template(
                Severity::Error,
                "tape-backup-err",
                serde_json::to_value(template_data)?,
                metadata,
            )
        }
    };

    let mode = TapeNotificationMode::from(job);

    match &mode {
        TapeNotificationMode::LegacySendmail { notify_user } => {
            let email = lookup_user_email(notify_user);

            if let Some(email) = email {
                send_sendmail_legacy_notification(notification, &email)?;
            }
        }
        TapeNotificationMode::NotificationSystem => {
            send_notification(notification)?;
        }
    }

    Ok(())
}

/// Send email to a person to request a manual media change
pub fn send_load_media_notification(
    mode: &TapeNotificationMode,
    changer: bool,
    device: &str,
    label_text: &str,
    reason: Option<String>,
) -> Result<(), Error> {
    let metadata = HashMap::from([
        ("hostname".into(), proxmox_sys::nodename().into()),
        ("type".into(), "tape-load".into()),
    ]);

    let device_type = if changer { "changer" } else { "drive" };

    let template_data = TapeLoadTemplateData {
        common: CommonData::new(),
        load_reason: reason,
        tape_drive: device.into(),
        drive_type: device_type.into(),
        drive_is_changer: changer,
        tape_label: label_text.into(),
    };

    let notification = Notification::from_template(
        Severity::Notice,
        "tape-load",
        serde_json::to_value(template_data)?,
        metadata,
    );

    match mode {
        TapeNotificationMode::LegacySendmail { notify_user } => {
            let email = lookup_user_email(notify_user);

            if let Some(email) = email {
                send_sendmail_legacy_notification(notification, &email)?;
            }
        }
        TapeNotificationMode::NotificationSystem => {
            send_notification(notification)?;
        }
    }

    Ok(())
}

pub fn send_updates_available(updates: &[&APTUpdateInfo]) -> Result<(), Error> {
    let hostname = proxmox_sys::nodename().to_string();

    let metadata = HashMap::from([
        ("hostname".into(), hostname),
        ("type".into(), "package-updates".into()),
    ]);

    let template_data = PackageUpdatesTemplateData::new(updates);

    let notification = Notification::from_template(
        Severity::Info,
        "package-updates",
        serde_json::to_value(template_data)?,
        metadata,
    );

    send_notification(notification)?;
    Ok(())
}

/// send email on certificate renewal failure.
pub fn send_certificate_renewal_mail(result: &Result<(), Error>) -> Result<(), Error> {
    let error: String = match result {
        Err(e) => format!("{e:#}"),
        _ => return Ok(()),
    };

    let metadata = HashMap::from([
        ("hostname".into(), proxmox_sys::nodename().into()),
        ("type".into(), "acme".into()),
    ]);

    let template_data = AcmeErrTemplateData {
        common: CommonData::new(),
        error,
    };

    let notification = Notification::from_template(
        Severity::Info,
        "acme-err",
        serde_json::to_value(template_data)?,
        metadata,
    );

    send_notification(notification)?;
    Ok(())
}

/// Lookup users email address
pub fn lookup_user_email(userid: &Userid) -> Option<String> {
    if let Ok(user_config) = pbs_config::user::cached_config() {
        if let Ok(user) = user_config.lookup::<User>("user", userid.as_str()) {
            return user.email;
        }
    }

    None
}

/// Lookup Datastore notify settings
pub fn lookup_datastore_notify_settings(
    store: &str,
) -> (Option<String>, DatastoreNotify, NotificationMode) {
    let mut email = None;

    let notify = DatastoreNotify {
        gc: None,
        verify: None,
        sync: None,
        prune: None,
    };

    let (config, _digest) = match pbs_config::datastore::config() {
        Ok(result) => result,
        Err(_) => return (email, notify, NotificationMode::default()),
    };

    let config: DataStoreConfig = match config.lookup("datastore", store) {
        Ok(result) => result,
        Err(_) => return (email, notify, NotificationMode::default()),
    };

    email = match config.notify_user {
        Some(ref userid) => lookup_user_email(userid),
        None => lookup_user_email(Userid::root_userid()),
    };

    let notification_mode = config.notification_mode.unwrap_or_default();
    let notify_str = config.notify.unwrap_or_default();

    if let Ok(value) = DatastoreNotify::API_SCHEMA.parse_property_string(&notify_str) {
        if let Ok(notify) = serde_json::from_value(value) {
            return (email, notify, notification_mode);
        }
    }

    (email, notify, notification_mode)
}
