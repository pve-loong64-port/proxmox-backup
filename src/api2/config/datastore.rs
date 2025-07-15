use std::path::{Path, PathBuf};
use std::sync::Arc;

use ::serde::{Deserialize, Serialize};
use anyhow::{bail, format_err, Context, Error};
use hex::FromHex;
use http_body_util::BodyExt;
use serde_json::Value;
use tracing::{info, warn};

use proxmox_router::{http_bail, Permission, Router, RpcEnvironment, RpcEnvironmentType};
use proxmox_s3_client::{S3Client, S3ClientConfig, S3ClientOptions};
use proxmox_schema::{api, param_bail, ApiType};
use proxmox_section_config::SectionConfigData;
use proxmox_uuid::Uuid;

use pbs_api_types::{
    Authid, DataStoreConfig, DataStoreConfigUpdater, DatastoreBackendConfig, DatastoreBackendType,
    DatastoreNotify, DatastoreTuning, KeepOptions, MaintenanceMode, Operation, PruneJobConfig,
    PruneJobOptions, DATASTORE_SCHEMA, PRIV_DATASTORE_ALLOCATE, PRIV_DATASTORE_AUDIT,
    PRIV_DATASTORE_MODIFY, PRIV_SYS_MODIFY, PROXMOX_CONFIG_DIGEST_SCHEMA, UPID_SCHEMA,
};
use pbs_config::s3::S3_CFG_TYPE_ID;
use pbs_config::BackupLockGuard;
use pbs_datastore::chunk_store::ChunkStore;

use crate::api2::admin::{
    datastore::do_mount_device, prune::list_prune_jobs, sync::list_config_sync_jobs,
    verify::list_verification_jobs,
};
use crate::api2::config::prune::{delete_prune_job, do_create_prune_job, has_prune_job};
use crate::api2::config::sync::delete_sync_job;
use crate::api2::config::tape_backup_job::{delete_tape_backup_job, list_tape_backup_jobs};
use crate::api2::config::verify::delete_verification_job;
use pbs_config::CachedUserInfo;

use pbs_datastore::{get_datastore_mount_status, DatastoreBackend};
use proxmox_rest_server::WorkerTask;
use proxmox_s3_client::S3ObjectKey;

use crate::server::jobstate;
use crate::tools::disks::unmount_by_mountpoint;

const S3_DATASTORE_IN_USE_MARKER: &str = ".in-use";

#[derive(Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct InUseContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    hostname: Option<String>,
}

#[api(
    input: {
        properties: {},
    },
    returns: {
        description: "List the configured datastores (with config digest).",
        type: Array,
        items: { type: DataStoreConfig },
    },
    access: {
        permission: &Permission::Anybody,
    },
)]
/// List all datastores
pub fn list_datastores(
    _param: Value,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<DataStoreConfig>, Error> {
    let (config, digest) = pbs_config::datastore::config()?;

    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let user_info = CachedUserInfo::new()?;

    rpcenv["digest"] = hex::encode(digest).into();

    let list: Vec<DataStoreConfig> = config.convert_to_typed_array("datastore")?;
    let filter_by_privs = |store: &DataStoreConfig| {
        let user_privs = user_info.lookup_privs(&auth_id, &["datastore", &store.name]);
        (user_privs & PRIV_DATASTORE_AUDIT) != 0
    };

    Ok(list.into_iter().filter(filter_by_privs).collect())
}

struct UnmountGuard {
    path: Option<PathBuf>,
}

impl UnmountGuard {
    fn new(path: Option<PathBuf>) -> Self {
        UnmountGuard { path }
    }

    fn disable(mut self) {
        self.path = None;
    }
}

impl Drop for UnmountGuard {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            if let Err(e) = unmount_by_mountpoint(path) {
                warn!("could not unmount device: {e}");
            }
        }
    }
}

pub(crate) fn do_create_datastore(
    _lock: BackupLockGuard,
    mut config: SectionConfigData,
    datastore: DataStoreConfig,
    reuse_datastore: bool,
    overwrite_in_use: bool,
) -> Result<(), Error> {
    let path: PathBuf = datastore.absolute_path().into();

    if path.parent().is_none() {
        bail!("cannot create datastore in root path");
    }

    let existing_stores = config.convert_to_typed_array("datastore")?;
    if let Err(err) = datastore.ensure_not_nested(&existing_stores) {
        param_bail!("path", err);
    }

    let tuning: DatastoreTuning = serde_json::from_value(
        DatastoreTuning::API_SCHEMA
            .parse_property_string(datastore.tuning.as_deref().unwrap_or(""))?,
    )?;

    let mut backend_s3_client = None;
    if let Some(ref backend_config) = datastore.backend {
        let backend_config: DatastoreBackendConfig = backend_config.parse()?;
        match backend_config.ty.unwrap_or_default() {
            DatastoreBackendType::Filesystem => (),
            DatastoreBackendType::S3 => {
                let s3_client_id = backend_config
                    .client
                    .as_ref()
                    .ok_or_else(|| format_err!("missing required client"))?;
                let bucket = backend_config
                    .bucket
                    .clone()
                    .ok_or_else(|| format_err!("missing required bucket"))?;
                let (config, _config_digest) =
                    pbs_config::s3::config().context("failed to get s3 config")?;
                let config: S3ClientConfig = config
                    .lookup(S3_CFG_TYPE_ID, s3_client_id)
                    .with_context(|| format!("no '{s3_client_id}' in config"))?;
                let options =
                    S3ClientOptions::from_config(config, bucket, datastore.name.to_owned());
                let s3_client = S3Client::new(options).context("failed to create s3 client")?;
                // Fine to block since this runs in worker task
                proxmox_async::runtime::block_on(s3_client.head_bucket())
                    .context("failed to access bucket")?;

                if !overwrite_in_use {
                    let object_key = S3ObjectKey::try_from(S3_DATASTORE_IN_USE_MARKER)
                        .context("failed to generate s3 object key")?;
                    if let Some(response) =
                        proxmox_async::runtime::block_on(s3_client.get_object(object_key.clone()))
                            .context("failed to get in-use marker from bucket")?
                    {
                        let content = proxmox_async::runtime::block_on(response.content.collect())
                            .unwrap_or_default();
                        let content =
                            String::from_utf8(content.to_bytes().to_vec()).unwrap_or_default();
                        let in_use: InUseContent =
                            serde_json::from_str(&content).unwrap_or_default();
                        if let Some(hostname) = in_use.hostname {
                            bail!("Bucket already contains datastore in use by host {hostname}");
                        } else {
                            bail!("Bucket already contains datastore in use");
                        }
                    }
                }
                backend_s3_client = Some(Arc::new(s3_client));
            }
        }
    }

    let unmount_guard = if datastore.backing_device.is_some() {
        do_mount_device(datastore.clone())?;
        UnmountGuard::new(Some(path.clone()))
    } else {
        UnmountGuard::new(None)
    };

    let chunk_store = if reuse_datastore && backend_s3_client.is_none() {
        ChunkStore::verify_chunkstore(&path).and_then(|_| {
            // Must be the only instance accessing and locking the chunk store,
            // dropping will close all other locks from this process on the lockfile as well.
            ChunkStore::open(
                &datastore.name,
                &path,
                tuning.sync_level.unwrap_or_default(),
            )
        })?
    } else {
        if let Ok(dir) = std::fs::read_dir(&path) {
            for file in dir {
                let name = file?.file_name();
                let name = name.to_str();
                if !name.is_some_and(|name| name.starts_with('.') || name == "lost+found") {
                    bail!("datastore path not empty");
                }
            }
        }
        let backup_user = pbs_config::backup_user()?;
        ChunkStore::create(
            &datastore.name,
            path.clone(),
            backup_user.uid,
            backup_user.gid,
            tuning.sync_level.unwrap_or_default(),
        )?
    };

    if let Some(ref s3_client) = backend_s3_client {
        let object_key = S3ObjectKey::try_from(S3_DATASTORE_IN_USE_MARKER)
            .context("failed to generate s3 object key")?;
        let content = serde_json::to_string(&InUseContent {
            hostname: Some(proxmox_sys::nodename().to_string()),
        })
        .context("failed to encode hostname")?;
        proxmox_async::runtime::block_on(s3_client.put_object(
            object_key,
            hyper::body::Bytes::from(content).into(),
            true,
        ))
        .context("failed to upload in-use marker for datastore")?;
    }

    if tuning.gc_atime_safety_check.unwrap_or(true) {
        chunk_store
            .check_fs_atime_updates(true, backend_s3_client)
            .context("access time safety check failed")?;
        info!("Access time update check successful.");
    } else {
        info!("Access time update check skipped.");
    }

    config.set_data(&datastore.name, "datastore", &datastore)?;

    pbs_config::datastore::save_config(&config)?;

    jobstate::create_state_file("garbage_collection", &datastore.name)?;

    unmount_guard.disable();

    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            config: {
                type: DataStoreConfig,
                flatten: true,
            },
            "reuse-datastore": {
                type: Boolean,
                optional: true,
                default: false,
                description: "Re-use existing datastore directory."
            },
            "overwrite-in-use": {
                type: Boolean,
                optional: true,
                default: false,
                description: "Overwrite in use marker (S3 backed datastores only)."
            },
        },
    },
    access: {
        description: "Requires Datastore.Allocate and, for a backing-device, Sys.Modify on '/system/disks'.",
        permission: &Permission::Privilege(&["datastore"], PRIV_DATASTORE_ALLOCATE, false),
    },
)]
/// Create new datastore config.
pub fn create_datastore(
    config: DataStoreConfig,
    reuse_datastore: bool,
    overwrite_in_use: bool,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<String, Error> {
    let lock = pbs_config::datastore::lock_config()?;

    let (section_config, _digest) = pbs_config::datastore::config()?;

    if section_config.sections.contains_key(&config.name) {
        param_bail!("name", "datastore '{}' already exists.", config.name);
    }

    if config.backing_device.is_none() && !config.path.starts_with("/") {
        param_bail!(
            "path",
            "expected an absolute path, '{}' is not",
            config.path
        );
    }
    if config.backing_device.is_some() && config.path.starts_with("/") {
        param_bail!(
            "path",
            "expected a relative on-device path, '{}' is not",
            config.path
        );
    }

    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let to_stdout = rpcenv.env_type() == RpcEnvironmentType::CLI;

    if config.backing_device.is_some() {
        let user_info = CachedUserInfo::new()?;
        user_info.check_privs(&auth_id, &["system", "disks"], PRIV_SYS_MODIFY, false)?;
    }

    let mut prune_job_config = None;
    if config.keep.keeps_something() || !has_prune_job(&config.name)? {
        prune_job_config = config.prune_schedule.as_ref().map(|schedule| {
            let mut id = format!("default-{}-{}", config.name, Uuid::generate());
            id.truncate(32);

            PruneJobConfig {
                id,
                store: config.name.clone(),
                comment: None,
                disable: false,
                schedule: schedule.clone(),
                options: PruneJobOptions {
                    keep: config.keep.clone(),
                    max_depth: None,
                    ns: None,
                },
            }
        });
    }

    // clearing prune settings in the datastore config, as they are now handled by prune jobs
    let config = DataStoreConfig {
        prune_schedule: None,
        keep: KeepOptions::default(),
        ..config
    };

    let store_name = config.name.to_string();
    WorkerTask::new_thread(
        "create-datastore",
        Some(store_name.clone()),
        auth_id.to_string(),
        to_stdout,
        move |_worker| {
            do_create_datastore(
                lock,
                section_config,
                config,
                reuse_datastore,
                overwrite_in_use,
            )?;

            if let Some(prune_job_config) = prune_job_config {
                do_create_prune_job(prune_job_config)?;
            }

            if reuse_datastore {
                let datastore = pbs_datastore::DataStore::lookup_datastore(
                    &store_name,
                    Some(Operation::Lookup),
                )
                .context("failed to lookup datastore")?;
                match datastore
                    .backend()
                    .context("failed to get datastore backend")?
                {
                    DatastoreBackend::Filesystem => (),
                    DatastoreBackend::S3(_s3_client) => {
                        proxmox_async::runtime::block_on(datastore.s3_refresh())
                            .context("S3 refresh failed")?;
                    }
                }
            }
            Ok(())
        },
    )
}

#[api(
   input: {
        properties: {
            name: {
                schema: DATASTORE_SCHEMA,
            },
        },
    },
    returns: { type: DataStoreConfig },
    access: {
        permission: &Permission::Privilege(&["datastore", "{name}"], PRIV_DATASTORE_AUDIT, false),
    },
)]
/// Read a datastore configuration.
pub fn read_datastore(
    name: String,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<DataStoreConfig, Error> {
    let (config, digest) = pbs_config::datastore::config()?;

    let store_config = config.lookup("datastore", &name)?;
    rpcenv["digest"] = hex::encode(digest).into();

    Ok(store_config)
}

#[api()]
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Deletable property name
pub enum DeletableProperty {
    /// Delete the comment property.
    Comment,
    /// Delete the garbage collection schedule.
    GcSchedule,
    /// Delete the prune job schedule.
    PruneSchedule,
    /// Delete the keep-last property
    KeepLast,
    /// Delete the keep-hourly property
    KeepHourly,
    /// Delete the keep-daily property
    KeepDaily,
    /// Delete the keep-weekly property
    KeepWeekly,
    /// Delete the keep-monthly property
    KeepMonthly,
    /// Delete the keep-yearly property
    KeepYearly,
    /// Delete the verify-new property
    VerifyNew,
    /// Delete the notify-user property
    NotifyUser,
    /// Delete the notify property
    Notify,
    /// Delete the notification-mode property
    NotificationMode,
    /// Delete the tuning property
    Tuning,
    /// Delete the maintenance-mode property
    MaintenanceMode,
}

#[api(
    protected: true,
    input: {
        properties: {
            name: {
                schema: DATASTORE_SCHEMA,
            },
            update: {
                type: DataStoreConfigUpdater,
                flatten: true,
            },
            delete: {
                description: "List of properties to delete.",
                type: Array,
                optional: true,
                items: {
                    type: DeletableProperty,
                }
            },
            digest: {
                optional: true,
                schema: PROXMOX_CONFIG_DIGEST_SCHEMA,
            },
        },
    },
    access: {
        permission: &Permission::Privilege(&["datastore", "{name}"], PRIV_DATASTORE_MODIFY, false),
    },
)]
/// Update datastore config.
pub fn update_datastore(
    update: DataStoreConfigUpdater,
    name: String,
    delete: Option<Vec<DeletableProperty>>,
    digest: Option<String>,
) -> Result<(), Error> {
    let _lock = pbs_config::datastore::lock_config()?;

    // pass/compare digest
    let (mut config, expected_digest) = pbs_config::datastore::config()?;

    if let Some(ref digest) = digest {
        let digest = <[u8; 32]>::from_hex(digest)?;
        crate::tools::detect_modified_configuration_file(&digest, &expected_digest)?;
    }

    let mut data: DataStoreConfig = config.lookup("datastore", &name)?;

    if let Some(delete) = delete {
        for delete_prop in delete {
            match delete_prop {
                DeletableProperty::Comment => {
                    data.comment = None;
                }
                DeletableProperty::GcSchedule => {
                    data.gc_schedule = None;
                }
                DeletableProperty::PruneSchedule => {
                    data.prune_schedule = None;
                }
                DeletableProperty::KeepLast => {
                    data.keep.keep_last = None;
                }
                DeletableProperty::KeepHourly => {
                    data.keep.keep_hourly = None;
                }
                DeletableProperty::KeepDaily => {
                    data.keep.keep_daily = None;
                }
                DeletableProperty::KeepWeekly => {
                    data.keep.keep_weekly = None;
                }
                DeletableProperty::KeepMonthly => {
                    data.keep.keep_monthly = None;
                }
                DeletableProperty::KeepYearly => {
                    data.keep.keep_yearly = None;
                }
                DeletableProperty::VerifyNew => {
                    data.verify_new = None;
                }
                DeletableProperty::Notify => {
                    data.notify = None;
                }
                DeletableProperty::NotifyUser => {
                    data.notify_user = None;
                }
                DeletableProperty::NotificationMode => {
                    data.notification_mode = None;
                }
                DeletableProperty::Tuning => {
                    data.tuning = None;
                }
                DeletableProperty::MaintenanceMode => {
                    data.set_maintenance_mode(None)?;
                }
            }
        }
    }

    if let Some(comment) = update.comment {
        let comment = comment.trim().to_string();
        if comment.is_empty() {
            data.comment = None;
        } else {
            data.comment = Some(comment);
        }
    }

    let mut gc_schedule_changed = false;
    if update.gc_schedule.is_some() {
        gc_schedule_changed = data.gc_schedule != update.gc_schedule;
        data.gc_schedule = update.gc_schedule;
    }

    macro_rules! prune_disabled {
        ($(($param:literal, $($member:tt)+)),+) => {
            $(
                if update.$($member)+.is_some() {
                    param_bail!(
                        $param,
                        "datastore prune settings have been replaced by prune jobs",
                    );
                }
            )+
        };
    }
    prune_disabled! {
        ("keep-last", keep.keep_last),
        ("keep-hourly", keep.keep_hourly),
        ("keep-daily", keep.keep_daily),
        ("keep-weekly", keep.keep_weekly),
        ("keep-monthly", keep.keep_monthly),
        ("keep-yearly", keep.keep_yearly),
        ("prune-schedule", prune_schedule)
    }

    if let Some(notify_str) = update.notify {
        let value = DatastoreNotify::API_SCHEMA.parse_property_string(&notify_str)?;
        let notify: DatastoreNotify = serde_json::from_value(value)?;
        if let DatastoreNotify {
            gc: None,
            verify: None,
            sync: None,
            prune: None,
        } = notify
        {
            data.notify = None;
        } else {
            data.notify = Some(notify_str);
        }
    }
    if update.verify_new.is_some() {
        data.verify_new = update.verify_new;
    }

    if update.notify_user.is_some() {
        data.notify_user = update.notify_user;
    }

    if update.notification_mode.is_some() {
        data.notification_mode = update.notification_mode;
    }

    if update.tuning.is_some() {
        data.tuning = update.tuning;
    }

    let mut maintenance_mode_changed = false;
    if update.maintenance_mode.is_some() {
        maintenance_mode_changed = data.maintenance_mode != update.maintenance_mode;

        let maintenance_mode = match update.maintenance_mode {
            Some(mode_str) => Some(MaintenanceMode::deserialize(
                proxmox_schema::de::SchemaDeserializer::new(mode_str, &MaintenanceMode::API_SCHEMA),
            )?),
            None => None,
        };
        data.set_maintenance_mode(maintenance_mode)?;
    }

    config.set_data(&name, "datastore", &data)?;

    pbs_config::datastore::save_config(&config)?;

    // we want to reset the statefiles, to avoid an immediate action in some cases
    // (e.g. going from monthly to weekly in the second week of the month)
    if gc_schedule_changed {
        jobstate::update_job_last_run_time("garbage_collection", &name)?;
    }

    // tell the proxy it might have to clear a cache entry
    if maintenance_mode_changed {
        tokio::spawn(async move {
            if let Ok(proxy_pid) =
                proxmox_rest_server::read_pid(pbs_buildcfg::PROXMOX_BACKUP_PROXY_PID_FN)
            {
                let sock = proxmox_daemon::command_socket::path_from_pid(proxy_pid);
                let _ = proxmox_daemon::command_socket::send_raw(
                    sock,
                    &format!(
                        "{{\"command\":\"update-datastore-cache\",\"args\":\"{}\"}}\n",
                        &name
                    ),
                )
                .await;
            }
        });
    }

    Ok(())
}

#[api(
    protected: true,
    input: {
        properties: {
            name: {
                schema: DATASTORE_SCHEMA,
            },
            "keep-job-configs": {
                description: "If enabled, the job configurations related to this datastore will be kept.",
                type: bool,
                optional: true,
                default: false,
            },
            "destroy-data": {
                description: "Delete the datastore's underlying contents",
                optional: true,
                type: bool,
                default: false,
            },
            digest: {
                optional: true,
                schema: PROXMOX_CONFIG_DIGEST_SCHEMA,
            },
        },
    },
    access: {
        description: "Requires Datastore.Allocate and, for a backing-device, Sys.Modify on '/system/disks'.",
        permission: &Permission::Privilege(&["datastore", "{name}"], PRIV_DATASTORE_ALLOCATE, false),
    },
    returns: {
        schema: UPID_SCHEMA,
    },
)]
/// Remove a datastore configuration and optionally delete all its contents.
pub async fn delete_datastore(
    name: String,
    keep_job_configs: bool,
    destroy_data: bool,
    digest: Option<String>,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<String, Error> {
    let _lock = pbs_config::datastore::lock_config()?;

    let (config, expected_digest) = pbs_config::datastore::config()?;

    if let Some(ref digest) = digest {
        let digest = <[u8; 32]>::from_hex(digest)?;
        crate::tools::detect_modified_configuration_file(&digest, &expected_digest)?;
    }

    if !config.sections.contains_key(&name) {
        http_bail!(NOT_FOUND, "datastore '{}' does not exist.", name);
    }

    let store_config: DataStoreConfig = config.lookup("datastore", &name)?;

    if store_config.backing_device.is_some() {
        let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
        let user_info = CachedUserInfo::new()?;
        user_info.check_privs(&auth_id, &["system", "disks"], PRIV_SYS_MODIFY, false)?;
    }

    if destroy_data && get_datastore_mount_status(&store_config) == Some(false) {
        http_bail!(
            BAD_REQUEST,
            "cannot destroy data on '{name}' unless the datastore is mounted"
        );
    }

    if !keep_job_configs {
        for job in list_verification_jobs(Some(name.clone()), Value::Null, rpcenv)? {
            delete_verification_job(job.config.id, None, rpcenv)?
        }
        for job in list_config_sync_jobs(Some(name.clone()), None, Value::Null, rpcenv)? {
            delete_sync_job(job.config.id, None, rpcenv)?
        }
        for job in list_prune_jobs(Some(name.clone()), Value::Null, rpcenv)? {
            delete_prune_job(job.config.id, None, rpcenv)?
        }

        let (mut tree, _digest) = pbs_config::acl::config()?;
        tree.delete_node(&format!("/datastore/{}", name));
        pbs_config::acl::save_config(&tree)?;

        let tape_jobs = list_tape_backup_jobs(Value::Null, rpcenv)?;
        for job_config in tape_jobs
            .into_iter()
            .filter(|config| config.setup.store == name)
        {
            delete_tape_backup_job(job_config.id, None, rpcenv)?;
        }
    }

    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let to_stdout = rpcenv.env_type() == RpcEnvironmentType::CLI;
    if let Ok(proxy_pid) = proxmox_rest_server::read_pid(pbs_buildcfg::PROXMOX_BACKUP_PROXY_PID_FN)
    {
        let sock = proxmox_daemon::command_socket::path_from_pid(proxy_pid);
        let _ = proxmox_daemon::command_socket::send_raw(
            sock,
            &format!(
                "{{\"command\":\"update-datastore-cache\",\"args\":\"{}\"}}\n",
                name.clone()
            ),
        )
        .await;
    };

    let upid = WorkerTask::new_thread(
        "delete-datastore",
        Some(name.clone()),
        auth_id.to_string(),
        to_stdout,
        move |_worker| {
            pbs_datastore::DataStore::destroy(&name, destroy_data)?;

            // ignore errors
            let _ = jobstate::remove_state_file("prune", &name);
            let _ = jobstate::remove_state_file("garbage_collection", &name);

            if let Err(err) =
                proxmox_async::runtime::block_on(crate::server::notify_datastore_removed())
            {
                warn!("failed to notify after datastore removal: {err}");
            }

            // cleanup for removable datastores
            //  - unmount
            //  - remove mount dir, if destroy_data
            if store_config.backing_device.is_some() {
                let mount_point = store_config.absolute_path();
                if get_datastore_mount_status(&store_config) == Some(true) {
                    if let Err(e) = unmount_by_mountpoint(Path::new(&mount_point)) {
                        warn!("could not unmount device after deletion: {e}");
                    }
                }
                if destroy_data {
                    if let Err(e) = std::fs::remove_dir(&mount_point) {
                        warn!("could not remove directory after deletion: {e}");
                    }
                }
            }

            Ok(())
        },
    )?;

    Ok(upid)
}

const ITEM_ROUTER: Router = Router::new()
    .get(&API_METHOD_READ_DATASTORE)
    .put(&API_METHOD_UPDATE_DATASTORE)
    .delete(&API_METHOD_DELETE_DATASTORE);

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_LIST_DATASTORES)
    .post(&API_METHOD_CREATE_DATASTORE)
    .match_all("name", &ITEM_ROUTER);
