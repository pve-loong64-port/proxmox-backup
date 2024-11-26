//! Datastore Synchronization Job Management

use anyhow::{bail, format_err, Error};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use proxmox_router::{
    list_subdirs_api_method, ApiMethod, Permission, Router, RpcEnvironment, RpcEnvironmentType,
    SubdirMap,
};
use proxmox_schema::api;
use proxmox_sortable_macro::sortable;

use pbs_api_types::{
    Authid, SyncDirection, SyncJobConfig, SyncJobStatus, DATASTORE_SCHEMA, JOB_ID_SCHEMA,
};
use pbs_config::sync;
use pbs_config::CachedUserInfo;

use crate::{
    api2::config::sync::{check_sync_job_modify_access, check_sync_job_read_access},
    server::jobstate::{compute_schedule_status, Job, JobState},
    server::sync::do_sync_job,
};

// FIXME: 4.x make 'all' the default
#[api()]
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// The direction of the listed sync jobs: push, pull or all.
pub enum ListSyncDirection {
    /// All directions
    All,
    /// Sync direction push
    Push,
    /// Sync direction pull
    #[default]
    Pull,
}

impl From<SyncDirection> for ListSyncDirection {
    fn from(value: SyncDirection) -> Self {
        match value {
            SyncDirection::Pull => ListSyncDirection::Pull,
            SyncDirection::Push => ListSyncDirection::Push,
        }
    }
}

impl ListSyncDirection {
    /// Checks whether a `ListSyncDirection` matches a given `SyncDirection`
    pub fn matches(&self, other: SyncDirection) -> bool {
        if *self == ListSyncDirection::All {
            return true;
        }
        *self == other.into()
    }
}

#[api(
    input: {
        properties: {
            store: {
                schema: DATASTORE_SCHEMA,
                optional: true,
            },
            "sync-direction": {
                type: ListSyncDirection,
                optional: true,
            },
        },
    },
    returns: {
        description: "List configured jobs and their status.",
        type: Array,
        items: { type: SyncJobStatus },
    },
    access: {
        description: "Limited to sync jobs where user has Datastore.Audit on target datastore, and Remote.Audit on source remote.",
        permission: &Permission::Anybody,
    },
)]
/// List all configured sync jobs
pub fn list_config_sync_jobs(
    store: Option<String>,
    sync_direction: Option<ListSyncDirection>,
    _param: Value,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<Vec<SyncJobStatus>, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let user_info = CachedUserInfo::new()?;

    let (config, digest) = sync::config()?;

    let sync_direction = sync_direction.unwrap_or_default();

    let mut list = Vec::with_capacity(config.sections.len());
    for (_, (_, job)) in config.sections.into_iter() {
        let job: SyncJobConfig = serde_json::from_value(job)?;
        let direction = job.sync_direction.unwrap_or_default();

        match &store {
            Some(store) if &job.store != store => continue,
            _ => {}
        }

        if !sync_direction.matches(direction) {
            continue;
        }

        if !check_sync_job_read_access(&user_info, &auth_id, &job) {
            continue;
        }

        let last_state = JobState::load("syncjob", &job.id)
            .map_err(|err| format_err!("could not open statefile for {}: {}", &job.id, err))?;

        let status = compute_schedule_status(&last_state, job.schedule.as_deref())?;

        list.push(SyncJobStatus {
            config: job,
            status,
        });
    }

    rpcenv["digest"] = hex::encode(digest).into();

    Ok(list)
}

#[api(
    input: {
        properties: {
            id: {
                schema: JOB_ID_SCHEMA,
            }
        }
    },
    access: {
        description: "User needs Datastore.Backup on target datastore, and Remote.Read on source remote. Additionally, remove_vanished requires Datastore.Prune, and any owner other than the user themselves requires Datastore.Modify",
        permission: &Permission::Anybody,
    },
)]
/// Runs the sync jobs manually.
pub fn run_sync_job(
    id: String,
    _info: &ApiMethod,
    rpcenv: &mut dyn RpcEnvironment,
) -> Result<String, Error> {
    let auth_id: Authid = rpcenv.get_auth_id().unwrap().parse()?;
    let user_info = CachedUserInfo::new()?;

    let (config, _digest) = sync::config()?;
    let sync_job: SyncJobConfig = config.lookup("sync", &id)?;

    if !check_sync_job_modify_access(&user_info, &auth_id, &sync_job) {
        bail!("permission check failed, '{auth_id}' is missing access");
    }

    let job = Job::new("syncjob", &id)?;

    let to_stdout = rpcenv.env_type() == RpcEnvironmentType::CLI;

    let upid_str = do_sync_job(job, sync_job, &auth_id, None, to_stdout)?;

    Ok(upid_str)
}

#[sortable]
const SYNC_INFO_SUBDIRS: SubdirMap = &[("run", &Router::new().post(&API_METHOD_RUN_SYNC_JOB))];

const SYNC_INFO_ROUTER: Router = Router::new()
    .get(&list_subdirs_api_method!(SYNC_INFO_SUBDIRS))
    .subdirs(SYNC_INFO_SUBDIRS);

pub const ROUTER: Router = Router::new()
    .get(&API_METHOD_LIST_CONFIG_SYNC_JOBS)
    .match_all("id", &SYNC_INFO_ROUTER);
