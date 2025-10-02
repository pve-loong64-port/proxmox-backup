use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::{bail, format_err, Context, Error};
use http_body_util::BodyExt;
use nix::unistd::{unlinkat, UnlinkatFlags};
use pbs_tools::lru_cache::LruCache;
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};

use proxmox_human_byte::HumanByte;
use proxmox_s3_client::{S3Client, S3ClientConf, S3ClientOptions, S3ObjectKey, S3PathPrefix};
use proxmox_schema::ApiType;

use proxmox_sys::error::SysError;
use proxmox_sys::fs::{file_read_optional_string, replace_file, CreateOptions};
use proxmox_sys::linux::procfs::MountInfo;
use proxmox_sys::process_locker::{ProcessLockExclusiveGuard, ProcessLockSharedGuard};
use proxmox_time::TimeSpan;
use proxmox_worker_task::WorkerTaskContext;

use pbs_api_types::{
    ArchiveType, Authid, BackupGroupDeleteStats, BackupNamespace, BackupType, ChunkOrder,
    DataStoreConfig, DatastoreBackendConfig, DatastoreBackendType, DatastoreFSyncLevel,
    DatastoreTuning, GarbageCollectionCacheStats, GarbageCollectionStatus, MaintenanceMode,
    MaintenanceType, Operation, UPID,
};
use pbs_config::s3::S3_CFG_TYPE_ID;
use pbs_config::BackupLockGuard;

use crate::backup_info::{
    BackupDir, BackupGroup, BackupInfo, OLD_LOCKING, PROTECTED_MARKER_FILENAME,
};
use crate::chunk_store::ChunkStore;
use crate::dynamic_index::{DynamicIndexReader, DynamicIndexWriter};
use crate::fixed_index::{FixedIndexReader, FixedIndexWriter};
use crate::hierarchy::{ListGroups, ListGroupsType, ListNamespaces, ListNamespacesRecursive};
use crate::index::IndexFile;
use crate::local_datastore_lru_cache::S3Cacher;
use crate::s3::S3_CONTENT_PREFIX;
use crate::task_tracking::{self, update_active_operations};
use crate::{DataBlob, LocalDatastoreLruCache};

static DATASTORE_MAP: LazyLock<Mutex<HashMap<String, Arc<DataStoreImpl>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Filename to store backup group notes
pub const GROUP_NOTES_FILE_NAME: &str = "notes";
/// Filename to store backup group owner
pub const GROUP_OWNER_FILE_NAME: &str = "owner";
/// Filename for in-use marker stored on S3 object store backend
pub const S3_DATASTORE_IN_USE_MARKER: &str = ".in-use";
const NAMESPACE_MARKER_FILENAME: &str = ".namespace";

/// checks if auth_id is owner, or, if owner is a token, if
/// auth_id is the user of the token
pub fn check_backup_owner(owner: &Authid, auth_id: &Authid) -> Result<(), Error> {
    let correct_owner =
        owner == auth_id || (owner.is_token() && &Authid::from(owner.user().clone()) == auth_id);
    if !correct_owner {
        bail!("backup owner check failed ({} != {})", auth_id, owner);
    }
    Ok(())
}

/// Check if a device with a given UUID is currently mounted at store_mount_point by
/// comparing the `st_rdev` values of `/dev/disk/by-uuid/<uuid>` and the source device in
/// /proc/self/mountinfo.
///
/// If we can't check if it is mounted, we treat that as not mounted,
/// returning false.
///
/// Reasons it could fail other than not being mounted where expected:
///  - could not read `/proc/self/mountinfo`
///  - could not stat `/dev/disk/by-uuid/<uuid>`
///  - `/dev/disk/by-uuid/<uuid>` is not a block device
///
/// Since these are very much out of our control, there is no real value in distinguishing
/// between them, so for this function they all are treated as 'device not mounted'
fn is_datastore_mounted_at(store_mount_point: String, device_uuid: &str) -> bool {
    use nix::sys::stat::SFlag;

    let store_mount_point = Path::new(&store_mount_point);

    let dev_node = match nix::sys::stat::stat(format!("/dev/disk/by-uuid/{device_uuid}").as_str()) {
        Ok(stat) if SFlag::from_bits_truncate(stat.st_mode) == SFlag::S_IFBLK => stat.st_rdev,
        _ => return false,
    };

    let Ok(mount_info) = MountInfo::read() else {
        return false;
    };

    for (_, entry) in mount_info {
        let Some(source) = entry.mount_source else {
            continue;
        };

        if entry.mount_point != store_mount_point || !source.as_bytes().starts_with(b"/") {
            continue;
        }

        if let Ok(stat) = nix::sys::stat::stat(source.as_os_str()) {
            let sflag = SFlag::from_bits_truncate(stat.st_mode);

            if sflag == SFlag::S_IFBLK && stat.st_rdev == dev_node {
                return true;
            }
        }
    }

    false
}

pub fn get_datastore_mount_status(config: &DataStoreConfig) -> Option<bool> {
    let device_uuid = config.backing_device.as_ref()?;
    Some(is_datastore_mounted_at(config.absolute_path(), device_uuid))
}

pub fn ensure_datastore_is_mounted(config: &DataStoreConfig) -> Result<(), Error> {
    match get_datastore_mount_status(config) {
        Some(true) => Ok(()),
        Some(false) => Err(format_err!("Datastore '{}' is not mounted", config.name)),
        None => Ok(()),
    }
}

/// Datastore Management
///
/// A Datastore can store severals backups, and provides the
/// management interface for backup.
pub struct DataStoreImpl {
    chunk_store: Arc<ChunkStore>,
    gc_mutex: Mutex<Option<ProcessLockExclusiveGuard>>,
    last_gc_status: Mutex<GarbageCollectionStatus>,
    verify_new: bool,
    chunk_order: ChunkOrder,
    last_digest: Option<[u8; 32]>,
    sync_level: DatastoreFSyncLevel,
    backend_config: DatastoreBackendConfig,
    lru_store_caching: Option<LocalDatastoreLruCache>,
}

impl DataStoreImpl {
    // This one just panics on everything
    #[doc(hidden)]
    pub(crate) unsafe fn new_test() -> Arc<Self> {
        Arc::new(Self {
            chunk_store: Arc::new(unsafe { ChunkStore::panic_store() }),
            gc_mutex: Mutex::new(None),
            last_gc_status: Mutex::new(GarbageCollectionStatus::default()),
            verify_new: false,
            chunk_order: Default::default(),
            last_digest: None,
            sync_level: Default::default(),
            backend_config: Default::default(),
            lru_store_caching: None,
        })
    }
}

pub struct DataStore {
    inner: Arc<DataStoreImpl>,
    operation: Option<Operation>,
}

impl Clone for DataStore {
    fn clone(&self) -> Self {
        let mut new_operation = self.operation;
        if let Some(operation) = self.operation {
            if let Err(e) = update_active_operations(self.name(), operation, 1) {
                log::error!("could not update active operations - {}", e);
                new_operation = None;
            }
        }

        DataStore {
            inner: self.inner.clone(),
            operation: new_operation,
        }
    }
}

impl Drop for DataStore {
    fn drop(&mut self) {
        if let Some(operation) = self.operation {
            let mut last_task = false;
            match update_active_operations(self.name(), operation, -1) {
                Err(e) => log::error!("could not update active operations - {}", e),
                Ok(updated_operations) => {
                    last_task = updated_operations.read + updated_operations.write == 0;
                }
            }

            // remove datastore from cache iff
            //  - last task finished, and
            //  - datastore is in a maintenance mode that mandates it
            let remove_from_cache = last_task
                && pbs_config::datastore::config()
                    .and_then(|(s, _)| s.lookup::<DataStoreConfig>("datastore", self.name()))
                    .is_ok_and(|c| {
                        c.get_maintenance_mode()
                            .is_some_and(|m| m.clear_from_cache())
                    });

            if remove_from_cache {
                DATASTORE_MAP.lock().unwrap().remove(self.name());
            }
        }
    }
}

#[derive(Clone)]
/// Storage backend type for a datastore.
pub enum DatastoreBackend {
    /// Storage is located on local filesystem.
    Filesystem,
    /// Storage is located on S3 compatible object store.
    S3(Arc<S3Client>),
}

impl DataStore {
    // This one just panics on everything
    #[doc(hidden)]
    pub(crate) unsafe fn new_test() -> Arc<Self> {
        Arc::new(Self {
            inner: unsafe { DataStoreImpl::new_test() },
            operation: None,
        })
    }

    /// Get the backend for this datastore based on it's configuration
    pub fn backend(&self) -> Result<DatastoreBackend, Error> {
        let backend_type = match self.inner.backend_config.ty.unwrap_or_default() {
            DatastoreBackendType::Filesystem => DatastoreBackend::Filesystem,
            DatastoreBackendType::S3 => {
                let s3_client_id = self
                    .inner
                    .backend_config
                    .client
                    .as_ref()
                    .ok_or_else(|| format_err!("missing client for s3 backend"))?;
                let bucket = self
                    .inner
                    .backend_config
                    .bucket
                    .clone()
                    .ok_or_else(|| format_err!("missing bucket for s3 backend"))?;

                let (config, _config_digest) = pbs_config::s3::config()?;
                let config: S3ClientConf = config.lookup(S3_CFG_TYPE_ID, s3_client_id)?;

                let options = S3ClientOptions::from_config(
                    config.config,
                    config.secret_key,
                    Some(bucket),
                    self.name().to_owned(),
                );
                let s3_client = S3Client::new(options)?;
                DatastoreBackend::S3(Arc::new(s3_client))
            }
        };

        Ok(backend_type)
    }

    pub fn cache(&self) -> Option<&LocalDatastoreLruCache> {
        self.inner.lru_store_caching.as_ref()
    }

    /// Check if the digest is present in the local datastore cache.
    /// Always returns false if there is no cache configured for this datastore.
    pub fn cache_contains(&self, digest: &[u8; 32]) -> bool {
        if let Some(cache) = self.inner.lru_store_caching.as_ref() {
            return cache.contains(digest);
        }
        false
    }

    /// Insert digest as most recently used on in the cache.
    /// Returns with success if there is no cache configured for this datastore.
    pub fn cache_insert(&self, digest: &[u8; 32], chunk: &DataBlob) -> Result<(), Error> {
        if let Some(cache) = self.inner.lru_store_caching.as_ref() {
            return cache.insert(digest, chunk);
        }
        Ok(())
    }

    /// Returns the cacher for datastores backed by S3 object stores.
    /// This allows to fetch chunks to the local cache store on-demand.
    pub fn cacher(&self) -> Result<Option<S3Cacher>, Error> {
        self.backend().map(|backend| match backend {
            DatastoreBackend::S3(s3_client) => {
                Some(S3Cacher::new(s3_client, self.inner.chunk_store.clone()))
            }
            DatastoreBackend::Filesystem => None,
        })
    }

    pub fn lookup_datastore(
        name: &str,
        operation: Option<Operation>,
    ) -> Result<Arc<DataStore>, Error> {
        // Avoid TOCTOU between checking maintenance mode and updating active operation counter, as
        // we use it to decide whether it is okay to delete the datastore.
        let _config_lock = pbs_config::datastore::lock_config()?;

        // we could use the ConfigVersionCache's generation for staleness detection, but  we load
        // the config anyway -> just use digest, additional benefit: manual changes get detected
        let (config, digest) = pbs_config::datastore::config()?;
        let config: DataStoreConfig = config.lookup("datastore", name)?;

        if let Some(maintenance_mode) = config.get_maintenance_mode() {
            if let Err(error) = maintenance_mode.check(operation) {
                bail!("datastore '{name}' is unavailable: {error}");
            }
        }

        if get_datastore_mount_status(&config) == Some(false) {
            let mut datastore_cache = DATASTORE_MAP.lock().unwrap();
            datastore_cache.remove(&config.name);
            bail!("datastore '{}' is not mounted", config.name);
        }

        let mut datastore_cache = DATASTORE_MAP.lock().unwrap();
        let entry = datastore_cache.get(name);

        // reuse chunk store so that we keep using the same process locker instance!
        let chunk_store = if let Some(datastore) = &entry {
            let last_digest = datastore.last_digest.as_ref();
            if let Some(true) = last_digest.map(|last_digest| last_digest == &digest) {
                if let Some(operation) = operation {
                    update_active_operations(name, operation, 1)?;
                }
                return Ok(Arc::new(Self {
                    inner: Arc::clone(datastore),
                    operation,
                }));
            }
            Arc::clone(&datastore.chunk_store)
        } else {
            let tuning: DatastoreTuning = serde_json::from_value(
                DatastoreTuning::API_SCHEMA
                    .parse_property_string(config.tuning.as_deref().unwrap_or(""))?,
            )?;
            Arc::new(ChunkStore::open(
                name,
                config.absolute_path(),
                tuning.sync_level.unwrap_or_default(),
            )?)
        };

        let datastore = DataStore::with_store_and_config(chunk_store, config, Some(digest))?;

        let datastore = Arc::new(datastore);
        datastore_cache.insert(name.to_string(), datastore.clone());

        if let Some(operation) = operation {
            update_active_operations(name, operation, 1)?;
        }

        Ok(Arc::new(Self {
            inner: datastore,
            operation,
        }))
    }

    /// removes all datastores that are not configured anymore
    pub fn remove_unused_datastores() -> Result<(), Error> {
        let (config, _digest) = pbs_config::datastore::config()?;

        let mut map = DATASTORE_MAP.lock().unwrap();
        // removes all elements that are not in the config
        map.retain(|key, _| config.sections.contains_key(key));
        Ok(())
    }

    /// trigger clearing cache entry based on maintenance mode. Entry will only
    /// be cleared iff there is no other task running, if there is, the end of the
    /// last running task will trigger the clearing of the cache entry.
    pub fn update_datastore_cache(name: &str) -> Result<(), Error> {
        let (config, _digest) = pbs_config::datastore::config()?;
        let datastore: DataStoreConfig = config.lookup("datastore", name)?;
        if datastore
            .get_maintenance_mode()
            .is_some_and(|m| m.clear_from_cache())
        {
            // the datastore drop handler does the checking if tasks are running and clears the
            // cache entry, so we just have to trigger it here
            let _ = DataStore::lookup_datastore(name, Some(Operation::Lookup));
        }

        Ok(())
    }

    /// Open a raw database given a name and a path.
    ///
    /// # Safety
    /// See the safety section in `open_from_config`
    pub unsafe fn open_path(
        name: &str,
        path: impl AsRef<Path>,
        operation: Option<Operation>,
    ) -> Result<Arc<Self>, Error> {
        let path = path
            .as_ref()
            .to_str()
            .ok_or_else(|| format_err!("non-utf8 paths not supported"))?
            .to_owned();
        unsafe { Self::open_from_config(DataStoreConfig::new(name.to_owned(), path), operation) }
    }

    /// Open a datastore given a raw configuration.
    ///
    /// # Safety
    /// There's no memory safety implication, but as this is opening a new ChunkStore it will
    /// create a new process locker instance, potentially on the same path as existing safely
    /// created ones. This is dangerous as dropping the reference of this and thus the underlying
    /// chunkstore's process locker will close all locks from our process on the config.path,
    /// breaking guarantees we need to uphold for safe long backup + GC interaction on newer/older
    /// process instances (from package update).
    unsafe fn open_from_config(
        config: DataStoreConfig,
        operation: Option<Operation>,
    ) -> Result<Arc<Self>, Error> {
        let name = config.name.clone();

        ensure_datastore_is_mounted(&config)?;

        let tuning: DatastoreTuning = serde_json::from_value(
            DatastoreTuning::API_SCHEMA
                .parse_property_string(config.tuning.as_deref().unwrap_or(""))?,
        )?;
        let chunk_store = ChunkStore::open(
            &name,
            config.absolute_path(),
            tuning.sync_level.unwrap_or_default(),
        )?;
        let inner = Arc::new(Self::with_store_and_config(
            Arc::new(chunk_store),
            config,
            None,
        )?);

        if let Some(operation) = operation {
            update_active_operations(&name, operation, 1)?;
        }

        Ok(Arc::new(Self { inner, operation }))
    }

    fn with_store_and_config(
        chunk_store: Arc<ChunkStore>,
        config: DataStoreConfig,
        last_digest: Option<[u8; 32]>,
    ) -> Result<DataStoreImpl, Error> {
        let mut gc_status_path = chunk_store.base_path();
        gc_status_path.push(".gc-status");

        let gc_status = if let Some(state) = file_read_optional_string(gc_status_path)? {
            match serde_json::from_str(&state) {
                Ok(state) => state,
                Err(err) => {
                    log::error!("error reading gc-status: {}", err);
                    GarbageCollectionStatus::default()
                }
            }
        } else {
            GarbageCollectionStatus::default()
        };

        let tuning: DatastoreTuning = serde_json::from_value(
            DatastoreTuning::API_SCHEMA
                .parse_property_string(config.tuning.as_deref().unwrap_or(""))?,
        )?;

        let backend_config: DatastoreBackendConfig = serde_json::from_value(
            DatastoreBackendConfig::API_SCHEMA
                .parse_property_string(config.backend.as_deref().unwrap_or(""))?,
        )?;

        let lru_store_caching = if DatastoreBackendType::S3 == backend_config.ty.unwrap_or_default()
        {
            let mut cache_capacity = 0;
            if let Ok(fs_info) = proxmox_sys::fs::fs_info(&chunk_store.base_path()) {
                cache_capacity = fs_info.available / (16 * 1024 * 1024);
            }
            if let Some(max_cache_size) = backend_config.max_cache_size {
                info!(
                    "Got requested max cache size {max_cache_size} for store {}",
                    config.name
                );
                let max_cache_capacity = max_cache_size.as_u64() / (16 * 1024 * 1024);
                cache_capacity = cache_capacity.min(max_cache_capacity);
            }
            let cache_capacity = usize::try_from(cache_capacity).unwrap_or_default();

            info!(
                "Using datastore cache with capacity {cache_capacity} for store {}",
                config.name
            );

            let cache = LocalDatastoreLruCache::new(cache_capacity, chunk_store.clone());
            Some(cache)
        } else {
            None
        };

        Ok(DataStoreImpl {
            chunk_store,
            gc_mutex: Mutex::new(None),
            last_gc_status: Mutex::new(gc_status),
            verify_new: config.verify_new.unwrap_or(false),
            chunk_order: tuning.chunk_order.unwrap_or_default(),
            last_digest,
            sync_level: tuning.sync_level.unwrap_or_default(),
            backend_config,
            lru_store_caching,
        })
    }

    pub fn get_chunk_iterator(
        &self,
    ) -> Result<
        impl Iterator<Item = (Result<proxmox_sys::fs::ReadDirEntry, Error>, usize, bool)>,
        Error,
    > {
        self.inner.chunk_store.get_chunk_iterator()
    }

    pub fn create_fixed_writer<P: AsRef<Path>>(
        &self,
        filename: P,
        size: usize,
        chunk_size: usize,
    ) -> Result<FixedIndexWriter, Error> {
        let index = FixedIndexWriter::create(
            self.inner.chunk_store.clone(),
            filename.as_ref(),
            size,
            chunk_size,
        )?;

        Ok(index)
    }

    pub fn open_fixed_reader<P: AsRef<Path>>(
        &self,
        filename: P,
    ) -> Result<FixedIndexReader, Error> {
        let full_path = self.inner.chunk_store.relative_path(filename.as_ref());

        let index = FixedIndexReader::open(&full_path)?;

        Ok(index)
    }

    pub fn create_dynamic_writer<P: AsRef<Path>>(
        &self,
        filename: P,
    ) -> Result<DynamicIndexWriter, Error> {
        let index = DynamicIndexWriter::create(self.inner.chunk_store.clone(), filename.as_ref())?;

        Ok(index)
    }

    pub fn open_dynamic_reader<P: AsRef<Path>>(
        &self,
        filename: P,
    ) -> Result<DynamicIndexReader, Error> {
        let full_path = self.inner.chunk_store.relative_path(filename.as_ref());

        let index = DynamicIndexReader::open(&full_path)?;

        Ok(index)
    }

    pub fn open_index<P>(&self, filename: P) -> Result<Box<dyn IndexFile + Send>, Error>
    where
        P: AsRef<Path>,
    {
        let filename = filename.as_ref();
        let out: Box<dyn IndexFile + Send> = match ArchiveType::from_path(filename)? {
            ArchiveType::DynamicIndex => Box::new(self.open_dynamic_reader(filename)?),
            ArchiveType::FixedIndex => Box::new(self.open_fixed_reader(filename)?),
            _ => bail!("cannot open index file of unknown type: {:?}", filename),
        };
        Ok(out)
    }

    /// Fast index verification - only check if chunks exists
    pub fn fast_index_verification(
        &self,
        index: &dyn IndexFile,
        checked: &mut HashSet<[u8; 32]>,
    ) -> Result<(), Error> {
        for pos in 0..index.index_count() {
            let info = index.chunk_info(pos).unwrap();
            if checked.contains(&info.digest) {
                continue;
            }

            self.stat_chunk(&info.digest).map_err(|err| {
                format_err!(
                    "fast_index_verification error, stat_chunk {} failed - {}",
                    hex::encode(info.digest),
                    err,
                )
            })?;

            checked.insert(info.digest);
        }

        Ok(())
    }

    pub fn name(&self) -> &str {
        self.inner.chunk_store.name()
    }

    pub fn base_path(&self) -> PathBuf {
        self.inner.chunk_store.base_path()
    }

    /// Returns the absolute path for a backup namespace on this datastore
    pub fn namespace_path(&self, ns: &BackupNamespace) -> PathBuf {
        let mut path = self.base_path();
        path.reserve(ns.path_len());
        for part in ns.components() {
            path.push("ns");
            path.push(part);
        }
        path
    }

    /// Returns the absolute path for a backup_type
    pub fn type_path(&self, ns: &BackupNamespace, backup_type: BackupType) -> PathBuf {
        let mut full_path = self.namespace_path(ns);
        full_path.push(backup_type.to_string());
        full_path
    }

    /// Returns the absolute path for a backup_group
    pub fn group_path(
        &self,
        ns: &BackupNamespace,
        backup_group: &pbs_api_types::BackupGroup,
    ) -> PathBuf {
        let mut full_path = self.namespace_path(ns);
        full_path.push(backup_group.to_string());
        full_path
    }

    /// Returns the absolute path of a backup groups notes file
    pub fn group_notes_path(
        &self,
        ns: &BackupNamespace,
        group: &pbs_api_types::BackupGroup,
    ) -> PathBuf {
        self.group_path(ns, group).join(GROUP_NOTES_FILE_NAME)
    }

    /// Returns the absolute path for backup_dir
    pub fn snapshot_path(
        &self,
        ns: &BackupNamespace,
        backup_dir: &pbs_api_types::BackupDir,
    ) -> PathBuf {
        let mut full_path = self.namespace_path(ns);
        full_path.push(backup_dir.to_string());
        full_path
    }

    /// Create a backup namespace.
    pub fn create_namespace(
        self: &Arc<Self>,
        parent: &BackupNamespace,
        name: String,
    ) -> Result<BackupNamespace, Error> {
        if !self.namespace_exists(parent) {
            bail!("cannot create new namespace, parent {parent} doesn't already exists");
        }

        // construct ns before mkdir to enforce max-depth and name validity
        let ns = BackupNamespace::from_parent_ns(parent, name)?;

        if let DatastoreBackend::S3(s3_client) = self.backend()? {
            let object_key = crate::s3::object_key_from_path(&ns.path(), NAMESPACE_MARKER_FILENAME)
                .context("invalid namespace marker object key")?;
            let _is_duplicate = proxmox_async::runtime::block_on(
                s3_client.upload_no_replace_with_retry(object_key, hyper::body::Bytes::from("")),
            )
            .context("failed to create namespace on s3 backend")?;
        }

        let mut ns_full_path = self.base_path();
        ns_full_path.push(ns.path());

        std::fs::create_dir_all(ns_full_path)?;

        Ok(ns)
    }

    /// Returns if the given namespace exists on the datastore
    pub fn namespace_exists(&self, ns: &BackupNamespace) -> bool {
        let mut path = self.base_path();
        path.push(ns.path());
        path.exists()
    }

    /// Remove all backup groups of a single namespace level but not the namespace itself.
    ///
    /// Does *not* descends into child-namespaces and doesn't remoes the namespace itself either.
    ///
    /// Returns a tuple with the first item being true if all the groups were removed, and false if some were protected.
    /// The second item returns the remove statistics.
    pub fn remove_namespace_groups(
        self: &Arc<Self>,
        ns: &BackupNamespace,
    ) -> Result<(bool, BackupGroupDeleteStats), Error> {
        // FIXME: locking? The single groups/snapshots are already protected, so may not be
        // necessary (depends on what we all allow to do with namespaces)
        log::info!("removing all groups in namespace {}:/{ns}", self.name());

        let mut removed_all_groups = true;
        let mut stats = BackupGroupDeleteStats::default();

        for group in self.iter_backup_groups(ns.to_owned())? {
            let group = group?;
            let backend = self.backend()?;
            let delete_stats = group.destroy(&backend)?;
            stats.add(&delete_stats);
            removed_all_groups = removed_all_groups && delete_stats.all_removed();
        }

        let base_file = std::fs::File::open(self.base_path())?;
        let base_fd = base_file.as_raw_fd();
        for ty in BackupType::iter() {
            let mut ty_dir = ns.path();
            ty_dir.push(ty.to_string());
            // best effort only, but we probably should log the error
            if let Err(err) = unlinkat(Some(base_fd), &ty_dir, UnlinkatFlags::RemoveDir) {
                if err != nix::errno::Errno::ENOENT {
                    log::error!("failed to remove backup type {ty} in {ns} - {err}");
                }
            }
        }

        Ok((removed_all_groups, stats))
    }

    /// Remove a complete backup namespace optionally including all it's, and child namespaces',
    /// groups. If  `removed_groups` is false this only prunes empty namespaces.
    ///
    /// Returns true if everything requested, and false if some groups were protected or if some
    /// namespaces weren't empty even though all groups were deleted (race with new backup)
    pub fn remove_namespace_recursive(
        self: &Arc<Self>,
        ns: &BackupNamespace,
        delete_groups: bool,
    ) -> Result<(bool, BackupGroupDeleteStats), Error> {
        let store = self.name();
        let mut removed_all_requested = true;
        let mut stats = BackupGroupDeleteStats::default();
        let backend = self.backend()?;

        if delete_groups {
            log::info!("removing whole namespace recursively below {store}:/{ns}",);
            for ns in self.recursive_iter_backup_ns(ns.to_owned())? {
                let (removed_ns_groups, delete_stats) = self.remove_namespace_groups(&ns?)?;
                stats.add(&delete_stats);
                removed_all_requested = removed_all_requested && removed_ns_groups;
            }

            if let DatastoreBackend::S3(s3_client) = &backend {
                let ns_dir = ns.path();
                let ns_prefix = ns_dir
                    .to_str()
                    .ok_or_else(|| format_err!("invalid namespace path prefix"))?;
                let prefix = format!("{S3_CONTENT_PREFIX}/{ns_prefix}");
                let delete_objects_error = proxmox_async::runtime::block_on(
                    s3_client.delete_objects_by_prefix_with_suffix_filter(
                        &S3PathPrefix::Some(prefix),
                        PROTECTED_MARKER_FILENAME,
                        &[GROUP_OWNER_FILE_NAME, GROUP_NOTES_FILE_NAME],
                    ),
                )?;
                if delete_objects_error {
                    bail!("deleting objects failed");
                }
            }
        } else {
            log::info!("pruning empty namespace recursively below {store}:/{ns}");
        }

        // now try to delete the actual namespaces, bottom up so that we can use safe rmdir that
        // will choke if a new backup/group appeared in the meantime (but not on an new empty NS)
        let mut children = self
            .recursive_iter_backup_ns(ns.to_owned())?
            .collect::<Result<Vec<BackupNamespace>, Error>>()?;

        children.sort_by_key(|b| std::cmp::Reverse(b.depth()));

        let base_file = std::fs::File::open(self.base_path())?;
        let base_fd = base_file.as_raw_fd();

        for ns in children.iter() {
            let mut ns_dir = ns.path();
            ns_dir.push("ns");
            let _ = unlinkat(Some(base_fd), &ns_dir, UnlinkatFlags::RemoveDir);

            if !ns.is_root() {
                match unlinkat(Some(base_fd), &ns.path(), UnlinkatFlags::RemoveDir) {
                    Ok(()) => log::debug!("removed namespace {ns}"),
                    Err(nix::errno::Errno::ENOENT) => {
                        log::debug!("namespace {ns} already removed")
                    }
                    Err(nix::errno::Errno::ENOTEMPTY) if !delete_groups => {
                        removed_all_requested = false;
                        log::debug!("skip removal of non-empty namespace {ns}")
                    }
                    Err(err) => {
                        removed_all_requested = false;
                        log::warn!("failed to remove namespace {ns} - {err}")
                    }
                }
                if let DatastoreBackend::S3(s3_client) = &backend {
                    // Only remove the namespace marker, if it was empty,
                    // than this is the same as the namespace being removed.
                    let object_key =
                        crate::s3::object_key_from_path(&ns.path(), NAMESPACE_MARKER_FILENAME)
                            .context("invalid namespace marker object key")?;
                    proxmox_async::runtime::block_on(s3_client.delete_object(object_key))?;
                }
            }
        }

        Ok((removed_all_requested, stats))
    }

    /// Remove a complete backup group including all snapshots.
    ///
    /// Returns `BackupGroupDeleteStats`, containing the number of deleted snapshots
    /// and number of protected snaphsots, which therefore were not removed.
    pub fn remove_backup_group(
        self: &Arc<Self>,
        ns: &BackupNamespace,
        backup_group: &pbs_api_types::BackupGroup,
    ) -> Result<BackupGroupDeleteStats, Error> {
        let backup_group = self.backup_group(ns.clone(), backup_group.clone());

        backup_group.destroy(&self.backend()?)
    }

    /// Remove a backup directory including all content
    pub fn remove_backup_dir(
        self: &Arc<Self>,
        ns: &BackupNamespace,
        backup_dir: &pbs_api_types::BackupDir,
        force: bool,
    ) -> Result<(), Error> {
        let backup_dir = self.backup_dir(ns.clone(), backup_dir.clone())?;

        backup_dir.destroy(force, &self.backend()?)
    }

    /// Returns the time of the last successful backup
    ///
    /// Or None if there is no backup in the group (or the group dir does not exist).
    pub fn last_successful_backup(
        self: &Arc<Self>,
        ns: &BackupNamespace,
        backup_group: &pbs_api_types::BackupGroup,
    ) -> Result<Option<i64>, Error> {
        let backup_group = self.backup_group(ns.clone(), backup_group.clone());

        let group_path = backup_group.full_group_path();

        if group_path.exists() {
            backup_group.last_successful_backup()
        } else {
            Ok(None)
        }
    }

    /// Return the path of the 'owner' file.
    pub(super) fn owner_path(
        &self,
        ns: &BackupNamespace,
        group: &pbs_api_types::BackupGroup,
    ) -> PathBuf {
        self.group_path(ns, group).join(GROUP_OWNER_FILE_NAME)
    }

    /// Returns the backup owner.
    ///
    /// The backup owner is the entity who first created the backup group.
    pub fn get_owner(
        &self,
        ns: &BackupNamespace,
        backup_group: &pbs_api_types::BackupGroup,
    ) -> Result<Authid, Error> {
        let full_path = self.owner_path(ns, backup_group);

        if let DatastoreBackend::S3(s3_client) = self.backend()? {
            let mut path = ns.path();
            path.push(backup_group.to_string());
            let object_key = crate::s3::object_key_from_path(&path, GROUP_OWNER_FILE_NAME)
                .context("invalid owner file object key")?;
            let response = proxmox_async::runtime::block_on(s3_client.get_object(object_key))?
                .ok_or_else(|| format_err!("fetching owner failed"))?;
            let content = proxmox_async::runtime::block_on(response.content.collect())?;
            let owner = String::from_utf8(content.to_bytes().trim_ascii_end().to_vec())?;
            return owner
                .parse()
                .map_err(|err| format_err!("parsing owner for {backup_group} failed: {err}"));
        }

        let owner = proxmox_sys::fs::file_read_firstline(full_path)?;
        owner
            .trim_end() // remove trailing newline
            .parse()
            .map_err(|err| format_err!("parsing owner for {backup_group} failed: {err}"))
    }

    pub fn owns_backup(
        &self,
        ns: &BackupNamespace,
        backup_group: &pbs_api_types::BackupGroup,
        auth_id: &Authid,
    ) -> Result<bool, Error> {
        let owner = self.get_owner(ns, backup_group)?;

        Ok(check_backup_owner(&owner, auth_id).is_ok())
    }

    /// Set the backup owner.
    pub fn set_owner(
        &self,
        ns: &BackupNamespace,
        backup_group: &pbs_api_types::BackupGroup,
        auth_id: &Authid,
        force: bool,
    ) -> Result<(), Error> {
        let path = self.owner_path(ns, backup_group);

        if let DatastoreBackend::S3(s3_client) = self.backend()? {
            let mut path = ns.path();
            path.push(backup_group.to_string());
            let object_key = crate::s3::object_key_from_path(&path, GROUP_OWNER_FILE_NAME)
                .context("invalid owner file object key")?;
            let data = hyper::body::Bytes::from(format!("{auth_id}\n"));
            let _is_duplicate = proxmox_async::runtime::block_on(
                s3_client.upload_replace_with_retry(object_key, data),
            )
            .context("failed to set owner on s3 backend")?;
        }

        let mut open_options = std::fs::OpenOptions::new();
        open_options.write(true);

        if force {
            open_options.truncate(true);
            open_options.create(true);
        } else {
            open_options.create_new(true);
        }

        let mut file = open_options
            .open(&path)
            .map_err(|err| format_err!("unable to create owner file {:?} - {}", path, err))?;

        writeln!(file, "{}", auth_id)
            .map_err(|err| format_err!("unable to write owner file  {:?} - {}", path, err))?;

        Ok(())
    }

    /// Create (if it does not already exists) and lock a backup group
    ///
    /// And set the owner to 'userid'. If the group already exists, it returns the
    /// current owner (instead of setting the owner).
    ///
    /// This also acquires an exclusive lock on the directory and returns the lock guard.
    pub fn create_locked_backup_group(
        self: &Arc<Self>,
        ns: &BackupNamespace,
        backup_group: &pbs_api_types::BackupGroup,
        auth_id: &Authid,
    ) -> Result<(Authid, BackupLockGuard), Error> {
        let backup_group = self.backup_group(ns.clone(), backup_group.clone());

        // create intermediate path first
        let full_path = backup_group.full_group_path();

        std::fs::create_dir_all(full_path.parent().ok_or_else(|| {
            format_err!("could not construct parent path for group {backup_group:?}")
        })?)?;

        // now create the group, this allows us to check whether it existed before
        match std::fs::create_dir(&full_path) {
            Ok(_) => {
                let guard = backup_group.lock().with_context(|| {
                    format!("while creating new locked backup group '{backup_group:?}'")
                })?;
                if let Err(err) = self.set_owner(ns, backup_group.group(), auth_id, false) {
                    let _ = std::fs::remove_dir(&full_path);
                    return Err(err);
                }
                let owner = self.get_owner(ns, backup_group.group())?; // just to be sure
                Ok((owner, guard))
            }
            Err(ref err) if err.kind() == io::ErrorKind::AlreadyExists => {
                let guard = backup_group.lock().with_context(|| {
                    format!("while creating locked backup group '{backup_group:?}'")
                })?;
                let owner = self.get_owner(ns, backup_group.group())?; // just to be sure
                Ok((owner, guard))
            }
            Err(err) => bail!("unable to create backup group {:?} - {}", full_path, err),
        }
    }

    /// Creates a new backup snapshot inside a BackupGroup
    ///
    /// The BackupGroup directory needs to exist.
    pub fn create_locked_backup_dir(
        self: &Arc<Self>,
        ns: &BackupNamespace,
        backup_dir: &pbs_api_types::BackupDir,
    ) -> Result<(PathBuf, bool, BackupLockGuard), Error> {
        let backup_dir = self.backup_dir(ns.clone(), backup_dir.clone())?;
        let relative_path = backup_dir.relative_path();

        match std::fs::create_dir(backup_dir.full_path()) {
            Ok(_) => {
                let guard = backup_dir.lock().with_context(|| {
                    format!("while creating new locked snapshot '{backup_dir:?}'")
                })?;
                Ok((relative_path, true, guard))
            }
            Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists => {
                let guard = backup_dir
                    .lock()
                    .with_context(|| format!("while creating locked snapshot '{backup_dir:?}'"))?;
                Ok((relative_path, false, guard))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Get a streaming iter over single-level backup namespaces of a datatstore
    ///
    /// The iterated item is still a Result that can contain errors from rather unexptected FS or
    /// parsing errors.
    pub fn iter_backup_ns(
        self: &Arc<DataStore>,
        ns: BackupNamespace,
    ) -> Result<ListNamespaces, Error> {
        ListNamespaces::new(Arc::clone(self), ns)
    }

    /// Get a streaming iter over single-level backup namespaces of a datatstore, filtered by Ok
    ///
    /// The iterated item's result is already unwrapped, if it contained an error it will be
    /// logged. Can be useful in iterator chain commands
    pub fn iter_backup_ns_ok(
        self: &Arc<DataStore>,
        ns: BackupNamespace,
    ) -> Result<impl Iterator<Item = BackupNamespace> + 'static, Error> {
        let this = Arc::clone(self);
        Ok(
            ListNamespaces::new(Arc::clone(self), ns)?.filter_map(move |ns| match ns {
                Ok(ns) => Some(ns),
                Err(err) => {
                    log::error!("list groups error on datastore {} - {}", this.name(), err);
                    None
                }
            }),
        )
    }

    /// Get a streaming iter over single-level backup namespaces of a datatstore
    ///
    /// The iterated item is still a Result that can contain errors from rather unexptected FS or
    /// parsing errors.
    pub fn recursive_iter_backup_ns(
        self: &Arc<DataStore>,
        ns: BackupNamespace,
    ) -> Result<ListNamespacesRecursive, Error> {
        ListNamespacesRecursive::new(Arc::clone(self), ns)
    }

    /// Get a streaming iter over single-level backup namespaces of a datatstore, filtered by Ok
    ///
    /// The iterated item's result is already unwrapped, if it contained an error it will be
    /// logged. Can be useful in iterator chain commands
    pub fn recursive_iter_backup_ns_ok(
        self: &Arc<DataStore>,
        ns: BackupNamespace,
        max_depth: Option<usize>,
    ) -> Result<impl Iterator<Item = BackupNamespace> + 'static, Error> {
        let this = Arc::clone(self);
        Ok(if let Some(depth) = max_depth {
            ListNamespacesRecursive::new_max_depth(Arc::clone(self), ns, depth)?
        } else {
            ListNamespacesRecursive::new(Arc::clone(self), ns)?
        }
        .filter_map(move |ns| match ns {
            Ok(ns) => Some(ns),
            Err(err) => {
                log::error!("list groups error on datastore {} - {}", this.name(), err);
                None
            }
        }))
    }

    /// Get a streaming iter over top-level backup groups of a datatstore of a particular type.
    ///
    /// The iterated item is still a Result that can contain errors from rather unexptected FS or
    /// parsing errors.
    pub fn iter_backup_type(
        self: &Arc<DataStore>,
        ns: BackupNamespace,
        ty: BackupType,
    ) -> Result<ListGroupsType, Error> {
        ListGroupsType::new(Arc::clone(self), ns, ty)
    }

    /// Get a streaming iter over top-level backup groups of a datastore of a particular type,
    /// filtered by `Ok` results
    ///
    /// The iterated item's result is already unwrapped, if it contained an error it will be
    /// logged. Can be useful in iterator chain commands
    pub fn iter_backup_type_ok(
        self: &Arc<DataStore>,
        ns: BackupNamespace,
        ty: BackupType,
    ) -> Result<impl Iterator<Item = BackupGroup> + 'static, Error> {
        Ok(self.iter_backup_type(ns, ty)?.ok())
    }

    /// Get a streaming iter over top-level backup groups of a datatstore
    ///
    /// The iterated item is still a Result that can contain errors from rather unexptected FS or
    /// parsing errors.
    pub fn iter_backup_groups(
        self: &Arc<DataStore>,
        ns: BackupNamespace,
    ) -> Result<ListGroups, Error> {
        ListGroups::new(Arc::clone(self), ns)
    }

    /// Get a streaming iter over top-level backup groups of a datatstore, filtered by Ok results
    ///
    /// The iterated item's result is already unwrapped, if it contained an error it will be
    /// logged. Can be useful in iterator chain commands
    pub fn iter_backup_groups_ok(
        self: &Arc<DataStore>,
        ns: BackupNamespace,
    ) -> Result<impl Iterator<Item = BackupGroup> + 'static, Error> {
        Ok(self.iter_backup_groups(ns)?.ok())
    }

    /// Get a in-memory vector for all top-level backup groups of a datatstore
    ///
    /// NOTE: using the iterator directly is most often more efficient w.r.t. memory usage
    pub fn list_backup_groups(
        self: &Arc<DataStore>,
        ns: BackupNamespace,
    ) -> Result<Vec<BackupGroup>, Error> {
        ListGroups::new(Arc::clone(self), ns)?.collect()
    }

    /// Lookup all index files to be found in the datastore without taking any logical iteration
    /// into account.
    /// The filesystem is walked recursevly to detect index files based on their archive type based
    /// on the filename. This however excludes the chunks folder, hidden files and does not follow
    /// symlinks.
    fn list_index_files(&self) -> Result<HashSet<PathBuf>, Error> {
        let base = self.base_path();

        let mut list = HashSet::new();

        use walkdir::WalkDir;

        let walker = WalkDir::new(base).into_iter();

        // make sure we skip .chunks (and other hidden files to keep it simple)
        fn is_hidden(entry: &walkdir::DirEntry) -> bool {
            entry
                .file_name()
                .to_str()
                .map(|s| s.starts_with('.'))
                .unwrap_or(false)
        }
        let handle_entry_err = |err: walkdir::Error| {
            // first, extract the actual IO error and the affected path
            let (inner, path) = match (err.io_error(), err.path()) {
                (None, _) => return Ok(()), // not an IO-error
                (Some(inner), Some(path)) => (inner, path),
                (Some(inner), None) => bail!("unexpected error on datastore traversal: {inner}"),
            };
            if inner.kind() == io::ErrorKind::PermissionDenied {
                if err.depth() <= 1 && path.ends_with("lost+found") {
                    // allow skipping of (root-only) ext4 fsck-directory on EPERM ..
                    return Ok(());
                }
                // .. but do not ignore EPERM in general, otherwise we might prune too many chunks.
                // E.g., if users messed up with owner/perms on a rsync
                bail!("cannot continue garbage-collection safely, permission denied on: {path:?}");
            } else if inner.kind() == io::ErrorKind::NotFound {
                log::info!("ignoring vanished file: {path:?}");
                return Ok(());
            } else {
                bail!("unexpected error on datastore traversal: {inner} - {path:?}");
            }
        };
        for entry in walker.filter_entry(|e| !is_hidden(e)) {
            let path = match entry {
                Ok(entry) => entry.into_path(),
                Err(err) => {
                    handle_entry_err(err)?;
                    continue;
                }
            };
            if let Ok(archive_type) = ArchiveType::from_path(&path) {
                if archive_type == ArchiveType::FixedIndex
                    || archive_type == ArchiveType::DynamicIndex
                {
                    list.insert(path);
                }
            }
        }

        Ok(list)
    }

    // Similar to open index, but return with Ok(None) if index file vanished.
    fn open_index_reader(&self, absolute_path: &Path) -> Result<Option<Box<dyn IndexFile>>, Error> {
        let archive_type = match ArchiveType::from_path(absolute_path) {
            // ignore archives with unknown archive type
            Ok(ArchiveType::Blob) | Err(_) => bail!("unexpected archive type"),
            Ok(archive_type) => archive_type,
        };

        if absolute_path.is_relative() {
            bail!("expected absolute path, got '{absolute_path:?}'");
        }

        let file = match std::fs::File::open(absolute_path) {
            Ok(file) => file,
            // ignore vanished files
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(Error::from(err).context(format!("can't open file '{absolute_path:?}'")))
            }
        };

        match archive_type {
            ArchiveType::FixedIndex => {
                let reader = FixedIndexReader::new(file)
                    .with_context(|| format!("can't open fixed index '{absolute_path:?}'"))?;
                Ok(Some(Box::new(reader)))
            }
            ArchiveType::DynamicIndex => {
                let reader = DynamicIndexReader::new(file)
                    .with_context(|| format!("can't open dynamic index '{absolute_path:?}'"))?;
                Ok(Some(Box::new(reader)))
            }
            ArchiveType::Blob => bail!("unexpected archive type blob"),
        }
    }

    // mark chunks  used by ``index`` as used
    fn index_mark_used_chunks(
        &self,
        index: Box<dyn IndexFile>,
        file_name: &Path, // only used for error reporting
        chunk_lru_cache: &mut Option<LruCache<[u8; 32], ()>>,
        status: &mut GarbageCollectionStatus,
        worker: &dyn WorkerTaskContext,
        s3_client: Option<Arc<S3Client>>,
    ) -> Result<(), Error> {
        status.index_file_count += 1;
        status.index_data_bytes += index.index_bytes();

        for pos in 0..index.index_count() {
            worker.check_abort()?;
            worker.fail_on_shutdown()?;
            let digest = index.index_digest(pos).unwrap();

            // Avoid multiple expensive atime updates by utimensat
            if let Some(chunk_lru_cache) = chunk_lru_cache {
                if chunk_lru_cache.insert(*digest, (), |_| Ok(()))? {
                    if let Some(cache_stats) = status.cache_stats.as_mut() {
                        cache_stats.hits += 1;
                    }
                    continue;
                }
                if let Some(cache_stats) = status.cache_stats.as_mut() {
                    cache_stats.misses += 1;
                }
            }

            match s3_client {
                None => {
                    // Filesystem backend
                    if !self.inner.chunk_store.cond_touch_chunk(digest, false)? {
                        let hex = hex::encode(digest);
                        warn!(
                            "warning: unable to access non-existent chunk {hex}, required by {file_name:?}"
                        );

                        // touch any corresponding .bad files to keep them around, meaning if a chunk is
                        // rewritten correctly they will be removed automatically, as well as if no index
                        // file requires the chunk anymore (won't get to this loop then)
                        for i in 0..=9 {
                            let bad_ext = format!("{}.bad", i);
                            let mut bad_path = PathBuf::new();
                            bad_path.push(self.chunk_path(digest).0);
                            bad_path.set_extension(bad_ext);
                            self.inner.chunk_store.cond_touch_path(&bad_path, false)?;
                        }
                    }
                }
                Some(ref _s3_client) => {
                    // Update atime on local cache marker files.
                    if !self.inner.chunk_store.cond_touch_chunk(digest, false)? {
                        let (chunk_path, _digest) = self.chunk_path(digest);
                        // Insert empty file as marker to tell GC phase2 that this is
                        // a chunk still in-use, so to keep in the S3 object store.
                        std::fs::File::options()
                            .write(true)
                            .create_new(true)
                            .open(&chunk_path)
                            .with_context(|| {
                                format!("failed to create marker for chunk {}", hex::encode(digest))
                            })?;
                    }
                }
            }
        }
        Ok(())
    }

    fn mark_used_chunks(
        &self,
        status: &mut GarbageCollectionStatus,
        worker: &dyn WorkerTaskContext,
        cache_capacity: usize,
        s3_client: Option<Arc<S3Client>>,
    ) -> Result<(), Error> {
        // Iterate twice over the datastore to fetch index files, even if this comes with an
        // additional runtime cost:
        // - First iteration to find all index files, no matter if they are in a location expected
        //   by the datastore's hierarchy
        // - Iterate using the datastore's helpers, so the namespaces, groups and snapshots are
        //   looked up given the expected hierarchy and iterator logic
        //
        // By this it is assured that all index files are used, even if they would not have been
        // seen by the regular logic and the user is informed by the garbage collection run about
        // the detected index files not following the iterators logic.

        let mut unprocessed_index_list = self.list_index_files()?;
        let mut index_count = unprocessed_index_list.len();

        let mut chunk_lru_cache = if cache_capacity > 0 {
            Some(LruCache::new(cache_capacity))
        } else {
            None
        };
        let mut processed_index_files = 0;
        let mut last_percentage: usize = 0;

        let arc_self = Arc::new(self.clone());
        for namespace in arc_self
            .recursive_iter_backup_ns(BackupNamespace::root())
            .context("creating namespace iterator failed")?
        {
            let namespace = namespace.context("iterating namespaces failed")?;
            for group in arc_self.iter_backup_groups(namespace)? {
                let group = group.context("iterating backup groups failed")?;

                // Avoid race between listing/marking of snapshots by GC and pruning the last
                // snapshot in the group, following a new snapshot creation. Otherwise known chunks
                // might only be referenced by the new snapshot, so it must be read as well.
                let mut retry_counter = 0;
                'retry: loop {
                    let _lock = match retry_counter {
                        0..=9 => None,
                        10 => Some(
                            group
                                .lock()
                                .context("exhausted retries and failed to lock group")?,
                        ),
                        _ => bail!("exhausted retries and unexpected counter overrun"),
                    };

                    let mut snapshots = match group.list_backups() {
                        Ok(snapshots) => snapshots,
                        Err(err) => {
                            if group.exists() {
                                return Err(err).context("listing snapshots failed")?;
                            }
                            break 'retry;
                        }
                    };

                    // Always start iteration with the last snapshot of the group to reduce race
                    // window with concurrent backup+prune previous last snapshot. Allows to retry
                    // without the need to keep track of already processed index files for the
                    // current group.
                    BackupInfo::sort_list(&mut snapshots, true);
                    for (count, snapshot) in snapshots.into_iter().rev().enumerate() {
                        for file in snapshot.files {
                            worker.check_abort()?;
                            worker.fail_on_shutdown()?;

                            match ArchiveType::from_path(&file) {
                                Ok(ArchiveType::FixedIndex) | Ok(ArchiveType::DynamicIndex) => (),
                                Ok(ArchiveType::Blob) | Err(_) => continue,
                            }

                            let mut path = snapshot.backup_dir.full_path();
                            path.push(file);

                            let index = match self.open_index_reader(&path)? {
                                Some(index) => index,
                                None => {
                                    unprocessed_index_list.remove(&path);
                                    if count == 0 {
                                        retry_counter += 1;
                                        continue 'retry;
                                    }
                                    continue;
                                }
                            };

                            self.index_mark_used_chunks(
                                index,
                                &path,
                                &mut chunk_lru_cache,
                                status,
                                worker,
                                s3_client.as_ref().cloned(),
                            )?;

                            if !unprocessed_index_list.remove(&path) {
                                info!("Encountered new index file '{path:?}', increment total index file count");
                                index_count += 1;
                            }

                            let percentage = (processed_index_files + 1) * 100 / index_count;
                            if percentage > last_percentage {
                                info!(
                                    "marked {percentage}% ({} of {index_count} index files)",
                                    processed_index_files + 1,
                                );
                                last_percentage = percentage;
                            }
                            processed_index_files += 1;
                        }
                    }

                    break;
                }
            }
        }

        let mut strange_paths_count = unprocessed_index_list.len();
        for path in unprocessed_index_list {
            let index = match self.open_index_reader(&path)? {
                Some(index) => index,
                None => {
                    // do not count vanished (pruned) backup snapshots as strange paths.
                    strange_paths_count -= 1;
                    continue;
                }
            };
            self.index_mark_used_chunks(
                index,
                &path,
                &mut chunk_lru_cache,
                status,
                worker,
                s3_client.as_ref().cloned(),
            )?;
            warn!("Marked chunks for unexpected index file at '{path:?}'");
        }
        if strange_paths_count > 0 {
            warn!("Found {strange_paths_count} index files outside of expected directory scheme");
        }

        Ok(())
    }

    pub fn last_gc_status(&self) -> GarbageCollectionStatus {
        self.inner.last_gc_status.lock().unwrap().clone()
    }

    pub fn garbage_collection_running(&self) -> bool {
        let lock = self.inner.gc_mutex.try_lock();

        // Mutex currently locked or contains an exclusive lock
        lock.is_err() || lock.unwrap().is_some()
    }

    pub fn garbage_collection(
        &self,
        worker: &dyn WorkerTaskContext,
        upid: &UPID,
    ) -> Result<(), Error> {
        if let Ok(mut mutex) = self.inner.gc_mutex.try_lock() {
            // avoids that we run GC if an old daemon process has still a
            // running backup writer, which is not save as we have no "oldest
            // writer" information and thus no safe atime cutoff
            let exclusive_lock = self.inner.chunk_store.try_exclusive_lock()?;
            // keep the exclusive lock around for the duration of GC
            if mutex.is_some() {
                bail!("Obtained exclusive lock on datastore twice - this should never happen!");
            }
            *mutex = Some(exclusive_lock);
            drop(mutex);

            let res = self.garbage_collection_impl(worker, upid);

            // and now drop it again
            let mut mutex = self.inner.gc_mutex.lock().unwrap();
            if mutex.take().is_none() {
                warn!("Lost exclusive datastore lock during GC - this should never happen!");
            }

            res
        } else {
            bail!("Start GC failed - (already running/locked)");
        }
    }

    fn garbage_collection_impl(
        &self,
        worker: &dyn WorkerTaskContext,
        upid: &UPID,
    ) -> Result<(), Error> {
        let (config, _digest) = pbs_config::datastore::config()?;
        let gc_store_config: DataStoreConfig = config.lookup("datastore", self.name())?;
        let all_stores = config.convert_to_typed_array("datastore")?;
        if let Err(err) = gc_store_config.ensure_not_nested(&all_stores) {
            info!(
                "Current datastore path: {path}",
                path = gc_store_config.absolute_path()
            );
            bail!("Aborting GC for safety reasons: {err}");
        }

        let phase1_start_time = proxmox_time::epoch_i64();
        let oldest_writer = self
            .inner
            .chunk_store
            .oldest_writer()
            .unwrap_or(phase1_start_time);

        let mut gc_status = GarbageCollectionStatus {
            upid: Some(upid.to_string()),
            cache_stats: Some(GarbageCollectionCacheStats::default()),
            ..Default::default()
        };
        let tuning: DatastoreTuning = serde_json::from_value(
            DatastoreTuning::API_SCHEMA
                .parse_property_string(gc_store_config.tuning.as_deref().unwrap_or(""))?,
        )?;

        let s3_client = match self.backend()? {
            DatastoreBackend::Filesystem => None,
            DatastoreBackend::S3(s3_client) => {
                proxmox_async::runtime::block_on(s3_client.head_bucket())
                    .context("failed to reach bucket")?;
                Some(s3_client)
            }
        };

        if tuning.gc_atime_safety_check.unwrap_or(true) {
            self.inner
                .chunk_store
                .check_fs_atime_updates(true, s3_client.clone())
                .context("atime safety check failed")?;
            info!("Access time update check successful, proceeding with GC.");
        } else {
            info!("Access time update check disabled by datastore tuning options.");
        };

        // Fallback to default 24h 5m if not set
        let cutoff = tuning
            .gc_atime_cutoff
            .map(|cutoff| cutoff * 60)
            .unwrap_or(3600 * 24 + 300);

        let mut min_atime = phase1_start_time - cutoff as i64;
        info!(
            "Using access time cutoff {}, minimum access time is {}",
            TimeSpan::from(Duration::from_secs(cutoff as u64)),
            proxmox_time::epoch_to_rfc3339_utc(min_atime)?,
        );
        if oldest_writer < min_atime {
            min_atime = oldest_writer - 300; // account for 5 min safety gap
            info!(
                "Oldest backup writer started at {}, extending minimum access time to {}",
                TimeSpan::from(Duration::from_secs(oldest_writer as u64)),
                proxmox_time::epoch_to_rfc3339_utc(min_atime)?,
            );
        }

        let tuning: DatastoreTuning = serde_json::from_value(
            DatastoreTuning::API_SCHEMA
                .parse_property_string(gc_store_config.tuning.as_deref().unwrap_or(""))?,
        )?;
        let gc_cache_capacity = if let Some(capacity) = tuning.gc_cache_capacity {
            info!("Using chunk digest cache capacity of {capacity}.");
            capacity
        } else {
            1024 * 1024
        };

        info!("Start GC phase1 (mark used chunks)");

        self.mark_used_chunks(
            &mut gc_status,
            worker,
            gc_cache_capacity,
            s3_client.as_ref().cloned(),
        )
        .context("marking used chunks failed")?;

        info!("Start GC phase2 (sweep unused chunks)");

        if let Some(ref s3_client) = s3_client {
            let mut chunk_count = 0;
            let prefix = S3PathPrefix::Some(".chunks/".to_string());
            // Operates in batches of 1000 objects max per request
            let mut list_bucket_result =
                proxmox_async::runtime::block_on(s3_client.list_objects_v2(&prefix, None))
                    .context("failed to list chunk in s3 object store")?;

            let mut delete_list = Vec::with_capacity(1000);
            loop {
                let lock = self.inner.chunk_store.mutex().lock().unwrap();

                for content in list_bucket_result.contents {
                    if self
                        .mark_chunk_for_object_key(
                            &content.key,
                            content.size,
                            min_atime,
                            oldest_writer,
                            &mut delete_list,
                            &mut gc_status,
                        )
                        .with_context(|| {
                            format!("failed to mark chunk for object key {}", content.key)
                        })?
                    {
                        chunk_count += 1;
                    }
                }

                if !delete_list.is_empty() {
                    let delete_objects_result =
                        proxmox_async::runtime::block_on(s3_client.delete_objects(&delete_list))?;
                    if let Some(_err) = delete_objects_result.error {
                        bail!("failed to delete some objects");
                    }
                    delete_list.clear();
                }

                drop(lock);

                // Process next batch of chunks if there is more
                if list_bucket_result.is_truncated {
                    list_bucket_result =
                        proxmox_async::runtime::block_on(s3_client.list_objects_v2(
                            &prefix,
                            list_bucket_result.next_continuation_token.as_deref(),
                        ))?;
                    continue;
                }

                break;
            }
            info!("processed {chunk_count} total chunks");

            // Phase 2 GC of Filesystem backed storage is phase 3 for S3 backed GC
            info!("Start GC phase3 (sweep unused chunk markers)");

            let mut tmp_gc_status = GarbageCollectionStatus {
                upid: Some(upid.to_string()),
                ..Default::default()
            };
            self.inner.chunk_store.sweep_unused_chunks(
                oldest_writer,
                min_atime,
                &mut tmp_gc_status,
                worker,
            )?;
        } else {
            self.inner.chunk_store.sweep_unused_chunks(
                oldest_writer,
                min_atime,
                &mut gc_status,
                worker,
            )?;
        }

        if let Some(cache_stats) = &gc_status.cache_stats {
            let total_cache_counts = cache_stats.hits + cache_stats.misses;
            if total_cache_counts > 0 {
                let cache_hit_ratio = (cache_stats.hits as f64 * 100.) / total_cache_counts as f64;
                info!(
                    "Chunk cache: hits {}, misses {} (hit ratio {cache_hit_ratio:.2}%)",
                    cache_stats.hits, cache_stats.misses,
                );
            }
        }
        info!(
            "Removed garbage: {}",
            HumanByte::from(gc_status.removed_bytes),
        );
        info!("Removed chunks: {}", gc_status.removed_chunks);
        if gc_status.pending_bytes > 0 {
            info!(
                "Pending removals: {} (in {} chunks)",
                HumanByte::from(gc_status.pending_bytes),
                gc_status.pending_chunks,
            );
        }
        if gc_status.removed_bad > 0 {
            info!("Removed bad chunks: {}", gc_status.removed_bad);
        }

        if gc_status.still_bad > 0 {
            info!("Leftover bad chunks: {}", gc_status.still_bad);
        }

        info!(
            "Original data usage: {}",
            HumanByte::from(gc_status.index_data_bytes),
        );

        if gc_status.index_data_bytes > 0 {
            let comp_per = (gc_status.disk_bytes as f64 * 100.) / gc_status.index_data_bytes as f64;
            info!(
                "On-Disk usage: {} ({comp_per:.2}%)",
                HumanByte::from(gc_status.disk_bytes)
            );
        }

        info!("On-Disk chunks: {}", gc_status.disk_chunks);

        let deduplication_factor = if gc_status.disk_bytes > 0 {
            (gc_status.index_data_bytes as f64) / (gc_status.disk_bytes as f64)
        } else {
            1.0
        };

        info!("Deduplication factor: {deduplication_factor:.2}");

        if gc_status.disk_chunks > 0 {
            let avg_chunk = gc_status.disk_bytes / (gc_status.disk_chunks as u64);
            info!("Average chunk size: {}", HumanByte::from(avg_chunk));
        }

        if let Ok(serialized) = serde_json::to_string(&gc_status) {
            let mut path = self.base_path();
            path.push(".gc-status");

            let backup_user = pbs_config::backup_user()?;
            let mode = nix::sys::stat::Mode::from_bits_truncate(0o0644);
            // set the correct owner/group/permissions while saving file
            // owner(rw) = backup, group(r)= backup
            let options = CreateOptions::new()
                .perm(mode)
                .owner(backup_user.uid)
                .group(backup_user.gid);

            // ignore errors
            let _ = replace_file(path, serialized.as_bytes(), options, false);
        }

        *self.inner.last_gc_status.lock().unwrap() = gc_status;
        Ok(())
    }

    // Mark the chunk marker in the local cache store for the given object key as in use
    // by updating it's atime.
    // Returns Ok(true) if the chunk was updated and Ok(false) if the object was not a chunk.
    fn mark_chunk_for_object_key(
        &self,
        object_key: &S3ObjectKey,
        size: u64,
        min_atime: i64,
        oldest_writer: i64,
        delete_list: &mut Vec<S3ObjectKey>,
        gc_status: &mut GarbageCollectionStatus,
    ) -> Result<bool, Error> {
        let (chunk_path, digest) = match self.chunk_path_from_object_key(object_key) {
            Some(path) => path,
            None => return Ok(false),
        };

        // Check local markers (created or atime updated during phase1) and
        // keep or delete chunk based on that.
        let atime = match std::fs::metadata(&chunk_path) {
            Ok(stat) => stat.accessed()?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // File not found, delete by setting atime to unix epoch
                info!("Not found, mark for deletion: {object_key}");
                SystemTime::UNIX_EPOCH
            }
            Err(err) => return Err(err.into()),
        };
        let atime = atime.duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as i64;

        let bad = chunk_path
            .as_path()
            .extension()
            .is_some_and(|ext| ext == "bad");

        if atime < min_atime {
            if let Some(cache) = self.cache() {
                // ignore errors, phase 3 will retry cleanup anyways
                let _ = cache.remove(&digest);
            }
            delete_list.push(object_key.clone());
            if bad {
                gc_status.removed_bad += 1;
            } else {
                gc_status.removed_chunks += 1;
            }
            gc_status.removed_bytes += size;
        } else if atime < oldest_writer {
            if bad {
                gc_status.still_bad += 1;
            } else {
                gc_status.pending_chunks += 1;
            }
            gc_status.pending_bytes += size;
        } else {
            if !bad {
                gc_status.disk_chunks += 1;
            }
            gc_status.disk_bytes += size;
        }

        Ok(true)
    }

    // Check and generate a chunk path from given object key
    fn chunk_path_from_object_key(&self, object_key: &S3ObjectKey) -> Option<(PathBuf, [u8; 32])> {
        // Check object is actually a chunk
        // file_name() should always be Some, as objects will have a filename
        let digest = Path::new::<str>(object_key).file_name()?;
        let bytes = digest.as_bytes();
        if bytes.len() != 64 && bytes.len() != 64 + ".0.bad".len() {
            return None;
        }
        if !bytes.iter().take(64).all(u8::is_ascii_hexdigit) {
            return None;
        }

        // Safe since contains valid ascii hexdigits only as checked above.
        let digest_str = digest.to_string_lossy();
        let hexdigit_prefix = unsafe { digest_str.get_unchecked(0..4) };
        let mut chunk_path = self.base_path();
        chunk_path.push(".chunks");
        chunk_path.push(hexdigit_prefix);
        chunk_path.push(digest);

        let mut digest_bytes = [0u8; 32];
        let digest = digest.as_bytes();
        // safe to unwrap as already checked above
        hex::decode_to_slice(&digest[..64], &mut digest_bytes).unwrap();

        Some((chunk_path, digest_bytes))
    }

    pub fn try_shared_chunk_store_lock(&self) -> Result<ProcessLockSharedGuard, Error> {
        self.inner.chunk_store.try_shared_lock()
    }

    pub fn chunk_path(&self, digest: &[u8; 32]) -> (PathBuf, String) {
        self.inner.chunk_store.chunk_path(digest)
    }

    pub fn cond_touch_chunk(&self, digest: &[u8; 32], assert_exists: bool) -> Result<bool, Error> {
        self.inner
            .chunk_store
            .cond_touch_chunk(digest, assert_exists)
    }

    pub fn insert_chunk(&self, chunk: &DataBlob, digest: &[u8; 32]) -> Result<(bool, u64), Error> {
        self.inner.chunk_store.insert_chunk(chunk, digest)
    }

    pub fn stat_chunk(&self, digest: &[u8; 32]) -> Result<std::fs::Metadata, Error> {
        let (chunk_path, _digest_str) = self.inner.chunk_store.chunk_path(digest);
        std::fs::metadata(chunk_path).map_err(Error::from)
    }

    pub fn load_chunk(&self, digest: &[u8; 32]) -> Result<DataBlob, Error> {
        let (chunk_path, digest_str) = self.inner.chunk_store.chunk_path(digest);

        proxmox_lang::try_block!({
            let mut file = std::fs::File::open(&chunk_path)?;
            DataBlob::load_from_reader(&mut file)
        })
        .map_err(|err| {
            format_err!(
                "store '{}', unable to load chunk '{}' - {}",
                self.name(),
                digest_str,
                err,
            )
        })
    }

    /// Updates the protection status of the specified snapshot.
    pub fn update_protection(&self, backup_dir: &BackupDir, protection: bool) -> Result<(), Error> {
        let full_path = backup_dir.full_path();

        if !full_path.exists() {
            bail!("snapshot {} does not exist!", backup_dir.dir());
        }

        let _guard = backup_dir.lock().with_context(|| {
            format!("while updating the protection status of snapshot '{backup_dir:?}'")
        })?;

        let protected_path = backup_dir.protected_file();
        if protection {
            std::fs::File::create(&protected_path)
                .map_err(|err| format_err!("could not create protection file: {}", err))?;
            if let DatastoreBackend::S3(s3_client) = self.backend()? {
                let object_key = crate::s3::object_key_from_path(
                    &backup_dir.relative_path(),
                    PROTECTED_MARKER_FILENAME,
                )
                .context("invalid protected marker object key")?;
                let _is_duplicate = proxmox_async::runtime::block_on(
                    s3_client
                        .upload_no_replace_with_retry(object_key, hyper::body::Bytes::from("")),
                )
                .context("failed to mark snapshot as protected on s3 backend")?;
            }
        } else {
            if let Err(err) = std::fs::remove_file(&protected_path) {
                // ignore error for non-existing file
                if err.kind() != std::io::ErrorKind::NotFound {
                    bail!("could not remove protection file: {err}");
                }
            }
            if let DatastoreBackend::S3(s3_client) = self.backend()? {
                let object_key = crate::s3::object_key_from_path(
                    &backup_dir.relative_path(),
                    PROTECTED_MARKER_FILENAME,
                )
                .context("invalid protected marker object key")?;
                if let Err(err) =
                    proxmox_async::runtime::block_on(s3_client.delete_object(object_key))
                {
                    std::fs::File::create(&protected_path)
                        .map_err(|err| format_err!("could not re-create protection file: {err}"))?;
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    pub fn verify_new(&self) -> bool {
        self.inner.verify_new
    }

    /// returns a list of chunks sorted by their inode number on disk chunks that couldn't get
    /// stat'ed are placed at the end of the list
    pub fn get_chunks_in_order<F, A>(
        &self,
        index: &(dyn IndexFile + Send),
        skip_chunk: F,
        check_abort: A,
    ) -> Result<Vec<(usize, u64)>, Error>
    where
        F: Fn(&[u8; 32]) -> bool,
        A: Fn(usize) -> Result<(), Error>,
    {
        let index_count = index.index_count();
        let mut chunk_list = Vec::with_capacity(index_count);
        use std::os::unix::fs::MetadataExt;
        for pos in 0..index_count {
            check_abort(pos)?;

            let info = index.chunk_info(pos).unwrap();

            if skip_chunk(&info.digest) {
                continue;
            }

            let ino = match self.inner.chunk_order {
                ChunkOrder::Inode => {
                    match self.stat_chunk(&info.digest) {
                        Err(_) => u64::MAX, // could not stat, move to end of list
                        Ok(metadata) => metadata.ino(),
                    }
                }
                ChunkOrder::None => 0,
            };

            chunk_list.push((pos, ino));
        }

        match self.inner.chunk_order {
            // sorting by inode improves data locality, which makes it lots faster on spinners
            ChunkOrder::Inode => {
                chunk_list.sort_unstable_by(|(_, ino_a), (_, ino_b)| ino_a.cmp(ino_b))
            }
            ChunkOrder::None => {}
        }

        Ok(chunk_list)
    }

    /// Open a backup group from this datastore.
    pub fn backup_group(
        self: &Arc<Self>,
        ns: BackupNamespace,
        group: pbs_api_types::BackupGroup,
    ) -> BackupGroup {
        BackupGroup::new(Arc::clone(self), ns, group)
    }

    /// Open a backup group from this datastore.
    pub fn backup_group_from_parts<T>(
        self: &Arc<Self>,
        ns: BackupNamespace,
        ty: BackupType,
        id: T,
    ) -> BackupGroup
    where
        T: Into<String>,
    {
        self.backup_group(ns, (ty, id.into()).into())
    }

    /*
    /// Open a backup group from this datastore by backup group path such as `vm/100`.
    ///
    /// Convenience method for `store.backup_group(path.parse()?)`
    pub fn backup_group_from_path(self: &Arc<Self>, path: &str) -> Result<BackupGroup, Error> {
        todo!("split out the namespace");
    }
    */

    /// Open a snapshot (backup directory) from this datastore.
    pub fn backup_dir(
        self: &Arc<Self>,
        ns: BackupNamespace,
        dir: pbs_api_types::BackupDir,
    ) -> Result<BackupDir, Error> {
        BackupDir::with_group(self.backup_group(ns, dir.group), dir.time)
    }

    /// Open a snapshot (backup directory) from this datastore.
    pub fn backup_dir_from_parts<T>(
        self: &Arc<Self>,
        ns: BackupNamespace,
        ty: BackupType,
        id: T,
        time: i64,
    ) -> Result<BackupDir, Error>
    where
        T: Into<String>,
    {
        self.backup_dir(ns, (ty, id.into(), time).into())
    }

    /// Open a snapshot (backup directory) from this datastore with a cached rfc3339 time string.
    pub fn backup_dir_with_rfc3339<T: Into<String>>(
        self: &Arc<Self>,
        group: BackupGroup,
        time_string: T,
    ) -> Result<BackupDir, Error> {
        BackupDir::with_rfc3339(group, time_string.into())
    }

    /*
    /// Open a snapshot (backup directory) from this datastore by a snapshot path.
    pub fn backup_dir_from_path(self: &Arc<Self>, path: &str) -> Result<BackupDir, Error> {
        todo!("split out the namespace");
    }
    */

    /// Syncs the filesystem of the datastore if 'sync_level' is set to
    /// [`DatastoreFSyncLevel::Filesystem`]. Uses syncfs(2).
    pub fn try_ensure_sync_level(&self) -> Result<(), Error> {
        if self.inner.sync_level != DatastoreFSyncLevel::Filesystem {
            return Ok(());
        }
        let file = std::fs::File::open(self.base_path())?;
        let fd = file.as_raw_fd();
        log::info!("syncing filesystem");
        if unsafe { libc::syncfs(fd) } < 0 {
            bail!("error during syncfs: {}", std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Destroy a datastore. This requires that there are no active operations on the datastore.
    ///
    /// This is a synchronous operation and should be run in a worker-thread.
    pub fn destroy(name: &str, destroy_data: bool) -> Result<(), Error> {
        let config_lock = pbs_config::datastore::lock_config()?;

        let (mut config, _digest) = pbs_config::datastore::config()?;
        let mut datastore_config: DataStoreConfig = config.lookup("datastore", name)?;

        datastore_config.set_maintenance_mode(Some(MaintenanceMode {
            ty: MaintenanceType::Delete,
            message: None,
        }))?;

        config.set_data(name, "datastore", &datastore_config)?;
        pbs_config::datastore::save_config(&config)?;
        drop(config_lock);

        let (operations, _lock) = task_tracking::get_active_operations_locked(name)?;

        if operations.read != 0 || operations.write != 0 {
            bail!("datastore is currently in use");
        }

        let base = PathBuf::from(datastore_config.absolute_path());

        let mut ok = true;
        if destroy_data {
            let remove = |subdir, ok: &mut bool| {
                if let Err(err) = std::fs::remove_dir_all(base.join(subdir)) {
                    if err.kind() != io::ErrorKind::NotFound {
                        warn!("failed to remove {subdir:?} subdirectory: {err}");
                        *ok = false;
                    }
                }
            };

            info!("Deleting datastore data...");
            remove("ns", &mut ok); // ns first
            remove("ct", &mut ok);
            remove("vm", &mut ok);
            remove("host", &mut ok);

            if ok {
                if let Err(err) = std::fs::remove_file(base.join(".gc-status")) {
                    if err.kind() != io::ErrorKind::NotFound {
                        warn!("failed to remove .gc-status file: {err}");
                        ok = false;
                    }
                }
            }

            if let (_backend, Some(s3_client)) =
                Self::s3_client_and_backend_from_datastore_config(&datastore_config)?
            {
                // Delete all objects within the datastore prefix
                let prefix = S3PathPrefix::Some(String::default());
                let delete_objects_error =
                    proxmox_async::runtime::block_on(s3_client.delete_objects_by_prefix(&prefix))?;
                if delete_objects_error {
                    bail!("deleting objects failed");
                }
            }

            // chunks get removed last and only if the backups were successfully deleted
            if ok {
                remove(".chunks", &mut ok);
            }
        } else if let (_backend, Some(s3_client)) =
            Self::s3_client_and_backend_from_datastore_config(&datastore_config)?
        {
            // Only delete in-use marker so datastore can be re-imported
            let object_key = S3ObjectKey::try_from(S3_DATASTORE_IN_USE_MARKER)
                .context("failed to generate in-use marker object key")?;
            proxmox_async::runtime::block_on(s3_client.delete_object(object_key))
                .context("failed to delete in-use marker")?;
        }

        // now the config
        if ok {
            info!("Removing datastore from config...");
            let _lock = pbs_config::datastore::lock_config()?;
            let _ = config.sections.remove(name);
            pbs_config::datastore::save_config(&config)?;
        }

        // finally the lock & toplevel directory
        if destroy_data {
            if ok {
                if let Err(err) = std::fs::remove_file(base.join(".lock")) {
                    if err.kind() != io::ErrorKind::NotFound {
                        warn!("failed to remove .lock file: {err}");
                        ok = false;
                    }
                }
            }

            if ok {
                info!("Finished deleting data.");

                match std::fs::remove_dir(base) {
                    Ok(()) => info!("Removed empty datastore directory."),
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        // weird, but ok
                    }
                    Err(err) if err.is_errno(nix::errno::Errno::EBUSY) => {
                        if datastore_config.backing_device.is_none() {
                            warn!("Cannot delete datastore directory (is it a mount point?).")
                        }
                    }
                    Err(err) if err.is_errno(nix::errno::Errno::ENOTEMPTY) => {
                        warn!("Datastore directory not empty, not deleting.")
                    }
                    Err(err) => {
                        warn!("Failed to remove datastore directory: {err}");
                    }
                }
            } else {
                info!("There were errors deleting data.");
            }
        }

        Ok(())
    }

    pub fn old_locking(&self) -> bool {
        *OLD_LOCKING
    }

    /// Set the datastore's maintenance mode to `S3Refresh`, fetch from S3 object store, clear and
    /// replace the local cache store contents. Once finished disable the maintenance mode again.
    /// Returns with error for other datastore backends without setting the maintenance mode.
    pub async fn s3_refresh(self: &Arc<Self>) -> Result<(), Error> {
        match self.backend()? {
            DatastoreBackend::Filesystem => bail!("store '{}' not backed by S3", self.name()),
            DatastoreBackend::S3(s3_client) => {
                let self_clone = Arc::clone(self);
                tokio::task::spawn_blocking(move || {
                    self_clone.maintenance_mode(Some(MaintenanceMode {
                        ty: MaintenanceType::S3Refresh,
                        message: None,
                    }))
                })
                .await?
                .context("failed to set maintenance mode")?;

                let tmp_base = proxmox_sys::fs::make_tmp_dir(self.base_path(), None)
                    .context("failed to create temporary content folder in {store_base}")?;

                if let Err(err) = async {
                    self.fetch_tmp_contents(&tmp_base, &s3_client).await?;
                    self.move_tmp_contents_in_place(&tmp_base).await
                }
                .await
                {
                    // igonre cleaunp errors, cannot act anyways
                    let _ = std::fs::remove_dir_all(&tmp_base);
                    return Err(err);
                }

                let self_clone = Arc::clone(self);
                tokio::task::spawn_blocking(move || self_clone.maintenance_mode(None))
                    .await?
                    .context("failed to clear maintenance mode")?;
            }
        }
        Ok(())
    }

    // Set or clear the datastores maintenance mode by locking and updating the datastore config
    fn maintenance_mode(&self, maintenance_mode: Option<MaintenanceMode>) -> Result<(), Error> {
        let _lock = pbs_config::datastore::lock_config()?;
        let (mut section_config, _digest) = pbs_config::datastore::config()?;
        let mut datastore: DataStoreConfig = section_config.lookup("datastore", self.name())?;
        datastore.set_maintenance_mode(maintenance_mode)?;
        section_config.set_data(self.name(), "datastore", &datastore)?;
        pbs_config::datastore::save_config(&section_config)?;
        Ok(())
    }

    // Fetch the contents (metadata, no chunks) of the datastore from the S3 object store to the
    // provided temporaray directory
    async fn fetch_tmp_contents(&self, tmp_base: &Path, s3_client: &S3Client) -> Result<(), Error> {
        let backup_user = pbs_config::backup_user().context("failed to get backup user")?;
        let mode = nix::sys::stat::Mode::from_bits_truncate(0o0644);
        let file_create_options = CreateOptions::new()
            .perm(mode)
            .owner(backup_user.uid)
            .group(backup_user.gid);
        let mode = nix::sys::stat::Mode::from_bits_truncate(0o0755);
        let dir_create_options = CreateOptions::new()
            .perm(mode)
            .owner(backup_user.uid)
            .group(backup_user.gid);

        let list_prefix = S3PathPrefix::Some(S3_CONTENT_PREFIX.to_string());
        let store_prefix = format!("{}/{S3_CONTENT_PREFIX}/", self.name());
        let mut next_continuation_token: Option<String> = None;
        loop {
            let list_objects_result = s3_client
                .list_objects_v2(&list_prefix, next_continuation_token.as_deref())
                .await
                .context("failed to list object")?;

            let objects_to_fetch: Vec<S3ObjectKey> = list_objects_result
                .contents
                .into_iter()
                .map(|item| item.key)
                .collect();

            for object_key in objects_to_fetch {
                let object_path = format!("{object_key}");
                let object_path = object_path.strip_prefix(&store_prefix).with_context(|| {
                    format!("failed to strip store context prefix {store_prefix} for {object_key}")
                })?;
                if object_path.ends_with(NAMESPACE_MARKER_FILENAME) {
                    continue;
                }

                info!("Fetching object {object_path}");

                let file_path = tmp_base.join(object_path);
                if let Some(parent) = file_path.parent() {
                    proxmox_sys::fs::create_path(
                        parent,
                        Some(dir_create_options),
                        Some(dir_create_options),
                    )?;
                }

                let mut target_file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .open(&file_path)
                    .await
                    .with_context(|| format!("failed to create target file {file_path:?}"))?;

                if let Some(response) = s3_client
                    .get_object(object_key)
                    .await
                    .with_context(|| format!("failed to fetch object {object_path}"))?
                {
                    let data = response
                        .content
                        .collect()
                        .await
                        .context("failed to collect object contents")?;
                    target_file
                        .write_all(&data.to_bytes())
                        .await
                        .context("failed to write to target file")?;
                    file_create_options
                        .apply_to(&mut target_file, &file_path)
                        .context("failed to set target file create options")?;
                    target_file
                        .flush()
                        .await
                        .context("failed to flush target file")?;
                } else {
                    bail!("failed to download {object_path}, not found");
                }
            }

            if list_objects_result.is_truncated {
                next_continuation_token = list_objects_result
                    .next_continuation_token
                    .as_ref()
                    .cloned();
                continue;
            }
            break;
        }
        Ok(())
    }

    // Fetch the contents (metadata, no chunks) of the datastore from the S3 object store to the
    // provided temporaray directory
    async fn move_tmp_contents_in_place(&self, tmp_base: &PathBuf) -> Result<(), Error> {
        for ty in ["vm", "ct", "host", "ns"] {
            let store_base_clone = self.base_path().clone();
            let tmp_base_clone = tmp_base.clone();
            tokio::task::spawn_blocking(move || {
                let type_dir = store_base_clone.join(ty);
                if let Err(err) = std::fs::remove_dir_all(&type_dir) {
                    if err.kind() != io::ErrorKind::NotFound {
                        return Err(err).with_context(|| {
                            format!("failed to remove old contents in {type_dir:?}")
                        });
                    }
                }
                let tmp_type_dir = tmp_base_clone.join(ty);
                if let Err(err) = std::fs::rename(&tmp_type_dir, &type_dir) {
                    if err.kind() != io::ErrorKind::NotFound {
                        return Err(err)
                            .with_context(|| format!("failed to rename {tmp_type_dir:?}"));
                    }
                }
                Ok::<(), Error>(())
            })
            .await?
            .with_context(|| format!("failed to refresh {:?}", self.base_path()))?;
        }

        std::fs::remove_dir_all(tmp_base)
            .with_context(|| format!("failed to cleanup temporary content in {tmp_base:?}"))?;

        Ok(())
    }

    pub fn s3_client_and_backend_from_datastore_config(
        datastore_config: &DataStoreConfig,
    ) -> Result<(DatastoreBackendType, Option<S3Client>), Error> {
        let backend_config: DatastoreBackendConfig =
            datastore_config.backend.as_deref().unwrap_or("").parse()?;
        let backend_type = backend_config.ty.unwrap_or_default();

        if backend_type != DatastoreBackendType::S3 {
            return Ok((backend_type, None));
        }

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
        let client_config: S3ClientConf = config
            .lookup(S3_CFG_TYPE_ID, s3_client_id)
            .with_context(|| format!("no '{s3_client_id}' in config"))?;
        let options = S3ClientOptions::from_config(
            client_config.config,
            client_config.secret_key,
            Some(bucket),
            datastore_config.name.to_owned(),
        );
        let s3_client = S3Client::new(options)
            .context("failed to create s3 client")
            .map_err(|err| format_err!("{err:#}"))?;
        Ok((backend_type, Some(s3_client)))
    }
}
