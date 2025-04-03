use std::fmt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::prelude::OsStrExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::{bail, format_err, Context, Error};

use proxmox_sys::fs::{lock_dir_noblock, lock_dir_noblock_shared, replace_file, CreateOptions};
use proxmox_systemd::escape_unit;

use pbs_api_types::{
    Authid, BackupGroupDeleteStats, BackupNamespace, BackupType, GroupFilter, VerifyState,
    BACKUP_DATE_REGEX, BACKUP_FILE_REGEX, CLIENT_LOG_BLOB_NAME, MANIFEST_BLOB_NAME,
};
use pbs_config::{open_backup_lockfile, BackupLockGuard};

use crate::manifest::{BackupManifest, MANIFEST_LOCK_NAME};
use crate::{DataBlob, DataStore};

pub const DATASTORE_LOCKS_DIR: &str = "/run/proxmox-backup/locks";

// TODO: Remove with PBS 5
// Note: The `expect()` call here will only happen if we can neither confirm nor deny the existence
// of the file. this should only happen if a user messes with the `/run/proxmox-backup` directory.
// if that happens, a lot more should fail as we rely on the existence of the directory throughout
// the code. so just panic with a reasonable message.
static OLD_LOCKING: LazyLock<bool> = LazyLock::new(|| {
    std::fs::exists("/run/proxmox-backup/old-locking")
        .expect("cannot read `/run/proxmox-backup`, please check permissions")
});

/// BackupGroup is a directory containing a list of BackupDir
#[derive(Clone)]
pub struct BackupGroup {
    store: Arc<DataStore>,

    ns: BackupNamespace,
    group: pbs_api_types::BackupGroup,
}

impl fmt::Debug for BackupGroup {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BackupGroup")
            .field("store", &self.store.name())
            .field("ns", &self.ns)
            .field("group", &self.group)
            .finish()
    }
}

impl BackupGroup {
    pub(crate) fn new(
        store: Arc<DataStore>,
        ns: BackupNamespace,
        group: pbs_api_types::BackupGroup,
    ) -> Self {
        Self { store, ns, group }
    }

    /// Access the underlying [`BackupGroup`](pbs_api_types::BackupGroup).
    #[inline]
    pub fn group(&self) -> &pbs_api_types::BackupGroup {
        &self.group
    }

    #[inline]
    pub fn backup_ns(&self) -> &BackupNamespace {
        &self.ns
    }

    #[inline]
    pub fn backup_type(&self) -> BackupType {
        self.group.ty
    }

    #[inline]
    pub fn backup_id(&self) -> &str {
        &self.group.id
    }

    pub fn full_group_path(&self) -> PathBuf {
        self.store.group_path(&self.ns, &self.group)
    }

    pub fn relative_group_path(&self) -> PathBuf {
        let mut path = self.ns.path();
        path.push(self.group.ty.as_str());
        path.push(&self.group.id);
        path
    }

    /// Simple check whether a group exists. This does not check whether there are any snapshots,
    /// but rather it simply checks whether the directory exists.
    pub fn exists(&self) -> bool {
        self.full_group_path().exists()
    }

    pub fn list_backups(&self) -> Result<Vec<BackupInfo>, Error> {
        let mut list = vec![];

        let path = self.full_group_path();

        proxmox_sys::fs::scandir(
            libc::AT_FDCWD,
            &path,
            &BACKUP_DATE_REGEX,
            |l2_fd, backup_time, file_type| {
                if file_type != nix::dir::Type::Directory {
                    return Ok(());
                }

                let backup_dir = self.backup_dir_with_rfc3339(backup_time)?;
                let files = list_backup_files(l2_fd, backup_time)?;

                let protected = backup_dir.is_protected();

                list.push(BackupInfo {
                    backup_dir,
                    files,
                    protected,
                });

                Ok(())
            },
        )?;
        Ok(list)
    }

    /// Finds the latest backup inside a backup group
    pub fn last_backup(&self, only_finished: bool) -> Result<Option<BackupInfo>, Error> {
        let backups = self.list_backups()?;
        Ok(backups
            .into_iter()
            .filter(|item| !only_finished || item.is_finished())
            .max_by_key(|item| item.backup_dir.backup_time()))
    }

    pub fn last_successful_backup(&self) -> Result<Option<i64>, Error> {
        let mut last = None;

        let path = self.full_group_path();

        proxmox_sys::fs::scandir(
            libc::AT_FDCWD,
            &path,
            &BACKUP_DATE_REGEX,
            |l2_fd, backup_time, file_type| {
                if file_type != nix::dir::Type::Directory {
                    return Ok(());
                }

                let mut manifest_path = PathBuf::from(backup_time);
                manifest_path.push(MANIFEST_BLOB_NAME.as_ref());

                use nix::fcntl::{openat, OFlag};
                match openat(
                    l2_fd,
                    &manifest_path,
                    OFlag::O_RDONLY | OFlag::O_CLOEXEC,
                    nix::sys::stat::Mode::empty(),
                ) {
                    Ok(rawfd) => {
                        /* manifest exists --> assume backup was successful */
                        /* close else this leaks! */
                        nix::unistd::close(rawfd)?;
                    }
                    Err(nix::errno::Errno::ENOENT) => {
                        return Ok(());
                    }
                    Err(err) => {
                        bail!("last_successful_backup: unexpected error - {}", err);
                    }
                }

                let timestamp = proxmox_time::parse_rfc3339(backup_time)?;
                if let Some(last_timestamp) = last {
                    if timestamp > last_timestamp {
                        last = Some(timestamp);
                    }
                } else {
                    last = Some(timestamp);
                }

                Ok(())
            },
        )?;

        Ok(last)
    }

    pub fn matches(&self, filter: &GroupFilter) -> bool {
        self.group.matches(filter)
    }

    pub fn backup_dir(&self, time: i64) -> Result<BackupDir, Error> {
        BackupDir::with_group(self.clone(), time)
    }

    pub fn backup_dir_with_rfc3339<T: Into<String>>(
        &self,
        time_string: T,
    ) -> Result<BackupDir, Error> {
        BackupDir::with_rfc3339(self.clone(), time_string.into())
    }

    pub fn iter_snapshots(&self) -> Result<crate::ListSnapshots, Error> {
        crate::ListSnapshots::new(self.clone())
    }

    /// Destroy the group inclusive all its backup snapshots (BackupDir's)
    ///
    /// Returns `BackupGroupDeleteStats`, containing the number of deleted snapshots
    /// and number of protected snaphsots, which therefore were not removed.
    pub fn destroy(&self) -> Result<BackupGroupDeleteStats, Error> {
        let _guard = self
            .lock()
            .with_context(|| format!("while destroying group '{self:?}'"))?;
        let path = self.full_group_path();

        log::info!("removing backup group {:?}", path);
        let mut delete_stats = BackupGroupDeleteStats::default();
        for snap in self.iter_snapshots()? {
            let snap = snap?;
            if snap.is_protected() {
                delete_stats.increment_protected_snapshots();
                continue;
            }
            snap.destroy(false)?;
            delete_stats.increment_removed_snapshots();
        }

        // Note: make sure the old locking mechanism isn't used as `remove_dir_all` is not safe in
        // that case
        if delete_stats.all_removed() && !*OLD_LOCKING {
            self.remove_group_dir()?;
            delete_stats.increment_removed_groups();
        }

        Ok(delete_stats)
    }

    /// Helper function, assumes that no more snapshots are present in the group.
    fn remove_group_dir(&self) -> Result<(), Error> {
        let owner_path = self.store.owner_path(&self.ns, &self.group);

        std::fs::remove_file(&owner_path).map_err(|err| {
            format_err!("removing the owner file '{owner_path:?}' failed - {err}")
        })?;

        let path = self.full_group_path();

        std::fs::remove_dir(&path)
            .map_err(|err| format_err!("removing group directory {path:?} failed - {err}"))?;

        let _ = std::fs::remove_file(self.lock_path());

        Ok(())
    }

    /// Returns the backup owner.
    ///
    /// The backup owner is the entity who first created the backup group.
    pub fn get_owner(&self) -> Result<Authid, Error> {
        self.store.get_owner(&self.ns, self.as_ref())
    }

    /// Set the backup owner.
    pub fn set_owner(&self, auth_id: &Authid, force: bool) -> Result<(), Error> {
        self.store
            .set_owner(&self.ns, self.as_ref(), auth_id, force)
    }

    /// Returns a file name for locking a group.
    ///
    /// The lock file will be located in:
    /// `${DATASTORE_LOCKS_DIR}/${datastore name}/${lock_file_path_helper(rpath)}`
    /// where `rpath` is the relative path of the group.
    fn lock_path(&self) -> PathBuf {
        let path = Path::new(DATASTORE_LOCKS_DIR).join(self.store.name());

        let rpath = Path::new(self.group.ty.as_str()).join(&self.group.id);

        path.join(lock_file_path_helper(&self.ns, rpath))
    }

    /// Locks a group exclusively.
    pub fn lock(&self) -> Result<BackupLockGuard, Error> {
        if *OLD_LOCKING {
            lock_dir_noblock(
                &self.full_group_path(),
                "backup group",
                "possible runing backup, group is in use",
            )
            .map(BackupLockGuard::from)
        } else {
            lock_helper(self.store.name(), &self.lock_path(), |p| {
                open_backup_lockfile(p, Some(Duration::from_secs(0)), true)
                    .with_context(|| format!("unable to acquire backup group lock {p:?}"))
            })
        }
    }
}

impl AsRef<pbs_api_types::BackupNamespace> for BackupGroup {
    #[inline]
    fn as_ref(&self) -> &pbs_api_types::BackupNamespace {
        &self.ns
    }
}

impl AsRef<pbs_api_types::BackupGroup> for BackupGroup {
    #[inline]
    fn as_ref(&self) -> &pbs_api_types::BackupGroup {
        &self.group
    }
}

impl From<&BackupGroup> for pbs_api_types::BackupGroup {
    fn from(group: &BackupGroup) -> pbs_api_types::BackupGroup {
        group.group.clone()
    }
}

impl From<BackupGroup> for pbs_api_types::BackupGroup {
    fn from(group: BackupGroup) -> pbs_api_types::BackupGroup {
        group.group
    }
}

impl From<BackupDir> for BackupGroup {
    fn from(dir: BackupDir) -> BackupGroup {
        BackupGroup {
            store: dir.store,
            ns: dir.ns,
            group: dir.dir.group,
        }
    }
}

impl From<&BackupDir> for BackupGroup {
    fn from(dir: &BackupDir) -> BackupGroup {
        BackupGroup {
            store: Arc::clone(&dir.store),
            ns: dir.ns.clone(),
            group: dir.dir.group.clone(),
        }
    }
}

/// Uniquely identify a Backup (relative to data store)
///
/// We also call this a backup snaphost.
#[derive(Clone)]
pub struct BackupDir {
    store: Arc<DataStore>,
    ns: BackupNamespace,
    dir: pbs_api_types::BackupDir,
    // backup_time as rfc3339
    backup_time_string: String,
}

impl fmt::Debug for BackupDir {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("BackupDir")
            .field("store", &self.store.name())
            .field("ns", &self.ns)
            .field("dir", &self.dir)
            .field("backup_time_string", &self.backup_time_string)
            .finish()
    }
}

impl BackupDir {
    /// Temporarily used for tests.
    #[doc(hidden)]
    pub fn new_test(dir: pbs_api_types::BackupDir) -> Self {
        Self {
            store: unsafe { DataStore::new_test() },
            backup_time_string: Self::backup_time_to_string(dir.time).unwrap(),
            ns: BackupNamespace::root(),
            dir,
        }
    }

    pub(crate) fn with_group(group: BackupGroup, backup_time: i64) -> Result<Self, Error> {
        let backup_time_string = Self::backup_time_to_string(backup_time)?;
        Ok(Self {
            store: group.store,
            ns: group.ns,
            dir: (group.group, backup_time).into(),
            backup_time_string,
        })
    }

    pub(crate) fn with_rfc3339(
        group: BackupGroup,
        backup_time_string: String,
    ) -> Result<Self, Error> {
        let backup_time = proxmox_time::parse_rfc3339(&backup_time_string)?;
        Ok(Self {
            store: group.store,
            ns: group.ns,
            dir: (group.group, backup_time).into(),
            backup_time_string,
        })
    }

    #[inline]
    pub fn backup_ns(&self) -> &BackupNamespace {
        &self.ns
    }

    #[inline]
    pub fn backup_type(&self) -> BackupType {
        self.dir.group.ty
    }

    #[inline]
    pub fn backup_id(&self) -> &str {
        &self.dir.group.id
    }

    #[inline]
    pub fn backup_time(&self) -> i64 {
        self.dir.time
    }

    pub fn backup_time_string(&self) -> &str {
        &self.backup_time_string
    }

    pub fn dir(&self) -> &pbs_api_types::BackupDir {
        &self.dir
    }

    pub fn group(&self) -> &pbs_api_types::BackupGroup {
        &self.dir.group
    }

    pub fn relative_path(&self) -> PathBuf {
        let mut path = self.ns.path();
        path.push(self.dir.group.ty.as_str());
        path.push(&self.dir.group.id);
        path.push(&self.backup_time_string);
        path
    }

    /// Returns the absolute path for backup_dir, using the cached formatted time string.
    pub fn full_path(&self) -> PathBuf {
        let mut path = self.store.base_path();
        path.push(self.relative_path());
        path
    }

    pub fn protected_file(&self) -> PathBuf {
        let mut path = self.full_path();
        path.push(".protected");
        path
    }

    pub fn is_protected(&self) -> bool {
        let path = self.protected_file();
        path.exists()
    }

    pub fn backup_time_to_string(backup_time: i64) -> Result<String, Error> {
        // fixme: can this fail? (avoid unwrap)
        proxmox_time::epoch_to_rfc3339_utc(backup_time)
    }

    /// load a `DataBlob` from this snapshot's backup dir.
    pub fn load_blob(&self, filename: &str) -> Result<DataBlob, Error> {
        let mut path = self.full_path();
        path.push(filename);

        proxmox_lang::try_block!({
            let mut file = std::fs::File::open(&path)?;
            DataBlob::load_from_reader(&mut file)
        })
        .map_err(|err| format_err!("unable to load blob '{:?}' - {}", path, err))
    }

    /// Returns the filename to lock a manifest
    ///
    /// Also creates the basedir. The lockfile is located in
    /// `${DATASTORE_LOCKS_DIR}/${datastore name}/${lock_file_path_helper(rpath)}.index.json.lck`
    /// where rpath is the relative path of the snapshot.
    fn manifest_lock_path(&self) -> PathBuf {
        let path = Path::new(DATASTORE_LOCKS_DIR).join(self.store.name());

        let rpath = Path::new(self.dir.group.ty.as_str())
            .join(&self.dir.group.id)
            .join(&self.backup_time_string)
            .join(MANIFEST_LOCK_NAME);

        path.join(lock_file_path_helper(&self.ns, rpath))
    }

    /// Locks the manifest of a snapshot, for example, to update or delete it.
    pub(crate) fn lock_manifest(&self) -> Result<BackupLockGuard, Error> {
        let path = if *OLD_LOCKING {
            // old manifest lock path
            let path = Path::new(DATASTORE_LOCKS_DIR)
                .join(self.store.name())
                .join(self.relative_path());

            std::fs::create_dir_all(&path)?;

            path.join(format!("{}{MANIFEST_LOCK_NAME}", self.backup_time_string()))
        } else {
            self.manifest_lock_path()
        };

        lock_helper(self.store.name(), &path, |p| {
            // update_manifest should never take a long time, so if
            // someone else has the lock we can simply block a bit
            // and should get it soon
            open_backup_lockfile(p, Some(Duration::from_secs(5)), true)
                .with_context(|| format_err!("unable to acquire manifest lock {p:?}"))
        })
    }

    /// Returns a file name for locking a snapshot.
    ///
    /// The lock file will be located in:
    /// `${DATASTORE_LOCKS_DIR}/${datastore name}/${lock_file_path_helper(rpath)}`
    /// where `rpath` is the relative path of the snapshot.
    fn lock_path(&self) -> PathBuf {
        let path = Path::new(DATASTORE_LOCKS_DIR).join(self.store.name());

        let rpath = Path::new(self.dir.group.ty.as_str())
            .join(&self.dir.group.id)
            .join(&self.backup_time_string);

        path.join(lock_file_path_helper(&self.ns, rpath))
    }

    /// Locks a snapshot exclusively.
    pub fn lock(&self) -> Result<BackupLockGuard, Error> {
        if *OLD_LOCKING {
            lock_dir_noblock(
                &self.full_path(),
                "snapshot",
                "backup is running or snapshot is in use",
            )
            .map(BackupLockGuard::from)
        } else {
            lock_helper(self.store.name(), &self.lock_path(), |p| {
                open_backup_lockfile(p, Some(Duration::from_secs(0)), true)
                    .with_context(|| format!("unable to acquire snapshot lock {p:?}"))
            })
        }
    }

    /// Acquires a shared lock on a snapshot.
    pub fn lock_shared(&self) -> Result<BackupLockGuard, Error> {
        if *OLD_LOCKING {
            lock_dir_noblock_shared(
                &self.full_path(),
                "snapshot",
                "backup is running or snapshot is in use, could not acquire shared lock",
            )
            .map(BackupLockGuard::from)
        } else {
            lock_helper(self.store.name(), &self.lock_path(), |p| {
                open_backup_lockfile(p, Some(Duration::from_secs(0)), false)
                    .with_context(|| format!("unable to acquire shared snapshot lock {p:?}"))
            })
        }
    }

    /// Destroy the whole snapshot, bails if it's protected
    ///
    /// Setting `force` to true skips locking and thus ignores if the backup is currently in use.
    pub fn destroy(&self, force: bool) -> Result<(), Error> {
        let (_guard, _manifest_guard);
        if !force {
            _guard = self
                .lock()
                .with_context(|| format!("while destroying snapshot '{self:?}'"))?;
            _manifest_guard = self.lock_manifest()?;
        }

        if self.is_protected() {
            bail!("cannot remove protected snapshot"); // use special error type?
        }

        let full_path = self.full_path();
        log::info!("removing backup snapshot {:?}", full_path);
        std::fs::remove_dir_all(&full_path).map_err(|err| {
            format_err!("removing backup snapshot {:?} failed - {}", full_path, err,)
        })?;

        // remove no longer needed lock files
        let _ = std::fs::remove_file(self.manifest_lock_path()); // ignore errors
        let _ = std::fs::remove_file(self.lock_path()); // ignore errors

        let group = BackupGroup::from(self);
        let guard = group.lock().with_context(|| {
            format!("while checking if group '{group:?}' is empty during snapshot destruction")
        });

        // Only remove the group if all of the following is true:
        //
        // - we can lock it: if we can't lock the group, it is still in use (either by another
        //   backup process or a parent caller (who needs to take care that empty groups are
        //   removed themselves).
        // - it is now empty: if the group isn't empty, removing it will fail (to avoid removing
        //   backups that might still be used).
        // - the new locking mechanism is used: if the old mechanism is used, a group removal here
        //   could lead to a race condition.
        //
        // Do not error out, as we have already removed the snapshot, there is nothing a user could
        // do to rectify the situation.
        if guard.is_ok() && group.list_backups()?.is_empty() && !*OLD_LOCKING {
            group.remove_group_dir()?;
        } else if let Err(err) = guard {
            log::debug!("{err:#}");
        }

        Ok(())
    }

    /// Get the datastore.
    pub fn datastore(&self) -> &Arc<DataStore> {
        &self.store
    }

    /// Returns the backup owner.
    ///
    /// The backup owner is the entity who first created the backup group.
    pub fn get_owner(&self) -> Result<Authid, Error> {
        self.store.get_owner(&self.ns, self.as_ref())
    }

    /// Lock the snapshot and open a reader.
    pub fn locked_reader(&self) -> Result<crate::SnapshotReader, Error> {
        crate::SnapshotReader::new_do(self.clone())
    }

    /// Load the manifest without a lock. Must not be written back.
    pub fn load_manifest(&self) -> Result<(BackupManifest, u64), Error> {
        let blob = self.load_blob(MANIFEST_BLOB_NAME.as_ref())?;
        let raw_size = blob.raw_size();
        let manifest = BackupManifest::try_from(blob)?;
        Ok((manifest, raw_size))
    }

    /// Update the manifest of the specified snapshot. Never write a manifest directly,
    /// only use this method - anything else may break locking guarantees.
    pub fn update_manifest(
        &self,
        update_fn: impl FnOnce(&mut BackupManifest),
    ) -> Result<(), Error> {
        let _guard = self.lock_manifest()?;
        let (mut manifest, _) = self.load_manifest()?;

        update_fn(&mut manifest);

        let manifest = serde_json::to_value(manifest)?;
        let manifest = serde_json::to_string_pretty(&manifest)?;
        let blob = DataBlob::encode(manifest.as_bytes(), None, true)?;
        let raw_data = blob.raw_data();

        let mut path = self.full_path();
        path.push(MANIFEST_BLOB_NAME.as_ref());

        // atomic replace invalidates flock - no other writes past this point!
        replace_file(&path, raw_data, CreateOptions::new(), false)?;
        Ok(())
    }

    /// Cleans up the backup directory by removing any file not mentioned in the manifest.
    pub fn cleanup_unreferenced_files(&self, manifest: &BackupManifest) -> Result<(), Error> {
        let full_path = self.full_path();

        let mut wanted_files = std::collections::HashSet::new();
        wanted_files.insert(MANIFEST_BLOB_NAME.to_string());
        wanted_files.insert(CLIENT_LOG_BLOB_NAME.to_string());
        manifest.files().iter().for_each(|item| {
            wanted_files.insert(item.filename.clone());
        });

        for item in proxmox_sys::fs::read_subdir(libc::AT_FDCWD, &full_path)?.flatten() {
            if let Some(file_type) = item.file_type() {
                if file_type != nix::dir::Type::File {
                    continue;
                }
            }
            let file_name = item.file_name().to_bytes();
            if file_name == b"." || file_name == b".." {
                continue;
            };
            if let Ok(name) = std::str::from_utf8(file_name) {
                if wanted_files.contains(name) {
                    continue;
                }
            }
            println!("remove unused file {:?}", item.file_name());
            let dirfd = item.parent_fd();
            let _res = unsafe { libc::unlinkat(dirfd, item.file_name().as_ptr(), 0) };
        }

        Ok(())
    }

    /// Load the verify state from the manifest.
    pub fn verify_state(&self) -> Result<Option<VerifyState>, anyhow::Error> {
        Ok(self.load_manifest()?.0.verify_state()?.map(|svs| svs.state))
    }
}

impl AsRef<pbs_api_types::BackupNamespace> for BackupDir {
    fn as_ref(&self) -> &pbs_api_types::BackupNamespace {
        &self.ns
    }
}

impl AsRef<pbs_api_types::BackupDir> for BackupDir {
    fn as_ref(&self) -> &pbs_api_types::BackupDir {
        &self.dir
    }
}

impl AsRef<pbs_api_types::BackupGroup> for BackupDir {
    fn as_ref(&self) -> &pbs_api_types::BackupGroup {
        &self.dir.group
    }
}

impl From<&BackupDir> for pbs_api_types::BackupGroup {
    fn from(dir: &BackupDir) -> pbs_api_types::BackupGroup {
        dir.dir.group.clone()
    }
}

impl From<BackupDir> for pbs_api_types::BackupGroup {
    fn from(dir: BackupDir) -> pbs_api_types::BackupGroup {
        dir.dir.group
    }
}

impl From<&BackupDir> for pbs_api_types::BackupDir {
    fn from(dir: &BackupDir) -> pbs_api_types::BackupDir {
        dir.dir.clone()
    }
}

impl From<BackupDir> for pbs_api_types::BackupDir {
    fn from(dir: BackupDir) -> pbs_api_types::BackupDir {
        dir.dir
    }
}

/// Detailed Backup Information, lists files inside a BackupDir
#[derive(Clone, Debug)]
pub struct BackupInfo {
    /// the backup directory
    pub backup_dir: BackupDir,
    /// List of data files
    pub files: Vec<String>,
    /// Protection Status
    pub protected: bool,
}

impl BackupInfo {
    pub fn new(backup_dir: BackupDir) -> Result<BackupInfo, Error> {
        let path = backup_dir.full_path();

        let files = list_backup_files(libc::AT_FDCWD, &path)?;
        let protected = backup_dir.is_protected();

        Ok(BackupInfo {
            backup_dir,
            files,
            protected,
        })
    }

    pub fn sort_list(list: &mut [BackupInfo], ascendending: bool) {
        if ascendending {
            // oldest first
            list.sort_unstable_by(|a, b| a.backup_dir.dir.time.cmp(&b.backup_dir.dir.time));
        } else {
            // newest first
            list.sort_unstable_by(|a, b| b.backup_dir.dir.time.cmp(&a.backup_dir.dir.time));
        }
    }

    pub fn is_finished(&self) -> bool {
        // backup is considered unfinished if there is no manifest
        self.files
            .iter()
            .any(|name| name == MANIFEST_BLOB_NAME.as_ref())
    }
}

fn list_backup_files<P: ?Sized + nix::NixPath>(
    dirfd: RawFd,
    path: &P,
) -> Result<Vec<String>, Error> {
    let mut files = vec![];

    proxmox_sys::fs::scandir(dirfd, path, &BACKUP_FILE_REGEX, |_, filename, file_type| {
        if file_type != nix::dir::Type::File {
            return Ok(());
        }
        files.push(filename.to_owned());
        Ok(())
    })?;

    Ok(files)
}

/// Creates a path to a lock file depending on the relative path of an object (snapshot, group,
/// manifest) in a datastore. First all namespaces will be concatenated with a colon (ns-folder).
/// Then the actual file name will depend on the length of the relative path without namespaces. If
/// it is shorter than 255 characters in its unit encoded form, than the unit encoded form will be
/// used directly. If not, the file name will consist of the first 80 character, the last 80
/// characters and the hash of the unit encoded relative path without namespaces. It will also be
/// placed into a "hashed" subfolder in the namespace folder.
///
/// Examples:
///
/// - vm-100
/// - vm-100-2022\x2d05\x2d02T08\x3a11\x3a33Z
/// - ns1:ns2:ns3:ns4:ns5:ns6:ns7/vm-100-2022\x2d05\x2d02T08\x3a11\x3a33Z
///
/// A "hashed" lock file would look like this:
/// - ns1:ns2:ns3/hashed/$first_eighty...$last_eighty-$hash
fn lock_file_path_helper(ns: &BackupNamespace, path: PathBuf) -> PathBuf {
    let to_return = PathBuf::from(
        ns.components()
            .map(String::from)
            .reduce(|acc, n| format!("{acc}:{n}"))
            .unwrap_or_default(),
    );

    let path_bytes = path.as_os_str().as_bytes();

    let enc = escape_unit(path_bytes, true);

    if enc.len() < 255 {
        return to_return.join(enc);
    }

    let to_return = to_return.join("hashed");

    let first_eigthy = &enc[..80];
    let last_eighty = &enc[enc.len() - 80..];
    let hash = hex::encode(openssl::sha::sha256(path_bytes));

    to_return.join(format!("{first_eigthy}...{last_eighty}-{hash}"))
}

/// Helps implement the double stat'ing procedure. It avoids certain race conditions upon lock
/// deletion.
///
/// It also creates the base directory for lock files.
fn lock_helper<F>(
    store_name: &str,
    path: &std::path::Path,
    lock_fn: F,
) -> Result<BackupLockGuard, Error>
where
    F: Fn(&std::path::Path) -> Result<BackupLockGuard, Error>,
{
    let mut lock_dir = Path::new(DATASTORE_LOCKS_DIR).join(store_name);

    if let Some(parent) = path.parent() {
        lock_dir = lock_dir.join(parent);
    };

    std::fs::create_dir_all(&lock_dir)?;

    let lock = lock_fn(path)?;

    let inode = nix::sys::stat::fstat(lock.as_raw_fd())?.st_ino;

    if nix::sys::stat::stat(path).map_or(true, |st| inode != st.st_ino) {
        bail!("could not acquire lock, another thread modified the lock file");
    }

    Ok(lock)
}
