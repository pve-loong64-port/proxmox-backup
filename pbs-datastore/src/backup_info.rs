use std::fmt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::prelude::OsStrExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::{bail, format_err, Context, Error};
use const_format::concatcp;
use tracing::info;

use proxmox_s3_client::{S3ObjectKey, S3PathPrefix};
use proxmox_sys::fs::{lock_dir_noblock, lock_dir_noblock_shared, replace_file, CreateOptions};
use proxmox_systemd::escape_unit;

use pbs_api_types::{
    ArchiveType, Authid, BackupGroupDeleteStats, BackupNamespace, BackupType, GroupFilter,
    VerifyState, BACKUP_DATE_REGEX, CLIENT_LOG_BLOB_NAME, MANIFEST_BLOB_NAME,
};
use pbs_config::{open_backup_lockfile, BackupLockGuard};

use crate::datastore::{GROUP_NOTES_FILE_NAME, GROUP_OWNER_FILE_NAME};
use crate::manifest::{BackupManifest, MANIFEST_LOCK_NAME};
use crate::move_journal;
use crate::s3::S3_CONTENT_PREFIX;
use crate::{DataBlob, DataStore, DatastoreBackend};

pub const DATASTORE_LOCKS_DIR: &str = "/run/proxmox-backup/locks";
pub const PROTECTED_MARKER_FILENAME: &str = ".protected";

proxmox_schema::const_regex! {
    pub BACKUP_FILES_AND_PROTECTED_REGEX = concatcp!(r"^(.*\.([fd]idx|blob)|\", PROTECTED_MARKER_FILENAME, ")$");
}

// TODO: Remove with PBS 5
// Note: The `expect()` call here will only happen if we can neither confirm nor deny the existence
// of the file. this should only happen if a user messes with the `/run/proxmox-backup` directory.
// if that happens, a lot more should fail as we rely on the existence of the directory throughout
// the code. so just panic with a reasonable message.
pub(crate) static OLD_LOCKING: LazyLock<bool> = LazyLock::new(|| {
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
                let (files, protected) = list_backup_files(l2_fd, backup_time)?;

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
                    Some(l2_fd),
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

    /// Destroy the group inclusive all its backup snapshots (BackupDir's).
    ///
    /// Consumes the group lock. The caller is responsible for acquiring it via
    /// [`Self::lock`] beforehand.
    ///
    /// Returns `BackupGroupDeleteStats`, containing the number of deleted snapshots
    /// and number of protected snaphsots, which therefore were not removed.
    pub(crate) fn destroy(
        &self,
        _lock_guard: BackupLockGuard,
        backend: &DatastoreBackend,
    ) -> Result<BackupGroupDeleteStats, Error> {
        let path = self.full_group_path();

        log::info!("removing backup group {:?}", path);
        let mut delete_stats = BackupGroupDeleteStats::default();
        for snap in self.iter_snapshots()? {
            let snap = snap?;
            if snap.is_protected() {
                delete_stats.increment_protected_snapshots();
                continue;
            }
            // also for S3 cleanup local only, the actual S3 objects will be removed below,
            // reducing the number of required API calls.
            snap.destroy(false, &DatastoreBackend::Filesystem)?;
            delete_stats.increment_removed_snapshots();
        }

        if let DatastoreBackend::S3(s3_client) = backend {
            let path = self.relative_group_path();
            let group_prefix = path
                .to_str()
                .ok_or_else(|| format_err!("invalid group path prefix"))?;
            let prefix = format!("{S3_CONTENT_PREFIX}/{group_prefix}");
            let delete_objects_errors = proxmox_async::runtime::block_on(
                s3_client.delete_objects_by_prefix_with_suffix_filter(
                    &S3PathPrefix::Some(prefix),
                    PROTECTED_MARKER_FILENAME,
                    &[GROUP_OWNER_FILE_NAME, GROUP_NOTES_FILE_NAME],
                ),
            )?;
            if !delete_objects_errors.is_empty() {
                crate::s3::log_s3_delete_objects_errors(&delete_objects_errors);
                bail!("deleting objects failed");
            }
        }

        // Note: make sure the old locking mechanism isn't used as `remove_dir_all` is not safe in
        // that case
        if delete_stats.all_removed() && !*OLD_LOCKING {
            self.remove_group_dir()?;
            delete_stats.increment_removed_groups();
        }

        Ok(delete_stats)
    }

    /// Check merge invariants for moving this group's snapshots into `target`.
    /// Returns an error if ownership differs or snapshot times mismatch.
    pub(crate) fn check_merge_invariants(&self, target: &BackupGroup) -> Result<(), Error> {
        let src_owner = self.get_owner()?;
        let tgt_owner = target.get_owner()?;
        if src_owner != tgt_owner {
            bail!(
                "cannot merge group '{}/{}' from '{}' into '{}': owner mismatch \
                (source: {src_owner}, target: {tgt_owner})",
                self.group.ty,
                self.group.id,
                self.ns,
                target.ns,
            );
        }

        let (src_oldest, src_oldest_str) = self.iter_snapshots()?.filter_map(Result::ok).fold(
            (i64::MAX, String::new()),
            |(min, min_str), s| {
                let curr = s.backup_time();
                if curr < min {
                    (curr, s.backup_time_string.clone())
                } else {
                    (min, min_str)
                }
            },
        );

        if src_oldest != i64::MAX {
            // Any target snapshot with time >= src_oldest violates the
            // "source strictly newer than target" merge invariant. Short-circuit on the first hit.
            if let Some(overlap) = target
                .iter_snapshots()?
                .filter_map(Result::ok)
                .find_map(|s| {
                    if s.backup_time() >= src_oldest {
                        Some(s.backup_time_string().to_owned())
                    } else {
                        None
                    }
                })
            {
                info!("oldest source snapshot: {src_oldest_str}");
                info!("conflicting target snapshot: {overlap}");
                bail!(
                    "cannot merge group '{}/{}' from '{}' into '{}': snapshot time mismatch",
                    self.group.ty,
                    self.group.id,
                    self.ns,
                    target.ns,
                );
            }
        }

        Ok(())
    }

    /// Move the group notes file (if any) from this group to `target`. Caller must hold
    /// exclusive group locks on both.
    ///
    /// Behavior:
    /// - If the source has no notes, do nothing.
    /// - If only the source has notes, upload them to the target's S3 notes object first
    ///   (when the backend is S3), then rename the local file. A failure in either step
    ///   aborts the move so the source is left intact for the caller to retry; the S3
    ///   upload uses replace semantics so re-running is idempotent.
    /// - If both source and target have notes (merge case):
    ///   - identical contents: leave the target unchanged. The source copy will be
    ///     removed by `destroy()`.
    ///   - diverging contents: log a warning and keep the target's notes.
    pub(crate) fn move_notes_to(
        &self,
        target: &BackupGroup,
        backend: &DatastoreBackend,
    ) -> Result<(), Error> {
        let src_notes_path = self.store.group_notes_path(&self.ns, &self.group);
        let dst_notes_path = target.store.group_notes_path(&target.ns, &target.group);

        let src_notes = match std::fs::read(&src_notes_path) {
            Ok(v) => v,
            Err(ref err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                bail!("reading source group notes {src_notes_path:?} failed: {err}")
            }
        };

        match std::fs::read(&dst_notes_path) {
            Ok(dst_notes) => {
                if dst_notes != src_notes {
                    log::warn!(
                        "group notes differ during merge of '{}' from '{}' into '{}' - keeping target's notes",
                        self.group, self.ns, target.ns,
                    );
                }
                // Identical or intentionally kept: source copy will be removed by destroy().
                return Ok(());
            }
            Err(ref err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                bail!("reading target group notes {dst_notes_path:?} failed: {err}")
            }
        }

        if let DatastoreBackend::S3(s3_client) = backend {
            let dst_key = crate::s3::object_key_from_path(
                &target.relative_group_path(),
                GROUP_NOTES_FILE_NAME,
            )
            .context("invalid target notes object key")?;
            let data = hyper::body::Bytes::copy_from_slice(&src_notes);
            proxmox_async::runtime::block_on(s3_client.upload_replace_with_retry(dst_key, data))
                .with_context(|| {
                    format!(
                        "failed to upload group notes on S3 backend for '{}' in '{}'",
                        target.group, target.ns,
                    )
                })?;
        }

        std::fs::rename(&src_notes_path, &dst_notes_path).with_context(|| {
            format!("failed to move group notes {src_notes_path:?} -> {dst_notes_path:?}")
        })?;

        Ok(())
    }

    /// Helper function, assumes that no more snapshots are present in the group.
    fn remove_group_dir(&self) -> Result<(), Error> {
        let note_path = self.store.group_notes_path(&self.ns, &self.group);
        if let Err(err) = std::fs::remove_file(&note_path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                bail!("removing the note file '{note_path:?}' failed - {err}")
            }
        }

        let owner_path = self.store.owner_path(&self.ns, &self.group);

        if let Err(err) = std::fs::remove_file(&owner_path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                bail!("removing the owner file '{owner_path:?}' failed - {err}");
            }
        }

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
        path.push(PROTECTED_MARKER_FILENAME);
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

    /// Move this snapshot into `target`.
    ///
    /// For the filesystem backend, renames the snapshot directory. For S3, copies all
    /// objects under the snapshot prefix to the target, renames the local cache directory,
    /// then deletes the source objects. A copy failure returns an error with the snapshot
    /// intact at source. A delete failure is logged as a warning.
    ///
    /// Before the rename, each index file's post-rename path is recorded in the per-datastore
    /// move journal so a concurrent GC phase-1 drain can still mark their chunks - see
    /// `move_journal` for the race this closes. If the rename then fails the journal entry
    /// becomes a ghost that the drain skips on `open_index_reader` returning `None`.
    ///
    /// The caller must hold an exclusive lock on this snapshot, hold exclusive locks on
    /// both source and target groups, and ensure the target group directory exists.
    pub(crate) fn move_to(
        &self,
        target: &BackupGroup,
        backend: &DatastoreBackend,
    ) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.store, &target.store) {
            bail!("cannot move snapshot across different datastores");
        }

        let target_snap = target.backup_dir_with_rfc3339(self.backup_time_string.clone())?;
        let src_snap_path = self.full_path();
        let dst_snap_path = target_snap.full_path();

        // Enumerate the index files at source (they are still there until the
        // rename below) and record their future paths in the move journal so a
        // concurrent GC phase-1 drain can mark their chunks even if the
        // hierarchy iteration missed both the source and target.
        let mut journal_entries: Vec<PathBuf> = Vec::new();
        for entry in std::fs::read_dir(&src_snap_path)
            .with_context(|| format!("failed to list source snapshot dir {src_snap_path:?}"))?
        {
            let entry =
                entry.with_context(|| format!("failed to read entry in {src_snap_path:?}"))?;
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            if matches!(
                ArchiveType::from_path(name_str),
                Ok(ArchiveType::FixedIndex) | Ok(ArchiveType::DynamicIndex)
            ) {
                journal_entries.push(dst_snap_path.join(&name));
            }
        }
        move_journal::append_moved_indices(self.store.name(), &journal_entries)?;

        match backend {
            DatastoreBackend::Filesystem => {
                std::fs::rename(&src_snap_path, &dst_snap_path).with_context(|| {
                    format!("failed to move snapshot {src_snap_path:?} to {dst_snap_path:?}")
                })?;
            }
            DatastoreBackend::S3(s3_client) => {
                let src_rel = self.relative_path();
                let src_rel_str = src_rel
                    .to_str()
                    .ok_or_else(|| format_err!("invalid source snapshot path"))?;
                let src_prefix_str = format!("{S3_CONTENT_PREFIX}/{src_rel_str}/");

                let dst_rel = target_snap.relative_path();
                let dst_rel_str = dst_rel
                    .to_str()
                    .ok_or_else(|| format_err!("invalid target snapshot path"))?;
                let dst_prefix_str = format!("{S3_CONTENT_PREFIX}/{dst_rel_str}/");

                let store_prefix = format!("{}/", self.store.name());

                // Copy all objects for this snapshot to the target prefix. On failure the
                // source snapshot remains intact and any partial target copies stay as
                // leftovers (visible via the API for cleanup).
                let prefix = S3PathPrefix::Some(src_prefix_str.clone());
                let mut token: Option<String> = None;
                let mut src_keys = Vec::new();

                loop {
                    let result = proxmox_async::runtime::block_on(
                        s3_client.list_objects_v2(&prefix, token.as_deref()),
                    )
                    .context("failed to list snapshot objects on S3 backend")?;

                    for item in result.contents {
                        let full_key_str: &str = &item.key;
                        let rel_key =
                            full_key_str.strip_prefix(&store_prefix).ok_or_else(|| {
                                format_err!("unexpected key prefix in '{full_key_str}'")
                            })?;
                        let src_key = S3ObjectKey::try_from(rel_key)?;

                        let suffix = rel_key
                            .strip_prefix(&src_prefix_str)
                            .ok_or_else(|| format_err!("unexpected key format '{rel_key}'"))?;
                        let dst_key_str = format!("{dst_prefix_str}{suffix}");
                        let dst_key = S3ObjectKey::try_from(dst_key_str.as_str())?;

                        proxmox_async::runtime::block_on(
                            s3_client.copy_object(src_key.clone(), dst_key),
                        )
                        .with_context(|| format!("failed to copy S3 object '{rel_key}'"))?;
                        src_keys.push(src_key);
                    }

                    if result.is_truncated {
                        token = result.next_continuation_token;
                    } else {
                        break;
                    }
                }

                std::fs::rename(&src_snap_path, &dst_snap_path).with_context(|| {
                    format!("failed to move snapshot cache {src_snap_path:?} to {dst_snap_path:?}")
                })?;

                // Delete source S3 objects. Treat failures as warnings since the snapshot
                // is already at the target.
                for src_key in src_keys {
                    if let Err(err) =
                        proxmox_async::runtime::block_on(s3_client.delete_object(src_key.clone()))
                    {
                        log::warn!(
                            "S3 move: failed to delete source object '{src_key:?}' \
                            (snapshot already at target, orphaned object requires manual removal): \
                            {err:#}"
                        );
                    }
                }
            }
        }

        // Clean up stale source lock files under /run for this snapshot.
        let _ = std::fs::remove_file(self.manifest_lock_path());
        let _ = std::fs::remove_file(self.lock_path());

        Ok(())
    }

    /// Destroy the whole snapshot, bails if it's protected
    ///
    /// Setting `force` to true skips locking and thus ignores if the backup is currently in use.
    pub(crate) fn destroy(&self, force: bool, backend: &DatastoreBackend) -> Result<(), Error> {
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

        if let DatastoreBackend::S3(s3_client) = backend {
            let path = self.relative_path();
            let snapshot_prefix = path
                .to_str()
                .ok_or_else(|| format_err!("invalid snapshot path"))?;
            let prefix = format!("{S3_CONTENT_PREFIX}/{snapshot_prefix}");
            let delete_objects_error = proxmox_async::runtime::block_on(
                s3_client.delete_objects_by_prefix(&S3PathPrefix::Some(prefix)),
            )?;
            if !delete_objects_error.is_empty() {
                crate::s3::log_s3_delete_objects_errors(&delete_objects_error);
                bail!("deleting objects failed");
            }
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
            if let DatastoreBackend::S3(s3_client) = backend {
                let object_key =
                    super::s3::object_key_from_path(&group.relative_group_path(), "owner")
                        .context("invalid owner file object key")?;
                proxmox_async::runtime::block_on(s3_client.delete_object(object_key))?;
            }
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
        backend: &DatastoreBackend,
        update_fn: impl FnOnce(&mut BackupManifest),
    ) -> Result<(), Error> {
        let _guard = self.lock_manifest()?;
        let (mut manifest, _) = self.load_manifest()?;

        update_fn(&mut manifest);

        let manifest = serde_json::to_value(manifest)?;
        let manifest = serde_json::to_string_pretty(&manifest)?;
        let blob = DataBlob::encode(manifest.as_bytes(), None, true)?;
        let raw_data = blob.raw_data();

        if let DatastoreBackend::S3(s3_client) = backend {
            let object_key =
                super::s3::object_key_from_path(&self.relative_path(), MANIFEST_BLOB_NAME.as_ref())
                    .context("invalid manifest object key")?;
            let data = hyper::body::Bytes::copy_from_slice(raw_data);
            proxmox_async::runtime::block_on(s3_client.upload_replace_with_retry(object_key, data))
                .context("failed to update manifest on s3 backend")?;
        }

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

        let (files, protected) = list_backup_files(libc::AT_FDCWD, &path)?;

        Ok(BackupInfo {
            backup_dir,
            files,
            protected,
        })
    }

    pub fn sort_list(list: &mut [BackupInfo], ascending: bool) {
        if ascending {
            // oldest first
            list.sort_unstable_by_key(|a| a.backup_dir.dir.time);
        } else {
            // newest first
            list.sort_unstable_by_key(|b| std::cmp::Reverse(b.backup_dir.dir.time));
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
) -> Result<(Vec<String>, bool), Error> {
    let mut files = vec![];
    let mut protected = false;

    proxmox_sys::fs::scandir(
        dirfd,
        path,
        &BACKUP_FILES_AND_PROTECTED_REGEX,
        |_, filename, file_type| {
            if file_type != nix::dir::Type::File {
                return Ok(());
            }
            // avoids more expensive check via `BackupDir::is_protected`
            if filename == ".protected" {
                protected = true;
            } else {
                files.push(filename.to_owned());
            }
            Ok(())
        },
    )?;

    Ok((files, protected))
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
pub(crate) fn lock_helper<F>(
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
