use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString, OsStr};
use std::fmt::Display;
use std::io::{self, Read};
use std::mem::size_of;
use std::ops::Range;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};

use anyhow::{bail, Context, Error};
use futures::future::BoxFuture;
use futures::FutureExt;
use nix::dir::Dir;
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::sys::stat::{FileStat, Mode};
use serde::{Deserialize, Serialize};

use pathpatterns::{MatchEntry, MatchFlag, MatchList, MatchType, PatternFlag};
use proxmox_sys::error::SysError;
use pxar::accessor::aio::{Accessor, Directory};
use pxar::accessor::ReadAt;
use pxar::encoder::{LinkOffset, PayloadOffset, SeqWrite};
use pxar::{EntryKind, Metadata, PxarVariant};

use proxmox_human_byte::HumanByte;
use proxmox_io::vec;
use proxmox_sys::fs::{self, acl, xattr};

use pbs_datastore::catalog::BackupCatalogWriter;
use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_datastore::index::IndexFile;

use crate::inject_reused_chunks::InjectChunks;
use crate::pxar::look_ahead_cache::{CacheEntry, CacheEntryData, PxarLookaheadCache};
use crate::pxar::metadata::errno_is_unsupported;
use crate::pxar::tools::assert_single_path_component;
use crate::pxar::Flags;

const CHUNK_PADDING_THRESHOLD: f64 = 0.1;

/// Pxar options for creating a pxar archive/stream
#[derive(Default)]
pub struct PxarCreateOptions {
    /// Device/mountpoint st_dev numbers that should be included. None for no limitation.
    pub device_set: Option<HashSet<u64>>,
    /// Exclusion patterns
    pub patterns: Vec<MatchEntry>,
    /// Maximum number of entries to hold in memory
    pub entries_max: usize,
    /// Skip lost+found directory
    pub skip_lost_and_found: bool,
    /// Skip xattrs of files that return E2BIG error
    pub skip_e2big_xattr: bool,
    /// Reference state for partial backups
    pub previous_ref: Option<PxarPrevRef>,
    /// Maximum number of lookahead cache entries
    pub max_cache_size: Option<usize>,
}

pub type MetadataArchiveReader = Arc<dyn ReadAt + Send + Sync + 'static>;

/// Stateful information of previous backups snapshots for partial backups
pub struct PxarPrevRef {
    /// Reference accessor for metadata comparison
    pub accessor: Accessor<MetadataArchiveReader>,
    /// Reference index for reusing payload chunks
    pub payload_index: DynamicIndexReader,
    /// Reference archive name for partial backups
    pub archive_name: String,
}

fn detect_fs_type(fd: RawFd) -> Result<i64, Error> {
    let mut fs_stat = std::mem::MaybeUninit::uninit();
    let res = unsafe { libc::fstatfs(fd, fs_stat.as_mut_ptr()) };
    Errno::result(res)?;
    let fs_stat = unsafe { fs_stat.assume_init() };

    Ok(fs_stat.f_type)
}

fn strip_ascii_whitespace(line: &[u8]) -> &[u8] {
    let line = match line.iter().position(|&b| !b.is_ascii_whitespace()) {
        Some(n) => &line[n..],
        None => return &[],
    };
    match line.iter().rev().position(|&b| !b.is_ascii_whitespace()) {
        Some(n) => &line[..(line.len() - n)],
        None => &[],
    }
}

#[rustfmt::skip]
pub fn is_virtual_file_system(magic: i64) -> bool {
    use proxmox_sys::linux::magic::*;

    matches!(magic, BINFMTFS_MAGIC |
        CGROUP2_SUPER_MAGIC |
        CGROUP_SUPER_MAGIC |
        CONFIGFS_MAGIC |
        DEBUGFS_MAGIC |
        DEVPTS_SUPER_MAGIC |
        EFIVARFS_MAGIC |
        FUSE_CTL_SUPER_MAGIC |
        HUGETLBFS_MAGIC |
        MQUEUE_MAGIC |
        NFSD_MAGIC |
        PROC_SUPER_MAGIC |
        PSTOREFS_MAGIC |
        RPCAUTH_GSSMAGIC |
        SECURITYFS_MAGIC |
        SELINUX_MAGIC |
        SMACK_MAGIC |
        SYSFS_MAGIC)
}

trait UniqueContext<T> {
    fn unique_context<S>(self, context: S) -> Result<T, anyhow::Error>
    where
        S: Display + Send + Sync + 'static;
}

impl<T> UniqueContext<T> for Result<T, anyhow::Error> {
    fn unique_context<S>(self, context: S) -> Result<T, anyhow::Error>
    where
        S: Display + Send + Sync + 'static,
    {
        match self {
            Ok(ok) => Ok(ok),
            Err(err) => {
                let last_error = err.chain().next();
                if let Some(e) = last_error {
                    if e.to_string() == context.to_string() {
                        return Err(err);
                    }
                }
                Err(err.context(context))
            }
        }
    }
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct HardLinkInfo {
    st_dev: u64,
    st_ino: u64,
}

#[derive(Default)]
struct ReuseStats {
    files_reused_count: u64,
    files_hardlink_count: u64,
    files_reencoded_count: u64,
    total_injected_count: u64,
    partial_chunks_count: u64,
    total_injected_size: u64,
    total_reused_payload_size: u64,
    total_reencoded_size: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct PbsClientPrelude {
    #[serde(skip_serializing_if = "Option::is_none")]
    exclude_patterns: Option<String>,
}

struct Archiver {
    feature_flags: Flags,
    fs_feature_flags: Flags,
    fs_magic: i64,
    patterns: Vec<MatchEntry>,
    #[allow(clippy::type_complexity)]
    callback: Box<dyn FnMut(&Path) -> Result<(), Error> + Send>,
    catalog: Option<Arc<Mutex<dyn BackupCatalogWriter + Send>>>,
    path: PathBuf,
    entry_counter: usize,
    entry_limit: usize,
    current_st_dev: libc::dev_t,
    device_set: Option<HashSet<u64>>,
    hardlinks: HashMap<HardLinkInfo, (PathBuf, LinkOffset)>,
    file_copy_buffer: Vec<u8>,
    skip_e2big_xattr: bool,
    forced_boundaries: Option<mpsc::Sender<InjectChunks>>,
    suggested_boundaries: Option<mpsc::Sender<u64>>,
    previous_payload_index: Option<DynamicIndexReader>,
    cache: PxarLookaheadCache,
    reuse_stats: ReuseStats,
    split_archive: bool,
}

type Encoder<'a, T> = pxar::encoder::aio::Encoder<'a, T>;

pub struct PxarWriters<T> {
    archive: PxarVariant<T, T>,
    catalog: Option<Arc<Mutex<dyn BackupCatalogWriter + Send>>>,
}

impl<T> PxarWriters<T> {
    pub fn new(
        archive: PxarVariant<T, T>,
        catalog: Option<Arc<Mutex<dyn BackupCatalogWriter + Send>>>,
    ) -> Self {
        Self { archive, catalog }
    }
}

pub async fn create_archive<T, F>(
    source_dir: Dir,
    writers: PxarWriters<T>,
    feature_flags: Flags,
    callback: F,
    options: PxarCreateOptions,
    forced_boundaries: Option<mpsc::Sender<InjectChunks>>,
    suggested_boundaries: Option<mpsc::Sender<u64>>,
) -> Result<(), Error>
where
    T: SeqWrite + Send,
    F: FnMut(&Path) -> Result<(), Error> + Send + 'static,
{
    let fs_magic = detect_fs_type(source_dir.as_raw_fd())?;
    if is_virtual_file_system(fs_magic) {
        bail!("refusing to backup a virtual file system");
    }

    let mut fs_feature_flags = Flags::from_magic(fs_magic);

    let stat = nix::sys::stat::fstat(source_dir.as_raw_fd())?;
    let metadata = get_metadata(
        source_dir.as_raw_fd(),
        &stat,
        feature_flags & fs_feature_flags,
        fs_magic,
        &mut fs_feature_flags,
        options.skip_e2big_xattr,
    )
    .context("failed to get metadata for source directory")?;

    let mut device_set = options.device_set.clone();
    if let Some(ref mut set) = device_set {
        set.insert(stat.st_dev);
    }

    let mut patterns = options.patterns;

    if options.skip_lost_and_found {
        patterns.push(MatchEntry::parse_pattern(
            "lost+found",
            PatternFlag::PATH_NAME,
            MatchType::Exclude,
        )?);
    }

    let split_archive = writers.archive.payload().is_some();
    let prelude = if split_archive && !patterns.is_empty() {
        let prelude = PbsClientPrelude {
            exclude_patterns: Some(String::from_utf8(generate_pxar_excludes_cli(
                &patterns[..],
            ))?),
        };
        Some(serde_json::to_vec(&prelude)?)
    } else {
        None
    };

    let metadata_mode = options.previous_ref.is_some() && split_archive;
    let (previous_payload_index, previous_metadata_accessor) =
        if let Some(refs) = options.previous_ref {
            (
                Some(refs.payload_index),
                refs.accessor.open_root().await.ok(),
            )
        } else {
            (None, None)
        };

    let mut encoder = Encoder::new(writers.archive, &metadata, prelude.as_deref()).await?;

    let mut archiver = Archiver {
        feature_flags,
        fs_feature_flags,
        fs_magic,
        callback: Box::new(callback),
        patterns,
        catalog: writers.catalog,
        path: PathBuf::new(),
        entry_counter: 0,
        entry_limit: options.entries_max,
        current_st_dev: stat.st_dev,
        device_set,
        hardlinks: HashMap::new(),
        file_copy_buffer: vec::undefined(4 * 1024 * 1024),
        skip_e2big_xattr: options.skip_e2big_xattr,
        forced_boundaries,
        suggested_boundaries,
        previous_payload_index,
        cache: PxarLookaheadCache::new(options.max_cache_size),
        reuse_stats: ReuseStats::default(),
        split_archive,
    };

    archiver
        .archive_dir_contents(&mut encoder, previous_metadata_accessor, source_dir, true)
        .await?;

    if metadata_mode {
        archiver
            .flush_cached_reusing_if_below_threshold(&mut encoder, false)
            .await?;
    }

    encoder.finish().await?;
    encoder.close().await?;

    if metadata_mode {
        log::info!("Change detection summary:");
        log::info!(
            " - {} total files ({} hardlinks)",
            archiver.reuse_stats.files_reused_count
                + archiver.reuse_stats.files_reencoded_count
                + archiver.reuse_stats.files_hardlink_count,
            archiver.reuse_stats.files_hardlink_count,
        );
        log::info!(
            " - {} unchanged, reusable files with {} data",
            archiver.reuse_stats.files_reused_count,
            HumanByte::from(archiver.reuse_stats.total_reused_payload_size),
        );
        log::info!(
            " - {} changed or non-reusable files with {} data",
            archiver.reuse_stats.files_reencoded_count,
            HumanByte::from(archiver.reuse_stats.total_reencoded_size),
        );
        log::info!(
            " - {} padding in {} partially reused chunks",
            HumanByte::from(
                archiver.reuse_stats.total_injected_size
                    - archiver.reuse_stats.total_reused_payload_size
            ),
            archiver.reuse_stats.partial_chunks_count,
        );
    }
    Ok(())
}

struct FileListEntry {
    name: CString,
    path: PathBuf,
    stat: FileStat,
}

impl Archiver {
    /// Get the currently effective feature flags. (Requested flags masked by the file system
    /// feature flags).
    fn flags(&self) -> Flags {
        self.feature_flags & self.fs_feature_flags
    }

    fn archive_dir_contents<'a, T: SeqWrite + Send>(
        &'a mut self,
        encoder: &'a mut Encoder<'_, T>,
        mut previous_metadata_accessor: Option<Directory<MetadataArchiveReader>>,
        mut dir: Dir,
        is_root: bool,
    ) -> BoxFuture<'a, Result<(), Error>> {
        async move {
            let entry_counter = self.entry_counter;

            let old_patterns_count = self.patterns.len();
            self.read_pxar_excludes(dir.as_raw_fd())?;

            let mut file_list = self.generate_directory_file_list(&mut dir, is_root)?;

            if is_root && old_patterns_count > 0 && previous_metadata_accessor.is_none() {
                file_list.push(FileListEntry {
                    name: CString::new(".pxarexclude-cli").unwrap(),
                    path: PathBuf::new(),
                    stat: unsafe { std::mem::zeroed() },
                });
            }

            let dir_fd = dir.as_raw_fd();

            let old_path = std::mem::take(&mut self.path);

            for file_entry in file_list {
                let file_name = file_entry.name.to_bytes();

                if is_root && file_name == b".pxarexclude-cli" {
                    if !self.split_archive {
                        self.encode_pxarexclude_cli(encoder, &file_entry.name, old_patterns_count)
                            .await?;
                    }
                    continue;
                }

                (self.callback)(&file_entry.path)?;
                self.path = file_entry.path;
                self.add_entry(
                    encoder,
                    &mut previous_metadata_accessor,
                    dir_fd,
                    &file_entry.name,
                    &file_entry.stat,
                )
                .await
                .unique_context(format!("error at {:?}", self.path))?;
            }
            self.path = old_path;
            self.entry_counter = entry_counter;
            self.patterns.truncate(old_patterns_count);

            Ok(())
        }
        .boxed()
    }

    async fn is_reusable_entry(
        &mut self,
        previous_metadata_accessor: &Option<Directory<MetadataArchiveReader>>,
        file_name: &Path,
        metadata: &Metadata,
    ) -> Result<Option<Range<u64>>, Error> {
        if let Some(previous_metadata_accessor) = previous_metadata_accessor {
            if let Some(file_entry) = previous_metadata_accessor.lookup(file_name).await? {
                if metadata == file_entry.metadata() {
                    if let EntryKind::File {
                        payload_offset: Some(offset),
                        size,
                        ..
                    } = file_entry.entry().kind()
                    {
                        let range =
                            *offset..*offset + size + size_of::<pxar::format::Header>() as u64;
                        log::debug!(
                            "reusable: {file_name:?} at range {range:?} has unchanged metadata."
                        );
                        return Ok(Some(range));
                    }
                    log::debug!("reencode: {file_name:?} not a regular file.");
                    return Ok(None);
                }
                log::debug!("reencode: {file_name:?} metadata did not match.");
                return Ok(None);
            }
            log::debug!("reencode: {file_name:?} not found in previous archive.");
        }

        Ok(None)
    }

    /// openat() wrapper which allows but logs `EACCES` and turns `ENOENT` into `None`.
    ///
    /// The `existed` flag is set when iterating through a directory to note that we know the file
    /// is supposed to exist and we should warn if it doesnt'.
    fn open_file(
        &mut self,
        parent: RawFd,
        file_name: &CStr,
        oflags: OFlag,
        existed: bool,
    ) -> Result<Option<OwnedFd>, Error> {
        // common flags we always want to use:
        let oflags = oflags | OFlag::O_CLOEXEC | OFlag::O_NOCTTY;

        let mut noatime = OFlag::O_NOATIME;
        loop {
            return match proxmox_sys::fd::openat(
                &parent,
                file_name,
                oflags | noatime,
                Mode::empty(),
            ) {
                Ok(fd) => Ok(Some(fd)),
                Err(Errno::ENOENT) => {
                    if existed {
                        self.report_vanished_file();
                    }
                    Ok(None)
                }
                Err(Errno::EACCES) => {
                    log::warn!("failed to open file: {:?}: access denied", file_name);
                    Ok(None)
                }
                Err(Errno::EPERM) if !noatime.is_empty() => {
                    // Retry without O_NOATIME:
                    noatime = OFlag::empty();
                    continue;
                }
                Err(other) => Err(Error::from(other)),
            };
        }
    }

    fn read_pxar_excludes(&mut self, parent: RawFd) -> Result<(), Error> {
        let fd = match self.open_file(parent, c".pxarexclude", OFlag::O_RDONLY, false)? {
            Some(fd) => fd,
            None => return Ok(()),
        };

        let old_pattern_count = self.patterns.len();

        let path_bytes = self.path.as_os_str().as_bytes();

        let file = unsafe { std::fs::File::from_raw_fd(fd.into_raw_fd()) };

        use io::BufRead;
        for line in io::BufReader::new(file).split(b'\n') {
            let line = match line {
                Ok(line) => line,
                Err(err) => {
                    log::warn!(
                        "ignoring .pxarexclude after read error in {:?}: {}",
                        self.path,
                        err,
                    );
                    self.patterns.truncate(old_pattern_count);
                    return Ok(());
                }
            };

            let line = strip_ascii_whitespace(&line);

            if line.is_empty() || line[0] == b'#' {
                continue;
            }

            let mut buf;
            let (line, mode, anchored) = if line[0] == b'/' {
                buf = Vec::with_capacity(path_bytes.len() + 1 + line.len());
                buf.extend(path_bytes);
                buf.extend(line);
                (&buf[..], MatchType::Exclude, true)
            } else if line.starts_with(b"!/") {
                // inverted case with absolute path
                buf = Vec::with_capacity(path_bytes.len() + line.len());
                buf.extend(path_bytes);
                buf.extend(&line[1..]); // without the '!'
                (&buf[..], MatchType::Include, true)
            } else if line.starts_with(b"!") {
                (&line[1..], MatchType::Include, false)
            } else {
                (line, MatchType::Exclude, false)
            };

            match MatchEntry::parse_pattern(line, PatternFlag::PATH_NAME, mode) {
                Ok(pattern) => {
                    if anchored {
                        self.patterns.push(pattern.add_flags(MatchFlag::ANCHORED));
                    } else {
                        self.patterns.push(pattern);
                    }
                }
                Err(err) => {
                    log::error!("bad pattern in {:?}: {}", self.path, err);
                }
            }
        }

        Ok(())
    }

    async fn encode_pxarexclude_cli<T: SeqWrite + Send>(
        &mut self,
        encoder: &mut Encoder<'_, T>,
        file_name: &CStr,
        patterns_count: usize,
    ) -> Result<(), Error> {
        let content = generate_pxar_excludes_cli(&self.patterns[..patterns_count]);
        if let Some(ref catalog) = self.catalog {
            catalog
                .lock()
                .unwrap()
                .add_file(file_name, content.len() as u64, 0)?;
        }

        let mut metadata = Metadata::default();
        metadata.stat.mode = pxar::format::mode::IFREG | 0o600;
        // use uid/gid of client process so the backup snapshot might be restored by the same
        // potentially non-root user
        metadata.stat.uid = unsafe { libc::getuid() };
        metadata.stat.gid = unsafe { libc::getgid() };

        let mut file = encoder
            .create_file(&metadata, ".pxarexclude-cli", content.len() as u64)
            .await?;
        file.write_all(&content).await?;

        Ok(())
    }

    fn generate_directory_file_list(
        &mut self,
        dir: &mut Dir,
        is_root: bool,
    ) -> Result<Vec<FileListEntry>, Error> {
        let dir_fd = dir.as_raw_fd();

        let mut file_list = Vec::new();

        for file in dir.iter() {
            let file = file?;

            let file_name = file.file_name();
            let file_name_bytes = file_name.to_bytes();
            if file_name_bytes == b"." || file_name_bytes == b".." {
                continue;
            }

            if is_root && file_name_bytes == b".pxarexclude-cli" {
                continue;
            }

            let os_file_name = OsStr::from_bytes(file_name_bytes);
            assert_single_path_component(os_file_name)?;
            let full_path = self.path.join(os_file_name);

            let match_path = PathBuf::from("/").join(full_path.clone());

            let mut stat_results: Option<FileStat> = None;

            let get_file_mode = || {
                nix::sys::stat::fstatat(dir_fd, file_name, nix::fcntl::AtFlags::AT_SYMLINK_NOFOLLOW)
            };

            let match_result = self
                .patterns
                .matches(match_path.as_os_str().as_bytes(), || {
                    Ok::<_, Errno>(match &stat_results {
                        Some(result) => result.st_mode,
                        None => stat_results.insert(get_file_mode()?).st_mode,
                    })
                });

            match match_result {
                Ok(Some(MatchType::Exclude)) => {
                    log::debug!("matched by exclude pattern '{full_path:?}'");
                    continue;
                }
                Ok(_) => (),
                Err(err) if err.not_found() => continue,
                Err(err) => {
                    return Err(err).with_context(|| format!("stat failed on {full_path:?}"))
                }
            }

            let stat = stat_results
                .map(Ok)
                .unwrap_or_else(get_file_mode)
                .with_context(|| format!("stat failed on {full_path:?}"))?;

            self.entry_counter += 1;
            if self.entry_counter > self.entry_limit {
                bail!(
                    "exceeded allowed number of file entries (> {})",
                    self.entry_limit
                );
            }

            file_list.push(FileListEntry {
                name: file_name.to_owned(),
                path: full_path,
                stat,
            });
        }

        file_list.sort_unstable_by(|a, b| a.name.cmp(&b.name));

        Ok(file_list)
    }

    fn report_vanished_file(&self) {
        log::warn!("warning: file vanished while reading: {:?}", self.path);
    }

    fn report_file_shrunk_while_reading(&self) {
        log::warn!(
            "warning: file size shrunk while reading: {:?}, file will be padded with zeros!",
            self.path,
        );
    }

    fn report_file_grew_while_reading(&self) {
        log::warn!(
            "warning: file size increased while reading: {:?}, file will be truncated!",
            self.path,
        );
    }

    async fn add_entry<T: SeqWrite + Send>(
        &mut self,
        encoder: &mut Encoder<'_, T>,
        previous_metadata: &mut Option<Directory<MetadataArchiveReader>>,
        parent: RawFd,
        c_file_name: &CStr,
        stat: &FileStat,
    ) -> Result<(), Error> {
        let file_mode = stat.st_mode & libc::S_IFMT;
        let open_mode = if file_mode == libc::S_IFREG || file_mode == libc::S_IFDIR {
            OFlag::empty()
        } else {
            OFlag::O_PATH
        };

        let fd = self.open_file(
            parent,
            c_file_name,
            open_mode | OFlag::O_RDONLY | OFlag::O_NOFOLLOW,
            true,
        )?;

        let fd = match fd {
            Some(fd) => fd,
            None => return Ok(()),
        };

        let metadata = get_metadata(
            fd.as_raw_fd(),
            stat,
            self.flags(),
            self.fs_magic,
            &mut self.fs_feature_flags,
            self.skip_e2big_xattr,
        )?;

        if self.previous_payload_index.is_none() {
            return self
                .add_entry_to_archive(encoder, &mut None, c_file_name, stat, fd, &metadata, None)
                .await;
        }

        // Avoid having to many open file handles in cached entries
        if self.cache.is_full() {
            log::debug!("Max cache size reached, reuse cached entries");
            self.flush_cached_reusing_if_below_threshold(encoder, true)
                .await?;
        }

        if metadata.is_regular_file() {
            if stat.st_nlink > 1 {
                let link_info = HardLinkInfo {
                    st_dev: stat.st_dev,
                    st_ino: stat.st_ino,
                };
                if self.cache.contains_hardlink(&link_info) {
                    // This hardlink has been seen by the lookahead cache already, put it on the cache
                    // with a dummy offset and continue without lookup and chunk injection.
                    // On flushing or re-encoding, the logic there will store the actual hardlink with
                    // offset.
                    self.cache.insert(
                        fd,
                        c_file_name.into(),
                        *stat,
                        metadata.clone(),
                        PayloadOffset::default(),
                        self.path.clone(),
                    );
                    return Ok(());
                } else {
                    // mark this hardlink as seen by the lookahead cache
                    self.cache.insert_hardlink(link_info);
                }
            }

            let file_name: &Path = OsStr::from_bytes(c_file_name.to_bytes()).as_ref();
            if let Some(payload_range) = self
                .is_reusable_entry(previous_metadata, file_name, &metadata)
                .await?
            {
                if !self.cache.try_extend_range(payload_range.clone()) {
                    log::debug!("Cache range has hole, new range: {payload_range:?}");
                    self.flush_cached_reusing_if_below_threshold(encoder, true)
                        .await?;
                    // range has to be set after flushing of cached entries, which resets the range
                    self.cache.update_range(payload_range.clone());
                }

                // offset relative to start of current range, does not include possible padding of
                // actual chunks, which needs to be added before encoding the payload reference
                let offset =
                    PayloadOffset::default().add(payload_range.start - self.cache.range().start);
                log::debug!("Offset relative to range start: {offset:?}");

                self.cache.insert(
                    fd,
                    c_file_name.into(),
                    *stat,
                    metadata.clone(),
                    offset,
                    self.path.clone(),
                );
                return Ok(());
            } else {
                self.flush_cached_reusing_if_below_threshold(encoder, false)
                    .await?;
            }
        } else if self.cache.caching_enabled() {
            self.cache.insert(
                fd.try_clone()?,
                c_file_name.into(),
                *stat,
                metadata.clone(),
                PayloadOffset::default(),
                self.path.clone(),
            );

            if metadata.is_dir() {
                self.add_directory(
                    encoder,
                    previous_metadata,
                    Dir::from_fd(fd.into_raw_fd())?,
                    c_file_name,
                    &metadata,
                    stat,
                )
                .await?;
            }
            return Ok(());
        }

        self.encode_entries_to_archive(encoder, None).await?;
        self.add_entry_to_archive(
            encoder,
            previous_metadata,
            c_file_name,
            stat,
            fd,
            &metadata,
            None,
        )
        .await
    }

    async fn add_entry_to_archive<T: SeqWrite + Send>(
        &mut self,
        encoder: &mut Encoder<'_, T>,
        previous_metadata: &mut Option<Directory<MetadataArchiveReader>>,
        c_file_name: &CStr,
        stat: &FileStat,
        fd: OwnedFd,
        metadata: &Metadata,
        payload_offset: Option<PayloadOffset>,
    ) -> Result<(), Error> {
        use pxar::format::mode;

        let file_name: &Path = OsStr::from_bytes(c_file_name.to_bytes()).as_ref();
        match metadata.file_type() {
            mode::IFREG => {
                let link_info = HardLinkInfo {
                    st_dev: stat.st_dev,
                    st_ino: stat.st_ino,
                };

                if stat.st_nlink > 1 {
                    if let Some((path, offset)) = self.hardlinks.get(&link_info) {
                        if let Some(ref catalog) = self.catalog {
                            catalog.lock().unwrap().add_hardlink(c_file_name)?;
                        }

                        encoder.add_hardlink(file_name, path, *offset).await?;

                        return Ok(());
                    }
                }

                let file_size = stat.st_size as u64;
                if let Some(ref catalog) = self.catalog {
                    catalog
                        .lock()
                        .unwrap()
                        .add_file(c_file_name, file_size, stat.st_mtime)?;
                }

                if let Some(sender) = self.suggested_boundaries.as_mut() {
                    let offset = encoder.payload_position()?.raw();
                    sender.send(offset)?;
                }

                let offset: LinkOffset = if let Some(payload_offset) = payload_offset {
                    self.reuse_stats.total_reused_payload_size +=
                        file_size + size_of::<pxar::format::Header>() as u64;
                    self.reuse_stats.files_reused_count += 1;

                    encoder
                        .add_payload_ref(metadata, file_name, file_size, payload_offset)
                        .await?
                } else {
                    self.reuse_stats.total_reencoded_size +=
                        file_size + size_of::<pxar::format::Header>() as u64;
                    self.reuse_stats.files_reencoded_count += 1;

                    self.add_regular_file(encoder, fd, file_name, metadata, file_size)
                        .await?
                };

                if stat.st_nlink > 1 {
                    self.reuse_stats.files_hardlink_count += 1;
                    self.hardlinks
                        .insert(link_info, (self.path.clone(), offset));
                }

                Ok(())
            }
            mode::IFDIR => {
                let dir = Dir::from_fd(fd.into_raw_fd())?;
                self.add_directory(encoder, previous_metadata, dir, c_file_name, metadata, stat)
                    .await
            }
            mode::IFSOCK => {
                if let Some(ref catalog) = self.catalog {
                    catalog.lock().unwrap().add_socket(c_file_name)?;
                }

                Ok(encoder.add_socket(metadata, file_name).await?)
            }
            mode::IFIFO => {
                if let Some(ref catalog) = self.catalog {
                    catalog.lock().unwrap().add_fifo(c_file_name)?;
                }

                Ok(encoder.add_fifo(metadata, file_name).await?)
            }
            mode::IFLNK => {
                if let Some(ref catalog) = self.catalog {
                    catalog.lock().unwrap().add_symlink(c_file_name)?;
                }

                self.add_symlink(encoder, fd, file_name, metadata).await
            }
            mode::IFBLK => {
                if let Some(ref catalog) = self.catalog {
                    catalog.lock().unwrap().add_block_device(c_file_name)?;
                }

                self.add_device(encoder, file_name, metadata, stat).await
            }
            mode::IFCHR => {
                if let Some(ref catalog) = self.catalog {
                    catalog.lock().unwrap().add_char_device(c_file_name)?;
                }

                self.add_device(encoder, file_name, metadata, stat).await
            }
            other => bail!(
                "encountered unknown file type: 0x{:x} (0o{:o})",
                other,
                other
            ),
        }
    }

    async fn flush_cached_reusing_if_below_threshold<T: SeqWrite + Send>(
        &mut self,
        encoder: &mut Encoder<'_, T>,
        keep_last_chunk: bool,
    ) -> Result<(), Error> {
        if self.cache.range().is_empty() {
            // only non regular file entries (e.g. directories) in cache, allows to do regular encoding
            self.encode_entries_to_archive(encoder, None).await?;
            return Ok(());
        }

        if let Some(ref ref_payload_index) = self.previous_payload_index {
            // Take ownership of previous last chunk, only update where it must be injected
            let prev_last_chunk = self.cache.take_last_chunk();
            let range = self.cache.range();
            let (mut indices, start_padding, end_padding) =
                lookup_dynamic_entries(ref_payload_index, range)?;
            let mut padding = start_padding + end_padding;
            let total_size = (range.end - range.start) + padding;

            // take into account used bytes of kept back chunk for padding
            if let (Some(first), Some(last)) = (indices.first(), prev_last_chunk.as_ref()) {
                if last.digest() == first.digest() {
                    // Update padding used for threshold calculation only
                    let used = last.size() - last.padding;
                    padding -= used;
                }
            }

            let ratio = padding as f64 / total_size as f64;

            // do not reuse chunks if introduced padding higher than threshold
            // opt for re-encoding in that case
            if ratio > CHUNK_PADDING_THRESHOLD {
                log::debug!(
                    "Padding ratio: {ratio} > {CHUNK_PADDING_THRESHOLD}, padding: {}, total {}, chunks: {}",
                    HumanByte::from(padding),
                    HumanByte::from(total_size),
                    indices.len(),
                );
                self.cache.update_last_chunk(prev_last_chunk);
                self.encode_entries_to_archive(encoder, None).await?;
            } else {
                log::debug!(
                    "Padding ratio: {ratio} < {CHUNK_PADDING_THRESHOLD}, padding: {}, total {}, chunks: {}",
                    HumanByte::from(padding),
                    HumanByte::from(total_size),
                    indices.len(),
                );

                // check for cases where kept back last is not equal first chunk because the range
                // end aligned with a chunk boundary, and the chunks therefore needs to be injected
                if let (Some(first), Some(last)) = (indices.first_mut(), prev_last_chunk) {
                    if last.digest() != first.digest() {
                        // make sure to inject previous last chunk before encoding entries
                        self.inject_chunks_at_current_payload_position(encoder, vec![last])?;
                    } else {
                        let used = last.size() - last.padding;
                        first.padding -= used;
                    }
                }

                let base_offset = Some(encoder.payload_position()?.add(start_padding));
                self.encode_entries_to_archive(encoder, base_offset).await?;

                if keep_last_chunk {
                    self.cache.update_last_chunk(indices.pop());
                }

                self.inject_chunks_at_current_payload_position(encoder, indices)?;
            }

            Ok(())
        } else {
            bail!("cannot reuse chunks without previous index reader");
        }
    }

    // Take ownership of cached entries and encode them to the archive
    // Encode with reused payload chunks when base offset is some, reencode otherwise
    async fn encode_entries_to_archive<T: SeqWrite + Send>(
        &mut self,
        encoder: &mut Encoder<'_, T>,
        base_offset: Option<PayloadOffset>,
    ) -> Result<(), Error> {
        if let Some(prev) = self.cache.take_last_chunk() {
            // make sure to inject previous last chunk before encoding entries
            self.inject_chunks_at_current_payload_position(encoder, vec![prev])?;
        }

        // take ownership of cached entries and reset caching state
        let (entries, start_path) = self.cache.take_and_reset();
        let old_path = self.path.clone();
        self.path = start_path;
        log::debug!(
            "Got {} cache entries to encode: reuse is {}",
            entries.len(),
            base_offset.is_some()
        );

        for entry in entries {
            match entry {
                CacheEntry::RegEntry(CacheEntryData {
                    fd,
                    c_file_name,
                    stat,
                    metadata,
                    payload_offset,
                }) => {
                    let file_name = OsStr::from_bytes(c_file_name.to_bytes());
                    self.path.push(file_name);
                    self.add_entry_to_archive(
                        encoder,
                        &mut None,
                        &c_file_name,
                        &stat,
                        fd,
                        &metadata,
                        base_offset.map(|base_offset| payload_offset.add(base_offset.raw())),
                    )
                    .await?;
                    self.path.pop();
                }
                CacheEntry::DirEntry(CacheEntryData {
                    c_file_name,
                    metadata,
                    ..
                }) => {
                    let file_name = OsStr::from_bytes(c_file_name.to_bytes());
                    self.path.push(file_name);
                    if let Some(ref catalog) = self.catalog {
                        catalog.lock().unwrap().start_directory(&c_file_name)?;
                    }
                    let dir_name = OsStr::from_bytes(c_file_name.to_bytes());
                    encoder.create_directory(dir_name, &metadata).await?;
                }
                CacheEntry::DirEnd => {
                    encoder.finish().await?;
                    if let Some(ref catalog) = self.catalog {
                        catalog.lock().unwrap().end_directory()?;
                    }
                    self.path.pop();
                }
            }
        }

        self.path = old_path;

        Ok(())
    }

    fn inject_chunks_at_current_payload_position<T: SeqWrite + Send>(
        &mut self,
        encoder: &mut Encoder<'_, T>,
        reused_chunks: Vec<ReusableDynamicEntry>,
    ) -> Result<(), Error> {
        let mut injection_boundary = encoder.payload_position()?;

        for chunks in reused_chunks.chunks(128) {
            let chunks = chunks.to_vec();
            let mut size = PayloadOffset::default();

            for chunk in chunks.iter() {
                log::debug!(
                    "Injecting chunk with {} padding (chunk size {})",
                    HumanByte::from(chunk.padding),
                    HumanByte::from(chunk.size()),
                );
                self.reuse_stats.total_injected_size += chunk.size();
                self.reuse_stats.total_injected_count += 1;

                if chunk.padding > 0 {
                    self.reuse_stats.partial_chunks_count += 1;
                }

                size = size.add(chunk.size());
            }

            let inject_chunks = InjectChunks {
                boundary: injection_boundary.raw(),
                chunks,
                size: size.raw() as usize,
            };

            if let Some(sender) = self.forced_boundaries.as_mut() {
                sender.send(inject_chunks)?;
            } else {
                bail!("missing injection queue");
            };

            injection_boundary = injection_boundary.add(size.raw());
            log::debug!("Advance payload position by: {size:?}");
            encoder.advance(size)?;
        }

        Ok(())
    }

    async fn add_directory<T: SeqWrite + Send>(
        &mut self,
        encoder: &mut Encoder<'_, T>,
        previous_metadata_accessor: &mut Option<Directory<MetadataArchiveReader>>,
        dir: Dir,
        c_dir_name: &CStr,
        metadata: &Metadata,
        stat: &FileStat,
    ) -> Result<(), Error> {
        let dir_name = OsStr::from_bytes(c_dir_name.to_bytes());

        if !self.cache.caching_enabled() {
            if let Some(ref catalog) = self.catalog {
                catalog.lock().unwrap().start_directory(c_dir_name)?;
            }
            encoder.create_directory(dir_name, metadata).await?;
        }

        let old_fs_magic = self.fs_magic;
        let old_fs_feature_flags = self.fs_feature_flags;
        let old_st_dev = self.current_st_dev;

        let mut skip_contents = false;
        if old_st_dev != stat.st_dev {
            self.fs_magic = detect_fs_type(dir.as_raw_fd())?;
            self.fs_feature_flags = Flags::from_magic(self.fs_magic);
            self.current_st_dev = stat.st_dev;

            if is_virtual_file_system(self.fs_magic) {
                skip_contents = true;
            } else if let Some(set) = &self.device_set {
                skip_contents = !set.contains(&stat.st_dev);
            }
        }

        let result = if skip_contents {
            log::info!("skipping mount point: {:?}", self.path);
            Ok(())
        } else {
            let mut dir_accessor = None;
            if let Some(accessor) = previous_metadata_accessor.as_mut() {
                if let Some(file_entry) = accessor.lookup(dir_name).await? {
                    if file_entry.entry().is_dir() {
                        let dir = file_entry.enter_directory().await?;
                        dir_accessor = Some(dir);
                    }
                }
            }
            self.archive_dir_contents(encoder, dir_accessor, dir, false)
                .await
        };

        self.fs_magic = old_fs_magic;
        self.fs_feature_flags = old_fs_feature_flags;
        self.current_st_dev = old_st_dev;

        if !self.cache.caching_enabled() {
            encoder.finish().await?;
            if let Some(ref catalog) = self.catalog {
                catalog.lock().unwrap().end_directory()?;
            }
        } else {
            self.cache.insert_dir_end();
        }

        result
    }

    async fn add_regular_file<T: SeqWrite + Send>(
        &mut self,
        encoder: &mut Encoder<'_, T>,
        fd: OwnedFd,
        file_name: &Path,
        metadata: &Metadata,
        file_size: u64,
    ) -> Result<LinkOffset, Error> {
        let mut file = unsafe { std::fs::File::from_raw_fd(fd.into_raw_fd()) };
        let mut remaining = file_size;
        let mut out = encoder.create_file(metadata, file_name, file_size).await?;
        while remaining != 0 {
            let mut got = match file.read(&mut self.file_copy_buffer[..]) {
                Ok(0) => break,
                Ok(got) => got,
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) => bail!(err),
            };
            if got as u64 > remaining {
                self.report_file_grew_while_reading();
                got = remaining as usize;
            }
            out.write_all(&self.file_copy_buffer[..got]).await?;
            remaining -= got as u64;
        }
        if remaining > 0 {
            self.report_file_shrunk_while_reading();
            let to_zero = remaining.min(self.file_copy_buffer.len() as u64) as usize;
            vec::clear(&mut self.file_copy_buffer[..to_zero]);
            while remaining != 0 {
                let fill = remaining.min(self.file_copy_buffer.len() as u64) as usize;
                out.write_all(&self.file_copy_buffer[..fill]).await?;
                remaining -= fill as u64;
            }
        }

        Ok(out.file_offset())
    }

    async fn add_symlink<T: SeqWrite + Send>(
        &mut self,
        encoder: &mut Encoder<'_, T>,
        fd: OwnedFd,
        file_name: &Path,
        metadata: &Metadata,
    ) -> Result<(), Error> {
        let dest = nix::fcntl::readlinkat(fd.as_raw_fd(), &b""[..])?;
        encoder.add_symlink(metadata, file_name, dest).await?;
        Ok(())
    }

    async fn add_device<T: SeqWrite + Send>(
        &mut self,
        encoder: &mut Encoder<'_, T>,
        file_name: &Path,
        metadata: &Metadata,
        stat: &FileStat,
    ) -> Result<(), Error> {
        Ok(encoder
            .add_device(
                metadata,
                file_name,
                pxar::format::Device::from_dev_t(stat.st_rdev),
            )
            .await?)
    }
}

/// Dynamic entry reusable by payload references
#[derive(Clone, Debug)]
#[repr(C)]
pub struct ReusableDynamicEntry {
    size: u64,
    padding: u64,
    digest: [u8; 32],
}

impl ReusableDynamicEntry {
    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[inline]
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// List of dynamic entries containing the data given by an offset range
fn lookup_dynamic_entries(
    index: &DynamicIndexReader,
    range: &Range<u64>,
) -> Result<(Vec<ReusableDynamicEntry>, u64, u64), Error> {
    let end_idx = index.index_count() - 1;
    let chunk_end = index.chunk_end(end_idx);
    let start = index.binary_search(0, 0, end_idx, chunk_end, range.start)?;

    let mut prev_end = if start == 0 {
        0
    } else {
        index.chunk_end(start - 1)
    };
    let padding_start = range.start - prev_end;
    let mut padding_end = 0;

    let mut indices = Vec::new();
    for dynamic_entry in &index.index()[start..] {
        let end = dynamic_entry.end();

        let reusable_dynamic_entry = ReusableDynamicEntry {
            size: (end - prev_end),
            padding: 0,
            digest: dynamic_entry.digest(),
        };
        indices.push(reusable_dynamic_entry);

        if range.end < end {
            padding_end = end - range.end;
            break;
        }
        prev_end = end;
    }

    if let Some(first) = indices.first_mut() {
        first.padding += padding_start;
    }

    if let Some(last) = indices.last_mut() {
        last.padding += padding_end;
    }

    Ok((indices, padding_start, padding_end))
}

fn get_metadata(
    fd: RawFd,
    stat: &FileStat,
    flags: Flags,
    fs_magic: i64,
    fs_feature_flags: &mut Flags,
    skip_e2big_xattr: bool,
) -> Result<Metadata, Error> {
    // required for some of these
    let proc_path = Path::new("/proc/self/fd/").join(fd.to_string());

    let mut meta = Metadata {
        stat: pxar::Stat {
            mode: u64::from(stat.st_mode),
            flags: 0,
            uid: stat.st_uid,
            gid: stat.st_gid,
            mtime: pxar::format::StatxTimestamp::new(stat.st_mtime, stat.st_mtime_nsec as u32),
        },
        ..Default::default()
    };

    get_xattr_fcaps_acl(
        &mut meta,
        fd,
        &proc_path,
        flags,
        fs_feature_flags,
        skip_e2big_xattr,
    )?;
    get_chattr(&mut meta, fd)?;
    get_fat_attr(&mut meta, fd, fs_magic)?;
    get_quota_project_id(&mut meta, fd, flags, fs_magic)?;
    Ok(meta)
}

fn get_fcaps(
    meta: &mut Metadata,
    fd: RawFd,
    flags: Flags,
    fs_feature_flags: &mut Flags,
) -> Result<(), Error> {
    if !flags.contains(Flags::WITH_FCAPS) {
        return Ok(());
    }

    match xattr::fgetxattr(fd, xattr::XATTR_NAME_FCAPS) {
        Ok(data) => {
            meta.fcaps = Some(pxar::format::FCaps { data });
            Ok(())
        }
        Err(Errno::ENODATA) => Ok(()),
        Err(Errno::EOPNOTSUPP) => {
            fs_feature_flags.remove(Flags::WITH_FCAPS);
            Ok(())
        }
        Err(Errno::EBADF) => Ok(()), // symlinks
        Err(err) => Err(err).context("failed to read file capabilities"),
    }
}

fn get_xattr_fcaps_acl(
    meta: &mut Metadata,
    fd: RawFd,
    proc_path: &Path,
    flags: Flags,
    fs_feature_flags: &mut Flags,
    skip_e2big_xattr: bool,
) -> Result<(), Error> {
    if !flags.contains(Flags::WITH_XATTRS) {
        return Ok(());
    }

    let xattrs = match xattr::flistxattr(fd) {
        Ok(names) => names,
        Err(Errno::EOPNOTSUPP) => {
            fs_feature_flags.remove(Flags::WITH_XATTRS);
            return Ok(());
        }
        Err(Errno::E2BIG) => {
            match skip_e2big_xattr {
                true => return Ok(()),
                false => {
                    bail!("{} (try --skip-e2big-xattr)", Errno::E2BIG.to_string());
                }
            };
        }
        Err(Errno::EBADF) => return Ok(()), // symlinks
        Err(err) => return Err(err).context("failed to read xattrs"),
    };

    for attr in &xattrs {
        if xattr::is_security_capability(attr) {
            get_fcaps(meta, fd, flags, fs_feature_flags)?;
            continue;
        }

        if xattr::is_acl(attr) {
            get_acl(meta, proc_path, flags, fs_feature_flags)?;
            continue;
        }

        if !xattr::is_valid_xattr_name(attr) {
            continue;
        }

        match xattr::fgetxattr(fd, attr) {
            Ok(data) => meta
                .xattrs
                .push(pxar::format::XAttr::new(attr.to_bytes(), data)),
            Err(Errno::ENODATA) => (), // it got removed while we were iterating...
            Err(Errno::EOPNOTSUPP) => (), // shouldn't be possible so just ignore this
            Err(Errno::EBADF) => (),   // symlinks, shouldn't be able to reach this either
            Err(Errno::E2BIG) => {
                match skip_e2big_xattr {
                    true => return Ok(()),
                    false => {
                        bail!("{} (try --skip-e2big-xattr)", Errno::E2BIG.to_string());
                    }
                };
            }
            Err(err) => {
                return Err(err).context(format!("error reading extended attribute {attr:?}"))
            }
        }
    }

    Ok(())
}

fn get_chattr(metadata: &mut Metadata, fd: RawFd) -> Result<(), Error> {
    let mut attr: libc::c_long = 0;

    match unsafe { fs::read_attr_fd(fd, &mut attr) } {
        Ok(_) => (),
        Err(errno) if errno_is_unsupported(errno) => {
            return Ok(());
        }
        Err(err) => return Err(err).context("failed to read file attributes"),
    }

    metadata.stat.flags |= Flags::from_chattr(attr).bits();

    Ok(())
}

fn get_fat_attr(metadata: &mut Metadata, fd: RawFd, fs_magic: i64) -> Result<(), Error> {
    use proxmox_sys::linux::magic::*;

    if fs_magic != MSDOS_SUPER_MAGIC && fs_magic != FUSE_SUPER_MAGIC {
        return Ok(());
    }

    let mut attr: u32 = 0;

    match unsafe { fs::read_fat_attr_fd(fd, &mut attr) } {
        Ok(_) => (),
        Err(errno) if errno_is_unsupported(errno) => {
            return Ok(());
        }
        Err(err) => return Err(err).context("failed to read fat attributes"),
    }

    metadata.stat.flags |= Flags::from_fat_attr(attr).bits();

    Ok(())
}

/// Read the quota project id for an inode, supported on ext4/XFS/FUSE/ZFS filesystems
fn get_quota_project_id(
    metadata: &mut Metadata,
    fd: RawFd,
    flags: Flags,
    magic: i64,
) -> Result<(), Error> {
    if !(metadata.is_dir() || metadata.is_regular_file()) {
        return Ok(());
    }

    if !flags.contains(Flags::WITH_QUOTA_PROJID) {
        return Ok(());
    }

    use proxmox_sys::linux::magic::*;

    match magic {
        EXT4_SUPER_MAGIC | XFS_SUPER_MAGIC | FUSE_SUPER_MAGIC | ZFS_SUPER_MAGIC => (),
        _ => return Ok(()),
    }

    let mut fsxattr = fs::FSXAttr::default();
    let res = unsafe { fs::fs_ioc_fsgetxattr(fd, &mut fsxattr) };

    // On some FUSE filesystems it can happen that ioctl is not supported.
    // For these cases projid is set to 0 while the error is ignored.
    if let Err(errno) = res {
        if errno_is_unsupported(errno) {
            return Ok(());
        } else {
            return Err(errno).context("error while reading quota project id");
        }
    }

    let projid = fsxattr.fsx_projid as u64;
    if projid != 0 {
        metadata.quota_project_id = Some(pxar::format::QuotaProjectId { projid });
    }
    Ok(())
}

fn get_acl(
    metadata: &mut Metadata,
    proc_path: &Path,
    flags: Flags,
    fs_feature_flags: &mut Flags,
) -> Result<(), Error> {
    if !flags.contains(Flags::WITH_ACL) {
        return Ok(());
    }

    if metadata.is_symlink() {
        return Ok(());
    }

    get_acl_do(metadata, proc_path, acl::ACL_TYPE_ACCESS, fs_feature_flags)?;

    if metadata.is_dir() {
        get_acl_do(metadata, proc_path, acl::ACL_TYPE_DEFAULT, fs_feature_flags)?;
    }

    Ok(())
}

fn get_acl_do(
    metadata: &mut Metadata,
    proc_path: &Path,
    acl_type: acl::ACLType,
    fs_feature_flags: &mut Flags,
) -> Result<(), Error> {
    // In order to be able to get ACLs with type ACL_TYPE_DEFAULT, we have
    // to create a path for acl_get_file(). acl_get_fd() only allows to get
    // ACL_TYPE_ACCESS attributes.
    let acl = match acl::ACL::get_file(proc_path, acl_type) {
        Ok(acl) => acl,
        // Don't bail if underlying endpoint does not support acls
        Err(Errno::EOPNOTSUPP) => {
            fs_feature_flags.remove(Flags::WITH_ACL);
            return Ok(());
        }
        // Don't bail if the endpoint cannot carry acls
        Err(Errno::EBADF) => return Ok(()),
        // Don't bail if there is no data
        Err(Errno::ENODATA) => return Ok(()),
        Err(err) => return Err(err).context("error while reading ACL"),
    };

    process_acl(metadata, acl, acl_type)
}

fn process_acl(
    metadata: &mut Metadata,
    acl: acl::ACL,
    acl_type: acl::ACLType,
) -> Result<(), Error> {
    use pxar::format::acl as pxar_acl;
    use pxar::format::acl::{Group, GroupObject, Permissions, User};

    let mut acl_user = Vec::new();
    let mut acl_group = Vec::new();
    let mut acl_group_obj = None;
    let mut acl_default = None;
    let mut user_obj_permissions = None;
    let mut group_obj_permissions = None;
    let mut other_permissions = None;
    let mut mask_permissions = None;

    for entry in &mut acl.entries() {
        let tag = entry.get_tag_type()?;
        let permissions = entry.get_permissions()?;
        match tag {
            acl::ACL_USER_OBJ => user_obj_permissions = Some(Permissions(permissions)),
            acl::ACL_GROUP_OBJ => group_obj_permissions = Some(Permissions(permissions)),
            acl::ACL_OTHER => other_permissions = Some(Permissions(permissions)),
            acl::ACL_MASK => mask_permissions = Some(Permissions(permissions)),
            acl::ACL_USER => {
                acl_user.push(User {
                    uid: entry.get_qualifier()?,
                    permissions: Permissions(permissions),
                });
            }
            acl::ACL_GROUP => {
                acl_group.push(Group {
                    gid: entry.get_qualifier()?,
                    permissions: Permissions(permissions),
                });
            }
            _ => bail!("Unexpected ACL tag encountered!"),
        }
    }

    acl_user.sort();
    acl_group.sort();

    match acl_type {
        acl::ACL_TYPE_ACCESS => {
            // The mask permissions are mapped to the stat group permissions
            // in case that the ACL group permissions were set.
            // Only in that case we need to store the group permissions,
            // in the other cases they are identical to the stat group permissions.
            if let (Some(gop), true) = (group_obj_permissions, mask_permissions.is_some()) {
                acl_group_obj = Some(GroupObject { permissions: gop });
            }

            metadata.acl.users = acl_user;
            metadata.acl.groups = acl_group;
            metadata.acl.group_obj = acl_group_obj;
        }
        acl::ACL_TYPE_DEFAULT => {
            if user_obj_permissions.is_some()
                || group_obj_permissions.is_some()
                || other_permissions.is_some()
                || mask_permissions.is_some()
            {
                acl_default = Some(pxar_acl::Default {
                    // The value is set to UINT64_MAX as placeholder if one
                    // of the permissions is not set
                    user_obj_permissions: user_obj_permissions.unwrap_or(Permissions::NO_MASK),
                    group_obj_permissions: group_obj_permissions.unwrap_or(Permissions::NO_MASK),
                    other_permissions: other_permissions.unwrap_or(Permissions::NO_MASK),
                    mask_permissions: mask_permissions.unwrap_or(Permissions::NO_MASK),
                });
            }

            metadata.acl.default_users = acl_user;
            metadata.acl.default_groups = acl_group;
            metadata.acl.default = acl_default;
        }
        _ => bail!("Unexpected ACL type encountered"),
    }

    Ok(())
}

/// Note that our pattern lists are "positive". `MatchType::Include` means the file is included.
/// Since we are generating an *exclude* list, we need to invert this, so includes get a `'!'`
/// prefix.
fn generate_pxar_excludes_cli(patterns: &[MatchEntry]) -> Vec<u8> {
    use pathpatterns::MatchPattern;

    let mut content = Vec::new();

    for pattern in patterns {
        match pattern.match_type() {
            MatchType::Include => content.push(b'!'),
            MatchType::Exclude => (),
        }

        match pattern.pattern() {
            MatchPattern::Literal(lit) => content.extend(lit),
            MatchPattern::Pattern(pat) => content.extend(pat.pattern().to_bytes()),
        }

        if pattern.match_flags() == MatchFlag::MATCH_DIRECTORIES && content.last() != Some(&b'/') {
            content.push(b'/');
        }

        content.push(b'\n');
    }

    content
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs::File;
    use std::fs::OpenOptions;
    use std::io::{self, BufReader, Seek, SeekFrom, Write};
    use std::pin::Pin;
    use std::process::Command;
    use std::sync::mpsc;
    use std::task::{Context, Poll};

    use pbs_datastore::dynamic_index::DynamicIndexReader;
    use pxar::accessor::sync::FileReader;
    use pxar::encoder::SeqWrite;

    use crate::pxar::extract::Extractor;
    use crate::pxar::OverwriteFlags;

    use super::*;

    struct DummyWriter {
        file: Option<File>,
    }

    impl DummyWriter {
        fn new<P: AsRef<Path>>(path: Option<P>) -> Result<Self, Error> {
            let file = if let Some(path) = path {
                Some(
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .truncate(true)
                        .create(true)
                        .open(path)?,
                )
            } else {
                None
            };
            Ok(Self { file })
        }
    }

    impl Write for DummyWriter {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            if let Some(file) = self.file.as_mut() {
                file.write_all(data)?;
            }
            Ok(data.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if let Some(file) = self.file.as_mut() {
                file.flush()?;
            }
            Ok(())
        }
    }

    impl SeqWrite for DummyWriter {
        fn poll_seq_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(self.as_mut().write(buf))
        }

        fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), io::Error>> {
            Poll::Ready(self.as_mut().flush())
        }
    }

    fn prepare<P: AsRef<Path>>(dir_path: P) -> Result<(), Error> {
        let dir = nix::dir::Dir::open(dir_path.as_ref(), OFlag::O_DIRECTORY, Mode::empty())?;

        let fs_magic = detect_fs_type(dir.as_raw_fd()).unwrap();
        let stat = nix::sys::stat::fstat(dir.as_raw_fd()).unwrap();
        let mut fs_feature_flags = Flags::from_magic(fs_magic);
        let metadata = get_metadata(
            dir.as_raw_fd(),
            &stat,
            fs_feature_flags,
            fs_magic,
            &mut fs_feature_flags,
            false,
        )?;

        let mut extractor = Extractor::new(
            dir,
            metadata.clone(),
            true,
            OverwriteFlags::empty(),
            fs_feature_flags,
        );

        let dir_metadata = Metadata {
            stat: pxar::Stat::default().mode(0o777u64).set_dir(),
            ..Default::default()
        };

        let file_metadata = Metadata {
            stat: pxar::Stat::default().mode(0o777u64).set_regular_file(),
            ..Default::default()
        };

        extractor.enter_directory(
            OsString::from("testdir".to_string()),
            dir_metadata.clone(),
            true,
        )?;

        let size = 1024 * 1024;
        let mut cursor = BufReader::new(std::io::Cursor::new(vec![0u8; size]));
        for i in 0..10 {
            extractor.enter_directory(
                OsString::from(format!("folder_{i}")),
                dir_metadata.clone(),
                true,
            )?;
            for j in 0..10 {
                cursor.seek(SeekFrom::Start(0))?;
                extractor.extract_file(
                    CString::new(format!("file_{j}").as_str())?.as_c_str(),
                    &file_metadata,
                    size as u64,
                    &mut cursor,
                    true,
                )?;
            }
            extractor.leave_directory()?;
        }

        extractor.leave_directory()?;

        Ok(())
    }

    #[test]
    fn test_create_archive_with_reference() -> Result<(), Error> {
        let euid = unsafe { libc::geteuid() };
        let egid = unsafe { libc::getegid() };

        if euid != 1000 || egid != 1000 {
            // skip test, cannot create test folder structure with correct ownership
            return Ok(());
        }

        let mut testdir = PathBuf::from("./target/testout");
        testdir.push(std::module_path!());

        let _ = std::fs::remove_dir_all(&testdir);
        let _ = std::fs::create_dir_all(&testdir);

        prepare(testdir.as_path())?;

        let previous_payload_index = Some(DynamicIndexReader::new(File::open(
            "../tests/pxar/backup-client-pxar-data.ppxar.didx",
        )?)?);
        let metadata_archive = File::open("../tests/pxar/backup-client-pxar-data.mpxar").unwrap();
        let metadata_size = metadata_archive.metadata()?.len();
        let reader: MetadataArchiveReader = Arc::new(FileReader::new(metadata_archive));

        let rt = tokio::runtime::Runtime::new().unwrap();
        let (suggested_boundaries, _rx) = mpsc::channel();
        let (forced_boundaries, _rx) = mpsc::channel();

        rt.block_on(async move {
            testdir.push("testdir");
            let source_dir =
                nix::dir::Dir::open(testdir.as_path(), OFlag::O_DIRECTORY, Mode::empty()).unwrap();

            let fs_magic = detect_fs_type(source_dir.as_raw_fd()).unwrap();
            let stat = nix::sys::stat::fstat(source_dir.as_raw_fd()).unwrap();
            let mut fs_feature_flags = Flags::from_magic(fs_magic);

            let metadata = get_metadata(
                source_dir.as_raw_fd(),
                &stat,
                fs_feature_flags,
                fs_magic,
                &mut fs_feature_flags,
                false,
            )?;

            let writer = DummyWriter::new(Some("./target/backup-client-pxar-run.mpxar")).unwrap();
            let payload_writer = DummyWriter::new::<PathBuf>(None).unwrap();

            let mut encoder = Encoder::new(
                pxar::PxarVariant::Split(writer, payload_writer),
                &metadata,
                Some(&[]),
            )
            .await?;

            let mut archiver = Archiver {
                feature_flags: Flags::from_magic(fs_magic),
                fs_feature_flags: Flags::from_magic(fs_magic),
                fs_magic,
                callback: Box::new(|_| Ok(())),
                patterns: Vec::new(),
                catalog: None,
                path: PathBuf::new(),
                entry_counter: 0,
                entry_limit: 1024,
                current_st_dev: stat.st_dev,
                device_set: None,
                hardlinks: HashMap::new(),
                file_copy_buffer: vec::undefined(4 * 1024 * 1024),
                skip_e2big_xattr: false,
                forced_boundaries: Some(forced_boundaries),
                previous_payload_index,
                suggested_boundaries: Some(suggested_boundaries),
                cache: PxarLookaheadCache::new(None),
                reuse_stats: ReuseStats::default(),
                split_archive: true,
            };

            let accessor = Accessor::new(pxar::PxarVariant::Unified(reader), metadata_size)
                .await
                .unwrap();
            let root = accessor.open_root().await.ok();
            archiver
                .archive_dir_contents(&mut encoder, root, source_dir, true)
                .await
                .unwrap();

            archiver
                .flush_cached_reusing_if_below_threshold(&mut encoder, false)
                .await
                .unwrap();

            encoder.finish().await.unwrap();
            encoder.close().await.unwrap();

            let status = Command::new("diff")
                .args([
                    "../tests/pxar/backup-client-pxar-expected.mpxar",
                    "./target/backup-client-pxar-run.mpxar",
                ])
                .status()
                .expect("failed to execute diff");
            assert!(status.success());

            Ok::<(), Error>(())
        })
    }
}
