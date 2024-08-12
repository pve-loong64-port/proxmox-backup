//! Some common methods used within the pxar code.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, format_err, Context, Error};
use nix::sys::stat::Mode;

use pxar::accessor::aio::{Accessor, FileEntry};
use pxar::accessor::ReadAt;
use pxar::format::{SignedDuration, StatxTimestamp};
use pxar::{mode, Entry, EntryKind, Metadata};

use pbs_datastore::catalog::{ArchiveEntry, DirEntryAttribute};

use pbs_datastore::dynamic_index::{BufferedDynamicReader, LocalDynamicReadAt};
use pbs_datastore::index::IndexFile;
use pbs_datastore::BackupManifest;
use pbs_tools::crypt_config::CryptConfig;

use crate::{BackupReader, RemoteChunkReader};

/// Get the file permissions as `nix::Mode`
pub(crate) fn perms_from_metadata(meta: &Metadata) -> Result<Mode, Error> {
    let mode = meta.stat.get_permission_bits();

    u32::try_from(mode)
        .context("couldn't narrow permission bits")
        .and_then(|mode| {
            Mode::from_bits(mode)
                .with_context(|| format!("mode contains illegal bits: 0x{:x} (0o{:o})", mode, mode))
        })
}

/// Make sure path is relative and not '.' or '..'.
pub(crate) fn assert_relative_path<S: AsRef<OsStr> + ?Sized>(path: &S) -> Result<(), Error> {
    assert_relative_path_do(Path::new(path))
}

/// Make sure path is a single component and not '.' or '..'.
pub(crate) fn assert_single_path_component<S: AsRef<OsStr> + ?Sized>(
    path: &S,
) -> Result<(), Error> {
    assert_single_path_component_do(Path::new(path))
}

fn assert_relative_path_do(path: &Path) -> Result<(), Error> {
    if !path.is_relative() {
        bail!("bad absolute file name in archive: {:?}", path);
    }

    Ok(())
}

fn assert_single_path_component_do(path: &Path) -> Result<(), Error> {
    assert_relative_path_do(path)?;

    let mut components = path.components();
    match components.next() {
        Some(std::path::Component::Normal(_)) => (),
        _ => bail!("invalid path component in archive: {:?}", path),
    }

    if components.next().is_some() {
        bail!(
            "invalid path with multiple components in archive: {:?}",
            path
        );
    }

    Ok(())
}

#[rustfmt::skip]
fn symbolic_mode(c: u64, special: bool, special_x: u8, special_no_x: u8) -> [u8; 3] {
    [
        if 0 != c & 4 { b'r' } else { b'-' },
        if 0 != c & 2 { b'w' } else { b'-' },
        match (c & 1, special) {
            (0, false) => b'-',
            (0, true) => special_no_x,
            (_, false) => b'x',
            (_, true) => special_x,
        }
    ]
}

fn mode_string(entry: &Entry) -> String {
    // https://www.gnu.org/software/coreutils/manual/html_node/What-information-is-listed.html#What-information-is-listed
    // additionally we use:
    //     file type capital 'L' hard links
    //     a second '+' after the mode to show non-acl xattr presence
    //
    // Trwxrwxrwx++ uid/gid size mtime filename [-> destination]

    let meta = entry.metadata();
    let mode = meta.stat.mode;
    let type_char = if entry.is_hardlink() {
        'L'
    } else {
        match mode & mode::IFMT {
            mode::IFREG => '-',
            mode::IFBLK => 'b',
            mode::IFCHR => 'c',
            mode::IFDIR => 'd',
            mode::IFLNK => 'l',
            mode::IFIFO => 'p',
            mode::IFSOCK => 's',
            _ => '?',
        }
    };

    let fmt_u = symbolic_mode((mode >> 6) & 7, 0 != mode & mode::ISUID, b's', b'S');
    let fmt_g = symbolic_mode((mode >> 3) & 7, 0 != mode & mode::ISGID, b's', b'S');
    let fmt_o = symbolic_mode(mode & 7, 0 != mode & mode::ISVTX, b't', b'T');

    let has_acls = if meta.acl.is_empty() { ' ' } else { '+' };

    let has_xattrs = if meta.xattrs.is_empty() { ' ' } else { '+' };

    format!(
        "{}{}{}{}{}{}",
        type_char,
        unsafe { std::str::from_utf8_unchecked(&fmt_u) },
        unsafe { std::str::from_utf8_unchecked(&fmt_g) },
        unsafe { std::str::from_utf8_unchecked(&fmt_o) },
        has_acls,
        has_xattrs,
    )
}

fn format_mtime(mtime: &StatxTimestamp) -> String {
    if let Ok(s) = proxmox_time::strftime_local("%Y-%m-%d %H:%M:%S", mtime.secs) {
        return s;
    }
    format!("{}.{}", mtime.secs, mtime.nanos)
}

pub fn format_single_line_entry(entry: &Entry) -> String {
    let mode_string = mode_string(entry);

    let meta = entry.metadata();

    let (size, link, payload_offset) = match entry.kind() {
        EntryKind::File {
            size,
            payload_offset,
            ..
        } => (format!("{}", *size), String::new(), *payload_offset),
        EntryKind::Symlink(link) => ("0".to_string(), format!(" -> {:?}", link.as_os_str()), None),
        EntryKind::Hardlink(link) => ("0".to_string(), format!(" -> {:?}", link.as_os_str()), None),
        EntryKind::Device(dev) => (format!("{},{}", dev.major, dev.minor), String::new(), None),
        _ => ("0".to_string(), String::new(), None),
    };

    let owner_string = format!("{}/{}", meta.stat.uid, meta.stat.gid);

    if let Some(offset) = payload_offset {
        format!(
            "{} {:<13} {} {:>8} {:?}{} {}",
            mode_string,
            owner_string,
            format_mtime(&meta.stat.mtime),
            size,
            entry.path(),
            link,
            offset,
        )
    } else {
        format!(
            "{} {:<13} {} {:>8} {:?}{}",
            mode_string,
            owner_string,
            format_mtime(&meta.stat.mtime),
            size,
            entry.path(),
            link,
        )
    }
}

pub fn format_multi_line_entry(entry: &Entry) -> String {
    let mode_string = mode_string(entry);

    let meta = entry.metadata();

    let (size, link, type_name, payload_offset) = match entry.kind() {
        EntryKind::Version(version) => (format!("{version:?}"), String::new(), "version", None),
        EntryKind::Prelude(prelude) => (
            "0".to_string(),
            format!("raw data: {:?} bytes", prelude.data.len()),
            "prelude",
            None,
        ),
        EntryKind::File {
            size,
            payload_offset,
            ..
        } => (format!("{}", *size), String::new(), "file", *payload_offset),
        EntryKind::Symlink(link) => (
            "0".to_string(),
            format!(" -> {:?}", link.as_os_str()),
            "symlink",
            None,
        ),
        EntryKind::Hardlink(link) => (
            "0".to_string(),
            format!(" -> {:?}", link.as_os_str()),
            "symlink",
            None,
        ),
        EntryKind::Device(dev) => (
            format!("{},{}", dev.major, dev.minor),
            String::new(),
            if meta.stat.is_chardev() {
                "characters pecial file"
            } else if meta.stat.is_blockdev() {
                "block special file"
            } else {
                "device"
            },
            None,
        ),
        EntryKind::Socket => ("0".to_string(), String::new(), "socket", None),
        EntryKind::Fifo => ("0".to_string(), String::new(), "fifo", None),
        EntryKind::Directory => ("0".to_string(), String::new(), "directory", None),
        EntryKind::GoodbyeTable => ("0".to_string(), String::new(), "bad entry", None),
    };

    let file_name = match std::str::from_utf8(entry.path().as_os_str().as_bytes()) {
        Ok(name) => std::borrow::Cow::Borrowed(name),
        Err(_) => std::borrow::Cow::Owned(format!("{:?}", entry.path())),
    };

    if let Some(offset) = payload_offset {
        format!(
            "  File: {}{}\n  \
               Size: {:<13} Type: {}\n\
             Access: ({:o}/{})  Uid: {:<5} Gid: {:<5}\n\
             Modify: {}\n
             PayloadOffset: {}\n",
            file_name,
            link,
            size,
            type_name,
            meta.file_mode(),
            mode_string,
            meta.stat.uid,
            meta.stat.gid,
            format_mtime(&meta.stat.mtime),
            offset,
        )
    } else {
        format!(
            "  File: {}{}\n  \
               Size: {:<13} Type: {}\n\
             Access: ({:o}/{})  Uid: {:<5} Gid: {:<5}\n\
             Modify: {}\n",
            file_name,
            link,
            size,
            type_name,
            meta.file_mode(),
            mode_string,
            meta.stat.uid,
            meta.stat.gid,
            format_mtime(&meta.stat.mtime),
        )
    }
}

/// Look up the directory entries of the given directory `path` in a pxar archive via it's given
/// `accessor` and return the entries formatted as [`ArchiveEntry`]'s, compatible with reading
/// entries from the catalog.
///
/// If the optional `path_prefix` is given, all returned entry paths will be prefixed with it.
pub async fn pxar_metadata_catalog_lookup<T: Clone + ReadAt>(
    accessor: Accessor<T>,
    path: &OsStr,
    path_prefix: Option<&str>,
) -> Result<Vec<ArchiveEntry>, Error> {
    let root = accessor.open_root().await?;
    let dir_entry = root
        .lookup(&path)
        .await
        .map_err(|err| format_err!("lookup failed - {err}"))?
        .ok_or_else(|| format_err!("lookup failed - error opening '{path:?}'"))?;

    let mut entries = Vec::new();
    if let EntryKind::Directory = dir_entry.kind() {
        let dir_entry = dir_entry
            .enter_directory()
            .await
            .map_err(|err| format_err!("failed to enter directory - {err}"))?;

        let mut entries_iter = dir_entry.read_dir();
        while let Some(entry) = entries_iter.next().await {
            let entry = entry?.decode_entry().await?;

            let entry_attr = match DirEntryAttribute::try_from(&entry) {
                Ok(attr) => attr,
                Err(_) => continue,
            };

            let entry_path = crate::pxar::tools::entry_path_with_prefix(&entry, path_prefix);
            entries.push(ArchiveEntry::new(
                entry_path.as_os_str().as_bytes(),
                Some(&entry_attr),
            ));
        }
    } else {
        bail!(format!(
            "expected directory entry, got entry kind '{:?}'",
            dir_entry.kind()
        ));
    }

    Ok(entries)
}

/// Decode possible format version and prelude entries before getting the root directory
/// entry.
///
/// Returns the root directory entry and, if present, the prelude entry
pub fn handle_root_with_optional_format_version_prelude<R: pxar::decoder::SeqRead>(
    decoder: &mut pxar::decoder::sync::Decoder<R>,
) -> Result<(pxar::Entry, Option<pxar::Entry>), Error> {
    let first = decoder
        .next()
        .ok_or_else(|| format_err!("missing root entry"))??;
    match first.kind() {
        pxar::EntryKind::Directory => {
            let version = pxar::format::FormatVersion::Version1;
            log::debug!("pxar format version '{version:?}'");
            Ok((first, None))
        }
        pxar::EntryKind::Version(version) => {
            log::debug!("pxar format version '{version:?}'");
            let second = decoder
                .next()
                .ok_or_else(|| format_err!("missing root entry"))??;
            match second.kind() {
                pxar::EntryKind::Directory => Ok((second, None)),
                pxar::EntryKind::Prelude(_prelude) => {
                    let third = decoder
                        .next()
                        .ok_or_else(|| format_err!("missing root entry"))??;
                    Ok((third, Some(second)))
                }
                _ => bail!("unexpected entry kind {:?}", second.kind()),
            }
        }
        _ => bail!("unexpected entry kind {:?}", first.kind()),
    }
}

pub async fn get_remote_pxar_reader(
    archive_name: &str,
    client: Arc<BackupReader>,
    manifest: &BackupManifest,
    crypt_config: Option<Arc<CryptConfig>>,
) -> Result<(LocalDynamicReadAt<RemoteChunkReader>, u64), Error> {
    let index = client
        .download_dynamic_index(manifest, archive_name)
        .await?;
    let most_used = index.find_most_used_chunks(8);

    let file_info = manifest.lookup_file_info(archive_name)?;
    let chunk_reader = RemoteChunkReader::new(
        client.clone(),
        crypt_config,
        file_info.chunk_crypt_mode(),
        most_used,
    );

    let reader = BufferedDynamicReader::new(index, chunk_reader);
    let archive_size = reader.archive_size();

    Ok((LocalDynamicReadAt::new(reader), archive_size))
}

/// Generate entry path for given [`FileEntry`], prefixed by given `path_prefix` component(s).
pub(crate) fn entry_path_with_prefix<T: Clone + ReadAt>(
    entry: &FileEntry<T>,
    path_prefix: Option<&str>,
) -> PathBuf {
    if let Some(prefix) = path_prefix {
        let mut entry_path = PathBuf::from(prefix);
        match entry.path().strip_prefix("/") {
            Ok(path) => entry_path.push(path),
            Err(_) => entry_path.push(entry.path()),
        }
        entry_path
    } else {
        PathBuf::from(entry.path())
    }
}
