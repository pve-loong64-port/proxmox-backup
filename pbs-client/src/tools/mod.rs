//! Shared tools useful for common CLI clients.
use std::collections::HashMap;
use std::env::VarError::{NotPresent, NotUnicode};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::FromRawFd;
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

use anyhow::{bail, format_err, Context, Error};
use serde_json::{json, Value};
use xdg::BaseDirectories;

use proxmox_http::uri::json_object_to_query;
use proxmox_router::cli::{complete_file_name, shellword_split};
use proxmox_schema::*;
use proxmox_sys::fs::file_get_json;

use pbs_api_types::{Authid, BackupNamespace, RateLimitConfig, UserWithTokens, BACKUP_REPO_URL};
use pbs_datastore::catalog::{ArchiveEntry, DirEntryAttribute};
use pbs_datastore::BackupManifest;
use pxar::accessor::aio::Accessor;
use pxar::accessor::ReadAt;
use pxar::format::SignedDuration;
use pxar::{mode, EntryKind};

use crate::{BackupRepository, HttpClient, HttpClientOptions};

pub mod key_source;

const ENV_VAR_PBS_FINGERPRINT: &str = "PBS_FINGERPRINT";
const ENV_VAR_PBS_PASSWORD: &str = "PBS_PASSWORD";

pub const REPO_URL_SCHEMA: Schema = StringSchema::new("Repository URL.")
    .format(&BACKUP_REPO_URL)
    .max_length(256)
    .schema();

pub const CHUNK_SIZE_SCHEMA: Schema = IntegerSchema::new("Chunk size in KB. Must be a power of 2.")
    .minimum(64)
    .maximum(4096)
    .default(4096)
    .schema();

/// Helper to read a secret through a environment variable (ENV).
///
/// Tries the following variable names in order and returns the value
/// it will resolve for the first defined one:
///
/// BASE_NAME => use value from ENV(BASE_NAME) directly as secret
/// BASE_NAME_FD => read the secret from the specified file descriptor
/// BASE_NAME_FILE => read the secret from the specified file name
/// BASE_NAME_CMD => read the secret from specified command first line of output on stdout
///
/// Only return the first line of data (without CRLF).
pub fn get_secret_from_env(base_name: &str) -> Result<Option<String>, Error> {
    let firstline = |data: String| -> String {
        match data.lines().next() {
            Some(line) => line.to_string(),
            None => String::new(),
        }
    };

    let firstline_file = |file: &mut File| -> Result<String, Error> {
        let reader = BufReader::new(file);
        match reader.lines().next() {
            Some(Ok(line)) => Ok(line),
            Some(Err(err)) => Err(err.into()),
            None => Ok(String::new()),
        }
    };

    match std::env::var(base_name) {
        Ok(p) => return Ok(Some(firstline(p))),
        Err(NotUnicode(_)) => bail!(format!("{} contains bad characters", base_name)),
        Err(NotPresent) => {}
    };

    let env_name = format!("{}_FD", base_name);
    match std::env::var(&env_name) {
        Ok(fd_str) => {
            let fd: i32 = fd_str.parse().map_err(|err| {
                format_err!(
                    "unable to parse file descriptor in ENV({}): {}",
                    env_name,
                    err
                )
            })?;
            let mut file = unsafe { File::from_raw_fd(fd) };
            return Ok(Some(firstline_file(&mut file)?));
        }
        Err(NotUnicode(_)) => bail!(format!("{} contains bad characters", env_name)),
        Err(NotPresent) => {}
    }

    let env_name = format!("{}_FILE", base_name);
    match std::env::var(&env_name) {
        Ok(filename) => {
            let mut file = std::fs::File::open(filename)
                .map_err(|err| format_err!("unable to open file in ENV({}): {}", env_name, err))?;
            return Ok(Some(firstline_file(&mut file)?));
        }
        Err(NotUnicode(_)) => bail!(format!("{} contains bad characters", env_name)),
        Err(NotPresent) => {}
    }

    let env_name = format!("{}_CMD", base_name);
    match std::env::var(&env_name) {
        Ok(ref command) => {
            let args = shellword_split(command)?;
            let mut command = Command::new(&args[0]);
            command.args(&args[1..]);
            let output = proxmox_sys::command::run_command(command, None)?;
            return Ok(Some(firstline(output)));
        }
        Err(NotUnicode(_)) => bail!(format!("{} contains bad characters", env_name)),
        Err(NotPresent) => {}
    }

    Ok(None)
}

pub fn get_default_repository() -> Option<String> {
    std::env::var("PBS_REPOSITORY").ok()
}

pub fn remove_repository_from_value(param: &mut Value) -> Result<BackupRepository, Error> {
    if let Some(url) = param
        .as_object_mut()
        .ok_or_else(|| format_err!("unable to get repository (parameter is not an object)"))?
        .remove("repository")
    {
        return url
            .as_str()
            .ok_or_else(|| format_err!("invalid repository value (must be a string)"))?
            .parse();
    }

    get_default_repository()
        .ok_or_else(|| format_err!("unable to get default repository"))?
        .parse()
}

pub fn extract_repository_from_value(param: &Value) -> Result<BackupRepository, Error> {
    let repo_url = param["repository"]
        .as_str()
        .map(String::from)
        .or_else(get_default_repository)
        .ok_or_else(|| format_err!("unable to get (default) repository"))?;

    let repo: BackupRepository = repo_url.parse()?;

    Ok(repo)
}

pub fn extract_repository_from_map(param: &HashMap<String, String>) -> Option<BackupRepository> {
    param
        .get("repository")
        .map(String::from)
        .or_else(get_default_repository)
        .and_then(|repo_url| repo_url.parse::<BackupRepository>().ok())
}

pub fn connect(repo: &BackupRepository) -> Result<HttpClient, Error> {
    let rate_limit = RateLimitConfig::default(); // unlimited
    connect_do(repo.host(), repo.port(), repo.auth_id(), rate_limit)
        .map_err(|err| format_err!("error building client for repository {} - {}", repo, err))
}

pub fn connect_rate_limited(
    repo: &BackupRepository,
    rate_limit: RateLimitConfig,
) -> Result<HttpClient, Error> {
    connect_do(repo.host(), repo.port(), repo.auth_id(), rate_limit)
        .map_err(|err| format_err!("error building client for repository {} - {}", repo, err))
}

fn connect_do(
    server: &str,
    port: u16,
    auth_id: &Authid,
    rate_limit: RateLimitConfig,
) -> Result<HttpClient, Error> {
    let fingerprint = std::env::var(ENV_VAR_PBS_FINGERPRINT).ok();

    let password = get_secret_from_env(ENV_VAR_PBS_PASSWORD)?;
    let options = HttpClientOptions::new_interactive(password, fingerprint).rate_limit(rate_limit);

    HttpClient::new(server, port, auth_id, options)
}

/// like get, but simply ignore errors and return Null instead
pub async fn try_get(repo: &BackupRepository, url: &str) -> Value {
    let fingerprint = std::env::var(ENV_VAR_PBS_FINGERPRINT).ok();
    let password = get_secret_from_env(ENV_VAR_PBS_PASSWORD).unwrap_or(None);

    // ticket cache, but no questions asked
    let options = HttpClientOptions::new_interactive(password, fingerprint).interactive(false);

    let client = match HttpClient::new(repo.host(), repo.port(), repo.auth_id(), options) {
        Ok(v) => v,
        _ => return Value::Null,
    };

    let mut resp = match client.get(url, None).await {
        Ok(v) => v,
        _ => return Value::Null,
    };

    if let Some(map) = resp.as_object_mut() {
        if let Some(data) = map.remove("data") {
            return data;
        }
    }
    Value::Null
}

pub fn complete_backup_group(_arg: &str, param: &HashMap<String, String>) -> Vec<String> {
    proxmox_async::runtime::main(async { complete_backup_group_do(param).await })
}

pub async fn complete_backup_group_do(param: &HashMap<String, String>) -> Vec<String> {
    let mut result = vec![];

    let repo = match extract_repository_from_map(param) {
        Some(v) => v,
        _ => return result,
    };

    let path = format!("api2/json/admin/datastore/{}/groups", repo.store());

    let data = try_get(&repo, &path).await;

    if let Some(list) = data.as_array() {
        for item in list {
            if let (Some(backup_id), Some(backup_type)) =
                (item["backup-id"].as_str(), item["backup-type"].as_str())
            {
                result.push(format!("{}/{}", backup_type, backup_id));
            }
        }
    }

    result
}

pub fn complete_group_or_snapshot(arg: &str, param: &HashMap<String, String>) -> Vec<String> {
    proxmox_async::runtime::main(async { complete_group_or_snapshot_do(arg, param).await })
}

pub async fn complete_group_or_snapshot_do(
    arg: &str,
    param: &HashMap<String, String>,
) -> Vec<String> {
    if arg.matches('/').count() < 2 {
        let groups = complete_backup_group_do(param).await;
        let mut result = vec![];
        for group in groups {
            result.push(group.to_string());
            result.push(format!("{}/", group));
        }
        return result;
    }

    complete_backup_snapshot_do(param).await
}

pub fn complete_backup_snapshot(_arg: &str, param: &HashMap<String, String>) -> Vec<String> {
    proxmox_async::runtime::main(async { complete_backup_snapshot_do(param).await })
}

pub async fn complete_backup_snapshot_do(param: &HashMap<String, String>) -> Vec<String> {
    let mut result = vec![];

    let repo = match extract_repository_from_map(param) {
        Some(v) => v,
        _ => return result,
    };

    let path = format!("api2/json/admin/datastore/{}/snapshots", repo.store());

    let data = try_get(&repo, &path).await;

    if let Value::Array(list) = data {
        for item in list {
            match serde_json::from_value::<pbs_api_types::BackupDir>(item) {
                Ok(item) => result.push(item.to_string()),
                Err(_) => {
                    // FIXME: print error in completion?
                    continue;
                }
            };
        }
    }

    result
}

pub fn complete_server_file_name(_arg: &str, param: &HashMap<String, String>) -> Vec<String> {
    proxmox_async::runtime::main(async { complete_server_file_name_do(param).await })
}

pub async fn complete_server_file_name_do(param: &HashMap<String, String>) -> Vec<String> {
    let mut result = vec![];

    let repo = match extract_repository_from_map(param) {
        Some(v) => v,
        _ => return result,
    };

    let snapshot: pbs_api_types::BackupDir = match param.get("snapshot") {
        Some(path) => match path.parse() {
            Ok(v) => v,
            _ => return result,
        },
        _ => return result,
    };

    let ns: pbs_api_types::BackupNamespace = match param.get("ns") {
        Some(ns) => match ns.parse() {
            Ok(v) => v,
            _ => return result,
        },
        _ => {
            // If no namespace flag is provided, we assume the root namespace
            pbs_api_types::BackupNamespace::root()
        }
    };

    let query = json_object_to_query(json!({
        "ns": ns,
        "backup-type": snapshot.group.ty,
        "backup-id": snapshot.group.id,
        "backup-time": snapshot.time,
    }))
    .unwrap();

    let path = format!("api2/json/admin/datastore/{}/files?{}", repo.store(), query);

    let data = try_get(&repo, &path).await;

    if let Some(list) = data.as_array() {
        for item in list {
            if let Some(filename) = item["filename"].as_str() {
                result.push(filename.to_owned());
            }
        }
    }

    result
}

pub fn complete_archive_name(arg: &str, param: &HashMap<String, String>) -> Vec<String> {
    complete_server_file_name(arg, param)
        .iter()
        .map(|v| pbs_tools::format::strip_server_file_extension(v).to_owned())
        .collect()
}

pub fn complete_pxar_archive_name(arg: &str, param: &HashMap<String, String>) -> Vec<String> {
    complete_server_file_name(arg, param)
        .iter()
        .filter_map(|name| {
            if has_pxar_filename_extension(name, true) {
                Some(pbs_tools::format::strip_server_file_extension(name).to_owned())
            } else {
                None
            }
        })
        .collect()
}

pub fn complete_img_archive_name(arg: &str, param: &HashMap<String, String>) -> Vec<String> {
    complete_server_file_name(arg, param)
        .iter()
        .filter_map(|name| {
            if name.ends_with(".img.fidx") {
                Some(pbs_tools::format::strip_server_file_extension(name).to_owned())
            } else {
                None
            }
        })
        .collect()
}

pub fn complete_chunk_size(_arg: &str, _param: &HashMap<String, String>) -> Vec<String> {
    let mut result = vec![];

    let mut size = 64;
    loop {
        result.push(size.to_string());
        size *= 2;
        if size > 4096 {
            break;
        }
    }

    result
}

pub fn complete_auth_id(_arg: &str, param: &HashMap<String, String>) -> Vec<String> {
    proxmox_async::runtime::main(async { complete_auth_id_do(param).await })
}

pub async fn complete_auth_id_do(param: &HashMap<String, String>) -> Vec<String> {
    let mut result = vec![];

    let repo = match extract_repository_from_map(param) {
        Some(v) => v,
        _ => return result,
    };

    let data = try_get(&repo, "api2/json/access/users?include_tokens=true").await;

    if let Ok(parsed) = serde_json::from_value::<Vec<UserWithTokens>>(data) {
        for user in parsed {
            result.push(user.userid.to_string());
            for token in user.tokens {
                result.push(token.tokenid.to_string());
            }
        }
    };

    result
}

pub fn complete_repository(_arg: &str, _param: &HashMap<String, String>) -> Vec<String> {
    let mut result = vec![];

    let base = match BaseDirectories::with_prefix("proxmox-backup") {
        Ok(v) => v,
        _ => return result,
    };

    // usually $HOME/.cache/proxmox-backup/repo-list
    let path = match base.place_cache_file("repo-list") {
        Ok(v) => v,
        _ => return result,
    };

    let data = file_get_json(path, None).unwrap_or_else(|_| json!({}));

    if let Some(map) = data.as_object() {
        for (repo, _count) in map {
            result.push(repo.to_owned());
        }
    }

    result
}

pub fn complete_backup_source(arg: &str, param: &HashMap<String, String>) -> Vec<String> {
    let mut result = vec![];

    let data: Vec<&str> = arg.splitn(2, ':').collect();

    if data.len() != 2 {
        result.push(String::from("root.pxar:/"));
        result.push(String::from("etc.pxar:/etc"));
        return result;
    }

    let files = complete_file_name(data[1], param);

    for file in files {
        result.push(format!("{}:{}", data[0], file));
    }

    result
}

pub fn complete_namespace(arg: &str, param: &HashMap<String, String>) -> Vec<String> {
    // the prefix includes the parent since we get the full namespace as API results
    let prefix = arg;
    let parent = match arg.rfind('/') {
        // we're at a slash, so use the full namespace as a parent, no filter
        Some(len) if len == arg.len() => &arg[..(len - 1)],
        // there was a slash in the namespace, pop off the final component, use the
        // remainder as a filter:
        Some(len) => &arg[..len],
        // no slashes, search root namespace
        None => "",
    };

    let parent: BackupNamespace = match parent.parse() {
        Ok(ns) => ns,
        Err(_) => return Vec::new(),
    };

    proxmox_async::runtime::main(complete_namespace_do(parent, prefix, param))
}

pub async fn complete_namespace_do(
    parent: BackupNamespace,
    prefix: &str,
    param: &HashMap<String, String>,
) -> Vec<String> {
    let repo = match extract_repository_from_map(param) {
        Some(v) => v,
        _ => return Vec::new(),
    };

    let mut param = json!({ "max-depth": 2 });
    if !parent.is_root() {
        param["parent"] = match serde_json::to_value(parent) {
            Ok(p) => p,
            Err(_) => return Vec::new(),
        };
    }
    let query = json_object_to_query(param).unwrap();
    let path = format!(
        "api2/json/admin/datastore/{}/namespace?{query}",
        repo.store()
    );

    let mut result = Vec::new();
    let data = try_get(&repo, &path).await;
    if let Value::Array(array) = data {
        for mut item in array {
            match item["ns"].take() {
                Value::String(s) if s.starts_with(prefix) => result.push(s),
                _ => (),
            }
        }
    }
    result
}

pub fn base_directories() -> Result<xdg::BaseDirectories, Error> {
    xdg::BaseDirectories::with_prefix("proxmox-backup").map_err(Error::from)
}

/// Convenience helper for better error messages:
pub fn find_xdg_file(
    file_name: impl AsRef<std::path::Path>,
    description: &'static str,
) -> Result<Option<std::path::PathBuf>, Error> {
    let file_name = file_name.as_ref();
    base_directories()
        .map(|base| base.find_config_file(file_name))
        .with_context(|| format!("error searching for {}", description))
}

pub fn place_xdg_file(
    file_name: impl AsRef<std::path::Path>,
    description: &'static str,
) -> Result<std::path::PathBuf, Error> {
    let file_name = file_name.as_ref();
    base_directories()
        .and_then(|base| base.place_config_file(file_name).map_err(Error::from))
        .with_context(|| format!("failed to place {} in xdg home", description))
}

pub fn get_pxar_archive_names(
    archive_name: &str,
    manifest: &BackupManifest,
) -> Result<(String, Option<String>), Error> {
    let (filename, ext) = match archive_name.strip_suffix(".didx") {
        Some(filename) => (filename, ".didx"),
        None => (archive_name, ""),
    };

    // Check if archive with given extension is present
    if manifest
        .files()
        .iter()
        .any(|fileinfo| fileinfo.filename == format!("{filename}.didx"))
    {
        // check if already given as one of split archive name variants
        if let Some(base) = filename
            .strip_suffix(".mpxar")
            .or_else(|| filename.strip_suffix(".ppxar"))
        {
            return Ok((
                format!("{base}.mpxar{ext}"),
                Some(format!("{base}.ppxar{ext}")),
            ));
        }
        return Ok((archive_name.to_owned(), None));
    }

    // if not, try fallback from regular to split archive
    if let Some(base) = filename.strip_suffix(".pxar") {
        return get_pxar_archive_names(&format!("{base}.mpxar{ext}"), manifest);
    }

    bail!("archive not found in manifest");
}

/// Check if the given filename has a valid pxar filename extension variant
///
/// If `with_didx_extension` is `true`, check the additional `.didx` ending.
pub fn has_pxar_filename_extension(name: &str, with_didx_extension: bool) -> bool {
    if with_didx_extension {
        name.ends_with(".pxar.didx")
            || name.ends_with(".mpxar.didx")
            || name.ends_with(".ppxar.didx")
    } else {
        name.ends_with(".pxar") || name.ends_with(".mpxar") || name.ends_with(".ppxar")
    }
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

/// Raise the soft limit for open file handles to the hard limit
///
/// Returns the values set before raising the limit as libc::rlimit64
pub fn raise_nofile_limit() -> Result<libc::rlimit64, Error> {
    let mut old = libc::rlimit64 {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if 0 != unsafe { libc::getrlimit64(libc::RLIMIT_NOFILE, &mut old as *mut libc::rlimit64) } {
        bail!("Failed to get nofile rlimit");
    }

    let mut new = libc::rlimit64 {
        rlim_cur: old.rlim_max,
        rlim_max: old.rlim_max,
    };
    if 0 != unsafe { libc::setrlimit64(libc::RLIMIT_NOFILE, &mut new as *mut libc::rlimit64) } {
        bail!("Failed to set nofile rlimit");
    }

    Ok(old)
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

            let entry_attr = match entry.kind() {
                EntryKind::Version(_) | EntryKind::Prelude(_) | EntryKind::GoodbyeTable => continue,
                EntryKind::Directory => DirEntryAttribute::Directory {
                    start: entry.entry_range_info().entry_range.start,
                },
                EntryKind::File { size, .. } => {
                    let mtime = match entry.metadata().mtime_as_duration() {
                        SignedDuration::Positive(val) => i64::try_from(val.as_secs())?,
                        SignedDuration::Negative(val) => -i64::try_from(val.as_secs())?,
                    };
                    DirEntryAttribute::File { size: *size, mtime }
                }
                EntryKind::Device(_) => match entry.metadata().file_type() {
                    mode::IFBLK => DirEntryAttribute::BlockDevice,
                    mode::IFCHR => DirEntryAttribute::CharDevice,
                    _ => bail!("encountered unknown device type"),
                },
                EntryKind::Symlink(_) => DirEntryAttribute::Symlink,
                EntryKind::Hardlink(_) => DirEntryAttribute::Hardlink,
                EntryKind::Fifo => DirEntryAttribute::Fifo,
                EntryKind::Socket => DirEntryAttribute::Socket,
            };

            let entry_path = if let Some(prefix) = path_prefix {
                let mut entry_path = PathBuf::from(prefix);
                match entry.path().strip_prefix("/") {
                    Ok(path) => entry_path.push(path),
                    Err(_) => entry_path.push(entry.path()),
                }
                entry_path
            } else {
                PathBuf::from(entry.path())
            };
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

/// Creates a temporary file (with `O_TMPFILE`) in `XDG_CACHE_HOME`. If we
/// cannot create the file there it will be created in `/tmp` instead.
pub fn create_tmp_file() -> std::io::Result<std::fs::File> {
    static TMP_PATH: OnceLock<std::path::PathBuf> = OnceLock::new();
    let tmp_path = TMP_PATH.get_or_init(|| {
        xdg::BaseDirectories::new()
            .map(|base| base.get_cache_home())
            .unwrap_or_else(|_| std::path::PathBuf::from("/tmp"))
    });

    let mut open_opts_binding = std::fs::OpenOptions::new();
    let builder = open_opts_binding
        .write(true)
        .read(true)
        .custom_flags(libc::O_TMPFILE);
    builder.open(tmp_path).or_else(|err| {
        if tmp_path != std::path::Path::new("/tmp") {
            builder.open("/tmp")
        } else {
            Err(err)
        }
    })
}
