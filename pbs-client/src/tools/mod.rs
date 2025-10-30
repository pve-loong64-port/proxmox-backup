//! Shared tools useful for common CLI clients.
use std::collections::HashMap;
use std::env::VarError::{NotPresent, NotUnicode};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::FromRawFd;
use std::process::Command;
use std::sync::OnceLock;

use anyhow::{bail, format_err, Context, Error};
use serde_json::{json, Value};
use xdg::BaseDirectories;

use proxmox_http::uri::json_object_to_query;
use proxmox_router::cli::{complete_file_name, shellword_split};
use proxmox_schema::*;
use proxmox_sys::fs::file_get_json;

use pbs_api_types::{
    Authid, BackupArchiveName, BackupNamespace, RateLimitConfig, UserWithTokens, BACKUP_REPO_URL,
};
use pbs_datastore::BackupManifest;

use crate::{BackupRepository, HttpClient, HttpClientOptions};

pub mod key_source;

const ENV_VAR_PBS_FINGERPRINT: &str = "PBS_FINGERPRINT";
const ENV_VAR_PBS_PASSWORD: &str = "PBS_PASSWORD";
const ENV_VAR_PBS_ENCRYPTION_PASSWORD: &str = "PBS_ENCRYPTION_PASSWORD";
const ENV_VAR_PBS_REPOSITORY: &str = "PBS_REPOSITORY";

/// Directory with system [credential]s. See systemd-creds(1).
///
/// [credential]: https://systemd.io/CREDENTIALS/
const ENV_VAR_CREDENTIALS_DIRECTORY: &str = "CREDENTIALS_DIRECTORY";
/// Credential name of the encryption password.
const CRED_PBS_ENCRYPTION_PASSWORD: &str = "proxmox-backup-client.encryption-password";
/// Credential name of the the password.
const CRED_PBS_PASSWORD: &str = "proxmox-backup-client.password";
/// Credential name of the the repository.
const CRED_PBS_REPOSITORY: &str = "proxmox-backup-client.repository";
/// Credential name of the the fingerprint.
const CRED_PBS_FINGERPRINT: &str = "proxmox-backup-client.fingerprint";

pub const REPO_URL_SCHEMA: Schema = StringSchema::new("Repository URL.")
    .format(&BACKUP_REPO_URL)
    .max_length(256)
    .schema();

pub const CHUNK_SIZE_SCHEMA: Schema = IntegerSchema::new("Chunk size in KB. Must be a power of 2.")
    .minimum(64)
    .maximum(4096)
    .default(4096)
    .schema();

/// Retrieves a secret stored in a [credential] provided by systemd.
///
/// Returns `Ok(None)` if the credential does not exist.
///
/// [credential]: https://systemd.io/CREDENTIALS/
fn get_credential(cred_name: &str) -> std::io::Result<Option<Vec<u8>>> {
    let Some(creds_dir) = std::env::var_os(ENV_VAR_CREDENTIALS_DIRECTORY) else {
        return Ok(None);
    };
    let path = std::path::Path::new(&creds_dir).join(cred_name);

    proxmox_log::debug!("attempting to use credential {cred_name} from {creds_dir:?}",);
    // We read the whole contents without a BufRead. As per systemd-creds(1):
    // Credentials are limited-size binary or textual objects.
    match std::fs::read(&path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            proxmox_log::debug!("no {cred_name} credential found in {creds_dir:?}");
            Ok(None)
        }
        Err(err) => Err(err),
    }
}

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
fn get_secret_from_env(base_name: &str) -> Result<Option<String>, Error> {
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

    let env_name = format!("{base_name}_FD");
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

    let env_name = format!("{base_name}_FILE");
    match std::env::var(&env_name) {
        Ok(filename) => {
            let mut file = std::fs::File::open(filename)
                .map_err(|err| format_err!("unable to open file in ENV({}): {}", env_name, err))?;
            return Ok(Some(firstline_file(&mut file)?));
        }
        Err(NotUnicode(_)) => bail!(format!("{} contains bad characters", env_name)),
        Err(NotPresent) => {}
    }

    let env_name = format!("{base_name}_CMD");
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

/// Gets a secret or value from the environment.
///
/// Checks for an environment variable named `env_variable`, and if missing, it
/// checks for a system [credential] named `credential_name`. Assumes the secret
/// is UTF-8 encoded.
///
/// [credential]: https://systemd.io/CREDENTIALS/
fn get_secret_impl(env_variable: &str, credential_name: &str) -> Result<Option<String>, Error> {
    if let Some(password) = get_secret_from_env(env_variable)? {
        Ok(Some(password))
    } else if let Some(password) = get_credential(credential_name)? {
        String::from_utf8(password)
            .map(Option::Some)
            .map_err(|_err| format_err!("credential {credential_name} is not utf8 encoded"))
    } else {
        Ok(None)
    }
}

/// Gets the backup server's password.
///
/// Looks for a password in the `PBS_PASSWORD` environment variable, if there
/// isn't one it reads the `proxmox-backup-client.password` [credential].
///
/// Returns `Ok(None)` if neither the environment variable or credentials are
/// present.
///
/// [credential]: https://systemd.io/CREDENTIALS/
pub fn get_password() -> Result<Option<String>, Error> {
    get_secret_impl(ENV_VAR_PBS_PASSWORD, CRED_PBS_PASSWORD)
}

/// Gets an encryption password.
///
///
/// Looks for a password in the `PBS_ENCRYPTION_PASSWORD` environment variable,
/// if there isn't one it reads the `proxmox-backup-client.encryption-password`
/// [credential].
///
/// Returns `Ok(None)` if neither the environment variable or credentials are
/// present.
///
/// [credential]: https://systemd.io/CREDENTIALS/
pub fn get_encryption_password() -> Result<Option<String>, Error> {
    get_secret_impl(
        ENV_VAR_PBS_ENCRYPTION_PASSWORD,
        CRED_PBS_ENCRYPTION_PASSWORD,
    )
}

pub fn get_default_repository() -> Option<String> {
    get_secret_impl(ENV_VAR_PBS_REPOSITORY, CRED_PBS_REPOSITORY)
        .inspect_err(|err| {
            proxmox_log::error!("could not read default repository: {err:#}");
        })
        .unwrap_or_default()
}

/// Gets the repository fingerprint.
///
/// Looks for the fingerprint in the `PBS_FINGERPRINT` environment variable, if
/// there isn't one it reads the `proxmox-backup-client.fingerprint`
/// [credential].
///
/// Returns `None` if neither the environment variable or the credential are
/// present.
///
/// [credential]: https://systemd.io/CREDENTIALS/
pub fn get_fingerprint() -> Option<String> {
    get_secret_impl(ENV_VAR_PBS_FINGERPRINT, CRED_PBS_FINGERPRINT)
        .inspect_err(|err| {
            proxmox_log::error!("could not read fingerprint: {err:#}");
        })
        .unwrap_or_default()
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
    let fingerprint = get_fingerprint();

    let password = get_password()?;
    let options = HttpClientOptions::new_interactive(password, fingerprint).rate_limit(rate_limit);

    HttpClient::new(server, port, auth_id, options)
}

/// like get, but simply ignore errors and return Null instead
pub async fn try_get(repo: &BackupRepository, url: &str) -> Value {
    let fingerprint = get_fingerprint();
    let password = get_password().unwrap_or(None);

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
                result.push(format!("{backup_type}/{backup_id}"));
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
            result.push(format!("{group}/"));
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
        .with_context(|| format!("error searching for {description}"))
}

pub fn place_xdg_file(
    file_name: impl AsRef<std::path::Path>,
    description: &'static str,
) -> Result<std::path::PathBuf, Error> {
    let file_name = file_name.as_ref();
    base_directories()
        .and_then(|base| base.place_config_file(file_name).map_err(Error::from))
        .with_context(|| format!("failed to place {description} in xdg home"))
}

pub fn get_pxar_archive_names(
    archive_name: &BackupArchiveName,
    manifest: &BackupManifest,
) -> Result<(BackupArchiveName, Option<BackupArchiveName>), Error> {
    let filename = archive_name.without_type_extension();
    let ext = archive_name.archive_type().extension();

    // Check if archive is given as split archive or regular archive and is present in manifest,
    // otherwise goto fallback below
    if manifest
        .files()
        .iter()
        .any(|fileinfo| fileinfo.filename == archive_name.as_ref())
    {
        // check if already given as one of split archive name variants
        if let Some(base) = filename
            .strip_suffix(".mpxar")
            .or_else(|| filename.strip_suffix(".ppxar"))
        {
            return Ok((
                format!("{base}.mpxar.{ext}").as_str().try_into()?,
                Some(format!("{base}.ppxar.{ext}").as_str().try_into()?),
            ));
        }
        return Ok((archive_name.to_owned(), None));
    }

    // if not, try fallback from regular to split archive
    if let Some(base) = filename.strip_suffix(".pxar") {
        return get_pxar_archive_names(
            &format!("{base}.mpxar.{ext}").as_str().try_into()?,
            manifest,
        );
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
