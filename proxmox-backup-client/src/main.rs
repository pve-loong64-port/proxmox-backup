use std::collections::HashSet;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Context;

use anyhow::{bail, format_err, Error};
use futures::stream::{StreamExt, TryStreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use xdg::BaseDirectories;

use pathpatterns::{MatchEntry, MatchType, PatternFlag};
use proxmox_async::blocking::TokioWriterAdapter;
use proxmox_io::StdChannelWriter;
use proxmox_router::{cli::*, ApiMethod, RpcEnvironment};
use proxmox_schema::api;
use proxmox_sys::fs::{file_get_json, image_size, replace_file, CreateOptions};
use proxmox_time::{epoch_i64, strftime_local};
use pxar::accessor::aio::Accessor;
use pxar::accessor::{MaybeReady, ReadAt, ReadAtOperation};

use pbs_api_types::{
    ArchiveType, Authid, BackupArchiveName, BackupDir, BackupGroup, BackupNamespace, BackupPart,
    BackupType, ClientRateLimitConfig, CryptMode, Fingerprint, GroupListItem, PathPattern,
    PruneJobOptions, PruneListItem, RateLimitConfig, SnapshotListItem, StorageStatus,
    BACKUP_ID_SCHEMA, BACKUP_NAMESPACE_SCHEMA, BACKUP_TIME_SCHEMA, BACKUP_TYPE_SCHEMA,
    CATALOG_NAME, ENCRYPTED_KEY_BLOB_NAME, MANIFEST_BLOB_NAME,
};
use pbs_client::catalog_shell::Shell;
use pbs_client::pxar::{ErrorHandler as PxarErrorHandler, MetadataArchiveReader, PxarPrevRef};
use pbs_client::tools::{
    complete_archive_name, complete_auth_id, complete_backup_group, complete_backup_snapshot,
    complete_backup_source, complete_chunk_size, complete_group_or_snapshot,
    complete_img_archive_name, complete_namespace, complete_pxar_archive_name, complete_repository,
    connect, connect_rate_limited, extract_repository_from_value,
    key_source::{
        crypto_parameters, format_key_source, get_encryption_key_password, KEYFD_SCHEMA,
        KEYFILE_SCHEMA, MASTER_PUBKEY_FD_SCHEMA, MASTER_PUBKEY_FILE_SCHEMA,
    },
    raise_nofile_limit, CHUNK_SIZE_SCHEMA, REPO_URL_SCHEMA,
};
use pbs_client::{
    delete_ticket_info, parse_backup_specification, view_task_result, BackupDetectionMode,
    BackupReader, BackupRepository, BackupSpecificationType, BackupStats, BackupWriter,
    BackupWriterOptions, ChunkStream, FixedChunkStream, HttpClient, InjectionData,
    PxarBackupStream, RemoteChunkReader, UploadOptions, BACKUP_SOURCE_SCHEMA,
};
use pbs_datastore::catalog::{BackupCatalogWriter, CatalogReader, CatalogWriter};
use pbs_datastore::chunk_store::verify_chunk_size;
use pbs_datastore::dynamic_index::{BufferedDynamicReader, DynamicIndexReader, LocalDynamicReadAt};
use pbs_datastore::fixed_index::FixedIndexReader;
use pbs_datastore::index::IndexFile;
use pbs_datastore::manifest::BackupManifest;
use pbs_datastore::read_chunk::AsyncReadChunk;
use pbs_key_config::{decrypt_key, rsa_encrypt_key_config, KeyConfig};
use pbs_tools::crypt_config::CryptConfig;
use pbs_tools::json;

pub mod key;
pub mod namespace;

mod benchmark;
pub use benchmark::*;

mod catalog;
pub use catalog::*;

mod group;
pub use group::*;

mod helper;
pub(crate) use helper::*;

mod mount;
pub use mount::*;

mod snapshot;
pub use snapshot::*;

mod task;
pub use task::*;

fn record_repository(repo: &BackupRepository) {
    let base = match BaseDirectories::with_prefix("proxmox-backup") {
        Ok(v) => v,
        _ => return,
    };

    // usually $HOME/.cache/proxmox-backup/repo-list
    let path = match base.place_cache_file("repo-list") {
        Ok(v) => v,
        _ => return,
    };

    let mut data = file_get_json(&path, None).unwrap_or_else(|_| json!({}));

    let repo = repo.to_string();

    data[&repo] = json! { data[&repo].as_i64().unwrap_or(0) + 1 };

    let mut map = serde_json::map::Map::new();

    loop {
        let mut max_used = 0;
        let mut max_repo = None;
        for (repo, count) in data.as_object().unwrap() {
            if map.contains_key(repo) {
                continue;
            }
            if let Some(count) = count.as_i64() {
                if count > max_used {
                    max_used = count;
                    max_repo = Some(repo);
                }
            }
        }
        if let Some(repo) = max_repo {
            map.insert(repo.to_owned(), json!(max_used));
        } else {
            break;
        }
        if map.len() > 10 {
            // store max. 10 repos
            break;
        }
    }

    let new_data = json!(map);

    let _ = replace_file(
        path,
        new_data.to_string().as_bytes(),
        CreateOptions::new(),
        false,
    );
}

async fn api_datastore_list_snapshots(
    client: &HttpClient,
    store: &str,
    ns: &BackupNamespace,
    group: Option<&BackupGroup>,
) -> Result<Value, Error> {
    let path = format!("api2/json/admin/datastore/{}/snapshots", store);

    let mut args = match group {
        Some(group) => serde_json::to_value(group)?,
        None => json!({}),
    };
    if !ns.is_root() {
        args["ns"] = serde_json::to_value(ns)?;
    }

    let mut result = client.get(&path, Some(args)).await?;

    Ok(result["data"].take())
}

pub async fn api_datastore_latest_snapshot(
    client: &HttpClient,
    store: &str,
    ns: &BackupNamespace,
    group: BackupGroup,
) -> Result<BackupDir, Error> {
    let list = api_datastore_list_snapshots(client, store, ns, Some(&group)).await?;
    let mut list: Vec<SnapshotListItem> = serde_json::from_value(list)?;

    if list.is_empty() {
        bail!("backup group {} does not contain any snapshots.", group);
    }

    list.sort_unstable_by(|a, b| b.backup.time.cmp(&a.backup.time));

    Ok((group, list[0].backup.time).into())
}

pub async fn dir_or_last_from_group(
    client: &HttpClient,
    repo: &BackupRepository,
    ns: &BackupNamespace,
    path: &str,
) -> Result<BackupDir, Error> {
    match path.parse::<BackupPart>()? {
        BackupPart::Dir(dir) => Ok(dir),
        BackupPart::Group(group) => {
            api_datastore_latest_snapshot(client, repo.store(), ns, group).await
        }
    }
}

type Catalog = CatalogWriter<TokioWriterAdapter<StdChannelWriter<Error>>>;

#[allow(clippy::too_many_arguments)]
async fn backup_directory<P: AsRef<Path>>(
    client: &BackupWriter,
    dir_path: P,
    archive_name: &BackupArchiveName,
    payload_target: Option<&BackupArchiveName>,
    chunk_size: Option<usize>,
    catalog: Option<Arc<Mutex<Catalog>>>,
    pxar_create_options: pbs_client::pxar::PxarCreateOptions,
    upload_options: UploadOptions,
) -> Result<(BackupStats, Option<BackupStats>), Error> {
    if upload_options.fixed_size.is_some() {
        bail!("cannot backup directory with fixed chunk size!");
    }

    let (payload_boundaries_tx, payload_boundaries_rx) = std::sync::mpsc::channel();
    let (pxar_stream, payload_stream) = PxarBackupStream::open(
        dir_path.as_ref(),
        catalog,
        pxar_create_options,
        Some(payload_boundaries_tx),
        payload_target.is_some(),
    )?;

    let mut chunk_stream = ChunkStream::new(pxar_stream, chunk_size, None, None);
    let (tx, rx) = mpsc::channel(10); // allow to buffer 10 chunks

    let stream = ReceiverStream::new(rx).map_err(Error::from);

    // spawn chunker inside a separate task so that it can run parallel
    tokio::spawn(async move {
        while let Some(v) = chunk_stream.next().await {
            let _ = tx.send(v).await;
        }
    });

    let stats = client.upload_stream(archive_name, stream, upload_options.clone(), None);

    if let Some(mut payload_stream) = payload_stream {
        let payload_target = payload_target
            .ok_or_else(|| format_err!("got payload stream, but no target archive name"))?;

        let (payload_injections_tx, payload_injections_rx) = std::sync::mpsc::channel();
        let injection_data = InjectionData::new(payload_boundaries_rx, payload_injections_tx);
        let suggested_boundaries = payload_stream.suggested_boundaries.take();
        let mut payload_chunk_stream = ChunkStream::new(
            payload_stream,
            chunk_size,
            Some(injection_data),
            suggested_boundaries,
        );
        let (payload_tx, payload_rx) = mpsc::channel(10); // allow to buffer 10 chunks
        let stream = ReceiverStream::new(payload_rx).map_err(Error::from);

        // spawn payload chunker inside a separate task so that it can run parallel
        tokio::spawn(async move {
            while let Some(v) = payload_chunk_stream.next().await {
                let _ = payload_tx.send(v).await;
            }
        });

        let payload_stats = client.upload_stream(
            payload_target,
            stream,
            upload_options,
            Some(payload_injections_rx),
        );

        match futures::join!(stats, payload_stats) {
            (Ok(stats), Ok(payload_stats)) => Ok((stats, Some(payload_stats))),
            (Err(err), Ok(_)) => Err(format_err!("upload failed: {err}")),
            (Ok(_), Err(err)) => Err(format_err!("upload failed: {err}")),
            (Err(err), Err(payload_err)) => {
                Err(format_err!("upload failed: {err} - {payload_err}"))
            }
        }
    } else {
        Ok((stats.await?, None))
    }
}

async fn backup_image<P: AsRef<Path>>(
    client: &BackupWriter,
    image_path: P,
    archive_name: &BackupArchiveName,
    chunk_size: Option<usize>,
    upload_options: UploadOptions,
) -> Result<BackupStats, Error> {
    let path = image_path.as_ref().to_owned();

    let file = tokio::fs::File::open(path).await?;

    let stream = tokio_util::codec::FramedRead::with_capacity(
        file,
        tokio_util::codec::BytesCodec::new(),
        4 * 1024 * 1024,
    )
    .map_err(Error::from);

    let stream = FixedChunkStream::new(stream, chunk_size.unwrap_or(4 * 1024 * 1024));

    if upload_options.fixed_size.is_none() {
        bail!("cannot backup image with dynamic chunk size!");
    }

    let stats = client
        .upload_stream(archive_name, stream, upload_options, None)
        .await?;

    Ok(stats)
}

pub fn optional_ns_param(param: &Value) -> Result<BackupNamespace, Error> {
    Ok(match param.get("ns") {
        Some(Value::String(ns)) => ns.parse()?,
        Some(_) => bail!("invalid namespace parameter"),
        None => BackupNamespace::root(),
    })
}

#[api(
   input: {
        properties: {
            repository: {
                schema: REPO_URL_SCHEMA,
                optional: true,
            },
            "ns": {
                type: BackupNamespace,
                optional: true,
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        }
   }
)]
/// List backup groups.
async fn list_backup_groups(param: Value) -> Result<Value, Error> {
    let output_format = get_output_format(&param);

    let repo = extract_repository_from_value(&param)?;

    let client = connect(&repo)?;

    let path = format!("api2/json/admin/datastore/{}/groups", repo.store());

    let backup_ns = optional_ns_param(&param)?;
    let mut result = client
        .get(
            &path,
            match backup_ns.is_root() {
                true => None,
                false => Some(json!({ "ns": backup_ns })),
            },
        )
        .await?;

    record_repository(&repo);

    let render_group_path = |_v: &Value, record: &Value| -> Result<String, Error> {
        let item = GroupListItem::deserialize(record)?;
        Ok(item.backup.to_string())
    };

    let render_last_backup = |_v: &Value, record: &Value| -> Result<String, Error> {
        let item = GroupListItem::deserialize(record)?;
        let snapshot = BackupDir {
            group: item.backup,
            time: item.last_backup,
        };
        Ok(snapshot.to_string())
    };

    let render_files = |_v: &Value, record: &Value| -> Result<String, Error> {
        let item = GroupListItem::deserialize(record)?;
        Ok(pbs_tools::format::render_backup_file_list(&item.files))
    };

    let options = default_table_format_options()
        .sortby("backup-type", false)
        .sortby("backup-id", false)
        .column(
            ColumnConfig::new("backup-id")
                .renderer(render_group_path)
                .header("group"),
        )
        .column(
            ColumnConfig::new("last-backup")
                .renderer(render_last_backup)
                .header("last snapshot")
                .right_align(false),
        )
        .column(ColumnConfig::new("backup-count"))
        .column(ColumnConfig::new("files").renderer(render_files));

    let mut data: Value = result["data"].take();

    let return_type = &pbs_api_types::ADMIN_DATASTORE_LIST_GROUPS_RETURN_TYPE;

    format_and_print_result_full(&mut data, return_type, &output_format, &options);

    Ok(Value::Null)
}

fn merge_group_into(to: &mut serde_json::Map<String, Value>, group: BackupGroup) {
    match serde_json::to_value(group).unwrap() {
        Value::Object(group) => to.extend(group),
        _ => unreachable!(),
    }
}

#[api(
   input: {
        properties: {
            repository: {
                schema: REPO_URL_SCHEMA,
                optional: true,
            },
            group: {
                type: String,
                description: "Backup group.",
            },
            "ns": {
                type: BackupNamespace,
                optional: true,
            },
            "new-owner": {
                type: Authid,
            },
        }
   }
)]
/// Change owner of a backup group
async fn change_backup_owner(group: String, mut param: Value) -> Result<(), Error> {
    let repo = extract_repository_from_value(&param)?;
    let ns = optional_ns_param(&param)?;

    let client = connect(&repo)?;

    param.as_object_mut().unwrap().remove("repository");

    let group: BackupGroup = group.parse()?;

    merge_group_into(param.as_object_mut().unwrap(), group);
    if !ns.is_root() {
        param["ns"] = serde_json::to_value(ns)?;
    }

    let path = format!("api2/json/admin/datastore/{}/change-owner", repo.store());
    client.post(&path, Some(param)).await?;

    record_repository(&repo);

    Ok(())
}

#[api(
   input: {
        properties: {
            repository: {
                schema: REPO_URL_SCHEMA,
                optional: true,
            },
        }
   }
)]
/// Try to login. If successful, store ticket.
async fn api_login(param: Value) -> Result<Value, Error> {
    let repo = extract_repository_from_value(&param)?;

    let client = connect(&repo)?;
    client.login().await?;

    record_repository(&repo);

    Ok(Value::Null)
}

#[api(
   input: {
        properties: {
            repository: {
                schema: REPO_URL_SCHEMA,
                optional: true,
            },
        }
   }
)]
/// Logout (delete stored ticket).
fn api_logout(param: Value) -> Result<Value, Error> {
    let repo = extract_repository_from_value(&param)?;

    delete_ticket_info("proxmox-backup", repo.host(), repo.user())?;

    Ok(Value::Null)
}

#[api(
   input: {
        properties: {
            repository: {
                schema: REPO_URL_SCHEMA,
                optional: true,
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        }
   }
)]
/// Show client and optional server version
async fn api_version(param: Value) -> Result<(), Error> {
    let output_format = get_output_format(&param);

    let mut version_info = json!({
        "client": {
            "version": pbs_buildcfg::PROXMOX_PKG_VERSION,
            "release": pbs_buildcfg::PROXMOX_PKG_RELEASE,
            "repoid": pbs_buildcfg::PROXMOX_PKG_REPOID,
        }
    });

    let repo = extract_repository_from_value(&param);
    if let Ok(repo) = repo {
        let client = connect(&repo)?;

        match client.get("api2/json/version", None).await {
            Ok(mut result) => version_info["server"] = result["data"].take(),
            Err(e) => log::error!("could not connect to server - {}", e),
        }
    }
    if output_format == "text" {
        println!(
            "client version: {}.{}",
            pbs_buildcfg::PROXMOX_PKG_VERSION,
            pbs_buildcfg::PROXMOX_PKG_RELEASE,
        );
        if let Some(server) = version_info["server"].as_object() {
            let server_version = server["version"].as_str().unwrap();
            let server_release = server["release"].as_str().unwrap();
            println!("server version: {}.{}", server_version, server_release);
        }
    } else {
        format_and_print_result(&version_info, &output_format);
    }

    Ok(())
}

#[api(
    input: {
        properties: {
            repository: {
                schema: REPO_URL_SCHEMA,
                optional: true,
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        },
    },
)]
/// Start garbage collection for a specific repository.
async fn start_garbage_collection(param: Value) -> Result<Value, Error> {
    let repo = extract_repository_from_value(&param)?;

    let output_format = get_output_format(&param);

    let client = connect(&repo)?;

    let path = format!("api2/json/admin/datastore/{}/gc", repo.store());

    let result = client.post(&path, None).await?;

    record_repository(&repo);

    view_task_result(&client, result, &output_format).await?;

    Ok(Value::Null)
}

struct CatalogUploadResult {
    catalog_writer: Arc<Mutex<CatalogWriter<TokioWriterAdapter<StdChannelWriter<Error>>>>>,
    result: tokio::sync::oneshot::Receiver<Result<BackupStats, Error>>,
}

fn spawn_catalog_upload(
    client: Arc<BackupWriter>,
    encrypt: bool,
) -> Result<CatalogUploadResult, Error> {
    let (catalog_tx, catalog_rx) = std::sync::mpsc::sync_channel(10); // allow to buffer 10 writes
    let catalog_stream = proxmox_async::blocking::StdChannelStream(catalog_rx);
    let catalog_chunk_size = 512 * 1024;
    let catalog_chunk_stream =
        ChunkStream::new(catalog_stream, Some(catalog_chunk_size), None, None);

    let catalog_writer = Arc::new(Mutex::new(CatalogWriter::new(TokioWriterAdapter::new(
        StdChannelWriter::new(catalog_tx),
    ))?));

    let (catalog_result_tx, catalog_result_rx) = tokio::sync::oneshot::channel();

    let upload_options = UploadOptions {
        encrypt,
        compress: true,
        ..UploadOptions::default()
    };

    tokio::spawn(async move {
        let catalog_upload_result = client
            .upload_stream(&CATALOG_NAME, catalog_chunk_stream, upload_options, None)
            .await;

        if let Err(ref err) = catalog_upload_result {
            log::error!("catalog upload error - {}", err);
            client.cancel();
        }

        let _ = catalog_result_tx.send(catalog_upload_result);
    });

    Ok(CatalogUploadResult {
        catalog_writer,
        result: catalog_result_rx,
    })
}

#[api(
    input: {
        properties: {
            backupspec: {
                type: Array,
                description:
                    "List of backup source specifications:\
                    \n\n[<archive-name>.<type>:<source-path>] ...\n\n\
                    The 'archive-name' must only contain alphanumerics, hyphens and underscores \
                    while the 'type' must be either 'pxar', 'img', 'conf' or 'log'.",
                items: {
                    schema: BACKUP_SOURCE_SCHEMA,
                }
            },
            repository: {
                schema: REPO_URL_SCHEMA,
                optional: true,
            },
            "include-dev": {
                description:
                    "Include mountpoints with same st_dev number (see ``man fstat``) as specified \
                    files.",
                optional: true,
                items: {
                    type: String,
                    description: "Path to file.",
                }
            },
            "all-file-systems": {
                type: Boolean,
                description: "Include all mounted subdirectories.",
                optional: true,
                default: false,
            },
            keyfile: {
                schema: KEYFILE_SCHEMA,
                optional: true,
            },
            "keyfd": {
                schema: KEYFD_SCHEMA,
                optional: true,
            },
            "master-pubkey-file": {
                schema: MASTER_PUBKEY_FILE_SCHEMA,
                optional: true,
            },
            "master-pubkey-fd": {
                schema: MASTER_PUBKEY_FD_SCHEMA,
                optional: true,
            },
            "crypt-mode": {
                type: CryptMode,
                optional: true,
            },
            "skip-lost-and-found": {
                type: Boolean,
                description: "Skip lost+found directory.",
                optional: true,
                default: false,
            },
            "ns": {
                schema: BACKUP_NAMESPACE_SCHEMA,
                optional: true,
            },
            "backup-type": {
                schema: BACKUP_TYPE_SCHEMA,
                optional: true,
            },
            "backup-id": {
                schema: BACKUP_ID_SCHEMA,
                optional: true,
            },
            "backup-time": {
                schema: BACKUP_TIME_SCHEMA,
                optional: true,
            },
            "chunk-size": {
                schema: CHUNK_SIZE_SCHEMA,
                optional: true,
            },
            limit: {
                type: ClientRateLimitConfig,
                flatten: true,
            },
            "change-detection-mode": {
                type: BackupDetectionMode,
                optional: true,
            },
            "exclude": {
                type: Array,
                description: "List of paths or patterns for matching files to exclude.",
                optional: true,
                items: {
                    type: String,
                    description: "Path or match pattern.",
                 }
            },
            "entries-max": {
                type: Integer,
                description: "Max number of entries to hold in memory.",
                optional: true,
                default: pbs_client::pxar::ENCODER_MAX_ENTRIES as isize,
            },
            "dry-run": {
                type: Boolean,
                description: "Just show what backup would do, but do not upload anything.",
                optional: true,
                default: false,
            },
            "skip-e2big-xattr": {
                type: Boolean,
                description:
                    "Ignore the E2BIG error when retrieving xattrs. This includes the file, but \
                    discards the metadata.",
                optional: true,
                default: false,
            },
        }
    }
 )]
/// Create (host) backup.
#[allow(clippy::too_many_arguments)]
async fn create_backup(
    param: Value,
    all_file_systems: bool,
    skip_lost_and_found: bool,
    change_detection_mode: Option<BackupDetectionMode>,
    dry_run: bool,
    skip_e2big_xattr: bool,
    limit: ClientRateLimitConfig,
    _info: &ApiMethod,
    _rpcenv: &mut dyn RpcEnvironment,
) -> Result<Value, Error> {
    let repo = extract_repository_from_value(&param)?;

    let backupspec_list = json::required_array_param(&param, "backupspec")?;

    let backup_time_opt = param["backup-time"].as_i64();

    let chunk_size_opt = param["chunk-size"].as_u64().map(|v| (v * 1024) as usize);

    if let Some(size) = chunk_size_opt {
        verify_chunk_size(size)?;
    }

    let rate_limit = RateLimitConfig::from_client_config(limit);

    let crypto = crypto_parameters(&param)?;

    let backup_id = param["backup-id"]
        .as_str()
        .unwrap_or_else(|| proxmox_sys::nodename());

    let backup_ns = optional_ns_param(&param)?;

    let backup_type: BackupType = param["backup-type"].as_str().unwrap_or("host").parse()?;

    let include_dev = param["include-dev"].as_array();

    let entries_max = param["entries-max"]
        .as_u64()
        .unwrap_or(pbs_client::pxar::ENCODER_MAX_ENTRIES as u64);

    let empty = Vec::new();
    let exclude_args = param["exclude"].as_array().unwrap_or(&empty);

    let mut pattern_list = Vec::with_capacity(exclude_args.len());
    for entry in exclude_args {
        let entry = entry
            .as_str()
            .ok_or_else(|| format_err!("Invalid pattern string slice"))?;
        pattern_list.push(
            MatchEntry::parse_pattern(entry, PatternFlag::PATH_NAME, MatchType::Exclude)
                .map_err(|err| format_err!("invalid exclude pattern entry: {}", err))?,
        );
    }

    let mut devices = if all_file_systems {
        None
    } else {
        Some(HashSet::new())
    };

    if let Some(include_dev) = include_dev {
        if all_file_systems {
            bail!("option 'all-file-systems' conflicts with option 'include-dev'");
        }

        let mut set = HashSet::new();
        for path in include_dev {
            let path = path.as_str().unwrap();
            let stat = nix::sys::stat::stat(path)
                .map_err(|err| format_err!("fstat {:?} failed - {}", path, err))?;
            set.insert(stat.st_dev);
        }
        devices = Some(set);
    }

    let mut upload_list = vec![];
    let mut target_set = HashSet::new();

    for backupspec in backupspec_list {
        let pbs_client::BackupSpecification {
            archive_name: target,
            config_string: filename,
            spec_type,
        } = parse_backup_specification(backupspec.as_str().unwrap())?;

        if target_set.contains(&target) {
            bail!("got target twice: '{}'", target);
        }
        target_set.insert(target.clone());

        use std::os::unix::fs::FileTypeExt;

        let metadata = std::fs::metadata(&filename)
            .map_err(|err| format_err!("unable to access '{}' - {}", filename, err))?;
        let file_type = metadata.file_type();

        match spec_type {
            BackupSpecificationType::PXAR => {
                if !file_type.is_dir() {
                    bail!("got unexpected file type (expected directory)");
                }
                upload_list.push((BackupSpecificationType::PXAR, filename, target, "didx", 0));
            }
            BackupSpecificationType::IMAGE => {
                if !(file_type.is_file() || file_type.is_block_device()) {
                    bail!("got unexpected file type (expected file or block device)");
                }

                let size = image_size(&PathBuf::from(&filename))?;

                if size == 0 {
                    bail!("got zero-sized file '{}'", filename);
                }

                upload_list.push((
                    BackupSpecificationType::IMAGE,
                    filename,
                    target,
                    "fidx",
                    size,
                ));
            }
            BackupSpecificationType::CONFIG => {
                if !file_type.is_file() {
                    bail!("got unexpected file type (expected regular file)");
                }
                upload_list.push((
                    BackupSpecificationType::CONFIG,
                    filename,
                    target,
                    "blob",
                    metadata.len(),
                ));
            }
            BackupSpecificationType::LOGFILE => {
                if !file_type.is_file() {
                    bail!("got unexpected file type (expected regular file)");
                }
                upload_list.push((
                    BackupSpecificationType::LOGFILE,
                    filename,
                    target,
                    "blob",
                    metadata.len(),
                ));
            }
        }
    }

    let backup_time = backup_time_opt.unwrap_or_else(epoch_i64);

    let detection_mode = change_detection_mode.unwrap_or_default();

    let http_client = connect_rate_limited(&repo, rate_limit)?;
    record_repository(&repo);

    let snapshot = BackupDir::from((backup_type, backup_id.to_owned(), backup_time));
    if backup_ns.is_root() {
        log::info!("Starting backup: {snapshot}");
    } else {
        log::info!("Starting backup: [{backup_ns}]:{snapshot}");
    }

    log::info!("Client name: {}", proxmox_sys::nodename());

    let start_time = std::time::Instant::now();

    log::info!(
        "Starting backup protocol: {}",
        strftime_local("%c", epoch_i64())?
    );

    let (crypt_config, rsa_encrypted_key) = match crypto.enc_key {
        None => (None, None),
        Some(key_with_source) => {
            log::info!(
                "{}",
                format_key_source(&key_with_source.source, "encryption")
            );

            let (key, created, fingerprint) =
                decrypt_key(&key_with_source.key, &get_encryption_key_password)?;
            log::info!("Encryption key fingerprint: {}", fingerprint);

            let crypt_config = CryptConfig::new(key)?;

            match crypto.master_pubkey {
                Some(pem_with_source) => {
                    log::info!("{}", format_key_source(&pem_with_source.source, "master"));

                    let rsa = openssl::rsa::Rsa::public_key_from_pem(&pem_with_source.key)?;

                    let mut key_config = KeyConfig::without_password(key)?;
                    key_config.created = created; // keep original value

                    let enc_key = rsa_encrypt_key_config(rsa, &key_config)?;

                    (Some(Arc::new(crypt_config)), Some(enc_key))
                }
                _ => (Some(Arc::new(crypt_config)), None),
            }
        }
    };

    let client = BackupWriter::start(
        &http_client,
        BackupWriterOptions {
            datastore: repo.store(),
            ns: &backup_ns,
            backup: &snapshot,
            crypt_config: crypt_config.clone(),
            debug: true,
            benchmark: false,
        },
    )
    .await?;

    let download_previous_manifest = match client.previous_backup_time().await {
        Ok(Some(backup_time)) => {
            log::info!(
                "Downloading previous manifest ({})",
                strftime_local("%c", backup_time)?
            );
            true
        }
        Ok(None) => {
            log::info!("No previous manifest available.");
            false
        }
        Err(_) => {
            // Fallback for outdated server, TODO remove/bubble up with 2.0
            true
        }
    };

    let previous_manifest = if download_previous_manifest {
        match client.download_previous_manifest().await {
            Ok(previous_manifest) => {
                match previous_manifest.check_fingerprint(crypt_config.as_ref().map(Arc::as_ref)) {
                    Ok(()) => Some(Arc::new(previous_manifest)),
                    Err(err) => {
                        log::error!("Couldn't re-use previous manifest - {}", err);
                        None
                    }
                }
            }
            Err(err) => {
                log::error!("Couldn't download previous manifest - {}", err);
                None
            }
        }
    } else {
        None
    };

    let mut manifest = BackupManifest::new(snapshot.clone());

    let mut catalog = None;
    let mut catalog_result_rx = None;

    let log_file = |desc: &str, file: &str, target: &str| {
        let what = if dry_run { "Would upload" } else { "Upload" };
        log::info!("{} {} '{}' to '{}' as {}", what, desc, file, repo, target);
    };

    for (backup_type, filename, target_base, extension, size) in upload_list {
        let target: BackupArchiveName = format!("{target_base}.{extension}").as_str().try_into()?;
        match (backup_type, dry_run) {
            // dry-run
            (BackupSpecificationType::CONFIG, true) => {
                log_file("config file", &filename, target.as_ref())
            }
            (BackupSpecificationType::LOGFILE, true) => {
                log_file("log file", &filename, target.as_ref())
            }
            (BackupSpecificationType::PXAR, true) => {
                log_file("directory", &filename, target.as_ref())
            }
            (BackupSpecificationType::IMAGE, true) => log_file("image", &filename, target.as_ref()),
            // no dry-run
            (BackupSpecificationType::CONFIG, false) => {
                let upload_options = UploadOptions {
                    compress: true,
                    encrypt: crypto.mode == CryptMode::Encrypt,
                    ..UploadOptions::default()
                };

                log_file("config file", &filename, target.as_ref());
                let stats = client
                    .upload_blob_from_file(&filename, target.as_ref(), upload_options)
                    .await?;
                manifest.add_file(&target, stats.size, stats.csum, crypto.mode)?;
            }
            (BackupSpecificationType::LOGFILE, false) => {
                // fixme: remove - not needed anymore ?
                let upload_options = UploadOptions {
                    compress: true,
                    encrypt: crypto.mode == CryptMode::Encrypt,
                    ..UploadOptions::default()
                };

                log_file("log file", &filename, target.as_ref());
                let stats = client
                    .upload_blob_from_file(&filename, target.as_ref(), upload_options)
                    .await?;
                manifest.add_file(&target, stats.size, stats.csum, crypto.mode)?;
            }
            (BackupSpecificationType::PXAR, false) => {
                let target_base = if let Some(base) = target_base.strip_suffix(".pxar") {
                    base.to_string()
                } else {
                    bail!("unexpected suffix in target: {target_base}");
                };

                let (target, payload_target) =
                    if detection_mode.is_metadata() || detection_mode.is_data() {
                        (
                            format!("{target_base}.mpxar.{extension}")
                                .as_str()
                                .try_into()?,
                            Some(
                                format!("{target_base}.ppxar.{extension}")
                                    .as_str()
                                    .try_into()?,
                            ),
                        )
                    } else {
                        (target, None)
                    };

                // start catalog upload on first use
                if catalog.is_none() && !detection_mode.is_data() && !detection_mode.is_metadata() {
                    let catalog_upload_res =
                        spawn_catalog_upload(client.clone(), crypto.mode == CryptMode::Encrypt)?;
                    catalog = Some(catalog_upload_res.catalog_writer);
                    catalog_result_rx = Some(catalog_upload_res.result);
                }

                log_file("directory", &filename, target.as_ref());
                if let Some(catalog) = catalog.as_ref() {
                    catalog
                        .lock()
                        .unwrap()
                        .start_directory(std::ffi::CString::new(target.as_ref())?.as_c_str())?;
                }

                let mut previous_ref = None;
                let max_cache_size = if detection_mode.is_metadata() {
                    let old_rlimit = raise_nofile_limit()?;
                    if let Some(ref manifest) = previous_manifest {
                        // BackupWriter::start created a new snapshot, get the one before
                        if let Some(backup_time) = client.previous_backup_time().await? {
                            let backup_dir: BackupDir =
                                (snapshot.group.clone(), backup_time).into();
                            let backup_reader = BackupReader::start(
                                &http_client,
                                crypt_config.clone(),
                                repo.store(),
                                &backup_ns,
                                &backup_dir,
                                true,
                            )
                            .await?;
                            previous_ref = prepare_reference(
                                &target,
                                manifest.clone(),
                                &client,
                                backup_reader,
                                crypt_config.clone(),
                                crypto.mode,
                            )
                            .await?
                        }
                    }

                    if old_rlimit.rlim_max <= 4096 {
                        log::info!(
                            "resource limit for open file handles low: {}",
                            old_rlimit.rlim_max,
                        );
                    }

                    Some(usize::try_from(
                        old_rlimit.rlim_max - old_rlimit.rlim_cur / 2,
                    )?)
                } else {
                    None
                };

                let pxar_options = pbs_client::pxar::PxarCreateOptions {
                    device_set: devices.clone(),
                    patterns: pattern_list.clone(),
                    entries_max: entries_max as usize,
                    skip_lost_and_found,
                    skip_e2big_xattr,
                    previous_ref,
                    max_cache_size,
                };

                let upload_options = UploadOptions {
                    previous_manifest: previous_manifest.clone(),
                    compress: true,
                    encrypt: crypto.mode == CryptMode::Encrypt,
                    ..UploadOptions::default()
                };

                let (stats, payload_stats) = backup_directory(
                    &client,
                    &filename,
                    &target,
                    payload_target.as_ref(),
                    chunk_size_opt,
                    catalog.as_ref().cloned(),
                    pxar_options,
                    upload_options,
                )
                .await?;

                if let Some(payload_stats) = payload_stats {
                    manifest.add_file(
                        &payload_target
                            .ok_or_else(|| format_err!("missing payload target archive"))?,
                        payload_stats.size,
                        payload_stats.csum,
                        crypto.mode,
                    )?;
                }
                manifest.add_file(&target, stats.size, stats.csum, crypto.mode)?;
                if let Some(catalog) = catalog.as_ref() {
                    catalog.lock().unwrap().end_directory()?;
                }
            }
            (BackupSpecificationType::IMAGE, false) => {
                log_file("image", &filename, target.as_ref());

                let upload_options = UploadOptions {
                    previous_manifest: previous_manifest.clone(),
                    fixed_size: Some(size),
                    compress: true,
                    encrypt: crypto.mode == CryptMode::Encrypt,
                };

                let stats =
                    backup_image(&client, &filename, &target, chunk_size_opt, upload_options)
                        .await?;
                manifest.add_file(&target, stats.size, stats.csum, crypto.mode)?;
            }
        }
    }

    if dry_run {
        log::info!("dry-run: no upload happened");
        return Ok(Value::Null);
    }

    // finalize and upload catalog
    if let Some(catalog) = catalog {
        let mutex = Arc::try_unwrap(catalog)
            .map_err(|_| format_err!("unable to get catalog (still used)"))?;
        let mut catalog = mutex.into_inner().unwrap();

        catalog.finish()?;

        drop(catalog); // close upload stream

        if let Some(catalog_result_rx) = catalog_result_rx {
            let stats = catalog_result_rx.await??;
            manifest.add_file(&CATALOG_NAME, stats.size, stats.csum, crypto.mode)?;
        }
    }

    if let Some(rsa_encrypted_key) = rsa_encrypted_key {
        log::info!(
            "Upload RSA encoded key to '{}' as {}",
            repo,
            *ENCRYPTED_KEY_BLOB_NAME
        );
        let options = UploadOptions {
            compress: false,
            encrypt: false,
            ..UploadOptions::default()
        };
        let stats = client
            .upload_blob_from_data(rsa_encrypted_key, ENCRYPTED_KEY_BLOB_NAME.as_ref(), options)
            .await?;
        manifest.add_file(
            &ENCRYPTED_KEY_BLOB_NAME,
            stats.size,
            stats.csum,
            crypto.mode,
        )?;
    }
    // create manifest (index.json)
    // manifests are never encrypted, but include a signature
    let manifest = manifest
        .to_string(crypt_config.as_ref().map(Arc::as_ref))
        .map_err(|err| format_err!("unable to format manifest - {}", err))?;

    log::debug!("Upload index.json to '{}'", repo);

    let options = UploadOptions {
        compress: true,
        encrypt: false,
        ..UploadOptions::default()
    };
    client
        .upload_blob_from_data(manifest.into_bytes(), MANIFEST_BLOB_NAME.as_ref(), options)
        .await?;

    client.finish().await?;

    let end_time = std::time::Instant::now();
    let elapsed = end_time.duration_since(start_time);
    log::info!("Duration: {:.2}s", elapsed.as_secs_f64());
    log::info!("End Time: {}", strftime_local("%c", epoch_i64())?);
    Ok(Value::Null)
}

async fn prepare_reference(
    target: &BackupArchiveName,
    manifest: Arc<BackupManifest>,
    backup_writer: &BackupWriter,
    backup_reader: Arc<BackupReader>,
    crypt_config: Option<Arc<CryptConfig>>,
    crypt_mode: CryptMode,
) -> Result<Option<PxarPrevRef>, Error> {
    let (target, payload_target) =
        match pbs_client::tools::get_pxar_archive_names(target, &manifest) {
            Ok((target, payload_target)) => (target, payload_target),
            Err(_) => return Ok(None),
        };
    let payload_target = if let Some(payload_target) = payload_target {
        payload_target
    } else {
        return Ok(None);
    };

    let metadata_ref_index = if let Ok(index) = backup_reader
        .download_dynamic_index(&manifest, &target)
        .await
    {
        index
    } else {
        log::info!("No previous metadata index, continue without reference");
        return Ok(None);
    };

    let file_info = match manifest.lookup_file_info(&payload_target) {
        Ok(file_info) => file_info,
        Err(_) => {
            log::info!("No previous payload index found in manifest, continue without reference");
            return Ok(None);
        }
    };

    if file_info.crypt_mode != crypt_mode {
        log::info!("Crypt mode mismatch, continue without reference");
        return Ok(None);
    }

    let known_payload_chunks = Arc::new(Mutex::new(HashSet::new()));
    let payload_ref_index = backup_writer
        .download_previous_dynamic_index(&payload_target, &manifest, known_payload_chunks)
        .await?;

    log::info!("Using previous index as metadata reference for '{target}'");

    let most_used = metadata_ref_index.find_most_used_chunks(8);
    let file_info = manifest.lookup_file_info(&target)?;
    let chunk_reader = RemoteChunkReader::new(
        backup_reader,
        crypt_config.clone(),
        file_info.chunk_crypt_mode(),
        most_used,
    );
    let reader = BufferedDynamicReader::new(metadata_ref_index, chunk_reader);
    let archive_size = reader.archive_size();
    let reader: MetadataArchiveReader = Arc::new(LocalDynamicReadAt::new(reader));
    // only care about the metadata, therefore do not attach payload reader
    let accessor = Accessor::new(pxar::PxarVariant::Unified(reader), archive_size).await?;

    Ok(Some(pbs_client::pxar::PxarPrevRef {
        accessor,
        payload_index: payload_ref_index,
        archive_name: target.to_string(),
    }))
}

async fn dump_image<W: Write>(
    client: Arc<BackupReader>,
    crypt_config: Option<Arc<CryptConfig>>,
    crypt_mode: CryptMode,
    index: FixedIndexReader,
    mut writer: W,
) -> Result<(), Error> {
    let most_used = index.find_most_used_chunks(8);

    let chunk_reader = RemoteChunkReader::new(client.clone(), crypt_config, crypt_mode, most_used);

    // Note: we avoid using BufferedFixedReader, because that add an additional buffer/copy
    // and thus slows down reading. Instead, directly use RemoteChunkReader
    let mut per = 0;
    let mut bytes = 0;
    let start_time = std::time::Instant::now();

    for pos in 0..index.index_count() {
        let digest = index.index_digest(pos).unwrap();
        let raw_data = chunk_reader.read_chunk(digest).await?;
        writer.write_all(&raw_data)?;
        bytes += raw_data.len();
        let next_per = ((pos + 1) * 100) / index.index_count();
        if per != next_per {
            log::debug!(
                "progress {}% (read {} bytes, duration {} sec)",
                next_per,
                bytes,
                start_time.elapsed().as_secs()
            );
            per = next_per;
        }
    }

    let end_time = std::time::Instant::now();
    let elapsed = end_time.duration_since(start_time);
    log::info!(
        "restore image complete (bytes={}, duration={:.2}s, speed={:.2}MB/s)",
        bytes,
        elapsed.as_secs_f64(),
        bytes as f64 / (1024.0 * 1024.0 * elapsed.as_secs_f64())
    );

    Ok(())
}

#[api(
    input: {
        properties: {
            repository: {
                schema: REPO_URL_SCHEMA,
                optional: true,
            },
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            snapshot: {
                type: String,
                description: "Group/Snapshot path.",
            },
            "archive-name": {
                description: "Backup archive name.",
                type: String,
            },
            target: {
                type: String,
                description: r###"Target directory path. Use '-' to write to standard output.

We do not extract '.pxar' archives when writing to standard output.

"###
            },
            limit: {
                type: ClientRateLimitConfig,
                flatten: true,
            },
            pattern: {
                type: Array,
                items: {
                    type: PathPattern,
                },
                description: "Path or match pattern to limit files that get restored.",
                optional: true,
            },
            "allow-existing-dirs": {
                type: Boolean,
                description: "Do not fail if directories already exists.",
                optional: true,
                default: false,
            },
            keyfile: {
                schema: KEYFILE_SCHEMA,
                optional: true,
            },
            "keyfd": {
                schema: KEYFD_SCHEMA,
                optional: true,
            },
            "crypt-mode": {
                type: CryptMode,
                optional: true,
            },
            "ignore-acls": {
                type: Boolean,
                description: "ignore acl settings",
                optional: true,
                default: false,
            },
            "ignore-xattrs": {
                type: Boolean,
                description: "ignore xattr settings",
                optional: true,
                default: false,
            },
            "ignore-ownership": {
                type: Boolean,
                description: "ignore owner settings (no chown)",
                optional: true,
                default: false,
            },
            "ignore-permissions": {
                type: Boolean,
                description: "ignore permission settings (no chmod)",
                optional: true,
                default: false,
            },
            "overwrite": {
                type: Boolean,
                description: "overwrite already existing files",
                optional: true,
                default: false,
            },
            "overwrite-files": {
                description: "overwrite already existing files",
                optional: true,
                default: false,
            },
            "overwrite-symlinks": {
                description: "overwrite already existing entries by archives symlink",
                optional: true,
                default: false,
            },
            "overwrite-hardlinks": {
                description: "overwrite already existing entries by archives hardlink",
                optional: true,
                default: false,
            },
            "ignore-extract-device-errors": {
                type: Boolean,
                description: "ignore errors that occur during device node extraction",
                optional: true,
                default: false,
            },
            "prelude-target": {
                description: "Path to restore prelude to, (pxar v2 archives only).",
                type: String,
                optional: true,
            },
        }
    }
)]
/// Restore backup repository.
#[allow(clippy::too_many_arguments)]
async fn restore(
    param: Value,
    allow_existing_dirs: bool,
    ignore_acls: bool,
    ignore_xattrs: bool,
    ignore_ownership: bool,
    ignore_permissions: bool,
    overwrite: bool,
    overwrite_files: bool,
    overwrite_symlinks: bool,
    overwrite_hardlinks: bool,
    limit: ClientRateLimitConfig,
    ignore_extract_device_errors: bool,
) -> Result<Value, Error> {
    let repo = extract_repository_from_value(&param)?;

    let archive_name: BackupArchiveName =
        json::required_string_param(&param, "archive-name")?.try_into()?;

    let rate_limit = RateLimitConfig::from_client_config(limit);

    let client = connect_rate_limited(&repo, rate_limit)?;
    record_repository(&repo);

    let ns = optional_ns_param(&param)?;
    let path = json::required_string_param(&param, "snapshot")?;

    let backup_dir = dir_or_last_from_group(&client, &repo, &ns, path).await?;

    let target = json::required_string_param(&param, "target")?;
    let target = if target == "-" { None } else { Some(target) };

    let mut match_list = Vec::new();
    if let Some(pattern) = param["pattern"].as_array() {
        if target.is_none() {
            bail!("patterns not allowed when restoring to stdout");
        }

        for p in pattern {
            if let Some(pattern) = p.as_str() {
                let match_entry =
                    MatchEntry::parse_pattern(pattern, PatternFlag::PATH_NAME, MatchType::Include)?;
                match_list.push(match_entry);
            }
        }
    };

    let crypto = crypto_parameters(&param)?;

    let crypt_config = match crypto.enc_key {
        None => None,
        Some(ref key) => {
            let (key, _, _) =
                decrypt_key(&key.key, &get_encryption_key_password).inspect_err(|_err| {
                    log::error!("{}", format_key_source(&key.source, "encryption"));
                })?;
            Some(Arc::new(CryptConfig::new(key)?))
        }
    };

    let client = BackupReader::start(
        &client,
        crypt_config.clone(),
        repo.store(),
        &ns,
        &backup_dir,
        true,
    )
    .await?;

    let (manifest, backup_index_data) = client.download_manifest().await?;

    if archive_name == *ENCRYPTED_KEY_BLOB_NAME && crypt_config.is_none() {
        log::info!(
            "Restoring encrypted key blob without original key - skipping manifest fingerprint \
            check!"
        );
    } else {
        if manifest.signature.is_some() {
            if let Some(key) = &crypto.enc_key {
                log::info!("{}", format_key_source(&key.source, "encryption"));
            }
            if let Some(config) = &crypt_config {
                log::info!("Fingerprint: {}", Fingerprint::new(config.fingerprint()));
            }
        }
        manifest.check_fingerprint(crypt_config.as_ref().map(Arc::as_ref))?;
    }

    if archive_name == *MANIFEST_BLOB_NAME {
        if let Some(target) = target {
            replace_file(target, &backup_index_data, CreateOptions::new(), false)?;
        } else {
            let stdout = std::io::stdout();
            let mut writer = stdout.lock();
            writer
                .write_all(&backup_index_data)
                .map_err(|err| format_err!("unable to pipe data - {}", err))?;
        }

        return Ok(Value::Null);
    }

    if archive_name.archive_type() == ArchiveType::Blob {
        let mut reader = client.download_blob(&manifest, &archive_name).await?;

        if let Some(target) = target {
            let mut writer = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(target)
                .map_err(|err| {
                    format_err!("unable to create target file {:?} - {}", target, err)
                })?;
            std::io::copy(&mut reader, &mut writer)?;
        } else {
            let stdout = std::io::stdout();
            let mut writer = stdout.lock();
            std::io::copy(&mut reader, &mut writer)
                .map_err(|err| format_err!("unable to pipe data - {}", err))?;
        }
    } else if archive_name.archive_type() == ArchiveType::DynamicIndex {
        let (archive_name, payload_archive_name) =
            pbs_client::tools::get_pxar_archive_names(&archive_name, &manifest)?;

        let mut reader = get_buffered_pxar_reader(
            &archive_name,
            client.clone(),
            &manifest,
            crypt_config.clone(),
        )
        .await?;

        let on_error = if ignore_extract_device_errors {
            let handler: PxarErrorHandler = Box::new(move |err: Error| {
                use pbs_client::pxar::PxarExtractContext;

                match err.downcast_ref::<PxarExtractContext>() {
                    Some(PxarExtractContext::ExtractDevice) => Ok(()),
                    _ => Err(err),
                }
            });

            Some(handler)
        } else {
            None
        };

        let mut overwrite_flags = pbs_client::pxar::OverwriteFlags::empty();
        overwrite_flags.set(pbs_client::pxar::OverwriteFlags::FILE, overwrite_files);
        overwrite_flags.set(
            pbs_client::pxar::OverwriteFlags::SYMLINK,
            overwrite_symlinks,
        );
        overwrite_flags.set(
            pbs_client::pxar::OverwriteFlags::HARDLINK,
            overwrite_hardlinks,
        );
        if overwrite {
            overwrite_flags.insert(pbs_client::pxar::OverwriteFlags::all());
        }

        let prelude_path = param["prelude-target"].as_str().map(PathBuf::from);

        let options = pbs_client::pxar::PxarExtractOptions {
            match_list: &match_list,
            extract_match_default: match_list.is_empty(),
            allow_existing_dirs,
            overwrite_flags,
            on_error,
            prelude_path,
        };

        let mut feature_flags = pbs_client::pxar::Flags::DEFAULT;

        if ignore_acls {
            feature_flags.remove(pbs_client::pxar::Flags::WITH_ACL);
        }
        if ignore_xattrs {
            feature_flags.remove(pbs_client::pxar::Flags::WITH_XATTRS);
        }
        if ignore_ownership {
            feature_flags.remove(pbs_client::pxar::Flags::WITH_OWNER);
        }
        if ignore_permissions {
            feature_flags.remove(pbs_client::pxar::Flags::WITH_PERMISSIONS);
        }

        if let Some(target) = target {
            let reader = if let Some(payload_archive_name) = payload_archive_name {
                let payload_reader = get_buffered_pxar_reader(
                    &payload_archive_name,
                    client.clone(),
                    &manifest,
                    crypt_config.clone(),
                )
                .await?;
                pxar::PxarVariant::Split(reader, payload_reader)
            } else {
                pxar::PxarVariant::Unified(reader)
            };
            let decoder = pxar::decoder::Decoder::from_std(reader)?;

            pbs_client::pxar::extract_archive(
                decoder,
                Path::new(target),
                feature_flags,
                |path| {
                    log::debug!("{:?}", path);
                },
                options,
            )
            .map_err(|err| format_err!("error extracting archive - {:#}", err))?;
        } else {
            if archive_name.ends_with(".mpxar.didx") || archive_name.ends_with(".ppxar.didx") {
                bail!("unable to pipe split archive");
            }
            let mut writer = std::fs::OpenOptions::new()
                .write(true)
                .open("/dev/stdout")
                .map_err(|err| format_err!("unable to open /dev/stdout - {}", err))?;

            std::io::copy(&mut reader, &mut writer)
                .map_err(|err| format_err!("unable to pipe data - {}", err))?;
        }
    } else if archive_name.archive_type() == ArchiveType::FixedIndex {
        let file_info = manifest.lookup_file_info(&archive_name)?;
        let index = client
            .download_fixed_index(&manifest, &archive_name)
            .await?;

        let mut writer = if let Some(target) = target {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(target)
                .map_err(|err| format_err!("unable to create target file {:?} - {}", target, err))?
        } else {
            std::fs::OpenOptions::new()
                .write(true)
                .open("/dev/stdout")
                .map_err(|err| format_err!("unable to open /dev/stdout - {}", err))?
        };

        dump_image(
            client.clone(),
            crypt_config.clone(),
            file_info.chunk_crypt_mode(),
            index,
            &mut writer,
        )
        .await?;
    }

    Ok(Value::Null)
}

#[api(
    input: {
        properties: {
            "dry-run": {
                type: bool,
                optional: true,
                description: "Just show what prune would do, but do not delete anything.",
            },
            group: {
                type: String,
                description: "Backup group",
            },
            "prune-options": {
                type: PruneJobOptions,
                flatten: true,
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
            quiet: {
                type: bool,
                optional: true,
                default: false,
                description: "Minimal output - only show removals.",
            },
            repository: {
                schema: REPO_URL_SCHEMA,
                optional: true,
            },
        },
    },
)]
/// Prune a backup repository.
async fn prune(
    dry_run: Option<bool>,
    group: String,
    prune_options: PruneJobOptions,
    quiet: bool,
    mut param: Value,
) -> Result<Value, Error> {
    let repo = extract_repository_from_value(&param)?;

    let client = connect(&repo)?;

    let path = format!("api2/json/admin/datastore/{}/prune", repo.store());

    let group: BackupGroup = group.parse()?;

    let output_format = extract_output_format(&mut param);

    let mut api_param = serde_json::to_value(prune_options)?;
    if let Some(dry_run) = dry_run {
        api_param["dry-run"] = dry_run.into();
    }
    merge_group_into(api_param.as_object_mut().unwrap(), group);

    let mut result = client.post(&path, Some(api_param)).await?;

    record_repository(&repo);

    let render_snapshot_path = |_v: &Value, record: &Value| -> Result<String, Error> {
        let item: PruneListItem = serde_json::from_value(record.to_owned())?;
        Ok(item.backup.to_string())
    };

    let render_prune_action = |v: &Value, _record: &Value| -> Result<String, Error> {
        Ok(match v.as_bool() {
            Some(true) => "keep",
            Some(false) => "remove",
            None => "unknown",
        }
        .to_string())
    };

    let options = default_table_format_options()
        .sortby("backup-type", false)
        .sortby("backup-id", false)
        .sortby("backup-time", false)
        .column(
            ColumnConfig::new("backup-id")
                .renderer(render_snapshot_path)
                .header("snapshot"),
        )
        .column(
            ColumnConfig::new("backup-time")
                .renderer(pbs_tools::format::render_epoch)
                .header("date"),
        )
        .column(
            ColumnConfig::new("keep")
                .renderer(render_prune_action)
                .header("action"),
        );

    let return_type = &pbs_api_types::ADMIN_DATASTORE_PRUNE_RETURN_TYPE;

    let mut data = result["data"].take();

    if quiet {
        let list: Vec<Value> = data
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["keep"].as_bool() == Some(false))
            .cloned()
            .collect();
        data = list.into();
    }

    format_and_print_result_full(&mut data, return_type, &output_format, &options);

    Ok(Value::Null)
}

#[api(
   input: {
       properties: {
           repository: {
               schema: REPO_URL_SCHEMA,
               optional: true,
           },
           "output-format": {
               schema: OUTPUT_FORMAT,
               optional: true,
           },
       }
   },
    returns: {
        type: StorageStatus,
    },
)]
/// Get repository status.
async fn status(param: Value) -> Result<Value, Error> {
    let repo = extract_repository_from_value(&param)?;

    let output_format = get_output_format(&param);

    let client = connect(&repo)?;

    let path = format!("api2/json/admin/datastore/{}/status", repo.store());

    let mut result = client.get(&path, None).await?;
    let mut data = result["data"].take();

    record_repository(&repo);

    let render_total_percentage = |v: &Value, record: &Value| -> Result<String, Error> {
        let v = v.as_u64().unwrap();
        let total = record["total"].as_u64().unwrap();
        let roundup = total / 200;
        if let Some(per) = ((v + roundup) * 100).checked_div(total) {
            let info = format!(" ({} %)", per);
            Ok(format!("{} {:>8}", v, info))
        } else {
            bail!("Cannot render total percentage: denominator is zero");
        }
    };

    let options = default_table_format_options()
        .noheader(true)
        .column(ColumnConfig::new("total").renderer(render_total_percentage))
        .column(ColumnConfig::new("used").renderer(render_total_percentage))
        .column(ColumnConfig::new("avail").renderer(render_total_percentage));

    let return_type = &API_METHOD_STATUS.returns;

    format_and_print_result_full(&mut data, return_type, &output_format, &options);

    Ok(Value::Null)
}

/// This is a workaround until we have cleaned up the chunk/reader/... infrastructure for better
/// async use!
///
/// Ideally BufferedDynamicReader gets replaced so the LruCache maps to `BroadcastFuture<Chunk>`,
/// so that we can properly access it from multiple threads simultaneously while not issuing
/// duplicate simultaneous reads over http.
pub struct BufferedDynamicReadAt {
    inner: Mutex<BufferedDynamicReader<RemoteChunkReader>>,
}

impl BufferedDynamicReadAt {
    fn new(inner: BufferedDynamicReader<RemoteChunkReader>) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

impl ReadAt for BufferedDynamicReadAt {
    fn start_read_at<'a>(
        self: Pin<&'a Self>,
        _cx: &mut Context,
        buf: &'a mut [u8],
        offset: u64,
    ) -> MaybeReady<io::Result<usize>, ReadAtOperation<'a>> {
        MaybeReady::Ready(tokio::task::block_in_place(move || {
            let mut reader = self.inner.lock().unwrap();
            reader.seek(SeekFrom::Start(offset))?;
            reader.read(buf)
        }))
    }

    fn poll_complete<'a>(
        self: Pin<&'a Self>,
        _op: ReadAtOperation<'a>,
    ) -> MaybeReady<io::Result<usize>, ReadAtOperation<'a>> {
        panic!("BufferedDynamicReadAt::start_read_at returned Pending");
    }
}

fn main() {
    pbs_tools::setup_libc_malloc_opts();
    proxmox_log::Logger::from_env("PBS_LOG", proxmox_log::LevelFilter::INFO)
        .stderr()
        .init()
        .expect("failed to initiate logger");

    let backup_cmd_def = CliCommand::new(&API_METHOD_CREATE_BACKUP)
        .arg_param(&["backupspec"])
        .completion_cb("repository", complete_repository)
        .completion_cb("backupspec", complete_backup_source)
        .completion_cb("keyfile", complete_file_name)
        .completion_cb("master-pubkey-file", complete_file_name)
        .completion_cb("chunk-size", complete_chunk_size);

    let benchmark_cmd_def = CliCommand::new(&API_METHOD_BENCHMARK)
        .completion_cb("repository", complete_repository)
        .completion_cb("keyfile", complete_file_name);

    let list_cmd_def = CliCommand::new(&API_METHOD_LIST_BACKUP_GROUPS)
        .completion_cb("ns", complete_namespace)
        .completion_cb("repository", complete_repository);

    let garbage_collect_cmd_def = CliCommand::new(&API_METHOD_START_GARBAGE_COLLECTION)
        .completion_cb("repository", complete_repository);

    let restore_cmd_def = CliCommand::new(&API_METHOD_RESTORE)
        .arg_param(&["snapshot", "archive-name", "target"])
        .completion_cb("repository", complete_repository)
        .completion_cb("ns", complete_namespace)
        .completion_cb("snapshot", complete_group_or_snapshot)
        .completion_cb("archive-name", complete_archive_name)
        .completion_cb("target", complete_file_name)
        .completion_cb("prelude-target", complete_file_name);

    let prune_cmd_def = CliCommand::new(&API_METHOD_PRUNE)
        .arg_param(&["group"])
        .completion_cb("ns", complete_namespace)
        .completion_cb("group", complete_backup_group)
        .completion_cb("repository", complete_repository);

    let status_cmd_def =
        CliCommand::new(&API_METHOD_STATUS).completion_cb("repository", complete_repository);

    let login_cmd_def =
        CliCommand::new(&API_METHOD_API_LOGIN).completion_cb("repository", complete_repository);

    let logout_cmd_def =
        CliCommand::new(&API_METHOD_API_LOGOUT).completion_cb("repository", complete_repository);

    let version_cmd_def =
        CliCommand::new(&API_METHOD_API_VERSION).completion_cb("repository", complete_repository);

    let change_owner_cmd_def = CliCommand::new(&API_METHOD_CHANGE_BACKUP_OWNER)
        .arg_param(&["group", "new-owner"])
        .completion_cb("ns", complete_namespace)
        .completion_cb("group", complete_backup_group)
        .completion_cb("new-owner", complete_auth_id)
        .completion_cb("repository", complete_repository);

    let cmd_def = CliCommandMap::new()
        .insert("backup", backup_cmd_def)
        .insert("garbage-collect", garbage_collect_cmd_def)
        .insert("list", list_cmd_def)
        .insert("login", login_cmd_def)
        .insert("logout", logout_cmd_def)
        .insert("prune", prune_cmd_def)
        .insert("restore", restore_cmd_def)
        .insert("snapshot", snapshot_mgtm_cli())
        .insert("status", status_cmd_def)
        .insert("key", key::cli())
        .insert("mount", mount_cmd_def())
        .insert("map", map_cmd_def())
        .insert("unmap", unmap_cmd_def())
        .insert("catalog", catalog_mgmt_cli())
        .insert("task", task_mgmt_cli())
        .insert("version", version_cmd_def)
        .insert("benchmark", benchmark_cmd_def)
        .insert("change-owner", change_owner_cmd_def)
        .insert("namespace", namespace::cli_map())
        .insert("group", group_mgmt_cli())
        .alias(&["files"], &["snapshot", "files"])
        .alias(&["forget"], &["snapshot", "forget"])
        .alias(&["upload-log"], &["snapshot", "upload-log"])
        .alias(&["snapshots"], &["snapshot", "list"]);

    let rpcenv = CliEnvironment::new();
    run_cli_command(
        cmd_def,
        rpcenv,
        Some(|future| proxmox_async::runtime::main(future)),
    );
}
