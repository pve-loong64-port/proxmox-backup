//! Sync datastore by pulling contents from remote server

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use super::sync::{
    exclude_not_verified_or_encrypted, filter_out_in_progress, ignore_not_verified_or_encrypted,
    LocalSource, RemoteSource, RemovedVanishedStats, SkipInfo, SkipReason, SyncSource,
    SyncSourceReader, SyncStats,
};
use crate::backup::{check_ns_modification_privs, check_ns_privs};
use crate::server::sync::SharedGroupProgress;
use anyhow::{bail, format_err, Context, Error};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use pbs_api_types::{
    print_store_and_ns, ArchiveType, Authid, BackupArchiveName, BackupDir, BackupGroup,
    BackupNamespace, CryptMode, GroupFilter, Operation, RateLimitConfig, Remote, SnapshotListItem,
    VerifyState, CLIENT_LOG_BLOB_NAME, MANIFEST_BLOB_NAME, MAX_NAMESPACE_DEPTH,
    PRIV_DATASTORE_AUDIT, PRIV_DATASTORE_BACKUP,
};
use pbs_client::BackupRepository;
use pbs_config::CachedUserInfo;
use pbs_datastore::data_blob::{DataBlob, DataChunkBuilder};
use pbs_datastore::dynamic_index::{DynamicIndexReader, DynamicIndexWriter};
use pbs_datastore::fixed_index::{FixedIndexReader, FixedIndexWriter};
use pbs_datastore::index::IndexFile;
use pbs_datastore::manifest::{BackupManifest, FileInfo};
use pbs_datastore::read_chunk::AsyncReadChunk;
use pbs_datastore::{
    check_backup_owner, check_namespace_depth_limit, DataBlobReader, DataStore, DatastoreBackend,
    StoreProgress,
};
use pbs_tools::bounded_join_set::BoundedJoinSet;
use pbs_tools::buffered_logger::{BufferedLogger, LogLineSender};
use pbs_tools::crypt_config::CryptConfig;
use pbs_tools::sha::sha256;
use proxmox_human_byte::HumanByte;
use proxmox_parallel_handler::ParallelHandler;
use proxmox_sys::fs::{replace_file, CreateOptions};
use tracing::{info, Level};

pub(crate) struct PullTarget {
    store: Arc<DataStore>,
    ns: BackupNamespace,
    // Contains the active S3Client in case of S3 backend
    backend: DatastoreBackend,
}

/// Parameters for a pull operation.
pub(crate) struct PullParameters {
    /// Where data is pulled from
    source: Arc<dyn SyncSource>,
    /// Where data should be pulled into
    target: PullTarget,
    /// Owner of synced groups (needs to match local owner of pre-existing groups)
    owner: Authid,
    /// Whether to remove groups which exist locally, but not on the remote end
    remove_vanished: bool,
    /// How many levels of sub-namespaces to pull (0 == no recursion, None == maximum recursion)
    max_depth: Option<usize>,
    /// Filters for reducing the pull scope
    group_filter: Vec<GroupFilter>,
    /// How many snapshots should be transferred at most (taking the newest N snapshots)
    transfer_last: Option<usize>,
    /// Only sync encrypted backup snapshots
    encrypted_only: bool,
    /// Only sync verified backup snapshots
    verified_only: bool,
    /// Whether to re-sync corrupted snapshots
    resync_corrupt: bool,
    /// Maximum number of worker threads to pull during sync job
    worker_threads: Option<usize>,
    /// Decryption key ids and configs to decrypt snapshots with matching key fingerprint
    crypt_configs: Vec<(String, Arc<CryptConfig>)>,
}

impl PullParameters {
    /// Creates a new instance of `PullParameters`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: &str,
        ns: BackupNamespace,
        remote: Option<&str>,
        remote_store: &str,
        remote_ns: BackupNamespace,
        owner: Authid,
        remove_vanished: Option<bool>,
        max_depth: Option<usize>,
        group_filter: Option<Vec<GroupFilter>>,
        limit: RateLimitConfig,
        transfer_last: Option<usize>,
        encrypted_only: Option<bool>,
        verified_only: Option<bool>,
        resync_corrupt: Option<bool>,
        worker_threads: Option<usize>,
        decryption_keys: Option<Vec<String>>,
    ) -> Result<Self, Error> {
        if let Some(max_depth) = max_depth {
            ns.check_max_depth(max_depth)?;
            remote_ns.check_max_depth(max_depth)?;
        };
        let remove_vanished = remove_vanished.unwrap_or(false);
        let resync_corrupt = resync_corrupt.unwrap_or(false);
        let encrypted_only = encrypted_only.unwrap_or(false);
        let verified_only = verified_only.unwrap_or(false);

        let source: Arc<dyn SyncSource> = if let Some(remote) = remote {
            let (remote_config, _digest) = pbs_config::remote::config()?;
            let remote: Remote = remote_config.lookup("remote", remote)?;

            let repo = BackupRepository::new(
                Some(remote.config.auth_id.clone()),
                Some(remote.config.host.clone()),
                remote.config.port,
                remote_store.to_string(),
            );
            let client = crate::api2::config::remote::remote_client_config(&remote, Some(limit))?;
            Arc::new(RemoteSource {
                repo,
                ns: remote_ns,
                client,
            })
        } else {
            let lookup = crate::tools::lookup_with(remote_store, Operation::Read);
            let store = DataStore::lookup_datastore(lookup)?;
            Arc::new(LocalSource {
                store,
                ns: remote_ns,
            })
        };
        let lookup = crate::tools::lookup_with(store, Operation::Write);
        let store = DataStore::lookup_datastore(lookup)?;
        let backend = store.backend()?;
        let target = PullTarget { store, ns, backend };

        let group_filter = group_filter.unwrap_or_default();

        let crypt_configs = if let Some(key_ids) = &decryption_keys {
            let mut crypt_configs = Vec::with_capacity(key_ids.len());
            for key_id in key_ids {
                let crypt_config =
                    crate::server::sync::check_privs_and_load_key_config(key_id, &owner, false)?;
                crypt_configs.push((key_id.to_string(), crypt_config));
            }
            crypt_configs
        } else {
            Vec::new()
        };

        Ok(Self {
            source,
            target,
            owner,
            remove_vanished,
            max_depth,
            group_filter,
            transfer_last,
            encrypted_only,
            verified_only,
            resync_corrupt,
            worker_threads,
            crypt_configs,
        })
    }
}

async fn pull_index_chunks<I: IndexFile>(
    chunk_reader: Arc<dyn AsyncReadChunk>,
    target: Arc<DataStore>,
    index: I,
    encountered_chunks: Arc<Mutex<EncounteredChunks>>,
    backend: &DatastoreBackend,
    archive_prefix: &str,
    log_sender: Arc<LogLineSender>,
    decrypted_index_writer: Option<DecryptedIndexWriter>,
) -> Result<SyncStats, Error> {
    use futures::stream::{self, StreamExt, TryStreamExt};

    let start_time = SystemTime::now();

    let stream = stream::iter(
        (0..index.index_count())
            .map(|pos| index.chunk_info(pos).unwrap())
            .filter(|info| {
                let guard = encountered_chunks.lock().unwrap();
                match guard.check_reusable(&info.digest) {
                    Some(reusable) => {
                        if reusable.decrypted_digest.is_some() {
                            // if there is a mapping, then the chunk digest must be rewritten to
                            // the index, cannot skip here but optimized when processing the stream
                            true
                        } else {
                            // reusable and already touched, can always skip
                            !reusable.touched
                        }
                    }
                    None => true,
                }
            }),
    );

    let target2 = target.clone();
    let backend = backend.clone();
    let verify_pool = ParallelHandler::new(
        "sync chunk writer",
        4,
        move |(chunk, digest, size): (DataBlob, [u8; 32], u64)| {
            // println!("verify and write {}", hex::encode(&digest));
            chunk.verify_unencrypted(size as usize, &digest)?;
            target2.insert_chunk(&chunk, &digest, &backend)?;
            Ok(())
        },
    );

    let verify_and_write_channel = verify_pool.channel();

    let bytes = Arc::new(AtomicUsize::new(0));
    let offset = Arc::new(AtomicU64::new(0));
    let chunk_count = Arc::new(AtomicUsize::new(0));

    let stream = stream.map(|info| {
        let target = Arc::clone(&target);
        let chunk_reader = chunk_reader.clone();
        let bytes = Arc::clone(&bytes);
        let chunk_count = Arc::clone(&chunk_count);
        let verify_and_write_channel = verify_and_write_channel.clone();
        let encountered_chunks = Arc::clone(&encountered_chunks);
        let offset = Arc::clone(&offset);
        let decrypted_index_writer = decrypted_index_writer.clone();

        Ok::<_, Error>(async move {
            //info!("sync {} chunk {}", pos, hex::encode(digest));
            let (chunk, digest, size) = match decrypted_index_writer {
                Some(DecryptedIndexWriter::Fixed(index)) => {
                    if let Some(reusable) = encountered_chunks
                        .lock()
                        .unwrap()
                        .check_reusable(&info.digest)
                    {
                        if let Some(decrypted_digest) = reusable.decrypted_digest {
                            // already got the decrypted digest and chunk has been written,
                            // no need to process again
                            let size = info.size();
                            let start_offset = offset.fetch_add(size, Ordering::SeqCst);

                            index.lock().unwrap().add_chunk(
                                start_offset,
                                size as u32,
                                decrypted_digest,
                            )?;

                            return Ok::<_, Error>(());
                        }
                    }

                    let chunk_data = chunk_reader.read_chunk(&info.digest).await?;
                    let (chunk, digest) =
                        DataChunkBuilder::new(&chunk_data).compress(true).build()?;

                    let size = chunk_data.len() as u64;
                    let start_offset = offset.fetch_add(size, Ordering::SeqCst);

                    index
                        .lock()
                        .unwrap()
                        .add_chunk(start_offset, size as u32, &digest)?;

                    encountered_chunks
                        .lock()
                        .unwrap()
                        .mark_reusable(&info.digest, Some(digest));

                    (chunk, digest, size)
                }
                Some(DecryptedIndexWriter::Dynamic(index)) => {
                    if let Some(reusable) = encountered_chunks
                        .lock()
                        .unwrap()
                        .check_reusable(&info.digest)
                    {
                        if let Some(decrypted_digest) = reusable.decrypted_digest {
                            // already got the decrypted digest and chunk has been written,
                            // no need to process again
                            let size = info.size();
                            let start_offset = offset.fetch_add(size, Ordering::SeqCst);
                            let end_offset = start_offset + size;

                            index
                                .lock()
                                .unwrap()
                                .add_chunk(end_offset, decrypted_digest)?;

                            return Ok::<_, Error>(());
                        }
                    }

                    let chunk_data = chunk_reader.read_chunk(&info.digest).await?;
                    let (chunk, digest) =
                        DataChunkBuilder::new(&chunk_data).compress(true).build()?;

                    let size = chunk_data.len() as u64;
                    let start_offset = offset.fetch_add(size, Ordering::SeqCst);
                    let end_offset = start_offset + size;

                    index.lock().unwrap().add_chunk(end_offset, &digest)?;

                    encountered_chunks
                        .lock()
                        .unwrap()
                        .mark_reusable(&info.digest, Some(digest));

                    (chunk, digest, size)
                }
                None => {
                    {
                        // limit guard scope
                        let mut guard = encountered_chunks.lock().unwrap();
                        if let Some(reusable) = guard.check_reusable(&info.digest) {
                            if reusable.touched {
                                return Ok::<_, Error>(());
                            }
                            let chunk_exists = proxmox_async::runtime::block_in_place(|| {
                                target.cond_touch_chunk(&info.digest, false)
                            })?;
                            if chunk_exists {
                                guard.mark_touched(&info.digest, None);
                                //info!("chunk {} exists {}", pos, hex::encode(digest));
                                return Ok::<_, Error>(());
                            }
                        }
                        // mark before actually downloading the chunk, so this happens only once
                        guard.mark_reusable(&info.digest, None);
                        guard.mark_touched(&info.digest, None);
                    }

                    let chunk = chunk_reader.read_raw_chunk(&info.digest).await?;
                    (chunk, info.digest, info.size())
                }
            };
            let raw_size = chunk.raw_size() as usize;

            // decode, verify and write in a separate threads to maximize throughput
            proxmox_async::runtime::block_in_place(|| {
                verify_and_write_channel.send((chunk, digest, size))
            })?;

            bytes.fetch_add(raw_size, Ordering::SeqCst);
            chunk_count.fetch_add(1, Ordering::SeqCst);

            Ok(())
        })
    });

    if decrypted_index_writer.is_none() {
        stream
            .try_buffer_unordered(20)
            .try_for_each(|_res| futures::future::ok(()))
            .await?;
    } else {
        // must keep chunk order to correctly rewrite index file
        stream.try_for_each(|item| item).await?;
    }

    drop(verify_and_write_channel);

    tokio::task::spawn_blocking(|| verify_pool.complete()).await??;

    let elapsed = start_time.elapsed()?;

    let bytes = bytes.load(Ordering::SeqCst);
    let chunk_count = chunk_count.load(Ordering::SeqCst);

    log_sender
        .log(
            Level::INFO,
            format!(
                "{archive_prefix}: downloaded {} ({}/s)",
                HumanByte::from(bytes),
                HumanByte::new_binary(bytes as f64 / elapsed.as_secs_f64()),
            ),
        )
        .await?;

    Ok(SyncStats {
        chunk_count,
        bytes,
        elapsed,
        removed: None,
    })
}

fn verify_archive(info: &FileInfo, csum: &[u8; 32], size: u64) -> Result<(), Error> {
    if size != info.size {
        bail!(
            "wrong size for file '{}' ({} != {})",
            info.filename,
            info.size,
            size
        );
    }

    if csum != &info.csum {
        bail!("wrong checksum for file '{}'", info.filename);
    }

    Ok(())
}

/// Pulls a single file referenced by a manifest.
///
/// Pulling an archive consists of the following steps:
/// - Load archive file into tmp file
///   -- Load file into tmp file
///   -- Verify tmp file checksum
/// - if archive is an index, pull referenced chunks
/// - Rename tmp file into real path
async fn pull_single_archive<'a>(
    reader: Arc<dyn SyncSourceReader + 'a>,
    snapshot: &'a pbs_datastore::BackupDir,
    archive_info: &'a FileInfo,
    encountered_chunks: Arc<Mutex<EncounteredChunks>>,
    crypt_config: Option<Arc<CryptConfig>>,
    backend: &DatastoreBackend,
    log_sender: Arc<LogLineSender>,
    new_manifest: Option<Arc<Mutex<BackupManifest>>>,
) -> Result<SyncStats, Error> {
    let archive_name = &archive_info.filename;
    let mut path = snapshot.full_path();
    path.push(archive_name);

    let mut tmp_path = path.clone();
    tmp_path.set_extension("tmp");

    let mut tmp_dec_path = path.clone();
    tmp_dec_path.set_extension("tmpdec");

    let mut sync_stats = SyncStats::default();

    let archive_prefix = format!("{}/{archive_name}", snapshot.backup_time_string());

    log_sender
        .log(Level::INFO, format!("{archive_prefix}: sync archive"))
        .await?;

    reader
        .load_file_into(archive_name, &tmp_path)
        .await
        .with_context(|| archive_prefix.clone())?;

    let mut tmpfile = std::fs::OpenOptions::new()
        .read(true)
        .open(&tmp_path)
        .with_context(|| archive_prefix.clone())?;

    let add_to_decrypted_manifest = |csum, size| {
        if let Some(new_manifest) = new_manifest {
            let name = archive_name.as_str().try_into()?;
            // size is identical to original, encrypted index
            new_manifest
                .lock()
                .unwrap()
                .add_file(&name, size, csum, CryptMode::None)
                .with_context(|| archive_prefix.clone())
        } else {
            Ok(())
        }
    };

    match ArchiveType::from_path(archive_name)? {
        ArchiveType::DynamicIndex => {
            let index = DynamicIndexReader::new(tmpfile).map_err(|err| {
                format_err!("{archive_prefix}: unable to read dynamic index {tmp_path:?} - {err}")
            })?;
            let (csum, size) = index.compute_csum();
            verify_archive(archive_info, &csum, size).with_context(|| archive_prefix.clone())?;

            if crypt_config.is_none() && reader.skip_chunk_sync(snapshot.datastore().name()) {
                log_sender
                    .log(
                        Level::INFO,
                        format!("{archive_prefix}: skipping chunk sync for same datastore"),
                    )
                    .await?;
            } else {
                let new_index_writer = if crypt_config.is_some() {
                    let writer = DynamicIndexWriter::create(&tmp_dec_path)?;
                    Some(DecryptedIndexWriter::Dynamic(Arc::new(Mutex::new(writer))))
                } else {
                    None
                };
                let stats = pull_index_chunks(
                    reader
                        .chunk_reader(crypt_config.clone(), archive_info.crypt_mode)
                        .context("failed to get chunk reader")
                        .with_context(|| archive_prefix.clone())?,
                    snapshot.datastore().clone(),
                    index,
                    encountered_chunks,
                    backend,
                    &archive_prefix,
                    Arc::clone(&log_sender),
                    new_index_writer.clone(),
                )
                .await
                .with_context(|| archive_prefix.clone())?;
                if let Some(DecryptedIndexWriter::Dynamic(index)) = &new_index_writer {
                    let csum = index.lock().unwrap().close()?;
                    add_to_decrypted_manifest(csum, size)?;
                }

                sync_stats.add(stats);
            }
        }
        ArchiveType::FixedIndex => {
            let index = FixedIndexReader::new(tmpfile).map_err(|err| {
                format_err!("{archive_prefix}: unable to read fixed index '{tmp_path:?}' - {err}")
            })?;
            let (csum, size) = index.compute_csum();
            verify_archive(archive_info, &csum, size).with_context(|| archive_prefix.clone())?;

            if crypt_config.is_none() && reader.skip_chunk_sync(snapshot.datastore().name()) {
                log_sender
                    .log(
                        Level::INFO,
                        format!("{archive_prefix}: skipping chunk sync for same datastore"),
                    )
                    .await?;
            } else {
                let new_index_writer = if crypt_config.is_some() {
                    let writer = FixedIndexWriter::create(
                        &tmp_dec_path,
                        Some(size),
                        index.chunk_size as u32,
                    )?;
                    Some(DecryptedIndexWriter::Fixed(Arc::new(Mutex::new(writer))))
                } else {
                    None
                };
                let stats = pull_index_chunks(
                    reader
                        .chunk_reader(crypt_config.clone(), archive_info.crypt_mode)
                        .context("failed to get chunk reader")
                        .with_context(|| archive_prefix.clone())?,
                    snapshot.datastore().clone(),
                    index,
                    encountered_chunks,
                    backend,
                    &archive_prefix,
                    Arc::clone(&log_sender),
                    new_index_writer.clone(),
                )
                .await
                .with_context(|| archive_prefix.clone())?;
                if let Some(DecryptedIndexWriter::Fixed(index)) = &new_index_writer {
                    let csum = index.lock().unwrap().close()?;
                    add_to_decrypted_manifest(csum, size)?;
                }

                sync_stats.add(stats);
            }
        }
        ArchiveType::Blob => {
            proxmox_lang::try_block!({
                tmpfile.rewind()?;
                let (csum, size) = sha256(&mut tmpfile)?;
                verify_archive(archive_info, &csum, size)
            })
            .with_context(|| archive_prefix.clone())?;

            if crypt_config.is_some() {
                let crypt_config = crypt_config.clone();

                let tmp_dec_path = tmp_dec_path.clone();

                let (csum, size) = tokio::task::spawn_blocking(move || {
                    // must rewind again since after verifying cursor is at the end of the file
                    tmpfile.rewind()?;
                    let mut reader = DataBlobReader::new(tmpfile, crypt_config)?;
                    let mut dec_raw_data = Vec::new();
                    reader.read_to_end(&mut dec_raw_data)?;
                    reader.finish()?;

                    let blob = DataBlob::encode(&dec_raw_data, None, true)?;

                    let (csum, size) = sha256(&mut blob.raw_data())?;
                    replace_file(tmp_dec_path, blob.raw_data(), CreateOptions::new(), true)?;
                    Ok((csum, size))
                })
                .await?
                .map_err(|err: Error| format_err!("Failed when decrypting blob {path:?}: {err}"))
                .with_context(|| archive_prefix.clone())?;

                add_to_decrypted_manifest(csum, size)?;
            }
        }
    }
    let source_path = if crypt_config.is_some() {
        if let Err(err) = std::fs::remove_file(&tmp_path) {
            bail!("{archive_prefix}: Failed to remove temp. file {tmp_path:?} failed - {err}");
        }
        tmp_dec_path
    } else {
        tmp_path
    };
    if let Err(err) = std::fs::rename(&source_path, &path) {
        bail!("{archive_prefix}: Atomic rename file {path:?} failed - {err}");
    }

    backend
        .upload_index_to_backend(snapshot, archive_name)
        .await
        .with_context(|| archive_prefix.clone())?;

    Ok(sync_stats)
}

/// Actual implementation of pulling a snapshot.
///
/// Pulling a snapshot consists of the following steps:
/// - (Re)download the manifest
///   -- if it matches and is not corrupt, only download log and treat snapshot as already synced
/// - Iterate over referenced files
///   -- if file already exists, verify contents
///   -- if not, pull it from the remote
/// - Download log if not already existing
async fn pull_snapshot<'a>(
    params: Arc<PullParameters>,
    reader: Arc<dyn SyncSourceReader + 'a>,
    snapshot: &'a pbs_datastore::BackupDir,
    encountered_chunks: Arc<Mutex<EncounteredChunks>>,
    corrupt: bool,
    is_new: bool,
    log_sender: Arc<LogLineSender>,
) -> Result<SyncStats, Error> {
    let prefix = snapshot.backup_time_string().to_owned();
    if is_new {
        log_sender
            .log(Level::INFO, format!("{prefix}: start sync"))
            .await?;
    } else if corrupt {
        log_sender
            .log(
                Level::INFO,
                format!("re-sync snapshot {prefix} due to corruption"),
            )
            .await?;
    } else {
        log_sender
            .log(Level::INFO, format!("re-sync snapshot {prefix}"))
            .await?;
    }

    let mut sync_stats = SyncStats::default();
    let mut manifest_name = snapshot.full_path();
    manifest_name.push(MANIFEST_BLOB_NAME.as_ref());

    let mut client_log_name = snapshot.full_path();
    client_log_name.push(CLIENT_LOG_BLOB_NAME.as_ref());

    let mut tmp_manifest_name = manifest_name.clone();
    tmp_manifest_name.set_extension("tmp");
    let Some(tmp_manifest_blob) = reader
        .load_file_into(MANIFEST_BLOB_NAME.as_ref(), &tmp_manifest_name)
        .await
        .with_context(|| prefix.clone())?
    else {
        return Ok(sync_stats);
    };

    let backend = &params.target.backend;

    let fetch_log = async || {
        if !client_log_name.exists() {
            reader
                .try_download_client_log(&client_log_name)
                .await
                .with_context(|| prefix.clone())?;
            if client_log_name.exists() {
                if let DatastoreBackend::S3(s3_client) = backend {
                    let object_key = pbs_datastore::s3::object_key_from_path(
                        &snapshot.relative_path(),
                        CLIENT_LOG_BLOB_NAME.as_ref(),
                    )
                    .context("invalid archive object key")
                    .with_context(|| prefix.clone())?;

                    let data = tokio::fs::read(&client_log_name)
                        .await
                        .context("failed to read log file contents")
                        .with_context(|| prefix.clone())?;
                    let contents = hyper::body::Bytes::from(data);
                    let _is_duplicate = s3_client
                        .upload_replace_with_retry(object_key, contents)
                        .await
                        .context("failed to upload client log to s3 backend")
                        .with_context(|| prefix.clone())?;
                }
            }
        }
        Ok::<(), Error>(())
    };
    let cleanup = async || {
        log_sender
            .log(Level::INFO, format!("{prefix}: no data changes"))
            .await?;
        let _ = std::fs::remove_file(&tmp_manifest_name);
        Ok::<(), Error>(())
    };

    let existing_target_manifest = if manifest_name.exists() && !corrupt {
        let manifest_blob = proxmox_lang::try_block!({
            let mut manifest_file = std::fs::File::open(&manifest_name).map_err(|err| {
                format_err!("{prefix}: unable to open local manifest {manifest_name:?} - {err}")
            })?;

            let manifest_blob =
                DataBlob::load_from_reader(&mut manifest_file).with_context(|| prefix.clone())?;
            Ok(manifest_blob)
        })
        .map_err(|err: Error| {
            format_err!("{prefix}: unable to read local manifest {manifest_name:?} - {err}")
        })?;

        if manifest_blob.raw_data() == tmp_manifest_blob.raw_data() {
            fetch_log().await?;
            cleanup().await?;
            return Ok(sync_stats); // nothing changed
        }

        Some(BackupManifest::try_from(manifest_blob).with_context(|| prefix.clone())?)
    } else {
        None
    };

    let mut manifest_data = tmp_manifest_blob.raw_data().to_vec();
    let manifest = BackupManifest::try_from(tmp_manifest_blob).with_context(|| prefix.clone())?;

    if ignore_not_verified_or_encrypted(
        &manifest,
        snapshot.dir(),
        params.verified_only,
        params.encrypted_only,
    ) {
        if is_new {
            let path = snapshot.full_path();
            // safe to remove as locked by caller
            std::fs::remove_dir_all(&path).map_err(|err| {
                format_err!("removing temporary backup snapshot {path:?} failed - {err}")
            })?;
        }
        return Ok(sync_stats);
    }

    let (crypt_config, new_manifest) = match optionally_use_decryption_key(
        Arc::clone(&params),
        &manifest,
        existing_target_manifest.as_ref(),
        prefix.clone(),
        Arc::clone(&log_sender),
    )
    .await?
    {
        (None, false) => (None, None), // regular pull without decryption
        (Some(crypt_config), false) => {
            // decrypt while pull
            let new_manifest = Arc::new(Mutex::new(BackupManifest::new(snapshot.into())));
            (Some(crypt_config), Some(new_manifest))
        }
        (_, true) => {
            // nothing changed
            fetch_log().await?;
            cleanup().await?;
            return Ok(sync_stats);
        }
    };

    for item in manifest.files() {
        let mut path = snapshot.full_path();
        path.push(&item.filename);

        if !corrupt && path.exists() {
            let filename: BackupArchiveName = item
                .filename
                .as_str()
                .try_into()
                .with_context(|| prefix.clone())?;
            match filename.archive_type() {
                ArchiveType::DynamicIndex => {
                    let index = DynamicIndexReader::open(&path).with_context(|| prefix.clone())?;
                    let (csum, size) = index.compute_csum();
                    match manifest.verify_file(&filename, &csum, size) {
                        Ok(_) => continue,
                        Err(err) => {
                            log_sender
                                .log(
                                    Level::INFO,
                                    format!("{prefix}: detected changed file {path:?} - {err}"),
                                )
                                .await?;
                        }
                    }
                }
                ArchiveType::FixedIndex => {
                    let index = FixedIndexReader::open(&path).with_context(|| prefix.clone())?;
                    let (csum, size) = index.compute_csum();
                    match manifest.verify_file(&filename, &csum, size) {
                        Ok(_) => continue,
                        Err(err) => {
                            log_sender
                                .log(
                                    Level::INFO,
                                    format!("{prefix}: detected changed file {path:?} - {err}"),
                                )
                                .await?;
                        }
                    }
                }
                ArchiveType::Blob => {
                    let mut tmpfile = std::fs::File::open(&path).with_context(|| prefix.clone())?;
                    let (csum, size) = sha256(&mut tmpfile).with_context(|| prefix.clone())?;
                    match manifest.verify_file(&filename, &csum, size) {
                        Ok(_) => continue,
                        Err(err) => {
                            log_sender
                                .log(
                                    Level::INFO,
                                    format!("{prefix}: detected changed file {path:?} - {err}"),
                                )
                                .await?;
                        }
                    }
                }
            }
        }

        let stats = pull_single_archive(
            reader.clone(),
            snapshot,
            item,
            encountered_chunks.clone(),
            crypt_config.clone(),
            backend,
            Arc::clone(&log_sender),
            new_manifest.clone(),
        )
        .await?;
        sync_stats.add(stats);
    }

    if let Some(new_manifest) = new_manifest {
        let mut new_manifest = Arc::try_unwrap(new_manifest)
            .map_err(|_arc| {
                format_err!("failed to take ownership of still referenced new manifest")
            })?
            .into_inner()
            .unwrap();

        // copy over notes ecc, but drop encryption key fingerprint and verify state, to be
        // reverified independent from the sync.
        new_manifest.unprotected = manifest.unprotected.clone();
        if let Some(unprotected) = new_manifest.unprotected.as_object_mut() {
            unprotected.remove("change-detection-fingerprint");
            unprotected.remove("key-fingerprint");
            unprotected.remove("verify_state");
        } else {
            bail!("Encountered unexpected manifest without 'unprotected' section.");
        }

        let manifest_string = new_manifest.to_string(None)?;
        let manifest_blob = DataBlob::encode(manifest_string.as_bytes(), None, true)?;
        // update contents to be uploaded to backend
        manifest_data = manifest_blob.raw_data().to_vec();

        let mut tmp_manifest_file = OpenOptions::new()
            .write(true)
            .truncate(true) // clear pre-existing manifest content
            .open(&tmp_manifest_name)
            .await?;
        tmp_manifest_file.write_all(&manifest_data).await?;
        tmp_manifest_file.flush().await?;
        nix::unistd::fsync(tmp_manifest_file.as_raw_fd())?;
    }

    if let Err(err) = std::fs::rename(&tmp_manifest_name, &manifest_name) {
        bail!("{prefix}: Atomic rename file {manifest_name:?} failed - {err}");
    }
    if let DatastoreBackend::S3(s3_client) = backend {
        let object_key = pbs_datastore::s3::object_key_from_path(
            &snapshot.relative_path(),
            MANIFEST_BLOB_NAME.as_ref(),
        )
        .context("invalid manifest object key")?;

        let data = hyper::body::Bytes::from(manifest_data);
        let _is_duplicate = s3_client
            .upload_replace_with_retry(object_key, data)
            .await
            .context("failed to upload manifest to s3 backend")
            .with_context(|| prefix.clone())?;
    }

    fetch_log().await?;

    snapshot
        .cleanup_unreferenced_files(&manifest)
        .map_err(|err| format_err!("{prefix}: failed to cleanup unreferenced files - {err}"))?;

    Ok(sync_stats)
}

/// Check if the decryption key should be used to decrypt the snapshot during
/// pull based on given pull parameter, source and optionally already present
/// target manifest.
///
/// The boolean flag in the returned tuple indicates whether the pull can be
/// skipped altogether, since the already existing target is unchanged.
/// If decryption should happen, the matching decryption key is returned.
async fn optionally_use_decryption_key(
    params: Arc<PullParameters>,
    manifest: &BackupManifest,
    existing_target_manifest: Option<&BackupManifest>,
    prefix: String,
    log_sender: Arc<LogLineSender>,
) -> Result<(Option<Arc<CryptConfig>>, bool), Error> {
    let Some(key_fp) = manifest.fingerprint().with_context(|| prefix.clone())? else {
        return Ok((None, false)); // no fingerprint on source, regular pull
    };

    // check if source is encrypted or contents signed
    let encrypted = manifest
        .files()
        .iter()
        .all(|f| f.chunk_crypt_mode() == CryptMode::Encrypt);

    if let Some(existing_manifest) = existing_target_manifest {
        if let Some(existing_fingerprint) = existing_manifest.fingerprint()? {
            if existing_fingerprint == key_fp {
                let target_encrypted = existing_manifest
                    .files()
                    .iter()
                    .all(|f| f.chunk_crypt_mode() == CryptMode::Encrypt);
                if encrypted == target_encrypted {
                    // both sides are signed or encrypted with the same key, just resync
                    return Ok((None, false));
                }
            } else {
                // pre-existing local manifest for encrypted snapshot with key mismatch
                bail!("Local encrypted or signed snapshot with different key detected, refuse to sync");
            }
        }
    };

    // source got key fingerprint, expect contents to be signed or encrypted
    let Some((key_id, config)) = params
        .crypt_configs
        .iter()
        .find(|(_id, crypt_conf)| crypt_conf.fingerprint() == *key_fp.bytes())
    else {
        // all the other cases are handled above
        if encrypted && existing_target_manifest.is_some() {
            bail!("No matching key found, refusing sync");
        }

        // regular sync
        if !params.crypt_configs.is_empty() {
            log_sender
                .log(
                    Level::INFO,
                    format!("{prefix}: No matching key found, sync without decryption"),
                )
                .await?;
        }

        return Ok((None, false));
    };

    // check if source is encrypted or contents signed
    if !encrypted {
        log_sender
            .log(
                Level::WARN,
                format!("Snapshot not fully encrypted, sync as is despite matching key '{key_id}' with fingerprint {key_fp}"),
            )
            .await?;
        return Ok((None, false));
    }

    manifest
        .check_signature(config)
        .context("failed to check source manifest signature")
        .with_context(|| prefix.clone())?;

    let mut skip_resync = false;

    // avoid overwriting pre-existing target manifest
    if let Some(existing_manifest) = existing_target_manifest {
        if let Some(source_fp) = manifest
            .get_change_detection_fingerprint()
            .context("failed to parse change detection fingerprint of source manifest")
            .with_context(|| prefix.clone())?
        {
            // Stored fp is HMAC over the unencrypted source's protected fields; recompute
            // over the locally decrypted manifest, not the fresh encrypted remote one.
            let target_fp = existing_manifest
                .signature(config)
                .with_context(|| prefix.clone())?;
            if target_fp == *source_fp.bytes() {
                skip_resync = true;
            } else {
                bail!("Change detection fingerprint mismatch, refuse to continue!");
            }
        } else {
            bail!("No change detection fingerprint found, refuse to continue!");
        }
    }

    log_sender
        .log(
            Level::INFO,
            format!("Found matching key '{key_id}' with fingerprint {key_fp}, decrypt on pull"),
        )
        .await?;

    Ok((Some(Arc::clone(config)), skip_resync))
}

/// Pulls a `snapshot`, removing newly created ones on error, but keeping existing ones in any case.
///
/// The `reader` is configured to read from the source backup directory, while the
/// `snapshot` is pointing to the local datastore and target namespace.
async fn pull_snapshot_from<'a>(
    params: Arc<PullParameters>,
    reader: Arc<dyn SyncSourceReader + 'a>,
    snapshot: &'a pbs_datastore::BackupDir,
    encountered_chunks: Arc<Mutex<EncounteredChunks>>,
    corrupt: bool,
    log_sender: Arc<LogLineSender>,
) -> Result<SyncStats, Error> {
    let prefix = snapshot.backup_time_string().to_string();

    let (_path, is_new, _snap_lock) = snapshot
        .datastore()
        .create_locked_backup_dir(snapshot.backup_ns(), snapshot.as_ref())
        .context(prefix.clone())?;

    let result = pull_snapshot(
        params,
        reader,
        snapshot,
        encountered_chunks,
        corrupt,
        is_new,
        Arc::clone(&log_sender),
    )
    .await;

    if is_new {
        // Cleanup directory on error if snapshot was not present before
        match result {
            Err(err) => {
                if let Err(cleanup_err) = snapshot.datastore().remove_backup_dir(
                    snapshot.backup_ns(),
                    snapshot.as_ref(),
                    true,
                ) {
                    log_sender
                        .log(
                            Level::INFO,
                            format!("{prefix}: cleanup error - {cleanup_err}"),
                        )
                        .await?;
                }
                return Err(err);
            }
            Ok(_) => {
                log_sender
                    .log(Level::INFO, format!("{prefix}: sync done"))
                    .await?
            }
        }
    }

    result
}

/// Pulls a group according to `params`.
///
/// Pulling a group consists of the following steps:
/// - Query the list of snapshots available for this group in the source namespace on the remote
/// - Sort by snapshot time
/// - Get last snapshot timestamp on local datastore
/// - Iterate over list of snapshots
///   -- pull snapshot, unless it's not finished yet or older than last local snapshot
/// - (remove_vanished) list all local snapshots, remove those that don't exist on remote
///
/// Backwards-compat: if `source_namespace` is [None], only the group type and ID will be sent to the
/// remote when querying snapshots. This allows us to interact with old remotes that don't have
/// namespace support yet.
///
/// Permission checks:
/// - remote snapshot access is checked by remote (twice: query and opening the backup reader)
/// - local group owner is already checked by pull_store
async fn pull_group(
    params: Arc<PullParameters>,
    source_namespace: &BackupNamespace,
    group: &BackupGroup,
    shared_group_progress: Arc<SharedGroupProgress>,
    log_sender: Arc<LogLineSender>,
) -> Result<SyncStats, Error> {
    let prefix = format!("{group}");
    let mut already_synced_skip_info = SkipInfo::new(SkipReason::AlreadySynced);
    let mut transfer_last_skip_info = SkipInfo::new(SkipReason::TransferLast);

    let mut raw_list: Vec<SnapshotListItem> = params
        .source
        .list_backup_snapshots(source_namespace, group)
        .await?;
    raw_list = filter_out_in_progress(raw_list, Arc::clone(&log_sender)).await?;
    raw_list.sort_unstable_by_key(|a| a.backup.time);

    let target_ns = source_namespace.map_prefix(&params.source.get_ns(), &params.target.ns)?;

    let mut source_snapshots = HashSet::new();
    let last_sync_time = params
        .target
        .store
        .last_successful_backup(&target_ns, group)?
        .unwrap_or(i64::MIN);

    // Filter remote BackupDirs to include in pull
    // Also stores if the snapshot is corrupt (verification job failed)
    let list: Vec<(BackupDir, bool)> = raw_list
        .into_iter()
        .filter_map(|item| {
            if exclude_not_verified_or_encrypted(&item, params.verified_only, params.encrypted_only)
            {
                return None;
            }

            let dir = item.backup;
            source_snapshots.insert(dir.time);
            // If resync_corrupt is set, check if the corresponding local snapshot failed to
            // verification
            if params.resync_corrupt {
                let local_dir = params
                    .target
                    .store
                    .backup_dir(target_ns.clone(), dir.clone());
                if let Ok(local_dir) = local_dir {
                    if local_dir.full_path().exists() {
                        match local_dir.verify_state() {
                            Ok(Some(state)) => {
                                if state == VerifyState::Failed {
                                    return Some((dir, true));
                                }
                            }
                            Ok(None) => {
                                // The verify_state item was not found in the manifest, this means the
                                // snapshot is new.
                            }
                            Err(_) => {
                                // There was an error loading the manifest, probably better if we
                                // resync.
                                return Some((dir, true));
                            }
                        }
                    }
                }
            }
            Some((dir, false))
        })
        .collect();

    let total_amount = list.len();
    let cutoff = params
        .transfer_last
        .map(|count| total_amount.saturating_sub(count))
        .unwrap_or_default();

    let list: Vec<(BackupDir, bool)> = list
        .into_iter()
        .enumerate()
        .filter_map(|(pos, (dir, needs_resync))| {
            if params.resync_corrupt && needs_resync {
                return Some((dir, needs_resync));
            }
            // Note: the snapshot represented by `last_sync_time` might be missing its backup log
            // or post-backup verification state if those were not yet available during the last
            // sync run, always resync it
            if last_sync_time > dir.time {
                already_synced_skip_info.update(dir.time);
                return None;
            }
            if pos < cutoff && last_sync_time != dir.time {
                transfer_last_skip_info.update(dir.time);
                return None;
            }
            Some((dir, needs_resync))
        })
        .collect();

    if already_synced_skip_info.count > 0 {
        log_sender
            .log(Level::INFO, format!("{prefix}: {already_synced_skip_info}"))
            .await?;
        already_synced_skip_info.reset();
    }
    if transfer_last_skip_info.count > 0 {
        log_sender
            .log(Level::INFO, format!("{prefix}: {transfer_last_skip_info}"))
            .await?;
        transfer_last_skip_info.reset();
    }

    // start with 65536 chunks (up to 256 GiB)
    let encountered_chunks = Arc::new(Mutex::new(EncounteredChunks::with_capacity(1024 * 64)));

    let backup_group = params
        .target
        .store
        .backup_group(target_ns.clone(), group.clone());
    if let Some(info) = backup_group.last_backup(true).unwrap_or(None) {
        if let Err(err) = proxmox_lang::try_block!({
            let mut reusable_chunks = encountered_chunks.lock().unwrap();
            let _snapshot_guard = info
                .backup_dir
                .lock_shared()
                .with_context(|| format!("failed locking last backup of group {info:?}"))?;

            let (manifest, _) = info.backup_dir.load_manifest().with_context(|| {
                format!("failed loading manifest of last backup of group {info:?}")
            })?;

            match manifest.verify_state()? {
                Some(verify_state) if verify_state.state == VerifyState::Failed => (),
                _ => {
                    for file in manifest.files() {
                        let index: Box<dyn IndexFile> = match ArchiveType::from_path(&file.filename)
                        {
                            Ok(ArchiveType::FixedIndex) => {
                                let mut path = info.backup_dir.full_path();
                                path.push(&file.filename);
                                let index =
                                    params.target.store.open_fixed_reader(&path).with_context(
                                        || format!("failed loading fixed index {path:?}"),
                                    )?;
                                Box::new(index)
                            }
                            Ok(ArchiveType::DynamicIndex) => {
                                let mut path = info.backup_dir.full_path();
                                path.push(&file.filename);
                                let index = params
                                    .target
                                    .store
                                    .open_dynamic_reader(&path)
                                    .with_context(|| {
                                        format!("failed loading dynamic index {path:?}")
                                    })?;
                                Box::new(index)
                            }
                            _ => continue,
                        };

                        for pos in 0..index.index_count() {
                            let chunk_info = index.chunk_info(pos).unwrap();
                            reusable_chunks.mark_reusable(&chunk_info.digest, None);
                        }
                    }
                }
            }
            Ok::<(), Error>(())
        }) {
            log_sender
                .log(
                    Level::WARN,
                    format!("Failed to collect reusable chunk from last backup: {err:#?}"),
                )
                .await?;
        }
    }

    let mut local_progress = StoreProgress::new(shared_group_progress.total_groups());
    local_progress.group_snapshots = list.len() as u64;

    let mut sync_stats = SyncStats::default();

    for (pos, (from_snapshot, corrupt)) in list.into_iter().enumerate() {
        let to_snapshot = params
            .target
            .store
            .backup_dir(target_ns.clone(), from_snapshot.clone())?;

        let reader = params
            .source
            .reader(source_namespace, &from_snapshot)
            .await?;
        let result = pull_snapshot_from(
            Arc::clone(&params),
            reader,
            &to_snapshot,
            encountered_chunks.clone(),
            corrupt,
            Arc::clone(&log_sender),
        )
        .await;

        // Update done groups progress by other parallel running pulls
        local_progress.done_groups = shared_group_progress.load_done();
        local_progress.done_snapshots = pos as u64 + 1;
        if params.worker_threads.unwrap_or(1) == 1 {
            log_sender
                .log(Level::INFO, format!("percentage done: {local_progress}"))
                .await?;
        } else {
            log_sender
                .log(
                    Level::INFO,
                    format!(
                        "snapshot {}/{} within {group} is done, {}/{} groups done",
                        local_progress.done_snapshots,
                        local_progress.group_snapshots,
                        local_progress.done_groups,
                        local_progress.total_groups,
                    ),
                )
                .await?;
        }

        let stats = result?; // stop on error
        sync_stats.add(stats);
    }

    if params.remove_vanished {
        let group = params
            .target
            .store
            .backup_group(target_ns.clone(), group.clone());
        let local_list = group.list_backups()?;
        for info in local_list {
            let snapshot = info.backup_dir;
            if source_snapshots.contains(&snapshot.backup_time()) {
                continue;
            }
            if snapshot.is_protected() {
                log_sender
                    .log(
                        Level::INFO,
                        format!(
                            "{prefix}: don't delete vanished snapshot {} (protected)",
                            snapshot.dir(),
                        ),
                    )
                    .await?;
                continue;
            }
            log_sender
                .log(
                    Level::INFO,
                    format!("delete vanished snapshot {}", snapshot.dir()),
                )
                .await?;
            params
                .target
                .store
                .remove_backup_dir(&target_ns, snapshot.as_ref(), false)?;
            sync_stats.add(SyncStats::from(RemovedVanishedStats {
                snapshots: 1,
                groups: 0,
                namespaces: 0,
            }));
        }
    }

    if params.worker_threads.unwrap_or(1) > 1 {
        log_sender
            .log(
                Level::INFO,
                format!("group sync done: percentage done: {local_progress}"),
            )
            .await?;
    }

    Ok(sync_stats)
}

fn check_and_create_ns(params: &PullParameters, ns: &BackupNamespace) -> Result<bool, Error> {
    let mut created = false;
    let store_ns_str = print_store_and_ns(params.target.store.name(), ns);

    if !ns.is_root() && !params.target.store.namespace_path(ns).exists() {
        check_ns_modification_privs(params.target.store.name(), ns, &params.owner)
            .map_err(|err| format_err!("Creating {ns} not allowed - {err}"))?;

        let name = match ns.components().last() {
            Some(name) => name.to_owned(),
            None => {
                bail!("Failed to determine last component of namespace.");
            }
        };

        if let Err(err) = params.target.store.create_namespace(&ns.parent(), name) {
            bail!("sync into {store_ns_str} failed - namespace creation failed: {err}");
        }
        created = true;
    }

    check_ns_privs(
        params.target.store.name(),
        ns,
        &params.owner,
        PRIV_DATASTORE_BACKUP,
    )
    .map_err(|err| format_err!("sync into {store_ns_str} not allowed - {err}"))?;

    Ok(created)
}

fn check_and_remove_ns(params: &PullParameters, local_ns: &BackupNamespace) -> Result<bool, Error> {
    check_ns_modification_privs(params.target.store.name(), local_ns, &params.owner)
        .map_err(|err| format_err!("Removing {local_ns} not allowed - {err}"))?;

    // The outer loop (check_and_remove_vanished_ns) iterates children first, so we only need
    // to act on this one level.
    let (removed_all, _delete_stats) = params.target.store.remove_namespace(local_ns, true)?;

    Ok(removed_all)
}

fn check_and_remove_vanished_ns(
    params: &PullParameters,
    synced_ns: HashSet<BackupNamespace>,
) -> Result<(bool, RemovedVanishedStats), Error> {
    let mut errors = false;
    let mut removed_stats = RemovedVanishedStats::default();
    let user_info = CachedUserInfo::new()?;

    // clamp like remote does so that we don't list more than we can ever have synced.
    let max_depth = params
        .max_depth
        .unwrap_or_else(|| MAX_NAMESPACE_DEPTH - params.source.get_ns().depth());

    let mut local_ns_list: Vec<BackupNamespace> = params
        .target
        .store
        .recursive_iter_backup_ns_ok(params.target.ns.clone(), Some(max_depth))?
        .filter(|ns| {
            let user_privs =
                user_info.lookup_privs(&params.owner, &ns.acl_path(params.target.store.name()));
            user_privs & (PRIV_DATASTORE_BACKUP | PRIV_DATASTORE_AUDIT) != 0
        })
        .collect();

    // children first!
    local_ns_list.sort_unstable_by_key(|b| std::cmp::Reverse(b.name_len()));

    for local_ns in local_ns_list {
        if local_ns == params.target.ns {
            continue;
        }

        if synced_ns.contains(&local_ns) {
            continue;
        }

        if local_ns.is_root() {
            continue;
        }
        match check_and_remove_ns(params, &local_ns) {
            Ok(true) => {
                info!("Removed namespace {local_ns}");
                removed_stats.namespaces += 1;
            }
            Ok(false) => info!("Did not remove namespace {local_ns} - protected snapshots remain"),
            Err(err) => {
                info!("Failed to remove namespace {local_ns} - {err}");
                errors = true;
            }
        }
    }

    Ok((errors, removed_stats))
}

/// Pulls a store according to `params`.
///
/// Pulling a store consists of the following steps:
/// - Query list of namespaces on the remote
/// - Iterate list
///   -- create sub-NS if needed (and allowed)
///   -- attempt to pull each NS in turn
/// - (remove_vanished && max_depth > 0) remove sub-NS which are not or no longer available on the remote
///
/// Backwards compat: if the remote namespace is `/` and recursion is disabled, no namespace is
/// passed to the remote at all to allow pulling from remotes which have no notion of namespaces.
///
/// Permission checks:
/// - access to local datastore, namespace anchor and remote entry need to be checked at call site
/// - remote namespaces are filtered by remote
/// - creation and removal of sub-NS checked here
/// - access to sub-NS checked here
pub(crate) async fn pull_store(mut params: PullParameters) -> Result<SyncStats, Error> {
    // explicit create shared lock to prevent GC on newly created chunks
    let _shared_store_lock = params.target.store.try_shared_chunk_store_lock()?;
    let mut errors = false;

    let old_max_depth = params.max_depth;
    let mut namespaces = if params.source.get_ns().is_root() && old_max_depth == Some(0) {
        vec![params.source.get_ns()] // backwards compat - don't query remote namespaces!
    } else {
        params
            .source
            .list_namespaces(&mut params.max_depth, Box::new(|_| true))
            .await?
    };

    check_namespace_depth_limit(&params.source.get_ns(), &params.target.ns, &namespaces)?;

    errors |= old_max_depth != params.max_depth; // fail job if we switched to backwards-compat mode
    namespaces.sort_unstable_by_key(|a| a.name_len());

    let (mut groups, mut snapshots) = (0, 0);
    let mut synced_ns = HashSet::with_capacity(namespaces.len());
    let mut sync_stats = SyncStats::default();
    let params = Arc::new(params);

    for namespace in namespaces {
        let source_store_ns_str = print_store_and_ns(params.source.get_store(), &namespace);

        let target_ns = namespace.map_prefix(&params.source.get_ns(), &params.target.ns)?;
        let target_store_ns_str = print_store_and_ns(params.target.store.name(), &target_ns);

        info!("----");
        info!("Syncing {source_store_ns_str} into {target_store_ns_str}");

        synced_ns.insert(target_ns.clone());

        match check_and_create_ns(&params, &target_ns) {
            Ok(true) => info!("Created namespace {target_ns}"),
            Ok(false) => {}
            Err(err) => {
                info!("Cannot sync {source_store_ns_str} into {target_store_ns_str} - {err}");
                errors = true;
                continue;
            }
        }

        match pull_ns(&namespace, Arc::clone(&params)).await {
            Ok((ns_progress, ns_sync_stats, ns_errors)) => {
                errors |= ns_errors;

                sync_stats.add(ns_sync_stats);

                if params.max_depth != Some(0) {
                    groups += ns_progress.done_groups;
                    snapshots += ns_progress.done_snapshots;

                    let ns = if namespace.is_root() {
                        "root namespace".into()
                    } else {
                        format!("namespace {namespace}")
                    };
                    info!(
                        "Finished syncing {ns}, current progress: {groups} groups, {snapshots} snapshots"
                    );
                }
            }
            Err(err) => {
                errors = true;
                info!("Encountered errors while syncing namespace {namespace} - {err}");
            }
        };
    }

    if params.remove_vanished {
        let (has_errors, stats) = check_and_remove_vanished_ns(&params, synced_ns)?;
        errors |= has_errors;
        sync_stats.add(SyncStats::from(stats));
    }

    if errors {
        bail!("sync failed with some errors.");
    }

    Ok(sync_stats)
}

/// Get and exclusive lock on the backup group, check ownership matches
/// sync job owner and pull group contents.
async fn lock_and_pull_group(
    params: Arc<PullParameters>,
    group: &BackupGroup,
    namespace: &BackupNamespace,
    target_namespace: &BackupNamespace,
    shared_group_progress: Arc<SharedGroupProgress>,
    log_sender: Arc<LogLineSender>,
) -> Result<SyncStats, Error> {
    let (owner, _lock_guard) =
        match params
            .target
            .store
            .create_locked_backup_group(target_namespace, group, &params.owner)
        {
            Ok(res) => res,
            Err(err) => {
                log_sender
                    .log(
                        Level::INFO,
                        format!("sync group {group} failed - group lock failed: {err}"),
                    )
                    .await?;
                log_sender
                    .log(Level::INFO, "create_locked_backup_group failed".to_string())
                    .await?;
                return Err(err);
            }
        };

    if params.owner != owner {
        // only the owner is allowed to create additional snapshots
        log_sender
            .log(
                Level::INFO,
                format!(
                    "sync group {group} failed - owner check failed ({} != {owner})",
                    params.owner,
                ),
            )
            .await?;
        return Err(format_err!("owner check failed"));
    }

    match pull_group(
        params,
        namespace,
        group,
        shared_group_progress,
        Arc::clone(&log_sender),
    )
    .await
    {
        Ok(stats) => Ok(stats),
        Err(err) => {
            log_sender
                .log(Level::INFO, format!("sync group {group} failed - {err:#}"))
                .await?;
            Err(err)
        }
    }
}

/// Pulls a namespace according to `params`.
///
/// Pulling a namespace consists of the following steps:
/// - Query list of groups on the remote (in `source_ns`)
/// - Filter list according to configured group filters
/// - Iterate list and attempt to pull each group in turn
/// - (remove_vanished) remove groups with matching owner and matching the configured group filters which are
///   not or no longer available on the remote
///
/// Permission checks:
/// - remote namespaces are filtered by remote
/// - owner check for vanished groups done here
async fn pull_ns(
    namespace: &BackupNamespace,
    params: Arc<PullParameters>,
) -> Result<(StoreProgress, SyncStats, bool), Error> {
    let list: Vec<BackupGroup> = params.source.list_groups(namespace, &params.owner).await?;

    let unfiltered_count = list.len();
    let mut list: Vec<BackupGroup> = list
        .into_iter()
        .filter(|group| group.apply_filters(&params.group_filter))
        .collect();

    list.sort_unstable();

    info!(
        "Found {} groups to sync (out of {unfiltered_count} total)",
        list.len()
    );

    let mut errors = false;

    let mut new_groups = HashSet::new();
    for group in list.iter() {
        new_groups.insert(group.clone());
    }

    let mut progress = StoreProgress::new(list.len() as u64);
    let mut sync_stats = SyncStats::default();

    let target_ns = namespace.map_prefix(&params.source.get_ns(), &params.target.ns)?;

    let shared_group_progress = Arc::new(SharedGroupProgress::with_total_groups(list.len()));
    let mut group_workers = BoundedJoinSet::new(params.worker_threads.unwrap_or(1));

    let (buffered_lines, max_duration) = if params.worker_threads.unwrap_or(1) > 1 {
        (5, Duration::from_secs(1))
    } else {
        (0, Duration::ZERO)
    };
    let sender_builder = BufferedLogger::new(buffered_lines, max_duration);

    let mut process_results = |results| {
        for result in results {
            progress.done_groups = shared_group_progress.increment_done();
            match result {
                Ok(stats) => {
                    sync_stats.add(stats);
                }
                Err(_err) => errors = true,
            }
        }
    };

    for group in list.into_iter() {
        let namespace = namespace.clone();
        let target_ns = target_ns.clone();
        let params = Arc::clone(&params);
        let group_progress_cloned = Arc::clone(&shared_group_progress);
        let log_sender: Arc<LogLineSender> =
            Arc::new(sender_builder.sender_with_label(group.to_string()));
        let results = group_workers
            .spawn_task(async move {
                let result = lock_and_pull_group(
                    Arc::clone(&params),
                    &group,
                    &namespace,
                    &target_ns,
                    group_progress_cloned,
                    Arc::clone(&log_sender),
                )
                .await;
                let _ = log_sender.flush().await;
                result
            })
            .await
            .map_err(|err| format_err!("failed to join on worker task: {err:#}"))?;
        process_results(results);
    }

    while let Some(result) = group_workers.join_next().await {
        let result = result.map_err(|err| format_err!("failed to join on worker task: {err:#}"))?;
        process_results(vec![result]);
    }

    // Force flush of pending messages
    sender_builder.close().await?;

    if params.remove_vanished {
        let result: Result<(), Error> = proxmox_lang::try_block!({
            for local_group in params.target.store.iter_backup_groups(target_ns.clone())? {
                let local_group = local_group?;
                let local_group = local_group.group();
                if new_groups.contains(local_group) {
                    continue;
                }
                let owner = params.target.store.get_owner(&target_ns, local_group)?;
                if check_backup_owner(&owner, &params.owner).is_err() {
                    continue;
                }
                if !local_group.apply_filters(&params.group_filter) {
                    continue;
                }
                info!("Delete vanished group '{local_group}'");
                let delete_stats_result = params
                    .target
                    .store
                    .remove_backup_group(&target_ns, local_group);

                match delete_stats_result {
                    Ok(stats) => {
                        if !stats.all_removed() {
                            info!("Kept some protected snapshots of group '{local_group}'");
                            sync_stats.add(SyncStats::from(RemovedVanishedStats {
                                snapshots: stats.removed_snapshots(),
                                groups: 0,
                                namespaces: 0,
                            }));
                        } else {
                            sync_stats.add(SyncStats::from(RemovedVanishedStats {
                                snapshots: stats.removed_snapshots(),
                                groups: 1,
                                namespaces: 0,
                            }));
                        }
                    }
                    Err(err) => {
                        info!("{err}");
                        errors = true;
                    }
                }
            }
            Ok(())
        });
        if let Err(err) = result {
            info!("Error during cleanup: {err}");
            errors = true;
        };
    }

    Ok((progress, sync_stats, errors))
}

struct EncounteredChunkInfo {
    reusable: bool,
    touched: bool,
    decrypted_digest: Option<[u8; 32]>,
}

/// Store the state of encountered chunks, tracking if they can be reused for the
/// index file currently being pulled and if the chunk has already been touched
/// during this sync.
struct EncounteredChunks {
    chunk_set: HashMap<[u8; 32], EncounteredChunkInfo>,
}

/// Propertires of a reusable chunk
struct ReusableEncounteredChunk<'a> {
    touched: bool,
    decrypted_digest: Option<&'a [u8; 32]>,
}

impl EncounteredChunks {
    /// Create a new instance with preallocated capacity
    fn with_capacity(capacity: usize) -> Self {
        Self {
            chunk_set: HashMap::with_capacity(capacity),
        }
    }

    /// Check if the current state allows to reuse this chunk and if so,
    /// if the chunk has already been touched.
    fn check_reusable(&self, digest: &[u8; 32]) -> Option<ReusableEncounteredChunk<'_>> {
        if let Some(chunk_info) = self.chunk_set.get(digest) {
            if !chunk_info.reusable {
                None
            } else {
                Some(ReusableEncounteredChunk {
                    touched: chunk_info.touched,
                    decrypted_digest: chunk_info.decrypted_digest.as_ref(),
                })
            }
        } else {
            None
        }
    }

    /// Mark chunk as reusable, inserting it as un-touched if not present.
    ///
    /// If the mapping already contains the digest, set the decrypted digest only
    /// if not already set previously.
    fn mark_reusable(&mut self, digest: &[u8; 32], decrypted_digest: Option<[u8; 32]>) {
        match self.chunk_set.entry(*digest) {
            Entry::Occupied(mut occupied) => {
                let chunk_info = occupied.get_mut();
                chunk_info.reusable = true;
                if chunk_info.decrypted_digest.is_none() {
                    chunk_info.decrypted_digest = decrypted_digest;
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(EncounteredChunkInfo {
                    reusable: true,
                    touched: false,
                    decrypted_digest,
                });
            }
        }
    }

    /// Mark chunk as touched during this sync, inserting it as not reusable
    /// but touched if not present.
    ///
    /// If the mapping already contains the digest, set the decrypted digest only
    /// if not already set previously.
    fn mark_touched(&mut self, digest: &[u8; 32], decrypted_digest: Option<[u8; 32]>) {
        match self.chunk_set.entry(*digest) {
            Entry::Occupied(mut occupied) => {
                let chunk_info = occupied.get_mut();
                chunk_info.touched = true;
                if chunk_info.decrypted_digest.is_none() {
                    chunk_info.decrypted_digest = decrypted_digest;
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(EncounteredChunkInfo {
                    reusable: false,
                    touched: true,
                    decrypted_digest,
                });
            }
        }
    }
}

#[derive(Clone)]
enum DecryptedIndexWriter {
    Fixed(Arc<Mutex<FixedIndexWriter>>),
    Dynamic(Arc<Mutex<DynamicIndexWriter>>),
}
