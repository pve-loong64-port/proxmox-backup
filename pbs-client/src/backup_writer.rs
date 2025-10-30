use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{bail, format_err, Error};
use futures::future::{self, AbortHandle, Either, FutureExt, TryFutureExt};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use openssl::sha::Sha256;
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

use pbs_api_types::{
    ArchiveType, BackupArchiveName, BackupDir, BackupNamespace, CATALOG_NAME, MANIFEST_BLOB_NAME,
};
use pbs_datastore::data_blob::{ChunkInfo, DataBlob, DataChunkBuilder};
use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_datastore::fixed_index::FixedIndexReader;
use pbs_datastore::index::IndexFile;
use pbs_datastore::manifest::BackupManifest;
use pbs_datastore::PROXMOX_BACKUP_PROTOCOL_ID_V1;
use pbs_tools::crypt_config::CryptConfig;

use proxmox_human_byte::HumanByte;
use proxmox_log::{debug, enabled, info, trace, warn, Level};
use proxmox_time::TimeSpan;

use super::backup_stats::{BackupStats, UploadCounters, UploadStats};
use super::inject_reused_chunks::{InjectChunks, InjectReusedChunks, InjectedChunksInfo};
use super::merge_known_chunks::{MergeKnownChunks, MergedChunkInfo};

use super::{H2Client, HttpClient};

pub struct BackupWriter {
    h2: H2Client,
    abort: AbortHandle,
    crypt_config: Option<Arc<CryptConfig>>,
}

impl Drop for BackupWriter {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// Options for uploading blobs/streams to the server
#[derive(Default, Clone)]
pub struct UploadOptions {
    pub previous_manifest: Option<Arc<BackupManifest>>,
    pub compress: bool,
    pub encrypt: bool,
    pub fixed_size: Option<u64>,
}

struct ChunkUploadResponse {
    future: h2::client::ResponseFuture,
    size: usize,
}

type UploadQueueSender = mpsc::Sender<(MergedChunkInfo, Option<ChunkUploadResponse>)>;
type UploadResultReceiver = oneshot::Receiver<Result<(), Error>>;

/// Additional configuration options for BackupWriter instance
pub struct BackupWriterOptions<'a> {
    /// Target datastore
    pub datastore: &'a str,
    /// Target namespace
    pub ns: &'a BackupNamespace,
    /// Target snapshot
    pub backup: &'a BackupDir,
    /// Crypto configuration
    pub crypt_config: Option<Arc<CryptConfig>>,
    /// Run in debug mode
    pub debug: bool,
    /// Start benchmark
    pub benchmark: bool,
    /// Skip datastore cache
    pub no_cache: bool,
}

impl BackupWriter {
    fn new(h2: H2Client, abort: AbortHandle, crypt_config: Option<Arc<CryptConfig>>) -> Arc<Self> {
        Arc::new(Self {
            h2,
            abort,
            crypt_config,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        client: &HttpClient,
        writer_options: BackupWriterOptions<'_>,
    ) -> Result<Arc<BackupWriter>, Error> {
        let mut param = json!({
            "backup-type": writer_options.backup.ty(),
            "backup-id": writer_options.backup.id(),
            "backup-time": writer_options.backup.time,
            "store": writer_options.datastore,
            "debug": writer_options.debug,
            "benchmark": writer_options.benchmark,
        });
        if writer_options.no_cache {
            param["no-cache"] = serde_json::to_value(writer_options.no_cache)?;
        }

        if !writer_options.ns.is_root() {
            param["ns"] = serde_json::to_value(writer_options.ns)?;
        }

        let req = HttpClient::request_builder(
            client.server(),
            client.port(),
            "GET",
            "/api2/json/backup",
            Some(param),
        )
        .unwrap();

        let (h2, abort) = client
            .start_h2_connection(req, String::from(PROXMOX_BACKUP_PROTOCOL_ID_V1!()))
            .await?;

        Ok(BackupWriter::new(h2, abort, writer_options.crypt_config))
    }

    pub async fn get(&self, path: &str, param: Option<Value>) -> Result<Value, Error> {
        self.h2.get(path, param).await
    }

    pub async fn put(&self, path: &str, param: Option<Value>) -> Result<Value, Error> {
        self.h2.put(path, param).await
    }

    pub async fn post(&self, path: &str, param: Option<Value>) -> Result<Value, Error> {
        self.h2.post(path, param).await
    }

    pub async fn upload_post(
        &self,
        path: &str,
        param: Option<Value>,
        content_type: &str,
        data: Vec<u8>,
    ) -> Result<Value, Error> {
        self.h2
            .upload("POST", path, param, content_type, data)
            .await
    }

    pub async fn send_upload_request(
        &self,
        method: &str,
        path: &str,
        param: Option<Value>,
        content_type: &str,
        data: Vec<u8>,
    ) -> Result<h2::client::ResponseFuture, Error> {
        let request =
            H2Client::request_builder("localhost", method, path, param, Some(content_type))
                .unwrap();
        let response_future = self
            .h2
            .send_request(request, Some(bytes::Bytes::from(data.clone())))
            .await?;
        Ok(response_future)
    }

    pub async fn upload_put(
        &self,
        path: &str,
        param: Option<Value>,
        content_type: &str,
        data: Vec<u8>,
    ) -> Result<Value, Error> {
        self.h2.upload("PUT", path, param, content_type, data).await
    }

    pub async fn finish(self: Arc<Self>) -> Result<(), Error> {
        let h2 = self.h2.clone();

        h2.post("finish", None)
            .map_ok(move |_| {
                self.abort.abort();
            })
            .await
    }

    pub fn cancel(&self) {
        self.abort.abort();
    }

    pub async fn upload_blob<R: std::io::Read>(
        &self,
        mut reader: R,
        file_name: &str,
    ) -> Result<BackupStats, Error> {
        let start_time = Instant::now();
        let mut raw_data = Vec::new();
        // fixme: avoid loading into memory
        reader.read_to_end(&mut raw_data)?;

        let csum = openssl::sha::sha256(&raw_data);
        let param = json!({"encoded-size": raw_data.len(), "file-name": file_name });
        let size = raw_data.len() as u64;
        let _value = self
            .h2
            .upload(
                "POST",
                "blob",
                Some(param),
                "application/octet-stream",
                raw_data,
            )
            .await?;
        Ok(BackupStats {
            size,
            csum,
            duration: start_time.elapsed(),
            chunk_count: 0,
        })
    }

    pub async fn upload_blob_from_data(
        &self,
        data: Vec<u8>,
        file_name: &str,
        options: UploadOptions,
    ) -> Result<BackupStats, Error> {
        let start_time = Instant::now();
        let blob = match (options.encrypt, &self.crypt_config) {
            (false, _) => DataBlob::encode(&data, None, options.compress)?,
            (true, None) => bail!("requested encryption without a crypt config"),
            (true, Some(crypt_config)) => {
                DataBlob::encode(&data, Some(crypt_config), options.compress)?
            }
        };

        let raw_data = blob.into_inner();
        let size = raw_data.len() as u64;

        let csum = openssl::sha::sha256(&raw_data);
        let param = json!({"encoded-size": size, "file-name": file_name });
        let _value = self
            .h2
            .upload(
                "POST",
                "blob",
                Some(param),
                "application/octet-stream",
                raw_data,
            )
            .await?;
        Ok(BackupStats {
            size,
            csum,
            duration: start_time.elapsed(),
            chunk_count: 0,
        })
    }

    pub async fn upload_blob_from_file<P: AsRef<std::path::Path>>(
        &self,
        src_path: P,
        file_name: &str,
        options: UploadOptions,
    ) -> Result<BackupStats, Error> {
        let src_path = src_path.as_ref();

        let mut file = tokio::fs::File::open(src_path)
            .await
            .map_err(|err| format_err!("unable to open file {:?} - {}", src_path, err))?;

        let mut contents = Vec::new();

        file.read_to_end(&mut contents)
            .await
            .map_err(|err| format_err!("unable to read file {:?} - {}", src_path, err))?;

        self.upload_blob_from_data(contents, file_name, options)
            .await
    }

    /// Upload chunks and index
    pub async fn upload_index_chunk_info(
        &self,
        archive_name: &BackupArchiveName,
        stream: impl Stream<Item = Result<MergedChunkInfo, Error>>,
        options: UploadOptions,
    ) -> Result<BackupStats, Error> {
        let mut param = json!({ "archive-name": archive_name });
        let prefix = if let Some(size) = options.fixed_size {
            param["size"] = size.into();
            "fixed"
        } else {
            "dynamic"
        };

        if options.encrypt && self.crypt_config.is_none() {
            bail!("requested encryption without a crypt config");
        }

        let wid = self
            .h2
            .post(&format!("{prefix}_index"), Some(param))
            .await?
            .as_u64()
            .unwrap();

        let mut counters = UploadCounters::new();
        let counters_readonly = counters.clone();

        let is_fixed_chunk_size = prefix == "fixed";

        let index_csum = Arc::new(Mutex::new(Some(Sha256::new())));
        let index_csum_2 = index_csum.clone();

        let stream = stream
            .and_then(move |mut merged_chunk_info| {
                match merged_chunk_info {
                    MergedChunkInfo::New(ref chunk_info) => {
                        let chunk_len = chunk_info.chunk_len;
                        let offset =
                            counters.add_new_chunk(chunk_len as usize, chunk_info.chunk.raw_size());
                        let end_offset = offset as u64 + chunk_len;
                        let mut guard = index_csum.lock().unwrap();
                        let csum = guard.as_mut().unwrap();
                        if !is_fixed_chunk_size {
                            csum.update(&end_offset.to_le_bytes());
                        }
                        csum.update(&chunk_info.digest);
                    }
                    MergedChunkInfo::Known(ref mut known_chunk_list) => {
                        for (chunk_len, digest) in known_chunk_list {
                            let offset = counters.add_known_chunk(*chunk_len as usize);
                            let end_offset = offset as u64 + *chunk_len;
                            let mut guard = index_csum.lock().unwrap();
                            let csum = guard.as_mut().unwrap();
                            if !is_fixed_chunk_size {
                                csum.update(&end_offset.to_le_bytes());
                            }
                            csum.update(digest);
                            // Replace size with offset, expected by further stream
                            *chunk_len = offset as u64;
                        }
                    }
                }
                future::ok(merged_chunk_info)
            })
            .merge_known_chunks();

        let upload_stats = Self::upload_merged_chunk_stream(
            self.h2.clone(),
            wid,
            archive_name,
            prefix,
            stream,
            index_csum_2,
            counters_readonly,
        )
        .await?;

        let param = json!({
            "wid": wid ,
            "chunk-count": upload_stats.chunk_count,
            "size": upload_stats.size,
            "csum": hex::encode(upload_stats.csum),
        });
        let _value = self
            .h2
            .post(&format!("{prefix}_close"), Some(param))
            .await?;

        Ok(upload_stats.to_backup_stats())
    }

    pub async fn upload_stream(
        &self,
        archive_name: &BackupArchiveName,
        stream: impl Stream<Item = Result<bytes::BytesMut, Error>>,
        options: UploadOptions,
        injections: Option<std::sync::mpsc::Receiver<InjectChunks>>,
    ) -> Result<BackupStats, Error> {
        let known_chunks = Arc::new(Mutex::new(HashSet::new()));

        let mut param = json!({ "archive-name": archive_name });
        let prefix = if let Some(size) = options.fixed_size {
            param["size"] = size.into();
            "fixed"
        } else {
            "dynamic"
        };

        if options.encrypt && self.crypt_config.is_none() {
            bail!("requested encryption without a crypt config");
        }

        let index_path = format!("{prefix}_index");
        let close_path = format!("{prefix}_close");

        if let Some(manifest) = options.previous_manifest {
            if !manifest
                .files()
                .iter()
                .any(|file| file.filename == archive_name.as_ref())
            {
                info!("Previous manifest does not contain an archive called '{archive_name}', skipping download..");
            } else {
                // try, but ignore errors
                match archive_name.archive_type() {
                    ArchiveType::FixedIndex => {
                        if let Err(err) = self
                            .download_previous_fixed_index(
                                archive_name,
                                &manifest,
                                known_chunks.clone(),
                            )
                            .await
                        {
                            warn!("Error downloading .fidx from previous manifest: {}", err);
                        }
                    }
                    ArchiveType::DynamicIndex => {
                        if let Err(err) = self
                            .download_previous_dynamic_index(
                                archive_name,
                                &manifest,
                                known_chunks.clone(),
                            )
                            .await
                        {
                            warn!("Error downloading .didx from previous manifest: {}", err);
                        }
                    }
                    _ => { /* do nothing */ }
                }
            }
        }

        let wid = self
            .h2
            .post(&index_path, Some(param))
            .await?
            .as_u64()
            .unwrap();

        let upload_stats = Self::upload_chunk_info_stream(
            self.h2.clone(),
            wid,
            stream,
            prefix,
            known_chunks.clone(),
            if options.encrypt {
                self.crypt_config.clone()
            } else {
                None
            },
            options.compress,
            injections,
            archive_name,
        )
        .await?;

        let size_dirty = upload_stats.size - upload_stats.size_reused;
        let size: HumanByte = upload_stats.size.into();
        let archive = if enabled!(Level::DEBUG) {
            archive_name.to_string()
        } else {
            archive_name.without_type_extension()
        };

        if upload_stats.chunk_injected > 0 {
            info!(
                "{archive}: reused {} from previous snapshot for unchanged files ({} chunks)",
                HumanByte::from(upload_stats.size_injected),
                upload_stats.chunk_injected,
            );
        }

        if *archive_name != *CATALOG_NAME {
            let speed: HumanByte =
                ((size_dirty * 1_000_000) / (upload_stats.duration.as_micros() as usize)).into();
            let size_dirty: HumanByte = size_dirty.into();
            let size_compressed: HumanByte = upload_stats.size_compressed.into();
            info!(
                "{archive}: had to backup {size_dirty} of {size} (compressed {size_compressed}) in {:.2} s (average {speed}/s)",
                upload_stats.duration.as_secs_f64()
            );
        } else {
            info!("Uploaded backup catalog ({})", size);
        }

        if upload_stats.size_reused > 0 && upload_stats.size > 1024 * 1024 {
            let reused_percent = upload_stats.size_reused as f64 * 100. / upload_stats.size as f64;
            let reused: HumanByte = upload_stats.size_reused.into();
            info!(
                "{}: backup was done incrementally, reused {} ({:.1}%)",
                archive, reused, reused_percent
            );
        }
        if enabled!(Level::DEBUG) && upload_stats.chunk_count > 0 {
            debug!(
                "{}: Reused {} from {} chunks.",
                archive, upload_stats.chunk_reused, upload_stats.chunk_count
            );
            debug!(
                "{}: Average chunk size was {}.",
                archive,
                HumanByte::from(upload_stats.size / upload_stats.chunk_count)
            );
            debug!(
                "{}: Average time per request: {} microseconds.",
                archive,
                (upload_stats.duration.as_micros()) / (upload_stats.chunk_count as u128)
            );
        }

        let param = json!({
            "wid": wid ,
            "chunk-count": upload_stats.chunk_count,
            "size": upload_stats.size,
            "csum": hex::encode(upload_stats.csum),
        });
        let _value = self.h2.post(&close_path, Some(param)).await?;
        Ok(upload_stats.to_backup_stats())
    }

    fn response_queue() -> (
        mpsc::Sender<h2::client::ResponseFuture>,
        oneshot::Receiver<Result<(), Error>>,
    ) {
        let (verify_queue_tx, verify_queue_rx) = mpsc::channel(100);
        let (verify_result_tx, verify_result_rx) = oneshot::channel();

        // FIXME: check if this works as expected as replacement for the combinator below?
        // tokio::spawn(async move {
        //     let result: Result<(), Error> = (async move {
        //         while let Some(response) = verify_queue_rx.recv().await {
        //             match H2Client::h2api_response(response.await?).await {
        //                 Ok(result) => println!("RESPONSE: {:?}", result),
        //                 Err(err) => bail!("pipelined request failed: {}", err),
        //             }
        //         }
        //         Ok(())
        //     }).await;
        //     let _ignore_closed_channel = verify_result_tx.send(result);
        // });
        // old code for reference?
        tokio::spawn(
            ReceiverStream::new(verify_queue_rx)
                .map(Ok::<_, Error>)
                .try_for_each(move |response: h2::client::ResponseFuture| {
                    response
                        .map_err(Error::from)
                        .and_then(H2Client::h2api_response)
                        .map_ok(move |result| debug!("RESPONSE: {:?}", result))
                        .map_err(|err| format_err!("pipelined request failed: {}", err))
                })
                .map(|result| {
                    let _ignore_closed_channel = verify_result_tx.send(result);
                }),
        );

        (verify_queue_tx, verify_result_rx)
    }

    fn append_chunk_queue(
        h2: H2Client,
        wid: u64,
        path: String,
        uploaded: Arc<AtomicUsize>,
    ) -> (UploadQueueSender, UploadResultReceiver) {
        let (verify_queue_tx, verify_queue_rx) = mpsc::channel(64);
        let (verify_result_tx, verify_result_rx) = oneshot::channel();

        // FIXME: async-block-ify this code!
        tokio::spawn(
            ReceiverStream::new(verify_queue_rx)
                .map(Ok::<_, Error>)
                .and_then(move |(merged_chunk_info, response): (MergedChunkInfo, Option<ChunkUploadResponse>)| {
                    match (response, merged_chunk_info) {
                        (Some(response), MergedChunkInfo::Known(list)) => {
                            Either::Left(
                                response
                                    .future
                                    .map_err(Error::from)
                                    .and_then(H2Client::h2api_response)
                                    .and_then({
                                        let uploaded = uploaded.clone();
                                        move |_result| {
                                            // account for uploaded bytes for progress output
                                            uploaded.fetch_add(response.size, Ordering::SeqCst);
                                            future::ok(MergedChunkInfo::Known(list))
                                        }
                                    })
                            )
                        }
                        (None, MergedChunkInfo::Known(list)) => {
                            Either::Right(future::ok(MergedChunkInfo::Known(list)))
                        }
                        _ => unreachable!(),
                    }
                })
                .merge_known_chunks()
                .and_then(move |merged_chunk_info| {
                    match merged_chunk_info {
                        MergedChunkInfo::Known(chunk_list) => {
                            let mut digest_list = vec![];
                            let mut offset_list = vec![];
                            for (offset, digest) in chunk_list {
                                digest_list.push(hex::encode(digest));
                                offset_list.push(offset);
                            }
                            debug!("append chunks list len ({})", digest_list.len());
                            let param = json!({ "wid": wid, "digest-list": digest_list, "offset-list": offset_list });
                            let request = H2Client::request_builder("localhost", "PUT", &path, None, Some("application/json")).unwrap();
                            let param_data = bytes::Bytes::from(param.to_string().into_bytes());
                            let upload_data = Some(param_data);
                            h2.send_request(request, upload_data)
                                .and_then(move |response| {
                                    response
                                        .map_err(Error::from)
                                        .and_then(H2Client::h2api_response)
                                        .map_ok(|_| ())
                                })
                                .map_err(|err| format_err!("pipelined request failed: {}", err))
                        }
                        _ => unreachable!(),
                    }
                })
                .try_for_each(|_| future::ok(()))
                .map(|result| {
                      let _ignore_closed_channel = verify_result_tx.send(result);
                })
        );

        (verify_queue_tx, verify_result_rx)
    }

    pub async fn download_previous_fixed_index(
        &self,
        archive_name: &BackupArchiveName,
        manifest: &BackupManifest,
        known_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
    ) -> Result<FixedIndexReader, Error> {
        let mut tmpfile = crate::tools::create_tmp_file()?;

        let param = json!({ "archive-name": archive_name });
        self.h2
            .download("previous", Some(param), &mut tmpfile)
            .await?;

        let index = FixedIndexReader::new(tmpfile).map_err(|err| {
            format_err!("unable to read fixed index '{}' - {}", archive_name, err)
        })?;
        // Note: do not use values stored in index (not trusted) - instead, computed them again
        let (csum, size) = index.compute_csum();
        manifest.verify_file(archive_name, &csum, size)?;

        // add index chunks to known chunks
        let mut known_chunks = known_chunks.lock().unwrap();
        for i in 0..index.index_count() {
            known_chunks.insert(*index.index_digest(i).unwrap());
        }

        debug!(
            "{}: known chunks list length is {}",
            archive_name,
            index.index_count()
        );

        Ok(index)
    }

    pub async fn download_previous_dynamic_index(
        &self,
        archive_name: &BackupArchiveName,
        manifest: &BackupManifest,
        known_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
    ) -> Result<DynamicIndexReader, Error> {
        let mut tmpfile = crate::tools::create_tmp_file()?;

        let param = json!({ "archive-name": archive_name });
        self.h2
            .download("previous", Some(param), &mut tmpfile)
            .await?;

        let index = DynamicIndexReader::new(tmpfile)
            .map_err(|err| format_err!("unable to read dynamic index '{archive_name}' - {err}"))?;
        // Note: do not use values stored in index (not trusted) - instead, computed them again
        let (csum, size) = index.compute_csum();
        manifest.verify_file(archive_name, &csum, size)?;

        // add index chunks to known chunks
        let mut known_chunks = known_chunks.lock().unwrap();
        for i in 0..index.index_count() {
            known_chunks.insert(*index.index_digest(i).unwrap());
        }

        debug!(
            "{}: known chunks list length is {}",
            archive_name,
            index.index_count()
        );

        Ok(index)
    }

    /// Retrieve backup time of last backup
    pub async fn previous_backup_time(&self) -> Result<Option<i64>, Error> {
        let data = self.h2.get("previous_backup_time", None).await?;
        serde_json::from_value(data).map_err(|err| {
            format_err!(
                "Failed to parse backup time value returned by server - {}",
                err
            )
        })
    }

    /// Download backup manifest (index.json) of last backup
    pub async fn download_previous_manifest(&self) -> Result<BackupManifest, Error> {
        let mut raw_data = Vec::with_capacity(64 * 1024);

        let param = json!({ "archive-name": MANIFEST_BLOB_NAME.to_string() });
        self.h2
            .download("previous", Some(param), &mut raw_data)
            .await?;

        let blob = DataBlob::load_from_reader(&mut &raw_data[..])?;
        // no expected digest available
        let data = blob.decode(self.crypt_config.as_ref().map(Arc::as_ref), None)?;

        let manifest =
            BackupManifest::from_data(&data[..], self.crypt_config.as_ref().map(Arc::as_ref))?;

        Ok(manifest)
    }

    // We have no `self` here for `h2` and `verbose`, the only other arg "common" with 1 other
    // function in the same path is `wid`, so those 3 could be in a struct, but there's no real use
    // since this is a private method.
    #[allow(clippy::too_many_arguments)]
    fn upload_chunk_info_stream(
        h2: H2Client,
        wid: u64,
        stream: impl Stream<Item = Result<bytes::BytesMut, Error>>,
        prefix: &str,
        known_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
        crypt_config: Option<Arc<CryptConfig>>,
        compress: bool,
        injections: Option<std::sync::mpsc::Receiver<InjectChunks>>,
        archive: &BackupArchiveName,
    ) -> impl Future<Output = Result<UploadStats, Error>> {
        let mut counters = UploadCounters::new();
        let counters_readonly = counters.clone();

        let is_fixed_chunk_size = prefix == "fixed";

        let index_csum = Arc::new(Mutex::new(Some(openssl::sha::Sha256::new())));
        let index_csum_2 = index_csum.clone();

        let stream = stream
            .inject_reused_chunks(injections, counters.clone())
            .and_then(move |chunk_info| match chunk_info {
                InjectedChunksInfo::Known(chunks) => {
                    // account for injected chunks
                    let mut known = Vec::new();
                    let mut guard = index_csum.lock().unwrap();
                    let csum = guard.as_mut().unwrap();
                    for chunk in chunks {
                        let offset = counters.add_injected_chunk(&chunk) as u64;
                        let digest = chunk.digest();
                        known.push((offset, digest));
                        let end_offset = offset + chunk.size();
                        csum.update(&end_offset.to_le_bytes());
                        csum.update(&digest);
                    }
                    future::ok(MergedChunkInfo::Known(known))
                }
                InjectedChunksInfo::Raw(data) => {
                    // account for not injected chunks (new and known)
                    let chunk_len = data.len();

                    let mut chunk_builder = DataChunkBuilder::new(data.as_ref()).compress(compress);

                    if let Some(ref crypt_config) = crypt_config {
                        chunk_builder = chunk_builder.crypt_config(crypt_config);
                    }

                    let mut known_chunks = known_chunks.lock().unwrap();
                    let digest = *chunk_builder.digest();
                    let (offset, res) = if known_chunks.contains(&digest) {
                        let offset = counters.add_known_chunk(chunk_len) as u64;
                        (offset, MergedChunkInfo::Known(vec![(offset, digest)]))
                    } else {
                        match chunk_builder.build() {
                            Ok((chunk, digest)) => {
                                let offset =
                                    counters.add_new_chunk(chunk_len, chunk.raw_size()) as u64;
                                known_chunks.insert(digest);
                                (
                                    offset,
                                    MergedChunkInfo::New(ChunkInfo {
                                        chunk,
                                        digest,
                                        chunk_len: chunk_len as u64,
                                        offset,
                                    }),
                                )
                            }
                            Err(err) => return future::err(err),
                        }
                    };

                    let mut guard = index_csum.lock().unwrap();
                    let csum = guard.as_mut().unwrap();

                    let chunk_end = offset + chunk_len as u64;

                    if !is_fixed_chunk_size {
                        csum.update(&chunk_end.to_le_bytes());
                    }
                    csum.update(&digest);

                    future::ok(res)
                }
            })
            .merge_known_chunks();

        Self::upload_merged_chunk_stream(
            h2,
            wid,
            archive,
            prefix,
            stream,
            index_csum_2,
            counters_readonly,
        )
    }

    fn upload_merged_chunk_stream(
        h2: H2Client,
        wid: u64,
        archive: &BackupArchiveName,
        prefix: &str,
        stream: impl Stream<Item = Result<MergedChunkInfo, Error>>,
        index_csum: Arc<Mutex<Option<Sha256>>>,
        counters: UploadCounters,
    ) -> impl Future<Output = Result<UploadStats, Error>> {
        let append_chunk_path = format!("{prefix}_index");
        let upload_chunk_path = format!("{prefix}_chunk");

        let start_time = std::time::Instant::now();
        let uploaded_len = Arc::new(AtomicUsize::new(0));

        let (upload_queue, upload_result) =
            Self::append_chunk_queue(h2.clone(), wid, append_chunk_path, uploaded_len.clone());

        let progress_handle = if archive.ends_with(".img.fidx")
            || archive.ends_with(".pxar.didx")
            || archive.ends_with(".ppxar.didx")
        {
            let counters = counters.clone();
            Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

                    let size = HumanByte::from(counters.total_stream_len());
                    let size_uploaded = HumanByte::from(uploaded_len.load(Ordering::SeqCst));
                    let elapsed = TimeSpan::from(start_time.elapsed());

                    info!("processed {size} in {elapsed}, uploaded {size_uploaded}");
                }
            }))
        } else {
            None
        };

        stream
            .try_for_each(move |merged_chunk_info| {
                let upload_queue = upload_queue.clone();

                if let MergedChunkInfo::New(chunk_info) = merged_chunk_info {
                    let offset = chunk_info.offset;
                    let digest = chunk_info.digest;
                    let digest_str = hex::encode(digest);

                    trace!(
                        "upload new chunk {} ({} bytes, offset {})",
                        digest_str,
                        chunk_info.chunk_len,
                        offset
                    );

                    let chunk_data = chunk_info.chunk.into_inner();
                    let param = json!({
                        "wid": wid,
                        "digest": digest_str,
                        "size": chunk_info.chunk_len,
                        "encoded-size": chunk_data.len(),
                    });

                    let ct = "application/octet-stream";
                    let request = H2Client::request_builder(
                        "localhost",
                        "POST",
                        &upload_chunk_path,
                        Some(param),
                        Some(ct),
                    )
                    .unwrap();
                    let upload_data = Some(bytes::Bytes::from(chunk_data));

                    let new_info = MergedChunkInfo::Known(vec![(offset, digest)]);

                    Either::Left(h2.send_request(request, upload_data).and_then(
                        move |response| async move {
                            upload_queue
                                .send((
                                    new_info,
                                    Some(ChunkUploadResponse {
                                        future: response,
                                        size: chunk_info.chunk_len as usize,
                                    }),
                                ))
                                .await
                                .map_err(|err| {
                                    format_err!("failed to send to upload queue: {}", err)
                                })
                        },
                    ))
                } else {
                    Either::Right(async move {
                        upload_queue
                            .send((merged_chunk_info, None))
                            .await
                            .map_err(|err| format_err!("failed to send to upload queue: {}", err))
                    })
                }
            })
            .then(move |result| async move { upload_result.await?.and(result) }.boxed())
            .and_then(move |_| {
                let mut guard = index_csum.lock().unwrap();
                let csum = guard.take().unwrap().finish();

                if let Some(handle) = progress_handle {
                    handle.abort();
                }

                futures::future::ok(counters.to_upload_stats(csum, start_time.elapsed()))
            })
    }

    /// Upload speed test - prints result to stderr
    pub async fn upload_speedtest(&self) -> Result<f64, Error> {
        let mut data = vec![];
        // generate pseudo random byte sequence
        for i in 0..1024 * 1024 {
            for j in 0..4 {
                let byte = ((i >> (j << 3)) & 0xff) as u8;
                data.push(byte);
            }
        }

        let item_len = data.len();

        let mut repeat = 0;

        let (upload_queue, upload_result) = Self::response_queue();

        let start_time = std::time::Instant::now();

        loop {
            repeat += 1;
            if start_time.elapsed().as_secs() >= 5 {
                break;
            }

            debug!("send test data ({} bytes)", data.len());
            let request =
                H2Client::request_builder("localhost", "POST", "speedtest", None, None).unwrap();
            let request_future = self
                .h2
                .send_request(request, Some(bytes::Bytes::from(data.clone())))
                .await?;

            upload_queue.send(request_future).await?;
        }

        drop(upload_queue); // close queue

        let _ = upload_result.await?;

        info!(
            "Uploaded {} chunks in {} seconds.",
            repeat,
            start_time.elapsed().as_secs()
        );
        let speed = ((item_len * (repeat as usize)) as f64) / start_time.elapsed().as_secs_f64();
        info!(
            "Time per request: {} microseconds.",
            (start_time.elapsed().as_micros()) / (repeat as u128)
        );

        Ok(speed)
    }
}
