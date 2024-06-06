use std::sync::Arc;

use anyhow::Error;
use pbs_client::{BackupReader, RemoteChunkReader};
use pbs_datastore::BackupManifest;
use pbs_tools::crypt_config::CryptConfig;

use crate::{BufferedDynamicReadAt, BufferedDynamicReader, IndexFile};

pub(crate) async fn get_pxar_fuse_accessor(
    archive_name: &str,
    client: Arc<BackupReader>,
    manifest: &BackupManifest,
    crypt_config: Option<Arc<CryptConfig>>,
) -> Result<pbs_pxar_fuse::Accessor, Error> {
    let (archive_name, payload_archive_name) =
        pbs_client::tools::get_pxar_archive_names(archive_name, &manifest)?;

    let (reader, archive_size) = get_pxar_fuse_reader(
        &archive_name,
        client.clone(),
        manifest,
        crypt_config.clone(),
    )
    .await?;

    let reader = if let Some(payload_archive_name) = payload_archive_name {
        let (payload_reader, payload_size) = get_pxar_fuse_reader(
            &payload_archive_name,
            client.clone(),
            manifest,
            crypt_config.clone(),
        )
        .await?;

        pxar::PxarVariant::Split(reader, (payload_reader, payload_size))
    } else {
        pxar::PxarVariant::Unified(reader)
    };

    let accessor = pbs_pxar_fuse::Accessor::new(reader, archive_size).await?;

    Ok(accessor)
}

pub(crate) async fn get_pxar_fuse_reader(
    archive_name: &str,
    client: Arc<BackupReader>,
    manifest: &BackupManifest,
    crypt_config: Option<Arc<CryptConfig>>,
) -> Result<(pbs_pxar_fuse::Reader, u64), Error> {
    let reader = get_buffered_pxar_reader(archive_name, client, manifest, crypt_config).await?;
    let archive_size = reader.archive_size();
    let reader: pbs_pxar_fuse::Reader = Arc::new(BufferedDynamicReadAt::new(reader));

    Ok((reader, archive_size))
}

pub(crate) async fn get_buffered_pxar_reader(
    archive_name: &str,
    client: Arc<BackupReader>,
    manifest: &BackupManifest,
    crypt_config: Option<Arc<CryptConfig>>,
) -> Result<BufferedDynamicReader<RemoteChunkReader>, Error> {
    let index = client
        .download_dynamic_index(manifest, archive_name)
        .await?;

    let most_used = index.find_most_used_chunks(8);
    let file_info = manifest.lookup_file_info(archive_name)?;
    let chunk_reader = RemoteChunkReader::new(
        client.clone(),
        crypt_config.clone(),
        file_info.chunk_crypt_mode(),
        most_used,
    );

    Ok(BufferedDynamicReader::new(index, chunk_reader))
}
