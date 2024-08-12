use std::io::{Seek, SeekFrom};
use std::sync::Arc;

use anyhow::{bail, format_err, Error};
use serde_json::Value;

use proxmox_router::cli::*;
use proxmox_schema::api;

use pbs_api_types::BackupNamespace;
use pbs_client::pxar::tools::get_remote_pxar_reader;
use pbs_client::tools::has_pxar_filename_extension;
use pbs_client::tools::key_source::get_encryption_key_password;
use pbs_client::{BackupReader, RemoteChunkReader};
use pbs_tools::crypt_config::CryptConfig;
use pbs_tools::json::required_string_param;
use pxar::accessor::aio::Accessor;

use crate::helper;
use crate::{
    complete_backup_snapshot, complete_group_or_snapshot, complete_namespace,
    complete_pxar_archive_name, complete_repository, connect, crypto_parameters, decrypt_key,
    dir_or_last_from_group, extract_repository_from_value, format_key_source, optional_ns_param,
    record_repository, BackupDir, BufferedDynamicReader, CatalogReader, DynamicIndexReader,
    IndexFile, Shell, CATALOG_NAME, KEYFD_SCHEMA, REPO_URL_SCHEMA,
};

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
                description: "Snapshot path.",
             },
            "keyfile": {
                optional: true,
                type: String,
                description: "Path to encryption key.",
            },
            "keyfd": {
                schema: KEYFD_SCHEMA,
                optional: true,
            },
        }
   }
)]
/// Dump catalog.
async fn dump_catalog(param: Value) -> Result<Value, Error> {
    let repo = extract_repository_from_value(&param)?;

    let backup_ns = optional_ns_param(&param)?;
    let path = required_string_param(&param, "snapshot")?;
    let snapshot: BackupDir = path.parse()?;

    let crypto = crypto_parameters(&param)?;

    let crypt_config = match crypto.enc_key {
        None => None,
        Some(key) => {
            let (key, _created, _fingerprint) = decrypt_key(&key.key, &get_encryption_key_password)
                .map_err(|err| {
                    log::error!("{}", format_key_source(&key.source, "encryption"));
                    err
                })?;
            let crypt_config = CryptConfig::new(key)?;
            Some(Arc::new(crypt_config))
        }
    };

    let client = connect(&repo)?;

    let client = BackupReader::start(
        &client,
        crypt_config.clone(),
        repo.store(),
        &backup_ns,
        &snapshot,
        true,
    )
    .await?;

    let (manifest, _) = client.download_manifest().await?;
    manifest.check_fingerprint(crypt_config.as_ref().map(Arc::as_ref))?;

    let file_info = match manifest.lookup_file_info(CATALOG_NAME) {
        Ok(file_info) => file_info,
        Err(err) => {
            let mut metadata_archives = Vec::new();
            // No catalog, fallback to metadata archives if present
            for archive in manifest.files() {
                if archive.filename.ends_with(".mpxar.didx") {
                    metadata_archives.push(archive.filename.clone());
                }
            }
            metadata_archives.sort_unstable_by(|a, b| a.cmp(b));

            for archive in &metadata_archives {
                let (reader, archive_size) = get_remote_pxar_reader(
                    &archive,
                    client.clone(),
                    &manifest,
                    crypt_config.clone(),
                )
                .await?;
                // only care about the metadata, don't attach a payload reader
                let reader = pxar::PxarVariant::Unified(reader);
                let accessor = Accessor::new(reader, archive_size).await?;
                let root_dir = accessor.open_root().await?;
                let prefix = format!("./{archive}");
                pbs_client::pxar::tools::pxar_metadata_catalog_dump_dir(root_dir, Some(&prefix))
                    .await?;
            }

            if !metadata_archives.is_empty() {
                return Ok(Value::Null);
            }

            bail!(err);
        }
    };

    let index = client
        .download_dynamic_index(&manifest, CATALOG_NAME)
        .await?;

    let most_used = index.find_most_used_chunks(8);

    let chunk_reader = RemoteChunkReader::new(
        client.clone(),
        crypt_config,
        file_info.chunk_crypt_mode(),
        most_used,
    );

    let mut reader = BufferedDynamicReader::new(index, chunk_reader);

    let mut catalogfile = pbs_client::tools::create_tmp_file()?;

    std::io::copy(&mut reader, &mut catalogfile)
        .map_err(|err| format_err!("unable to download catalog - {}", err))?;

    catalogfile.seek(SeekFrom::Start(0))?;

    let mut catalog_reader = CatalogReader::new(catalogfile);

    catalog_reader.dump()?;

    record_repository(&repo);

    Ok(Value::Null)
}

#[api(
    input: {
        properties: {
            ns: {
                type: BackupNamespace,
                optional: true,
            },
            "snapshot": {
                type: String,
                description: "Group/Snapshot path.",
            },
            "archive-name": {
                type: String,
                description: "Backup archive name.",
            },
            "repository": {
                optional: true,
                schema: REPO_URL_SCHEMA,
            },
            "keyfile": {
                optional: true,
                type: String,
                description: "Path to encryption key.",
            },
            "keyfd": {
                schema: KEYFD_SCHEMA,
                optional: true,
            },
         },
    },
)]
/// Shell to interactively inspect and restore snapshots.
async fn catalog_shell(param: Value) -> Result<(), Error> {
    let repo = extract_repository_from_value(&param)?;
    let client = connect(&repo)?;
    let backup_ns = optional_ns_param(&param)?;
    let path = required_string_param(&param, "snapshot")?;
    let archive_name = required_string_param(&param, "archive-name")?;

    let backup_dir = dir_or_last_from_group(&client, &repo, &backup_ns, path).await?;

    let crypto = crypto_parameters(&param)?;

    let crypt_config = match crypto.enc_key {
        None => None,
        Some(key) => {
            let (key, _created, _fingerprint) = decrypt_key(&key.key, &get_encryption_key_password)
                .map_err(|err| {
                    log::error!("{}", format_key_source(&key.source, "encryption"));
                    err
                })?;
            let crypt_config = CryptConfig::new(key)?;
            Some(Arc::new(crypt_config))
        }
    };

    let server_archive_name = if has_pxar_filename_extension(archive_name, false) {
        format!("{}.didx", archive_name)
    } else {
        bail!("Can only mount pxar archives.");
    };

    let client = BackupReader::start(
        &client,
        crypt_config.clone(),
        repo.store(),
        &backup_ns,
        &backup_dir,
        true,
    )
    .await?;

    let mut tmpfile = pbs_client::tools::create_tmp_file()?;

    let (manifest, _) = client.download_manifest().await?;
    manifest.check_fingerprint(crypt_config.as_ref().map(Arc::as_ref))?;

    let decoder = helper::get_pxar_fuse_accessor(
        &server_archive_name,
        client.clone(),
        &manifest,
        crypt_config.clone(),
    )
    .await?;

    client.download(CATALOG_NAME, &mut tmpfile).await?;
    let index = DynamicIndexReader::new(tmpfile)
        .map_err(|err| format_err!("unable to read catalog index - {}", err))?;

    // Note: do not use values stored in index (not trusted) - instead, computed them again
    let (csum, size) = index.compute_csum();
    manifest.verify_file(CATALOG_NAME, &csum, size)?;

    let most_used = index.find_most_used_chunks(8);

    let file_info = manifest.lookup_file_info(CATALOG_NAME)?;
    let chunk_reader = RemoteChunkReader::new(
        client.clone(),
        crypt_config,
        file_info.chunk_crypt_mode(),
        most_used,
    );
    let mut reader = BufferedDynamicReader::new(index, chunk_reader);
    let mut catalogfile = pbs_client::tools::create_tmp_file()?;

    std::io::copy(&mut reader, &mut catalogfile)
        .map_err(|err| format_err!("unable to download catalog - {}", err))?;

    catalogfile.seek(SeekFrom::Start(0))?;
    let catalog_reader = CatalogReader::new(catalogfile);
    let state = Shell::new(catalog_reader, &server_archive_name, decoder).await?;

    log::info!("Starting interactive shell");
    state.shell().await?;

    record_repository(&repo);

    Ok(())
}

pub fn catalog_mgmt_cli() -> CliCommandMap {
    let catalog_shell_cmd_def = CliCommand::new(&API_METHOD_CATALOG_SHELL)
        .arg_param(&["snapshot", "archive-name"])
        .completion_cb("repository", complete_repository)
        .completion_cb("ns", complete_namespace)
        .completion_cb("archive-name", complete_pxar_archive_name)
        .completion_cb("snapshot", complete_group_or_snapshot);

    let catalog_dump_cmd_def = CliCommand::new(&API_METHOD_DUMP_CATALOG)
        .arg_param(&["snapshot"])
        .completion_cb("repository", complete_repository)
        .completion_cb("ns", complete_namespace)
        .completion_cb("snapshot", complete_backup_snapshot);

    CliCommandMap::new()
        .insert("dump", catalog_dump_cmd_def)
        .insert("shell", catalog_shell_cmd_def)
}
