use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, format_err, Error};
use hex::FromHex;
use serde_json::{json, Value};
use walkdir::WalkDir;

use proxmox_router::cli::{
    format_and_print_result, get_output_format, CliCommand, CliCommandMap, CommandLineInterface,
    OUTPUT_FORMAT,
};
use proxmox_schema::api;

use pbs_client::tools::key_source::get_encryption_key_password;
use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_datastore::file_formats::{
    COMPRESSED_BLOB_MAGIC_1_0, DYNAMIC_SIZED_CHUNK_INDEX_1_0, ENCRYPTED_BLOB_MAGIC_1_0,
    ENCR_COMPR_BLOB_MAGIC_1_0, FIXED_SIZED_CHUNK_INDEX_1_0, UNCOMPRESSED_BLOB_MAGIC_1_0,
};
use pbs_datastore::fixed_index::FixedIndexReader;
use pbs_datastore::index::IndexFile;
use pbs_datastore::DataBlob;
use pbs_key_config::load_and_decrypt_key;
use pbs_tools::crypt_config::CryptConfig;

/// Decodes a blob and writes its content either to stdout or into a file
fn decode_blob(
    mut output_path: Option<&Path>,
    key_file: Option<&Path>,
    digest: Option<&[u8; 32]>,
    blob: &DataBlob,
) -> Result<(), Error> {
    let mut crypt_conf_opt = None;
    let crypt_conf;

    if blob.is_encrypted() && key_file.is_some() {
        let (key, _created, _fingerprint) =
            load_and_decrypt_key(key_file.unwrap(), &get_encryption_key_password)?;
        crypt_conf = CryptConfig::new(key)?;
        crypt_conf_opt = Some(&crypt_conf);
    }

    output_path = match output_path {
        Some(path) if path.eq(Path::new("-")) => None,
        _ => output_path,
    };

    crate::outfile_or_stdout(output_path)?
        .write_all(blob.decode(crypt_conf_opt, digest)?.as_slice())?;
    Ok(())
}

#[api(
    input: {
        properties: {
            chunk: {
                description: "The chunk file.",
                type: String,
            },
            "reference-filter": {
                description: "Path to the directory that should be searched for references.",
                type: String,
                optional: true,
            },
            "digest": {
                description: "Needed when searching for references, if set, it will be used for verification when decoding.",
                type: String,
                optional: true,
            },
            "decode": {
                description: "Path to the file to which the chunk should be decoded, '-' -> decode to stdout.",
                type: String,
                optional: true,
            },
            "keyfile": {
                description: "Path to the keyfile with which the chunk was encrypted.",
                type: String,
                optional: true,
            },
            "use-filename-as-digest": {
                description: "The filename should be used as digest for reference search and decode verification, if no digest is specified.",
                type: bool,
                optional: true,
                default: true,
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        }
    }
)]
/// Inspect a chunk
fn inspect_chunk(
    chunk: String,
    reference_filter: Option<String>,
    mut digest: Option<String>,
    decode: Option<String>,
    keyfile: Option<String>,
    use_filename_as_digest: bool,
    param: Value,
) -> Result<(), Error> {
    let output_format = get_output_format(&param);
    let chunk_path = Path::new(&chunk);

    if digest.is_none() && use_filename_as_digest {
        digest = Some(if let Some((_, filename)) = chunk.rsplit_once('/') {
            String::from(filename)
        } else {
            chunk.clone()
        });
    };

    let digest_raw: Option<[u8; 32]> = digest
        .map(|ref d| {
            <[u8; 32]>::from_hex(d).map_err(|e| format_err!("could not parse chunk - {}", e))
        })
        .map_or(Ok(None), |r| r.map(Some))?;

    let search_path = reference_filter.as_ref().map(Path::new);
    let key_file_path = keyfile.as_ref().map(Path::new);
    let decode_output_path = decode.as_ref().map(Path::new);

    let blob = DataBlob::load_from_reader(
        &mut std::fs::File::open(chunk_path)
            .map_err(|e| format_err!("could not open chunk file - {}", e))?,
    )?;

    let referenced_by = if let (Some(search_path), Some(digest_raw)) = (search_path, digest_raw) {
        let mut references = Vec::new();
        for entry in WalkDir::new(search_path)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            use std::os::unix::ffi::OsStrExt;
            let file_name = entry.file_name().as_bytes();

            let index: Box<dyn IndexFile> = if file_name.ends_with(b".fidx") {
                match FixedIndexReader::open(entry.path()) {
                    Ok(index) => Box::new(index),
                    Err(_) => continue,
                }
            } else if file_name.ends_with(b".didx") {
                match DynamicIndexReader::open(entry.path()) {
                    Ok(index) => Box::new(index),
                    Err(_) => continue,
                }
            } else {
                continue;
            };

            for pos in 0..index.index_count() {
                if let Some(index_chunk_digest) = index.index_digest(pos) {
                    if digest_raw.eq(index_chunk_digest) {
                        references.push(entry.path().to_string_lossy().into_owned());
                        break;
                    }
                }
            }
        }
        if !references.is_empty() {
            Some(references)
        } else {
            None
        }
    } else {
        None
    };

    if decode_output_path.is_some() {
        decode_blob(
            decode_output_path,
            key_file_path,
            digest_raw.as_ref(),
            &blob,
        )?;
    }

    let crc_status = format!(
        "{}({})",
        blob.compute_crc(),
        blob.verify_crc().map_or("BAD", |_| "OK")
    );

    let val = match referenced_by {
        Some(references) => json!({
            "crc": crc_status,
            "encryption": blob.crypt_mode()?,
            "is-compressed": blob.is_compressed(),
            "size": blob.raw_size(),
            "referenced-by": references
        }),
        None => json!({
             "crc": crc_status,
             "encryption": blob.crypt_mode()?,
             "is-compressed": blob.is_compressed(),
             "size": blob.raw_size(),
        }),
    };

    if output_format == "text" {
        println!("CRC: {}", val["crc"]);
        println!("encryption: {}", val["encryption"]);
        println!("is-compressed: {}", val["is-compressed"]);
        println!("size: {}", val["size"]);
        if let Some(refs) = val["referenced-by"].as_array() {
            println!("referenced by:");
            for reference in refs {
                println!("  {}", reference);
            }
        }
    } else {
        format_and_print_result(&val, &output_format);
    }
    Ok(())
}

#[api(
    input: {
        properties: {
            file: {
                description: "Path to the file.",
                type: String,
            },
            "decode": {
                description: "Path to the file to which the file should be decoded, '-' -> decode to stdout.",
                type: String,
                optional: true,
            },
            "keyfile": {
                description: "Path to the keyfile with which the file was encrypted.",
                type: String,
                optional: true,
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        }
    }
)]
/// Inspect a file, for blob file without decode only the size and encryption mode is printed
fn inspect_file(
    file: String,
    decode: Option<String>,
    keyfile: Option<String>,
    param: Value,
) -> Result<(), Error> {
    let output_format = get_output_format(&param);

    let mut file = File::open(Path::new(&file))?;
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    file.seek(SeekFrom::Start(0))?;
    let val = match magic {
        UNCOMPRESSED_BLOB_MAGIC_1_0
        | COMPRESSED_BLOB_MAGIC_1_0
        | ENCRYPTED_BLOB_MAGIC_1_0
        | ENCR_COMPR_BLOB_MAGIC_1_0 => {
            let data_blob = DataBlob::load_from_reader(&mut file)?;
            let key_file_path = keyfile.as_ref().map(Path::new);

            let decode_output_path = decode.as_ref().map(Path::new);

            if decode_output_path.is_some() {
                decode_blob(decode_output_path, key_file_path, None, &data_blob)?;
            }

            let crypt_mode = data_blob.crypt_mode()?;
            json!({
                "encryption": crypt_mode,
                "size": data_blob.raw_size(),
            })
        }
        FIXED_SIZED_CHUNK_INDEX_1_0 | DYNAMIC_SIZED_CHUNK_INDEX_1_0 => {
            let index: Box<dyn IndexFile> = match magic {
                FIXED_SIZED_CHUNK_INDEX_1_0 => {
                    Box::new(FixedIndexReader::new(file)?) as Box<dyn IndexFile>
                }
                DYNAMIC_SIZED_CHUNK_INDEX_1_0 => {
                    Box::new(DynamicIndexReader::new(file)?) as Box<dyn IndexFile>
                }
                _ => bail!(format_err!("This is technically not possible")),
            };

            let mut ctime_str = index.index_ctime().to_string();
            if let Ok(s) = proxmox_time::strftime_local("%c", index.index_ctime()) {
                ctime_str = s;
            }

            let mut chunk_digests = HashSet::new();

            for pos in 0..index.index_count() {
                let digest = index.index_digest(pos).unwrap();
                chunk_digests.insert(hex::encode(digest));
            }

            json!({
                "size": index.index_size(),
                "ctime": ctime_str,
                "chunk-digests": chunk_digests
            })
        }
        _ => bail!(format_err!(
            "Only .blob, .fidx and .didx files may be inspected"
        )),
    };

    if output_format == "text" {
        println!("size: {}", val["size"]);
        if let Some(encryption) = val["encryption"].as_str() {
            println!("encryption: {}", encryption);
        }
        if let Some(ctime) = val["ctime"].as_str() {
            println!("creation time: {}", ctime);
        }
        if let Some(chunks) = val["chunk-digests"].as_array() {
            println!("chunks:");
            for chunk in chunks {
                println!("  {}", chunk);
            }
        }
    } else {
        format_and_print_result(&val, &output_format);
    }

    Ok(())
}

/// Return the count of VM, CT and host backup groups and the count of namespaces
/// as this tuple (vm, ct, host, ns)
fn get_basic_ds_info(path: String) -> Result<(i64, i64, i64, i64), Error> {
    let mut vms = 0;
    let mut cts = 0;
    let mut hosts = 0;
    let mut ns = 0;
    let mut walker = WalkDir::new(path).into_iter();

    while let Some(entry_result) = walker.next() {
        let entry = entry_result?;
        if !entry.file_type().is_dir() {
            continue;
        }

        let Some(name) = entry.path().file_name().and_then(|a| a.to_str()) else {
            continue;
        };

        if name == ".chunks" {
            walker.skip_current_dir();
            continue;
        }

        let dir_count = std::fs::read_dir(entry.path())?
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_dir())
            .count() as i64;

        match name {
            "ns" => ns += dir_count,
            "vm" => {
                vms += dir_count;
                walker.skip_current_dir();
            }
            "ct" => {
                cts += dir_count;
                walker.skip_current_dir();
            }
            "host" => {
                hosts += dir_count;
                walker.skip_current_dir();
            }
            _ => {
                // root or ns dir
            }
        }
    }

    Ok((vms, cts, hosts, ns))
}

#[api(
    input: {
        properties: {
            device: {
                description: "Device path, usually /dev/...",
                type: String,
            },
            "output-format": {
                schema: OUTPUT_FORMAT,
                optional: true,
            },
        }
    }
)]
/// Inspect a device for possible datastores on it
fn inspect_device(device: String, param: Value) -> Result<(), Error> {
    let output_format = get_output_format(&param);
    let tmp_mount_path = format!(
        "{}/{:x}",
        pbs_buildcfg::rundir!("/mount"),
        proxmox_uuid::Uuid::generate()
    );

    let default_options = proxmox_sys::fs::CreateOptions::new();
    proxmox_sys::fs::create_path(
        &tmp_mount_path,
        Some(default_options.clone()),
        Some(default_options.clone()),
    )?;
    let mut mount_cmd = std::process::Command::new("mount");
    mount_cmd.arg(device.clone());
    mount_cmd.arg(tmp_mount_path.clone());
    proxmox_sys::command::run_command(mount_cmd, None)?;

    let mut walker = WalkDir::new(tmp_mount_path.clone()).into_iter();

    let mut stores = Vec::new();

    let mut ds_count = 0;
    while let Some(entry_result) = walker.next() {
        let entry = entry_result?;

        if entry.file_type().is_dir()
            && entry
                .file_name()
                .to_str()
                .is_some_and(|name| name == ".chunks")
        {
            let store_path = entry
                .path()
                .to_str()
                .and_then(|n| n.strip_suffix("/.chunks"));

            if let Some(store_path) = store_path {
                ds_count += 1;
                let (vm, ct, host, ns) = get_basic_ds_info(store_path.to_string())?;
                stores.push(json!({
                    "path": store_path.strip_prefix(&tmp_mount_path).unwrap_or("???"),
                    "vm-count": vm,
                    "ct-count": ct,
                    "host-count": host,
                    "ns-count": ns,
                }));
            };

            walker.skip_current_dir();
        }
    }

    let mut umount_cmd = std::process::Command::new("umount");
    umount_cmd.arg(tmp_mount_path.clone());
    proxmox_sys::command::run_command(umount_cmd, None)?;
    std::fs::remove_dir(std::path::Path::new(&tmp_mount_path))?;

    if output_format == "text" {
        println!("Device contains {ds_count} stores");
        println!("---------------");
        for s in stores {
            println!(
                "Datastore at {} | VM: {}, CT: {}, HOST: {}, NS: {}",
                s["path"], s["vm-count"], s["ct-count"], s["host-count"], s["ns-count"]
            );
        }
    } else {
        format_and_print_result(
            &json!({"store_count": stores.len(), "stores": stores}),
            &output_format,
        );
    }

    Ok(())
}

pub fn inspect_commands() -> CommandLineInterface {
    let cmd_def = CliCommandMap::new()
        .insert(
            "chunk",
            CliCommand::new(&API_METHOD_INSPECT_CHUNK).arg_param(&["chunk"]),
        )
        .insert(
            "file",
            CliCommand::new(&API_METHOD_INSPECT_FILE).arg_param(&["file"]),
        )
        .insert(
            "device",
            CliCommand::new(&API_METHOD_INSPECT_DEVICE).arg_param(&["device"]),
        );

    cmd_def.into()
}
