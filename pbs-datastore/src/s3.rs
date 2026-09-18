use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Error, bail, format_err};
use hex::FromHex;

use pbs_api_types::{BackupArchiveName, BackupGroup, BackupNamespace};
use proxmox_s3_client::{DeleteObjectError, S3ObjectKey};

use crate::{
    backup_info::PROTECTED_MARKER_FILENAME,
    datastore::{GROUP_NOTES_FILE_NAME, GROUP_OWNER_FILE_NAME, NAMESPACE_MARKER_FILENAME},
};

/// Object key prefix to group regular datastore contents (not chunks)
pub const S3_CONTENT_PREFIX: &str = ".cnt";

/// Generate a relative object key with content prefix from given path and filename
pub fn object_key_from_path(path: &Path, filename: &str) -> Result<S3ObjectKey, Error> {
    // Force the use of relative paths, otherwise this would loose the content prefix
    if path.is_absolute() {
        bail!("cannot generate object key from absolute path");
    }
    if filename.contains('/') {
        bail!("invalid filename containing slashes");
    }
    let mut object_path = PathBuf::from(S3_CONTENT_PREFIX);
    object_path.push(path);
    object_path.push(filename);

    let object_key_str = object_path
        .to_str()
        .ok_or_else(|| format_err!("unexpected object key path"))?;
    S3ObjectKey::try_from(object_key_str)
}

/// Generate a relative object key with chunk prefix from given digest
pub fn object_key_from_digest(digest: &[u8; 32]) -> Result<S3ObjectKey, Error> {
    let object_key = hex::encode(digest);
    let digest_prefix = &object_key[..4];
    let object_key_string = format!(".chunks/{digest_prefix}/{object_key}");
    S3ObjectKey::try_from(object_key_string.as_str())
}

/// Generate a relative object key with chunk prefix from given digest, extended by suffix
pub fn object_key_from_digest_with_suffix(
    digest: &[u8; 32],
    suffix: &str,
) -> Result<S3ObjectKey, Error> {
    if suffix.contains('/') {
        bail!("invalid suffix containing slashes");
    }
    let object_key = hex::encode(digest);
    let digest_prefix = &object_key[..4];
    let object_key_string = format!(".chunks/{digest_prefix}/{object_key}{suffix}");
    S3ObjectKey::try_from(object_key_string.as_str())
}

/// Extract filename, digest, and suffix from the last part of an S3ObjectKey.
pub(crate) fn digest_from_object_key(key: &S3ObjectKey) -> Option<(&str, [u8; 32], &str)> {
    let filename = key.rsplit('/').next()?;
    let (hex_digest, suffix) = filename.split_at_checked(64)?;

    let digest = FromHex::from_hex(hex_digest).ok()?;

    Some((filename, digest, suffix))
}

/// Extract file path relative to datastore base from object key. Key is expected to follow the
/// same component layout and naming restrictions as for datastores and be an object pointing
/// to a file.
/// Object keys ending with a slash, including the bare content prefix, are interpreted as directory
/// and return `None`.
///
/// Errors if the provided object key does not match a valid pattern for a datastore path.
pub(crate) fn content_filepath_from_object_key(
    key: &S3ObjectKey,
    store_cnt_prefix: &str,
) -> Result<Option<PathBuf>, Error> {
    let key = key.to_string();
    let key = key
        .strip_prefix(store_cnt_prefix)
        .ok_or_else(|| format_err!("failed to strip store context prefix"))?;

    if key.starts_with('/') {
        bail!("unexpected leading slash");
    }

    if key.is_empty() || key.ends_with('/') {
        // interpreted as directory, empty if the key is the content prefix itself
        return Ok(None);
    }

    let (mut prefix, filename) = key
        .rsplit_once('/')
        .ok_or_else(|| format_err!("unexpected path without directory components"))?;

    let mut namespace = BackupNamespace::root();
    // inside content prefix, must be followed by components for valid namespaces
    while let Some(remaining) = prefix.strip_prefix("ns/") {
        match remaining.split_once('/') {
            Some((name, remaining)) => {
                // checks nesting level limit and format
                namespace.push(name.to_string())?;
                prefix = remaining;
            }
            None => {
                // checks nesting level limit and format
                namespace.push(remaining.to_string())?;
                if filename == NAMESPACE_MARKER_FILENAME {
                    // valid namespace with marker object, done
                    return Ok(Some(PathBuf::from(key)));
                }
                bail!("unexpected file object in namespace");
            }
        }
    }

    // inside valid namespace, must be at least a backup group
    let (backup_type, prefix) = prefix
        .split_once('/')
        .ok_or_else(|| format_err!("failed to split backup-type of group"))?;

    let prefix = match prefix.split_once('/') {
        Some((backup_id, prefix)) => {
            BackupGroup::from_str(&format!("{backup_type}/{backup_id}"))?;
            prefix
        }
        None => {
            // already at final directory component, prefix == backup_id
            BackupGroup::from_str(&format!("{backup_type}/{prefix}"))?;
            if filename == GROUP_OWNER_FILE_NAME || filename == GROUP_NOTES_FILE_NAME {
                // valid (optionally namespaced) group path with owner or group note filename
                return Ok(Some(PathBuf::from(key)));
            } else {
                bail!("unexpected file object in group");
            }
        }
    };

    // inside valid group but not note or owner file, must be final snapshot directory component
    if prefix.contains('/') {
        bail!("unexpected sub-directory component in snapshot");
    }

    if proxmox_time::parse_rfc3339(prefix).is_err() {
        bail!("invalid snapshot component");
    }

    // inside valid snapshot, must be protected marker or a valid archive name
    if filename == PROTECTED_MARKER_FILENAME {
        return Ok(Some(PathBuf::from(key)));
    }

    let _archive_name = BackupArchiveName::try_from_strict(filename)?;

    Ok(Some(PathBuf::from(key)))
}

/// Log errors from delete objects api calls
pub(crate) fn log_s3_delete_objects_errors(errors: &[DeleteObjectError]) {
    for error in errors {
        log::error!(
            "delete object failed: {} {} {}",
            error
                .key
                .as_ref()
                .map(|key| key.to_string())
                .unwrap_or_else(|| "None".into()),
            error.code.as_deref().unwrap_or("None"),
            error.message.as_deref().unwrap_or("None"),
        );
    }
}

#[test]
fn test_content_filepath_from_object_key() {
    let k = |s: &str| S3ObjectKey::try_from(s).unwrap();
    let store_cnt_prefix = "store/.cnt/";

    content_filepath_from_object_key(&k("store/.cnt/ns/test/vm/100/owner"), store_cnt_prefix)
        .unwrap();
    content_filepath_from_object_key(&k("store/.cnt/ns/test/vm/100/notes"), store_cnt_prefix)
        .unwrap();
    content_filepath_from_object_key(
        &k("store/.cnt/vm/100/2025-07-14T14:20:02Z/.protected"),
        store_cnt_prefix,
    )
    .unwrap();
    content_filepath_from_object_key(
        &k("store/.cnt/vm/100/2025-07-14T14:20:02Z/drive-scsci0.img.fidx"),
        store_cnt_prefix,
    )
    .unwrap();
    content_filepath_from_object_key(
        &k("store/.cnt/ct/100/2025-07-14T14:20:02Z/test.pxar.didx"),
        store_cnt_prefix,
    )
    .unwrap();
    content_filepath_from_object_key(
        &k("store/.cnt/host/myhost/2025-07-14T14:20:02Z/test.pxar.didx"),
        store_cnt_prefix,
    )
    .unwrap();
    content_filepath_from_object_key(
        &k("store/.cnt/ns/test/vm/100/2025-07-14T14:20:02Z/.protected"),
        store_cnt_prefix,
    )
    .unwrap();
    content_filepath_from_object_key(
        &k("store/.cnt/ns/test/vm/100/2025-07-14T14:20:02Z/drive-scsci0.img.fidx"),
        store_cnt_prefix,
    )
    .unwrap();
    content_filepath_from_object_key(&k("store/.cnt/ns/test/.namespace"), store_cnt_prefix)
        .unwrap();

    assert_eq!(
        content_filepath_from_object_key(&k("store/.cnt/ns/test/"), store_cnt_prefix).unwrap(),
        None
    );

    assert!(content_filepath_from_object_key(&k(""), store_cnt_prefix).is_err());
    assert_eq!(
        content_filepath_from_object_key(&k("store/.cnt/"), store_cnt_prefix).unwrap(),
        None
    );
    assert!(
        content_filepath_from_object_key(&k("store/.cnt/standalone-file"), store_cnt_prefix)
            .is_err()
    );
    assert!(
        content_filepath_from_object_key(
            &k("store/.cnt/ns/t/ns/t/ns/t/ns/t/ns/t/ns/t/ns/t/ns/t"),
            store_cnt_prefix
        )
        .is_err()
    );
    assert!(
        content_filepath_from_object_key(
            &k("store/.cnt/vm/100/2025-07-14T14:20:02Z/drive-scsci0.img"),
            store_cnt_prefix
        )
        .is_err()
    );
    assert!(
        content_filepath_from_object_key(
            &k("store/.cnt/vm/2025-07-14T14:20:02Z/drive-scsci0.img.fidx"),
            store_cnt_prefix
        )
        .is_err()
    );
    assert!(
        content_filepath_from_object_key(
            &k("store/.cnt/vm/100/drive-scsci0.img.fidx"),
            store_cnt_prefix
        )
        .is_err()
    );
    assert!(
        content_filepath_from_object_key(&k("store/.cnt/vm/100/.protected"), store_cnt_prefix)
            .is_err()
    );
    assert!(
        content_filepath_from_object_key(
            &k("store/.cnt/ns/invalid$namespace/vm/100/2025-07-14T14:20:02Z/drive-scsci0.img.fidx"),
            store_cnt_prefix
        )
        .is_err()
    );
    assert!(
        content_filepath_from_object_key(
            &k("store/.cnt/ns/test/invalid/100/2025-07-14T14:20:02Z/drive-scsci0.img.fidx"),
            store_cnt_prefix
        )
        .is_err()
    );
    assert!(
        content_filepath_from_object_key(
            &k("store/.cnt/ns/test/vm/invalid$/2025-07-14T14:20:02Z/drive-scsci0.img.fidx"),
            store_cnt_prefix
        )
        .is_err()
    );
    assert!(
        content_filepath_from_object_key(
            &k("store/.cnt/ns/test/vm/100/2025-07-14T14:20:02Z/invalid-%-scsci0.img.fidx"),
            store_cnt_prefix
        )
        .is_err()
    );
    assert!(
        content_filepath_from_object_key(
            &k("store/.cnt/ns/test/../vm/100/2025-07-14T14:20:02Z/drive-scsci0.img.fidx"),
            store_cnt_prefix
        )
        .is_err()
    );
}

#[test]
fn test_object_key_from_path() {
    let path = Path::new("vm/100/2025-07-14T14:20:02Z");
    let filename = "drive-scsci0.img.fidx";
    assert_eq!(
        object_key_from_path(path, filename).unwrap().to_string(),
        ".cnt/vm/100/2025-07-14T14:20:02Z/drive-scsci0.img.fidx",
    );
}

#[test]
fn test_object_key_from_empty_path() {
    let path = Path::new("");
    let filename = ".marker";
    assert_eq!(
        object_key_from_path(path, filename).unwrap().to_string(),
        ".cnt/.marker",
    );
}

#[test]
fn test_object_key_from_absolute_path() {
    assert!(object_key_from_path(Path::new("/"), ".marker").is_err());
}

#[test]
fn test_object_key_from_path_incorrect_filename() {
    assert!(object_key_from_path(Path::new(""), "/.marker").is_err());
}

#[test]
fn test_object_key_from_digest() {
    let digest =
        <[u8; 32]>::from_hex("bb9f8df61474d25e71fa00722318cd387396ca1736605e1248821cc0de3d3af8")
            .unwrap();
    assert_eq!(
        object_key_from_digest(&digest).unwrap().to_string(),
        ".chunks/bb9f/bb9f8df61474d25e71fa00722318cd387396ca1736605e1248821cc0de3d3af8",
    );
}

#[test]
fn test_object_key_from_digest_with_suffix() {
    let digest =
        <[u8; 32]>::from_hex("bb9f8df61474d25e71fa00722318cd387396ca1736605e1248821cc0de3d3af8")
            .unwrap();
    assert_eq!(
        object_key_from_digest_with_suffix(&digest, ".0.bad")
            .unwrap()
            .to_string(),
        ".chunks/bb9f/bb9f8df61474d25e71fa00722318cd387396ca1736605e1248821cc0de3d3af8.0.bad",
    );
}

#[test]
fn test_object_key_from_digest_with_invalid_suffix() {
    let digest =
        <[u8; 32]>::from_hex("bb9f8df61474d25e71fa00722318cd387396ca1736605e1248821cc0de3d3af8")
            .unwrap();
    assert!(object_key_from_digest_with_suffix(&digest, "/.0.bad").is_err());
}

#[test]
fn test_digest_from_object_key() {
    let hex = "bb9f8df61474d25e71fa00722318cd387396ca1736605e1248821cc0de3d3af8";
    let digest = <[u8; 32]>::from_hex(hex).unwrap();
    let k = |s: &str| S3ObjectKey::try_from(s).unwrap();

    assert_eq!(digest_from_object_key(&k(&hex[1..])), None);
    let invalid_hex = hex.replace("b", "X");
    assert_eq!(digest_from_object_key(&k(&invalid_hex)), None);

    assert_eq!(digest_from_object_key(&k(hex)), Some((hex, digest, "")));

    let k1 = object_key_from_digest_with_suffix(&digest, "suffix1").unwrap();
    assert_eq!(
        digest_from_object_key(&k1),
        Some((format!("{hex}suffix1").as_str(), digest, "suffix1"))
    );

    let k2 = S3ObjectKey::try_from(format!(".chunks/bb9f/{hex}.0.bad").as_str()).unwrap();
    assert_eq!(
        digest_from_object_key(&k2),
        Some((format!("{hex}.0.bad").as_str(), digest, ".0.bad"))
    );
}
