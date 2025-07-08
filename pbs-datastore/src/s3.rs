use std::path::{Path, PathBuf};

use anyhow::{bail, format_err, Error};

use proxmox_s3_client::S3ObjectKey;

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
    use hex::FromHex;
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
    use hex::FromHex;
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
    use hex::FromHex;
    let digest =
        <[u8; 32]>::from_hex("bb9f8df61474d25e71fa00722318cd387396ca1736605e1248821cc0de3d3af8")
            .unwrap();
    assert!(object_key_from_digest_with_suffix(&digest, "/.0.bad").is_err());
}
