use anyhow::Error;

use pbs_api_types::CryptMode;
use pbs_datastore::BackupManifest;

use super::check_previous_chunk_modes;

fn manifest(mode: CryptMode) -> Result<BackupManifest, Error> {
    let mut manifest = BackupManifest::new("host/test/2026-06-11T09:16:31Z".parse()?);
    manifest.add_file(&"test.pxar.didx".try_into()?, 200, [1; 32], mode)?;
    Ok(manifest)
}

#[test]
fn previous_chunk_mode_transitions() -> Result<(), Error> {
    let modes = [CryptMode::None, CryptMode::SignOnly, CryptMode::Encrypt];
    for previous_mode in modes {
        let previous = manifest(previous_mode)?;
        for source_mode in modes {
            let source = manifest(source_mode)?;
            for encrypt in [false, true] {
                let target_encrypted = encrypt || source_mode == CryptMode::Encrypt;
                assert_eq!(
                    check_previous_chunk_modes(&previous, &source, encrypt).is_ok(),
                    (previous_mode == CryptMode::Encrypt) == target_encrypted,
                    "{previous_mode:?} -> {source_mode:?}, encrypt: {encrypt}",
                );
            }
        }
    }
    Ok(())
}

#[test]
fn previous_mixed_chunk_modes() -> Result<(), Error> {
    let mut source = manifest(CryptMode::None)?;
    source.add_file(
        &"disk.img.fidx".try_into()?,
        400,
        [2; 32],
        CryptMode::Encrypt,
    )?;
    let mut previous = manifest(CryptMode::SignOnly)?;
    previous.add_file(
        &"disk.img.fidx".try_into()?,
        400,
        [2; 32],
        CryptMode::Encrypt,
    )?;
    assert!(check_previous_chunk_modes(&previous, &source, false).is_ok());
    assert!(check_previous_chunk_modes(&previous, &source, true).is_err());
    Ok(())
}

#[test]
fn previous_unreferenced_archives() -> Result<(), Error> {
    let source = manifest(CryptMode::None)?;
    let mut previous = manifest(CryptMode::SignOnly)?;
    previous.add_file(
        &"old.img.fidx".try_into()?,
        400,
        [2; 32],
        CryptMode::Encrypt,
    )?;
    previous.add_file(&"config.blob".try_into()?, 400, [3; 32], CryptMode::Encrypt)?;
    assert!(check_previous_chunk_modes(&previous, &source, false).is_ok());
    Ok(())
}
