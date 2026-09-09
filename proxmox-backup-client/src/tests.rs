use anyhow::Error;

use pbs_api_types::CryptMode;
use pbs_datastore::BackupManifest;
use pbs_tools::crypt_config::CryptConfig;

use super::check_previous_manifest;

fn manifest(crypt_mode: CryptMode, key: &CryptConfig) -> Result<BackupManifest, Error> {
    let mut manifest = BackupManifest::new("host/test/2026-06-11T09:16:31Z".parse()?);
    manifest.add_file(&"test.mpxar.didx".try_into()?, 200, [1; 32], crypt_mode)?;
    manifest.add_file(&"test.ppxar.didx".try_into()?, 400, [2; 32], crypt_mode)?;
    let key = (crypt_mode != CryptMode::None).then_some(key);
    Ok(serde_json::from_str(&manifest.to_string(key)?)?)
}

#[test]
fn previous_manifest_crypt_mode_transitions() -> Result<(), Error> {
    let key = CryptConfig::new([7; 32])?;
    let modes = [CryptMode::None, CryptMode::SignOnly, CryptMode::Encrypt];

    for previous_mode in modes {
        let previous = manifest(previous_mode, &key)?;
        for current_mode in modes {
            let current_key = (current_mode != CryptMode::None).then_some(&key);
            let compatible =
                (previous_mode == CryptMode::Encrypt) == (current_mode == CryptMode::Encrypt);
            assert_eq!(
                check_previous_manifest(&previous, current_key, current_mode).is_ok(),
                compatible,
                "{previous_mode:?} -> {current_mode:?}",
            );
        }
    }

    Ok(())
}

#[test]
fn previous_manifest_key_mismatch() -> Result<(), Error> {
    let key = CryptConfig::new([7; 32])?;
    let other_key = CryptConfig::new([8; 32])?;

    for mode in [CryptMode::SignOnly, CryptMode::Encrypt] {
        let previous = manifest(mode, &key)?;
        assert!(check_previous_manifest(&previous, Some(&other_key), mode).is_err());
    }

    Ok(())
}

#[test]
fn previous_manifest_mixed_chunk_modes() -> Result<(), Error> {
    let key = CryptConfig::new([7; 32])?;
    let mut previous = manifest(CryptMode::None, &key)?;
    previous.add_file(
        &"disk.img.fidx".try_into()?,
        800,
        [3; 32],
        CryptMode::Encrypt,
    )?;

    for mode in [CryptMode::None, CryptMode::SignOnly, CryptMode::Encrypt] {
        let current_key = (mode != CryptMode::None).then_some(&key);
        assert!(check_previous_manifest(&previous, current_key, mode).is_err());
    }

    Ok(())
}
