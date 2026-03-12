use std::collections::HashMap;
use std::fs;
use std::io::ErrorKind;
use std::sync::LazyLock;
use std::time::SystemTime;

use anyhow::{bail, format_err, Error};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, Value};

use proxmox_sys::fs::CreateOptions;
use proxmox_time::epoch_i64;

use pbs_api_types::Authid;
//use crate::auth;
use crate::{open_backup_lockfile, BackupLockGuard};

const LOCK_FILE: &str = pbs_buildcfg::configdir!("/token.shadow.lock");
const CONF_FILE: &str = pbs_buildcfg::configdir!("/token.shadow");

/// Global in-memory cache for successfully verified API token secrets.
/// The cache stores plain text secrets for token Authids that have already been
/// verified against the hashed values in `token.shadow`. This allows for cheap
/// subsequent authentications for the same token+secret combination, avoiding
/// recomputing the password hash on every request.
static TOKEN_SECRET_CACHE: LazyLock<RwLock<ApiTokenSecretCache>> = LazyLock::new(|| {
    RwLock::new(ApiTokenSecretCache {
        secrets: HashMap::new(),
        shared_gen: 0,
        shadow: None,
    })
});

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// ApiToken id / secret pair
pub struct ApiTokenSecret {
    pub tokenid: Authid,
    pub secret: String,
}

// Get exclusive lock
fn lock_config() -> Result<BackupLockGuard, Error> {
    open_backup_lockfile(LOCK_FILE, None, true)
}

fn read_file() -> Result<HashMap<Authid, String>, Error> {
    let json = proxmox_sys::fs::file_get_json(CONF_FILE, Some(Value::Null))?;

    if json == Value::Null {
        Ok(HashMap::new())
    } else {
        // swallow serde error which might contain sensitive data
        from_value(json).map_err(|_err| format_err!("unable to parse '{}'", CONF_FILE))
    }
}

fn write_file(data: HashMap<Authid, String>) -> Result<(), Error> {
    let backup_user = crate::backup_user()?;
    let options = CreateOptions::new()
        .perm(nix::sys::stat::Mode::from_bits_truncate(0o0640))
        .owner(backup_user.uid)
        .group(backup_user.gid);

    let json = serde_json::to_vec(&data)?;
    proxmox_sys::fs::replace_file(CONF_FILE, &json, options, true)
}

/// Refreshes the in-memory cache if the on-disk token.shadow file changed.
/// Returns true if the cache is valid to use, false if not.
fn refresh_cache_if_file_changed() -> bool {
    let now = epoch_i64();

    // Best-effort refresh under write lock.
    let Some(mut cache) = TOKEN_SECRET_CACHE.try_write() else {
        return false;
    };

    let Some(shared_gen_now) = token_shadow_shared_gen() else {
        return false;
    };

    // If another process bumped the generation, we don't know what changed -> clear cache
    if cache.shared_gen != shared_gen_now {
        cache.reset_and_set_gen(shared_gen_now);
    }

    // Stat the file to detect manual edits.
    let Ok((new_mtime, new_len)) = shadow_mtime_len() else {
        return false;
    };

    // If the file didn't change, only update last_checked
    if let Some(shadow) = cache.shadow.as_mut() {
        if shadow.mtime == new_mtime && shadow.len == new_len {
            shadow.last_checked = now;
            return true;
        }
    }

    cache.secrets.clear();

    let prev = cache.shadow.replace(ShadowFileInfo {
        mtime: new_mtime,
        len: new_len,
        last_checked: now,
    });

    if prev.is_some() {
        // Best-effort propagation to other processes if a change was detected
        if let Some(shared_gen_new) = bump_token_shadow_shared_gen() {
            cache.shared_gen = shared_gen_new;
        }
    }

    false
}

/// Verifies that an entry for given tokenid / API token secret exists
pub fn verify_secret(tokenid: &Authid, secret: &str) -> Result<(), Error> {
    if !tokenid.is_token() {
        bail!("not an API token ID");
    }

    // Fast path
    if refresh_cache_if_file_changed() && cache_try_secret_matches(tokenid, secret) {
        return Ok(());
    }

    // Slow path
    // First, capture the shared generation before doing the hash verification.
    let gen_before = token_shadow_shared_gen();

    let data = read_file()?;
    match data.get(tokenid) {
        Some(hashed_secret) => {
            proxmox_sys::crypt::verify_crypt_pw(secret, hashed_secret)?;

            // Try to cache only if nothing changed while verifying the secret.
            if let Some(gen_before) = gen_before {
                cache_try_insert_secret(tokenid.clone(), secret.to_owned(), gen_before);
            }

            Ok(())
        }
        None => bail!("invalid API token"),
    }
}

/// Generates a new secret for the given tokenid / API token, sets it then returns it.
/// The secret is stored as salted hash.
pub fn generate_and_set_secret(tokenid: &Authid) -> Result<String, Error> {
    let secret = format!("{:x}", proxmox_uuid::Uuid::generate());
    set_secret(tokenid, &secret)?;
    Ok(secret)
}

/// Adds a new entry for the given tokenid / API token secret. The secret is stored as salted hash.
fn set_secret(tokenid: &Authid, secret: &str) -> Result<(), Error> {
    if !tokenid.is_token() {
        bail!("not an API token ID");
    }

    let guard = lock_config()?;

    // Capture state before we write to detect external edits.
    let pre_meta = shadow_mtime_len().unwrap_or((None, None));

    let mut data = read_file()?;
    let hashed_secret = proxmox_sys::crypt::encrypt_pw(secret)?;
    data.insert(tokenid.clone(), hashed_secret);
    write_file(data)?;

    apply_api_mutation(guard, tokenid, Some(secret), pre_meta);

    Ok(())
}

/// Deletes the entry for the given tokenid.
pub fn delete_secret(tokenid: &Authid) -> Result<(), Error> {
    if !tokenid.is_token() {
        bail!("not an API token ID");
    }

    let guard = lock_config()?;

    // Capture state before we write to detect external edits.
    let pre_meta = shadow_mtime_len().unwrap_or((None, None));

    let mut data = read_file()?;
    data.remove(tokenid);
    write_file(data)?;

    apply_api_mutation(guard, tokenid, None, pre_meta);

    Ok(())
}

/// Cached secret.
struct CachedSecret {
    secret: String,
}

struct ApiTokenSecretCache {
    /// Keys are token Authids, values are the corresponding plain text secrets.
    /// Entries are added after a successful on-disk verification in
    /// `verify_secret` or when a new token secret is generated by
    /// `generate_and_set_secret`. Used to avoid repeated
    /// password-hash computation on subsequent authentications.
    secrets: HashMap<Authid, CachedSecret>,
    /// Shared generation to detect mutations of the underlying token.shadow file.
    shared_gen: usize,
    /// Shadow file info to detect changes
    shadow: Option<ShadowFileInfo>,
}

impl ApiTokenSecretCache {
    /// Resets all local cache contents and sets/updates the cached generation.
    fn reset_and_set_gen(&mut self, new_gen: usize) {
        self.secrets.clear();
        self.shared_gen = new_gen;
        self.shadow = None;
    }

    /// Caches a secret and sets/updates the cache generation.
    fn insert_and_set_gen(&mut self, tokenid: Authid, secret: CachedSecret, new_gen: usize) {
        self.secrets.insert(tokenid, secret);
        self.shared_gen = new_gen;
    }

    /// Evicts a cached secret and sets/updates the cached generation.
    fn evict_and_set_gen(&mut self, tokenid: &Authid, new_gen: usize) {
        self.secrets.remove(tokenid);
        self.shared_gen = new_gen;
    }
}

/// Shadow file info
struct ShadowFileInfo {
    // shadow file mtime to detect changes
    mtime: Option<SystemTime>,
    // shadow file length to detect changes
    len: Option<u64>,
    // last time the file metadata was checked
    last_checked: i64,
}

fn cache_try_insert_secret(tokenid: Authid, secret: String, shared_gen_before: usize) {
    let Some(mut cache) = TOKEN_SECRET_CACHE.try_write() else {
        return;
    };

    let Some(shared_gen_now) = token_shadow_shared_gen() else {
        return;
    };

    // If this process missed a generation bump, its cache is stale.
    if cache.shared_gen != shared_gen_now {
        cache.reset_and_set_gen(shared_gen_now);
    }

    // If a mutation happened while we were verifying the secret, do not insert.
    if shared_gen_now == shared_gen_before {
        cache.insert_and_set_gen(tokenid, CachedSecret { secret }, shared_gen_now);
    }
}

/// Tries to match the given token secret against the cached secret.
///
/// Verifies the generation/version before doing the constant-time
/// comparison to reduce TOCTOU risk. During token rotation or deletion
/// tokens for in-flight requests may still validate against the previous
/// generation.
fn cache_try_secret_matches(tokenid: &Authid, secret: &str) -> bool {
    let Some(cache) = TOKEN_SECRET_CACHE.try_read() else {
        return false;
    };
    let Some(entry) = cache.secrets.get(tokenid) else {
        return false;
    };
    let Some(current_gen) = token_shadow_shared_gen() else {
        return false;
    };

    if current_gen == cache.shared_gen {
        let cached_secret_bytes = entry.secret.as_bytes();
        let secret_bytes = secret.as_bytes();

        return cached_secret_bytes.len() == secret_bytes.len()
            && openssl::memcmp::eq(cached_secret_bytes, secret_bytes);
    }

    false
}

fn apply_api_mutation(
    _guard: BackupLockGuard,
    tokenid: &Authid,
    new_secret: Option<&str>,
    pre_write_meta: (Option<SystemTime>, Option<u64>),
) {
    let now = epoch_i64();

    // Signal cache invalidation to other processes (best-effort).
    let bumped_gen = bump_token_shadow_shared_gen();

    let mut cache = TOKEN_SECRET_CACHE.write();

    // If we cannot get the current generation, we cannot trust the cache
    let Some(current_gen) = token_shadow_shared_gen() else {
        cache.reset_and_set_gen(0);
        return;
    };

    // If we cannot bump the shared generation, or if it changed after
    // obtaining the cache write lock, we cannot trust the cache
    if bumped_gen != Some(current_gen) {
        cache.reset_and_set_gen(current_gen);
        return;
    }

    // If our cached file metadata does not match the on-disk state before our write,
    // we likely missed an external/manual edit. We can no longer trust any cached secrets.
    if cache
        .shadow
        .as_ref()
        .is_some_and(|s| (s.mtime, s.len) != pre_write_meta)
    {
        cache.secrets.clear();
    }

    // Apply the new mutation.
    match new_secret {
        Some(secret) => {
            let cached_secret = CachedSecret {
                secret: secret.to_owned(),
            };
            cache.insert_and_set_gen(tokenid.clone(), cached_secret, current_gen);
        }
        None => cache.evict_and_set_gen(tokenid, current_gen),
    }

    // Update our view of the file metadata to the post-write state (best-effort).
    // (If this fails, drop local cache so callers fall back to slow path until refreshed.)
    match shadow_mtime_len() {
        Ok((mtime, len)) => {
            cache.shadow = Some(ShadowFileInfo {
                mtime,
                len,
                last_checked: now,
            });
        }
        Err(_) => {
            // If we cannot validate state, do not trust cache.
            cache.reset_and_set_gen(current_gen);
        }
    }
}

/// Get the current shared generation.
fn token_shadow_shared_gen() -> Option<usize> {
    crate::ConfigVersionCache::new()
        .ok()
        .map(|cvc| cvc.token_shadow_generation())
}

/// Bump and return the new shared generation.
fn bump_token_shadow_shared_gen() -> Option<usize> {
    crate::ConfigVersionCache::new()
        .ok()
        .map(|cvc| cvc.increase_token_shadow_generation() + 1)
}

fn shadow_mtime_len() -> Result<(Option<SystemTime>, Option<u64>), Error> {
    match fs::metadata(CONF_FILE) {
        Ok(meta) => Ok((meta.modified().ok(), Some(meta.len()))),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok((None, None)),
        Err(e) => Err(e.into()),
    }
}
