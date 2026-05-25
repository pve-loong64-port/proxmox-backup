//! Per-datastore journal used to coordinate snapshot renames with a concurrent garbage collection
//! phase-1 mark.
//!
//! # Race fixed
//!
//! GC phase 1 first calls `list_index_files()` to snapshot the set of absolute index-file paths in
//! the datastore, then iterates namespaces live and touches the atime of every referenced chunk.
//! If a `move_group`/`move_namespace` relocates a snapshot between those two steps, and the target
//! namespace is visited by GC before the source (`readdir(2)` order, not deterministic), the moved
//! index is at neither location when GC looks: missing from the target (already iterated) and
//! missing from the source (iterated after the rename). Its old path lands in the leftover
//! `unprocessed_index_list` and is discarded as a vanished file. Chunks referenced only by that
//! index never get their atime bumped and phase 2 sweeps them.
//!
//! # Protocol
//!
//! Write-ahead journal for renames:
//!
//! - **Before** renaming a snapshot, the move records the new path of each index file it is about
//!   to create, under a brief exclusive flock.
//! - At the end of phase-1 mark, GC acquires the same exclusive flock, reads every recorded path,
//!   runs the normal `index_mark_used_chunks` on each, truncates, and releases before entering phase 2.
//!
//! Why write-before-rename rather than write-after-rename with a long-held shared lock by each
//! mover: the invariant is "if the new path exists, a journal entry for it exists too". So the
//! drain - which runs only after iteration finishes - is guaranteed to catch anything iteration
//! missed:
//!
//! - If the source-ns iteration found the index at the old path, its chunks are already marked.
//!   The journal entry is then either a redundant re-mark (rename completed before the drain, LRU
//!   dedups it) or a no-op skip (rename not yet, `open_index_reader` returns `None`) - harmless
//!   either way.
//! - If the source-ns iteration missed it, then the rename already happened by the time iteration
//!   reached source, which is before the drain, so `open_index_reader(new_path)` at drain time
//!   succeeds and marks the chunks.
//!
//! A move that crashes between the journal write and the rename leaves a "ghost" entry. The
//! drain's `open_index_reader` returns `None` and skips, and the truncate step clears it. This is
//! handled by the existing vanished-file logic in the caller.
//!
//! The file lives under `/run/proxmox-backup/locks/<datastore>/move-journal`. Tmpfs is correct
//! here: a reboot aborts any in-progress GC, and the next GC rebuilds state from a fresh
//! `list_index_files()` against the post-move filesystem - there is nothing worth persisting.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Error, bail, format_err};
use nix::sys::stat::Mode;

use proxmox_sys::fs::{CreateOptions, open_file_locked};

use pbs_config::backup_user;

use crate::backup_info::DATASTORE_LOCKS_DIR;

const JOURNAL_FILENAME: &str = "move-journal";
const APPEND_LOCK_TIMEOUT: Duration = Duration::from_secs(10);
// Long enough to cover any in-flight append, if it takes longer than this something is very wrong
// and we'd rather fail GC than hang forever.
const DRAIN_LOCK_TIMEOUT: Duration = Duration::from_secs(10);

fn journal_path(datastore_name: &str) -> PathBuf {
    Path::new(DATASTORE_LOCKS_DIR)
        .join(datastore_name)
        .join(JOURNAL_FILENAME)
}

fn ensure_parent(path: &Path) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create move-journal parent dir {parent:?}"))?;
    }
    Ok(())
}

fn open_locked_exclusive(path: &Path, timeout: Duration) -> Result<File, Error> {
    ensure_parent(path)?;
    let user = backup_user()?;
    let options = CreateOptions::new()
        .perm(Mode::from_bits_truncate(0o660))
        .owner(user.uid)
        .group(user.gid);
    open_file_locked(path, timeout, true, options)
        .with_context(|| format!("failed to acquire exclusive move-journal lock at {path:?}"))
}

/// Append one or more absolute index-file paths to the journal under a brief exclusive flock. The
/// caller passes the *post-rename* paths, the rename must happen after this returns.
pub fn append_moved_indices(datastore_name: &str, paths: &[PathBuf]) -> Result<(), Error> {
    if paths.is_empty() {
        return Ok(());
    }

    let mut buf = Vec::new();
    for path in paths {
        if !path.is_absolute() {
            bail!("move-journal: refusing to record non-absolute path {path:?}");
        }
        let s = path
            .to_str()
            .ok_or_else(|| format_err!("move-journal: non-UTF-8 path {path:?}"))?;
        if s.as_bytes().contains(&b'\n') {
            bail!("move-journal: path contains newline {path:?}");
        }
        buf.extend_from_slice(s.as_bytes());
        buf.push(b'\n');
    }

    let path = journal_path(datastore_name);
    let mut file = open_locked_exclusive(&path, APPEND_LOCK_TIMEOUT)?;
    file.write_all(&buf)
        .context("failed to append to move journal")?;
    Ok(())
}

/// Drain the journal under an exclusive lock, calling `f` for each recorded path. Blocks only for
/// the brief window of a concurrent append. After the callback runs on every entry, the journal is
/// truncated under the same lock.
///
/// On a processing error the entry is left in the journal (no truncate) so the next GC will retry.
pub fn drain_move_journal<F>(datastore_name: &str, mut f: F) -> Result<(), Error>
where
    F: FnMut(&Path) -> Result<(), Error>,
{
    let path = journal_path(datastore_name);
    let mut file = open_locked_exclusive(&path, DRAIN_LOCK_TIMEOUT)?;

    file.seek(SeekFrom::Start(0))
        .context("failed to rewind move journal for draining")?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .context("failed to read move journal")?;

    for line in contents.lines() {
        let entry = line.trim();
        if entry.is_empty() {
            continue;
        }
        f(Path::new(entry))
            .with_context(|| format!("move-journal: processing '{entry}' failed"))?;
    }

    file.set_len(0)
        .context("failed to truncate move journal after drain")?;
    Ok(())
}
