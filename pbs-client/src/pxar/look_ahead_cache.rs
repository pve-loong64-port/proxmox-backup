use std::collections::HashSet;
use std::ffi::CString;
use std::ops::Range;
use std::os::unix::io::OwnedFd;
use std::path::PathBuf;

use nix::sys::stat::FileStat;

use pxar::Metadata;
use pxar::encoder::PayloadOffset;

use super::create::*;

const DEFAULT_CACHE_SIZE: usize = 512;

pub(crate) struct CacheEntryData {
    pub(crate) fd: OwnedFd,
    pub(crate) c_file_name: CString,
    pub(crate) stat: FileStat,
    pub(crate) metadata: Metadata,
    pub(crate) payload_offset: PayloadOffset,
}

pub(crate) enum CacheEntry {
    RegEntry(CacheEntryData),
    DirEntry(CacheEntryData),
    DirEnd,
}

pub(crate) struct PxarLookaheadCache {
    // Current state of the cache
    enabled: bool,
    // Cached entries
    entries: Vec<CacheEntry>,
    // Entries encountered having more than one link given by stat
    hardlinks: HashSet<HardLinkInfo>,
    // Payload range covered by the currently cached entries
    range: Range<u64>,
    // Possible held back last chunk from last flush, used for possible chunk continuation
    last_chunk: Option<ReusableDynamicEntry>,
    // Path when started caching
    start_path: PathBuf,
    // Number of entries with file descriptors
    fd_entries: usize,
    // Max number of entries with file descriptors
    cache_size: usize,
}

impl PxarLookaheadCache {
    pub(crate) fn new(size: Option<usize>) -> Self {
        Self {
            enabled: false,
            entries: Vec::new(),
            hardlinks: HashSet::new(),
            range: 0..0,
            last_chunk: None,
            start_path: PathBuf::new(),
            fd_entries: 0,
            cache_size: size.unwrap_or(DEFAULT_CACHE_SIZE),
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        self.fd_entries >= self.cache_size
    }

    pub(crate) fn caching_enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn insert(
        &mut self,
        fd: OwnedFd,
        c_file_name: CString,
        stat: FileStat,
        metadata: Metadata,
        payload_offset: PayloadOffset,
        path: PathBuf,
    ) {
        if !self.enabled {
            self.start_path = path;
            if !metadata.is_dir() {
                self.start_path.pop();
            }
        }
        self.enabled = true;
        self.fd_entries += 1;
        if metadata.is_dir() {
            self.entries.push(CacheEntry::DirEntry(CacheEntryData {
                fd,
                c_file_name,
                stat,
                metadata,
                payload_offset,
            }))
        } else {
            self.entries.push(CacheEntry::RegEntry(CacheEntryData {
                fd,
                c_file_name,
                stat,
                metadata,
                payload_offset,
            }))
        }
    }

    pub(crate) fn insert_dir_end(&mut self) {
        self.entries.push(CacheEntry::DirEnd);
    }

    pub(crate) fn take_and_reset(&mut self) -> (Vec<CacheEntry>, PathBuf) {
        self.fd_entries = 0;
        self.enabled = false;
        // keep end for possible continuation if cache has been cleared because
        // it was full, but further caching would be fine
        self.range = self.range.end..self.range.end;
        (
            std::mem::take(&mut self.entries),
            std::mem::take(&mut self.start_path),
        )
    }

    pub(crate) fn contains_hardlink(&self, info: &HardLinkInfo) -> bool {
        self.hardlinks.contains(info)
    }

    pub(crate) fn insert_hardlink(&mut self, info: HardLinkInfo) -> bool {
        self.hardlinks.insert(info)
    }

    pub(crate) fn range(&self) -> &Range<u64> {
        &self.range
    }

    pub(crate) fn update_range(&mut self, range: Range<u64>) {
        self.range = range;
    }

    pub(crate) fn try_extend_range(&mut self, range: Range<u64>) -> bool {
        if self.range.end == 0 {
            // initialize first range to start and end with start of new range
            self.range.start = range.start;
            self.range.end = range.start;
        }

        // range continued, update end
        if self.range.end == range.start {
            self.range.end = range.end;
            return true;
        }

        false
    }

    pub(crate) fn take_last_chunk(&mut self) -> Option<ReusableDynamicEntry> {
        self.last_chunk.take()
    }

    pub(crate) fn update_last_chunk(&mut self, chunk: Option<ReusableDynamicEntry>) {
        self.last_chunk = chunk;
    }
}
