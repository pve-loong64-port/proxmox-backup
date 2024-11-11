//! Implements counters to generate statistics for log outputs during uploads with backup writer

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::pxar::create::ReusableDynamicEntry;

/// Basic backup run statistics and archive checksum
pub struct BackupStats {
    pub size: u64,
    pub csum: [u8; 32],
    pub duration: Duration,
    pub chunk_count: u64,
}

/// Extended backup run statistics and archive checksum
pub(crate) struct UploadStats {
    pub(crate) chunk_count: usize,
    pub(crate) chunk_reused: usize,
    pub(crate) chunk_injected: usize,
    pub(crate) size: usize,
    pub(crate) size_reused: usize,
    pub(crate) size_injected: usize,
    pub(crate) size_compressed: usize,
    pub(crate) duration: Duration,
    pub(crate) csum: [u8; 32],
}

impl UploadStats {
    /// Convert the upload stats to the more concise [`BackupStats`]
    #[inline(always)]
    pub(crate) fn to_backup_stats(&self) -> BackupStats {
        BackupStats {
            chunk_count: self.chunk_count as u64,
            size: self.size as u64,
            duration: self.duration,
            csum: self.csum,
        }
    }
}

/// Atomic counters for accounting upload stream progress information
#[derive(Clone)]
pub(crate) struct UploadCounters {
    injected_chunk_count: Arc<AtomicUsize>,
    known_chunk_count: Arc<AtomicUsize>,
    total_chunk_count: Arc<AtomicUsize>,
    compressed_stream_len: Arc<AtomicU64>,
    injected_stream_len: Arc<AtomicUsize>,
    reused_stream_len: Arc<AtomicUsize>,
    total_stream_len: Arc<AtomicUsize>,
}

impl UploadCounters {
    /// Create and zero init new upload counters
    pub(crate) fn new() -> Self {
        Self {
            total_chunk_count: Arc::new(AtomicUsize::new(0)),
            injected_chunk_count: Arc::new(AtomicUsize::new(0)),
            known_chunk_count: Arc::new(AtomicUsize::new(0)),
            compressed_stream_len: Arc::new(AtomicU64::new(0)),
            injected_stream_len: Arc::new(AtomicUsize::new(0)),
            reused_stream_len: Arc::new(AtomicUsize::new(0)),
            total_stream_len: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[inline(always)]
    pub(crate) fn add_known_chunk(&mut self, chunk_len: usize) -> usize {
        self.known_chunk_count.fetch_add(1, Ordering::SeqCst);
        self.total_chunk_count.fetch_add(1, Ordering::SeqCst);
        self.reused_stream_len
            .fetch_add(chunk_len, Ordering::SeqCst);
        self.total_stream_len.fetch_add(chunk_len, Ordering::SeqCst)
    }

    #[inline(always)]
    pub(crate) fn add_new_chunk(&mut self, chunk_len: usize, chunk_raw_size: u64) -> usize {
        self.total_chunk_count.fetch_add(1, Ordering::SeqCst);
        self.compressed_stream_len
            .fetch_add(chunk_raw_size, Ordering::SeqCst);
        self.total_stream_len.fetch_add(chunk_len, Ordering::SeqCst)
    }

    #[inline(always)]
    pub(crate) fn add_injected_chunk(&mut self, chunk: &ReusableDynamicEntry) -> usize {
        self.total_chunk_count.fetch_add(1, Ordering::SeqCst);
        self.injected_chunk_count.fetch_add(1, Ordering::SeqCst);

        self.reused_stream_len
            .fetch_add(chunk.size() as usize, Ordering::SeqCst);
        self.injected_stream_len
            .fetch_add(chunk.size() as usize, Ordering::SeqCst);
        self.total_stream_len
            .fetch_add(chunk.size() as usize, Ordering::SeqCst)
    }

    #[inline(always)]
    pub(crate) fn total_stream_len(&self) -> usize {
        self.total_stream_len.load(Ordering::SeqCst)
    }

    /// Convert the counters to [`UploadStats`], including given archive checksum and runtime.
    #[inline(always)]
    pub(crate) fn to_upload_stats(&self, csum: [u8; 32], duration: Duration) -> UploadStats {
        UploadStats {
            chunk_count: self.total_chunk_count.load(Ordering::SeqCst),
            chunk_reused: self.known_chunk_count.load(Ordering::SeqCst),
            chunk_injected: self.injected_chunk_count.load(Ordering::SeqCst),
            size: self.total_stream_len.load(Ordering::SeqCst),
            size_reused: self.reused_stream_len.load(Ordering::SeqCst),
            size_injected: self.injected_stream_len.load(Ordering::SeqCst),
            size_compressed: self.compressed_stream_len.load(Ordering::SeqCst) as usize,
            duration,
            csum,
        }
    }
}
