//! Sync datastore contents from source to target, either in push or pull direction

use std::time::Duration;

#[derive(Default)]
pub(crate) struct RemovedVanishedStats {
    pub(crate) groups: usize,
    pub(crate) snapshots: usize,
    pub(crate) namespaces: usize,
}

impl RemovedVanishedStats {
    pub(crate) fn add(&mut self, rhs: RemovedVanishedStats) {
        self.groups += rhs.groups;
        self.snapshots += rhs.snapshots;
        self.namespaces += rhs.namespaces;
    }
}

#[derive(Default)]
pub(crate) struct SyncStats {
    pub(crate) chunk_count: usize,
    pub(crate) bytes: usize,
    pub(crate) elapsed: Duration,
    pub(crate) removed: Option<RemovedVanishedStats>,
}

impl From<RemovedVanishedStats> for SyncStats {
    fn from(removed: RemovedVanishedStats) -> Self {
        Self {
            removed: Some(removed),
            ..Default::default()
        }
    }
}

impl SyncStats {
    pub(crate) fn add(&mut self, rhs: SyncStats) {
        self.chunk_count += rhs.chunk_count;
        self.bytes += rhs.bytes;
        self.elapsed += rhs.elapsed;

        if let Some(rhs_removed) = rhs.removed {
            if let Some(ref mut removed) = self.removed {
                removed.add(rhs_removed);
            } else {
                self.removed = Some(rhs_removed);
            }
        }
    }
}
