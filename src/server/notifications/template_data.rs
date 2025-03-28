use pbs_api_types::GarbageCollectionStatus;
use serde::Serialize;

// NOTE: For some of these types, the `XyzOkTemplateData` and `XyzErrTemplateData`
// types are almost identical except for the `error` member.
// While at first glance I might make sense
// to consolidate the two and make `error` an `Option`, I would argue
// that it is actually quite nice to have a single, distinct type for
// each template. This makes it 100% clear which params are accessible
// for every single template, at the cost of some boilerplate code.

/// Template data which should be available in *all* notifications.
/// The fields of this struct will be flattened into the individual
/// *TemplateData structs.
#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CommonData {
    /// The hostname of the PBS host.
    pub hostname: String,
    /// The FQDN of the PBS host.
    pub fqdn: String,
    /// The base URL for building links to the web interface.
    pub base_url: String,
}

impl CommonData {
    pub fn new() -> CommonData {
        let nodename = proxmox_sys::nodename();
        let mut fqdn = nodename.to_owned();

        if let Ok(resolv_conf) = crate::api2::node::dns::read_etc_resolv_conf() {
            if let Some(search) = resolv_conf["search"].as_str() {
                fqdn.push('.');
                fqdn.push_str(search);
            }
        }

        // TODO: Some users might want to be able to override this.
        let base_url = format!("https://{fqdn}:8007");

        CommonData {
            hostname: nodename.into(),
            fqdn,
            base_url,
        }
    }
}

/// Template data for the gc-ok template.
#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct GcOkTemplateData {
    /// Common properties.
    #[serde(flatten)]
    pub common: CommonData,
    /// The datastore.
    pub datastore: String,
    /// The task's UPID.
    pub upid: Option<String>,
    /// Number of processed index files.
    pub index_file_count: usize,
    /// Sum of bytes referred by index files.
    pub index_data_bytes: u64,
    /// Bytes used on disk.
    pub disk_bytes: u64,
    /// Chunks used on disk.
    pub disk_chunks: usize,
    /// Sum of removed bytes.
    pub removed_bytes: u64,
    /// Number of removed chunks.
    pub removed_chunks: usize,
    /// Sum of pending bytes (pending removal - kept for safety).
    pub pending_bytes: u64,
    /// Number of pending chunks (pending removal - kept for safety).
    pub pending_chunks: usize,
    /// Number of chunks marked as .bad by verify that have been removed by GC.
    pub removed_bad: usize,
    /// Number of chunks still marked as .bad after garbage collection.
    pub still_bad: usize,
    /// Factor of deduplication.
    pub deduplication_factor: String,
}

impl GcOkTemplateData {
    /// Create new a new instance.
    pub fn new(datastore: String, status: &GarbageCollectionStatus) -> Self {
        let deduplication_factor = if status.disk_bytes > 0 {
            (status.index_data_bytes as f64) / (status.disk_bytes as f64)
        } else {
            1.0
        };
        let deduplication_factor = format!("{:.2}", deduplication_factor);

        Self {
            common: CommonData::new(),
            datastore,
            upid: status.upid.clone(),
            index_file_count: status.index_file_count,
            index_data_bytes: status.index_data_bytes,
            disk_bytes: status.disk_bytes,
            disk_chunks: status.disk_chunks,
            removed_bytes: status.removed_bytes,
            removed_chunks: status.removed_chunks,
            pending_bytes: status.pending_bytes,
            pending_chunks: status.pending_chunks,
            removed_bad: status.removed_bad,
            still_bad: status.still_bad,
            deduplication_factor,
        }
    }
}

/// Template data for the gc-err template.
#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct GcErrTemplateData {
    /// Common properties.
    #[serde(flatten)]
    pub common: CommonData,
    /// The datastore.
    pub datastore: String,
    /// The error that occured during the GC job.
    pub error: String,
}

impl GcErrTemplateData {
    /// Create new a new instance.
    pub fn new(datastore: String, error: String) -> Self {
        Self {
            common: CommonData::new(),
            datastore,
            error,
        }
    }
}
