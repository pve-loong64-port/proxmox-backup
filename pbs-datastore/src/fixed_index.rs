use std::fs::File;
use std::io::Write;
use std::io::{Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::Arc;

use anyhow::{bail, format_err, Error};

use proxmox_io::ReadExt;
use proxmox_uuid::Uuid;

use crate::chunk_store::ChunkStore;
use crate::file_formats;
use crate::index::{ChunkReadInfo, IndexFile};

/// Header format definition for fixed index files (`.fidx`)
#[repr(C)]
pub struct FixedIndexHeader {
    pub magic: [u8; 8],
    pub uuid: [u8; 16],
    pub ctime: i64,
    /// Sha256 over the index ``SHA256(digest1||digest2||...)``
    pub index_csum: [u8; 32],
    pub size: u64,
    pub chunk_size: u64,
    reserved: [u8; 4016], // overall size is one page (4096 bytes)
}
proxmox_lang::static_assert_size!(FixedIndexHeader, 4096);

// split image into fixed size chunks

pub struct FixedIndexReader {
    _file: File,
    pub chunk_size: usize,
    pub size: u64,
    index_length: usize,
    index: *mut u8,
    pub uuid: [u8; 16],
    pub ctime: i64,
    pub index_csum: [u8; 32],
}

// `index` is mmap()ed which cannot be thread-local so should be sendable
unsafe impl Send for FixedIndexReader {}
unsafe impl Sync for FixedIndexReader {}

impl Drop for FixedIndexReader {
    fn drop(&mut self) {
        if let Err(err) = self.unmap() {
            log::error!("Unable to unmap file - {}", err);
        }
    }
}

impl FixedIndexReader {
    pub fn open(path: &Path) -> Result<Self, Error> {
        File::open(path)
            .map_err(Error::from)
            .and_then(Self::new)
            .map_err(|err| format_err!("Unable to open fixed index {:?} - {}", path, err))
    }

    pub fn new(mut file: std::fs::File) -> Result<Self, Error> {
        file.seek(SeekFrom::Start(0))?;

        let header_size = std::mem::size_of::<FixedIndexHeader>();

        let stat = match nix::sys::stat::fstat(file.as_raw_fd()) {
            Ok(stat) => stat,
            Err(err) => bail!("fstat failed - {}", err),
        };

        let size = stat.st_size as usize;

        if size < header_size {
            bail!("index too small ({})", stat.st_size);
        }

        let header: Box<FixedIndexHeader> = unsafe { file.read_host_value_boxed()? };

        if header.magic != file_formats::FIXED_SIZED_CHUNK_INDEX_1_0 {
            bail!("got unknown magic number");
        }

        let size = u64::from_le(header.size);
        let ctime = i64::from_le(header.ctime);
        let chunk_size = u64::from_le(header.chunk_size);

        let index_length = size.div_ceil(chunk_size) as usize;
        let index_size = index_length * 32;

        let expected_index_size = (stat.st_size as usize) - header_size;
        if index_size != expected_index_size {
            bail!(
                "got unexpected file size ({} != {})",
                index_size,
                expected_index_size
            );
        }

        let data = unsafe {
            nix::sys::mman::mmap(
                None,
                std::num::NonZeroUsize::new(index_size)
                    .ok_or_else(|| format_err!("invalid index size"))?,
                nix::sys::mman::ProtFlags::PROT_READ,
                nix::sys::mman::MapFlags::MAP_PRIVATE,
                &file,
                header_size as i64,
            )
        }?
        .as_ptr()
        .cast::<u8>();

        Ok(Self {
            _file: file,
            chunk_size: chunk_size as usize,
            size,
            index_length,
            index: data,
            ctime,
            uuid: header.uuid,
            index_csum: header.index_csum,
        })
    }

    fn unmap(&mut self) -> Result<(), Error> {
        let Some(index) = NonNull::new(self.index as *mut std::ffi::c_void) else {
            return Ok(());
        };

        let index_size = self.index_length * 32;

        if let Err(err) = unsafe { nix::sys::mman::munmap(index, index_size) } {
            bail!("unmap file failed - {}", err);
        }

        self.index = std::ptr::null_mut();

        Ok(())
    }
}

impl IndexFile for FixedIndexReader {
    fn index_count(&self) -> usize {
        self.index_length
    }

    fn index_digest(&self, pos: usize) -> Option<&[u8; 32]> {
        if pos >= self.index_length {
            None
        } else {
            Some(unsafe { &*(self.index.add(pos * 32) as *const [u8; 32]) })
        }
    }

    fn index_bytes(&self) -> u64 {
        self.size
    }

    fn chunk_info(&self, pos: usize) -> Option<ChunkReadInfo> {
        if pos >= self.index_length {
            return None;
        }

        let start = (pos * self.chunk_size) as u64;
        let mut end = start + self.chunk_size as u64;

        if end > self.size {
            end = self.size;
        }

        let digest = self.index_digest(pos).unwrap();
        Some(ChunkReadInfo {
            range: start..end,
            digest: *digest,
        })
    }

    fn index_ctime(&self) -> i64 {
        self.ctime
    }

    fn index_size(&self) -> usize {
        self.size as usize
    }

    fn compute_csum(&self) -> ([u8; 32], u64) {
        let mut csum = openssl::sha::Sha256::new();
        let mut chunk_end = 0;
        for pos in 0..self.index_count() {
            let info = self.chunk_info(pos).unwrap();
            chunk_end = info.range.end;
            csum.update(&info.digest);
        }
        let csum = csum.finish();

        (csum, chunk_end)
    }

    fn chunk_from_offset(&self, offset: u64) -> Option<(usize, u64)> {
        if offset >= self.size {
            return None;
        }

        Some((
            (offset / self.chunk_size as u64) as usize,
            offset & (self.chunk_size - 1) as u64, // fast modulo, valid for 2^x chunk_size
        ))
    }
}

pub struct FixedIndexWriter {
    file: File,
    filename: PathBuf,
    tmp_filename: PathBuf,
    chunk_size: usize,
    size: usize,
    index_length: usize,
    index: *mut u8,
    pub uuid: [u8; 16],
    pub ctime: i64,
}

// `index` is mmap()ed which cannot be thread-local so should be sendable
unsafe impl Send for FixedIndexWriter {}

impl Drop for FixedIndexWriter {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.tmp_filename); // ignore errors
        if let Err(err) = self.unmap() {
            log::error!("Unable to unmap file {:?} - {}", self.tmp_filename, err);
        }
    }
}

impl FixedIndexWriter {
    #[allow(clippy::cast_ptr_alignment)]
    // Requires obtaining a shared chunk store lock beforehand
    pub fn create(
        store: Arc<ChunkStore>,
        path: &Path,
        size: usize,
        chunk_size: usize,
    ) -> Result<Self, Error> {
        let full_path = store.relative_path(path);
        let mut tmp_path = full_path.clone();
        tmp_path.set_extension("tmp_fidx");

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&tmp_path)?;

        let header_size = std::mem::size_of::<FixedIndexHeader>();

        // todo: use static assertion when available in rust
        if header_size != 4096 {
            panic!("got unexpected header size");
        }

        let ctime = proxmox_time::epoch_i64();

        let uuid = Uuid::generate();

        let buffer = vec![0u8; header_size];
        let header = unsafe { &mut *(buffer.as_ptr() as *mut FixedIndexHeader) };

        header.magic = file_formats::FIXED_SIZED_CHUNK_INDEX_1_0;
        header.ctime = i64::to_le(ctime);
        header.size = u64::to_le(size as u64);
        header.chunk_size = u64::to_le(chunk_size as u64);
        header.uuid = *uuid.as_bytes();

        header.index_csum = [0u8; 32];

        file.write_all(&buffer)?;

        let index_length = size.div_ceil(chunk_size);
        let index_size = index_length * 32;
        nix::unistd::ftruncate(&file, (header_size + index_size) as i64)?;

        let data = unsafe {
            nix::sys::mman::mmap(
                None,
                std::num::NonZeroUsize::new(index_size)
                    .ok_or_else(|| format_err!("invalid index size"))?,
                nix::sys::mman::ProtFlags::PROT_READ | nix::sys::mman::ProtFlags::PROT_WRITE,
                nix::sys::mman::MapFlags::MAP_SHARED,
                &file,
                header_size as i64,
            )
        }?
        .as_ptr()
        .cast::<u8>();

        Ok(Self {
            file,
            filename: full_path,
            tmp_filename: tmp_path,
            chunk_size,
            size,
            index_length,
            index: data,
            ctime,
            uuid: *uuid.as_bytes(),
        })
    }

    pub fn index_length(&self) -> usize {
        self.index_length
    }

    fn unmap(&mut self) -> Result<(), Error> {
        let Some(index) = NonNull::new(self.index as *mut std::ffi::c_void) else {
            return Ok(());
        };

        let index_size = self.index_length * 32;

        if let Err(err) = unsafe { nix::sys::mman::munmap(index, index_size) } {
            bail!("unmap file {:?} failed - {}", self.tmp_filename, err);
        }

        self.index = std::ptr::null_mut();

        Ok(())
    }

    pub fn close(&mut self) -> Result<[u8; 32], Error> {
        if self.index.is_null() {
            bail!("cannot close already closed index file.");
        }

        let index_size = self.index_length * 32;
        let data = unsafe { std::slice::from_raw_parts(self.index, index_size) };
        let index_csum = openssl::sha::sha256(data);

        self.unmap()?;

        let csum_offset = std::mem::offset_of!(FixedIndexHeader, index_csum);
        self.file.seek(SeekFrom::Start(csum_offset as u64))?;
        self.file.write_all(&index_csum)?;
        self.file.flush()?;

        if let Err(err) = std::fs::rename(&self.tmp_filename, &self.filename) {
            bail!("Atomic rename file {:?} failed - {}", self.filename, err);
        }

        Ok(index_csum)
    }

    pub fn check_chunk_alignment(&self, offset: usize, chunk_len: usize) -> Result<usize, Error> {
        if offset < chunk_len {
            bail!("got chunk with small offset ({} < {}", offset, chunk_len);
        }

        let pos = offset - chunk_len;

        if offset > self.size {
            bail!("chunk data exceeds size ({} >= {})", offset, self.size);
        }

        // last chunk can be smaller
        if ((offset != self.size) && (chunk_len != self.chunk_size))
            || (chunk_len > self.chunk_size)
            || (chunk_len == 0)
        {
            bail!(
                "chunk with unexpected length ({} != {}",
                chunk_len,
                self.chunk_size
            );
        }

        if pos & (self.chunk_size - 1) != 0 {
            bail!("got unaligned chunk (pos = {})", pos);
        }

        Ok(pos / self.chunk_size)
    }

    pub fn add_digest(&mut self, index: usize, digest: &[u8; 32]) -> Result<(), Error> {
        if index >= self.index_length {
            bail!(
                "add digest failed - index out of range ({} >= {})",
                index,
                self.index_length
            );
        }

        if self.index.is_null() {
            bail!("cannot write to closed index file.");
        }

        let index_pos = index * 32;
        unsafe {
            let dst = self.index.add(index_pos);
            dst.copy_from_nonoverlapping(digest.as_ptr(), 32);
        }

        Ok(())
    }

    pub fn clone_data_from(&mut self, reader: &FixedIndexReader) -> Result<(), Error> {
        if self.index_length != reader.index_count() {
            bail!("clone_data_from failed - index sizes not equal");
        }

        for i in 0..self.index_length {
            self.add_digest(i, reader.index_digest(i).unwrap())?;
        }

        Ok(())
    }
}
