use std::fs::File;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;

use anyhow::{Context, Error, bail, format_err};

use proxmox_uuid::Uuid;

use crate::file_formats;
use crate::index::{ChunkReadInfo, IndexFile};
use crate::index_mmap::IndexMmap;

/// Chunk size for fixed index files.
///
/// Currently the Proxmox Backup Server API is limited to only allow writing
/// fixed index files with 4M size. This may change in the future but is
/// enforced for now.
pub const FIXED_CHUNK_SIZE: u32 = 4096 * 1024; // TODO: other allowed sizes?

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
    mmap: IndexMmap<FixedIndexHeader, [u8; 32]>,
    _file: File,
    pub chunk_size: usize,
    pub size: u64,
    pub uuid: [u8; 16],
    pub ctime: i64,
    pub index_csum: [u8; 32],
}

impl FixedIndexReader {
    pub fn open(path: &Path) -> Result<Self, Error> {
        File::open(path)
            .map_err(Error::from)
            .and_then(Self::new)
            .map_err(|err| format_err!("Unable to open fixed index {:?} - {}", path, err))
    }

    pub fn new(file: File) -> Result<Self, Error> {
        let mmap = unsafe { IndexMmap::map_read(&file)? };

        let header: &FixedIndexHeader = mmap.header();

        if header.magic != file_formats::FIXED_SIZED_CHUNK_INDEX_1_0 {
            bail!("got unknown magic number");
        }

        let size = u64::from_le(header.size);
        let ctime = i64::from_le(header.ctime);
        let chunk_size = u64::from_le(header.chunk_size);

        if chunk_size != FIXED_CHUNK_SIZE as u64 {
            bail!("got unsupported fixed chunk size: {chunk_size}");
        }

        if !chunk_size.is_power_of_two() {
            bail!("got non-power-of-two chunk size: {chunk_size}");
        }

        let Ok(index_length) = usize::try_from(size.div_ceil(chunk_size)) else {
            bail!("index length does not fit in usize: ceil({size} / {chunk_size})");
        };

        let expected_index_size = size_of_val(mmap.index());
        if index_length.checked_mul(32) != Some(expected_index_size) {
            bail!(
                "unexpected index size: {expected_index_size} != 32 * ceil({size} / {chunk_size})"
            );
        }

        if mmap.index().is_empty() {
            bail!("index must not be empty");
        }

        let chunk_size = usize::try_from(chunk_size)?;

        Ok(Self {
            _file: file,
            chunk_size,
            size,
            ctime,
            uuid: header.uuid,
            index_csum: header.index_csum,
            mmap,
        })
    }
}

impl IndexFile for FixedIndexReader {
    fn index_count(&self) -> usize {
        self.mmap.index().len()
    }

    fn index_digest(&self, pos: usize) -> Option<&[u8; 32]> {
        self.mmap.index().get(pos)
    }

    fn index_bytes(&self) -> u64 {
        self.size
    }

    fn chunk_info(&self, pos: usize) -> Option<ChunkReadInfo> {
        let digest = self.index_digest(pos)?;

        let start = (pos * self.chunk_size) as u64;
        let end = (start + self.chunk_size as u64).min(self.size);

        Some(ChunkReadInfo {
            range: start..end,
            digest: *digest,
        })
    }

    fn index_ctime(&self) -> i64 {
        self.ctime
    }

    fn index_size(&self) -> usize {
        self.mmap.size()
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

struct MmapPtr(NonNull<std::ffi::c_void>);

impl MmapPtr {
    fn header(&self) -> NonNull<FixedIndexHeader> {
        self.0.cast::<FixedIndexHeader>()
    }

    fn index(&self) -> NonNull<u8> {
        unsafe { self.0.byte_add(size_of::<FixedIndexHeader>()).cast::<u8>() }
    }
}

pub struct FixedIndexWriter {
    file: File,
    filename: PathBuf,
    tmp_filename: PathBuf,
    /// Most places use u32 because values are just a few MiB, but here
    /// u64 is sightly more convenient for calculations involving size.
    chunk_size: u64,
    size: u64,
    index_length: usize,
    index_capacity: usize,
    memory: Option<MmapPtr>,
    pub uuid: [u8; 16],
    pub ctime: i64,
    growable_size: bool,
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
    /// The initial capacity, if the total size is unknown.
    ///
    /// This capacity takes up the same amount of space as the header
    /// and can refer to 128 Blocks * 4 MiB/Block = 512 MiB of content.
    ///
    /// On systems with 4 KiB page size this value ensures that the
    /// mapped length is a multiple of the page size, but this is not
    /// strictly necessary.
    const INITIAL_CAPACITY: usize = 4096 / 32;

    // Requires obtaining a shared chunk store lock beforehand
    pub fn create(
        full_path: impl Into<PathBuf>,
        known_size: Option<u64>,
        chunk_size: u32,
    ) -> Result<Self, Error> {
        let full_path = full_path.into();
        let mut tmp_path = full_path.clone();
        tmp_path.set_extension("tmp_fidx");

        let file = std::fs::OpenOptions::new()
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

        let chunk_size = u64::from(chunk_size);
        if !chunk_size.is_power_of_two() {
            bail!("got non-power-of-two chunk size: {chunk_size}");
        }

        if chunk_size != FIXED_CHUNK_SIZE as u64 {
            bail!("got unsupported fixed chunk size: {chunk_size}");
        }

        let ctime = proxmox_time::epoch_i64();
        let size = known_size.unwrap_or(0);

        let uuid = Uuid::generate();

        let (index_length, index_capacity) = match known_size {
            Some(s) => {
                let len = s.div_ceil(chunk_size).try_into()?;
                (len, len)
            }
            None => (0, Self::INITIAL_CAPACITY),
        };

        let file_size = Self::file_size(index_capacity)?;
        nix::unistd::ftruncate(&file, file_size)?;

        let memory = MmapPtr(unsafe {
            nix::sys::mman::mmap(
                None,
                std::num::NonZeroUsize::new(file_size as usize)
                    .ok_or_else(|| format_err!("index file size cannot be zero"))?,
                nix::sys::mman::ProtFlags::PROT_READ | nix::sys::mman::ProtFlags::PROT_WRITE,
                nix::sys::mman::MapFlags::MAP_SHARED,
                &file,
                0,
            )
        }?);

        let header = unsafe { memory.header().as_mut() };
        header.magic = file_formats::FIXED_SIZED_CHUNK_INDEX_1_0;
        header.ctime = i64::to_le(ctime);
        header.chunk_size = u64::to_le(chunk_size);
        header.uuid = *uuid.as_bytes();

        Ok(Self {
            file,
            filename: full_path,
            tmp_filename: tmp_path,
            chunk_size,
            size,
            index_length,
            index_capacity,
            memory: Some(memory),
            ctime,
            uuid: *uuid.as_bytes(),
            growable_size: known_size.is_none(),
        })
    }

    /// Computes the size of a fidx file containing `index_length`
    /// chunk digests.
    ///
    /// Guarantees that the size fits into usize, isize and i64.
    fn file_size(index_length: usize) -> Result<i64, Error> {
        if index_length == 0 {
            bail!("fidx file must have at least one chunk");
        }
        index_length
            .checked_mul(32)
            .and_then(|s| s.checked_add(size_of::<FixedIndexHeader>()))
            .filter(|s| *s <= isize::MAX as usize)
            .and_then(|s| i64::try_from(s).ok())
            .ok_or_else(|| format_err!("fidx file size overflow for {index_length} chunks"))
    }

    /// If this returns an error, the sizes may be out of sync,
    /// which is especially bad if the capacity was reduced.
    fn set_index_capacity(&mut self, new_capacity: usize) -> Result<(), Error> {
        if new_capacity == self.index_capacity {
            return Ok(());
        }
        let old_size = Self::file_size(self.index_capacity)?;
        let new_size = Self::file_size(new_capacity)?;

        let Some(MmapPtr(index_addr)) = self.memory else {
            bail!("Can't resize unmapped FixedIndexWriter");
        };

        nix::unistd::ftruncate(&self.file, new_size)?;

        let new_index = unsafe {
            nix::sys::mman::mremap(
                index_addr,
                old_size as usize,
                new_size as usize,
                nix::sys::mman::MRemapFlags::MREMAP_MAYMOVE,
                None,
            )
        }?;

        self.memory = Some(MmapPtr(new_index));
        self.index_capacity = new_capacity;
        Ok(())
    }

    /// Unmapping ensures future add and close operations fail.
    fn set_index_capacity_or_unmap(&mut self, new_capacity: usize) -> Result<(), Error> {
        self.set_index_capacity(new_capacity).map_err(|e| {
            let unmap_result = self.unmap();
            let message = format!(
                "failed to resize index capacity from {} to {new_capacity} with backing file: {:?}",
                self.index_capacity, self.tmp_filename
            );
            if let Err(unmap_err) = unmap_result {
                e.context(message).context(unmap_err)
            } else {
                e.context(message)
            }
        })
    }

    /// Increase the content size to be at least `requested_size` and
    /// ensure there is enough capacity.
    ///
    /// Only writers that were created without a known size can grow.
    /// The size also becomes fixed as soon as it is no longer divisible
    /// by the block size, to ensure that only the last block can be
    /// smaller.
    pub fn grow_to_size(&mut self, requested_size: u64) -> Result<(), Error> {
        if self.size < requested_size {
            if !self.growable_size {
                bail!("refusing to resize from {} to {requested_size}", self.size);
            }
            let new_len = requested_size.div_ceil(self.chunk_size).try_into()?;
            if new_len as u64 * self.chunk_size != requested_size {
                // not a full chunk, so this must be the last one
                self.growable_size = false;
                self.set_index_capacity_or_unmap(new_len)?;
            } else if new_len > self.index_capacity {
                let new_capacity = new_len
                    .checked_next_power_of_two()
                    .ok_or_else(|| format_err!("capacity overflow"))?;
                self.set_index_capacity_or_unmap(new_capacity)?;
            }
            assert!(new_len <= self.index_capacity);
            self.index_length = new_len;
            self.size = requested_size;
        }
        Ok(())
    }

    /// The current length of the index.
    pub fn index_length(&self) -> usize {
        self.index_length
    }

    /// The current total size of the referenced content.
    pub fn size(&self) -> u64 {
        self.size
    }

    fn unmap(&mut self) -> Result<(), Error> {
        if let Some(ptr) = self.memory.take() {
            let len = Self::file_size(self.index_capacity).context(
                "calculation of index file size for unmapping failed - this should never happen!",
            )?;
            if let Err(err) = unsafe { nix::sys::mman::munmap(ptr.0, len as usize) } {
                bail!("unmap file {:?} failed - {}", self.tmp_filename, err);
            }
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<[u8; 32], Error> {
        let Some(ptr) = &self.memory else {
            bail!("cannot close already closed index file.");
        };

        let index_size = self.index_length * 32;
        let index_csum = openssl::sha::sha256(unsafe {
            std::slice::from_raw_parts(ptr.index().as_ptr(), index_size)
        });

        {
            let header = unsafe { ptr.header().as_mut() };
            header.index_csum = index_csum;
            header.size = self.size.to_le();
        }

        self.unmap()?;

        if self.index_length < self.index_capacity {
            let file_size = Self::file_size(self.index_length)?;
            nix::unistd::ftruncate(&self.file, file_size)?;
            self.index_capacity = self.index_length;
        }

        if let Err(err) = std::fs::rename(&self.tmp_filename, &self.filename) {
            bail!("Atomic rename file {:?} failed - {}", self.filename, err);
        }

        Ok(index_csum)
    }

    fn check_chunk_alignment(&self, offset: u64, chunk_len: u64) -> Result<usize, Error> {
        let Some(pos) = offset.checked_sub(chunk_len) else {
            bail!("got chunk with small offset ({} < {}", offset, chunk_len);
        };

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

        Ok((pos / self.chunk_size) as usize)
    }

    fn add_digest(&mut self, index: usize, digest: &[u8; 32]) -> Result<(), Error> {
        if index >= self.index_length {
            bail!(
                "add digest failed - index out of range ({} >= {})",
                index,
                self.index_length
            );
        }
        self.add_digest_unchecked(index, digest)
    }

    fn add_digest_unchecked(&mut self, index: usize, digest: &[u8; 32]) -> Result<(), Error> {
        let Some(ptr) = &self.memory else {
            bail!("cannot write to closed index file.");
        };

        let index_pos = index * 32;
        unsafe {
            let dst = ptr.index().as_ptr().add(index_pos);
            dst.copy_from_nonoverlapping(digest.as_ptr(), 32);
        }

        Ok(())
    }

    /// Write the digest of a chunk into this index file.
    ///
    /// The `start` and `size` parameters encode the range of
    /// content that is backed up. It is verified that `start` is
    /// aligned and that only the last chunk may be smaller.
    ///
    /// If this writer has been created without a fixed size, the
    /// index capacity and content size are increased automatically
    /// until an incomplete chunk is encountered.
    pub fn add_chunk(&mut self, start: u64, size: u32, digest: &[u8; 32]) -> Result<(), Error> {
        let size = u64::from(size);
        let Some(end) = start.checked_add(size) else {
            bail!("add_chunk: start and size are too large: {start}+{size}");
        };
        self.grow_to_size(end)?;
        let idx = self.check_chunk_alignment(end, size)?;
        self.add_digest(idx, digest)
    }

    /// Copy the chunk hashes from a Reader to the start of this Writer.
    ///
    /// If this writer is resizable the capacity may increase,
    /// but the size and length stay the same.
    pub fn clone_data_from(&mut self, reader: &FixedIndexReader) -> Result<(), Error> {
        if self.chunk_size != reader.chunk_size as u64 {
            bail!("can't reuse file with different chunk size");
        }

        let count = reader.index_count();
        if self.growable_size && self.index_capacity < count {
            self.set_index_capacity_or_unmap(count)?;
        }

        for i in 0..count.min(self.index_capacity) {
            self.add_digest_unchecked(i, reader.index_digest(i).unwrap())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use openssl::sha::sha256;
    use tempfile::TempDir;

    use super::*;

    const CS: u32 = FIXED_CHUNK_SIZE;

    #[track_caller]
    fn check_error_contains<T>(result: Result<T, Error>, substring: &str) {
        let error = result.map(|_| ()).unwrap_err().to_string();
        assert!(
            error.contains(substring),
            "'{error}' does not contain '{substring}'"
        );
    }

    #[test]
    fn test_no_invalid_chunk_size() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("overflow.fidx");

        for chunk_size in &[
            1u64,
            1024u64,
            1 * 1024 * 1024u64,
            8 * 1024 * 1024u64,
            u64::MAX,
        ] {
            let mut buf = vec![0u8; 4096 + 32]; // (2^59+1) / (4 * 1024 * 1024) * 32 = 32 (mod 2^64)
            buf[0..8].copy_from_slice(&crate::file_formats::FIXED_SIZED_CHUNK_INDEX_1_0);
            buf[64..72].copy_from_slice(&576460752303423489u64.to_le_bytes()); // size = 2^59+1
            buf[72..80].copy_from_slice(&chunk_size.to_le_bytes()); // chunk_size
            fs::write(&path, &buf).unwrap();
            check_error_contains(
                FixedIndexReader::open(&path),
                &format!("got unsupported fixed chunk size: {chunk_size}"),
            );
        }
    }

    #[test]
    fn test_index_reader() {
        let dir = TempDir::new().unwrap();

        check_error_contains(
            FixedIndexReader::open(Path::new("/dev/stdin")),
            "not a regular file",
        );
        check_error_contains(
            FixedIndexReader::open(Path::new("/dev/zero")),
            "not a regular file",
        );

        let path = dir.path().join("file");
        check_error_contains(FixedIndexReader::open(&path), "No such file or directory");

        fs::write(&path, []).unwrap();
        check_error_contains(FixedIndexReader::open(&path), "file too small (0)");

        let mut data = vec![0; size_of::<FixedIndexHeader>()];

        fs::write(&path, &data).unwrap();
        check_error_contains(FixedIndexReader::open(&path), "got unknown magic number");

        data[..8].copy_from_slice(&file_formats::FIXED_SIZED_CHUNK_INDEX_1_0);
        fs::write(&path, &data).unwrap();
        check_error_contains(
            FixedIndexReader::open(&path),
            "got unsupported fixed chunk size: 0",
        );

        let chunk_size = 4096 * 1024u64;
        data[72..][..8].copy_from_slice(&chunk_size.to_le_bytes());
        fs::write(&path, &data).unwrap();
        check_error_contains(FixedIndexReader::open(&path), "index must not be empty");

        let size = chunk_size + 1;
        data[64..][..8].copy_from_slice(&size.to_le_bytes());
        data.extend_from_slice(&[0, 1, 2]);
        fs::write(&path, &data).unwrap();
        check_error_contains(
            FixedIndexReader::open(&path),
            "index size is not divisible by element size: 3",
        );

        data.extend(3u8..32);
        fs::write(&path, &data).unwrap();
        check_error_contains(
            FixedIndexReader::open(&path),
            "unexpected index size: 32 != 32 * ceil(4194305 / 4194304)",
        );

        data.extend(32u8..64);
        fs::write(&path, &data).unwrap();
        let reader = FixedIndexReader::open(&path).unwrap();
        assert_eq!(reader.index_bytes(), size);
        assert_eq!(reader.index_size(), 4096 + 64);
        assert_eq!(reader.chunk_size, chunk_size as usize);
        assert_eq!(reader.index_count(), 2);
        assert_eq!(
            reader.index_digest(0),
            Some(&std::array::from_fn(|i| i as u8))
        );
        assert_eq!(
            reader.index_digest(1),
            Some(&std::array::from_fn(|i| i as u8 + 32))
        );
        assert_eq!(
            reader.compute_csum(),
            (sha256(&data[size_of::<FixedIndexHeader>()..]), size)
        );

        dir.close().unwrap();
    }

    #[test]
    fn test_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_empty");
        let mut w = FixedIndexWriter::create(&path, None, CS).unwrap();

        assert!(w.add_digest(0, &[1u8; 32]).is_err(), "out of bounds");

        assert_eq!(0, w.size);
        assert_eq!(0, w.index_length(), "returns length, not capacity");
        assert_eq!(FixedIndexWriter::INITIAL_CAPACITY, w.index_capacity);

        assert!(w.close().is_err(), "should refuse to create empty file");

        drop(w);
        assert!(!fs::exists(path).unwrap());

        dir.close().unwrap();
    }

    #[test]
    fn test_single_partial_chunk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_single_partial_chunk");
        let mut w = FixedIndexWriter::create(&path, None, CS).unwrap();

        let size = CS as u64 - 1;
        let expected = test_data(size);
        w.grow_to_size(size).unwrap();
        expected[0].add_to(&mut w);

        w.close().unwrap();
        drop(w);

        check_with_reader(&path, size, &expected);
        compare_to_known_size_writer(&path, size, &expected);

        dir.close().unwrap();
    }

    #[test]
    fn test_grow_to_multiples_of_chunk_size() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_grow_to_multiples_of_chunk_size");
        let mut w = FixedIndexWriter::create(&path, None, CS).unwrap();

        let initial = FixedIndexWriter::INITIAL_CAPACITY;
        let steps = [1, 2, initial, initial + 1, 5 * initial, 10 * initial + 1];
        let expected = test_data(*steps.last().unwrap() as u64 * CS as u64);

        let mut begin = 0;
        for chunk_count in steps {
            let last = &expected[chunk_count - 1];
            w.grow_to_size(last.end).unwrap();
            assert_eq!(last.index + 1, w.index_length());
            assert!(w.add_digest(last.index + 1, &[1u8; 32]).is_err());

            for c in expected[begin..chunk_count].iter().rev() {
                c.add_to(&mut w);
            }
            begin = chunk_count;
        }
        w.close().unwrap();
        drop(w);

        let size = expected.len() as u64 * CS as u64;
        check_with_reader(&path, size, &expected);
        compare_to_known_size_writer(&path, size, &expected);

        dir.close().unwrap();
    }

    #[test]
    fn test_grow_to_misaligned_size() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_grow_to_misaligned_size");
        let mut w = FixedIndexWriter::create(&path, None, CS).unwrap();

        let size = (FixedIndexWriter::INITIAL_CAPACITY as u64 + 42) * CS as u64 - 1; // last is not full
        let expected = test_data(size);

        w.grow_to_size(size).unwrap();
        assert!(w.grow_to_size(size + 1).is_err(), "size must be fixed now");
        assert_eq!(expected.len(), w.index_length());
        assert!(w.add_digest(expected.len(), &[1u8; 32]).is_err());

        for c in expected.iter().rev() {
            c.add_to(&mut w);
        }

        w.close().unwrap();
        drop(w);

        check_with_reader(&path, size, &expected);
        compare_to_known_size_writer(&path, size, &expected);

        dir.close().unwrap();
    }

    #[test]
    fn test_clone_data_from() {
        let dir = TempDir::new().unwrap();
        let size = (FixedIndexWriter::INITIAL_CAPACITY as u64 + 3) * CS as u64;
        let mut expected = test_data(size);

        let reused = dir.path().join("reused");
        let mut w = FixedIndexWriter::create(&reused, Some(size), CS).unwrap();
        for c in expected.iter() {
            c.add_to(&mut w);
        }
        w.close().unwrap();
        drop(w);

        let reused = FixedIndexReader::open(&reused).unwrap();

        let truncated = dir.path().join("truncated");
        let size = size - CS as u64;
        expected.pop();
        let mut w = FixedIndexWriter::create(&truncated, Some(size), CS).unwrap();
        w.clone_data_from(&reused).unwrap();
        w.close().unwrap();
        drop(w);
        check_with_reader(&truncated, size, &expected);
        compare_to_known_size_writer(&truncated, size, &expected);

        let modified = dir.path().join("modified");
        let mut w = FixedIndexWriter::create(&modified, None, CS).unwrap();
        w.clone_data_from(&reused).unwrap();
        {
            let i = expected.len() / 2;
            expected[i].digest[1] += 1;
            let chunk = &expected[i];
            let chunk_pos = chunk.end - chunk.size as u64;
            w.add_chunk(chunk_pos, chunk.size, &chunk.digest).unwrap();
        }
        w.grow_to_size(size).unwrap();
        w.close().unwrap();
        drop(w);
        check_with_reader(&modified, size, &expected);
        compare_to_known_size_writer(&modified, size, &expected);

        dir.close().unwrap();
    }

    struct TestChunk {
        digest: [u8; 32],
        index: usize,
        size: u32,
        end: u64,
    }

    impl TestChunk {
        fn add_to(&self, w: &mut FixedIndexWriter) {
            assert_eq!(
                self.index,
                w.check_chunk_alignment(self.end, self.size as u64).unwrap()
            );
            w.add_digest(self.index, &self.digest).unwrap();
        }
    }

    fn test_data(size: u64) -> Vec<TestChunk> {
        (0..size.div_ceil(CS as u64))
            .map(|index| {
                let mut digest = [0u8; 32];
                let i = &index.to_le_bytes();
                for c in digest.chunks_mut(i.len()) {
                    c.copy_from_slice(i);
                }
                let size = if ((index + 1) * CS as u64) <= size {
                    CS
                } else {
                    (size % CS as u64) as u32
                };
                TestChunk {
                    digest,
                    index: index as usize,
                    size,
                    end: index * CS as u64 + size as u64,
                }
            })
            .collect()
    }

    fn check_with_reader(path: &Path, size: u64, chunks: &[TestChunk]) {
        let reader = FixedIndexReader::open(path).unwrap();
        assert_eq!(size, reader.index_bytes());
        assert_eq!(chunks.len(), reader.index_count());
        for c in chunks {
            assert_eq!(&c.digest, reader.index_digest(c.index).unwrap());
        }
    }

    fn compare_to_known_size_writer(file: &Path, size: u64, chunks: &[TestChunk]) {
        let mut path = file.to_path_buf();
        path.set_extension("reference");
        let mut w = FixedIndexWriter::create(&path, Some(size), CS).unwrap();
        for c in chunks {
            c.add_to(&mut w);
        }
        w.close().unwrap();
        drop(w);

        let mut reference = fs::read(file).unwrap();
        let mut tested = fs::read(path).unwrap();

        // ignore uuid and ctime
        reference[8..32].fill(0);
        tested[8..32].fill(0);

        assert_eq!(reference, tested);
    }
}
