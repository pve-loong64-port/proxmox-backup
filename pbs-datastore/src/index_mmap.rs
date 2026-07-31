use std::fs::File;
use std::marker::PhantomData;
use std::os::unix::io::AsRawFd;
use std::ptr::NonNull;

use anyhow::{Error, bail, format_err};
use nix::sys::mman::{MapFlags, ProtFlags};
use nix::sys::stat::{SFlag, fstat};

use proxmox_sys::mmap::Mmap;

pub struct IndexMmap<H, T> {
    _header: PhantomData<H>,
    _index: PhantomData<[T]>,
    /// used for the Drop, Send, and Sync impls
    memory: Mmap<u8>,
    index_len: usize,
}

impl<H, T> IndexMmap<H, T> {
    /// Map a read-only file consisting of a header followed by an array into memory.
    ///
    /// SAFETY:
    /// * `H` and `T` must be:
    ///     * plain data types
    ///     * of non-zero size
    ///     * without interior mutability
    ///     * where any bit pattern is valid.
    /// * `file` *must not* be modified by anyone else!
    ///     * modifying the bytes while a reference exists is UB
    ///     * truncation may cause SIGBUS on access
    pub unsafe fn map_read(file: &File) -> Result<Self, Error> {
        let stat = fstat(file.as_raw_fd()).map_err(|e| format_err!("fstat failed - {e}"))?;
        if (stat.st_mode & SFlag::S_IFMT.bits()) != SFlag::S_IFREG.bits() {
            bail!("not a regular file");
        }

        let size = usize::try_from(stat.st_size)
            .map_err(|_| format_err!("file too small ({})", stat.st_size))?;

        let index_size = size
            .checked_sub(size_of::<H>())
            .ok_or_else(|| format_err!("file too small ({size})"))?;

        let index_len = index_size
            .checked_div(size_of::<T>())
            .filter(|l| l * size_of::<T>() == index_size)
            .ok_or_else(|| {
                format_err!("index size is not divisible by element size: {index_size}")
            })?;

        let (prot, flags) = (ProtFlags::PROT_READ, MapFlags::MAP_PRIVATE);

        let memory = unsafe { Mmap::map_fd(file, 0, size, prot, flags) }
            .map_err(|e| format_err!("mmap failed - {e}"))?;

        let mmap = Self {
            _header: PhantomData,
            _index: PhantomData,
            memory,
            index_len,
        };
        if !mmap.header_ptr().is_aligned() || !mmap.index_ptr().is_aligned() {
            bail!("alignment error");
        }
        Ok(mmap)
    }

    fn header_ptr(&self) -> NonNull<H> {
        self.memory.as_non_null().cast::<H>()
    }

    fn index_ptr(&self) -> NonNull<T> {
        let ptr = self.memory.as_non_null();
        unsafe { ptr.byte_add(size_of::<H>()).cast::<T>() }
    }

    pub fn header(&self) -> &H {
        unsafe { self.header_ptr().as_ref() }
    }

    pub fn index(&self) -> &[T] {
        unsafe { NonNull::slice_from_raw_parts(self.index_ptr(), self.index_len).as_ref() }
    }

    pub fn size(&self) -> usize {
        self.memory.len()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::path::Path;

    use tempfile::TempDir;

    use super::*;

    #[track_caller]
    fn check_error_contains<T>(result: Result<T, Error>, substring: &str) {
        let error = result.map(|_| ()).unwrap_err().to_string();
        assert!(
            error.contains(substring),
            "'{error}' does not contain '{substring}'"
        );
    }

    fn map<H, T>(path: &Path) -> Result<IndexMmap<H, T>, Error> {
        let file = File::open(path).unwrap();
        unsafe { IndexMmap::<H, T>::map_read(&file) }
    }

    #[repr(align(8))]
    struct A8(u64);

    type A1 = u8;

    #[test]
    fn test_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("file");

        fs::write(&path, &[]).unwrap();
        check_error_contains(map::<A8, A8>(&path), "file too small (0)");
        check_error_contains(map::<A1, A1>(&path), "file too small (0)");
    }

    #[test]
    fn test_small() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("file");

        fs::write(&path, [1, 2, 3, 4, 5, 6, 7]).unwrap();
        check_error_contains(map::<A8, A1>(&path), "file too small (7)");

        let mmap = map::<A1, A1>(&path).unwrap();
        assert_eq!(mmap.size(), 7);
        assert_eq!(mmap.header(), &1);
        assert_eq!(mmap.index(), &[2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_divisibility_and_alignment() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("file");

        fs::write(&path, [1; 33]).unwrap();
        check_error_contains(
            map::<A8, A8>(&path),
            "index size is not divisible by element size: 25",
        );

        // could be prevented statically, but it makes a good test for the sanity check
        check_error_contains(map::<A1, A8>(&path), "alignment error");
    }

    #[test]
    fn test_medium() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("file");

        fs::write(&path, [1; 32]).unwrap();

        let mmap = map::<A8, A8>(&path).unwrap();
        assert_eq!(mmap.size(), 32);
        assert_eq!(mmap.header().0, 0x0101010101010101);
        assert_eq!(mmap.index().len(), 3);
    }
}
