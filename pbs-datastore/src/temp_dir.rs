use core::convert::AsRef;
use std::convert::From;
use std::env;
use std::mem;
use std::path::{Path, PathBuf};

use anyhow::{Context, Error};

/// A temporary directory that is automatically removed on drop.
/// Deletion is best-effort and errors are silently ignored, unless
/// the explicit `delete` method is used.
#[derive(Debug)]
pub struct TempDir {
    directory: PathBuf,
    delete_on_drop: bool,
}

impl TempDir {
    /// Create an empty directory in `env::temp_dir()`.
    pub fn new() -> Result<Self, Error> {
        TempDir::new_in(env::temp_dir())
    }

    /// Create an empty child directory in the specified one.
    pub fn new_in(parent: impl AsRef<Path>) -> Result<Self, Error> {
        let parent = parent.as_ref();
        let directory = proxmox_sys::fs::make_tmp_dir(parent, None)
            .with_context(|| format!("Failed to create temporary directory in {parent:?}"))?;
        Ok(Self {
            directory,
            delete_on_drop: true,
        })
    }

    /// The path to this directory.
    pub fn path(&self) -> &Path {
        &self.directory
    }

    /// Disable automatic deletion.
    /// May be useful when debugging tests.
    pub fn disable_deletion(&mut self) {
        self.delete_on_drop = false;
    }

    /// Disable atomatic deletion and turn into a normal `PathBuf`.
    pub fn to_persistent(mut self) -> PathBuf {
        self.disable_deletion();
        mem::take(&mut self.directory)
    }

    /// Delete the directory with the possibility of handling the error.
    pub fn delete(mut self) -> Result<(), Error> {
        self.disable_deletion();
        std::fs::remove_dir_all(&self.directory)
            .with_context(|| format!("Failed to delete temporary directory {:?}", self.directory))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if self.delete_on_drop {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_empty() {
        let temp_dir = TempDir::new().unwrap();

        let copy = PathBuf::from(temp_dir.path());
        assert!(fs::exists(&copy).unwrap());
        assert!(copy.is_dir());

        drop(temp_dir);

        assert!(!fs::exists(copy).unwrap());
    }

    #[test]
    fn test_delete_with_content() {
        let temp_dir = TempDir::new().unwrap();

        let copy = PathBuf::from(temp_dir.path());
        assert!(fs::exists(&copy).unwrap());
        assert!(copy.is_dir());

        fs::write(temp_dir.path().join("test.txt"), "hello").unwrap();

        let subdir = temp_dir.path().join("foo").join("bar");
        fs::create_dir_all(&subdir).unwrap();
        fs::write(subdir.join("baz.txt"), "world").unwrap();

        temp_dir.delete().unwrap();

        assert!(!fs::exists(copy).unwrap());
    }

    #[test]
    fn test_disable_deletion() {
        let mut temp_dir = TempDir::new().unwrap();

        let copy = PathBuf::from(temp_dir.path());
        temp_dir.disable_deletion();
        drop(temp_dir);

        assert!(
            fs::exists(&copy).unwrap(),
            "Drop must not delte the directory"
        );

        fs::remove_dir_all(copy).unwrap(); // cleanup
    }

    #[test]
    fn test_to_persistent() {
        let temp_dir = TempDir::new().unwrap();

        let copy = PathBuf::from(temp_dir.path());

        let persistent = temp_dir.to_persistent();
        assert_eq!(copy, persistent, "Path must be correct");
        assert!(
            fs::exists(&persistent).unwrap(),
            "Directory must still exist."
        );

        fs::remove_dir_all(persistent).unwrap(); // cleanup
    }
}
