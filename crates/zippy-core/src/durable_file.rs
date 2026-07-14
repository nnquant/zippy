//! Crash-safe publication of files owned by one managed data directory.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use tempfile::NamedTempFile;
use zippy_types::table::ManagedDataPath;

use crate::data_directory::{
    create_owner_directory, io_error, require_owner_permissions, sync_directory_io, DataDirectory,
    DataDirectoryError,
};

/// An unpublished file whose temporary inode lives beside its final target.
///
/// Dropping the transaction removes the temporary file. Publication consumes
/// the transaction and never overwrites an existing target.
#[derive(Debug)]
pub struct DurableFileTransaction {
    temporary: NamedTempFile,
    managed_path: ManagedDataPath,
    final_path: PathBuf,
    parent: PathBuf,
}

impl DurableFileTransaction {
    /// Returns the writable temporary file.
    pub fn file_mut(&mut self) -> &mut fs::File {
        self.temporary.as_file_mut()
    }

    /// Returns the temporary path used before publication.
    pub fn temporary_path(&self) -> &Path {
        self.temporary.path()
    }

    /// Returns the final absolute path without publishing the file.
    pub fn final_path(&self) -> &Path {
        &self.final_path
    }

    /// Synchronizes file contents, publishes without clobbering, and syncs the parent directory.
    pub fn publish_noclobber(self) -> Result<DurablePublishedFile, DataDirectoryError> {
        self.temporary
            .as_file()
            .sync_all()
            .map_err(|error| io_error(self.temporary.path(), error))?;
        let managed_path = self.managed_path;
        let final_path = self.final_path;
        let parent = self.parent;
        let published = self
            .temporary
            .persist_noclobber(&final_path)
            .map_err(|error| {
                if error.error.kind() == std::io::ErrorKind::AlreadyExists {
                    DataDirectoryError::AlreadyExists {
                        path: final_path.clone(),
                    }
                } else {
                    io_error(&final_path, error.error)
                }
            })?;
        let size_bytes = published
            .metadata()
            .map_err(|error| io_error(&final_path, error))?
            .len();
        sync_directory_io(&parent).map_err(|error| io_error(&parent, error))?;
        Ok(DurablePublishedFile {
            managed_path,
            path: final_path,
            size_bytes,
        })
    }
}

impl Write for DurableFileTransaction {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.temporary.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.temporary.flush()
    }
}

/// A file whose contents and directory entry passed the durable publication boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurablePublishedFile {
    managed_path: ManagedDataPath,
    path: PathBuf,
    size_bytes: u64,
}

impl DurablePublishedFile {
    /// Returns the validated path relative to the data root.
    pub fn managed_path(&self) -> &ManagedDataPath {
        &self.managed_path
    }

    /// Returns the final absolute path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the file size observed after publication.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

impl DataDirectory {
    /// Creates an unpublished file under this managed data root.
    ///
    /// Missing parent directories are created with owner-only permissions and
    /// durably linked one level at a time. Existing symbolic-link components
    /// are rejected before the temporary file is created.
    pub fn begin_durable_file(
        &self,
        managed_path: &ManagedDataPath,
    ) -> Result<DurableFileTransaction, DataDirectoryError> {
        begin_durable_file(self.root(), managed_path)
    }
}

pub(crate) fn begin_durable_file(
    root: &Path,
    managed_path: &ManagedDataPath,
) -> Result<DurableFileTransaction, DataDirectoryError> {
    let relative = Path::new(managed_path.as_str());
    let relative_parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let parent = ensure_parent_directories(root, relative_parent)?;
    let final_path = root.join(relative);
    let file_name = relative
        .file_name()
        .ok_or_else(|| DataDirectoryError::InvalidManagedPath {
            path: relative.to_path_buf(),
        })?;
    let mut prefix = std::ffi::OsString::from(".");
    prefix.push(file_name);
    prefix.push(".tmp-");
    let temporary = tempfile::Builder::new()
        .prefix(&prefix)
        .tempfile_in(&parent)
        .map_err(|error| io_error(&final_path, error))?;
    Ok(DurableFileTransaction {
        temporary,
        managed_path: managed_path.clone(),
        final_path,
        parent,
    })
}

fn ensure_parent_directories(
    root: &Path,
    relative_parent: &Path,
) -> Result<PathBuf, DataDirectoryError> {
    let mut current = root.to_path_buf();
    for component in relative_parent.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(DataDirectoryError::InvalidManagedPath {
                path: relative_parent.to_path_buf(),
            });
        };
        let next = current.join(component);
        match fs::symlink_metadata(&next) {
            Ok(metadata) => validate_parent_component(&next, &metadata)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match create_owner_directory(&next) {
                    Ok(()) => {
                        sync_directory_io(&current).map_err(|error| io_error(&current, error))?
                    }
                    Err(create_error) => match fs::symlink_metadata(&next) {
                        Ok(metadata) => validate_parent_component(&next, &metadata)?,
                        Err(_) => return Err(create_error),
                    },
                }
            }
            Err(error) => return Err(io_error(&next, error)),
        }
        current = next;
    }
    Ok(current)
}

fn validate_parent_component(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<(), DataDirectoryError> {
    if metadata.file_type().is_symlink() {
        return Err(DataDirectoryError::SymbolicLink {
            path: path.to_path_buf(),
        });
    }
    if !metadata.is_dir() {
        return Err(DataDirectoryError::NotDirectory {
            path: path.to_path_buf(),
        });
    }
    require_owner_permissions(path)
}
