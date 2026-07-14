//! Crash-safe publication of files owned by one managed data directory.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use tempfile::NamedTempFile;
use zippy_types::table::ManagedDataPath;

use crate::data_directory::{
    create_owner_directory, io_error, require_owner_permissions, sync_directory_io,
    DataDirectoryError,
};
use crate::data_directory_ownership::OwnedDataDirectory;

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

/// Result of durably removing one managed regular file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableRemoveOutcome {
    /// The file was unlinked and its parent directory was synchronized.
    Removed,
    /// No directory entry existed at the requested managed path.
    NotFound,
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

impl OwnedDataDirectory {
    /// Creates an unpublished file under this managed data root.
    ///
    /// Missing parent directories are created with owner-only permissions and
    /// durably linked one level at a time. Existing symbolic-link components
    /// are rejected before the temporary file is created.
    pub fn begin_durable_file(
        &self,
        managed_path: &ManagedDataPath,
    ) -> Result<DurableFileTransaction, DataDirectoryError> {
        begin_durable_file(self.directory().root(), managed_path)
    }

    /// Removes one managed regular file and synchronizes its parent directory.
    ///
    /// Missing files are idempotent. Symbolic links, non-regular targets,
    /// insecure permissions, foreign ownership, and extra hard links are
    /// rejected before unlinking.
    pub fn remove_durable_file(
        &self,
        managed_path: &ManagedDataPath,
    ) -> Result<DurableRemoveOutcome, DataDirectoryError> {
        remove_durable_file(self.directory().root(), managed_path)
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

fn remove_durable_file(
    root: &Path,
    managed_path: &ManagedDataPath,
) -> Result<DurableRemoveOutcome, DataDirectoryError> {
    let relative = Path::new(managed_path.as_str());
    let relative_parent = relative.parent().unwrap_or_else(|| Path::new(""));
    let Some(parent) = find_existing_parent(root, relative_parent)? else {
        return Ok(DurableRemoveOutcome::NotFound);
    };
    let file_name = relative
        .file_name()
        .ok_or_else(|| DataDirectoryError::InvalidManagedPath {
            path: relative.to_path_buf(),
        })?;
    remove_file_from_parent(&parent, file_name)
}

fn find_existing_parent(
    root: &Path,
    relative_parent: &Path,
) -> Result<Option<PathBuf>, DataDirectoryError> {
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
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error(&next, error)),
        }
        current = next;
    }
    Ok(Some(current))
}

#[cfg(unix)]
fn remove_file_from_parent(
    parent: &Path,
    file_name: &std::ffi::OsStr,
) -> Result<DurableRemoveOutcome, DataDirectoryError> {
    use std::fs::File;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let path = parent.join(file_name);
    let parent_descriptor = rustix::fs::open(
        parent,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| io_error(parent, std::io::Error::from(error)))?;
    let parent_directory = File::from(parent_descriptor);
    let target_descriptor = match rustix::fs::openat(
        &parent_directory,
        file_name,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    ) {
        Ok(descriptor) => descriptor,
        Err(rustix::io::Errno::NOENT) => return Ok(DurableRemoveOutcome::NotFound),
        Err(rustix::io::Errno::LOOP) => return Err(DataDirectoryError::SymbolicLink { path }),
        Err(error) => return Err(io_error(&path, std::io::Error::from(error))),
    };
    let target = File::from(target_descriptor);
    let descriptor_metadata = target.metadata().map_err(|error| io_error(&path, error))?;
    if !descriptor_metadata.is_file() {
        return Err(DataDirectoryError::NotRegularFile { path });
    }
    let current_uid = rustix::process::geteuid().as_raw();
    if descriptor_metadata.uid() != current_uid {
        return Err(DataDirectoryError::WrongOwner {
            path,
            owner_uid: descriptor_metadata.uid(),
            current_uid,
        });
    }
    if descriptor_metadata.nlink() != 1 {
        return Err(DataDirectoryError::UnexpectedLinkCount {
            path,
            links: descriptor_metadata.nlink(),
        });
    }
    let mode = descriptor_metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(DataDirectoryError::InsecurePermissions { path, mode });
    }
    let path_metadata = fs::symlink_metadata(&path).map_err(|error| io_error(&path, error))?;
    if path_metadata.file_type().is_symlink() {
        return Err(DataDirectoryError::SymbolicLink { path });
    }
    if descriptor_metadata.dev() != path_metadata.dev()
        || descriptor_metadata.ino() != path_metadata.ino()
    {
        return Err(DataDirectoryError::PathIdentityChanged { path });
    }
    match rustix::fs::unlinkat(&parent_directory, file_name, rustix::fs::AtFlags::empty()) {
        Ok(()) => {}
        Err(rustix::io::Errno::NOENT) => {
            return Err(DataDirectoryError::PathIdentityChanged { path });
        }
        Err(error) => return Err(io_error(&path, std::io::Error::from(error))),
    }
    parent_directory
        .sync_all()
        .map_err(|error| io_error(parent, error))?;
    Ok(DurableRemoveOutcome::Removed)
}

#[cfg(not(unix))]
fn remove_file_from_parent(
    _parent: &Path,
    _file_name: &std::ffi::OsStr,
) -> Result<DurableRemoveOutcome, DataDirectoryError> {
    Err(DataDirectoryError::UnsupportedPlatformSecurity)
}
