//! Crash-safe publication of files owned by one managed data directory.

use std::fmt;
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
    pub fn publish_noclobber(self) -> Result<DurablePublishedFile, DurableFilePublishError> {
        self.publish_noclobber_with(sync_directory_io)
    }

    fn publish_noclobber_with<F>(
        self,
        sync_parent: F,
    ) -> Result<DurablePublishedFile, DurableFilePublishError>
    where
        F: FnOnce(&Path) -> std::io::Result<()>,
    {
        self.temporary.as_file().sync_all().map_err(|error| {
            DurableFilePublishError::unpublished(io_error(self.temporary.path(), error))
        })?;
        let size_bytes = self
            .temporary
            .as_file()
            .metadata()
            .map_err(|error| {
                DurableFilePublishError::unpublished(io_error(self.temporary.path(), error))
            })?
            .len();
        let managed_path = self.managed_path;
        let final_path = self.final_path;
        let parent = self.parent;
        self.temporary
            .persist_noclobber(&final_path)
            .map_err(|error| {
                DurableFilePublishError::unpublished(
                    if error.error.kind() == std::io::ErrorKind::AlreadyExists {
                        DataDirectoryError::AlreadyExists {
                            path: final_path.clone(),
                        }
                    } else {
                        io_error(&final_path, error.error)
                    },
                )
            })?;
        sync_parent(&parent).map_err(|error| {
            DurableFilePublishError::published_not_durable(io_error(&parent, error))
        })?;
        Ok(DurablePublishedFile {
            managed_path,
            path: final_path,
            size_bytes,
        })
    }
}

/// Whether a failed durable-file publication created its final directory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableFilePublicationState {
    /// Publication failed before the final path was created by this transaction.
    Unpublished,
    /// The final path was created, but parent-directory durability was not confirmed.
    PublishedNotDurable,
}

/// A durable-file publication failure with explicit final-entry ownership state.
#[derive(Debug)]
pub struct DurableFilePublishError {
    error: DataDirectoryError,
    state: DurableFilePublicationState,
}

impl DurableFilePublishError {
    fn unpublished(error: DataDirectoryError) -> Self {
        Self {
            error,
            state: DurableFilePublicationState::Unpublished,
        }
    }

    fn published_not_durable(error: DataDirectoryError) -> Self {
        Self {
            error,
            state: DurableFilePublicationState::PublishedNotDurable,
        }
    }

    /// Returns whether this transaction created the final directory entry.
    #[must_use]
    pub const fn publication_state(&self) -> DurableFilePublicationState {
        self.state
    }

    /// Returns the underlying managed-data-directory failure.
    #[must_use]
    pub const fn error(&self) -> &DataDirectoryError {
        &self.error
    }

    /// Consumes the classified failure into its underlying error and publication state.
    #[must_use]
    pub fn into_parts(self) -> (DataDirectoryError, DurableFilePublicationState) {
        (self.error, self.state)
    }
}

impl fmt::Display for DurableFilePublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "durable file publication failed state=[{:?}] error=[{}]",
            self.state, self.error
        )
    }
}

impl std::error::Error for DurableFilePublishError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::TempDir;

    use super::{begin_durable_file, DurableFilePublicationState};
    use zippy_types::table::ManagedDataPath;

    #[test]
    fn directory_sync_failure_retains_published_entry_state() {
        let directory = TempDir::new().expect("create temporary data root");
        let managed_path =
            ManagedDataPath::new("parquet/table/file.parquet").expect("construct managed path");
        let mut transaction =
            begin_durable_file(directory.path(), &managed_path).expect("begin durable publication");
        transaction
            .write_all(b"published but not confirmed durable")
            .expect("write temporary payload");
        let temporary_path = transaction.temporary_path().to_path_buf();
        let final_path = transaction.final_path().to_path_buf();

        let failure = transaction
            .publish_noclobber_with(|_| Err(std::io::Error::other("injected sync failure")))
            .expect_err("directory sync failure must be classified");

        assert_eq!(
            failure.publication_state(),
            DurableFilePublicationState::PublishedNotDurable
        );
        assert!(!temporary_path.exists());
        assert!(final_path.exists());
    }
}
