//! Process-scoped exclusive ownership of one managed data directory.

use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::data_directory::{
    initialize_prepared_data_root, io_error, prepare_data_root, DataDirectory, DataDirectoryError,
    DATA_FORMAT_VERSION,
};

/// Stable file name used for the data-directory process lock.
pub const DATA_DIRECTORY_LOCK_FILE: &str = "server.lock";

const OWNERSHIP_RECORD_VERSION: u32 = 1;

/// Diagnostic identity written into `server.lock` by the current owner.
///
/// The advisory kernel lock, rather than this record, is the ownership
/// authority. The record remains after crashes so operators can diagnose the
/// last process that held the directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataDirectoryOwner {
    record_version: u32,
    data_format_version: u32,
    pid: u32,
    acquired_unix_ms: u64,
    zippy_version: String,
}

impl DataDirectoryOwner {
    /// Returns the operating-system process identifier of the owner.
    pub const fn pid(&self) -> u32 {
        self.pid
    }

    /// Returns the wall-clock acquisition timestamp in Unix milliseconds.
    pub const fn acquired_unix_ms(&self) -> u64 {
        self.acquired_unix_ms
    }

    /// Returns the Zippy version that acquired the lock.
    pub fn zippy_version(&self) -> &str {
        &self.zippy_version
    }
}

/// Exclusive process ownership of a validated data directory.
///
/// Dropping this value closes the lock descriptor and releases the kernel
/// lock. The lock file itself is deliberately retained to prevent an
/// unlink-and-recreate race during ordinary lifecycle cleanup.
#[derive(Debug)]
pub struct OwnedDataDirectory {
    directory: DataDirectory,
    owner: DataDirectoryOwner,
    lock_path: PathBuf,
    _lock_file: File,
    _root_directory: File,
}

impl OwnedDataDirectory {
    /// Acquires ownership before initializing or repairing a managed layout.
    ///
    /// This is the server startup entry point. The root and lock file are made
    /// safe first; `FORMAT_VERSION` and managed subdirectories are touched only
    /// after the non-blocking exclusive kernel lock has been acquired.
    pub fn acquire_or_initialize(path: impl AsRef<Path>) -> Result<Self, DataDirectoryError> {
        let root = prepare_data_root(path.as_ref())?;
        let (root_directory, lock_file, owner, lock_path) = acquire_root_lock(&root)?;
        let directory = initialize_prepared_data_root(root)?;
        Ok(Self {
            directory,
            owner,
            lock_path,
            _lock_file: lock_file,
            _root_directory: root_directory,
        })
    }

    /// Returns the validated data-directory descriptor.
    pub fn directory(&self) -> &DataDirectory {
        &self.directory
    }

    /// Returns the current process ownership identity.
    pub fn owner(&self) -> &DataDirectoryOwner {
        &self.owner
    }

    /// Returns the stable lock-file path.
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }
}

impl DataDirectory {
    /// Acquires non-blocking exclusive process ownership of this data root.
    ///
    /// Ordinary `DataDirectory::open` calls remain available while ownership
    /// is held. Any component that publishes authoritative files must instead
    /// retain the returned `OwnedDataDirectory` for its complete lifecycle.
    pub fn acquire_process_ownership(self) -> Result<OwnedDataDirectory, DataDirectoryError> {
        acquire_process_ownership(self)
    }
}

fn acquire_process_ownership(
    directory: DataDirectory,
) -> Result<OwnedDataDirectory, DataDirectoryError> {
    let root = directory.root().to_path_buf();
    let (root_directory, lock_file, owner, lock_path) = acquire_root_lock(&root)?;
    let directory = DataDirectory::open(root)?;
    Ok(OwnedDataDirectory {
        directory,
        owner,
        lock_path,
        _lock_file: lock_file,
        _root_directory: root_directory,
    })
}

fn acquire_root_lock(
    root: &Path,
) -> Result<(File, File, DataDirectoryOwner, PathBuf), DataDirectoryError> {
    let lock_path = root.join(DATA_DIRECTORY_LOCK_FILE);
    let root_directory = open_root_directory(root)?;
    let mut lock_file = open_lock_file(&root_directory, &lock_path)?;
    acquire_exclusive_lock(&lock_file, &lock_path)?;

    let owner = current_owner(&lock_path)?;
    write_owner_record(&mut lock_file, &owner, &lock_path)?;
    root_directory
        .sync_all()
        .map_err(|error| io_error(root, error))?;
    Ok((root_directory, lock_file, owner, lock_path))
}

#[cfg(unix)]
fn open_root_directory(path: &Path) -> Result<File, DataDirectoryError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let descriptor = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| io_error(path, std::io::Error::from(error)))?;
    let directory = File::from(descriptor);
    let descriptor_metadata = directory
        .metadata()
        .map_err(|error| io_error(path, error))?;
    if !descriptor_metadata.is_dir() {
        return Err(DataDirectoryError::NotDirectory {
            path: path.to_path_buf(),
        });
    }
    let current_uid = rustix::process::geteuid().as_raw();
    if descriptor_metadata.uid() != current_uid {
        return Err(DataDirectoryError::WrongOwner {
            path: path.to_path_buf(),
            owner_uid: descriptor_metadata.uid(),
            current_uid,
        });
    }
    let mode = descriptor_metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(DataDirectoryError::InsecurePermissions {
            path: path.to_path_buf(),
            mode,
        });
    }
    let path_metadata = std::fs::metadata(path).map_err(|error| io_error(path, error))?;
    if descriptor_metadata.dev() != path_metadata.dev()
        || descriptor_metadata.ino() != path_metadata.ino()
    {
        return Err(DataDirectoryError::PathIdentityChanged {
            path: path.to_path_buf(),
        });
    }
    Ok(directory)
}

#[cfg(not(unix))]
fn open_root_directory(_path: &Path) -> Result<File, DataDirectoryError> {
    Err(DataDirectoryError::UnsupportedPlatformSecurity)
}

#[cfg(unix)]
fn open_lock_file(root_directory: &File, path: &Path) -> Result<File, DataDirectoryError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    reject_existing_non_file(path)?;
    let descriptor = rustix::fs::openat(
        root_directory,
        DATA_DIRECTORY_LOCK_FILE,
        rustix::fs::OFlags::RDWR
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
    )
    .map_err(|error| {
        let error = std::io::Error::from(error);
        if error.raw_os_error() == Some(libc::ELOOP) {
            DataDirectoryError::SymbolicLink {
                path: path.to_path_buf(),
            }
        } else {
            io_error(path, error)
        }
    })?;
    let file = File::from(descriptor);
    let metadata = file.metadata().map_err(|error| io_error(path, error))?;
    if !metadata.is_file() {
        return Err(DataDirectoryError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }
    let current_uid = rustix::process::geteuid().as_raw();
    if metadata.uid() != current_uid {
        return Err(DataDirectoryError::WrongOwner {
            path: path.to_path_buf(),
            owner_uid: metadata.uid(),
            current_uid,
        });
    }
    if metadata.nlink() != 1 {
        return Err(DataDirectoryError::UnexpectedLinkCount {
            path: path.to_path_buf(),
            links: metadata.nlink(),
        });
    }
    let path_metadata = std::fs::symlink_metadata(path).map_err(|error| io_error(path, error))?;
    if metadata.dev() != path_metadata.dev() || metadata.ino() != path_metadata.ino() {
        return Err(DataDirectoryError::PathIdentityChanged {
            path: path.to_path_buf(),
        });
    }
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(DataDirectoryError::InsecurePermissions {
            path: path.to_path_buf(),
            mode,
        });
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_lock_file(_root_directory: &File, _path: &Path) -> Result<File, DataDirectoryError> {
    Err(DataDirectoryError::UnsupportedPlatformSecurity)
}

fn reject_existing_non_file(path: &Path) -> Result<(), DataDirectoryError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(DataDirectoryError::SymbolicLink {
                path: path.to_path_buf(),
            })
        }
        Ok(metadata) if !metadata.is_file() => Err(DataDirectoryError::NotRegularFile {
            path: path.to_path_buf(),
        }),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(path, error)),
    }
}

#[cfg(unix)]
fn acquire_exclusive_lock(file: &File, path: &Path) -> Result<(), DataDirectoryError> {
    match rustix::fs::flock(file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(()),
        Err(error)
            if error == rustix::io::Errno::AGAIN || error == rustix::io::Errno::WOULDBLOCK =>
        {
            Err(DataDirectoryError::OwnershipHeld {
                path: path.to_path_buf(),
            })
        }
        Err(error) => Err(io_error(path, std::io::Error::from(error))),
    }
}

#[cfg(not(unix))]
fn acquire_exclusive_lock(_file: &File, _path: &Path) -> Result<(), DataDirectoryError> {
    Err(DataDirectoryError::UnsupportedPlatformSecurity)
}

fn current_owner(path: &Path) -> Result<DataDirectoryOwner, DataDirectoryError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            io_error(
                path,
                std::io::Error::other(format!("system clock precedes Unix epoch: {error}")),
            )
        })?;
    let acquired_unix_ms = u64::try_from(elapsed.as_millis()).map_err(|_| {
        io_error(
            path,
            std::io::Error::other("ownership timestamp exceeds u64 milliseconds"),
        )
    })?;
    Ok(DataDirectoryOwner {
        record_version: OWNERSHIP_RECORD_VERSION,
        data_format_version: DATA_FORMAT_VERSION,
        pid: std::process::id(),
        acquired_unix_ms,
        zippy_version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

fn write_owner_record(
    file: &mut File,
    owner: &DataDirectoryOwner,
    path: &Path,
) -> Result<(), DataDirectoryError> {
    let mut encoded = serde_json::to_vec(owner).map_err(|error| {
        io_error(
            path,
            std::io::Error::other(format!("failed to encode ownership record: {error}")),
        )
    })?;
    encoded.push(b'\n');
    file.set_len(0).map_err(|error| io_error(path, error))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|error| io_error(path, error))?;
    file.write_all(&encoded)
        .map_err(|error| io_error(path, error))?;
    file.sync_all().map_err(|error| io_error(path, error))
}
