use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;
use zippy_types::table::ManagedDataPath;

/// Current on-disk format understood by this Zippy binary.
pub const DATA_FORMAT_VERSION: u32 = 1;

/// Name of the marker checked before any managed data is opened.
pub const DATA_FORMAT_VERSION_FILE: &str = "FORMAT_VERSION";

const DATA_FORMAT_VERSION_TEMP_PREFIX: &str = ".FORMAT_VERSION.tmp-";

/// Directories owned and managed by a single Zippy server data root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedDataDirectory {
    Catalog,
    Wal,
    Segments,
    Parquet,
    Spill,
    Temporary,
    Backup,
    Logs,
    Run,
}

impl ManagedDataDirectory {
    /// Return the stable relative directory name used on disk.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Wal => "wal",
            Self::Segments => "segments",
            Self::Parquet => "parquet",
            Self::Spill => "spill",
            Self::Temporary => "tmp",
            Self::Backup => "backup",
            Self::Logs => "logs",
            Self::Run => "run",
        }
    }
}

const MANAGED_DIRECTORIES: [ManagedDataDirectory; 9] = [
    ManagedDataDirectory::Catalog,
    ManagedDataDirectory::Wal,
    ManagedDataDirectory::Segments,
    ManagedDataDirectory::Parquet,
    ManagedDataDirectory::Spill,
    ManagedDataDirectory::Temporary,
    ManagedDataDirectory::Backup,
    ManagedDataDirectory::Logs,
    ManagedDataDirectory::Run,
];

/// Failure returned while establishing ownership of a server data directory.
#[derive(Debug, Error)]
pub enum DataDirectoryError {
    #[error("data directory IO failed path=[{path}] reason=[{reason}]")]
    Io { path: PathBuf, reason: String },
    #[error("data root must be a directory path=[{path}]")]
    NotDirectory { path: PathBuf },
    #[error("managed data file must be a regular file path=[{path}]")]
    NotRegularFile { path: PathBuf },
    #[error("data root must not be a symbolic link path=[{path}]")]
    SymbolicLink { path: PathBuf },
    #[error("managed data path is invalid path=[{path}]")]
    InvalidManagedPath { path: PathBuf },
    #[error("durable data file already exists path=[{path}]")]
    AlreadyExists { path: PathBuf },
    #[error("data directory is already owned by another process path=[{path}]")]
    OwnershipHeld { path: PathBuf },
    #[cfg(unix)]
    #[error(
        "managed data path owner does not match the current user path=[{path}] owner_uid=[{owner_uid}] current_uid=[{current_uid}]"
    )]
    WrongOwner {
        path: PathBuf,
        owner_uid: u32,
        current_uid: u32,
    },
    #[cfg(unix)]
    #[error("managed data file must have exactly one hard link path=[{path}] links=[{links}]")]
    UnexpectedLinkCount { path: PathBuf, links: u64 },
    #[error("managed data path identity changed during secure open path=[{path}]")]
    PathIdentityChanged { path: PathBuf },
    #[error("unversioned data root is not empty path=[{path}]")]
    UnversionedNonEmpty { path: PathBuf },
    #[error("invalid data format version path=[{path}] value=[{value}]")]
    InvalidFormatVersion { path: PathBuf, value: String },
    #[error("unsupported data format version found=[{found}] supported=[{supported}]")]
    UnsupportedFormatVersion { found: u32, supported: u32 },
    #[cfg(unix)]
    #[error("data path permissions must be owner-only path=[{path}] mode=[{mode:o}]")]
    InsecurePermissions { path: PathBuf, mode: u32 },
    #[cfg(not(unix))]
    #[error("managed data directory security is unsupported on this platform")]
    UnsupportedPlatformSecurity,
}

/// Validated ownership boundary for all files managed by one Zippy server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataDirectory {
    root: PathBuf,
}

impl DataDirectory {
    /// Initialize an empty data root or reopen an already initialized one.
    ///
    /// An existing non-empty directory without `FORMAT_VERSION` is rejected so
    /// legacy or unrelated files cannot be silently claimed by the new server.
    /// Initialization temporarily owns the process lock and releases it before
    /// returning. Server runtimes should use
    /// `OwnedDataDirectory::acquire_or_initialize` so ownership remains held
    /// for their complete lifecycle.
    pub fn initialize(path: impl AsRef<Path>) -> Result<Self, DataDirectoryError> {
        let owned =
            crate::data_directory_ownership::OwnedDataDirectory::acquire_or_initialize(path)?;
        Ok(owned.directory().clone())
    }

    /// Opens a previously initialized data root without mutating its layout.
    ///
    /// Missing managed directories are rejected rather than repaired. Only an
    /// owning initialization path may create or repair managed layout entries.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DataDirectoryError> {
        require_supported_platform()?;
        let requested = path.as_ref();
        reject_root_symlink(requested)?;
        require_directory(requested)?;
        require_owner_permissions(requested)?;

        let root = canonicalize(requested)?;
        let version_path = root.join(DATA_FORMAT_VERSION_FILE);
        require_regular_file(&version_path)?;
        require_owner_permissions(&version_path)?;
        let found = read_format_version(&version_path)?;
        if found != DATA_FORMAT_VERSION {
            return Err(DataDirectoryError::UnsupportedFormatVersion {
                found,
                supported: DATA_FORMAT_VERSION,
            });
        }

        validate_managed_directories(&root)?;
        Ok(Self { root })
    }

    /// Return the canonical data root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return a managed child path after initialization has validated it.
    pub fn path(&self, directory: ManagedDataDirectory) -> PathBuf {
        self.root.join(directory.as_str())
    }
}

pub(crate) fn prepare_data_root(path: &Path) -> Result<PathBuf, DataDirectoryError> {
    require_supported_platform()?;
    reject_root_symlink(path)?;
    match fs::symlink_metadata(path) {
        Ok(_) => require_directory(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match create_owner_directory(path) {
                Ok(()) => sync_created_root_parent(path)?,
                Err(create_error) => match fs::symlink_metadata(path) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(DataDirectoryError::SymbolicLink {
                            path: path.to_path_buf(),
                        });
                    }
                    Ok(metadata) if metadata.is_dir() => {}
                    Ok(_) => {
                        return Err(DataDirectoryError::NotDirectory {
                            path: path.to_path_buf(),
                        });
                    }
                    Err(_) => return Err(create_error),
                },
            }
        }
        Err(error) => return Err(io_error(path, error)),
    }

    let version_path = path.join(DATA_FORMAT_VERSION_FILE);
    if fs::symlink_metadata(&version_path).is_ok() {
        require_owner_permissions(path)?;
    } else {
        if directory_has_unowned_entries(path)? {
            return Err(DataDirectoryError::UnversionedNonEmpty {
                path: canonicalize(path)?,
            });
        }
        restrict_owner_permissions(path)?;
    }
    canonicalize(path)
}

pub(crate) fn initialize_prepared_data_root(
    root: PathBuf,
) -> Result<DataDirectory, DataDirectoryError> {
    let version_path = root.join(DATA_FORMAT_VERSION_FILE);
    match fs::symlink_metadata(&version_path) {
        Ok(_) => validate_format_version(&version_path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if directory_has_unowned_entries(&root)? {
                return Err(DataDirectoryError::UnversionedNonEmpty { path: root });
            }
            restrict_owner_permissions(&root)?;
            write_format_version(&root, &version_path, DATA_FORMAT_VERSION)?;
        }
        Err(error) => return Err(io_error(&version_path, error)),
    }
    ensure_managed_directories(&root)?;
    Ok(DataDirectory { root })
}

fn validate_format_version(path: &Path) -> Result<(), DataDirectoryError> {
    require_regular_file(path)?;
    require_owner_permissions(path)?;
    let found = read_format_version(path)?;
    if found == DATA_FORMAT_VERSION {
        Ok(())
    } else {
        Err(DataDirectoryError::UnsupportedFormatVersion {
            found,
            supported: DATA_FORMAT_VERSION,
        })
    }
}

fn reject_root_symlink(path: &Path) -> Result<(), DataDirectoryError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(DataDirectoryError::SymbolicLink {
                path: path.to_path_buf(),
            })
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(path, error)),
    }
}

fn require_directory(path: &Path) -> Result<(), DataDirectoryError> {
    let metadata = fs::metadata(path).map_err(|error| io_error(path, error))?;
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(DataDirectoryError::NotDirectory {
            path: path.to_path_buf(),
        })
    }
}

fn require_regular_file(path: &Path) -> Result<(), DataDirectoryError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| io_error(path, error))?;
    if metadata.file_type().is_symlink() {
        return Err(DataDirectoryError::SymbolicLink {
            path: path.to_path_buf(),
        });
    }
    if metadata.is_file() {
        Ok(())
    } else {
        Err(DataDirectoryError::NotRegularFile {
            path: path.to_path_buf(),
        })
    }
}

fn canonicalize(path: &Path) -> Result<PathBuf, DataDirectoryError> {
    path.canonicalize().map_err(|error| io_error(path, error))
}

fn directory_has_unowned_entries(path: &Path) -> Result<bool, DataDirectoryError> {
    for entry in fs::read_dir(path).map_err(|error| io_error(path, error))? {
        let entry = entry.map_err(|error| io_error(path, error))?;
        let file_name = entry.file_name();
        if file_name == crate::data_directory_ownership::DATA_DIRECTORY_LOCK_FILE {
            continue;
        }
        let is_stale_version_temp = file_name
            .to_str()
            .is_some_and(|name| name.starts_with(DATA_FORMAT_VERSION_TEMP_PREFIX));
        if !is_stale_version_temp {
            return Ok(true);
        }

        let file_type = entry
            .file_type()
            .map_err(|error| io_error(&entry.path(), error))?;
        if !file_type.is_file() || file_type.is_symlink() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn read_format_version(path: &Path) -> Result<u32, DataDirectoryError> {
    let mut value = String::new();
    File::open(path)
        .and_then(|mut file| file.read_to_string(&mut value))
        .map_err(|error| io_error(path, error))?;
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| DataDirectoryError::InvalidFormatVersion {
            path: path.to_path_buf(),
            value: value.trim().to_string(),
        })
}

fn write_format_version(
    root: &Path,
    path: &Path,
    expected_version: u32,
) -> Result<(), DataDirectoryError> {
    let managed_path = ManagedDataPath::new(DATA_FORMAT_VERSION_FILE)
        .expect("the fixed format marker is a valid managed path");
    let mut temporary = crate::durable_file::begin_durable_file(root, &managed_path)?;
    writeln!(temporary, "{expected_version}").map_err(|error| io_error(path, error))?;
    match temporary.publish_noclobber() {
        Ok(_) => {}
        Err(DataDirectoryError::AlreadyExists { .. }) => {
            require_regular_file(path)?;
            require_owner_permissions(path)?;
            let found = read_format_version(path)?;
            if found != expected_version {
                return Err(DataDirectoryError::UnsupportedFormatVersion {
                    found,
                    supported: expected_version,
                });
            }
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

fn ensure_managed_directories(root: &Path) -> Result<(), DataDirectoryError> {
    let mut created = false;
    for directory in MANAGED_DIRECTORIES {
        let path = root.join(directory.as_str());
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(DataDirectoryError::SymbolicLink { path });
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(DataDirectoryError::NotDirectory { path });
            }
            Ok(_) => require_owner_permissions(&path)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_owner_directory(&path)?;
                created = true;
            }
            Err(error) => return Err(io_error(&path, error)),
        }
    }
    if created {
        sync_directory_io(root).map_err(|error| io_error(root, error))?;
    }
    Ok(())
}

fn validate_managed_directories(root: &Path) -> Result<(), DataDirectoryError> {
    for directory in MANAGED_DIRECTORIES {
        let path = root.join(directory.as_str());
        let metadata = fs::symlink_metadata(&path).map_err(|error| io_error(&path, error))?;
        if metadata.file_type().is_symlink() {
            return Err(DataDirectoryError::SymbolicLink { path });
        }
        if !metadata.is_dir() {
            return Err(DataDirectoryError::NotDirectory { path });
        }
        require_owner_permissions(&path)?;
    }
    Ok(())
}

fn sync_created_root_parent(path: &Path) -> Result<(), DataDirectoryError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let parent = parent.unwrap_or_else(|| Path::new("."));
    sync_directory_io(parent).map_err(|error| io_error(parent, error))
}

#[cfg(unix)]
pub(crate) fn create_owner_directory(path: &Path) -> Result<(), DataDirectoryError> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(path).map_err(|error| io_error(path, error))
}

#[cfg(not(unix))]
pub(crate) fn create_owner_directory(path: &Path) -> Result<(), DataDirectoryError> {
    fs::create_dir(path).map_err(|error| io_error(path, error))
}

#[cfg(unix)]
fn restrict_owner_permissions(path: &Path) -> Result<(), DataDirectoryError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| io_error(path, error))
}

#[cfg(not(unix))]
fn restrict_owner_permissions(_path: &Path) -> Result<(), DataDirectoryError> {
    Ok(())
}

#[cfg(unix)]
pub(crate) fn require_owner_permissions(path: &Path) -> Result<(), DataDirectoryError> {
    use std::os::unix::fs::PermissionsExt;

    let mode = fs::metadata(path)
        .map_err(|error| io_error(path, error))?
        .permissions()
        .mode()
        & 0o777;
    if mode & 0o077 == 0 {
        Ok(())
    } else {
        Err(DataDirectoryError::InsecurePermissions {
            path: path.to_path_buf(),
            mode,
        })
    }
}

#[cfg(not(unix))]
pub(crate) fn require_owner_permissions(_path: &Path) -> Result<(), DataDirectoryError> {
    Ok(())
}

#[cfg(unix)]
pub(crate) fn sync_directory_io(path: &Path) -> std::io::Result<()> {
    File::open(path).and_then(|directory| directory.sync_all())
}

#[cfg(not(unix))]
pub(crate) fn sync_directory_io(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn require_supported_platform() -> Result<(), DataDirectoryError> {
    Ok(())
}

#[cfg(not(unix))]
fn require_supported_platform() -> Result<(), DataDirectoryError> {
    Err(DataDirectoryError::UnsupportedPlatformSecurity)
}

pub(crate) fn io_error(path: &Path, error: std::io::Error) -> DataDirectoryError {
    DataDirectoryError::Io {
        path: path.to_path_buf(),
        reason: error.to_string(),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::{read_format_version, write_format_version, DataDirectoryError};

    #[test]
    fn format_publish_never_overwrites_a_competing_version() {
        let root = tempfile::tempdir().unwrap();
        let marker = root.path().join("FORMAT_VERSION");
        write_format_version(root.path(), &marker, 2).unwrap();

        let error = write_format_version(root.path(), &marker, 1).unwrap_err();

        assert!(matches!(
            error,
            DataDirectoryError::UnsupportedFormatVersion {
                found: 2,
                supported: 1
            }
        ));
        assert_eq!(read_format_version(&marker).unwrap(), 2);
    }
}
