#[cfg(unix)]
use std::fs;

use tempfile::tempdir;
use zippy_core::{DataDirectory, DataDirectoryError};
#[cfg(unix)]
use zippy_core::{ManagedDataDirectory, DATA_FORMAT_VERSION, DATA_FORMAT_VERSION_FILE};

#[cfg(unix)]
#[test]
fn initialize_creates_versioned_managed_layout() {
    let parent = tempdir().unwrap();
    let root = parent.path().join("server-data");

    let data_directory = DataDirectory::initialize(&root).unwrap();

    assert_eq!(data_directory.root(), root.canonicalize().unwrap());
    assert_eq!(
        fs::read_to_string(root.join(DATA_FORMAT_VERSION_FILE)).unwrap(),
        format!("{DATA_FORMAT_VERSION}\n")
    );
    for directory in [
        ManagedDataDirectory::Catalog,
        ManagedDataDirectory::Wal,
        ManagedDataDirectory::Segments,
        ManagedDataDirectory::Parquet,
        ManagedDataDirectory::Spill,
        ManagedDataDirectory::Temporary,
        ManagedDataDirectory::Backup,
        ManagedDataDirectory::Logs,
        ManagedDataDirectory::Run,
    ] {
        assert!(data_directory.path(directory).is_dir());
    }
}

#[cfg(unix)]
#[test]
fn initialized_data_directory_can_be_reopened() {
    let parent = tempdir().unwrap();
    let root = parent.path().join("server-data");
    let initialized = DataDirectory::initialize(&root).unwrap();

    let reopened = DataDirectory::open(&root).unwrap();

    assert_eq!(reopened, initialized);
}

#[cfg(unix)]
#[test]
fn initialize_recovers_from_stale_version_temporary_file() {
    let parent = tempdir().unwrap();
    let root = parent.path().join("server-data");
    fs::create_dir(&root).unwrap();
    fs::write(root.join(".FORMAT_VERSION.tmp-stale"), b"").unwrap();

    let data_directory = DataDirectory::initialize(&root).unwrap();

    assert_eq!(data_directory.root(), root.canonicalize().unwrap());
    assert_eq!(
        fs::read_to_string(root.join(DATA_FORMAT_VERSION_FILE)).unwrap(),
        format!("{DATA_FORMAT_VERSION}\n")
    );
}

#[cfg(unix)]
#[test]
fn initialize_rejects_non_empty_unversioned_directory() {
    let root = tempdir().unwrap();
    fs::write(root.path().join("legacy-registry.json"), b"{}").unwrap();

    let error = DataDirectory::initialize(root.path()).unwrap_err();

    assert!(matches!(
        error,
        DataDirectoryError::UnversionedNonEmpty { .. }
    ));
}

#[cfg(unix)]
#[test]
fn rejected_unversioned_directory_keeps_existing_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let parent = tempdir().unwrap();
    let root = parent.path().join("legacy-data");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o750)).unwrap();
    fs::write(root.join("legacy-registry.json"), b"{}").unwrap();

    let error = DataDirectory::initialize(&root).unwrap_err();
    let mode = fs::metadata(&root).unwrap().permissions().mode() & 0o777;

    assert!(matches!(
        error,
        DataDirectoryError::UnversionedNonEmpty { .. }
    ));
    assert_eq!(mode, 0o750);
}

#[cfg(unix)]
#[test]
fn open_rejects_unknown_or_malformed_format_versions() {
    let parent = tempdir().unwrap();
    let future_root = parent.path().join("future");
    let malformed_root = parent.path().join("malformed");
    DataDirectory::initialize(&future_root).unwrap();
    DataDirectory::initialize(&malformed_root).unwrap();

    fs::write(
        future_root.join(DATA_FORMAT_VERSION_FILE),
        format!("{}\n", DATA_FORMAT_VERSION + 1),
    )
    .unwrap();
    fs::write(malformed_root.join(DATA_FORMAT_VERSION_FILE), b"v1\n").unwrap();

    assert!(matches!(
        DataDirectory::open(future_root).unwrap_err(),
        DataDirectoryError::UnsupportedFormatVersion { .. }
    ));
    assert!(matches!(
        DataDirectory::open(malformed_root).unwrap_err(),
        DataDirectoryError::InvalidFormatVersion { .. }
    ));
}

#[cfg(unix)]
#[test]
fn initialize_enforces_owner_only_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let parent = tempdir().unwrap();
    let root = parent.path().join("server-data");
    DataDirectory::initialize(&root).unwrap();

    let root_mode = fs::metadata(&root).unwrap().permissions().mode() & 0o777;
    let catalog_mode = fs::metadata(root.join("catalog"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    let version_mode = fs::metadata(root.join(DATA_FORMAT_VERSION_FILE))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(root_mode, 0o700);
    assert_eq!(catalog_mode, 0o700);
    assert_eq!(version_mode, 0o600);
}

#[cfg(unix)]
#[test]
fn open_rejects_symbolic_link_data_root() {
    use std::os::unix::fs::symlink;

    let parent = tempdir().unwrap();
    let target = parent.path().join("target");
    let link = parent.path().join("link");
    DataDirectory::initialize(&target).unwrap();
    symlink(&target, &link).unwrap();

    assert!(matches!(
        DataDirectory::open(link).unwrap_err(),
        DataDirectoryError::SymbolicLink { .. }
    ));
}

#[cfg(unix)]
#[test]
fn open_rejects_symbolic_link_format_marker() {
    use std::os::unix::fs::symlink;

    let parent = tempdir().unwrap();
    let root = parent.path().join("server-data");
    let external_marker = parent.path().join("external-version");
    DataDirectory::initialize(&root).unwrap();
    fs::write(&external_marker, format!("{DATA_FORMAT_VERSION}\n")).unwrap();
    fs::remove_file(root.join(DATA_FORMAT_VERSION_FILE)).unwrap();
    symlink(&external_marker, root.join(DATA_FORMAT_VERSION_FILE)).unwrap();

    assert!(matches!(
        DataDirectory::open(root).unwrap_err(),
        DataDirectoryError::SymbolicLink { .. }
    ));
}

#[cfg(unix)]
#[test]
fn open_rejects_symbolic_link_managed_directory() {
    use std::os::unix::fs::symlink;

    let parent = tempdir().unwrap();
    let root = parent.path().join("server-data");
    let external_catalog = parent.path().join("external-catalog");
    DataDirectory::initialize(&root).unwrap();
    fs::create_dir(&external_catalog).unwrap();
    fs::remove_dir(root.join("catalog")).unwrap();
    symlink(&external_catalog, root.join("catalog")).unwrap();

    assert!(matches!(
        DataDirectory::open(root).unwrap_err(),
        DataDirectoryError::SymbolicLink { .. }
    ));
}

#[cfg(not(unix))]
#[test]
fn managed_data_directory_fails_closed_without_owner_acl_support() {
    let root = tempdir().unwrap();

    assert!(matches!(
        DataDirectory::initialize(root.path()).unwrap_err(),
        DataDirectoryError::UnsupportedPlatformSecurity
    ));
}
