use std::fs;
use std::io::Write;

use tempfile::TempDir;
use zippy_core::{DataDirectory, DataDirectoryError, OwnedDataDirectory};
use zippy_types::table::ManagedDataPath;

fn data_directory(directory: &TempDir) -> OwnedDataDirectory {
    DataDirectory::initialize(directory.path().join("data"))
        .expect("initialize data root")
        .acquire_process_ownership()
        .expect("acquire data root ownership")
}

fn managed_path(value: &str) -> ManagedDataPath {
    ManagedDataPath::new(value).expect("valid managed path")
}

#[test]
fn durable_file_publishes_nested_contents_and_metadata() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = managed_path("parquet/table-1/file-1.parquet");
    let mut transaction = data_directory
        .begin_durable_file(&path)
        .expect("begin durable file");
    let temporary_path = transaction.temporary_path().to_path_buf();
    assert!(temporary_path.exists());
    transaction
        .write_all(b"durable payload")
        .expect("write temporary file");

    let published = transaction
        .publish_noclobber()
        .expect("durably publish file");

    assert!(!temporary_path.exists());
    assert_eq!(published.managed_path(), &path);
    assert_eq!(
        published.path(),
        data_directory.directory().root().join(path.as_str())
    );
    assert_eq!(published.size_bytes(), 15);
    assert_eq!(
        fs::read(published.path()).expect("read final file"),
        b"durable payload"
    );
}

#[test]
fn durable_file_never_overwrites_an_existing_target() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = managed_path("parquet/table-1/file-1.parquet");

    let mut first = data_directory
        .begin_durable_file(&path)
        .expect("begin first file");
    first.write_all(b"first").expect("write first file");
    first.publish_noclobber().expect("publish first file");

    let mut second = data_directory
        .begin_durable_file(&path)
        .expect("begin competing file");
    let temporary_path = second.temporary_path().to_path_buf();
    second.write_all(b"second").expect("write competing file");
    let error = second
        .publish_noclobber()
        .expect_err("competing publish must not replace the final file");

    assert!(matches!(
        error,
        DataDirectoryError::AlreadyExists { path: existing }
            if existing == data_directory.directory().root().join(path.as_str())
    ));
    assert!(!temporary_path.exists());
    assert_eq!(
        fs::read(data_directory.directory().root().join(path.as_str()))
            .expect("read original file"),
        b"first"
    );
}

#[test]
fn dropping_an_unpublished_file_removes_its_temporary_inode() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = managed_path("parquet/table-1/file-1.parquet");
    let mut transaction = data_directory
        .begin_durable_file(&path)
        .expect("begin durable file");
    transaction
        .write_all(b"pending")
        .expect("write temporary file");
    let temporary_path = transaction.temporary_path().to_path_buf();

    drop(transaction);

    assert!(!temporary_path.exists());
    assert!(!data_directory
        .directory()
        .root()
        .join(path.as_str())
        .exists());
}

#[cfg(unix)]
#[test]
fn durable_file_rejects_a_symbolic_link_parent() {
    use std::os::unix::fs::symlink;

    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let external = directory.path().join("external");
    fs::create_dir(&external).expect("create external directory");
    let link = data_directory.directory().root().join("parquet/link");
    symlink(&external, &link).expect("create parent symlink");

    let error = data_directory
        .begin_durable_file(&managed_path("parquet/link/file.parquet"))
        .expect_err("managed publication must reject symlink traversal");

    assert!(matches!(
        error,
        DataDirectoryError::SymbolicLink { path } if path == link
    ));
    assert!(fs::read_dir(&external)
        .expect("read external directory")
        .next()
        .is_none());
}

#[test]
fn durable_file_rejects_a_regular_file_parent_component() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let parent = data_directory
        .directory()
        .root()
        .join("parquet/not-a-directory");
    fs::write(&parent, b"file").expect("create regular parent component");

    let error = data_directory
        .begin_durable_file(&managed_path("parquet/not-a-directory/file.parquet"))
        .expect_err("managed publication must require directory parents");

    assert!(matches!(
        error,
        DataDirectoryError::NotDirectory { path } if path == parent
    ));
}
