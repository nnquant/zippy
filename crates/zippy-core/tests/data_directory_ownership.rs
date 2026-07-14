use std::fs;
use std::process::Command;

use tempfile::TempDir;
use zippy_core::{DataDirectory, DataDirectoryError, DataDirectoryOwner, DATA_DIRECTORY_LOCK_FILE};

const CHILD_ROOT_ENV: &str = "ZIPPY_TEST_OWNERSHIP_CHILD_ROOT";
const CHILD_READY_ENV: &str = "ZIPPY_TEST_OWNERSHIP_CHILD_READY";
const CHILD_EXIT_CODE: i32 = 86;

fn initialized_directory(directory: &TempDir) -> DataDirectory {
    DataDirectory::initialize(directory.path().join("data")).expect("initialize data root")
}

#[test]
fn ownership_is_exclusive_but_does_not_block_read_only_open() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = initialized_directory(&directory);
    let root = data_directory.root().to_path_buf();
    let owner = data_directory
        .clone()
        .acquire_process_ownership()
        .expect("acquire first owner");

    let reopened = DataDirectory::open(&root).expect("read-only open while owned");
    let error = reopened
        .acquire_process_ownership()
        .expect_err("second owner must fail without waiting");
    assert!(matches!(
        error,
        DataDirectoryError::OwnershipHeld { path }
            if path == root.join(DATA_DIRECTORY_LOCK_FILE)
    ));

    drop(owner);
    DataDirectory::open(&root)
        .expect("reopen data directory")
        .acquire_process_ownership()
        .expect("reacquire after owner drop");
}

#[cfg(unix)]
#[test]
fn ownership_record_is_owner_only_and_diagnostic() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TempDir::new().expect("create temp directory");
    let owner = initialized_directory(&directory)
        .acquire_process_ownership()
        .expect("acquire owner");
    let metadata = fs::metadata(owner.lock_path()).expect("read lock metadata");
    let recorded: DataDirectoryOwner =
        serde_json::from_slice(&fs::read(owner.lock_path()).expect("read ownership record"))
            .expect("decode ownership record");

    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!(recorded, *owner.owner());
    assert_eq!(recorded.pid(), std::process::id());
    assert!(recorded.acquired_unix_ms() > 0);
    assert_eq!(recorded.zippy_version(), env!("CARGO_PKG_VERSION"));
}

#[cfg(unix)]
#[test]
fn ownership_rejects_symbolic_link_lock_file() {
    use std::os::unix::fs::symlink;

    let directory = TempDir::new().expect("create temp directory");
    let data_directory = initialized_directory(&directory);
    let external = directory.path().join("external.lock");
    fs::write(&external, b"external").expect("create external file");
    let lock_path = data_directory.root().join(DATA_DIRECTORY_LOCK_FILE);
    fs::remove_file(&lock_path).expect("remove inactive lock file for tamper test");
    symlink(&external, &lock_path).expect("create lock symlink");

    let error = data_directory
        .acquire_process_ownership()
        .expect_err("ownership must reject lock symlinks");

    assert!(matches!(
        error,
        DataDirectoryError::SymbolicLink { path } if path == lock_path
    ));
    assert_eq!(fs::read(external).expect("read external file"), b"external");
}

#[cfg(unix)]
#[test]
fn ownership_rejects_a_hard_linked_lock_file() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = initialized_directory(&directory);
    let lock_path = data_directory.root().join(DATA_DIRECTORY_LOCK_FILE);
    let alias_path = data_directory.root().join("server.lock.alias");
    fs::hard_link(&lock_path, &alias_path).expect("create lock hard link");

    let error = data_directory
        .acquire_process_ownership()
        .expect_err("ownership must reject multiply linked lock inodes");

    assert!(matches!(
        error,
        DataDirectoryError::UnexpectedLinkCount { path, links: 2 } if path == lock_path
    ));
}

#[cfg(unix)]
#[test]
fn ownership_reuses_the_stable_lock_inode() {
    use std::os::unix::fs::MetadataExt;

    let directory = TempDir::new().expect("create temp directory");
    let data_directory = initialized_directory(&directory);
    let lock_path = data_directory.root().join(DATA_DIRECTORY_LOCK_FILE);
    let before = fs::metadata(&lock_path).expect("stat initial lock");

    let owner = data_directory
        .clone()
        .acquire_process_ownership()
        .expect("acquire owner");
    drop(owner);
    let owner = data_directory
        .acquire_process_ownership()
        .expect("reacquire owner");
    let after = fs::metadata(owner.lock_path()).expect("stat reacquired lock");

    assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
}

#[cfg(unix)]
#[test]
fn abrupt_process_exit_releases_kernel_ownership() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = initialized_directory(&directory);
    let root = data_directory.root().to_path_buf();
    let ready_path = directory.path().join("child-ready");
    let status = Command::new(std::env::current_exe().expect("resolve current test executable"))
        .arg("--exact")
        .arg("ownership_child_exits_without_drop")
        .arg("--nocapture")
        .env(CHILD_ROOT_ENV, &root)
        .env(CHILD_READY_ENV, &ready_path)
        .status()
        .expect("run ownership child");

    assert_eq!(status.code(), Some(CHILD_EXIT_CODE));
    assert!(
        ready_path.exists(),
        "child exited before acquiring ownership"
    );
    assert!(root.join(DATA_DIRECTORY_LOCK_FILE).exists());
    DataDirectory::open(root)
        .expect("open after abrupt child exit")
        .acquire_process_ownership()
        .expect("kernel must release ownership when the process exits");
}

#[cfg(unix)]
#[test]
fn ownership_child_exits_without_drop() {
    let Some(root) = std::env::var_os(CHILD_ROOT_ENV) else {
        return;
    };
    let ready_path = std::env::var_os(CHILD_READY_ENV).expect("child ready path");
    let _owner = DataDirectory::open(root)
        .expect("child opens data directory")
        .acquire_process_ownership()
        .expect("child acquires ownership");
    fs::write(ready_path, b"ready").expect("publish child readiness");
    std::process::exit(CHILD_EXIT_CODE);
}
