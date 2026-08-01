#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use zippy_segment_store::ShmRegion;

#[cfg(target_os = "linux")]
fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

#[cfg(unix)]
#[test]
fn shm_region_backing_file_is_preallocated_to_avoid_sparse_mmap_sigbus() {
    let size = 8 * 1024 * 1024;
    let owner = ShmRegion::create(size).unwrap();
    let path = owner
        .os_id()
        .strip_prefix("file:")
        .expect("expected file-backed shm os_id");
    let metadata = std::fs::metadata(path).unwrap();
    let allocated_bytes = metadata.blocks() * 512;

    assert!(
        allocated_bytes >= size as u64,
        "backing file should be fully allocated allocated_bytes=[{}] expected_bytes=[{}]",
        allocated_bytes,
        size
    );
}

#[test]
fn shm_region_reopens_by_os_id_and_reads_existing_bytes() {
    let mut owner = ShmRegion::create(4096).unwrap();
    let os_id = owner.os_id().to_string();
    let expected = 42_u64.to_ne_bytes();

    owner.write_at(128, &expected).unwrap();

    let reader = ShmRegion::open(&os_id).unwrap();
    let mut actual = [0_u8; 8];
    reader.read_at(128, &mut actual).unwrap();

    assert_eq!(actual, expected);
    assert_eq!(reader.len(), 4096);
}

#[cfg(target_os = "linux")]
#[test]
fn shm_region_mappings_do_not_retain_backing_file_descriptors() {
    let before = open_fd_count();
    let owners = (0..64)
        .map(|_| ShmRegion::create(4096).unwrap())
        .collect::<Vec<_>>();
    let readers = owners
        .iter()
        .map(|owner| ShmRegion::open(owner.os_id()).unwrap())
        .collect::<Vec<_>>();
    let after = open_fd_count();

    assert_eq!(owners.len(), 64);
    assert_eq!(readers.len(), 64);
    assert!(
        after <= before + 2,
        "mapped regions retained backing descriptors before=[{before}] after=[{after}]"
    );
}

#[test]
fn shm_region_writes_from_second_attach_are_visible_to_owner() {
    let owner = ShmRegion::create(4096).unwrap();
    let os_id = owner.os_id().to_string();
    let mut peer = ShmRegion::open(&os_id).unwrap();
    let expected = 7_u64.to_ne_bytes();

    peer.write_at(256, &expected).unwrap();

    let mut actual = [0_u8; 8];
    owner.read_at(256, &mut actual).unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn shm_region_publishes_u64_with_release_acquire_across_attach() {
    let owner = ShmRegion::create(4096).unwrap();
    let reader = ShmRegion::open(owner.os_id()).unwrap();

    owner.store_u64_release(128, 9).unwrap();

    assert_eq!(reader.load_u64_acquire(128).unwrap(), 9);
}

#[test]
fn shm_region_rejects_unaligned_atomic_u64_offsets() {
    let owner = ShmRegion::create(4096).unwrap();

    let err = owner.store_u64_release(129, 1).unwrap_err();

    assert!(format!("{err}").contains("not aligned"));
}
