use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

#[test]
fn partition_writer_multistep_api_is_not_externally_callable() {
    let source_path = std::env::temp_dir().join(format!(
        "zippy-partition-writer-api-{}.rs",
        std::process::id()
    ));
    fs::write(
        &source_path,
        r#"
use zippy_segment_store::{SegmentStore, SegmentStoreConfig};

pub fn attempt_multistep_partition_write() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();
    let _ = writer.begin_row();
}
"#,
    )
    .unwrap();
    let metadata_path = source_path.with_extension("rmeta");

    let target_deps = target_deps_dir();
    let output = Command::new("rustc")
        .arg("--edition=2021")
        .arg("--crate-type=lib")
        .arg("--emit=metadata")
        .arg("-o")
        .arg(&metadata_path)
        .arg(&source_path)
        .arg("--extern")
        .arg(format!(
            "zippy_segment_store={}",
            find_segment_store_rlib(&target_deps).display()
        ))
        .arg("-L")
        .arg(format!("dependency={}", target_deps.display()))
        .output()
        .unwrap();

    let _ = fs::remove_file(&source_path);
    let _ = fs::remove_file(&metadata_path);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "expected multistep partition writer API to be inaccessible, stderr=[{}]",
        stderr
    );
    assert!(
        stderr.contains("no method named") && stderr.contains("begin_row"),
        "expected missing begin_row method error, stderr=[{}]",
        stderr
    );
}

fn target_deps_dir() -> PathBuf {
    if let Some(target_dir) = std::env::var_os("CARGO_TARGET_DIR") {
        return PathBuf::from(target_dir).join("debug").join("deps");
    }

    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("expected crate to live under workspace/crates")
        .join("target")
        .join("debug")
        .join("deps")
}

fn find_segment_store_rlib(target_deps: &Path) -> PathBuf {
    let mut candidates = fs::read_dir(target_deps)
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?;
            (name.starts_with("libzippy_segment_store-") && name.ends_with(".rlib")).then_some(path)
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|path| fs::metadata(path).and_then(|meta| meta.modified()).ok());
    candidates
        .pop()
        .expect("expected zippy_segment_store rlib under target/debug/deps")
}
