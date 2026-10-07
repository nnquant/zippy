use std::path::PathBuf;
use std::process::Command;

use zippy_core::{base_version, build_revision};

#[test]
fn base_version_is_current_release_baseline() {
    assert_eq!(base_version(), "0.5.0");
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root should resolve")
}

fn git_output(args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace_root())
        .args(args)
        .output()
        .expect("git command should execute");

    assert!(
        output.status.success(),
        "git command failed args={:?} stderr={}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout)
        .expect("git stdout should be utf8")
        .trim()
        .to_string()
}

#[test]
fn build_revision_is_separate_from_release_version() {
    if workspace_root().join(".git").exists() {
        assert_eq!(
            build_revision(),
            Some(git_output(&["rev-parse", "HEAD"]).as_str())
        );
    } else {
        assert_eq!(build_revision(), None);
    }
}
