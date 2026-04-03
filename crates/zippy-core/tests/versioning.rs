use std::path::PathBuf;
use std::process::Command;

use zippy_core::{base_version, python_dev_version, rust_dev_version};

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
fn derived_versions_match_current_git_head() {
    let commit_count = git_output(&["rev-list", "--count", "HEAD"]);
    let short_sha = git_output(&["rev-parse", "--short", "HEAD"]);

    assert_eq!(
        rust_dev_version(),
        format!("{}-dev.{}+g{}", base_version(), commit_count, short_sha)
    );
    assert_eq!(
        python_dev_version(),
        format!("{}.dev{}+g{}", base_version(), commit_count, short_sha)
    );
}
