use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let workspace_root = workspace_root();
    println!("cargo:rerun-if-changed=../");
    println!(
        "cargo:rerun-if-changed={}",
        workspace_root.join("Cargo.toml").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        workspace_root.join("Cargo.lock").display()
    );
    println!("cargo:rerun-if-env-changed=ZIPPY_BUILD_GIT_SHA");
    println!("cargo:rerun-if-env-changed=ZIPPY_BUILD_GIT_DIRTY");

    emit_git_rerun_path(&workspace_root, "HEAD");
    emit_git_rerun_path(&workspace_root, "packed-refs");
    if let Some(head_ref) = git_output(&workspace_root, &["symbolic-ref", "-q", "HEAD"]) {
        emit_git_rerun_path(&workspace_root, &head_ref);
    }

    let git_sha = env::var("ZIPPY_BUILD_GIT_SHA")
        .ok()
        .or_else(|| git_output(&workspace_root, &["rev-parse", "HEAD"]));
    if let Some(git_sha) = git_sha {
        println!("cargo:rustc-env=ZIPPY_PERF_GIT_SHA={git_sha}");
    }
    let dirty = env::var("ZIPPY_BUILD_GIT_DIRTY").ok().or_else(|| {
        git_output(
            &workspace_root,
            &["status", "--porcelain=v1", "--untracked-files=normal"],
        )
        .map(|status| {
            if status.is_empty() {
                "false".to_string()
            } else {
                "true".to_string()
            }
        })
    });
    if let Some(dirty) = dirty {
        println!("cargo:rustc-env=ZIPPY_PERF_GIT_DIRTY={dirty}");
    }
}

fn emit_git_rerun_path(workspace_root: &Path, git_path: &str) {
    let Some(path) = git_output(workspace_root, &["rev-parse", "--git-path", git_path]) else {
        return;
    };
    let path = PathBuf::from(path);
    let path = if path.is_absolute() {
        path
    } else {
        workspace_root.join(path)
    };
    println!("cargo:rerun-if-changed={}", path.display());
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("cargo manifest dir should exist"))
        .join("../..")
        .canonicalize()
        .expect("workspace root should resolve")
}

fn git_output(workspace_root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|stdout| stdout.trim().to_string())
}
