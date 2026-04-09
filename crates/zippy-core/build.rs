use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let workspace_root = workspace_root();
    let (git_dir, git_common_dir) = resolve_git_dirs(&workspace_root);

    emit_rerun_paths(&git_dir, &git_common_dir);

    if let Some(commit_count) = git_output(&workspace_root, &["rev-list", "--count", "HEAD"]) {
        println!("cargo:rustc-env=ZIPPY_GIT_COMMIT_COUNT={commit_count}");
    }

    if let Some(short_sha) = git_output(&workspace_root, &["rev-parse", "--short", "HEAD"]) {
        println!("cargo:rustc-env=ZIPPY_GIT_SHORT_SHA={short_sha}");
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("cargo manifest dir should exist"))
        .join("../..")
        .canonicalize()
        .expect("workspace root should resolve")
}

fn resolve_git_dirs(workspace_root: &Path) -> (PathBuf, PathBuf) {
    let dot_git = workspace_root.join(".git");

    let git_dir = if dot_git.is_dir() {
        dot_git
    } else {
        let content =
            fs::read_to_string(&dot_git).expect(".git indirection file should be readable");
        let path = content
            .trim()
            .strip_prefix("gitdir: ")
            .expect(".git file should start with gitdir: ");
        workspace_root
            .join(path)
            .canonicalize()
            .expect("git dir should resolve")
    };

    let git_common_dir = match fs::read_to_string(git_dir.join("commondir")) {
        Ok(path) => git_dir
            .join(path.trim())
            .canonicalize()
            .expect("common git dir should resolve"),
        Err(_) => git_dir.clone(),
    };

    (git_dir, git_common_dir)
}

fn emit_rerun_paths(git_dir: &Path, git_common_dir: &Path) {
    let head_path = git_dir.join("HEAD");
    println!("cargo:rerun-if-changed={}", head_path.display());
    println!(
        "cargo:rerun-if-changed={}",
        git_common_dir.join("packed-refs").display()
    );

    if let Ok(content) = fs::read_to_string(&head_path) {
        if let Some(reference) = content.trim().strip_prefix("ref: ") {
            println!(
                "cargo:rerun-if-changed={}",
                git_common_dir.join(reference).display()
            );
        }
    }
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
