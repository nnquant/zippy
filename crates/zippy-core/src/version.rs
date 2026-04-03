fn derived_components() -> Option<(&'static str, &'static str)> {
    let commit_count = option_env!("ZIPPY_GIT_COMMIT_COUNT")?;
    let short_sha = option_env!("ZIPPY_GIT_SHORT_SHA")?;
    Some((commit_count, short_sha))
}

/// Return the static base version declared in Cargo metadata.
pub fn base_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Return the Rust-facing development version derived from the current git HEAD.
pub fn rust_dev_version() -> String {
    match derived_components() {
        Some((commit_count, short_sha)) => {
            format!("{}-dev.{}+g{}", base_version(), commit_count, short_sha)
        }
        None => base_version().to_string(),
    }
}

/// Return the Python-facing PEP 440 development version derived from the current git HEAD.
pub fn python_dev_version() -> String {
    match derived_components() {
        Some((commit_count, short_sha)) => {
            format!("{}.dev{}+g{}", base_version(), commit_count, short_sha)
        }
        None => base_version().to_string(),
    }
}
