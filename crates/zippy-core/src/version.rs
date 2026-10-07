/// Return the release version declared in Cargo metadata.
pub fn base_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Return the optional source commit, independently of the release version.
///
/// This identifies the checkout base; it does not assert that the working tree was clean.
/// Source archives without Git metadata return `None`.
pub fn build_revision() -> Option<&'static str> {
    option_env!("ZIPPY_BUILD_REVISION")
}
