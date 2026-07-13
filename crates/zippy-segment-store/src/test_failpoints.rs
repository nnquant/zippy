//! Opt-in process-abort failpoints for persistence integration tests.
//!
//! This module is compiled only with the `test-failpoints` feature. A failpoint
//! remains disabled unless [`ACTIVE_FAILPOINT_ENV`] exactly matches its name.
//! These points validate process termination ordering, not power-loss
//! durability. Durability tests require explicit file and directory sync points.

use std::ffi::OsStr;

/// Environment variable selecting the single failpoint armed in a child process.
pub const ACTIVE_FAILPOINT_ENV: &str = "ZIPPY_UNSAFE_TEST_FAILPOINT";

/// Crash after a complete Parquet temporary file is closed but before it is renamed.
pub const PERSISTENCE_AFTER_PARQUET_CLOSE: &str = "persistence.after_parquet_close_before_rename";

pub(crate) fn hit(name: &str) {
    let armed = std::env::var_os(ACTIVE_FAILPOINT_ENV);
    if armed.as_deref() == Some(OsStr::new(name)) {
        std::process::abort();
    }
}
