pub mod daemon;
pub mod legacy_catalog;
pub mod legacy_runtime;
pub mod registry;
pub mod server;
pub mod snapshot;

pub fn crate_name() -> &'static str {
    "zippy-master"
}
