pub mod bus;
pub mod daemon;
pub mod registry;
pub mod snapshot;
pub mod ring;
pub mod server;

pub fn crate_name() -> &'static str {
    "zippy-master"
}
