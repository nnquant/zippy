pub mod bus;
pub mod daemon;
pub mod registry;
pub mod ring;
pub mod server;
pub mod snapshot;

pub fn crate_name() -> &'static str {
    "zippy-master"
}
