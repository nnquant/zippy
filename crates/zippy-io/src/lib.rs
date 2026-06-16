pub mod publisher;
pub mod zmq;

pub use publisher::{FanoutPublisher, NullPublisher};
pub use zippy_core::Publisher;
pub use zmq::{ZmqPublisher, ZmqSource, ZmqStreamPublisher, ZmqSubscriber};

pub fn crate_name() -> &'static str {
    "zippy-io"
}
