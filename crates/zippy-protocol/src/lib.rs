//! Versioned Arrow Flight contracts for Zippy query frontends.
//!
//! The crate owns wire validation, stable error translation, one-use query
//! tickets, and the transport-facing query abstraction. Storage and execution
//! implementations remain in `zippy-server`.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod auth;
pub mod error;
pub mod query;
pub mod runtime;
pub mod server;

/// Generated protobuf-compatible wire structures for protocol version 1.
///
/// The canonical schema lives at
/// `proto/zippy/flight/v1/protocol.proto`. These definitions are checked in so
/// normal builds do not require a host `protoc` binary.
#[allow(missing_docs)]
pub mod wire;

/// Current Zippy Flight protocol version.
pub const CURRENT_PROTOCOL_VERSION: u32 = 1;

/// Binary response metadata key carrying a serialized structured Zippy error.
pub const ERROR_METADATA_KEY: &str = "x-zippy-error-bin";

/// Request metadata key that identifies the owning Zippy client session.
pub const SESSION_METADATA_KEY: &str = "x-zippy-session-id";

/// Standard request metadata key carrying bootstrap or session bearer credentials.
pub const AUTHORIZATION_METADATA_KEY: &str = "authorization";

/// Flight action returning server protocol capabilities.
pub const CAPABILITIES_ACTION: &str = "zippy.capabilities.v1";
