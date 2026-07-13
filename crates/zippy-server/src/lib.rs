//! Composition root for the incremental single-node server refactor.
//!
//! Phase 1 keeps the existing master and gateway implementations behind the
//! stable service contracts. Capability metadata makes every compatibility
//! limitation explicit so callers cannot mistake an adapter for the future
//! SQLite catalog or DataFusion query engine.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod composition;
mod health;
mod legacy_query;
mod runtime;

pub use composition::LegacyServerServices;
pub use health::{LegacyServerCapabilities, LegacyServerHealth, ServiceHealth};
pub use legacy_query::{
    LegacyGatewayPlan, LegacyQueryEngineAdapter, LegacyQueryEngineConfig,
    LEGACY_GATEWAY_PLAN_FORMAT_VERSION,
};
pub use runtime::{
    run_server_daemon, LegacyServerMode, LegacyServerReadiness, LegacyServerRuntime,
    LegacyServerRuntimeConfig, LegacyServerRuntimeHealth, LegacyServerShutdownOutcome,
};
