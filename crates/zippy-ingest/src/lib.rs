//! Server-owned table runtimes for native ingest and persisted/live coordination.
//!
//! This crate deliberately does not depend on the legacy master, gateway, or
//! stream materializer. Catalog recovery creates immutable bootstrap records;
//! later actor and persistence layers mutate table state through one owner.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod actor;
mod registry;

pub use actor::{
    TableActorAdmissionControl, TableActorHandle, TableActorRegistry, TableActorRegistryConfig,
    TableActorRegistryHandle, TableActorSnapshot, TableActorSnapshotRequest,
};
pub use registry::{TableRuntime, TableRuntimeBootstrap, TableRuntimeRegistry};
