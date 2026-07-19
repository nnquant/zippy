//! Server-owned table runtimes for native ingest and persisted/live coordination.
//!
//! This crate deliberately does not depend on the legacy master, gateway, or
//! stream materializer. Catalog recovery creates immutable bootstrap records;
//! later actor and persistence layers mutate table state through one owner.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod actor;
mod memory;
mod registry;

pub use actor::{
    TableActorAdmissionControl, TableActorFailureMonitor, TableActorHandle,
    TableActorLiveSegmentPublication, TableActorLiveSegmentRole, TableActorLiveSegmentSnapshot,
    TableActorLiveSegmentsPublication, TableActorLiveSegmentsPublishRequest,
    TableActorPersistRequest, TableActorRegistry, TableActorRegistryConfig,
    TableActorRegistryHandle, TableActorSnapshot, TableActorSnapshotRequest,
    TablePersistenceStatus,
};
pub use memory::{
    MemoryIngestCatalog, MemoryIngestConfig, MemoryIngestHandle, MemoryIngestRegistration,
    MemoryIngestRuntime, MemoryIngestShutdownOutcome, MemoryIngestStatus,
    DEFAULT_MEMORY_INGEST_MAX_ACTIVE_TABLES, DEFAULT_MEMORY_INGEST_OPERATION_TIMEOUT,
    MAX_MEMORY_INGEST_ACTIVE_TABLES,
};
pub use registry::{TableRuntime, TableRuntimeBootstrap, TableRuntimeRegistry};
