//! Server-owned table runtimes for native ingest and persisted/live coordination.
//!
//! This crate deliberately does not depend on the legacy master, gateway, or
//! stream materializer. Catalog recovery creates immutable bootstrap records;
//! later actor and persistence layers mutate table state through one owner.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod actor;
mod materializer;
mod memory;
mod native;
mod registry;
mod wal;

pub use actor::{
    TableActorAdmissionControl, TableActorFailureMonitor, TableActorHandle,
    TableActorLiveSegmentPublication, TableActorLiveSegmentRole, TableActorLiveSegmentSnapshot,
    TableActorLiveSegmentsPublication, TableActorLiveSegmentsPublishRequest,
    TableActorPersistRequest, TableActorRegistry, TableActorRegistryConfig,
    TableActorRegistryHandle, TableActorSnapshot, TableActorSnapshotRequest,
    TableActorWriterEpochUpdateRequest, TablePersistenceStatus,
};
pub use materializer::{
    NativeAppendMaterializerConfig, NativeAppendMaterializerSink,
    DEFAULT_NATIVE_APPEND_MAX_LIVE_SEGMENTS, DEFAULT_NATIVE_APPEND_ROW_CAPACITY,
    MAX_NATIVE_APPEND_LIVE_SEGMENTS,
};
pub use memory::{
    MemoryIngestCatalog, MemoryIngestConfig, MemoryIngestHandle, MemoryIngestRegistration,
    MemoryIngestRuntime, MemoryIngestShutdownOutcome, MemoryIngestStatus,
    DEFAULT_MEMORY_INGEST_MAX_ACTIVE_TABLES, DEFAULT_MEMORY_INGEST_OPERATION_TIMEOUT,
    MAX_MEMORY_INGEST_ACTIVE_TABLES,
};
pub use native::{
    NativeIngestApplyContext, NativeIngestConfig, NativeIngestHandle, NativeIngestRuntime,
    NativeIngestShutdownOutcome, NativeIngestSink, NativeIngestSinkError, NativeIngestStatus,
    NativeIngestTableBootstrap, DEFAULT_NATIVE_INGEST_MAX_ACTIVE_OPERATIONS,
    DEFAULT_NATIVE_INGEST_MAX_PENDING_DURABLE_ACKS, DEFAULT_NATIVE_INGEST_MAX_RECEIPTS_PER_EPOCH,
    MAX_NATIVE_INGEST_ACTIVE_OPERATIONS, MAX_NATIVE_INGEST_PENDING_DURABLE_ACKS,
    MAX_NATIVE_INGEST_RECEIPTS_PER_EPOCH,
};
pub use registry::{TableRuntime, TableRuntimeBootstrap, TableRuntimeRegistry};
pub use wal::{
    recover_table_wal, ArrowIpcMutationFingerprintComputer, IngestWalConfig, IngestWalIdentity,
    IngestWalRecovery, IngestWalWriter, RecoveredWalRecord, WalAppendPosition, WalCommitReceipt,
    CURRENT_INGEST_WAL_FORMAT_VERSION,
};
