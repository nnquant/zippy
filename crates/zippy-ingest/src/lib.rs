//! Server-owned table runtimes for native ingest and persisted/live coordination.
//!
//! This crate deliberately does not depend on the legacy master, gateway, or
//! stream materializer. Catalog recovery creates immutable bootstrap records;
//! later actor and persistence layers mutate table state through one owner.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod actor;
mod checkpoint;
mod materializer;
mod native;
mod registry;
mod wal;

pub use actor::{
    TableActorAdmissionControl, TableActorChanges, TableActorFailureMonitor, TableActorHandle,
    TableActorLiveSegmentPublication, TableActorLiveSegmentRole, TableActorLiveSegmentSnapshot,
    TableActorLiveSegmentsPublication, TableActorLiveSegmentsPublishRequest,
    TableActorPersistRequest, TableActorRegistrar, TableActorRegistry, TableActorRegistryConfig,
    TableActorRegistryHandle, TableActorRetirement, TableActorSnapshot, TableActorSnapshotRequest,
    TableActorWriterEpochUpdateRequest, TablePersistenceStatus,
};
pub use materializer::{
    NativeTableMaterializerConfig, NativeTableMaterializerSink,
    DEFAULT_NATIVE_TABLE_MAX_LIVE_SEGMENTS, DEFAULT_NATIVE_TABLE_ROW_CAPACITY,
    MAX_NATIVE_TABLE_LIVE_SEGMENTS,
};
pub use native::{
    NativeIngestApplyContext, NativeIngestConfig, NativeIngestHandle, NativeIngestRuntime,
    NativeIngestShutdownOutcome, NativeIngestSink, NativeIngestSinkError, NativeIngestStatus,
    NativeIngestTableBootstrap, NativePersistedState, NativeTableCheckpointGuard,
    NativeTableRegistration, NativeTableTransitionGuard,
    DEFAULT_NATIVE_INGEST_MAX_ACTIVE_OPERATIONS, DEFAULT_NATIVE_INGEST_MAX_PENDING_DURABLE_ACKS,
    DEFAULT_NATIVE_INGEST_MAX_RECEIPTS_PER_EPOCH, MAX_NATIVE_INGEST_ACTIVE_OPERATIONS,
    MAX_NATIVE_INGEST_PENDING_DURABLE_ACKS, MAX_NATIVE_INGEST_RECEIPTS_PER_EPOCH,
};
pub use registry::{TableRuntime, TableRuntimeBootstrap, TableRuntimeRegistry};
pub use wal::{
    recover_table_wal, ArrowIpcMutationFingerprintComputer, IngestWalConfig, IngestWalIdentity,
    IngestWalRecovery, IngestWalWriter, RecoveredWalRecord, WalAppendPosition, WalCommitReceipt,
    CURRENT_INGEST_WAL_FORMAT_VERSION,
};
