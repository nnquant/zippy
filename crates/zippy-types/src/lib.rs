//! Stable, protocol-neutral contracts for the Zippy single-node server.
//!
//! This crate contains identifiers, data transfer objects, error codes, and
//! service traits shared by the catalog, storage, ingest, and query layers. It
//! deliberately does not depend on Tokio, SQLite, or an execution engine.
//! In-process hot paths exchange the workspace Arrow `RecordBatch` type; IPC
//! remains a transport concern, while implementations choose their own runtime
//! and storage libraries.
//!
//! The crate is additive during the server refactor. Existing runtime callers
//! are migrated through adapters in later phases, so introducing these types
//! does not change current behavior.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod catalog;
pub mod error;
pub mod id;
pub mod ingest;
pub mod offset;
pub mod query;
pub mod resource;
pub mod runtime;
pub mod schema_contract;
pub mod schema_hash;
pub mod schema_memory;
pub mod storage;
pub mod subscription;
pub mod table;

use std::future::Future;
use std::pin::Pin;

pub use catalog::{
    Catalog, CatalogIdempotencyRead, CatalogPersistence, CatalogPublicationControl,
    CatalogPublicationExclusion, CatalogPublicationPermit, CatalogRead, CatalogWrite,
};
pub use error::{ErrorCode, Result, ServerError, ServerErrorDetails};
pub use id::{
    ConnectionId, ConsumerGroupId, DurableConsumerId, EngineId, FileId, JobId, LeaseId,
    NamespaceId, ProcessId, ProducerId, QueryId, RequestId, SessionId, SourceId, TableId,
};
pub use ingest::IngestService;
pub use offset::{
    CommittedOffset, ConsumerOffsetAuthority, LogOffset, OffsetRange, OffsetWatermark,
    SequenceWatermark,
};
pub use query::QueryEngine;
pub use resource::ResourceLimit;
pub use runtime::RuntimeRegistry;
pub use storage::StorageEngine;
pub use subscription::SubscriptionService;
pub use table::TableKind;

/// An object-safe asynchronous return value used by service traits.
///
/// Service implementations may create this future with `Box::pin(async move
/// { ... })`. Keeping the alias in this crate avoids coupling contracts to a
/// specific async runtime or proc-macro implementation.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
