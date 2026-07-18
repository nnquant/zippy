//! Query planning, opaque ownership, Arrow streaming, status, and cancellation.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::table::TableRef;
use crate::{BoxFuture, QueryId, ResourceLimit, Result, SessionId, TableId};

/// The current Zippy logical-plan envelope format.
pub const CURRENT_LOGICAL_PLAN_FORMAT_VERSION: u32 = 1;

/// A versioned, non-empty Zippy logical-plan payload.
///
/// Format version 1 is reserved for the native Zippy protobuf plan. Migration
/// adapters may allocate a distinct version for a compatibility codec; an
/// engine must reject every version it does not explicitly implement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LogicalPlanEnvelope {
    format_version: u32,
    payload: Vec<u8>,
}

impl LogicalPlanEnvelope {
    /// Creates a versioned plan envelope.
    pub fn new(
        format_version: u32,
        payload: Vec<u8>,
    ) -> std::result::Result<Self, InvalidLogicalPlanEnvelope> {
        if format_version == 0 {
            return Err(InvalidLogicalPlanEnvelope::ZeroFormatVersion);
        }
        if payload.is_empty() {
            return Err(InvalidLogicalPlanEnvelope::EmptyPayload);
        }
        Ok(Self {
            format_version,
            payload,
        })
    }

    /// Creates an envelope using the current format version.
    pub fn current(payload: Vec<u8>) -> std::result::Result<Self, InvalidLogicalPlanEnvelope> {
        Self::new(CURRENT_LOGICAL_PLAN_FORMAT_VERSION, payload)
    }

    /// Returns the fixed-width plan format version.
    #[must_use]
    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Borrows the version-specific encoded payload.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl<'de> Deserialize<'de> for LogicalPlanEnvelope {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawEnvelope {
            format_version: u32,
            payload: Vec<u8>,
        }

        let raw = RawEnvelope::deserialize(deserializer)?;
        Self::new(raw.format_version, raw.payload).map_err(serde::de::Error::custom)
    }
}

/// An invalid logical-plan envelope invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidLogicalPlanEnvelope {
    /// Format version zero is reserved and invalid.
    #[error("logical plan format version must not be zero")]
    ZeroFormatVersion,
    /// A typed logical-plan payload must not be empty.
    #[error("logical plan payload must not be empty")]
    EmptyPayload,
}

/// A server-owned logical query input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum QueryInput {
    /// SQL text parsed, bound, and optimized by the server.
    Sql(String),
    /// A versioned Zippy logical-plan envelope.
    LogicalPlan(LogicalPlanEnvelope),
}

/// Admission priority for a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryPriority {
    /// Latency-sensitive interactive work.
    Interactive,
    /// Ordinary application and analytical work.
    Normal,
    /// Background work that must not exhaust interactive resources.
    Maintenance,
}

/// The lifecycle state of a submitted query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryState {
    /// The query was accepted but is not yet in an admission queue.
    Submitted,
    /// The query is waiting for planning resources.
    Queued,
    /// The query is resolving snapshots and building a physical plan.
    Planning,
    /// A physical plan and its leases are waiting for execution.
    Planned,
    /// Operators are executing or results are being delivered.
    Running,
    /// The query and result delivery completed successfully.
    Finished,
    /// Planning or execution failed.
    Failed,
    /// A client, plan drop, stream drop, or operator cancelled the query.
    Cancelled,
    /// The query or an unexecuted plan exceeded its deadline or TTL.
    TimedOut,
}

/// One process-local query incarnation identified by caller ID and server generation.
///
/// `QueryId` is caller-assigned and may be reused after terminal retention expires. The nonzero
/// generation prevents delayed status or cancellation requests from targeting a newer incarnation
/// that happens to reuse the same caller ID. This value is an ABA fence, not authorization; every
/// control operation must also prove an engine-issued owner or administrator capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct QueryInstanceId {
    query_id: QueryId,
    generation: NonZeroU64,
}

impl QueryInstanceId {
    /// Creates an exact query-instance identity.
    #[must_use]
    pub const fn new(query_id: QueryId, generation: NonZeroU64) -> Self {
        Self {
            query_id,
            generation,
        }
    }

    /// Returns the caller-assigned query identifier.
    #[must_use]
    pub const fn query_id(self) -> QueryId {
        self.query_id
    }

    /// Returns the server-assigned process-local generation.
    #[must_use]
    pub const fn generation(self) -> NonZeroU64 {
        self.generation
    }
}

impl QueryState {
    /// Returns true when no further state transition is allowed.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Finished | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }

    /// Returns whether the query state machine permits a transition.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Submitted,
                Self::Queued | Self::Planning | Self::Cancelled
            ) | (
                Self::Queued,
                Self::Planning | Self::Cancelled | Self::TimedOut
            ) | (
                Self::Planning,
                Self::Planned | Self::Failed | Self::Cancelled | Self::TimedOut
            ) | (
                Self::Planned,
                Self::Running | Self::Failed | Self::Cancelled | Self::TimedOut
            ) | (
                Self::Running,
                Self::Finished | Self::Failed | Self::Cancelled | Self::TimedOut
            )
        )
    }
}

/// A query submission with explicit resource and deadline boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryRequest {
    /// The caller-assigned identifier used for status and cancellation.
    pub query_id: QueryId,
    /// The client session that owns the query.
    pub session_id: SessionId,
    /// The SQL or typed plan executed by the server.
    pub input: QueryInput,
    /// Optional explicit table references used by typed plan inputs.
    pub tables: Vec<TableRef>,
    /// The resource admission class.
    pub priority: QueryPriority,
    /// The absolute Unix-nanosecond execution deadline, when specified.
    pub deadline_ns: Option<u64>,
    /// The server-default, disabled, or explicit nonzero memory byte budget.
    pub memory_limit_bytes: ResourceLimit,
    /// The server-default, disabled, or explicit nonzero spill byte budget.
    pub spill_limit_bytes: ResourceLimit,
    /// The preferred maximum number of rows per streamed result batch.
    pub result_batch_rows: Option<NonZeroU32>,
}

/// Immutable public metadata for an implementation-owned planned query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPlanInfo {
    /// The planned query identity.
    pub query_id: QueryId,
    /// The catalog revision used to bind the query.
    pub catalog_revision: u64,
    /// The persisted and schema versions selected for each table.
    pub snapshots: Vec<QueryTableVersion>,
    /// The time planning completed as Unix nanoseconds in UTC.
    pub planned_at_ns: u64,
    /// The time the unexecuted plan expires as Unix nanoseconds in UTC.
    pub expires_at_ns: u64,
}

/// Exact table versions bound during query planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryTableVersion {
    /// The immutable table identity.
    pub table_id: TableId,
    /// The exact persisted manifest version.
    pub snapshot_version: u64,
    /// The exact schema version used for planning and decoding.
    pub schema_version: u64,
}

/// An engine-issued, non-serializable capability for one client session.
///
/// Engines first downcast [`QueryOwnerCapability::as_any`] to their private
/// capability type, then validate a private engine-instance nonce stored in
/// that type. Downcast alone is insufficient because two engine instances may
/// use the same concrete implementation. Possessing a `SessionId` alone is not
/// authorization to observe or cancel its queries.
pub trait QueryOwnerCapability: Any + Send + Sync {
    /// Returns the session identity represented by this opaque capability.
    fn session_id(&self) -> SessionId;

    /// Exposes a borrow solely for concrete engine-origin validation.
    fn as_any(&self) -> &dyn Any;
}

/// A type-erased query-owner capability.
pub type OpaqueQueryOwnerCapability = Box<dyn QueryOwnerCapability>;

/// An opaque physical-plan handle created and consumed by one query engine.
///
/// Clients cannot serialize this handle. `execute` must reject a handle from a
/// different engine instance. The engine consumes `into_any`, downcasts to its
/// private token type, and then validates that type's private instance nonce;
/// the concrete type alone cannot distinguish two instances. Dropping an
/// unexecuted handle must synchronously enqueue cancellation and lease release;
/// a background TTL sweeper is still required so leaked or cyclic
/// implementation state cannot retain snapshots.
pub trait QueryExecutionToken: Any + Send {
    /// Returns immutable audit metadata without exposing the physical plan.
    fn info(&self) -> &QueryPlanInfo;

    /// Returns the owner session bound into this one-time token.
    fn owner_session_id(&self) -> SessionId;

    /// Returns the exact query incarnation controlled by this token.
    fn instance_id(&self) -> QueryInstanceId;

    /// Consumes the token into the engine's concrete origin-check boundary.
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send>;
}

/// A type-erased, one-time implementation-owned execution token.
///
/// ```compile_fail
/// fn duplicate(token: zippy_types::query::OpaqueQueryExecutionToken) {
///     let _second_execution = token.clone();
/// }
/// ```
pub type OpaqueQueryExecutionToken = Box<dyn QueryExecutionToken>;

/// One non-cloneable Arrow result batch and its implementation-owned memory lease.
///
/// Native engines place their exact result-memory permit in the opaque owner. Transport code
/// must keep this value alive while reading or encoding [`Self::record_batch`]. Dropping the
/// value releases both the Arrow batch and its accounting owner together.
///
/// ```compile_fail
/// fn duplicate(batch: zippy_types::query::QueryResultBatch) {
///     let _copy = batch.clone();
/// }
/// ```
pub struct QueryResultBatch {
    batch: RecordBatch,
    owner: Option<Box<dyn Any + Send>>,
}

impl QueryResultBatch {
    /// Wraps a batch produced by an engine that has no separate accounting owner.
    #[must_use]
    pub fn unaccounted(batch: RecordBatch) -> Self {
        Self { batch, owner: None }
    }

    /// Wraps a batch together with an opaque owner released after the batch itself.
    ///
    /// Engine implementations use this constructor to transfer a permit across an object-safe
    /// query boundary without exposing or making that permit cloneable.
    #[must_use]
    pub fn with_owner<T>(batch: RecordBatch, owner: T) -> Self
    where
        T: Any + Send,
    {
        Self {
            batch,
            owner: Some(Box::new(owner)),
        }
    }

    /// Borrows the Arrow batch while retaining its opaque owner.
    #[must_use]
    pub const fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Returns whether this batch carries a separate implementation-owned lease.
    #[must_use]
    pub const fn has_owner(&self) -> bool {
        self.owner.is_some()
    }
}

impl fmt::Debug for QueryResultBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryResultBatch")
            .field("rows", &self.batch.num_rows())
            .field("columns", &self.batch.num_columns())
            .field("has_owner", &self.has_owner())
            .finish()
    }
}

/// A type-safe asynchronous stream of Arrow record batches.
///
/// Implementations return non-cloneable permit-owned Arrow batches. Reaching EOF, observing an
/// execution error, or dropping the stream must cancel remaining operators and release query
/// snapshot leases exactly once.
pub trait RecordBatchStream: Send {
    /// Returns the exact query whose lease ownership was transferred to this stream.
    fn instance_id(&self) -> QueryInstanceId;

    /// Returns the caller-assigned query identifier.
    fn query_id(&self) -> QueryId {
        self.instance_id().query_id()
    }

    /// Returns the stable schema shared by every batch in the stream.
    fn schema(&self) -> SchemaRef;

    /// Returns a bounded point-in-time query profile using stable metric names.
    ///
    /// Implementations that do not expose native plan or operator metrics return an empty map.
    /// Transport code should call this only for low-frequency profile or terminal frames.
    fn metrics(&self) -> BTreeMap<String, u64> {
        BTreeMap::new()
    }

    /// Polls the next batch without allocating a future per result batch.
    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<QueryResultBatch>>>;
}

/// A sendable, type-erased Arrow record batch stream.
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream>>;

/// A point-in-time status and profile summary for one query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryStatus {
    /// The query identifier.
    pub query_id: QueryId,
    /// The server-assigned generation paired with `query_id` for exact control operations.
    pub generation: NonZeroU64,
    /// The owning client session.
    pub session_id: SessionId,
    /// The current query state.
    pub state: QueryState,
    /// The submission time as Unix nanoseconds in UTC.
    pub submitted_at_ns: u64,
    /// The execution start time as Unix nanoseconds in UTC.
    pub started_at_ns: Option<u64>,
    /// The terminal completion time as Unix nanoseconds in UTC.
    pub finished_at_ns: Option<u64>,
    /// The catalog revision used to bind the query, when planned.
    pub catalog_revision: Option<u64>,
    /// The persisted snapshot version selected for each table.
    pub snapshots: Vec<QueryTableVersion>,
    /// Monotonic counters and byte/time measurements keyed by stable names.
    pub metrics: BTreeMap<String, u64>,
    /// A structured failure, cancellation, or timeout summary.
    pub error: Option<crate::ServerError>,
}

impl QueryStatus {
    /// Returns the exact query incarnation represented by this status.
    #[must_use]
    pub const fn instance_id(&self) -> QueryInstanceId {
        QueryInstanceId::new(self.query_id, self.generation)
    }
}

/// The sole authority for query lifecycle, plans, execution, and cancellation.
///
/// Query state must not be duplicated in `RuntimeRegistry`. `plan` registers
/// TTL cleanup before returning its handle. `execute` consumes the handle and
/// transfers snapshot-lease ownership to the result stream. `cancel` applies
/// to planning, unexecuted plans, and running streams and is idempotent.
pub trait QueryEngine: Send + Sync {
    /// Resolves snapshots and returns a one-time engine-owned execution token.
    /// The capability session must equal `request.session_id`.
    fn plan<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        request: QueryRequest,
    ) -> BoxFuture<'a, Result<OpaqueQueryExecutionToken>>;

    /// Consumes a token exactly once and rejects foreign concrete types or nonce.
    fn execute(
        &self,
        token: OpaqueQueryExecutionToken,
    ) -> BoxFuture<'_, Result<SendableRecordBatchStream>>;

    /// Cancels one exact lifecycle incarnation after proving owner authority.
    fn cancel<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        instance_id: QueryInstanceId,
    ) -> BoxFuture<'a, Result<()>>;

    /// Returns status for one exact lifecycle incarnation after proving owner authority.
    fn status<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        instance_id: QueryInstanceId,
    ) -> BoxFuture<'a, Result<QueryStatus>>;
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;

    use super::{
        LogicalPlanEnvelope, OpaqueQueryExecutionToken, OpaqueQueryOwnerCapability, QueryEngine,
        QueryExecutionToken, QueryInput, QueryInstanceId, QueryOwnerCapability, QueryPlanInfo,
        QueryPriority, QueryRequest, QueryResultBatch, QueryState, QueryTableVersion,
        SendableRecordBatchStream,
    };
    use crate::{QueryId, ResourceLimit, SessionId};

    struct TestOwner {
        session_id: SessionId,
        instance_nonce: u128,
    }

    impl QueryOwnerCapability for TestOwner {
        fn session_id(&self) -> SessionId {
            self.session_id
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct ForeignOwner(SessionId);

    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl QueryOwnerCapability for ForeignOwner {
        fn session_id(&self) -> SessionId {
            self.0
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct TestToken {
        info: QueryPlanInfo,
        owner_session_id: SessionId,
        instance_nonce: u128,
        generation: NonZeroU64,
    }

    impl QueryExecutionToken for TestToken {
        fn info(&self) -> &QueryPlanInfo {
            &self.info
        }

        fn owner_session_id(&self) -> SessionId {
            self.owner_session_id
        }

        fn instance_id(&self) -> QueryInstanceId {
            QueryInstanceId::new(self.info.query_id, self.generation)
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }

    struct ForeignToken(TestToken);

    impl QueryExecutionToken for ForeignToken {
        fn info(&self) -> &QueryPlanInfo {
            &self.0.info
        }

        fn owner_session_id(&self) -> SessionId {
            self.0.owner_session_id
        }

        fn instance_id(&self) -> QueryInstanceId {
            self.0.instance_id()
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }

    fn test_plan_info() -> QueryPlanInfo {
        QueryPlanInfo {
            query_id: QueryId::from_u128(2).unwrap(),
            catalog_revision: 3,
            snapshots: Vec::new(),
            planned_at_ns: 4,
            expires_at_ns: 5,
        }
    }

    struct TestEngineOrigin {
        instance_nonce: u128,
    }

    impl TestEngineOrigin {
        fn accepts_owner(&self, owner: &dyn QueryOwnerCapability) -> bool {
            owner
                .as_any()
                .downcast_ref::<TestOwner>()
                .is_some_and(|owner| owner.instance_nonce == self.instance_nonce)
        }

        fn accepts_token(&self, token: OpaqueQueryExecutionToken) -> bool {
            token
                .into_any()
                .downcast::<TestToken>()
                .is_ok_and(|token| token.instance_nonce == self.instance_nonce)
        }
    }

    fn assert_object_safe(_: Option<&dyn QueryEngine>) {}
    fn assert_stream_is_object_safe(_: Option<SendableRecordBatchStream>) {}

    #[test]
    fn query_engine_and_stream_are_object_safe() {
        assert_object_safe(None);
        assert_stream_is_object_safe(None);
        let capability: OpaqueQueryOwnerCapability = Box::new(TestOwner {
            session_id: SessionId::from_u128(1).unwrap(),
            instance_nonce: 7,
        });
        assert_eq!(capability.session_id(), SessionId::from_u128(1).unwrap());
    }

    #[test]
    fn result_batch_releases_opaque_owner_with_the_batch() {
        let drops = Arc::new(AtomicUsize::new(0));
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let owned = QueryResultBatch::with_owner(batch, DropProbe(Arc::clone(&drops)));
        assert!(owned.has_owner());
        assert_eq!(owned.record_batch().num_rows(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(owned);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn engine_origin_checks_reject_foreign_types_and_instance_nonces() {
        let session_id = SessionId::from_u128(1).unwrap();
        let engine = TestEngineOrigin { instance_nonce: 7 };
        let local_owner = TestOwner {
            session_id,
            instance_nonce: 7,
        };
        let wrong_instance_owner = TestOwner {
            session_id,
            instance_nonce: 8,
        };
        let foreign_owner = ForeignOwner(session_id);

        assert!(engine.accepts_owner(&local_owner));
        assert!(!engine.accepts_owner(&wrong_instance_owner));
        assert!(!engine.accepts_owner(&foreign_owner));

        let token = |instance_nonce| TestToken {
            info: test_plan_info(),
            owner_session_id: session_id,
            instance_nonce,
            generation: NonZeroU64::new(1).unwrap(),
        };
        assert!(engine.accepts_token(Box::new(token(7))));
        assert!(!engine.accepts_token(Box::new(token(8))));
        assert!(!engine.accepts_token(Box::new(ForeignToken(token(7)))));
    }

    #[test]
    fn query_state_machine_has_explicit_plan_ownership_phase() {
        assert!(QueryState::Planning.can_transition_to(QueryState::Planned));
        assert!(QueryState::Planned.can_transition_to(QueryState::Running));
        assert!(QueryState::Planned.can_transition_to(QueryState::Failed));
        assert!(QueryState::Running.can_transition_to(QueryState::Finished));
        assert!(!QueryState::Finished.can_transition_to(QueryState::Running));
        assert!(QueryState::Cancelled.is_terminal());
    }

    #[test]
    fn query_instance_identity_rejects_zero_generation() {
        let instance =
            QueryInstanceId::new(QueryId::from_u128(9).unwrap(), NonZeroU64::new(3).unwrap());

        assert_eq!(instance.query_id(), QueryId::from_u128(9).unwrap());
        assert_eq!(instance.generation().get(), 3);
        assert_eq!(
            serde_json::to_string(&instance).unwrap(),
            r#"{"query_id":"00000000-0000-0000-0000-000000000009","generation":3}"#
        );
        assert!(serde_json::from_str::<QueryInstanceId>(
            r#"{"query_id":"00000000-0000-0000-0000-000000000009","generation":0}"#,
        )
        .is_err());
    }

    #[test]
    fn logical_plan_envelope_has_a_golden_versioned_form() {
        let envelope = LogicalPlanEnvelope::current(vec![1, 2, 3]).unwrap();

        assert_eq!(
            serde_json::to_string(&envelope).unwrap(),
            r#"{"format_version":1,"payload":[1,2,3]}"#
        );
        assert!(serde_json::from_str::<LogicalPlanEnvelope>(
            r#"{"format_version":0,"payload":[1]}"#
        )
        .is_err());
        assert!(LogicalPlanEnvelope::current(Vec::new()).is_err());
    }

    #[test]
    fn planned_table_version_cannot_omit_bound_schema() {
        let version = QueryTableVersion {
            table_id: crate::TableId::from_u128(2).unwrap(),
            snapshot_version: 3,
            schema_version: 4,
        };

        assert_eq!(serde_json::to_value(version).unwrap()["schema_version"], 4);
        assert!(serde_json::from_str::<QueryTableVersion>(
            r#"{"table_id":"00000000-0000-0000-0000-000000000002","snapshot_version":3}"#
        )
        .is_err());
    }

    #[test]
    fn query_request_serializes_explicit_resource_policies() {
        let request = QueryRequest {
            query_id: QueryId::from_u128(1).unwrap(),
            session_id: SessionId::from_u128(2).unwrap(),
            input: QueryInput::Sql("select 1".to_owned()),
            tables: Vec::new(),
            priority: QueryPriority::Interactive,
            deadline_ns: None,
            memory_limit_bytes: ResourceLimit::ServerDefault,
            spill_limit_bytes: ResourceLimit::Disabled,
            result_batch_rows: None,
        };
        let value = serde_json::to_value(request).unwrap();

        assert_eq!(value["memory_limit_bytes"]["type"], "server_default");
        assert_eq!(value["spill_limit_bytes"]["type"], "disabled");
        assert_eq!(
            serde_json::to_value(ResourceLimit::Bytes(NonZeroU64::new(64).unwrap())).unwrap(),
            serde_json::json!({"type": "bytes", "value": 64})
        );
    }
}
