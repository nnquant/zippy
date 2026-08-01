//! Bounded Arrow Flight query service and transport-neutral frontend contract.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU64;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, CancelFlightInfoRequest, CancelFlightInfoResult, CancelStatus, Criteria,
    Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use prost::Message;
use prost_types::Timestamp;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};
use zippy_types::query::{
    QueryInstanceId, QueryPlanInfo, QueryResultBatch, SendableRecordBatchStream,
};
use zippy_types::{
    BoxFuture, ErrorCode, QueryId, Result, ServerError, ServerErrorDetails, SessionId,
};

use crate::auth::{
    AuthenticatedFlightSession, FlightBearerAuthConfig, FlightSessionRole, SessionAuthority,
};
use crate::error::server_error_to_status;
use crate::ingest::{
    decode_put_descriptor, validate_put_batch_result, FlightPutBatch, FlightPutCommand,
    FlightPutLimits,
};
use crate::query::{
    decode_query_descriptor, decode_query_ticket, encode_query_ticket, query_info,
    FlightQueryRequest, OPAQUE_TICKET_ID_BYTES,
};
use crate::sql::{
    decode_flight_sql_metadata_request, decode_metadata_ticket, encode_metadata_ticket,
    FlightSqlMetadataLimits, FlightSqlMetadataRequest, PreparedFlightSqlMetadata,
};
use crate::wire::{
    CapabilitiesRequest, CapabilitiesResponse, MetadataInfo, MetadataTicket, QueryTicket,
    SessionOpenRequest, SessionOpenResponse, SessionRole,
};
use crate::{CAPABILITIES_ACTION, CURRENT_PROTOCOL_VERSION, SESSION_METADATA_KEY};

const MIN_FLIGHT_MESSAGE_BYTES: usize = 64 * 1024;
const MAX_FLIGHT_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const ENCODED_MESSAGE_OVERHEAD_BYTES: usize = 256;
const QUERY_CAPABILITY: &str = "query.get_flight_info_do_get.v1";
const STRUCTURED_ERROR_CAPABILITY: &str = "error.details.v1";
const SINGLE_USE_TICKET_CAPABILITY: &str = "query.single_use_ticket.v1";
const SERVER_SESSION_CAPABILITY: &str = "session.server_authority.v1";
const BEARER_HANDSHAKE_CAPABILITY: &str = "auth.bearer_handshake.v1";
const SESSION_ROLE_CAPABILITY: &str = "auth.session_roles.v1";
const QUERY_CANCEL_CAPABILITY: &str = "query.cancel_flight_info.v1";
const FLIGHT_SQL_METADATA_CAPABILITY: &str = "flight_sql.metadata.v1";
const FLIGHT_MEMORY_INGEST_CAPABILITY: &str = "ingest.append.memory.v1";
const FLIGHT_WAL_INGEST_CAPABILITY: &str = "ingest.append.wal.v1";
const CANCEL_FLIGHT_INFO_ACTION: &str = "CancelFlightInfo";
const MAX_HANDSHAKE_PAYLOAD_BYTES: usize = 4 * 1024;

/// A prepared query whose exact snapshot and output schema are already fixed.
///
/// Implementations must be non-cloneable. Dropping an unexecuted value must
/// cancel the plan and release every snapshot and resource lease it owns.
pub trait PreparedFlightQuery: Send {
    /// Returns immutable planning metadata for the fixed snapshot.
    fn info(&self) -> &QueryPlanInfo;

    /// Returns the exact process-local query incarnation.
    fn instance_id(&self) -> QueryInstanceId;

    /// Returns the output Arrow schema fixed during physical preparation.
    fn schema(&self) -> SchemaRef;

    /// Consumes this one-use handle and starts the result stream.
    fn execute(self: Box<Self>) -> Result<SendableRecordBatchStream>;
}

/// Type-erased non-cloneable prepared query owned by the Flight ticket registry.
pub type OpaquePreparedFlightQuery = Box<dyn PreparedFlightQuery>;

/// One producer-fenced writer owned by exactly one Flight `DoPut` stream.
pub trait FlightIngestWriter: Send {
    /// Applies one bounded batch and returns its authoritative wire receipt.
    fn write_batch(
        &mut self,
        batch: FlightPutBatch,
    ) -> BoxFuture<'_, Result<crate::wire::PutBatchResult>>;
}

/// Type-erased stream-scoped Flight ingest writer.
pub type OpaqueFlightIngestWriter = Box<dyn FlightIngestWriter>;

/// Query planner boundary consumed by the protocol service.
pub trait FlightQueryFrontend: Send + Sync + 'static {
    /// Validates, binds, and physically prepares one query against an exact
    /// snapshot without producing result batches yet.
    fn prepare(
        &self,
        request: FlightQueryRequest,
    ) -> BoxFuture<'_, Result<OpaquePreparedFlightQuery>>;

    /// Cancels one exact query after proving its owning session.
    fn cancel_owned(
        &self,
        _session_id: SessionId,
        instance_id: QueryInstanceId,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                "Flight query cancellation is not configured",
            )
            .with_query_id(instance_id.query_id())
            .with_retryable(false))
        })
    }

    /// Cancels one exact query with separately authenticated administrator authority.
    fn cancel_administrator(&self, instance_id: QueryInstanceId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                "Flight administrator cancellation is not configured",
            )
            .with_query_id(instance_id.query_id())
            .with_retryable(false))
        })
    }

    /// Materializes one bounded, revision-fixed Flight SQL metadata result.
    fn prepare_metadata(
        &self,
        _request: FlightSqlMetadataRequest,
        _limits: FlightSqlMetadataLimits,
    ) -> BoxFuture<'_, Result<PreparedFlightSqlMetadata>> {
        Box::pin(async {
            Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                "Flight SQL metadata is not configured",
            )
            .with_retryable(false))
        })
    }

    /// Opens one stream-scoped producer-fenced append writer.
    fn open_ingest_writer(
        &self,
        command: Arc<FlightPutCommand>,
    ) -> BoxFuture<'_, Result<OpaqueFlightIngestWriter>> {
        Box::pin(async move {
            Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                "Flight ingest is not configured",
            )
            .with_request_id(command.put_id)
            .with_retryable(false))
        })
    }
}

/// Resource and compatibility limits for one Flight query service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightServiceConfig {
    server_version: String,
    max_message_bytes: usize,
    max_pending_tickets: usize,
    transport_buffer_bytes: usize,
    bearer_auth: Option<FlightBearerAuthConfig>,
    query_control_enabled: bool,
    metadata: Option<FlightSqlMetadataLimits>,
    ingest: Option<FlightPutLimits>,
}

impl FlightServiceConfig {
    /// Creates validated Flight service limits.
    pub fn try_new(
        server_version: impl Into<String>,
        max_message_bytes: usize,
        max_pending_tickets: usize,
        transport_buffer_bytes: usize,
    ) -> Result<Self> {
        let server_version = server_version.into();
        if server_version.trim().is_empty() {
            return Err(invalid("Flight server version must not be empty"));
        }
        if !(MIN_FLIGHT_MESSAGE_BYTES..=MAX_FLIGHT_MESSAGE_BYTES).contains(&max_message_bytes) {
            return Err(invalid(
                "Flight message bytes are outside the supported range",
            ));
        }
        if max_pending_tickets == 0 {
            return Err(invalid("Flight pending ticket capacity must be positive"));
        }
        if transport_buffer_bytes < max_message_bytes {
            return Err(invalid(
                "Flight transport buffer must hold at least one maximum message",
            ));
        }
        if transport_buffer_bytes > u32::MAX as usize {
            return Err(invalid("Flight transport buffer exceeds semaphore limits"));
        }
        Ok(Self {
            server_version,
            max_message_bytes,
            max_pending_tickets,
            transport_buffer_bytes,
            bearer_auth: None,
            query_control_enabled: false,
            metadata: None,
            ingest: None,
        })
    }

    /// Enables bearer bootstrap authentication and server-issued sessions.
    #[must_use]
    pub fn with_bearer_auth(mut self, auth: FlightBearerAuthConfig) -> Self {
        self.bearer_auth = Some(auth);
        self
    }

    /// Enables standard owner-bound and administrator `CancelFlightInfo` actions.
    #[must_use]
    pub fn with_query_control(mut self) -> Self {
        self.query_control_enabled = true;
        self
    }

    /// Enables bounded Flight SQL Catalog metadata commands.
    #[must_use]
    pub fn with_sql_metadata(mut self, metadata: FlightSqlMetadataLimits) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Enables bounded append-only `DoPut` with process-local memory acknowledgements.
    #[must_use]
    pub fn with_ingest(mut self, ingest: FlightPutLimits) -> Self {
        self.ingest = Some(ingest);
        self
    }

    /// Returns the maximum approximate encoded Flight message size.
    #[must_use]
    pub const fn max_message_bytes(&self) -> usize {
        self.max_message_bytes
    }

    /// Returns the maximum number of prepared, unconsumed tickets.
    #[must_use]
    pub const fn max_pending_tickets(&self) -> usize {
        self.max_pending_tickets
    }

    /// Returns the process-local encoded transport byte budget.
    #[must_use]
    pub const fn transport_buffer_bytes(&self) -> usize {
        self.transport_buffer_bytes
    }
}

/// Point-in-time lifecycle counters for one Flight query service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlightServiceStatus {
    /// Whether ticket admission remains open.
    pub admission_open: bool,
    /// Query or metadata requests currently being prepared before ticket publication.
    pub preparing_queries: usize,
    /// Prepared tickets waiting for one `DoGet` consumer.
    pub pending_tickets: usize,
    /// Tickets issued since service construction.
    pub issued_tickets: u64,
    /// Tickets consumed successfully since service construction.
    pub consumed_tickets: u64,
    /// Unconsumed tickets released after expiry.
    pub expired_tickets: u64,
    /// Requests rejected by the bounded ticket registry.
    pub admission_rejections: u64,
    /// Encoded Flight bytes handed to gRPC streams.
    pub encoded_bytes: u64,
    /// Shared retained/encoded transport byte capacity.
    pub transport_buffer_bytes_capacity: usize,
    /// Shared transport bytes currently retained by tickets or in-flight messages.
    pub transport_buffer_bytes_in_use: usize,
    /// Whether query RPCs require a server-issued bearer session.
    pub authentication_required: bool,
    /// Whether new authenticated sessions may still be issued.
    pub session_admission_open: bool,
    /// Server-issued sessions that have not expired or been closed.
    pub active_sessions: usize,
    /// Active sessions issued from the distinct administrator credential.
    pub active_administrator_sessions: usize,
    /// Active sessions issued from the distinct writer credential.
    pub active_writer_sessions: usize,
    /// Server-issued sessions created since service construction.
    pub opened_sessions: u64,
    /// Server-issued sessions removed after their fixed expiry.
    pub expired_sessions: u64,
    /// Bootstrap or session bearer validation failures.
    pub authentication_failures: u64,
    /// Handshakes rejected by the bounded session registry.
    pub session_capacity_rejections: u64,
    /// Pending tickets revoked because their owning session expired.
    pub session_revoked_tickets: u64,
    /// Successful owner-bound query cancellation actions.
    pub owner_cancellations: u64,
    /// Successful administrator query cancellation actions.
    pub administrator_cancellations: u64,
    /// Flight SQL metadata tickets issued since service construction.
    pub metadata_tickets_issued: u64,
    /// Flight SQL metadata tickets consumed since service construction.
    pub metadata_tickets_consumed: u64,
    /// `DoPut` streams admitted since service construction.
    pub puts_started: u64,
    /// `DoPut` streams that reached a clean input EOF.
    pub puts_completed: u64,
    /// `DoPut` streams rejected or failed before reaching a clean input EOF.
    pub put_rejections: u64,
    /// Append batches acknowledged at memory durability.
    pub put_batches: u64,
    /// Rows acknowledged at memory durability.
    pub put_rows: u64,
    /// Decoded Arrow bytes acknowledged at memory durability.
    pub put_bytes: u64,
    /// Currently admitted `DoPut` streams.
    pub active_puts: usize,
}

/// Arrow Flight implementation for bounded, snapshot-fixed Zippy queries.
#[derive(Clone)]
pub struct ZippyFlightService {
    inner: Arc<FlightServiceInner>,
}

struct FlightServiceInner {
    config: FlightServiceConfig,
    frontend: Arc<dyn FlightQueryFrontend>,
    tickets: TicketRegistry,
    sessions: Option<SessionAuthority>,
    transport_budget: Arc<Semaphore>,
    put_admission: Option<Arc<Semaphore>>,
    metrics: FlightMetrics,
}

#[derive(Default)]
struct FlightMetrics {
    issued_tickets: AtomicU64,
    consumed_tickets: AtomicU64,
    expired_tickets: AtomicU64,
    admission_rejections: AtomicU64,
    encoded_bytes: AtomicU64,
    session_revoked_tickets: AtomicU64,
    owner_cancellations: AtomicU64,
    administrator_cancellations: AtomicU64,
    metadata_tickets_issued: AtomicU64,
    metadata_tickets_consumed: AtomicU64,
    puts_started: AtomicU64,
    puts_completed: AtomicU64,
    put_rejections: AtomicU64,
    put_batches: AtomicU64,
    put_rows: AtomicU64,
    put_bytes: AtomicU64,
}

impl ZippyFlightService {
    /// Creates a service with a random boot-scoped server identity.
    pub fn try_new(
        config: FlightServiceConfig,
        frontend: Arc<dyn FlightQueryFrontend>,
    ) -> Result<Self> {
        let mut server_instance_id = [0_u8; OPAQUE_TICKET_ID_BYTES];
        fill_random_nonzero(&mut server_instance_id)?;
        if config.ingest.is_some()
            && !config
                .bearer_auth
                .as_ref()
                .is_some_and(FlightBearerAuthConfig::writer_enabled)
        {
            return Err(invalid(
                "Flight ingest requires a distinct authenticated writer credential",
            ));
        }
        if config
            .ingest
            .is_some_and(|limits| limits.max_batch_bytes() > config.transport_buffer_bytes)
        {
            return Err(invalid(
                "Flight ingest batch bytes must not exceed the shared transport budget",
            ));
        }
        let sessions = config.bearer_auth.clone().map(SessionAuthority::new);
        let put_admission = config
            .ingest
            .map(|limits| Arc::new(Semaphore::new(limits.max_concurrent_puts())));
        Ok(Self {
            inner: Arc::new(FlightServiceInner {
                tickets: TicketRegistry::new(server_instance_id, config.max_pending_tickets),
                transport_budget: Arc::new(Semaphore::new(config.transport_buffer_bytes)),
                put_admission,
                sessions,
                config,
                frontend,
                metrics: FlightMetrics::default(),
            }),
        })
    }

    /// Stops new plans and releases every unconsumed prepared query.
    pub fn close_admission(&self) {
        if let Some(sessions) = &self.inner.sessions {
            let closed = sessions.close();
            let revoked = self.inner.tickets.revoke_sessions(&closed);
            self.inner.metrics.session_revoked_tickets.fetch_add(
                u64::try_from(revoked).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        self.inner.tickets.close();
        self.inner.transport_budget.close();
    }

    /// Releases every prepared query whose one-use ticket has expired.
    #[must_use]
    pub fn sweep_expired(&self) -> usize {
        let now_ns = unix_time_ns();
        let revoked = self.sweep_expired_sessions_at(now_ns);
        let expired = self.inner.tickets.sweep_expired(now_ns);
        self.inner.metrics.expired_tickets.fetch_add(
            u64::try_from(expired).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        expired.saturating_add(revoked)
    }

    /// Returns bounded ticket and transport counters for readiness diagnostics.
    #[must_use]
    pub fn status(&self) -> FlightServiceStatus {
        let registry = self.inner.tickets.status();
        let sessions = self.inner.sessions.as_ref().map(SessionAuthority::status);
        let active_puts = self
            .inner
            .config
            .ingest
            .zip(self.inner.put_admission.as_ref())
            .map(|(limits, admission)| {
                limits
                    .max_concurrent_puts()
                    .saturating_sub(admission.available_permits())
            })
            .unwrap_or(0);
        let transport_buffer_bytes_capacity = self.inner.config.transport_buffer_bytes;
        let transport_buffer_bytes_in_use = transport_buffer_bytes_capacity.saturating_sub(
            self.inner
                .transport_budget
                .available_permits()
                .min(transport_buffer_bytes_capacity),
        );
        FlightServiceStatus {
            admission_open: registry.admission_open,
            preparing_queries: registry.preparing_queries,
            pending_tickets: registry.pending_tickets,
            issued_tickets: self.inner.metrics.issued_tickets.load(Ordering::Relaxed),
            consumed_tickets: self.inner.metrics.consumed_tickets.load(Ordering::Relaxed),
            expired_tickets: self.inner.metrics.expired_tickets.load(Ordering::Relaxed),
            admission_rejections: self
                .inner
                .metrics
                .admission_rejections
                .load(Ordering::Relaxed),
            encoded_bytes: self.inner.metrics.encoded_bytes.load(Ordering::Relaxed),
            transport_buffer_bytes_capacity,
            transport_buffer_bytes_in_use,
            authentication_required: sessions.is_some(),
            session_admission_open: sessions.is_none_or(|status| status.admission_open),
            active_sessions: sessions.map_or(0, |status| status.active_sessions),
            active_administrator_sessions: sessions
                .map_or(0, |status| status.active_administrator_sessions),
            active_writer_sessions: sessions.map_or(0, |status| status.active_writer_sessions),
            opened_sessions: sessions.map_or(0, |status| status.opened_sessions),
            expired_sessions: sessions.map_or(0, |status| status.expired_sessions),
            authentication_failures: sessions.map_or(0, |status| status.auth_failures),
            session_capacity_rejections: sessions.map_or(0, |status| status.capacity_rejections),
            session_revoked_tickets: self
                .inner
                .metrics
                .session_revoked_tickets
                .load(Ordering::Relaxed),
            owner_cancellations: self
                .inner
                .metrics
                .owner_cancellations
                .load(Ordering::Relaxed),
            administrator_cancellations: self
                .inner
                .metrics
                .administrator_cancellations
                .load(Ordering::Relaxed),
            metadata_tickets_issued: self
                .inner
                .metrics
                .metadata_tickets_issued
                .load(Ordering::Relaxed),
            metadata_tickets_consumed: self
                .inner
                .metrics
                .metadata_tickets_consumed
                .load(Ordering::Relaxed),
            puts_started: self.inner.metrics.puts_started.load(Ordering::Relaxed),
            puts_completed: self.inner.metrics.puts_completed.load(Ordering::Relaxed),
            put_rejections: self.inner.metrics.put_rejections.load(Ordering::Relaxed),
            put_batches: self.inner.metrics.put_batches.load(Ordering::Relaxed),
            put_rows: self.inner.metrics.put_rows.load(Ordering::Relaxed),
            put_bytes: self.inner.metrics.put_bytes.load(Ordering::Relaxed),
            active_puts,
        }
    }

    /// Returns the configured gRPC message limit used by the listener wrapper.
    #[must_use]
    pub fn max_message_bytes(&self) -> usize {
        self.inner.config.max_message_bytes
    }

    fn capabilities(&self) -> CapabilitiesResponse {
        let authentication_required = self.inner.sessions.is_some();
        let mut capabilities = vec![
            QUERY_CAPABILITY.to_owned(),
            SINGLE_USE_TICKET_CAPABILITY.to_owned(),
            STRUCTURED_ERROR_CAPABILITY.to_owned(),
        ];
        if authentication_required {
            capabilities.push(SERVER_SESSION_CAPABILITY.to_owned());
            capabilities.push(BEARER_HANDSHAKE_CAPABILITY.to_owned());
            capabilities.push(SESSION_ROLE_CAPABILITY.to_owned());
        }
        if self.inner.config.query_control_enabled {
            capabilities.push(QUERY_CANCEL_CAPABILITY.to_owned());
        }
        if self.inner.config.metadata.is_some() {
            capabilities.push(FLIGHT_SQL_METADATA_CAPABILITY.to_owned());
        }
        if self.inner.config.ingest.is_some() {
            capabilities.push(FLIGHT_MEMORY_INGEST_CAPABILITY.to_owned());
            capabilities.push(FLIGHT_WAL_INGEST_CAPABILITY.to_owned());
        }
        let metadata = self.inner.config.metadata;
        let ingest = self.inner.config.ingest;
        CapabilitiesResponse {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            server_version: self.inner.config.server_version.clone(),
            capabilities,
            max_message_bytes: u64::try_from(self.inner.config.max_message_bytes)
                .unwrap_or(u64::MAX),
            max_pending_tickets: u64::try_from(self.inner.config.max_pending_tickets)
                .unwrap_or(u64::MAX),
            authentication_required,
            session_ttl_ms: self
                .inner
                .sessions
                .as_ref()
                .map(|sessions| {
                    u64::try_from(sessions.session_ttl().as_millis()).unwrap_or(u64::MAX)
                })
                .unwrap_or(0),
            administrator_authentication_enabled: self
                .inner
                .config
                .bearer_auth
                .as_ref()
                .is_some_and(FlightBearerAuthConfig::administrator_enabled),
            metadata_max_rows: metadata
                .map(|limits| u64::try_from(limits.max_rows()).unwrap_or(u64::MAX))
                .unwrap_or(0),
            metadata_max_bytes: metadata
                .map(|limits| u64::try_from(limits.max_bytes()).unwrap_or(u64::MAX))
                .unwrap_or(0),
            writer_authentication_enabled: self
                .inner
                .config
                .bearer_auth
                .as_ref()
                .is_some_and(FlightBearerAuthConfig::writer_enabled),
            ingest_enabled: ingest.is_some(),
            ingest_max_concurrent_puts: ingest
                .map(|limits| u64::try_from(limits.max_concurrent_puts()).unwrap_or(u64::MAX))
                .unwrap_or(0),
            ingest_max_batches_per_put: ingest
                .map(|limits| u64::try_from(limits.max_batches_per_put()).unwrap_or(u64::MAX))
                .unwrap_or(0),
            ingest_max_rows_per_batch: ingest.map(FlightPutLimits::max_rows_per_batch).unwrap_or(0),
            ingest_max_rows_per_put: ingest.map(FlightPutLimits::max_rows_per_put).unwrap_or(0),
            ingest_max_batch_bytes: ingest
                .map(|limits| u64::try_from(limits.max_batch_bytes()).unwrap_or(u64::MAX))
                .unwrap_or(0),
            ingest_max_bytes_per_put: ingest
                .map(|limits| u64::try_from(limits.max_bytes_per_put()).unwrap_or(u64::MAX))
                .unwrap_or(0),
        }
    }

    fn request_session(
        &self,
        metadata: &MetadataMap,
    ) -> std::result::Result<AuthenticatedFlightSession, Status> {
        match &self.inner.sessions {
            Some(sessions) => {
                let now_ns = unix_time_ns();
                let _ = self.sweep_expired_sessions_at(now_ns);
                sessions
                    .authenticate(metadata, now_ns)
                    .map_err(|error| server_error_to_status(&error))
            }
            None => legacy_request_session(metadata).map(|session_id| AuthenticatedFlightSession {
                session_id,
                role: FlightSessionRole::Read,
            }),
        }
    }

    fn request_read_session(
        &self,
        metadata: &MetadataMap,
    ) -> std::result::Result<AuthenticatedFlightSession, Status> {
        let principal = self.request_session(metadata)?;
        if principal.role != FlightSessionRole::Read {
            return Err(server_error_to_status(
                &ServerError::new(
                    ErrorCode::PermissionDenied,
                    "Flight read RPCs require a read session",
                )
                .with_retryable(false),
            ));
        }
        Ok(principal)
    }

    fn request_write_session(
        &self,
        metadata: &MetadataMap,
    ) -> std::result::Result<AuthenticatedFlightSession, Status> {
        let principal = self.request_session(metadata)?;
        if principal.role != FlightSessionRole::Writer {
            self.inner
                .metrics
                .put_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Err(server_error_to_status(
                &ServerError::new(
                    ErrorCode::PermissionDenied,
                    "Flight DoPut requires a writer session",
                )
                .with_retryable(false),
            ));
        }
        Ok(principal)
    }

    fn sweep_expired_sessions_at(&self, now_ns: u64) -> usize {
        let revoked = self
            .inner
            .sessions
            .as_ref()
            .map(|sessions| {
                let expired_sessions = sessions.sweep_expired(now_ns);
                self.inner.tickets.revoke_sessions(&expired_sessions)
            })
            .unwrap_or(0);
        self.inner.metrics.session_revoked_tickets.fetch_add(
            u64::try_from(revoked).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        revoked
    }

    async fn cancel_flight_info(
        &self,
        request: CancelFlightInfoRequest,
        principal: AuthenticatedFlightSession,
    ) -> Result<CancelFlightInfoResult> {
        if principal.role == FlightSessionRole::Writer {
            return Err(ServerError::new(
                ErrorCode::PermissionDenied,
                "Flight writer sessions cannot cancel queries",
            )
            .with_retryable(false));
        }
        if !self.inner.config.query_control_enabled {
            return Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                "Flight query cancellation is not configured",
            )
            .with_retryable(false));
        }
        let info = request
            .info
            .ok_or_else(|| invalid("CancelFlightInfo request has no FlightInfo"))?;
        if info.endpoint.len() != 1 {
            return Err(invalid(
                "CancelFlightInfo requires exactly one Zippy Flight endpoint",
            ));
        }
        let ticket = info.endpoint[0]
            .ticket
            .as_ref()
            .ok_or_else(|| invalid("CancelFlightInfo endpoint has no ticket"))?;
        if decode_metadata_ticket(ticket)?.is_some() {
            return Ok(CancelFlightInfoResult::new(CancelStatus::NotCancellable));
        }
        let ticket = decode_query_ticket(ticket)?;
        let instance_id = ticket_instance_id(&ticket)?;
        match self
            .inner
            .tickets
            .cancel_pending_query(&ticket, principal)?
        {
            PendingCancellation::Cancelled => {}
            PendingCancellation::NotPending => match principal.role {
                FlightSessionRole::Read => {
                    self.inner
                        .frontend
                        .cancel_owned(principal.session_id, instance_id)
                        .await?;
                }
                FlightSessionRole::Administrator => {
                    self.inner
                        .frontend
                        .cancel_administrator(instance_id)
                        .await?;
                }
                FlightSessionRole::Writer => {
                    return Err(ServerError::new(
                        ErrorCode::PermissionDenied,
                        "Flight writer sessions cannot cancel queries",
                    )
                    .with_retryable(false));
                }
            },
        }
        match principal.role {
            FlightSessionRole::Read => {
                self.inner
                    .metrics
                    .owner_cancellations
                    .fetch_add(1, Ordering::Relaxed);
            }
            FlightSessionRole::Administrator => {
                self.inner
                    .metrics
                    .administrator_cancellations
                    .fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    component = "flight",
                    event = "flight_administrator_query_cancelled",
                    administrator_session_id = %principal.session_id,
                    query_id = %instance_id.query_id(),
                    generation = instance_id.generation().get(),
                    "administrator cancelled an exact Flight query"
                );
            }
            FlightSessionRole::Writer => {
                return Err(ServerError::new(
                    ErrorCode::PermissionDenied,
                    "Flight writer sessions cannot cancel queries",
                )
                .with_retryable(false));
            }
        }
        Ok(CancelFlightInfoResult::new(CancelStatus::Cancelled))
    }

    fn encoded_stream(
        &self,
        stream: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> BoxStream<'static, std::result::Result<FlightData, Status>> {
        let input = QueryBatchInput {
            inner: stream,
            retained_batch: None,
        };
        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_max_flight_data_size(
                self.inner
                    .config
                    .max_message_bytes
                    .saturating_sub(ENCODED_MESSAGE_OVERHEAD_BYTES),
            )
            .build(input);
        let budget = Arc::clone(&self.inner.transport_budget);
        let metrics = Arc::clone(&self.inner);
        let max_message_bytes = self.inner.config.max_message_bytes;
        encoder
            .then(move |result| {
                let budget = Arc::clone(&budget);
                let metrics = Arc::clone(&metrics);
                async move {
                    let data = result.map_err(flight_error_to_status)?;
                    budget_flight_data(data, budget, max_message_bytes, metrics).await
                }
            })
            .boxed()
    }

    fn encoded_metadata_batch(
        &self,
        batch: RecordBatch,
    ) -> BoxStream<'static, std::result::Result<FlightData, Status>> {
        let schema = batch.schema();
        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_max_flight_data_size(
                self.inner
                    .config
                    .max_message_bytes
                    .saturating_sub(ENCODED_MESSAGE_OVERHEAD_BYTES),
            )
            .build(futures::stream::once(async move { Ok(batch) }));
        let budget = Arc::clone(&self.inner.transport_budget);
        let metrics = Arc::clone(&self.inner);
        let max_message_bytes = self.inner.config.max_message_bytes;
        encoder
            .then(move |result| {
                let budget = Arc::clone(&budget);
                let metrics = Arc::clone(&metrics);
                async move {
                    let data = result.map_err(flight_error_to_status)?;
                    budget_flight_data(data, budget, max_message_bytes, metrics).await
                }
            })
            .boxed()
    }

    async fn process_put(
        &self,
        mut decoder: FlightDataDecoder,
        session: AuthenticatedFlightSession,
        limits: FlightPutLimits,
        responses: mpsc::Sender<std::result::Result<PutResult, Status>>,
    ) -> Result<()> {
        let first = decoder
            .next()
            .await
            .ok_or_else(|| invalid("Flight DoPut stream is empty"))?
            .map_err(flight_decode_error)?;
        let DecodedPayload::Schema(_) = first.payload else {
            return Err(invalid(
                "Flight DoPut must begin with exactly one Arrow schema message",
            ));
        };
        let descriptor = first
            .inner
            .flight_descriptor
            .as_ref()
            .ok_or_else(|| invalid("Flight DoPut schema has no command descriptor"))?;
        let command = Arc::new(decode_put_descriptor(descriptor)?);
        if command.session_id != session.session_id {
            return Err(ServerError::new(
                ErrorCode::PermissionDenied,
                "Flight ingest command session differs from request metadata",
            )
            .with_request_id(command.put_id)
            .with_retryable(false));
        }

        let mut batch_count = 0_usize;
        let mut total_rows = 0_u64;
        let mut total_bytes = 0_usize;
        let mut writer: Option<OpaqueFlightIngestWriter> = None;
        while let Some(decoded) = decoder.next().await {
            let decoded = decoded.map_err(flight_decode_error)?;
            if decoded.inner.flight_descriptor.is_some() {
                return Err(invalid(
                    "Flight DoPut descriptor may appear only on the first schema message",
                )
                .with_request_id(command.put_id));
            }
            let batch = match decoded.payload {
                DecodedPayload::RecordBatch(batch) => batch,
                DecodedPayload::Schema(_) => {
                    return Err(invalid(
                        "Flight DoPut does not permit mid-stream schema replacement",
                    )
                    .with_request_id(command.put_id));
                }
                DecodedPayload::None => {
                    return Err(
                        invalid("Flight DoPut contains an unsupported empty IPC message")
                            .with_request_id(command.put_id),
                    );
                }
            };
            if batch_count >= limits.max_batches_per_put() {
                return Err(put_resource_exhausted(
                    command.put_id,
                    "batch_count",
                    u64::try_from(batch_count.saturating_add(1)).unwrap_or(u64::MAX),
                    u64::try_from(limits.max_batches_per_put()).unwrap_or(u64::MAX),
                ));
            }
            let row_count = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
            if row_count > limits.max_rows_per_batch() {
                return Err(put_resource_exhausted(
                    command.put_id,
                    "batch_rows",
                    row_count,
                    limits.max_rows_per_batch(),
                ));
            }
            let batch_bytes = batch.get_array_memory_size();
            if batch_bytes > limits.max_batch_bytes() {
                return Err(put_resource_exhausted(
                    command.put_id,
                    "batch_bytes",
                    u64::try_from(batch_bytes).unwrap_or(u64::MAX),
                    u64::try_from(limits.max_batch_bytes()).unwrap_or(u64::MAX),
                ));
            }
            total_rows = total_rows.checked_add(row_count).ok_or_else(|| {
                put_resource_exhausted(
                    command.put_id,
                    "put_rows",
                    u64::MAX,
                    limits.max_rows_per_put(),
                )
            })?;
            if total_rows > limits.max_rows_per_put() {
                return Err(put_resource_exhausted(
                    command.put_id,
                    "put_rows",
                    total_rows,
                    limits.max_rows_per_put(),
                ));
            }
            total_bytes = total_bytes.checked_add(batch_bytes).ok_or_else(|| {
                put_resource_exhausted(
                    command.put_id,
                    "put_bytes",
                    u64::MAX,
                    u64::try_from(limits.max_bytes_per_put()).unwrap_or(u64::MAX),
                )
            })?;
            if total_bytes > limits.max_bytes_per_put() {
                return Err(put_resource_exhausted(
                    command.put_id,
                    "put_bytes",
                    u64::try_from(total_bytes).unwrap_or(u64::MAX),
                    u64::try_from(limits.max_bytes_per_put()).unwrap_or(u64::MAX),
                ));
            }
            let batch_sequence = command
                .first_batch_sequence
                .checked_add(u64::try_from(batch_count).unwrap_or(u64::MAX))
                .ok_or_else(|| {
                    invalid("Flight ingest batch sequence exhausted u64")
                        .with_request_id(command.put_id)
                })?;
            let permit_count = u32::try_from(batch_bytes.max(1)).map_err(|_| {
                internal("Flight ingest batch bytes exceed semaphore representation")
                    .with_request_id(command.put_id)
            })?;
            let batch_permit = Arc::clone(&self.inner.transport_budget)
                .try_acquire_many_owned(permit_count)
                .map_err(|_| {
                    ServerError::new(
                        ErrorCode::ResourceExhausted,
                        "Flight ingest shared transport byte budget is exhausted",
                    )
                    .with_request_id(command.put_id)
                    .with_retryable(true)
                })?;
            if writer.is_none() {
                writer = Some(
                    self.inner
                        .frontend
                        .open_ingest_writer(Arc::clone(&command))
                        .await?,
                );
            }
            let result = writer
                .as_mut()
                .expect("writer is installed before the first batch")
                .write_batch(FlightPutBatch {
                    command: Arc::clone(&command),
                    batch_sequence,
                    batch,
                })
                .await?;
            validate_put_batch_result(&command, batch_sequence, row_count, &result)?;
            drop(batch_permit);

            self.inner
                .metrics
                .put_batches
                .fetch_add(1, Ordering::Relaxed);
            self.inner
                .metrics
                .put_rows
                .fetch_add(row_count, Ordering::Relaxed);
            self.inner.metrics.put_bytes.fetch_add(
                u64::try_from(batch_bytes).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let result = PutResult {
                app_metadata: Bytes::from(result.encode_to_vec()),
            };
            if responses.send(Ok(result)).await.is_err() {
                return Err(unavailable("Flight DoPut response consumer disconnected")
                    .with_request_id(command.put_id));
            }
            batch_count += 1;
        }
        if batch_count == 0 {
            return Err(
                invalid("Flight DoPut contains no record batches").with_request_id(command.put_id)
            );
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl FlightService for ZippyFlightService {
    type HandshakeStream = BoxStream<'static, std::result::Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, std::result::Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, std::result::Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, std::result::Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, std::result::Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, std::result::Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, std::result::Result<FlightData, Status>>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        let sessions = self
            .inner
            .sessions
            .as_ref()
            .ok_or_else(|| unsupported_status("Handshake"))?;
        let metadata = request.metadata().clone();
        let mut requests = request.into_inner();
        let request = requests
            .message()
            .await?
            .ok_or_else(|| server_error_to_status(&invalid("Flight handshake is empty")))?;
        if request.protocol_version != 0 || request.payload.len() > MAX_HANDSHAKE_PAYLOAD_BYTES {
            return Err(server_error_to_status(&invalid(
                "Flight handshake envelope is unsupported or too large",
            )));
        }
        let open = SessionOpenRequest::decode(request.payload)
            .map_err(|_| server_error_to_status(&invalid("Flight session request is malformed")))?;
        if open.protocol_version != CURRENT_PROTOCOL_VERSION {
            return Err(server_error_to_status(&invalid(
                "Flight session protocol version is unsupported",
            )));
        }
        let now_ns = unix_time_ns();
        let _ = self.sweep_expired_sessions_at(now_ns);
        let opened = sessions
            .open(&metadata, now_ns)
            .map_err(|error| server_error_to_status(&error))?;
        let payload = SessionOpenResponse {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            session_id: Bytes::copy_from_slice(&opened.session_id.into_bytes()),
            bearer_token: opened.bearer_token,
            expires_at_ns: opened.expires_at_ns,
            role: match opened.role {
                FlightSessionRole::Read => SessionRole::Read as i32,
                FlightSessionRole::Administrator => SessionRole::Administrator as i32,
                FlightSessionRole::Writer => SessionRole::Writer as i32,
            },
        }
        .encode_to_vec();
        Ok(Response::new(
            futures::stream::once(async move {
                Ok(HandshakeResponse {
                    protocol_version: u64::from(CURRENT_PROTOCOL_VERSION),
                    payload: Bytes::from(payload),
                })
            })
            .boxed(),
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(unsupported_status("ListFlights"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let session = self.request_read_session(request.metadata())?;
        let descriptor = request.into_inner();
        if let Some(metadata_request) = decode_flight_sql_metadata_request(&descriptor)
            .map_err(|error| server_error_to_status(&error))?
        {
            let limits = self
                .inner
                .config
                .metadata
                .ok_or_else(|| unsupported_status("FlightSqlMetadata"))?;
            let reservation = self
                .inner
                .tickets
                .reserve(unix_time_ns())
                .map_err(|error| {
                    self.inner
                        .metrics
                        .admission_rejections
                        .fetch_add(1, Ordering::Relaxed);
                    server_error_to_status(&error)
                })?;
            let preparation_bytes = u32::try_from(limits.max_bytes()).map_err(|_| {
                server_error_to_status(&internal(
                    "Flight SQL metadata byte limit exceeds semaphore representation",
                ))
            })?;
            let mut retained_permit = Arc::clone(&self.inner.transport_budget)
                .try_acquire_many_owned(preparation_bytes)
                .map_err(|_| {
                    server_error_to_status(
                        &ServerError::new(
                            ErrorCode::ResourceExhausted,
                            "Flight SQL metadata working byte budget is exhausted",
                        )
                        .with_retryable(true),
                    )
                })?;
            let prepared = self
                .inner
                .frontend
                .prepare_metadata(metadata_request, limits)
                .await
                .map_err(|error| server_error_to_status(&error))?;
            let unused_bytes = limits.max_bytes().saturating_sub(prepared.retained_bytes());
            if unused_bytes > 0 {
                drop(retained_permit.split(unused_bytes).ok_or_else(|| {
                    server_error_to_status(&internal(
                        "prepared Flight SQL metadata exceeded its reserved byte budget",
                    ))
                })?);
            }
            let schema = prepared.batch().schema();
            let catalog_revision = prepared.catalog_revision();
            let expires_at_ns = unix_time_ns()
                .saturating_add(u64::try_from(limits.ticket_ttl().as_nanos()).unwrap_or(u64::MAX));
            let response = FlightInfo::new()
                .try_with_schema(schema.as_ref())
                .map_err(|_| {
                    server_error_to_status(&internal(
                        "prepared Flight SQL metadata schema could not be encoded",
                    ))
                })?
                .with_descriptor(descriptor)
                .with_ordered(true)
                .with_app_metadata(
                    MetadataInfo {
                        protocol_version: CURRENT_PROTOCOL_VERSION,
                        catalog_revision,
                        expires_at_ns,
                    }
                    .encode_to_vec(),
                );
            let registered = reservation
                .install_metadata(
                    session.session_id,
                    prepared,
                    retained_permit,
                    expires_at_ns,
                    catalog_revision,
                )
                .map_err(|error| server_error_to_status(&error))?;
            self.inner
                .metrics
                .issued_tickets
                .fetch_add(1, Ordering::Relaxed);
            self.inner
                .metrics
                .metadata_tickets_issued
                .fetch_add(1, Ordering::Relaxed);
            let endpoint = FlightEndpoint::new()
                .with_ticket(encode_metadata_ticket(&registered.ticket))
                .with_expiration_time(expiration_timestamp(expires_at_ns)?);
            return Ok(Response::new(response.with_endpoint(endpoint)));
        }
        let query =
            decode_query_descriptor(&descriptor).map_err(|error| server_error_to_status(&error))?;
        if query.request.session_id != session.session_id {
            return Err(server_error_to_status(
                &ServerError::new(
                    ErrorCode::PermissionDenied,
                    "query command session differs from request metadata",
                )
                .with_query_id(query.request.query_id)
                .with_retryable(false),
            ));
        }
        let reservation = self
            .inner
            .tickets
            .reserve(unix_time_ns())
            .map_err(|error| {
                self.inner
                    .metrics
                    .admission_rejections
                    .fetch_add(1, Ordering::Relaxed);
                server_error_to_status(&error)
            })?;
        let prepared = self
            .inner
            .frontend
            .prepare(query)
            .await
            .map_err(|error| server_error_to_status(&error))?;
        let schema = prepared.schema();
        let plan = prepared.info().clone();
        let instance_id = prepared.instance_id();
        if instance_id.query_id() != plan.query_id {
            return Err(server_error_to_status(&internal(
                "prepared Flight query identity differs from its plan metadata",
            )));
        }
        let metadata = query_info(&plan, instance_id.generation()).encode_to_vec();
        let response = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|_| {
                server_error_to_status(&internal(
                    "prepared Flight query schema could not be encoded",
                ))
            })?
            .with_descriptor(descriptor)
            .with_ordered(true)
            .with_app_metadata(metadata);
        let registered = reservation
            .install_query(session.session_id, prepared)
            .map_err(|error| server_error_to_status(&error))?;
        self.inner
            .metrics
            .issued_tickets
            .fetch_add(1, Ordering::Relaxed);
        let endpoint = FlightEndpoint::new()
            .with_ticket(encode_query_ticket(&registered.ticket))
            .with_expiration_time(expiration_timestamp(registered.ticket.expires_at_ns)?);
        Ok(Response::new(response.with_endpoint(endpoint)))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(unsupported_status("PollFlightInfo"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        Err(unsupported_status("GetSchema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let session = self.request_read_session(request.metadata())?;
        if let Some(ticket) = decode_metadata_ticket(request.get_ref())
            .map_err(|error| server_error_to_status(&error))?
        {
            let prepared = self
                .inner
                .tickets
                .take_metadata(&ticket, session.session_id, unix_time_ns())
                .map_err(|error| server_error_to_status(&error))?;
            self.inner
                .metrics
                .consumed_tickets
                .fetch_add(1, Ordering::Relaxed);
            self.inner
                .metrics
                .metadata_tickets_consumed
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Response::new(
                self.encoded_metadata_batch(prepared.into_batch()),
            ));
        }
        let ticket = decode_query_ticket(request.get_ref())
            .map_err(|error| server_error_to_status(&error))?;
        let prepared = self
            .inner
            .tickets
            .take_query(&ticket, session.session_id, unix_time_ns())
            .map_err(|error| server_error_to_status(&error))?;
        self.inner
            .metrics
            .consumed_tickets
            .fetch_add(1, Ordering::Relaxed);
        let schema = prepared.schema();
        let stream = prepared
            .execute()
            .map_err(|error| server_error_to_status(&error))?;
        Ok(Response::new(self.encoded_stream(stream, schema)))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        let limits = self
            .inner
            .config
            .ingest
            .ok_or_else(|| unsupported_status("DoPut"))?;
        let session = self.request_write_session(request.metadata())?;
        let admission = Arc::clone(
            self.inner
                .put_admission
                .as_ref()
                .expect("configured Flight ingest owns an admission semaphore"),
        )
        .try_acquire_owned()
        .map_err(|_| {
            self.inner
                .metrics
                .put_rejections
                .fetch_add(1, Ordering::Relaxed);
            server_error_to_status(
                &ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "Flight concurrent DoPut capacity is exhausted",
                )
                .with_retryable(true),
            )
        })?;
        self.inner
            .metrics
            .puts_started
            .fetch_add(1, Ordering::Relaxed);

        let input = request
            .into_inner()
            .map(|result| result.map_err(FlightError::from));
        let decoder = FlightDataDecoder::new(input);
        let service = self.clone();
        let (responses, receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            let result = service
                .process_put(decoder, session, limits, responses.clone())
                .await;
            match result {
                Ok(()) => {
                    service
                        .inner
                        .metrics
                        .puts_completed
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    service
                        .inner
                        .metrics
                        .put_rejections
                        .fetch_add(1, Ordering::Relaxed);
                    let _ = responses.send(Err(server_error_to_status(&error))).await;
                }
            }
            drop(admission);
        });
        Ok(Response::new(ReceiverStream::new(receiver).boxed()))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        if request.get_ref().r#type == CANCEL_FLIGHT_INFO_ACTION {
            let principal = self.request_session(request.metadata())?;
            let action = request.into_inner();
            let cancel = CancelFlightInfoRequest::decode(action.body)
                .map_err(|_| server_error_to_status(&invalid("CancelFlightInfo is malformed")))?;
            let result = self
                .cancel_flight_info(cancel, principal)
                .await
                .map_err(|error| server_error_to_status(&error))?;
            return Ok(Response::new(
                futures::stream::once(async move {
                    Ok(arrow_flight::Result {
                        body: Bytes::from(result.encode_to_vec()),
                    })
                })
                .boxed(),
            ));
        }
        let action = request.into_inner();
        if action.r#type != CAPABILITIES_ACTION {
            return Err(unsupported_status("DoAction"));
        }
        let request = if action.body.is_empty() {
            CapabilitiesRequest {
                protocol_version: CURRENT_PROTOCOL_VERSION,
            }
        } else {
            CapabilitiesRequest::decode(action.body).map_err(|_| {
                server_error_to_status(&invalid("capabilities request is malformed"))
            })?
        };
        if request.protocol_version != CURRENT_PROTOCOL_VERSION {
            return Err(server_error_to_status(&invalid(
                "capabilities protocol version is unsupported",
            )));
        }
        let body = Bytes::from(self.capabilities().encode_to_vec());
        Ok(Response::new(
            futures::stream::once(async move { Ok(arrow_flight::Result { body }) }).boxed(),
        ))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        let mut actions = vec![Ok(ActionType {
            r#type: CAPABILITIES_ACTION.to_owned(),
            description: "return Zippy Flight protocol capabilities".to_owned(),
        })];
        if self.inner.config.query_control_enabled {
            actions.push(Ok(ActionType {
                r#type: CANCEL_FLIGHT_INFO_ACTION.to_owned(),
                description: "cancel one exact owner-bound Flight query".to_owned(),
            }));
        }
        Ok(Response::new(futures::stream::iter(actions).boxed()))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(unsupported_status("DoExchange"))
    }
}

struct QueryBatchInput {
    inner: SendableRecordBatchStream,
    retained_batch: Option<QueryResultBatch>,
}

impl Stream for QueryBatchInput {
    type Item = arrow_flight::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        drop(self.retained_batch.take());
        match self.inner.as_mut().poll_next(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(batch))) => {
                let record_batch = batch.record_batch().clone();
                self.retained_batch = Some(batch);
                Poll::Ready(Some(Ok(record_batch)))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(FlightError::Tonic(
                server_error_to_status(&error),
            )))),
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}

struct TicketRegistry {
    server_instance_id: [u8; OPAQUE_TICKET_ID_BYTES],
    max_pending: usize,
    state: Mutex<TicketRegistryState>,
}

struct TicketRegistryState {
    admission_open: bool,
    preparing_queries: usize,
    entries: HashMap<[u8; OPAQUE_TICKET_ID_BYTES], TicketEntry>,
}

struct TicketEntry {
    session_id: SessionId,
    expires_at_ns: u64,
    payload: TicketPayload,
}

enum TicketPayload {
    Query {
        instance_id: QueryInstanceId,
        prepared: OpaquePreparedFlightQuery,
    },
    Metadata {
        catalog_revision: u64,
        prepared: PreparedFlightSqlMetadata,
        _retained_permit: OwnedSemaphorePermit,
    },
}

struct TicketReservation<'a> {
    registry: &'a TicketRegistry,
    active: bool,
}

struct RegisteredQueryTicket {
    ticket: QueryTicket,
}

struct RegisteredMetadataTicket {
    ticket: MetadataTicket,
}

enum PendingCancellation {
    Cancelled,
    NotPending,
}

#[derive(Debug, Clone, Copy)]
struct TicketRegistryStatus {
    admission_open: bool,
    preparing_queries: usize,
    pending_tickets: usize,
}

impl TicketRegistry {
    fn new(server_instance_id: [u8; OPAQUE_TICKET_ID_BYTES], max_pending: usize) -> Self {
        Self {
            server_instance_id,
            max_pending,
            state: Mutex::new(TicketRegistryState {
                admission_open: true,
                preparing_queries: 0,
                entries: HashMap::with_capacity(max_pending.min(1024)),
            }),
        }
    }

    fn reserve(&self, now_ns: u64) -> Result<TicketReservation<'_>> {
        self.sweep_expired(now_ns);
        let mut state = self.lock();
        if !state.admission_open {
            return Err(unavailable("Flight ticket admission is closed"));
        }
        if state.preparing_queries.saturating_add(state.entries.len()) >= self.max_pending {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                "Flight pending ticket capacity is exhausted",
            )
            .with_retryable(true));
        }
        state.preparing_queries += 1;
        Ok(TicketReservation {
            registry: self,
            active: true,
        })
    }

    fn take_query(
        &self,
        ticket: &QueryTicket,
        session_id: SessionId,
        now_ns: u64,
    ) -> Result<OpaquePreparedFlightQuery> {
        self.validate_server_instance(ticket.server_instance_id.as_ref())?;
        if ticket.expires_at_ns <= now_ns {
            return Err(ServerError::new(
                ErrorCode::DeadlineExceeded,
                "Flight query ticket expired before result consumption",
            )
            .with_query_id(ticket_query_id(ticket)?)
            .with_retryable(false));
        }
        let ticket_id: [u8; OPAQUE_TICKET_ID_BYTES] = ticket
            .ticket_id
            .as_ref()
            .try_into()
            .map_err(|_| invalid("Flight query ticket identity is malformed"))?;
        let mut state = self.lock();
        let entry = state
            .entries
            .get(&ticket_id)
            .ok_or_else(|| not_found("Flight query ticket was already consumed or is unknown"))?;
        if entry.session_id != session_id {
            return Err(ServerError::new(
                ErrorCode::PermissionDenied,
                "Flight query ticket belongs to another client session",
            )
            .with_retryable(false));
        }
        let TicketPayload::Query { instance_id, .. } = &entry.payload else {
            return Err(not_found("Flight ticket does not identify a query result"));
        };
        validate_query_ticket_entry(ticket, *instance_id, entry.expires_at_ns)?;
        let entry = state
            .entries
            .remove(&ticket_id)
            .expect("validated Flight ticket entry remains present under the same lock");
        drop(state);
        match entry.payload {
            TicketPayload::Query { prepared, .. } => Ok(prepared),
            TicketPayload::Metadata { .. } => {
                unreachable!("validated query payload remains a query")
            }
        }
    }

    fn take_metadata(
        &self,
        ticket: &MetadataTicket,
        session_id: SessionId,
        now_ns: u64,
    ) -> Result<PreparedFlightSqlMetadata> {
        self.validate_server_instance(ticket.server_instance_id.as_ref())?;
        if ticket.expires_at_ns <= now_ns {
            return Err(ServerError::new(
                ErrorCode::DeadlineExceeded,
                "Flight SQL metadata ticket expired before result consumption",
            )
            .with_retryable(false));
        }
        let ticket_id = ticket_id(ticket.ticket_id.as_ref(), "metadata")?;
        let mut state = self.lock();
        let entry = state.entries.get(&ticket_id).ok_or_else(|| {
            not_found("Flight SQL metadata ticket was already consumed or is unknown")
        })?;
        if entry.session_id != session_id {
            return Err(ServerError::new(
                ErrorCode::PermissionDenied,
                "Flight SQL metadata ticket belongs to another client session",
            )
            .with_retryable(false));
        }
        let TicketPayload::Metadata {
            catalog_revision, ..
        } = &entry.payload
        else {
            return Err(not_found("Flight ticket does not identify metadata"));
        };
        if ticket.catalog_revision != *catalog_revision
            || ticket.expires_at_ns != entry.expires_at_ns
        {
            return Err(not_found(
                "Flight SQL metadata ticket integrity check failed",
            ));
        }
        let entry = state
            .entries
            .remove(&ticket_id)
            .expect("validated metadata ticket remains present under the same lock");
        drop(state);
        match entry.payload {
            TicketPayload::Metadata { prepared, .. } => Ok(prepared),
            TicketPayload::Query { .. } => {
                unreachable!("validated metadata payload remains metadata")
            }
        }
    }

    fn cancel_pending_query(
        &self,
        ticket: &QueryTicket,
        principal: AuthenticatedFlightSession,
    ) -> Result<PendingCancellation> {
        self.validate_server_instance(ticket.server_instance_id.as_ref())?;
        let ticket_id = ticket_id(ticket.ticket_id.as_ref(), "query")?;
        let mut state = self.lock();
        let Some(entry) = state.entries.get(&ticket_id) else {
            return Ok(PendingCancellation::NotPending);
        };
        if entry.session_id != principal.session_id
            && principal.role != FlightSessionRole::Administrator
        {
            return Err(ServerError::new(
                ErrorCode::PermissionDenied,
                "Flight query cancellation requires owner or administrator authority",
            )
            .with_retryable(false));
        }
        let TicketPayload::Query { instance_id, .. } = &entry.payload else {
            return Err(not_found(
                "Flight cancellation ticket does not identify a query",
            ));
        };
        validate_query_ticket_entry(ticket, *instance_id, entry.expires_at_ns)?;
        let entry = state
            .entries
            .remove(&ticket_id)
            .expect("validated cancellation ticket remains present under the same lock");
        drop(state);
        drop(entry);
        Ok(PendingCancellation::Cancelled)
    }

    fn validate_server_instance(&self, candidate: &[u8]) -> Result<()> {
        if candidate != self.server_instance_id {
            return Err(not_found(
                "Flight ticket is unknown to this server instance",
            ));
        }
        Ok(())
    }

    fn sweep_expired(&self, now_ns: u64) -> usize {
        let expired = {
            let mut state = self.lock();
            let ticket_ids = state
                .entries
                .iter()
                .filter_map(|(ticket_id, entry)| {
                    (entry.expires_at_ns <= now_ns).then_some(*ticket_id)
                })
                .collect::<Vec<_>>();
            ticket_ids
                .into_iter()
                .filter_map(|ticket_id| state.entries.remove(&ticket_id))
                .collect::<Vec<_>>()
        };
        let expired_count = expired.len();
        drop(expired);
        expired_count
    }

    fn revoke_sessions(&self, session_ids: &[SessionId]) -> usize {
        if session_ids.is_empty() {
            return 0;
        }
        let session_ids = session_ids.iter().copied().collect::<HashSet<_>>();
        let revoked = {
            let mut state = self.lock();
            let ticket_ids = state
                .entries
                .iter()
                .filter_map(|(ticket_id, entry)| {
                    session_ids
                        .contains(&entry.session_id)
                        .then_some(*ticket_id)
                })
                .collect::<Vec<_>>();
            ticket_ids
                .into_iter()
                .filter_map(|ticket_id| state.entries.remove(&ticket_id))
                .collect::<Vec<_>>()
        };
        let revoked_count = revoked.len();
        drop(revoked);
        revoked_count
    }

    fn close(&self) {
        let entries = {
            let mut state = self.lock();
            state.admission_open = false;
            std::mem::take(&mut state.entries)
        };
        drop(entries);
    }

    fn status(&self) -> TicketRegistryStatus {
        let state = self.lock();
        TicketRegistryStatus {
            admission_open: state.admission_open,
            preparing_queries: state.preparing_queries,
            pending_tickets: state.entries.len(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, TicketRegistryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl TicketReservation<'_> {
    fn install_query(
        mut self,
        session_id: SessionId,
        prepared: OpaquePreparedFlightQuery,
    ) -> Result<RegisteredQueryTicket> {
        let instance_id = prepared.instance_id();
        let expires_at_ns = prepared.info().expires_at_ns;
        if expires_at_ns <= unix_time_ns() {
            return Err(ServerError::new(
                ErrorCode::DeadlineExceeded,
                "prepared Flight query expired before ticket publication",
            )
            .with_query_id(instance_id.query_id())
            .with_retryable(false));
        }
        let mut ticket_id = [0_u8; OPAQUE_TICKET_ID_BYTES];
        let mut state = self.registry.lock();
        if !state.admission_open {
            return Err(unavailable(
                "Flight ticket admission closed during query preparation",
            ));
        }
        let mut unique = false;
        for _ in 0..16 {
            fill_random_nonzero(&mut ticket_id)?;
            if !state.entries.contains_key(&ticket_id) {
                unique = true;
                break;
            }
        }
        if !unique {
            return Err(internal(
                "could not allocate a unique Flight ticket identity",
            ));
        }
        state.preparing_queries = state.preparing_queries.saturating_sub(1);
        state.entries.insert(
            ticket_id,
            TicketEntry {
                session_id,
                expires_at_ns,
                payload: TicketPayload::Query {
                    instance_id,
                    prepared,
                },
            },
        );
        self.active = false;
        Ok(RegisteredQueryTicket {
            ticket: QueryTicket {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                server_instance_id: Bytes::copy_from_slice(&self.registry.server_instance_id),
                ticket_id: Bytes::copy_from_slice(&ticket_id),
                query_id: Bytes::copy_from_slice(&instance_id.query_id().into_bytes()),
                generation: instance_id.generation().get(),
                expires_at_ns,
            },
        })
    }

    fn install_metadata(
        mut self,
        session_id: SessionId,
        prepared: PreparedFlightSqlMetadata,
        retained_permit: OwnedSemaphorePermit,
        expires_at_ns: u64,
        catalog_revision: u64,
    ) -> Result<RegisteredMetadataTicket> {
        if expires_at_ns <= unix_time_ns() || prepared.catalog_revision() != catalog_revision {
            return Err(internal(
                "prepared Flight SQL metadata has an invalid publication boundary",
            ));
        }
        let mut ticket_id = [0_u8; OPAQUE_TICKET_ID_BYTES];
        let mut state = self.registry.lock();
        if !state.admission_open {
            return Err(unavailable(
                "Flight ticket admission closed during metadata preparation",
            ));
        }
        let mut unique = false;
        for _ in 0..16 {
            fill_random_nonzero(&mut ticket_id)?;
            if !state.entries.contains_key(&ticket_id) {
                unique = true;
                break;
            }
        }
        if !unique {
            return Err(internal(
                "could not allocate a unique Flight metadata ticket identity",
            ));
        }
        state.preparing_queries = state.preparing_queries.saturating_sub(1);
        state.entries.insert(
            ticket_id,
            TicketEntry {
                session_id,
                expires_at_ns,
                payload: TicketPayload::Metadata {
                    catalog_revision,
                    prepared,
                    _retained_permit: retained_permit,
                },
            },
        );
        self.active = false;
        Ok(RegisteredMetadataTicket {
            ticket: MetadataTicket {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                server_instance_id: Bytes::copy_from_slice(&self.registry.server_instance_id),
                ticket_id: Bytes::copy_from_slice(&ticket_id),
                catalog_revision,
                expires_at_ns,
            },
        })
    }
}

impl Drop for TicketReservation<'_> {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.registry.lock();
            state.preparing_queries = state.preparing_queries.saturating_sub(1);
        }
    }
}

struct BudgetedBytes {
    bytes: Bytes,
    _permit: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for BudgetedBytes {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

async fn budget_flight_data(
    mut data: FlightData,
    budget: Arc<Semaphore>,
    max_message_bytes: usize,
    service: Arc<FlightServiceInner>,
) -> std::result::Result<FlightData, Status> {
    let payload_bytes = data
        .data_header
        .len()
        .saturating_add(data.data_body.len())
        .saturating_add(data.app_metadata.len());
    let encoded_bytes = payload_bytes.saturating_add(ENCODED_MESSAGE_OVERHEAD_BYTES);
    if encoded_bytes > max_message_bytes {
        return Err(server_error_to_status(&ServerError::new(
            ErrorCode::ResourceExhausted,
            "encoded Flight message exceeds the configured transport limit",
        )));
    }
    let permit = budget
        .acquire_many_owned(encoded_bytes as u32)
        .await
        .map_err(|_| server_error_to_status(&unavailable("Flight transport budget is closed")))?;
    service
        .metrics
        .encoded_bytes
        .fetch_add(encoded_bytes as u64, Ordering::Relaxed);
    data.data_header = Bytes::from_owner(BudgetedBytes {
        bytes: data.data_header,
        _permit: permit,
    });
    Ok(data)
}

fn legacy_request_session(metadata: &MetadataMap) -> std::result::Result<SessionId, Status> {
    let value = metadata
        .get(SESSION_METADATA_KEY)
        .ok_or_else(|| {
            server_error_to_status(&ServerError::new(
                ErrorCode::Unauthenticated,
                "Flight request is missing the Zippy session metadata",
            ))
        })?
        .to_str()
        .map_err(|_| {
            server_error_to_status(&ServerError::new(
                ErrorCode::Unauthenticated,
                "Flight session metadata is not valid ASCII",
            ))
        })?;
    SessionId::from_str(value).map_err(|_| {
        server_error_to_status(&ServerError::new(
            ErrorCode::Unauthenticated,
            "Flight session metadata is not a valid non-nil session ID",
        ))
    })
}

fn ticket_query_id(ticket: &QueryTicket) -> Result<QueryId> {
    let bytes: [u8; 16] = ticket
        .query_id
        .as_ref()
        .try_into()
        .map_err(|_| invalid("Flight query ticket query ID is malformed"))?;
    QueryId::from_bytes(bytes).map_err(|_| invalid("Flight query ticket query ID is nil"))
}

fn ticket_instance_id(ticket: &QueryTicket) -> Result<QueryInstanceId> {
    let query_id = ticket_query_id(ticket)?;
    let generation = NonZeroU64::new(ticket.generation)
        .ok_or_else(|| invalid("Flight query ticket generation is zero"))?;
    Ok(QueryInstanceId::new(query_id, generation))
}

fn ticket_id(value: &[u8], kind: &str) -> Result<[u8; OPAQUE_TICKET_ID_BYTES]> {
    value
        .try_into()
        .map_err(|_| invalid(format!("Flight {kind} ticket identity is malformed")))
}

fn validate_query_ticket_entry(
    ticket: &QueryTicket,
    instance_id: QueryInstanceId,
    expires_at_ns: u64,
) -> Result<()> {
    if ticket_instance_id(ticket)? != instance_id || ticket.expires_at_ns != expires_at_ns {
        return Err(not_found("Flight query ticket integrity check failed"));
    }
    Ok(())
}

fn expiration_timestamp(expires_at_ns: u64) -> std::result::Result<Timestamp, Status> {
    let seconds = expires_at_ns / 1_000_000_000;
    Ok(Timestamp {
        seconds: i64::try_from(seconds).map_err(|_| {
            server_error_to_status(&internal("Flight ticket expiry exceeds timestamp range"))
        })?,
        nanos: i32::try_from(expires_at_ns % 1_000_000_000)
            .expect("subsecond nanoseconds always fit i32"),
    })
}

fn flight_error_to_status(error: FlightError) -> Status {
    match error {
        FlightError::Tonic(status) => status,
        other => server_error_to_status(&internal(format!(
            "Arrow Flight encoder failed error=[{other}]"
        ))),
    }
}

fn flight_decode_error(error: FlightError) -> ServerError {
    match error {
        FlightError::Tonic(status) => ServerError::new(
            ErrorCode::Unavailable,
            format!(
                "Flight DoPut input stream failed status=[{}]",
                status.code()
            ),
        )
        .with_retryable(true),
        other => ServerError::new(
            ErrorCode::InvalidArgument,
            format!("Flight DoPut Arrow IPC is malformed error=[{other}]"),
        )
        .with_retryable(false),
    }
}

fn put_resource_exhausted(
    put_id: zippy_types::RequestId,
    resource: &str,
    requested: u64,
    maximum: u64,
) -> ServerError {
    ServerError::new(
        ErrorCode::ResourceExhausted,
        format!(
            "Flight ingest resource limit exceeded resource=[{resource}] requested=[{requested}] maximum=[{maximum}]"
        ),
    )
    .with_request_id(put_id)
    .with_retryable(false)
}

fn unsupported_status(operation: &str) -> Status {
    let error = ServerError::new(
        ErrorCode::UnsupportedPlan,
        format!("Flight operation is not enabled in protocol stage 6.4 operation=[{operation}]"),
    )
    .with_retryable(false)
    .with_details(ServerErrorDetails::UnsupportedPlan {
        operator: operation.to_owned(),
    })
    .expect("unsupported-plan details match their stable error code");
    server_error_to_status(&error)
}

fn fill_random_nonzero(target: &mut [u8; OPAQUE_TICKET_ID_BYTES]) -> Result<()> {
    for _ in 0..16 {
        getrandom::fill(target).map_err(|error| {
            internal(format!(
                "operating system randomness failed error=[{error}]"
            ))
        })?;
        if target.iter().any(|byte| *byte != 0) {
            return Ok(());
        }
    }
    Err(internal(
        "operating system returned repeated nil identities",
    ))
}

fn unix_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn not_found(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::NotFound, message).with_retryable(false)
}

fn unavailable(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Unavailable, message).with_retryable(true)
}

fn internal(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Internal, message).with_retryable(false)
}
