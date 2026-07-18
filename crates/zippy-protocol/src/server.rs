//! Bounded Arrow Flight query service and transport-neutral frontend contract.

use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use prost::Message;
use prost_types::Timestamp;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};
use zippy_types::query::{
    QueryInstanceId, QueryPlanInfo, QueryResultBatch, SendableRecordBatchStream,
};
use zippy_types::{
    BoxFuture, ErrorCode, QueryId, Result, ServerError, ServerErrorDetails, SessionId,
};

use crate::error::server_error_to_status;
use crate::query::{
    decode_query_descriptor, decode_query_ticket, encode_query_ticket, query_info,
    FlightQueryRequest, OPAQUE_TICKET_ID_BYTES,
};
use crate::wire::{CapabilitiesRequest, CapabilitiesResponse, QueryTicket};
use crate::{CAPABILITIES_ACTION, CURRENT_PROTOCOL_VERSION, SESSION_METADATA_KEY};

const MIN_FLIGHT_MESSAGE_BYTES: usize = 64 * 1024;
const MAX_FLIGHT_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const ENCODED_MESSAGE_OVERHEAD_BYTES: usize = 256;
const QUERY_CAPABILITY: &str = "query.get_flight_info_do_get.v1";
const STRUCTURED_ERROR_CAPABILITY: &str = "error.details.v1";
const SINGLE_USE_TICKET_CAPABILITY: &str = "query.single_use_ticket.v1";

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

/// Query planner boundary consumed by the protocol service.
pub trait FlightQueryFrontend: Send + Sync + 'static {
    /// Validates, binds, and physically prepares one query against an exact
    /// snapshot without producing result batches yet.
    fn prepare(
        &self,
        request: FlightQueryRequest,
    ) -> BoxFuture<'_, Result<OpaquePreparedFlightQuery>>;
}

/// Resource and compatibility limits for one Flight query service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightServiceConfig {
    server_version: String,
    max_message_bytes: usize,
    max_pending_tickets: usize,
    transport_buffer_bytes: usize,
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
        })
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
    /// Queries currently being prepared before ticket publication.
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
    transport_budget: Arc<Semaphore>,
    metrics: FlightMetrics,
}

#[derive(Default)]
struct FlightMetrics {
    issued_tickets: AtomicU64,
    consumed_tickets: AtomicU64,
    expired_tickets: AtomicU64,
    admission_rejections: AtomicU64,
    encoded_bytes: AtomicU64,
}

impl ZippyFlightService {
    /// Creates a service with a random boot-scoped server identity.
    pub fn try_new(
        config: FlightServiceConfig,
        frontend: Arc<dyn FlightQueryFrontend>,
    ) -> Result<Self> {
        let mut server_instance_id = [0_u8; OPAQUE_TICKET_ID_BYTES];
        fill_random_nonzero(&mut server_instance_id)?;
        Ok(Self {
            inner: Arc::new(FlightServiceInner {
                tickets: TicketRegistry::new(server_instance_id, config.max_pending_tickets),
                transport_budget: Arc::new(Semaphore::new(config.transport_buffer_bytes)),
                config,
                frontend,
                metrics: FlightMetrics::default(),
            }),
        })
    }

    /// Stops new plans and releases every unconsumed prepared query.
    pub fn close_admission(&self) {
        self.inner.tickets.close();
        self.inner.transport_budget.close();
    }

    /// Releases every prepared query whose one-use ticket has expired.
    #[must_use]
    pub fn sweep_expired(&self) -> usize {
        let expired = self.inner.tickets.sweep_expired(unix_time_ns());
        self.inner
            .metrics
            .expired_tickets
            .fetch_add(expired as u64, Ordering::Relaxed);
        expired
    }

    /// Returns bounded ticket and transport counters for readiness diagnostics.
    #[must_use]
    pub fn status(&self) -> FlightServiceStatus {
        let registry = self.inner.tickets.status();
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
        }
    }

    /// Returns the configured gRPC message limit used by the listener wrapper.
    #[must_use]
    pub fn max_message_bytes(&self) -> usize {
        self.inner.config.max_message_bytes
    }

    fn capabilities(&self) -> CapabilitiesResponse {
        CapabilitiesResponse {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            server_version: self.inner.config.server_version.clone(),
            capabilities: vec![
                QUERY_CAPABILITY.to_owned(),
                SINGLE_USE_TICKET_CAPABILITY.to_owned(),
                STRUCTURED_ERROR_CAPABILITY.to_owned(),
            ],
            max_message_bytes: self.inner.config.max_message_bytes as u64,
            max_pending_tickets: self.inner.config.max_pending_tickets as u64,
        }
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
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(unsupported_status("Handshake"))
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
        let session_id = request_session(request.metadata())?;
        let descriptor = request.into_inner();
        let query =
            decode_query_descriptor(&descriptor).map_err(|error| server_error_to_status(&error))?;
        if query.request.session_id != session_id {
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
            .install(session_id, prepared)
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
        let session_id = request_session(request.metadata())?;
        let ticket = decode_query_ticket(request.get_ref())
            .map_err(|error| server_error_to_status(&error))?;
        let prepared = self
            .inner
            .tickets
            .take(&ticket, session_id, unix_time_ns())
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
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(unsupported_status("DoPut"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
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
        Ok(Response::new(
            futures::stream::once(async {
                Ok(ActionType {
                    r#type: CAPABILITIES_ACTION.to_owned(),
                    description: "return Zippy Flight protocol capabilities".to_owned(),
                })
            })
            .boxed(),
        ))
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
    instance_id: QueryInstanceId,
    expires_at_ns: u64,
    prepared: OpaquePreparedFlightQuery,
}

struct TicketReservation<'a> {
    registry: &'a TicketRegistry,
    active: bool,
}

struct RegisteredTicket {
    ticket: QueryTicket,
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

    fn take(
        &self,
        ticket: &QueryTicket,
        session_id: SessionId,
        now_ns: u64,
    ) -> Result<OpaquePreparedFlightQuery> {
        if ticket.server_instance_id.as_ref() != self.server_instance_id {
            return Err(not_found(
                "Flight query ticket is unknown to this server instance",
            ));
        }
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
        if ticket.query_id.as_ref() != entry.instance_id.query_id().into_bytes()
            || ticket.generation != entry.instance_id.generation().get()
            || ticket.expires_at_ns != entry.expires_at_ns
        {
            return Err(not_found("Flight query ticket integrity check failed"));
        }
        let entry = state
            .entries
            .remove(&ticket_id)
            .expect("validated Flight ticket entry remains present under the same lock");
        drop(state);
        Ok(entry.prepared)
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
    fn install(
        mut self,
        session_id: SessionId,
        prepared: OpaquePreparedFlightQuery,
    ) -> Result<RegisteredTicket> {
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
                instance_id,
                expires_at_ns,
                prepared,
            },
        );
        self.active = false;
        Ok(RegisteredTicket {
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

fn request_session(metadata: &MetadataMap) -> std::result::Result<SessionId, Status> {
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

fn unsupported_status(operation: &str) -> Status {
    let error = ServerError::new(
        ErrorCode::UnsupportedPlan,
        format!("Flight operation is not enabled in protocol stage 6.1 operation=[{operation}]"),
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
