use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient as RawFlightClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{
    CommandGetDbSchemas, CommandGetTableTypes, CommandGetTables, ProstMessageExt,
};
use arrow_flight::{Action, CancelFlightInfoRequest, CancelStatus, FlightClient, HandshakeRequest};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyUsagePurpose};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate as TonicCertificate, ClientTlsConfig, Identity};
use zippy_protocol::auth::FlightBearerAuthConfig;
use zippy_protocol::error::status_to_server_error;
use zippy_protocol::query::{
    decode_query_ticket, encode_query_descriptor, encode_query_request, encode_query_ticket,
    FlightQueryRequest, FlightTableBinding,
};
use zippy_protocol::runtime::{FlightServerRuntime, FlightServerTlsConfig};
use zippy_protocol::server::{
    FlightQueryFrontend, FlightServiceConfig, OpaquePreparedFlightQuery, PreparedFlightQuery,
    ZippyFlightService,
};
use zippy_protocol::sql::{
    FlightSqlMetadataLimits, FlightSqlMetadataRequest, PreparedFlightSqlMetadata,
};
use zippy_protocol::wire::{
    CapabilitiesResponse, MetadataInfo, SessionOpenRequest, SessionOpenResponse, SessionRole,
};
use zippy_protocol::{
    AUTHORIZATION_METADATA_KEY, CAPABILITIES_ACTION, CURRENT_PROTOCOL_VERSION, SESSION_METADATA_KEY,
};
use zippy_types::query::{
    QueryInput, QueryInstanceId, QueryPlanInfo, QueryPriority, QueryRequest, QueryResultBatch,
    RecordBatchStream, SendableRecordBatchStream,
};
use zippy_types::table::{CatalogName, TableRef};
use zippy_types::{BoxFuture, ErrorCode, QueryId, ResourceLimit, Result, ServerError, SessionId};

const TEST_MESSAGE_BYTES: usize = 64 * 1024;
const TEST_TRANSPORT_BYTES: usize = 4 * TEST_MESSAGE_BYTES;
const TEST_BOOTSTRAP_TOKEN: &str = "test-only-flight-bootstrap-token-0001";
const TEST_ADMIN_BOOTSTRAP_TOKEN: &str = "test-only-flight-admin-token-00000001";

struct FakeFrontend {
    next_generation: AtomicU64,
    lease_drops: Arc<AtomicUsize>,
    expires_after: Duration,
    hang_after_first: bool,
    poll_block: Option<PollBlock>,
    metadata_payload_bytes: usize,
}

impl FakeFrontend {
    fn new(lease_drops: Arc<AtomicUsize>, expires_after: Duration, hang_after_first: bool) -> Self {
        Self {
            next_generation: AtomicU64::new(1),
            lease_drops,
            expires_after,
            hang_after_first,
            poll_block: None,
            metadata_payload_bytes: 0,
        }
    }

    fn with_poll_block(
        mut self,
        started: Arc<AtomicBool>,
        release: Arc<(Mutex<bool>, Condvar)>,
    ) -> Self {
        self.poll_block = Some(PollBlock { started, release });
        self
    }

    fn with_metadata_payload_bytes(mut self, metadata_payload_bytes: usize) -> Self {
        self.metadata_payload_bytes = metadata_payload_bytes;
        self
    }
}

#[derive(Clone)]
struct PollBlock {
    started: Arc<AtomicBool>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl FlightQueryFrontend for FakeFrontend {
    fn prepare(
        &self,
        request: FlightQueryRequest,
    ) -> BoxFuture<'_, Result<OpaquePreparedFlightQuery>> {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let lease_drops = Arc::clone(&self.lease_drops);
        let expires_at_ns = unix_time_ns()
            .saturating_add(u64::try_from(self.expires_after.as_nanos()).unwrap_or(u64::MAX));
        let hang_after_first = self.hang_after_first;
        let poll_block = self.poll_block.clone();
        Box::pin(async move {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef],
            )
            .expect("build deterministic fake batch");
            let generation = NonZeroU64::new(generation)
                .expect("fake generation starts at one and does not wrap in tests");
            Ok(Box::new(FakePreparedQuery {
                instance_id: QueryInstanceId::new(request.request.query_id, generation),
                plan: QueryPlanInfo {
                    query_id: request.request.query_id,
                    catalog_revision: 7,
                    snapshots: Vec::new(),
                    planned_at_ns: unix_time_ns(),
                    expires_at_ns,
                },
                schema,
                batches: Some(VecDeque::from([batch])),
                lease: Some(DropCounter(lease_drops)),
                hang_after_first,
                poll_block,
            }) as OpaquePreparedFlightQuery)
        })
    }

    fn prepare_metadata(
        &self,
        request: FlightSqlMetadataRequest,
        limits: FlightSqlMetadataLimits,
    ) -> BoxFuture<'_, Result<PreparedFlightSqlMetadata>> {
        let metadata_payload_bytes = self.metadata_payload_bytes;
        Box::pin(async move {
            let batch = match request {
                FlightSqlMetadataRequest::Catalogs(command) => {
                    let mut builder = command.into_builder();
                    builder.append("zippy");
                    builder.build().map_err(metadata_build_error)?
                }
                FlightSqlMetadataRequest::DbSchemas(command) => {
                    let mut builder = command.into_builder();
                    builder.append("zippy", "default");
                    builder.build().map_err(metadata_build_error)?
                }
                FlightSqlMetadataRequest::Tables(command) => {
                    let mut builder = command.into_builder();
                    builder
                        .append(
                            "zippy",
                            "default",
                            "ticks",
                            "TABLE",
                            &Schema::new(vec![Field::new("value", DataType::Int64, false)]),
                        )
                        .map_err(metadata_build_error)?;
                    builder.build().map_err(metadata_build_error)?
                }
                FlightSqlMetadataRequest::TableTypes(command) => {
                    let mut builder = command.into_builder();
                    let table_type = if metadata_payload_bytes == 0 {
                        "TABLE".to_owned()
                    } else {
                        "T".repeat(metadata_payload_bytes)
                    };
                    builder.append(table_type);
                    builder.build().map_err(metadata_build_error)?
                }
            };
            PreparedFlightSqlMetadata::try_new(7, batch, limits)
        })
    }
}

fn metadata_build_error(error: impl std::fmt::Display) -> ServerError {
    ServerError::new(
        ErrorCode::Internal,
        format!("build test Flight SQL metadata failed error=[{error}]"),
    )
    .with_retryable(false)
}

struct FakePreparedQuery {
    instance_id: QueryInstanceId,
    plan: QueryPlanInfo,
    schema: SchemaRef,
    batches: Option<VecDeque<RecordBatch>>,
    lease: Option<DropCounter>,
    hang_after_first: bool,
    poll_block: Option<PollBlock>,
}

impl PreparedFlightQuery for FakePreparedQuery {
    fn info(&self) -> &QueryPlanInfo {
        &self.plan
    }

    fn instance_id(&self) -> QueryInstanceId {
        self.instance_id
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn execute(mut self: Box<Self>) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(FakeResultStream {
            instance_id: self.instance_id,
            schema: Arc::clone(&self.schema),
            batches: self
                .batches
                .take()
                .expect("fake prepared query executes at most once"),
            _lease: self
                .lease
                .take()
                .expect("fake prepared query transfers its lease once"),
            hang_after_first: self.hang_after_first,
            poll_block: self.poll_block.take(),
            emitted: 0,
        }))
    }
}

struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct FakeResultStream {
    instance_id: QueryInstanceId,
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    _lease: DropCounter,
    hang_after_first: bool,
    poll_block: Option<PollBlock>,
    emitted: usize,
}

impl RecordBatchStream for FakeResultStream {
    fn instance_id(&self) -> QueryInstanceId {
        self.instance_id
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn poll_next(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<QueryResultBatch>>> {
        if let Some(block) = self.poll_block.take() {
            block.started.store(true, Ordering::SeqCst);
            let (release, condition) = block.release.as_ref();
            let mut released = release
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !*released {
                released = condition
                    .wait(released)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
        if let Some(batch) = self.batches.pop_front() {
            self.emitted += 1;
            return Poll::Ready(Some(Ok(QueryResultBatch::unaccounted(batch))));
        }
        if self.hang_after_first && self.emitted > 0 {
            return Poll::Pending;
        }
        Poll::Ready(None)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flight_round_trip_is_snapshot_fixed_and_ticket_is_single_use() {
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let (service, mut runtime) =
        start_service(Arc::clone(&lease_drops), Duration::from_secs(5), false, 4);
    let session = SessionId::from_u128(11).unwrap();
    let mut client = client(&runtime, session).await;

    let info = client
        .get_flight_info(descriptor(session, QueryId::from_u128(12).unwrap()))
        .await
        .expect("GetFlightInfo succeeds");
    assert_eq!(info.clone().try_decode_schema().unwrap().fields().len(), 1);
    let ticket = info.endpoint[0]
        .ticket
        .clone()
        .expect("query endpoint carries a ticket");
    let batches: Vec<RecordBatch> = client
        .do_get(ticket.clone())
        .await
        .expect("DoGet starts")
        .try_collect()
        .await
        .expect("Flight batches decode");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);

    let error = client
        .do_get(ticket)
        .await
        .expect_err("ticket cannot execute twice");
    assert_eq!(flight_error_code(error), ErrorCode::NotFound);
    assert_eq!(service.status().pending_tickets, 0);
    wait_for_count(&lease_drops, 1).await;
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_handshake_issues_authoritative_session_and_rejects_spoofing() {
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let frontend = Arc::new(FakeFrontend::new(
        Arc::clone(&lease_drops),
        Duration::from_secs(5),
        false,
    ));
    let (service, mut runtime) =
        start_authenticated_frontend(frontend, 4, Duration::from_secs(60), 4);
    let (mut client, session, bearer_token) =
        authenticated_client(&runtime, TEST_BOOTSTRAP_TOKEN).await;
    assert_eq!(service.status().active_sessions, 1);
    assert_eq!(service.status().opened_sessions, 1);

    let spoofed_value = if session.as_u128() == 1 {
        2
    } else {
        session.as_u128() - 1
    };
    let spoofed = SessionId::from_u128(spoofed_value).unwrap();
    let error = client
        .get_flight_info(descriptor(spoofed, QueryId::from_u128(101).unwrap()))
        .await
        .expect_err("client-chosen session cannot replace the server authority");
    assert_eq!(flight_error_code(error), ErrorCode::PermissionDenied);

    let info = client
        .get_flight_info(descriptor(session, QueryId::from_u128(102).unwrap()))
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    client
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            "Bearer zfs1_ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        )
        .unwrap();
    let error = client
        .do_get(ticket.clone())
        .await
        .expect_err("invalid bearer cannot consume a ticket");
    assert_eq!(flight_error_code(error), ErrorCode::Unauthenticated);
    assert_eq!(service.status().pending_tickets, 1);

    client
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            &format!("Bearer {bearer_token}"),
        )
        .unwrap();
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 3);
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_sessions_are_bounded_and_expiry_revokes_pending_tickets() {
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let frontend = Arc::new(FakeFrontend::new(
        Arc::clone(&lease_drops),
        Duration::from_secs(5),
        false,
    ));
    let (service, mut runtime) =
        start_authenticated_frontend(frontend, 4, Duration::from_secs(1), 1);
    let (mut client, session, bearer_token) =
        authenticated_client(&runtime, TEST_BOOTSTRAP_TOKEN).await;
    let info = client
        .get_flight_info(descriptor(session, QueryId::from_u128(111).unwrap()))
        .await
        .unwrap();
    assert_eq!(service.status().pending_tickets, 1);

    let mut second = raw_client(&runtime).await;
    second
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            &format!("Bearer {TEST_BOOTSTRAP_TOKEN}"),
        )
        .unwrap();
    let error = second
        .handshake(
            SessionOpenRequest {
                protocol_version: CURRENT_PROTOCOL_VERSION,
            }
            .encode_to_vec(),
        )
        .await
        .expect_err("session capacity is enforced");
    assert_eq!(flight_error_code(error), ErrorCode::ResourceExhausted);

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let _ = service.sweep_expired();
    wait_for_count(&lease_drops, 1).await;
    assert_eq!(service.status().active_sessions, 0);
    assert_eq!(service.status().pending_tickets, 0);
    assert_eq!(service.status().session_revoked_tickets, 1);
    assert_eq!(service.status().session_capacity_rejections, 1);

    client
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            &format!("Bearer {bearer_token}"),
        )
        .unwrap();
    let error = client
        .do_get(info.endpoint[0].ticket.clone().unwrap())
        .await
        .expect_err("expired session cannot consume its former ticket");
    assert_eq!(flight_error_code(error), ErrorCode::Unauthenticated);

    let _ = authenticated_client(&runtime, TEST_BOOTSTRAP_TOKEN).await;
    assert_eq!(service.status().active_sessions, 1);
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_handshake_does_not_wait_for_client_stream_eof() {
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let frontend = Arc::new(FakeFrontend::new(
        lease_drops,
        Duration::from_secs(5),
        false,
    ));
    let (_service, mut runtime) =
        start_authenticated_frontend(frontend, 4, Duration::from_secs(60), 4);
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", runtime.endpoint()))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = RawFlightClient::new(channel);
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    sender
        .send(HandshakeRequest {
            protocol_version: 0,
            payload: SessionOpenRequest {
                protocol_version: CURRENT_PROTOCOL_VERSION,
            }
            .encode_to_vec()
            .into(),
        })
        .await
        .unwrap();
    let mut request = tonic::Request::new(ReceiverStream::new(receiver));
    request.metadata_mut().insert(
        AUTHORIZATION_METADATA_KEY,
        format!("Bearer {TEST_BOOTSTRAP_TOKEN}").parse().unwrap(),
    );

    let response = tokio::time::timeout(Duration::from_secs(1), client.handshake(request))
        .await
        .expect("server responds without waiting for the client stream EOF")
        .unwrap();
    let mut responses = response.into_inner();
    let opened = responses.message().await.unwrap().unwrap();
    assert_eq!(opened.protocol_version, u64::from(CURRENT_PROTOCOL_VERSION));
    assert!(SessionOpenResponse::decode(opened.payload).is_ok());
    drop(sender);
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_listener_rejects_plaintext_and_untrusted_clients() {
    let material = test_tls_material();
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let frontend = Arc::new(FakeFrontend::new(
        lease_drops,
        Duration::from_secs(5),
        false,
    ));
    let (_service, mut runtime) = start_tls_frontend(frontend, &material, false);

    let plaintext_channel =
        tonic::transport::Endpoint::from_shared(format!("http://{}", runtime.endpoint()))
            .unwrap()
            .connect()
            .await;
    if let Ok(plaintext_channel) = plaintext_channel {
        let mut plaintext = FlightClient::new(plaintext_channel);
        assert!(plaintext.list_actions().await.is_err());
    }

    let untrusted = tonic::transport::Endpoint::from_shared(format!(
        "https://127.0.0.1:{}",
        runtime.endpoint().port()
    ))
    .unwrap()
    .tls_config(ClientTlsConfig::new().domain_name("localhost"))
    .unwrap()
    .connect()
    .await;
    assert!(untrusted.is_err());

    let mut trusted = tls_client(&runtime, &material, false).await;
    assert!(trusted.list_actions().await.is_ok());
    assert!(runtime.tls_status().is_some());
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn invalid_tls_identity_fails_before_startup_readiness() {
    let frontend = Arc::new(FakeFrontend::new(
        Arc::new(AtomicUsize::new(0)),
        Duration::from_secs(5),
        false,
    ));
    let service = ZippyFlightService::try_new(service_config(4), frontend).unwrap();
    let tls = FlightServerTlsConfig::try_new(
        b"not-a-certificate".to_vec(),
        b"not-a-private-key".to_vec(),
        None,
        false,
    )
    .unwrap();

    let error = FlightServerRuntime::start_with_tls(
        "127.0.0.1:0".parse().unwrap(),
        service,
        tls,
        Duration::from_secs(5),
    )
    .err()
    .expect("invalid TLS identity must fail listener startup");

    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    assert!(error
        .message()
        .contains("configure Flight TLS identity failed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutual_tls_requires_a_trusted_client_identity() {
    let material = test_tls_material();
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let frontend = Arc::new(FakeFrontend::new(
        lease_drops,
        Duration::from_secs(5),
        false,
    ));
    let (_service, mut runtime) = start_tls_frontend(frontend, &material, true);

    let without_identity = tls_channel(&runtime, &material, false).await;
    if let Ok(channel) = without_identity {
        let mut client = FlightClient::new(channel);
        assert!(client.list_actions().await.is_err());
    }
    let mut with_identity = tls_client(&runtime, &material, true).await;
    assert!(with_identity.list_actions().await.is_ok());
    assert!(runtime.tls_status().unwrap().client_certificate_required());
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn certificate_rotation_takes_effect_after_bounded_listener_restart() {
    let first = test_tls_material();
    let second = test_tls_material();
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let frontend = Arc::new(FakeFrontend::new(
        Arc::clone(&lease_drops),
        Duration::from_secs(5),
        false,
    ));
    let (_service, mut runtime) = start_tls_frontend(frontend, &first, false);
    let endpoint = runtime.endpoint();
    let first_fingerprint = runtime
        .tls_status()
        .unwrap()
        .certificate_sha256()
        .to_owned();
    assert!(tls_client(&runtime, &first, false)
        .await
        .list_actions()
        .await
        .is_ok());
    runtime.shutdown(Duration::from_secs(5)).unwrap();

    let frontend = Arc::new(FakeFrontend::new(
        lease_drops,
        Duration::from_secs(5),
        false,
    ));
    let config = service_config(4);
    let service = ZippyFlightService::try_new(config, frontend).unwrap();
    let tls = FlightServerTlsConfig::try_new(
        second.server_chain.clone(),
        second.server_key.clone(),
        None,
        false,
    )
    .unwrap();
    let mut runtime =
        FlightServerRuntime::start_with_tls(endpoint, service, tls, Duration::from_secs(5))
            .unwrap();
    assert_ne!(
        runtime.tls_status().unwrap().certificate_sha256(),
        first_fingerprint
    );
    assert!(tls_channel(&runtime, &first, false).await.is_err());
    assert!(tls_client(&runtime, &second, false)
        .await
        .list_actions()
        .await
        .is_ok());
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_session_does_not_consume_ticket() {
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let (_service, mut runtime) =
        start_service(Arc::clone(&lease_drops), Duration::from_secs(5), false, 4);
    let owner = SessionId::from_u128(21).unwrap();
    let mut owner_client = client(&runtime, owner).await;
    let info = owner_client
        .get_flight_info(descriptor(owner, QueryId::from_u128(22).unwrap()))
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let mut foreign_client = client(&runtime, SessionId::from_u128(23).unwrap()).await;

    let error = foreign_client
        .do_get(ticket.clone())
        .await
        .expect_err("foreign session is rejected");
    assert_eq!(flight_error_code(error), ErrorCode::PermissionDenied);
    let batches: Vec<RecordBatch> = owner_client
        .do_get(ticket)
        .await
        .expect("owner can still consume ticket")
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 3);
    wait_for_count(&lease_drops, 1).await;
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unconsumed_ticket_expires_and_releases_prepared_query() {
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let (service, mut runtime) = start_service(
        Arc::clone(&lease_drops),
        Duration::from_millis(50),
        false,
        4,
    );
    let session = SessionId::from_u128(31).unwrap();
    let mut client = client(&runtime, session).await;
    let info = client
        .get_flight_info(descriptor(session, QueryId::from_u128(32).unwrap()))
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();

    wait_for_count(&lease_drops, 1).await;
    assert_eq!(service.status().pending_tickets, 0);
    let error = client
        .do_get(ticket)
        .await
        .expect_err("expired ticket is no longer retained");
    assert_eq!(flight_error_code(error), ErrorCode::DeadlineExceeded);
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capabilities_and_pending_ticket_capacity_are_enforced() {
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let (service, mut runtime) =
        start_service(Arc::clone(&lease_drops), Duration::from_secs(5), false, 1);
    let session = SessionId::from_u128(35).unwrap();
    let mut client = client(&runtime, session).await;

    let actions = client
        .list_actions()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].r#type, CAPABILITIES_ACTION);
    let bodies = client
        .do_action(Action::new(CAPABILITIES_ACTION, Vec::<u8>::new()))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let capabilities = CapabilitiesResponse::decode(bodies[0].clone()).unwrap();
    assert_eq!(capabilities.protocol_version, CURRENT_PROTOCOL_VERSION);
    assert_eq!(capabilities.max_pending_tickets, 1);
    assert!(capabilities
        .capabilities
        .iter()
        .any(|capability| capability == "query.single_use_ticket.v1"));

    let first = client
        .get_flight_info(descriptor(session, QueryId::from_u128(36).unwrap()))
        .await
        .unwrap();
    let error = client
        .get_flight_info(descriptor(session, QueryId::from_u128(37).unwrap()))
        .await
        .expect_err("one unconsumed ticket exhausts the configured capacity");
    assert_eq!(flight_error_code(error), ErrorCode::ResourceExhausted);
    assert_eq!(service.status().pending_tickets, 1);
    assert_eq!(service.status().admission_rejections, 1);

    client
        .do_get(first.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect::<Vec<RecordBatch>>()
        .await
        .unwrap();
    let second = client
        .get_flight_info(descriptor(session, QueryId::from_u128(37).unwrap()))
        .await
        .expect("consuming a ticket reopens one bounded slot");
    assert_eq!(service.status().pending_tickets, 1);

    drop(client);
    runtime.shutdown(Duration::from_secs(5)).unwrap();
    drop(second);
    wait_for_count(&lease_drops, 2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standard_cancel_is_owner_bound_and_fenced_by_exact_ticket_identity() {
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let frontend = Arc::new(FakeFrontend::new(
        Arc::clone(&lease_drops),
        Duration::from_secs(5),
        false,
    ));
    let config = service_config(4).with_query_control();
    let (service, mut runtime) = start_frontend_with_config(frontend, config);
    let owner = SessionId::from_u128(61).unwrap();
    let mut owner_client = client(&runtime, owner).await;
    let info = owner_client
        .get_flight_info(descriptor(owner, QueryId::from_u128(62).unwrap()))
        .await
        .unwrap();

    let mut foreign_client = client(&runtime, SessionId::from_u128(63).unwrap()).await;
    let error = foreign_client
        .cancel_flight_info(CancelFlightInfoRequest::new(info.clone()))
        .await
        .expect_err("a read session cannot cancel another session's query");
    assert_eq!(flight_error_code(error), ErrorCode::PermissionDenied);
    assert_eq!(service.status().pending_tickets, 1);

    let mut stale_info = info.clone();
    let stale_endpoint = &mut stale_info.endpoint[0];
    let mut stale_ticket = decode_query_ticket(stale_endpoint.ticket.as_ref().unwrap()).unwrap();
    let mut foreign_instance = stale_ticket.server_instance_id.to_vec();
    foreign_instance[0] ^= 0xff;
    stale_ticket.server_instance_id = foreign_instance.into();
    stale_endpoint.ticket = Some(encode_query_ticket(&stale_ticket));
    let error = owner_client
        .cancel_flight_info(CancelFlightInfoRequest::new(stale_info))
        .await
        .expect_err("a ticket from another server incarnation is rejected");
    assert_eq!(flight_error_code(error), ErrorCode::NotFound);
    assert_eq!(service.status().pending_tickets, 1);

    let mut stale_info = info.clone();
    let stale_endpoint = &mut stale_info.endpoint[0];
    let mut stale_ticket = decode_query_ticket(stale_endpoint.ticket.as_ref().unwrap()).unwrap();
    stale_ticket.generation = stale_ticket.generation.saturating_add(1);
    stale_endpoint.ticket = Some(encode_query_ticket(&stale_ticket));
    let error = owner_client
        .cancel_flight_info(CancelFlightInfoRequest::new(stale_info))
        .await
        .expect_err("a different query generation cannot be cancelled through a stale ticket");
    assert_eq!(flight_error_code(error), ErrorCode::NotFound);
    assert_eq!(service.status().pending_tickets, 1);

    let result = owner_client
        .cancel_flight_info(CancelFlightInfoRequest::new(info.clone()))
        .await
        .unwrap();
    assert_eq!(result.status(), CancelStatus::Cancelled);
    wait_for_count(&lease_drops, 1).await;
    assert_eq!(service.status().pending_tickets, 0);
    assert_eq!(service.status().owner_cancellations, 1);
    let error = owner_client
        .do_get(info.endpoint[0].ticket.clone().unwrap())
        .await
        .expect_err("a cancelled pending ticket is no longer consumable");
    assert_eq!(flight_error_code(error), ErrorCode::NotFound);
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_administrator_session_can_cancel_an_exact_foreign_query() {
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let frontend = Arc::new(FakeFrontend::new(
        Arc::clone(&lease_drops),
        Duration::from_secs(5),
        false,
    ));
    let auth = FlightBearerAuthConfig::try_new(
        TEST_BOOTSTRAP_TOKEN.as_bytes(),
        Duration::from_secs(60),
        4,
    )
    .unwrap()
    .with_admin_token(TEST_ADMIN_BOOTSTRAP_TOKEN.as_bytes())
    .unwrap();
    let config = service_config(4)
        .with_bearer_auth(auth)
        .with_query_control();
    let (service, mut runtime) = start_frontend_with_config(frontend, config);
    let (mut owner_client, owner, _) = authenticated_client(&runtime, TEST_BOOTSTRAP_TOKEN).await;
    let info = owner_client
        .get_flight_info(descriptor(owner, QueryId::from_u128(72).unwrap()))
        .await
        .unwrap();
    let (mut administrator, administrator_session, _, role) =
        open_authenticated_client(&runtime, TEST_ADMIN_BOOTSTRAP_TOKEN).await;
    assert_eq!(role, SessionRole::Administrator);
    let error = administrator
        .get_flight_info(descriptor(
            administrator_session,
            QueryId::from_u128(73).unwrap(),
        ))
        .await
        .expect_err("administrator sessions are control-only");
    assert_eq!(flight_error_code(error), ErrorCode::PermissionDenied);

    let result = administrator
        .cancel_flight_info(CancelFlightInfoRequest::new(info.clone()))
        .await
        .unwrap();
    assert_eq!(result.status(), CancelStatus::Cancelled);
    wait_for_count(&lease_drops, 1).await;
    assert_eq!(service.status().active_administrator_sessions, 1);
    assert_eq!(service.status().administrator_cancellations, 1);
    assert_eq!(service.status().owner_cancellations, 0);
    let error = owner_client
        .do_get(info.endpoint[0].ticket.clone().unwrap())
        .await
        .expect_err("administrator cancellation revokes the owner's pending ticket");
    assert_eq!(flight_error_code(error), ErrorCode::NotFound);
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standard_flight_sql_client_reads_all_supported_metadata_commands() {
    let frontend = Arc::new(FakeFrontend::new(
        Arc::new(AtomicUsize::new(0)),
        Duration::from_secs(5),
        false,
    ));
    let limits =
        FlightSqlMetadataLimits::try_new(16, TEST_MESSAGE_BYTES, Duration::from_secs(5)).unwrap();
    let config = service_config(8)
        .with_query_control()
        .with_sql_metadata(limits);
    let (service, mut runtime) = start_frontend_with_config(frontend, config);
    let session = SessionId::from_u128(81).unwrap();
    let mut capability_client = client(&runtime, session).await;
    let bodies = capability_client
        .do_action(Action::new(CAPABILITIES_ACTION, Vec::<u8>::new()))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let capabilities = CapabilitiesResponse::decode(bodies[0].clone()).unwrap();
    assert!(capabilities
        .capabilities
        .iter()
        .any(|capability| capability == "query.cancel_flight_info.v1"));
    assert!(capabilities
        .capabilities
        .iter()
        .any(|capability| capability == "flight_sql.metadata.v1"));
    assert_eq!(capabilities.metadata_max_rows, 16);
    assert_eq!(
        capabilities.metadata_max_bytes,
        u64::try_from(TEST_MESSAGE_BYTES).unwrap()
    );
    assert!(!capabilities.administrator_authentication_enabled);
    let mut sql_client = FlightSqlServiceClient::new(channel(&runtime).await);
    sql_client.set_header(SESSION_METADATA_KEY, session.to_string());

    let catalogs = sql_client.get_catalogs().await.unwrap();
    let metadata = MetadataInfo::decode(catalogs.app_metadata.clone()).unwrap();
    assert_eq!(metadata.catalog_revision, 7);
    assert!(metadata.expires_at_ns > unix_time_ns());
    let mut cancel_client = client(&runtime, session).await;
    let result = cancel_client
        .cancel_flight_info(CancelFlightInfoRequest::new(catalogs.clone()))
        .await
        .unwrap();
    assert_eq!(result.status(), CancelStatus::NotCancellable);
    let batches: Vec<RecordBatch> = sql_client
        .do_get(catalogs.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    let schemas = sql_client
        .get_db_schemas(CommandGetDbSchemas {
            catalog: Some("zippy".to_owned()),
            db_schema_filter_pattern: Some("def%".to_owned()),
        })
        .await
        .unwrap();
    let tables = sql_client
        .get_tables(CommandGetTables {
            catalog: Some("zippy".to_owned()),
            db_schema_filter_pattern: Some("default".to_owned()),
            table_name_filter_pattern: Some("tick%".to_owned()),
            table_types: vec!["TABLE".to_owned()],
            include_schema: true,
        })
        .await
        .unwrap();
    let table_types = sql_client.get_table_types().await.unwrap();
    for info in [schemas, tables, table_types] {
        let batches: Vec<RecordBatch> = sql_client
            .do_get(info.endpoint[0].ticket.clone().unwrap())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }
    let status = service.status();
    assert_eq!(status.metadata_tickets_issued, 4);
    assert_eq!(status.metadata_tickets_consumed, 4);
    assert_eq!(status.pending_tickets, 0);
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_metadata_tickets_share_the_transport_byte_budget() {
    let frontend = Arc::new(
        FakeFrontend::new(Arc::new(AtomicUsize::new(0)), Duration::from_secs(5), false)
            .with_metadata_payload_bytes(40 * 1024),
    );
    let limits =
        FlightSqlMetadataLimits::try_new(4, TEST_MESSAGE_BYTES, Duration::from_secs(5)).unwrap();
    let config =
        FlightServiceConfig::try_new("test-0.5.0", TEST_MESSAGE_BYTES, 4, TEST_MESSAGE_BYTES)
            .unwrap()
            .with_sql_metadata(limits);
    let (service, mut runtime) = start_frontend_with_config(frontend, config);
    let session = SessionId::from_u128(91).unwrap();
    let mut client = client(&runtime, session).await;
    let descriptor =
        arrow_flight::FlightDescriptor::new_cmd(CommandGetTableTypes {}.as_any().encode_to_vec());
    let first = client.get_flight_info(descriptor.clone()).await.unwrap();
    assert_eq!(
        service.status().transport_buffer_bytes_capacity,
        TEST_MESSAGE_BYTES
    );
    assert!(service.status().transport_buffer_bytes_in_use > 0);
    let error = client
        .get_flight_info(descriptor.clone())
        .await
        .expect_err("two retained metadata batches exceed the shared byte budget");
    assert_eq!(flight_error_code(error), ErrorCode::ResourceExhausted);

    let batches: Vec<RecordBatch> = client
        .do_get(first.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(service.status().transport_buffer_bytes_in_use, 0);
    let reopened = client
        .get_flight_info(descriptor)
        .await
        .expect("consuming metadata returns its retained byte permit");
    assert_eq!(reopened.endpoint.len(), 1);
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_client_stream_releases_hanging_query() {
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let (_service, mut runtime) =
        start_service(Arc::clone(&lease_drops), Duration::from_secs(5), true, 4);
    let session = SessionId::from_u128(41).unwrap();
    let mut client = client(&runtime, session).await;
    let info = client
        .get_flight_info(descriptor(session, QueryId::from_u128(42).unwrap()))
        .await
        .unwrap();
    let mut stream = client
        .do_get(info.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("first batch arrives")
        .expect("stream has one batch")
        .expect("first batch decodes");
    assert_eq!(first.num_rows(), 3);

    drop(stream);
    drop(client);
    wait_for_count(&lease_drops, 1).await;
    runtime.shutdown(Duration::from_secs(5)).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_timeout_and_drop_do_not_wait_for_a_blocked_stream_poll() {
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let poll_started = Arc::new(AtomicBool::new(false));
    let poll_release = Arc::new((Mutex::new(false), Condvar::new()));
    let frontend = Arc::new(
        FakeFrontend::new(Arc::clone(&lease_drops), Duration::from_secs(5), false)
            .with_poll_block(Arc::clone(&poll_started), Arc::clone(&poll_release)),
    );
    let (service, mut runtime) = start_frontend(frontend, 4);
    let session = SessionId::from_u128(51).unwrap();
    let mut client = client(&runtime, session).await;
    let info = client
        .get_flight_info(descriptor(session, QueryId::from_u128(52).unwrap()))
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let poll = tokio::spawn(async move {
        let mut stream = client.do_get(ticket).await.unwrap();
        stream.next().await
    });
    wait_for_flag(&poll_started).await;

    let shutdown_started = Instant::now();
    let error = runtime
        .shutdown(Duration::from_millis(20))
        .expect_err("blocked worker exceeds the explicit shutdown deadline");
    assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
    assert!(shutdown_started.elapsed() < Duration::from_millis(200));
    assert!(!service.status().admission_open);

    let drop_started = Instant::now();
    drop(runtime);
    assert!(drop_started.elapsed() < Duration::from_millis(200));
    let (release, condition) = poll_release.as_ref();
    *release
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
    condition.notify_all();
    let _ = tokio::time::timeout(Duration::from_secs(2), poll)
        .await
        .expect("detached listener cleanup completes after the blocked poll");
    wait_for_count(&lease_drops, 1).await;
}

fn start_service(
    lease_drops: Arc<AtomicUsize>,
    expires_after: Duration,
    hang_after_first: bool,
    max_pending_tickets: usize,
) -> (ZippyFlightService, FlightServerRuntime) {
    let frontend = Arc::new(FakeFrontend::new(
        lease_drops,
        expires_after,
        hang_after_first,
    ));
    start_frontend(frontend, max_pending_tickets)
}

fn start_frontend(
    frontend: Arc<dyn FlightQueryFrontend>,
    max_pending_tickets: usize,
) -> (ZippyFlightService, FlightServerRuntime) {
    start_frontend_with_config(frontend, service_config(max_pending_tickets))
}

fn start_frontend_with_config(
    frontend: Arc<dyn FlightQueryFrontend>,
    config: FlightServiceConfig,
) -> (ZippyFlightService, FlightServerRuntime) {
    let service = ZippyFlightService::try_new(config, frontend).unwrap();
    let runtime = FlightServerRuntime::start(
        "127.0.0.1:0".parse().unwrap(),
        service.clone(),
        Duration::from_secs(5),
    )
    .unwrap();
    (service, runtime)
}

fn service_config(max_pending_tickets: usize) -> FlightServiceConfig {
    FlightServiceConfig::try_new(
        "test-0.5.0",
        TEST_MESSAGE_BYTES,
        max_pending_tickets,
        TEST_TRANSPORT_BYTES,
    )
    .unwrap()
}

struct TestTlsMaterial {
    server_chain: Vec<u8>,
    server_key: Vec<u8>,
    ca: Vec<u8>,
    client_chain: Vec<u8>,
    client_key: Vec<u8>,
}

fn test_tls_material() -> TestTlsMaterial {
    let mut ca_params = CertificateParams::new(Vec::new());
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca = rcgen::Certificate::from_params(ca_params).unwrap();
    let ca_pem = ca.serialize_pem().unwrap();

    let mut server_params = CertificateParams::new(vec!["localhost".to_owned()]);
    server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server = rcgen::Certificate::from_params(server_params).unwrap();
    let server_pem = server.serialize_pem_with_signer(&ca).unwrap();

    let mut client_params = CertificateParams::new(vec!["zippy-test-client".to_owned()]);
    client_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client = rcgen::Certificate::from_params(client_params).unwrap();
    let client_pem = client.serialize_pem_with_signer(&ca).unwrap();

    TestTlsMaterial {
        server_chain: format!("{server_pem}{ca_pem}").into_bytes(),
        server_key: server.serialize_private_key_pem().into_bytes(),
        ca: ca_pem.into_bytes(),
        client_chain: client_pem.into_bytes(),
        client_key: client.serialize_private_key_pem().into_bytes(),
    }
}

fn start_tls_frontend(
    frontend: Arc<dyn FlightQueryFrontend>,
    material: &TestTlsMaterial,
    require_client_certificate: bool,
) -> (ZippyFlightService, FlightServerRuntime) {
    let service = ZippyFlightService::try_new(service_config(4), frontend).unwrap();
    let tls = FlightServerTlsConfig::try_new(
        material.server_chain.clone(),
        material.server_key.clone(),
        require_client_certificate.then(|| material.ca.clone()),
        require_client_certificate,
    )
    .unwrap();
    let runtime = FlightServerRuntime::start_with_tls(
        "127.0.0.1:0".parse().unwrap(),
        service.clone(),
        tls,
        Duration::from_secs(5),
    )
    .unwrap();
    (service, runtime)
}

async fn tls_channel(
    runtime: &FlightServerRuntime,
    material: &TestTlsMaterial,
    with_client_identity: bool,
) -> std::result::Result<tonic::transport::Channel, tonic::transport::Error> {
    let mut tls = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(TonicCertificate::from_pem(&material.ca));
    if with_client_identity {
        tls = tls.identity(Identity::from_pem(
            &material.client_chain,
            &material.client_key,
        ));
    }
    tonic::transport::Endpoint::from_shared(format!(
        "https://127.0.0.1:{}",
        runtime.endpoint().port()
    ))
    .unwrap()
    .tls_config(tls)
    .unwrap()
    .connect()
    .await
}

async fn tls_client(
    runtime: &FlightServerRuntime,
    material: &TestTlsMaterial,
    with_client_identity: bool,
) -> FlightClient {
    FlightClient::new(
        tls_channel(runtime, material, with_client_identity)
            .await
            .unwrap(),
    )
}

fn start_authenticated_frontend(
    frontend: Arc<dyn FlightQueryFrontend>,
    max_pending_tickets: usize,
    session_ttl: Duration,
    max_sessions: usize,
) -> (ZippyFlightService, FlightServerRuntime) {
    let auth =
        FlightBearerAuthConfig::try_new(TEST_BOOTSTRAP_TOKEN.as_bytes(), session_ttl, max_sessions)
            .unwrap();
    let config = FlightServiceConfig::try_new(
        "test-0.5.0",
        TEST_MESSAGE_BYTES,
        max_pending_tickets,
        TEST_TRANSPORT_BYTES,
    )
    .unwrap()
    .with_bearer_auth(auth);
    let service = ZippyFlightService::try_new(config, frontend).unwrap();
    let runtime = FlightServerRuntime::start(
        "127.0.0.1:0".parse().unwrap(),
        service.clone(),
        Duration::from_secs(5),
    )
    .unwrap();
    (service, runtime)
}

async fn authenticated_client(
    runtime: &FlightServerRuntime,
    bootstrap_token: &str,
) -> (FlightClient, SessionId, String) {
    let (client, session_id, bearer_token, role) =
        open_authenticated_client(runtime, bootstrap_token).await;
    assert_eq!(role, SessionRole::Read);
    (client, session_id, bearer_token)
}

async fn open_authenticated_client(
    runtime: &FlightServerRuntime,
    bootstrap_token: &str,
) -> (FlightClient, SessionId, String, SessionRole) {
    let mut client = raw_client(runtime).await;
    client
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            &format!("Bearer {bootstrap_token}"),
        )
        .unwrap();
    let payload = client
        .handshake(
            SessionOpenRequest {
                protocol_version: CURRENT_PROTOCOL_VERSION,
            }
            .encode_to_vec(),
        )
        .await
        .unwrap();
    let opened = SessionOpenResponse::decode(payload).unwrap();
    assert_eq!(opened.protocol_version, CURRENT_PROTOCOL_VERSION);
    let role = SessionRole::try_from(opened.role).unwrap();
    let session_id = SessionId::from_bytes(opened.session_id.as_ref().try_into().unwrap()).unwrap();
    client
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            &format!("Bearer {}", opened.bearer_token),
        )
        .unwrap();
    (client, session_id, opened.bearer_token, role)
}

async fn raw_client(runtime: &FlightServerRuntime) -> FlightClient {
    FlightClient::new(channel(runtime).await)
}

async fn channel(runtime: &FlightServerRuntime) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(format!("http://{}", runtime.endpoint()))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

async fn client(runtime: &FlightServerRuntime, session: SessionId) -> FlightClient {
    let mut client = raw_client(runtime).await;
    client
        .add_header(SESSION_METADATA_KEY, &session.to_string())
        .unwrap();
    client
}

fn descriptor(session: SessionId, query_id: QueryId) -> arrow_flight::FlightDescriptor {
    let table = TableRef::Name {
        namespace: CatalogName::new("default").unwrap(),
        table: CatalogName::new("ticks").unwrap(),
    };
    let request = FlightQueryRequest {
        request: QueryRequest {
            query_id,
            session_id: session,
            input: QueryInput::Sql("select * from ticks".to_owned()),
            tables: vec![table.clone()],
            priority: QueryPriority::Interactive,
            deadline_ns: None,
            memory_limit_bytes: ResourceLimit::ServerDefault,
            spill_limit_bytes: ResourceLimit::Disabled,
            result_batch_rows: None,
        },
        bindings: vec![FlightTableBinding {
            sql_name: "ticks".to_owned(),
            table,
        }],
    };
    encode_query_descriptor(&encode_query_request(&request).unwrap())
}

fn flight_error_code(error: FlightError) -> ErrorCode {
    match error {
        FlightError::Tonic(status) => status_to_server_error(&status).code(),
        other => panic!("expected a structured tonic error, got {other}"),
    }
}

async fn wait_for_count(counter: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if counter.load(Ordering::Acquire) >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("resource owner is released within the cancellation deadline");
}

async fn wait_for_flag(flag: &AtomicBool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !flag.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("blocking stream poll starts before the shutdown assertion");
}

fn unix_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
