#![cfg(unix)]

#[allow(dead_code)]
mod support;

#[path = "server_runtime/transport_performance.rs"]
mod transport_performance;

use std::collections::BTreeMap;
use std::fs;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{BinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::CommandGetTables;
use arrow_flight::FlightClient;
use futures::TryStreamExt;
use prost::Message;
use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyUsagePurpose};
use tonic::transport::{Certificate as TonicCertificate, ClientTlsConfig};
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::{ControlEndpoint, DataDirectoryError, MasterClient, OwnedDataDirectory};
use zippy_master::daemon::MasterDaemonConfig;
use zippy_protocol::error::status_to_server_error;
use zippy_protocol::ingest::{encode_put_descriptor, put_batch_request_id};
use zippy_protocol::query::{
    encode_query_descriptor, encode_query_request, FlightQueryRequest, FlightTableBinding,
};
use zippy_protocol::wire::{
    MetadataInfo, PutBatchResult, PutCommand, PutDurability, PutMutation, QueryInfo,
    SessionOpenRequest, SessionOpenResponse, SessionRole,
};
use zippy_protocol::{AUTHORIZATION_METADATA_KEY, CURRENT_PROTOCOL_VERSION};
use zippy_server::{ServerMode, ServerRuntime, ServerRuntimeConfig, ServiceHealth};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogWrite,
    CreateNamespaceCommand, CreateTableCommand,
};
use zippy_types::query::{QueryInput, QueryPriority, QueryRequest};
use zippy_types::runtime::RuntimeRegistry;
use zippy_types::table::{
    AppendTableDefinition, CatalogName, SchemaIpcPayload, TableDefinition, TableRef,
};
use zippy_types::{
    ConnectionId, ErrorCode, NamespaceId, ProducerId, QueryId, RequestId, ResourceLimit, SessionId,
    TableId,
};

use support::wait_until_bindable;

fn reserve_tcp_addr() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn write_runtime_config(temp: &tempfile::TempDir) -> std::path::PathBuf {
    write_runtime_config_with_shutdown(temp, 1_000)
}

fn write_runtime_config_with_shutdown(
    temp: &tempfile::TempDir,
    shutdown_timeout_ms: u64,
) -> std::path::PathBuf {
    let config_path = temp.path().join("zippy.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[master]
shutdown_timeout_ms = {shutdown_timeout_ms}

[flight]
enabled = false

"#
        ),
    )
    .unwrap();
    config_path
}

fn runtime_config(temp: &tempfile::TempDir) -> ServerRuntimeConfig {
    let master =
        MasterDaemonConfig::new(ControlEndpoint::Unix(temp.path().join("zippy-server.sock")))
            .with_config_path(write_runtime_config(temp))
            .with_logging(
                std::env::temp_dir().join(format!(
                    "zippy-server-runtime-test-logs-{}",
                    std::process::id()
                )),
                "info",
                false,
            );
    ServerRuntimeConfig::new(master).with_startup_timeout(Duration::from_secs(3))
}

fn runtime_config_with_shutdown(
    temp: &tempfile::TempDir,
    shutdown_timeout_ms: u64,
) -> ServerRuntimeConfig {
    let master =
        MasterDaemonConfig::new(ControlEndpoint::Unix(temp.path().join("zippy-server.sock")))
            .with_config_path(write_runtime_config_with_shutdown(
                temp,
                shutdown_timeout_ms,
            ))
            .with_logging(
                std::env::temp_dir().join(format!(
                    "zippy-server-runtime-test-logs-{}",
                    std::process::id()
                )),
                "info",
                false,
            );
    ServerRuntimeConfig::new(master).with_startup_timeout(Duration::from_secs(3))
}

fn runtime_config_with_data_dir(
    temp: &tempfile::TempDir,
    data_dir: &std::path::Path,
) -> ServerRuntimeConfig {
    let config_path = temp.path().join("zippy-owned-data.toml");
    fs::write(
        &config_path,
        format!(
            "[server]\ndata_dir = {:?}\n\n[master]\nshutdown_timeout_ms = 1000\n\n[flight]\nenabled = false\n",
            data_dir.display().to_string()
        ),
    )
    .unwrap();
    let master =
        MasterDaemonConfig::new(ControlEndpoint::Unix(temp.path().join("zippy-server.sock")))
            .with_config_path(config_path)
            .with_logging(
                std::env::temp_dir().join(format!(
                    "zippy-server-runtime-test-logs-{}",
                    std::process::id()
                )),
                "info",
                false,
            );
    ServerRuntimeConfig::new(master).with_startup_timeout(Duration::from_secs(3))
}

fn runtime_config_with_native_flight_frontend(
    temp: &tempfile::TempDir,
    data_dir: &std::path::Path,
    flight_endpoint: std::net::SocketAddr,
) -> ServerRuntimeConfig {
    let config_path = temp.path().join("zippy-native-flight.toml");
    fs::write(
        &config_path,
        format!(
            concat!(
                "[server]\n",
                "data_dir = {:?}\n",
                "",
                "[catalog]\n",
                "\n",
                "[master]\n",
                "shutdown_timeout_ms = 3000\n\n",
                "[query]\n",
                "",
                "operator_memory_bytes = 268435456\n\n",
                "[query.resources]\n",
                "max_memory_bytes = 1073741824\n\n",
                "[flight]\n",
                "endpoint = {:?}\n",
                "max_message_bytes = 2097152\n",
                "max_pending_tickets = 16\n"
            ),
            data_dir.display().to_string(),
            flight_endpoint.to_string(),
        ),
    )
    .unwrap();
    let master =
        MasterDaemonConfig::new(ControlEndpoint::Unix(temp.path().join("zippy-server.sock")))
            .with_config_path(config_path)
            .with_logging(
                std::env::temp_dir().join(format!(
                    "zippy-server-runtime-test-logs-{}",
                    std::process::id()
                )),
                "info",
                false,
            );
    ServerRuntimeConfig::new(master).with_startup_timeout(Duration::from_secs(5))
}

fn append_table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

async fn seed_native_append_table(data_dir: &Path, stream_name: &str) {
    let schema = append_table_schema();
    let mut schema_ipc = Vec::new();
    let mut writer = StreamWriter::try_new(&mut schema_ipc, schema.as_ref()).unwrap();
    writer.finish().unwrap();
    drop(writer);

    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(data_dir).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    let actor = CatalogActor {
        kind: CatalogActorKind::Server,
        principal: "server-native-flight-ingest-test".to_owned(),
    };
    catalog
        .command(
            CatalogCommandEnvelopeV1::new(
                RequestId::from_u128(2_080).unwrap(),
                Some(0),
                actor.clone(),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(2_081).unwrap(),
                    name: CatalogName::default_namespace(),
                }),
            )
            .unwrap()
            .into(),
        )
        .await
        .unwrap();
    catalog
        .command(
            CatalogCommandEnvelopeV1::new(
                RequestId::from_u128(2_082).unwrap(),
                Some(1),
                actor,
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: TableId::from_u128(2_083).unwrap(),
                    namespace_id: NamespaceId::from_u128(2_081).unwrap(),
                    name: CatalogName::new(stream_name).unwrap(),
                    definition: TableDefinition::Append(AppendTableDefinition::default()),
                    schema_ipc: SchemaIpcPayload::new(schema_ipc).unwrap(),
                    config: BTreeMap::new(),
                }),
            )
            .unwrap()
            .into(),
        )
        .await
        .unwrap();
    catalog.close().unwrap();
}

async fn seed_native_append_values(data_dir: &Path, values: Vec<i64>) {
    use zippy_server::{NativeStorageRuntime, NativeStorageRuntimeConfig};
    use zippy_storage::{ParquetCandidateSpec, PersistCandidateInput, PersistRequest};
    use zippy_types::table::{
        MetadataMap, NativeDataFileProvenance, SourceSegmentIdentity, SourceSegmentLineage,
        SourceSegmentLineages,
    };
    use zippy_types::{FileId, LogOffset, OffsetRange, OffsetWatermark};

    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(data_dir).unwrap());
    let mut storage =
        NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
    let table_id = TableId::from_u128(2_083).unwrap();
    let last = LogOffset::new(u64::try_from(values.len() - 1).unwrap());
    let offsets = OffsetRange::new(LogOffset::new(0), last).unwrap();
    let provenance = NativeDataFileProvenance::new(
        offsets,
        SourceSegmentLineages::new(vec![SourceSegmentLineage {
            source: SourceSegmentIdentity {
                segment_id: 1,
                generation: 1,
                writer_epoch: 0,
            },
            offsets,
        }])
        .unwrap(),
    )
    .unwrap();
    let schema = append_table_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap();
    let candidate = PersistCandidateInput::new(
        ParquetCandidateSpec {
            file_id: FileId::from_u128(2_084).unwrap(),
            table_id,
            schema_version: 1,
            schema,
            provenance,
            partition: MetadataMap::new(),
            metadata: MetadataMap::new(),
            created_at_ns: 1_752_492_600_000_000_000,
        },
        vec![batch],
    )
    .unwrap();
    storage
        .table_actor(table_id)
        .unwrap()
        .persist(
            PersistRequest::new(
                RequestId::from_u128(2_085).unwrap(),
                table_id,
                1,
                0,
                vec![candidate],
                OffsetWatermark::Known(last),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(storage.shutdown(Duration::from_secs(5)).is_complete());
}

async fn read_flight_i64_with_token(
    endpoint: std::net::SocketAddr,
    bootstrap_token: &str,
    namespace: &str,
    stream_name: &str,
    query_id: QueryId,
) -> Vec<i64> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{endpoint}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut reader = FlightClient::new(channel);
    reader
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            &format!("Bearer {bootstrap_token}"),
        )
        .unwrap();
    let opened = SessionOpenResponse::decode(
        reader
            .handshake(
                SessionOpenRequest {
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                }
                .encode_to_vec(),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    let read_session =
        SessionId::from_bytes(opened.session_id.as_ref().try_into().unwrap()).unwrap();
    reader
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            &format!("Bearer {}", opened.bearer_token),
        )
        .unwrap();
    let table_ref = TableRef::Name {
        namespace: CatalogName::new(namespace).unwrap(),
        table: CatalogName::new(stream_name).unwrap(),
    };
    let query = FlightQueryRequest {
        fixed_snapshot_id: None,
        key_value_snapshot: false,
        request: QueryRequest {
            query_id,
            session_id: read_session,
            input: QueryInput::Sql("SELECT value FROM ticks ORDER BY value".to_owned()),
            tables: vec![table_ref.clone()],
            priority: QueryPriority::Interactive,
            deadline_ns: None,
            memory_limit_bytes: ResourceLimit::ServerDefault,
            spill_limit_bytes: ResourceLimit::Disabled,
            result_batch_rows: NonZeroU32::new(16),
        },
        bindings: vec![FlightTableBinding {
            sql_name: "ticks".to_owned(),
            table: table_ref,
        }],
    };
    let info = reader
        .get_flight_info(encode_query_descriptor(
            &encode_query_request(&query).unwrap(),
        ))
        .await
        .unwrap();
    reader
        .do_get(info.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .try_collect::<Vec<RecordBatch>>()
        .await
        .unwrap()
        .into_iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn control_only_runtime_is_ready_without_fabricating_query_service() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("zippy-server.sock");
    let mut runtime = ServerRuntime::start(runtime_config(&temp)).unwrap();
    let data_directory = runtime
        .data_directory()
        .expect("running server owns its data directory")
        .directory()
        .clone();

    assert!(matches!(
        data_directory.clone().acquire_process_ownership(),
        Err(DataDirectoryError::OwnershipHeld { .. })
    ));

    assert_eq!(runtime.mode(), ServerMode::ControlOnly);
    assert_eq!(
        runtime.control_endpoint(),
        &ControlEndpoint::Unix(socket_path)
    );
    assert!(runtime.native_storage_reconciliation_report().is_some());
    assert!(data_directory.root().join("catalog/catalog.db").exists());
    assert!(runtime.catalog().is_some());

    let health = runtime.health();
    assert!(health.readiness.is_ready());
    assert!(health.readiness.master_listener_ready);
    assert_eq!(health.services.catalog, ServiceHealth::Ready);
    assert_eq!(health.services.query, ServiceHealth::Unavailable);
    assert_eq!(health.services.end_to_end_liveness, Some(true));

    let mut client = MasterClient::connect_endpoint(runtime.control_endpoint().clone()).unwrap();
    client
        .register_process("control_only_runtime_probe")
        .unwrap();
    client.unregister_process().unwrap();

    let runtime_registry = runtime.runtime_registry();
    assert!(runtime_registry.snapshot().is_ok());
    runtime.begin_shutdown();
    assert_eq!(
        runtime_registry.snapshot().unwrap_err().code(),
        ErrorCode::Unavailable
    );

    let outcome = runtime.shutdown();
    assert!(outcome.is_clean(), "shutdown outcome: {outcome:?}");
    assert!(runtime.data_directory().is_none());
    data_directory
        .acquire_process_ownership()
        .expect("clean server shutdown releases data ownership");
    assert!(!temp.path().join("zippy-server.sock").exists());
}

#[test]
fn configured_native_storage_recovers_before_server_readiness() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let mut runtime = ServerRuntime::start(runtime_config_with_data_dir(&temp, &data_dir)).unwrap();

    let health = runtime.health();
    assert!(health.readiness.is_ready());
    assert!(health.readiness.native_catalog_ready);
    assert_eq!(health.services.catalog, ServiceHealth::Ready);
    assert!(health.services.capabilities.catalog_transactional_writes);
    assert!(health.services.capabilities.catalog_historical_snapshots);
    assert!(health.readiness.native_current_snapshot_files_ready);
    assert!(health.readiness.storage_reconciliation_complete);
    assert!(health.readiness.commit_recovery_complete);
    assert!(runtime
        .native_storage_reconciliation_report()
        .expect("enabled native storage must expose startup evidence")
        .has_no_observed_backlog());
    let physical = runtime
        .native_storage_catalog_file_verification_report()
        .expect("enabled native storage must expose physical verification evidence");
    assert_eq!(physical.table_count, 0);
    assert_eq!(physical.managed_file_count, 0);
    assert_eq!(physical.file_count, 0);
    assert!(data_dir.join("catalog/catalog.db").is_file());

    assert!(runtime.shutdown().is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_runtime_streams_real_native_snapshot_and_releases_ticket() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let values = (0_i64..2_048).collect::<Vec<_>>();
    seed_native_append_table(&data_dir, "events").await;
    seed_native_append_values(&data_dir, values.clone()).await;
    let flight_addr = reserve_tcp_addr();
    let mut runtime = ServerRuntime::start(runtime_config_with_native_flight_frontend(
        &temp,
        &data_dir,
        flight_addr,
    ))
    .expect("start Flight-only native query runtime");

    assert_eq!(runtime.mode(), ServerMode::CatalogAndQuery);
    assert!(runtime.native_query_engine().is_some());
    let flight_endpoint = flight_addr.to_string();
    assert_eq!(runtime.flight_endpoint(), Some(flight_endpoint.as_str()));
    let health = runtime.health();
    assert!(health.readiness.flight_configured);
    assert!(health.readiness.flight_listener_ready);
    assert!(health.readiness.flight_protocol_ready);
    assert_eq!(health.services.query, ServiceHealth::Ready);

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{flight_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let query_id = QueryId::from_u128(2_002).unwrap();
    let mut client = FlightClient::new(channel.clone());
    let payload = client
        .handshake(
            zippy_protocol::wire::SessionOpenRequest {
                protocol_version: CURRENT_PROTOCOL_VERSION,
            }
            .encode_to_vec(),
        )
        .await
        .unwrap();
    let opened = zippy_protocol::wire::SessionOpenResponse::decode(payload).unwrap();
    let session_id = SessionId::from_bytes(opened.session_id.as_ref().try_into().unwrap()).unwrap();
    let session_token = opened.bearer_token;
    client
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            &format!("Bearer {session_token}"),
        )
        .unwrap();
    let table = TableRef::Name {
        namespace: CatalogName::new("default").unwrap(),
        table: CatalogName::new("events").unwrap(),
    };
    let request = FlightQueryRequest {
        fixed_snapshot_id: None,
        key_value_snapshot: false,
        request: QueryRequest {
            query_id,
            session_id,
            input: QueryInput::Sql("SELECT value FROM events".to_owned()),
            tables: vec![table.clone()],
            priority: QueryPriority::Interactive,
            deadline_ns: None,
            memory_limit_bytes: ResourceLimit::ServerDefault,
            spill_limit_bytes: ResourceLimit::Disabled,
            result_batch_rows: NonZeroU32::new(256),
        },
        bindings: vec![FlightTableBinding {
            sql_name: "events".to_owned(),
            table,
        }],
    };
    let info = client
        .get_flight_info(encode_query_descriptor(
            &encode_query_request(&request).unwrap(),
        ))
        .await
        .expect("Flight GetFlightInfo prepares the real native plan");
    let query_info = QueryInfo::decode(info.app_metadata.clone()).unwrap();
    assert_eq!(query_info.query_id.as_ref(), &query_id.into_bytes());
    assert!(query_info.catalog_revision > 0);
    assert_eq!(query_info.snapshots.len(), 1);
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches: Vec<RecordBatch> = client
        .do_get(ticket.clone())
        .await
        .expect("Flight DoGet starts native execution")
        .try_collect()
        .await
        .expect("Flight native batches decode");
    let actual = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, values);

    let error = client
        .do_get(ticket)
        .await
        .expect_err("Flight ticket is consumed exactly once");
    let status = match error {
        FlightError::Tonic(status) => status,
        other => panic!("expected structured Flight status, got {other}"),
    };
    assert_eq!(status_to_server_error(&status).code(), ErrorCode::NotFound);

    let mut sql_client = FlightSqlServiceClient::new(channel);
    sql_client.set_header(
        AUTHORIZATION_METADATA_KEY,
        format!("Bearer {session_token}"),
    );
    let table_info = sql_client
        .get_tables(CommandGetTables {
            catalog: Some("zippy".to_owned()),
            db_schema_filter_pattern: Some("default".to_owned()),
            table_name_filter_pattern: Some("events".to_owned()),
            table_types: vec!["TABLE".to_owned()],
            include_schema: true,
        })
        .await
        .expect("standard Flight SQL GetTables prepares Catalog metadata");
    let metadata = MetadataInfo::decode(table_info.app_metadata.clone()).unwrap();
    assert_eq!(metadata.catalog_revision, query_info.catalog_revision);
    let table_batches: Vec<RecordBatch> = sql_client
        .do_get(table_info.endpoint[0].ticket.clone().unwrap())
        .await
        .expect("Flight SQL metadata DoGet starts")
        .try_collect()
        .await
        .expect("Flight SQL metadata batches decode");
    assert_eq!(table_batches.len(), 1);
    assert_eq!(table_batches[0].num_rows(), 1);
    let table_names = table_batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(table_names.value(0), "events");
    let table_schemas = table_batches[0]
        .column(4)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert!(!table_schemas.value(0).is_empty());

    assert_eq!(runtime.flight_status().unwrap().pending_tickets, 0);
    assert_eq!(runtime.flight_status().unwrap().consumed_tickets, 2);
    assert_eq!(runtime.flight_status().unwrap().metadata_tickets_issued, 1);
    assert_eq!(
        runtime.flight_status().unwrap().metadata_tickets_consumed,
        1
    );
    assert_eq!(
        runtime
            .native_query_engine()
            .unwrap()
            .process_memory_status()
            .reserved_bytes,
        0
    );

    assert!(runtime.shutdown().is_clean());
    wait_until_bindable(flight_addr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flight_writer_durable_put_uses_native_ingest_without_gateway() {
    use std::os::unix::fs::PermissionsExt;

    const ADMIN_TOKEN: &str = "test-only-flight-admin-token-000000001";
    const READ_TOKEN: &str = "test-only-flight-read-token-0000000001";
    const WRITER_TOKEN: &str = "test-only-flight-writer-token-00000001";

    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let flight_addr = reserve_tcp_addr();
    let config = runtime_config_with_native_flight_frontend(&temp, &data_dir, flight_addr);
    let config_path = config
        .master()
        .config_path
        .as_ref()
        .expect("Flight ingest test has a config file");
    let read_token_path = temp.path().join("flight-read.token");
    let admin_token_path = temp.path().join("flight-admin.token");
    fs::write(&admin_token_path, format!("{ADMIN_TOKEN}\n")).unwrap();
    fs::set_permissions(&admin_token_path, fs::Permissions::from_mode(0o600)).unwrap();
    let writer_token_path = temp.path().join("flight-writer.token");
    fs::write(&read_token_path, format!("{READ_TOKEN}\n")).unwrap();
    fs::write(&writer_token_path, format!("{WRITER_TOKEN}\n")).unwrap();
    fs::set_permissions(&read_token_path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::set_permissions(&writer_token_path, fs::Permissions::from_mode(0o600)).unwrap();

    let mut source = fs::read_to_string(config_path).unwrap();
    source.push_str(&format!(
        concat!(
            "\n[flight.auth]\n",
            "enabled = true\n",
            "token_file = {:?}\n",
            "writer_token_file = {:?}\n",
            "admin_token_file = {:?}\n",
            "session_ttl_ms = 60000\n",
            "max_sessions = 8\n\n",
            "[flight.ingest]\n",
            "enabled = true\n",
            "max_concurrent_puts = 2\n",
            "max_batches_per_put = 4\n",
            "max_rows_per_batch = 16\n",
            "max_rows_per_put = 32\n",
            "max_batch_bytes = 1048576\n",
            "max_bytes_per_put = 2097152\n"
        ),
        read_token_path.display().to_string(),
        writer_token_path.display().to_string(),
        admin_token_path.display().to_string(),
    ));
    fs::write(config_path, source).unwrap();

    let restart_config = config.clone();
    let mut runtime = ServerRuntime::start(config).expect("start Flight ingest runtime");
    let health = runtime.health();
    assert!(health.readiness.flight_protocol_ready);

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{flight_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut writer = FlightClient::new(channel);
    writer
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            &format!("Bearer {WRITER_TOKEN}"),
        )
        .unwrap();
    let response = writer
        .handshake(
            SessionOpenRequest {
                protocol_version: CURRENT_PROTOCOL_VERSION,
            }
            .encode_to_vec(),
        )
        .await
        .unwrap();
    let opened = SessionOpenResponse::decode(response).unwrap();
    assert_eq!(
        SessionRole::try_from(opened.role).unwrap(),
        SessionRole::Writer
    );
    let session_id = SessionId::from_bytes(opened.session_id.as_ref().try_into().unwrap()).unwrap();
    writer
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            &format!("Bearer {}", opened.bearer_token),
        )
        .unwrap();

    // Create through the same public Flight endpoint used by external clients.
    let mut admin = FlightClient::new(
        tonic::transport::Endpoint::from_shared(format!("http://{flight_addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap(),
    );
    admin
        .add_header(AUTHORIZATION_METADATA_KEY, &format!("Bearer {ADMIN_TOKEN}"))
        .unwrap();
    let opened = SessionOpenResponse::decode(
        admin
            .handshake(
                SessionOpenRequest {
                    protocol_version: CURRENT_PROTOCOL_VERSION,
                }
                .encode_to_vec(),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(opened.role, SessionRole::Administrator as i32);
    admin
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            &format!("Bearer {}", opened.bearer_token),
        )
        .unwrap();
    let namespace_id = NamespaceId::from_u128(2_081).unwrap().into_bytes();
    let namespace = zippy_protocol::wire::CreateNamespaceRequest {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: RequestId::from_u128(2_080)
            .unwrap()
            .into_bytes()
            .to_vec()
            .into(),
        namespace_id: namespace_id.to_vec().into(),
        name: "market".to_owned(),
    };
    let action = arrow_flight::Action {
        r#type: zippy_protocol::admin::CREATE_NAMESPACE_ACTION.to_owned(),
        body: namespace.encode_to_vec().into(),
    };
    assert!(
        writer.do_action(action.clone()).await.is_err(),
        "writer must not create namespaces"
    );
    let results = admin
        .do_action(action.clone())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    let repeated = admin
        .do_action(action)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(results, repeated);
    let schema = append_table_schema();
    let mut schema_ipc = Vec::new();
    let mut ipc = StreamWriter::try_new(&mut schema_ipc, schema.as_ref()).unwrap();
    ipc.finish().unwrap();
    drop(ipc);
    let table = zippy_protocol::wire::CreateTableRequest {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: RequestId::from_u128(2_082)
            .unwrap()
            .into_bytes()
            .to_vec()
            .into(),
        table_id: TableId::from_u128(2_083)
            .unwrap()
            .into_bytes()
            .to_vec()
            .into(),
        namespace_id: namespace_id.to_vec().into(),
        name: "flight_ingest_ticks".to_owned(),
        schema_ipc: schema_ipc.into(),
        definition: Some(
            zippy_protocol::wire::create_table_request::Definition::Append(
                zippy_protocol::wire::AppendTableOptions {
                    event_time_column: None,
                },
            ),
        ),
    };
    let created = admin
        .do_action(arrow_flight::Action {
            r#type: zippy_protocol::admin::CREATE_TABLE_ACTION.to_owned(),
            body: table.encode_to_vec().into(),
        })
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(created.len(), 1);
    let created = zippy_protocol::wire::CatalogMutationResult::decode(created[0].clone()).unwrap();
    assert_eq!(created.object_id, table.table_id);
    assert_eq!(created.schema_version, 1);

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![101_i64, 102, 103]))],
    )
    .unwrap();
    let put_id = RequestId::from_u128(2_101).unwrap();
    let producer_id = ProducerId::from_u128(2_102).unwrap();
    let connection_id = ConnectionId::from_u128(2_103).unwrap();
    let descriptor = encode_put_descriptor(&PutCommand {
        expected_table_id: None,
        namespace: "market".to_owned(),
        protocol_version: CURRENT_PROTOCOL_VERSION,
        session_id: session_id.into_bytes().to_vec().into(),
        put_id: put_id.into_bytes().to_vec().into(),
        stream_name: "flight_ingest_ticks".to_owned(),
        first_batch_sequence: 0,
        mutation: PutMutation::Append as i32,
        durability: PutDurability::Wal as i32,
        producer_id: producer_id.into_bytes().to_vec().into(),
        connection_id: connection_id.into_bytes().to_vec().into(),
        expected_writer_epoch: None,
    });
    let input = FlightDataEncoderBuilder::new()
        .with_schema(Arc::clone(&schema))
        .with_flight_descriptor(Some(descriptor))
        .build(futures::stream::iter([Ok(batch)]));
    let results = writer
        .do_put(input)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    let result = PutBatchResult::decode(results[0].app_metadata.clone()).unwrap();
    assert_eq!((result.batch_sequence, result.row_count), (0, 3));
    assert_eq!(result.durability_reached, PutDurability::Wal as i32);
    assert_eq!(result.producer_epoch, 1);
    assert_eq!(
        result.request_id.as_ref(),
        put_batch_request_id(put_id, 0).into_bytes()
    );
    assert_eq!(
        (result.first_log_offset, result.last_log_offset),
        (Some(0), Some(2))
    );
    assert_eq!(
        result.previous_log_offset.unwrap().state,
        zippy_protocol::wire::offset_watermark::State::Empty as i32
    );
    let committed = result.committed_log_offset.unwrap();
    assert_eq!(
        committed.state,
        zippy_protocol::wire::offset_watermark::State::Known as i32
    );
    assert_eq!(committed.offset, 2);

    let resumed_put_id = RequestId::from_u128(2_104).unwrap();
    let resumed_descriptor = encode_put_descriptor(&PutCommand {
        expected_table_id: None,
        namespace: "market".to_owned(),
        protocol_version: CURRENT_PROTOCOL_VERSION,
        session_id: session_id.into_bytes().to_vec().into(),
        put_id: resumed_put_id.into_bytes().to_vec().into(),
        stream_name: "flight_ingest_ticks".to_owned(),
        first_batch_sequence: 1,
        mutation: PutMutation::Append as i32,
        durability: PutDurability::Persisted as i32,
        producer_id: producer_id.into_bytes().to_vec().into(),
        connection_id: connection_id.into_bytes().to_vec().into(),
        expected_writer_epoch: Some(1),
    });
    let resumed_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![104_i64]))],
    )
    .unwrap();
    let resumed_input = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_flight_descriptor(Some(resumed_descriptor))
        .build(futures::stream::iter([Ok(resumed_batch)]));
    let resumed_results = writer
        .do_put(resumed_input)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(resumed_results.len(), 1);
    let resumed = PutBatchResult::decode(resumed_results[0].app_metadata.clone()).unwrap();
    assert_eq!((resumed.batch_sequence, resumed.row_count), (1, 1));
    assert_eq!(resumed.durability_reached, PutDurability::Persisted as i32);
    assert!(resumed
        .persisted_snapshot_version
        .is_some_and(|version| version > 0));
    assert_eq!(resumed.producer_epoch, 1);
    assert_eq!(
        resumed.request_id.as_ref(),
        put_batch_request_id(resumed_put_id, 0).into_bytes()
    );
    assert_eq!(
        (resumed.first_log_offset, resumed.last_log_offset),
        (Some(3), Some(3))
    );

    let values = read_flight_i64_with_token(
        flight_addr,
        READ_TOKEN,
        "market",
        "flight_ingest_ticks",
        QueryId::from_u128(2_105).unwrap(),
    )
    .await;
    assert_eq!(values, [101, 102, 103, 104]);

    let flight = runtime.flight_status().unwrap();
    assert_eq!(flight.active_writer_sessions, 1);
    assert_eq!(flight.puts_completed, 2);
    assert_eq!(flight.put_rows, 4);
    let native_ingest = runtime.native_ingest_status().unwrap();
    assert!(native_ingest.accepting);
    assert_eq!(native_ingest.write_batches_total, 2);
    assert_eq!(native_ingest.written_rows_total, 4);
    assert_eq!(native_ingest.wal_commits_total, 2);
    assert_eq!(native_ingest.writer_acquisitions_total, 1);
    assert_eq!(native_ingest.writer_resumes_total, 1);
    drop(writer);

    let outcome = runtime.shutdown();
    assert!(outcome.is_clean(), "shutdown outcome: {outcome:?}");
    wait_until_bindable(flight_addr);

    let mut recovered = ServerRuntime::start(restart_config)
        .expect("restart Flight ingest runtime and recover committed WAL");
    let recovered_values = read_flight_i64_with_token(
        flight_addr,
        READ_TOKEN,
        "market",
        "flight_ingest_ticks",
        QueryId::from_u128(2_106).unwrap(),
    )
    .await;
    assert_eq!(recovered_values, [101, 102, 103, 104]);
    let outcome = recovered.shutdown();
    assert!(
        outcome.is_clean(),
        "recovered shutdown outcome: {outcome:?}"
    );
    wait_until_bindable(flight_addr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_runtime_loads_protected_credentials_and_issues_tls_flight_session() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let flight_addr = reserve_tcp_addr();
    let config = runtime_config_with_native_flight_frontend(&temp, &data_dir, flight_addr);
    let config_path = config
        .master()
        .config_path
        .as_ref()
        .expect("secure Flight test has a config file");

    let mut ca_params = CertificateParams::new(Vec::new());
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
    ];
    let ca = rcgen::Certificate::from_params(ca_params).unwrap();
    let ca_pem = ca.serialize_pem().unwrap();
    let mut server_params = CertificateParams::new(vec!["localhost".to_owned()]);
    server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server = rcgen::Certificate::from_params(server_params).unwrap();
    let cert_path = temp.path().join("flight-server.pem");
    let key_path = temp.path().join("flight-server.key");
    let token_path = temp.path().join("flight.token");
    let admin_token_path = temp.path().join("flight-admin.token");
    fs::write(
        &cert_path,
        format!("{}{ca_pem}", server.serialize_pem_with_signer(&ca).unwrap()),
    )
    .unwrap();
    fs::write(&key_path, server.serialize_private_key_pem()).unwrap();
    fs::write(&token_path, "test-only-flight-bootstrap-token-0001\n").unwrap();
    fs::write(&admin_token_path, "test-only-flight-admin-token-00000001\n").unwrap();
    fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::set_permissions(&admin_token_path, fs::Permissions::from_mode(0o600)).unwrap();

    let mut source = fs::read_to_string(config_path).unwrap();
    source.push_str(&format!(
        concat!(
            "\n[flight.tls]\n",
            "enabled = true\n",
            "cert_file = {:?}\n",
            "key_file = {:?}\n\n",
            "[flight.auth]\n",
            "enabled = true\n",
            "token_file = {:?}\n",
            "admin_token_file = {:?}\n",
            "session_ttl_ms = 60000\n",
            "max_sessions = 8\n"
        ),
        cert_path.display().to_string(),
        key_path.display().to_string(),
        token_path.display().to_string(),
        admin_token_path.display().to_string(),
    ));
    fs::write(config_path, source).unwrap();

    let mut runtime = ServerRuntime::start(config).expect("start secure Flight runtime");
    let tls_status = runtime
        .flight_tls_status()
        .expect("secure Flight runtime exposes TLS status");
    assert_eq!(tls_status.certificate_sha256().len(), 64);
    assert!(!tls_status.client_certificate_required());
    let status = runtime.flight_status().unwrap();
    assert!(status.authentication_required);
    assert_eq!(status.active_sessions, 0);

    let channel = tonic::transport::Endpoint::from_shared(format!(
        "https://127.0.0.1:{}",
        flight_addr.port()
    ))
    .unwrap()
    .tls_config(
        ClientTlsConfig::new()
            .domain_name("localhost")
            .ca_certificate(TonicCertificate::from_pem(ca_pem)),
    )
    .unwrap()
    .connect()
    .await
    .unwrap();
    let mut client = FlightClient::new(channel.clone());
    client
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            "Bearer test-only-flight-bootstrap-token-0001",
        )
        .unwrap();
    let response = client
        .handshake(
            SessionOpenRequest {
                protocol_version: CURRENT_PROTOCOL_VERSION,
            }
            .encode_to_vec(),
        )
        .await
        .unwrap();
    let opened = SessionOpenResponse::decode(response).unwrap();
    assert_eq!(opened.protocol_version, CURRENT_PROTOCOL_VERSION);
    assert_eq!(
        SessionRole::try_from(opened.role).unwrap(),
        SessionRole::Read
    );
    assert!(SessionId::from_bytes(opened.session_id.as_ref().try_into().unwrap()).is_ok());
    assert!(opened.bearer_token.starts_with("zfs1_"));
    let mut administrator = FlightClient::new(channel);
    administrator
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            "Bearer test-only-flight-admin-token-00000001",
        )
        .unwrap();
    let response = administrator
        .handshake(
            SessionOpenRequest {
                protocol_version: CURRENT_PROTOCOL_VERSION,
            }
            .encode_to_vec(),
        )
        .await
        .unwrap();
    let opened = SessionOpenResponse::decode(response).unwrap();
    assert_eq!(
        SessionRole::try_from(opened.role).unwrap(),
        SessionRole::Administrator
    );
    assert_eq!(runtime.flight_status().unwrap().active_sessions, 2);
    assert_eq!(
        runtime
            .flight_status()
            .unwrap()
            .active_administrator_sessions,
        1
    );

    assert!(runtime.shutdown().is_clean());
    wait_until_bindable(flight_addr);
}

#[test]
fn corrupt_native_catalog_prevents_listener_bind_and_releases_ownership_after_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let ownership = zippy_core::OwnedDataDirectory::acquire_or_initialize(&data_dir).unwrap();
    fs::write(
        data_dir.join("catalog/catalog.db"),
        b"not a sqlite database",
    )
    .unwrap();
    drop(ownership);

    let error = match ServerRuntime::start(runtime_config_with_data_dir(&temp, &data_dir)) {
        Ok(mut runtime) => {
            let _ = runtime.shutdown();
            panic!("corrupt native Catalog unexpectedly started the server");
        }
        Err(error) => error,
    };

    assert!(error.to_string().to_ascii_lowercase().contains("catalog"));
    assert!(!temp.path().join("zippy-server.sock").exists());
    zippy_core::OwnedDataDirectory::acquire_or_initialize(&data_dir)
        .expect("failed native startup must finish cleanup before returning an error");
}

#[test]
fn different_endpoints_cannot_own_the_same_data_root() {
    let shared = tempfile::tempdir().unwrap();
    let first_temp = tempfile::tempdir().unwrap();
    let second_temp = tempfile::tempdir().unwrap();
    let data_dir = shared.path().join("database");
    let mut first =
        ServerRuntime::start(runtime_config_with_data_dir(&first_temp, &data_dir)).unwrap();

    let error = match ServerRuntime::start(runtime_config_with_data_dir(&second_temp, &data_dir)) {
        Ok(mut runtime) => {
            let _ = runtime.shutdown();
            panic!("second endpoint unexpectedly acquired an owned data root");
        }
        Err(error) => error,
    };

    assert_eq!(error.code(), ErrorCode::Conflict);
    assert!(error.message().contains("already owned by another process"));
    assert!(!second_temp.path().join("zippy-server.sock").exists());
    assert!(first.shutdown().is_clean());

    let mut replacement =
        ServerRuntime::start(runtime_config_with_data_dir(&second_temp, &data_dir)).unwrap();
    assert!(replacement.shutdown().is_clean());
}

#[test]
fn repeated_shutdown_retries_cleanup_and_preserves_timed_out_processes() {
    let temp = tempfile::tempdir().unwrap();
    let mut runtime = ServerRuntime::start(runtime_config_with_shutdown(&temp, 30))
        .expect("control-only runtime should start");
    let data_directory = runtime
        .data_directory()
        .expect("running server owns its data directory")
        .directory()
        .clone();
    let mut client = MasterClient::connect_endpoint(runtime.control_endpoint().clone()).unwrap();
    let process = client
        .register_process("server_shutdown_timeout_probe")
        .unwrap();

    let started = std::time::Instant::now();
    let first = runtime.shutdown();
    let elapsed = started.elapsed();
    let second = runtime.shutdown();

    assert_eq!(first.timed_out_processes, second.timed_out_processes);
    assert!(!second.is_clean());
    assert_eq!(first.timed_out_processes, vec![process]);
    assert!(!first.is_clean());
    assert!(runtime.data_directory().is_some());
    assert!(matches!(
        data_directory.acquire_process_ownership(),
        Err(DataDirectoryError::OwnershipHeld { .. })
    ));
    assert!(
        elapsed < Duration::from_millis(250),
        "elapsed=[{elapsed:?}]"
    );
}
