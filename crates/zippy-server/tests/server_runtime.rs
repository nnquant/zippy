#![cfg(unix)]

#[allow(dead_code)]
mod support;

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Cursor, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_flight::FlightClient;
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use prost::Message;
use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyUsagePurpose};
use tonic::transport::{Certificate as TonicCertificate, ClientTlsConfig};
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::{
    canonical_schema_hash, schema_metadata, ControlEndpoint, DataDirectoryError,
    ManagedDataDirectory, MasterClient, OwnedDataDirectory,
};
use zippy_master::daemon::MasterDaemonConfig;
use zippy_master::snapshot::{
    RegistrySnapshot, SnapshotDroppedTableRecord, SnapshotSourceRecord, SnapshotStore,
    SnapshotStreamRecord,
};
use zippy_protocol::error::status_to_server_error;
use zippy_protocol::query::{
    encode_query_descriptor, encode_query_request, FlightQueryRequest, FlightTableBinding,
};
use zippy_protocol::wire::{QueryInfo, SessionOpenRequest, SessionOpenResponse};
use zippy_protocol::{AUTHORIZATION_METADATA_KEY, CURRENT_PROTOCOL_VERSION, SESSION_METADATA_KEY};
use zippy_server::{
    LegacyCatalogMigrationDegradedReason, LegacyCatalogMigrationState,
    LegacyCatalogShadowShutdownOutcome, LegacyCatalogShadowState, LegacyServerMode,
    LegacyServerRuntime, LegacyServerRuntimeConfig, ServiceHealth,
};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogRead,
    CatalogSnapshotSelector, CatalogWrite, CreateNamespaceCommand,
    LegacyCatalogAdvanceCheckpointCommandV4, LegacyCatalogCheckpointModeV4,
    LegacyCatalogProjectionCommandV4, LegacyCatalogProjectionEnvelopeV4,
    LegacyCatalogProjectionOutcomeV4, LegacyCatalogProjectionPhaseV4,
    LegacyCatalogProjectionTableStateV4, LegacyFileViewContinuity, ListTablesRequest,
};
use zippy_types::query::{QueryInput, QueryPriority, QueryRequest};
use zippy_types::runtime::RuntimeRegistry;
use zippy_types::table::{CatalogName, DataFileLocation, TableRef};
use zippy_types::{ErrorCode, NamespaceId, QueryId, RequestId, ResourceLimit, SessionId};

use support::{reserve_tcp_addr, wait_until_bindable};

fn write_runtime_config(
    temp: &tempfile::TempDir,
    gateway_endpoint: Option<std::net::SocketAddr>,
) -> std::path::PathBuf {
    write_runtime_config_with_shutdown(temp, gateway_endpoint, 1_000)
}

fn write_runtime_config_with_shutdown(
    temp: &tempfile::TempDir,
    gateway_endpoint: Option<std::net::SocketAddr>,
    shutdown_timeout_ms: u64,
) -> std::path::PathBuf {
    let config_path = temp.path().join("zippy.toml");
    let gateway = gateway_endpoint.map_or_else(String::new, |endpoint| {
        format!(
            r#"
[gateway]
enabled = true
endpoint = "{endpoint}"
token = "phase1-test-token"
"#
        )
    });
    fs::write(
        &config_path,
        format!(
            r#"
[master]
shutdown_timeout_ms = {shutdown_timeout_ms}

{gateway}
"#
        ),
    )
    .unwrap();
    config_path
}

fn runtime_config(
    temp: &tempfile::TempDir,
    gateway_endpoint: Option<std::net::SocketAddr>,
) -> LegacyServerRuntimeConfig {
    let master =
        MasterDaemonConfig::new(ControlEndpoint::Unix(temp.path().join("zippy-server.sock")))
            .with_config_path(write_runtime_config(temp, gateway_endpoint))
            .with_logging(
                std::env::temp_dir().join(format!(
                    "zippy-server-runtime-test-logs-{}",
                    std::process::id()
                )),
                "info",
                false,
            );
    LegacyServerRuntimeConfig::new(master).with_startup_timeout(Duration::from_secs(3))
}

fn runtime_config_with_shutdown(
    temp: &tempfile::TempDir,
    gateway_endpoint: Option<std::net::SocketAddr>,
    shutdown_timeout_ms: u64,
) -> LegacyServerRuntimeConfig {
    let master =
        MasterDaemonConfig::new(ControlEndpoint::Unix(temp.path().join("zippy-server.sock")))
            .with_config_path(write_runtime_config_with_shutdown(
                temp,
                gateway_endpoint,
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
    LegacyServerRuntimeConfig::new(master).with_startup_timeout(Duration::from_secs(3))
}

fn runtime_config_with_data_dir(
    temp: &tempfile::TempDir,
    data_dir: &std::path::Path,
) -> LegacyServerRuntimeConfig {
    let config_path = temp.path().join("zippy-owned-data.toml");
    fs::write(
        &config_path,
        format!(
            "[server]\ndata_dir = {:?}\n\n[master]\nshutdown_timeout_ms = 1000\n",
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
    LegacyServerRuntimeConfig::new(master).with_startup_timeout(Duration::from_secs(3))
}

fn runtime_config_with_native_storage(
    temp: &tempfile::TempDir,
    data_dir: &std::path::Path,
) -> LegacyServerRuntimeConfig {
    let config_path = temp.path().join("zippy-native-storage.toml");
    fs::write(
        &config_path,
        format!(
            concat!(
                "[server]\n",
                "data_dir = {:?}\n",
                "native_storage_enabled = true\n\n",
                "[master]\n",
                "shutdown_timeout_ms = 1000\n"
            ),
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
    LegacyServerRuntimeConfig::new(master).with_startup_timeout(Duration::from_secs(3))
}

fn runtime_config_with_native_storage_and_gateway(
    temp: &tempfile::TempDir,
    data_dir: &std::path::Path,
    gateway_endpoint: std::net::SocketAddr,
) -> LegacyServerRuntimeConfig {
    let config_path = temp.path().join("zippy-native-storage-gateway.toml");
    fs::write(
        &config_path,
        format!(
            concat!(
                "[server]\n",
                "data_dir = {:?}\n",
                "native_storage_enabled = true\n\n",
                "[master]\n",
                "shutdown_timeout_ms = 1000\n\n",
                "[gateway]\n",
                "enabled = true\n",
                "endpoint = {:?}\n",
                "token = \"native-storage-rollback-test\"\n"
            ),
            data_dir.display().to_string(),
            gateway_endpoint.to_string()
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
    LegacyServerRuntimeConfig::new(master).with_startup_timeout(Duration::from_secs(3))
}

fn runtime_config_with_shadow_catalog(
    temp: &tempfile::TempDir,
    data_dir: &std::path::Path,
    gateway_endpoint: std::net::SocketAddr,
) -> LegacyServerRuntimeConfig {
    runtime_config_with_shadow_catalog_and_persist_root(temp, data_dir, gateway_endpoint, None)
}

fn runtime_config_with_native_query_frontend(
    temp: &tempfile::TempDir,
    data_dir: &std::path::Path,
    gateway_endpoint: std::net::SocketAddr,
) -> LegacyServerRuntimeConfig {
    let runtime = runtime_config_with_shadow_catalog(temp, data_dir, gateway_endpoint);
    let config_path = runtime
        .master()
        .config_path
        .as_ref()
        .expect("test runtime config has an explicit path");
    let mut source = fs::read_to_string(config_path).unwrap();
    source.push_str("\n[query]\nnative_enabled = true\n");
    fs::write(config_path, source).unwrap();
    runtime
}

fn runtime_config_with_native_query_frontend_and_persist_root(
    temp: &tempfile::TempDir,
    data_dir: &std::path::Path,
    gateway_endpoint: std::net::SocketAddr,
    persist_root: &Path,
) -> LegacyServerRuntimeConfig {
    let runtime = runtime_config_with_shadow_catalog_and_persist_root(
        temp,
        data_dir,
        gateway_endpoint,
        Some(persist_root),
    );
    let config_path = runtime
        .master()
        .config_path
        .as_ref()
        .expect("test runtime config has an explicit path");
    let mut source = fs::read_to_string(config_path).unwrap();
    source.push_str(concat!(
        "\n[query]\nnative_enabled = true\n",
        "operator_memory_bytes = 268435456\n\n",
        "[query.resources]\nmax_memory_bytes = 1073741824\n"
    ));
    fs::write(config_path, source).unwrap();
    runtime
}

fn runtime_config_with_native_flight_frontend_and_persist_root(
    temp: &tempfile::TempDir,
    data_dir: &std::path::Path,
    flight_endpoint: std::net::SocketAddr,
    persist_root: &Path,
) -> LegacyServerRuntimeConfig {
    let config_path = temp.path().join("zippy-native-flight.toml");
    fs::write(
        &config_path,
        format!(
            concat!(
                "[server]\n",
                "data_dir = {:?}\n",
                "native_storage_enabled = true\n\n",
                "[table.persist]\n",
                "data_dir = {:?}\n\n",
                "[catalog]\n",
                "backend = \"sqlite_shadow\"\n\n",
                "[master]\n",
                "shutdown_timeout_ms = 3000\n\n",
                "[query]\n",
                "native_enabled = true\n",
                "operator_memory_bytes = 268435456\n\n",
                "[query.resources]\n",
                "max_memory_bytes = 1073741824\n\n",
                "[flight]\n",
                "enabled = true\n",
                "endpoint = {:?}\n",
                "max_message_bytes = 2097152\n",
                "max_pending_tickets = 16\n"
            ),
            data_dir.display().to_string(),
            persist_root.display().to_string(),
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
    LegacyServerRuntimeConfig::new(master).with_startup_timeout(Duration::from_secs(5))
}

fn runtime_config_with_shadow_catalog_and_persist_root(
    temp: &tempfile::TempDir,
    data_dir: &std::path::Path,
    gateway_endpoint: std::net::SocketAddr,
    persist_root: Option<&Path>,
) -> LegacyServerRuntimeConfig {
    let config_path = temp.path().join("zippy-shadow-catalog.toml");
    let persist = persist_root.map_or_else(String::new, |root| {
        format!(
            "[table.persist]\ndata_dir = {:?}\n\n",
            root.display().to_string()
        )
    });
    let metadata_budget_bytes = if persist_root.is_some() {
        131_072
    } else {
        4_096
    };
    let max_comparison_metadata_bytes = if persist_root.is_some() {
        65_536
    } else {
        1_024
    };
    fs::write(
        &config_path,
        format!(
            concat!(
                "[server]\n",
                "data_dir = {:?}\n",
                "native_storage_enabled = true\n\n",
                "{persist}",
                "[catalog]\n",
                "backend = \"sqlite_shadow\"\n\n",
                "[catalog.runtime_snapshot_pins]\n",
                "max_active_pins = 7\n",
                "max_active_tables = 5\n\n",
                "[catalog.shadow]\n",
                "queue_capacity = 3\n",
                "metadata_budget_bytes = {}\n",
                "max_files_per_comparison = 8\n",
                "max_metadata_bytes_per_comparison = {}\n",
                "comparison_timeout_ms = 250\n",
                "dedup_capacity = 5\n\n",
                "[master]\n",
                "shutdown_timeout_ms = 1000\n\n",
                "[gateway]\n",
                "enabled = true\n",
                "endpoint = {:?}\n",
                "token = \"shadow-catalog-test\"\n"
            ),
            data_dir.display().to_string(),
            metadata_budget_bytes,
            max_comparison_metadata_bytes,
            gateway_endpoint.to_string(),
            persist = persist,
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
    LegacyServerRuntimeConfig::new(master).with_startup_timeout(Duration::from_secs(3))
}

fn append_table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn append_table_schema_with_custom_metadata() -> Arc<Schema> {
    let field_metadata = (0..3)
        .map(|index| (format!("field_key_{index}"), format!("field_value_{index}")))
        .collect::<HashMap<_, _>>();
    let schema_metadata = (0..3)
        .map(|index| {
            (
                format!("schema_key_{index}"),
                format!("schema_value_{index}"),
            )
        })
        .collect::<HashMap<_, _>>();
    Arc::new(Schema::new_with_metadata(
        vec![Field::new("value", DataType::Int64, false).with_metadata(field_metadata)],
        schema_metadata,
    ))
}

fn write_test_parquet(path: &Path, values: Vec<i64>) {
    let schema = append_table_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn seed_legacy_append_table(
    data_dir: &Path,
    persist_root: &Path,
    initial_file: &Path,
    row_count: usize,
) -> std::path::PathBuf {
    let schema = append_table_schema();
    let schema_hash = canonical_schema_hash(&schema);
    let canonical_root = fs::canonicalize(persist_root).unwrap();
    let canonical_file = fs::canonicalize(initial_file).unwrap();
    let ownership = OwnedDataDirectory::acquire_or_initialize(data_dir).unwrap();
    let snapshot_path = ownership
        .directory()
        .path(ManagedDataDirectory::Catalog)
        .join("legacy-master-registry.json");
    SnapshotStore::write(
        &snapshot_path,
        &RegistrySnapshot {
            catalog_revision: 1,
            streams: vec![SnapshotStreamRecord {
                stream_name: "events".to_owned(),
                schema: schema_metadata(&schema),
                schema_hash: schema_hash.clone(),
                data_path: "segment".to_owned(),
                descriptor_generation: 0,
                sealed_segments: Vec::new(),
                persisted_files: vec![serde_json::json!({
                    "persist_file_id": "events-0",
                    "file_path": canonical_file,
                    "persist_data_root": canonical_root,
                    "stream_name": "events",
                    "schema_hash": schema_hash,
                    "writer_epoch": 1,
                    "row_count": row_count,
                    "created_at": 1,
                })],
                persist_events: Vec::new(),
                persist_revision: 1,
                persisted_files_revision: 1,
                segment_reader_leases: Vec::new(),
                writer_epoch: 1,
                buffer_size: 64,
                frame_size: 64,
                status: "restored".to_owned(),
            }],
            sources: vec![SnapshotSourceRecord {
                source_name: "events_source".to_owned(),
                source_type: "test".to_owned(),
                process_id: "legacy_writer".to_owned(),
                output_stream: "events".to_owned(),
                config: serde_json::json!({
                    "zippy_table_kind": "append_table",
                    "persist_data_root": persist_root,
                }),
                status: "restored".to_owned(),
                metrics: serde_json::json!({}),
                writer_epoch: 1,
                revision: 1,
            }],
            catalog_sources: Vec::new(),
            dropped_tables: Vec::new(),
            engines: Vec::new(),
        },
    )
    .unwrap();
    drop(ownership);
    snapshot_path
}

fn write_gateway_request(stream: &mut TcpStream, header: &serde_json::Value) {
    let header = serde_json::to_vec(header).unwrap();
    stream
        .write_all(&(header.len() as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&0_u64.to_be_bytes()).unwrap();
    stream.write_all(&header).unwrap();
}

fn read_gateway_frame(stream: &mut TcpStream) -> (serde_json::Value, Vec<u8>) {
    let mut prefix = [0_u8; 12];
    stream.read_exact(&mut prefix).unwrap();
    let header_len = u32::from_be_bytes(prefix[..4].try_into().unwrap()) as usize;
    let payload_len = u64::from_be_bytes(prefix[4..].try_into().unwrap()) as usize;
    let mut header = vec![0_u8; header_len];
    stream.read_exact(&mut header).unwrap();
    let mut payload = vec![0_u8; payload_len];
    stream.read_exact(&mut payload).unwrap();
    (serde_json::from_slice(&header).unwrap(), payload)
}

fn gateway_stream_frames(
    endpoint: &str,
    header: serde_json::Value,
) -> Vec<(serde_json::Value, Vec<u8>)> {
    let mut stream = TcpStream::connect(endpoint).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write_gateway_request(&mut stream, &header);
    let mut frames = Vec::new();
    loop {
        let frame = read_gateway_frame(&mut stream);
        let kind = frame.0.get("kind").and_then(serde_json::Value::as_str);
        let terminal =
            frame.0["status"] == "error" || matches!(kind, Some("collect_end" | "query_explain"));
        frames.push(frame);
        if terminal {
            return frames;
        }
    }
}

fn gateway_i64_values(frames: &[(serde_json::Value, Vec<u8>)]) -> Vec<i64> {
    let mut values = Vec::new();
    for (_, payload) in frames
        .iter()
        .filter(|(header, _)| header["kind"] == "collect_chunk")
    {
        let reader = StreamReader::try_new(Cursor::new(payload), None).unwrap();
        for batch in reader {
            values.extend_from_slice(
                batch
                    .unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values(),
            );
        }
    }
    values
}

async fn seed_incompatible_native_catalog_revision(data_dir: &std::path::Path) {
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(data_dir).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    let command = CatalogCommandEnvelopeV1::new(
        RequestId::from_u128(701).unwrap(),
        Some(0),
        CatalogActor {
            kind: CatalogActorKind::Server,
            principal: "server-migration-incompatibility-test".to_owned(),
        },
        CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
            namespace_id: NamespaceId::from_u128(702).unwrap(),
            name: CatalogName::new("unrelated").unwrap(),
        }),
    )
    .unwrap()
    .into();
    catalog.command(command).await.unwrap();
    catalog.close().unwrap();
    drop(ownership);
}

#[tokio::test]
async fn catalog_only_runtime_is_ready_without_fabricating_query_service() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("zippy-server.sock");
    let mut runtime = LegacyServerRuntime::start(runtime_config(&temp, None)).unwrap();
    let data_directory = runtime
        .data_directory()
        .expect("running server owns its data directory")
        .directory()
        .clone();

    assert!(matches!(
        data_directory.clone().acquire_process_ownership(),
        Err(DataDirectoryError::OwnershipHeld { .. })
    ));

    assert_eq!(runtime.mode(), LegacyServerMode::CatalogOnly);
    assert_eq!(
        runtime.control_endpoint(),
        &ControlEndpoint::Unix(socket_path)
    );
    assert_eq!(runtime.gateway_endpoint(), None);
    assert!(runtime.query().is_none());
    assert!(!runtime.runtime_config().server.native_storage_enabled);
    assert!(runtime.native_storage_reconciliation_report().is_none());
    assert!(!data_directory.root().join("catalog/catalog.db").exists());
    assert_eq!(runtime.catalog().revision().await.unwrap(), 0);

    let health = runtime.health();
    assert!(health.readiness.is_ready());
    assert!(health.readiness.master_listener_ready);
    assert!(!health.readiness.gateway_configured);
    assert!(!health.readiness.gateway_listener_ready);
    assert!(!health.readiness.gateway_master_reachable);
    assert_eq!(health.services.catalog, ServiceHealth::Degraded);
    assert_eq!(health.services.query, ServiceHealth::Unavailable);
    assert_eq!(health.services.end_to_end_liveness, Some(true));

    let mut client = MasterClient::connect_endpoint(runtime.control_endpoint().clone()).unwrap();
    client
        .register_process("catalog_only_runtime_probe")
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
    let mut runtime =
        LegacyServerRuntime::start(runtime_config_with_native_storage(&temp, &data_dir)).unwrap();

    let health = runtime.health();
    assert!(health.readiness.is_ready());
    assert!(health.readiness.native_storage_configured);
    assert!(health.readiness.native_catalog_ready);
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
    assert_eq!(physical.external_legacy_file_count, 0);
    assert_eq!(physical.file_count, 0);
    assert!(data_dir.join("catalog/catalog.db").is_file());

    assert!(runtime.shutdown().is_clean());
}

#[test]
fn native_query_frontend_is_advertised_only_while_its_runtime_is_ready() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let gateway_endpoint = reserve_tcp_addr();
    let mut runtime = LegacyServerRuntime::start(runtime_config_with_native_query_frontend(
        &temp,
        &data_dir,
        gateway_endpoint,
    ))
    .unwrap();

    let health = runtime.health();
    assert!(health.readiness.is_ready());
    assert_eq!(health.services.query, ServiceHealth::Ready);
    assert!(health.services.capabilities.query_sql);
    assert!(health.services.capabilities.query_manifest_snapshot_pinning);
    assert_eq!(
        runtime.gateway_metrics().unwrap()["native_query_enabled"],
        true
    );

    runtime.begin_shutdown();
    let health = runtime.health();
    assert!(!health.services.capabilities.query_sql);
    assert!(!health.services.capabilities.query_manifest_snapshot_pinning);
    assert_eq!(health.services.query, ServiceHealth::Unavailable);
    assert!(runtime.shutdown().is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_gateway_query_matches_legacy_and_releases_on_budget_or_disconnect() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let persist_root = temp.path().join("persisted");
    fs::create_dir_all(&persist_root).unwrap();
    let parquet_path = persist_root.join("events.parquet");
    let values = (0_i64..2_048).collect::<Vec<_>>();
    write_test_parquet(&parquet_path, values.clone());
    seed_legacy_append_table(&data_dir, &persist_root, &parquet_path, values.len());
    let gateway_addr = reserve_tcp_addr();
    let mut runtime =
        LegacyServerRuntime::start(runtime_config_with_native_query_frontend_and_persist_root(
            &temp,
            &data_dir,
            gateway_addr,
            &persist_root,
        ))
        .expect("start native query Gateway over a migrated persisted table");
    let endpoint = runtime.gateway_endpoint().unwrap().to_string();

    let legacy = gateway_stream_frames(
        &endpoint,
        serde_json::json!({
            "kind": "collect_stream",
            "token": "shadow-catalog-test",
            "source": "events",
            "plan": [],
            "snapshot": true,
        }),
    );
    let explained = gateway_stream_frames(
        &endpoint,
        serde_json::json!({
            "kind": "query_explain",
            "token": "shadow-catalog-test",
            "query_id": QueryId::from_u128(999).unwrap(),
            "session_id": SessionId::from_u128(1000).unwrap(),
            "sql": concat!(
                "SELECT value, count(*) AS copies FROM events ",
                "GROUP BY value ORDER BY value LIMIT 10"
            ),
            "bindings": [{
                "sql_name": "events",
                "table": {"type": "name", "namespace": "default", "table": "events"},
            }],
        }),
    );
    assert_eq!(explained.len(), 1);
    assert_eq!(explained[0].0["kind"], "query_explain");
    assert_eq!(
        explained[0].0["explanation"]["output_fields"][0]["name"],
        "value"
    );
    assert!(
        explained[0].0["explanation"]["metrics"]["operator.sort.count"]
            .as_u64()
            .unwrap()
            >= 1
    );
    assert!(
        explained[0].0["explanation"]["metrics"]["operator.aggregate.count"]
            .as_u64()
            .unwrap()
            >= 1
    );
    let native = gateway_stream_frames(
        &endpoint,
        serde_json::json!({
            "kind": "query_sql",
            "token": "shadow-catalog-test",
            "query_id": QueryId::from_u128(1001).unwrap(),
            "session_id": SessionId::from_u128(1002).unwrap(),
            "sql": "SELECT value FROM events",
            "bindings": [{
                "sql_name": "events",
                "table": {"type": "name", "namespace": "default", "table": "events"},
            }],
        }),
    );
    assert_eq!(legacy.last().unwrap().0["kind"], "collect_end");
    assert_eq!(
        native.last().unwrap().0["kind"],
        "collect_end",
        "native frames: {native:?}"
    );
    assert_eq!(native[0].0["query_authority"], "native");
    assert!(
        native.last().unwrap().0["metrics"]["operator.scan.count"]
            .as_u64()
            .unwrap()
            >= 1
    );
    assert_eq!(
        native.last().unwrap().0["metrics"]["operator.memory_reserved_bytes"],
        0
    );
    assert_eq!(gateway_i64_values(&legacy), values);
    assert_eq!(gateway_i64_values(&native), values);

    let over_budget = gateway_stream_frames(
        &endpoint,
        serde_json::json!({
            "kind": "query_sql",
            "token": "shadow-catalog-test",
            "query_id": QueryId::from_u128(1003).unwrap(),
            "session_id": SessionId::from_u128(1004).unwrap(),
            "sql": "SELECT value FROM events ORDER BY value",
            "bindings": [{
                "sql_name": "events",
                "table": {"type": "name", "namespace": "default", "table": "events"},
            }],
            "memory_limit_bytes": {"type": "bytes", "value": 1},
        }),
    );
    assert_eq!(over_budget.len(), 1);
    assert_eq!(over_budget[0].0["kind"], "query_error");
    assert_eq!(over_budget[0].0["error_code"], "RESOURCE_EXHAUSTED");

    let query_id = QueryId::from_u128(1005).unwrap();
    let session_id = SessionId::from_u128(1006).unwrap();
    let mut disconnected = TcpStream::connect(&endpoint).unwrap();
    disconnected
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write_gateway_request(
        &mut disconnected,
        &serde_json::json!({
            "kind": "query_sql",
            "token": "shadow-catalog-test",
            "query_id": query_id,
            "session_id": session_id,
            "sql": concat!(
                "SELECT left_events.value AS left_value, ",
                "right_events.value AS right_value ",
                "FROM events AS left_events CROSS JOIN events AS right_events ",
                "ORDER BY left_value, right_value"
            ),
            "bindings": [{
                "sql_name": "events",
                "table": {"type": "name", "namespace": "default", "table": "events"},
            }],
            "result_batch_rows": 256,
        }),
    );
    let (start, _) = read_gateway_frame(&mut disconnected);
    assert_eq!(start["kind"], "collect_start", "start frame: {start}");
    let (first_chunk, _) = read_gateway_frame(&mut disconnected);
    assert_eq!(
        first_chunk["kind"], "collect_chunk",
        "chunk frame: {first_chunk}"
    );
    disconnected.shutdown(Shutdown::Both).unwrap();
    drop(disconnected);

    let engine = runtime.native_query_engine().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let gateway = runtime.gateway_metrics().unwrap();
        let pins = runtime
            .health()
            .catalog_runtime_snapshot_pins
            .expect("native query runtime exposes snapshot pin accounting");
        if gateway["native_query_requests_active"] == 0
            && gateway["native_query_transport_failures_total"]
                .as_u64()
                .unwrap()
                >= 1
            && engine.process_memory_status().reserved_bytes == 0
            && pins.active_pins == 0
            && pins.active_tables == 0
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "disconnected query did not release transport, operator, and snapshot resources"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(engine.process_memory_status().reserved_bytes, 0);
    let metrics = runtime.gateway_metrics().unwrap();
    assert!(
        metrics["native_query_transport_failures_total"]
            .as_u64()
            .unwrap()
            >= 1
    );

    assert!(runtime.shutdown().is_clean());
    wait_until_bindable(gateway_addr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flight_only_runtime_streams_real_native_snapshot_and_releases_ticket() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let persist_root = temp.path().join("persisted");
    fs::create_dir_all(&persist_root).unwrap();
    let parquet_path = persist_root.join("events.parquet");
    let values = (0_i64..2_048).collect::<Vec<_>>();
    write_test_parquet(&parquet_path, values.clone());
    seed_legacy_append_table(&data_dir, &persist_root, &parquet_path, values.len());
    let flight_addr = reserve_tcp_addr();
    let mut runtime =
        LegacyServerRuntime::start(runtime_config_with_native_flight_frontend_and_persist_root(
            &temp,
            &data_dir,
            flight_addr,
            &persist_root,
        ))
        .expect("start Flight-only native query runtime");

    assert_eq!(runtime.mode(), LegacyServerMode::CatalogAndQuery);
    assert!(runtime.gateway_endpoint().is_none());
    assert!(
        runtime.query().is_none(),
        "legacy Gateway adapter stays disabled"
    );
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
    let session_id = SessionId::from_u128(2_001).unwrap();
    let query_id = QueryId::from_u128(2_002).unwrap();
    let mut client = FlightClient::new(channel);
    client
        .add_header(SESSION_METADATA_KEY, &session_id.to_string())
        .unwrap();
    let table = TableRef::Name {
        namespace: CatalogName::new("default").unwrap(),
        table: CatalogName::new("events").unwrap(),
    };
    let request = FlightQueryRequest {
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
    assert_eq!(runtime.flight_status().unwrap().pending_tickets, 0);
    assert_eq!(runtime.flight_status().unwrap().consumed_tickets, 1);
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
async fn server_runtime_loads_protected_credentials_and_issues_tls_flight_session() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let persist_root = temp.path().join("persisted");
    fs::create_dir_all(&persist_root).unwrap();
    let flight_addr = reserve_tcp_addr();
    let config = runtime_config_with_native_flight_frontend_and_persist_root(
        &temp,
        &data_dir,
        flight_addr,
        &persist_root,
    );
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
    fs::write(
        &cert_path,
        format!("{}{ca_pem}", server.serialize_pem_with_signer(&ca).unwrap()),
    )
    .unwrap();
    fs::write(&key_path, server.serialize_private_key_pem()).unwrap();
    fs::write(&token_path, "test-only-flight-bootstrap-token-0001\n").unwrap();
    fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600)).unwrap();

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
            "session_ttl_ms = 60000\n",
            "max_sessions = 8\n"
        ),
        cert_path.display().to_string(),
        key_path.display().to_string(),
        token_path.display().to_string(),
    ));
    fs::write(config_path, source).unwrap();

    let mut runtime = LegacyServerRuntime::start(config).expect("start secure Flight runtime");
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
    let mut client = FlightClient::new(channel);
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
    assert!(SessionId::from_bytes(opened.session_id.as_ref().try_into().unwrap()).is_ok());
    assert!(opened.bearer_token.starts_with("zfs1_"));
    assert_eq!(runtime.flight_status().unwrap().active_sessions, 1);

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

    let error =
        match LegacyServerRuntime::start(runtime_config_with_native_storage(&temp, &data_dir)) {
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
        LegacyServerRuntime::start(runtime_config_with_data_dir(&first_temp, &data_dir)).unwrap();

    let error =
        match LegacyServerRuntime::start(runtime_config_with_data_dir(&second_temp, &data_dir)) {
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
        LegacyServerRuntime::start(runtime_config_with_data_dir(&second_temp, &data_dir)).unwrap();
    assert!(replacement.shutdown().is_clean());
}

#[test]
fn full_runtime_opens_admission_only_after_gateway_master_probe() {
    let temp = tempfile::tempdir().unwrap();
    let gateway_addr = reserve_tcp_addr();
    let mut runtime =
        LegacyServerRuntime::start(runtime_config(&temp, Some(gateway_addr))).unwrap();
    let expected_gateway_endpoint = gateway_addr.to_string();

    assert_eq!(runtime.mode(), LegacyServerMode::CatalogAndQuery);
    assert_eq!(
        runtime.gateway_endpoint(),
        Some(expected_gateway_endpoint.as_str())
    );
    assert!(runtime.query().is_some());

    let health = runtime.health();
    assert!(health.readiness.is_ready());
    assert!(health.readiness.admission_open);
    assert!(health.readiness.master_listener_ready);
    assert!(health.readiness.gateway_configured);
    assert!(health.readiness.gateway_listener_ready);
    assert!(health.readiness.gateway_master_reachable);
    assert_eq!(health.services.catalog, ServiceHealth::Degraded);
    assert_eq!(health.services.query, ServiceHealth::Degraded);
    assert_eq!(health.services.end_to_end_liveness, Some(true));

    runtime.begin_shutdown();
    let health = runtime.health();
    assert!(!health.readiness.admission_open);
    assert!(!health.readiness.is_ready());
    assert_eq!(health.services.end_to_end_liveness, Some(false));

    let outcome = runtime.shutdown();
    assert!(outcome.is_clean(), "shutdown outcome: {outcome:?}");
    wait_until_bindable(gateway_addr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_shadow_runtime_exposes_bounded_gateway_comparison_worker() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let gateway_addr = reserve_tcp_addr();
    let mut runtime = LegacyServerRuntime::start(runtime_config_with_shadow_catalog(
        &temp,
        &data_dir,
        gateway_addr,
    ))
    .expect("start server with SQLite Catalog shadow");

    let metrics = runtime
        .gateway_metrics()
        .expect("enabled Gateway must expose metrics");
    let shadow = metrics
        .get("persisted_read_shadow")
        .expect("shadow mode must install comparison metrics");
    assert_eq!(shadow["worker_running"], true);
    assert_eq!(shadow["queue_depth"], 0);
    assert_eq!(shadow["config"]["queue_capacity"], 3);
    assert_eq!(shadow["config"]["metadata_budget_bytes"], 4096);
    assert_eq!(shadow["config"]["comparison_timeout_ms"], 250);

    let command_shadow = runtime
        .health()
        .legacy_catalog_shadow
        .expect("SQLite shadow mode must expose command shadow status after its initial attempt");
    assert_eq!(
        command_shadow.state,
        LegacyCatalogShadowState::EligibleCandidate
    );
    assert_eq!(command_shadow.reconciled_catalog_revision, 0);
    assert_eq!(command_shadow.publisher.baseline_revision, 0);
    assert_eq!(command_shadow.publisher.continuity_loss_epoch, 0);

    let migration = runtime.legacy_catalog_migration_status();
    assert_eq!(migration.state, LegacyCatalogMigrationState::Complete);
    let pin_status = runtime
        .health()
        .catalog_runtime_snapshot_pins
        .expect("native Catalog pin status must be observable through server health");
    assert!(pin_status.admission_open);
    assert_eq!(pin_status.active_pins, 0);
    assert_eq!(pin_status.active_tables, 0);
    assert_eq!(pin_status.max_active_pins, 7);
    assert_eq!(pin_status.max_active_tables, 5);
    assert_eq!(pin_status.pin_budget_exhaustions_total, 0);
    assert_eq!(pin_status.table_budget_exhaustions_total, 0);
    let report = migration
        .report
        .as_ref()
        .expect("shadow startup must expose an auditable migration report");
    assert_eq!(report.submitted_commands, 1);
    assert_eq!(report.replayed_commands, 0);
    assert_eq!(report.target_revision_after, 1);
    assert_eq!(runtime.health().legacy_catalog_migration, migration.clone());
    let migration_metrics = runtime.legacy_catalog_migration_metrics();
    assert_eq!(migration_metrics["configured"], true);
    assert_eq!(migration_metrics["state"], "complete");
    assert_eq!(migration_metrics["completion_mode"], "legacy_import");
    assert_eq!(migration_metrics["checkpoint_reuses"], 0);
    assert_eq!(migration_metrics["submitted_commands"], 1);

    let shutdown = runtime.shutdown();
    assert!(shutdown.is_clean());
    assert_eq!(
        shutdown.legacy_catalog_shadow_outcome,
        Some(LegacyCatalogShadowShutdownOutcome::Complete)
    );
    wait_until_bindable(gateway_addr);

    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&data_dir).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    let read = catalog.read_handle().unwrap();
    let current = read.read_legacy_projection_checkpoint_v4().await.unwrap();
    let reconcile = catalog.legacy_reconcile_handle().unwrap();
    let target_shadow_epoch = current.shadow_epoch.checked_add(1).unwrap();
    let envelope = LegacyCatalogProjectionEnvelopeV4::new(
        RequestId::from_u128(811).unwrap(),
        CatalogActor {
            kind: CatalogActorKind::Server,
            principal: "server-runtime-loss-restart-test".to_owned(),
        },
        current.shadow_epoch,
        current.source_projection_hash.clone(),
        current.source_catalog_revision,
        LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(
            LegacyCatalogAdvanceCheckpointCommandV4 {
                expected_checkpoint_generation: current.checkpoint_generation,
                expected_source_catalog_revision: current.source_catalog_revision,
                expected_source_projection_hash: current.source_projection_hash.clone(),
                expected_phase: current.phase,
                shadow_epoch: target_shadow_epoch,
                phase: current.phase,
                mode: LegacyCatalogCheckpointModeV4::FullReconcile,
                target_projection_digest: current.projection_digest.clone(),
                observed_continuity_loss_epoch: 5,
            },
        ),
    )
    .unwrap();
    let response = reconcile
        .apply_legacy_catalog_projection_v4(envelope)
        .await
        .unwrap();
    let LegacyCatalogProjectionOutcomeV4::AdvanceCheckpoint(durable_checkpoint) = response.outcome
    else {
        panic!("expected injected durable checkpoint outcome");
    };
    assert_eq!(durable_checkpoint.resolved_continuity_loss_epoch, 5);
    drop(reconcile);
    drop(read);
    catalog.close().unwrap();
    drop(ownership);

    let mut restarted = LegacyServerRuntime::start(runtime_config_with_shadow_catalog(
        &temp,
        &data_dir,
        gateway_addr,
    ))
    .expect("restart server with the same SQLite Catalog shadow");
    let migration = restarted.legacy_catalog_migration_status();
    assert_eq!(migration.state, LegacyCatalogMigrationState::Complete);
    assert!(migration.report.is_none());
    assert!(migration.degradation.is_none());
    let metrics = restarted.legacy_catalog_migration_metrics();
    assert_eq!(metrics["completion_mode"], "v4_checkpoint");
    assert_eq!(metrics["checkpoint_reuses"], 1);
    assert_eq!(metrics["attempts"], 0);
    assert_eq!(metrics["completed_attempts"], 0);
    let restarted_shadow = restarted
        .health()
        .legacy_catalog_shadow
        .expect("restarted V4 shadow must expose durable continuity evidence");
    assert_eq!(
        restarted_shadow.state,
        LegacyCatalogShadowState::EligibleCandidate
    );
    assert_eq!(restarted_shadow.recovered_continuity_loss_epoch, 5);
    assert_eq!(
        restarted_shadow.recovered_worker_local_continuity_loss_epoch,
        0
    );
    assert_eq!(restarted_shadow.v4_checkpoint_total, 0);
    assert_eq!(
        restarted_shadow
            .checkpoint
            .as_ref()
            .expect("restarted shadow must retain the durable checkpoint")
            .checkpoint_generation,
        durable_checkpoint.checkpoint_generation
    );
    assert!(restarted.shutdown().is_clean());
    wait_until_bindable(gateway_addr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_snapshot_without_catalog_revision_rebuilds_v4_shadow_baseline() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let persist_root = temp.path().join("persisted");
    fs::create_dir_all(&persist_root).unwrap();
    let parquet_path = persist_root.join("events-0.parquet");
    write_test_parquet(&parquet_path, vec![1, 2]);
    let snapshot_path = seed_legacy_append_table(&data_dir, &persist_root, &parquet_path, 2);
    let mut snapshot =
        serde_json::from_slice::<serde_json::Value>(&fs::read(&snapshot_path).unwrap()).unwrap();
    snapshot
        .as_object_mut()
        .expect("registry snapshot must be an object")
        .remove("catalog_revision");
    fs::write(
        &snapshot_path,
        serde_json::to_vec_pretty(&snapshot).unwrap(),
    )
    .unwrap();

    let gateway_addr = reserve_tcp_addr();
    let mut runtime =
        LegacyServerRuntime::start(runtime_config_with_shadow_catalog_and_persist_root(
            &temp,
            &data_dir,
            gateway_addr,
            Some(&persist_root),
        ))
        .expect("start server from a pre-revision legacy snapshot");
    let shadow = runtime
        .health()
        .legacy_catalog_shadow
        .expect("restored V4 shadow status must be observable");
    assert_eq!(shadow.state, LegacyCatalogShadowState::EligibleCandidate);
    assert!(shadow.publisher.baseline_revision > 0);
    assert_eq!(
        shadow.reconciled_catalog_revision,
        shadow.publisher.baseline_revision
    );
    assert_eq!(shadow.v4_adopt_total, 1);
    assert_eq!(
        shadow
            .checkpoint
            .as_ref()
            .expect("restored shadow must expose a durable checkpoint")
            .source_catalog_revision,
        shadow.publisher.baseline_revision
    );

    let shutdown = runtime.shutdown();
    assert!(shutdown.is_clean(), "shutdown outcome: {shutdown:?}");
    wait_until_bindable(gateway_addr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_file_commit_reconciles_native_exact_snapshot_through_shadow_worker() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let persist_root = temp.path().join("persisted");
    fs::create_dir_all(&persist_root).unwrap();
    let initial_parquet_path = persist_root.join("events-0.parquet");
    write_test_parquet(&initial_parquet_path, vec![1, 2]);
    seed_legacy_append_table(&data_dir, &persist_root, &initial_parquet_path, 2);
    let gateway_addr = reserve_tcp_addr();
    let mut runtime =
        LegacyServerRuntime::start(runtime_config_with_shadow_catalog_and_persist_root(
            &temp,
            &data_dir,
            gateway_addr,
            Some(&persist_root),
        ))
        .expect("start server with a migrated legacy append table");

    let initial = runtime
        .health()
        .legacy_catalog_shadow
        .expect("SQLite shadow mode must expose command shadow status after its initial attempt");
    assert_eq!(initial.state, LegacyCatalogShadowState::EligibleCandidate);
    assert_eq!(initial.reconciled_catalog_revision, 1);
    assert_eq!(initial.publisher.baseline_revision, 1);
    assert_eq!(initial.publisher.published_total, 0);
    assert_eq!(initial.exact_diffs_total, 1);
    assert_eq!(initial.v3_reconciles_total, 0);
    assert_eq!(initial.v4_adopt_total, 1);
    assert_eq!(initial.v4_checkpoint_total, 1);
    assert_eq!(
        initial
            .checkpoint
            .as_ref()
            .expect("eligible V4 shadow must expose its durable checkpoint")
            .phase,
        LegacyCatalogProjectionPhaseV4::FullShadow
    );

    let mut client = MasterClient::connect_endpoint(runtime.control_endpoint().clone()).unwrap();
    client
        .register_process("persisted_file_shadow_test")
        .unwrap();
    client
        .register_stream("events", append_table_schema(), 64, 64)
        .unwrap();
    let new_parquet_path = persist_root.join("events-1.parquet");
    write_test_parquet(&new_parquet_path, vec![11, 12, 13]);
    client
        .replace_persisted_files(
            "events",
            vec![
                serde_json::json!({
                    "persist_file_id": "events-0",
                    "file_path": initial_parquet_path,
                    "row_count": 2,
                    "created_at": 1,
                }),
                serde_json::json!({
                    "persist_file_id": "events-1",
                    "file_path": new_parquet_path,
                    "row_count": 3,
                    "created_at": 2,
                }),
            ],
        )
        .unwrap();
    let legacy_stream = client.get_stream("events").unwrap();
    assert_eq!(legacy_stream.persisted_files_revision, 2);
    assert_eq!(legacy_stream.persisted_files.len(), 2);

    let reconcile_deadline = Instant::now() + Duration::from_secs(3);
    let reconciled = loop {
        let status = runtime
            .health()
            .legacy_catalog_shadow
            .expect("command shadow status must remain observable");
        if status.state == LegacyCatalogShadowState::EligibleCandidate
            && status.reconciled_catalog_revision == 2
            && status.v3_reconciles_total == 1
        {
            break status;
        }
        assert!(
            Instant::now() < reconcile_deadline,
            "persisted-file commit did not reach V3 reconcile: {status:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(reconciled.consumed_catalog_revision, 2);
    assert_eq!(
        reconciled.full_reconciles_total,
        initial.full_reconciles_total + 1
    );
    assert_eq!(reconciled.exact_diffs_total, initial.exact_diffs_total + 1);
    assert_eq!(reconciled.publisher.published_total, 1);
    assert_eq!(reconciled.publisher.latest_observed_revision, 2);
    assert_eq!(reconciled.publisher.continuity_loss_epoch, 0);
    assert_eq!(reconciled.revision_gaps_total, 0);
    assert_eq!(reconciled.timeouts_total, 0);
    assert_eq!(reconciled.catalog_errors_total, 0);
    assert_eq!(reconciled.unsupported_mutations_total, 0);
    assert_eq!(reconciled.last_error, None);
    client.unregister_process().unwrap();
    drop(client);

    let shutdown = runtime.shutdown();
    assert!(shutdown.is_clean(), "shutdown outcome: {shutdown:?}");
    assert_eq!(
        shutdown.legacy_catalog_shadow_outcome,
        Some(LegacyCatalogShadowShutdownOutcome::Complete)
    );
    wait_until_bindable(gateway_addr);

    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&data_dir).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    let tables = catalog
        .list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(8).unwrap(),
        })
        .await
        .unwrap();
    let table = tables
        .tables
        .into_iter()
        .find(|table| table.name.as_str() == "events")
        .expect("migrated events table must remain in native Catalog");
    let read = catalog.read_handle().unwrap();
    let projection = read
        .read_legacy_table_projection_v4(table.table_id)
        .await
        .unwrap()
        .expect("migrated table must retain a complete V4 projection");
    assert_eq!(
        projection.completeness,
        zippy_types::catalog::LegacyCatalogProjectionCompletenessV4::Complete
    );
    assert_eq!(
        projection.state,
        LegacyCatalogProjectionTableStateV4::Active
    );
    assert_eq!(projection.legacy_files_revision, 2);
    let checkpoint = read.read_legacy_projection_checkpoint_v4().await.unwrap();
    assert_eq!(checkpoint.phase, LegacyCatalogProjectionPhaseV4::FullShadow);
    assert_eq!(checkpoint.source_catalog_revision, 2);
    let mapping = catalog
        .resolve_legacy_file_view_mapping(table.table_id, 2)
        .await
        .expect("V3 reconcile must publish an exact legacy revision mapping");
    assert_eq!(mapping.table_id, table.table_id);
    assert_eq!(mapping.legacy_files_revision, 2);
    assert_eq!(mapping.source_catalog_revision, 2);
    assert_eq!(mapping.continuity, LegacyFileViewContinuity::Contiguous);
    assert!(mapping.request_id.is_some());
    assert_eq!(
        table.current_snapshot_version,
        mapping.native_snapshot_version
    );

    let selector = CatalogSnapshotSelector {
        table_id: table.table_id,
        snapshot_version: mapping.native_snapshot_version,
        schema_version: table.current_schema_version,
    };
    let snapshot = catalog
        .read_snapshot_at(std::slice::from_ref(&selector))
        .await
        .expect("read exact native snapshot selected by the legacy mapping");
    assert_eq!(snapshot.tables().len(), 1);
    let native = &snapshot.tables()[0];
    assert_eq!(native.selector(), &selector);
    assert_eq!(native.files().as_slice().len(), 2);
    let mut files = native.files().as_slice().iter().collect::<Vec<_>>();
    files.sort_unstable_by_key(|file| file.legacy_discovery_order());
    let expected = [(0, &initial_parquet_path, 2), (1, &new_parquet_path, 3)];
    for (file, (order, expected_path, row_count)) in files.into_iter().zip(expected) {
        assert_eq!(file.legacy_discovery_order(), Some(order));
        assert_eq!(file.file.row_count.get(), row_count);
        assert!(file.file.checksum.starts_with("sha256-v1:"));
        match &file.file.location {
            DataFileLocation::ExternalLegacy(path) => assert_eq!(
                path.as_str(),
                fs::canonicalize(expected_path).unwrap().to_str().unwrap()
            ),
            other => panic!("legacy shadow file has unexpected location: {other:?}"),
        }
    }
    drop(read);
    catalog.close().unwrap();
    drop(ownership);

    let mut restarted =
        LegacyServerRuntime::start(runtime_config_with_shadow_catalog_and_persist_root(
            &temp,
            &data_dir,
            gateway_addr,
            Some(&persist_root),
        ))
        .expect("restart server after reconciling multiple persisted writer outputs");
    let restarted_shadow = restarted
        .health()
        .legacy_catalog_shadow
        .expect("restarted SQLite shadow status must remain observable");
    assert_eq!(
        restarted_shadow.state,
        LegacyCatalogShadowState::EligibleCandidate
    );
    assert_eq!(restarted_shadow.reconciled_catalog_revision, 2);
    assert_eq!(restarted_shadow.last_error, None);
    let migration = restarted.legacy_catalog_migration_status();
    assert_eq!(migration.state, LegacyCatalogMigrationState::Complete);
    assert!(migration.report.is_none());
    assert!(migration.degradation.is_none());
    let migration_metrics = restarted.legacy_catalog_migration_metrics();
    assert_eq!(migration_metrics["completion_mode"], "v4_checkpoint");
    assert_eq!(migration_metrics["checkpoint_reuses"], 1);

    let restarted_client =
        MasterClient::connect_endpoint(restarted.control_endpoint().clone()).unwrap();
    let restarted_stream = restarted_client.get_stream("events").unwrap();
    assert_eq!(restarted_stream.persisted_files_revision, 2);
    assert_eq!(restarted_stream.persisted_files.len(), 2);
    let mut persisted_file_ids = restarted_stream
        .persisted_files
        .iter()
        .map(|file| file["persist_file_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    persisted_file_ids.sort_unstable();
    assert_eq!(persisted_file_ids, vec!["events-0", "events-1"]);
    drop(restarted_client);

    let restarted_shutdown = restarted.shutdown();
    assert!(
        restarted_shutdown.is_clean(),
        "restart shutdown outcome: {restarted_shutdown:?}"
    );
    assert_eq!(
        restarted_shutdown.legacy_catalog_shadow_outcome,
        Some(LegacyCatalogShadowShutdownOutcome::Complete)
    );
    wait_until_bindable(gateway_addr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_unregister_is_liveness_only_and_offline_shadow_shutdown_is_clean() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let gateway_addr = reserve_tcp_addr();
    let mut runtime = LegacyServerRuntime::start(runtime_config_with_shadow_catalog(
        &temp,
        &data_dir,
        gateway_addr,
    ))
    .expect("start server with SQLite Catalog shadow");
    let mut client = MasterClient::connect_endpoint(runtime.control_endpoint().clone()).unwrap();
    client.register_process("offline_table_writer").unwrap();
    client
        .register_stream("offline_events", append_table_schema(), 16, 4)
        .unwrap();
    client
        .register_source(
            "offline_events_source",
            "test",
            "offline_events",
            serde_json::json!({"zippy_table_kind": "append_table"}),
        )
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    let online = loop {
        let shadow = runtime
            .health()
            .legacy_catalog_shadow
            .expect("command shadow status must remain observable");
        if shadow.state == LegacyCatalogShadowState::EligibleCandidate
            && shadow.v4_bootstrap_total == 1
        {
            break shadow;
        }
        assert!(
            Instant::now() < deadline,
            "offline-table bootstrap did not converge: {shadow:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };

    client.unregister_source("offline_events_source").unwrap();
    client.unregister_process().unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let offline = runtime
        .health()
        .legacy_catalog_shadow
        .expect("command shadow status must remain observable");
    assert_eq!(offline.state, LegacyCatalogShadowState::EligibleCandidate);
    assert_eq!(
        offline.reconciled_catalog_revision,
        online.reconciled_catalog_revision
    );
    assert_eq!(offline.v4_drop_total, 0);
    assert_eq!(offline.last_error, None);

    let shutdown = runtime.shutdown();
    assert!(shutdown.is_clean(), "shutdown outcome: {shutdown:?}");
    wait_until_bindable(gateway_addr);

    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&data_dir).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    let read = catalog.read_handle().unwrap();
    let projection = read
        .list_legacy_table_projections_v4()
        .await
        .unwrap()
        .into_iter()
        .find(|projection| projection.state == LegacyCatalogProjectionTableStateV4::Active)
        .expect("source liveness loss must not delete the durable projection");
    assert_eq!(projection.sources.len(), 1);
    let checkpoint = read.read_legacy_projection_checkpoint_v4().await.unwrap();
    assert_eq!(checkpoint.phase, LegacyCatalogProjectionPhaseV4::FullShadow);
    drop(read);
    catalog.close().unwrap();
    drop(ownership);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_drop_tombstone_reconciles_after_shadow_misses_event() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let gateway_addr = reserve_tcp_addr();
    let mut runtime = LegacyServerRuntime::start(runtime_config_with_shadow_catalog(
        &temp,
        &data_dir,
        gateway_addr,
    ))
    .expect("start server with SQLite Catalog shadow");
    let mut client = MasterClient::connect_endpoint(runtime.control_endpoint().clone()).unwrap();
    client.register_process("missed_drop_writer").unwrap();
    client
        .register_stream("missed_drop_events", append_table_schema(), 16, 4)
        .unwrap();
    client
        .register_source(
            "missed_drop_events_source",
            "test",
            "missed_drop_events",
            serde_json::json!({"zippy_table_kind": "append_table"}),
        )
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let shadow = runtime
            .health()
            .legacy_catalog_shadow
            .expect("command shadow status must remain observable");
        if shadow.state == LegacyCatalogShadowState::EligibleCandidate
            && shadow.v4_bootstrap_total == 1
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "missed-drop table bootstrap did not converge: {shadow:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    client.unregister_process().unwrap();
    let shutdown = runtime.shutdown();
    assert!(shutdown.is_clean(), "shutdown outcome: {shutdown:?}");
    wait_until_bindable(gateway_addr);

    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&data_dir).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    let read = catalog.read_handle().unwrap();
    let active_projection = read
        .list_legacy_table_projections_v4()
        .await
        .unwrap()
        .into_iter()
        .find(|projection| projection.state == LegacyCatalogProjectionTableStateV4::Active)
        .expect("initial V4 projection must be active");
    let table_id = active_projection.table_id;
    drop(read);
    catalog.close().unwrap();
    drop(ownership);

    let snapshot_path = data_dir.join("catalog/legacy-master-registry.json");
    let mut snapshot = SnapshotStore::load(&snapshot_path).unwrap();
    snapshot
        .streams
        .retain(|stream| stream.stream_name != "missed_drop_events");
    snapshot
        .sources
        .retain(|source| source.output_stream != "missed_drop_events");
    snapshot
        .catalog_sources
        .retain(|source| source.output_stream != "missed_drop_events");
    snapshot.catalog_revision = snapshot.catalog_revision.saturating_add(1).max(1);
    snapshot.dropped_tables.push(SnapshotDroppedTableRecord {
        table_id,
        table_name: "missed_drop_events".to_owned(),
        dropped_catalog_revision: snapshot.catalog_revision,
    });
    let dropped_catalog_revision = snapshot.catalog_revision;
    SnapshotStore::write(&snapshot_path, &snapshot).unwrap();

    let mut restarted = LegacyServerRuntime::start(runtime_config_with_shadow_catalog(
        &temp,
        &data_dir,
        gateway_addr,
    ))
    .expect("restart server after the shadow missed an explicit drop event");
    let deadline = Instant::now() + Duration::from_secs(3);
    let reconciled = loop {
        let shadow = restarted
            .health()
            .legacy_catalog_shadow
            .expect("restarted command shadow status must remain observable");
        if shadow.state == LegacyCatalogShadowState::EligibleCandidate
            && shadow.reconciled_catalog_revision == dropped_catalog_revision
            && shadow.v4_drop_total == 1
        {
            break shadow;
        }
        assert!(
            Instant::now() < deadline,
            "durable drop tombstone did not reconcile after restart: {shadow:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(reconciled.last_error, None);
    assert!(reconciled
        .checkpoint
        .as_ref()
        .is_some_and(|checkpoint| checkpoint.phase == LegacyCatalogProjectionPhaseV4::FullShadow));

    let shutdown = restarted.shutdown();
    assert!(shutdown.is_clean(), "shutdown outcome: {shutdown:?}");
    wait_until_bindable(gateway_addr);
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&data_dir).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    let read = catalog.read_handle().unwrap();
    let projection = read
        .read_legacy_table_projection_v4(table_id)
        .await
        .unwrap()
        .expect("dropped table projection must remain durable");
    assert_eq!(
        projection.state,
        LegacyCatalogProjectionTableStateV4::Dropped
    );
    drop(read);
    catalog.close().unwrap();
    drop(ownership);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn online_table_source_liveness_and_explicit_drop_converge_through_v4_projection() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let persist_root = temp.path().join("persist");
    fs::create_dir_all(&persist_root).unwrap();
    let gateway_addr = reserve_tcp_addr();
    let mut runtime =
        LegacyServerRuntime::start(runtime_config_with_shadow_catalog_and_persist_root(
            &temp,
            &data_dir,
            gateway_addr,
            Some(&persist_root),
        ))
        .expect("start server with SQLite Catalog shadow");
    let table_schema = append_table_schema_with_custom_metadata();
    let mut client = MasterClient::connect_endpoint(runtime.control_endpoint().clone()).unwrap();
    client.register_process("late_table_writer").unwrap();
    client
        .register_stream("late_table", Arc::clone(&table_schema), 16, 4)
        .unwrap();
    client
        .register_source(
            "late_table_source",
            "test",
            "late_table",
            serde_json::json!({"zippy_table_kind": "append_table", "feed": "primary"}),
        )
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    let bootstrapped = loop {
        let health = runtime.health();
        let shadow = health
            .legacy_catalog_shadow
            .expect("command shadow status must remain observable");
        if shadow.state == LegacyCatalogShadowState::EligibleCandidate
            && shadow.reconciled_catalog_revision == 2
            && shadow.v4_bootstrap_total == 1
        {
            assert!(health.readiness.is_ready());
            break shadow;
        }
        assert!(
            Instant::now() < deadline,
            "online table did not bootstrap through V4: {shadow:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(bootstrapped.unsupported_mutations_total, 0);
    assert_eq!(bootstrapped.last_error, None);

    client.unregister_process().unwrap();
    let mut replacement =
        MasterClient::connect_endpoint(runtime.control_endpoint().clone()).unwrap();
    replacement
        .register_process("late_table_writer_rebound")
        .unwrap();
    replacement
        .register_stream("late_table", Arc::clone(&table_schema), 16, 4)
        .unwrap();
    replacement
        .register_source(
            "late_table_source",
            "test",
            "late_table",
            serde_json::json!({"zippy_table_kind": "append_table", "feed": "primary"}),
        )
        .unwrap();
    let sync_deadline = Instant::now() + Duration::from_secs(3);
    let synced = loop {
        let shadow = runtime
            .health()
            .legacy_catalog_shadow
            .expect("command shadow status must remain observable");
        if shadow.state == LegacyCatalogShadowState::EligibleCandidate
            && shadow.reconciled_catalog_revision > bootstrapped.reconciled_catalog_revision
            && shadow.v4_metadata_sync_total == 1
        {
            break shadow;
        }
        assert!(
            Instant::now() < sync_deadline,
            "source metadata did not synchronize through V4: {shadow:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(synced.v4_bootstrap_total, 1);

    replacement.unregister_source("late_table_source").unwrap();
    let stream = replacement.get_stream("late_table").unwrap();
    assert_eq!(stream.writer_process_id, None);
    assert_eq!(stream.active_segment_descriptor, None);
    std::thread::sleep(Duration::from_millis(100));
    let offline = runtime
        .health()
        .legacy_catalog_shadow
        .expect("command shadow status must remain observable");
    assert_eq!(offline.state, LegacyCatalogShadowState::EligibleCandidate);
    assert_eq!(
        offline.reconciled_catalog_revision,
        synced.reconciled_catalog_revision
    );
    assert_eq!(offline.v4_drop_total, 0);
    assert_eq!(offline.last_error, None);
    replacement
        .register_source(
            "late_table_source",
            "test",
            "late_table",
            serde_json::json!({"zippy_table_kind": "append_table", "feed": "primary"}),
        )
        .unwrap();
    let recovery_deadline = Instant::now() + Duration::from_secs(3);
    let recovered = loop {
        let shadow = runtime
            .health()
            .legacy_catalog_shadow
            .expect("command shadow status must remain observable");
        if shadow.state == LegacyCatalogShadowState::EligibleCandidate
            && shadow.reconciled_catalog_revision > offline.reconciled_catalog_revision
            && shadow.last_error.is_none()
        {
            break shadow;
        }
        assert!(
            Instant::now() < recovery_deadline,
            "source liveness recovery did not converge: {shadow:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(recovered.v4_drop_total, 0);

    replacement.drop_table("late_table", false).unwrap();
    let drop_deadline = Instant::now() + Duration::from_secs(3);
    let dropped = loop {
        let shadow = runtime
            .health()
            .legacy_catalog_shadow
            .expect("command shadow status must remain observable");
        if shadow.state == LegacyCatalogShadowState::EligibleCandidate
            && shadow.reconciled_catalog_revision > recovered.reconciled_catalog_revision
            && shadow.v4_drop_total == 1
        {
            break shadow;
        }
        assert!(
            Instant::now() < drop_deadline,
            "table removal did not converge through V4: {shadow:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(dropped.v4_metadata_sync_total >= 1);
    assert_eq!(dropped.unsupported_mutations_total, 0);
    assert_eq!(
        dropped
            .checkpoint
            .as_ref()
            .expect("eligible V4 shadow must expose a checkpoint")
            .phase,
        LegacyCatalogProjectionPhaseV4::FullShadow
    );
    replacement.unregister_process().unwrap();

    let shutdown = runtime.shutdown();
    assert!(shutdown.is_clean(), "shutdown outcome: {shutdown:?}");
    wait_until_bindable(gateway_addr);

    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&data_dir).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    let read = catalog.read_handle().unwrap();
    let projection = read
        .list_legacy_table_projections_v4()
        .await
        .unwrap()
        .into_iter()
        .find(|projection| projection.state == LegacyCatalogProjectionTableStateV4::Dropped)
        .expect("dropped V4 projection must remain durable");
    assert_eq!(
        projection.state,
        LegacyCatalogProjectionTableStateV4::Dropped
    );
    let checkpoint = read.read_legacy_projection_checkpoint_v4().await.unwrap();
    assert_eq!(checkpoint.phase, LegacyCatalogProjectionPhaseV4::FullShadow);
    assert!(checkpoint.source_catalog_revision >= 4);
    drop(read);
    catalog.close().unwrap();
    drop(ownership);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_shadow_migration_failure_is_degraded_without_closing_legacy_authority() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let gateway_addr = reserve_tcp_addr();
    seed_incompatible_native_catalog_revision(&data_dir).await;

    let mut runtime = LegacyServerRuntime::start(runtime_config_with_shadow_catalog(
        &temp,
        &data_dir,
        gateway_addr,
    ))
    .expect("incompatible shadow migration must not prevent Legacy startup");

    assert!(runtime.health().readiness.is_ready());
    assert_eq!(runtime.catalog().revision().await.unwrap(), 0);
    assert!(runtime.query().is_some());
    let migration = runtime.legacy_catalog_migration_status();
    assert_eq!(migration.state, LegacyCatalogMigrationState::Degraded);
    assert!(migration.report.is_none());
    let degradation = migration
        .degradation
        .as_ref()
        .expect("failed execution must retain structured degradation");
    assert_eq!(
        degradation.reason,
        LegacyCatalogMigrationDegradedReason::ExecutionFailed
    );
    assert_eq!(degradation.error_code, Some(ErrorCode::Conflict));
    assert!(degradation.message.as_deref().is_some_and(|message| {
        message.contains("code=[CONFLICT]")
            && message.contains("request_id=[")
            && message.contains("table_id=[")
            && !message.contains("target revision does not match")
    }));
    let metrics = runtime.legacy_catalog_migration_metrics();
    assert_eq!(metrics["state"], "degraded");
    assert!(metrics["completion_mode"].is_null());
    assert_eq!(metrics["checkpoint_reuses"], 0);
    assert_eq!(metrics["degraded_reason"], "execution_failed");
    assert_eq!(metrics["error_code"], "CONFLICT");

    assert!(runtime.shutdown().is_clean());
    wait_until_bindable(gateway_addr);
}

#[test]
fn gateway_bind_failure_rolls_back_ready_master_listener() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("zippy-server.sock");
    let owner_path = temp.path().join("zippy-server.sock.owner");
    let gateway_addr = reserve_tcp_addr();
    let blocker = TcpListener::bind(gateway_addr).unwrap();

    let error = match LegacyServerRuntime::start(runtime_config(&temp, Some(gateway_addr))) {
        Ok(mut runtime) => {
            let _ = runtime.shutdown();
            panic!("server runtime unexpectedly bound an occupied gateway port");
        }
        Err(error) => error,
    };

    assert!(error.message().contains("failed to bind gateway endpoint"));
    assert!(!socket_path.exists());
    assert!(!owner_path.exists());

    drop(blocker);
    wait_until_bindable(gateway_addr);

    let mut restarted =
        LegacyServerRuntime::start(runtime_config(&temp, Some(gateway_addr))).unwrap();
    let outcome = restarted.shutdown();
    assert!(outcome.is_clean(), "shutdown outcome: {outcome:?}");
}

#[test]
fn gateway_bind_failure_also_closes_configured_native_storage() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("database");
    let gateway_addr = reserve_tcp_addr();
    let blocker = TcpListener::bind(gateway_addr).unwrap();

    let error = match LegacyServerRuntime::start(runtime_config_with_native_storage_and_gateway(
        &temp,
        &data_dir,
        gateway_addr,
    )) {
        Ok(mut runtime) => {
            let _ = runtime.shutdown();
            panic!("server unexpectedly bound an occupied Gateway port");
        }
        Err(error) => error,
    };

    assert!(error.message().contains("failed to bind gateway endpoint"));
    assert!(!temp.path().join("zippy-server.sock").exists());
    zippy_core::OwnedDataDirectory::acquire_or_initialize(&data_dir)
        .expect("Gateway rollback must close native storage before returning");

    drop(blocker);
    wait_until_bindable(gateway_addr);
}

#[test]
fn repeated_shutdown_preserves_the_first_timeout_outcome() {
    let temp = tempfile::tempdir().unwrap();
    let mut runtime = LegacyServerRuntime::start(runtime_config_with_shutdown(&temp, None, 30))
        .expect("catalog-only runtime should start");
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

    assert_eq!(first, second);
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
