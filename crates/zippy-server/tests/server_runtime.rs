#![cfg(unix)]

#[allow(dead_code)]
mod support;

use std::fs::{self, File};
use std::net::TcpListener;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::{
    canonical_schema_hash, schema_metadata, ControlEndpoint, DataDirectoryError,
    ManagedDataDirectory, MasterClient, OwnedDataDirectory,
};
use zippy_master::daemon::MasterDaemonConfig;
use zippy_master::snapshot::{
    RegistrySnapshot, SnapshotSourceRecord, SnapshotStore, SnapshotStreamRecord,
};
use zippy_server::{
    LegacyCatalogMigrationDegradedReason, LegacyCatalogMigrationState,
    LegacyCatalogShadowShutdownOutcome, LegacyCatalogShadowState, LegacyServerMode,
    LegacyServerRuntime, LegacyServerRuntimeConfig, ServiceHealth,
};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogRead,
    CatalogSnapshotSelector, CatalogWrite, CreateNamespaceCommand, LegacyFileViewContinuity,
    ListTablesRequest,
};
use zippy_types::runtime::RuntimeRegistry;
use zippy_types::table::{CatalogName, DataFileLocation};
use zippy_types::{ErrorCode, NamespaceId, RequestId};

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

fn seed_legacy_append_table(data_dir: &Path, persist_root: &Path, initial_file: &Path) {
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
                    "row_count": 2,
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
            engines: Vec::new(),
        },
    )
    .unwrap();
    drop(ownership);
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

#[test]
fn sqlite_shadow_runtime_exposes_bounded_gateway_comparison_worker() {
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

    let deadline = Instant::now() + Duration::from_secs(2);
    let command_shadow = loop {
        let status = runtime
            .health()
            .legacy_catalog_shadow
            .expect("SQLite shadow mode must expose command shadow status");
        if status.state == LegacyCatalogShadowState::EligibleCandidate {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "command shadow did not become eligible: {status:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
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
    assert_eq!(migration_metrics["submitted_commands"], 1);

    let shutdown = runtime.shutdown();
    assert!(shutdown.is_clean());
    assert_eq!(
        shutdown.legacy_catalog_shadow_outcome,
        Some(LegacyCatalogShadowShutdownOutcome::Complete)
    );
    wait_until_bindable(gateway_addr);

    let mut restarted = LegacyServerRuntime::start(runtime_config_with_shadow_catalog(
        &temp,
        &data_dir,
        gateway_addr,
    ))
    .expect("restart server with the same SQLite Catalog shadow");
    let replay = restarted
        .legacy_catalog_migration_status()
        .report
        .as_ref()
        .expect("restart must retain migration replay evidence");
    assert_eq!(replay.submitted_commands, 0);
    assert_eq!(replay.replayed_commands, 1);
    assert_eq!(replay.target_revision_before, 1);
    assert_eq!(replay.target_revision_after, 1);
    assert!(restarted.shutdown().is_clean());
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
    seed_legacy_append_table(&data_dir, &persist_root, &initial_parquet_path);
    let gateway_addr = reserve_tcp_addr();
    let mut runtime =
        LegacyServerRuntime::start(runtime_config_with_shadow_catalog_and_persist_root(
            &temp,
            &data_dir,
            gateway_addr,
            Some(&persist_root),
        ))
        .expect("start server with a migrated legacy append table");

    let initial_deadline = Instant::now() + Duration::from_secs(3);
    let initial = loop {
        let status = runtime
            .health()
            .legacy_catalog_shadow
            .expect("SQLite shadow mode must expose command shadow status");
        if status.state == LegacyCatalogShadowState::EligibleCandidate {
            break status;
        }
        assert!(
            Instant::now() < initial_deadline,
            "initial table reconcile did not become eligible: {status:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(initial.reconciled_catalog_revision, 1);
    assert_eq!(initial.publisher.baseline_revision, 1);
    assert_eq!(initial.publisher.published_total, 0);
    assert_eq!(initial.exact_diffs_total, 1);
    assert_eq!(initial.v3_reconciles_total, 0);

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
    catalog.close().unwrap();
    drop(ownership);
}

#[test]
fn unsupported_online_table_creation_degrades_only_the_shadow_candidate() {
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
    client.register_process("late_table_writer").unwrap();
    client
        .register_stream(
            "late_table",
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            16,
            4,
        )
        .unwrap();
    client.unregister_process().unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let health = runtime.health();
        let shadow = health
            .legacy_catalog_shadow
            .expect("command shadow status must remain observable");
        if shadow.state == LegacyCatalogShadowState::BlockedUnsupported {
            assert_eq!(shadow.unsupported_mutations_total, 1);
            assert_eq!(
                shadow.last_error.as_deref(),
                Some("new_legacy_table_requires_migration_primitive")
            );
            assert!(health.readiness.is_ready());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "command shadow did not fail closed: {shadow:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let shutdown = runtime.shutdown();
    assert!(shutdown.is_clean(), "shutdown outcome: {shutdown:?}");
    wait_until_bindable(gateway_addr);
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
