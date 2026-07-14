#![cfg(unix)]

#[allow(dead_code)]
mod support;

use std::fs;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::{ControlEndpoint, DataDirectoryError, MasterClient, OwnedDataDirectory};
use zippy_master::daemon::MasterDaemonConfig;
use zippy_server::{
    LegacyCatalogMigrationDegradedReason, LegacyCatalogMigrationState, LegacyServerMode,
    LegacyServerRuntime, LegacyServerRuntimeConfig, ServiceHealth,
};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogRead,
    CatalogWrite, CreateNamespaceCommand,
};
use zippy_types::runtime::RuntimeRegistry;
use zippy_types::table::CatalogName;
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
    let config_path = temp.path().join("zippy-shadow-catalog.toml");
    fs::write(
        &config_path,
        format!(
            concat!(
                "[server]\n",
                "data_dir = {:?}\n",
                "native_storage_enabled = true\n\n",
                "[catalog]\n",
                "backend = \"sqlite_shadow\"\n\n",
                "[catalog.runtime_snapshot_pins]\n",
                "max_active_pins = 7\n",
                "max_active_tables = 5\n\n",
                "[catalog.shadow]\n",
                "queue_capacity = 3\n",
                "metadata_budget_bytes = 4096\n",
                "max_files_per_comparison = 8\n",
                "max_metadata_bytes_per_comparison = 1024\n",
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

    assert!(runtime.shutdown().is_clean());
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
    assert!(degradation
        .message
        .as_deref()
        .is_some_and(|message| { message.contains("target revision does not match") }));
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
