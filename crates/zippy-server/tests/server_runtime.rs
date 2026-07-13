#![cfg(unix)]

#[allow(dead_code)]
mod support;

use std::fs;
use std::net::TcpListener;
use std::time::Duration;

use zippy_core::{ControlEndpoint, MasterClient};
use zippy_master::daemon::MasterDaemonConfig;
use zippy_server::{
    LegacyServerMode, LegacyServerRuntime, LegacyServerRuntimeConfig, ServiceHealth,
};
use zippy_types::catalog::CatalogRead;
use zippy_types::runtime::RuntimeRegistry;
use zippy_types::ErrorCode;

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

#[tokio::test]
async fn catalog_only_runtime_is_ready_without_fabricating_query_service() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("zippy-server.sock");
    let mut runtime = LegacyServerRuntime::start(runtime_config(&temp, None)).unwrap();

    assert_eq!(runtime.mode(), LegacyServerMode::CatalogOnly);
    assert_eq!(
        runtime.control_endpoint(),
        &ControlEndpoint::Unix(socket_path)
    );
    assert_eq!(runtime.gateway_endpoint(), None);
    assert!(runtime.query().is_none());
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
    assert!(!temp.path().join("zippy-server.sock").exists());
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
fn repeated_shutdown_preserves_the_first_timeout_outcome() {
    let temp = tempfile::tempdir().unwrap();
    let mut runtime = LegacyServerRuntime::start(runtime_config_with_shutdown(&temp, None, 30))
        .expect("catalog-only runtime should start");
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
    assert!(
        elapsed < Duration::from_millis(250),
        "elapsed=[{elapsed:?}]"
    );
}
