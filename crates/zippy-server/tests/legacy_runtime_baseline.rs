mod support;

use std::num::NonZeroU32;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema};
use serde_json::json;
use zippy_core::MasterClient;
use zippy_master::server::MasterServer;
use zippy_server::{LegacyServerServices, ServiceHealth};
use zippy_types::catalog::{CatalogRead, ListTablesRequest};

use support::{
    gateway_metrics, gateway_server, reserve_tcp_addr, wait_for_gateway_process,
    wait_until_bindable, GatewayHarness, MasterHarness,
};

#[test]
fn master_ready_barrier_precedes_control_wire_and_shutdown_releases_port() {
    let mut master = MasterHarness::start(MasterServer::default());
    let mut client = MasterClient::connect_endpoint(master.endpoint().clone()).unwrap();

    let process_id = client.register_process("phase1_ready_probe").unwrap();
    assert!(process_id.starts_with("proc_"));
    client.unregister_process().unwrap();

    let outcome = master.graceful_shutdown(Duration::from_secs(1));
    assert!(outcome.timed_out_processes.is_empty());
}

#[test]
fn gateway_bind_conflict_leaves_no_gateway_runtime_or_bound_port() {
    let mut master = MasterHarness::start(MasterServer::default());
    let master_addr = master.addr();
    let gateway_addr = reserve_tcp_addr();
    let blocker = std::net::TcpListener::bind(gateway_addr).unwrap();

    let start_result = gateway_server(master.endpoint().clone(), gateway_addr).start();
    let error = match start_result {
        Ok(server) => {
            server.stop();
            panic!("gateway unexpectedly bound an occupied port");
        }
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("failed to bind gateway endpoint"));

    drop(blocker);
    wait_until_bindable(gateway_addr);

    let outcome = master.graceful_shutdown(Duration::from_secs(1));
    assert!(outcome.timed_out_processes.is_empty());

    let mut master = MasterHarness::start_at(MasterServer::default(), master_addr);
    let mut gateway = GatewayHarness::start_at(master.endpoint().clone(), gateway_addr);
    let response = gateway_metrics(gateway.addr());
    assert_eq!(response["status"], "ok");
    let gateway_process_id = wait_for_gateway_process(master.server());
    gateway.shutdown();
    assert!(master
        .server()
        .registry()
        .lock()
        .unwrap()
        .alive_process_ids()
        .is_empty());
    let outcome = master.graceful_shutdown(Duration::from_secs(1));
    assert!(
        outcome.timed_out_processes.is_empty(),
        "gateway process lease was not released process_id=[{}] timed_out={:?}",
        gateway_process_id,
        outcome.timed_out_processes,
    );
}

#[test]
fn legacy_composition_baseline_preserves_both_listener_wires() {
    let mut master = MasterHarness::start(MasterServer::default());
    let mut gateway = GatewayHarness::start(master.endpoint().clone());
    let services = LegacyServerServices::new(master.server(), gateway.server()).unwrap();

    assert!(!services.health().admission_open);
    services.activate().unwrap();
    assert_eq!(services.health().catalog, ServiceHealth::Degraded);
    assert_eq!(services.health().query, ServiceHealth::Degraded);
    assert_eq!(services.health().end_to_end_liveness, None);

    let mut client = MasterClient::connect_endpoint(master.endpoint().clone()).unwrap();
    client.register_process("phase1_wire_probe").unwrap();
    assert!(client.list_streams().unwrap().is_empty());
    assert_eq!(gateway_metrics(gateway.addr())["status"], "ok");
    let _gateway_process_id = wait_for_gateway_process(master.server());

    services.begin_shutdown();
    assert!(!services.health().admission_open);
    client.unregister_process().unwrap();
    gateway.shutdown();
    assert!(master
        .server()
        .registry()
        .lock()
        .unwrap()
        .alive_process_ids()
        .is_empty());
    let outcome = master.graceful_shutdown(Duration::from_secs(1));
    assert!(
        outcome.timed_out_processes.is_empty(),
        "gateway left master process leases behind: {:?}",
        outcome.timed_out_processes
    );
}

#[tokio::test]
async fn restart_keeps_durable_catalog_but_rejects_old_runtime_capability() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot_path = temp.path().join("master-registry.json");
    let first_server = MasterServer::with_runtime_config(
        Some(snapshot_path.clone()),
        Duration::from_secs(10),
        Duration::from_secs(2),
    );
    let mut first = MasterHarness::start(first_server);
    let master_addr = first.addr();
    let mut old_client = MasterClient::connect_endpoint(first.endpoint().clone()).unwrap();
    let old_process_id = old_client.register_process("phase1_restart_probe").unwrap();
    let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    old_client
        .register_stream("durable_ticks", schema, 64, 64)
        .unwrap();
    old_client
        .register_source(
            "durable_ticks_source",
            "test",
            "durable_ticks",
            json!({"zippy_table_kind": "append_table"}),
        )
        .unwrap();

    let outcome = first.graceful_shutdown(Duration::from_millis(20));
    assert_eq!(outcome.timed_out_processes, vec![old_process_id.clone()]);

    let restored_server = MasterServer::from_snapshot_path(&snapshot_path).unwrap();
    let mut restored = MasterHarness::start_at(restored_server, master_addr);

    let old_heartbeat_error = old_client.heartbeat().unwrap_err();
    assert!(old_heartbeat_error.to_string().contains(&old_process_id));

    let tables = restored
        .server()
        .legacy_catalog_adapter()
        .list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(16).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(tables.tables.len(), 1);
    assert_eq!(tables.tables[0].name.as_str(), "durable_ticks");

    let mut new_client = MasterClient::connect_endpoint(restored.endpoint().clone()).unwrap();
    let new_process_id = new_client.register_process("phase1_restart_probe").unwrap();
    assert_ne!(new_process_id, old_process_id);
    new_client.unregister_process().unwrap();

    let outcome = restored.graceful_shutdown(Duration::from_secs(1));
    assert!(outcome.timed_out_processes.is_empty());
}
