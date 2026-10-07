#[allow(dead_code)]
mod support;

use std::time::Duration;

use zippy_core::MasterClient;
use zippy_master::server::MasterServer;

use support::MasterHarness;

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
