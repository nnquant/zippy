use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zippy_core::{ControlEndpoint, MasterClient};
use zippy_master::server::MasterServer;

#[test]
fn master_server_accepts_tcp_control_requests() {
    let endpoint = ControlEndpoint::Tcp(unused_loopback_addr());
    let server = MasterServer::default();
    let server_for_thread = server.clone();
    let endpoint_for_thread = endpoint.clone();
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);

    let handle = thread::spawn(move || {
        server_for_thread
            .serve_endpoint_with_ready(&endpoint_for_thread, Some(ready_tx))
            .unwrap();
    });
    ready_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();

    let mut client = MasterClient::connect_endpoint(endpoint).unwrap();
    let process_id = client.register_process("tcp_roundtrip").unwrap();

    assert!(process_id.starts_with("proc_"));
    server.shutdown();
    handle.join().unwrap();
}

fn unused_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    listener.local_addr().unwrap()
}
