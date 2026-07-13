use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use zippy_core::{ControlEndpoint, Result as CoreResult};
use zippy_gateway::{GatewayServer, GatewayServerConfig};
use zippy_master::server::{GracefulShutdownOutcome, MasterServer};

const READY_TIMEOUT: Duration = Duration::from_secs(5);
const PORT_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);
const GATEWAY_TOKEN: &str = "phase1-test-token";

/// Owns one master listener thread and waits for its explicit ready barrier.
pub(crate) struct MasterHarness {
    addr: SocketAddr,
    endpoint: ControlEndpoint,
    server: MasterServer,
    join_handle: Option<JoinHandle<CoreResult<()>>>,
}

impl MasterHarness {
    /// Starts a master on a reserved loopback address.
    pub(crate) fn start(server: MasterServer) -> Self {
        Self::start_at(server, reserve_tcp_addr())
    }

    /// Starts a master on a fixed address and waits for the listener ready signal.
    pub(crate) fn start_at(server: MasterServer, addr: SocketAddr) -> Self {
        let endpoint = ControlEndpoint::Tcp(addr);
        let serve_endpoint = endpoint.clone();
        let serve_server = server.clone();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let join_handle = thread::spawn(move || {
            serve_server.serve_endpoint_with_ready(&serve_endpoint, Some(ready_tx))
        });

        let ready = ready_rx
            .recv_timeout(READY_TIMEOUT)
            .expect("master ready barrier timed out");
        ready.expect("master failed before publishing readiness");

        Self {
            addr,
            endpoint,
            server,
            join_handle: Some(join_handle),
        }
    }

    /// Returns the bound TCP address.
    pub(crate) const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the control endpoint used by clients and the gateway.
    pub(crate) fn endpoint(&self) -> &ControlEndpoint {
        &self.endpoint
    }

    /// Returns the in-process server handle used by the composition root.
    pub(crate) const fn server(&self) -> &MasterServer {
        &self.server
    }

    /// Requests graceful shutdown, joins the listener, and verifies port release.
    pub(crate) fn graceful_shutdown(&mut self, timeout: Duration) -> GracefulShutdownOutcome {
        let outcome = self.server.request_graceful_shutdown(timeout);
        self.join();
        wait_until_bindable(self.addr);
        outcome
    }

    fn join(&mut self) {
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .join()
                .expect("master listener thread panicked")
                .expect("master listener returned an error");
        }
    }
}

impl Drop for MasterHarness {
    fn drop(&mut self) {
        self.server.shutdown();
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

/// Owns one gateway runtime and verifies that shutdown releases its port.
pub(crate) struct GatewayHarness {
    addr: SocketAddr,
    server: Option<GatewayServer>,
}

impl GatewayHarness {
    /// Starts a gateway on a reserved loopback address.
    pub(crate) fn start(master_endpoint: ControlEndpoint) -> Self {
        Self::start_at(master_endpoint, reserve_tcp_addr())
    }

    /// Starts a gateway on a fixed address.
    pub(crate) fn start_at(master_endpoint: ControlEndpoint, addr: SocketAddr) -> Self {
        let server = gateway_server(master_endpoint, addr)
            .start()
            .expect("gateway failed to bind");
        assert_eq!(server.endpoint(), addr.to_string());
        Self {
            addr,
            server: Some(server),
        }
    }

    /// Returns the bound TCP address.
    pub(crate) const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the live gateway handle used by the composition root.
    pub(crate) fn server(&self) -> &GatewayServer {
        self.server.as_ref().expect("gateway already stopped")
    }

    /// Stops the gateway, joins its runtime, and verifies port release.
    pub(crate) fn shutdown(&mut self) {
        if let Some(server) = self.server.take() {
            server
                .stop_gracefully()
                .expect("gateway failed to release its master process");
            wait_until_bindable(self.addr);
        }
    }
}

impl Drop for GatewayHarness {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.stop();
        }
    }
}

/// Builds an unstarted gateway using the same stable test configuration.
pub(crate) fn gateway_server(master_endpoint: ControlEndpoint, addr: SocketAddr) -> GatewayServer {
    GatewayServer::new(GatewayServerConfig {
        endpoint: addr.to_string(),
        master_endpoint,
        token: Some(GATEWAY_TOKEN.to_string()),
        max_write_rows: None,
        max_connections: None,
        max_subscribers: None,
        max_blocking_requests: None,
        write_timeout_ms: None,
    })
    .expect("gateway configuration must be valid")
}

/// Reserves a currently free loopback address for a test-owned listener.
pub(crate) fn reserve_tcp_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("failed to reserve loopback port")
        .local_addr()
        .expect("failed to read reserved loopback address")
}

/// Waits until an address can be rebound, proving the previous listener is gone.
pub(crate) fn wait_until_bindable(addr: SocketAddr) {
    let deadline = Instant::now() + PORT_RELEASE_TIMEOUT;
    loop {
        match TcpListener::bind(addr) {
            Ok(listener) => {
                drop(listener);
                return;
            }
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("listener port was not released addr=[{addr}] error=[{error}]"),
        }
    }
}

/// Waits until Gateway has completed its boot-scoped process registration.
pub(crate) fn wait_for_gateway_process(master: &MasterServer) -> String {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        let registry = master.registry();
        let registry = registry.lock().unwrap();
        let process_id = registry.alive_process_ids().into_iter().find(|process_id| {
            registry
                .get_process(process_id)
                .is_some_and(|process| process.app == "zippy_gateway")
        });
        if let Some(process_id) = process_id {
            return process_id;
        }
        drop(registry);
        assert!(
            Instant::now() < deadline,
            "gateway did not register a master process before timeout"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

/// Executes the legacy gateway metrics frame as an end-to-end listener probe.
pub(crate) fn gateway_metrics(addr: SocketAddr) -> Value {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(1))
        .expect("failed to connect to gateway metrics endpoint");
    let header = serde_json::to_vec(&json!({
        "kind": "metrics",
        "token": GATEWAY_TOKEN,
    }))
    .expect("failed to encode gateway metrics request");
    stream
        .write_all(&(header.len() as u32).to_be_bytes())
        .expect("failed to write gateway header length");
    stream
        .write_all(&0_u64.to_be_bytes())
        .expect("failed to write gateway payload length");
    stream
        .write_all(&header)
        .expect("failed to write gateway header");

    let mut prefix = [0_u8; 12];
    stream
        .read_exact(&mut prefix)
        .expect("failed to read gateway response prefix");
    let header_len = u32::from_be_bytes(prefix[0..4].try_into().unwrap()) as usize;
    let payload_len = u64::from_be_bytes(prefix[4..12].try_into().unwrap()) as usize;
    let mut response_header = vec![0_u8; header_len];
    stream
        .read_exact(&mut response_header)
        .expect("failed to read gateway response header");
    let mut response_payload = vec![0_u8; payload_len];
    stream
        .read_exact(&mut response_payload)
        .expect("failed to read gateway response payload");
    serde_json::from_slice(&response_header).expect("invalid gateway response header")
}
