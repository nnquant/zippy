use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use zippy_core::{ControlEndpoint, Result as CoreResult};
use zippy_master::server::{GracefulShutdownOutcome, MasterServer};

const READY_TIMEOUT: Duration = Duration::from_secs(5);
const PORT_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

/// Owns one master listener thread and waits for its explicit ready barrier.
pub(crate) struct MasterHarness {
    addr: SocketAddr,
    endpoint: ControlEndpoint,
    server: MasterServer,
    join_handle: Option<JoinHandle<CoreResult<()>>>,
}

impl MasterHarness {
    /// Starts a master while retaining ownership of its bound loopback listener.
    pub(crate) fn start(server: MasterServer) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test master listener");
        Self::start_listener(server, listener)
    }

    /// Starts a master on a fixed address and waits for the listener ready signal.
    pub(crate) fn start_at(server: MasterServer, addr: SocketAddr) -> Self {
        let listener = TcpListener::bind(addr).expect("bind fixed test master listener");
        Self::start_listener(server, listener)
    }

    fn start_listener(server: MasterServer, listener: TcpListener) -> Self {
        let addr = listener
            .local_addr()
            .expect("read test master listener address");
        let endpoint = ControlEndpoint::Tcp(addr);
        let serve_server = server.clone();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let join_handle = thread::spawn(move || {
            serve_server.serve_tcp_listener_with_ready(listener, Some(ready_tx))
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
