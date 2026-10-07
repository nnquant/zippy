#![cfg(unix)]
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use zippy_core::{MasterClient, ZippyConfig};
use zippy_master::server::MasterServer;

fn unique_socket_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("zippy-master-roundtrip-{nanos}.sock"))
}

fn spawn_test_server_with_config(
    socket_path: &Path,
    config: ZippyConfig,
) -> (MasterServer, thread::JoinHandle<()>) {
    let server = MasterServer::with_config(config);
    let handle_server = server.clone();
    let socket_path = socket_path.to_path_buf();
    let wait_path = socket_path.clone();
    let join_handle = thread::spawn(move || {
        handle_server.serve(&socket_path).unwrap();
    });
    wait_for_socket_ready(&wait_path);
    (server, join_handle)
}

fn wait_for_socket_ready(socket_path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match std::os::unix::net::UnixStream::connect(socket_path) {
            Ok(stream) => {
                drop(stream);
                return;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound
                        | std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionReset
                ) =>
            {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!(
                "unexpected error while waiting for socket path=[{}] error=[{}]",
                socket_path.display(),
                error
            ),
        }
    }

    panic!("socket was not ready path=[{}]", socket_path.display());
}

#[test]
fn master_client_fetches_master_runtime_config() {
    let socket_path = unique_socket_path();
    let mut expected_config = ZippyConfig::default();
    expected_config.log.level = "warn".to_string();
    let (server, handle) = spawn_test_server_with_config(&socket_path, expected_config.clone());

    let client = MasterClient::connect(&socket_path).unwrap();
    let config = client.get_config().unwrap();

    assert_eq!(config, expected_config);

    server.shutdown();
    handle.join().unwrap();
}
