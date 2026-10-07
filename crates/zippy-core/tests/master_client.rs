#![cfg(unix)]

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use zippy_core::{
    ControlEndpoint, ControlRequest, ControlResponse, HeartbeatRequest, MasterClient,
    RegisterProcessRequest, WatchResource, ZippyConfig, ZippyError,
};

#[test]
fn master_client_request_timeout_rejects_zero_and_can_be_restored() {
    let mut client = MasterClient::connect_endpoint(ControlEndpoint::Unix(unique_socket_path()))
        .expect("construct Master client without connecting");
    assert_eq!(client.request_timeout(), Duration::from_secs(30));

    let error = client.set_request_timeout(Duration::ZERO).unwrap_err();
    assert!(matches!(error, ZippyError::InvalidConfig { .. }));
    assert_eq!(client.request_timeout(), Duration::from_secs(30));

    client
        .set_request_timeout(Duration::from_millis(250))
        .unwrap();
    assert_eq!(client.request_timeout(), Duration::from_millis(250));
    client.set_request_timeout(Duration::from_secs(30)).unwrap();
    assert_eq!(client.request_timeout(), Duration::from_secs(30));
}

#[test]
fn watch_uses_its_long_poll_budget_when_ordinary_requests_have_a_short_timeout() {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    let endpoint = ControlEndpoint::Tcp(listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        for index in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            let mut line = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            let request = serde_json::from_str::<ControlRequest>(line.trim_end()).unwrap();
            let response = match index {
                0 => {
                    assert!(matches!(request, ControlRequest::RegisterProcess(_)));
                    ControlResponse::ProcessRegistered {
                        process_id: "proc_watch".to_owned(),
                        process_token: "token_watch".to_owned(),
                    }
                }
                _ => {
                    assert!(matches!(request, ControlRequest::Watch(_)));
                    thread::sleep(Duration::from_millis(200));
                    ControlResponse::ResourceChanged { event: None }
                }
            };
            stream
                .write_all(serde_json::to_string(&response).unwrap().as_bytes())
                .unwrap();
            stream.write_all(b"\n").unwrap();
        }
    });
    let mut client = MasterClient::connect_endpoint(endpoint).unwrap();
    client.register_process("watch_client").unwrap();
    client
        .set_request_timeout(Duration::from_millis(100))
        .unwrap();
    let result = client
        .watch_resource(WatchResource::Shutdown, 0, Duration::from_millis(500))
        .unwrap();
    assert!(result.is_none());
    let excessive = client
        .watch_resource(WatchResource::Shutdown, 0, Duration::from_secs(31))
        .expect_err("oversized watch cannot occupy a control connection");
    assert!(matches!(excessive, ZippyError::InvalidConfig { .. }));
    server.join().unwrap();
}

#[test]
fn master_client_register_process_over_tcp() {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    let endpoint = ControlEndpoint::Tcp(listener.local_addr().unwrap());
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut line = String::new();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        reader.read_line(&mut line).unwrap();
        let request = serde_json::from_str::<ControlRequest>(line.trim_end()).unwrap();
        assert!(matches!(request, ControlRequest::RegisterProcess(_)));
        let response = serde_json::to_string(&ControlResponse::ProcessRegistered {
            process_id: "proc_tcp".to_string(),
            process_token: "token_1".to_string(),
        })
        .unwrap();
        stream.write_all(response.as_bytes()).unwrap();
        stream.write_all(b"\n").unwrap();
    });

    let mut client = MasterClient::connect_endpoint(endpoint).unwrap();
    let process_id = client.register_process("tcp_client").unwrap();

    assert_eq!(process_id, "proc_tcp");
    handle.join().unwrap();
}

fn unique_socket_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("zippy-master-client-test-{nanos}.sock"))
}

fn wait_for_socket(socket_path: &Path) {
    for _ in 0..50 {
        if socket_path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("socket did not appear path={}", socket_path.display());
}

fn fake_watch_response(
    process_id: String,
    resource: WatchResource,
    after_revision: u64,
    timeout_ms: u64,
) -> ControlResponse {
    match resource {
        WatchResource::Shutdown => {
            assert_eq!(process_id, "proc_1");
            assert_eq!(after_revision, 0);
            assert!(timeout_ms > 0);
            ControlResponse::ResourceChanged { event: None }
        }
        WatchResource::Process { process_id } => ControlResponse::ResourceChanged {
            event: Some(zippy_core::ResourceEvent {
                resource: WatchResource::Process {
                    process_id: process_id.clone(),
                },
                revision: after_revision + 1,
                payload: serde_json::json!({
                    "process": {
                        "process_id": process_id,
                        "lease_status": "alive",
                    },
                }),
            }),
        },
    }
}

fn spawn_fake_server(socket_path: &Path, expected_connections: usize) -> thread::JoinHandle<()> {
    let socket_path = socket_path.to_path_buf();
    thread::spawn(move || {
        if socket_path.exists() {
            let _ = fs::remove_file(&socket_path);
        }

        let listener = UnixListener::bind(&socket_path).unwrap();
        for stream in listener.incoming().take(expected_connections) {
            let mut stream = stream.unwrap();
            let mut line = String::new();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            reader.read_line(&mut line).unwrap();

            let request: ControlRequest = serde_json::from_str(line.trim_end()).unwrap();
            let response = match request {
                ControlRequest::RegisterProcess(RegisterProcessRequest { app }) => {
                    assert_eq!(app, "local_dc");
                    ControlResponse::ProcessRegistered {
                        process_id: "proc_1".to_string(),
                        process_token: "token_1".to_string(),
                    }
                }
                ControlRequest::Heartbeat(HeartbeatRequest { process_id, .. }) => {
                    assert_eq!(process_id, "proc_1");
                    ControlResponse::HeartbeatAccepted {
                        process_id: "proc_1".to_string(),
                    }
                }
                ControlRequest::Watch(request) => fake_watch_response(
                    request.process_id,
                    request.resource,
                    request.after_revision,
                    request.timeout_ms,
                ),
                ControlRequest::UnregisterProcess(request) => {
                    assert_eq!(request.process_id, "proc_1");
                    ControlResponse::ProcessUnregistered {
                        process_id: request.process_id,
                    }
                }
                ControlRequest::GetConfig(_) => {
                    let mut config = ZippyConfig::default();
                    config.log.level = "warn".into();
                    ControlResponse::ConfigFetched {
                        config: config.to_json_value(),
                    }
                }
            };

            let payload = serde_json::to_string(&response).unwrap();
            stream.write_all(payload.as_bytes()).unwrap();
            stream.write_all(b"\n").unwrap();
            stream.flush().unwrap();
        }

        let _ = fs::remove_file(&socket_path);
    })
}

fn fetch_wire_config(config: serde_json::Value) -> zippy_core::Result<ZippyConfig> {
    let socket_path = unique_socket_path();
    let server_path = socket_path.clone();
    let server = thread::spawn(move || {
        let listener = UnixListener::bind(&server_path).unwrap();
        let (mut stream, _) = listener.accept().unwrap();
        let mut line = String::new();
        BufReader::new(stream.try_clone().unwrap())
            .read_line(&mut line)
            .unwrap();
        let request = serde_json::from_str::<ControlRequest>(line.trim_end()).unwrap();
        assert!(matches!(request, ControlRequest::GetConfig(_)));
        let response = serde_json::to_string(&ControlResponse::ConfigFetched { config }).unwrap();
        stream.write_all(response.as_bytes()).unwrap();
        stream.write_all(b"\n").unwrap();
        let _ = fs::remove_file(&server_path);
    });
    wait_for_socket(&socket_path);

    let result = MasterClient::connect(&socket_path).unwrap().get_config();

    server.join().unwrap();
    result
}

#[test]
fn master_client_gets_config() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 1);
    wait_for_socket(&socket_path);

    let client = MasterClient::connect(&socket_path).unwrap();
    let config = client.get_config().unwrap();

    assert_eq!(config.log.level, "warn");
    assert_eq!(config.query.plan_ttl_ms, 30_000);

    server.join().unwrap();
}

#[test]
fn master_client_accepts_canonical_and_rejects_removed_cpu_wire_config() {
    let mut config = ZippyConfig::default();
    config.runtime.max_cpu_cores = 6;
    config.query.resources.max_active_tasks = 24;
    let wire = config.to_json_value();
    let decoded = fetch_wire_config(wire.clone()).unwrap();
    assert_eq!(decoded.runtime.max_cpu_cores, 6);
    for canonical in [true, false] {
        let mut invalid = wire.clone();
        let runtime = invalid["runtime"].as_object_mut().unwrap();
        if !canonical {
            runtime.remove("max_cpu_cores");
        }
        runtime.insert("max_parallelism".to_string(), serde_json::json!(6));
        assert!(fetch_wire_config(invalid)
            .unwrap_err()
            .to_string()
            .contains("unknown field `max_parallelism`"));
    }
}

#[test]
fn master_client_sends_heartbeat_for_registered_process() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 2);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    client.heartbeat().unwrap();

    server.join().unwrap();
}

#[test]
fn master_client_wait_shutdown_times_out_without_shutdown_request() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 2);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();

    assert!(!client.wait_shutdown(Duration::from_millis(10)).unwrap());

    server.join().unwrap();
}

#[test]
fn master_client_unregisters_registered_process() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 2);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    client.unregister_process().unwrap();

    assert_eq!(client.process_id(), None);

    server.join().unwrap();
}
