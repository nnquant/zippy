#![cfg(unix)]

use zippy_core::bus_protocol::{
    ControlRequest, ControlResponse, RegisterProcessRequest, UnregisterProcessRequest,
    WatchRequest, WatchResource,
};
use zippy_core::{setup_log, LogConfig};
use zippy_master::registry::Registry;
use zippy_master::server::MasterServer;

use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;

const MASTER_SERVER_LOGGING_CASE_ENV: &str = "ZIPPY_MASTER_SERVER_LOGGING_CASE";
const MASTER_SERVER_LOGGING_TEMP_ENV: &str = "ZIPPY_MASTER_SERVER_LOGGING_TEMP";

fn unique_socket_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("zippy-master-test-{nanos}.sock"))
}

fn spawn_test_server(socket_path: &Path) -> (MasterServer, thread::JoinHandle<()>) {
    let server = MasterServer::default();
    let handle_server = server.clone();
    let socket_path = socket_path.to_path_buf();
    let wait_path = socket_path.clone();
    let join_handle = thread::spawn(move || {
        handle_server.serve(&socket_path).unwrap();
    });
    wait_for_socket_ready(&wait_path);
    (server, join_handle)
}

fn spawn_test_server_result(
    socket_path: &Path,
) -> (MasterServer, thread::JoinHandle<zippy_core::Result<()>>) {
    let server = MasterServer::default();
    let handle_server = server.clone();
    let socket_path = socket_path.to_path_buf();
    let wait_path = socket_path.clone();
    let join_handle = thread::spawn(move || handle_server.serve(&socket_path));
    wait_for_socket_ready(&wait_path);
    (server, join_handle)
}

fn wait_for_socket_ready(socket_path: &Path) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        match UnixStream::connect(socket_path) {
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

fn register_process(socket_path: &Path, app: &str) -> (String, String) {
    let response = send_control_request(
        socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: app.to_string(),
        }),
    );
    match response {
        ControlResponse::ProcessRegistered {
            process_id,
            process_token,
        } => (process_id, process_token),
        other => panic!("unexpected register process response: {other:?}"),
    }
}

fn send_register_process(socket_path: &Path, app: &str) -> String {
    send_request(
        socket_path,
        &format!("{{\"RegisterProcess\":{{\"app\":\"{app}\"}}}}\n"),
    )
}

fn send_control_request(socket_path: &Path, request: ControlRequest) -> ControlResponse {
    let payload = format!("{}\n", serde_json::to_string(&request).unwrap());
    let response = send_request(socket_path, &payload);
    serde_json::from_str(response.trim_end()).unwrap()
}

fn send_request(socket_path: &Path, payload: &str) -> String {
    let mut stream = UnixStream::connect(socket_path).unwrap();
    stream.write_all(payload.as_bytes()).unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

fn send_request_with_timeout(socket_path: &Path, payload: &str) -> String {
    let mut stream = UnixStream::connect(socket_path).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    stream.write_all(payload.as_bytes()).unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

fn send_payload_and_close(socket_path: &Path, payload: &str) {
    let mut stream = UnixStream::connect(socket_path).unwrap();
    stream.write_all(payload.as_bytes()).unwrap();
    let _ = stream.shutdown(std::net::Shutdown::Both);
}

#[test]
fn protocol_types_roundtrip_debug_repr() {
    let request = ControlRequest::RegisterProcess(RegisterProcessRequest {
        app: "worker".into(),
    });
    let wire = serde_json::to_string(&request).unwrap();
    let decoded: ControlRequest = serde_json::from_str(&wire).unwrap();
    assert_eq!(format!("{request:?}"), format!("{decoded:?}"));
}

#[test]
fn removed_control_envelopes_are_rejected() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);
    for payload in [
        serde_json::json!({"Envelope": {"version": 2, "request_id": "same", "verb": "register_process", "payload": {"app": "old"}}}),
        serde_json::json!({"Envelope": {"version": 999, "request_id": "same", "inner": {"ListStreams": {}}}}),
    ] {
        assert!(serde_json::from_value::<ControlRequest>(payload.clone()).is_err());
        let mut stream = UnixStream::connect(&socket_path).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        writeln!(stream, "{payload}").unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.is_empty());
    }
    assert_eq!(server.registry().lock().unwrap().processes_len(), 0);
    server.shutdown();
    join_handle.join().unwrap();
}

#[test]
fn master_server_rejects_raw_heartbeat_without_process_capability() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "owner".to_string(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id,
        process_token: _,
    } = process_response
    else {
        panic!("unexpected process response {process_response:?}");
    };

    let response = send_control_request(
        &socket_path,
        ControlRequest::Heartbeat(zippy_core::HeartbeatRequest {
            process_id,
            process_token: None,
        }),
    );

    assert!(
        matches!(response, ControlResponse::Error { .. }),
        "raw heartbeat without process capability must be rejected response=[{response:?}]"
    );

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn registry_stores_only_process_lease_records() {
    let mut registry = Registry::default();
    let process_id = registry.register_process("worker").unwrap();
    assert_eq!(registry.processes_len(), 1);
    assert_eq!(registry.get_process(&process_id).unwrap().app, "worker");
    registry.unregister_process(&process_id).unwrap();
    assert_eq!(registry.processes_len(), 0);
}

#[test]
fn registry_updates_process_lease_timestamp_on_heartbeat() {
    let mut registry = Registry::default();
    let process_id = registry.register_process("local_dc").unwrap();

    let first_seen = registry.get_process(&process_id).unwrap().last_heartbeat_at;
    thread::sleep(Duration::from_millis(5));
    registry.record_heartbeat(&process_id).unwrap();
    let second_seen = registry.get_process(&process_id).unwrap().last_heartbeat_at;

    assert!(second_seen >= first_seen);
    assert_eq!(
        registry.get_process(&process_id).unwrap().lease_status,
        "alive"
    );
}

#[test]
fn registry_rejects_heartbeat_for_expired_process() {
    let mut registry = Registry::default();
    let process_id = registry.register_process("local_dc").unwrap();

    assert!(registry.claim_expired_process(&process_id, 0).unwrap());

    let error = registry.record_heartbeat(&process_id).unwrap_err();
    assert!(format!("{error}").contains("process lease expired"));
    assert!(format!("{error}").contains(&process_id));
}

#[test]
fn master_server_accepts_process_heartbeat_over_unix_socket() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_register_process(&socket_path, "local_dc");
    let process_response: ControlResponse =
        serde_json::from_str(process_response.trim_end()).unwrap();
    let (process_id, process_token) = match process_response {
        ControlResponse::ProcessRegistered {
            process_id,
            process_token,
        } => (process_id, process_token),
        other => panic!("unexpected process response: {:?}", other),
    };

    let response = send_control_request(
        &socket_path,
        ControlRequest::Heartbeat(zippy_core::HeartbeatRequest {
            process_id: process_id.clone(),
            process_token: Some(process_token),
        }),
    );
    match response {
        ControlResponse::HeartbeatAccepted {
            process_id: accepted_process_id,
        } => assert_eq!(accepted_process_id, process_id),
        other => panic!("unexpected heartbeat response: {:?}", other),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_returns_structured_error_for_unknown_process_heartbeat() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let response = send_request(
        &socket_path,
        "{\"Heartbeat\":{\"process_id\":\"proc_missing\"}}\n",
    );
    let response: ControlResponse = serde_json::from_str(response.trim_end()).unwrap();
    match response {
        ControlResponse::Error { reason, .. } => {
            assert!(reason.contains("process not found"));
            assert!(reason.contains("proc_missing"));
        }
        other => panic!("unexpected heartbeat response: {:?}", other),
    }

    let process_response = send_register_process(&socket_path, "local_dc");
    assert!(process_response.contains("proc_1"));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_registers_process_over_unix_socket() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let response = send_register_process(&socket_path, "local_dc");
    assert!(response.contains("proc_1"));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_graceful_shutdown_notifies_process_and_waits_for_unregister() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "local_dc".to_string(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id,
        process_token,
    } = process_response
    else {
        panic!("unexpected process response {process_response:?}");
    };

    let shutdown_server = server.clone();
    let shutdown_handle =
        thread::spawn(move || shutdown_server.request_graceful_shutdown(Duration::from_secs(1)));

    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    let shutdown_response = loop {
        let response = send_control_request(
            &socket_path,
            ControlRequest::Heartbeat(zippy_core::HeartbeatRequest {
                process_id: process_id.clone(),
                process_token: Some(process_token.clone()),
            }),
        );
        if matches!(response, ControlResponse::ShutdownRequested { .. }) {
            break response;
        }
        if std::time::Instant::now() >= deadline {
            panic!("heartbeat did not receive shutdown request response={response:?}");
        }
        thread::sleep(Duration::from_millis(10));
    };
    match shutdown_response {
        ControlResponse::ShutdownRequested {
            process_id: notified_process_id,
            reason,
        } => {
            assert_eq!(notified_process_id, process_id);
            assert_eq!(reason, "master shutdown requested");
        }
        other => panic!("unexpected shutdown response {other:?}"),
    }

    let unregister_response = send_control_request(
        &socket_path,
        ControlRequest::UnregisterProcess(UnregisterProcessRequest {
            process_id: process_id.clone(),
            process_token: Some(process_token),
        }),
    );
    match unregister_response {
        ControlResponse::ProcessUnregistered {
            process_id: unregistered_process_id,
        } => assert_eq!(unregistered_process_id, process_id),
        other => panic!("unexpected unregister response {other:?}"),
    }

    let outcome = shutdown_handle.join().unwrap();
    assert!(outcome.timed_out_processes.is_empty());
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_rejects_unbounded_watch_before_waiting() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);
    let registered = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "bounded_watch".to_owned(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id,
        process_token,
    } = registered
    else {
        panic!("unexpected registration response {registered:?}");
    };

    let request = ControlRequest::Watch(WatchRequest {
        process_id: process_id.clone(),
        process_token: Some(process_token.clone()),
        resource: WatchResource::Shutdown,
        after_revision: 0,
        timeout_ms: u64::MAX,
    });
    let payload = format!("{}\n", serde_json::to_string(&request).unwrap());
    let response: ControlResponse =
        serde_json::from_str(send_request_with_timeout(&socket_path, &payload).trim_end()).unwrap();
    assert!(matches!(
        response,
        ControlResponse::Error {
            code: zippy_types::ErrorCode::InvalidArgument,
            retryable: false,
            ..
        }
    ));

    let unregistered = send_control_request(
        &socket_path,
        ControlRequest::UnregisterProcess(UnregisterProcessRequest {
            process_id,
            process_token: Some(process_token),
        }),
    );
    assert!(matches!(
        unregistered,
        ControlResponse::ProcessUnregistered { .. }
    ));
    server.request_graceful_shutdown(Duration::from_secs(1));
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_watch_shutdown_wakes_without_heartbeat_polling() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "local_dc".to_string(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id,
        process_token,
    } = process_response
    else {
        panic!("unexpected process response {process_response:?}");
    };

    let wait_socket_path = socket_path.clone();
    let wait_process_id = process_id.clone();
    let wait_process_token = process_token.clone();
    let wait_handle = thread::spawn(move || {
        send_control_request(
            &wait_socket_path,
            ControlRequest::Watch(WatchRequest {
                process_id: wait_process_id,
                process_token: Some(wait_process_token),
                resource: WatchResource::Shutdown,
                after_revision: 0,
                timeout_ms: 1_000,
            }),
        )
    });

    thread::sleep(Duration::from_millis(20));
    let shutdown_server = server.clone();
    let shutdown_handle =
        thread::spawn(move || shutdown_server.request_graceful_shutdown(Duration::from_secs(1)));

    let response = wait_handle.join().unwrap();
    match response {
        ControlResponse::ResourceChanged { event: Some(event) } => {
            assert_eq!(event.resource, WatchResource::Shutdown);
            assert_eq!(event.revision, 1);
            assert_eq!(event.payload["process_id"], serde_json::json!(process_id));
            assert_eq!(
                event.payload["reason"],
                serde_json::json!("master shutdown requested")
            );
        }
        other => panic!("unexpected watch shutdown response {other:?}"),
    }

    let unregister_response = send_control_request(
        &socket_path,
        ControlRequest::UnregisterProcess(UnregisterProcessRequest {
            process_id: process_id.clone(),
            process_token: Some(process_token),
        }),
    );
    assert!(matches!(
        unregister_response,
        ControlResponse::ProcessUnregistered { .. }
    ));
    let outcome = shutdown_handle.join().unwrap();
    assert!(outcome.timed_out_processes.is_empty());
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_graceful_shutdown_times_out_with_alive_processes() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "local_dc".to_string(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id,
        process_token: _,
    } = process_response
    else {
        panic!("unexpected process response {process_response:?}");
    };

    let outcome = server.request_graceful_shutdown(Duration::from_millis(20));

    assert_eq!(outcome.timed_out_processes, vec![process_id.clone()]);
    assert!(
        !server.is_running(),
        "master must stop after strict shutdown timeout"
    );
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_watch_process_wakes_on_heartbeat() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let target_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "openctp".to_string(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id,
        process_token,
    } = target_response
    else {
        panic!("unexpected target process response {target_response:?}");
    };
    let watcher_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "process_watcher".to_string(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id: watcher_process_id,
        process_token: watcher_process_token,
    } = watcher_response
    else {
        panic!("unexpected watcher process response {watcher_response:?}");
    };

    let watch_socket_path = socket_path.clone();
    let watch_process_id = process_id.clone();
    let watch_handle = thread::spawn(move || {
        send_control_request(
            &watch_socket_path,
            ControlRequest::Watch(WatchRequest {
                process_id: watcher_process_id,
                process_token: Some(watcher_process_token),
                resource: WatchResource::Process {
                    process_id: watch_process_id,
                },
                after_revision: 1,
                timeout_ms: 1_000,
            }),
        )
    });

    thread::sleep(Duration::from_millis(20));
    let heartbeat_response = send_control_request(
        &socket_path,
        ControlRequest::Heartbeat(zippy_core::bus_protocol::HeartbeatRequest {
            process_id: process_id.clone(),
            process_token: Some(process_token),
        }),
    );
    assert!(matches!(
        heartbeat_response,
        ControlResponse::HeartbeatAccepted { .. }
    ));

    let watch_response = watch_handle.join().unwrap();
    match watch_response {
        ControlResponse::ResourceChanged { event: Some(event) } => {
            assert_eq!(
                event.resource,
                WatchResource::Process {
                    process_id: process_id.clone()
                }
            );
            assert!(event.revision > 1);
            assert_eq!(
                event.payload["process"]["process_id"],
                serde_json::json!(process_id)
            );
            assert_eq!(
                event.payload["process"]["lease_status"],
                serde_json::json!("alive")
            );
        }
        other => panic!("unexpected watch process response {other:?}"),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_survives_bad_request_and_serves_next_request() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    send_payload_and_close(&socket_path, "not json\n");
    thread::sleep(Duration::from_millis(100));

    let response = send_register_process(&socket_path, "local_dc");
    assert!(response.contains("proc_1"));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_handles_slow_client_without_blocking_accept_loop() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let silent_client = UnixStream::connect(&socket_path).unwrap();
    thread::sleep(Duration::from_millis(100));

    let response = send_request_with_timeout(
        &socket_path,
        "{\"RegisterProcess\":{\"app\":\"local_dc\"}}\n",
    );
    assert!(response.contains("proc_1"));

    drop(silent_client);
    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_rejects_regular_file_at_socket_path() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    fs::write(&socket_path, "not a socket").unwrap();

    let server = MasterServer::default();
    let result = server.serve(&socket_path);

    assert!(result.is_err());
    assert!(socket_path.exists());
}

#[test]
fn master_server_emits_structured_control_plane_logs() {
    run_master_server_logging_case("control_plane_logs");
}

#[test]
fn master_server_ignores_empty_control_probe_logs() {
    run_master_server_logging_case("empty_control_probe");
}

#[test]
fn master_server_logs_cleanup_failure_without_stopped_log() {
    let temp = tempfile::tempdir().unwrap();
    let socket_dir = temp.path().join("srv");
    fs::create_dir_all(&socket_dir).unwrap();

    let socket_path = socket_dir.join("master.sock");
    let log_dir = temp.path().join("logs");

    let snapshot = setup_log(LogConfig::new(
        "master_server_test",
        "info",
        &log_dir,
        false,
        true,
    ))
    .unwrap();
    let log_file = snapshot.file_path.expect("missing log file path");

    let (server, join_handle) = spawn_test_server_result(&socket_path);

    let metadata = fs::metadata(&socket_dir).unwrap();
    let mut permissions = metadata.permissions();
    permissions.set_mode(0o555);
    fs::set_permissions(&socket_dir, permissions).unwrap();

    server.shutdown();
    let result = join_handle.join().unwrap();
    assert!(result.is_err());

    let records = read_jsonl_records(&log_file);
    assert_record_has_fields(
        &records,
        "master_cleanup_error",
        "error",
        "ERROR",
        &[("component", "master")],
        &[],
        &[],
    );
    assert!(
        !records.iter().any(|record| {
            record["event"] == "master_stopped"
                && record["status"] == "stopped"
                && record["control_endpoint"].as_str()
                    == Some(socket_path.to_string_lossy().as_ref())
        }),
        "unexpected stopped log after cleanup failure"
    );
}

#[test]
fn master_server_logging_case_dispatch() {
    let Ok(case) = std::env::var(MASTER_SERVER_LOGGING_CASE_ENV) else {
        return;
    };
    let temp_dir = PathBuf::from(std::env::var(MASTER_SERVER_LOGGING_TEMP_ENV).unwrap());

    match case.as_str() {
        "control_plane_logs" => master_server_control_plane_logging_case(&temp_dir),
        "empty_control_probe" => master_server_empty_control_probe_logging_case(&temp_dir),
        other => panic!("unknown master server logging test case: {other}"),
    }
}

fn run_master_server_logging_case(case: &str) {
    let temp = tempfile::tempdir().unwrap();
    let current_exe = std::env::current_exe().unwrap();
    let status = Command::new(current_exe)
        .arg("--exact")
        .arg("master_server_logging_case_dispatch")
        .env(MASTER_SERVER_LOGGING_CASE_ENV, case)
        .env(MASTER_SERVER_LOGGING_TEMP_ENV, temp.path())
        .status()
        .unwrap();

    assert!(
        status.success(),
        "master server logging case failed case=[{case}]"
    );
}

fn master_server_control_plane_logging_case(temp_dir: &Path) {
    let socket_path = temp_dir.join("master.sock");
    let log_dir = temp_dir.join("logs");

    let snapshot = setup_log(LogConfig::new(
        "master_server_test",
        "info",
        &log_dir,
        false,
        true,
    ))
    .unwrap();
    let log_file = snapshot.file_path.expect("missing log file path");

    let (server, join_handle) = spawn_test_server(&socket_path);

    register_process(&socket_path, "local_dc");

    let config_response = send_request(&socket_path, "{\"GetConfig\":{}}\n");
    assert!(config_response.contains("ConfigFetched"));
    assert!(config_response.contains("\"max_cpu_cores\":4"));
    assert!(config_response.contains("\"max_cpu_cores\":4"));

    server.shutdown();
    join_handle.join().unwrap();

    let records = read_jsonl_records(&log_file);
    assert_record_has_fields(
        &records,
        "register_process",
        "success",
        "INFO",
        &[("component", "master_server"), ("process_id", "proc_1")],
        &[],
        &[],
    );
    assert_no_record(&records, "get_config", "success", "INFO");
}

fn master_server_empty_control_probe_logging_case(temp_dir: &Path) {
    let socket_path = temp_dir.join("master.sock");
    let log_dir = temp_dir.join("logs");

    let snapshot = setup_log(LogConfig::new(
        "master_server_test",
        "info",
        &log_dir,
        false,
        true,
    ))
    .unwrap();
    let log_file = snapshot.file_path.expect("missing log file path");

    let (server, join_handle) = spawn_test_server(&socket_path);
    let stream = UnixStream::connect(&socket_path).unwrap();
    drop(stream);
    thread::sleep(Duration::from_millis(100));

    server.shutdown();
    join_handle.join().unwrap();

    let records = read_jsonl_records(&log_file);
    assert!(
        !records
            .iter()
            .any(|record| record["event"] == "control_connection_error"),
        "empty control probes should not emit control connection warnings"
    );
}

fn read_jsonl_records(log_file: &Path) -> Vec<Value> {
    let contents = fs::read_to_string(log_file).unwrap();
    contents
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect()
}

fn assert_record_has_fields(
    records: &[Value],
    event: &str,
    status: &str,
    level: &str,
    expected_fields: &[(&str, &str)],
    expected_numbers: &[(&str, u64)],
    expected_bools: &[(&str, bool)],
) {
    let record = records
        .iter()
        .find(|record| {
            if !(record["event"] == event && record["status"] == status && record["level"] == level)
            {
                return false;
            }

            let fields_match = expected_fields
                .iter()
                .all(|(field, expected)| record[*field].as_str() == Some(*expected));
            let numbers_match = expected_numbers
                .iter()
                .all(|(field, expected)| record[*field].as_u64() == Some(*expected));
            let bools_match = expected_bools
                .iter()
                .all(|(field, expected)| record[*field].as_bool() == Some(*expected));

            fields_match && numbers_match && bools_match
        })
        .unwrap_or_else(|| {
            panic!(
                "missing log record event=[{}] status=[{}] level=[{}]",
                event, status, level
            )
        });

    for (field, expected) in expected_fields {
        assert_eq!(
            record[*field].as_str(),
            Some(*expected),
            "unexpected field field=[{}] event=[{}]",
            field,
            event
        );
    }
    for (field, expected) in expected_numbers {
        assert_eq!(
            record[*field].as_u64(),
            Some(*expected),
            "unexpected numeric field field=[{}] event=[{}]",
            field,
            event
        );
    }
    for (field, expected) in expected_bools {
        assert_eq!(
            record[*field].as_bool(),
            Some(*expected),
            "unexpected bool field field=[{}] event=[{}]",
            field,
            event
        );
    }
    assert!(
        record["message"].as_str().is_some(),
        "missing message field event=[{}]",
        event
    );
}

fn assert_no_record(records: &[Value], event: &str, status: &str, level: &str) {
    assert!(
        !records.iter().any(|record| {
            record["event"] == event && record["status"] == status && record["level"] == level
        }),
        "unexpected log record event=[{}] status=[{}] level=[{}]",
        event,
        status,
        level
    );
}

#[test]
fn retired_data_control_requests_are_rejected_without_mutation() {
    let socket_path = unique_socket_path();
    let (server, handle) = spawn_test_server(&socket_path);
    for verb in [
        "RegisterStream",
        "RegisterSource",
        "UnregisterSource",
        "RegisterEngine",
        "UpdateStatus",
        "ListStreams",
        "ListStreamStatuses",
        "GetStream",
        "GetStreamStatus",
        "DropTable",
        "PublishSegmentDescriptor",
        "PublishPersistedFile",
        "ReplacePersistedFiles",
        "PublishPersistEvent",
        "AcquireSegmentReaderLease",
        "ReleaseSegmentReaderLease",
        "GetSegmentDescriptor",
    ] {
        let payload = format!("{{\"{verb}\":{{}}}}\n");
        assert!(
            serde_json::from_str::<ControlRequest>(&payload).is_err(),
            "{verb}"
        );
        assert!(send_request(&socket_path, &payload).is_empty(), "{verb}");
        assert_eq!(server.registry().lock().unwrap().processes_len(), 0);
    }
    for resource in ["Source", "Stream", "PersistedFile", "SegmentDescriptor"] {
        let payload = format!("{{\"{resource}\":{{}}}}");
        assert!(
            serde_json::from_str::<WatchResource>(&payload).is_err(),
            "{resource}"
        );
    }
    let (id, token) = register_process(&socket_path, "native_client");
    assert!(!token.is_empty());
    assert_eq!(
        server
            .registry()
            .lock()
            .unwrap()
            .get_process(&id)
            .unwrap()
            .app,
        "native_client"
    );
    server.shutdown();
    handle.join().unwrap();
}

#[test]
fn unauthenticated_test_control_request_cannot_expire_a_process() {
    let socket_path = unique_socket_path();
    let (server, handle) = spawn_test_server(&socket_path);
    let (process_id, process_token) = register_process(&socket_path, "worker");
    let revision = server
        .registry()
        .lock()
        .unwrap()
        .get_process(&process_id)
        .unwrap()
        .revision;

    let payload = format!("{{\"ExpireProcessForTest\":{{\"process_id\":\"{process_id}\"}}}}\n");
    assert!(serde_json::from_str::<ControlRequest>(&payload).is_err());
    assert!(send_request(&socket_path, &payload).is_empty());
    let registry = server.registry();
    let registry = registry.lock().unwrap();
    let process = registry.get_process(&process_id).unwrap();
    assert_eq!(process.lease_status, "alive");
    assert_eq!(process.revision, revision);
    drop(registry);

    let heartbeat = send_control_request(
        &socket_path,
        ControlRequest::Heartbeat(zippy_core::bus_protocol::HeartbeatRequest {
            process_id,
            process_token: Some(process_token),
        }),
    );
    assert!(matches!(
        heartbeat,
        ControlResponse::HeartbeatAccepted { .. }
    ));
    server.shutdown();
    handle.join().unwrap();
}

#[test]
fn process_registration_rejects_oversized_app_without_mutation() {
    let socket_path = unique_socket_path();
    let (server, handle) = spawn_test_server(&socket_path);
    let response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "a".repeat(zippy_master::registry::MAX_PROCESS_APP_BYTES + 1),
        }),
    );
    assert!(matches!(
        response,
        ControlResponse::Error {
            code: zippy_types::ErrorCode::InvalidArgument,
            retryable: false,
            ..
        }
    ));
    assert_eq!(server.registry().lock().unwrap().processes_len(), 0);
    assert_eq!(register_process(&socket_path, "worker").0, "proc_1");
    server.shutdown();
    handle.join().unwrap();
}

#[test]
fn lease_reaper_revokes_process_and_preserves_configuration_discovery() {
    let socket_path = unique_socket_path();
    let server =
        MasterServer::with_runtime_config(Duration::from_millis(20), Duration::from_millis(5));
    let worker = server.clone();
    let path = socket_path.clone();
    let handle = thread::spawn(move || worker.serve(&path).unwrap());
    wait_for_socket_ready(&socket_path);
    let (id, token) = register_process(&socket_path, "expired_client");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while server
        .registry()
        .lock()
        .unwrap()
        .get_process(&id)
        .unwrap()
        .lease_status
        == "alive"
    {
        assert!(
            std::time::Instant::now() < deadline,
            "lease reaper failed to revoke process"
        );
        thread::sleep(Duration::from_millis(5));
    }
    let response = send_control_request(
        &socket_path,
        ControlRequest::Heartbeat(zippy_core::HeartbeatRequest {
            process_id: id,
            process_token: Some(token),
        }),
    );
    assert!(matches!(
        response,
        ControlResponse::Error {
            code: zippy_types::ErrorCode::Unauthenticated,
            ..
        }
    ));
    let response = send_request(&socket_path, "{\"GetConfig\":{}}\n");
    assert!(matches!(
        serde_json::from_str::<ControlResponse>(&response).unwrap(),
        ControlResponse::ConfigFetched { .. }
    ));
    server.shutdown();
    handle.join().unwrap();
}
