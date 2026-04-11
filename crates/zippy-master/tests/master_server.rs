use zippy_core::bus_protocol::{
    ControlRequest, ControlResponse, RegisterProcessRequest, BUS_LAYOUT_VERSION,
};
use zippy_core::{setup_log, LogConfig};
use zippy_master::bus::Bus;
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

#[test]
fn protocol_types_roundtrip_debug_repr() {
    let request = ControlRequest::RegisterProcess(RegisterProcessRequest {
        app: "local_dc".to_string(),
    });

    let debug = format!("{request:?}");
    assert!(debug.contains("RegisterProcess"));

    let response = ControlResponse::ProcessRegistered {
        process_id: "proc_1".to_string(),
    };
    assert!(format!("{response:?}").contains("proc_1"));
}

#[test]
fn registry_stores_process_and_stream_records() {
    let mut registry = Registry::default();
    let process_id = registry.register_process("local_dc");
    registry.register_stream("openctp_ticks", 1024).unwrap();

    assert_eq!(registry.processes_len(), 1);
    assert_eq!(registry.streams_len(), 1);
    assert!(registry.get_process(&process_id).is_some());
    assert!(registry.get_stream("openctp_ticks").is_some());
}

#[test]
fn registry_rejects_duplicate_stream_names() {
    let mut registry = Registry::default();
    registry.register_stream("openctp_ticks", 1024).unwrap();

    let error = registry.register_stream("openctp_ticks", 1024).unwrap_err();
    assert!(format!("{error}").contains("stream already exists"));
}

#[test]
fn bus_enforces_single_writer_and_multiple_readers() {
    let mut bus = Bus::default();
    bus.create_stream("openctp_ticks", 1024).unwrap();

    let writer = bus.write_to("openctp_ticks", "proc_1").unwrap();
    let reader_a = bus.read_from("openctp_ticks", "proc_2").unwrap();
    let reader_b = bus.read_from("openctp_ticks", "proc_3").unwrap();

    assert_eq!(writer.stream_name, "openctp_ticks");
    assert_eq!(writer.ring_capacity, 1024);
    assert_eq!(writer.layout_version, BUS_LAYOUT_VERSION);
    assert!(writer.shm_name.contains("openctp_ticks"));
    assert_eq!(writer.next_write_seq, 1);
    assert_eq!(reader_a.stream_name, "openctp_ticks");
    assert_eq!(reader_a.ring_capacity, 1024);
    assert_eq!(reader_a.layout_version, BUS_LAYOUT_VERSION);
    assert!(reader_a.shm_name.contains("openctp_ticks"));
    assert_eq!(reader_a.next_read_seq, 1);
    assert_eq!(reader_b.stream_name, "openctp_ticks");
    assert_ne!(reader_a.reader_id, reader_b.reader_id);
    assert!(bus.write_to("openctp_ticks", "proc_9").is_err());
}

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

fn send_register_process(socket_path: &Path, app: &str) -> String {
    send_request(
        socket_path,
        &format!("{{\"RegisterProcess\":{{\"app\":\"{app}\"}}}}\n"),
    )
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
fn master_server_registers_stream_and_attaches_reader_writer() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_register_process(&socket_path, "local_dc");
    assert!(process_response.contains("proc_1"));

    let stream_response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"ring_capacity\":1024}}\n",
    );
    assert!(stream_response.contains("StreamRegistered"));
    assert!(stream_response.contains("openctp_ticks"));

    let writer_response = send_request(
        &socket_path,
        "{\"WriteTo\":{\"stream_name\":\"openctp_ticks\",\"process_id\":\"proc_1\"}}\n",
    );
    let writer_response: ControlResponse =
        serde_json::from_str(writer_response.trim_end()).unwrap();
    let writer_descriptor = match writer_response {
        ControlResponse::WriterAttached { descriptor } => descriptor,
        other => panic!("unexpected writer response: {:?}", other),
    };
    assert_eq!(writer_descriptor.stream_name, "openctp_ticks");
    assert_eq!(writer_descriptor.ring_capacity, 1024);
    assert_eq!(writer_descriptor.layout_version, BUS_LAYOUT_VERSION);
    assert!(writer_descriptor.shm_name.contains("openctp_ticks"));
    assert_eq!(writer_descriptor.next_write_seq, 1);

    let reader_response = send_request(
        &socket_path,
        "{\"ReadFrom\":{\"stream_name\":\"openctp_ticks\",\"process_id\":\"proc_2\"}}\n",
    );
    let reader_response: ControlResponse =
        serde_json::from_str(reader_response.trim_end()).unwrap();
    let reader_descriptor = match reader_response {
        ControlResponse::ReaderAttached { descriptor } => descriptor,
        other => panic!("unexpected reader response: {:?}", other),
    };
    assert_eq!(reader_descriptor.stream_name, "openctp_ticks");
    assert_eq!(reader_descriptor.ring_capacity, 1024);
    assert_eq!(reader_descriptor.layout_version, BUS_LAYOUT_VERSION);
    assert!(reader_descriptor.shm_name.contains("openctp_ticks"));
    assert_eq!(reader_descriptor.next_read_seq, 1);
    assert_eq!(reader_descriptor.reader_id, "reader_1");

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_rejects_duplicate_stream_names() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_register_process(&socket_path, "local_dc");
    assert!(process_response.contains("proc_1"));

    let first_response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"ring_capacity\":1024}}\n",
    );
    assert!(first_response.contains("StreamRegistered"));

    let duplicate_response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"ring_capacity\":1024}}\n",
    );
    assert!(duplicate_response.contains("Error"));
    assert!(duplicate_response.contains("stream already exists"));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_rejects_zero_capacity_stream_registration() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_register_process(&socket_path, "local_dc");
    assert!(process_response.contains("proc_1"));

    let response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"ring_capacity\":0}}\n",
    );
    assert!(response.contains("Error"));
    assert!(response.contains("invalid ring capacity"));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_lists_registered_streams_over_unix_socket() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_register_process(&socket_path, "local_dc");
    assert!(process_response.contains("proc_1"));

    let stream_response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"ring_capacity\":1024}}\n",
    );
    assert!(stream_response.contains("StreamRegistered"));

    let response = send_request(&socket_path, "{\"ListStreams\":{}}\n");
    let response: ControlResponse = serde_json::from_str(response.trim_end()).unwrap();
    let streams = match response {
        ControlResponse::StreamsListed(response) => response.streams,
        other => panic!("unexpected stream list response: {:?}", other),
    };
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].stream_name, "openctp_ticks");
    assert_eq!(streams[0].ring_capacity, 1024);

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_fetches_single_stream_over_unix_socket() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_register_process(&socket_path, "local_dc");
    assert!(process_response.contains("proc_1"));

    let stream_response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"ring_capacity\":1024}}\n",
    );
    assert!(stream_response.contains("StreamRegistered"));

    let response = send_request(
        &socket_path,
        "{\"GetStream\":{\"stream_name\":\"openctp_ticks\"}}\n",
    );
    let response: ControlResponse = serde_json::from_str(response.trim_end()).unwrap();
    let stream = match response {
        ControlResponse::StreamFetched(response) => response.stream,
        other => panic!("unexpected get stream response: {:?}", other),
    };
    assert_eq!(stream.stream_name, "openctp_ticks");
    assert_eq!(stream.ring_capacity, 1024);

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
    );
    assert!(
        !records
            .iter()
            .any(|record| { record["event"] == "master_stopped" && record["status"] == "stopped" }),
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

    let process_response = send_register_process(&socket_path, "local_dc");
    assert!(process_response.contains("proc_1"));

    let stream_response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"ring_capacity\":1024}}\n",
    );
    assert!(stream_response.contains("StreamRegistered"));

    let duplicate_response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"ring_capacity\":1024}}\n",
    );
    assert!(duplicate_response.contains("Error"));
    assert!(duplicate_response.contains("stream already exists"));

    let writer_response = send_request(
        &socket_path,
        "{\"WriteTo\":{\"stream_name\":\"openctp_ticks\",\"process_id\":\"proc_1\"}}\n",
    );
    assert!(writer_response.contains("WriterAttached"));

    let reader_response = send_request(
        &socket_path,
        "{\"ReadFrom\":{\"stream_name\":\"openctp_ticks\",\"process_id\":\"proc_2\"}}\n",
    );
    assert!(reader_response.contains("ReaderAttached"));

    let list_response = send_request(&socket_path, "{\"ListStreams\":{}}\n");
    assert!(list_response.contains("StreamsListed"));

    let get_response = send_request(
        &socket_path,
        "{\"GetStream\":{\"stream_name\":\"openctp_ticks\"}}\n",
    );
    assert!(get_response.contains("StreamFetched"));

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
    );
    assert_record_has_fields(
        &records,
        "register_stream",
        "success",
        "INFO",
        &[
            ("component", "master_server"),
            ("stream_name", "openctp_ticks"),
        ],
        &[("ring_capacity", 1024)],
    );
    assert_record_has_fields(
        &records,
        "register_stream",
        "error",
        "ERROR",
        &[
            ("component", "master_server"),
            ("stream_name", "openctp_ticks"),
            ("error", "stream already exists stream_name=[openctp_ticks]"),
        ],
        &[("ring_capacity", 1024)],
    );
    assert_record_has_fields(
        &records,
        "write_to",
        "success",
        "INFO",
        &[
            ("component", "master_server"),
            ("stream_name", "openctp_ticks"),
            ("process_id", "proc_1"),
        ],
        &[],
    );
    assert_record_has_fields(
        &records,
        "read_from",
        "success",
        "INFO",
        &[
            ("component", "master_server"),
            ("stream_name", "openctp_ticks"),
            ("process_id", "proc_2"),
        ],
        &[],
    );
    assert_record_has_fields(
        &records,
        "list_streams",
        "success",
        "INFO",
        &[("component", "master_server")],
        &[("stream_count", 1)],
    );
    assert_record_has_fields(
        &records,
        "get_stream",
        "success",
        "INFO",
        &[
            ("component", "master_server"),
            ("stream_name", "openctp_ticks"),
        ],
        &[],
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
) {
    let record = records
        .iter()
        .find(|record| {
            record["event"] == event && record["status"] == status && record["level"] == level
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
    assert!(
        record["message"].as_str().is_some(),
        "missing message field event=[{}]",
        event
    );
}
