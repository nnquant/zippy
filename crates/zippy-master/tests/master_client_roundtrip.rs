#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::datatypes::{DataType, Field, Schema};
use zippy_core::{canonical_schema_hash, MasterClient, SchemaRef, ZippyConfig};
use zippy_master::server::MasterServer;
use zippy_master::snapshot::SnapshotStore;

fn test_schema() -> SchemaRef {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("mid_price", DataType::Float64, false),
    ]))
}

fn incompatible_schema() -> SchemaRef {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("bid_price", DataType::Float64, false),
    ]))
}

fn unique_socket_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("zippy-master-roundtrip-{nanos}.sock"))
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

fn spawn_test_server_with_lease(
    socket_path: &Path,
    lease_timeout: Duration,
    lease_reaper_interval: Duration,
) -> (MasterServer, thread::JoinHandle<()>) {
    let server = MasterServer::with_runtime_config(None, lease_timeout, lease_reaper_interval);
    let handle_server = server.clone();
    let socket_path = socket_path.to_path_buf();
    let wait_path = socket_path.clone();
    let join_handle = thread::spawn(move || {
        handle_server.serve(&socket_path).unwrap();
    });
    wait_for_socket_ready(&wait_path);
    (server, join_handle)
}

fn seed_sealed_segment_identity(
    server: &MasterServer,
    stream_name: &str,
    segment_id: u64,
    generation: u64,
) {
    let registry_handle = server.registry();
    let mut registry = registry_handle.lock().unwrap();
    let stream = registry.get_stream(stream_name).unwrap().clone();
    registry
        .set_stream_segment_metadata(
            stream_name,
            stream.descriptor_generation.saturating_add(1),
            vec![serde_json::json!({
                "segment_id": segment_id,
                "generation": generation,
            })],
            stream.persisted_files,
            stream.persist_events,
            stream.persist_revision,
            stream.segment_reader_leases,
            stream.writer_epoch,
        )
        .unwrap();
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

fn persist_root_config(persist_root: &Path) -> ZippyConfig {
    let mut config = ZippyConfig::default();
    config.table.persist.data_dir = persist_root.to_string_lossy().to_string();
    config
}

fn spawn_test_server_with_snapshot_and_config(
    socket_path: &Path,
    snapshot_path: PathBuf,
    config: ZippyConfig,
) -> (MasterServer, thread::JoinHandle<()>) {
    let server = MasterServer::with_runtime_config_and_config(
        Some(snapshot_path),
        Duration::from_secs(10),
        Duration::from_secs(2),
        config,
    );
    let handle_server = server.clone();
    let socket_path = socket_path.to_path_buf();
    let wait_path = socket_path.clone();
    let join_handle = thread::spawn(move || {
        handle_server.serve(&socket_path).unwrap();
    });
    wait_for_socket_ready(&wait_path);
    (server, join_handle)
}

fn spawn_test_server_with_snapshot_and_lease(
    socket_path: &Path,
    snapshot_path: PathBuf,
    lease_timeout: Duration,
    lease_reaper_interval: Duration,
) -> (MasterServer, thread::JoinHandle<()>) {
    let server = MasterServer::with_runtime_config(
        Some(snapshot_path),
        lease_timeout,
        lease_reaper_interval,
    );
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

fn wait_for_file(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("file did not appear path=[{}]", path.display());
}

fn run_control_plane_child_process() {
    let Ok(role) = std::env::var("ZIPPY_CONTROL_PLANE_CHILD_ROLE") else {
        return;
    };
    let socket_path = PathBuf::from(std::env::var("ZIPPY_CONTROL_PLANE_SOCKET").unwrap());
    let ready_path = PathBuf::from(std::env::var("ZIPPY_CONTROL_PLANE_READY").unwrap());
    let trigger_path = PathBuf::from(std::env::var("ZIPPY_CONTROL_PLANE_TRIGGER").unwrap());
    let result_path = PathBuf::from(std::env::var("ZIPPY_CONTROL_PLANE_RESULT").unwrap());

    match role.as_str() {
        "old_writer" => {
            let mut client = MasterClient::connect(&socket_path).unwrap();
            client.register_process("old_os_writer").unwrap();
            client
                .register_stream("os_ticks", test_schema(), 64, 4096)
                .unwrap();
            client
                .register_source(
                    "old_os_source",
                    "openctp",
                    "os_ticks",
                    serde_json::json!({}),
                )
                .unwrap();
            client
                .publish_segment_descriptor(
                    "os_ticks",
                    serde_json::json!({
                        "magic": "zippy.segment.active",
                        "version": 1,
                        "schema_id": 7,
                        "row_capacity": 64,
                        "shm_os_id": "/tmp/zippy-os-writer-old",
                        "payload_offset": 64,
                        "committed_row_count_offset": 40,
                        "segment_id": 1,
                        "generation": 0,
                    }),
                )
                .unwrap();
            fs::write(&ready_path, b"ready").unwrap();
            wait_for_file(&trigger_path, Duration::from_secs(2));
            let result = client
                .publish_segment_descriptor(
                    "os_ticks",
                    serde_json::json!({
                        "magic": "zippy.segment.active",
                        "version": 1,
                        "schema_id": 7,
                        "row_capacity": 64,
                        "shm_os_id": "/tmp/zippy-os-writer-stale",
                        "payload_offset": 64,
                        "committed_row_count_offset": 40,
                        "segment_id": 3,
                        "generation": 2,
                        "writer_epoch": 1,
                    }),
                )
                .map(|_| "ok".to_string())
                .unwrap_or_else(|error| error.to_string());
            fs::write(&result_path, result).unwrap();
        }
        "new_writer" => {
            let mut client = MasterClient::connect(&socket_path).unwrap();
            client.register_process("new_os_writer").unwrap();
            client
                .register_stream("os_ticks", test_schema(), 64, 4096)
                .unwrap();
            client
                .register_source(
                    "new_os_source",
                    "openctp",
                    "os_ticks",
                    serde_json::json!({}),
                )
                .unwrap();
            client
                .publish_segment_descriptor(
                    "os_ticks",
                    serde_json::json!({
                        "magic": "zippy.segment.active",
                        "version": 1,
                        "schema_id": 7,
                        "row_capacity": 64,
                        "shm_os_id": "/tmp/zippy-os-writer-new",
                        "payload_offset": 64,
                        "committed_row_count_offset": 40,
                        "segment_id": 2,
                        "generation": 1,
                    }),
                )
                .unwrap();
            fs::write(&ready_path, b"ready").unwrap();
        }
        other => panic!("unknown control plane child role role=[{}]", other),
    }
}

#[test]
fn control_plane_child_process_entrypoint() {
    run_control_plane_child_process();
}

#[test]
fn master_client_fetches_master_runtime_config() {
    let socket_path = unique_socket_path();
    let mut expected_config = ZippyConfig::default();
    expected_config.log.level = "warn".to_string();
    expected_config.table.row_capacity = 2048;
    expected_config.table.retention_segments = Some(6);
    expected_config.table.persist.enabled = true;
    expected_config.table.persist.data_dir = "archive-data".to_string();
    expected_config.table.persist.partition.dt_column = Some("dt".to_string());
    expected_config.table.persist.partition.id_column = Some("instrument_id".to_string());
    expected_config.table.persist.partition.dt_part = Some("%Y%m".to_string());
    let (server, handle) = spawn_test_server_with_config(&socket_path, expected_config.clone());

    let client = MasterClient::connect(&socket_path).unwrap();
    let config = client.get_config().unwrap();

    assert_eq!(config, expected_config);

    server.shutdown();
    handle.join().unwrap();
}

#[test]
fn master_client_roundtrip_returns_registered_stream_schema_metadata() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let schema = test_schema();
    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("catalog_client").unwrap();
    client
        .register_stream("ticks", std::sync::Arc::clone(&schema), 64, 4096)
        .unwrap();

    let stream = client.get_stream("ticks").unwrap();
    assert_eq!(stream.stream_name, "ticks");
    assert_eq!(stream.schema_hash, canonical_schema_hash(&schema));
    assert_eq!(stream.schema["fields"][0]["name"], "instrument_id");
    assert_eq!(stream.schema["fields"][0]["segment_type"], "utf8");
    assert_eq!(stream.schema["fields"][0]["nullable"], false);
    assert_eq!(stream.schema["fields"][1]["name"], "mid_price");
    assert_eq!(stream.schema["fields"][1]["segment_type"], "float64");
    assert_eq!(stream.data_path, "segment");
    assert_eq!(stream.descriptor_generation, 0);

    let listed = client.list_streams().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].schema_hash, stream.schema_hash);
    assert_eq!(listed[0].schema, stream.schema);

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_client_rejects_same_stream_with_different_schema() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut first = MasterClient::connect(&socket_path).unwrap();
    let mut second = MasterClient::connect(&socket_path).unwrap();
    first.register_process("first").unwrap();
    second.register_process("second").unwrap();
    first
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();

    let error = second
        .register_stream("ticks", incompatible_schema(), 64, 4096)
        .unwrap_err();

    assert!(format!("{error}").contains("schema mismatch"));
    assert!(format!("{error}").contains("ticks"));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn unregistering_active_source_detaches_writer_and_clears_descriptor() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut old = MasterClient::connect(&socket_path).unwrap();
    old.register_process("old_source").unwrap();
    old.register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    old.register_source("openctp_old", "openctp", "ticks", serde_json::json!({}))
        .unwrap();
    old.publish_segment_descriptor(
        "ticks",
        serde_json::json!({
            "magic": "zippy.segment.active",
            "version": 1,
            "schema_id": 7,
            "row_capacity": 64,
            "shm_os_id": "/tmp/zippy-old-source",
            "payload_offset": 64,
            "committed_row_count_offset": 40,
            "segment_id": 1,
            "generation": 0,
        }),
    )
    .unwrap();
    old.unregister_source("openctp_old").unwrap();

    let stream = old.get_stream("ticks").unwrap();
    assert_eq!(stream.writer_process_id, None);
    assert_eq!(stream.active_segment_descriptor, None);
    assert_eq!(stream.status, "registered");

    let mut new = MasterClient::connect(&socket_path).unwrap();
    new.register_process("new_source").unwrap();
    new.register_source("openctp_new", "openctp", "ticks", serde_json::json!({}))
        .unwrap();
    new.publish_segment_descriptor(
        "ticks",
        serde_json::json!({
            "magic": "zippy.segment.active",
            "version": 1,
            "schema_id": 7,
            "row_capacity": 64,
            "shm_os_id": "/tmp/zippy-new-source",
            "payload_offset": 64,
            "committed_row_count_offset": 40,
            "segment_id": 2,
            "generation": 0,
        }),
    )
    .unwrap();
    let stream = new.get_stream("ticks").unwrap();
    assert_eq!(stream.writer_process_id.as_deref(), new.process_id());

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn get_stream_reports_unreadable_active_segment_preflight() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("preflight_writer").unwrap();
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    client
        .register_source("openctp_md", "openctp", "ticks", serde_json::json!({}))
        .unwrap();
    client
        .publish_segment_descriptor(
            "ticks",
            serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 2,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "file:/tmp/zippy-missing-active-segment-for-preflight",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 1,
                "generation": 0,
            }),
        )
        .unwrap();

    let stream = client.get_stream("ticks").unwrap();
    let stream_json = serde_json::to_value(stream).unwrap();
    let preflight = stream_json
        .get("active_segment_preflight")
        .expect("stream metadata should include active segment preflight");

    assert_eq!(
        stream_json
            .get("segment_row_capacity")
            .and_then(serde_json::Value::as_u64),
        Some(64)
    );
    assert_eq!(
        preflight.get("status").and_then(serde_json::Value::as_str),
        Some("error")
    );
    assert_eq!(
        preflight
            .get("row_capacity")
            .and_then(serde_json::Value::as_u64),
        Some(64)
    );
    assert_eq!(
        preflight.get("kind").and_then(serde_json::Value::as_str),
        Some("active_segment_unreadable")
    );
    assert!(preflight
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .contains("failed to open active segment"));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn get_stream_reports_invalid_active_descriptor_preflight() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("preflight_writer").unwrap();
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    client
        .register_source("openctp_md", "openctp", "ticks", serde_json::json!({}))
        .unwrap();
    client
        .publish_segment_descriptor(
            "ticks",
            serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 2,
                "schema_id": 7,
                "shm_os_id": "file:/tmp/zippy-missing-active-segment-for-preflight",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 1,
                "generation": 0,
            }),
        )
        .unwrap();

    let stream = client.get_stream("ticks").unwrap();
    let stream_json = serde_json::to_value(stream).unwrap();
    let preflight = stream_json
        .get("active_segment_preflight")
        .expect("stream metadata should include active segment preflight");

    assert!(
        stream_json.get("segment_row_capacity").is_none(),
        "invalid descriptor must not expose segment_row_capacity"
    );
    assert_eq!(
        preflight.get("status").and_then(serde_json::Value::as_str),
        Some("error")
    );
    assert_eq!(
        preflight.get("kind").and_then(serde_json::Value::as_str),
        Some("active_descriptor_invalid")
    );
    assert!(preflight
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .contains("missing row_capacity"));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn expired_source_writer_cannot_publish_after_new_source_takeover() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut old_writer = MasterClient::connect(&socket_path).unwrap();
    old_writer.register_process("openctp_old").unwrap();
    old_writer
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    old_writer
        .register_source("openctp_old", "openctp", "ticks", serde_json::json!({}))
        .unwrap();
    old_writer
        .publish_segment_descriptor(
            "ticks",
            serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "/tmp/zippy-segment-old",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 1,
                "generation": 0,
            }),
        )
        .unwrap();

    old_writer
        .update_status("source", "openctp_old", "lost", None)
        .unwrap();

    let mut new_writer = MasterClient::connect(&socket_path).unwrap();
    new_writer.register_process("openctp_new").unwrap();
    new_writer
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    new_writer
        .register_source("openctp_new", "openctp", "ticks", serde_json::json!({}))
        .unwrap();
    new_writer
        .publish_segment_descriptor(
            "ticks",
            serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "/tmp/zippy-segment-new",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 2,
                "generation": 1,
            }),
        )
        .unwrap();
    let stream = new_writer.get_stream("ticks").unwrap();
    assert_eq!(stream.writer_epoch, 2);

    let stale_error = old_writer
        .publish_segment_descriptor(
            "ticks",
            serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "/tmp/zippy-segment-stale",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 3,
                "generation": 2,
                "writer_epoch": 1,
            }),
        )
        .unwrap_err();
    assert!(
        stale_error
            .to_string()
            .contains("segment descriptor publisher not authorized"),
        "unexpected stale publish error: {stale_error}"
    );

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn forwarded_segment_descriptor_can_use_control_writer_epoch_for_authorization() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut writer = MasterClient::connect(&socket_path).unwrap();
    writer.register_process("writer").unwrap();
    writer
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    writer
        .register_source("openctp", "openctp", "ticks", serde_json::json!({}))
        .unwrap();
    let stream = writer.get_stream("ticks").unwrap();

    writer
        .publish_segment_descriptor(
            "ticks",
            serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "/tmp/zippy-segment-forwarded",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 1,
                "generation": 0,
                "writer_epoch": 7,
                "control_writer_epoch": stream.writer_epoch,
            }),
        )
        .unwrap();
    let stream = writer.get_stream("ticks").unwrap();
    let descriptor = stream.active_segment_descriptor.unwrap();
    assert_eq!(descriptor["writer_epoch"], 7);
    assert_eq!(descriptor["control_writer_epoch"], stream.writer_epoch);

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn os_process_source_writer_takeover_rejects_stale_publish() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let old_ready = temp.path().join("old.ready");
    let new_ready = temp.path().join("new.ready");
    let trigger = temp.path().join("publish_stale.trigger");
    let result = temp.path().join("old.result");
    let (server, join_handle) = spawn_test_server(&socket_path);

    let child_exe = std::env::current_exe().unwrap();
    let mut old_child = Command::new(&child_exe)
        .arg("--exact")
        .arg("control_plane_child_process_entrypoint")
        .arg("--nocapture")
        .env("ZIPPY_CONTROL_PLANE_CHILD_ROLE", "old_writer")
        .env("ZIPPY_CONTROL_PLANE_SOCKET", &socket_path)
        .env("ZIPPY_CONTROL_PLANE_READY", &old_ready)
        .env("ZIPPY_CONTROL_PLANE_TRIGGER", &trigger)
        .env("ZIPPY_CONTROL_PLANE_RESULT", &result)
        .spawn()
        .unwrap();
    wait_for_file(&old_ready, Duration::from_secs(2));

    let mut admin = MasterClient::connect(&socket_path).unwrap();
    admin.set_token(server.token());
    admin
        .update_status("source", "old_os_source", "lost", None)
        .unwrap();

    let new_status = Command::new(&child_exe)
        .arg("--exact")
        .arg("control_plane_child_process_entrypoint")
        .arg("--nocapture")
        .env("ZIPPY_CONTROL_PLANE_CHILD_ROLE", "new_writer")
        .env("ZIPPY_CONTROL_PLANE_SOCKET", &socket_path)
        .env("ZIPPY_CONTROL_PLANE_READY", &new_ready)
        .env("ZIPPY_CONTROL_PLANE_TRIGGER", &trigger)
        .env("ZIPPY_CONTROL_PLANE_RESULT", &result)
        .status()
        .unwrap();
    assert!(new_status.success());
    wait_for_file(&new_ready, Duration::from_secs(2));

    fs::write(&trigger, b"go").unwrap();
    let old_status = old_child.wait().unwrap();
    assert!(old_status.success());
    let stale_result = fs::read_to_string(&result).unwrap();
    assert!(
        stale_result.contains("segment descriptor publisher not authorized"),
        "unexpected stale publish result: {stale_result}"
    );

    server.shutdown();
    join_handle.join().unwrap();
}

#[test]
fn lease_reaper_reclaims_expired_segment_reader_leases() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server_with_lease(
        &socket_path,
        Duration::from_millis(50),
        Duration::from_millis(10),
    );

    let mut writer = MasterClient::connect(&socket_path).unwrap();
    writer.register_process("writer").unwrap();
    writer
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    seed_sealed_segment_identity(&server, "ticks", 1, 0);

    let mut reader = MasterClient::connect(&socket_path).unwrap();
    reader.register_process("query_reader").unwrap();
    let lease_id = reader.acquire_segment_reader_lease("ticks", 1, 0).unwrap();
    assert_eq!(lease_id, "segment-lease-1");
    assert_eq!(
        writer
            .get_stream("ticks")
            .unwrap()
            .segment_reader_leases
            .len(),
        1
    );

    thread::sleep(Duration::from_millis(120));

    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    loop {
        let stream = writer.get_stream("ticks").unwrap();
        if stream.segment_reader_leases.is_empty() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "expired segment reader lease was not reclaimed leases=[{:?}]",
                stream.segment_reader_leases
            );
        }
        thread::sleep(Duration::from_millis(20));
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn segment_reader_leases_are_not_persisted_to_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let snapshot_path = temp.path().join("master-registry.json");
    let (server, join_handle) = spawn_test_server_with_snapshot_and_lease(
        &socket_path,
        snapshot_path.clone(),
        Duration::from_millis(50),
        Duration::from_millis(10),
    );

    let mut writer = MasterClient::connect(&socket_path).unwrap();
    writer.register_process("writer").unwrap();
    writer
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    seed_sealed_segment_identity(&server, "ticks", 1, 0);

    let mut reader = MasterClient::connect(&socket_path).unwrap();
    reader.register_process("query_reader").unwrap();
    reader.acquire_segment_reader_lease("ticks", 1, 0).unwrap();
    assert_eq!(
        writer
            .get_stream("ticks")
            .unwrap()
            .segment_reader_leases
            .len(),
        1
    );
    assert!(SnapshotStore::load(&snapshot_path).unwrap().streams[0]
        .segment_reader_leases
        .is_empty());

    thread::sleep(Duration::from_millis(120));

    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    loop {
        let stream = writer.get_stream("ticks").unwrap();
        if stream.segment_reader_leases.is_empty() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "expired segment reader lease was not reclaimed leases=[{:?}]",
                stream.segment_reader_leases
            );
        }
        thread::sleep(Duration::from_millis(20));
    }

    let snapshot = SnapshotStore::load(&snapshot_path).unwrap();
    assert!(snapshot.streams[0].segment_reader_leases.is_empty());

    server.shutdown();
    join_handle.join().unwrap();
}

#[test]
fn master_client_waits_for_segment_descriptor_change() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut writer = MasterClient::connect(&socket_path).unwrap();
    writer.register_process("writer").unwrap();
    writer
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    writer
        .register_source("openctp_md", "openctp", "ticks", serde_json::json!({}))
        .unwrap();
    writer
        .publish_segment_descriptor(
            "ticks",
            serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "/tmp/zippy-segment-1",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 1,
                "generation": 0,
            }),
        )
        .unwrap();

    let wait_socket_path = socket_path.clone();
    let wait_handle = thread::spawn(move || {
        let mut watcher = MasterClient::connect(wait_socket_path).unwrap();
        watcher.register_process("watcher").unwrap();
        watcher
            .wait_segment_descriptor("ticks", 1, Duration::from_secs(2))
            .unwrap()
    });

    thread::sleep(Duration::from_millis(20));
    writer
        .publish_segment_descriptor(
            "ticks",
            serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "/tmp/zippy-segment-2",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 2,
                "generation": 1,
            }),
        )
        .unwrap();

    let update = wait_handle
        .join()
        .unwrap()
        .expect("descriptor update should be returned after publish");
    assert_eq!(update.descriptor_generation, 2);
    assert_eq!(update.descriptor["segment_id"], 2);

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_client_exposes_sealed_segments_as_stream_metadata() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("writer").unwrap();
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    client
        .register_source("openctp_md", "openctp", "ticks", serde_json::json!({}))
        .unwrap();

    client
        .publish_segment_descriptor(
            "ticks",
            serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "/tmp/zippy-segment-2",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 2,
                "generation": 1,
                "sealed_segments": [
                    {
                        "magic": "zippy.segment.active",
                        "version": 1,
                        "schema_id": 7,
                        "row_capacity": 64,
                        "shm_os_id": "/tmp/zippy-segment-1",
                        "payload_offset": 64,
                        "committed_row_count_offset": 40,
                        "segment_id": 1,
                        "generation": 0
                    }
                ]
            }),
        )
        .unwrap();

    let stream = client.get_stream("ticks").unwrap();
    assert_eq!(stream.active_segment_descriptor.unwrap()["segment_id"], 2);
    assert_eq!(stream.sealed_segments.len(), 1);
    assert_eq!(stream.sealed_segments[0]["segment_id"], 1);
    assert_eq!(stream.writer_epoch, 1);
    assert!(stream.persisted_files.is_empty());

    let descriptor = client.get_segment_descriptor("ticks").unwrap().unwrap();
    assert_eq!(descriptor["segment_id"], 2);
    assert!(descriptor.get("sealed_segments").is_none());

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_client_publishes_persisted_file_metadata() {
    let socket_path = unique_socket_path();
    let temp = tempfile::tempdir().unwrap();
    let persist_root = temp.path().join("persisted");
    fs::create_dir_all(&persist_root).unwrap();
    let parquet_file = persist_root
        .join("trading_day=20260426")
        .join("part-000001.parquet");
    fs::create_dir_all(parquet_file.parent().unwrap()).unwrap();
    fs::write(&parquet_file, b"not really parquet").unwrap();
    let (server, join_handle) =
        spawn_test_server_with_config(&socket_path, persist_root_config(&persist_root));

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("persist_writer").unwrap();
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    client
        .register_source("openctp_md", "openctp", "ticks", serde_json::json!({}))
        .unwrap();

    client
        .publish_persisted_file(
            "ticks",
            serde_json::json!({
                "file_path": parquet_file.to_string_lossy(),
                "row_count": 1024,
                "min_seq": 1,
                "max_seq": 1024,
                "min_event_ts": 1777017600000000000i64,
                "max_event_ts": 1777017660000000000i64,
                "source_segment_id": 1
            }),
        )
        .unwrap();

    let stream = client.get_stream("ticks").unwrap();
    assert_eq!(stream.persisted_files.len(), 1);
    assert_eq!(stream.persisted_files[0]["stream_name"], "ticks");
    assert_eq!(
        stream.persisted_files[0]["schema_hash"],
        canonical_schema_hash(&test_schema())
    );
    assert_eq!(stream.persisted_files[0]["row_count"], 1024);
    assert_eq!(stream.persisted_files[0]["source_segment_id"], 1);

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_client_status_fetches_omit_historical_metadata() {
    let socket_path = unique_socket_path();
    let temp = tempfile::tempdir().unwrap();
    let persist_root = temp.path().join("persisted");
    fs::create_dir_all(&persist_root).unwrap();
    let parquet_file = persist_root.join("part-000001.parquet");
    fs::write(&parquet_file, b"not really parquet").unwrap();
    let (server, join_handle) =
        spawn_test_server_with_config(&socket_path, persist_root_config(&persist_root));

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("persist_writer").unwrap();
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    client
        .register_source("openctp_md", "openctp", "ticks", serde_json::json!({}))
        .unwrap();
    client
        .publish_persisted_file(
            "ticks",
            serde_json::json!({
                "file_path": parquet_file.to_string_lossy(),
                "row_count": 1024,
                "min_seq": 1,
                "max_seq": 1024,
                "source_segment_id": 1
            }),
        )
        .unwrap();

    let full_stream = client.get_stream("ticks").unwrap();
    assert_eq!(full_stream.persisted_files.len(), 1);

    let status_stream = client.get_stream_status("ticks").unwrap();
    assert_eq!(status_stream.stream_name, "ticks");
    assert_eq!(status_stream.schema_hash, full_stream.schema_hash);
    assert!(status_stream.persisted_files.is_empty());
    assert!(status_stream.persist_events.is_empty());
    assert!(status_stream.sealed_segments.is_empty());

    let listed = client.list_streams_status().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].stream_name, "ticks");
    assert!(listed[0].persisted_files.is_empty());
    assert!(listed[0].persist_events.is_empty());
    assert!(listed[0].sealed_segments.is_empty());

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_client_upserts_persisted_file_metadata_by_persist_file_id() {
    let socket_path = unique_socket_path();
    let temp = tempfile::tempdir().unwrap();
    let persist_root = temp.path().join("persisted");
    fs::create_dir_all(&persist_root).unwrap();
    let parquet_file = persist_root.join("part-000000.parquet");
    fs::write(&parquet_file, b"not really parquet").unwrap();
    let (server, join_handle) =
        spawn_test_server_with_config(&socket_path, persist_root_config(&persist_root));

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("persist_writer").unwrap();
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    client
        .register_source("openctp_md", "openctp", "ticks", serde_json::json!({}))
        .unwrap();

    let persist_file_id = "ticks:1:0:0";
    client
        .publish_persisted_file(
            "ticks",
            serde_json::json!({
                "persist_file_id": persist_file_id,
                "file_path": parquet_file.to_string_lossy(),
                "row_count": 1024,
                "source_segment_id": 1,
                "source_generation": 0,
                "persist_status": "committed"
            }),
        )
        .unwrap();
    client
        .publish_persisted_file(
            "ticks",
            serde_json::json!({
                "persist_file_id": persist_file_id,
                "file_path": parquet_file.to_string_lossy(),
                "row_count": 1024,
                "source_segment_id": 1,
                "source_generation": 0,
                "persist_status": "committed",
                "retry_attempt": 2
            }),
        )
        .unwrap();

    let stream = client.get_stream("ticks").unwrap();
    assert_eq!(stream.persisted_files.len(), 1);
    assert_eq!(
        stream.persisted_files[0]["persist_file_id"],
        persist_file_id
    );
    assert_eq!(stream.persisted_files[0]["retry_attempt"], 2);

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_rejects_persisted_file_outside_configured_data_root() {
    let socket_path = unique_socket_path();
    let temp = tempfile::tempdir().unwrap();
    let persist_root = temp.path().join("persisted");
    let outside_root = temp.path().join("outside");
    fs::create_dir_all(&persist_root).unwrap();
    fs::create_dir_all(&outside_root).unwrap();
    let outside_file = outside_root.join("attacker.parquet");
    fs::write(&outside_file, b"not really parquet").unwrap();
    let (server, join_handle) =
        spawn_test_server_with_config(&socket_path, persist_root_config(&persist_root));

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("persist_writer").unwrap();
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    client
        .register_source("openctp_md", "openctp", "ticks", serde_json::json!({}))
        .unwrap();

    let error = client
        .publish_persisted_file(
            "ticks",
            serde_json::json!({
                "file_path": outside_file,
                "row_count": 1,
                "source_segment_id": 1
            }),
        )
        .unwrap_err();

    assert!(
        error.to_string().contains("outside persist data root"),
        "unexpected outside-root error: {error}"
    );
    assert!(client
        .get_stream("ticks")
        .unwrap()
        .persisted_files
        .is_empty());

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_rejects_spoofed_persisted_file_stream_and_schema_metadata() {
    let socket_path = unique_socket_path();
    let temp = tempfile::tempdir().unwrap();
    let persist_root = temp.path().join("persisted");
    fs::create_dir_all(&persist_root).unwrap();
    let parquet_file = persist_root.join("part-000001.parquet");
    fs::write(&parquet_file, b"not really parquet").unwrap();
    let (server, join_handle) =
        spawn_test_server_with_config(&socket_path, persist_root_config(&persist_root));

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("persist_writer").unwrap();
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    client
        .register_source("openctp_md", "openctp", "ticks", serde_json::json!({}))
        .unwrap();

    let stream_error = client
        .publish_persisted_file(
            "ticks",
            serde_json::json!({
                "file_path": parquet_file,
                "stream_name": "other_table",
                "row_count": 1,
                "source_segment_id": 1
            }),
        )
        .unwrap_err();
    assert!(
        stream_error
            .to_string()
            .contains("persisted_file.stream_name mismatch"),
        "unexpected stream spoof error: {stream_error}"
    );

    let schema_error = client
        .publish_persisted_file(
            "ticks",
            serde_json::json!({
                "file_path": parquet_file,
                "schema_hash": "wrong_hash",
                "row_count": 1,
                "source_segment_id": 1
            }),
        )
        .unwrap_err();
    assert!(
        schema_error
            .to_string()
            .contains("persisted_file.schema_hash mismatch"),
        "unexpected schema spoof error: {schema_error}"
    );
    assert!(client
        .get_stream("ticks")
        .unwrap()
        .persisted_files
        .is_empty());

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn drop_table_prevalidates_persisted_files_before_removing_registry_state() {
    let socket_path = unique_socket_path();
    let temp = tempfile::tempdir().unwrap();
    let persist_root = temp.path().join("persisted");
    let bad_path = persist_root.join("bad-directory.parquet");
    fs::create_dir_all(&bad_path).unwrap();
    let (server, join_handle) =
        spawn_test_server_with_config(&socket_path, persist_root_config(&persist_root));

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("drop_table_test").unwrap();
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    server
        .registry()
        .lock()
        .unwrap()
        .set_stream_persisted_files(
            "ticks",
            vec![serde_json::json!({
                "file_path": bad_path,
                "stream_name": "ticks",
                "schema_hash": canonical_schema_hash(&test_schema()),
                "row_count": 1
            })],
        )
        .unwrap();

    let error = client.drop_table("ticks", true).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("persisted table path is not a file"),
        "unexpected drop prevalidation error: {error}"
    );
    assert!(
        client.get_stream("ticks").is_ok(),
        "stream registry state must remain committed when persisted delete prevalidation fails"
    );
    assert!(bad_path.exists());

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_client_publishes_persist_event_metadata() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("persist_writer").unwrap();
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    client
        .register_source("openctp_md", "openctp", "ticks", serde_json::json!({}))
        .unwrap();

    client
        .publish_persist_event(
            "ticks",
            serde_json::json!({
                "persist_event_id": "ticks:1:0:failed",
                "persist_event_type": "persist_failed",
                "source_segment_id": 1,
                "source_generation": 0,
                "attempts": 2,
                "error": "publisher failed"
            }),
        )
        .unwrap();

    let stream = client.get_stream("ticks").unwrap();
    assert_eq!(stream.persist_events.len(), 1);
    assert_eq!(stream.persist_events[0]["stream_name"], "ticks");
    assert_eq!(
        stream.persist_events[0]["persist_event_type"],
        "persist_failed"
    );
    assert_eq!(stream.persist_events[0]["source_segment_id"], 1);

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_client_acquires_and_releases_segment_reader_lease() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("segment_reader").unwrap();
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    seed_sealed_segment_identity(&server, "ticks", 1, 0);

    let lease_id = client.acquire_segment_reader_lease("ticks", 1, 0).unwrap();
    let stream = client.get_stream("ticks").unwrap();
    assert_eq!(stream.segment_reader_leases.len(), 1);
    assert_eq!(stream.segment_reader_leases[0]["lease_id"], lease_id);
    assert_eq!(stream.segment_reader_leases[0]["source_segment_id"], 1);
    assert_eq!(stream.segment_reader_leases[0]["source_generation"], 0);

    client
        .release_segment_reader_lease("ticks", &lease_id)
        .unwrap();
    let stream = client.get_stream("ticks").unwrap();
    assert!(stream.segment_reader_leases.is_empty());

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_client_drop_table_removes_catalog_dependencies_and_persisted_files() {
    let socket_path = unique_socket_path();
    let temp = tempfile::tempdir().unwrap();
    let snapshot_path = temp.path().join("master-registry.json");
    let persist_dir = temp.path().join("tables").join("ticks");
    fs::create_dir_all(&persist_dir).unwrap();
    let parquet_file = persist_dir.join("ticks-segment-00000000000000000001.parquet");
    fs::write(&parquet_file, b"persisted rows").unwrap();
    let (server, join_handle) = spawn_test_server_with_snapshot_and_config(
        &socket_path,
        snapshot_path.clone(),
        persist_root_config(temp.path().join("tables").as_path()),
    );

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("drop_table_test").unwrap();
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    client
        .register_source("openctp_md", "openctp", "ticks", serde_json::json!({}))
        .unwrap();
    client
        .register_engine(
            "tick_factor",
            "reactive",
            "ticks",
            "tick_factors",
            serde_json::json!({}),
        )
        .unwrap();
    client
        .publish_persisted_file(
            "ticks",
            serde_json::json!({
                "file_path": parquet_file.to_string_lossy(),
                "row_count": 1,
                "source_segment_id": 1
            }),
        )
        .unwrap();

    let result = client.drop_table("ticks", true).unwrap();

    assert_eq!(result.table_name, "ticks");
    assert!(result.dropped);
    assert_eq!(result.persisted_files_deleted, 1);
    assert!(!parquet_file.exists());
    assert!(client
        .get_stream("ticks")
        .unwrap_err()
        .to_string()
        .contains("stream not found"));

    let snapshot = SnapshotStore::load(&snapshot_path).unwrap();
    assert!(snapshot.streams.is_empty());
    assert!(snapshot.sources.is_empty());
    assert!(snapshot.engines.is_empty());

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}
