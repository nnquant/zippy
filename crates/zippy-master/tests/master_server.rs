use zippy_core::bus_protocol::{
    AttachStreamRequest, ControlRequest, ControlResponse, RegisterEngineRequest,
    RegisterProcessRequest, RegisterSinkRequest, RegisterSourceRequest, RegisterStreamRequest,
    UpdateRecordStatusRequest, BUS_LAYOUT_VERSION,
};
use zippy_core::{setup_log, LogConfig};
use zippy_master::bus::Bus;
use zippy_master::registry::Registry;
use zippy_master::server::MasterServer;
use zippy_master::snapshot::{RegistrySnapshot, SnapshotStore, SnapshotStreamRecord};
use zippy_master::snapshot::{SnapshotEngineRecord, SnapshotSinkRecord, SnapshotSourceRecord};

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

fn test_stream_schema() -> Value {
    serde_json::json!({
        "fields": [
            {
                "name": "instrument_id",
                "data_type": "Utf8",
                "nullable": false,
                "metadata": {},
            }
        ],
        "metadata": {},
    })
}

fn test_stream_schema_hash() -> &'static str {
    "test-schema-v1"
}

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
    registry
        .register_stream(
            "openctp_ticks",
            test_stream_schema(),
            test_stream_schema_hash(),
            1024,
            256,
        )
        .unwrap();

    assert_eq!(registry.processes_len(), 1);
    assert_eq!(registry.streams_len(), 1);
    assert!(registry.get_process(&process_id).is_some());
    let stream = registry.get_stream("openctp_ticks").unwrap();
    assert_eq!(stream.buffer_size, 1024);
    assert_eq!(stream.frame_size, 256);
}

#[test]
fn registry_source_owner_publishes_segment_descriptor() {
    let mut registry = Registry::default();
    let process_id = registry.register_process("openctp");
    registry
        .register_stream(
            "openctp_ticks",
            test_stream_schema(),
            test_stream_schema_hash(),
            1024,
            256,
        )
        .unwrap();
    registry
        .register_source(
            "openctp_md",
            "openctp",
            &process_id,
            "openctp_ticks",
            serde_json::json!({}),
        )
        .unwrap();
    let descriptor = serde_json::json!({
        "magic": "zippy.segment.active",
        "version": 1,
        "schema_id": 7,
        "row_capacity": 64,
        "shm_os_id": "/tmp/zippy-segment",
        "payload_offset": 64,
        "committed_row_count_offset": 40,
        "segment_id": 1,
        "generation": 0,
    });

    registry
        .publish_segment_descriptor("openctp_ticks", &process_id, descriptor.clone())
        .unwrap();

    assert_eq!(
        registry.segment_descriptor("openctp_ticks").unwrap(),
        Some(descriptor)
    );
}

#[test]
fn registry_rejects_segment_descriptor_publish_from_lost_source() {
    let mut registry = Registry::default();
    let process_id = registry.register_process("openctp");
    registry
        .register_stream(
            "openctp_ticks",
            test_stream_schema(),
            test_stream_schema_hash(),
            1024,
            256,
        )
        .unwrap();
    registry
        .register_source(
            "openctp_md",
            "openctp",
            &process_id,
            "openctp_ticks",
            serde_json::json!({}),
        )
        .unwrap();
    registry.force_expire_process(&process_id).unwrap();
    registry.mark_records_lost_for_process(&process_id);

    let error = registry
        .publish_segment_descriptor(
            "openctp_ticks",
            &process_id,
            serde_json::json!({"magic": "zippy.segment.active"}),
        )
        .unwrap_err();

    assert!(format!("{error}").contains("process lease expired"));
}

#[test]
fn registry_segment_descriptor_requires_alive_process() {
    let mut registry = Registry::default();
    let source_process_id = registry.register_process("openctp");
    let reader_process_id = registry.register_process("segment_reader");
    registry
        .register_stream(
            "openctp_ticks",
            test_stream_schema(),
            test_stream_schema_hash(),
            1024,
            256,
        )
        .unwrap();
    registry
        .register_source(
            "openctp_md",
            "openctp",
            &source_process_id,
            "openctp_ticks",
            serde_json::json!({}),
        )
        .unwrap();
    registry
        .publish_segment_descriptor(
            "openctp_ticks",
            &source_process_id,
            serde_json::json!({"magic": "zippy.segment.active"}),
        )
        .unwrap();
    registry.force_expire_process(&reader_process_id).unwrap();

    let error = registry
        .segment_descriptor_for_process("openctp_ticks", &reader_process_id)
        .unwrap_err();

    assert!(format!("{error}").contains("process lease expired"));
}

#[test]
fn master_restores_registered_streams_from_snapshot_as_restored() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot_path = temp.path().join("master-registry.json");

    SnapshotStore::write(
        &snapshot_path,
        &RegistrySnapshot {
            streams: vec![SnapshotStreamRecord {
                stream_name: "openctp_ticks".to_string(),
                schema: test_stream_schema(),
                schema_hash: test_stream_schema_hash().to_string(),
                data_path: "segment".to_string(),
                descriptor_generation: 0,
                sealed_segments: Vec::new(),
                persisted_files: Vec::new(),
                persist_events: Vec::new(),
                segment_reader_leases: Vec::new(),
                buffer_size: 1024,
                frame_size: 256,
                status: "registered".to_string(),
            }],
            sources: vec![],
            engines: vec![],
            sinks: vec![],
        },
    )
    .unwrap();

    let server = MasterServer::from_snapshot_path(&snapshot_path).unwrap();
    let stream = server
        .registry()
        .lock()
        .unwrap()
        .get_stream("openctp_ticks")
        .unwrap()
        .clone();

    assert_eq!(stream.status, "restored");
    assert_eq!(stream.buffer_size, 1024);
    assert_eq!(stream.frame_size, 256);
}

#[test]
fn master_restores_legacy_snapshot_without_frame_size() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot_path = temp.path().join("master-registry.json");

    fs::write(
        &snapshot_path,
        r#"{
  "streams": [
    {
      "stream_name": "openctp_ticks",
      "ring_capacity": 1024,
      "status": "registered"
    }
  ],
  "sources": [],
  "engines": [],
  "sinks": []
}"#,
    )
    .unwrap();

    let server = MasterServer::from_snapshot_path(&snapshot_path).unwrap();
    let stream = server
        .registry()
        .lock()
        .unwrap()
        .get_stream("openctp_ticks")
        .unwrap()
        .clone();

    assert_eq!(stream.status, "restored");
    assert_eq!(stream.buffer_size, 1024);
    assert_eq!(stream.frame_size, 1024);
}

#[test]
fn master_restores_control_plane_entities_from_snapshot_as_restored() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot_path = temp.path().join("master-registry.json");

    SnapshotStore::write(
        &snapshot_path,
        &RegistrySnapshot {
            streams: vec![SnapshotStreamRecord {
                stream_name: "openctp_ticks".to_string(),
                schema: test_stream_schema(),
                schema_hash: test_stream_schema_hash().to_string(),
                data_path: "segment".to_string(),
                descriptor_generation: 0,
                sealed_segments: Vec::new(),
                persisted_files: Vec::new(),
                persist_events: Vec::new(),
                segment_reader_leases: Vec::new(),
                buffer_size: 1024,
                frame_size: 256,
                status: "registered".to_string(),
            }],
            sources: vec![SnapshotSourceRecord {
                source_name: "openctp_md".to_string(),
                source_type: "openctp".to_string(),
                process_id: "proc_1".to_string(),
                output_stream: "openctp_ticks".to_string(),
                config: serde_json::json!({"front": "tcp://example"}),
                status: "running".to_string(),
                metrics: serde_json::json!({}),
            }],
            engines: vec![SnapshotEngineRecord {
                engine_name: "mid_price_factor".to_string(),
                engine_type: "reactive".to_string(),
                process_id: "proc_2".to_string(),
                input_stream: "openctp_ticks".to_string(),
                output_stream: "openctp_mid_price_factors".to_string(),
                sink_names: vec!["factor_sink".to_string()],
                config: serde_json::json!({"id_filter": ["IF2606"]}),
                status: "running".to_string(),
                metrics: serde_json::json!({}),
            }],
            sinks: vec![SnapshotSinkRecord {
                sink_name: "factor_sink".to_string(),
                sink_type: "parquet".to_string(),
                process_id: "proc_3".to_string(),
                input_stream: "openctp_mid_price_factors".to_string(),
                config: serde_json::json!({"path": "data/factors"}),
                status: "running".to_string(),
            }],
        },
    )
    .unwrap();

    let server = MasterServer::from_snapshot_path(&snapshot_path).unwrap();
    let registry_handle = server.registry();
    let registry = registry_handle.lock().unwrap();
    assert_eq!(
        registry.get_stream("openctp_ticks").unwrap().status,
        "restored"
    );
    assert_eq!(
        registry.get_stream("openctp_ticks").unwrap().buffer_size,
        1024
    );
    assert_eq!(
        registry.get_stream("openctp_ticks").unwrap().frame_size,
        256
    );
    assert_eq!(
        registry.get_source("openctp_md").unwrap().status,
        "restored"
    );
    assert_eq!(
        registry.get_engine("mid_price_factor").unwrap().status,
        "restored"
    );
    assert_eq!(registry.get_sink("factor_sink").unwrap().status, "restored");
}

#[test]
fn master_does_not_restore_active_segment_descriptor_from_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let snapshot_path = temp.path().join("master-registry.json");

    let server = MasterServer::with_runtime_config(
        Some(snapshot_path.clone()),
        Duration::from_secs(10),
        Duration::from_secs(2),
    );
    let handle_server = server.clone();
    let wait_path = socket_path.clone();
    let join_handle = thread::spawn(move || handle_server.serve(&socket_path).unwrap());
    wait_for_socket_ready(&wait_path);

    assert!(send_register_process(&wait_path, "openctp").contains("proc_1"));
    assert!(send_request(
        &wait_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
    )
    .contains("StreamRegistered"));
    assert!(send_request(
        &wait_path,
        "{\"RegisterSource\":{\"source_name\":\"openctp_md\",\"source_type\":\"openctp\",\"process_id\":\"proc_1\",\"output_stream\":\"openctp_ticks\",\"config\":{}}}\n",
    )
    .contains("SourceRegistered"));
    assert!(send_request(
        &wait_path,
        "{\"PublishSegmentDescriptor\":{\"stream_name\":\"openctp_ticks\",\"process_id\":\"proc_1\",\"descriptor\":{\"magic\":\"zippy.segment.active\"}}}\n",
    )
    .contains("SegmentDescriptorPublished"));

    server.shutdown();
    join_handle.join().unwrap();

    let server = MasterServer::from_snapshot_path(&snapshot_path).unwrap();
    let reader_process_id = server
        .registry()
        .lock()
        .unwrap()
        .register_process("segment_reader");
    let descriptor = server
        .registry()
        .lock()
        .unwrap()
        .segment_descriptor_for_process("openctp_ticks", &reader_process_id)
        .unwrap();

    assert!(descriptor.is_none());
    let _ = fs::remove_file(wait_path);
}

#[test]
fn master_persists_registered_streams_to_snapshot_on_register_stream() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let snapshot_path = temp.path().join("master-registry.json");

    let server = MasterServer::with_runtime_config(
        Some(snapshot_path.clone()),
        Duration::from_secs(10),
        Duration::from_secs(2),
    );
    let handle_server = server.clone();
    let wait_path = socket_path.clone();
    let join_handle = thread::spawn(move || handle_server.serve(&socket_path).unwrap());
    wait_for_socket_ready(&wait_path);

    let response = send_request(
        &wait_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
    );
    assert!(response.contains("StreamRegistered"));

    server.shutdown();
    join_handle.join().unwrap();

    let snapshot = SnapshotStore::load(&snapshot_path).unwrap();
    assert_eq!(snapshot.streams.len(), 1);
    assert_eq!(snapshot.streams[0].stream_name, "openctp_ticks");
    assert_eq!(snapshot.streams[0].buffer_size, 1024);
    assert_eq!(snapshot.streams[0].frame_size, 256);

    let snapshot_json = fs::read_to_string(&snapshot_path).unwrap();
    assert!(snapshot_json.contains("\"buffer_size\": 1024"));
    assert!(!snapshot_json.contains("\"ring_capacity\""));
}

#[test]
fn snapshot_store_reads_legacy_ring_capacity_into_buffer_and_frame_size() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot_path = temp.path().join("master-registry.json");

    fs::write(
        &snapshot_path,
        r#"{
  "streams": [
    {
      "stream_name": "ticks",
      "ring_capacity": 512,
      "status": "registered"
    }
  ],
  "sources": [],
  "engines": [],
  "sinks": []
}"#,
    )
    .unwrap();

    let snapshot = SnapshotStore::load(&snapshot_path).unwrap();
    assert_eq!(snapshot.streams.len(), 1);
    assert_eq!(snapshot.streams[0].buffer_size, 512);
    assert_eq!(snapshot.streams[0].frame_size, 512);
}

#[test]
fn registry_updates_process_lease_timestamp_on_heartbeat() {
    let mut registry = Registry::default();
    let process_id = registry.register_process("local_dc");

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
    let process_id = registry.register_process("local_dc");

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
    let process_id = match process_response {
        ControlResponse::ProcessRegistered { process_id } => process_id,
        other => panic!("unexpected process response: {:?}", other),
    };

    let response = send_request(
        &socket_path,
        &format!("{{\"Heartbeat\":{{\"process_id\":\"{process_id}\"}}}}\n"),
    );
    let response: ControlResponse = serde_json::from_str(response.trim_end()).unwrap();
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
        ControlResponse::Error { reason } => {
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
fn master_server_rolls_back_stream_registration_when_snapshot_write_fails() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let blocked_parent = temp.path().join("blocked-parent");
    fs::write(&blocked_parent, b"not-a-dir").unwrap();
    let snapshot_path = blocked_parent.join("master-registry.json");

    let server = MasterServer::with_runtime_config(
        Some(snapshot_path),
        Duration::from_secs(10),
        Duration::from_secs(2),
    );
    let handle_server = server.clone();
    let wait_path = socket_path.clone();
    let join_handle = thread::spawn(move || handle_server.serve(&socket_path).unwrap_err());
    wait_for_socket_ready(&wait_path);

    let response = send_request(
        &wait_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
    );
    assert!(response.contains("Error"));
    assert!(response.contains("failed to create registry snapshot parent"));

    let stream = server
        .registry()
        .lock()
        .unwrap()
        .get_stream("openctp_ticks")
        .cloned();
    assert!(stream.is_none());

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(wait_path);
}

#[test]
fn master_server_returns_error_when_shutdown_snapshot_flush_fails() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let blocked_parent = temp.path().join("blocked-parent");
    fs::write(&blocked_parent, b"not-a-dir").unwrap();
    let snapshot_path = blocked_parent.join("master-registry.json");

    let server = MasterServer::with_runtime_config(
        Some(snapshot_path),
        Duration::from_secs(10),
        Duration::from_secs(2),
    );
    let handle_server = server.clone();
    let wait_path = socket_path.clone();
    let join_handle = thread::spawn(move || handle_server.serve(&socket_path));
    wait_for_socket_ready(&wait_path);

    server.shutdown();
    let result = join_handle.join().unwrap();
    assert!(result.is_err());
    assert!(
        format!("{}", result.unwrap_err()).contains("failed to create registry snapshot parent")
    );
}

#[test]
fn registry_marks_control_plane_entities_lost_when_process_expires() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_register_process(&socket_path, "local_dc");
    let process_response: ControlResponse =
        serde_json::from_str(process_response.trim_end()).unwrap();
    let process_id = match process_response {
        ControlResponse::ProcessRegistered { process_id } => process_id,
        other => panic!("unexpected process response: {:?}", other),
    };

    let source_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterSource(RegisterSourceRequest {
            source_name: "openctp_md".to_string(),
            source_type: "openctp".to_string(),
            process_id: process_id.clone(),
            output_stream: "openctp_ticks".to_string(),
            config: serde_json::json!({"front": "tcp://example"}),
        }),
    );
    assert!(matches!(
        source_response,
        ControlResponse::SourceRegistered { .. }
    ));

    let engine_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterEngine(RegisterEngineRequest {
            engine_name: "mid_price_factor".to_string(),
            engine_type: "reactive".to_string(),
            process_id: process_id.clone(),
            input_stream: "openctp_ticks".to_string(),
            output_stream: "openctp_mid_price_factors".to_string(),
            sink_names: vec!["factor_sink".to_string()],
            config: serde_json::json!({"id_filter": ["IF2606"]}),
        }),
    );
    assert!(matches!(
        engine_response,
        ControlResponse::EngineRegistered { .. }
    ));

    let sink_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterSink(RegisterSinkRequest {
            sink_name: "factor_sink".to_string(),
            sink_type: "parquet".to_string(),
            process_id: process_id.clone(),
            input_stream: "openctp_mid_price_factors".to_string(),
            config: serde_json::json!({"path": "data/factors"}),
        }),
    );
    assert!(matches!(
        sink_response,
        ControlResponse::SinkRegistered { .. }
    ));

    let status_response = send_control_request(
        &socket_path,
        ControlRequest::UpdateStatus(UpdateRecordStatusRequest {
            kind: "engine".to_string(),
            name: "mid_price_factor".to_string(),
            status: "running".to_string(),
            metrics: Some(serde_json::json!({"processed_rows_total": 42})),
        }),
    );
    assert!(matches!(
        status_response,
        ControlResponse::StatusUpdated { .. }
    ));

    let expire_response = send_request(
        &socket_path,
        &format!("{{\"ExpireProcessForTest\":{{\"process_id\":\"{process_id}\"}}}}\n"),
    );
    assert!(expire_response.contains(&process_id));

    let registry_handle = server.registry();
    let registry = registry_handle.lock().unwrap();
    assert_eq!(registry.get_source("openctp_md").unwrap().status, "lost");
    assert_eq!(
        registry.get_engine("mid_price_factor").unwrap().status,
        "lost"
    );
    assert_eq!(registry.get_sink("factor_sink").unwrap().status, "lost");
    drop(registry);

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn registry_rejects_duplicate_stream_names() {
    let mut registry = Registry::default();
    registry
        .register_stream(
            "openctp_ticks",
            test_stream_schema(),
            test_stream_schema_hash(),
            1024,
            256,
        )
        .unwrap();

    let error = registry
        .register_stream(
            "openctp_ticks",
            test_stream_schema(),
            test_stream_schema_hash(),
            1024,
            256,
        )
        .unwrap_err();
    assert!(format!("{error}").contains("stream already exists"));
}

#[test]
fn bus_enforces_single_writer_and_multiple_readers() {
    let mut bus = Bus::default();
    bus.create_stream("openctp_ticks", 1024).unwrap();

    let writer = bus.write_to("openctp_ticks", "proc_1").unwrap();
    let reader_a = bus.read_from("openctp_ticks", "proc_2", None).unwrap();
    let reader_b = bus.read_from("openctp_ticks", "proc_3", None).unwrap();

    assert_eq!(writer.stream_name, "openctp_ticks");
    assert_eq!(writer.buffer_size, 1024);
    assert_eq!(writer.frame_size, 1024);
    assert_eq!(writer.layout_version, BUS_LAYOUT_VERSION);
    assert!(writer.shm_name.contains("openctp_ticks"));
    assert_eq!(writer.next_write_seq, 1);
    assert_eq!(reader_a.stream_name, "openctp_ticks");
    assert_eq!(reader_a.buffer_size, 1024);
    assert_eq!(reader_a.frame_size, 1024);
    assert_eq!(reader_a.layout_version, BUS_LAYOUT_VERSION);
    assert!(reader_a.shm_name.contains("openctp_ticks"));
    assert_eq!(reader_a.next_read_seq, 1);
    assert_eq!(reader_b.stream_name, "openctp_ticks");
    assert_ne!(reader_a.reader_id, reader_b.reader_id);
    assert!(bus.write_to("openctp_ticks", "proc_9").is_err());
}

#[test]
fn master_server_roundtrips_reader_instrument_filter_over_control_plane() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "local_dc".to_string(),
        }),
    );
    let process_id = match process_response {
        ControlResponse::ProcessRegistered { process_id } => process_id,
        other => panic!("unexpected process response: {:?}", other),
    };

    let stream_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterStream(RegisterStreamRequest {
            stream_name: "ticks".to_string(),
            schema: test_stream_schema(),
            schema_hash: test_stream_schema_hash().to_string(),
            buffer_size: 1024,
            frame_size: 256,
        }),
    );
    assert!(matches!(
        stream_response,
        ControlResponse::StreamRegistered { .. }
    ));

    let read_response = send_control_request(
        &socket_path,
        ControlRequest::ReadFrom(AttachStreamRequest {
            stream_name: "ticks".to_string(),
            process_id,
            instrument_ids: Some(vec!["IF2606".to_string(), "IH2606".to_string()]),
        }),
    );

    match read_response {
        ControlResponse::ReaderAttached { descriptor } => {
            assert_eq!(descriptor.stream_name, "ticks");
            assert_eq!(
                descriptor.instrument_filter,
                Some(vec!["IF2606".to_string(), "IH2606".to_string()])
            );
        }
        other => panic!("unexpected read response: {:?}", other),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
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
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
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
    assert_eq!(writer_descriptor.buffer_size, 1024);
    assert_eq!(writer_descriptor.frame_size, 256);
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
    assert_eq!(reader_descriptor.buffer_size, 1024);
    assert_eq!(reader_descriptor.frame_size, 256);
    assert_eq!(reader_descriptor.layout_version, BUS_LAYOUT_VERSION);
    assert!(reader_descriptor.shm_name.contains("openctp_ticks"));
    assert_eq!(reader_descriptor.next_read_seq, 1);
    assert_eq!(reader_descriptor.reader_id, "openctp_ticks_reader_1");

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_publishes_and_fetches_segment_descriptor() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    assert!(send_register_process(&socket_path, "openctp").contains("proc_1"));
    assert!(send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
    )
    .contains("StreamRegistered"));
    assert!(send_request(
        &socket_path,
        "{\"RegisterSource\":{\"source_name\":\"openctp_md\",\"source_type\":\"openctp\",\"process_id\":\"proc_1\",\"output_stream\":\"openctp_ticks\",\"config\":{}}}\n",
    )
    .contains("SourceRegistered"));
    assert!(send_register_process(&socket_path, "segment_reader").contains("proc_2"));

    let publish_response = send_request(
        &socket_path,
        "{\"PublishSegmentDescriptor\":{\"stream_name\":\"openctp_ticks\",\"process_id\":\"proc_1\",\"descriptor\":{\"magic\":\"zippy.segment.active\",\"version\":1,\"schema_id\":7,\"row_capacity\":64,\"shm_os_id\":\"/tmp/zippy-segment\",\"payload_offset\":64,\"committed_row_count_offset\":40,\"segment_id\":1,\"generation\":0}}}\n",
    );
    let publish_response: ControlResponse =
        serde_json::from_str(publish_response.trim_end()).unwrap();
    match publish_response {
        ControlResponse::SegmentDescriptorPublished { stream_name } => {
            assert_eq!(stream_name, "openctp_ticks");
        }
        other => panic!("unexpected publish response: {:?}", other),
    }

    let fetch_response = send_request(
        &socket_path,
        "{\"GetSegmentDescriptor\":{\"stream_name\":\"openctp_ticks\",\"process_id\":\"proc_2\"}}\n",
    );
    let fetch_response: ControlResponse = serde_json::from_str(fetch_response.trim_end()).unwrap();
    match fetch_response {
        ControlResponse::SegmentDescriptorFetched {
            stream_name,
            descriptor,
        } => {
            assert_eq!(stream_name, "openctp_ticks");
            assert_eq!(
                descriptor.unwrap()["magic"],
                serde_json::json!("zippy.segment.active")
            );
        }
        other => panic!("unexpected fetch response: {:?}", other),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_rejects_close_writer_from_other_process() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    assert!(send_register_process(&socket_path, "writer").contains("proc_1"));
    assert!(send_register_process(&socket_path, "intruder").contains("proc_2"));
    assert!(send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
    )
    .contains("StreamRegistered"));

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

    let close_response = send_request(
        &socket_path,
        &format!(
            "{{\"CloseWriter\":{{\"stream_name\":\"openctp_ticks\",\"process_id\":\"proc_2\",\"writer_id\":\"{}\"}}}}\n",
            writer_descriptor.writer_id
        ),
    );
    let close_response: ControlResponse = serde_json::from_str(close_response.trim_end()).unwrap();
    match close_response {
        ControlResponse::Error { reason } => {
            assert!(reason.contains("writer not owned"));
            assert!(reason.contains("proc_2"));
        }
        other => panic!("unexpected close writer response: {:?}", other),
    }

    let stream = send_request(
        &socket_path,
        "{\"GetStream\":{\"stream_name\":\"openctp_ticks\"}}\n",
    );
    assert!(stream.contains("\"writer_process_id\":\"proc_1\""));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_rejects_close_reader_from_other_process() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    assert!(send_register_process(&socket_path, "reader_a").contains("proc_1"));
    assert!(send_register_process(&socket_path, "reader_b").contains("proc_2"));
    assert!(send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
    )
    .contains("StreamRegistered"));

    let reader_response = send_request(
        &socket_path,
        "{\"ReadFrom\":{\"stream_name\":\"openctp_ticks\",\"process_id\":\"proc_1\"}}\n",
    );
    let reader_response: ControlResponse =
        serde_json::from_str(reader_response.trim_end()).unwrap();
    let reader_descriptor = match reader_response {
        ControlResponse::ReaderAttached { descriptor } => descriptor,
        other => panic!("unexpected reader response: {:?}", other),
    };

    let close_response = send_request(
        &socket_path,
        &format!(
            "{{\"CloseReader\":{{\"stream_name\":\"openctp_ticks\",\"process_id\":\"proc_2\",\"reader_id\":\"{}\"}}}}\n",
            reader_descriptor.reader_id
        ),
    );
    let close_response: ControlResponse = serde_json::from_str(close_response.trim_end()).unwrap();
    match close_response {
        ControlResponse::Error { reason } => {
            assert!(reason.contains("reader not owned"));
            assert!(reason.contains("proc_2"));
        }
        other => panic!("unexpected close reader response: {:?}", other),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_treats_duplicate_stream_registration_as_idempotent_success() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_register_process(&socket_path, "local_dc");
    assert!(process_response.contains("proc_1"));

    let first_response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
    );
    assert!(first_response.contains("StreamRegistered"));

    let duplicate_response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
    );
    assert!(duplicate_response.contains("StreamRegistered"));
    assert!(duplicate_response.contains("openctp_ticks"));

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
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":0,\"frame_size\":0}}\n",
    );
    assert!(response.contains("Error"));
    assert!(response.contains("invalid stream sizing"));
    assert!(response.contains("buffer_size=[0]"));
    assert!(response.contains("frame_size=[0]"));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_rejects_stream_registration_with_frame_size_mismatch() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_register_process(&socket_path, "local_dc");
    assert!(process_response.contains("proc_1"));

    let first_response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
    );
    assert!(first_response.contains("StreamRegistered"));

    let mismatch_response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":512}}\n",
    );
    assert!(mismatch_response.contains("stream configuration mismatch"));
    assert!(mismatch_response.contains("existing_buffer_size=[1024]"));
    assert!(mismatch_response.contains("existing_frame_size=[256]"));
    assert!(mismatch_response.contains("requested_buffer_size=[1024]"));
    assert!(mismatch_response.contains("requested_frame_size=[512]"));

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
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
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
    assert_eq!(streams[0].buffer_size, 1024);
    assert_eq!(streams[0].frame_size, 256);
    assert_eq!(streams[0].write_seq, 0);

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
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
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
    assert_eq!(stream.buffer_size, 1024);
    assert_eq!(stream.frame_size, 256);
    assert_eq!(stream.write_seq, 0);

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
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
    );
    assert!(stream_response.contains("StreamRegistered"));

    let duplicate_response = send_request(
        &socket_path,
        "{\"RegisterStream\":{\"stream_name\":\"openctp_ticks\",\"buffer_size\":1024,\"frame_size\":256}}\n",
    );
    assert!(duplicate_response.contains("StreamRegistered"));

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
        &[("buffer_size", 1024), ("frame_size", 256)],
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
        &[("buffer_size", 1024), ("frame_size", 256)],
        &[("existing", true)],
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
        &[],
    );
    assert_record_has_fields(
        &records,
        "list_streams",
        "success",
        "INFO",
        &[("component", "master_server")],
        &[("stream_count", 1)],
        &[],
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
