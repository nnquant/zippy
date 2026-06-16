#![cfg(unix)]

use zippy_core::bus_protocol::{
    AcquireSegmentReaderLeaseRequest, ControlEnvelopeRequest, ControlRequest, ControlResponse,
    ListStreamsRequest, RegisterEngineRequest, RegisterProcessRequest, RegisterSourceRequest,
    RegisterStreamRequest, ReleaseSegmentReaderLeaseRequest, UnregisterProcessRequest,
    UpdateRecordStatusRequest, WatchRequest, WatchResource, CONTROL_PROTOCOL_VERSION,
};
use zippy_core::{setup_log, LogConfig, ZippyConfig};
use zippy_master::registry::{Registry, RegistryError};
use zippy_master::server::MasterServer;
use zippy_master::snapshot::{
    RegistrySnapshot, SnapshotEngineRecord, SnapshotSourceRecord, SnapshotStore,
    SnapshotStreamRecord,
};

use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
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

fn register_stream(
    socket_path: &Path,
    process_id: &str,
    process_token: &str,
    stream_name: &str,
    buffer_size: usize,
    frame_size: usize,
) -> ControlResponse {
    send_control_request(
        socket_path,
        ControlRequest::RegisterStream(RegisterStreamRequest {
            process_id: Some(process_id.to_string()),
            process_token: Some(process_token.to_string()),
            token: None,
            stream_name: stream_name.to_string(),
            schema: test_stream_schema(),
            schema_hash: test_stream_schema_hash().to_string(),
            buffer_size,
            frame_size,
        }),
    )
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

fn write_test_parquet_batch(path: &Path, batch: RecordBatch) {
    let file = fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn read_single_parquet_batch(path: &Path) -> RecordBatch {
    let file = fs::File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.build().unwrap();
    reader.next().unwrap().unwrap()
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
        process_token: "token_1".to_string(),
    };
    assert!(format!("{response:?}").contains("proc_1"));
}

#[test]
fn master_server_accepts_control_envelope_and_echoes_request_id() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let response = send_control_request(
        &socket_path,
        ControlRequest::Envelope(ControlEnvelopeRequest {
            version: CONTROL_PROTOCOL_VERSION,
            request_id: "req_control_v2_1".to_string(),
            process_id: None,
            process_token: None,
            token: None,
            verb: None,
            resource: None,
            revision: None,
            timeout_ms: None,
            payload: None,
            inner: Some(Box::new(ControlRequest::ListStreams(ListStreamsRequest {}))),
        }),
    );

    match response {
        ControlResponse::Envelope(envelope) => {
            assert_eq!(envelope.version, CONTROL_PROTOCOL_VERSION);
            assert_eq!(envelope.request_id, "req_control_v2_1");
            assert!(matches!(*envelope.inner, ControlResponse::StreamsListed(_)));
        }
        other => panic!("unexpected envelope response {other:?}"),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_accepts_control_envelope_watch_without_inner_request() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "control_v2_client".to_string(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id,
        process_token,
    } = process_response
    else {
        panic!("unexpected process response {process_response:?}");
    };

    let response = send_control_request(
        &socket_path,
        ControlRequest::Envelope(ControlEnvelopeRequest {
            version: CONTROL_PROTOCOL_VERSION,
            request_id: "req_control_v2_watch_1".to_string(),
            process_id: Some(process_id),
            process_token: Some(process_token),
            token: None,
            verb: Some("watch".to_string()),
            resource: Some(WatchResource::GatewayConfig),
            revision: Some(0),
            timeout_ms: Some(1_000),
            payload: None,
            inner: None,
        }),
    );

    match response {
        ControlResponse::Envelope(envelope) => {
            assert_eq!(envelope.version, CONTROL_PROTOCOL_VERSION);
            assert_eq!(envelope.request_id, "req_control_v2_watch_1");
            match *envelope.inner {
                ControlResponse::ResourceChanged { event: Some(event) } => {
                    assert_eq!(event.resource, WatchResource::GatewayConfig);
                    assert_eq!(event.revision, 1);
                }
                other => panic!("unexpected envelope inner response {other:?}"),
            }
        }
        other => panic!("unexpected envelope response {other:?}"),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_control_envelope_register_process_is_idempotent_by_request_id() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let request = ControlRequest::Envelope(ControlEnvelopeRequest {
        version: CONTROL_PROTOCOL_VERSION,
        request_id: "req_register_process_once".to_string(),
        process_id: None,
        process_token: None,
        token: None,
        verb: Some("register_process".to_string()),
        resource: None,
        revision: None,
        timeout_ms: None,
        payload: Some(serde_json::json!({"app": "idempotent_client"})),
        inner: None,
    });

    let first_response = send_control_request(&socket_path, request.clone());
    let second_response = send_control_request(&socket_path, request);

    let first_process_id = match first_response {
        ControlResponse::Envelope(envelope) => match *envelope.inner {
            ControlResponse::ProcessRegistered { process_id, .. } => process_id,
            other => panic!("unexpected first inner response {other:?}"),
        },
        other => panic!("unexpected first response {other:?}"),
    };
    let second_process_id = match second_response {
        ControlResponse::Envelope(envelope) => match *envelope.inner {
            ControlResponse::ProcessRegistered { process_id, .. } => process_id,
            other => panic!("unexpected second inner response {other:?}"),
        },
        other => panic!("unexpected second response {other:?}"),
    };

    assert_eq!(first_process_id, second_process_id);
    assert_eq!(server.registry().lock().unwrap().processes_len(), 1);

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_control_envelope_updates_source_status() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "openctp".to_string(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id,
        process_token,
    } = process_response
    else {
        panic!("unexpected process response {process_response:?}");
    };
    assert!(matches!(
        send_control_request(
            &socket_path,
            ControlRequest::RegisterStream(RegisterStreamRequest {
                process_id: Some(process_id.clone()),
                process_token: Some(process_token.clone()),
                token: None,
                stream_name: "ticks".to_string(),
                schema: test_stream_schema(),
                schema_hash: test_stream_schema_hash().to_string(),
                buffer_size: 1024,
                frame_size: 256,
            }),
        ),
        ControlResponse::StreamRegistered { .. }
    ));
    assert!(matches!(
        send_control_request(
            &socket_path,
            ControlRequest::RegisterSource(RegisterSourceRequest {
                source_name: "openctp_md".to_string(),
                source_type: "openctp".to_string(),
                process_id: process_id.clone(),
                process_token: Some(process_token.clone()),
                output_stream: "ticks".to_string(),
                config: serde_json::json!({}),
            }),
        ),
        ControlResponse::SourceRegistered { .. }
    ));

    let response = send_control_request(
        &socket_path,
        ControlRequest::Envelope(ControlEnvelopeRequest {
            version: CONTROL_PROTOCOL_VERSION,
            request_id: "req_update_source_status".to_string(),
            process_id: Some(process_id),
            process_token: Some(process_token),
            token: None,
            verb: Some("update_status".to_string()),
            resource: None,
            revision: None,
            timeout_ms: None,
            payload: Some(serde_json::json!({
                "kind": "source",
                "name": "openctp_md",
                "status": "running",
                "metrics": {"rows_total": 10}
            })),
            inner: None,
        }),
    );

    match response {
        ControlResponse::Envelope(envelope) => {
            assert!(matches!(
                *envelope.inner,
                ControlResponse::StatusUpdated { .. }
            ));
        }
        other => panic!("unexpected update envelope response {other:?}"),
    }
    {
        let registry = server.registry();
        let registry = registry.lock().unwrap();
        let source = registry.get_source("openctp_md").unwrap();
        assert_eq!(source.status, "running");
        assert_eq!(source.metrics["rows_total"], serde_json::json!(10));
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
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
fn master_server_compaction_worker_sorts_and_replaces_persisted_files() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = unique_socket_path();
    let mut config = ZippyConfig::default();
    config.table.persist.compaction.enabled = true;
    config.table.persist.compaction.interval_sec = 0.01;
    config.table.persist.compaction.min_files = 2;
    config.table.persist.compaction.delete_sources = false;
    config.table.persist.compaction.sort_column = Some("dt".to_string());
    config.table.persist.data_dir = temp.path().join("persisted").to_string_lossy().to_string();
    let (server, join_handle) = spawn_test_server_with_config(&socket_path, config);

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("dt", DataType::Int64, false),
        Field::new("last_price", DataType::Float64, false),
    ]));
    assert!(matches!(
        send_control_request(
            &socket_path,
            ControlRequest::RegisterStream(RegisterStreamRequest {
                process_id: None,
                process_token: None,
                token: Some(server.token().to_string()),
                stream_name: "ctp_ticks".to_string(),
                schema: serde_json::json!({
                    "fields": [
                        {
                            "name": "instrument_id",
                            "data_type": "Utf8",
                            "nullable": false,
                            "metadata": {},
                        },
                        {
                            "name": "dt",
                            "data_type": "Int64",
                            "nullable": false,
                            "metadata": {},
                        },
                        {
                            "name": "last_price",
                            "data_type": "Float64",
                            "nullable": false,
                            "metadata": {},
                        },
                    ],
                    "metadata": {},
                }),
                schema_hash: "schema-a".to_string(),
                buffer_size: 1024,
                frame_size: 256,
            }),
        ),
        ControlResponse::StreamRegistered { .. }
    ));

    let partition_dir = temp
        .path()
        .join("persisted")
        .join("dt_part=202604")
        .join("instrument_id=IF2606");
    fs::create_dir_all(&partition_dir).unwrap();
    let first_file = partition_dir.join("part-000001.parquet");
    let second_file = partition_dir.join("part-000002.parquet");
    write_test_parquet_batch(
        &first_file,
        RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(StringArray::from(vec!["IF2606", "IF2606"])) as ArrayRef,
                std::sync::Arc::new(Int64Array::from(vec![3, 1])) as ArrayRef,
                std::sync::Arc::new(Float64Array::from(vec![4103.5, 4101.5])) as ArrayRef,
            ],
        )
        .unwrap(),
    );
    write_test_parquet_batch(
        &second_file,
        RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(StringArray::from(vec!["IF2606"])) as ArrayRef,
                std::sync::Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                std::sync::Arc::new(Float64Array::from(vec![4102.5])) as ArrayRef,
            ],
        )
        .unwrap(),
    );

    assert!(matches!(
        send_control_request(
            &socket_path,
            ControlRequest::ReplacePersistedFiles(zippy_core::ReplacePersistedFilesRequest {
                stream_name: "ctp_ticks".to_string(),
                process_id: None,
                process_token: None,
                token: Some(server.token().to_string()),
                persisted_files: vec![
                    serde_json::json!({
                        "persist_file_id": "first",
                        "file_path": first_file.to_string_lossy(),
                        "row_count": 2,
                        "source_segment_id": 1,
                        "source_generation": 0,
                        "partition_path": "dt_part=202604/instrument_id=IF2606",
                        "partition": {"dt_part": "202604", "instrument_id": "IF2606"},
                        "partition_spec": {
                            "dt_column": "dt",
                            "id_column": "instrument_id",
                            "dt_part": "%Y%m"
                        }
                    }),
                    serde_json::json!({
                        "persist_file_id": "second",
                        "file_path": second_file.to_string_lossy(),
                        "row_count": 1,
                        "source_segment_id": 2,
                        "source_generation": 0,
                        "partition_path": "dt_part=202604/instrument_id=IF2606",
                        "partition": {"dt_part": "202604", "instrument_id": "IF2606"},
                        "partition_spec": {
                            "dt_column": "dt",
                            "id_column": "instrument_id",
                            "dt_part": "%Y%m"
                        }
                    }),
                ],
            }),
        ),
        ControlResponse::PersistedFilesReplaced { .. }
    ));

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let compacted = loop {
        let persisted_files = {
            let registry = server.registry();
            let registry = registry.lock().unwrap();
            registry
                .get_stream("ctp_ticks")
                .unwrap()
                .persisted_files
                .clone()
        };
        if persisted_files.len() == 1
            && persisted_files[0]
                .get("compacted")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        {
            break persisted_files[0].clone();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "compaction worker did not replace persisted files files=[{:?}]",
            persisted_files
        );
        thread::sleep(Duration::from_millis(20));
    };

    let compacted_path = PathBuf::from(compacted["file_path"].as_str().unwrap());
    let batch = read_single_parquet_batch(&compacted_path);
    let dt = batch
        .column(batch.schema().index_of("dt").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        (0..dt.len())
            .map(|index| dt.value(index))
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(compacted["row_count"], serde_json::json!(3));
    assert_eq!(compacted["stats"]["dt"]["min"], serde_json::json!(1));
    assert_eq!(compacted["stats"]["dt"]["max"], serde_json::json!(3));
    assert_eq!(compacted["min_event_ts"], serde_json::json!(1));
    assert_eq!(compacted["max_event_ts"], serde_json::json!(3));
    assert!(first_file.exists());
    assert!(second_file.exists());

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
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

    let mut expected_descriptor = descriptor;
    expected_descriptor["writer_epoch"] = serde_json::Value::from(1);
    assert_eq!(
        registry.segment_descriptor("openctp_ticks").unwrap(),
        Some(expected_descriptor)
    );
}

#[test]
fn registry_source_descriptor_takes_writer_ownership_after_old_writer_lost() {
    let mut registry = Registry::default();
    let old_process_id = registry.register_process("openctp");
    let new_process_id = registry.register_process("openctp");
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
        .attach_writer("openctp_ticks", &old_process_id)
        .unwrap();
    registry
        .register_source(
            "openctp_md_old",
            "openctp",
            &old_process_id,
            "openctp_ticks",
            serde_json::json!({"front": "old"}),
        )
        .unwrap();
    registry.force_expire_process(&old_process_id).unwrap();
    registry.mark_records_lost_for_process(&old_process_id);
    registry
        .register_source(
            "openctp_md_restarted",
            "openctp",
            &new_process_id,
            "openctp_ticks",
            serde_json::json!({}),
        )
        .unwrap();
    registry
        .publish_segment_descriptor(
            "openctp_ticks",
            &new_process_id,
            serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "/tmp/zippy-segment-restarted",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 1,
                "generation": 0,
            }),
        )
        .unwrap();

    assert_eq!(
        registry.streams_for_writer_process(&old_process_id),
        Vec::<String>::new()
    );
    assert_eq!(
        registry.streams_for_writer_process(&new_process_id),
        vec!["openctp_ticks".to_string()]
    );
    assert_eq!(
        registry.get_stream("openctp_ticks").unwrap().status,
        "writer_attached"
    );
    let descriptor = registry
        .segment_descriptor("openctp_ticks")
        .unwrap()
        .unwrap();
    assert_eq!(
        descriptor
            .get("writer_epoch")
            .and_then(serde_json::Value::as_u64),
        Some(3)
    );
}

#[test]
fn registry_rejects_stale_writer_after_new_source_takes_epoch() {
    let mut registry = Registry::default();
    let old_process_id = registry.register_process("openctp");
    let new_process_id = registry.register_process("openctp");
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
            "openctp_md_old",
            "openctp",
            &old_process_id,
            "openctp_ticks",
            serde_json::json!({"front": "old"}),
        )
        .unwrap();
    registry
        .attach_writer("openctp_ticks", &old_process_id)
        .unwrap();
    registry
        .set_source_status("openctp_md_old", "lost", None)
        .unwrap();
    registry
        .register_source(
            "openctp_md_new",
            "openctp",
            &new_process_id,
            "openctp_ticks",
            serde_json::json!({"front": "new"}),
        )
        .unwrap();

    let error = registry
        .publish_persisted_file(
            "openctp_ticks",
            &old_process_id,
            serde_json::json!({
                "file_path": "/tmp/zippy-old.parquet",
                "writer_epoch": 1,
            }),
        )
        .unwrap_err();
    assert_eq!(
        error,
        RegistryError::PersistedFilePublisherNotAuthorized {
            stream_name: "openctp_ticks".to_string(),
            process_id: old_process_id.clone(),
        }
    );

    let error = registry
        .publish_segment_descriptor(
            "openctp_ticks",
            &old_process_id,
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
                "writer_epoch": 1,
            }),
        )
        .unwrap_err();
    assert_eq!(
        error,
        RegistryError::SegmentDescriptorPublisherNotAuthorized {
            stream_name: "openctp_ticks".to_string(),
            process_id: old_process_id,
        }
    );
}

#[test]
fn registry_rejects_second_active_writer_source_for_stream() {
    let mut registry = Registry::default();
    let old_process_id = registry.register_process("openctp");
    let new_process_id = registry.register_process("openctp");
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
            "openctp_md_old",
            "openctp",
            &old_process_id,
            "openctp_ticks",
            serde_json::json!({"front": "old"}),
        )
        .unwrap();

    let error = registry
        .register_source(
            "openctp_md_restarted",
            "openctp",
            &new_process_id,
            "openctp_ticks",
            serde_json::json!({"front": "new"}),
        )
        .unwrap_err();

    assert_eq!(
        error,
        RegistryError::StreamWriterSourceAlreadyExists {
            stream_name: "openctp_ticks".to_string(),
            source_name: "openctp_md_restarted".to_string(),
            owner_source_name: "openctp_md_old".to_string(),
        }
    );
    assert_eq!(
        registry
            .get_stream("openctp_ticks")
            .unwrap()
            .active_writer_source_name
            .as_deref(),
        Some("openctp_md_old")
    );
}

#[test]
fn registry_register_source_rebinds_same_definition_to_new_process() {
    let mut registry = Registry::default();
    let old_process_id = registry.register_process("openctp");
    let new_process_id = registry.register_process("openctp");
    let config = serde_json::json!({"front": "sim"});
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
            &old_process_id,
            "openctp_ticks",
            config.clone(),
        )
        .unwrap();
    registry.force_expire_process(&old_process_id).unwrap();
    registry.mark_records_lost_for_process(&old_process_id);
    assert_eq!(
        registry.get_stream("openctp_ticks").unwrap().status,
        "stale"
    );

    registry
        .register_source(
            "openctp_md",
            "openctp",
            &new_process_id,
            "openctp_ticks",
            config,
        )
        .unwrap();

    let source = registry.get_source("openctp_md").unwrap();
    assert_eq!(registry.sources_len(), 1);
    assert_eq!(source.process_id, new_process_id);
    assert_eq!(source.status, "registered");
    assert_eq!(
        registry.get_stream("openctp_ticks").unwrap().status,
        "stale"
    );

    let descriptor = serde_json::json!({
        "magic": "zippy.segment.active",
        "version": 1,
        "schema_id": 7,
        "row_capacity": 64,
        "shm_os_id": "/tmp/zippy-segment-restarted",
        "payload_offset": 64,
        "committed_row_count_offset": 40,
        "segment_id": 1,
        "generation": 0,
    });
    registry
        .publish_segment_descriptor("openctp_ticks", &new_process_id, descriptor)
        .unwrap();
    assert_eq!(
        registry.get_stream("openctp_ticks").unwrap().status,
        "writer_attached"
    );
}

#[test]
fn registry_register_source_rejects_rebind_while_owner_is_alive() {
    let mut registry = Registry::default();
    let old_process_id = registry.register_process("openctp");
    let new_process_id = registry.register_process("openctp");
    let config = serde_json::json!({"front": "sim"});
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
            &old_process_id,
            "openctp_ticks",
            config.clone(),
        )
        .unwrap();

    let error = registry
        .register_source(
            "openctp_md",
            "openctp",
            &new_process_id,
            "openctp_ticks",
            config,
        )
        .unwrap_err();

    assert_eq!(
        error,
        RegistryError::SourceAlreadyExists {
            source_name: "openctp_md".to_string()
        }
    );
    let source = registry.get_source("openctp_md").unwrap();
    assert_eq!(source.process_id, old_process_id);
    assert_eq!(source.status, "registered");
}

#[test]
fn registry_unregister_source_requires_owner_process() {
    let mut registry = Registry::default();
    let owner_process_id = registry.register_process("openctp");
    let other_process_id = registry.register_process("other");
    let config = serde_json::json!({"front": "sim"});
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
            &owner_process_id,
            "openctp_ticks",
            config,
        )
        .unwrap();

    let error = registry
        .unregister_source_for_process("openctp_md", &other_process_id)
        .unwrap_err();

    assert_eq!(
        error,
        RegistryError::SourceNotOwnedByProcess {
            source_name: "openctp_md".to_string(),
            process_id: other_process_id,
            owner_process_id: Some(owner_process_id.clone()),
        }
    );
    assert!(registry.get_source("openctp_md").is_some());

    registry
        .unregister_source_for_process("openctp_md", &owner_process_id)
        .unwrap();
    assert!(registry.get_source("openctp_md").is_none());
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
                persist_revision: 0,
                segment_reader_leases: Vec::new(),
                writer_epoch: 0,
                buffer_size: 1024,
                frame_size: 256,
                status: "registered".to_string(),
            }],
            sources: vec![],
            engines: vec![],
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
  "engines": []
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
                persist_revision: 0,
                segment_reader_leases: Vec::new(),
                writer_epoch: 0,
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
                writer_epoch: 1,
                revision: 1,
            }],
            engines: vec![SnapshotEngineRecord {
                engine_name: "mid_price_factor".to_string(),
                engine_type: "reactive".to_string(),
                process_id: "proc_2".to_string(),
                input_stream: "openctp_ticks".to_string(),
                output_stream: "openctp_mid_price_factors".to_string(),
                config: serde_json::json!({"id_filter": ["IF2606"]}),
                status: "running".to_string(),
                metrics: serde_json::json!({}),
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

    let (process_id, process_token) = register_process(&wait_path, "openctp");
    assert!(matches!(
        register_stream(
            &wait_path,
            &process_id,
            &process_token,
            "openctp_ticks",
            1024,
            256
        ),
        ControlResponse::StreamRegistered { .. }
    ));
    assert!(matches!(
        send_control_request(
            &wait_path,
            ControlRequest::RegisterSource(RegisterSourceRequest {
                source_name: "openctp_md".to_string(),
                source_type: "openctp".to_string(),
                process_id: process_id.clone(),
                process_token: Some(process_token.clone()),
                output_stream: "openctp_ticks".to_string(),
                config: serde_json::json!({}),
            }),
        ),
        ControlResponse::SourceRegistered { .. }
    ));
    assert!(matches!(
        send_control_request(
            &wait_path,
            ControlRequest::PublishSegmentDescriptor(zippy_core::PublishSegmentDescriptorRequest {
                stream_name: "openctp_ticks".to_string(),
                process_id,
                process_token: Some(process_token),
                descriptor: serde_json::json!({"magic": "zippy.segment.active"}),
            },),
        ),
        ControlResponse::SegmentDescriptorPublished { .. }
    ));

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

    let (process_id, process_token) = register_process(&wait_path, "local_dc");
    let response = register_stream(
        &wait_path,
        &process_id,
        &process_token,
        "openctp_ticks",
        1024,
        256,
    );
    assert!(matches!(response, ControlResponse::StreamRegistered { .. }));

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
  "engines": []
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

    let (process_id, process_token) = register_process(&wait_path, "local_dc");
    let response = register_stream(
        &wait_path,
        &process_id,
        &process_token,
        "openctp_ticks",
        1024,
        256,
    );
    match response {
        ControlResponse::Error { reason } => {
            assert!(reason.contains("failed to create registry snapshot parent"));
        }
        other => panic!("unexpected register stream response: {:?}", other),
    }

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
fn master_server_acquires_segment_reader_lease_without_snapshot_write() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let snapshot_parent = temp.path().join("snapshot");
    let snapshot_path = snapshot_parent.join("master-registry.json");
    let server = MasterServer::with_runtime_config(
        Some(snapshot_path),
        Duration::from_secs(10),
        Duration::from_secs(2),
    );
    let handle_server = server.clone();
    let wait_path = socket_path.clone();
    let join_handle = thread::spawn(move || handle_server.serve(&socket_path).unwrap_err());
    wait_for_socket_ready(&wait_path);

    let (process_id, process_token) = register_process(&wait_path, "lease_reader");
    assert!(matches!(
        register_stream(&wait_path, &process_id, &process_token, "ticks", 64, 4096),
        ControlResponse::StreamRegistered { .. }
    ));
    seed_sealed_segment_identity(&server, "ticks", 1, 0);

    fs::remove_dir_all(&snapshot_parent).unwrap();
    fs::write(&snapshot_parent, b"not-a-directory").unwrap();
    let response = send_control_request(
        &wait_path,
        ControlRequest::AcquireSegmentReaderLease(AcquireSegmentReaderLeaseRequest {
            stream_name: "ticks".to_string(),
            process_id: process_id.clone(),
            process_token: Some(process_token.clone()),
            source_segment_id: 1,
            source_generation: 0,
        }),
    );
    let lease_id = match response {
        ControlResponse::SegmentReaderLeaseAcquired { lease_id, .. } => lease_id,
        other => panic!("unexpected acquire lease response: {other:?}"),
    };

    let leases = server
        .registry()
        .lock()
        .unwrap()
        .get_stream("ticks")
        .unwrap()
        .segment_reader_leases
        .clone();
    assert_eq!(leases.len(), 1);
    assert_eq!(
        leases[0].get("lease_id").and_then(Value::as_str),
        Some(lease_id.as_str())
    );

    server.shutdown();
    let _ = join_handle.join();
}

#[test]
fn master_server_rejects_segment_reader_lease_for_unknown_segment() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let (process_id, process_token) = register_process(&socket_path, "lease_reader");
    assert!(matches!(
        register_stream(&socket_path, &process_id, &process_token, "ticks", 64, 4096),
        ControlResponse::StreamRegistered { .. }
    ));

    let response = send_control_request(
        &socket_path,
        ControlRequest::AcquireSegmentReaderLease(AcquireSegmentReaderLeaseRequest {
            stream_name: "ticks".to_string(),
            process_id: process_id.clone(),
            process_token: Some(process_token.clone()),
            source_segment_id: 999,
            source_generation: 0,
        }),
    );
    match response {
        ControlResponse::Error { reason } => {
            assert!(reason.contains("segment reader lease target not found"));
        }
        other => panic!("unexpected acquire lease response: {other:?}"),
    }

    let leases = server
        .registry()
        .lock()
        .unwrap()
        .get_stream("ticks")
        .unwrap()
        .segment_reader_leases
        .clone();
    assert!(leases.is_empty());

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_releases_segment_reader_lease_without_snapshot_write() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let snapshot_parent = temp.path().join("snapshot");
    let snapshot_path = snapshot_parent.join("master-registry.json");
    let server = MasterServer::with_runtime_config(
        Some(snapshot_path),
        Duration::from_secs(10),
        Duration::from_secs(2),
    );
    let handle_server = server.clone();
    let wait_path = socket_path.clone();
    let join_handle = thread::spawn(move || handle_server.serve(&socket_path).unwrap_err());
    wait_for_socket_ready(&wait_path);

    let (process_id, process_token) = register_process(&wait_path, "lease_reader");
    assert!(matches!(
        register_stream(&wait_path, &process_id, &process_token, "ticks", 64, 4096),
        ControlResponse::StreamRegistered { .. }
    ));
    seed_sealed_segment_identity(&server, "ticks", 1, 0);
    let lease_id = match send_control_request(
        &wait_path,
        ControlRequest::AcquireSegmentReaderLease(AcquireSegmentReaderLeaseRequest {
            stream_name: "ticks".to_string(),
            process_id: process_id.clone(),
            process_token: Some(process_token.clone()),
            source_segment_id: 1,
            source_generation: 0,
        }),
    ) {
        ControlResponse::SegmentReaderLeaseAcquired { lease_id, .. } => lease_id,
        other => panic!("unexpected acquire lease response: {other:?}"),
    };

    fs::remove_dir_all(&snapshot_parent).unwrap();
    fs::write(&snapshot_parent, b"not-a-directory").unwrap();
    let response = send_control_request(
        &wait_path,
        ControlRequest::ReleaseSegmentReaderLease(ReleaseSegmentReaderLeaseRequest {
            stream_name: "ticks".to_string(),
            process_id: process_id.clone(),
            process_token: Some(process_token.clone()),
            lease_id: lease_id.clone(),
        }),
    );
    assert!(matches!(
        response,
        ControlResponse::SegmentReaderLeaseReleased { .. }
    ));

    let leases = server
        .registry()
        .lock()
        .unwrap()
        .get_stream("ticks")
        .unwrap()
        .segment_reader_leases
        .clone();
    assert!(leases.is_empty());

    server.shutdown();
    let _ = join_handle.join();
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
    let (process_id, process_token) = match process_response {
        ControlResponse::ProcessRegistered {
            process_id,
            process_token,
        } => (process_id, process_token),
        other => panic!("unexpected process response: {:?}", other),
    };

    let source_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterSource(RegisterSourceRequest {
            source_name: "openctp_md".to_string(),
            source_type: "openctp".to_string(),
            process_id: process_id.clone(),
            process_token: Some(process_token.clone()),
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
            process_token: Some(process_token.clone()),
            input_stream: "openctp_ticks".to_string(),
            output_stream: "openctp_mid_price_factors".to_string(),
            config: serde_json::json!({"id_filter": ["IF2606"]}),
        }),
    );
    assert!(matches!(
        engine_response,
        ControlResponse::EngineRegistered { .. }
    ));

    let status_response = send_control_request(
        &socket_path,
        ControlRequest::UpdateStatus(UpdateRecordStatusRequest {
            process_id: Some(process_id.clone()),
            process_token: Some(process_token),
            token: None,
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
fn master_server_graceful_shutdown_timeout_keeps_writer_source_alive_in_snapshot() {
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

    let process_response = send_control_request(
        &wait_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "openctp".to_string(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id,
        process_token,
    } = process_response
    else {
        panic!("unexpected process response {process_response:?}");
    };
    assert!(matches!(
        send_control_request(
            &wait_path,
            ControlRequest::RegisterStream(RegisterStreamRequest {
                process_id: Some(process_id.clone()),
                process_token: Some(process_token.clone()),
                token: None,
                stream_name: "ldc_ctp_ticks".to_string(),
                schema: test_stream_schema(),
                schema_hash: test_stream_schema_hash().to_string(),
                buffer_size: 1024,
                frame_size: 256,
            }),
        ),
        ControlResponse::StreamRegistered { .. }
    ));
    assert!(matches!(
        send_control_request(
            &wait_path,
            ControlRequest::RegisterSource(RegisterSourceRequest {
                source_name: "ldc_ctp_md_old".to_string(),
                source_type: "openctp".to_string(),
                process_id: process_id.clone(),
                process_token: Some(process_token),
                output_stream: "ldc_ctp_ticks".to_string(),
                config: serde_json::json!({}),
            }),
        ),
        ControlResponse::SourceRegistered { .. }
    ));

    let outcome = server.request_graceful_shutdown(Duration::from_millis(20));
    assert_eq!(outcome.timed_out_processes, vec![process_id.clone()]);
    assert!(!server.is_running());
    join_handle.join().unwrap();

    let snapshot = SnapshotStore::load(&snapshot_path).unwrap();
    let source = snapshot
        .sources
        .iter()
        .find(|source| source.source_name == "ldc_ctp_md_old")
        .unwrap();
    assert_eq!(source.status, "registered");
    let stream = snapshot
        .streams
        .iter()
        .find(|stream| stream.stream_name == "ldc_ctp_ticks")
        .unwrap();
    assert_eq!(stream.status, "registered");

    let restored = MasterServer::from_snapshot_path(&snapshot_path).unwrap();
    let registry = restored.registry();
    let mut registry = registry.lock().unwrap();
    assert_eq!(
        registry.get_source("ldc_ctp_md_old").unwrap().status,
        "restored"
    );
    assert_eq!(
        registry.get_stream("ldc_ctp_ticks").unwrap().status,
        "restored"
    );
    let new_process_id = registry.register_process("openctp");
    assert_ne!(new_process_id, process_id);
    registry
        .register_source(
            "ldc_ctp_md_new",
            "openctp",
            &new_process_id,
            "ldc_ctp_ticks",
            serde_json::json!({}),
        )
        .unwrap();

    let _ = fs::remove_file(wait_path);
}

#[test]
fn master_server_publishes_and_fetches_segment_descriptor() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let (writer_process_id, writer_process_token) = register_process(&socket_path, "openctp");
    assert!(matches!(
        register_stream(
            &socket_path,
            &writer_process_id,
            &writer_process_token,
            "openctp_ticks",
            1024,
            256
        ),
        ControlResponse::StreamRegistered { .. }
    ));
    assert!(matches!(
        send_control_request(
            &socket_path,
            ControlRequest::RegisterSource(RegisterSourceRequest {
                source_name: "openctp_md".to_string(),
                source_type: "openctp".to_string(),
                process_id: writer_process_id.clone(),
                process_token: Some(writer_process_token.clone()),
                output_stream: "openctp_ticks".to_string(),
                config: serde_json::json!({}),
            }),
        ),
        ControlResponse::SourceRegistered { .. }
    ));
    let (reader_process_id, reader_process_token) =
        register_process(&socket_path, "segment_reader");

    let publish_response = send_control_request(
        &socket_path,
        ControlRequest::PublishSegmentDescriptor(zippy_core::PublishSegmentDescriptorRequest {
            stream_name: "openctp_ticks".to_string(),
            process_id: writer_process_id,
            process_token: Some(writer_process_token),
            descriptor: serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "/tmp/zippy-segment",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 1,
                "generation": 0,
            }),
        }),
    );
    match publish_response {
        ControlResponse::SegmentDescriptorPublished { stream_name } => {
            assert_eq!(stream_name, "openctp_ticks");
        }
        other => panic!("unexpected publish response: {:?}", other),
    }

    let fetch_response = send_control_request(
        &socket_path,
        ControlRequest::GetSegmentDescriptor(zippy_core::GetSegmentDescriptorRequest {
            stream_name: "openctp_ticks".to_string(),
            process_id: reader_process_id,
            process_token: Some(reader_process_token),
        }),
    );
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
fn master_server_watch_segment_descriptor_wakes_on_publish() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let (writer_process_id, writer_process_token) = register_process(&socket_path, "openctp");
    assert!(matches!(
        register_stream(
            &socket_path,
            &writer_process_id,
            &writer_process_token,
            "openctp_ticks",
            1024,
            256
        ),
        ControlResponse::StreamRegistered { .. }
    ));
    assert!(matches!(
        send_control_request(
            &socket_path,
            ControlRequest::RegisterSource(RegisterSourceRequest {
                source_name: "openctp_md".to_string(),
                source_type: "openctp".to_string(),
                process_id: writer_process_id.clone(),
                process_token: Some(writer_process_token.clone()),
                output_stream: "openctp_ticks".to_string(),
                config: serde_json::json!({}),
            }),
        ),
        ControlResponse::SourceRegistered { .. }
    ));
    let (reader_process_id, reader_process_token) =
        register_process(&socket_path, "segment_reader");

    let watch_socket_path = socket_path.clone();
    let watch_handle = thread::spawn(move || {
        send_control_request(
            &watch_socket_path,
            ControlRequest::Watch(WatchRequest {
                process_id: reader_process_id,
                process_token: Some(reader_process_token),
                resource: WatchResource::SegmentDescriptor {
                    stream_name: "openctp_ticks".to_string(),
                },
                after_revision: 0,
                timeout_ms: 1_000,
            }),
        )
    });

    thread::sleep(Duration::from_millis(20));
    let publish_response = send_control_request(
        &socket_path,
        ControlRequest::PublishSegmentDescriptor(zippy_core::PublishSegmentDescriptorRequest {
            stream_name: "openctp_ticks".to_string(),
            process_id: writer_process_id,
            process_token: Some(writer_process_token),
            descriptor: serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "/tmp/zippy-segment",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 1,
                "generation": 0,
            }),
        }),
    );
    assert!(matches!(
        publish_response,
        ControlResponse::SegmentDescriptorPublished { .. }
    ));

    let watch_response = watch_handle.join().unwrap();
    match watch_response {
        ControlResponse::ResourceChanged { event: Some(event) } => {
            assert_eq!(
                event.resource,
                WatchResource::SegmentDescriptor {
                    stream_name: "openctp_ticks".to_string(),
                }
            );
            assert_eq!(event.revision, 1);
            assert_eq!(
                event.payload["descriptor"]["magic"],
                serde_json::json!("zippy.segment.active")
            );
        }
        other => panic!("unexpected watch segment descriptor response {other:?}"),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_watch_stream_wakes_on_descriptor_publish() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let (writer_process_id, writer_process_token) = register_process(&socket_path, "openctp");
    assert!(matches!(
        register_stream(
            &socket_path,
            &writer_process_id,
            &writer_process_token,
            "openctp_ticks",
            1024,
            256
        ),
        ControlResponse::StreamRegistered { .. }
    ));
    assert!(matches!(
        send_control_request(
            &socket_path,
            ControlRequest::RegisterSource(RegisterSourceRequest {
                source_name: "openctp_md".to_string(),
                source_type: "openctp".to_string(),
                process_id: writer_process_id.clone(),
                process_token: Some(writer_process_token.clone()),
                output_stream: "openctp_ticks".to_string(),
                config: serde_json::json!({}),
            }),
        ),
        ControlResponse::SourceRegistered { .. }
    ));
    let (watcher_process_id, watcher_process_token) =
        register_process(&socket_path, "stream_watcher");

    let watch_socket_path = socket_path.clone();
    let watch_handle = thread::spawn(move || {
        send_control_request(
            &watch_socket_path,
            ControlRequest::Watch(WatchRequest {
                process_id: watcher_process_id,
                process_token: Some(watcher_process_token),
                resource: WatchResource::Stream {
                    stream_name: "openctp_ticks".to_string(),
                },
                after_revision: 0,
                timeout_ms: 1_000,
            }),
        )
    });

    thread::sleep(Duration::from_millis(20));
    let publish_response = send_control_request(
        &socket_path,
        ControlRequest::PublishSegmentDescriptor(zippy_core::PublishSegmentDescriptorRequest {
            stream_name: "openctp_ticks".to_string(),
            process_id: writer_process_id,
            process_token: Some(writer_process_token),
            descriptor: serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "/tmp/zippy-segment",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 1,
                "generation": 0,
            }),
        }),
    );
    assert!(matches!(
        publish_response,
        ControlResponse::SegmentDescriptorPublished { .. }
    ));

    let watch_response = watch_handle.join().unwrap();
    match watch_response {
        ControlResponse::ResourceChanged { event: Some(event) } => {
            assert_eq!(
                event.resource,
                WatchResource::Stream {
                    stream_name: "openctp_ticks".to_string(),
                }
            );
            assert_eq!(event.revision, 1);
            assert_eq!(
                event.payload["stream"]["stream_name"],
                serde_json::json!("openctp_ticks")
            );
            assert_eq!(
                event.payload["stream"]["descriptor_generation"],
                serde_json::json!(1)
            );
        }
        other => panic!("unexpected watch stream response {other:?}"),
    }

    server.shutdown();
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
fn master_server_watch_source_wakes_on_status_update() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let writer_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "openctp".to_string(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id,
        process_token,
    } = writer_response
    else {
        panic!("unexpected writer response {writer_response:?}");
    };
    let watcher_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "source_watcher".to_string(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id: watcher_process_id,
        process_token: watcher_process_token,
    } = watcher_response
    else {
        panic!("unexpected watcher response {watcher_response:?}");
    };
    assert!(matches!(
        send_control_request(
            &socket_path,
            ControlRequest::RegisterStream(RegisterStreamRequest {
                process_id: None,
                process_token: None,
                token: Some(server.token().to_string()),
                stream_name: "openctp_ticks".to_string(),
                schema: test_stream_schema(),
                schema_hash: test_stream_schema_hash().to_string(),
                buffer_size: 1024,
                frame_size: 256,
            }),
        ),
        ControlResponse::StreamRegistered { .. }
    ));
    assert!(matches!(
        send_control_request(
            &socket_path,
            ControlRequest::RegisterSource(RegisterSourceRequest {
                source_name: "openctp_md".to_string(),
                source_type: "openctp".to_string(),
                process_id: process_id.clone(),
                process_token: Some(process_token.clone()),
                output_stream: "openctp_ticks".to_string(),
                config: serde_json::json!({}),
            }),
        ),
        ControlResponse::SourceRegistered { .. }
    ));

    let watch_socket_path = socket_path.clone();
    let watch_handle = thread::spawn(move || {
        send_control_request(
            &watch_socket_path,
            ControlRequest::Watch(WatchRequest {
                process_id: watcher_process_id,
                process_token: Some(watcher_process_token),
                resource: WatchResource::Source {
                    source_name: "openctp_md".to_string(),
                },
                after_revision: 3,
                timeout_ms: 1_000,
            }),
        )
    });

    thread::sleep(Duration::from_millis(20));
    let status_response = send_control_request(
        &socket_path,
        ControlRequest::UpdateStatus(UpdateRecordStatusRequest {
            process_id: Some(process_id),
            process_token: Some(process_token),
            token: None,
            kind: "source".to_string(),
            name: "openctp_md".to_string(),
            status: "running".to_string(),
            metrics: Some(serde_json::json!({"rows_total": 7})),
        }),
    );
    assert!(matches!(
        status_response,
        ControlResponse::StatusUpdated { .. }
    ));

    let watch_response = watch_handle.join().unwrap();
    match watch_response {
        ControlResponse::ResourceChanged { event: Some(event) } => {
            assert_eq!(
                event.resource,
                WatchResource::Source {
                    source_name: "openctp_md".to_string()
                }
            );
            assert!(event.revision > 1);
            assert_eq!(
                event.payload["source"]["source_name"],
                serde_json::json!("openctp_md")
            );
            assert_eq!(
                event.payload["source"]["status"],
                serde_json::json!("running")
            );
            assert_eq!(
                event.payload["source"]["metrics"]["rows_total"],
                serde_json::json!(7)
            );
        }
        other => panic!("unexpected watch source response {other:?}"),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_watch_persisted_file_wakes_on_publish() {
    let socket_path = unique_socket_path();
    let temp = tempfile::tempdir().unwrap();
    let persist_root = temp.path().join("persisted");
    fs::create_dir_all(&persist_root).unwrap();
    let persisted_file_path = persist_root.join("file_1.parquet");
    fs::write(&persisted_file_path, b"not really parquet").unwrap();
    let mut config = ZippyConfig::default();
    config.table.persist.data_dir = persist_root.to_string_lossy().to_string();
    let (server, join_handle) = spawn_test_server_with_config(&socket_path, config);

    let (writer_process_id, writer_process_token) = register_process(&socket_path, "openctp");
    assert!(matches!(
        register_stream(
            &socket_path,
            &writer_process_id,
            &writer_process_token,
            "openctp_ticks",
            1024,
            256
        ),
        ControlResponse::StreamRegistered { .. }
    ));
    assert!(matches!(
        send_control_request(
            &socket_path,
            ControlRequest::RegisterSource(RegisterSourceRequest {
                source_name: "openctp_md".to_string(),
                source_type: "openctp".to_string(),
                process_id: writer_process_id.clone(),
                process_token: Some(writer_process_token.clone()),
                output_stream: "openctp_ticks".to_string(),
                config: serde_json::json!({}),
            }),
        ),
        ControlResponse::SourceRegistered { .. }
    ));
    let (watcher_process_id, watcher_process_token) =
        register_process(&socket_path, "persist_watcher");

    let watch_socket_path = socket_path.clone();
    let watch_handle = thread::spawn(move || {
        send_control_request(
            &watch_socket_path,
            ControlRequest::Watch(WatchRequest {
                process_id: watcher_process_id,
                process_token: Some(watcher_process_token),
                resource: WatchResource::PersistedFile {
                    stream_name: "openctp_ticks".to_string(),
                },
                after_revision: 0,
                timeout_ms: 1_000,
            }),
        )
    });

    thread::sleep(Duration::from_millis(20));
    let publish_response = send_control_request(
        &socket_path,
        ControlRequest::PublishPersistedFile(
            zippy_core::bus_protocol::PublishPersistedFileRequest {
                stream_name: "openctp_ticks".to_string(),
                process_id: writer_process_id,
                process_token: Some(writer_process_token),
                persisted_file: serde_json::json!({
                    "persist_file_id": "file_1",
                    "file_path": persisted_file_path,
                }),
            },
        ),
    );
    assert!(matches!(
        publish_response,
        ControlResponse::PersistedFilePublished { .. }
    ));

    let watch_response = watch_handle.join().unwrap();
    match watch_response {
        ControlResponse::ResourceChanged { event: Some(event) } => {
            assert_eq!(
                event.resource,
                WatchResource::PersistedFile {
                    stream_name: "openctp_ticks".to_string()
                }
            );
            assert_eq!(event.revision, 1);
            assert_eq!(
                event.payload["stream_name"],
                serde_json::json!("openctp_ticks")
            );
            assert_eq!(
                event.payload["persisted_files"][0]["persist_file_id"],
                serde_json::json!("file_1")
            );
        }
        other => panic!("unexpected watch persisted file response {other:?}"),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_watch_gateway_config_returns_current_config() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let process_response = send_control_request(
        &socket_path,
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "gateway_watcher".to_string(),
        }),
    );
    let ControlResponse::ProcessRegistered {
        process_id,
        process_token,
    } = process_response
    else {
        panic!("unexpected process response {process_response:?}");
    };

    let watch_response = send_control_request(
        &socket_path,
        ControlRequest::Watch(WatchRequest {
            process_id,
            process_token: Some(process_token),
            resource: WatchResource::GatewayConfig,
            after_revision: 0,
            timeout_ms: 1_000,
        }),
    );

    match watch_response {
        ControlResponse::ResourceChanged { event: Some(event) } => {
            assert_eq!(event.resource, WatchResource::GatewayConfig);
            assert_eq!(event.revision, 1);
            assert!(event.payload.get("config").is_some());
        }
        other => panic!("unexpected watch gateway config response {other:?}"),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_treats_duplicate_stream_registration_as_idempotent_success() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let (process_id, process_token) = register_process(&socket_path, "local_dc");

    let first_response = register_stream(
        &socket_path,
        &process_id,
        &process_token,
        "openctp_ticks",
        1024,
        256,
    );
    assert!(matches!(
        first_response,
        ControlResponse::StreamRegistered { .. }
    ));

    let duplicate_response = register_stream(
        &socket_path,
        &process_id,
        &process_token,
        "openctp_ticks",
        1024,
        256,
    );
    match duplicate_response {
        ControlResponse::StreamRegistered { stream_name } => {
            assert_eq!(stream_name, "openctp_ticks");
        }
        other => panic!("unexpected duplicate stream response: {:?}", other),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_rejects_zero_capacity_stream_registration() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let (process_id, process_token) = register_process(&socket_path, "local_dc");

    let response = register_stream(
        &socket_path,
        &process_id,
        &process_token,
        "openctp_ticks",
        0,
        0,
    );
    match response {
        ControlResponse::Error { reason } => {
            assert!(reason.contains("invalid stream sizing"));
            assert!(reason.contains("buffer_size=[0]"));
            assert!(reason.contains("frame_size=[0]"));
        }
        other => panic!("unexpected zero capacity response: {:?}", other),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_rejects_stream_registration_with_frame_size_mismatch() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let (process_id, process_token) = register_process(&socket_path, "local_dc");

    let first_response = register_stream(
        &socket_path,
        &process_id,
        &process_token,
        "openctp_ticks",
        1024,
        256,
    );
    assert!(matches!(
        first_response,
        ControlResponse::StreamRegistered { .. }
    ));

    let mismatch_response = register_stream(
        &socket_path,
        &process_id,
        &process_token,
        "openctp_ticks",
        1024,
        512,
    );
    match mismatch_response {
        ControlResponse::Error { reason } => {
            assert!(reason.contains("stream configuration mismatch"));
            assert!(reason.contains("existing_buffer_size=[1024]"));
            assert!(reason.contains("existing_frame_size=[256]"));
            assert!(reason.contains("requested_buffer_size=[1024]"));
            assert!(reason.contains("requested_frame_size=[512]"));
        }
        other => panic!("unexpected mismatch response: {:?}", other),
    }

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn master_server_lists_registered_streams_over_unix_socket() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let (process_id, process_token) = register_process(&socket_path, "local_dc");

    let stream_response = register_stream(
        &socket_path,
        &process_id,
        &process_token,
        "openctp_ticks",
        1024,
        256,
    );
    assert!(matches!(
        stream_response,
        ControlResponse::StreamRegistered { .. }
    ));

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

    let (process_id, process_token) = register_process(&socket_path, "local_dc");

    let stream_response = register_stream(
        &socket_path,
        &process_id,
        &process_token,
        "openctp_ticks",
        1024,
        256,
    );
    assert!(matches!(
        stream_response,
        ControlResponse::StreamRegistered { .. }
    ));

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

    let (writer_process_id, writer_process_token) = register_process(&socket_path, "local_dc");

    let stream_response = register_stream(
        &socket_path,
        &writer_process_id,
        &writer_process_token,
        "openctp_ticks",
        1024,
        256,
    );
    assert!(matches!(
        stream_response,
        ControlResponse::StreamRegistered { .. }
    ));

    let duplicate_response = register_stream(
        &socket_path,
        &writer_process_id,
        &writer_process_token,
        "openctp_ticks",
        1024,
        256,
    );
    assert!(matches!(
        duplicate_response,
        ControlResponse::StreamRegistered { .. }
    ));

    let list_response = send_request(&socket_path, "{\"ListStreams\":{}}\n");
    assert!(list_response.contains("StreamsListed"));

    let get_response = send_request(
        &socket_path,
        "{\"GetStream\":{\"stream_name\":\"openctp_ticks\"}}\n",
    );
    assert!(get_response.contains("StreamFetched"));

    let descriptor_response = send_control_request(
        &socket_path,
        ControlRequest::GetSegmentDescriptor(
            zippy_core::bus_protocol::GetSegmentDescriptorRequest {
                stream_name: "openctp_ticks".to_string(),
                process_id: writer_process_id.clone(),
                process_token: Some(writer_process_token.clone()),
            },
        ),
    );
    assert!(matches!(
        descriptor_response,
        ControlResponse::SegmentDescriptorFetched { .. }
    ));

    let config_response = send_request(&socket_path, "{\"GetConfig\":{}}\n");
    assert!(config_response.contains("ConfigFetched"));

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
    assert_no_record(&records, "list_streams", "success", "INFO");
    assert_no_record(&records, "get_stream", "success", "INFO");
    assert_no_record(&records, "get_segment_descriptor", "success", "INFO");
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
