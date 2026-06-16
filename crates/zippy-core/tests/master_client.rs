#![cfg(unix)]

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::datatypes::Schema;
use zippy_core::{
    canonical_schema_hash, schema_metadata, ControlEndpoint, ControlRequest, ControlResponse,
    HeartbeatRequest, MasterClient, RegisterEngineRequest, RegisterProcessRequest,
    RegisterSourceRequest, RegisterStreamRequest, SchemaRef, StreamInfo, UnregisterSourceRequest,
    UpdateRecordStatusRequest, WatchResource,
};

fn empty_schema() -> SchemaRef {
    std::sync::Arc::new(Schema::empty())
}

fn empty_schema_metadata() -> serde_json::Value {
    schema_metadata(&empty_schema())
}

fn empty_schema_hash() -> String {
    canonical_schema_hash(&empty_schema())
}

fn test_stream_info() -> StreamInfo {
    StreamInfo {
        stream_name: "ticks".to_string(),
        schema: empty_schema_metadata(),
        schema_hash: empty_schema_hash(),
        data_path: "segment".to_string(),
        descriptor_generation: 0,
        active_segment_descriptor: None,
        active_segment_preflight: None,
        segment_row_capacity: None,
        sealed_segments: Vec::new(),
        persisted_files: Vec::new(),
        persist_events: Vec::new(),
        segment_reader_leases: Vec::new(),
        buffer_size: 1024,
        frame_size: 256,
        write_seq: 42,
        writer_process_id: None,
        writer_epoch: 0,
        reader_count: 0,
        status: "registered".to_string(),
    }
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
        WatchResource::Source { source_name } => ControlResponse::ResourceChanged {
            event: Some(zippy_core::ResourceEvent {
                resource: WatchResource::Source {
                    source_name: source_name.clone(),
                },
                revision: after_revision + 1,
                payload: serde_json::json!({
                    "source": {
                        "source_name": source_name,
                        "status": "running",
                    },
                }),
            }),
        },
        WatchResource::PersistedFile { stream_name } => ControlResponse::ResourceChanged {
            event: Some(zippy_core::ResourceEvent {
                resource: WatchResource::PersistedFile {
                    stream_name: stream_name.clone(),
                },
                revision: after_revision + 1,
                payload: serde_json::json!({
                    "stream_name": stream_name,
                    "persisted_files": [],
                    "persist_events": [],
                }),
            }),
        },
        WatchResource::GatewayConfig => ControlResponse::ResourceChanged {
            event: Some(zippy_core::ResourceEvent {
                resource: WatchResource::GatewayConfig,
                revision: 1,
                payload: serde_json::json!({
                    "config": {},
                }),
            }),
        },
        WatchResource::SegmentDescriptor { stream_name } => {
            assert_eq!(stream_name, "ticks");
            assert_eq!(process_id, "proc_1");
            ControlResponse::ResourceChanged {
                event: Some(zippy_core::ResourceEvent {
                    resource: WatchResource::SegmentDescriptor { stream_name },
                    revision: after_revision + 1,
                    payload: serde_json::json!({
                        "descriptor": {
                            "magic": "zippy.segment.active",
                            "version": 1,
                            "schema_id": 7,
                            "row_capacity": 64,
                            "shm_os_id": "/tmp/zippy-segment",
                            "payload_offset": 64,
                            "committed_row_count_offset": 40,
                            "segment_id": 2,
                            "generation": 1,
                        },
                    }),
                }),
            }
        }
        WatchResource::Stream { stream_name } => {
            assert_eq!(stream_name, "ticks");
            assert_eq!(process_id, "proc_1");
            ControlResponse::ResourceChanged {
                event: Some(zippy_core::ResourceEvent {
                    resource: WatchResource::Stream {
                        stream_name: stream_name.clone(),
                    },
                    revision: after_revision + 1,
                    payload: serde_json::json!({
                        "stream": {
                            "stream_name": stream_name,
                            "descriptor_generation": after_revision + 1,
                        },
                    }),
                }),
            }
        }
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
                ControlRequest::Envelope(envelope) => {
                    let inner = if envelope.verb.as_deref() == Some("watch") {
                        fake_watch_response(
                            envelope.process_id.expect("watch process_id"),
                            envelope.resource.expect("watch resource"),
                            envelope.revision.unwrap_or_default(),
                            envelope.timeout_ms.unwrap_or_default(),
                        )
                    } else {
                        ControlResponse::Error {
                            reason: "fake server does not handle envelope".to_string(),
                        }
                    };
                    ControlResponse::Envelope(zippy_core::ControlEnvelopeResponse {
                        version: envelope.version,
                        request_id: envelope.request_id,
                        inner: Box::new(inner),
                    })
                }
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
                ControlRequest::RegisterStream(RegisterStreamRequest {
                    stream_name,
                    schema,
                    schema_hash,
                    buffer_size,
                    frame_size,
                    ..
                }) => {
                    assert_eq!(stream_name, "ticks");
                    assert_eq!(schema, empty_schema_metadata());
                    assert_eq!(schema_hash, empty_schema_hash());
                    assert_eq!(buffer_size, 1024);
                    assert_eq!(frame_size, 256);
                    ControlResponse::StreamRegistered { stream_name }
                }
                ControlRequest::RegisterSource(RegisterSourceRequest {
                    source_name,
                    source_type,
                    output_stream,
                    ..
                }) => {
                    assert_eq!(source_type, "openctp");
                    assert_eq!(output_stream, "openctp_ticks");
                    ControlResponse::SourceRegistered { source_name }
                }
                ControlRequest::UnregisterSource(UnregisterSourceRequest {
                    source_name,
                    process_id,
                    ..
                }) => {
                    assert_eq!(source_name, "openctp_md");
                    assert_eq!(process_id, "proc_1");
                    ControlResponse::SourceUnregistered { source_name }
                }
                ControlRequest::RegisterEngine(RegisterEngineRequest {
                    engine_name,
                    engine_type,
                    input_stream,
                    output_stream,
                    ..
                }) => {
                    assert_eq!(engine_type, "reactive");
                    assert_eq!(input_stream, "openctp_ticks");
                    assert_eq!(output_stream, "openctp_mid_price_factors");
                    ControlResponse::EngineRegistered { engine_name }
                }
                ControlRequest::UpdateStatus(UpdateRecordStatusRequest {
                    kind,
                    name,
                    status,
                    metrics,
                    ..
                }) => {
                    assert_eq!(kind, "engine");
                    assert_eq!(name, "mid_price_factor");
                    assert_eq!(status, "running");
                    assert_eq!(
                        metrics,
                        Some(serde_json::json!({
                            "rows": 12,
                        }))
                    );
                    ControlResponse::StatusUpdated { kind, name }
                }
                ControlRequest::PublishSegmentDescriptor(request) => {
                    assert_eq!(request.stream_name, "ticks");
                    assert_eq!(request.process_id, "proc_1");
                    assert_eq!(request.descriptor["magic"], "zippy.segment.active");
                    ControlResponse::SegmentDescriptorPublished {
                        stream_name: request.stream_name,
                    }
                }
                ControlRequest::PublishPersistedFile(request) => {
                    assert_eq!(request.stream_name, "ticks");
                    assert_eq!(request.process_id, "proc_1");
                    assert_eq!(request.persisted_file["file_path"], "/data/ticks.parquet");
                    ControlResponse::PersistedFilePublished {
                        stream_name: request.stream_name,
                    }
                }
                ControlRequest::ReplacePersistedFiles(request) => {
                    assert_eq!(request.stream_name, "ticks");
                    assert_eq!(
                        request.persisted_files[0]["file_path"],
                        "/data/compact.parquet"
                    );
                    ControlResponse::PersistedFilesReplaced {
                        stream_name: request.stream_name,
                    }
                }
                ControlRequest::PublishPersistEvent(request) => {
                    assert_eq!(request.stream_name, "ticks");
                    assert_eq!(request.process_id, "proc_1");
                    assert_eq!(
                        request.persist_event["persist_event_type"],
                        "persist_failed"
                    );
                    ControlResponse::PersistEventPublished {
                        stream_name: request.stream_name,
                    }
                }
                ControlRequest::AcquireSegmentReaderLease(request) => {
                    assert_eq!(request.stream_name, "ticks");
                    assert_eq!(request.process_id, "proc_1");
                    assert_eq!(request.source_segment_id, 1);
                    assert_eq!(request.source_generation, 0);
                    ControlResponse::SegmentReaderLeaseAcquired {
                        stream_name: request.stream_name,
                        lease_id: "segment-lease-1".to_string(),
                    }
                }
                ControlRequest::ReleaseSegmentReaderLease(request) => {
                    assert_eq!(request.stream_name, "ticks");
                    assert_eq!(request.process_id, "proc_1");
                    assert_eq!(request.lease_id, "segment-lease-1");
                    ControlResponse::SegmentReaderLeaseReleased {
                        stream_name: request.stream_name,
                        lease_id: request.lease_id,
                    }
                }
                ControlRequest::GetSegmentDescriptor(request) => {
                    assert_eq!(request.stream_name, "ticks");
                    assert_eq!(request.process_id, "proc_1");
                    ControlResponse::SegmentDescriptorFetched {
                        stream_name: request.stream_name,
                        descriptor: Some(serde_json::json!({
                            "magic": "zippy.segment.active",
                            "version": 1,
                            "schema_id": 7,
                            "row_capacity": 64,
                            "shm_os_id": "/tmp/zippy-segment",
                            "payload_offset": 64,
                            "committed_row_count_offset": 40,
                            "segment_id": 1,
                            "generation": 0,
                        })),
                    }
                }
                ControlRequest::ListStreams(_) => {
                    ControlResponse::StreamsListed(zippy_core::ListStreamsResponse {
                        streams: vec![test_stream_info()],
                    })
                }
                ControlRequest::ListStreamStatuses(_) => {
                    ControlResponse::StreamsListed(zippy_core::ListStreamsResponse {
                        streams: vec![test_stream_info()],
                    })
                }
                ControlRequest::GetStream(_) => {
                    ControlResponse::StreamFetched(zippy_core::GetStreamResponse {
                        stream: test_stream_info(),
                    })
                }
                ControlRequest::GetStreamStatus(_) => {
                    ControlResponse::StreamFetched(zippy_core::GetStreamResponse {
                        stream: test_stream_info(),
                    })
                }
                ControlRequest::GetConfig(_) => ControlResponse::ConfigFetched {
                    config: serde_json::json!({
                        "log": {
                            "level": "warn",
                        },
                        "table": {
                            "row_capacity": 2048,
                            "persist": {
                                "enabled": true,
                                "method": "parquet",
                                "data_dir": "data",
                                "partition": {
                                    "dt_column": null,
                                    "id_column": null,
                                    "dt_part": null,
                                },
                            },
                        },
                    }),
                },
                ControlRequest::DropTable(request) => {
                    ControlResponse::TableDropped(zippy_core::DropTableResult {
                        table_name: request.table_name,
                        dropped: true,
                        sources_removed: 0,
                        engines_removed: 0,
                        persisted_files_deleted: 0,
                    })
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

#[test]
fn master_client_gets_config() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 1);
    wait_for_socket(&socket_path);

    let client = MasterClient::connect(&socket_path).unwrap();
    let config = client.get_config().unwrap();

    assert_eq!(config.log.level, "warn");
    assert_eq!(config.table.row_capacity, 2048);
    assert!(config.table.persist.enabled);
    assert_eq!(config.table.persist.method, "parquet");
    assert_eq!(config.table.persist.data_dir, "data");

    server.join().unwrap();
}

#[test]
fn master_client_lists_streams() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 3);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    client
        .register_stream("ticks", empty_schema(), 1024, 256)
        .unwrap();

    let streams = client.list_streams().unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].stream_name, "ticks");
    assert_eq!(streams[0].buffer_size, 1024);
    assert_eq!(streams[0].frame_size, 256);
    assert_eq!(streams[0].write_seq, 42);
    assert_eq!(streams[0].status, "registered");

    server.join().unwrap();
}

#[test]
fn master_client_lists_stream_statuses() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 3);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    client
        .register_stream("ticks", empty_schema(), 1024, 256)
        .unwrap();

    let streams = client.list_streams_status().unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].stream_name, "ticks");
    assert_eq!(streams[0].buffer_size, 1024);
    assert_eq!(streams[0].frame_size, 256);
    assert_eq!(streams[0].write_seq, 42);
    assert_eq!(streams[0].status, "registered");

    server.join().unwrap();
}

#[test]
fn master_client_gets_single_stream() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 3);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    client
        .register_stream("ticks", empty_schema(), 1024, 256)
        .unwrap();

    let stream = client.get_stream("ticks").unwrap();
    assert_eq!(stream.stream_name, "ticks");
    assert_eq!(stream.buffer_size, 1024);
    assert_eq!(stream.frame_size, 256);
    assert_eq!(stream.write_seq, 42);
    assert_eq!(stream.status, "registered");

    server.join().unwrap();
}

#[test]
fn master_client_gets_single_stream_status() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 3);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    client
        .register_stream("ticks", empty_schema(), 1024, 256)
        .unwrap();

    let stream = client.get_stream_status("ticks").unwrap();
    assert_eq!(stream.stream_name, "ticks");
    assert_eq!(stream.buffer_size, 1024);
    assert_eq!(stream.frame_size, 256);
    assert_eq!(stream.write_seq, 42);
    assert_eq!(stream.status, "registered");

    server.join().unwrap();
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
fn master_client_watches_generic_stream_resource() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 2);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();

    let event = client
        .watch_resource(
            WatchResource::Stream {
                stream_name: "ticks".to_string(),
            },
            0,
            Duration::from_millis(10),
        )
        .unwrap()
        .expect("stream resource should change");

    assert_eq!(
        event.resource,
        WatchResource::Stream {
            stream_name: "ticks".to_string(),
        }
    );
    assert_eq!(event.revision, 1);
    assert_eq!(
        event.payload["stream"]["stream_name"],
        serde_json::json!("ticks")
    );

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

#[test]
fn master_client_registers_control_plane_entities_and_updates_status() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 4);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    client
        .register_source(
            "openctp_md",
            "openctp",
            "openctp_ticks",
            serde_json::json!({
                "front": "tcp://127.0.0.1:12345",
            }),
        )
        .unwrap();
    client
        .register_engine(
            "mid_price_factor",
            "reactive",
            "openctp_ticks",
            "openctp_mid_price_factors",
            serde_json::json!({
                "id_filter": ["IF2606"],
            }),
        )
        .unwrap();
    client
        .update_status(
            "engine",
            "mid_price_factor",
            "running",
            Some(serde_json::json!({
                "rows": 12,
            })),
        )
        .unwrap();

    server.join().unwrap();
}

#[test]
fn master_client_unregisters_source_for_registered_process() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 3);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    client
        .register_source(
            "openctp_md",
            "openctp",
            "openctp_ticks",
            serde_json::json!({
                "front": "tcp://127.0.0.1:12345",
            }),
        )
        .unwrap();
    client.unregister_source("openctp_md").unwrap();

    server.join().unwrap();
}

#[test]
fn control_request_serialization_uses_buffer_and_frame_sizes() {
    let request = ControlRequest::RegisterStream(RegisterStreamRequest {
        process_id: None,
        process_token: None,
        token: None,
        stream_name: "ticks".to_string(),
        schema: empty_schema_metadata(),
        schema_hash: empty_schema_hash(),
        buffer_size: 1024,
        frame_size: 256,
    });

    let json = serde_json::to_string(&request).unwrap();

    assert!(json.contains("\"buffer_size\":1024"));
    assert!(json.contains("\"frame_size\":256"));
    assert!(!json.contains("ring_capacity"));
}

#[test]
fn master_client_publishes_and_fetches_segment_descriptor() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 8);
    wait_for_socket(&socket_path);

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
    let bytes = serde_json::to_vec(&descriptor).unwrap();

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    client
        .publish_segment_descriptor_bytes("ticks", &bytes)
        .unwrap();
    client
        .publish_persisted_file(
            "ticks",
            serde_json::json!({
                "file_path": "/data/ticks.parquet",
            }),
        )
        .unwrap();
    client
        .replace_persisted_files(
            "ticks",
            vec![serde_json::json!({
                "file_path": "/data/compact.parquet",
            })],
        )
        .unwrap();
    let fetched = client.get_segment_descriptor("ticks").unwrap();

    assert_eq!(fetched, Some(descriptor));

    server.join().unwrap();
}

#[test]
fn stream_info_serialization_includes_write_seq() {
    let stream = test_stream_info();

    let json = serde_json::to_string(&stream).unwrap();

    assert!(json.contains("\"write_seq\":42"));
    assert!(json.contains("\"buffer_size\":1024"));
    assert!(json.contains("\"frame_size\":256"));
    assert!(!json.contains("ring_capacity"));
}
