use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use zippy_core::{
    AttachStreamRequest, ControlRequest, ControlResponse, DetachReaderRequest, DetachWriterRequest,
    HeartbeatRequest, MasterClient, ReaderDescriptor, RegisterEngineRequest,
    RegisterProcessRequest, RegisterSinkRequest, RegisterSourceRequest, RegisterStreamRequest,
    SchemaRef, StreamInfo, UpdateRecordStatusRequest, WriterDescriptor, BUS_LAYOUT_VERSION,
};
use zippy_shm_bridge::SharedFrameRing;

fn empty_schema() -> SchemaRef {
    std::sync::Arc::new(Schema::empty())
}

fn instrument_schema() -> SchemaRef {
    std::sync::Arc::new(Schema::new(vec![Field::new(
        "instrument_id",
        DataType::Utf8,
        false,
    )]))
}

fn encode_arrow_payload(batch: &RecordBatch) -> Vec<u8> {
    let mut payload = Vec::new();
    let mut writer = StreamWriter::try_new(&mut payload, &batch.schema()).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    payload
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
                    }
                }
                ControlRequest::Heartbeat(HeartbeatRequest { process_id }) => {
                    assert_eq!(process_id, "proc_1");
                    ControlResponse::HeartbeatAccepted {
                        process_id: "proc_1".to_string(),
                    }
                }
                ControlRequest::RegisterStream(RegisterStreamRequest {
                    stream_name,
                    buffer_size,
                    frame_size,
                }) => {
                    assert_eq!(stream_name, "ticks");
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
                ControlRequest::RegisterEngine(RegisterEngineRequest {
                    engine_name,
                    engine_type,
                    input_stream,
                    output_stream,
                    sink_names,
                    ..
                }) => {
                    assert_eq!(engine_type, "reactive");
                    assert_eq!(input_stream, "openctp_ticks");
                    assert_eq!(output_stream, "openctp_mid_price_factors");
                    assert_eq!(sink_names, vec!["factor_sink".to_string()]);
                    ControlResponse::EngineRegistered { engine_name }
                }
                ControlRequest::RegisterSink(RegisterSinkRequest {
                    sink_name,
                    sink_type,
                    input_stream,
                    ..
                }) => {
                    assert_eq!(sink_type, "parquet");
                    assert_eq!(input_stream, "openctp_mid_price_factors");
                    ControlResponse::SinkRegistered { sink_name }
                }
                ControlRequest::UpdateStatus(UpdateRecordStatusRequest {
                    kind,
                    name,
                    status,
                    metrics,
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
                ControlRequest::WriteTo(AttachStreamRequest {
                    stream_name,
                    process_id,
                    instrument_ids: _,
                }) => ControlResponse::WriterAttached {
                    descriptor: WriterDescriptor {
                        stream_name,
                        buffer_size: 1024,
                        frame_size: 256,
                        layout_version: BUS_LAYOUT_VERSION,
                        shm_name: "shm_ticks".to_string(),
                        writer_id: "ticks_writer".to_string(),
                        process_id,
                        next_write_seq: 1,
                    },
                },
                ControlRequest::ReadFrom(AttachStreamRequest {
                    stream_name,
                    process_id,
                    instrument_ids,
                }) => ControlResponse::ReaderAttached {
                    descriptor: ReaderDescriptor {
                        stream_name,
                        buffer_size: 1024,
                        frame_size: 256,
                        layout_version: BUS_LAYOUT_VERSION,
                        shm_name: "shm_ticks".to_string(),
                        reader_id: "reader_1".to_string(),
                        process_id,
                        next_read_seq: 1,
                        instrument_filter: instrument_ids,
                    },
                },
                ControlRequest::CloseWriter(DetachWriterRequest {
                    stream_name,
                    writer_id,
                    ..
                }) => ControlResponse::WriterDetached {
                    stream_name,
                    writer_id,
                },
                ControlRequest::CloseReader(DetachReaderRequest {
                    stream_name,
                    reader_id,
                    ..
                }) => ControlResponse::ReaderDetached {
                    stream_name,
                    reader_id,
                },
                ControlRequest::ListStreams(_) => {
                    ControlResponse::StreamsListed(zippy_core::ListStreamsResponse {
                        streams: vec![StreamInfo {
                            stream_name: "ticks".to_string(),
                            buffer_size: 1024,
                            frame_size: 256,
                            write_seq: 42,
                            writer_process_id: None,
                            reader_count: 0,
                            status: "registered".to_string(),
                        }],
                    })
                }
                ControlRequest::GetStream(_) => {
                    ControlResponse::StreamFetched(zippy_core::GetStreamResponse {
                        stream: StreamInfo {
                            stream_name: "ticks".to_string(),
                            buffer_size: 1024,
                            frame_size: 256,
                            write_seq: 42,
                            writer_process_id: None,
                            reader_count: 0,
                            status: "registered".to_string(),
                        },
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
fn master_client_registers_process_and_fetches_writer_descriptor() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 4);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    let process_id = client.register_process("local_dc").unwrap();
    assert_eq!(process_id, "proc_1");
    assert_eq!(client.process_id(), Some("proc_1"));

    client
        .register_stream("ticks", empty_schema(), 1024, 256)
        .unwrap();
    let writer = client.write_to("ticks").unwrap();
    let reader = client.read_from("ticks").unwrap();

    assert_eq!(writer.descriptor().stream_name, "ticks");
    assert_eq!(writer.descriptor().process_id, "proc_1");
    assert_eq!(reader.descriptor().stream_name, "ticks");
    assert_eq!(reader.descriptor().process_id, "proc_1");

    server.join().unwrap();
}

#[test]
fn master_client_sends_instrument_filter_when_attaching_reader() {
    let socket_path = unique_socket_path();
    let server = thread::spawn({
        let socket_path = socket_path.clone();
        move || {
            if socket_path.exists() {
                let _ = fs::remove_file(&socket_path);
            }

            let listener = UnixListener::bind(&socket_path).unwrap();
            for stream in listener.incoming().take(2) {
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
                        }
                    }
                    ControlRequest::ReadFrom(AttachStreamRequest {
                        stream_name,
                        process_id,
                        instrument_ids,
                    }) => {
                        assert_eq!(stream_name, "ticks");
                        assert_eq!(process_id, "proc_1");
                        assert_eq!(
                            instrument_ids,
                            Some(vec!["IF2606".to_string(), "IH2606".to_string()])
                        );
                        ControlResponse::ReaderAttached {
                            descriptor: ReaderDescriptor {
                                stream_name,
                                buffer_size: 1024,
                                frame_size: 256,
                                layout_version: BUS_LAYOUT_VERSION,
                                shm_name: "shm_ticks".to_string(),
                                reader_id: "reader_1".to_string(),
                                process_id,
                                next_read_seq: 1,
                                instrument_filter: Some(vec![
                                    "IF2606".to_string(),
                                    "IH2606".to_string(),
                                ]),
                            },
                        }
                    }
                    other => panic!("unexpected request: {other:?}"),
                };

                let payload = serde_json::to_string(&response).unwrap();
                stream.write_all(payload.as_bytes()).unwrap();
                stream.write_all(b"\n").unwrap();
                stream.flush().unwrap();
            }

            let _ = fs::remove_file(&socket_path);
        }
    });
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    let reader = client
        .read_from_filtered("ticks", vec!["IF2606".to_string(), "IH2606".to_string()])
        .unwrap();

    assert_eq!(
        reader.descriptor().instrument_filter,
        Some(vec!["IF2606".to_string(), "IH2606".to_string()])
    );

    server.join().unwrap();
}

#[test]
fn master_client_normalizes_empty_instrument_filter_when_attaching_reader() {
    let socket_path = unique_socket_path();
    let server = thread::spawn({
        let socket_path = socket_path.clone();
        move || {
            if socket_path.exists() {
                let _ = fs::remove_file(&socket_path);
            }

            let listener = UnixListener::bind(&socket_path).unwrap();
            for stream in listener.incoming().take(2) {
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
                        }
                    }
                    ControlRequest::ReadFrom(AttachStreamRequest {
                        stream_name,
                        process_id,
                        instrument_ids,
                    }) => {
                        assert_eq!(stream_name, "ticks");
                        assert_eq!(process_id, "proc_1");
                        assert_eq!(instrument_ids, None);
                        ControlResponse::ReaderAttached {
                            descriptor: ReaderDescriptor {
                                stream_name,
                                buffer_size: 1024,
                                frame_size: 256,
                                layout_version: BUS_LAYOUT_VERSION,
                                shm_name: "shm_ticks".to_string(),
                                reader_id: "reader_1".to_string(),
                                process_id,
                                next_read_seq: 1,
                                instrument_filter: None,
                            },
                        }
                    }
                    other => panic!("unexpected request: {other:?}"),
                };

                let payload = serde_json::to_string(&response).unwrap();
                stream.write_all(payload.as_bytes()).unwrap();
                stream.write_all(b"\n").unwrap();
                stream.flush().unwrap();
            }

            let _ = fs::remove_file(&socket_path);
        }
    });
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    let reader = client.read_from_filtered("ticks", Vec::new()).unwrap();

    assert_eq!(reader.descriptor().instrument_filter, None);

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
fn master_client_registers_control_plane_entities_and_updates_status() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 5);
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
            vec!["factor_sink".to_string()],
            serde_json::json!({
                "id_filter": ["IF2606"],
            }),
        )
        .unwrap();
    client
        .register_sink(
            "factor_sink",
            "parquet",
            "openctp_mid_price_factors",
            serde_json::json!({
                "path": "data/openctp_mid_price_factors",
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
fn control_request_serialization_uses_buffer_and_frame_sizes() {
    let request = ControlRequest::RegisterStream(RegisterStreamRequest {
        stream_name: "ticks".to_string(),
        buffer_size: 1024,
        frame_size: 256,
    });

    let json = serde_json::to_string(&request).unwrap();

    assert!(json.contains("\"buffer_size\":1024"));
    assert!(json.contains("\"frame_size\":256"));
    assert!(!json.contains("ring_capacity"));
}

#[test]
fn attach_stream_request_omits_filter_fields_when_not_set() {
    let request = ControlRequest::ReadFrom(AttachStreamRequest {
        stream_name: "ticks".to_string(),
        process_id: "proc_1".to_string(),
        instrument_ids: None,
    });
    let response = ControlResponse::ReaderAttached {
        descriptor: ReaderDescriptor {
            stream_name: "ticks".to_string(),
            buffer_size: 1024,
            frame_size: 256,
            layout_version: BUS_LAYOUT_VERSION,
            shm_name: "shm_ticks".to_string(),
            reader_id: "reader_1".to_string(),
            process_id: "proc_1".to_string(),
            next_read_seq: 1,
            instrument_filter: None,
        },
    };

    let request_json = serde_json::to_string(&request).unwrap();
    let response_json = serde_json::to_string(&response).unwrap();

    assert!(!request_json.contains("instrument_ids"));
    assert!(!response_json.contains("instrument_filter"));
    assert!(!request_json.contains(":null"));
    assert!(!response_json.contains(":null"));
}

#[test]
fn control_response_display_uses_buffer_and_frame_sizes() {
    let writer_output = format!(
        "{}",
        ControlResponse::WriterAttached {
            descriptor: WriterDescriptor {
                stream_name: "ticks".to_string(),
                buffer_size: 1024,
                frame_size: 256,
                layout_version: BUS_LAYOUT_VERSION,
                shm_name: "shm_ticks".to_string(),
                writer_id: "writer_1".to_string(),
                process_id: "proc_1".to_string(),
                next_write_seq: 1,
            },
        }
    );
    assert!(writer_output.contains("buffer_size=[1024]"));
    assert!(writer_output.contains("frame_size=[256]"));
    assert!(!writer_output.contains("ring_capacity"));

    let stream_output = format!(
        "{}",
        ControlResponse::StreamFetched(zippy_core::GetStreamResponse {
            stream: StreamInfo {
                stream_name: "ticks".to_string(),
                buffer_size: 1024,
                frame_size: 256,
                write_seq: 42,
                writer_process_id: None,
                reader_count: 0,
                status: "registered".to_string(),
            },
        })
    );
    assert!(stream_output.contains("buffer_size=[1024]"));
    assert!(stream_output.contains("frame_size=[256]"));
    assert!(stream_output.contains("write_seq=[42]"));
    assert!(!stream_output.contains("ring_capacity"));
}

#[test]
fn stream_info_serialization_includes_write_seq() {
    let stream = StreamInfo {
        stream_name: "ticks".to_string(),
        buffer_size: 1024,
        frame_size: 256,
        write_seq: 42,
        writer_process_id: None,
        reader_count: 0,
        status: "registered".to_string(),
    };

    let json = serde_json::to_string(&stream).unwrap();

    assert!(json.contains("\"write_seq\":42"));
    assert!(json.contains("\"buffer_size\":1024"));
    assert!(json.contains("\"frame_size\":256"));
    assert!(!json.contains("ring_capacity"));
}

#[test]
fn writer_and_reader_hot_path_do_not_create_seq_ipc_files() {
    let socket_path = unique_socket_path();
    let shm_dir = std::env::temp_dir().join(format!(
        "zippy-master-client-frame-ring-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&shm_dir).unwrap();
    let flink_path = shm_dir.join("ticks.flink");
    let shm_name = flink_path.to_string_lossy().into_owned();
    let shm_name_for_server = shm_name.clone();

    let socket_path_for_server = socket_path.clone();
    let server = thread::spawn(move || {
        if socket_path_for_server.exists() {
            let _ = fs::remove_file(&socket_path_for_server);
        }

        let listener = UnixListener::bind(&socket_path_for_server).unwrap();
        for stream in listener.incoming().take(4) {
            let mut stream = stream.unwrap();
            let mut line = String::new();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            reader.read_line(&mut line).unwrap();

            let request: ControlRequest = serde_json::from_str(line.trim_end()).unwrap();
            let response = match request {
                ControlRequest::RegisterProcess(_) => ControlResponse::ProcessRegistered {
                    process_id: "proc_1".to_string(),
                },
                ControlRequest::RegisterStream(RegisterStreamRequest { stream_name, .. }) => {
                    ControlResponse::StreamRegistered { stream_name }
                }
                ControlRequest::WriteTo(AttachStreamRequest {
                    stream_name,
                    process_id,
                    instrument_ids,
                }) => {
                    assert_eq!(instrument_ids, None);
                    ControlResponse::WriterAttached {
                        descriptor: WriterDescriptor {
                            stream_name,
                            buffer_size: 4,
                            frame_size: 4096,
                            layout_version: BUS_LAYOUT_VERSION,
                            shm_name: shm_name_for_server.clone(),
                            writer_id: "ticks_writer".to_string(),
                            process_id,
                            next_write_seq: 1,
                        },
                    }
                }
                ControlRequest::ReadFrom(AttachStreamRequest {
                    stream_name,
                    process_id,
                    instrument_ids,
                }) => {
                    assert_eq!(instrument_ids, None);
                    ControlResponse::ReaderAttached {
                        descriptor: ReaderDescriptor {
                            stream_name,
                            buffer_size: 4,
                            frame_size: 4096,
                            layout_version: BUS_LAYOUT_VERSION,
                            shm_name: shm_name_for_server.clone(),
                            reader_id: "reader_1".to_string(),
                            process_id,
                            next_read_seq: 1,
                            instrument_filter: None,
                        },
                    }
                }
                _ => panic!("unexpected request"),
            };

            let payload = serde_json::to_string(&response).unwrap();
            stream.write_all(payload.as_bytes()).unwrap();
            stream.write_all(b"\n").unwrap();
            stream.flush().unwrap();
        }

        let _ = fs::remove_file(&socket_path_for_server);
    });

    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    client
        .register_stream("ticks", empty_schema(), 4, 4096)
        .unwrap();

    let mut writer = client.write_to("ticks").unwrap();
    let mut reader = client.read_from("ticks").unwrap();

    let batch = arrow::record_batch::RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "instrument_id",
            arrow::datatypes::DataType::Utf8,
            false,
        )])),
        vec![std::sync::Arc::new(StringArray::from(vec!["IF2606"]))],
    )
    .unwrap();
    writer.write(batch.clone()).unwrap();
    let received = reader.read(Some(1000)).unwrap();

    assert_eq!(received.num_rows(), batch.num_rows());
    let entry_names = fs::read_dir(&shm_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        entry_names.iter().all(|name| !name.starts_with("seq_")),
        "seq_*.ipc files should not be created path=[{}] entries={entry_names:?}",
        shm_dir.display()
    );
    assert!(
        entry_names.iter().any(|name| name == "ticks.flink"),
        "shared memory flink should exist path=[{}] entries={entry_names:?}",
        shm_dir.display()
    );

    server.join().unwrap();
    let _ = fs::remove_dir_all(shm_dir);
}

#[test]
fn filtered_reader_rejects_legacy_frame_payloads() {
    let socket_path = unique_socket_path();
    let shm_dir = std::env::temp_dir().join(format!(
        "zippy-master-client-legacy-frame-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&shm_dir).unwrap();
    let flink_path = shm_dir.join("ticks.flink");
    let shm_name = flink_path.to_string_lossy().into_owned();
    let shm_name_for_server = shm_name.clone();

    let socket_path_for_server = socket_path.clone();
    let server = thread::spawn(move || {
        if socket_path_for_server.exists() {
            let _ = fs::remove_file(&socket_path_for_server);
        }

        let listener = UnixListener::bind(&socket_path_for_server).unwrap();
        for stream in listener.incoming().take(3) {
            let mut stream = stream.unwrap();
            let mut line = String::new();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            reader.read_line(&mut line).unwrap();

            let request: ControlRequest = serde_json::from_str(line.trim_end()).unwrap();
            let response = match request {
                ControlRequest::RegisterProcess(_) => ControlResponse::ProcessRegistered {
                    process_id: "proc_1".to_string(),
                },
                ControlRequest::ReadFrom(AttachStreamRequest {
                    stream_name,
                    process_id,
                    instrument_ids,
                }) => {
                    assert_eq!(stream_name, "ticks");
                    assert_eq!(process_id, "proc_1");
                    assert_eq!(instrument_ids, Some(vec!["IF2606".to_string()]));
                    ControlResponse::ReaderAttached {
                        descriptor: ReaderDescriptor {
                            stream_name,
                            buffer_size: 4,
                            frame_size: 4096,
                            layout_version: BUS_LAYOUT_VERSION,
                            shm_name: shm_name_for_server.clone(),
                            reader_id: "reader_1".to_string(),
                            process_id,
                            next_read_seq: 1,
                            instrument_filter: Some(vec!["IF2606".to_string()]),
                        },
                    }
                }
                ControlRequest::CloseReader(DetachReaderRequest {
                    stream_name,
                    reader_id,
                    ..
                }) => ControlResponse::ReaderDetached {
                    stream_name,
                    reader_id,
                },
                other => panic!("unexpected request: {other:?}"),
            };

            let payload = serde_json::to_string(&response).unwrap();
            stream.write_all(payload.as_bytes()).unwrap();
            stream.write_all(b"\n").unwrap();
            stream.flush().unwrap();
        }

        let _ = fs::remove_file(&socket_path_for_server);
    });

    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    let mut reader = client
        .read_from_filtered("ticks", vec!["IF2606".to_string()])
        .unwrap();

    let mut ring = SharedFrameRing::create_or_open(&shm_name, 4, 4096).unwrap();
    let batch = RecordBatch::try_new(
        instrument_schema(),
        vec![std::sync::Arc::new(StringArray::from(vec!["IF2606"]))],
    )
    .unwrap();
    ring.publish(&encode_arrow_payload(&batch)).unwrap();

    let error = reader.read(Some(1000)).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("filtered reader requires enveloped bus frame"),
        "unexpected error: {error}"
    );

    reader.close().unwrap();
    server.join().unwrap();
    let _ = fs::remove_dir_all(shm_dir);
}
