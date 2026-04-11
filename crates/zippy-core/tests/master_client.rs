use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::datatypes::Schema;
use zippy_core::{
    AttachStreamRequest, ControlRequest, ControlResponse, MasterClient, ReaderDescriptor,
    RegisterProcessRequest, RegisterStreamRequest, SchemaRef, StreamInfo, WriterDescriptor,
    BUS_LAYOUT_VERSION,
};

fn empty_schema() -> SchemaRef {
    std::sync::Arc::new(Schema::empty())
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
                ControlRequest::RegisterProcess(RegisterProcessRequest { .. }) => {
                    ControlResponse::ProcessRegistered {
                        process_id: "proc_1".to_string(),
                    }
                }
                ControlRequest::RegisterStream(RegisterStreamRequest { stream_name, .. }) => {
                    ControlResponse::StreamRegistered { stream_name }
                }
                ControlRequest::WriteTo(AttachStreamRequest {
                    stream_name,
                    process_id,
                }) => ControlResponse::WriterAttached {
                    descriptor: WriterDescriptor {
                        stream_name,
                        ring_capacity: 1024,
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
                }) => ControlResponse::ReaderAttached {
                    descriptor: ReaderDescriptor {
                        stream_name,
                        ring_capacity: 1024,
                        layout_version: BUS_LAYOUT_VERSION,
                        shm_name: "shm_ticks".to_string(),
                        reader_id: "reader_1".to_string(),
                        process_id,
                        next_read_seq: 1,
                    },
                },
                ControlRequest::ListStreams(_) => {
                    ControlResponse::StreamsListed(zippy_core::ListStreamsResponse {
                        streams: vec![StreamInfo {
                            stream_name: "ticks".to_string(),
                            ring_capacity: 1024,
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
                            ring_capacity: 1024,
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
        .register_stream("ticks", empty_schema(), 1024)
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
fn master_client_lists_streams() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 3);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("writer").unwrap();
    client
        .register_stream("ticks", empty_schema(), 1024)
        .unwrap();

    let streams = client.list_streams().unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].stream_name, "ticks");
    assert_eq!(streams[0].ring_capacity, 1024);
    assert_eq!(streams[0].status, "registered");

    server.join().unwrap();
}

#[test]
fn master_client_gets_single_stream() {
    let socket_path = unique_socket_path();
    let server = spawn_fake_server(&socket_path, 3);
    wait_for_socket(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("writer").unwrap();
    client
        .register_stream("ticks", empty_schema(), 1024)
        .unwrap();

    let stream = client.get_stream("ticks").unwrap();
    assert_eq!(stream.stream_name, "ticks");
    assert_eq!(stream.ring_capacity, 1024);
    assert_eq!(stream.status, "registered");

    server.join().unwrap();
}
