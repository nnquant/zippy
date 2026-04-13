use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{MasterClient, SchemaRef};
use zippy_master::bus::Bus;
use zippy_master::ring::{ReadError, RingWriteError, StreamRing};
use zippy_master::server::MasterServer;

fn test_schema() -> SchemaRef {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("mid_price", DataType::Float64, false),
    ]))
}

fn test_batch() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["IF2606", "IH2606"])),
            std::sync::Arc::new(Float64Array::from(vec![3210.5, 2987.0])),
        ],
    )
    .unwrap()
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

fn wait_for_socket_ready(socket_path: &Path) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
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

fn send_request(socket_path: &Path, payload: &str) -> String {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    let mut stream = UnixStream::connect(socket_path).unwrap();
    stream.write_all(payload.as_bytes()).unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

#[test]
fn frame_ring_new_reader_on_existing_stream_starts_after_latest_frame() {
    let mut ring = StreamRing::new(4, 1024).unwrap();
    ring.publish_frame(b"a").unwrap();
    ring.publish_frame(b"b").unwrap();

    let reader = ring.attach_reader_at_latest();
    assert_eq!(reader.next_read_seq, 3);
    assert!(ring.read_frames(&reader.reader_id).unwrap().is_empty());

    ring.publish_frame(b"c").unwrap();

    let frames = ring.read_frames(&reader.reader_id).unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].seq, 3);
    assert_eq!(frames[0].bytes, b"c");
}

#[test]
fn frame_ring_reports_lagged_reader_after_overwrite() {
    let mut ring = StreamRing::new(2, 1024).unwrap();
    let reader = ring.attach_reader();
    ring.publish_frame(b"a").unwrap();
    ring.publish_frame(b"b").unwrap();
    ring.publish_frame(b"c").unwrap();

    let error = ring.read_frames(&reader.reader_id).unwrap_err();
    assert!(matches!(error, ReadError::ReaderLagged(_)));
}

#[test]
fn frame_ring_rejects_frame_larger_than_frame_size() {
    let mut ring = StreamRing::new(2, 2).unwrap();

    let error = ring.publish_frame(b"abc").unwrap_err();
    assert!(matches!(
        error,
        RingWriteError::FrameTooLarge {
            frame_len: 3,
            frame_size: 2,
        }
    ));
}

#[test]
fn writer_and_reader_roundtrip_batches_through_master_bus() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut client_a = MasterClient::connect(&socket_path).unwrap();
    let mut client_b = MasterClient::connect(&socket_path).unwrap();
    client_a.register_process("writer").unwrap();
    client_b.register_process("reader").unwrap();
    client_a
        .register_stream("ticks", test_schema(), 64)
        .unwrap();

    let batch = test_batch();
    let mut writer = client_a.write_to("ticks").unwrap();
    let mut reader = client_b.read_from("ticks").unwrap();

    writer.write(batch.clone()).unwrap();
    let received = reader.read(Some(1000)).unwrap();
    assert_eq!(received.num_rows(), batch.num_rows());
    assert_eq!(received.num_columns(), batch.num_columns());
    assert_eq!(format!("{received:?}"), format!("{batch:?}"));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn closing_writer_allows_restarting_same_stream_writer() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("writer").unwrap();
    client.register_stream("ticks", test_schema(), 64).unwrap();

    let mut first_writer = client.write_to("ticks").unwrap();
    first_writer.close().unwrap();

    let second_writer = client.write_to("ticks").unwrap();
    assert_eq!(second_writer.descriptor().stream_name, "ticks");

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn expired_process_writer_is_reclaimed_for_new_writer() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut first = MasterClient::connect(&socket_path).unwrap();
    first.register_process("writer_a").unwrap();
    first.register_stream("ticks", test_schema(), 64).unwrap();
    let _writer = first.write_to("ticks").unwrap();

    let response = send_request(
        &socket_path,
        "{\"ExpireProcessForTest\":{\"process_id\":\"proc_1\"}}\n",
    );
    assert!(response.contains("proc_1"));

    let mut second = MasterClient::connect(&socket_path).unwrap();
    second.register_process("writer_b").unwrap();
    second.register_stream("ticks", test_schema(), 64).unwrap();
    let second_writer = second.write_to("ticks").unwrap();
    assert_eq!(second_writer.descriptor().process_id, "proc_2");

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn lease_reaper_reclaims_expired_writer_for_new_writer() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server_with_lease(
        &socket_path,
        Duration::from_millis(50),
        Duration::from_millis(10),
    );

    let mut first = MasterClient::connect(&socket_path).unwrap();
    first.register_process("writer_a").unwrap();
    first.register_stream("ticks", test_schema(), 64).unwrap();
    let _writer = first.write_to("ticks").unwrap();

    thread::sleep(Duration::from_millis(120));

    let mut second = MasterClient::connect(&socket_path).unwrap();
    second.register_process("writer_b").unwrap();
    second.register_stream("ticks", test_schema(), 64).unwrap();
    let second_writer = second.write_to("ticks").unwrap();
    assert_eq!(second_writer.descriptor().process_id, "proc_2");

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn lease_reaper_reclaims_expired_reader_attachments() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server_with_lease(
        &socket_path,
        Duration::from_millis(50),
        Duration::from_millis(10),
    );

    let mut writer = MasterClient::connect(&socket_path).unwrap();
    writer.register_process("writer").unwrap();
    writer.register_stream("ticks", test_schema(), 64).unwrap();
    let _writer = writer.write_to("ticks").unwrap();

    let mut reader = MasterClient::connect(&socket_path).unwrap();
    reader.register_process("reader").unwrap();
    let _reader = reader.read_from("ticks").unwrap();

    thread::sleep(Duration::from_millis(120));

    let stream_response = send_request(
        &socket_path,
        "{\"GetStream\":{\"stream_name\":\"ticks\"}}\n",
    );
    assert!(stream_response.contains("\"reader_count\":0"));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
fn bus_read_from_returns_latest_next_read_seq() {
    let mut bus = Bus::default();
    bus.ensure_stream_with_sizes("ticks", 4, 1024).unwrap();

    let writer = bus.write_to("ticks", "proc_1").unwrap();
    assert_eq!(writer.next_write_seq, 1);

    bus.publish_test_frame("ticks", b"abc").unwrap();

    let reader = bus.read_from("ticks", "proc_2").unwrap();
    assert_eq!(reader.next_read_seq, 2);
}
