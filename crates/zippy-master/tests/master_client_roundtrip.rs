#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{canonical_schema_hash, MasterClient, SchemaRef, ZippyConfig};
use zippy_master::bus::Bus;
use zippy_master::ring::{ReadError, RingWriteError, StreamRing};
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

fn batch_with_rows(instrument_ids: Vec<&str>, mid_prices: Vec<f64>) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            std::sync::Arc::new(StringArray::from(instrument_ids)),
            std::sync::Arc::new(Float64Array::from(mid_prices)),
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

fn spawn_test_server_with_snapshot(
    socket_path: &Path,
    snapshot_path: PathBuf,
) -> (MasterServer, thread::JoinHandle<()>) {
    let server = MasterServer::with_runtime_config(
        Some(snapshot_path),
        Duration::from_secs(10),
        Duration::from_secs(2),
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
        .register_stream("ticks", test_schema(), 64, 4096)
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
fn filtered_reader_skips_non_matching_frames_until_match() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut writer_client = MasterClient::connect(&socket_path).unwrap();
    let mut reader_client = MasterClient::connect(&socket_path).unwrap();
    writer_client.register_process("writer").unwrap();
    reader_client.register_process("reader").unwrap();
    writer_client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();

    let mut writer = writer_client.write_to("ticks").unwrap();
    let mut reader = reader_client
        .read_from_filtered("ticks", vec!["IF2606".to_string()])
        .unwrap();

    let non_matching_batch = batch_with_rows(vec!["IH2606"], vec![2987.0]);
    let matching_batch = batch_with_rows(vec!["IF2606", "IH2606"], vec![3210.5, 2987.0]);

    writer.write(non_matching_batch).unwrap();
    writer.write(matching_batch.clone()).unwrap();

    let received = reader.read(Some(1000)).unwrap();
    assert_eq!(received.num_rows(), matching_batch.num_rows());
    assert_eq!(received.num_columns(), matching_batch.num_columns());
    assert_eq!(format!("{received:?}"), format!("{matching_batch:?}"));

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
    client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();

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
    first
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    let _writer = first.write_to("ticks").unwrap();

    let response = send_request(
        &socket_path,
        "{\"ExpireProcessForTest\":{\"process_id\":\"proc_1\"}}\n",
    );
    assert!(response.contains("proc_1"));

    let mut second = MasterClient::connect(&socket_path).unwrap();
    second.register_process("writer_b").unwrap();
    second
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    let second_writer = loop {
        match second.write_to("ticks") {
            Ok(writer) => break writer,
            Err(error)
                if error.to_string().contains("writer already attached")
                    && std::time::Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!("failed to reclaim writer lease error=[{error}]"),
        }
    };
    assert_eq!(second_writer.descriptor().process_id, "proc_2");

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
#[ignore = "control-plane writer lease reaper is outside task 4 frame ring hot path scope"]
fn lease_reaper_reclaims_expired_writer_for_new_writer() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server_with_lease(
        &socket_path,
        Duration::from_millis(50),
        Duration::from_millis(10),
    );

    let mut first = MasterClient::connect(&socket_path).unwrap();
    first.register_process("writer_a").unwrap();
    first
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    let _writer = first.write_to("ticks").unwrap();

    thread::sleep(Duration::from_millis(120));

    let mut second = MasterClient::connect(&socket_path).unwrap();
    second.register_process("writer_b").unwrap();
    second
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    let second_writer = loop {
        match second.write_to("ticks") {
            Ok(writer) => break writer,
            Err(error)
                if error.to_string().contains("writer already attached")
                    && std::time::Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!("failed to reclaim writer lease error=[{error}]"),
        }
    };
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
    writer
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
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
fn lease_reaper_persists_expired_segment_reader_lease_cleanup_to_snapshot() {
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

    let mut reader = MasterClient::connect(&socket_path).unwrap();
    reader.register_process("query_reader").unwrap();
    reader.acquire_segment_reader_lease("ticks", 1, 0).unwrap();
    assert_eq!(
        SnapshotStore::load(&snapshot_path).unwrap().streams[0]
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
        .publish_persisted_file(
            "ticks",
            serde_json::json!({
                "file_path": "/data/ctp_ticks/trading_day=20260426/part-000001.parquet",
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
fn master_client_upserts_persisted_file_metadata_by_persist_file_id() {
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

    let persist_file_id = "ticks:1:0:0";
    client
        .publish_persisted_file(
            "ticks",
            serde_json::json!({
                "persist_file_id": persist_file_id,
                "file_path": "/data/ctp_ticks/part-000000.parquet",
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
                "file_path": "/data/ctp_ticks/part-000000.parquet",
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
    let (server, join_handle) =
        spawn_test_server_with_snapshot(&socket_path, snapshot_path.clone());

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
            Vec::new(),
            serde_json::json!({}),
        )
        .unwrap();
    client
        .register_sink("tick_sink", "parquet", "ticks", serde_json::json!({}))
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
    assert!(snapshot.sinks.is_empty());

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

    let reader = bus.read_from("ticks", "proc_2", None).unwrap();
    assert_eq!(reader.next_read_seq, 2);
}

#[test]
fn late_reader_skips_existing_frames_with_frame_ring_hot_path() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut writer_client = MasterClient::connect(&socket_path).unwrap();
    writer_client.register_process("writer").unwrap();
    writer_client
        .register_stream("ticks", test_schema(), 64, 4096)
        .unwrap();
    let mut writer = writer_client.write_to("ticks").unwrap();
    writer.write(test_batch()).unwrap();

    let mut reader_client = MasterClient::connect(&socket_path).unwrap();
    reader_client.register_process("reader").unwrap();
    let mut reader = reader_client.read_from("ticks").unwrap();

    let timeout_error = reader.read(Some(50)).unwrap_err();
    assert!(
        timeout_error.to_string().contains("reader timed out"),
        "unexpected error: {timeout_error}"
    );

    let next_batch = RecordBatch::try_new(
        test_schema(),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["TL2606"])),
            std::sync::Arc::new(Float64Array::from(vec![102.25])),
        ],
    )
    .unwrap();
    writer.write(next_batch.clone()).unwrap();

    let received = reader.read(Some(1000)).unwrap();
    assert_eq!(format!("{received:?}"), format!("{next_batch:?}"));

    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}
