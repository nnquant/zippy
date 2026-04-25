use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{MasterClient, SchemaRef};
use zippy_master::server::MasterServer;

fn smoke_schema() -> SchemaRef {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("mid_price", DataType::Float64, false),
    ]))
}

fn smoke_batch() -> RecordBatch {
    RecordBatch::try_new(
        smoke_schema(),
        vec![
            std::sync::Arc::new(StringArray::from(vec![
                "IF2606", "IH2606", "TL2606", "IC2606",
            ])),
            std::sync::Arc::new(Float64Array::from(vec![3210.5, 2987.0, 120.25, 5867.5])),
        ],
    )
    .unwrap()
}

fn batch_for_instrument(instrument_id: &str) -> RecordBatch {
    let mid_price = match instrument_id {
        "IF2606" => 3210.5,
        "IH2606" => 2987.0,
        "TL2606" => 120.25,
        "IC2606" => 5867.5,
        other => panic!("unexpected instrument_id=[{}]", other),
    };

    RecordBatch::try_new(
        smoke_schema(),
        vec![
            std::sync::Arc::new(StringArray::from(vec![instrument_id])),
            std::sync::Arc::new(Float64Array::from(vec![mid_price])),
        ],
    )
    .unwrap()
}

fn unique_socket_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("zippy-master-bus-perf-{nanos}.sock"))
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

fn spawn_master_server(socket_path: &Path) -> (MasterServer, thread::JoinHandle<()>) {
    let server = MasterServer::default();
    let handle_server = server.clone();
    let socket_path = socket_path.to_path_buf();
    let wait_path = socket_path.clone();
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let join_handle = thread::spawn(move || {
        let result = handle_server.serve_with_ready(&socket_path, Some(ready_tx));
        result.unwrap();
    });
    match ready_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("master server failed to start error=[{}]", error),
        Err(error) => panic!("master server did not report readiness error=[{}]", error),
    }
    wait_for_socket_ready(&wait_path);
    (server, join_handle)
}

#[test]
fn master_bus_perf_smoke_roundtrips_batches() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_master_server(&socket_path);

    let mut writer_client = MasterClient::connect(&socket_path).unwrap();
    let mut reader_client = MasterClient::connect(&socket_path).unwrap();
    writer_client
        .register_process("master_bus_perf_writer")
        .unwrap();
    reader_client
        .register_process("master_bus_perf_reader")
        .unwrap();

    let stream_name = format!(
        "smoke_ticks_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    writer_client
        .register_stream(&stream_name, smoke_schema(), 131072, 65536)
        .unwrap();

    let mut writer = writer_client.write_to(&stream_name).unwrap();
    let mut reader = reader_client.read_from(&stream_name).unwrap();
    let batch = smoke_batch();
    let rounds = 1000usize;

    let start = Instant::now();
    for _ in 0..rounds {
        writer.write(batch.clone()).unwrap();
        let received = reader.read(Some(1000)).unwrap();
        assert_eq!(received.num_rows(), batch.num_rows());
        assert_eq!(received.num_columns(), batch.num_columns());
        assert_eq!(format!("{received:?}"), format!("{batch:?}"));
    }
    let elapsed = start.elapsed();

    eprintln!(
        "master bus perf smoke rounds=[{}] elapsed_ms=[{}] avg_us_per_round=[{}]",
        rounds,
        elapsed.as_millis(),
        elapsed.as_micros() as f64 / rounds as f64
    );

    reader.close().unwrap();
    writer.close().unwrap();
    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}

#[test]
#[ignore = "perf smoke"]
fn master_bus_perf_smoke_filtered_reader_skips_non_matching_batches() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_master_server(&socket_path);

    let mut writer_client = MasterClient::connect(&socket_path).unwrap();
    let mut reader_client = MasterClient::connect(&socket_path).unwrap();
    writer_client.register_process("perf_writer").unwrap();
    reader_client.register_process("perf_reader").unwrap();
    let rounds = 5_000usize;

    let stream_name = format!(
        "perf_ticks_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    writer_client
        .register_stream(&stream_name, smoke_schema(), rounds, 2048)
        .unwrap();

    let mut writer = writer_client.write_to(&stream_name).unwrap();
    let mut reader = reader_client
        .read_from_filtered(&stream_name, vec!["IF2606".to_string()])
        .unwrap();

    let expected_matches = rounds / 100;
    let expected_batch = batch_for_instrument("IF2606");
    let start = Instant::now();
    for round in 0..rounds {
        let instrument_id = if round % 100 == 0 { "IF2606" } else { "IH2606" };
        writer.write(batch_for_instrument(instrument_id)).unwrap();
    }

    let mut observed_matches = 0usize;
    for _ in 0..expected_matches {
        let received = reader.read(Some(1_000)).unwrap();
        assert_eq!(received.num_rows(), expected_batch.num_rows());
        assert_eq!(received.num_columns(), expected_batch.num_columns());
        assert_eq!(format!("{received:?}"), format!("{expected_batch:?}"));
        observed_matches += 1;
    }

    eprintln!(
        "filtered reader perf smoke rounds=[{}] matches=[{}] elapsed_ms=[{}]",
        rounds,
        observed_matches,
        start.elapsed().as_millis(),
    );

    assert_eq!(observed_matches, expected_matches);

    reader.close().unwrap();
    writer.close().unwrap();
    server.shutdown();
    join_handle.join().unwrap();
    let _ = fs::remove_file(socket_path);
}
