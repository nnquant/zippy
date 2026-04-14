use std::fs;
use std::path::{Path, PathBuf};
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
    let join_handle = thread::spawn(move || {
        handle_server.serve(&socket_path).unwrap();
    });
    wait_for_socket_ready(&wait_path);
    (server, join_handle)
}

#[test]
fn master_bus_perf_smoke_roundtrips_batches() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_master_server(&socket_path);

    let mut writer_client = MasterClient::connect(&socket_path).unwrap();
    let mut reader_client = MasterClient::connect(&socket_path).unwrap();
    writer_client.register_process("master_bus_perf_writer").unwrap();
    reader_client.register_process("master_bus_perf_reader").unwrap();

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
