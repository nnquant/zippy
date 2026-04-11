use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{MasterClient, SchemaRef};
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
    let join_handle = thread::spawn(move || {
        handle_server.serve(&socket_path).unwrap();
    });
    thread::sleep(Duration::from_millis(50));
    (server, join_handle)
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
