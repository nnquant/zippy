use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde_json::json;
use zippy_core::{ControlEndpoint, MasterClient};
use zippy_gateway::{GatewayServer, GatewayServerConfig};
use zippy_master::server::MasterServer;

#[test]
fn native_gateway_accepts_arrow_write_batch_and_publishes_descriptor() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("native-gateway-master.sock");
    let master_endpoint = ControlEndpoint::Unix(socket_path.clone());
    let (master, master_thread) = spawn_master(master_endpoint.clone());
    let gateway_endpoint = format!("127.0.0.1:{}", reserve_tcp_port());
    let gateway = GatewayServer::new(GatewayServerConfig {
        endpoint: gateway_endpoint.clone(),
        master_endpoint,
        token: Some("dev-token".to_string()),
        max_write_rows: Some(1024),
    })
    .unwrap()
    .start()
    .unwrap();

    let batch = RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("instrument_id", DataType::Utf8, false),
            Field::new("last_price", DataType::Float64, false),
        ])),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["IF2606"])),
            std::sync::Arc::new(Float64Array::from(vec![4102.5])),
        ],
    )
    .unwrap();
    let response = send_gateway_frame(
        gateway.endpoint(),
        json!({
            "kind": "write_batch",
            "stream_name": "native_gateway_ticks",
            "token": "dev-token",
            "rows": 1
        }),
        encode_ipc_batch(&batch),
    );

    assert_eq!(response["status"], "ok");

    let metrics = gateway.metrics();
    assert_eq!(metrics["write_batches_total"], json!(1));
    assert_eq!(metrics["written_rows_total"], json!(1));

    let mut client = MasterClient::connect(socket_path).unwrap();
    client.register_process("native_gateway_test").unwrap();
    let stream = client.get_stream("native_gateway_ticks").unwrap();
    assert_eq!(stream.stream_name, "native_gateway_ticks");
    assert!(stream.active_segment_descriptor.is_some());

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

fn spawn_master(
    endpoint: ControlEndpoint,
) -> (MasterServer, thread::JoinHandle<zippy_core::Result<()>>) {
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let server = MasterServer::default();
    let server_for_thread = server.clone();
    let handle = thread::spawn(move || {
        server_for_thread.serve_endpoint_with_ready(&endpoint, Some(ready_tx))
    });
    ready_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    (server, handle)
}

fn reserve_tcp_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn encode_ipc_batch(batch: &RecordBatch) -> Vec<u8> {
    let mut payload = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    payload
}

fn send_gateway_frame(
    endpoint: &str,
    header: serde_json::Value,
    payload: Vec<u8>,
) -> serde_json::Value {
    let mut stream = TcpStream::connect(endpoint).unwrap();
    let header_bytes = serde_json::to_vec(&header).unwrap();
    stream
        .write_all(&(header_bytes.len() as u32).to_be_bytes())
        .unwrap();
    stream
        .write_all(&(payload.len() as u64).to_be_bytes())
        .unwrap();
    stream.write_all(&header_bytes).unwrap();
    stream.write_all(&payload).unwrap();

    let mut prefix = [0u8; 12];
    stream.read_exact(&mut prefix).unwrap();
    let header_len = u32::from_be_bytes(prefix[0..4].try_into().unwrap()) as usize;
    let payload_len = u64::from_be_bytes(prefix[4..12].try_into().unwrap()) as usize;
    let mut response_header = vec![0u8; header_len];
    stream.read_exact(&mut response_header).unwrap();
    if payload_len > 0 {
        let mut response_payload = vec![0u8; payload_len];
        stream.read_exact(&mut response_payload).unwrap();
    }
    serde_json::from_slice(&response_header).unwrap()
}
