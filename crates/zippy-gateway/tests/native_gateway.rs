use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde_json::json;
use zippy_core::{
    connect_control_endpoint, ControlEndpoint, Engine, MasterClient, SegmentTableView,
};
use zippy_engines::StreamTableMaterializer;
use zippy_gateway::{GatewayServer, GatewayServerConfig};
use zippy_master::server::MasterServer;

#[test]
fn native_gateway_accepts_arrow_write_batch_and_publishes_descriptor() {
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) = spawn_master(master_endpoint.clone());
    let gateway_endpoint = format!("127.0.0.1:{}", reserve_tcp_port());
    let gateway = GatewayServer::new(GatewayServerConfig {
        endpoint: gateway_endpoint.clone(),
        master_endpoint: master_endpoint.clone(),
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

    let mut client = MasterClient::connect_endpoint(master_endpoint).unwrap();
    client.register_process("native_gateway_test").unwrap();
    let stream = client.get_stream("native_gateway_ticks").unwrap();
    assert_eq!(stream.stream_name, "native_gateway_ticks");
    assert!(stream.active_segment_descriptor.is_some());

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_collects_existing_segment_stream() {
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) = spawn_master(master_endpoint.clone());
    let gateway_endpoint = format!("127.0.0.1:{}", reserve_tcp_port());
    let gateway = GatewayServer::new(GatewayServerConfig {
        endpoint: gateway_endpoint.clone(),
        master_endpoint: master_endpoint.clone(),
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

    let mut client = MasterClient::connect_endpoint(master_endpoint).unwrap();
    client.register_process("external_segment_writer").unwrap();
    client
        .register_stream("external_ticks", batch.schema(), 64, 4096)
        .unwrap();
    client
        .register_source("external_source", "test", "external_ticks", json!({}))
        .unwrap();
    let mut materializer = StreamTableMaterializer::new("external_ticks", batch.schema()).unwrap();
    client
        .publish_segment_descriptor_bytes(
            "external_ticks",
            &materializer.active_descriptor_envelope_bytes().unwrap(),
        )
        .unwrap();
    materializer
        .on_data(SegmentTableView::from_record_batch(batch))
        .unwrap();
    materializer.on_flush().unwrap();

    let (response, payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": "external_ticks",
            "token": "dev-token",
            "plan": [
                {"op": "select", "exprs": [{"kind": "col", "value": "instrument_id"}]}
            ]
        }),
        vec![],
    );

    assert_eq!(response["status"], "ok");
    let collected = decode_ipc_batch(&payload);
    assert_eq!(collected.num_rows(), 1);
    assert_eq!(collected.num_columns(), 1);
    let instruments = collected
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(instruments.value(0), "IF2606");

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_pushes_tail_collect_into_segment_reader() {
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) = spawn_master(master_endpoint.clone());
    let gateway_endpoint = format!("127.0.0.1:{}", reserve_tcp_port());
    let gateway = GatewayServer::new(GatewayServerConfig {
        endpoint: gateway_endpoint.clone(),
        master_endpoint: master_endpoint.clone(),
        token: Some("dev-token".to_string()),
        max_write_rows: Some(1024),
    })
    .unwrap()
    .start()
    .unwrap();

    let batch = RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Int64, false),
            Field::new("instrument_id", DataType::Utf8, false),
        ])),
        vec![
            std::sync::Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            std::sync::Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
        ],
    )
    .unwrap();

    let mut client = MasterClient::connect_endpoint(master_endpoint).unwrap();
    client.register_process("external_tail_writer").unwrap();
    client
        .register_stream("tail_external_ticks", batch.schema(), 2, 4096)
        .unwrap();
    client
        .register_source(
            "external_tail_source",
            "test",
            "tail_external_ticks",
            json!({}),
        )
        .unwrap();
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("tail_external_ticks", batch.schema(), 2)
            .unwrap();
    materializer
        .on_data(SegmentTableView::from_record_batch(batch))
        .unwrap();
    materializer.on_flush().unwrap();
    let descriptor_bytes = materializer.active_descriptor_envelope_bytes().unwrap();
    let mut descriptor: serde_json::Value = serde_json::from_slice(&descriptor_bytes).unwrap();
    let sealed_segments = descriptor["sealed_segments"].as_array_mut().unwrap();
    assert!(!sealed_segments.is_empty());
    sealed_segments[0]["shm_os_id"] = json!("/tmp/zippy-missing-sealed-segment");
    client
        .publish_segment_descriptor_bytes(
            "tail_external_ticks",
            &serde_json::to_vec(&descriptor).unwrap(),
        )
        .unwrap();

    let (response, payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": "tail_external_ticks",
            "token": "dev-token",
            "plan": [{"op": "tail", "n": 1}]
        }),
        vec![],
    );

    assert_eq!(response["status"], "ok");
    let collected = decode_ipc_batch(&payload);
    assert_eq!(collected.num_rows(), 1);
    let instrument_id = collected
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(instrument_id.value(0), "e");

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_applies_lazy_shape_collect_plan() {
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) = spawn_master(master_endpoint.clone());
    let gateway_endpoint = format!("127.0.0.1:{}", reserve_tcp_port());
    let gateway = GatewayServer::new(GatewayServerConfig {
        endpoint: gateway_endpoint.clone(),
        master_endpoint: master_endpoint.clone(),
        token: Some("dev-token".to_string()),
        max_write_rows: Some(1024),
    })
    .unwrap()
    .start()
    .unwrap();

    let batch = RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Int64, false),
            Field::new("instrument_id", DataType::Utf8, false),
            Field::new("last_price", DataType::Float64, false),
            Field::new("volume", DataType::Int64, false),
        ])),
        vec![
            std::sync::Arc::new(Int64Array::from(vec![3, 1, 2, 4])),
            std::sync::Arc::new(StringArray::from(vec!["c", "a", "b", "d"])),
            std::sync::Arc::new(Float64Array::from(vec![30.0, 10.0, 20.0, 40.0])),
            std::sync::Arc::new(Int64Array::from(vec![300, 100, 200, 400])),
        ],
    )
    .unwrap();
    let response = send_gateway_frame(
        gateway.endpoint(),
        json!({
            "kind": "write_batch",
            "stream_name": "plan_ticks",
            "token": "dev-token",
            "rows": 4
        }),
        encode_ipc_batch(&batch),
    );
    assert_eq!(response["status"], "ok");

    let (response, payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": "plan_ticks",
            "token": "dev-token",
            "plan": [
                {"op": "filter", "expr": {
                    "kind": "binary",
                    "op": "lt",
                    "args": [
                        {"kind": "col", "value": "last_price"},
                        {"kind": "literal", "value": 40.0}
                    ]
                }},
                {"op": "sort", "by": [{"kind": "col", "value": "dt"}], "descending": false},
                {"op": "slice", "offset": 1, "length": 3},
                {"op": "head", "n": 2},
                {"op": "drop", "columns": ["volume"]},
                {"op": "rename", "mapping": {"last_price": "price"}},
                {"op": "tail", "n": 1}
            ]
        }),
        vec![],
    );

    assert_eq!(response["status"], "ok");
    let collected = decode_ipc_batch(&payload);
    assert_eq!(collected.num_rows(), 1);
    assert_eq!(
        collected
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["dt", "instrument_id", "price"]
    );
    let dt = collected
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let instrument_id = collected
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let price = collected
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(dt.value(0), 3);
    assert_eq!(instrument_id.value(0), "c");
    assert_eq!(price.value(0), 30.0);

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_reregisters_master_process_after_lease_expiry() {
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) = spawn_master(master_endpoint.clone());
    let gateway_endpoint = format!("127.0.0.1:{}", reserve_tcp_port());
    let gateway = GatewayServer::new(GatewayServerConfig {
        endpoint: gateway_endpoint.clone(),
        master_endpoint: master_endpoint.clone(),
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

    let mut client = MasterClient::connect_endpoint(master_endpoint.clone()).unwrap();
    client.register_process("external_segment_writer").unwrap();
    client
        .register_stream("external_ticks", batch.schema(), 64, 4096)
        .unwrap();
    client
        .register_source("external_source", "test", "external_ticks", json!({}))
        .unwrap();
    let mut materializer = StreamTableMaterializer::new("external_ticks", batch.schema()).unwrap();
    client
        .publish_segment_descriptor_bytes(
            "external_ticks",
            &materializer.active_descriptor_envelope_bytes().unwrap(),
        )
        .unwrap();
    materializer
        .on_data(SegmentTableView::from_record_batch(batch))
        .unwrap();
    materializer.on_flush().unwrap();

    let (response, payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": "external_ticks",
            "token": "dev-token",
        }),
        vec![],
    );
    assert_eq!(response["status"], "ok");
    assert_eq!(decode_ipc_batch(&payload).num_rows(), 1);

    let expire_response = send_raw_control_line(
        &master_endpoint,
        "{\"ExpireProcessForTest\":{\"process_id\":\"proc_2\"}}\n",
    );
    assert!(expire_response.to_string().contains("proc_2"));

    let (response, payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": "external_ticks",
            "token": "dev-token",
        }),
        vec![],
    );
    assert_eq!(response["status"], "ok");
    assert_eq!(decode_ipc_batch(&payload).num_rows(), 1);

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

fn send_raw_control_line(endpoint: &ControlEndpoint, line: &str) -> serde_json::Value {
    let mut stream = connect_control_endpoint(endpoint).unwrap();
    stream.write_all(line.as_bytes()).unwrap();
    stream.flush().unwrap();

    let mut response = String::new();
    let mut reader = BufReader::new(stream);
    reader.read_line(&mut response).unwrap();
    serde_json::from_str(&response).unwrap()
}

fn reserve_tcp_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn loopback_control_endpoint() -> ControlEndpoint {
    ControlEndpoint::Tcp(SocketAddr::from(([127, 0, 0, 1], reserve_tcp_port())))
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
    send_gateway_frame_with_payload(endpoint, header, payload).0
}

fn send_gateway_frame_with_payload(
    endpoint: &str,
    header: serde_json::Value,
    payload: Vec<u8>,
) -> (serde_json::Value, Vec<u8>) {
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
    let mut response_payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut response_payload).unwrap();
    }
    (
        serde_json::from_slice(&response_header).unwrap(),
        response_payload,
    )
}

fn decode_ipc_batch(payload: &[u8]) -> RecordBatch {
    let mut reader = StreamReader::try_new(Cursor::new(payload), None).unwrap();
    reader.next().unwrap().unwrap()
}
