use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use serde_json::json;
use zippy_core::{
    connect_control_endpoint, ControlEndpoint, Engine, MasterClient, SegmentTableView, ZippyConfig,
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
    assert_eq!(stream.buffer_size, 64);
    assert_eq!(stream.segment_row_capacity, Some(65_536));
    assert_eq!(
        stream
            .active_segment_preflight
            .as_ref()
            .and_then(|preflight| preflight.get("status"))
            .and_then(serde_json::Value::as_str),
        Some("ok")
    );

    let gateway_stream_response = send_gateway_frame(
        gateway.endpoint(),
        json!({
            "kind": "get_stream",
            "source": "native_gateway_ticks",
            "token": "dev-token",
        }),
        vec![],
    );
    assert_eq!(
        gateway_stream_response["stream"]["active_segment_preflight"]["status"],
        json!("ok")
    );

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_binds_writer_epoch_to_master_source_epoch() {
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

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("last_price", DataType::Float64, false),
    ]));
    let _burn_store_id = StreamTableMaterializer::new("burn_writer_epoch", schema.clone()).unwrap();
    let batch = RecordBatch::try_new(
        schema,
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
            "stream_name": "gateway_epoch_ticks",
            "token": "dev-token",
            "rows": 1
        }),
        encode_ipc_batch(&batch),
    );

    assert_eq!(response["status"], "ok");

    let mut client = MasterClient::connect_endpoint(master_endpoint).unwrap();
    client.register_process("gateway_epoch_probe").unwrap();
    let stream = client.get_stream("gateway_epoch_ticks").unwrap();
    assert_eq!(stream.writer_epoch, 1);
    let descriptor = stream.active_segment_descriptor.unwrap();
    assert_eq!(descriptor["writer_epoch"], json!(1));

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_rejects_bad_token_before_reading_payload() {
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

    let response = send_gateway_header_without_payload(
        gateway.endpoint(),
        json!({
            "kind": "write_batch",
            "stream_name": "auth_ticks",
            "token": "bad-token",
        }),
        1024 * 1024,
    );

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();

    let response = response.expect("gateway must reject unauthorized header without payload");
    assert_eq!(response["status"], "error");
    assert!(response["reason"]
        .as_str()
        .unwrap()
        .contains("unauthorized"));
}

#[test]
fn native_gateway_rejects_oversized_payload_length_before_allocation() {
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

    let response = send_gateway_header_without_payload(
        gateway.endpoint(),
        json!({
            "kind": "write_batch",
            "stream_name": "oversized_ticks",
            "token": "dev-token",
        }),
        128 * 1024 * 1024,
    );

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();

    match response {
        Ok(response) => {
            assert_eq!(response["status"], "error");
            assert!(response["reason"]
                .as_str()
                .unwrap()
                .contains("payload length"));
        }
        Err(error) => {
            assert_ne!(error.kind(), std::io::ErrorKind::WouldBlock);
            assert_ne!(error.kind(), std::io::ErrorKind::TimedOut);
        }
    }
}

#[test]
fn native_gateway_runtime_starts_and_stops_cleanly() {
    for _ in 0..3 {
        let master_endpoint = loopback_control_endpoint();
        let (master, master_thread) = spawn_master(master_endpoint.clone());
        let gateway_endpoint = format!("127.0.0.1:{}", reserve_tcp_port());
        let gateway = GatewayServer::new(GatewayServerConfig {
            endpoint: gateway_endpoint,
            master_endpoint: master_endpoint.clone(),
            token: Some("dev-token".to_string()),
            max_write_rows: Some(1024),
        })
        .unwrap()
        .start()
        .unwrap();

        let response = send_gateway_frame(
            gateway.endpoint(),
            json!({"kind": "metrics", "token": "dev-token"}),
            vec![],
        );

        assert_eq!(response["status"], "ok");

        gateway.stop();
        master.shutdown();
        master_thread.join().unwrap().unwrap();
    }
}

#[test]
fn native_gateway_idle_connection_does_not_block_metrics_request() {
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) = spawn_master(master_endpoint.clone());
    let gateway_endpoint = format!("127.0.0.1:{}", reserve_tcp_port());
    let gateway = GatewayServer::new(GatewayServerConfig {
        endpoint: gateway_endpoint,
        master_endpoint: master_endpoint.clone(),
        token: Some("dev-token".to_string()),
        max_write_rows: Some(1024),
    })
    .unwrap()
    .start()
    .unwrap();

    let idle_stream = TcpStream::connect(gateway.endpoint()).unwrap();
    for _ in 0..50 {
        if gateway.metrics()["connections_active"]
            .as_u64()
            .unwrap_or(0)
            >= 1
        {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        gateway.metrics()["connections_active"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );

    let response = send_gateway_frame(
        gateway.endpoint(),
        json!({"kind": "metrics", "token": "dev-token"}),
        vec![],
    );

    assert_eq!(response["status"], "ok");
    drop(idle_stream);
    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_reports_async_master_control_requests() {
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
            "stream_name": "async_master_metrics_ticks",
            "token": "dev-token",
            "rows": 1
        }),
        encode_ipc_batch(&batch),
    );
    assert_eq!(response["status"], "ok");

    let response = send_gateway_frame(
        gateway.endpoint(),
        json!({
            "kind": "get_stream",
            "source": "async_master_metrics_ticks",
            "token": "dev-token",
        }),
        vec![],
    );
    assert_eq!(response["status"], "ok");

    let metrics = gateway.metrics();
    assert!(metrics["master_async_requests_total"].as_u64().unwrap() >= 1);

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
    let mut materializer =
        master_bound_materializer(&mut client, "external_ticks", batch.schema(), 64);
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
                {"op": "filter", "expr": {
                    "kind": "binary",
                    "op": "eq",
                    "args": [
                        {"kind": "col", "value": "instrument_id"},
                        {"kind": "literal", "value": "IF2606"}
                    ]
                }},
                {"op": "select", "exprs": [{"kind": "col", "value": "instrument_id"}]}
            ]
        }),
        vec![],
    );

    assert_eq!(response["status"], "ok");
    assert_eq!(response["metrics"]["scanned_rows"], 1);
    assert_eq!(response["metrics"]["returned_rows"], 1);
    assert_eq!(response["metrics"]["plan_ops"], 2);
    assert_eq!(
        response["metrics"]["pushed_filters"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(response["metrics"]["residual_filters"], json!([]));
    assert_eq!(
        response["metrics"]["scan_projection_columns"],
        json!(["instrument_id"])
    );
    assert!(response["metrics"]["elapsed_ms"].as_f64().unwrap() >= 0.0);
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
fn native_gateway_collect_uses_pinned_snapshot_high_watermark() {
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

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("seq", DataType::Int64, false),
    ]));
    let mut client = MasterClient::connect_endpoint(master_endpoint).unwrap();
    client.register_process("snapshot_writer").unwrap();
    client
        .register_stream("snapshot_ticks", schema.clone(), 64, 4096)
        .unwrap();
    client
        .register_source("snapshot_source", "test", "snapshot_ticks", json!({}))
        .unwrap();
    let mut materializer = master_bound_materializer(&mut client, "snapshot_ticks", schema, 64);
    client
        .publish_segment_descriptor_bytes(
            "snapshot_ticks",
            &materializer.active_descriptor_envelope_bytes().unwrap(),
        )
        .unwrap();

    materializer
        .on_data(SegmentTableView::from_record_batch(
            RecordBatch::try_new(
                materializer.output_schema(),
                vec![
                    std::sync::Arc::new(StringArray::from(vec!["IF2606"])),
                    std::sync::Arc::new(Int64Array::from(vec![1])),
                ],
            )
            .unwrap(),
        ))
        .unwrap();
    materializer.on_flush().unwrap();

    let response = send_gateway_frame(
        gateway.endpoint(),
        json!({
            "kind": "create_snapshot",
            "source": "snapshot_ticks",
            "token": "dev-token",
        }),
        vec![],
    );
    assert_eq!(response["status"], "ok");
    let snapshot_id = response["snapshot"]["snapshot_id"].as_str().unwrap();

    materializer
        .on_data(SegmentTableView::from_record_batch(
            RecordBatch::try_new(
                materializer.output_schema(),
                vec![
                    std::sync::Arc::new(StringArray::from(vec!["IF2607"])),
                    std::sync::Arc::new(Int64Array::from(vec![2])),
                ],
            )
            .unwrap(),
        ))
        .unwrap();
    materializer.on_flush().unwrap();

    let (response, payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": "snapshot_ticks",
            "snapshot_id": snapshot_id,
            "token": "dev-token",
            "plan": [],
        }),
        vec![],
    );

    assert_eq!(response["status"], "ok");
    let collected = decode_ipc_batch(&payload);
    assert_eq!(collected.num_rows(), 1);
    let instruments = collected
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let seq = collected
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(instruments.value(0), "IF2606");
    assert_eq!(seq.value(0), 1);

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_collects_persisted_parquet_catalog_rows_without_live_segment() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("ticks.parquet");
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) =
        spawn_master_with_persist_root(master_endpoint.clone(), temp.path());
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
            std::sync::Arc::new(StringArray::from(vec!["IF2606", "IH2606"])),
            std::sync::Arc::new(Float64Array::from(vec![4102.5, 2801.0])),
        ],
    )
    .unwrap();
    write_parquet_batch(&parquet_path, &batch);

    let mut client = MasterClient::connect_endpoint(master_endpoint).unwrap();
    client.register_process("persisted_catalog_writer").unwrap();
    client
        .register_stream("persisted_ticks", batch.schema(), 64, 4096)
        .unwrap();
    client
        .register_source("persisted_source", "test", "persisted_ticks", json!({}))
        .unwrap();
    client
        .publish_persisted_file(
            "persisted_ticks",
            json!({
                "file_path": parquet_path.to_string_lossy(),
                "row_count": 2,
                "source_segment_id": 1,
                "source_generation": 0
            }),
        )
        .unwrap();

    let (response, payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": "persisted_ticks",
            "token": "dev-token",
            "plan": [
                {"op": "filter", "expr": {
                    "kind": "binary",
                    "op": "eq",
                    "args": [
                        {"kind": "col", "value": "instrument_id"},
                        {"kind": "literal", "value": "IF2606"}
                    ]
                }},
                {"op": "select", "exprs": [{"kind": "col", "value": "last_price"}]}
            ]
        }),
        vec![],
    );

    assert_eq!(response["status"], "ok");
    assert_eq!(
        response["metrics"]["scanned_files"],
        json!([parquet_path.to_string_lossy()])
    );
    assert_eq!(response["metrics"]["scanned_rows"], 2);
    assert_eq!(response["metrics"]["scanned_live_rows"], 0);
    assert_eq!(response["metrics"]["returned_rows"], 1);
    let collected = decode_ipc_batch(&payload);
    assert_eq!(collected.num_rows(), 1);
    assert_eq!(collected.num_columns(), 1);
    let prices = collected
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(prices.value(0), 4102.5);

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_collect_streams_persisted_files_in_default_order() {
    let temp = tempfile::tempdir().unwrap();
    let first_path = temp.path().join("first.parquet");
    let second_path = temp.path().join("second.parquet");
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) =
        spawn_master_with_persist_root(master_endpoint.clone(), temp.path());
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

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("seq", DataType::Int64, false),
    ]));
    let first_batch = RecordBatch::try_new(
        std::sync::Arc::clone(&schema),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["IF2606"])),
            std::sync::Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let second_batch = RecordBatch::try_new(
        std::sync::Arc::clone(&schema),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["IF2607"])),
            std::sync::Arc::new(Int64Array::from(vec![2])),
        ],
    )
    .unwrap();
    write_parquet_batch(&first_path, &first_batch);
    write_parquet_batch(&second_path, &second_batch);

    let mut client = MasterClient::connect_endpoint(master_endpoint).unwrap();
    client.register_process("stream_persisted_writer").unwrap();
    client
        .register_stream("stream_persisted_ticks", schema, 64, 4096)
        .unwrap();
    client
        .register_source(
            "stream_persisted_source",
            "test",
            "stream_persisted_ticks",
            json!({}),
        )
        .unwrap();
    client
        .publish_persisted_file(
            "stream_persisted_ticks",
            json!({
                "file_path": first_path.to_string_lossy(),
                "row_count": 1,
                "source_segment_id": 1,
                "source_generation": 0
            }),
        )
        .unwrap();
    client
        .publish_persisted_file(
            "stream_persisted_ticks",
            json!({
                "file_path": second_path.to_string_lossy(),
                "row_count": 1,
                "source_segment_id": 2,
                "source_generation": 0
            }),
        )
        .unwrap();

    let collect_plan = json!([
        {"op": "select", "exprs": [{"kind": "col", "value": "seq"}]}
    ]);
    let (default_response, default_payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": "stream_persisted_ticks",
            "token": "dev-token",
            "plan": collect_plan
        }),
        vec![],
    );
    assert_eq!(default_response["status"], "ok");

    let frames = send_gateway_stream_frames(
        gateway.endpoint(),
        json!({
            "kind": "collect_stream",
            "source": "stream_persisted_ticks",
            "token": "dev-token",
            "chunk_rows": 1,
            "plan": collect_plan
        }),
        vec![],
    );

    assert_eq!(frames.first().unwrap().0["kind"], "collect_start");
    assert_eq!(frames.last().unwrap().0["kind"], "collect_end");
    let streamed_batches = frames
        .iter()
        .filter(|(header, _)| header["kind"] == "collect_chunk")
        .map(|(_, payload)| decode_ipc_batch(payload))
        .collect::<Vec<_>>();
    let default_batch = decode_ipc_batch(&default_payload);
    let streamed_values = streamed_batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    let default_values = default_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(streamed_values, default_values);
    assert_eq!(
        frames.last().unwrap().0["metrics"]["streaming"],
        json!(true)
    );
    assert_eq!(
        frames.last().unwrap().0["metrics"]["scanned_files"],
        json!(2)
    );
    assert_eq!(gateway.metrics()["collect_requests_total"], json!(2));

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_collect_stream_applies_filter_then_final_select_for_persisted_files() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("ticks.parquet");
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) =
        spawn_master_with_persist_root(master_endpoint.clone(), temp.path());
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
            std::sync::Arc::new(StringArray::from(vec!["IF2606", "IH2606"])),
            std::sync::Arc::new(Float64Array::from(vec![4102.5, 2801.0])),
        ],
    )
    .unwrap();
    write_parquet_batch(&parquet_path, &batch);

    let mut client = MasterClient::connect_endpoint(master_endpoint).unwrap();
    client
        .register_process("stream_filter_select_writer")
        .unwrap();
    client
        .register_stream("stream_filter_select_ticks", batch.schema(), 64, 4096)
        .unwrap();
    client
        .register_source(
            "stream_filter_select_source",
            "test",
            "stream_filter_select_ticks",
            json!({}),
        )
        .unwrap();
    client
        .publish_persisted_file(
            "stream_filter_select_ticks",
            json!({
                "file_path": parquet_path.to_string_lossy(),
                "row_count": 2,
                "source_segment_id": 1,
                "source_generation": 0
            }),
        )
        .unwrap();

    let frames = send_gateway_stream_frames(
        gateway.endpoint(),
        json!({
            "kind": "collect_stream",
            "source": "stream_filter_select_ticks",
            "token": "dev-token",
            "chunk_rows": 1,
            "plan": [
                {"op": "filter", "expr": {
                    "kind": "binary",
                    "op": "eq",
                    "args": [
                        {"kind": "col", "value": "instrument_id"},
                        {"kind": "literal", "value": "IF2606"}
                    ]
                }},
                {"op": "select", "exprs": [{"kind": "col", "value": "last_price"}]}
            ]
        }),
        vec![],
    );

    let chunks = frames
        .iter()
        .filter(|(header, _)| header["kind"] == "collect_chunk")
        .map(|(_, payload)| decode_ipc_batch(payload))
        .collect::<Vec<_>>();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].num_rows(), 1);
    assert_eq!(
        chunks[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["last_price"]
    );
    let prices = chunks[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(prices.value(0), 4102.5);

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_collect_stream_keeps_mixed_persisted_and_live_on_materialized_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("persisted.parquet");
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) =
        spawn_master_with_persist_root(master_endpoint.clone(), temp.path());
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

    let schema = std::sync::Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
    let persisted_batch = RecordBatch::try_new(
        std::sync::Arc::clone(&schema),
        vec![std::sync::Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let live_batch = RecordBatch::try_new(
        std::sync::Arc::clone(&schema),
        vec![std::sync::Arc::new(Int64Array::from(vec![2]))],
    )
    .unwrap();
    write_parquet_batch(&parquet_path, &persisted_batch);

    let mut client = MasterClient::connect_endpoint(master_endpoint).unwrap();
    client.register_process("stream_mixed_writer").unwrap();
    client
        .register_stream(
            "stream_mixed_ticks",
            std::sync::Arc::clone(&schema),
            64,
            4096,
        )
        .unwrap();
    client
        .register_source(
            "stream_mixed_source",
            "test",
            "stream_mixed_ticks",
            json!({}),
        )
        .unwrap();
    client
        .publish_persisted_file(
            "stream_mixed_ticks",
            json!({
                "file_path": parquet_path.to_string_lossy(),
                "row_count": 1,
                "source_segment_id": 0,
                "source_generation": 0
            }),
        )
        .unwrap();
    let mut materializer = master_bound_materializer(&mut client, "stream_mixed_ticks", schema, 64);
    client
        .publish_segment_descriptor_bytes(
            "stream_mixed_ticks",
            &materializer.active_descriptor_envelope_bytes().unwrap(),
        )
        .unwrap();
    materializer
        .on_data(SegmentTableView::from_record_batch(live_batch))
        .unwrap();
    materializer.on_flush().unwrap();

    let frames = send_gateway_stream_frames(
        gateway.endpoint(),
        json!({
            "kind": "collect_stream",
            "source": "stream_mixed_ticks",
            "token": "dev-token",
            "chunk_rows": 1,
            "plan": [{"op": "select", "exprs": [{"kind": "col", "value": "seq"}]}]
        }),
        vec![],
    );

    let streamed_values = streamed_i64_values(&frames);
    assert_eq!(streamed_values, vec![1, 2]);
    assert_eq!(
        frames.last().unwrap().0["metrics"]["scanned_files"],
        json!(1)
    );
    assert_eq!(
        frames.last().unwrap().0["metrics"]["scanned_file_paths"],
        json!([parquet_path.to_string_lossy()])
    );

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_collect_stream_head_matches_default_collect() {
    assert_streaming_row_range_matches_default("head", json!({"op": "head", "n": 1}));
}

#[test]
fn native_gateway_collect_stream_tail_matches_default_collect() {
    assert_streaming_row_range_matches_default("tail", json!({"op": "tail", "n": 1}));
}

#[test]
fn native_gateway_pushes_tail_collect_into_persisted_catalog_files() {
    let temp = tempfile::tempdir().unwrap();
    let old_path = temp.path().join("segment-1.parquet");
    let new_path = temp.path().join("segment-2.parquet");
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) =
        spawn_master_with_persist_root(master_endpoint.clone(), temp.path());
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

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("dt", DataType::Int64, false),
        Field::new("instrument_id", DataType::Utf8, false),
    ]));
    let old_batch = RecordBatch::try_new(
        std::sync::Arc::clone(&schema),
        vec![
            std::sync::Arc::new(Int64Array::from(vec![1, 2])),
            std::sync::Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    let new_batch = RecordBatch::try_new(
        std::sync::Arc::clone(&schema),
        vec![
            std::sync::Arc::new(Int64Array::from(vec![3, 4])),
            std::sync::Arc::new(StringArray::from(vec!["c", "d"])),
        ],
    )
    .unwrap();
    write_parquet_batch(&old_path, &old_batch);
    write_parquet_batch(&new_path, &new_batch);

    let mut client = MasterClient::connect_endpoint(master_endpoint).unwrap();
    client.register_process("persisted_tail_writer").unwrap();
    client
        .register_stream("persisted_tail_ticks", schema, 64, 4096)
        .unwrap();
    client
        .register_source(
            "persisted_tail_source",
            "test",
            "persisted_tail_ticks",
            json!({}),
        )
        .unwrap();
    client
        .publish_persisted_file(
            "persisted_tail_ticks",
            json!({
                "file_path": old_path.to_string_lossy(),
                "row_count": 2,
                "source_segment_id": 1,
                "source_generation": 0
            }),
        )
        .unwrap();
    client
        .publish_persisted_file(
            "persisted_tail_ticks",
            json!({
                "file_path": new_path.to_string_lossy(),
                "row_count": 2,
                "source_segment_id": 2,
                "source_generation": 0
            }),
        )
        .unwrap();

    let (response, payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": "persisted_tail_ticks",
            "token": "dev-token",
            "plan": [{"op": "tail", "n": 1}]
        }),
        vec![],
    );

    assert_eq!(response["status"], "ok");
    assert_eq!(response["metrics"]["tail_pushdown"], true);
    assert_eq!(
        response["metrics"]["scanned_files"],
        json!([new_path.to_string_lossy()])
    );
    assert_eq!(response["metrics"]["scanned_rows"], 1);
    assert_eq!(response["metrics"]["returned_rows"], 1);
    let collected = decode_ipc_batch(&payload);
    assert_eq!(collected.num_rows(), 1);
    let instrument_id = collected
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(instrument_id.value(0), "d");

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
        master_bound_materializer(&mut client, "tail_external_ticks", batch.schema(), 2);
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
fn native_gateway_pushes_head_collect_into_segment_reader() {
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
    client.register_process("external_head_writer").unwrap();
    client
        .register_stream("head_external_ticks", batch.schema(), 2, 4096)
        .unwrap();
    client
        .register_source(
            "external_head_source",
            "test",
            "head_external_ticks",
            json!({}),
        )
        .unwrap();
    let mut materializer =
        master_bound_materializer(&mut client, "head_external_ticks", batch.schema(), 2);
    materializer
        .on_data(SegmentTableView::from_record_batch(batch))
        .unwrap();
    materializer.on_flush().unwrap();
    let descriptor_bytes = materializer.active_descriptor_envelope_bytes().unwrap();
    let mut descriptor: serde_json::Value = serde_json::from_slice(&descriptor_bytes).unwrap();
    descriptor["shm_os_id"] = json!("/tmp/zippy-missing-active-segment");
    client
        .publish_segment_descriptor_bytes(
            "head_external_ticks",
            &serde_json::to_vec(&descriptor).unwrap(),
        )
        .unwrap();

    let (response, payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": "head_external_ticks",
            "token": "dev-token",
            "plan": [
                {"op": "head", "n": 1},
                {"op": "select", "exprs": [{"kind": "col", "value": "instrument_id"}]}
            ]
        }),
        vec![],
    );

    assert_eq!(response["status"], "ok");
    assert_eq!(response["metrics"]["row_range_pushdown"], "head");
    assert_eq!(response["metrics"]["residual_plan_ops"], 1);
    assert_eq!(
        response["metrics"]["projection_columns"],
        json!(["instrument_id"])
    );
    assert_eq!(
        response["metrics"]["scan_projection_columns"],
        json!(["instrument_id"])
    );
    assert_eq!(response["metrics"]["residual_filters"], json!([]));
    let collected = decode_ipc_batch(&payload);
    assert_eq!(collected.num_rows(), 1);
    assert_eq!(collected.num_columns(), 1);
    let instrument_id = collected
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(instrument_id.value(0), "a");

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_pushes_slice_collect_into_segment_reader() {
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
    client.register_process("external_slice_writer").unwrap();
    client
        .register_stream("slice_external_ticks", batch.schema(), 2, 4096)
        .unwrap();
    client
        .register_source(
            "external_slice_source",
            "test",
            "slice_external_ticks",
            json!({}),
        )
        .unwrap();
    let mut materializer =
        master_bound_materializer(&mut client, "slice_external_ticks", batch.schema(), 2);
    materializer
        .on_data(SegmentTableView::from_record_batch(batch))
        .unwrap();
    materializer.on_flush().unwrap();
    let descriptor_bytes = materializer.active_descriptor_envelope_bytes().unwrap();
    let mut descriptor: serde_json::Value = serde_json::from_slice(&descriptor_bytes).unwrap();
    descriptor["shm_os_id"] = json!("/tmp/zippy-missing-active-segment");
    client
        .publish_segment_descriptor_bytes(
            "slice_external_ticks",
            &serde_json::to_vec(&descriptor).unwrap(),
        )
        .unwrap();

    let (response, payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": "slice_external_ticks",
            "token": "dev-token",
            "plan": [{"op": "slice", "offset": 2, "length": 1}]
        }),
        vec![],
    );

    assert_eq!(response["status"], "ok");
    assert_eq!(response["metrics"]["row_range_pushdown"], "slice");
    let collected = decode_ipc_batch(&payload);
    assert_eq!(collected.num_rows(), 1);
    let instrument_id = collected
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(instrument_id.value(0), "c");

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_collect_stream_returns_start_chunks_and_end_frames() {
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
            std::sync::Arc::new(StringArray::from(vec!["IF2606", "IF2607"])),
            std::sync::Arc::new(Float64Array::from(vec![4102.5, 4103.5])),
        ],
    )
    .unwrap();
    let write_response = send_gateway_frame(
        gateway.endpoint(),
        json!({
            "kind": "write_batch",
            "stream_name": "stream_collect_ticks",
            "token": "dev-token",
            "rows": 2
        }),
        encode_ipc_batch(&batch),
    );
    assert_eq!(write_response["status"], "ok");

    let frames = send_gateway_stream_frames(
        gateway.endpoint(),
        json!({
            "kind": "collect_stream",
            "source": "stream_collect_ticks",
            "token": "dev-token",
            "chunk_rows": 1,
            "plan": [{"op": "select", "exprs": [{"kind": "col", "value": "instrument_id"}]}]
        }),
        vec![],
    );

    assert_eq!(frames[0].0["status"], "ok");
    assert_eq!(frames[0].0["kind"], "collect_start");
    assert_eq!(frames[1].0["kind"], "collect_chunk");
    assert_eq!(frames[2].0["kind"], "collect_chunk");
    assert_eq!(frames[3].0["kind"], "collect_end");
    assert_eq!(frames[3].0["metrics"]["returned_rows"], json!(2));
    assert_eq!(frames[3].0["metrics"]["streaming"], json!(true));

    let first_chunk = decode_ipc_batch(&frames[1].1);
    let second_chunk = decode_ipc_batch(&frames[2].1);
    assert_eq!(first_chunk.num_rows(), 1);
    assert_eq!(second_chunk.num_rows(), 1);
    let first_ids = first_chunk
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let second_ids = second_chunk
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(first_ids.value(0), "IF2606");
    assert_eq!(second_ids.value(0), "IF2607");

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_collect_stream_reports_streaming_metrics() {
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
        std::sync::Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)])),
        vec![std::sync::Arc::new(Int64Array::from(vec![1_i64, 2]))],
    )
    .unwrap();
    let write_response = send_gateway_frame(
        gateway.endpoint(),
        json!({
            "kind": "write_batch",
            "stream_name": "stream_metrics_ticks",
            "token": "dev-token",
            "rows": 2
        }),
        encode_ipc_batch(&batch),
    );
    assert_eq!(write_response["status"], "ok");

    let frames = send_gateway_stream_frames(
        gateway.endpoint(),
        json!({
            "kind": "collect_stream",
            "source": "stream_metrics_ticks",
            "token": "dev-token",
            "chunk_rows": 1,
            "plan": []
        }),
        vec![],
    );
    let metrics = &frames.last().unwrap().0["metrics"];

    assert_eq!(metrics["streaming"], json!(true));
    assert_eq!(metrics["returned_rows"], json!(2));
    assert!(metrics["encode_elapsed_ms"].as_f64().unwrap() >= 0.0);
    assert!(metrics["write_elapsed_ms"].as_f64().unwrap() >= 0.0);
    assert_eq!(metrics["materialized_live_batches"], json!(0));
    assert_eq!(metrics["segment_streamed_batches"], json!(2));
    assert_eq!(metrics["segment_streamed_rows"], json!(2));

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_collect_stream_live_segment_applies_projection_and_filter_by_chunk() {
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
            Field::new("seq", DataType::Int64, false),
            Field::new("last_price", DataType::Float64, false),
        ])),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["IF2606", "IF2607", "IF2608"])),
            std::sync::Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            std::sync::Arc::new(Float64Array::from(vec![4102.5, 4103.5, 4104.5])),
        ],
    )
    .unwrap();
    let write_response = send_gateway_frame(
        gateway.endpoint(),
        json!({
            "kind": "write_batch",
            "stream_name": "stream_live_chunk_filter_ticks",
            "token": "dev-token",
            "rows": 3
        }),
        encode_ipc_batch(&batch),
    );
    assert_eq!(write_response["status"], "ok");

    let frames = send_gateway_stream_frames(
        gateway.endpoint(),
        json!({
            "kind": "collect_stream",
            "source": "stream_live_chunk_filter_ticks",
            "token": "dev-token",
            "chunk_rows": 1,
            "plan": [
                {
                    "op": "filter",
                    "expr": {
                        "kind": "binary",
                        "op": "gt",
                        "args": [
                            {"kind": "col", "value": "seq"},
                            {"kind": "literal", "value": 1}
                        ]
                    }
                },
                {
                    "op": "select",
                    "exprs": [
                        {"kind": "col", "value": "instrument_id"},
                        {"kind": "col", "value": "last_price"}
                    ]
                }
            ]
        }),
        vec![],
    );

    let chunks = frames
        .iter()
        .filter(|(header, _)| header["kind"] == json!("collect_chunk"))
        .map(|(_, payload)| decode_ipc_batch(payload))
        .collect::<Vec<_>>();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].schema().field(0).name(), "instrument_id");
    assert_eq!(chunks[0].schema().field(1).name(), "last_price");

    let first_ids = chunks[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let second_ids = chunks[1]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(first_ids.value(0), "IF2607");
    assert_eq!(second_ids.value(0), "IF2608");

    let metrics = &frames.last().unwrap().0["metrics"];
    assert_eq!(metrics["segment_streamed_batches"], json!(3));
    assert_eq!(metrics["segment_streamed_rows"], json!(3));
    assert_eq!(metrics["returned_rows"], json!(2));

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_collect_stream_rejects_residual_plan_before_start() {
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
    let write_response = send_gateway_frame(
        gateway.endpoint(),
        json!({
            "kind": "write_batch",
            "stream_name": "stream_residual_ticks",
            "token": "dev-token",
            "rows": 1
        }),
        encode_ipc_batch(&batch),
    );
    assert_eq!(write_response["status"], "ok");

    let response = send_gateway_header_without_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect_stream",
            "source": "stream_residual_ticks",
            "token": "dev-token",
            "plan": [
                {"op": "with_columns", "exprs": [
                    {"kind": "literal", "value": 1, "alias": "one"}
                ]}
            ]
        }),
        0,
    )
    .unwrap();

    assert_eq!(response["status"], "error");
    assert!(response["reason"]
        .as_str()
        .unwrap()
        .contains("collect(stream=True) requires a fully streamable plan"));

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

#[test]
fn native_gateway_collect_stream_rejects_row_range_then_filter_before_start() {
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

    let response = send_gateway_header_without_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect_stream",
            "source": "stream_range_filter_ticks",
            "token": "dev-token",
            "plan": [
                {"op": "head", "n": 1},
                {"op": "filter", "expr": {
                    "kind": "binary",
                    "op": "eq",
                    "args": [
                        {"kind": "col", "value": "seq"},
                        {"kind": "literal", "value": 2}
                    ]
                }}
            ]
        }),
        0,
    )
    .unwrap();

    assert_eq!(response["status"], "error");
    assert!(response["reason"]
        .as_str()
        .unwrap()
        .contains("collect(stream=True) requires a fully streamable plan"));

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
    assert_eq!(
        response["metrics"]["residual_filters"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(response["metrics"]["pushed_filters"], json!([]));
    assert_eq!(
        response["metrics"]["scan_projection_columns"],
        serde_json::Value::Null
    );
    assert_eq!(
        response["metrics"]["projection_columns"],
        serde_json::Value::Null
    );
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
    let mut materializer =
        master_bound_materializer(&mut client, "external_ticks", batch.schema(), 64);
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
    spawn_master_with_config(endpoint, ZippyConfig::default())
}

fn spawn_master_with_persist_root(
    endpoint: ControlEndpoint,
    persist_root: &Path,
) -> (MasterServer, thread::JoinHandle<zippy_core::Result<()>>) {
    let mut config = ZippyConfig::default();
    config.table.persist.data_dir = persist_root.to_string_lossy().to_string();
    spawn_master_with_config(endpoint, config)
}

fn spawn_master_with_config(
    endpoint: ControlEndpoint,
    config: ZippyConfig,
) -> (MasterServer, thread::JoinHandle<zippy_core::Result<()>>) {
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let server = MasterServer::with_config(config);
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

fn assert_streaming_row_range_matches_default(label: &str, row_range_op: serde_json::Value) {
    let temp = tempfile::tempdir().unwrap();
    let first_path = temp.path().join(format!("{}_first.parquet", label));
    let second_path = temp.path().join(format!("{}_second.parquet", label));
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) =
        spawn_master_with_persist_root(master_endpoint.clone(), temp.path());
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

    let schema = std::sync::Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
    let first_batch = RecordBatch::try_new(
        std::sync::Arc::clone(&schema),
        vec![std::sync::Arc::new(Int64Array::from(vec![1_i64]))],
    )
    .unwrap();
    let second_batch = RecordBatch::try_new(
        std::sync::Arc::clone(&schema),
        vec![std::sync::Arc::new(Int64Array::from(vec![2_i64]))],
    )
    .unwrap();
    write_parquet_batch(&first_path, &first_batch);
    write_parquet_batch(&second_path, &second_batch);

    let stream_name = format!("stream_range_{}_ticks", label);
    let mut client = MasterClient::connect_endpoint(master_endpoint).unwrap();
    client
        .register_process(&format!("stream_range_{}_writer", label))
        .unwrap();
    client
        .register_stream(&stream_name, schema, 64, 4096)
        .unwrap();
    client
        .register_source(
            &format!("stream_range_{}_source", label),
            "test",
            &stream_name,
            json!({}),
        )
        .unwrap();
    client
        .publish_persisted_file(
            &stream_name,
            json!({
                "file_path": first_path.to_string_lossy(),
                "row_count": 1,
                "source_segment_id": 1,
                "source_generation": 0
            }),
        )
        .unwrap();
    client
        .publish_persisted_file(
            &stream_name,
            json!({
                "file_path": second_path.to_string_lossy(),
                "row_count": 1,
                "source_segment_id": 2,
                "source_generation": 0
            }),
        )
        .unwrap();

    let plan = json!([
        row_range_op,
        {"op": "select", "exprs": [{"kind": "col", "value": "seq"}]}
    ]);
    let (default_response, default_payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": stream_name,
            "token": "dev-token",
            "plan": plan
        }),
        vec![],
    );
    assert_eq!(default_response["status"], "ok");

    let frames = send_gateway_stream_frames(
        gateway.endpoint(),
        json!({
            "kind": "collect_stream",
            "source": stream_name,
            "token": "dev-token",
            "chunk_rows": 1,
            "plan": plan
        }),
        vec![],
    );
    let default_batch = decode_ipc_batch(&default_payload);
    let default_values = default_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(streamed_i64_values(&frames), default_values);
    assert_eq!(
        frames.last().unwrap().0["metrics"]["row_range_pushdown"],
        json!(label)
    );
    let expected_scanned_files = if label == "tail" { 1 } else { 2 };
    assert_eq!(
        frames.last().unwrap().0["metrics"]["scanned_files"],
        json!(expected_scanned_files)
    );

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

fn reserve_tcp_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn loopback_control_endpoint() -> ControlEndpoint {
    ControlEndpoint::Tcp(SocketAddr::from(([127, 0, 0, 1], reserve_tcp_port())))
}

fn master_bound_materializer(
    client: &mut MasterClient,
    stream_name: &str,
    schema: std::sync::Arc<Schema>,
    row_capacity: usize,
) -> StreamTableMaterializer {
    let writer_epoch = client.get_stream(stream_name).unwrap().writer_epoch;
    StreamTableMaterializer::new_with_row_capacity_and_writer_epoch(
        stream_name,
        schema,
        row_capacity,
        Some(writer_epoch),
    )
    .unwrap()
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

fn write_parquet_batch(path: &Path, batch: &RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
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

fn send_gateway_stream_frames(
    endpoint: &str,
    header: serde_json::Value,
    payload: Vec<u8>,
) -> Vec<(serde_json::Value, Vec<u8>)> {
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

    let mut frames = Vec::new();
    loop {
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
        let response = serde_json::from_slice::<serde_json::Value>(&response_header).unwrap();
        let kind = response.get("kind").and_then(serde_json::Value::as_str);
        let is_terminal = matches!(kind, Some("collect_end")) || response["status"] == "error";
        frames.push((response, response_payload));
        if is_terminal {
            break;
        }
    }
    frames
}

fn send_gateway_header_without_payload(
    endpoint: &str,
    header: serde_json::Value,
    payload_len: u64,
) -> std::io::Result<serde_json::Value> {
    let mut stream = TcpStream::connect(endpoint)?;
    stream.set_read_timeout(Some(Duration::from_millis(250)))?;
    let header_bytes = serde_json::to_vec(&header).unwrap();
    stream.write_all(&(header_bytes.len() as u32).to_be_bytes())?;
    stream.write_all(&payload_len.to_be_bytes())?;
    stream.write_all(&header_bytes)?;

    let mut prefix = [0u8; 12];
    stream.read_exact(&mut prefix)?;
    let header_len = u32::from_be_bytes(prefix[0..4].try_into().unwrap()) as usize;
    let payload_len = u64::from_be_bytes(prefix[4..12].try_into().unwrap()) as usize;
    let mut response_header = vec![0u8; header_len];
    stream.read_exact(&mut response_header)?;
    if payload_len > 0 {
        let mut response_payload = vec![0u8; payload_len];
        stream.read_exact(&mut response_payload)?;
    }
    Ok(serde_json::from_slice(&response_header).unwrap())
}

fn decode_ipc_batch(payload: &[u8]) -> RecordBatch {
    let mut reader = StreamReader::try_new(Cursor::new(payload), None).unwrap();
    reader.next().unwrap().unwrap()
}

fn streamed_i64_values(frames: &[(serde_json::Value, Vec<u8>)]) -> Vec<i64> {
    frames
        .iter()
        .filter(|(header, _)| header["kind"] == "collect_chunk")
        .flat_map(|(_, payload)| {
            let batch = decode_ipc_batch(payload);
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}
