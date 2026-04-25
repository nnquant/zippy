#![cfg(feature = "zmq-publisher")]

use std::sync::{Arc, Mutex};
use std::{thread, time::Duration};

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::convert::{fb_to_schema, IpcSchemaEncoder};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, SchemaRef, Source, SourceEvent, SourceHandle, SourceMode, SourceSink};
use zippy_io::{Publisher, ZmqSource, ZmqStreamPublisher};

const PROTOCOL_VERSION: u16 = 1;
const STREAM_NAME: &str = "bars";
const MANUAL_SEND_REPEAT_COUNT: usize = 3;
const WILDCARD_ENDPOINT: &str = "tcp://127.0.0.1:*";

#[derive(Debug)]
enum RecordedEvent {
    Hello {
        protocol_version: u16,
        stream_name: String,
        schema: SchemaRef,
        schema_hash: String,
    },
    Data(RecordBatch),
    Flush,
    Stop,
    Error(String),
}

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<RecordedEvent>>,
}

impl RecordingSink {
    fn snapshot(&self) -> Vec<RecordedEventSnapshot> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(RecordedEventSnapshot::from)
            .collect()
    }
}

impl SourceSink for RecordingSink {
    fn emit(&self, event: SourceEvent) -> Result<()> {
        let recorded = match event {
            SourceEvent::Hello(hello) => RecordedEvent::Hello {
                protocol_version: hello.protocol_version,
                stream_name: hello.stream_name,
                schema: hello.schema,
                schema_hash: hello.schema_hash,
            },
            SourceEvent::Data(table) => RecordedEvent::Data(table.to_record_batch()?),
            SourceEvent::Flush => RecordedEvent::Flush,
            SourceEvent::Stop => RecordedEvent::Stop,
            SourceEvent::Error(reason) => RecordedEvent::Error(reason),
        };
        self.events.lock().unwrap().push(recorded);
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum RecordedEventSnapshot {
    Hello {
        protocol_version: u16,
        stream_name: String,
        schema: SchemaRef,
        schema_hash: String,
    },
    Data(RecordBatch),
    Flush,
    Stop,
    Error(String),
}

impl From<&RecordedEvent> for RecordedEventSnapshot {
    fn from(value: &RecordedEvent) -> Self {
        match value {
            RecordedEvent::Hello {
                protocol_version,
                stream_name,
                schema,
                schema_hash,
            } => Self::Hello {
                protocol_version: *protocol_version,
                stream_name: stream_name.clone(),
                schema: schema.clone(),
                schema_hash: schema_hash.clone(),
            },
            RecordedEvent::Data(batch) => Self::Data(batch.clone()),
            RecordedEvent::Flush => Self::Flush,
            RecordedEvent::Stop => Self::Stop,
            RecordedEvent::Error(reason) => Self::Error(reason.clone()),
        }
    }
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Float64,
        false,
    )]))
}

fn other_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "volume",
        DataType::Float64,
        false,
    )]))
}

fn test_batch() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]))],
    )
    .unwrap()
}

struct ManualPublisher {
    _context: zmq::Context,
    socket: zmq::Socket,
    endpoint: String,
}

fn sleep_for_pubsub() {
    thread::sleep(Duration::from_millis(150));
}

fn encode_schema(schema: &SchemaRef) -> Vec<u8> {
    let mut encoder = IpcSchemaEncoder::new();
    encoder
        .schema_to_fb(schema.as_ref())
        .finished_data()
        .to_vec()
}

fn encode_batch(batch: &RecordBatch) -> Vec<u8> {
    let mut payload = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    payload
}

fn encode_meta(
    protocol_version: u16,
    stream_name: &str,
    seq_no: Option<u64>,
    schema_hash: Option<&str>,
    message: Option<&str>,
) -> Vec<u8> {
    let mut lines = vec![
        format!("protocol_version:{protocol_version}"),
        format!("stream_name:{stream_name}"),
    ];
    if let Some(seq_no) = seq_no {
        lines.push(format!("seq_no:{seq_no}"));
    }
    if let Some(schema_hash) = schema_hash {
        lines.push(format!("schema_hash:{schema_hash}"));
    }
    if let Some(message) = message {
        lines.push(format!("message:{message}"));
    }
    lines.join("\n").into_bytes()
}

fn decode_meta(meta: &[u8]) -> Vec<(String, String)> {
    std::str::from_utf8(meta)
        .unwrap()
        .lines()
        .map(|line| {
            let (key, value) = line.split_once(':').unwrap();
            (key.to_string(), value.to_string())
        })
        .collect()
}

fn meta_value<'a>(meta: &'a [(String, String)], key: &str) -> Option<&'a str> {
    meta.iter()
        .find_map(|(meta_key, meta_value)| (meta_key == key).then_some(meta_value.as_str()))
}

fn expected_schema_hash(schema: &SchemaRef) -> String {
    zippy_core::StreamHello::new(STREAM_NAME, schema.clone(), PROTOCOL_VERSION)
        .unwrap()
        .schema_hash
}

impl ManualPublisher {
    fn bind() -> Self {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::PUB).unwrap();
        socket.bind(WILDCARD_ENDPOINT).unwrap();
        let endpoint = socket.get_last_endpoint().unwrap().unwrap_or_else(|bytes| {
            panic!("failed to decode zmq last endpoint bytes=[{:?}]", bytes)
        });

        Self {
            _context: context,
            socket,
            endpoint,
        }
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

fn bind_manual_publisher() -> ManualPublisher {
    ManualPublisher::bind()
}

fn send_manual_message(socket: &zmq::Socket, kind: &str, meta: Vec<u8>, payload: Option<Vec<u8>>) {
    for _ in 0..MANUAL_SEND_REPEAT_COUNT {
        let mut frames = vec![kind.as_bytes().to_vec(), meta.clone()];
        if let Some(payload) = payload.clone() {
            frames.push(payload);
        }
        socket.send_multipart(frames, 0).unwrap();
        thread::sleep(Duration::from_millis(10));
    }
}

fn start_source(endpoint: &str) -> (SourceHandle, Arc<RecordingSink>) {
    let source =
        ZmqSource::connect("bars_source", endpoint, test_schema(), SourceMode::Pipeline).unwrap();
    let sink = Arc::new(RecordingSink::default());
    let handle = Box::new(source).start(sink.clone()).unwrap();
    sleep_for_pubsub();
    (handle, sink)
}

#[test]
fn zmq_source_protocol_hello_schema_encodes_correctly() {
    let mut publisher =
        ZmqStreamPublisher::bind(WILDCARD_ENDPOINT, STREAM_NAME, test_schema()).unwrap();
    let endpoint = publisher.last_endpoint().unwrap();
    let context = zmq::Context::new();
    let subscriber = context.socket(zmq::SUB).unwrap();
    subscriber.set_subscribe(b"").unwrap();
    subscriber.set_rcvtimeo(1_000).unwrap();
    subscriber.connect(&endpoint).unwrap();
    sleep_for_pubsub();

    publisher.publish_hello().unwrap();
    let frames = subscriber.recv_multipart(0).unwrap();

    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0], b"HELLO");

    let meta = decode_meta(&frames[1]);
    assert!(meta.contains(&("protocol_version".to_string(), PROTOCOL_VERSION.to_string())));
    assert!(meta.contains(&("stream_name".to_string(), STREAM_NAME.to_string())));
    assert!(meta.contains(&(
        "schema_hash".to_string(),
        expected_schema_hash(&test_schema()),
    )));

    let schema = fb_to_schema(arrow::ipc::root_as_schema(&frames[2]).unwrap());
    assert_eq!(Arc::new(schema), test_schema());
}

#[test]
fn zmq_source_decodes_data_payload_from_stream_publisher() {
    let mut publisher =
        ZmqStreamPublisher::bind(WILDCARD_ENDPOINT, STREAM_NAME, test_schema()).unwrap();
    let endpoint = publisher.last_endpoint().unwrap();
    let (handle, sink) = start_source(&endpoint);

    let batch = test_batch();
    sleep_for_pubsub();
    publisher.publish_hello().unwrap();
    publisher.publish(&batch).unwrap();
    publisher.close().unwrap();

    handle.join().unwrap();

    let events = sink.snapshot();
    assert!(matches!(&events[0], RecordedEventSnapshot::Hello { .. }));
    assert_eq!(events[1], RecordedEventSnapshot::Data(batch));
}

#[test]
fn zmq_source_decodes_flush_and_stop_without_payload_when_seq_no_present() {
    let manual_publisher = bind_manual_publisher();
    let endpoint = manual_publisher.endpoint().to_string();
    let (handle, sink) = start_source(&endpoint);
    let socket = &manual_publisher.socket;

    sleep_for_pubsub();
    send_manual_message(
        socket,
        "HELLO",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(2),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        Some(encode_schema(&test_schema())),
    );
    thread::sleep(Duration::from_millis(100));
    send_manual_message(
        socket,
        "FLUSH",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(3),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        None,
    );
    send_manual_message(
        socket,
        "STOP",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(4),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        None,
    );

    handle.join().unwrap();

    let events = sink.snapshot();
    assert!(events
        .iter()
        .any(|event| *event == RecordedEventSnapshot::Flush));
    assert!(events
        .iter()
        .any(|event| *event == RecordedEventSnapshot::Stop));
}

#[test]
fn zmq_source_ignores_data_before_hello_until_stream_lock() {
    let manual_publisher = bind_manual_publisher();
    let endpoint = manual_publisher.endpoint().to_string();
    let (handle, sink) = start_source(&endpoint);
    let socket = &manual_publisher.socket;

    sleep_for_pubsub();
    send_manual_message(
        socket,
        "DATA",
        encode_meta(PROTOCOL_VERSION, STREAM_NAME, Some(1), None, None),
        Some(encode_batch(&test_batch())),
    );
    thread::sleep(Duration::from_millis(100));
    send_manual_message(
        socket,
        "HELLO",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(2),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        Some(encode_schema(&test_schema())),
    );
    thread::sleep(Duration::from_millis(100));
    send_manual_message(
        socket,
        "DATA",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(3),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        Some(encode_batch(&test_batch())),
    );
    send_manual_message(
        socket,
        "STOP",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(4),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        None,
    );

    handle.join().unwrap();

    let events = sink.snapshot();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, RecordedEventSnapshot::Hello { .. }))
            .count(),
        1
    );
    let data_events = events
        .iter()
        .filter(|event| matches!(event, RecordedEventSnapshot::Data(_)))
        .count();
    assert_eq!(data_events, MANUAL_SEND_REPEAT_COUNT);
    assert!(events
        .iter()
        .any(|event| *event == RecordedEventSnapshot::Stop));
}

#[test]
fn zmq_source_fails_fast_on_schema_mismatch() {
    let manual_publisher = bind_manual_publisher();
    let endpoint = manual_publisher.endpoint().to_string();
    let (handle, _sink) = start_source(&endpoint);
    let socket = &manual_publisher.socket;

    sleep_for_pubsub();
    send_manual_message(
        socket,
        "HELLO",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            None,
            Some(&expected_schema_hash(&other_schema())),
            None,
        ),
        Some(encode_schema(&other_schema())),
    );

    let error = handle.join().unwrap_err();
    assert!(error.to_string().contains("schema"));
}

#[test]
fn zmq_source_accepts_repeated_consistent_hello() {
    let manual_publisher = bind_manual_publisher();
    let endpoint = manual_publisher.endpoint().to_string();
    let (handle, sink) = start_source(&endpoint);
    let socket = &manual_publisher.socket;

    sleep_for_pubsub();
    send_manual_message(
        socket,
        "HELLO",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(1),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        Some(encode_schema(&test_schema())),
    );
    thread::sleep(Duration::from_millis(100));
    send_manual_message(
        socket,
        "HELLO",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(2),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        Some(encode_schema(&test_schema())),
    );
    send_manual_message(
        socket,
        "DATA",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(3),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        Some(encode_batch(&test_batch())),
    );
    send_manual_message(
        socket,
        "STOP",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(4),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        None,
    );

    handle.join().unwrap();

    let events = sink.snapshot();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, RecordedEventSnapshot::Hello { .. }))
            .count(),
        1
    );
    assert!(events
        .iter()
        .any(|event| matches!(event, RecordedEventSnapshot::Data(_))));
    assert!(events
        .iter()
        .any(|event| *event == RecordedEventSnapshot::Stop));
}

#[test]
fn zmq_source_rejects_repeated_inconsistent_hello() {
    let manual_publisher = bind_manual_publisher();
    let endpoint = manual_publisher.endpoint().to_string();
    let (handle, _sink) = start_source(&endpoint);
    let socket = &manual_publisher.socket;

    sleep_for_pubsub();
    send_manual_message(
        socket,
        "HELLO",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(1),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        Some(encode_schema(&test_schema())),
    );
    thread::sleep(Duration::from_millis(100));
    send_manual_message(
        socket,
        "HELLO",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(2),
            Some("deadbeef"),
            None,
        ),
        Some(encode_schema(&test_schema())),
    );

    let error = handle.join().unwrap_err();
    assert!(error.to_string().contains("hello") || error.to_string().contains("hash"));
}

#[test]
fn zmq_source_rejects_hello_without_schema_hash() {
    let manual_publisher = bind_manual_publisher();
    let endpoint = manual_publisher.endpoint().to_string();
    let (handle, _sink) = start_source(&endpoint);
    let socket = &manual_publisher.socket;

    sleep_for_pubsub();
    send_manual_message(
        socket,
        "HELLO",
        encode_meta(PROTOCOL_VERSION, STREAM_NAME, Some(1), None, None),
        Some(encode_schema(&test_schema())),
    );

    let error = handle.join().unwrap_err();
    assert!(error.to_string().contains("schema_hash"));
}

#[test]
fn zmq_source_rejects_flush_without_seq_no() {
    let manual_publisher = bind_manual_publisher();
    let endpoint = manual_publisher.endpoint().to_string();
    let (handle, _sink) = start_source(&endpoint);
    let socket = &manual_publisher.socket;

    sleep_for_pubsub();
    send_manual_message(
        socket,
        "HELLO",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(1),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        Some(encode_schema(&test_schema())),
    );
    thread::sleep(Duration::from_millis(100));
    send_manual_message(
        socket,
        "FLUSH",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            None,
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        None,
    );

    let error = handle.join().unwrap_err();
    assert!(error.to_string().contains("seq"));
}

#[test]
fn zmq_source_rejects_stop_without_seq_no() {
    let manual_publisher = bind_manual_publisher();
    let endpoint = manual_publisher.endpoint().to_string();
    let (handle, _sink) = start_source(&endpoint);
    let socket = &manual_publisher.socket;

    sleep_for_pubsub();
    send_manual_message(
        socket,
        "HELLO",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            Some(1),
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        Some(encode_schema(&test_schema())),
    );
    thread::sleep(Duration::from_millis(100));
    send_manual_message(
        socket,
        "STOP",
        encode_meta(
            PROTOCOL_VERSION,
            STREAM_NAME,
            None,
            Some(&expected_schema_hash(&test_schema())),
            None,
        ),
        None,
    );

    let error = handle.join().unwrap_err();
    assert!(error.to_string().contains("seq"));
}

#[test]
fn zmq_source_publisher_emits_seq_no_on_flush_and_stop() {
    let mut publisher =
        ZmqStreamPublisher::bind(WILDCARD_ENDPOINT, STREAM_NAME, test_schema()).unwrap();
    let endpoint = publisher.last_endpoint().unwrap();
    let context = zmq::Context::new();
    let subscriber = context.socket(zmq::SUB).unwrap();
    subscriber.set_subscribe(b"").unwrap();
    subscriber.set_rcvtimeo(1_000).unwrap();
    subscriber.connect(&endpoint).unwrap();
    sleep_for_pubsub();
    publisher.publish_hello().unwrap();
    let _startup_hello = subscriber.recv_multipart(0).unwrap();

    publisher.flush().unwrap();
    publisher.close().unwrap();

    let mut flush_seq_no = None;
    let mut stop_seq_no = None;
    for _ in 0..6 {
        let frames = subscriber.recv_multipart(0).unwrap();
        let meta = decode_meta(&frames[1]);
        match frames[0].as_slice() {
            b"FLUSH" if flush_seq_no.is_none() => {
                flush_seq_no = meta_value(&meta, "seq_no").map(str::to_string);
            }
            b"STOP" if stop_seq_no.is_none() => {
                stop_seq_no = meta_value(&meta, "seq_no").map(str::to_string);
            }
            _ => {}
        }
    }

    assert!(flush_seq_no.is_some());
    assert!(stop_seq_no.is_some());
}

#[test]
fn zmq_source_publisher_repeats_hello_during_runtime() {
    let mut publisher =
        ZmqStreamPublisher::bind(WILDCARD_ENDPOINT, STREAM_NAME, test_schema()).unwrap();
    let endpoint = publisher.last_endpoint().unwrap();
    let context = zmq::Context::new();
    let subscriber = context.socket(zmq::SUB).unwrap();
    subscriber.set_subscribe(b"").unwrap();
    subscriber.set_rcvtimeo(1_000).unwrap();
    subscriber.connect(&endpoint).unwrap();
    sleep_for_pubsub();
    publisher.publish_hello().unwrap();
    let first_hello = subscriber.recv_multipart(0).unwrap();
    let first_hello_meta = decode_meta(&first_hello[1]);

    thread::sleep(Duration::from_millis(300));
    publisher.publish(&test_batch()).unwrap();

    let mut second_hello_meta = None;
    let mut saw_data = false;
    for _ in 0..4 {
        let frames = subscriber.recv_multipart(0).unwrap();
        match frames[0].as_slice() {
            b"HELLO" if second_hello_meta.is_none() => {
                second_hello_meta = Some(decode_meta(&frames[1]));
            }
            b"DATA" => {
                saw_data = true;
                break;
            }
            _ => {}
        }
    }

    assert!(meta_value(&first_hello_meta, "seq_no").is_some());
    let second_hello_meta = second_hello_meta.expect("expected repeated hello before data");
    assert!(meta_value(&second_hello_meta, "seq_no").is_some());
    assert_ne!(
        meta_value(&first_hello_meta, "seq_no"),
        meta_value(&second_hello_meta, "seq_no")
    );
    assert!(saw_data);
}

#[test]
fn zmq_source_publisher_repeats_hello_while_idle() {
    let publisher =
        ZmqStreamPublisher::bind(WILDCARD_ENDPOINT, STREAM_NAME, test_schema()).unwrap();
    let endpoint = publisher.last_endpoint().unwrap();
    let context = zmq::Context::new();
    let subscriber = context.socket(zmq::SUB).unwrap();
    subscriber.set_subscribe(b"").unwrap();
    subscriber.set_rcvtimeo(1_000).unwrap();
    subscriber.connect(&endpoint).unwrap();
    sleep_for_pubsub();

    let first_hello = subscriber.recv_multipart(0).unwrap();
    let second_hello = subscriber.recv_multipart(0).unwrap();

    assert_eq!(first_hello[0], b"HELLO");
    assert_eq!(second_hello[0], b"HELLO");

    let first_hello_meta = decode_meta(&first_hello[1]);
    let second_hello_meta = decode_meta(&second_hello[1]);
    assert_ne!(
        meta_value(&first_hello_meta, "seq_no"),
        meta_value(&second_hello_meta, "seq_no")
    );
}
