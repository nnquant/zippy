use std::sync::Arc;
#[cfg(feature = "zmq-publisher")]
use std::{io::Cursor, thread, time::Duration};

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(feature = "zmq-publisher")]
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
#[cfg(not(feature = "zmq-publisher"))]
use zippy_core::ZippyError;
#[cfg(not(feature = "zmq-publisher"))]
use zippy_io::ZmqPublisher;
use zippy_io::{NullPublisher, Publisher};
#[cfg(feature = "zmq-publisher")]
use zippy_io::{ZmqPublisher, ZmqSubscriber};

#[test]
fn null_publisher_accepts_arrow_batches() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Float64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![1.0]))]).unwrap();

    let mut publisher = NullPublisher::default();
    publisher.publish(&batch).unwrap();
    publisher.flush().unwrap();

    assert_eq!(publisher.published_batches(), 1);
}

#[cfg(not(feature = "zmq-publisher"))]
#[test]
fn zmq_publisher_bind_fails_when_feature_is_disabled() {
    let error = match ZmqPublisher::bind("inproc://zippy-test") {
        Ok(_) => panic!("expected disabled zmq feature to return an io error"),
        Err(error) => error,
    };

    assert!(matches!(error, ZippyError::Io { .. }));
}

#[cfg(feature = "zmq-publisher")]
#[test]
fn zmq_publisher_binds_and_publishes_arrow_batches() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Float64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![1.0]))]).unwrap();
    let mut publisher = ZmqPublisher::bind("tcp://127.0.0.1:*").unwrap();

    publisher.publish(&batch).unwrap();
}

#[cfg(feature = "zmq-publisher")]
#[test]
fn zmq_publisher_roundtrips_arrow_batches_to_subscriber() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Float64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![1.0]))]).unwrap();
    let mut publisher = ZmqPublisher::bind("tcp://127.0.0.1:*").unwrap();
    let endpoint = publisher.last_endpoint().unwrap();
    let context = zmq::Context::new();
    let subscriber = context.socket(zmq::SUB).unwrap();

    subscriber.set_subscribe(b"").unwrap();
    subscriber.set_rcvtimeo(1_000).unwrap();
    subscriber.connect(&endpoint).unwrap();
    thread::sleep(Duration::from_millis(100));

    publisher.publish(&batch).unwrap();

    let payload = subscriber.recv_bytes(0).unwrap();
    let mut reader = StreamReader::try_new(Cursor::new(payload), None).unwrap();
    let received = reader.next().unwrap().unwrap();

    assert_eq!(received, batch);
}

#[cfg(feature = "zmq-publisher")]
#[test]
fn zmq_subscriber_receives_arrow_batches_from_publisher() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Float64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![2.0]))]).unwrap();
    let mut publisher = ZmqPublisher::bind("tcp://127.0.0.1:*").unwrap();
    let endpoint = publisher.last_endpoint().unwrap();
    let mut subscriber = ZmqSubscriber::connect(&endpoint, 1_000).unwrap();

    thread::sleep(Duration::from_millis(100));
    publisher.publish(&batch).unwrap();

    let received = subscriber.recv().unwrap();

    assert_eq!(received, batch);
}
