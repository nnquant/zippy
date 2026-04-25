use std::sync::{Arc, Mutex};
use std::time::Duration;

use zippy_operator_runtime::{Operator, PartitionReader, PartitionRuntime};
use zippy_segment_store::{SegmentStore, SegmentStoreConfig};

struct CountingOperator {
    seen_rows: Arc<Mutex<Vec<usize>>>,
}

impl Operator for CountingOperator {
    fn name(&self) -> &'static str {
        "counting"
    }

    fn required_columns(&self) -> &'static [&'static str] {
        &["last_price"]
    }

    fn on_rows(&mut self, span: &zippy_segment_store::RowSpanView) -> Result<(), String> {
        self.seen_rows
            .lock()
            .unwrap()
            .push(span.end_row() - span.start_row());
        Ok(())
    }
}

#[test]
fn partition_runtime_fans_out_one_span_to_multiple_operators() {
    let seen_a = Arc::new(Mutex::new(Vec::new()));
    let seen_b = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = PartitionRuntime::for_test();

    runtime.add_operator(Box::new(CountingOperator {
        seen_rows: seen_a.clone(),
    }));
    runtime.add_operator(Box::new(CountingOperator {
        seen_rows: seen_b.clone(),
    }));

    runtime.dispatch_test_span(0, 3).unwrap();

    assert_eq!(*seen_a.lock().unwrap(), vec![3]);
    assert_eq!(*seen_b.lock().unwrap(), vec![3]);
}

#[test]
fn rolling_state_updates_incrementally() {
    let mut state = zippy_operator_runtime::RollingMeanState::new(3);
    state.push(1.0);
    state.push(2.0);
    state.push(3.0);
    assert_eq!(state.mean(), Some(2.0));

    state.push(4.0);
    assert_eq!(state.mean(), Some(3.0));
}

#[test]
#[should_panic(expected = "rolling mean window must be greater than zero")]
fn rolling_state_rejects_zero_window() {
    let _ = zippy_operator_runtime::RollingMeanState::new(0);
}

#[test]
fn partition_reader_waits_and_reads_incremental_span() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();

    let mut reader = PartitionReader::new(store, handle, "reader-a").unwrap();
    let first = reader
        .read_available()
        .unwrap()
        .expect("expected first span");
    assert_eq!((first.start_row(), first.end_row()), (0, 1));
    assert!(reader.read_available().unwrap().is_none());

    writer.append_tick_for_test(2, "rb2501", 4124.5).unwrap();
    assert!(reader.wait_timeout(Duration::from_millis(20)).unwrap());

    let second = reader
        .read_available()
        .unwrap()
        .expect("expected second span");
    assert_eq!((second.start_row(), second.end_row()), (1, 2));
}

#[test]
fn partition_reader_resets_cursor_when_active_segment_rolls_over() {
    let store = SegmentStore::new(SegmentStoreConfig {
        default_row_capacity: 2,
    })
    .unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.append_tick_for_test(2, "rb2501", 4124.5).unwrap();

    let mut reader = PartitionReader::new(store, handle, "reader-a").unwrap();
    let first = reader
        .read_available()
        .unwrap()
        .expect("expected first span");
    assert_eq!((first.start_row(), first.end_row()), (0, 2));

    writer.rollover().unwrap();
    writer.append_tick_for_test(3, "rb2501", 4125.5).unwrap();

    let second = reader
        .read_available()
        .unwrap()
        .expect("expected first span from new segment");
    assert_eq!((second.start_row(), second.end_row()), (0, 1));
}
