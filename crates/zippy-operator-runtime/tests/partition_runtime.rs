use std::sync::{Arc, Mutex};

use zippy_operator_runtime::{Operator, PartitionRuntime};

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
