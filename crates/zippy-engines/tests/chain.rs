use std::mem::size_of;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, Result, SchemaRef, SegmentTableView, ZippyError};
use zippy_engines::{EngineChain, IdentityEngine};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}
fn batch(value: i64) -> SegmentTableView {
    SegmentTableView::from_record_batch(
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
        )
        .unwrap(),
    )
}
fn values(tables: Vec<SegmentTableView>) -> Vec<i64> {
    tables
        .into_iter()
        .map(|table| {
            table
                .column_at(0)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        })
        .collect()
}

struct Buffer {
    name: &'static str,
    pending: Vec<SegmentTableView>,
    trace: Arc<Mutex<Vec<String>>>,
    fail: bool,
    fanout: usize,
}
impl Engine for Buffer {
    fn name(&self) -> &str {
        self.name
    }
    fn input_schema(&self) -> SchemaRef {
        schema()
    }
    fn output_schema(&self) -> SchemaRef {
        schema()
    }
    fn on_data(&mut self, table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        self.trace
            .lock()
            .unwrap()
            .push(format!("{}.data", self.name));
        if self.fail {
            return Err(ZippyError::InvalidState {
                status: "injected failure",
            });
        }
        if self.fanout > 0 {
            return Ok(vec![table; self.fanout]);
        }
        self.pending.push(table);
        Ok(vec![])
    }
    fn on_flush(&mut self) -> Result<Vec<SegmentTableView>> {
        self.trace
            .lock()
            .unwrap()
            .push(format!("{}.flush", self.name));
        Ok(std::mem::take(&mut self.pending))
    }
    fn on_stop(&mut self) -> Result<Vec<SegmentTableView>> {
        self.trace
            .lock()
            .unwrap()
            .push(format!("{}.stop", self.name));
        Ok(std::mem::take(&mut self.pending))
    }
}
fn buffer(
    name: &'static str,
    trace: &Arc<Mutex<Vec<String>>>,
    fail: bool,
    fanout: usize,
) -> Box<dyn Engine> {
    Box::new(Buffer {
        name,
        pending: Vec::new(),
        trace: trace.clone(),
        fail,
        fanout,
    })
}

#[test]
fn flush_and_stop_route_upstream_tail_before_closing_downstream_once() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let mut chain = EngineChain::new(
        "windows",
        vec![
            buffer("first", &trace, false, 0),
            buffer("second", &trace, false, 0),
        ],
        4096,
    )
    .unwrap();
    assert!(chain.on_data(batch(1)).unwrap().is_empty());
    assert!(chain.on_data(batch(2)).unwrap().is_empty());
    assert_eq!(values(chain.on_flush().unwrap()), vec![1, 2]);
    assert!(chain.on_flush().unwrap().is_empty());
    assert!(chain.on_data(batch(3)).unwrap().is_empty());
    assert_eq!(values(chain.on_stop().unwrap()), vec![3]);
    assert!(chain.on_stop().unwrap().is_empty());
    assert!(chain.on_data(batch(4)).is_err());
    assert_eq!(
        *trace.lock().unwrap(),
        vec![
            "first.data",
            "first.data",
            "first.flush",
            "second.data",
            "second.data",
            "second.flush",
            "first.flush",
            "second.flush",
            "first.data",
            "first.stop",
            "second.data",
            "second.stop"
        ]
    );
}

#[test]
fn execution_failure_is_terminal_and_does_not_expose_partial_output() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let mut chain = EngineChain::new(
        "failure",
        vec![
            buffer("first", &trace, false, 1),
            buffer("second", &trace, true, 0),
        ],
        4096,
    )
    .unwrap();
    assert!(chain.on_data(batch(1)).is_err());
    assert!(chain.on_data(batch(2)).is_err());
    assert!(chain.on_flush().is_err());
    assert!(chain.on_stop().is_err());
    assert_eq!(*trace.lock().unwrap(), vec!["first.data", "second.data"]);
}

#[test]
fn output_budget_counts_fanout_even_with_shared_arrow_buffers() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let maximum =
        batch(1).to_record_batch().unwrap().get_array_memory_size() + size_of::<RecordBatch>();
    let mut chain =
        EngineChain::new("fanout", vec![buffer("expand", &trace, false, 3)], maximum).unwrap();
    let error = chain.on_data(batch(1)).unwrap_err();
    assert!(error.to_string().contains("byte limit"));
    assert!(chain.on_flush().is_err());
}

#[test]
fn identity_preserves_schema_buffers_and_complete_batches() {
    let mut engine = IdentityEngine::new("raw", schema());
    let input = batch(7);
    let column = input.column_at(0).unwrap();
    let output = engine.on_data(input).unwrap();
    assert!(Arc::ptr_eq(&column, &output[0].column_at(0).unwrap()));
    assert_eq!(values(output), vec![7]);
}

#[test]
fn schema_rejection_happens_before_execution_and_constructor_checks_metadata() {
    let changed = Arc::new(
        schema()
            .as_ref()
            .clone()
            .with_metadata([("source".into(), "other".into())].into()),
    );
    assert!(EngineChain::new(
        "invalid",
        vec![
            Box::new(IdentityEngine::new("a", schema())),
            Box::new(IdentityEngine::new("b", changed.clone()))
        ],
        4096
    )
    .is_err());
    let trace = Arc::new(Mutex::new(Vec::new()));
    let mut chain =
        EngineChain::new("valid", vec![buffer("first", &trace, false, 1)], 4096).unwrap();
    let wrong = RecordBatch::try_new(changed, vec![batch(1).column_at(0).unwrap()]).unwrap();
    assert!(chain
        .on_data(SegmentTableView::from_record_batch(wrong))
        .is_err());
    assert!(trace.lock().unwrap().is_empty());
    assert_eq!(values(chain.on_data(batch(2)).unwrap()), vec![2]);
    assert!(EngineChain::new("empty", Vec::new(), 4096).is_err());
    assert!(EngineChain::new(
        "zero",
        vec![Box::new(IdentityEngine::new("raw", schema()))],
        0
    )
    .is_err());
}

#[test]
fn flush_budget_includes_outputs_from_all_stages() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let maximum =
        batch(1).to_record_batch().unwrap().get_array_memory_size() + size_of::<RecordBatch>();
    let first = Buffer {
        name: "first",
        pending: vec![batch(1)],
        trace: trace.clone(),
        fail: false,
        fanout: 0,
    };
    let second = Buffer {
        name: "second",
        pending: vec![batch(2)],
        trace,
        fail: false,
        fanout: 1,
    };
    let mut chain =
        EngineChain::new("flush", vec![Box::new(first), Box::new(second)], maximum).unwrap();
    assert!(chain
        .on_flush()
        .unwrap_err()
        .to_string()
        .contains("byte limit"));
    assert!(chain.on_stop().is_err());
}

#[test]
fn flush_cannot_redefine_its_declared_output_schema() {
    struct Drift(SchemaRef);
    impl Engine for Drift {
        fn name(&self) -> &str {
            "drift"
        }
        fn input_schema(&self) -> SchemaRef {
            schema()
        }
        fn output_schema(&self) -> SchemaRef {
            self.0.clone()
        }
        fn on_data(&mut self, _: SegmentTableView) -> Result<Vec<SegmentTableView>> {
            Ok(vec![])
        }
        fn on_flush(&mut self) -> Result<Vec<SegmentTableView>> {
            self.0 = Arc::new(
                schema()
                    .as_ref()
                    .clone()
                    .with_metadata([("changed".into(), "true".into())].into()),
            );
            Ok(vec![SegmentTableView::from_record_batch(
                RecordBatch::try_new(self.0.clone(), vec![batch(1).column_at(0)?]).unwrap(),
            )])
        }
    }
    let mut chain = EngineChain::new("drift", vec![Box::new(Drift(schema()))], 4096).unwrap();
    assert!(chain
        .on_flush()
        .unwrap_err()
        .to_string()
        .contains("declared output schema"));
    assert!(chain.on_stop().is_err());
}
