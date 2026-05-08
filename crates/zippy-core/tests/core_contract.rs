use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use zippy_core::{
    ControlRequest, Engine, EngineStatus, LateDataPolicy, OverflowPolicy, Result, SegmentTableView,
};

struct NoopEngine {
    schema: Arc<Schema>,
}

impl Engine for NoopEngine {
    fn name(&self) -> &str {
        "noop"
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn on_data(&mut self, _batch: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        Ok(vec![])
    }
}

#[test]
fn core_types_match_v1_contract() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Float64,
        false,
    )]));
    let engine = NoopEngine { schema };

    assert_eq!(engine.name(), "noop");
    assert_eq!(OverflowPolicy::default(), OverflowPolicy::Block);
    assert_eq!(LateDataPolicy::default(), LateDataPolicy::Reject);
    assert_eq!(EngineStatus::Created.as_str(), "created");
}

#[test]
fn control_protocol_rejects_removed_legacy_wait_requests() {
    let wait_shutdown = serde_json::json!({
        "WaitShutdown": {
            "process_id": "proc_1",
            "timeout_ms": 1,
        },
    });
    let wait_segment_descriptor = serde_json::json!({
        "WaitSegmentDescriptor": {
            "stream_name": "ticks",
            "process_id": "proc_1",
            "after_descriptor_generation": 0,
            "timeout_ms": 1,
        },
    });

    assert!(serde_json::from_value::<ControlRequest>(wait_shutdown).is_err());
    assert!(serde_json::from_value::<ControlRequest>(wait_segment_descriptor).is_err());
}
