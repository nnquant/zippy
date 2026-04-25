use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use zippy_core::{Engine, EngineStatus, LateDataPolicy, OverflowPolicy, Result, SegmentTableView};

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
