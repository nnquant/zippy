//! DataFusion integration for server-owned analytical queries.
//!
//! This crate is deliberately separate from the ingest and callback runtimes. It owns
//! per-session DataFusion policy and, in later slices, Zippy table providers and physical
//! operators. Keeping this boundary explicit prevents analytical planning or execution from
//! entering the real-time data hot path.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::num::NonZeroUsize;

use datafusion::execution::context::{SessionConfig, SessionContext};

/// Immutable policy used to create one query session context.
///
/// A context is intentionally not shared between client sessions. Catalog registration,
/// variables, and future session-level limits therefore cannot leak across owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuerySessionPolicy {
    target_partitions: NonZeroUsize,
}

impl QuerySessionPolicy {
    /// Creates a policy with an explicit optimizer target partition count.
    #[must_use]
    pub const fn new(target_partitions: NonZeroUsize) -> Self {
        Self { target_partitions }
    }

    /// Returns the optimizer target partition count for this session.
    #[must_use]
    pub const fn target_partitions(self) -> NonZeroUsize {
        self.target_partitions
    }

    /// Creates an isolated DataFusion session context using this policy.
    #[must_use]
    pub fn create_context(self) -> SessionContext {
        let config = SessionConfig::new().with_target_partitions(self.target_partitions.get());
        SessionContext::new_with_config(config)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    use super::QuerySessionPolicy;

    #[test]
    fn workspace_and_datafusion_record_batches_share_one_arrow_abi() {
        let values = Int64Array::from(vec![1_i64, 2, 3]);
        let values_ptr = values.values().as_ptr();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let workspace_batch =
            RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("build workspace batch");

        let datafusion_batch: datafusion::arrow::record_batch::RecordBatch =
            workspace_batch.clone();
        let round_trip: RecordBatch = datafusion_batch;
        let round_trip_values = round_trip
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("retain workspace Arrow array type");

        assert_eq!(round_trip_values.values().as_ptr(), values_ptr);
    }

    #[tokio::test]
    async fn isolated_session_executes_sql_over_workspace_record_batches() {
        let policy = QuerySessionPolicy::new(NonZeroUsize::new(2).unwrap());
        let context = policy.create_context();
        assert_eq!(context.state().config().target_partitions(), 2);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        context.register_table("numbers", Arc::new(table)).unwrap();

        let batches = context
            .sql("select value from numbers where value >= 2 order by value")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(policy.target_partitions().get(), 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[2, 3]
        );
    }
}
