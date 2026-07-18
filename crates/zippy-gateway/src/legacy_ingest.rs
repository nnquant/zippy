//! State-hiding compatibility write facade for standard protocol frontends.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use zippy_core::{Result, ZippyError};

use super::GatewayState;

const MAX_COMPATIBILITY_STREAM_NAME_BYTES: usize = 512;

/// Cloneable append entry point that reuses the Gateway materializer authority.
#[derive(Clone)]
pub struct LegacyWriteFacade {
    state: Arc<GatewayState>,
}

impl LegacyWriteFacade {
    pub(crate) fn new(state: Arc<GatewayState>) -> Self {
        Self { state }
    }

    /// Applies and flushes one Arrow batch through the existing table writer.
    ///
    /// This is a process-local memory acknowledgement. It intentionally does
    /// not claim producer fencing, WAL recovery, or persisted durability.
    pub fn write_batch(&self, stream_name: &str, batch: RecordBatch) -> Result<()> {
        if self.state.stopped.load(Ordering::Acquire) {
            return Err(ZippyError::InvalidState {
                status: "gateway is stopping",
            });
        }
        if stream_name.trim().is_empty() || stream_name.len() > MAX_COMPATIBILITY_STREAM_NAME_BYTES
        {
            return Err(ZippyError::InvalidConfig {
                reason: "legacy ingest stream name is empty or too large".to_string(),
            });
        }
        let row_count = batch.num_rows();
        if self
            .state
            .max_write_rows
            .is_some_and(|maximum| row_count > maximum)
        {
            self.state
                .increment_metric(|metrics| metrics.write_rejections_total += 1);
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "write batch row count exceeds limit rows=[{row_count}] max_write_rows=[{}]",
                    self.state.max_write_rows.unwrap_or_default()
                ),
            });
        }
        self.state.write_batch(stream_name, batch)?;
        self.state.increment_metric(|metrics| {
            metrics.write_batches_total += 1;
            metrics.written_rows_total = metrics
                .written_rows_total
                .saturating_add(u64::try_from(row_count).unwrap_or(u64::MAX));
        });
        Ok(())
    }
}
