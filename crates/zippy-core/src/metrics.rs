use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EngineMetricsDelta {
    pub late_rows_total: u64,
}

#[derive(Default)]
pub struct EngineMetrics {
    processed_batches_total: AtomicU64,
    processed_rows_total: AtomicU64,
    output_batches_total: AtomicU64,
    dropped_batches_total: AtomicU64,
    late_rows_total: AtomicU64,
    publish_errors_total: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EngineMetricsSnapshot {
    pub processed_batches_total: u64,
    pub processed_rows_total: u64,
    pub output_batches_total: u64,
    pub dropped_batches_total: u64,
    pub late_rows_total: u64,
    pub publish_errors_total: u64,
    pub queue_depth: usize,
}

impl EngineMetrics {
    pub fn inc_processed_batch(&self, rows: usize) {
        self.processed_batches_total.fetch_add(1, Ordering::Relaxed);
        self.processed_rows_total
            .fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub fn inc_output_batches(&self, count: usize) {
        self.output_batches_total
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn inc_dropped_batches(&self, count: usize) {
        self.dropped_batches_total
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn inc_late_rows(&self, count: u64) {
        self.late_rows_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_publish_errors(&self, count: u64) {
        self.publish_errors_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn apply_delta(&self, delta: EngineMetricsDelta) {
        self.inc_late_rows(delta.late_rows_total);
    }

    pub fn snapshot(&self, queue_depth: usize) -> EngineMetricsSnapshot {
        EngineMetricsSnapshot {
            processed_batches_total: self.processed_batches_total.load(Ordering::Relaxed),
            processed_rows_total: self.processed_rows_total.load(Ordering::Relaxed),
            output_batches_total: self.output_batches_total.load(Ordering::Relaxed),
            dropped_batches_total: self.dropped_batches_total.load(Ordering::Relaxed),
            late_rows_total: self.late_rows_total.load(Ordering::Relaxed),
            publish_errors_total: self.publish_errors_total.load(Ordering::Relaxed),
            queue_depth,
        }
    }
}
