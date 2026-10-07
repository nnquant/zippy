#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EngineMetricsDelta {
    pub late_rows_total: u64,
    pub filtered_rows_total: u64,
}
