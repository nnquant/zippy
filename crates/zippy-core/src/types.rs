#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LateDataPolicy {
    #[default]
    Reject,
    DropWithMetric,
}
