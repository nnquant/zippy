#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineStatus {
    Created,
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl EngineStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OverflowPolicy {
    #[default]
    Block,
    Reject,
    DropOldest,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LateDataPolicy {
    #[default]
    Reject,
    DropWithMetric,
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub name: String,
    pub buffer_capacity: usize,
    pub overflow_policy: OverflowPolicy,
    pub late_data_policy: LateDataPolicy,
    pub xfast: bool,
}

impl EngineConfig {
    pub fn validate(&self) -> crate::Result<()> {
        if self.name.is_empty() {
            return Err(crate::ZippyError::InvalidConfig {
                reason: "engine name must not be empty".to_string(),
            });
        }

        if self.buffer_capacity == 0 {
            return Err(crate::ZippyError::InvalidConfig {
                reason: "buffer capacity must be greater than zero".to_string(),
            });
        }

        Ok(())
    }
}
