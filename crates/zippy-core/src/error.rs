use thiserror::Error;

pub type Result<T> = std::result::Result<T, ZippyError>;

#[derive(Debug, Error, Clone)]
pub enum ZippyError {
    #[error("service error code=[{code}] reason=[{reason}]")]
    Service {
        code: zippy_types::ErrorCode,
        reason: String,
        retryable: bool,
    },
    #[error("authentication failed reason=[{reason}]")]
    Unauthenticated { reason: String },
    #[error("invalid config reason=[{reason}]")]
    InvalidConfig { reason: String },
    #[error("resource exhausted reason=[{reason}]")]
    ResourceExhausted { reason: String },
    #[error("resource not found reason=[{reason}]")]
    NotFound { reason: String },
    #[error("operation conflicts with current state reason=[{reason}]")]
    Conflict { reason: String },
    #[error("data corruption reason=[{reason}]")]
    DataCorruption { reason: String },
    #[error("schema mismatch reason=[{reason}]")]
    SchemaMismatch { reason: String },
    #[error("operation cancelled reason=[{reason}]")]
    Cancelled { reason: String },
    #[error("deadline exceeded reason=[{reason}]")]
    DeadlineExceeded { reason: String },
    #[error("late data detected dt=[{dt}] last_dt=[{last_dt}]")]
    LateData { dt: i64, last_dt: i64 },
    #[error("engine state invalid status=[{status}]")]
    InvalidState { status: &'static str },
    #[error("master shutdown requested process_id=[{process_id}] reason=[{reason}]")]
    MasterShutdownRequested { process_id: String, reason: String },
    #[error("io error reason=[{reason}]")]
    Io { reason: String },
}

impl ZippyError {
    /// Returns the retry policy without inspecting diagnostic text.
    pub fn retryable(&self) -> bool {
        match self {
            Self::Service { retryable, .. } => *retryable,
            _ => self.code().retryable_by_default(),
        }
    }

    /// Return the structured service code without inspecting diagnostic text.
    pub fn code(&self) -> zippy_types::ErrorCode {
        use zippy_types::ErrorCode;
        match self {
            Self::Service { code, .. } => *code,
            Self::Unauthenticated { .. } => ErrorCode::Unauthenticated,
            Self::InvalidConfig { .. } => ErrorCode::InvalidArgument,
            Self::ResourceExhausted { .. } => ErrorCode::ResourceExhausted,
            Self::NotFound { .. } => ErrorCode::NotFound,
            Self::Conflict { .. } => ErrorCode::Conflict,
            Self::DataCorruption { .. } => ErrorCode::DataCorruption,
            Self::SchemaMismatch { .. } => ErrorCode::SchemaMismatch,
            Self::Cancelled { .. } => ErrorCode::Cancelled,
            Self::DeadlineExceeded { .. } => ErrorCode::DeadlineExceeded,
            Self::MasterShutdownRequested { .. } | Self::InvalidState { .. } | Self::Io { .. } => {
                ErrorCode::Unavailable
            }
            Self::LateData { .. } => ErrorCode::Internal,
        }
    }
}
