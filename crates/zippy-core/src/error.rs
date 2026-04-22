use thiserror::Error;

pub type Result<T> = std::result::Result<T, ZippyError>;

#[derive(Debug, Error, Clone)]
pub enum ZippyError {
    #[error("invalid config reason=[{reason}]")]
    InvalidConfig { reason: String },
    #[error("schema mismatch reason=[{reason}]")]
    SchemaMismatch { reason: String },
    #[error("late data detected dt=[{dt}] last_dt=[{last_dt}]")]
    LateData { dt: i64, last_dt: i64 },
    #[error("engine state invalid status=[{status}]")]
    InvalidState { status: &'static str },
    #[error("channel send failed")]
    ChannelSend,
    #[error("channel receive failed")]
    ChannelReceive,
    #[error("queue closed")]
    QueueClosed,
    #[error("io error reason=[{reason}]")]
    Io { reason: String },
}
