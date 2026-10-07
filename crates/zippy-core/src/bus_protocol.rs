//! Process leases and configuration discovery for the native server control listener.

use serde::{Deserialize, Serialize};
use std::fmt;
use zippy_types::ErrorCode;

/// Long-poll cap that keeps each authenticated control connection bounded.
pub const MAX_CONTROL_WATCH_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterProcessRequest {
    pub app: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchRequest {
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    pub resource: WatchResource,
    pub after_revision: u64,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEvent {
    pub resource: WatchResource,
    pub revision: u64,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnregisterProcessRequest {
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetConfigRequest {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchResource {
    Shutdown,
    Process { process_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRequest {
    RegisterProcess(RegisterProcessRequest),
    Heartbeat(HeartbeatRequest),
    Watch(WatchRequest),
    UnregisterProcess(UnregisterProcessRequest),
    GetConfig(GetConfigRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    ProcessRegistered {
        process_id: String,
        process_token: String,
    },
    HeartbeatAccepted {
        process_id: String,
    },
    ShutdownRequested {
        process_id: String,
        reason: String,
    },
    ProcessUnregistered {
        process_id: String,
    },
    ConfigFetched {
        config: serde_json::Value,
    },
    ResourceChanged {
        event: Option<ResourceEvent>,
    },
    Error {
        code: ErrorCode,
        retryable: bool,
        reason: String,
    },
}

impl fmt::Display for ControlResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProcessRegistered { process_id, .. } => {
                write!(f, "process registered process_id=[{}]", process_id)
            }
            Self::HeartbeatAccepted { process_id } => {
                write!(f, "heartbeat accepted process_id=[{}]", process_id)
            }
            Self::ShutdownRequested { process_id, reason } => write!(
                f,
                "shutdown requested process_id=[{}] reason=[{}]",
                process_id, reason
            ),
            Self::ProcessUnregistered { process_id } => {
                write!(f, "process unregistered process_id=[{}]", process_id)
            }
            Self::ConfigFetched { .. } => write!(f, "config fetched"),
            Self::ResourceChanged { event } => {
                write!(f, "resource changed has_event=[{}]", event.is_some())
            }
            Self::Error {
                code,
                reason,
                retryable,
            } => write!(
                f,
                "control error code=[{}] retryable=[{}] reason=[{}]",
                code, retryable, reason
            ),
        }
    }
}
