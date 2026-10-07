use std::path::PathBuf;
use std::time::Duration;

use crate::bus_protocol::{
    ControlRequest, ControlResponse, GetConfigRequest, HeartbeatRequest, RegisterProcessRequest,
    UnregisterProcessRequest, WatchRequest, WatchResource, MAX_CONTROL_WATCH_TIMEOUT_MS,
};
use crate::{
    resolve_control_endpoint, send_control_line_request_with_timeout, ControlEndpoint, Result,
    ZippyConfig, ZippyError,
};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const WATCH_RESPONSE_GRACE: Duration = Duration::from_secs(5);

/// Synchronous control-plane client for zippy-master.
#[derive(Debug, Clone)]
pub struct MasterClient {
    endpoint: ControlEndpoint,
    process_id: Option<String>,
    process_token: Option<String>,
    process_app: Option<String>,
    request_timeout: Duration,
}

impl MasterClient {
    pub fn connect(socket_path: impl Into<PathBuf>) -> Result<Self> {
        Self::connect_endpoint(ControlEndpoint::Unix(socket_path.into()))
    }

    pub fn connect_uri(uri: impl AsRef<str>) -> Result<Self> {
        Self::connect_endpoint(resolve_control_endpoint(uri.as_ref())?)
    }

    pub fn connect_endpoint(endpoint: ControlEndpoint) -> Result<Self> {
        Ok(Self {
            endpoint,
            process_id: None,
            process_token: None,
            process_app: None,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        })
    }

    pub fn endpoint(&self) -> &ControlEndpoint {
        &self.endpoint
    }

    pub fn process_id(&self) -> Option<&str> {
        self.process_id.as_deref()
    }

    pub fn process_token(&self) -> Option<&str> {
        self.process_token.as_deref()
    }

    pub fn process_app(&self) -> Option<&str> {
        self.process_app.as_deref()
    }

    /// Applies one positive timeout to all subsequent control requests.
    pub fn set_request_timeout(&mut self, timeout: Duration) -> Result<()> {
        if timeout.is_zero() {
            return Err(ZippyError::InvalidConfig {
                reason: "master client request timeout must be positive".to_string(),
            });
        }
        self.request_timeout = timeout;
        Ok(())
    }

    /// Returns the positive deadline budget for ordinary control requests.
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub fn register_process(&mut self, app: &str) -> Result<String> {
        let response =
            self.send_request(ControlRequest::RegisterProcess(RegisterProcessRequest {
                app: app.to_string(),
            }))?;

        match response {
            ControlResponse::ProcessRegistered {
                process_id,
                process_token,
            } => {
                self.process_id = Some(process_id.clone());
                self.process_token = Some(process_token);
                self.process_app = Some(app.to_string());
                Ok(process_id)
            }
            other => Err(unexpected_response("ProcessRegistered", other)),
        }
    }

    pub fn reregister_process(&mut self) -> Result<String> {
        let app = self.process_app.clone().ok_or(ZippyError::InvalidState {
            status: "master client process app missing",
        })?;
        self.process_id = None;
        self.process_token = None;
        self.register_process(&app)
    }

    pub fn heartbeat(&self) -> Result<()> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let response = self.send_request(ControlRequest::Heartbeat(HeartbeatRequest {
            process_id,
            process_token,
        }))?;

        match response {
            ControlResponse::HeartbeatAccepted { .. } => Ok(()),
            ControlResponse::ShutdownRequested { process_id, reason } => {
                Err(ZippyError::MasterShutdownRequested { process_id, reason })
            }
            other => Err(unexpected_response("HeartbeatAccepted", other)),
        }
    }

    pub fn wait_shutdown(&self, timeout: Duration) -> Result<bool> {
        match self.watch_resource(WatchResource::Shutdown, 0, timeout)? {
            None => Ok(false),
            Some(event) => match event.resource {
                WatchResource::Shutdown => {
                    let reason = event
                        .payload
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("master shutdown requested")
                        .to_string();
                    let process_id = self.require_process_id()?;
                    Err(ZippyError::MasterShutdownRequested { process_id, reason })
                }
                other => Err(unexpected_watch_resource("Shutdown", other)),
            },
        }
    }

    pub fn unregister_process(&mut self) -> Result<()> {
        let process_id = self.require_process_id()?;
        let process_token = Some(self.require_process_token()?);
        let response = self.send_request(ControlRequest::UnregisterProcess(
            UnregisterProcessRequest {
                process_id,
                process_token,
            },
        ))?;

        match response {
            ControlResponse::ProcessUnregistered { .. } => {
                self.process_id = None;
                self.process_token = None;
                self.process_app = None;
                Ok(())
            }
            other => Err(unexpected_response("ProcessUnregistered", other)),
        }
    }

    pub fn get_config(&self) -> Result<ZippyConfig> {
        let response = self.send_request(ControlRequest::GetConfig(GetConfigRequest {}))?;

        match response {
            ControlResponse::ConfigFetched { config } => ZippyConfig::from_wire_json(config),
            other => Err(unexpected_response("ConfigFetched", other)),
        }
    }

    pub fn watch_resource(
        &self,
        resource: WatchResource,
        after_revision: u64,
        timeout: Duration,
    ) -> Result<Option<crate::ResourceEvent>> {
        let process_id = self.require_process_id()?;
        let process_token = self.require_process_token()?;
        let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        if timeout_ms > MAX_CONTROL_WATCH_TIMEOUT_MS {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "master client watch timeout exceeds maximum requested_ms=[{timeout_ms}] maximum_ms=[{MAX_CONTROL_WATCH_TIMEOUT_MS}]"
                ),
            });
        }
        // Bounded by MAX_CONTROL_WATCH_TIMEOUT_MS above, so the grace addition cannot overflow.
        let response_timeout = timeout + WATCH_RESPONSE_GRACE;
        let response = send_control_line_request_with_timeout(
            &self.endpoint,
            ControlRequest::Watch(WatchRequest {
                process_id,
                process_token: Some(process_token),
                resource,
                after_revision,
                timeout_ms,
            }),
            response_timeout.max(self.request_timeout),
        )?;
        match response {
            ControlResponse::ResourceChanged { event } => Ok(event),
            other => Err(unexpected_response("ResourceChanged", other)),
        }
    }

    fn require_process_id(&self) -> Result<String> {
        self.process_id.clone().ok_or(ZippyError::InvalidState {
            status: "master client process not registered",
        })
    }

    fn require_process_token(&self) -> Result<String> {
        self.process_token.clone().ok_or(ZippyError::InvalidState {
            status: "master client process token missing",
        })
    }

    fn send_request(&self, request: ControlRequest) -> Result<ControlResponse> {
        send_control_line_request_with_timeout(&self.endpoint, request, self.request_timeout)
    }
}

fn unexpected_response(expected: &str, response: ControlResponse) -> ZippyError {
    ZippyError::Io {
        reason: format!(
            "unexpected control response expected=[{}] actual=[{}]",
            expected, response
        ),
    }
}

fn unexpected_watch_resource(expected: &str, resource: WatchResource) -> ZippyError {
    ZippyError::Io {
        reason: format!(
            "unexpected watch resource expected=[{}] actual=[{:?}]",
            expected, resource
        ),
    }
}
