//! Boot-scoped process leases and typed runtime records; table authority lives in SQLite.
use crate::process_runtime::RuntimeStateStore;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
use zippy_types::runtime::{RuntimeCasUpdate, RuntimeObjectId, RuntimeRecord, RuntimeRegistration};
use zippy_types::{ErrorCode, Result as ServerResult, ServerError};

/// Maximum process records retained by the boot-scoped control registry.
pub const MAX_PROCESS_RECORDS: usize = 4_096;
/// Maximum UTF-8 byte length of a process application label.
pub const MAX_PROCESS_APP_BYTES: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessRecord {
    pub process_id: String,
    pub process_token: String,
    pub app: String,
    pub registered_at: u64,
    pub last_heartbeat_at: u64,
    pub lease_status: String,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    ProcessNotFound { process_id: String },
    ProcessLeaseExpired { process_id: String },
    ProcessTokenInvalid { process_id: String },
}
impl RegistryError {
    /// Returns the protocol error code for a process lease failure.
    pub fn code(&self) -> zippy_types::ErrorCode {
        match self {
            Self::ProcessNotFound { .. } => zippy_types::ErrorCode::NotFound,
            Self::ProcessLeaseExpired { .. } | Self::ProcessTokenInvalid { .. } => {
                zippy_types::ErrorCode::Unauthenticated
            }
        }
    }
    /// Returns the shared retry policy for this failure.
    pub fn retryable(&self) -> bool {
        self.code().retryable_by_default()
    }
}
impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProcessNotFound { process_id } => {
                write!(f, "process not found process_id=[{}]", process_id)
            }
            Self::ProcessLeaseExpired { process_id } => {
                write!(f, "process lease expired process_id=[{}]", process_id)
            }
            Self::ProcessTokenInvalid { process_id } => {
                write!(f, "process token invalid process_id=[{}]", process_id)
            }
        }
    }
}

impl std::error::Error for RegistryError {}
fn generate_process_token() -> Result<String, getrandom::Error> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn validate_process_app(app: &str) -> ServerResult<()> {
    if app.len() > MAX_PROCESS_APP_BYTES {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "process app exceeds byte limit length=[{}] maximum=[{}]",
                app.len(),
                MAX_PROCESS_APP_BYTES
            ),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Default)]
pub struct Registry {
    processes: BTreeMap<String, ProcessRecord>,
    next_process_id: u64,
    control_revision: u64,
    runtime_state: RuntimeStateStore,
}
impl Registry {
    #[cfg(test)]
    pub(crate) fn with_runtime_state(runtime_state: RuntimeStateStore) -> Self {
        Self {
            runtime_state,
            ..Self::default()
        }
    }

    pub(crate) const fn runtime_state(&self) -> &RuntimeStateStore {
        &self.runtime_state
    }

    pub(crate) fn runtime_register(
        &mut self,
        registration: RuntimeRegistration,
    ) -> ServerResult<RuntimeRecord> {
        self.runtime_state.register(registration)
    }

    pub(crate) fn runtime_compare_and_set(
        &mut self,
        update: RuntimeCasUpdate,
    ) -> ServerResult<RuntimeRecord> {
        self.runtime_state.compare_and_set(update)
    }

    pub(crate) fn runtime_remove(
        &mut self,
        object_id: RuntimeObjectId,
        expected_boot_id: zippy_types::SessionId,
        expected_revision: u64,
    ) -> ServerResult<()> {
        self.runtime_state
            .remove(object_id, expected_boot_id, expected_revision)
    }

    fn now_epoch_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn next_control_revision(&mut self) -> u64 {
        self.control_revision = self.control_revision.saturating_add(1).max(1);
        self.control_revision
    }

    pub fn register_process(&mut self, app: &str) -> ServerResult<String> {
        self.register_process_with_token(app, generate_process_token())
    }

    fn register_process_with_token(
        &mut self,
        app: &str,
        process_token: Result<String, getrandom::Error>,
    ) -> ServerResult<String> {
        validate_process_app(app)?;
        let process_token = process_token.map_err(|_| {
            ServerError::new(
                ErrorCode::Unavailable,
                "os randomness unavailable for process token",
            )
        })?;
        if self.processes.len() >= MAX_PROCESS_RECORDS {
            let expired_id = self
                .processes
                .values()
                .filter(|process| process.lease_status == "expired")
                .min_by_key(|process| (process.registered_at, process.process_id.as_str()))
                .map(|process| process.process_id.clone());
            let Some(expired_id) = expired_id else {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        "process registry is full maximum_records=[{}]",
                        MAX_PROCESS_RECORDS
                    ),
                ));
            };
            self.processes.remove(&expired_id);
            self.runtime_state.remove_control_process(&expired_id);
        }
        self.next_process_id += 1;
        let process_id = format!("proc_{}", self.next_process_id);
        let now = Self::now_epoch_millis();
        let revision = self.next_control_revision();
        let record = ProcessRecord {
            process_id: process_id.clone(),
            process_token,
            app: app.to_string(),
            registered_at: now,
            last_heartbeat_at: now,
            lease_status: "alive".to_string(),
            revision,
        };
        self.runtime_state.observe_process_registered(&record);
        self.processes.insert(process_id.clone(), record);
        Ok(process_id)
    }

    pub fn get_process_token(&self, process_id: &str) -> Option<&str> {
        self.processes
            .get(process_id)
            .map(|process| process.process_token.as_str())
    }

    pub fn record_heartbeat(&mut self, process_id: &str) -> Result<(), RegistryError> {
        self.alive_process(process_id)?;
        let revision = self.next_control_revision();
        let process = self.processes.get_mut(process_id).unwrap();
        process.last_heartbeat_at = Self::now_epoch_millis();
        process.revision = revision;
        self.runtime_state.observe_process_heartbeat(process);
        Ok(())
    }

    pub fn unregister_process(&mut self, process_id: &str) -> Result<(), RegistryError> {
        self.processes
            .remove(process_id)
            .ok_or_else(|| RegistryError::ProcessNotFound {
                process_id: process_id.to_string(),
            })?;
        self.runtime_state.remove_control_process(process_id);
        Ok(())
    }

    pub fn alive_process_ids(&self) -> Vec<String> {
        self.processes
            .values()
            .filter(|process| process.lease_status == "alive")
            .map(|process| process.process_id.clone())
            .collect()
    }

    pub fn validate_process_alive(&self, process_id: &str) -> Result<(), RegistryError> {
        self.alive_process(process_id).map(|_| ())
    }

    fn process(&self, process_id: &str) -> Result<&ProcessRecord, RegistryError> {
        self.processes
            .get(process_id)
            .ok_or_else(|| RegistryError::ProcessNotFound {
                process_id: process_id.to_string(),
            })
    }

    fn alive_process(&self, process_id: &str) -> Result<&ProcessRecord, RegistryError> {
        let process = self.process(process_id)?;
        if process.lease_status != "alive" {
            return Err(RegistryError::ProcessLeaseExpired {
                process_id: process_id.to_string(),
            });
        }
        Ok(process)
    }

    pub fn validate_process_capability(
        &self,
        process_id: &str,
        process_token: Option<&str>,
    ) -> Result<(), RegistryError> {
        let process = self.alive_process(process_id)?;
        if process_token != Some(process.process_token.as_str()) {
            return Err(RegistryError::ProcessTokenInvalid {
                process_id: process_id.to_string(),
            });
        }
        Ok(())
    }

    pub fn claim_expired_process(
        &mut self,
        process_id: &str,
        lease_timeout_millis: u64,
    ) -> Result<bool, RegistryError> {
        let process = self.process(process_id)?;
        if process.lease_status != "alive" {
            return Ok(false);
        }
        let now = Self::now_epoch_millis();
        if now.saturating_sub(process.last_heartbeat_at) < lease_timeout_millis {
            return Ok(false);
        }
        let revision = self.next_control_revision();
        let process = self.processes.get_mut(process_id).unwrap();
        process.lease_status = "expired".to_string();
        process.revision = revision;
        self.runtime_state.observe_process_failed(process);
        Ok(true)
    }

    #[cfg(test)]
    pub fn force_expire_process(&mut self, process_id: &str) -> Result<(), RegistryError> {
        let process = self.process(process_id)?;
        if process.lease_status == "expired" {
            return Ok(());
        }
        let revision = self.next_control_revision();
        let process = self.processes.get_mut(process_id).unwrap();
        process.lease_status = "expired".to_string();
        process.revision = revision;
        self.runtime_state.observe_process_failed(process);
        Ok(())
    }

    pub fn expired_processes(&self, lease_timeout_millis: u64) -> Vec<String> {
        let now = Self::now_epoch_millis();
        self.processes
            .values()
            .filter(|process| {
                process.lease_status == "alive"
                    && now.saturating_sub(process.last_heartbeat_at) >= lease_timeout_millis
            })
            .map(|process| process.process_id.clone())
            .collect()
    }

    pub fn processes_len(&self) -> usize {
        self.processes.len()
    }

    pub fn get_process(&self, process_id: &str) -> Option<&ProcessRecord> {
        self.processes.get(process_id)
    }

    pub fn process_update_for_process(
        &self,
        target_process_id: &str,
        watcher_process_id: &str,
        after_revision: u64,
    ) -> Result<Option<(u64, ProcessRecord)>, RegistryError> {
        self.validate_process_alive(watcher_process_id)?;
        let process = self.processes.get(target_process_id).ok_or_else(|| {
            RegistryError::ProcessNotFound {
                process_id: target_process_id.to_string(),
            }
        })?;
        if process.revision <= after_revision {
            return Ok(None);
        }
        Ok(Some((process.revision, process.clone())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_failure_does_not_register_process_or_advance_control_revision() {
        let mut registry = Registry::default();
        assert!(registry
            .register_process_with_token("worker", Err(getrandom::Error::UNSUPPORTED))
            .is_err());
        assert_eq!(registry.processes_len(), 0);
        assert_eq!(registry.control_revision, 0);
        assert_eq!(registry.next_process_id, 0);

        let process_id = registry.register_process("worker").unwrap();
        assert_eq!(process_id, "proc_1");
        assert_eq!(registry.get_process_token(&process_id).unwrap().len(), 64);
    }

    #[test]
    fn process_record_requires_current_token_and_revision_fields() {
        let record = serde_json::json!({
            "process_id": "proc_1",
            "app": "worker",
            "registered_at": 1,
            "last_heartbeat_at": 1,
            "lease_status": "alive"
        });
        assert!(serde_json::from_value::<ProcessRecord>(record.clone()).is_err());
        let mut with_token = record;
        with_token["process_token"] = serde_json::json!("current-token");
        assert!(serde_json::from_value::<ProcessRecord>(with_token).is_err());
    }

    #[test]
    fn registration_bounds_app_bytes_and_live_process_records() {
        let mut registry = Registry::default();
        let error = registry
            .register_process_with_token(&"a".repeat(MAX_PROCESS_APP_BYTES + 1), Ok("t".into()))
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert_eq!(registry.processes_len(), 0);

        for _ in 0..MAX_PROCESS_RECORDS {
            registry
                .register_process_with_token("worker", Ok("t".into()))
                .unwrap();
        }
        let revision = registry.control_revision;
        let error = registry
            .register_process_with_token("worker", Ok("t".into()))
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert_eq!(registry.processes_len(), MAX_PROCESS_RECORDS);
        assert_eq!(registry.control_revision, revision);
        assert_eq!(registry.next_process_id, MAX_PROCESS_RECORDS as u64);

        registry.force_expire_process("proc_1").unwrap();
        let replacement = registry
            .register_process_with_token("worker", Ok("t".into()))
            .unwrap();
        assert_eq!(replacement, format!("proc_{}", MAX_PROCESS_RECORDS + 1));
        assert!(registry.get_process("proc_1").is_none());
        assert_eq!(registry.processes_len(), MAX_PROCESS_RECORDS);
        assert_eq!(
            registry.get_process("proc_2").unwrap().lease_status,
            "alive"
        );
    }

    #[test]
    fn randomness_failure_does_not_evict_an_expired_record_at_capacity() {
        let mut registry = Registry::default();
        for _ in 0..MAX_PROCESS_RECORDS {
            registry
                .register_process_with_token("worker", Ok("t".into()))
                .unwrap();
        }
        registry.force_expire_process("proc_1").unwrap();
        let revision = registry.control_revision;
        let error = registry
            .register_process_with_token("worker", Err(getrandom::Error::UNSUPPORTED))
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::Unavailable);
        assert!(registry.get_process("proc_1").is_some());
        assert_eq!(registry.processes_len(), MAX_PROCESS_RECORDS);
        assert_eq!(registry.control_revision, revision);
    }
}
