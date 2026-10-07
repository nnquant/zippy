//! Capability and health reporting for the server runtime.

use serde::{Deserialize, Serialize};

/// Readiness of one service inside the single-node server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceHealth {
    /// The service can accept every operation it advertises.
    Ready,
    /// The service cannot accept work.
    Unavailable,
}

/// Feature matrix for the active native services.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerCapabilities {
    /// Native namespace and table discovery is available.
    pub catalog_current_discovery: bool,
    /// Transactional typed catalog mutations are available.
    pub catalog_transactional_writes: bool,
    /// Historical exact catalog snapshots are available.
    pub catalog_historical_snapshots: bool,
    /// Server-side SQL planning is available.
    pub query_sql: bool,
    /// Query plans pin immutable manifest snapshots.
    pub query_manifest_snapshot_pinning: bool,
}

impl ServerCapabilities {
    /// Returns the capability matrix of a control-only runtime.
    #[must_use]
    pub const fn control_only() -> Self {
        Self {
            catalog_current_discovery: false,
            catalog_transactional_writes: false,
            catalog_historical_snapshots: false,
            query_sql: false,
            query_manifest_snapshot_pinning: false,
        }
    }

    /// Returns the capability matrix after applying the native query authority gate.
    #[must_use]
    pub const fn with_native_query(mut self, enabled: bool) -> Self {
        self.query_sql = enabled;
        self.query_manifest_snapshot_pinning = enabled;
        self
    }
}

/// Unified health view published by the server runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerHealth {
    /// Whether control or native services can currently accept new work.
    pub accepting_work: bool,
    /// Whether the composition root has opened service admission.
    pub admission_open: bool,
    /// Native SQLite catalog health.
    pub catalog: ServiceHealth,
    /// Native Flight query service health.
    pub query: ServiceHealth,
    /// Whether the local master lifecycle has not entered its stopped state.
    ///
    /// This is an in-process lifecycle observation, not a listener probe.
    pub master_lifecycle_running: bool,
    /// End-to-end listener and dependency liveness, when verified by an external probe.
    ///
    /// The initial control-only view reports `None` instead of inferring
    /// liveness from local lifecycle flags.
    pub end_to_end_liveness: Option<bool>,
    /// Explicit native capabilities.
    pub capabilities: ServerCapabilities,
}

impl ServerHealth {
    /// Builds the control-only readiness view from local lifecycle observations.
    ///
    /// Local lifecycle flags do not imply downstream dependency liveness.
    #[must_use]
    pub const fn control_only(admission_open: bool, master_lifecycle_running: bool) -> Self {
        Self {
            accepting_work: admission_open && master_lifecycle_running,
            admission_open,
            catalog: ServiceHealth::Unavailable,
            query: ServiceHealth::Unavailable,
            master_lifecycle_running,
            end_to_end_liveness: None,
            capabilities: ServerCapabilities::control_only(),
        }
    }

    pub(crate) fn set_native_catalog_ready(&mut self, ready: bool) {
        self.catalog = if ready {
            ServiceHealth::Ready
        } else {
            ServiceHealth::Unavailable
        };
        self.capabilities.catalog_current_discovery = ready;
        self.capabilities.catalog_transactional_writes = ready;
        self.capabilities.catalog_historical_snapshots = ready;
    }

    pub(crate) fn set_native_query_ready(&mut self, ready: bool) {
        self.capabilities = self.capabilities.with_native_query(ready);
        if ready {
            self.query = ServiceHealth::Ready;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ServerCapabilities, ServerHealth, ServiceHealth};

    #[test]
    fn control_only_capabilities_never_claim_future_engine_features() {
        let health = ServerHealth::control_only(true, true);

        assert_eq!(health.catalog, ServiceHealth::Unavailable);
        assert_eq!(health.query, ServiceHealth::Unavailable);
        assert!(health.accepting_work);
        assert!(health.admission_open);
        assert_eq!(health.end_to_end_liveness, None);
        assert!(serde_json::to_value(&health).unwrap()["end_to_end_liveness"].is_null());
        assert!(!health.capabilities.catalog_current_discovery);
        assert!(!health.capabilities.catalog_transactional_writes);
        assert!(!health.capabilities.catalog_historical_snapshots);
        assert!(!health.capabilities.query_sql);
        assert!(!health.capabilities.query_manifest_snapshot_pinning);
        assert_eq!(health.capabilities, ServerCapabilities::control_only());
    }

    #[test]
    fn closed_admission_makes_both_adapters_unavailable() {
        let health = ServerHealth::control_only(false, true);

        assert_eq!(health.catalog, ServiceHealth::Unavailable);
        assert_eq!(health.query, ServiceHealth::Unavailable);
        assert!(!health.accepting_work);
    }

    #[test]
    fn control_only_health_does_not_advertise_queries() {
        let health = ServerHealth::control_only(true, true);

        assert_eq!(health.catalog, ServiceHealth::Unavailable);
        assert_eq!(health.query, ServiceHealth::Unavailable);
        assert!(health.accepting_work);
    }

    #[test]
    fn stopped_master_removes_catalog_and_query_readiness() {
        let health = ServerHealth::control_only(true, false);

        assert_eq!(health.catalog, ServiceHealth::Unavailable);
        assert_eq!(health.query, ServiceHealth::Unavailable);
        assert!(!health.accepting_work);
        assert!(!health.master_lifecycle_running);
    }
}
