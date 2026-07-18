//! Capability and health reporting for the legacy composition root.

use serde::{Deserialize, Serialize};

/// Readiness of one service inside the single-node server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceHealth {
    /// The service can accept every operation it advertises.
    Ready,
    /// The service is usable but exposes only compatibility capabilities.
    Degraded,
    /// The service cannot accept work.
    Unavailable,
}

/// Honest feature matrix for the Phase 1 legacy adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyServerCapabilities {
    /// Current legacy namespace and table discovery is available.
    pub catalog_current_discovery: bool,
    /// Transactional typed catalog mutations are available.
    pub catalog_transactional_writes: bool,
    /// Historical exact catalog snapshots are available.
    pub catalog_historical_snapshots: bool,
    /// Bounded legacy collect plans can stream Arrow batches.
    pub query_legacy_collect_streaming: bool,
    /// Server-side SQL planning is available.
    pub query_sql: bool,
    /// Query plans pin immutable manifest snapshots.
    pub query_manifest_snapshot_pinning: bool,
}

impl LegacyServerCapabilities {
    /// Returns the fixed capability matrix of the compatibility layer.
    #[must_use]
    pub const fn phase_one() -> Self {
        Self {
            catalog_current_discovery: true,
            catalog_transactional_writes: false,
            catalog_historical_snapshots: false,
            query_legacy_collect_streaming: true,
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

/// Unified health view published by the legacy composition root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyServerHealth {
    /// Whether at least one compatibility service can currently accept new work.
    pub accepting_work: bool,
    /// Whether the composition root has opened adapter admission.
    pub admission_open: bool,
    /// Catalog adapter health.
    pub catalog: ServiceHealth,
    /// Query adapter health.
    pub query: ServiceHealth,
    /// Whether the local master lifecycle has not entered its stopped state.
    ///
    /// This is an in-process lifecycle observation, not a listener probe.
    pub master_lifecycle_running: bool,
    /// Whether the in-process gateway facade has not entered its stopped state.
    ///
    /// This does not prove that the gateway listener or configured master endpoint is reachable.
    pub gateway_facade_available: bool,
    /// End-to-end listener and dependency liveness, when verified by an external probe.
    ///
    /// Phase 1 has no such probe, so the composition root reports `None` instead of inferring
    /// liveness from local lifecycle flags.
    pub end_to_end_liveness: Option<bool>,
    /// Explicit compatibility capabilities.
    pub capabilities: LegacyServerCapabilities,
}

impl LegacyServerHealth {
    /// Builds the Phase 1 adapter-readiness view from local lifecycle observations.
    ///
    /// `master_lifecycle_running` and `gateway_facade_available` deliberately do not imply
    /// listener or downstream dependency liveness.
    #[must_use]
    pub const fn phase_one(
        admission_open: bool,
        master_lifecycle_running: bool,
        gateway_facade_available: bool,
    ) -> Self {
        let catalog = if admission_open && master_lifecycle_running {
            ServiceHealth::Degraded
        } else {
            ServiceHealth::Unavailable
        };
        let query = if admission_open && master_lifecycle_running && gateway_facade_available {
            ServiceHealth::Degraded
        } else {
            ServiceHealth::Unavailable
        };
        Self {
            accepting_work: !matches!(catalog, ServiceHealth::Unavailable)
                || !matches!(query, ServiceHealth::Unavailable),
            admission_open,
            catalog,
            query,
            master_lifecycle_running,
            gateway_facade_available,
            end_to_end_liveness: None,
            capabilities: LegacyServerCapabilities::phase_one(),
        }
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
    use super::{LegacyServerCapabilities, LegacyServerHealth, ServiceHealth};

    #[test]
    fn phase_one_capabilities_never_claim_future_engine_features() {
        let health = LegacyServerHealth::phase_one(true, true, true);

        assert_eq!(health.catalog, ServiceHealth::Degraded);
        assert_eq!(health.query, ServiceHealth::Degraded);
        assert!(health.accepting_work);
        assert!(health.admission_open);
        assert_eq!(health.end_to_end_liveness, None);
        assert!(serde_json::to_value(&health).unwrap()["end_to_end_liveness"].is_null());
        assert!(health.capabilities.catalog_current_discovery);
        assert!(health.capabilities.query_legacy_collect_streaming);
        assert!(!health.capabilities.catalog_transactional_writes);
        assert!(!health.capabilities.catalog_historical_snapshots);
        assert!(!health.capabilities.query_sql);
        assert!(!health.capabilities.query_manifest_snapshot_pinning);
        assert_eq!(health.capabilities, LegacyServerCapabilities::phase_one());
    }

    #[test]
    fn closed_admission_makes_both_adapters_unavailable() {
        let health = LegacyServerHealth::phase_one(false, true, true);

        assert_eq!(health.catalog, ServiceHealth::Unavailable);
        assert_eq!(health.query, ServiceHealth::Unavailable);
        assert!(!health.accepting_work);
    }

    #[test]
    fn stopped_gateway_only_removes_query_readiness() {
        let health = LegacyServerHealth::phase_one(true, true, false);

        assert_eq!(health.catalog, ServiceHealth::Degraded);
        assert_eq!(health.query, ServiceHealth::Unavailable);
        assert!(health.accepting_work);
        assert!(!health.gateway_facade_available);
    }

    #[test]
    fn stopped_master_removes_catalog_and_query_readiness() {
        let health = LegacyServerHealth::phase_one(true, false, true);

        assert_eq!(health.catalog, ServiceHealth::Unavailable);
        assert_eq!(health.query, ServiceHealth::Unavailable);
        assert!(!health.accepting_work);
        assert!(!health.master_lifecycle_running);
    }
}
