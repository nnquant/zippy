//! Phase 1 service composition over the existing master and gateway runtime.

use std::sync::Arc;

use zippy_gateway::{GatewayServer, LegacyCollectFacade, LegacyCollectRequest};
use zippy_master::legacy_catalog::LegacyCatalogAdapter;
use zippy_master::legacy_runtime::LegacyRuntimeRegistryAdapter;
use zippy_master::server::MasterServer;
use zippy_types::catalog::CatalogRead;
use zippy_types::{ErrorCode, Result, ServerError};

use crate::{LegacyQueryEngineAdapter, LegacyServerHealth};

/// Stable service handles composed from the existing single-node runtime.
///
/// This object deliberately does not start listeners. The legacy CLI remains
/// the lifecycle authority during the additive migration; callers activate the
/// service view only after those listeners have completed their existing
/// readiness checks.
pub struct LegacyServerServices {
    master: MasterServer,
    gateway_facade: LegacyCollectFacade,
    gateway_health_probe: LegacyCollectRequest,
    catalog: Arc<LegacyCatalogAdapter>,
    runtime: Arc<LegacyRuntimeRegistryAdapter>,
    query: Arc<LegacyQueryEngineAdapter>,
}

impl LegacyServerServices {
    /// Builds stable catalog and query services without changing wire behavior.
    pub fn new(master: &MasterServer, gateway: &GatewayServer) -> Result<Self> {
        let catalog = Arc::new(master.legacy_catalog_adapter());
        let runtime = Arc::new(master.legacy_runtime_registry_adapter_inactive());
        let catalog_read: Arc<dyn CatalogRead> = catalog.clone();
        let gateway_facade = gateway.legacy_collect_facade();
        let gateway_health_probe =
            LegacyCollectRequest::new("__zippy_health_probe__", Vec::new(), 1).map_err(
                |error| {
                    ServerError::new(
                        ErrorCode::Internal,
                        format!("failed to build gateway health probe: {error}"),
                    )
                },
            )?;
        let query = Arc::new(LegacyQueryEngineAdapter::new_inactive(
            gateway_facade.clone(),
            catalog_read,
        )?);
        Ok(Self {
            master: master.clone(),
            gateway_facade,
            gateway_health_probe,
            catalog,
            runtime,
            query,
        })
    }

    /// Returns the legacy catalog adapter.
    ///
    /// Phase 1 returns the raw compatibility adapter. A previously cloned handle is not wrapped in
    /// the composition admission gate, but it does share the master lifecycle and rejects reads
    /// after that lifecycle stops. Dispatchers must still stop using it when composition admission
    /// closes before the master lifecycle changes.
    #[must_use]
    pub fn catalog(&self) -> Arc<LegacyCatalogAdapter> {
        Arc::clone(&self.catalog)
    }

    /// Returns the boot-scoped legacy runtime registry adapter.
    #[must_use]
    pub fn runtime(&self) -> Arc<LegacyRuntimeRegistryAdapter> {
        Arc::clone(&self.runtime)
    }

    /// Returns the legacy query engine adapter.
    #[must_use]
    pub fn query(&self) -> Arc<LegacyQueryEngineAdapter> {
        Arc::clone(&self.query)
    }

    /// Opens adapter admission after the caller verifies legacy runtime dependencies.
    ///
    /// This method rejects already-stopped local master and gateway lifecycles. It does not probe
    /// listener reachability or the gateway's configured master endpoint; those checks remain the
    /// responsibility of the legacy runtime orchestrator during Phase 1.
    pub fn activate(&self) -> Result<()> {
        if !self.master.is_running() {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "legacy master lifecycle is stopped",
            ));
        }
        self.gateway_facade
            .validate(&self.gateway_health_probe)
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    format!("legacy gateway facade is unavailable: {error}"),
                )
            })?;
        self.runtime.activate()?;
        if let Err(error) = self.query.activate() {
            self.runtime.begin_shutdown();
            return Err(error);
        }
        Ok(())
    }

    /// Rejects new composition-root work before legacy runtime shutdown begins.
    pub fn begin_shutdown(&self) {
        self.runtime.begin_shutdown();
        self.query.begin_shutdown();
    }

    /// Returns in-process adapter readiness and the explicit capability matrix.
    ///
    /// `LegacyServerHealth::end_to_end_liveness` remains `None`: this method deliberately does not
    /// infer listener or downstream dependency health from local lifecycle flags.
    #[must_use]
    pub fn health(&self) -> LegacyServerHealth {
        let gateway_facade_available = self
            .gateway_facade
            .validate(&self.gateway_health_probe)
            .is_ok();
        LegacyServerHealth::phase_one(
            self.query.is_accepting_work(),
            self.master.is_running(),
            gateway_facade_available,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use zippy_core::ControlEndpoint;
    use zippy_gateway::{GatewayServer, GatewayServerConfig};
    use zippy_master::server::MasterServer;
    use zippy_types::catalog::CatalogRead;
    use zippy_types::query::{QueryEngine, QueryInput, QueryPriority, QueryRequest};
    use zippy_types::runtime::RuntimeRegistry;
    use zippy_types::{ErrorCode, QueryId, ResourceLimit, SessionId};

    use super::LegacyServerServices;
    use crate::{LegacyGatewayPlan, ServiceHealth};

    #[test]
    fn composition_publishes_readiness_only_after_explicit_activation() {
        let master = MasterServer::default();
        let gateway = GatewayServer::new(GatewayServerConfig {
            endpoint: "127.0.0.1:0".to_string(),
            master_endpoint: ControlEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
            token: None,
            max_write_rows: None,
            max_connections: None,
            max_subscribers: None,
            max_blocking_requests: None,
            write_timeout_ms: None,
        })
        .unwrap();
        let services = LegacyServerServices::new(&master, &gateway).unwrap();

        assert_eq!(services.health().catalog, ServiceHealth::Unavailable);
        assert_eq!(
            services.runtime().boot_id(),
            master.legacy_runtime_registry_adapter().boot_id()
        );
        assert_eq!(
            services.runtime().snapshot().unwrap_err().code(),
            ErrorCode::Unavailable
        );
        services.activate().unwrap();
        assert!(services.runtime().snapshot().unwrap().records().is_empty());
        assert_eq!(services.health().catalog, ServiceHealth::Degraded);
        assert_eq!(services.health().query, ServiceHealth::Degraded);
        assert_eq!(services.health().end_to_end_liveness, None);
        assert!(services.health().accepting_work);
        services.begin_shutdown();
        assert!(!services.health().accepting_work);
        assert_eq!(services.health().catalog, ServiceHealth::Unavailable);
        assert_eq!(services.health().query, ServiceHealth::Unavailable);
        assert_eq!(
            services.runtime().snapshot().unwrap_err().code(),
            ErrorCode::Unavailable
        );
        assert_eq!(services.activate().unwrap_err().code(), ErrorCode::Conflict);
    }

    #[test]
    fn gateway_shutdown_only_removes_query_readiness() {
        let master = MasterServer::default();
        let gateway = GatewayServer::new(GatewayServerConfig {
            endpoint: "127.0.0.1:0".to_string(),
            master_endpoint: ControlEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
            token: None,
            max_write_rows: None,
            max_connections: None,
            max_subscribers: None,
            max_blocking_requests: None,
            write_timeout_ms: None,
        })
        .unwrap();
        let services = LegacyServerServices::new(&master, &gateway).unwrap();
        services.activate().unwrap();

        drop(gateway);

        let health = services.health();
        assert_eq!(health.catalog, ServiceHealth::Degraded);
        assert_eq!(health.query, ServiceHealth::Unavailable);
        assert!(!health.gateway_facade_available);
        assert!(health.accepting_work);
    }

    #[tokio::test]
    async fn master_shutdown_removes_readiness_and_rejects_catalog_and_query_work() {
        let master = MasterServer::default();
        let gateway = GatewayServer::new(GatewayServerConfig {
            endpoint: "127.0.0.1:0".to_string(),
            master_endpoint: ControlEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
            token: None,
            max_write_rows: None,
            max_connections: None,
            max_subscribers: None,
            max_blocking_requests: None,
            write_timeout_ms: None,
        })
        .unwrap();
        let services = LegacyServerServices::new(&master, &gateway).unwrap();
        services.activate().unwrap();
        let catalog = services.catalog();
        let query = services.query();
        let session_id = SessionId::from_u128(1).unwrap();
        let owner = query.owner(session_id);

        master.shutdown();

        let health = services.health();
        assert_eq!(health.catalog, ServiceHealth::Unavailable);
        assert_eq!(health.query, ServiceHealth::Unavailable);
        assert!(!health.master_lifecycle_running);
        assert!(!health.accepting_work);

        let catalog_error = catalog.revision().await.unwrap_err();
        assert_eq!(catalog_error.code(), ErrorCode::Unavailable);
        let query_error = query
            .plan(
                owner.as_ref(),
                QueryRequest {
                    query_id: QueryId::from_u128(1).unwrap(),
                    session_id,
                    input: QueryInput::LogicalPlan(
                        LegacyGatewayPlan::new("ticks", Vec::new())
                            .unwrap()
                            .encode()
                            .unwrap(),
                    ),
                    tables: Vec::new(),
                    priority: QueryPriority::Interactive,
                    deadline_ns: None,
                    memory_limit_bytes: ResourceLimit::ServerDefault,
                    spill_limit_bytes: ResourceLimit::Disabled,
                    result_batch_rows: NonZeroU32::new(128),
                },
            )
            .await
            .err()
            .expect("stopped master must reject query planning");
        assert_eq!(query_error.code(), ErrorCode::Unavailable);
    }
}
