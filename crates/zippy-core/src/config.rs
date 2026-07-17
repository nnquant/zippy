use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Result, ZippyError};

pub const DEFAULT_CONFIG_PATH: &str = "~/.zippy/config.toml";
pub const DEFAULT_LOG_LEVEL: &str = "info";
pub const DEFAULT_TABLE_ROW_CAPACITY: usize = 65_536;
pub const DEFAULT_TABLE_PERSIST_METHOD: &str = "parquet";
pub const DEFAULT_TABLE_PERSIST_DATA_DIR: &str = "data";
pub const DEFAULT_TABLE_PERSIST_COMPACTION_INTERVAL_SEC: f64 = 300.0;
pub const DEFAULT_TABLE_PERSIST_COMPACTION_MIN_FILES: usize = 8;
pub const DEFAULT_MASTER_SHUTDOWN_TIMEOUT_MS: u64 = 5_000;
pub const DEFAULT_MAX_CPU_CORES: usize = 4;
pub const MAX_CPU_CORES: usize = 256;
/// Deprecated compatibility alias for APIs that have not migrated to CPU-budget terminology.
pub const DEFAULT_MAX_PARALLELISM: usize = DEFAULT_MAX_CPU_CORES;
/// Deprecated compatibility alias for APIs that have not migrated to CPU-budget terminology.
pub const MAX_PARALLELISM: usize = MAX_CPU_CORES;
pub const DEFAULT_MAX_QUERY_BUFFER_BYTES: usize = 512 * 1024 * 1024;
pub const DEFAULT_QUERY_MAX_BUFFERED_BYTES: usize = 128 * 1024 * 1024;
/// Default lifetime of a query plan that has not started execution.
pub const DEFAULT_QUERY_PLAN_TTL_MS: u64 = 30_000;
/// Maximum configurable lifetime of an unexecuted query plan.
pub const MAX_QUERY_PLAN_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
pub const MAX_QUERY_BUFFER_BYTES: usize = u32::MAX as usize;
/// Default number of logical native queries admitted concurrently.
pub const DEFAULT_QUERY_MAX_CONCURRENT_QUERIES: usize = 64;
/// Native query registry capacity available to concurrently admitted logical queries.
pub const MAX_QUERY_CONCURRENT_QUERIES: usize = 4_096;
const DEFAULT_QUERY_ACTIVE_TASKS_PER_CPU: usize = 4;
/// Default number of native query tasks admitted across all logical queries.
///
/// Sixteen preserves the existing default of four tasks per default CPU core without claiming
/// that all Zippy thread pools share one process-wide CPU semaphore.
pub const DEFAULT_QUERY_MAX_ACTIVE_TASKS: usize =
    DEFAULT_MAX_CPU_CORES * DEFAULT_QUERY_ACTIVE_TASKS_PER_CPU;
/// Default native query task-memory admission budget.
///
/// This keeps the previous 512 MiB admission level while separating it from Gateway Arrow buffers.
pub const DEFAULT_QUERY_MAX_MEMORY_BYTES: u64 = 512 * 1024 * 1024;
/// Default maximum rows consumed by one persisted-only native query canary run.
pub const DEFAULT_QUERY_CANARY_MAX_ROWS: u64 = 1_000_000;
/// Default maximum Arrow batches consumed by one persisted-only native query canary run.
pub const DEFAULT_QUERY_CANARY_MAX_BATCHES: usize = 4_096;
/// Default cumulative encoded or accounted result bytes accepted by one canary run.
pub const DEFAULT_QUERY_CANARY_MAX_OUTPUT_BYTES: u64 = 256 * 1024 * 1024;
/// Preferred DataFusion operator pool size when the global query budget permits it.
pub const DEFAULT_QUERY_CANARY_OPERATOR_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_CATALOG_SHADOW_QUEUE_CAPACITY: usize = 64;
pub const DEFAULT_CATALOG_SHADOW_METADATA_BUDGET_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_CATALOG_SHADOW_MAX_FILES_PER_COMPARISON: usize = 4_096;
pub const DEFAULT_CATALOG_SHADOW_MAX_METADATA_BYTES_PER_COMPARISON: usize = 8 * 1024 * 1024;
pub const DEFAULT_CATALOG_SHADOW_COMPARISON_TIMEOUT_MS: u64 = 5_000;
pub const DEFAULT_CATALOG_SHADOW_DEDUP_CAPACITY: usize = 1_024;
/// Default process-local query snapshot pin budget.
pub const DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_PINS: usize = 131_072;
/// Default number of table entries retained by query pins and GC exclusion.
pub const DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_TABLES: usize = 16_384;
/// Platform configuration ceiling for process-local query snapshot pins.
pub const MAX_CATALOG_RUNTIME_SNAPSHOT_PINS: usize = 1_048_576;
/// Platform configuration ceiling for table entries retained by pin coordination.
pub const MAX_CATALOG_RUNTIME_SNAPSHOT_PIN_TABLES: usize = 131_072;
const SUPPORTED_TABLE_PERSIST_DT_PARTS: &[&str] = &["%Y", "%Y%m", "%Y%m%d", "%Y%m%d%H"];

/// Process-wide Zippy runtime configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZippyConfig {
    #[serde(default)]
    pub server: ZippyServerConfig,
    #[serde(default)]
    pub catalog: ZippyCatalogConfig,
    #[serde(default)]
    pub runtime: ZippyRuntimeConfig,
    #[serde(default)]
    pub master: ZippyMasterConfig,
    pub log: ZippyLogConfig,
    pub table: ZippyTableConfig,
    #[serde(default)]
    pub gateway: ZippyGatewayConfig,
    #[serde(default)]
    pub query: ZippyQueryConfig,
}

/// Catalog metadata authority selected during the SQLite migration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZippyCatalogBackend {
    /// The legacy registry remains the read and write authority.
    #[default]
    Legacy,
    /// Legacy remains authoritative while SQLite is compared asynchronously.
    SqliteShadow,
    /// SQLite is authoritative. This mode is reserved until cutover is complete.
    Sqlite,
}

/// Catalog migration configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyCatalogConfig {
    /// Metadata authority and migration mode.
    #[serde(default)]
    pub backend: ZippyCatalogBackend,
    /// Process-local query snapshot pin admission limits.
    #[serde(default)]
    pub runtime_snapshot_pins: ZippyCatalogRuntimeSnapshotPinConfig,
    /// Bounded best-effort comparison resources used only in shadow mode.
    #[serde(default)]
    pub shadow: ZippyCatalogShadowConfig,
}

/// Resource limits for process-local query snapshot pins retained against safe GC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyCatalogRuntimeSnapshotPinConfig {
    /// Maximum number of simultaneously retained table snapshot pins.
    pub max_active_pins: usize,
    /// Maximum number of table entries retained by pin and GC coordination.
    pub max_active_tables: usize,
}

impl Default for ZippyCatalogRuntimeSnapshotPinConfig {
    fn default() -> Self {
        Self {
            max_active_pins: DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_PINS,
            max_active_tables: DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_TABLES,
        }
    }
}

/// Resource limits for asynchronous legacy-versus-SQLite metadata comparisons.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyCatalogShadowConfig {
    pub queue_capacity: usize,
    pub metadata_budget_bytes: usize,
    pub max_files_per_comparison: usize,
    pub max_metadata_bytes_per_comparison: usize,
    pub comparison_timeout_ms: u64,
    pub dedup_capacity: usize,
}

impl Default for ZippyCatalogShadowConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_CATALOG_SHADOW_QUEUE_CAPACITY,
            metadata_budget_bytes: DEFAULT_CATALOG_SHADOW_METADATA_BUDGET_BYTES,
            max_files_per_comparison: DEFAULT_CATALOG_SHADOW_MAX_FILES_PER_COMPARISON,
            max_metadata_bytes_per_comparison:
                DEFAULT_CATALOG_SHADOW_MAX_METADATA_BYTES_PER_COMPARISON,
            comparison_timeout_ms: DEFAULT_CATALOG_SHADOW_COMPARISON_TIMEOUT_MS,
            dedup_capacity: DEFAULT_CATALOG_SHADOW_DEDUP_CAPACITY,
        }
    }
}

/// Single-node server storage configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyServerConfig {
    /// Managed database root. `None` derives an endpoint-scoped default.
    pub data_dir: Option<PathBuf>,
    /// Allows one offline import from the legacy endpoint-scoped snapshot.
    pub import_legacy_endpoint_snapshot: bool,
    /// Enables the server-owned native storage lifecycle during migration.
    #[serde(default)]
    pub native_storage_enabled: bool,
}

/// Process-wide execution resource limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ZippyRuntimeConfig {
    /// Process-wide CPU budget consumed by components as an upper bound.
    pub max_cpu_cores: usize,
    #[serde(default = "default_max_query_buffer_bytes")]
    pub max_query_buffer_bytes: usize,
}

impl Default for ZippyRuntimeConfig {
    fn default() -> Self {
        Self {
            max_cpu_cores: DEFAULT_MAX_CPU_CORES,
            max_query_buffer_bytes: DEFAULT_MAX_QUERY_BUFFER_BYTES,
        }
    }
}

fn default_max_query_buffer_bytes() -> usize {
    DEFAULT_MAX_QUERY_BUFFER_BYTES
}

impl<'de> Deserialize<'de> for ZippyRuntimeConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RuntimeWireConfig {
            #[serde(default, deserialize_with = "deserialize_optional_non_null_usize")]
            max_cpu_cores: Option<usize>,
            #[serde(default, deserialize_with = "deserialize_optional_non_null_usize")]
            max_parallelism: Option<usize>,
            #[serde(default = "default_max_query_buffer_bytes")]
            max_query_buffer_bytes: usize,
        }

        let wire = RuntimeWireConfig::deserialize(deserializer)?;
        let max_cpu_cores = resolve_runtime_cpu_aliases(
            wire.max_cpu_cores,
            wire.max_parallelism,
            "serialized runtime",
        )
        .map_err(serde::de::Error::custom)?
        .ok_or_else(|| serde::de::Error::missing_field("max_cpu_cores"))?;
        Ok(Self {
            max_cpu_cores,
            max_query_buffer_bytes: wire.max_query_buffer_bytes,
        })
    }
}

fn deserialize_optional_non_null_usize<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    usize::deserialize(deserializer).map(Some)
}

/// Native query admission limits independent from Gateway Arrow buffer budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyQueryResourceConfig {
    pub max_concurrent_queries: usize,
    pub max_active_tasks: usize,
    pub max_memory_bytes: u64,
}

/// Explicitly opt-in limits for the persisted-only native query canary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyQueryCanaryConfig {
    /// Enables internal canary submission; this does not grant Gateway query authority.
    #[serde(default)]
    pub enabled: bool,
    /// Maximum rows consumed before the canary fails the query.
    pub max_rows: u64,
    /// Maximum result batches consumed before the canary fails the query.
    pub max_batches: usize,
    /// Maximum cumulative Arrow and canonical IPC bytes consumed by one run.
    pub max_output_bytes: u64,
    /// Optional hard DataFusion operator pool size; `None` derives a bounded value.
    pub operator_memory_bytes: Option<u64>,
}

impl Default for ZippyQueryCanaryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_rows: DEFAULT_QUERY_CANARY_MAX_ROWS,
            max_batches: DEFAULT_QUERY_CANARY_MAX_BATCHES,
            max_output_bytes: DEFAULT_QUERY_CANARY_MAX_OUTPUT_BYTES,
            operator_memory_bytes: None,
        }
    }
}

impl Default for ZippyQueryResourceConfig {
    fn default() -> Self {
        Self {
            max_concurrent_queries: DEFAULT_QUERY_MAX_CONCURRENT_QUERIES,
            max_active_tasks: DEFAULT_QUERY_MAX_ACTIVE_TASKS,
            max_memory_bytes: DEFAULT_QUERY_MAX_MEMORY_BYTES,
        }
    }
}

/// Optional query-specific limits plus native query admission resources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyQueryConfig {
    pub parallelism: Option<usize>,
    pub max_buffered_bytes: Option<usize>,
    #[serde(default = "default_query_plan_ttl_ms")]
    pub plan_ttl_ms: u64,
    #[serde(default)]
    pub resources: ZippyQueryResourceConfig,
    #[serde(default)]
    pub canary: ZippyQueryCanaryConfig,
}

impl Default for ZippyQueryConfig {
    fn default() -> Self {
        Self {
            parallelism: None,
            max_buffered_bytes: None,
            plan_ttl_ms: DEFAULT_QUERY_PLAN_TTL_MS,
            resources: ZippyQueryResourceConfig::default(),
            canary: ZippyQueryCanaryConfig::default(),
        }
    }
}

const fn default_query_plan_ttl_ms() -> u64 {
    DEFAULT_QUERY_PLAN_TTL_MS
}

/// Master control endpoint defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyMasterConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub shutdown_timeout_ms: u64,
    #[serde(default, skip_serializing)]
    pub token: Option<String>,
}

impl Default for ZippyMasterConfig {
    fn default() -> Self {
        Self {
            host: None,
            port: None,
            shutdown_timeout_ms: DEFAULT_MASTER_SHUTDOWN_TIMEOUT_MS,
            token: None,
        }
    }
}

/// Logging defaults shared by master and Python clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyLogConfig {
    pub level: String,
}

/// Stream table defaults.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZippyTableConfig {
    pub row_capacity: usize,
    pub retention_segments: Option<usize>,
    pub persist: ZippyTablePersistConfig,
}

/// Stream table persistence defaults.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZippyTablePersistConfig {
    pub enabled: bool,
    pub method: String,
    pub data_dir: String,
    pub partition: ZippyTablePersistPartitionConfig,
    #[serde(default)]
    pub compaction: ZippyTablePersistCompactionConfig,
}

/// Stream table persistence partition defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyTablePersistPartitionConfig {
    pub dt_column: Option<String>,
    pub id_column: Option<String>,
    pub dt_part: Option<String>,
}

/// Stream table persisted parquet compaction defaults.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZippyTablePersistCompactionConfig {
    pub enabled: bool,
    pub interval_sec: f64,
    pub min_files: usize,
    pub delete_sources: bool,
    pub sort_column: Option<String>,
}

impl Default for ZippyTablePersistCompactionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_sec: DEFAULT_TABLE_PERSIST_COMPACTION_INTERVAL_SEC,
            min_files: DEFAULT_TABLE_PERSIST_COMPACTION_MIN_FILES,
            delete_sources: false,
            sort_column: None,
        }
    }
}

/// GatewayServer capability advertised by master for cross-platform data access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyGatewayConfig {
    pub enabled: bool,
    pub endpoint: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub token: Option<String>,
    pub protocol_version: u16,
}

impl Default for ZippyGatewayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            host: None,
            port: None,
            token: None,
            protocol_version: 1,
        }
    }
}

#[derive(Debug, Deserialize)]
struct PartialZippyConfig {
    server: Option<PartialServerConfig>,
    catalog: Option<PartialCatalogConfig>,
    runtime: Option<PartialRuntimeConfig>,
    master: Option<PartialMasterConfig>,
    log: Option<PartialLogConfig>,
    table: Option<PartialTableConfig>,
    gateway: Option<PartialGatewayConfig>,
    query: Option<PartialQueryConfig>,
}

#[derive(Debug, Deserialize)]
struct PartialCatalogConfig {
    backend: Option<ZippyCatalogBackend>,
    runtime_snapshot_pins: Option<PartialCatalogRuntimeSnapshotPinConfig>,
    shadow: Option<PartialCatalogShadowConfig>,
}

#[derive(Debug, Deserialize)]
struct PartialCatalogRuntimeSnapshotPinConfig {
    max_active_pins: Option<usize>,
    max_active_tables: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct PartialCatalogShadowConfig {
    queue_capacity: Option<usize>,
    metadata_budget_bytes: Option<usize>,
    max_files_per_comparison: Option<usize>,
    max_metadata_bytes_per_comparison: Option<usize>,
    comparison_timeout_ms: Option<u64>,
    dedup_capacity: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct PartialServerConfig {
    data_dir: Option<String>,
    import_legacy_endpoint_snapshot: Option<bool>,
    native_storage_enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct PartialRuntimeConfig {
    max_cpu_cores: Option<usize>,
    max_parallelism: Option<usize>,
    max_query_buffer_bytes: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct PartialQueryConfig {
    parallelism: Option<usize>,
    max_buffered_bytes: Option<usize>,
    plan_ttl_ms: Option<u64>,
    resources: Option<PartialQueryResourceConfig>,
    canary: Option<PartialQueryCanaryConfig>,
}

#[derive(Debug, Deserialize)]
struct PartialQueryResourceConfig {
    max_concurrent_queries: Option<usize>,
    max_active_tasks: Option<usize>,
    max_memory_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PartialQueryCanaryConfig {
    enabled: Option<bool>,
    max_rows: Option<u64>,
    max_batches: Option<usize>,
    max_output_bytes: Option<u64>,
    operator_memory_bytes: Option<u64>,
}

#[derive(Debug, Default)]
struct ConfigResolutionState {
    query_max_active_tasks_explicit: bool,
}

#[derive(Debug, Deserialize)]
struct PartialMasterConfig {
    host: Option<String>,
    port: Option<u16>,
    shutdown_timeout_ms: Option<u64>,
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PartialLogConfig {
    level: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PartialTableConfig {
    row_capacity: Option<usize>,
    retention_segments: Option<usize>,
    persist: Option<PartialTablePersistConfig>,
}

#[derive(Debug, Deserialize)]
struct PartialTablePersistConfig {
    enabled: Option<bool>,
    method: Option<String>,
    data_dir: Option<String>,
    partition: Option<PartialTablePersistPartitionConfig>,
    compaction: Option<PartialTablePersistCompactionConfig>,
}

#[derive(Debug, Deserialize)]
struct PartialTablePersistPartitionConfig {
    dt_column: Option<String>,
    id_column: Option<String>,
    dt_part: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PartialTablePersistCompactionConfig {
    enabled: Option<bool>,
    interval_sec: Option<f64>,
    min_files: Option<usize>,
    delete_sources: Option<bool>,
    sort_column: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PartialGatewayConfig {
    enabled: Option<bool>,
    endpoint: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    token: Option<String>,
    protocol_version: Option<u16>,
}

impl Default for ZippyConfig {
    fn default() -> Self {
        Self {
            server: ZippyServerConfig::default(),
            catalog: ZippyCatalogConfig::default(),
            runtime: ZippyRuntimeConfig::default(),
            master: ZippyMasterConfig::default(),
            log: ZippyLogConfig {
                level: DEFAULT_LOG_LEVEL.to_string(),
            },
            table: ZippyTableConfig {
                row_capacity: DEFAULT_TABLE_ROW_CAPACITY,
                retention_segments: None,
                persist: ZippyTablePersistConfig {
                    enabled: false,
                    method: DEFAULT_TABLE_PERSIST_METHOD.to_string(),
                    data_dir: DEFAULT_TABLE_PERSIST_DATA_DIR.to_string(),
                    partition: ZippyTablePersistPartitionConfig {
                        dt_column: None,
                        id_column: None,
                        dt_part: None,
                    },
                    compaction: ZippyTablePersistCompactionConfig::default(),
                },
            },
            gateway: ZippyGatewayConfig::default(),
            query: ZippyQueryConfig::default(),
        }
    }
}

impl ZippyConfig {
    /// Load config from the default path, then apply `ZIPPY_*` environment overrides.
    pub fn load_default() -> Result<Self> {
        Self::load_from_path(Some(default_config_path()))
    }

    /// Load config from an optional TOML file path, then apply `ZIPPY_*` environment overrides.
    pub fn load_from_path(path: Option<impl AsRef<Path>>) -> Result<Self> {
        let mut config = Self::default();
        let mut resolution = ConfigResolutionState::default();
        if let Some(path) = path {
            let path = path.as_ref();
            if path.exists() {
                let content = std::fs::read_to_string(path).map_err(|error| ZippyError::Io {
                    reason: format!(
                        "failed to read zippy config path=[{}] error=[{}]",
                        path.display(),
                        error
                    ),
                })?;
                let config_base = absolute_parent(path)?;
                config.apply_partial(
                    parse_config_toml(&content, path)?,
                    &config_base,
                    &mut resolution,
                    "TOML [runtime]",
                )?;
            }
        }
        config.apply_env(&mut resolution)?;
        config.finalize_resolution(&resolution)?;
        Ok(config)
    }

    /// Resolve a dynamic JSON config overlay without applying environment overrides.
    ///
    /// Relative managed paths are resolved against the explicit absolute `config_base`, making
    /// the result independent from later working-directory changes.
    pub fn from_json_overlay(
        overlay: serde_json::Value,
        config_base: impl AsRef<Path>,
    ) -> Result<Self> {
        let config_base = config_base.as_ref();
        if !config_base.is_absolute() {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "JSON config overlay base must be absolute path=[{}]",
                    config_base.display()
                ),
            });
        }
        if !overlay.is_object() {
            return Err(ZippyError::InvalidConfig {
                reason: "JSON config overlay must be an object".to_string(),
            });
        }
        let mut config = Self::default();
        reject_non_nullable_json_nulls(&overlay, &config.to_json_value(), "")?;
        let partial = serde_json::from_value::<PartialZippyConfig>(overlay).map_err(|error| {
            ZippyError::InvalidConfig {
                reason: format!("failed to parse JSON config overlay error=[{}]", error),
            }
        })?;
        let mut resolution = ConfigResolutionState::default();
        config.apply_partial(
            partial,
            config_base,
            &mut resolution,
            "JSON overlay [runtime]",
        )?;
        config.finalize_resolution(&resolution)?;
        Ok(config)
    }

    pub(crate) fn from_wire_json(mut value: serde_json::Value) -> Result<Self> {
        let resolution = ConfigResolutionState {
            query_max_active_tasks_explicit: value
                .pointer("/query/resources/max_active_tasks")
                .is_some(),
        };
        canonicalize_runtime_cpu_wire_aliases(&mut value)?;
        let mut config =
            serde_json::from_value::<Self>(value).map_err(|error| ZippyError::InvalidConfig {
                reason: format!("failed to decode zippy config error=[{}]", error),
            })?;
        config.finalize_resolution(&resolution)?;
        Ok(config)
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        let mut value = serde_json::to_value(self).expect("zippy config must serialize");
        if let Some(runtime) = value
            .get_mut("runtime")
            .and_then(serde_json::Value::as_object_mut)
        {
            runtime.insert(
                "max_parallelism".to_string(),
                serde_json::json!(self.runtime.max_cpu_cores),
            );
        }
        value
    }

    fn apply_partial(
        &mut self,
        partial: PartialZippyConfig,
        config_base: &Path,
        resolution: &mut ConfigResolutionState,
        runtime_cpu_layer: &str,
    ) -> Result<()> {
        if let Some(server) = partial.server {
            if let Some(data_dir) = server.data_dir {
                let data_dir = non_empty(data_dir, "server.data_dir")?;
                self.server.data_dir = Some(resolve_config_path(&data_dir, config_base));
            }
            if let Some(import) = server.import_legacy_endpoint_snapshot {
                self.server.import_legacy_endpoint_snapshot = import;
            }
            if let Some(enabled) = server.native_storage_enabled {
                self.server.native_storage_enabled = enabled;
            }
        }
        if let Some(catalog) = partial.catalog {
            if let Some(backend) = catalog.backend {
                self.catalog.backend = backend;
            }
            if let Some(runtime_snapshot_pins) = catalog.runtime_snapshot_pins {
                if let Some(value) = runtime_snapshot_pins.max_active_pins {
                    self.catalog.runtime_snapshot_pins.max_active_pins = value;
                }
                if let Some(value) = runtime_snapshot_pins.max_active_tables {
                    self.catalog.runtime_snapshot_pins.max_active_tables = value;
                }
            }
            if let Some(shadow) = catalog.shadow {
                if let Some(value) = shadow.queue_capacity {
                    self.catalog.shadow.queue_capacity = value;
                }
                if let Some(value) = shadow.metadata_budget_bytes {
                    self.catalog.shadow.metadata_budget_bytes = value;
                }
                if let Some(value) = shadow.max_files_per_comparison {
                    self.catalog.shadow.max_files_per_comparison = value;
                }
                if let Some(value) = shadow.max_metadata_bytes_per_comparison {
                    self.catalog.shadow.max_metadata_bytes_per_comparison = value;
                }
                if let Some(value) = shadow.comparison_timeout_ms {
                    self.catalog.shadow.comparison_timeout_ms = value;
                }
                if let Some(value) = shadow.dedup_capacity {
                    self.catalog.shadow.dedup_capacity = value;
                }
            }
        }
        if let Some(runtime) = partial.runtime {
            if let Some(max_cpu_cores) = resolve_runtime_cpu_aliases(
                runtime.max_cpu_cores,
                runtime.max_parallelism,
                runtime_cpu_layer,
            )? {
                self.runtime.max_cpu_cores = max_cpu_cores;
            }
            if let Some(max_query_buffer_bytes) = runtime.max_query_buffer_bytes {
                self.runtime.max_query_buffer_bytes = max_query_buffer_bytes;
            }
        }
        if let Some(master) = partial.master {
            if let Some(host) = master.host {
                self.master.host = Some(non_empty(host, "master.host")?);
            }
            if let Some(port) = master.port {
                self.master.port = Some(port);
            }
            if let Some(shutdown_timeout_ms) = master.shutdown_timeout_ms {
                self.master.shutdown_timeout_ms = shutdown_timeout_ms;
            }
            if let Some(token) = master.token {
                self.master.token = Some(non_empty(token, "master.token")?);
            }
        }
        if let Some(log) = partial.log {
            if let Some(level) = log.level {
                self.log.level = non_empty(level, "log.level")?;
            }
        }
        if let Some(table) = partial.table {
            if let Some(row_capacity) = table.row_capacity {
                self.table.row_capacity = row_capacity;
            }
            if let Some(retention_segments) = table.retention_segments {
                self.table.retention_segments = Some(retention_segments);
            }
            if let Some(persist) = table.persist {
                if let Some(enabled) = persist.enabled {
                    self.table.persist.enabled = enabled;
                }
                if let Some(method) = persist.method {
                    self.table.persist.method = non_empty(method, "table.persist.method")?;
                }
                if let Some(data_dir) = persist.data_dir {
                    self.table.persist.data_dir = non_empty(data_dir, "table.persist.data_dir")?;
                }
                if let Some(partition) = persist.partition {
                    if let Some(dt_column) = partition.dt_column {
                        self.table.persist.partition.dt_column =
                            Some(non_empty(dt_column, "table.persist.partition.dt_column")?);
                    }
                    if let Some(id_column) = partition.id_column {
                        self.table.persist.partition.id_column =
                            Some(non_empty(id_column, "table.persist.partition.id_column")?);
                    }
                    if let Some(dt_part) = partition.dt_part {
                        self.table.persist.partition.dt_part =
                            Some(non_empty(dt_part, "table.persist.partition.dt_part")?);
                    }
                }
                if let Some(compaction) = persist.compaction {
                    if let Some(enabled) = compaction.enabled {
                        self.table.persist.compaction.enabled = enabled;
                    }
                    if let Some(interval_sec) = compaction.interval_sec {
                        self.table.persist.compaction.interval_sec = interval_sec;
                    }
                    if let Some(min_files) = compaction.min_files {
                        self.table.persist.compaction.min_files = min_files;
                    }
                    if let Some(delete_sources) = compaction.delete_sources {
                        self.table.persist.compaction.delete_sources = delete_sources;
                    }
                    if let Some(sort_column) = compaction.sort_column {
                        self.table.persist.compaction.sort_column = Some(non_empty(
                            sort_column,
                            "table.persist.compaction.sort_column",
                        )?);
                    }
                }
            }
        }
        if let Some(gateway) = partial.gateway {
            if let Some(enabled) = gateway.enabled {
                self.gateway.enabled = enabled;
            }
            if let Some(endpoint) = gateway.endpoint {
                self.gateway.endpoint = Some(non_empty(endpoint, "gateway.endpoint")?);
            }
            if let Some(host) = gateway.host {
                self.gateway.host = Some(non_empty(host, "gateway.host")?);
            }
            if let Some(port) = gateway.port {
                self.gateway.port = Some(port);
            }
            if let Some(token) = gateway.token {
                self.gateway.token = Some(non_empty(token, "gateway.token")?);
            }
            if let Some(protocol_version) = gateway.protocol_version {
                self.gateway.protocol_version = protocol_version;
            }
        }
        if let Some(query) = partial.query {
            if let Some(parallelism) = query.parallelism {
                self.query.parallelism = Some(parallelism);
            }
            if let Some(max_buffered_bytes) = query.max_buffered_bytes {
                self.query.max_buffered_bytes = Some(max_buffered_bytes);
            }
            if let Some(plan_ttl_ms) = query.plan_ttl_ms {
                self.query.plan_ttl_ms = plan_ttl_ms;
            }
            if let Some(resources) = query.resources {
                if let Some(max_concurrent_queries) = resources.max_concurrent_queries {
                    self.query.resources.max_concurrent_queries = max_concurrent_queries;
                }
                if let Some(max_active_tasks) = resources.max_active_tasks {
                    self.query.resources.max_active_tasks = max_active_tasks;
                    resolution.query_max_active_tasks_explicit = true;
                }
                if let Some(max_memory_bytes) = resources.max_memory_bytes {
                    self.query.resources.max_memory_bytes = max_memory_bytes;
                }
            }
            if let Some(canary) = query.canary {
                if let Some(enabled) = canary.enabled {
                    self.query.canary.enabled = enabled;
                }
                if let Some(max_rows) = canary.max_rows {
                    self.query.canary.max_rows = max_rows;
                }
                if let Some(max_batches) = canary.max_batches {
                    self.query.canary.max_batches = max_batches;
                }
                if let Some(max_output_bytes) = canary.max_output_bytes {
                    self.query.canary.max_output_bytes = max_output_bytes;
                }
                if let Some(operator_memory_bytes) = canary.operator_memory_bytes {
                    self.query.canary.operator_memory_bytes = Some(operator_memory_bytes);
                }
            }
        }
        Ok(())
    }

    fn apply_env(&mut self, resolution: &mut ConfigResolutionState) -> Result<()> {
        if let Some(data_dir) = std::env::var_os("ZIPPY_SERVER_DATA_DIR") {
            let data_dir = expand_home_path(PathBuf::from(data_dir));
            if data_dir.as_os_str().is_empty() {
                return Err(ZippyError::InvalidConfig {
                    reason: "env var must not be empty name=[ZIPPY_SERVER_DATA_DIR]".to_string(),
                });
            }
            if !data_dir.is_absolute() {
                return Err(ZippyError::InvalidConfig {
                    reason: "env path must be absolute name=[ZIPPY_SERVER_DATA_DIR]".to_string(),
                });
            }
            self.server.data_dir = Some(data_dir);
        }
        if let Some(value) = env_string("ZIPPY_SERVER_IMPORT_LEGACY_SNAPSHOT") {
            self.server.import_legacy_endpoint_snapshot =
                parse_bool_env("ZIPPY_SERVER_IMPORT_LEGACY_SNAPSHOT", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_SERVER_NATIVE_STORAGE") {
            self.server.native_storage_enabled =
                parse_bool_env("ZIPPY_SERVER_NATIVE_STORAGE", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_CATALOG_BACKEND") {
            self.catalog.backend = parse_catalog_backend_env(&value)?;
        }
        if let Some(value) = env_string("ZIPPY_CATALOG_RUNTIME_SNAPSHOT_PINS_MAX_ACTIVE_PINS") {
            self.catalog.runtime_snapshot_pins.max_active_pins = parse_usize_env(
                "ZIPPY_CATALOG_RUNTIME_SNAPSHOT_PINS_MAX_ACTIVE_PINS",
                &value,
            )?;
        }
        if let Some(value) = env_string("ZIPPY_CATALOG_RUNTIME_SNAPSHOT_PINS_MAX_ACTIVE_TABLES") {
            self.catalog.runtime_snapshot_pins.max_active_tables = parse_usize_env(
                "ZIPPY_CATALOG_RUNTIME_SNAPSHOT_PINS_MAX_ACTIVE_TABLES",
                &value,
            )?;
        }
        if let Some(value) = env_string("ZIPPY_CATALOG_SHADOW_QUEUE_CAPACITY") {
            self.catalog.shadow.queue_capacity =
                parse_usize_env("ZIPPY_CATALOG_SHADOW_QUEUE_CAPACITY", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_CATALOG_SHADOW_METADATA_BUDGET_BYTES") {
            self.catalog.shadow.metadata_budget_bytes =
                parse_usize_env("ZIPPY_CATALOG_SHADOW_METADATA_BUDGET_BYTES", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_CATALOG_SHADOW_MAX_FILES_PER_COMPARISON") {
            self.catalog.shadow.max_files_per_comparison =
                parse_usize_env("ZIPPY_CATALOG_SHADOW_MAX_FILES_PER_COMPARISON", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_CATALOG_SHADOW_MAX_METADATA_BYTES_PER_COMPARISON") {
            self.catalog.shadow.max_metadata_bytes_per_comparison = parse_usize_env(
                "ZIPPY_CATALOG_SHADOW_MAX_METADATA_BYTES_PER_COMPARISON",
                &value,
            )?;
        }
        if let Some(value) = env_string("ZIPPY_CATALOG_SHADOW_COMPARISON_TIMEOUT_MS") {
            self.catalog.shadow.comparison_timeout_ms =
                parse_u64_env("ZIPPY_CATALOG_SHADOW_COMPARISON_TIMEOUT_MS", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_CATALOG_SHADOW_DEDUP_CAPACITY") {
            self.catalog.shadow.dedup_capacity =
                parse_usize_env("ZIPPY_CATALOG_SHADOW_DEDUP_CAPACITY", &value)?;
        }
        let max_cpu_cores = env_string("ZIPPY_MAX_CPU_CORES")
            .map(|value| parse_usize_env("ZIPPY_MAX_CPU_CORES", &value))
            .transpose()?;
        let max_parallelism = env_string("ZIPPY_MAX_PARALLELISM")
            .map(|value| parse_usize_env("ZIPPY_MAX_PARALLELISM", &value))
            .transpose()?;
        if let Some(value) =
            resolve_runtime_cpu_aliases(max_cpu_cores, max_parallelism, "environment")?
        {
            self.runtime.max_cpu_cores = value;
        }
        if let Some(value) = env_string("ZIPPY_MAX_QUERY_BUFFER_BYTES") {
            self.runtime.max_query_buffer_bytes =
                parse_usize_env("ZIPPY_MAX_QUERY_BUFFER_BYTES", &value)?;
        }
        if let Some(host) = env_string("ZIPPY_MASTER_HOST") {
            self.master.host = Some(non_empty(host, "ZIPPY_MASTER_HOST")?);
        }
        if let Some(value) = env_string("ZIPPY_MASTER_PORT") {
            self.master.port =
                Some(
                    value
                        .parse::<u16>()
                        .map_err(|error| ZippyError::InvalidConfig {
                            reason: format!(
                                "env var must parse as u16 name=[ZIPPY_MASTER_PORT] error=[{}]",
                                error
                            ),
                        })?,
                );
        }
        if let Some(value) = env_string("ZIPPY_MASTER_SHUTDOWN_TIMEOUT_MS") {
            self.master.shutdown_timeout_ms =
                value
                    .parse::<u64>()
                    .map_err(|error| ZippyError::InvalidConfig {
                        reason: format!(
                            "env var must parse as u64 name=[ZIPPY_MASTER_SHUTDOWN_TIMEOUT_MS] error=[{}]",
                            error
                        ),
                    })?;
        }
        if let Some(token) = env_string("ZIPPY_MASTER_TOKEN") {
            self.master.token = Some(non_empty(token, "ZIPPY_MASTER_TOKEN")?);
        }
        if let Some(level) = env_string("ZIPPY_LOG_LEVEL") {
            self.log.level = non_empty(level, "ZIPPY_LOG_LEVEL")?;
        }
        if let Some(value) = env_string("ZIPPY_QUERY_PARALLELISM") {
            self.query.parallelism = Some(parse_usize_env("ZIPPY_QUERY_PARALLELISM", &value)?);
        }
        if let Some(value) = env_string("ZIPPY_QUERY_MAX_BUFFERED_BYTES") {
            self.query.max_buffered_bytes =
                Some(parse_usize_env("ZIPPY_QUERY_MAX_BUFFERED_BYTES", &value)?);
        }
        if let Some(value) = env_string("ZIPPY_QUERY_PLAN_TTL_MS") {
            self.query.plan_ttl_ms = parse_u64_env("ZIPPY_QUERY_PLAN_TTL_MS", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_QUERY_MAX_CONCURRENT_QUERIES") {
            self.query.resources.max_concurrent_queries =
                parse_usize_env("ZIPPY_QUERY_MAX_CONCURRENT_QUERIES", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_QUERY_MAX_ACTIVE_TASKS") {
            self.query.resources.max_active_tasks =
                parse_usize_env("ZIPPY_QUERY_MAX_ACTIVE_TASKS", &value)?;
            resolution.query_max_active_tasks_explicit = true;
        }
        if let Some(value) = env_string("ZIPPY_QUERY_MAX_MEMORY_BYTES") {
            self.query.resources.max_memory_bytes =
                parse_u64_env("ZIPPY_QUERY_MAX_MEMORY_BYTES", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_QUERY_CANARY_ENABLED") {
            self.query.canary.enabled = parse_bool_env("ZIPPY_QUERY_CANARY_ENABLED", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_QUERY_CANARY_MAX_ROWS") {
            self.query.canary.max_rows = parse_u64_env("ZIPPY_QUERY_CANARY_MAX_ROWS", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_QUERY_CANARY_MAX_BATCHES") {
            self.query.canary.max_batches =
                parse_usize_env("ZIPPY_QUERY_CANARY_MAX_BATCHES", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_QUERY_CANARY_MAX_OUTPUT_BYTES") {
            self.query.canary.max_output_bytes =
                parse_u64_env("ZIPPY_QUERY_CANARY_MAX_OUTPUT_BYTES", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_QUERY_CANARY_OPERATOR_MEMORY_BYTES") {
            self.query.canary.operator_memory_bytes = Some(parse_u64_env(
                "ZIPPY_QUERY_CANARY_OPERATOR_MEMORY_BYTES",
                &value,
            )?);
        }
        if let Some(value) = env_string("ZIPPY_TABLE_ROW_CAPACITY") {
            self.table.row_capacity =
                value
                    .parse::<usize>()
                    .map_err(|error| ZippyError::InvalidConfig {
                        reason: format!(
                        "env var must parse as usize name=[ZIPPY_TABLE_ROW_CAPACITY] error=[{}]",
                        error
                    ),
                    })?;
        }
        if let Some(value) = env_string("ZIPPY_TABLE_RETENTION_SEGMENTS") {
            self.table.retention_segments =
                Some(value.parse::<usize>().map_err(|error| ZippyError::InvalidConfig {
                    reason: format!(
                        "env var must parse as usize name=[ZIPPY_TABLE_RETENTION_SEGMENTS] error=[{}]",
                        error
                    ),
                })?);
        }
        if let Some(value) = env_string("ZIPPY_TABLE_PERSIST") {
            self.table.persist.enabled = parse_bool_env("ZIPPY_TABLE_PERSIST", &value)?;
            if self.table.persist.enabled && self.table.persist.method.trim().is_empty() {
                self.table.persist.method = DEFAULT_TABLE_PERSIST_METHOD.to_string();
            }
        }
        if let Some(method) = env_string("ZIPPY_TABLE_PERSIST_METHOD") {
            self.table.persist.method = non_empty(method, "ZIPPY_TABLE_PERSIST_METHOD")?;
        }
        if let Some(data_dir) = env_string("ZIPPY_TABLE_PERSIST_DATA_DIR") {
            self.table.persist.data_dir = non_empty(data_dir, "ZIPPY_TABLE_PERSIST_DATA_DIR")?;
        }
        if let Some(dt_column) = env_string("ZIPPY_TABLE_PERSIST_PARTITION_DT_COLUMN") {
            self.table.persist.partition.dt_column = Some(non_empty(
                dt_column,
                "ZIPPY_TABLE_PERSIST_PARTITION_DT_COLUMN",
            )?);
        }
        if let Some(id_column) = env_string("ZIPPY_TABLE_PERSIST_PARTITION_ID_COLUMN") {
            self.table.persist.partition.id_column = Some(non_empty(
                id_column,
                "ZIPPY_TABLE_PERSIST_PARTITION_ID_COLUMN",
            )?);
        }
        if let Some(dt_part) = env_string("ZIPPY_TABLE_PERSIST_PARTITION_DT_PART") {
            self.table.persist.partition.dt_part =
                Some(non_empty(dt_part, "ZIPPY_TABLE_PERSIST_PARTITION_DT_PART")?);
        }
        if let Some(value) = env_string("ZIPPY_TABLE_PERSIST_COMPACTION") {
            self.table.persist.compaction.enabled =
                parse_bool_env("ZIPPY_TABLE_PERSIST_COMPACTION", &value)?;
        }
        if let Some(value) = env_string("ZIPPY_TABLE_PERSIST_COMPACTION_INTERVAL_SEC") {
            self.table.persist.compaction.interval_sec =
                value
                    .parse::<f64>()
                    .map_err(|error| ZippyError::InvalidConfig {
                        reason: format!(
                            "env var must parse as f64 name=[ZIPPY_TABLE_PERSIST_COMPACTION_INTERVAL_SEC] error=[{}]",
                            error
                        ),
                    })?;
        }
        if let Some(value) = env_string("ZIPPY_TABLE_PERSIST_COMPACTION_MIN_FILES") {
            self.table.persist.compaction.min_files =
                value
                    .parse::<usize>()
                    .map_err(|error| ZippyError::InvalidConfig {
                        reason: format!(
                            "env var must parse as usize name=[ZIPPY_TABLE_PERSIST_COMPACTION_MIN_FILES] error=[{}]",
                            error
                        ),
                    })?;
        }
        if let Some(value) = env_string("ZIPPY_TABLE_PERSIST_COMPACTION_DELETE_SOURCES") {
            self.table.persist.compaction.delete_sources =
                parse_bool_env("ZIPPY_TABLE_PERSIST_COMPACTION_DELETE_SOURCES", &value)?;
        }
        if let Some(sort_column) = env_string("ZIPPY_TABLE_PERSIST_COMPACTION_SORT_COLUMN") {
            self.table.persist.compaction.sort_column = Some(non_empty(
                sort_column,
                "ZIPPY_TABLE_PERSIST_COMPACTION_SORT_COLUMN",
            )?);
        }
        if let Some(value) = env_string("ZIPPY_GATEWAY") {
            self.gateway.enabled = parse_bool_env("ZIPPY_GATEWAY", &value)?;
        }
        if let Some(endpoint) = env_string("ZIPPY_GATEWAY_ENDPOINT") {
            self.gateway.endpoint = Some(non_empty(endpoint, "ZIPPY_GATEWAY_ENDPOINT")?);
        }
        if let Some(host) = env_string("ZIPPY_GATEWAY_HOST") {
            self.gateway.host = Some(non_empty(host, "ZIPPY_GATEWAY_HOST")?);
        }
        if let Some(value) = env_string("ZIPPY_GATEWAY_PORT") {
            self.gateway.port =
                Some(
                    value
                        .parse::<u16>()
                        .map_err(|error| ZippyError::InvalidConfig {
                            reason: format!(
                                "env var must parse as u16 name=[ZIPPY_GATEWAY_PORT] error=[{}]",
                                error
                            ),
                        })?,
                );
        }
        if let Some(token) = env_string("ZIPPY_GATEWAY_TOKEN") {
            self.gateway.token = Some(non_empty(token, "ZIPPY_GATEWAY_TOKEN")?);
        }
        if let Some(value) = env_string("ZIPPY_GATEWAY_PROTOCOL_VERSION") {
            self.gateway.protocol_version =
                value
                    .parse::<u16>()
                    .map_err(|error| ZippyError::InvalidConfig {
                        reason: format!(
                        "env var must parse as u16 name=[ZIPPY_GATEWAY_PROTOCOL_VERSION] error=[{}]",
                        error
                    ),
                    })?;
        }
        Ok(())
    }

    fn resolve_query_resource_defaults(
        &mut self,
        resolution: &ConfigResolutionState,
    ) -> Result<()> {
        if !resolution.query_max_active_tasks_explicit {
            self.query.resources.max_active_tasks = self
                .query_parallelism()
                .checked_mul(DEFAULT_QUERY_ACTIVE_TASKS_PER_CPU)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: format!(
                        "derived query max_active_tasks exceeds usize query_parallelism=[{}]",
                        self.query_parallelism()
                    ),
                })?;
        }
        Ok(())
    }

    fn finalize_resolution(&mut self, resolution: &ConfigResolutionState) -> Result<()> {
        self.resolve_query_resource_defaults(resolution)?;
        self.derive_endpoints()?;
        self.validate()
    }

    /// Resolves the managed server data root.
    ///
    /// `default` should be stable for one control endpoint. It is used only
    /// when neither `[server].data_dir` nor `ZIPPY_SERVER_DATA_DIR` is set.
    pub fn server_data_dir(&self, default: impl AsRef<Path>) -> PathBuf {
        self.server
            .data_dir
            .clone()
            .unwrap_or_else(|| default.as_ref().to_path_buf())
    }

    fn validate(&self) -> Result<()> {
        for (name, value, maximum) in [
            (
                "catalog.runtime_snapshot_pins.max_active_pins",
                self.catalog.runtime_snapshot_pins.max_active_pins,
                MAX_CATALOG_RUNTIME_SNAPSHOT_PINS,
            ),
            (
                "catalog.runtime_snapshot_pins.max_active_tables",
                self.catalog.runtime_snapshot_pins.max_active_tables,
                MAX_CATALOG_RUNTIME_SNAPSHOT_PIN_TABLES,
            ),
        ] {
            if !(1..=maximum).contains(&value) {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "{} must be between 1 and {} value=[{}]",
                        name, maximum, value
                    ),
                });
            }
        }
        for (name, value) in [
            (
                "catalog.shadow.queue_capacity",
                self.catalog.shadow.queue_capacity,
            ),
            (
                "catalog.shadow.metadata_budget_bytes",
                self.catalog.shadow.metadata_budget_bytes,
            ),
            (
                "catalog.shadow.max_files_per_comparison",
                self.catalog.shadow.max_files_per_comparison,
            ),
            (
                "catalog.shadow.max_metadata_bytes_per_comparison",
                self.catalog.shadow.max_metadata_bytes_per_comparison,
            ),
            (
                "catalog.shadow.dedup_capacity",
                self.catalog.shadow.dedup_capacity,
            ),
        ] {
            if value == 0 {
                return Err(ZippyError::InvalidConfig {
                    reason: format!("{} must be greater than zero", name),
                });
            }
        }
        if self.catalog.shadow.max_metadata_bytes_per_comparison
            > self.catalog.shadow.metadata_budget_bytes
        {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "catalog shadow per-comparison metadata budget must not exceed total budget \
                     per_comparison=[{}] total=[{}]",
                    self.catalog.shadow.max_metadata_bytes_per_comparison,
                    self.catalog.shadow.metadata_budget_bytes
                ),
            });
        }
        if self.catalog.shadow.comparison_timeout_ms == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "catalog.shadow.comparison_timeout_ms must be greater than zero"
                    .to_string(),
            });
        }
        match self.catalog.backend {
            ZippyCatalogBackend::Legacy => {}
            ZippyCatalogBackend::SqliteShadow if !self.server.native_storage_enabled => {
                return Err(ZippyError::InvalidConfig {
                    reason: "catalog sqlite_shadow backend requires server native_storage_enabled"
                        .to_string(),
                });
            }
            ZippyCatalogBackend::SqliteShadow => {}
            ZippyCatalogBackend::Sqlite => {
                return Err(ZippyError::InvalidConfig {
                    reason: "catalog sqlite authority is not available before explicit cutover"
                        .to_string(),
                });
            }
        }
        if self.runtime.max_cpu_cores == 0 || self.runtime.max_cpu_cores > MAX_CPU_CORES {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "runtime max_cpu_cores must be between 1 and {} value=[{}]",
                    MAX_CPU_CORES, self.runtime.max_cpu_cores
                ),
            });
        }
        if let Some(parallelism) = self.query.parallelism {
            if parallelism == 0 || parallelism > self.runtime.max_cpu_cores {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "query parallelism must be between 1 and runtime max_cpu_cores value=[{}] max_cpu_cores=[{}]",
                        parallelism, self.runtime.max_cpu_cores
                    ),
                });
            }
        }
        if self.query.plan_ttl_ms == 0 || self.query.plan_ttl_ms > MAX_QUERY_PLAN_TTL_MS {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "query plan_ttl_ms must be between 1 and {} value=[{}]",
                    MAX_QUERY_PLAN_TTL_MS, self.query.plan_ttl_ms
                ),
            });
        }
        if self.query.resources.max_concurrent_queries == 0
            || self.query.resources.max_concurrent_queries > MAX_QUERY_CONCURRENT_QUERIES
        {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "query resources max_concurrent_queries must be between 1 and {} value=[{}]",
                    MAX_QUERY_CONCURRENT_QUERIES, self.query.resources.max_concurrent_queries
                ),
            });
        }
        if self.query.resources.max_active_tasks == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "query resources max_active_tasks must be greater than zero".to_string(),
            });
        }
        if self.query.resources.max_memory_bytes == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "query resources max_memory_bytes must be greater than zero".to_string(),
            });
        }
        if self.query.canary.max_rows == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "query canary max_rows must be greater than zero".to_string(),
            });
        }
        if self.query.canary.max_batches == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "query canary max_batches must be greater than zero".to_string(),
            });
        }
        if self.query.canary.max_output_bytes == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "query canary max_output_bytes must be greater than zero".to_string(),
            });
        }
        if self.query.canary.operator_memory_bytes == Some(0) {
            return Err(ZippyError::InvalidConfig {
                reason: "query canary operator_memory_bytes must be greater than zero".to_string(),
            });
        }
        if self
            .query
            .canary
            .operator_memory_bytes
            .is_some_and(|value| value > self.query.resources.max_memory_bytes)
        {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "query canary operator_memory_bytes must not exceed query resources memory \
                     operator_memory_bytes=[{}] max_memory_bytes=[{}]",
                    self.query.canary.operator_memory_bytes.unwrap_or_default(),
                    self.query.resources.max_memory_bytes
                ),
            });
        }
        if self.query.canary.enabled && !self.server.native_storage_enabled {
            return Err(ZippyError::InvalidConfig {
                reason: "query canary requires server native_storage_enabled".to_string(),
            });
        }
        let query_parallelism = self.query_parallelism();
        if self.query.resources.max_active_tasks < query_parallelism {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    concat!(
                        "query resources max_active_tasks must not be less than effective query ",
                        "parallelism value=[{}] query_parallelism=[{}]"
                    ),
                    self.query.resources.max_active_tasks, query_parallelism
                ),
            });
        }
        if self.runtime.max_query_buffer_bytes == 0
            || self.runtime.max_query_buffer_bytes > MAX_QUERY_BUFFER_BYTES
        {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "runtime max_query_buffer_bytes must be between 1 and {} value=[{}]",
                    MAX_QUERY_BUFFER_BYTES, self.runtime.max_query_buffer_bytes
                ),
            });
        }
        if let Some(max_buffered_bytes) = self.query.max_buffered_bytes {
            if max_buffered_bytes == 0 {
                return Err(ZippyError::InvalidConfig {
                    reason: "query max_buffered_bytes must be greater than zero".to_string(),
                });
            }
            if max_buffered_bytes > self.runtime.max_query_buffer_bytes {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "query max_buffered_bytes must not exceed runtime max_query_buffer_bytes value=[{}] max_query_buffer_bytes=[{}]",
                        max_buffered_bytes, self.runtime.max_query_buffer_bytes
                    ),
                });
            }
        }
        if self.table.row_capacity == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "table row_capacity must be greater than zero".to_string(),
            });
        }
        if self.table.persist.enabled && self.table.persist.method != DEFAULT_TABLE_PERSIST_METHOD {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "unsupported table persist method method=[{}]",
                    self.table.persist.method
                ),
            });
        }
        if self.table.persist.enabled && self.table.persist.data_dir.trim().is_empty() {
            return Err(ZippyError::InvalidConfig {
                reason: "table persist data_dir must not be empty".to_string(),
            });
        }
        validate_persist_partition(&self.table.persist.partition)?;
        validate_persist_compaction(&self.table.persist.compaction)?;
        validate_host_port("master", self.master.host.as_ref(), self.master.port)?;
        if self.master.shutdown_timeout_ms == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "master shutdown_timeout_ms must be greater than zero".to_string(),
            });
        }
        validate_gateway_host_port(&self.gateway)?;
        validate_gateway(&self.gateway)?;
        Ok(())
    }

    fn derive_endpoints(&mut self) -> Result<()> {
        validate_host_port("master", self.master.host.as_ref(), self.master.port)?;
        let should_derive_gateway =
            self.gateway.enabled || self.gateway.host.is_some() || self.gateway.port.is_some();
        if should_derive_gateway && self.gateway.endpoint.is_none() {
            self.gateway.endpoint = self.gateway_endpoint();
        }
        Ok(())
    }

    pub fn master_endpoint(&self) -> Option<String> {
        match (self.master.host.as_ref(), self.master.port) {
            (Some(host), Some(port)) => Some(format!("{host}:{port}")),
            _ => None,
        }
    }

    pub fn gateway_endpoint(&self) -> Option<String> {
        self.gateway.endpoint.clone().or_else(|| {
            let host = self.gateway.host.as_ref().or(self.master.host.as_ref())?;
            let port = self.gateway.port.or_else(|| {
                self.master
                    .port
                    .and_then(|master_port| master_port.checked_add(1))
            })?;
            Some(format!("{host}:{port}"))
        })
    }

    /// Return the effective query limit after applying runtime inheritance.
    pub fn query_parallelism(&self) -> usize {
        self.query.parallelism.unwrap_or(self.runtime.max_cpu_cores)
    }

    /// Return the effective per-query buffered Arrow byte limit.
    pub fn query_max_buffered_bytes(&self) -> usize {
        self.query
            .max_buffered_bytes
            .unwrap_or(DEFAULT_QUERY_MAX_BUFFERED_BYTES)
            .min(self.runtime.max_query_buffer_bytes)
    }

    /// Return the hard native canary operator pool derived inside the global query budget.
    pub fn query_canary_operator_memory_bytes(&self) -> u64 {
        self.query.canary.operator_memory_bytes.unwrap_or_else(|| {
            DEFAULT_QUERY_CANARY_OPERATOR_MEMORY_BYTES
                .min(self.query.resources.max_memory_bytes.saturating_div(2))
                .max(1)
        })
    }
}

pub fn default_config_path() -> PathBuf {
    expand_home_config_path(DEFAULT_CONFIG_PATH)
}

fn parse_config_toml(content: &str, path: &Path) -> Result<PartialZippyConfig> {
    toml::from_str(content).map_err(|error| ZippyError::InvalidConfig {
        reason: format!(
            "failed to parse zippy config path=[{}] error=[{}]",
            path.display(),
            error
        ),
    })
}

fn env_string(key: &str) -> Option<String> {
    std::env::var_os(key).map(|value| value.to_string_lossy().to_string())
}

fn resolve_runtime_cpu_aliases(
    max_cpu_cores: Option<usize>,
    max_parallelism: Option<usize>,
    layer: &str,
) -> Result<Option<usize>> {
    match (max_cpu_cores, max_parallelism) {
        (Some(canonical), Some(legacy)) if canonical != legacy => Err(ZippyError::InvalidConfig {
            reason: format!(
                concat!(
                    "conflicting runtime CPU limits layer=[{}] ",
                    "max_cpu_cores=[{}] max_parallelism=[{}]"
                ),
                layer, canonical, legacy
            ),
        }),
        (Some(canonical), _) => Ok(Some(canonical)),
        (None, Some(legacy)) => Ok(Some(legacy)),
        (None, None) => Ok(None),
    }
}

fn canonicalize_runtime_cpu_wire_aliases(value: &mut serde_json::Value) -> Result<()> {
    let Some(runtime) = value
        .get_mut("runtime")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return Ok(());
    };
    let max_cpu_cores = parse_wire_usize(runtime.get("max_cpu_cores"), "runtime.max_cpu_cores")?;
    let max_parallelism =
        parse_wire_usize(runtime.get("max_parallelism"), "runtime.max_parallelism")?;
    let resolved =
        resolve_runtime_cpu_aliases(max_cpu_cores, max_parallelism, "GetConfig wire response")?;
    runtime.remove("max_parallelism");
    if let Some(max_cpu_cores) = resolved {
        runtime.insert(
            "max_cpu_cores".to_string(),
            serde_json::json!(max_cpu_cores),
        );
    }
    Ok(())
}

fn parse_wire_usize(value: Option<&serde_json::Value>, name: &str) -> Result<Option<usize>> {
    value
        .cloned()
        .map(|value| {
            serde_json::from_value::<usize>(value).map_err(|error| ZippyError::InvalidConfig {
                reason: format!(
                    "wire config field must parse as usize name=[{}] error=[{}]",
                    name, error
                ),
            })
        })
        .transpose()
}

fn reject_non_nullable_json_nulls(
    overlay: &serde_json::Value,
    defaults: &serde_json::Value,
    path: &str,
) -> Result<()> {
    match (overlay, defaults) {
        (serde_json::Value::Null, default) if !default.is_null() => {
            Err(ZippyError::InvalidConfig {
                reason: format!("JSON config overlay field must not be null path=[{}]", path),
            })
        }
        (serde_json::Value::Object(overlay), serde_json::Value::Object(defaults)) => {
            for (key, value) in overlay {
                let Some(default) = defaults.get(key) else {
                    continue;
                };
                let field_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                reject_non_nullable_json_nulls(value, default, &field_path)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn parse_bool_env(key: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ZippyError::InvalidConfig {
            reason: format!(
                "env var must parse as bool name=[{}] value=[{}]",
                key, value
            ),
        }),
    }
}

fn parse_catalog_backend_env(value: &str) -> Result<ZippyCatalogBackend> {
    match value.trim().to_ascii_lowercase().as_str() {
        "legacy" => Ok(ZippyCatalogBackend::Legacy),
        "sqlite_shadow" => Ok(ZippyCatalogBackend::SqliteShadow),
        "sqlite" => Ok(ZippyCatalogBackend::Sqlite),
        _ => Err(ZippyError::InvalidConfig {
            reason: format!(
                concat!(
                    "env var must be one of legacy, sqlite_shadow, or sqlite ",
                    "name=[ZIPPY_CATALOG_BACKEND] value=[{}]"
                ),
                value
            ),
        }),
    }
}

fn parse_usize_env(key: &str, value: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .map_err(|error| ZippyError::InvalidConfig {
            reason: format!(
                "env var must parse as usize name=[{}] error=[{}]",
                key, error
            ),
        })
}

fn parse_u64_env(key: &str, value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|error| ZippyError::InvalidConfig {
            reason: format!("env var must parse as u64 name=[{}] error=[{}]", key, error),
        })
}

fn non_empty(value: String, name: &str) -> Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(ZippyError::InvalidConfig {
            reason: format!("config value must not be empty name=[{}]", name),
        });
    }
    Ok(value)
}

fn validate_persist_partition(partition: &ZippyTablePersistPartitionConfig) -> Result<()> {
    if partition.dt_part.is_some() && partition.dt_column.is_none() {
        return Err(ZippyError::InvalidConfig {
            reason: "table persist partition dt_part requires dt_column".to_string(),
        });
    }
    if partition.dt_column.is_some() && partition.dt_part.is_none() {
        return Err(ZippyError::InvalidConfig {
            reason: "table persist partition dt_column requires dt_part".to_string(),
        });
    }
    if let Some(dt_part) = &partition.dt_part {
        if !SUPPORTED_TABLE_PERSIST_DT_PARTS.contains(&dt_part.as_str()) {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "unsupported table persist partition dt_part value=[{}]",
                    dt_part
                ),
            });
        }
    }
    Ok(())
}

fn validate_persist_compaction(compaction: &ZippyTablePersistCompactionConfig) -> Result<()> {
    if compaction.interval_sec <= 0.0 || !compaction.interval_sec.is_finite() {
        return Err(ZippyError::InvalidConfig {
            reason: "table persist compaction interval_sec must be positive".to_string(),
        });
    }
    if compaction.min_files < 2 {
        return Err(ZippyError::InvalidConfig {
            reason: "table persist compaction min_files must be at least 2".to_string(),
        });
    }
    if compaction.enabled && compaction.delete_sources {
        return Err(ZippyError::InvalidConfig {
            reason: "table persist compaction delete_sources requires persisted snapshot leases"
                .to_string(),
        });
    }
    Ok(())
}

fn validate_host_port(name: &str, host: Option<&String>, port: Option<u16>) -> Result<()> {
    match (host, port) {
        (Some(_), Some(0)) => Err(ZippyError::InvalidConfig {
            reason: format!("{name} port must be greater than zero"),
        }),
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) | (None, Some(_)) => Err(ZippyError::InvalidConfig {
            reason: format!("{name} host and port must be set together"),
        }),
    }
}

fn validate_gateway_host_port(gateway: &ZippyGatewayConfig) -> Result<()> {
    if gateway.port == Some(0) {
        return Err(ZippyError::InvalidConfig {
            reason: "gateway port must be greater than zero".to_string(),
        });
    }
    Ok(())
}

fn validate_gateway(gateway: &ZippyGatewayConfig) -> Result<()> {
    if gateway.protocol_version == 0 {
        return Err(ZippyError::InvalidConfig {
            reason: "gateway protocol_version must be greater than zero".to_string(),
        });
    }
    if gateway.enabled {
        match &gateway.endpoint {
            Some(endpoint) if !endpoint.trim().is_empty() => {}
            _ => {
                return Err(ZippyError::InvalidConfig {
                    reason:
                        "gateway endpoint must be set when enabled or derivable from master port"
                            .to_string(),
                });
            }
        }
    }
    Ok(())
}

fn expand_home_config_path(path: &str) -> PathBuf {
    expand_home_path(PathBuf::from(path))
}

fn expand_home_path(path: PathBuf) -> PathBuf {
    if path == Path::new("~") {
        return home_dir();
    }
    if let Ok(relative) = path.strip_prefix("~") {
        return home_dir().join(relative);
    }
    path
}

fn resolve_config_path(path: &str, config_base: &Path) -> PathBuf {
    let path = expand_home_config_path(path);
    if path.is_absolute() {
        path
    } else {
        config_base.join(path)
    }
}

fn absolute_parent(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    if parent.is_absolute() {
        return Ok(parent.to_path_buf());
    }
    std::env::current_dir()
        .map(|current| current.join(parent))
        .map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to resolve zippy config parent path=[{}] error=[{}]",
                path.display(),
                error
            ),
        })
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
}
