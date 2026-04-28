use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Result, ZippyError};

pub const DEFAULT_CONFIG_PATH: &str = "~/.zippy/config.toml";
pub const DEFAULT_LOG_LEVEL: &str = "info";
pub const DEFAULT_TABLE_ROW_CAPACITY: usize = 65_536;
pub const DEFAULT_TABLE_PERSIST_METHOD: &str = "parquet";
pub const DEFAULT_TABLE_PERSIST_DATA_DIR: &str = "data";
const SUPPORTED_TABLE_PERSIST_DT_PARTS: &[&str] = &["%Y", "%Y%m", "%Y%m%d", "%Y%m%d%H"];

/// Process-wide Zippy runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyConfig {
    pub log: ZippyLogConfig,
    pub table: ZippyTableConfig,
}

/// Logging defaults shared by master and Python clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyLogConfig {
    pub level: String,
}

/// Stream table defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyTableConfig {
    pub row_capacity: usize,
    pub retention_segments: Option<usize>,
    pub persist: ZippyTablePersistConfig,
}

/// Stream table persistence defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyTablePersistConfig {
    pub enabled: bool,
    pub method: String,
    pub data_dir: String,
    pub partition: ZippyTablePersistPartitionConfig,
}

/// Stream table persistence partition defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZippyTablePersistPartitionConfig {
    pub dt_column: Option<String>,
    pub id_column: Option<String>,
    pub dt_part: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PartialZippyConfig {
    log: Option<PartialLogConfig>,
    table: Option<PartialTableConfig>,
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
}

#[derive(Debug, Deserialize)]
struct PartialTablePersistPartitionConfig {
    dt_column: Option<String>,
    id_column: Option<String>,
    dt_part: Option<String>,
}

impl Default for ZippyConfig {
    fn default() -> Self {
        Self {
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
                },
            },
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
                config.apply_partial(parse_config_toml(&content, path)?)?;
            }
        }
        config.apply_env()?;
        config.validate()?;
        Ok(config)
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("zippy config must serialize")
    }

    fn apply_partial(&mut self, partial: PartialZippyConfig) -> Result<()> {
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
            }
        }
        Ok(())
    }

    fn apply_env(&mut self) -> Result<()> {
        if let Some(level) = env_string("ZIPPY_LOG_LEVEL") {
            self.log.level = non_empty(level, "ZIPPY_LOG_LEVEL")?;
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
        Ok(())
    }

    fn validate(&self) -> Result<()> {
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
        Ok(())
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

fn expand_home_config_path(path: &str) -> PathBuf {
    if let Some(relative) = path.strip_prefix("~/") {
        return home_dir().join(relative);
    }
    PathBuf::from(path)
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
}
