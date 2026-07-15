use std::fs;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize};

use zippy_core::{Result, ZippyError};
use zippy_types::TableId;

#[derive(Debug, Clone, Serialize)]
pub struct SnapshotStreamRecord {
    pub stream_name: String,
    pub schema: serde_json::Value,
    pub schema_hash: String,
    pub data_path: String,
    pub descriptor_generation: u64,
    #[serde(default)]
    pub sealed_segments: Vec<serde_json::Value>,
    #[serde(default)]
    pub persisted_files: Vec<serde_json::Value>,
    #[serde(default)]
    pub persist_events: Vec<serde_json::Value>,
    #[serde(default)]
    pub persist_revision: u64,
    #[serde(default)]
    pub persisted_files_revision: u64,
    #[serde(default)]
    pub segment_reader_leases: Vec<serde_json::Value>,
    #[serde(default)]
    pub writer_epoch: u64,
    pub buffer_size: usize,
    pub frame_size: usize,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SnapshotStreamRecordV1 {
    stream_name: String,
    #[serde(default = "default_schema")]
    schema: serde_json::Value,
    #[serde(default)]
    schema_hash: String,
    #[serde(default = "default_data_path")]
    data_path: String,
    #[serde(default)]
    descriptor_generation: u64,
    #[serde(default)]
    sealed_segments: Vec<serde_json::Value>,
    #[serde(default)]
    persisted_files: Vec<serde_json::Value>,
    #[serde(default)]
    persist_events: Vec<serde_json::Value>,
    #[serde(default)]
    persist_revision: u64,
    #[serde(default)]
    persisted_files_revision: Option<u64>,
    #[serde(default)]
    segment_reader_leases: Vec<serde_json::Value>,
    #[serde(default)]
    writer_epoch: u64,
    #[serde(default)]
    buffer_size: Option<usize>,
    #[serde(default)]
    ring_capacity: Option<usize>,
    #[serde(default)]
    frame_size: Option<usize>,
    status: String,
}

impl<'de> Deserialize<'de> for SnapshotStreamRecord {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let record = SnapshotStreamRecordV1::deserialize(deserializer)?;
        let buffer_size = record
            .buffer_size
            .or(record.ring_capacity)
            .ok_or_else(|| serde::de::Error::missing_field("buffer_size"))?;
        let frame_size = record.frame_size.unwrap_or(buffer_size);
        let persisted_files_revision = record.persisted_files_revision.unwrap_or_else(|| {
            if record.persisted_files.is_empty() {
                0
            } else {
                record.persist_revision.max(1)
            }
        });
        Ok(Self {
            stream_name: record.stream_name,
            schema: record.schema,
            schema_hash: record.schema_hash,
            data_path: record.data_path,
            descriptor_generation: record.descriptor_generation,
            sealed_segments: record.sealed_segments,
            persisted_files: record.persisted_files,
            persist_events: record.persist_events,
            persist_revision: record.persist_revision,
            persisted_files_revision,
            segment_reader_leases: record.segment_reader_leases,
            writer_epoch: record.writer_epoch,
            buffer_size,
            frame_size,
            status: record.status,
        })
    }
}

fn default_schema() -> serde_json::Value {
    serde_json::json!({
        "fields": [],
        "metadata": {},
    })
}

fn default_data_path() -> String {
    "segment".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotSourceRecord {
    pub source_name: String,
    pub source_type: String,
    pub process_id: String,
    pub output_stream: String,
    pub config: serde_json::Value,
    pub status: String,
    pub metrics: serde_json::Value,
    #[serde(default)]
    pub writer_epoch: u64,
    #[serde(default)]
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotCatalogSourceRecord {
    pub source_name: String,
    pub source_type: String,
    pub output_stream: String,
    pub config: serde_json::Value,
    pub writer_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotDroppedTableRecord {
    pub table_id: TableId,
    pub table_name: String,
    pub dropped_catalog_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEngineRecord {
    pub engine_name: String,
    pub engine_type: String,
    pub process_id: String,
    pub input_stream: String,
    pub output_stream: String,
    pub config: serde_json::Value,
    pub status: String,
    pub metrics: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegistrySnapshot {
    #[serde(default)]
    pub catalog_revision: u64,
    pub streams: Vec<SnapshotStreamRecord>,
    pub sources: Vec<SnapshotSourceRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub catalog_sources: Vec<SnapshotCatalogSourceRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dropped_tables: Vec<SnapshotDroppedTableRecord>,
    pub engines: Vec<SnapshotEngineRecord>,
}

pub struct SnapshotStore;

impl SnapshotStore {
    pub fn write(path: &Path, snapshot: &RegistrySnapshot) -> Result<()> {
        let parent = path.parent().ok_or_else(|| ZippyError::Io {
            reason: format!(
                "registry snapshot path has no parent path=[{}]",
                path.display()
            ),
        })?;
        fs::create_dir_all(parent).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to create registry snapshot parent path=[{}] error=[{}]",
                parent.display(),
                error
            ),
        })?;

        let bytes = serde_json::to_vec_pretty(snapshot).map_err(|error| ZippyError::Io {
            reason: format!("failed to serialize registry snapshot error=[{}]", error),
        })?;
        let mut temp_file = tempfile::Builder::new()
            .prefix(".legacy-master-registry.tmp-")
            .tempfile_in(parent)
            .map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to create registry snapshot temp file parent=[{}] error=[{}]",
                    parent.display(),
                    error
                ),
            })?;
        temp_file
            .write_all(&bytes)
            .map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to write registry snapshot temp file error=[{}]",
                    error
                ),
            })?;
        temp_file
            .as_file()
            .sync_all()
            .map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to fsync registry snapshot temp file error=[{}]",
                    error
                ),
            })?;
        temp_file.persist(path).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to move registry snapshot into place path=[{}] error=[{}]",
                path.display(),
                error.error
            ),
        })?;
        sync_snapshot_parent(parent)?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<RegistrySnapshot> {
        let metadata = fs::symlink_metadata(path).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to inspect registry snapshot path=[{}] error=[{}]",
                path.display(),
                error
            ),
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(ZippyError::Io {
                reason: format!(
                    "registry snapshot must be a regular non-symlink file path=[{}]",
                    path.display()
                ),
            });
        }
        let bytes = fs::read(path).map_err(|error| ZippyError::Io {
            reason: format!("failed to read registry snapshot error=[{}]", error),
        })?;
        serde_json::from_slice(&bytes).map_err(|error| ZippyError::Io {
            reason: format!("failed to decode registry snapshot error=[{}]", error),
        })
    }
}

#[cfg(unix)]
fn sync_snapshot_parent(parent: &Path) -> Result<()> {
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to fsync registry snapshot parent path=[{}] error=[{}]",
                parent.display(),
                error
            ),
        })
}

#[cfg(not(unix))]
fn sync_snapshot_parent(_parent: &Path) -> Result<()> {
    Ok(())
}
