use std::fs;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize};

use zippy_core::{Result, ZippyError};

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
    pub segment_reader_leases: Vec<serde_json::Value>,
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
    segment_reader_leases: Vec<serde_json::Value>,
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
        Ok(Self {
            stream_name: record.stream_name,
            schema: record.schema,
            schema_hash: record.schema_hash,
            data_path: record.data_path,
            descriptor_generation: record.descriptor_generation,
            sealed_segments: record.sealed_segments,
            persisted_files: record.persisted_files,
            persist_events: record.persist_events,
            segment_reader_leases: record.segment_reader_leases,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEngineRecord {
    pub engine_name: String,
    pub engine_type: String,
    pub process_id: String,
    pub input_stream: String,
    pub output_stream: String,
    pub sink_names: Vec<String>,
    pub config: serde_json::Value,
    pub status: String,
    pub metrics: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotSinkRecord {
    pub sink_name: String,
    pub sink_type: String,
    pub process_id: String,
    pub input_stream: String,
    pub config: serde_json::Value,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegistrySnapshot {
    pub streams: Vec<SnapshotStreamRecord>,
    pub sources: Vec<SnapshotSourceRecord>,
    pub engines: Vec<SnapshotEngineRecord>,
    pub sinks: Vec<SnapshotSinkRecord>,
}

pub struct SnapshotStore;

impl SnapshotStore {
    pub fn write(path: &Path, snapshot: &RegistrySnapshot) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to create registry snapshot parent error=[{}]",
                    error
                ),
            })?;
        }

        let temp_path = path.with_extension("tmp");
        let bytes = serde_json::to_vec_pretty(snapshot).map_err(|error| ZippyError::Io {
            reason: format!("failed to serialize registry snapshot error=[{}]", error),
        })?;
        let mut temp_file = fs::File::create(&temp_path).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to create registry snapshot temp file error=[{}]",
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
        temp_file.sync_all().map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to fsync registry snapshot temp file error=[{}]",
                error
            ),
        })?;
        fs::rename(&temp_path, path).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to move registry snapshot into place error=[{}]",
                error
            ),
        })?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<RegistrySnapshot> {
        let bytes = fs::read(path).map_err(|error| ZippyError::Io {
            reason: format!("failed to read registry snapshot error=[{}]", error),
        })?;
        serde_json::from_slice(&bytes).map_err(|error| ZippyError::Io {
            reason: format!("failed to decode registry snapshot error=[{}]", error),
        })
    }
}
