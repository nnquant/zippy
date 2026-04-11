use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use zippy_core::{Result, ZippyError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotStreamRecord {
    pub stream_name: String,
    pub ring_capacity: usize,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegistrySnapshot {
    pub streams: Vec<SnapshotStreamRecord>,
    pub sources: Vec<serde_json::Value>,
    pub engines: Vec<serde_json::Value>,
    pub sinks: Vec<serde_json::Value>,
}

pub struct SnapshotStore;

impl SnapshotStore {
    pub fn write(path: &Path, snapshot: &RegistrySnapshot) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| ZippyError::Io {
                reason: format!("failed to create registry snapshot parent error=[{}]", error),
            })?;
        }

        let temp_path = path.with_extension("tmp");
        let bytes = serde_json::to_vec_pretty(snapshot).map_err(|error| ZippyError::Io {
            reason: format!("failed to serialize registry snapshot error=[{}]", error),
        })?;
        fs::write(&temp_path, bytes).map_err(|error| ZippyError::Io {
            reason: format!("failed to write registry snapshot temp file error=[{}]", error),
        })?;
        fs::rename(&temp_path, path).map_err(|error| ZippyError::Io {
            reason: format!("failed to move registry snapshot into place error=[{}]", error),
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
