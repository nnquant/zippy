use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct ProcessRecord {
    pub process_id: String,
    pub app: String,
    pub registered_at: u64,
    pub last_heartbeat_at: u64,
    pub lease_status: String,
}

#[derive(Debug, Clone)]
pub struct StreamRecord {
    pub stream_name: String,
    pub ring_capacity: usize,
    pub writer_process_id: Option<String>,
    pub reader_count: usize,
    pub status: String,
    reader_process_ids: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    ProcessNotFound {
        process_id: String,
    },
    StreamNotFound {
        stream_name: String,
    },
    StreamAlreadyExists {
        stream_name: String,
    },
    InvalidRingCapacity {
        stream_name: String,
        ring_capacity: usize,
    },
    StreamRingCapacityMismatch {
        stream_name: String,
        existing_ring_capacity: usize,
        requested_ring_capacity: usize,
    },
    WriterNotOwnedByProcess {
        stream_name: String,
        process_id: String,
        owner_process_id: Option<String>,
    },
    ReaderNotOwnedByProcess {
        stream_name: String,
        reader_id: String,
        process_id: String,
        owner_process_id: Option<String>,
    },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProcessNotFound { process_id } => {
                write!(f, "process not found process_id=[{}]", process_id)
            }
            Self::StreamNotFound { stream_name } => {
                write!(f, "stream not found stream_name=[{}]", stream_name)
            }
            Self::StreamAlreadyExists { stream_name } => {
                write!(f, "stream already exists stream_name=[{}]", stream_name)
            }
            Self::InvalidRingCapacity {
                stream_name,
                ring_capacity,
            } => write!(
                f,
                "invalid ring capacity stream_name=[{}] ring_capacity=[{}]",
                stream_name, ring_capacity
            ),
            Self::StreamRingCapacityMismatch {
                stream_name,
                existing_ring_capacity,
                requested_ring_capacity,
            } => write!(
                f,
                "stream ring capacity mismatch stream_name=[{}] existing_ring_capacity=[{}] requested_ring_capacity=[{}]",
                stream_name, existing_ring_capacity, requested_ring_capacity
            ),
            Self::WriterNotOwnedByProcess {
                stream_name,
                process_id,
                owner_process_id,
            } => write!(
                f,
                "writer not owned stream_name=[{}] process_id=[{}] owner_process_id=[{:?}]",
                stream_name, process_id, owner_process_id
            ),
            Self::ReaderNotOwnedByProcess {
                stream_name,
                reader_id,
                process_id,
                owner_process_id,
            } => write!(
                f,
                "reader not owned stream_name=[{}] reader_id=[{}] process_id=[{}] owner_process_id=[{:?}]",
                stream_name, reader_id, process_id, owner_process_id
            ),
        }
    }
}

impl std::error::Error for RegistryError {}

#[derive(Debug, Default)]
pub struct Registry {
    processes: BTreeMap<String, ProcessRecord>,
    streams: BTreeMap<String, StreamRecord>,
    next_process_id: u64,
}

impl Registry {
    fn now_epoch_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    pub fn ensure_stream(
        &mut self,
        stream_name: &str,
        ring_capacity: usize,
    ) -> Result<bool, RegistryError> {
        if ring_capacity == 0 {
            return Err(RegistryError::InvalidRingCapacity {
                stream_name: stream_name.to_string(),
                ring_capacity,
            });
        }

        if let Some(existing) = self.streams.get(stream_name) {
            if existing.ring_capacity != ring_capacity {
                return Err(RegistryError::StreamRingCapacityMismatch {
                    stream_name: stream_name.to_string(),
                    existing_ring_capacity: existing.ring_capacity,
                    requested_ring_capacity: ring_capacity,
                });
            }
            return Ok(false);
        }

        self.register_stream(stream_name, ring_capacity)?;
        Ok(true)
    }

    pub fn register_process(&mut self, app: &str) -> String {
        self.next_process_id += 1;
        let process_id = format!("proc_{}", self.next_process_id);
        let now = Self::now_epoch_millis();
        let record = ProcessRecord {
            process_id: process_id.clone(),
            app: app.to_string(),
            registered_at: now,
            last_heartbeat_at: now,
            lease_status: "alive".to_string(),
        };
        self.processes.insert(process_id.clone(), record);
        process_id
    }

    pub fn record_heartbeat(&mut self, process_id: &str) -> Result<(), RegistryError> {
        let process = self
            .processes
            .get_mut(process_id)
            .ok_or_else(|| RegistryError::ProcessNotFound {
                process_id: process_id.to_string(),
            })?;
        process.last_heartbeat_at = Self::now_epoch_millis();
        process.lease_status = "alive".to_string();
        Ok(())
    }

    pub fn mark_process_expired(&mut self, process_id: &str) -> Result<(), RegistryError> {
        let process = self
            .processes
            .get_mut(process_id)
            .ok_or_else(|| RegistryError::ProcessNotFound {
                process_id: process_id.to_string(),
            })?;
        process.lease_status = "expired".to_string();
        Ok(())
    }

    pub fn register_stream(
        &mut self,
        stream_name: &str,
        ring_capacity: usize,
    ) -> Result<(), RegistryError> {
        if ring_capacity == 0 {
            return Err(RegistryError::InvalidRingCapacity {
                stream_name: stream_name.to_string(),
                ring_capacity,
            });
        }

        if self.streams.contains_key(stream_name) {
            return Err(RegistryError::StreamAlreadyExists {
                stream_name: stream_name.to_string(),
            });
        }

        let record = StreamRecord {
            stream_name: stream_name.to_string(),
            ring_capacity,
            writer_process_id: None,
            reader_count: 0,
            status: "registered".to_string(),
            reader_process_ids: BTreeMap::new(),
        };
        self.streams.insert(stream_name.to_string(), record);
        Ok(())
    }

    pub fn processes_len(&self) -> usize {
        self.processes.len()
    }

    pub fn streams_len(&self) -> usize {
        self.streams.len()
    }

    pub fn get_process(&self, process_id: &str) -> Option<&ProcessRecord> {
        self.processes.get(process_id)
    }

    pub fn get_stream(&self, stream_name: &str) -> Option<&StreamRecord> {
        self.streams.get(stream_name)
    }

    pub fn set_stream_status(
        &mut self,
        stream_name: &str,
        status: &str,
    ) -> Result<(), RegistryError> {
        let stream = self
            .streams
            .get_mut(stream_name)
            .ok_or_else(|| RegistryError::StreamNotFound {
                stream_name: stream_name.to_string(),
            })?;
        stream.status = status.to_string();
        Ok(())
    }

    pub fn attach_writer(&mut self, stream_name: &str, process_id: &str) -> Result<(), RegistryError> {
        let stream = self
            .streams
            .get_mut(stream_name)
            .ok_or_else(|| RegistryError::StreamNotFound {
                stream_name: stream_name.to_string(),
            })?;
        stream.writer_process_id = Some(process_id.to_string());
        stream.status = "writer_attached".to_string();
        Ok(())
    }

    pub fn validate_writer_owner(
        &self,
        stream_name: &str,
        process_id: &str,
    ) -> Result<(), RegistryError> {
        let stream = self
            .streams
            .get(stream_name)
            .ok_or_else(|| RegistryError::StreamNotFound {
                stream_name: stream_name.to_string(),
            })?;

        if stream.writer_process_id.as_deref() != Some(process_id) {
            return Err(RegistryError::WriterNotOwnedByProcess {
                stream_name: stream_name.to_string(),
                process_id: process_id.to_string(),
                owner_process_id: stream.writer_process_id.clone(),
            });
        }

        Ok(())
    }

    pub fn detach_writer(&mut self, stream_name: &str) -> Result<(), RegistryError> {
        let stream = self
            .streams
            .get_mut(stream_name)
            .ok_or_else(|| RegistryError::StreamNotFound {
                stream_name: stream_name.to_string(),
            })?;
        stream.writer_process_id = None;
        stream.status = if stream.reader_count > 0 {
            "active".to_string()
        } else {
            "registered".to_string()
        };
        Ok(())
    }

    pub fn attach_reader(
        &mut self,
        stream_name: &str,
        process_id: &str,
        reader_id: &str,
    ) -> Result<(), RegistryError> {
        let stream = self
            .streams
            .get_mut(stream_name)
            .ok_or_else(|| RegistryError::StreamNotFound {
                stream_name: stream_name.to_string(),
            })?;
        stream.reader_count += 1;
        stream
            .reader_process_ids
            .insert(reader_id.to_string(), process_id.to_string());
        if stream.writer_process_id.is_some() {
            stream.status = "active".to_string();
        } else {
            stream.status = "reader_attached".to_string();
        }
        Ok(())
    }

    pub fn validate_reader_owner(
        &self,
        stream_name: &str,
        reader_id: &str,
        process_id: &str,
    ) -> Result<(), RegistryError> {
        let stream = self
            .streams
            .get(stream_name)
            .ok_or_else(|| RegistryError::StreamNotFound {
                stream_name: stream_name.to_string(),
            })?;
        let owner_process_id = stream.reader_process_ids.get(reader_id).cloned();
        if owner_process_id.as_deref() != Some(process_id) {
            return Err(RegistryError::ReaderNotOwnedByProcess {
                stream_name: stream_name.to_string(),
                reader_id: reader_id.to_string(),
                process_id: process_id.to_string(),
                owner_process_id,
            });
        }

        Ok(())
    }

    pub fn detach_reader(
        &mut self,
        stream_name: &str,
        reader_id: &str,
    ) -> Result<(), RegistryError> {
        let stream = self
            .streams
            .get_mut(stream_name)
            .ok_or_else(|| RegistryError::StreamNotFound {
                stream_name: stream_name.to_string(),
            })?;
        stream.reader_process_ids.remove(reader_id);
        stream.reader_count = stream.reader_count.saturating_sub(1);
        stream.status = if stream.writer_process_id.is_some() {
            "writer_attached".to_string()
        } else if stream.reader_count > 0 {
            "reader_attached".to_string()
        } else {
            "registered".to_string()
        };
        Ok(())
    }

    pub fn list_streams(&self) -> Vec<StreamRecord> {
        self.streams.values().cloned().collect()
    }

    pub fn streams_for_writer_process(&self, process_id: &str) -> Vec<String> {
        self.streams
            .values()
            .filter(|stream| stream.writer_process_id.as_deref() == Some(process_id))
            .map(|stream| stream.stream_name.clone())
            .collect()
    }

    pub fn unregister_stream(&mut self, stream_name: &str) -> Option<StreamRecord> {
        self.streams.remove(stream_name)
    }
}
