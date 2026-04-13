use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessRecord {
    pub process_id: String,
    pub app: String,
    pub registered_at: u64,
    pub last_heartbeat_at: u64,
    pub lease_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecord {
    pub stream_name: String,
    pub buffer_size: usize,
    pub frame_size: usize,
    pub writer_process_id: Option<String>,
    pub reader_count: usize,
    pub status: String,
    reader_process_ids: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRecord {
    pub source_name: String,
    pub source_type: String,
    pub process_id: String,
    pub output_stream: String,
    pub config: serde_json::Value,
    pub status: String,
    pub metrics: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineRecord {
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
pub struct SinkRecord {
    pub sink_name: String,
    pub sink_type: String,
    pub process_id: String,
    pub input_stream: String,
    pub config: serde_json::Value,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    ProcessNotFound {
        process_id: String,
    },
    ProcessLeaseExpired {
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
    SourceNotFound {
        source_name: String,
    },
    EngineNotFound {
        engine_name: String,
    },
    SinkNotFound {
        sink_name: String,
    },
    SourceAlreadyExists {
        source_name: String,
    },
    EngineAlreadyExists {
        engine_name: String,
    },
    SinkAlreadyExists {
        sink_name: String,
    },
    InvalidRecordKind {
        kind: String,
    },
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
            Self::SourceNotFound { source_name } => {
                write!(f, "source not found source_name=[{}]", source_name)
            }
            Self::EngineNotFound { engine_name } => {
                write!(f, "engine not found engine_name=[{}]", engine_name)
            }
            Self::SinkNotFound { sink_name } => {
                write!(f, "sink not found sink_name=[{}]", sink_name)
            }
            Self::SourceAlreadyExists { source_name } => {
                write!(f, "source already exists source_name=[{}]", source_name)
            }
            Self::EngineAlreadyExists { engine_name } => {
                write!(f, "engine already exists engine_name=[{}]", engine_name)
            }
            Self::SinkAlreadyExists { sink_name } => {
                write!(f, "sink already exists sink_name=[{}]", sink_name)
            }
            Self::InvalidRecordKind { kind } => {
                write!(f, "invalid record kind kind=[{}]", kind)
            }
        }
    }
}

impl std::error::Error for RegistryError {}

#[derive(Debug, Default)]
pub struct Registry {
    processes: BTreeMap<String, ProcessRecord>,
    streams: BTreeMap<String, StreamRecord>,
    sources: BTreeMap<String, SourceRecord>,
    engines: BTreeMap<String, EngineRecord>,
    sinks: BTreeMap<String, SinkRecord>,
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
        self.ensure_stream_with_sizes(stream_name, ring_capacity, ring_capacity)
    }

    pub fn ensure_stream_with_sizes(
        &mut self,
        stream_name: &str,
        buffer_size: usize,
        frame_size: usize,
    ) -> Result<bool, RegistryError> {
        if buffer_size == 0 {
            return Err(RegistryError::InvalidRingCapacity {
                stream_name: stream_name.to_string(),
                ring_capacity: buffer_size,
            });
        }

        if let Some(existing) = self.streams.get(stream_name) {
            if existing.buffer_size != buffer_size || existing.frame_size != frame_size {
                return Err(RegistryError::StreamRingCapacityMismatch {
                    stream_name: stream_name.to_string(),
                    existing_ring_capacity: existing.buffer_size,
                    requested_ring_capacity: buffer_size,
                });
            }
            return Ok(false);
        }

        self.register_stream_with_sizes(stream_name, buffer_size, frame_size)?;
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
        let process =
            self.processes
                .get_mut(process_id)
                .ok_or_else(|| RegistryError::ProcessNotFound {
                    process_id: process_id.to_string(),
                })?;
        if process.lease_status != "alive" {
            return Err(RegistryError::ProcessLeaseExpired {
                process_id: process_id.to_string(),
            });
        }
        process.last_heartbeat_at = Self::now_epoch_millis();
        process.lease_status = "alive".to_string();
        Ok(())
    }

    pub fn claim_expired_process(
        &mut self,
        process_id: &str,
        lease_timeout_millis: u64,
    ) -> Result<bool, RegistryError> {
        let process =
            self.processes
                .get_mut(process_id)
                .ok_or_else(|| RegistryError::ProcessNotFound {
                    process_id: process_id.to_string(),
                })?;
        if process.lease_status != "alive" {
            return Ok(false);
        }
        let now = Self::now_epoch_millis();
        if now.saturating_sub(process.last_heartbeat_at) < lease_timeout_millis {
            return Ok(false);
        }
        process.lease_status = "expired".to_string();
        Ok(true)
    }

    pub fn force_expire_process(&mut self, process_id: &str) -> Result<(), RegistryError> {
        let process =
            self.processes
                .get_mut(process_id)
                .ok_or_else(|| RegistryError::ProcessNotFound {
                    process_id: process_id.to_string(),
                })?;
        process.lease_status = "expired".to_string();
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

    pub fn register_stream(
        &mut self,
        stream_name: &str,
        ring_capacity: usize,
    ) -> Result<(), RegistryError> {
        self.register_stream_with_sizes(stream_name, ring_capacity, ring_capacity)
    }

    pub fn register_stream_with_sizes(
        &mut self,
        stream_name: &str,
        buffer_size: usize,
        frame_size: usize,
    ) -> Result<(), RegistryError> {
        if buffer_size == 0 {
            return Err(RegistryError::InvalidRingCapacity {
                stream_name: stream_name.to_string(),
                ring_capacity: buffer_size,
            });
        }

        if self.streams.contains_key(stream_name) {
            return Err(RegistryError::StreamAlreadyExists {
                stream_name: stream_name.to_string(),
            });
        }

        let record = StreamRecord {
            stream_name: stream_name.to_string(),
            buffer_size,
            frame_size,
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

    pub fn sources_len(&self) -> usize {
        self.sources.len()
    }

    pub fn engines_len(&self) -> usize {
        self.engines.len()
    }

    pub fn sinks_len(&self) -> usize {
        self.sinks.len()
    }

    pub fn get_process(&self, process_id: &str) -> Option<&ProcessRecord> {
        self.processes.get(process_id)
    }

    pub fn get_stream(&self, stream_name: &str) -> Option<&StreamRecord> {
        self.streams.get(stream_name)
    }

    pub fn get_source(&self, source_name: &str) -> Option<&SourceRecord> {
        self.sources.get(source_name)
    }

    pub fn get_engine(&self, engine_name: &str) -> Option<&EngineRecord> {
        self.engines.get(engine_name)
    }

    pub fn get_sink(&self, sink_name: &str) -> Option<&SinkRecord> {
        self.sinks.get(sink_name)
    }

    pub fn register_source(
        &mut self,
        source_name: &str,
        source_type: &str,
        process_id: &str,
        output_stream: &str,
        config: serde_json::Value,
    ) -> Result<(), RegistryError> {
        if self.sources.contains_key(source_name) {
            return Err(RegistryError::SourceAlreadyExists {
                source_name: source_name.to_string(),
            });
        }
        self.sources.insert(
            source_name.to_string(),
            SourceRecord {
                source_name: source_name.to_string(),
                source_type: source_type.to_string(),
                process_id: process_id.to_string(),
                output_stream: output_stream.to_string(),
                config,
                status: "registered".to_string(),
                metrics: serde_json::json!({}),
            },
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register_engine(
        &mut self,
        engine_name: &str,
        engine_type: &str,
        process_id: &str,
        input_stream: &str,
        output_stream: &str,
        sink_names: Vec<String>,
        config: serde_json::Value,
    ) -> Result<(), RegistryError> {
        if self.engines.contains_key(engine_name) {
            return Err(RegistryError::EngineAlreadyExists {
                engine_name: engine_name.to_string(),
            });
        }
        self.engines.insert(
            engine_name.to_string(),
            EngineRecord {
                engine_name: engine_name.to_string(),
                engine_type: engine_type.to_string(),
                process_id: process_id.to_string(),
                input_stream: input_stream.to_string(),
                output_stream: output_stream.to_string(),
                sink_names,
                config,
                status: "registered".to_string(),
                metrics: serde_json::json!({}),
            },
        );
        Ok(())
    }

    pub fn register_sink(
        &mut self,
        sink_name: &str,
        sink_type: &str,
        process_id: &str,
        input_stream: &str,
        config: serde_json::Value,
    ) -> Result<(), RegistryError> {
        if self.sinks.contains_key(sink_name) {
            return Err(RegistryError::SinkAlreadyExists {
                sink_name: sink_name.to_string(),
            });
        }
        self.sinks.insert(
            sink_name.to_string(),
            SinkRecord {
                sink_name: sink_name.to_string(),
                sink_type: sink_type.to_string(),
                process_id: process_id.to_string(),
                input_stream: input_stream.to_string(),
                config,
                status: "registered".to_string(),
            },
        );
        Ok(())
    }

    pub fn set_stream_status(
        &mut self,
        stream_name: &str,
        status: &str,
    ) -> Result<(), RegistryError> {
        let stream =
            self.streams
                .get_mut(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        stream.status = status.to_string();
        Ok(())
    }

    pub fn set_source_status(
        &mut self,
        source_name: &str,
        status: &str,
        metrics: Option<serde_json::Value>,
    ) -> Result<(), RegistryError> {
        let source =
            self.sources
                .get_mut(source_name)
                .ok_or_else(|| RegistryError::SourceNotFound {
                    source_name: source_name.to_string(),
                })?;
        source.status = status.to_string();
        if let Some(metrics) = metrics {
            source.metrics = metrics;
        }
        Ok(())
    }

    pub fn set_engine_status(
        &mut self,
        engine_name: &str,
        status: &str,
        metrics: Option<serde_json::Value>,
    ) -> Result<(), RegistryError> {
        let engine =
            self.engines
                .get_mut(engine_name)
                .ok_or_else(|| RegistryError::EngineNotFound {
                    engine_name: engine_name.to_string(),
                })?;
        engine.status = status.to_string();
        if let Some(metrics) = metrics {
            engine.metrics = metrics;
        }
        Ok(())
    }

    pub fn set_sink_status(&mut self, sink_name: &str, status: &str) -> Result<(), RegistryError> {
        let sink = self
            .sinks
            .get_mut(sink_name)
            .ok_or_else(|| RegistryError::SinkNotFound {
                sink_name: sink_name.to_string(),
            })?;
        sink.status = status.to_string();
        Ok(())
    }

    pub fn attach_writer(
        &mut self,
        stream_name: &str,
        process_id: &str,
    ) -> Result<(), RegistryError> {
        let stream =
            self.streams
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
        let stream =
            self.streams
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
        let stream =
            self.streams
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
        let stream =
            self.streams
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
        let stream =
            self.streams
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
        let stream =
            self.streams
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

    pub fn list_sources(&self) -> Vec<SourceRecord> {
        self.sources.values().cloned().collect()
    }

    pub fn list_engines(&self) -> Vec<EngineRecord> {
        self.engines.values().cloned().collect()
    }

    pub fn list_sinks(&self) -> Vec<SinkRecord> {
        self.sinks.values().cloned().collect()
    }

    pub fn streams_for_writer_process(&self, process_id: &str) -> Vec<String> {
        self.streams
            .values()
            .filter(|stream| stream.writer_process_id.as_deref() == Some(process_id))
            .map(|stream| stream.stream_name.clone())
            .collect()
    }

    pub fn readers_for_process(&self, process_id: &str) -> Vec<(String, String)> {
        self.streams
            .values()
            .flat_map(|stream| {
                stream
                    .reader_process_ids
                    .iter()
                    .filter(move |(_, owner_process_id)| owner_process_id.as_str() == process_id)
                    .map(move |(reader_id, _)| (stream.stream_name.clone(), reader_id.clone()))
            })
            .collect()
    }

    pub fn mark_records_lost_for_process(&mut self, process_id: &str) {
        for source in self.sources.values_mut() {
            if source.process_id == process_id {
                source.status = "lost".to_string();
            }
        }
        for engine in self.engines.values_mut() {
            if engine.process_id == process_id {
                engine.status = "lost".to_string();
            }
        }
        for sink in self.sinks.values_mut() {
            if sink.process_id == process_id {
                sink.status = "lost".to_string();
            }
        }
    }

    pub fn unregister_source(&mut self, source_name: &str) -> Option<SourceRecord> {
        self.sources.remove(source_name)
    }

    pub fn unregister_engine(&mut self, engine_name: &str) -> Option<EngineRecord> {
        self.engines.remove(engine_name)
    }

    pub fn unregister_sink(&mut self, sink_name: &str) -> Option<SinkRecord> {
        self.sinks.remove(sink_name)
    }

    pub fn unregister_stream(&mut self, stream_name: &str) -> Option<StreamRecord> {
        self.streams.remove(stream_name)
    }
}
