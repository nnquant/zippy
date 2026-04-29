use std::collections::{BTreeMap, BTreeSet};
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
    pub writer_process_id: Option<String>,
    pub reader_count: usize,
    pub status: String,
    #[serde(default)]
    pub active_segment_descriptor: Option<serde_json::Value>,
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
    InvalidStreamConfig {
        stream_name: String,
        buffer_size: usize,
        frame_size: usize,
    },
    StreamConfigMismatch {
        stream_name: String,
        existing_buffer_size: usize,
        existing_frame_size: usize,
        requested_buffer_size: usize,
        requested_frame_size: usize,
    },
    StreamSchemaMismatch {
        stream_name: String,
        existing_schema_hash: String,
        requested_schema_hash: String,
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
    SegmentDescriptorPublisherNotAuthorized {
        stream_name: String,
        process_id: String,
    },
    InvalidSegmentDescriptor {
        stream_name: String,
        reason: String,
    },
    PersistedFilePublisherNotAuthorized {
        stream_name: String,
        process_id: String,
    },
    PersistEventPublisherNotAuthorized {
        stream_name: String,
        process_id: String,
    },
    InvalidPersistedFile {
        stream_name: String,
        reason: String,
    },
    InvalidPersistEvent {
        stream_name: String,
        reason: String,
    },
    SegmentReaderLeaseNotFound {
        stream_name: String,
        lease_id: String,
    },
    SegmentReaderLeaseNotOwnedByProcess {
        stream_name: String,
        lease_id: String,
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
            Self::InvalidStreamConfig {
                stream_name,
                buffer_size,
                frame_size,
            } => write!(
                f,
                "invalid stream sizing stream_name=[{}] buffer_size=[{}] frame_size=[{}]",
                stream_name, buffer_size, frame_size
            ),
            Self::StreamConfigMismatch {
                stream_name,
                existing_buffer_size,
                existing_frame_size,
                requested_buffer_size,
                requested_frame_size,
            } => write!(
                f,
                "stream configuration mismatch stream_name=[{}] existing_buffer_size=[{}] existing_frame_size=[{}] requested_buffer_size=[{}] requested_frame_size=[{}]",
                stream_name,
                existing_buffer_size,
                existing_frame_size,
                requested_buffer_size,
                requested_frame_size
            ),
            Self::StreamSchemaMismatch {
                stream_name,
                existing_schema_hash,
                requested_schema_hash,
            } => write!(
                f,
                "stream schema mismatch stream_name=[{}] existing_schema_hash=[{}] requested_schema_hash=[{}]",
                stream_name, existing_schema_hash, requested_schema_hash
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
            Self::SegmentDescriptorPublisherNotAuthorized {
                stream_name,
                process_id,
            } => write!(
                f,
                "segment descriptor publisher not authorized stream_name=[{}] process_id=[{}]",
                stream_name, process_id
            ),
            Self::InvalidSegmentDescriptor {
                stream_name,
                reason,
            } => write!(
                f,
                "invalid segment descriptor stream_name=[{}] reason=[{}]",
                stream_name, reason
            ),
            Self::PersistedFilePublisherNotAuthorized {
                stream_name,
                process_id,
            } => write!(
                f,
                "persisted file publisher not authorized stream_name=[{}] process_id=[{}]",
                stream_name, process_id
            ),
            Self::PersistEventPublisherNotAuthorized {
                stream_name,
                process_id,
            } => write!(
                f,
                "persist event publisher not authorized stream_name=[{}] process_id=[{}]",
                stream_name, process_id
            ),
            Self::InvalidPersistedFile {
                stream_name,
                reason,
            } => write!(
                f,
                "invalid persisted file stream_name=[{}] reason=[{}]",
                stream_name, reason
            ),
            Self::InvalidPersistEvent {
                stream_name,
                reason,
            } => write!(
                f,
                "invalid persist event stream_name=[{}] reason=[{}]",
                stream_name, reason
            ),
            Self::SegmentReaderLeaseNotFound {
                stream_name,
                lease_id,
            } => write!(
                f,
                "segment reader lease not found stream_name=[{}] lease_id=[{}]",
                stream_name, lease_id
            ),
            Self::SegmentReaderLeaseNotOwnedByProcess {
                stream_name,
                lease_id,
                process_id,
                owner_process_id,
            } => write!(
                f,
                "segment reader lease not owned stream_name=[{}] lease_id=[{}] process_id=[{}] owner_process_id=[{:?}]",
                stream_name, lease_id, process_id, owner_process_id
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

#[derive(Debug, Default, Clone)]
pub struct Registry {
    processes: BTreeMap<String, ProcessRecord>,
    streams: BTreeMap<String, StreamRecord>,
    sources: BTreeMap<String, SourceRecord>,
    engines: BTreeMap<String, EngineRecord>,
    sinks: BTreeMap<String, SinkRecord>,
    next_process_id: u64,
    next_segment_reader_lease_id: u64,
}

#[derive(Debug, Default)]
pub struct DroppedTableRecords {
    pub stream: Option<StreamRecord>,
    pub sources: Vec<SourceRecord>,
    pub engines: Vec<EngineRecord>,
    pub sinks: Vec<SinkRecord>,
}

fn split_active_segment_descriptor(
    stream_name: &str,
    mut descriptor: serde_json::Value,
) -> Result<(serde_json::Value, Vec<serde_json::Value>), RegistryError> {
    let sealed_segments = match descriptor
        .as_object_mut()
        .and_then(|object| object.remove("sealed_segments"))
    {
        Some(serde_json::Value::Array(sealed_segments)) => sealed_segments,
        Some(_) => {
            return Err(RegistryError::InvalidSegmentDescriptor {
                stream_name: stream_name.to_string(),
                reason: "sealed_segments must be an array".to_string(),
            });
        }
        None => Vec::new(),
    };
    Ok((descriptor, sealed_segments))
}

fn persisted_created_at_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or_default()
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
        schema: serde_json::Value,
        schema_hash: &str,
        buffer_size: usize,
        frame_size: usize,
    ) -> Result<bool, RegistryError> {
        if buffer_size == 0 || frame_size == 0 {
            return Err(RegistryError::InvalidStreamConfig {
                stream_name: stream_name.to_string(),
                buffer_size,
                frame_size,
            });
        }

        if let Some(existing) = self.streams.get(stream_name) {
            if existing.buffer_size != buffer_size || existing.frame_size != frame_size {
                return Err(RegistryError::StreamConfigMismatch {
                    stream_name: stream_name.to_string(),
                    existing_buffer_size: existing.buffer_size,
                    existing_frame_size: existing.frame_size,
                    requested_buffer_size: buffer_size,
                    requested_frame_size: frame_size,
                });
            }
            if existing.schema_hash != schema_hash {
                return Err(RegistryError::StreamSchemaMismatch {
                    stream_name: stream_name.to_string(),
                    existing_schema_hash: existing.schema_hash.clone(),
                    requested_schema_hash: schema_hash.to_string(),
                });
            }
            return Ok(false);
        }

        self.register_stream(stream_name, schema, schema_hash, buffer_size, frame_size)?;
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

    pub fn validate_process_alive(&self, process_id: &str) -> Result<(), RegistryError> {
        let process =
            self.processes
                .get(process_id)
                .ok_or_else(|| RegistryError::ProcessNotFound {
                    process_id: process_id.to_string(),
                })?;
        if process.lease_status != "alive" {
            return Err(RegistryError::ProcessLeaseExpired {
                process_id: process_id.to_string(),
            });
        }
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
        schema: serde_json::Value,
        schema_hash: &str,
        buffer_size: usize,
        frame_size: usize,
    ) -> Result<(), RegistryError> {
        if buffer_size == 0 || frame_size == 0 {
            return Err(RegistryError::InvalidStreamConfig {
                stream_name: stream_name.to_string(),
                buffer_size,
                frame_size,
            });
        }

        if self.streams.contains_key(stream_name) {
            return Err(RegistryError::StreamAlreadyExists {
                stream_name: stream_name.to_string(),
            });
        }

        let record = StreamRecord {
            stream_name: stream_name.to_string(),
            schema,
            schema_hash: schema_hash.to_string(),
            data_path: "segment".to_string(),
            descriptor_generation: 0,
            sealed_segments: Vec::new(),
            persisted_files: Vec::new(),
            persist_events: Vec::new(),
            segment_reader_leases: Vec::new(),
            buffer_size,
            frame_size,
            writer_process_id: None,
            reader_count: 0,
            status: "registered".to_string(),
            active_segment_descriptor: None,
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

    pub fn segment_descriptor(
        &self,
        stream_name: &str,
    ) -> Result<Option<serde_json::Value>, RegistryError> {
        let stream =
            self.streams
                .get(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        Ok(stream.active_segment_descriptor.clone())
    }

    pub fn segment_descriptor_for_process(
        &self,
        stream_name: &str,
        process_id: &str,
    ) -> Result<Option<serde_json::Value>, RegistryError> {
        self.validate_process_alive(process_id)?;
        self.segment_descriptor(stream_name)
    }

    pub fn segment_descriptor_update_for_process(
        &self,
        stream_name: &str,
        process_id: &str,
        after_descriptor_generation: u64,
    ) -> Result<Option<(u64, serde_json::Value)>, RegistryError> {
        self.validate_process_alive(process_id)?;
        let stream =
            self.streams
                .get(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        if stream.descriptor_generation <= after_descriptor_generation {
            return Ok(None);
        }
        let Some(descriptor) = stream.active_segment_descriptor.clone() else {
            return Ok(None);
        };
        Ok(Some((stream.descriptor_generation, descriptor)))
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
        if let Some(existing) = self.sources.get(source_name) {
            if existing.source_type != source_type
                || existing.output_stream != output_stream
                || existing.config != config
            {
                return Err(RegistryError::SourceAlreadyExists {
                    source_name: source_name.to_string(),
                });
            }
            if existing.process_id != process_id && self.process_is_alive(&existing.process_id) {
                return Err(RegistryError::SourceAlreadyExists {
                    source_name: source_name.to_string(),
                });
            }
        }
        if let Some(existing) = self.sources.get_mut(source_name) {
            if existing.process_id != process_id {
                existing.process_id = process_id.to_string();
                existing.metrics = serde_json::json!({});
            }
            existing.status = "registered".to_string();
            return Ok(());
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

    fn process_is_alive(&self, process_id: &str) -> bool {
        self.processes
            .get(process_id)
            .map(|process| process.lease_status == "alive")
            .unwrap_or(false)
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

    pub fn set_stream_segment_metadata(
        &mut self,
        stream_name: &str,
        descriptor_generation: u64,
        sealed_segments: Vec<serde_json::Value>,
        persisted_files: Vec<serde_json::Value>,
        persist_events: Vec<serde_json::Value>,
        segment_reader_leases: Vec<serde_json::Value>,
    ) -> Result<(), RegistryError> {
        let stream =
            self.streams
                .get_mut(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        stream.descriptor_generation = descriptor_generation;
        stream.sealed_segments = sealed_segments;
        stream.persisted_files = persisted_files;
        stream.persist_events = persist_events;
        stream.segment_reader_leases = segment_reader_leases;
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

    pub fn publish_segment_descriptor(
        &mut self,
        stream_name: &str,
        process_id: &str,
        descriptor: serde_json::Value,
    ) -> Result<(), RegistryError> {
        self.validate_process_alive(process_id)?;
        let stream =
            self.streams
                .get(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        let writer_owner = stream.writer_process_id.as_deref() == Some(process_id);
        let source_owner = self.sources.values().any(|source| {
            source.output_stream == stream_name
                && source.process_id == process_id
                && source.status != "lost"
        });
        if !writer_owner && !source_owner {
            return Err(RegistryError::SegmentDescriptorPublisherNotAuthorized {
                stream_name: stream_name.to_string(),
                process_id: process_id.to_string(),
            });
        }

        let (active_descriptor, sealed_segments) =
            split_active_segment_descriptor(stream_name, descriptor)?;
        let stream =
            self.streams
                .get_mut(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        stream.active_segment_descriptor = Some(active_descriptor);
        stream.sealed_segments = sealed_segments;
        stream.descriptor_generation = stream.descriptor_generation.saturating_add(1);
        Ok(())
    }

    pub fn publish_persisted_file(
        &mut self,
        stream_name: &str,
        process_id: &str,
        persisted_file: serde_json::Value,
    ) -> Result<(), RegistryError> {
        self.validate_process_alive(process_id)?;
        let stream =
            self.streams
                .get(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        let writer_owner = stream.writer_process_id.as_deref() == Some(process_id);
        let source_owner = self.sources.values().any(|source| {
            source.output_stream == stream_name
                && source.process_id == process_id
                && source.status != "lost"
        });
        let sink_owner = self.sinks.values().any(|sink| {
            sink.input_stream == stream_name
                && sink.process_id == process_id
                && sink.status != "lost"
        });
        if !writer_owner && !source_owner && !sink_owner {
            return Err(RegistryError::PersistedFilePublisherNotAuthorized {
                stream_name: stream_name.to_string(),
                process_id: process_id.to_string(),
            });
        }

        let schema_hash = stream.schema_hash.clone();
        let mut persisted_file = persisted_file;
        let Some(object) = persisted_file.as_object_mut() else {
            return Err(RegistryError::InvalidPersistedFile {
                stream_name: stream_name.to_string(),
                reason: "persisted_file must be an object".to_string(),
            });
        };
        let Some(file_path) = object.get("file_path").and_then(serde_json::Value::as_str) else {
            return Err(RegistryError::InvalidPersistedFile {
                stream_name: stream_name.to_string(),
                reason: "persisted_file.file_path must be a string".to_string(),
            });
        };
        if file_path.is_empty() {
            return Err(RegistryError::InvalidPersistedFile {
                stream_name: stream_name.to_string(),
                reason: "persisted_file.file_path must not be empty".to_string(),
            });
        }
        object
            .entry("stream_name")
            .or_insert_with(|| serde_json::Value::String(stream_name.to_string()));
        object
            .entry("schema_hash")
            .or_insert_with(|| serde_json::Value::String(schema_hash));
        object
            .entry("created_at")
            .or_insert_with(|| serde_json::Value::from(persisted_created_at_millis()));
        let persist_file_id = object
            .get("persist_file_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);

        let stream =
            self.streams
                .get_mut(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        if let Some(persist_file_id) = persist_file_id {
            if let Some(existing) = stream.persisted_files.iter_mut().find(|persisted_file| {
                persisted_file
                    .get("persist_file_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(persist_file_id.as_str())
            }) {
                *existing = persisted_file;
                return Ok(());
            }
        }
        stream.persisted_files.push(persisted_file);
        Ok(())
    }

    pub fn publish_persist_event(
        &mut self,
        stream_name: &str,
        process_id: &str,
        persist_event: serde_json::Value,
    ) -> Result<(), RegistryError> {
        self.validate_process_alive(process_id)?;
        let stream =
            self.streams
                .get(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        let writer_owner = stream.writer_process_id.as_deref() == Some(process_id);
        let source_owner = self.sources.values().any(|source| {
            source.output_stream == stream_name
                && source.process_id == process_id
                && source.status != "lost"
        });
        let sink_owner = self.sinks.values().any(|sink| {
            sink.input_stream == stream_name
                && sink.process_id == process_id
                && sink.status != "lost"
        });
        if !writer_owner && !source_owner && !sink_owner {
            return Err(RegistryError::PersistEventPublisherNotAuthorized {
                stream_name: stream_name.to_string(),
                process_id: process_id.to_string(),
            });
        }

        let mut persist_event = persist_event;
        let Some(object) = persist_event.as_object_mut() else {
            return Err(RegistryError::InvalidPersistEvent {
                stream_name: stream_name.to_string(),
                reason: "persist_event must be an object".to_string(),
            });
        };
        let Some(event_type) = object
            .get("persist_event_type")
            .and_then(serde_json::Value::as_str)
        else {
            return Err(RegistryError::InvalidPersistEvent {
                stream_name: stream_name.to_string(),
                reason: "persist_event.persist_event_type must be a string".to_string(),
            });
        };
        if event_type.is_empty() {
            return Err(RegistryError::InvalidPersistEvent {
                stream_name: stream_name.to_string(),
                reason: "persist_event.persist_event_type must not be empty".to_string(),
            });
        }
        object
            .entry("stream_name")
            .or_insert_with(|| serde_json::Value::String(stream_name.to_string()));
        object
            .entry("created_at")
            .or_insert_with(|| serde_json::Value::from(persisted_created_at_millis()));
        let persist_event_id = object
            .get("persist_event_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);

        let stream =
            self.streams
                .get_mut(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        if let Some(persist_event_id) = persist_event_id {
            if let Some(existing) = stream.persist_events.iter_mut().find(|event| {
                event
                    .get("persist_event_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(persist_event_id.as_str())
            }) {
                *existing = persist_event;
                return Ok(());
            }
        }
        stream.persist_events.push(persist_event);
        Ok(())
    }

    pub fn set_stream_persisted_files(
        &mut self,
        stream_name: &str,
        persisted_files: Vec<serde_json::Value>,
    ) -> Result<(), RegistryError> {
        let stream =
            self.streams
                .get_mut(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        stream.persisted_files = persisted_files;
        Ok(())
    }

    pub fn set_stream_persist_events(
        &mut self,
        stream_name: &str,
        persist_events: Vec<serde_json::Value>,
    ) -> Result<(), RegistryError> {
        let stream =
            self.streams
                .get_mut(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        stream.persist_events = persist_events;
        Ok(())
    }

    pub fn acquire_segment_reader_lease(
        &mut self,
        stream_name: &str,
        process_id: &str,
        source_segment_id: u64,
        source_generation: u64,
    ) -> Result<String, RegistryError> {
        self.validate_process_alive(process_id)?;
        self.next_segment_reader_lease_id = self.next_segment_reader_lease_id.saturating_add(1);
        let lease_id = format!("segment-lease-{}", self.next_segment_reader_lease_id);
        let stream =
            self.streams
                .get_mut(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        stream.segment_reader_leases.push(serde_json::json!({
            "lease_id": lease_id,
            "stream_name": stream_name,
            "process_id": process_id,
            "source_segment_id": source_segment_id,
            "source_generation": source_generation,
            "created_at": persisted_created_at_millis(),
        }));
        Ok(lease_id)
    }

    pub fn release_segment_reader_lease(
        &mut self,
        stream_name: &str,
        process_id: &str,
        lease_id: &str,
    ) -> Result<(), RegistryError> {
        self.validate_process_alive(process_id)?;
        let stream =
            self.streams
                .get_mut(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        let index = stream
            .segment_reader_leases
            .iter()
            .position(|lease| {
                lease.get("lease_id").and_then(serde_json::Value::as_str) == Some(lease_id)
            })
            .ok_or_else(|| RegistryError::SegmentReaderLeaseNotFound {
                stream_name: stream_name.to_string(),
                lease_id: lease_id.to_string(),
            })?;
        let owner_process_id = stream.segment_reader_leases[index]
            .get("process_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if owner_process_id.as_deref() != Some(process_id) {
            return Err(RegistryError::SegmentReaderLeaseNotOwnedByProcess {
                stream_name: stream_name.to_string(),
                lease_id: lease_id.to_string(),
                process_id: process_id.to_string(),
                owner_process_id,
            });
        }
        stream.segment_reader_leases.remove(index);
        Ok(())
    }

    pub fn remove_segment_reader_leases_for_process(&mut self, process_id: &str) -> usize {
        let mut removed = 0;
        for stream in self.streams.values_mut() {
            let before = stream.segment_reader_leases.len();
            stream.segment_reader_leases.retain(|lease| {
                lease.get("process_id").and_then(serde_json::Value::as_str) != Some(process_id)
            });
            removed += before.saturating_sub(stream.segment_reader_leases.len());
        }
        removed
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

    pub fn drop_table(&mut self, table_name: &str) -> DroppedTableRecords {
        let stream = self.streams.remove(table_name);

        let source_names = self
            .sources
            .values()
            .filter(|source| source.output_stream == table_name)
            .map(|source| source.source_name.clone())
            .collect::<Vec<_>>();
        let sources = source_names
            .into_iter()
            .filter_map(|source_name| self.sources.remove(&source_name))
            .collect::<Vec<_>>();

        let engine_names = self
            .engines
            .values()
            .filter(|engine| {
                engine.input_stream == table_name || engine.output_stream == table_name
            })
            .map(|engine| engine.engine_name.clone())
            .collect::<Vec<_>>();
        let mut engine_sink_names = BTreeSet::new();
        let mut engines = Vec::new();
        for engine_name in engine_names {
            if let Some(engine) = self.engines.remove(&engine_name) {
                engine_sink_names.extend(engine.sink_names.iter().cloned());
                engines.push(engine);
            }
        }

        let sink_names = self
            .sinks
            .values()
            .filter(|sink| {
                sink.input_stream == table_name || engine_sink_names.contains(&sink.sink_name)
            })
            .map(|sink| sink.sink_name.clone())
            .collect::<Vec<_>>();
        let sinks = sink_names
            .into_iter()
            .filter_map(|sink_name| self.sinks.remove(&sink_name))
            .collect::<Vec<_>>();

        DroppedTableRecords {
            stream,
            sources,
            engines,
            sinks,
        }
    }
}
