use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

type PersistedFileUpdate = Option<(u64, Vec<serde_json::Value>, Vec<serde_json::Value>)>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessRecord {
    pub process_id: String,
    #[serde(default)]
    pub process_token: String,
    pub app: String,
    pub registered_at: u64,
    pub last_heartbeat_at: u64,
    pub lease_status: String,
    #[serde(default)]
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecord {
    pub stream_name: String,
    #[serde(default)]
    pub owner_process_id: Option<String>,
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
    pub segment_reader_leases: Vec<serde_json::Value>,
    pub buffer_size: usize,
    pub frame_size: usize,
    pub writer_process_id: Option<String>,
    #[serde(default)]
    pub active_writer_source_name: Option<String>,
    #[serde(default)]
    pub writer_epoch: u64,
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
    #[serde(default)]
    pub writer_epoch: u64,
    #[serde(default)]
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineRecord {
    pub engine_name: String,
    pub engine_type: String,
    pub process_id: String,
    pub input_stream: String,
    pub output_stream: String,
    pub config: serde_json::Value,
    pub status: String,
    pub metrics: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    ProcessNotFound {
        process_id: String,
    },
    ProcessLeaseExpired {
        process_id: String,
    },
    ProcessTokenInvalid {
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
    SegmentReaderLeaseTargetNotFound {
        stream_name: String,
        source_segment_id: u64,
        source_generation: u64,
    },
    SegmentReaderLeaseNotOwnedByProcess {
        stream_name: String,
        lease_id: String,
        process_id: String,
        owner_process_id: Option<String>,
    },
    SourceNotOwnedByProcess {
        source_name: String,
        process_id: String,
        owner_process_id: Option<String>,
    },
    SourceNotFound {
        source_name: String,
    },
    EngineNotFound {
        engine_name: String,
    },
    SourceAlreadyExists {
        source_name: String,
    },
    StreamWriterSourceAlreadyExists {
        stream_name: String,
        source_name: String,
        owner_source_name: String,
    },
    EngineAlreadyExists {
        engine_name: String,
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
            Self::ProcessTokenInvalid { process_id } => {
                write!(f, "process token invalid process_id=[{}]", process_id)
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
            Self::SegmentReaderLeaseTargetNotFound {
                stream_name,
                source_segment_id,
                source_generation,
            } => write!(
                f,
                "segment reader lease target not found stream_name=[{}] source_segment_id=[{}] source_generation=[{}]",
                stream_name, source_segment_id, source_generation
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
            Self::SourceNotOwnedByProcess {
                source_name,
                process_id,
                owner_process_id,
            } => write!(
                f,
                "source not owned source_name=[{}] process_id=[{}] owner_process_id=[{:?}]",
                source_name, process_id, owner_process_id
            ),
            Self::SourceNotFound { source_name } => {
                write!(f, "source not found source_name=[{}]", source_name)
            }
            Self::EngineNotFound { engine_name } => {
                write!(f, "engine not found engine_name=[{}]", engine_name)
            }
            Self::SourceAlreadyExists { source_name } => {
                write!(f, "source already exists source_name=[{}]", source_name)
            }
            Self::StreamWriterSourceAlreadyExists {
                stream_name,
                source_name,
                owner_source_name,
            } => write!(
                f,
                "stream writer source already exists stream_name=[{}] source_name=[{}] owner_source_name=[{}]",
                stream_name, source_name, owner_source_name
            ),
            Self::EngineAlreadyExists { engine_name } => {
                write!(f, "engine already exists engine_name=[{}]", engine_name)
            }
            Self::InvalidRecordKind { kind } => {
                write!(f, "invalid record kind kind=[{}]", kind)
            }
        }
    }
}

fn generate_process_token(process_id: &str, app: &str) -> String {
    if let Some(token) = random_token_from_os() {
        return token;
    }
    let now = Registry::now_epoch_millis();
    let mut hasher = DefaultHasher::new();
    process_id.hash(&mut hasher);
    app.hash(&mut hasher);
    now.hash(&mut hasher);
    format!("{:016x}{:016x}", hasher.finish(), now)
}

fn random_token_from_os() -> Option<String> {
    let mut bytes = [0_u8; 32];
    let mut file = File::open("/dev/urandom").ok()?;
    file.read_exact(&mut bytes).ok()?;
    Some(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

impl std::error::Error for RegistryError {}

#[derive(Debug, Default, Clone)]
pub struct Registry {
    processes: BTreeMap<String, ProcessRecord>,
    streams: BTreeMap<String, StreamRecord>,
    sources: BTreeMap<String, SourceRecord>,
    engines: BTreeMap<String, EngineRecord>,
    next_process_id: u64,
    next_segment_reader_lease_id: u64,
    control_revision: u64,
}

#[derive(Debug, Default)]
pub struct DroppedTableRecords {
    pub stream: Option<StreamRecord>,
    pub sources: Vec<SourceRecord>,
    pub engines: Vec<EngineRecord>,
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

fn validate_metadata_writer_epoch(
    stream_name: &str,
    metadata: &serde_json::Value,
    writer_epoch: u64,
    unauthorized: impl FnOnce() -> RegistryError,
) -> Result<(), RegistryError> {
    let Some(value) = metadata
        .get("control_writer_epoch")
        .or_else(|| metadata.get("writer_epoch"))
    else {
        return Ok(());
    };
    let Some(metadata_writer_epoch) = value.as_u64() else {
        return Err(RegistryError::InvalidSegmentDescriptor {
            stream_name: stream_name.to_string(),
            reason: "writer epoch metadata must be an unsigned integer".to_string(),
        });
    };
    if metadata_writer_epoch != writer_epoch {
        return Err(unauthorized());
    }
    Ok(())
}

fn set_metadata_writer_epoch(metadata: &mut serde_json::Value, writer_epoch: u64) {
    if let Some(object) = metadata.as_object_mut() {
        object
            .entry("writer_epoch")
            .or_insert_with(|| serde_json::Value::from(writer_epoch));
    }
}

fn persisted_created_at_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or_default()
}

fn normalize_persisted_file(
    stream_name: &str,
    schema_hash: &str,
    mut persisted_file: serde_json::Value,
) -> Result<serde_json::Value, RegistryError> {
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
    if let Some(existing_stream_name) = object
        .get("stream_name")
        .and_then(serde_json::Value::as_str)
    {
        if existing_stream_name != stream_name {
            return Err(RegistryError::InvalidPersistedFile {
                stream_name: stream_name.to_string(),
                reason: format!(
                    "persisted_file.stream_name mismatch expected=[{}] actual=[{}]",
                    stream_name, existing_stream_name
                ),
            });
        }
    }
    if let Some(existing_schema_hash) = object
        .get("schema_hash")
        .and_then(serde_json::Value::as_str)
    {
        if existing_schema_hash != schema_hash {
            return Err(RegistryError::InvalidPersistedFile {
                stream_name: stream_name.to_string(),
                reason: format!(
                    "persisted_file.schema_hash mismatch expected=[{}] actual=[{}]",
                    schema_hash, existing_schema_hash
                ),
            });
        }
    }
    object.insert(
        "stream_name".to_string(),
        serde_json::Value::String(stream_name.to_string()),
    );
    object.insert(
        "schema_hash".to_string(),
        serde_json::Value::String(schema_hash.to_string()),
    );
    object
        .entry("created_at")
        .or_insert_with(|| serde_json::Value::from(persisted_created_at_millis()));
    Ok(persisted_file)
}

fn stream_has_segment_identity(
    stream: &StreamRecord,
    source_segment_id: u64,
    source_generation: u64,
) -> bool {
    let expected = (source_segment_id, source_generation);
    stream
        .active_segment_descriptor
        .as_ref()
        .and_then(active_segment_identity)
        == Some(expected)
        || stream
            .sealed_segments
            .iter()
            .filter_map(active_segment_identity)
            .any(|identity| identity == expected)
        || stream
            .persisted_files
            .iter()
            .flat_map(persisted_segment_identities)
            .any(|identity| identity == expected)
        || stream
            .active_segment_descriptor
            .as_ref()
            .into_iter()
            .flat_map(retained_replacement_segment_identities)
            .any(|identity| identity == expected)
}

fn active_segment_identity(value: &serde_json::Value) -> Option<(u64, u64)> {
    Some((
        value
            .get("segment_id")
            .and_then(serde_json::Value::as_u64)?,
        value
            .get("generation")
            .and_then(serde_json::Value::as_u64)?,
    ))
}

fn persisted_segment_identities(value: &serde_json::Value) -> Vec<(u64, u64)> {
    if let Some(source_segments) = value
        .get("source_segments")
        .and_then(serde_json::Value::as_array)
    {
        return source_segments
            .iter()
            .filter_map(persisted_segment_identity)
            .collect();
    }
    persisted_segment_identity(value).into_iter().collect()
}

fn persisted_segment_identity(value: &serde_json::Value) -> Option<(u64, u64)> {
    Some((
        value
            .get("source_segment_id")
            .and_then(serde_json::Value::as_u64)?,
        value
            .get("source_generation")
            .and_then(serde_json::Value::as_u64)?,
    ))
}

fn retained_replacement_segment_identities(value: &serde_json::Value) -> Vec<(u64, u64)> {
    value
        .get("retained_replacement_segments")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(active_segment_identity)
        .collect()
}

impl Registry {
    fn now_epoch_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn next_control_revision(&mut self) -> u64 {
        self.control_revision = self.control_revision.saturating_add(1).max(1);
        self.control_revision
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
        let process_token = generate_process_token(&process_id, app);
        let now = Self::now_epoch_millis();
        let revision = self.next_control_revision();
        let record = ProcessRecord {
            process_id: process_id.clone(),
            process_token,
            app: app.to_string(),
            registered_at: now,
            last_heartbeat_at: now,
            lease_status: "alive".to_string(),
            revision,
        };
        self.processes.insert(process_id.clone(), record);
        process_id
    }

    pub fn get_process_token(&self, process_id: &str) -> Option<&str> {
        self.processes
            .get(process_id)
            .map(|process| process.process_token.as_str())
    }

    pub fn reserve_process_id(&mut self, process_id: &str) {
        let Some(raw_id) = process_id.strip_prefix("proc_") else {
            return;
        };
        let Ok(process_number) = raw_id.parse::<u64>() else {
            return;
        };
        self.next_process_id = self.next_process_id.max(process_number);
    }

    pub fn record_heartbeat(&mut self, process_id: &str) -> Result<(), RegistryError> {
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
        let revision = self.next_control_revision();
        let process = self.processes.get_mut(process_id).unwrap();
        process.last_heartbeat_at = Self::now_epoch_millis();
        process.lease_status = "alive".to_string();
        process.revision = revision;
        Ok(())
    }

    pub fn unregister_process(&mut self, process_id: &str) -> Result<(), RegistryError> {
        self.processes
            .remove(process_id)
            .ok_or_else(|| RegistryError::ProcessNotFound {
                process_id: process_id.to_string(),
            })?;
        self.remove_segment_reader_leases_for_process(process_id);
        for stream in self.streams.values_mut() {
            if stream.writer_process_id.as_deref() == Some(process_id) {
                stream.writer_process_id = None;
                stream.status = if stream.reader_count > 0 {
                    "active".to_string()
                } else {
                    "registered".to_string()
                };
            }
            let removed_readers = stream
                .reader_process_ids
                .iter()
                .filter(|(_, owner)| owner.as_str() == process_id)
                .map(|(reader_id, _)| reader_id.clone())
                .collect::<Vec<_>>();
            for reader_id in removed_readers {
                stream.reader_process_ids.remove(&reader_id);
                stream.reader_count = stream.reader_count.saturating_sub(1);
            }
        }
        Ok(())
    }

    pub fn alive_process_ids(&self) -> Vec<String> {
        self.processes
            .values()
            .filter(|process| process.lease_status == "alive")
            .map(|process| process.process_id.clone())
            .collect()
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

    pub fn validate_process_capability(
        &self,
        process_id: &str,
        process_token: Option<&str>,
    ) -> Result<(), RegistryError> {
        self.validate_process_alive(process_id)?;
        let Some(process_token) = process_token else {
            return Err(RegistryError::ProcessTokenInvalid {
                process_id: process_id.to_string(),
            });
        };
        let stored_token = self
            .processes
            .get(process_id)
            .map(|process| process.process_token.as_str())
            .unwrap_or_default();
        if stored_token.is_empty() || stored_token != process_token {
            return Err(RegistryError::ProcessTokenInvalid {
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
                .get(process_id)
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
        let revision = self.next_control_revision();
        let process = self.processes.get_mut(process_id).unwrap();
        process.lease_status = "expired".to_string();
        process.revision = revision;
        Ok(true)
    }

    pub fn force_expire_process(&mut self, process_id: &str) -> Result<(), RegistryError> {
        let process =
            self.processes
                .get(process_id)
                .ok_or_else(|| RegistryError::ProcessNotFound {
                    process_id: process_id.to_string(),
                })?;
        if process.lease_status == "expired" {
            return Ok(());
        }
        let revision = self.next_control_revision();
        let process = self.processes.get_mut(process_id).unwrap();
        process.lease_status = "expired".to_string();
        process.revision = revision;
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

        let active_writer_source = self.sources.values().find(|source| {
            source.output_stream == stream_name
                && source.status != "lost"
                && self.process_is_alive(&source.process_id)
        });
        let active_writer_source_name =
            active_writer_source.map(|source| source.source_name.clone());
        let writer_epoch = active_writer_source
            .map(|source| source.writer_epoch)
            .unwrap_or_default();
        let record = StreamRecord {
            stream_name: stream_name.to_string(),
            owner_process_id: None,
            schema,
            schema_hash: schema_hash.to_string(),
            data_path: "segment".to_string(),
            descriptor_generation: 0,
            sealed_segments: Vec::new(),
            persisted_files: Vec::new(),
            persist_events: Vec::new(),
            persist_revision: 0,
            segment_reader_leases: Vec::new(),
            buffer_size,
            frame_size,
            writer_process_id: None,
            active_writer_source_name,
            writer_epoch,
            reader_count: 0,
            status: "registered".to_string(),
            active_segment_descriptor: None,
            reader_process_ids: BTreeMap::new(),
        };
        self.streams.insert(stream_name.to_string(), record);
        Ok(())
    }

    pub fn set_stream_owner(
        &mut self,
        stream_name: &str,
        process_id: &str,
    ) -> Result<(), RegistryError> {
        self.validate_process_alive(process_id)?;
        let stream =
            self.streams
                .get_mut(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        if stream.owner_process_id.is_none() {
            stream.owner_process_id = Some(process_id.to_string());
        }
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

    pub fn persisted_file_update_for_process(
        &self,
        stream_name: &str,
        process_id: &str,
        after_revision: u64,
    ) -> Result<PersistedFileUpdate, RegistryError> {
        self.validate_process_alive(process_id)?;
        let stream =
            self.streams
                .get(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        if stream.persist_revision <= after_revision {
            return Ok(None);
        }
        Ok(Some((
            stream.persist_revision,
            stream.persisted_files.clone(),
            stream.persist_events.clone(),
        )))
    }

    pub fn process_update_for_process(
        &self,
        target_process_id: &str,
        watcher_process_id: &str,
        after_revision: u64,
    ) -> Result<Option<(u64, ProcessRecord)>, RegistryError> {
        self.validate_process_alive(watcher_process_id)?;
        let process = self.processes.get(target_process_id).ok_or_else(|| {
            RegistryError::ProcessNotFound {
                process_id: target_process_id.to_string(),
            }
        })?;
        if process.revision <= after_revision {
            return Ok(None);
        }
        Ok(Some((process.revision, process.clone())))
    }

    pub fn source_update_for_process(
        &self,
        source_name: &str,
        watcher_process_id: &str,
        after_revision: u64,
    ) -> Result<Option<(u64, SourceRecord)>, RegistryError> {
        self.validate_process_alive(watcher_process_id)?;
        let source =
            self.sources
                .get(source_name)
                .ok_or_else(|| RegistryError::SourceNotFound {
                    source_name: source_name.to_string(),
                })?;
        if source.revision <= after_revision {
            return Ok(None);
        }
        Ok(Some((source.revision, source.clone())))
    }

    pub fn get_source(&self, source_name: &str) -> Option<&SourceRecord> {
        self.sources.get(source_name)
    }

    pub fn get_engine(&self, engine_name: &str) -> Option<&EngineRecord> {
        self.engines.get(engine_name)
    }

    pub fn register_source(
        &mut self,
        source_name: &str,
        source_type: &str,
        process_id: &str,
        output_stream: &str,
        config: serde_json::Value,
    ) -> Result<(), RegistryError> {
        self.validate_single_active_writer_source(source_name, output_stream)?;
        let existing_writer_epoch = self
            .sources
            .get(source_name)
            .map(|source| source.writer_epoch)
            .unwrap_or_default();
        let current_stream_writer_epoch = self
            .streams
            .get(output_stream)
            .map(|stream| stream.writer_epoch)
            .unwrap_or_default();
        let source_takes_new_writer_epoch =
            self.source_takes_new_writer_epoch(source_name, process_id, output_stream);
        let writer_epoch = if source_takes_new_writer_epoch {
            current_stream_writer_epoch.saturating_add(1).max(1)
        } else {
            existing_writer_epoch.max(current_stream_writer_epoch)
        };
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
        let revision = self.next_control_revision();
        if let Some(existing) = self.sources.get_mut(source_name) {
            if existing.process_id != process_id {
                existing.process_id = process_id.to_string();
                existing.metrics = serde_json::json!({});
            }
            existing.writer_epoch = writer_epoch;
            existing.status = "registered".to_string();
            existing.revision = revision;
            if let Some(stream) = self.streams.get_mut(output_stream) {
                stream.active_writer_source_name = Some(source_name.to_string());
                stream.writer_epoch = writer_epoch;
            }
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
                writer_epoch,
                revision,
            },
        );
        if let Some(stream) = self.streams.get_mut(output_stream) {
            stream.active_writer_source_name = Some(source_name.to_string());
            stream.writer_epoch = writer_epoch;
        }
        Ok(())
    }

    fn source_takes_new_writer_epoch(
        &self,
        source_name: &str,
        process_id: &str,
        output_stream: &str,
    ) -> bool {
        let Some(stream) = self.streams.get(output_stream) else {
            return false;
        };
        match stream.active_writer_source_name.as_deref() {
            Some(active_source_name) if active_source_name == source_name => self
                .sources
                .get(source_name)
                .map(|source| source.process_id != process_id || source.status == "lost")
                .unwrap_or(true),
            _ => true,
        }
    }

    fn validate_single_active_writer_source(
        &self,
        source_name: &str,
        output_stream: &str,
    ) -> Result<(), RegistryError> {
        for source in self.sources.values() {
            if source.source_name == source_name
                || source.output_stream != output_stream
                || source.status == "lost"
                || !self.process_is_alive(&source.process_id)
            {
                continue;
            }
            return Err(RegistryError::StreamWriterSourceAlreadyExists {
                stream_name: output_stream.to_string(),
                source_name: source_name.to_string(),
                owner_source_name: source.source_name.clone(),
            });
        }
        Ok(())
    }

    fn stream_active_source_owned_by(&self, stream_name: &str, process_id: &str) -> bool {
        let Some(stream) = self.streams.get(stream_name) else {
            return false;
        };
        stream
            .active_writer_source_name
            .as_deref()
            .and_then(|source_name| self.sources.get(source_name))
            .map(|source| {
                source.output_stream == stream_name
                    && source.process_id == process_id
                    && source.status != "lost"
                    && source.writer_epoch == stream.writer_epoch
            })
            .unwrap_or(false)
    }

    fn stream_writer_process_authorized(&self, stream: &StreamRecord, process_id: &str) -> bool {
        if stream.writer_process_id.as_deref() != Some(process_id) {
            return false;
        }
        let Some(source_name) = stream.active_writer_source_name.as_deref() else {
            return true;
        };
        self.sources
            .get(source_name)
            .map(|source| {
                source.process_id == process_id
                    && source.status != "lost"
                    && source.writer_epoch == stream.writer_epoch
            })
            .unwrap_or(false)
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
                config,
                status: "registered".to_string(),
                metrics: serde_json::json!({}),
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

    #[allow(clippy::too_many_arguments)]
    pub fn set_stream_segment_metadata(
        &mut self,
        stream_name: &str,
        descriptor_generation: u64,
        sealed_segments: Vec<serde_json::Value>,
        persisted_files: Vec<serde_json::Value>,
        persist_events: Vec<serde_json::Value>,
        persist_revision: u64,
        segment_reader_leases: Vec<serde_json::Value>,
        writer_epoch: u64,
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
        stream.persist_revision = persist_revision;
        stream.segment_reader_leases = segment_reader_leases;
        stream.writer_epoch = writer_epoch;
        Ok(())
    }

    pub fn set_source_writer_epoch(
        &mut self,
        source_name: &str,
        writer_epoch: u64,
    ) -> Result<(), RegistryError> {
        if !self.sources.contains_key(source_name) {
            return Err(RegistryError::SourceNotFound {
                source_name: source_name.to_string(),
            });
        }
        let revision = self.next_control_revision();
        let source = self.sources.get_mut(source_name).unwrap();
        source.writer_epoch = writer_epoch;
        source.revision = revision;
        if let Some(stream) = self.streams.get_mut(&source.output_stream) {
            if stream.active_writer_source_name.as_deref() == Some(source_name) {
                stream.writer_epoch = writer_epoch;
            }
        }
        Ok(())
    }

    pub fn set_source_status(
        &mut self,
        source_name: &str,
        status: &str,
        metrics: Option<serde_json::Value>,
    ) -> Result<(), RegistryError> {
        if !self.sources.contains_key(source_name) {
            return Err(RegistryError::SourceNotFound {
                source_name: source_name.to_string(),
            });
        }
        let revision = self.next_control_revision();
        let source = self.sources.get_mut(source_name).unwrap();
        source.status = status.to_string();
        if let Some(metrics) = metrics {
            source.metrics = metrics;
        }
        source.revision = revision;
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

    pub fn attach_writer(
        &mut self,
        stream_name: &str,
        process_id: &str,
    ) -> Result<(), RegistryError> {
        self.validate_process_alive(process_id)?;
        let stream =
            self.streams
                .get_mut(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        if stream.writer_process_id.as_deref() != Some(process_id) || stream.writer_epoch == 0 {
            stream.writer_epoch = stream.writer_epoch.saturating_add(1).max(1);
        }
        stream.writer_process_id = Some(process_id.to_string());
        stream.status = "writer_attached".to_string();
        Ok(())
    }

    pub fn publish_segment_descriptor(
        &mut self,
        stream_name: &str,
        process_id: &str,
        mut descriptor: serde_json::Value,
    ) -> Result<(), RegistryError> {
        self.validate_process_alive(process_id)?;
        let stream =
            self.streams
                .get(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        let writer_owner = self.stream_writer_process_authorized(stream, process_id);
        let source_owner = self.stream_active_source_owned_by(stream_name, process_id);
        if !writer_owner && !source_owner {
            return Err(RegistryError::SegmentDescriptorPublisherNotAuthorized {
                stream_name: stream_name.to_string(),
                process_id: process_id.to_string(),
            });
        }
        validate_metadata_writer_epoch(stream_name, &descriptor, stream.writer_epoch, || {
            RegistryError::SegmentDescriptorPublisherNotAuthorized {
                stream_name: stream_name.to_string(),
                process_id: process_id.to_string(),
            }
        })?;
        set_metadata_writer_epoch(&mut descriptor, stream.writer_epoch);
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
        stream.writer_process_id = Some(process_id.to_string());
        stream.writer_epoch = stream.writer_epoch.max(1);
        stream.status = "writer_attached".to_string();
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
        let writer_owner = self.stream_writer_process_authorized(stream, process_id);
        let source_owner = self.stream_active_source_owned_by(stream_name, process_id);
        if !writer_owner && !source_owner {
            return Err(RegistryError::PersistedFilePublisherNotAuthorized {
                stream_name: stream_name.to_string(),
                process_id: process_id.to_string(),
            });
        }
        validate_metadata_writer_epoch(stream_name, &persisted_file, stream.writer_epoch, || {
            RegistryError::PersistedFilePublisherNotAuthorized {
                stream_name: stream_name.to_string(),
                process_id: process_id.to_string(),
            }
        })?;

        let schema_hash = stream.schema_hash.clone();
        let mut persisted_file =
            normalize_persisted_file(stream_name, &schema_hash, persisted_file)?;
        set_metadata_writer_epoch(&mut persisted_file, stream.writer_epoch);
        let persist_file_id = persisted_file
            .as_object()
            .expect("normalized persisted file is an object")
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
                stream.persist_revision = stream.persist_revision.saturating_add(1).max(1);
                return Ok(());
            }
        }
        stream.persisted_files.push(persisted_file);
        stream.persist_revision = stream.persist_revision.saturating_add(1).max(1);
        Ok(())
    }

    pub fn replace_persisted_files(
        &mut self,
        stream_name: &str,
        persisted_files: Vec<serde_json::Value>,
    ) -> Result<(), RegistryError> {
        let schema_hash = self
            .streams
            .get(stream_name)
            .ok_or_else(|| RegistryError::StreamNotFound {
                stream_name: stream_name.to_string(),
            })?
            .schema_hash
            .clone();
        let normalized_files = persisted_files
            .into_iter()
            .map(|persisted_file| {
                normalize_persisted_file(stream_name, &schema_hash, persisted_file)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let stream =
            self.streams
                .get_mut(stream_name)
                .ok_or_else(|| RegistryError::StreamNotFound {
                    stream_name: stream_name.to_string(),
                })?;
        stream.persisted_files = normalized_files;
        stream.persist_revision = stream.persist_revision.saturating_add(1).max(1);
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
        let writer_owner = self.stream_writer_process_authorized(stream, process_id);
        let source_owner = self.stream_active_source_owned_by(stream_name, process_id);
        if !writer_owner && !source_owner {
            return Err(RegistryError::PersistEventPublisherNotAuthorized {
                stream_name: stream_name.to_string(),
                process_id: process_id.to_string(),
            });
        }
        validate_metadata_writer_epoch(stream_name, &persist_event, stream.writer_epoch, || {
            RegistryError::PersistEventPublisherNotAuthorized {
                stream_name: stream_name.to_string(),
                process_id: process_id.to_string(),
            }
        })?;

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
        object
            .entry("writer_epoch")
            .or_insert_with(|| serde_json::Value::from(stream.writer_epoch));
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
                stream.persist_revision = stream.persist_revision.saturating_add(1).max(1);
                return Ok(());
            }
        }
        stream.persist_events.push(persist_event);
        stream.persist_revision = stream.persist_revision.saturating_add(1).max(1);
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
        {
            let stream =
                self.streams
                    .get(stream_name)
                    .ok_or_else(|| RegistryError::StreamNotFound {
                        stream_name: stream_name.to_string(),
                    })?;
            if !stream_has_segment_identity(stream, source_segment_id, source_generation) {
                return Err(RegistryError::SegmentReaderLeaseTargetNotFound {
                    stream_name: stream_name.to_string(),
                    source_segment_id,
                    source_generation,
                });
            }
        }
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
        self.validate_process_alive(process_id)?;
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

    pub fn stream_records(&self) -> impl Iterator<Item = &StreamRecord> {
        self.streams.values()
    }

    pub fn list_sources(&self) -> Vec<SourceRecord> {
        self.sources.values().cloned().collect()
    }

    pub fn list_engines(&self) -> Vec<EngineRecord> {
        self.engines.values().cloned().collect()
    }

    pub fn sources_for_stream(&self, stream_name: &str) -> Vec<SourceRecord> {
        self.sources
            .values()
            .filter(|source| source.output_stream == stream_name)
            .cloned()
            .collect()
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
        let mut lost_output_streams = Vec::new();
        let mut lost_source_names = Vec::new();
        for source in self.sources.values_mut() {
            if source.process_id == process_id {
                source.status = "lost".to_string();
                lost_source_names.push(source.source_name.clone());
                lost_output_streams
                    .push((source.output_stream.clone(), source.source_name.clone()));
            }
        }
        for source_name in lost_source_names {
            let revision = self.next_control_revision();
            if let Some(source) = self.sources.get_mut(&source_name) {
                source.revision = revision;
            }
        }
        for (stream_name, source_name) in lost_output_streams {
            if let Some(stream) = self.streams.get_mut(&stream_name) {
                let lost_source_still_owns_stream =
                    stream.active_writer_source_name.as_deref() == Some(source_name.as_str());
                if lost_source_still_owns_stream {
                    stream.active_writer_source_name = None;
                }
                let lost_process_still_owns_writer =
                    stream.writer_process_id.as_deref() == Some(process_id);
                if lost_source_still_owns_stream || lost_process_still_owns_writer {
                    stream.status = "stale".to_string();
                }
            }
        }
        for engine in self.engines.values_mut() {
            if engine.process_id == process_id {
                engine.status = "lost".to_string();
            }
        }
    }

    pub fn unregister_source(&mut self, source_name: &str) -> Option<SourceRecord> {
        let removed = self.sources.remove(source_name);
        if let Some(source) = &removed {
            self.clear_active_writer_source(&source.output_stream, source_name);
        }
        removed
    }

    pub fn unregister_source_for_process(
        &mut self,
        source_name: &str,
        process_id: &str,
    ) -> Result<SourceRecord, RegistryError> {
        let source =
            self.sources
                .get(source_name)
                .ok_or_else(|| RegistryError::SourceNotFound {
                    source_name: source_name.to_string(),
                })?;
        if source.process_id != process_id {
            return Err(RegistryError::SourceNotOwnedByProcess {
                source_name: source_name.to_string(),
                process_id: process_id.to_string(),
                owner_process_id: Some(source.process_id.clone()),
            });
        }
        let removed =
            self.sources
                .remove(source_name)
                .ok_or_else(|| RegistryError::SourceNotFound {
                    source_name: source_name.to_string(),
                })?;
        self.clear_active_writer_source(&removed.output_stream, source_name);
        Ok(removed)
    }

    fn clear_active_writer_source(&mut self, output_stream: &str, source_name: &str) {
        let Some(stream) = self.streams.get_mut(output_stream) else {
            return;
        };
        if stream.active_writer_source_name.as_deref() == Some(source_name) {
            stream.active_writer_source_name = None;
            stream.writer_process_id = None;
            stream.active_segment_descriptor = None;
            stream.status = if stream.reader_count > 0 {
                "reader_attached".to_string()
            } else {
                "registered".to_string()
            };
        }
    }

    pub fn unregister_engine(&mut self, engine_name: &str) -> Option<EngineRecord> {
        self.engines.remove(engine_name)
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
        let mut engines = Vec::new();
        for engine_name in engine_names {
            if let Some(engine) = self.engines.remove(&engine_name) {
                engines.push(engine);
            }
        }

        DroppedTableRecords {
            stream,
            sources,
            engines,
        }
    }
}
