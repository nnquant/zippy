use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use zippy_core::bus_protocol::{ReaderDescriptor, WriterDescriptor, BUS_LAYOUT_VERSION};
use zippy_shm_bridge::{SharedFrameRing, SharedFrameRingError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusError {
    StreamAlreadyExists {
        stream_name: String,
    },
    StreamNotFound {
        stream_name: String,
    },
    InvalidBufferOrFrameSize {
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
    WriterAlreadyAttached {
        stream_name: String,
        writer_process_id: String,
    },
    ReaderNotFound {
        stream_name: String,
        reader_id: String,
    },
    WriterNotFound {
        stream_name: String,
        writer_id: String,
    },
    StorageInitFailed {
        stream_name: String,
        reason: String,
    },
}

impl fmt::Display for BusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StreamAlreadyExists { stream_name } => {
                write!(f, "stream already exists stream_name=[{}]", stream_name)
            }
            Self::StreamNotFound { stream_name } => {
                write!(f, "stream not found stream_name=[{}]", stream_name)
            }
            Self::InvalidBufferOrFrameSize {
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
            Self::WriterAlreadyAttached {
                stream_name,
                writer_process_id,
            } => write!(
                f,
                "writer already attached stream_name=[{}] writer_process_id=[{}]",
                stream_name, writer_process_id
            ),
            Self::ReaderNotFound {
                stream_name,
                reader_id,
            } => write!(
                f,
                "reader not found stream_name=[{}] reader_id=[{}]",
                stream_name, reader_id
            ),
            Self::WriterNotFound {
                stream_name,
                writer_id,
            } => write!(
                f,
                "writer not found stream_name=[{}] writer_id=[{}]",
                stream_name, writer_id
            ),
            Self::StorageInitFailed {
                stream_name,
                reason,
            } => write!(
                f,
                "storage init failed stream_name=[{}] reason=[{}]",
                stream_name, reason
            ),
        }
    }
}

impl std::error::Error for BusError {}

#[derive(Debug)]
struct StreamState {
    shared_ring: SharedFrameRing,
    buffer_size: usize,
    frame_size: usize,
    writer_process_id: Option<String>,
    writer_id: Option<String>,
    reader_ids: BTreeSet<String>,
    next_reader_id: u64,
    shm_name: String,
    layout_version: u32,
}

#[derive(Debug)]
pub struct Bus {
    streams: BTreeMap<String, StreamState>,
    root_dir: PathBuf,
    instance_id: String,
}

impl Default for Bus {
    fn default() -> Self {
        let instance_id = format!(
            "{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let root_dir = std::env::temp_dir().join("zippy-master-bus");
        Self {
            streams: BTreeMap::new(),
            root_dir,
            instance_id,
        }
    }
}

impl Bus {
    pub fn ensure_stream(
        &mut self,
        stream_name: &str,
        buffer_size: usize,
    ) -> Result<bool, BusError> {
        self.ensure_stream_with_sizes(stream_name, buffer_size, buffer_size)
    }

    pub fn ensure_stream_with_sizes(
        &mut self,
        stream_name: &str,
        buffer_size: usize,
        frame_size: usize,
    ) -> Result<bool, BusError> {
        if let Some(existing) = self.streams.get(stream_name) {
            if existing.buffer_size != buffer_size || existing.frame_size != frame_size {
                return Err(BusError::StreamConfigMismatch {
                    stream_name: stream_name.to_string(),
                    existing_buffer_size: existing.buffer_size,
                    existing_frame_size: existing.frame_size,
                    requested_buffer_size: buffer_size,
                    requested_frame_size: frame_size,
                });
            }
            return Ok(false);
        }

        self.create_stream_with_sizes(stream_name, buffer_size, frame_size)?;
        Ok(true)
    }

    pub fn create_stream(
        &mut self,
        stream_name: &str,
        buffer_size: usize,
    ) -> Result<(), BusError> {
        self.create_stream_with_sizes(stream_name, buffer_size, buffer_size)
    }

    pub fn create_stream_with_sizes(
        &mut self,
        stream_name: &str,
        buffer_size: usize,
        frame_size: usize,
    ) -> Result<(), BusError> {
        if self.streams.contains_key(stream_name) {
            return Err(BusError::StreamAlreadyExists {
                stream_name: stream_name.to_string(),
            });
        }

        std::fs::create_dir_all(&self.root_dir).map_err(|error| BusError::StorageInitFailed {
            stream_name: stream_name.to_string(),
            reason: error.to_string(),
        })?;

        let shm_path = self
            .root_dir
            .join(format!("{}_{}.flink", stream_name, self.instance_id));
        let shared_ring =
            SharedFrameRing::create_or_open(&shm_path, buffer_size, frame_size).map_err(
                |error| match error {
                    SharedFrameRingError::InvalidConfig {
                        buffer_size,
                        frame_size,
                    } => BusError::InvalidBufferOrFrameSize {
                            stream_name: stream_name.to_string(),
                            buffer_size,
                            frame_size,
                        },
                    other => BusError::StorageInitFailed {
                        stream_name: stream_name.to_string(),
                        reason: other.to_string(),
                    },
                },
            )?;

        self.streams.insert(
            stream_name.to_string(),
            StreamState {
                shared_ring,
                buffer_size,
                frame_size,
                writer_process_id: None,
                writer_id: None,
                reader_ids: BTreeSet::new(),
                next_reader_id: 1,
                shm_name: shm_path.to_string_lossy().into_owned(),
                layout_version: BUS_LAYOUT_VERSION,
            },
        );
        Ok(())
    }

    pub fn write_to(
        &mut self,
        stream_name: &str,
        process_id: &str,
    ) -> Result<WriterDescriptor, BusError> {
        let Some(stream) = self.streams.get_mut(stream_name) else {
            return Err(BusError::StreamNotFound {
                stream_name: stream_name.to_string(),
            });
        };

        if let Some(writer_process_id) = &stream.writer_process_id {
            return Err(BusError::WriterAlreadyAttached {
                stream_name: stream_name.to_string(),
                writer_process_id: writer_process_id.clone(),
            });
        }

        let writer_id = format!("{stream_name}_writer");
        let buffer_size = stream.buffer_size;
        let frame_size = stream.frame_size;
        let next_write_seq =
            stream
                .shared_ring
                .next_seq()
                .map_err(|error| BusError::StorageInitFailed {
                    stream_name: stream_name.to_string(),
                    reason: error.to_string(),
                })?;
        stream.writer_process_id = Some(process_id.to_string());
        stream.writer_id = Some(writer_id.clone());

        Ok(WriterDescriptor {
            stream_name: stream_name.to_string(),
            buffer_size,
            frame_size,
            layout_version: stream.layout_version,
            shm_name: stream.shm_name.clone(),
            writer_id,
            process_id: process_id.to_string(),
            next_write_seq,
        })
    }

    pub fn read_from(
        &mut self,
        stream_name: &str,
        process_id: &str,
        instrument_filter: Option<Vec<String>>,
    ) -> Result<ReaderDescriptor, BusError> {
        let Some(stream) = self.streams.get_mut(stream_name) else {
            return Err(BusError::StreamNotFound {
                stream_name: stream_name.to_string(),
            });
        };

        let reader_id = format!("{}_reader_{}", stream_name, stream.next_reader_id);
        let next_read_seq =
            stream
                .shared_ring
                .seek_latest()
                .map_err(|error| BusError::StorageInitFailed {
                    stream_name: stream_name.to_string(),
                    reason: error.to_string(),
                })?;
        stream.next_reader_id += 1;
        stream.reader_ids.insert(reader_id.clone());
        let instrument_filter = match instrument_filter {
            Some(filters) if filters.is_empty() => None,
            other => other,
        };

        Ok(ReaderDescriptor {
            stream_name: stream_name.to_string(),
            buffer_size: stream.buffer_size,
            frame_size: stream.frame_size,
            layout_version: stream.layout_version,
            shm_name: stream.shm_name.clone(),
            reader_id,
            process_id: process_id.to_string(),
            next_read_seq,
            instrument_filter,
        })
    }

    pub fn publish_test_frame(
        &mut self,
        stream_name: &str,
        frame_bytes: &[u8],
    ) -> Result<u64, BusError> {
        let Some(stream) = self.streams.get_mut(stream_name) else {
            return Err(BusError::StreamNotFound {
                stream_name: stream_name.to_string(),
            });
        };

        stream
            .shared_ring
            .publish(frame_bytes)
            .map_err(|error| BusError::StorageInitFailed {
                stream_name: stream_name.to_string(),
                reason: error.to_string(),
            })
    }

    pub fn detach_writer(&mut self, stream_name: &str, writer_id: &str) -> Result<(), BusError> {
        let Some(stream) = self.streams.get_mut(stream_name) else {
            return Err(BusError::StreamNotFound {
                stream_name: stream_name.to_string(),
            });
        };

        if stream.writer_id.as_deref() != Some(writer_id) {
            return Err(BusError::WriterNotFound {
                stream_name: stream_name.to_string(),
                writer_id: writer_id.to_string(),
            });
        }

        stream.writer_id = None;
        stream.writer_process_id = None;
        Ok(())
    }

    pub fn detach_reader(&mut self, stream_name: &str, reader_id: &str) -> Result<(), BusError> {
        let Some(stream) = self.streams.get_mut(stream_name) else {
            return Err(BusError::StreamNotFound {
                stream_name: stream_name.to_string(),
            });
        };

        if stream.reader_ids.remove(reader_id) {
            Ok(())
        } else {
            Err(BusError::ReaderNotFound {
                stream_name: stream_name.to_string(),
                reader_id: reader_id.to_string(),
            })
        }
    }

    pub fn reader_count(&self, stream_name: &str) -> Result<usize, BusError> {
        let Some(stream) = self.streams.get(stream_name) else {
            return Err(BusError::StreamNotFound {
                stream_name: stream_name.to_string(),
            });
        };

        Ok(stream.reader_ids.len())
    }

    pub fn remove_stream(&mut self, stream_name: &str) {
        if let Some(stream) = self.streams.remove(stream_name) {
            let shm_name = stream.shm_name.clone();
            let _ = std::fs::remove_file(&shm_name);
            let _ = std::fs::remove_file(format!("{shm_name}.lock"));
        }
    }
}
