use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use zippy_core::bus_protocol::{ReaderDescriptor, WriterDescriptor, BUS_LAYOUT_VERSION};

use crate::ring::{ReaderAttachment, StreamRing};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusError {
    StreamAlreadyExists {
        stream_name: String,
    },
    StreamNotFound {
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
    ring: StreamRing,
    buffer_size: usize,
    frame_size: usize,
    writer_process_id: Option<String>,
    writer_id: Option<String>,
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

fn next_storage_sequence(shm_name: &str) -> std::io::Result<u64> {
    let stream_dir = std::path::Path::new(shm_name);
    if !stream_dir.exists() {
        return Ok(1);
    }

    let mut latest = 0_u64;
    for entry in std::fs::read_dir(stream_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let Some(sequence) = parse_seq_file_name(&entry.file_name().to_string_lossy()) else {
            continue;
        };
        latest = latest.max(sequence);
    }
    Ok(latest + 1)
}

fn parse_seq_file_name(name: &str) -> Option<u64> {
    if !name.starts_with("seq_") || !name.ends_with(".ipc") {
        return None;
    }
    name[4..name.len() - 4].parse::<u64>().ok()
}

impl Bus {
    pub fn ensure_stream(
        &mut self,
        stream_name: &str,
        ring_capacity: usize,
    ) -> Result<bool, BusError> {
        self.ensure_stream_with_sizes(stream_name, ring_capacity, ring_capacity)
    }

    pub fn ensure_stream_with_sizes(
        &mut self,
        stream_name: &str,
        buffer_size: usize,
        frame_size: usize,
    ) -> Result<bool, BusError> {
        if let Some(existing) = self.streams.get(stream_name) {
            let existing_ring_capacity = existing.ring.capacity();
            if existing_ring_capacity != buffer_size
                || existing.buffer_size != buffer_size
                || existing.frame_size != frame_size
            {
                return Err(BusError::StreamRingCapacityMismatch {
                    stream_name: stream_name.to_string(),
                    existing_ring_capacity,
                    requested_ring_capacity: buffer_size,
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
        ring_capacity: usize,
    ) -> Result<(), BusError> {
        self.create_stream_with_sizes(stream_name, ring_capacity, ring_capacity)
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

        let ring =
            StreamRing::new(buffer_size).map_err(|error| BusError::InvalidRingCapacity {
                stream_name: stream_name.to_string(),
                ring_capacity: error.capacity,
            })?;

        std::fs::create_dir_all(&self.root_dir).map_err(|error| BusError::StorageInitFailed {
            stream_name: stream_name.to_string(),
            reason: error.to_string(),
        })?;

        let stream_dir = self
            .root_dir
            .join(format!("{}_{}", stream_name, self.instance_id));
        std::fs::create_dir_all(&stream_dir).map_err(|error| BusError::StorageInitFailed {
            stream_name: stream_name.to_string(),
            reason: error.to_string(),
        })?;

        self.streams.insert(
            stream_name.to_string(),
            StreamState {
                ring,
                buffer_size,
                frame_size,
                writer_process_id: None,
                writer_id: None,
                shm_name: stream_dir.to_string_lossy().into_owned(),
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
        let next_write_seq = next_storage_sequence(&stream.shm_name).map_err(|error| {
            BusError::StorageInitFailed {
                stream_name: stream_name.to_string(),
                reason: error.to_string(),
            }
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
    ) -> Result<ReaderDescriptor, BusError> {
        let Some(stream) = self.streams.get_mut(stream_name) else {
            return Err(BusError::StreamNotFound {
                stream_name: stream_name.to_string(),
            });
        };

        let ReaderAttachment {
            reader_id,
            next_read_seq: _,
        } = stream.ring.attach_reader();
        let next_read_seq = next_storage_sequence(&stream.shm_name).map_err(|error| {
            BusError::StorageInitFailed {
                stream_name: stream_name.to_string(),
                reason: error.to_string(),
            }
        })?;

        Ok(ReaderDescriptor {
            stream_name: stream_name.to_string(),
            buffer_size: stream.buffer_size,
            frame_size: stream.frame_size,
            layout_version: stream.layout_version,
            shm_name: stream.shm_name.clone(),
            reader_id,
            process_id: process_id.to_string(),
            next_read_seq,
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

        stream
            .ring
            .detach_reader(reader_id)
            .map_err(|_| BusError::ReaderNotFound {
                stream_name: stream_name.to_string(),
                reader_id: reader_id.to_string(),
            })
    }

    pub fn reader_count(&self, stream_name: &str) -> Result<usize, BusError> {
        let Some(stream) = self.streams.get(stream_name) else {
            return Err(BusError::StreamNotFound {
                stream_name: stream_name.to_string(),
            });
        };

        Ok(stream.ring.reader_count())
    }

    pub fn remove_stream(&mut self, stream_name: &str) {
        if let Some(stream) = self.streams.remove(stream_name) {
            let _ = std::fs::remove_dir_all(stream.shm_name);
        }
    }
}
