use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone)]
pub struct ProcessRecord {
    pub process_id: String,
    pub app: String,
}

#[derive(Debug, Clone)]
pub struct StreamRecord {
    pub stream_name: String,
    pub ring_capacity: usize,
    pub writer_process_id: Option<String>,
    pub reader_count: usize,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    StreamAlreadyExists {
        stream_name: String,
    },
    InvalidRingCapacity {
        stream_name: String,
        ring_capacity: usize,
    },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
    pub fn register_process(&mut self, app: &str) -> String {
        self.next_process_id += 1;
        let process_id = format!("proc_{}", self.next_process_id);
        let record = ProcessRecord {
            process_id: process_id.clone(),
            app: app.to_string(),
        };
        self.processes.insert(process_id.clone(), record);
        process_id
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

    pub fn list_streams(&self) -> Vec<StreamRecord> {
        self.streams.values().cloned().collect()
    }

    pub fn unregister_stream(&mut self, stream_name: &str) -> Option<StreamRecord> {
        self.streams.remove(stream_name)
    }
}
