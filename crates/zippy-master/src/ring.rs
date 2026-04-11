use std::collections::{BTreeMap, VecDeque};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderLagged {
    pub reader_id: String,
    pub requested_seq: u64,
    pub oldest_available_seq: u64,
    pub latest_write_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderNotFound {
    pub reader_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadError {
    ReaderLagged(ReaderLagged),
    ReaderNotFound(ReaderNotFound),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingWriteError {
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingConfigError {
    pub capacity: usize,
}

impl fmt::Display for RingConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "stream ring capacity must be positive capacity=[{}]",
            self.capacity
        )
    }
}

impl std::error::Error for RingConfigError {}

#[derive(Debug, Clone)]
struct RingItem {
    seq: u64,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderAttachment {
    pub reader_id: String,
    pub next_read_seq: u64,
}

/// Best-effort per-stream ring buffer with independent reader offsets.
#[derive(Debug)]
pub struct StreamRing {
    capacity: usize,
    write_seq: u64,
    items: VecDeque<RingItem>,
    readers: BTreeMap<String, u64>,
    next_reader_id: u64,
}

impl StreamRing {
    pub fn new(capacity: usize) -> Result<Self, RingConfigError> {
        if capacity == 0 {
            return Err(RingConfigError { capacity });
        }

        Ok(Self {
            capacity,
            write_seq: 0,
            items: VecDeque::new(),
            readers: BTreeMap::new(),
            next_reader_id: 0,
        })
    }

    pub fn attach_reader(&mut self) -> ReaderAttachment {
        self.next_reader_id += 1;
        let reader_id = format!("reader_{}", self.next_reader_id);
        let next_read_seq = self.write_seq + 1;
        self.readers.insert(reader_id.clone(), next_read_seq);
        ReaderAttachment {
            reader_id,
            next_read_seq,
        }
    }

    pub fn write(&mut self, payload: Vec<u8>) -> Result<(), RingWriteError> {
        self.write_seq += 1;
        self.items.push_back(RingItem {
            seq: self.write_seq,
            payload,
        });

        while self.items.len() > self.capacity {
            self.items.pop_front();
        }

        Ok(())
    }

    pub fn read(&mut self, reader_id: &str) -> Result<Vec<Vec<u8>>, ReadError> {
        let oldest_available_seq = self.oldest_available_seq();
        let latest_write_seq = self.write_seq;

        let Some(next_seq) = self.readers.get_mut(reader_id) else {
            return Err(ReadError::ReaderNotFound(ReaderNotFound {
                reader_id: reader_id.to_string(),
            }));
        };

        let requested_seq = *next_seq;

        if requested_seq < oldest_available_seq {
            return Err(ReadError::ReaderLagged(ReaderLagged {
                reader_id: reader_id.to_string(),
                requested_seq,
                oldest_available_seq,
                latest_write_seq,
            }));
        }

        let mut output = Vec::new();
        for item in &self.items {
            if item.seq >= requested_seq {
                output.push(item.payload.clone());
            }
        }

        *next_seq = latest_write_seq + 1;
        Ok(output)
    }

    pub fn seek_latest(&mut self, reader_id: &str) -> Result<(), ReaderNotFound> {
        let Some(next_seq) = self.readers.get_mut(reader_id) else {
            return Err(ReaderNotFound {
                reader_id: reader_id.to_string(),
            });
        };

        *next_seq = self.write_seq + 1;
        Ok(())
    }

    pub fn detach_reader(&mut self, reader_id: &str) -> Result<(), ReaderNotFound> {
        if self.readers.remove(reader_id).is_some() {
            return Ok(());
        }

        Err(ReaderNotFound {
            reader_id: reader_id.to_string(),
        })
    }

    pub fn reader_count(&self) -> usize {
        self.readers.len()
    }

    fn oldest_available_seq(&self) -> u64 {
        self.items
            .front()
            .map(|item| item.seq)
            .unwrap_or(self.write_seq + 1)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn next_write_seq(&self) -> u64 {
        self.write_seq + 1
    }
}
