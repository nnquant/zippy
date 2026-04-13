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
pub enum RingWriteError {
    FrameTooLarge {
        payload_len: usize,
        frame_size: usize,
    },
}

impl fmt::Display for RingWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge {
                payload_len,
                frame_size,
            } => write!(
                f,
                "frame payload exceeds frame size payload_len=[{}] frame_size=[{}]",
                payload_len, frame_size
            ),
        }
    }
}

impl std::error::Error for RingWriteError {}

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
pub struct Frame {
    pub seq: u64,
    pub payload: Vec<u8>,
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
    frame_size: usize,
    write_seq: u64,
    items: VecDeque<RingItem>,
    readers: BTreeMap<String, u64>,
    next_reader_id: u64,
}

impl StreamRing {
    /// Create a legacy best-effort ring without per-frame size enforcement.
    pub fn new(capacity: usize) -> Result<Self, RingConfigError> {
        Self::with_frame_size(capacity, usize::MAX)
    }

    /// Create a frame ring with fixed payload capacity for each logical frame.
    pub fn with_frame_size(capacity: usize, frame_size: usize) -> Result<Self, RingConfigError> {
        if capacity == 0 {
            return Err(RingConfigError { capacity });
        }

        Ok(Self {
            capacity,
            frame_size,
            write_seq: 0,
            items: VecDeque::new(),
            readers: BTreeMap::new(),
            next_reader_id: 0,
        })
    }

    pub fn attach_reader(&mut self) -> ReaderAttachment {
        self.attach_reader_at_seq(self.write_seq + 1)
    }

    pub fn attach_reader_at_latest(&mut self) -> ReaderAttachment {
        self.attach_reader_at_seq(self.write_seq + 1)
    }

    fn attach_reader_at_seq(&mut self, next_read_seq: u64) -> ReaderAttachment {
        self.next_reader_id += 1;
        let reader_id = format!("reader_{}", self.next_reader_id);
        self.readers.insert(reader_id.clone(), next_read_seq);
        ReaderAttachment {
            reader_id,
            next_read_seq,
        }
    }

    pub fn write(&mut self, payload: Vec<u8>) -> Result<(), RingWriteError> {
        self.publish_frame(&payload)?;
        Ok(())
    }

    pub fn publish_frame(&mut self, payload: &[u8]) -> Result<u64, RingWriteError> {
        self.validate_frame_size(payload.len())?;
        self.write_seq += 1;
        self.items.push_back(RingItem {
            seq: self.write_seq,
            payload: payload.to_vec(),
        });

        while self.items.len() > self.capacity {
            self.items.pop_front();
        }

        Ok(self.write_seq)
    }

    pub fn read(&mut self, reader_id: &str) -> Result<Vec<Vec<u8>>, ReadError> {
        Ok(self
            .read_frames(reader_id)?
            .into_iter()
            .map(|frame| frame.payload)
            .collect())
    }

    pub fn read_frames(&mut self, reader_id: &str) -> Result<Vec<Frame>, ReadError> {
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
                output.push(Frame {
                    seq: item.seq,
                    payload: item.payload.clone(),
                });
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

    pub fn frame_size(&self) -> usize {
        self.frame_size
    }

    pub fn next_write_seq(&self) -> u64 {
        self.write_seq + 1
    }

    fn validate_frame_size(&self, payload_len: usize) -> Result<(), RingWriteError> {
        if payload_len > self.frame_size {
            return Err(RingWriteError::FrameTooLarge {
                payload_len,
                frame_size: self.frame_size,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ReadError, RingWriteError, StreamRing};

    #[test]
    fn frame_ring_new_reader_starts_after_current_write_seq() {
        let mut ring = StreamRing::with_frame_size(4, 1024).unwrap();
        ring.publish_frame(b"a").unwrap();
        ring.publish_frame(b"b").unwrap();

        let attachment = ring.attach_reader_at_latest();
        assert_eq!(attachment.next_read_seq, 3);
    }

    #[test]
    fn frame_ring_reports_lagged_reader_after_overwrite() {
        let mut ring = StreamRing::with_frame_size(2, 1024).unwrap();
        let reader = ring.attach_reader();
        ring.publish_frame(b"a").unwrap();
        ring.publish_frame(b"b").unwrap();
        ring.publish_frame(b"c").unwrap();

        let error = ring.read_frames(&reader.reader_id).unwrap_err();
        assert!(matches!(error, ReadError::ReaderLagged(_)));
    }

    #[test]
    fn frame_ring_rejects_payload_larger_than_frame_size() {
        let mut ring = StreamRing::with_frame_size(2, 2).unwrap();

        let error = ring.publish_frame(b"abc").unwrap_err();
        assert!(matches!(
            error,
            RingWriteError::FrameTooLarge {
                payload_len: 3,
                frame_size: 2,
            }
        ));
    }
}
