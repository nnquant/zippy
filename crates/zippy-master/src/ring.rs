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
    FrameTooLarge { frame_len: usize, frame_size: usize },
}

impl fmt::Display for RingWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge {
                frame_len,
                frame_size,
            } => write!(
                f,
                "frame size exceeds configured limit frame_len=[{}] frame_size=[{}]",
                frame_len, frame_size
            ),
        }
    }
}

impl std::error::Error for RingWriteError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingConfigError {
    pub buffer_size: usize,
}

impl fmt::Display for RingConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "stream ring buffer size must be positive buffer_size=[{}]",
            self.buffer_size
        )
    }
}

impl std::error::Error for RingConfigError {}

#[derive(Debug, Clone)]
struct StoredFrame {
    seq: u64,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub seq: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderAttachment {
    pub reader_id: String,
    pub next_read_seq: u64,
}

/// Best-effort per-stream ring buffer with independent reader offsets.
#[derive(Debug)]
pub struct StreamRing {
    buffer_size: usize,
    frame_size: usize,
    latest_frame_seq: u64,
    frames: VecDeque<StoredFrame>,
    readers: BTreeMap<String, u64>,
    next_reader_id: u64,
}

impl StreamRing {
    /// Create a frame ring using buffer_size/frame_size semantics.
    pub fn new(buffer_size: usize, frame_size: usize) -> Result<Self, RingConfigError> {
        if buffer_size == 0 {
            return Err(RingConfigError { buffer_size });
        }

        Ok(Self {
            buffer_size,
            frame_size,
            latest_frame_seq: 0,
            frames: VecDeque::new(),
            readers: BTreeMap::new(),
            next_reader_id: 0,
        })
    }

    /// Create a legacy best-effort ring without per-frame size enforcement.
    pub fn new_best_effort(buffer_size: usize) -> Result<Self, RingConfigError> {
        Self::new(buffer_size, usize::MAX)
    }

    pub fn attach_reader(&mut self) -> ReaderAttachment {
        self.attach_reader_at_seq(self.latest_frame_seq + 1)
    }

    pub fn attach_reader_at_latest(&mut self) -> ReaderAttachment {
        self.attach_reader_at_seq(self.latest_frame_seq + 1)
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

    pub fn publish_frame(&mut self, frame_bytes: &[u8]) -> Result<u64, RingWriteError> {
        self.validate_frame_size(frame_bytes.len())?;
        self.latest_frame_seq += 1;
        self.frames.push_back(StoredFrame {
            seq: self.latest_frame_seq,
            bytes: frame_bytes.to_vec(),
        });

        while self.frames.len() > self.buffer_size {
            self.frames.pop_front();
        }

        Ok(self.latest_frame_seq)
    }

    pub fn read(&mut self, reader_id: &str) -> Result<Vec<Vec<u8>>, ReadError> {
        Ok(self
            .read_frames(reader_id)?
            .into_iter()
            .map(|frame| frame.bytes)
            .collect())
    }

    pub fn read_frames(&mut self, reader_id: &str) -> Result<Vec<Frame>, ReadError> {
        let oldest_available_seq = self.oldest_available_seq();
        let latest_write_seq = self.latest_frame_seq;

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
        for frame in &self.frames {
            if frame.seq >= requested_seq {
                output.push(Frame {
                    seq: frame.seq,
                    bytes: frame.bytes.clone(),
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

        *next_seq = self.latest_frame_seq + 1;
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
        self.frames
            .front()
            .map(|frame| frame.seq)
            .unwrap_or(self.latest_frame_seq + 1)
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    pub fn frame_size(&self) -> usize {
        self.frame_size
    }

    pub fn next_write_seq(&self) -> u64 {
        self.latest_frame_seq + 1
    }

    fn validate_frame_size(&self, frame_len: usize) -> Result<(), RingWriteError> {
        if frame_len > self.frame_size {
            return Err(RingWriteError::FrameTooLarge {
                frame_len,
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
        let mut ring = StreamRing::new(4, 1024).unwrap();
        ring.publish_frame(b"a").unwrap();
        ring.publish_frame(b"b").unwrap();

        let attachment = ring.attach_reader_at_latest();
        assert_eq!(attachment.next_read_seq, 3);
    }

    #[test]
    fn frame_ring_reports_lagged_reader_after_overwrite() {
        let mut ring = StreamRing::new(2, 1024).unwrap();
        let reader = ring.attach_reader();
        ring.publish_frame(b"a").unwrap();
        ring.publish_frame(b"b").unwrap();
        ring.publish_frame(b"c").unwrap();

        let error = ring.read_frames(&reader.reader_id).unwrap_err();
        assert!(matches!(error, ReadError::ReaderLagged(_)));
    }

    #[test]
    fn frame_ring_rejects_payload_larger_than_frame_size() {
        let mut ring = StreamRing::new(2, 2).unwrap();

        let error = ring.publish_frame(b"abc").unwrap_err();
        assert!(matches!(
            error,
            RingWriteError::FrameTooLarge {
                frame_len: 3,
                frame_size: 2,
            }
        ));
    }
}
