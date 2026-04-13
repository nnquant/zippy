use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use shared_memory::{Shmem, ShmemConf, ShmemError};

const RING_MAGIC: u64 = 0x5a49_5050_595f_5247;
const RING_LAYOUT_VERSION: u32 = 1;
const GLOBAL_HEADER_SIZE: usize = 64;
const FRAME_HEADER_SIZE: usize = 16;

const MAGIC_OFFSET: usize = 0;
const VERSION_OFFSET: usize = 8;
const BUFFER_SIZE_OFFSET: usize = 16;
const FRAME_SIZE_OFFSET: usize = 24;
const LATEST_SEQ_OFFSET: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub seq: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadResult {
    Ready(Frame),
    Pending,
    Lagged { oldest_seq: u64, latest_seq: u64 },
}

#[derive(Debug)]
pub enum SharedFrameRingError {
    InvalidConfig {
        buffer_size: usize,
        frame_size: usize,
    },
    SizeOverflow,
    FrameTooLarge {
        frame_len: usize,
        frame_size: usize,
    },
    Io(std::io::Error),
    Mapping(String),
    LayoutMismatch {
        expected_buffer_size: usize,
        actual_buffer_size: usize,
        expected_frame_size: usize,
        actual_frame_size: usize,
    },
    Corrupted {
        reason: String,
    },
}

impl fmt::Display for SharedFrameRingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig {
                buffer_size,
                frame_size,
            } => write!(
                f,
                "invalid shared frame ring config buffer_size=[{}] frame_size=[{}]",
                buffer_size, frame_size
            ),
            Self::SizeOverflow => write!(f, "shared frame ring size overflow"),
            Self::FrameTooLarge {
                frame_len,
                frame_size,
            } => write!(
                f,
                "frame too large frame_len=[{}] frame_size=[{}]",
                frame_len, frame_size
            ),
            Self::Io(error) => write!(f, "shared frame ring io failed reason=[{}]", error),
            Self::Mapping(reason) => {
                write!(f, "shared frame ring mapping failed reason=[{}]", reason)
            }
            Self::LayoutMismatch {
                expected_buffer_size,
                actual_buffer_size,
                expected_frame_size,
                actual_frame_size,
            } => write!(
                f,
                "shared frame ring layout mismatch expected_buffer_size=[{}] actual_buffer_size=[{}] expected_frame_size=[{}] actual_frame_size=[{}]",
                expected_buffer_size,
                actual_buffer_size,
                expected_frame_size,
                actual_frame_size
            ),
            Self::Corrupted { reason } => {
                write!(f, "shared frame ring corrupted reason=[{}]", reason)
            }
        }
    }
}

impl std::error::Error for SharedFrameRingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for SharedFrameRingError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ShmemError> for SharedFrameRingError {
    fn from(value: ShmemError) -> Self {
        Self::Mapping(value.to_string())
    }
}

struct RingHeader {
    buffer_size: usize,
    frame_size: usize,
    latest_seq: u64,
}

#[derive(Debug)]
struct FileLockGuard {
    file: File,
}

impl FileLockGuard {
    fn acquire(flink_path: &Path) -> Result<Self, SharedFrameRingError> {
        if let Some(parent) = flink_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(lock_path_for(flink_path))?;

        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(SharedFrameRingError::Io(std::io::Error::last_os_error()));
        }

        Ok(Self { file })
    }
}

impl Drop for FileLockGuard {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

pub struct SharedFrameRing {
    flink_path: PathBuf,
    buffer_size: usize,
    frame_size: usize,
    total_size: usize,
    shmem: Shmem,
}

impl fmt::Debug for SharedFrameRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedFrameRing")
            .field("flink_path", &self.flink_path)
            .field("buffer_size", &self.buffer_size)
            .field("frame_size", &self.frame_size)
            .finish_non_exhaustive()
    }
}

// Safety: access to the underlying mapping is only exposed through methods that
// synchronize with a process-shared file lock before reading or writing bytes.
unsafe impl Send for SharedFrameRing {}

// Safety: immutable references do not expose interior pointers, and every public
// operation still serializes access through the same file lock.
unsafe impl Sync for SharedFrameRing {}

impl SharedFrameRing {
    pub fn create_or_open(
        flink_path: impl AsRef<Path>,
        buffer_size: usize,
        frame_size: usize,
    ) -> Result<Self, SharedFrameRingError> {
        if buffer_size == 0 || frame_size == 0 {
            return Err(SharedFrameRingError::InvalidConfig {
                buffer_size,
                frame_size,
            });
        }

        let flink_path = flink_path.as_ref().to_path_buf();
        let total_size = total_size(buffer_size, frame_size)?;
        let _lock = FileLockGuard::acquire(&flink_path)?;
        let shmem = match ShmemConf::new()
            .size(total_size)
            .flink(&flink_path)
            .create()
        {
            Ok(shmem) => shmem,
            Err(ShmemError::LinkExists) => ShmemConf::new().flink(&flink_path).open()?,
            Err(error) => return Err(error.into()),
        };

        if shmem.len() != total_size {
            return Err(SharedFrameRingError::Corrupted {
                reason: format!(
                    "shared memory size mismatch flink_path=[{}] expected_bytes=[{}] actual_bytes=[{}]",
                    flink_path.display(),
                    total_size,
                    shmem.len()
                ),
            });
        }

        let mut ring = Self {
            flink_path,
            buffer_size,
            frame_size,
            total_size,
            shmem,
        };

        if ring.shmem.is_owner() {
            ring.initialize_layout()?;
        } else {
            ring.validate_layout()?;
        }

        Ok(ring)
    }

    pub fn next_seq(&self) -> Result<u64, SharedFrameRingError> {
        let _lock = FileLockGuard::acquire(&self.flink_path)?;
        let header = self.read_header()?;
        Ok(header.latest_seq + 1)
    }

    pub fn publish(&mut self, payload: &[u8]) -> Result<u64, SharedFrameRingError> {
        if payload.len() > self.frame_size {
            return Err(SharedFrameRingError::FrameTooLarge {
                frame_len: payload.len(),
                frame_size: self.frame_size,
            });
        }

        let _lock = FileLockGuard::acquire(&self.flink_path)?;
        let header = self.read_header()?;
        let seq = header.latest_seq + 1;
        let slot = slot_index(seq, self.buffer_size);
        let frame_header_offset = frame_header_offset(slot);
        let payload_offset = payload_offset(self.buffer_size, self.frame_size, slot)?;
        let bytes = self.mapping_bytes_mut();

        bytes[payload_offset..payload_offset + payload.len()].copy_from_slice(payload);
        write_u64(bytes, frame_header_offset, seq);
        write_u64(bytes, frame_header_offset + 8, payload.len() as u64);
        write_u64(bytes, LATEST_SEQ_OFFSET, seq);

        Ok(seq)
    }

    pub fn read(&self, requested_seq: u64) -> Result<ReadResult, SharedFrameRingError> {
        let _lock = FileLockGuard::acquire(&self.flink_path)?;
        let header = self.read_header()?;

        if header.latest_seq == 0 || requested_seq > header.latest_seq {
            return Ok(ReadResult::Pending);
        }

        let oldest_seq = oldest_seq(header.latest_seq, self.buffer_size);
        if requested_seq < oldest_seq {
            return Ok(ReadResult::Lagged {
                oldest_seq,
                latest_seq: header.latest_seq,
            });
        }

        let slot = slot_index(requested_seq, self.buffer_size);
        let frame_header_offset = frame_header_offset(slot);
        let bytes = self.mapping_bytes();
        let stored_seq = read_u64(bytes, frame_header_offset);
        let stored_len = read_u64(bytes, frame_header_offset + 8) as usize;

        if stored_seq != requested_seq {
            return Err(SharedFrameRingError::Corrupted {
                reason: format!(
                    "frame sequence mismatch requested_seq=[{}] stored_seq=[{}] slot=[{}]",
                    requested_seq, stored_seq, slot
                ),
            });
        }

        if stored_len > self.frame_size {
            return Err(SharedFrameRingError::Corrupted {
                reason: format!(
                    "frame length exceeds configured size stored_len=[{}] frame_size=[{}]",
                    stored_len, self.frame_size
                ),
            });
        }

        let payload_offset = payload_offset(self.buffer_size, self.frame_size, slot)?;
        let payload = bytes[payload_offset..payload_offset + stored_len].to_vec();

        Ok(ReadResult::Ready(Frame {
            seq: requested_seq,
            payload,
        }))
    }

    pub fn seek_latest(&self) -> Result<u64, SharedFrameRingError> {
        let _lock = FileLockGuard::acquire(&self.flink_path)?;
        let header = self.read_header()?;
        Ok(header.latest_seq + 1)
    }

    fn initialize_layout(&mut self) -> Result<(), SharedFrameRingError> {
        let buffer_size = self.buffer_size as u64;
        let frame_size = self.frame_size as u64;
        let bytes = self.mapping_bytes_mut();
        bytes.fill(0);
        write_u64(bytes, MAGIC_OFFSET, RING_MAGIC);
        write_u32(bytes, VERSION_OFFSET, RING_LAYOUT_VERSION);
        write_u64(bytes, BUFFER_SIZE_OFFSET, buffer_size);
        write_u64(bytes, FRAME_SIZE_OFFSET, frame_size);
        write_u64(bytes, LATEST_SEQ_OFFSET, 0);
        Ok(())
    }

    fn validate_layout(&self) -> Result<(), SharedFrameRingError> {
        let header = self.read_header()?;
        if header.buffer_size != self.buffer_size || header.frame_size != self.frame_size {
            return Err(SharedFrameRingError::LayoutMismatch {
                expected_buffer_size: self.buffer_size,
                actual_buffer_size: header.buffer_size,
                expected_frame_size: self.frame_size,
                actual_frame_size: header.frame_size,
            });
        }
        Ok(())
    }

    fn read_header(&self) -> Result<RingHeader, SharedFrameRingError> {
        let bytes = self.mapping_bytes();
        let magic = read_u64(bytes, MAGIC_OFFSET);
        let layout_version = read_u32(bytes, VERSION_OFFSET);
        let buffer_size = read_u64(bytes, BUFFER_SIZE_OFFSET) as usize;
        let frame_size = read_u64(bytes, FRAME_SIZE_OFFSET) as usize;
        let latest_seq = read_u64(bytes, LATEST_SEQ_OFFSET);

        if magic != RING_MAGIC {
            return Err(SharedFrameRingError::Corrupted {
                reason: format!("invalid ring magic value=[{}]", magic),
            });
        }

        if layout_version != RING_LAYOUT_VERSION {
            return Err(SharedFrameRingError::Corrupted {
                reason: format!(
                    "unsupported ring layout version value=[{}]",
                    layout_version
                ),
            });
        }

        if buffer_size == 0 || frame_size == 0 {
            return Err(SharedFrameRingError::Corrupted {
                reason: format!(
                    "invalid ring header buffer_size=[{}] frame_size=[{}]",
                    buffer_size, frame_size
                ),
            });
        }

        Ok(RingHeader {
            buffer_size,
            frame_size,
            latest_seq,
        })
    }

    fn mapping_bytes(&self) -> &[u8] {
        assert_eq!(self.shmem.len(), self.total_size);
        unsafe { self.shmem.as_slice() }
    }

    fn mapping_bytes_mut(&mut self) -> &mut [u8] {
        assert_eq!(self.shmem.len(), self.total_size);
        unsafe { self.shmem.as_slice_mut() }
    }
}

impl Drop for SharedFrameRing {
    fn drop(&mut self) {
        if self.shmem.is_owner() {
            let _ = fs::remove_file(lock_path_for(&self.flink_path));
        }
    }
}

fn total_size(buffer_size: usize, frame_size: usize) -> Result<usize, SharedFrameRingError> {
    let header_bytes = buffer_size
        .checked_mul(FRAME_HEADER_SIZE)
        .and_then(|value| value.checked_add(GLOBAL_HEADER_SIZE))
        .ok_or(SharedFrameRingError::SizeOverflow)?;
    let payload_bytes = buffer_size
        .checked_mul(frame_size)
        .ok_or(SharedFrameRingError::SizeOverflow)?;

    header_bytes
        .checked_add(payload_bytes)
        .ok_or(SharedFrameRingError::SizeOverflow)
}

fn lock_path_for(flink_path: &Path) -> PathBuf {
    let mut value = OsString::from(flink_path.as_os_str());
    value.push(".lock");
    PathBuf::from(value)
}

fn oldest_seq(latest_seq: u64, buffer_size: usize) -> u64 {
    let buffer_size = buffer_size as u64;
    if latest_seq < buffer_size {
        1
    } else {
        latest_seq - buffer_size + 1
    }
}

fn slot_index(seq: u64, buffer_size: usize) -> usize {
    ((seq - 1) % buffer_size as u64) as usize
}

fn frame_header_offset(slot: usize) -> usize {
    GLOBAL_HEADER_SIZE + slot * FRAME_HEADER_SIZE
}

fn payload_offset(
    buffer_size: usize,
    frame_size: usize,
    slot: usize,
) -> Result<usize, SharedFrameRingError> {
    let payload_start = GLOBAL_HEADER_SIZE
        .checked_add(
            buffer_size
                .checked_mul(FRAME_HEADER_SIZE)
                .ok_or(SharedFrameRingError::SizeOverflow)?,
        )
        .ok_or(SharedFrameRingError::SizeOverflow)?;

    payload_start
        .checked_add(
            slot.checked_mul(frame_size)
                .ok_or(SharedFrameRingError::SizeOverflow)?,
        )
        .ok_or(SharedFrameRingError::SizeOverflow)
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut value = [0_u8; 8];
    value.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(value)
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    let mut value = [0_u8; 4];
    value.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(value)
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::{Frame, ReadResult, SharedFrameRing};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_flink_path() -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("zippy-shm-bridge-test-{nanos}.flink"))
    }

    #[test]
    fn second_handle_reads_published_frame() {
        let flink_path = unique_flink_path();
        let mut writer = SharedFrameRing::create_or_open(&flink_path, 4, 64).unwrap();
        let reader = SharedFrameRing::create_or_open(&flink_path, 4, 64).unwrap();

        let published_seq = writer.publish(b"abc").unwrap();
        assert_eq!(published_seq, 1);

        let result = reader.read(1).unwrap();
        assert_eq!(
            result,
            ReadResult::Ready(Frame {
                seq: 1,
                payload: b"abc".to_vec(),
            })
        );
    }

    #[test]
    fn seek_latest_returns_latest_plus_one() {
        let flink_path = unique_flink_path();
        let mut ring = SharedFrameRing::create_or_open(&flink_path, 4, 64).unwrap();

        assert_eq!(ring.seek_latest().unwrap(), 1);
        ring.publish(b"a").unwrap();
        ring.publish(b"b").unwrap();
        assert_eq!(ring.seek_latest().unwrap(), 3);
    }

    #[test]
    fn read_reports_lagged_after_overwrite() {
        let flink_path = unique_flink_path();
        let mut ring = SharedFrameRing::create_or_open(&flink_path, 2, 64).unwrap();

        ring.publish(b"a").unwrap();
        ring.publish(b"b").unwrap();
        ring.publish(b"c").unwrap();

        let result = ring.read(1).unwrap();
        assert_eq!(
            result,
            ReadResult::Lagged {
                oldest_seq: 2,
                latest_seq: 3,
            }
        );
    }

    #[test]
    fn publish_rejects_frame_larger_than_frame_size() {
        let flink_path = unique_flink_path();
        let mut ring = SharedFrameRing::create_or_open(&flink_path, 2, 2).unwrap();

        let error = ring.publish(b"abc").unwrap_err();
        assert_eq!(error.to_string(), "frame too large frame_len=[3] frame_size=[2]");
    }
}
