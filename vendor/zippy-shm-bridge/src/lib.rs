use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::mem::align_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
#[cfg(windows)]
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE},
    System::Memory::{
        CreateFileMappingW, MapViewOfFile, UnmapViewOfFile, FILE_MAP_ALL_ACCESS,
        MEMORY_MAPPED_VIEW_ADDRESS, PAGE_READWRITE,
    },
};

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

struct RingHeader {
    buffer_size: usize,
    frame_size: usize,
    latest_seq: u64,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FrameState {
    Empty = 0,
    Writing = 1,
    Ready = 2,
}

impl FrameState {
    fn from_u8(value: u8) -> Result<Self, SharedFrameRingError> {
        match value {
            0 => Ok(Self::Empty),
            1 => Ok(Self::Writing),
            2 => Ok(Self::Ready),
            other => Err(SharedFrameRingError::Corrupted {
                reason: format!("invalid frame state value=[{}]", other),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PublishInProgress {
    seq: u64,
    slot: usize,
    payload_len: u32,
}

#[derive(Debug)]
struct FileLockGuard {
    #[cfg(unix)]
    file: File,
    #[cfg(not(unix))]
    _file: File,
}

impl FileLockGuard {
    fn acquire(mmap_path: &Path) -> Result<Self, SharedFrameRingError> {
        if let Some(parent) = mmap_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path_for(mmap_path))?;

        #[cfg(unix)]
        {
            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
            if result != 0 {
                return Err(SharedFrameRingError::Io(std::io::Error::last_os_error()));
            }
        }

        #[cfg(unix)]
        return Ok(Self { file });

        #[cfg(not(unix))]
        return Ok(Self { _file: file });
    }
}

impl Drop for FileLockGuard {
    fn drop(&mut self) {
        #[cfg(unix)]
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

pub struct SharedFrameRing {
    mmap_path: PathBuf,
    buffer_size: usize,
    frame_size: usize,
    total_size: usize,
    mapping: MappedFile,
}

impl fmt::Debug for SharedFrameRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedFrameRing")
            .field("mmap_path", &self.mmap_path)
            .field("buffer_size", &self.buffer_size)
            .field("frame_size", &self.frame_size)
            .finish_non_exhaustive()
    }
}

// Safety: access to the underlying mapping is only exposed through methods that
// use atomic publish/observe ordering for the hot path and a file lock only
// during create/open layout initialization.
unsafe impl Send for SharedFrameRing {}

// Safety: immutable references do not expose interior pointers, and the public
// data path is synchronized with atomics rather than aliasing mutable references.
unsafe impl Sync for SharedFrameRing {}

impl SharedFrameRing {
    pub fn create_or_open(
        mmap_path: impl AsRef<Path>,
        buffer_size: usize,
        frame_size: usize,
    ) -> Result<Self, SharedFrameRingError> {
        if buffer_size == 0 || frame_size == 0 {
            return Err(SharedFrameRingError::InvalidConfig {
                buffer_size,
                frame_size,
            });
        }

        let mmap_path = mmap_path.as_ref().to_path_buf();
        let total_size = total_size(buffer_size, frame_size)?;
        let _lock = FileLockGuard::acquire(&mmap_path)?;
        let (mapping, needs_initialize) = MappedFile::create_or_open(&mmap_path, total_size)?;

        let mut ring = Self {
            mmap_path,
            buffer_size,
            frame_size,
            total_size,
            mapping,
        };

        if needs_initialize {
            ring.initialize_layout()?;
        } else {
            ring.validate_layout()?;
        }

        Ok(ring)
    }

    pub fn next_seq(&self) -> Result<u64, SharedFrameRingError> {
        Ok(self.latest_seq().load(Ordering::Acquire) + 1)
    }

    pub fn publish(&mut self, payload: &[u8]) -> Result<u64, SharedFrameRingError> {
        if payload.len() > self.frame_size {
            return Err(SharedFrameRingError::FrameTooLarge {
                frame_len: payload.len(),
                frame_size: self.frame_size,
            });
        }

        let seq = self.next_seq()?;
        let progress = self.begin_publish(seq, payload)?;
        self.finish_publish(progress)?;
        Ok(seq)
    }

    pub fn publish_with_builder<F>(
        &mut self,
        payload_len: usize,
        build: F,
    ) -> Result<u64, SharedFrameRingError>
    where
        F: FnOnce(&mut [u8]),
    {
        if payload_len > self.frame_size {
            return Err(SharedFrameRingError::FrameTooLarge {
                frame_len: payload_len,
                frame_size: self.frame_size,
            });
        }

        let seq = self.next_seq()?;
        let progress = self.begin_publish_reserved(seq, payload_len as u32)?;
        {
            let offset = payload_offset(self.buffer_size, self.frame_size, progress.slot)?;
            let bytes = self.mapping_bytes_mut();
            build(&mut bytes[offset..offset + payload_len]);
        }
        self.prepare_publish(progress);
        self.finish_publish(progress)?;
        Ok(seq)
    }

    pub fn read(&self, requested_seq: u64) -> Result<ReadResult, SharedFrameRingError> {
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
        let state = self.frame_state_load(frame_header_offset)?;
        let stored_seq = self.frame_seq_load(frame_header_offset)?;

        match state {
            FrameState::Ready if stored_seq == requested_seq => {
                let stored_len = self.frame_len_load(frame_header_offset)? as usize;
                if stored_len > self.frame_size {
                    return Err(SharedFrameRingError::Corrupted {
                        reason: format!(
                            "frame length exceeds configured size stored_len=[{}] frame_size=[{}]",
                            stored_len, self.frame_size
                        ),
                    });
                }

                let payload_offset = payload_offset(self.buffer_size, self.frame_size, slot)?;
                let bytes = self.mapping_bytes();
                let payload = bytes[payload_offset..payload_offset + stored_len].to_vec();
                let confirmed_state = self.frame_state_load(frame_header_offset)?;
                let confirmed_seq = self.frame_seq_load(frame_header_offset)?;

                if confirmed_state != FrameState::Ready || confirmed_seq != requested_seq {
                    return Ok(self.classify_frame_availability(requested_seq, confirmed_seq));
                }

                Ok(ReadResult::Ready(Frame {
                    seq: requested_seq,
                    payload,
                }))
            }
            FrameState::Ready | FrameState::Writing | FrameState::Empty => {
                Ok(self.classify_frame_availability(requested_seq, stored_seq))
            }
        }
    }

    pub fn seek_latest(&self) -> Result<u64, SharedFrameRingError> {
        Ok(self.latest_seq().load(Ordering::Acquire) + 1)
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
        let latest_seq = self.latest_seq().load(Ordering::Acquire);

        if magic != RING_MAGIC {
            return Err(SharedFrameRingError::Corrupted {
                reason: format!("invalid ring magic value=[{}]", magic),
            });
        }

        if layout_version != RING_LAYOUT_VERSION {
            return Err(SharedFrameRingError::Corrupted {
                reason: format!("unsupported ring layout version value=[{}]", layout_version),
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
        assert_eq!(self.mapping.len(), self.total_size);
        self.mapping.as_slice()
    }

    fn mapping_bytes_mut(&mut self) -> &mut [u8] {
        assert_eq!(self.mapping.len(), self.total_size);
        self.mapping.as_slice_mut()
    }

    fn latest_seq(&self) -> &AtomicU64 {
        self.atomic_u64(LATEST_SEQ_OFFSET)
    }

    fn frame_seq(&self, slot: usize) -> &AtomicU64 {
        self.atomic_u64(frame_header_offset(slot))
    }

    fn frame_len(&self, slot: usize) -> &AtomicU32 {
        self.atomic_u32(frame_header_offset(slot) + 8)
    }

    fn frame_state(&self, slot: usize) -> &AtomicU8 {
        self.atomic_u8(frame_header_offset(slot) + 12)
    }

    fn begin_publish(
        &mut self,
        seq: u64,
        payload: &[u8],
    ) -> Result<PublishInProgress, SharedFrameRingError> {
        let progress = self.begin_publish_reserved(seq, payload.len() as u32)?;
        let payload_offset = payload_offset(self.buffer_size, self.frame_size, progress.slot)?;
        let bytes = self.mapping_bytes_mut();
        bytes[payload_offset..payload_offset + payload.len()].copy_from_slice(payload);
        self.prepare_publish(progress);
        Ok(progress)
    }

    fn begin_publish_reserved(
        &mut self,
        seq: u64,
        payload_len: u32,
    ) -> Result<PublishInProgress, SharedFrameRingError> {
        let slot = slot_index(seq, self.buffer_size);
        self.frame_state(slot)
            .store(FrameState::Writing as u8, Ordering::Release);
        Ok(PublishInProgress {
            seq,
            slot,
            payload_len,
        })
    }

    fn prepare_publish(&self, progress: PublishInProgress) {
        self.frame_len(progress.slot)
            .store(progress.payload_len, Ordering::Release);
        self.frame_seq(progress.slot)
            .store(progress.seq, Ordering::Release);
    }

    fn finish_publish(&self, progress: PublishInProgress) -> Result<(), SharedFrameRingError> {
        if progress.payload_len as usize > self.frame_size {
            return Err(SharedFrameRingError::FrameTooLarge {
                frame_len: progress.payload_len as usize,
                frame_size: self.frame_size,
            });
        }

        self.frame_state(progress.slot)
            .store(FrameState::Ready as u8, Ordering::Release);
        self.latest_seq().store(progress.seq, Ordering::Release);
        Ok(())
    }

    fn classify_frame_availability(&self, requested_seq: u64, observed_seq: u64) -> ReadResult {
        let latest_seq = self.latest_seq().load(Ordering::Acquire);
        if latest_seq == 0 || requested_seq > latest_seq {
            return ReadResult::Pending;
        }

        let oldest_seq = oldest_seq(latest_seq, self.buffer_size);
        if requested_seq < oldest_seq || observed_seq > requested_seq {
            return ReadResult::Lagged {
                oldest_seq,
                latest_seq,
            };
        }

        ReadResult::Pending
    }

    fn frame_seq_load(&self, frame_header_offset: usize) -> Result<u64, SharedFrameRingError> {
        Ok(unsafe { (*self.atomic_u64_ptr(frame_header_offset)).load(Ordering::Acquire) })
    }

    fn frame_len_load(&self, frame_header_offset: usize) -> Result<u32, SharedFrameRingError> {
        Ok(unsafe { (*self.atomic_u32_ptr(frame_header_offset + 8)).load(Ordering::Acquire) })
    }

    fn frame_state_load(
        &self,
        frame_header_offset: usize,
    ) -> Result<FrameState, SharedFrameRingError> {
        let value =
            unsafe { (*self.atomic_u8_ptr(frame_header_offset + 12)).load(Ordering::Acquire) };
        FrameState::from_u8(value)
    }

    fn atomic_u64(&self, offset: usize) -> &AtomicU64 {
        unsafe { &*self.atomic_u64_ptr(offset) }
    }

    fn atomic_u32(&self, offset: usize) -> &AtomicU32 {
        unsafe { &*self.atomic_u32_ptr(offset) }
    }

    fn atomic_u8(&self, offset: usize) -> &AtomicU8 {
        unsafe { &*self.atomic_u8_ptr(offset) }
    }

    fn atomic_u64_ptr(&self, offset: usize) -> *const AtomicU64 {
        assert_eq!(offset % align_of::<AtomicU64>(), 0);
        let base = self.mapping_bytes().as_ptr();
        assert_eq!((base as usize) % align_of::<AtomicU64>(), 0);
        unsafe { base.add(offset) as *const AtomicU64 }
    }

    fn atomic_u32_ptr(&self, offset: usize) -> *const AtomicU32 {
        assert_eq!(offset % align_of::<AtomicU32>(), 0);
        let base = self.mapping_bytes().as_ptr();
        assert_eq!((base as usize) % align_of::<AtomicU32>(), 0);
        unsafe { base.add(offset) as *const AtomicU32 }
    }

    fn atomic_u8_ptr(&self, offset: usize) -> *const AtomicU8 {
        let base = self.mapping_bytes().as_ptr();
        unsafe { base.add(offset) as *const AtomicU8 }
    }
}

impl Drop for SharedFrameRing {
    fn drop(&mut self) {
        if self.mapping.is_owner() {
            self.mapping.close();
            let _ = fs::remove_file(&self.mmap_path);
            let _ = fs::remove_file(lock_path_for(&self.mmap_path));
        }
    }
}

struct MappedFile {
    file: Option<File>,
    ptr: *mut u8,
    len: usize,
    owner: bool,
    #[cfg(windows)]
    mapping_handle: HANDLE,
}

impl fmt::Debug for MappedFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MappedFile")
            .field("len", &self.len)
            .field("owner", &self.owner)
            .finish()
    }
}

impl MappedFile {
    fn create_or_open(path: &Path, len: usize) -> Result<(Self, bool), SharedFrameRingError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let existed = path.exists();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let actual_len = file.metadata()?.len();
        let needs_initialize = !existed || actual_len == 0;
        if needs_initialize {
            file.set_len(len as u64)?;
        } else if actual_len != len as u64 {
            return Err(SharedFrameRingError::Corrupted {
                reason: format!(
                    "shared frame ring file size mismatch path=[{}] expected_bytes=[{}] actual_bytes=[{}]",
                    path.display(),
                    len,
                    actual_len
                ),
            });
        }

        preallocate_file(&file, len)?;

        #[cfg(unix)]
        let ptr = map_file(&file, len)?;
        #[cfg(windows)]
        let (ptr, mapping_handle) = map_file(&file, len)?;

        Ok((
            Self {
                file: Some(file),
                ptr,
                len,
                owner: needs_initialize,
                #[cfg(windows)]
                mapping_handle,
            },
            needs_initialize,
        ))
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_owner(&self) -> bool {
        self.owner
    }

    fn close(&mut self) {
        if !self.ptr.is_null() {
            #[cfg(unix)]
            {
                let _ = unsafe { libc::munmap(self.ptr.cast(), self.len) };
            }
            #[cfg(windows)]
            {
                let _ = unsafe {
                    UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS {
                        Value: self.ptr.cast(),
                    })
                };
            }
            self.ptr = std::ptr::null_mut();
        }
        #[cfg(windows)]
        if !self.mapping_handle.is_null() {
            let _ = unsafe { CloseHandle(self.mapping_handle) };
            self.mapping_handle = std::ptr::null_mut();
        }
        drop(self.file.take());
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for MappedFile {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(unix)]
fn map_file(file: &File, len: usize) -> Result<*mut u8, SharedFrameRingError> {
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(SharedFrameRingError::Mapping(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    Ok(ptr.cast::<u8>())
}

#[cfg(windows)]
fn map_file(file: &File, len: usize) -> Result<(*mut u8, HANDLE), SharedFrameRingError> {
    let len = u64::try_from(len).map_err(|_| SharedFrameRingError::SizeOverflow)?;
    let mapping_handle = unsafe {
        CreateFileMappingW(
            file.as_raw_handle() as HANDLE,
            std::ptr::null(),
            PAGE_READWRITE,
            (len >> 32) as u32,
            len as u32,
            std::ptr::null(),
        )
    };
    if mapping_handle.is_null() {
        return Err(SharedFrameRingError::Mapping(
            std::io::Error::last_os_error().to_string(),
        ));
    }

    let view = unsafe { MapViewOfFile(mapping_handle, FILE_MAP_ALL_ACCESS, 0, 0, len as usize) };
    if view.Value.is_null() {
        let error = std::io::Error::last_os_error();
        let _ = unsafe { CloseHandle(mapping_handle) };
        return Err(SharedFrameRingError::Mapping(error.to_string()));
    }

    Ok((view.Value.cast::<u8>(), mapping_handle))
}

#[cfg(unix)]
fn preallocate_file(file: &File, len: usize) -> Result<(), SharedFrameRingError> {
    let len = libc::off_t::try_from(len).map_err(|_| SharedFrameRingError::SizeOverflow)?;
    let result = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, len) };
    if result != 0 {
        return Err(SharedFrameRingError::Io(std::io::Error::from_raw_os_error(
            result,
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn preallocate_file(_file: &File, _len: usize) -> Result<(), SharedFrameRingError> {
    Ok(())
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

fn lock_path_for(mmap_path: &Path) -> PathBuf {
    let mut value = OsString::from(mmap_path.as_os_str());
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
    use super::{total_size, Frame, MappedFile, ReadResult, SharedFrameRing};
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_mmap_path() -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("zippy-shm-bridge-test-{nanos}.mmap"))
    }

    #[cfg(unix)]
    #[test]
    fn mapped_file_is_preallocated_before_first_touch_to_avoid_sparse_mmap_sigbus() {
        let mmap_path = unique_mmap_path();
        let buffer_size = 8;
        let frame_size = 1024 * 1024;
        let expected_bytes = total_size(buffer_size, frame_size).unwrap();
        let (_mapping, needs_initialize) =
            MappedFile::create_or_open(&mmap_path, expected_bytes).unwrap();
        let metadata = std::fs::metadata(&mmap_path).unwrap();
        let allocated_bytes = metadata.blocks() * 512;

        assert!(needs_initialize);
        assert!(
            allocated_bytes >= expected_bytes as u64,
            "backing file should be fully allocated allocated_bytes=[{}] expected_bytes=[{}]",
            allocated_bytes,
            expected_bytes
        );
        drop(_mapping);
        let _ = std::fs::remove_file(&mmap_path);
    }

    #[test]
    fn second_handle_reads_published_frame() {
        let mmap_path = unique_mmap_path();
        let mut writer = SharedFrameRing::create_or_open(&mmap_path, 4, 64).unwrap();
        let reader = SharedFrameRing::create_or_open(&mmap_path, 4, 64).unwrap();

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
    fn reader_is_pending_before_first_publish() {
        let mmap_path = unique_mmap_path();
        let reader = SharedFrameRing::create_or_open(&mmap_path, 4, 64).unwrap();

        assert_eq!(reader.read(1).unwrap(), ReadResult::Pending);
    }

    #[test]
    fn seek_latest_returns_latest_plus_one() {
        let mmap_path = unique_mmap_path();
        let mut ring = SharedFrameRing::create_or_open(&mmap_path, 4, 64).unwrap();

        assert_eq!(ring.seek_latest().unwrap(), 1);
        ring.publish(b"a").unwrap();
        ring.publish(b"b").unwrap();
        assert_eq!(ring.seek_latest().unwrap(), 3);
    }

    #[test]
    fn read_reports_lagged_after_overwrite() {
        let mmap_path = unique_mmap_path();
        let mut ring = SharedFrameRing::create_or_open(&mmap_path, 2, 64).unwrap();

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
    fn reader_does_not_observe_in_progress_frame() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let mmap_path = unique_mmap_path();
        let mut writer = SharedFrameRing::create_or_open(&mmap_path, 4, 64).unwrap();
        let reader = SharedFrameRing::create_or_open(&mmap_path, 4, 64).unwrap();
        let ready = Arc::new(Barrier::new(2));
        let proceed = Arc::new(Barrier::new(2));
        let ready_writer = Arc::clone(&ready);
        let proceed_writer = Arc::clone(&proceed);

        let handle = thread::spawn(move || {
            let seq = writer.next_seq().unwrap();
            let progress = writer.begin_publish(seq, b"abc").unwrap();
            ready_writer.wait();
            proceed_writer.wait();
            writer.finish_publish(progress).unwrap();
        });

        ready.wait();
        assert_eq!(reader.read(1).unwrap(), ReadResult::Pending);
        proceed.wait();
        handle.join().unwrap();

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
    fn publish_rejects_frame_larger_than_frame_size() {
        let mmap_path = unique_mmap_path();
        let mut ring = SharedFrameRing::create_or_open(&mmap_path, 2, 2).unwrap();

        let error = ring.publish(b"abc").unwrap_err();
        assert_eq!(
            error.to_string(),
            "frame too large frame_len=[3] frame_size=[2]"
        );
    }
}
