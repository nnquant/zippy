use std::{
    fmt,
    fs::{self, File, OpenOptions},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::ZippySegmentStoreError;

/// 共享内存 region 的最小封装。
///
/// 当前阶段只负责：
/// - 创建固定大小的共享内存段
/// - 通过 `os_id` 重新打开既有段
/// - 在指定偏移读写字节
///
/// 它暂时还不承载 segment 布局语义，后续再接入 active segment builder。
pub struct ShmRegion {
    inner: MappedFile,
    os_id: String,
}

// SAFETY:
// `ShmRegion` exposes bounds-checked byte copies and aligned atomic u64
// operations only. The underlying mmap address is stable for the lifetime of
// the region, and cross-thread/process visibility is controlled by the caller's
// segment commit protocol. Mutable byte writes still require `&mut self`.
unsafe impl Send for ShmRegion {}

// SAFETY:
// Shared references do not expose mutable slices. Concurrent readers are safe,
// and the committed-row protocol uses release/acquire atomics for the only
// cross-thread synchronization primitive currently exposed through `&self`.
unsafe impl Sync for ShmRegion {}

impl fmt::Debug for ShmRegion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShmRegion")
            .field("os_id", &self.os_id)
            .field("len", &self.inner.len())
            .finish()
    }
}

impl ShmRegion {
    /// 创建新的共享内存 region。
    pub fn create(size: usize) -> Result<Self, ZippySegmentStoreError> {
        if size == 0 {
            return Err(ZippySegmentStoreError::Shmem(
                "shared memory region size must be greater than zero".to_string(),
            ));
        }

        let inner = MappedFile::create_unique(size)?;
        let os_id = format!("{FILE_OS_ID_PREFIX}{}", inner.path().display());
        Ok(Self { inner, os_id })
    }

    /// 通过 `os_id` 打开既有共享内存 region。
    pub fn open(os_id: &str) -> Result<Self, ZippySegmentStoreError> {
        let path = os_id.strip_prefix(FILE_OS_ID_PREFIX).ok_or_else(|| {
            ZippySegmentStoreError::Shmem(format!(
                "unsupported shared memory os_id format os_id=[{os_id}]"
            ))
        })?;
        let inner = MappedFile::open_existing(Path::new(path))?;
        Ok(Self {
            inner,
            os_id: os_id.to_string(),
        })
    }

    /// 返回该 region 的操作系统标识。
    pub fn os_id(&self) -> &str {
        &self.os_id
    }

    /// 返回该 region 的总字节数。
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// 从指定偏移读取字节。
    pub fn read_at(
        &self,
        offset: usize,
        destination: &mut [u8],
    ) -> Result<(), ZippySegmentStoreError> {
        self.checked_range(offset, destination.len())?;
        // SAFETY:
        // - `checked_range` 已确保切片范围落在 mapping 内。
        // - 这里只暴露只读拷贝，不泄漏对底层内存的长期借用。
        let source = &self.inner.as_slice()[offset..offset + destination.len()];
        destination.copy_from_slice(source);
        Ok(())
    }

    /// 向指定偏移写入字节。
    pub fn write_at(&mut self, offset: usize, bytes: &[u8]) -> Result<(), ZippySegmentStoreError> {
        self.checked_range(offset, bytes.len())?;
        // SAFETY:
        // - `checked_range` 已确保切片范围落在 mapping 内。
        // - `&mut self` 保证当前调用方持有该 `ShmRegion` 的独占访问权。
        let destination = &mut self.inner.as_slice_mut()[offset..offset + bytes.len()];
        destination.copy_from_slice(bytes);
        Ok(())
    }

    /// 以 release 语义向共享内存中的 8 字节对齐位置写入 `u64`。
    pub fn store_u64_release(
        &self,
        offset: usize,
        value: u64,
    ) -> Result<(), ZippySegmentStoreError> {
        self.checked_atomic_u64_offset(offset)?;
        // SAFETY:
        // - `checked_atomic_u64_offset` 保证该地址在 mapping 内且满足 `AtomicU64` 对齐。
        // - 共享内存本身允许跨进程并发访问；这里用原子操作发布可见性边界。
        let atomic = unsafe { &*(self.inner.as_ptr().add(offset).cast::<AtomicU64>()) };
        atomic.store(value, Ordering::Release);
        Ok(())
    }

    /// 以 acquire 语义从共享内存中的 8 字节对齐位置读取 `u64`。
    pub fn load_u64_acquire(&self, offset: usize) -> Result<u64, ZippySegmentStoreError> {
        self.checked_atomic_u64_offset(offset)?;
        // SAFETY:
        // - `checked_atomic_u64_offset` 保证该地址在 mapping 内且满足 `AtomicU64` 对齐。
        // - 与 writer 侧 `store_u64_release` 配对，作为 committed prefix 的发布协议。
        let atomic = unsafe { &*(self.inner.as_ptr().add(offset).cast::<AtomicU64>()) };
        Ok(atomic.load(Ordering::Acquire))
    }

    fn checked_range(&self, offset: usize, len: usize) -> Result<(), ZippySegmentStoreError> {
        let end = offset.checked_add(len).ok_or_else(|| {
            ZippySegmentStoreError::Shmem("shared memory range overflow".to_string())
        })?;
        if end > self.inner.len() {
            return Err(ZippySegmentStoreError::Shmem(format!(
                "shared memory range out of bounds offset=[{offset}] len=[{len}] region_len=[{}]",
                self.inner.len()
            )));
        }
        Ok(())
    }

    fn checked_atomic_u64_offset(&self, offset: usize) -> Result<(), ZippySegmentStoreError> {
        self.checked_range(offset, std::mem::size_of::<u64>())?;
        if offset % std::mem::align_of::<AtomicU64>() != 0 {
            return Err(ZippySegmentStoreError::Shmem(format!(
                "shared memory atomic u64 offset is not aligned offset=[{offset}] align=[{}]",
                std::mem::align_of::<AtomicU64>()
            )));
        }
        Ok(())
    }
}

const FILE_OS_ID_PREFIX: &str = "file:";
const FILE_CREATE_ATTEMPTS: usize = 100;

struct MappedFile {
    _file: File,
    path: PathBuf,
    ptr: *mut u8,
    len: usize,
    owner: bool,
}

impl fmt::Debug for MappedFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MappedFile")
            .field("path", &self.path)
            .field("len", &self.len)
            .field("owner", &self.owner)
            .finish()
    }
}

impl MappedFile {
    fn create_unique(size: usize) -> Result<Self, ZippySegmentStoreError> {
        let dir = std::env::temp_dir().join("zippy-segment-store");
        fs::create_dir_all(&dir)
            .map_err(|error| ZippySegmentStoreError::Shmem(error.to_string()))?;
        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        for attempt in 0..FILE_CREATE_ATTEMPTS {
            let path = dir.join(format!("segment-{pid}-{nanos}-{attempt}.shm"));
            match Self::create_new(&path, size) {
                Ok(mapping) => return Ok(mapping),
                Err(ZippySegmentStoreError::Shmem(reason))
                    if reason.contains("File exists") || reason.contains("file exists") =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }

        Err(ZippySegmentStoreError::Shmem(format!(
            "failed to create unique shared memory file attempts=[{FILE_CREATE_ATTEMPTS}]"
        )))
    }

    fn create_new(path: &Path, size: usize) -> Result<Self, ZippySegmentStoreError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| ZippySegmentStoreError::Shmem(error.to_string()))?;
        file.set_len(size as u64)
            .map_err(|error| ZippySegmentStoreError::Shmem(error.to_string()))?;
        preallocate_file(&file, size)?;
        Self::map_file(file, path.to_path_buf(), size, true)
    }

    fn open_existing(path: &Path) -> Result<Self, ZippySegmentStoreError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| ZippySegmentStoreError::Shmem(error.to_string()))?;
        let len = file
            .metadata()
            .map_err(|error| ZippySegmentStoreError::Shmem(error.to_string()))?
            .len();
        let len = usize::try_from(len).map_err(|_| {
            ZippySegmentStoreError::Shmem("shared memory file length overflows usize".to_string())
        })?;
        if len == 0 {
            return Err(ZippySegmentStoreError::Shmem(
                "shared memory file length must be greater than zero".to_string(),
            ));
        }
        Self::map_file(file, path.to_path_buf(), len, false)
    }

    fn map_file(
        file: File,
        path: PathBuf,
        len: usize,
        owner: bool,
    ) -> Result<Self, ZippySegmentStoreError> {
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
            return Err(ZippySegmentStoreError::Shmem(
                std::io::Error::last_os_error().to_string(),
            ));
        }

        Ok(Self {
            _file: file,
            path,
            ptr: ptr.cast::<u8>(),
            len,
            owner,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn len(&self) -> usize {
        self.len
    }

    fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

fn preallocate_file(file: &File, len: usize) -> Result<(), ZippySegmentStoreError> {
    let len = libc::off_t::try_from(len).map_err(|_| {
        ZippySegmentStoreError::Shmem("shared memory file length overflows off_t".to_string())
    })?;
    let result = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, len) };
    if result != 0 {
        return Err(ZippySegmentStoreError::Shmem(format!(
            "failed to preallocate shared memory file reason=[{}]",
            std::io::Error::from_raw_os_error(result)
        )));
    }
    Ok(())
}

impl Drop for MappedFile {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            let _ = unsafe { libc::munmap(self.ptr.cast(), self.len) };
        }
        if self.owner {
            let _ = fs::remove_file(&self.path);
        }
    }
}
