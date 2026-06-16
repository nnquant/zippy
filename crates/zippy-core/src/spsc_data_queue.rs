use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use crate::{Result, ZippyError};

/// 固定容量的单生产者单消费者数据队列。
///
/// 队列保证先进先出语义，写入在队列满时阻塞，读取在队列空时立即返回 `None`。
/// 非满写入和非空读取走原子 SPSC ring fast path；只有队列满时生产者才进入等待路径。
pub struct SpscDataQueue<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    slots: Box<[Slot<T>]>,
    capacity: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
    closed: AtomicBool,
    full_wait: Mutex<()>,
    not_full: Condvar,
}

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Send> Sync for Slot<T> {}
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> SpscDataQueue<T> {
    /// 创建一个固定容量队列。
    ///
    /// :param capacity: 队列容量，必须大于零。
    /// :type capacity: usize
    /// :returns: 成功时返回可共享的队列实例。
    /// :rtype: crate::Result<Self>
    /// :raises ZippyError::InvalidConfig: 当容量为零时返回错误。
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "capacity must be greater than zero".to_string(),
            });
        }

        let slots = (0..capacity)
            .map(|_| Slot {
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Ok(Self {
            inner: Arc::new(Inner {
                slots,
                capacity,
                head: AtomicUsize::new(0),
                tail: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
                full_wait: Mutex::new(()),
                not_full: Condvar::new(),
            }),
        })
    }

    /// 阻塞写入一个元素。
    ///
    /// :param value: 待写入的值。
    /// :type value: T
    /// :returns: 写入成功后返回 `Ok(())`。
    /// :rtype: crate::Result<()>
    pub fn push_blocking(&self, value: T) -> Result<()> {
        let mut value = Some(value);

        loop {
            if self.inner.closed.load(Ordering::Acquire) {
                return Err(ZippyError::QueueClosed);
            }

            match self.inner.try_push(value.take().unwrap()) {
                Ok(()) => return Ok(()),
                Err(pending) => {
                    value = Some(pending);
                }
            }

            let mut guard = self.inner.full_wait.lock().unwrap();
            while self.inner.is_full() && !self.inner.closed.load(Ordering::Acquire) {
                guard = self.inner.not_full.wait(guard).unwrap();
            }
        }
    }

    /// 尝试弹出一个元素。
    ///
    /// :returns: 队列非空时返回首个元素，否则返回 `None`。
    /// :rtype: Option<T>
    pub fn try_pop(&self) -> Option<T> {
        let value = self.inner.try_pop();
        if value.is_some() {
            self.inner.not_full.notify_one();
        }
        value
    }

    /// 关闭队列并唤醒等待中的生产者和消费者。
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.not_full.notify_all();
    }

    /// 判断队列是否为空。
    ///
    /// :returns: 队列为空时返回 `true`，否则返回 `false`。
    /// :rtype: bool
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T> Inner<T> {
    fn try_push(&self, value: T) -> std::result::Result<(), T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        if tail.wrapping_sub(head) >= self.capacity {
            return Err(value);
        }

        let index = tail % self.capacity;
        unsafe {
            (*self.slots[index].value.get()).write(value);
        }
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    fn try_pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        if head == tail {
            return None;
        }

        let index = head % self.capacity;
        let value = unsafe { (*self.slots[index].value.get()).assume_init_read() };
        self.head.store(head.wrapping_add(1), Ordering::Release);
        Some(value)
    }

    fn is_full(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        tail.wrapping_sub(head) >= self.capacity
    }

    fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Acquire)
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        while self.try_pop().is_some() {}
    }
}
