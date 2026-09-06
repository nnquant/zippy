//! 有界快速数据队列，满载时阻塞，关闭时唤醒所有等待中的写者。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};

use crate::{Result, ZippyError};

/// 保留 SPSC 使用接口的有界队列，也支持安全的并发调用。
///
/// 非满写入和非空读取复用 crossbeam 的原子数组队列。只有阻塞写入才注册
/// 等待；关闭信号使用独立通道，避免检查队列状态与进入等待之间丢失唤醒。
pub struct SpscDataQueue<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
    closed: AtomicBool,
    close_sender: Mutex<Option<Sender<()>>>,
    close_receiver: Receiver<()>,
}

impl<T> SpscDataQueue<T> {
    /// 创建固定容量队列。
    ///
    /// :param capacity: 队列容量，必须大于零。
    /// :returns: 空队列。
    /// :raises ZippyError::InvalidConfig: 容量为零。
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "capacity must be greater than zero".to_string(),
            });
        }
        let (sender, receiver) = bounded(capacity);
        let (close_sender, close_receiver) = bounded(0);
        Ok(Self {
            sender,
            receiver,
            closed: AtomicBool::new(false),
            close_sender: Mutex::new(Some(close_sender)),
            close_receiver,
        })
    }

    /// 写入一个元素，队列满时等待空位或关闭信号。
    ///
    /// :param value: 待写入的元素。
    /// :returns: 写入成功返回 ``Ok(())``。
    /// :raises ZippyError::QueueClosed: 队列已关闭。
    pub fn push_blocking(&self, value: T) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ZippyError::QueueClosed);
        }
        let value = match self.sender.try_send(value) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Full(value)) => value,
            Err(TrySendError::Disconnected(_)) => return Err(ZippyError::QueueClosed),
        };
        crossbeam_channel::select! {
            send(self.sender, value) -> result => result.map_err(|_| ZippyError::QueueClosed),
            recv(self.close_receiver) -> _ => Err(ZippyError::QueueClosed),
        }
    }

    /// 非阻塞读取队首元素；关闭后仍可排空已经接收的数据。
    ///
    /// :returns: 队首元素，队列为空时返回 ``None``。
    pub fn try_pop(&self) -> Option<T> {
        self.receiver.try_recv().ok()
    }

    /// 关闭队列并唤醒所有阻塞写者；可重复调用。
    ///
    /// 与关闭并发的在途写入可能成功，关闭返回后的新写入均被拒绝。
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        // Dropping the sole sender makes every current and future close wait
        // immediately ready, unlike a single consumable notification.
        self.close_sender.lock().unwrap().take();
    }

    /// 返回队列当前是否为空。
    ///
    /// :returns: 队列为空时返回 ``true``。
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }
}
