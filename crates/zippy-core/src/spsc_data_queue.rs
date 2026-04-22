use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

use crate::{Result, ZippyError};

/// 固定容量的单生产者单消费者数据队列。
///
/// 队列保证先进先出语义，写入在队列满时阻塞，读取在队列空时立即返回 `None`。
pub struct SpscDataQueue<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    state: Mutex<State<T>>,
    not_empty: Condvar,
    not_full: Condvar,
}

struct State<T> {
    items: VecDeque<T>,
    capacity: usize,
}

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

        Ok(Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    items: VecDeque::with_capacity(capacity),
                    capacity,
                }),
                not_empty: Condvar::new(),
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
        let mut state = self.inner.state.lock().unwrap();

        while state.items.len() >= state.capacity {
            state = self.inner.not_full.wait(state).unwrap();
        }

        state.items.push_back(value);
        self.inner.not_empty.notify_one();
        Ok(())
    }

    /// 尝试弹出一个元素。
    ///
    /// :returns: 队列非空时返回首个元素，否则返回 `None`。
    /// :rtype: Option<T>
    pub fn try_pop(&self) -> Option<T> {
        let mut state = self.inner.state.lock().unwrap();
        let value = state.items.pop_front();
        if value.is_some() {
            self.inner.not_full.notify_one();
        }
        value
    }

    /// 判断队列是否为空。
    ///
    /// :returns: 队列为空时返回 `true`，否则返回 `false`。
    /// :rtype: bool
    pub fn is_empty(&self) -> bool {
        self.inner.state.lock().unwrap().items.is_empty()
    }
}
