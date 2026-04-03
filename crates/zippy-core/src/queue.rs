use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

pub struct BoundedQueue<T> {
    sender: QueueSender<T>,
    receiver: Mutex<Option<QueueReceiver<T>>>,
}

pub struct QueueSender<T> {
    inner: Arc<QueueInner<T>>,
}

pub struct QueueReceiver<T> {
    inner: Arc<QueueInner<T>>,
}

struct QueueInner<T> {
    state: Mutex<QueueState<T>>,
    not_empty: Condvar,
    not_full: Condvar,
}

struct QueueState<T> {
    items: VecDeque<T>,
    capacity: usize,
    sender_count: usize,
    receiver_alive: bool,
}

pub enum QueueSendError<T> {
    Full(T),
    Disconnected(T),
}

pub enum QueueRecvError {
    Disconnected,
}

impl<T> BoundedQueue<T> {
    pub fn new(capacity: usize) -> Self {
        let inner = Arc::new(QueueInner {
            state: Mutex::new(QueueState {
                items: VecDeque::with_capacity(capacity),
                capacity,
                sender_count: 1,
                receiver_alive: true,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        });

        Self {
            sender: QueueSender {
                inner: Arc::clone(&inner),
            },
            receiver: Mutex::new(Some(QueueReceiver { inner })),
        }
    }

    pub fn sender(&self) -> QueueSender<T> {
        self.sender.clone()
    }

    pub fn receiver(&self) -> QueueReceiver<T> {
        self.receiver
            .lock()
            .unwrap()
            .take()
            .expect("receiver already taken")
    }

    pub fn len(&self) -> usize {
        self.sender.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }
}

impl<T> Clone for QueueSender<T> {
    fn clone(&self) -> Self {
        let mut state = self.inner.state.lock().unwrap();
        state.sender_count += 1;
        drop(state);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> QueueSender<T> {
    pub fn len(&self) -> usize {
        self.inner.state.lock().unwrap().items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn send(&self, value: T) -> Result<(), QueueSendError<T>> {
        let mut state = self.inner.state.lock().unwrap();
        let mut pending = Some(value);

        loop {
            if !state.receiver_alive {
                return Err(QueueSendError::Disconnected(pending.take().unwrap()));
            }

            if state.items.len() < state.capacity {
                state.items.push_back(pending.take().unwrap());
                self.inner.not_empty.notify_one();
                return Ok(());
            }

            state = self.inner.not_full.wait(state).unwrap();
        }
    }

    pub fn try_send(&self, value: T) -> Result<(), QueueSendError<T>> {
        let mut state = self.inner.state.lock().unwrap();
        if !state.receiver_alive {
            return Err(QueueSendError::Disconnected(value));
        }
        if state.items.len() >= state.capacity {
            return Err(QueueSendError::Full(value));
        }

        state.items.push_back(value);
        self.inner.not_empty.notify_one();
        Ok(())
    }

    pub fn send_drop_oldest<F>(
        &self,
        value: T,
        can_drop: F,
    ) -> Result<Option<T>, QueueSendError<T>>
    where
        F: Fn(&T) -> bool,
    {
        let mut state = self.inner.state.lock().unwrap();
        let mut pending = Some(value);

        loop {
            if !state.receiver_alive {
                return Err(QueueSendError::Disconnected(pending.take().unwrap()));
            }

            if state.items.len() < state.capacity {
                state.items.push_back(pending.take().unwrap());
                self.inner.not_empty.notify_one();
                return Ok(None);
            }

            if let Some(index) = state.items.iter().position(&can_drop) {
                let dropped = state.items.remove(index).unwrap();
                state.items.push_back(pending.take().unwrap());
                self.inner.not_empty.notify_one();
                return Ok(Some(dropped));
            }

            state = self.inner.not_full.wait(state).unwrap();
        }
    }
}

impl<T> Drop for QueueSender<T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.sender_count = state.sender_count.saturating_sub(1);
        drop(state);
        self.inner.not_empty.notify_all();
    }
}

impl<T> QueueReceiver<T> {
    pub fn recv(&self) -> Result<T, QueueRecvError> {
        let mut state = self.inner.state.lock().unwrap();

        loop {
            if let Some(value) = state.items.pop_front() {
                self.inner.not_full.notify_one();
                return Ok(value);
            }

            if state.sender_count == 0 {
                return Err(QueueRecvError::Disconnected);
            }

            state = self.inner.not_empty.wait(state).unwrap();
        }
    }
}

impl<T> Drop for QueueReceiver<T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.receiver_alive = false;
        drop(state);
        self.inner.not_full.notify_all();
    }
}
