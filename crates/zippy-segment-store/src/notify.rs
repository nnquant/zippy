use std::{
    collections::HashMap,
    os::fd::{AsFd, OwnedFd},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use rustix::io;

/// 基于 eventfd 的最小通知器。
#[derive(Debug)]
pub struct SegmentNotifier {
    fd: OwnedFd,
    subscription_id: u64,
    subscriptions: Weak<Mutex<HashMap<u64, OwnedFd>>>,
}

impl SegmentNotifier {
    /// 创建新的通知器。
    fn new(
        subscription_id: u64,
        subscriptions: Weak<Mutex<HashMap<u64, OwnedFd>>>,
    ) -> std::io::Result<Self> {
        let fd = rustix::event::eventfd(0, rustix::event::EventfdFlags::CLOEXEC)?;
        Ok(Self {
            fd,
            subscription_id,
            subscriptions,
        })
    }

    /// 返回可供 broadcaster 保存的文件描述符副本。
    pub(crate) fn try_clone_fd(&self) -> std::io::Result<OwnedFd> {
        self.fd.as_fd().try_clone_to_owned()
    }

    /// 发送一次通知。
    pub fn notify(&self) -> std::io::Result<()> {
        write_eventfd(&self.fd, 1)?;
        Ok(())
    }

    /// 在超时内等待通知。
    pub fn wait_timeout(&self, timeout: Duration) -> std::io::Result<bool> {
        let mut fds = [rustix::event::PollFd::new(
            &self.fd,
            rustix::event::PollFlags::IN,
        )];
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let ready = rustix::event::poll(&mut fds, timeout_ms)?;
        if ready == 0 {
            return Ok(false);
        }
        let _ = read_eventfd(&self.fd)?;
        Ok(true)
    }
}

impl Drop for SegmentNotifier {
    fn drop(&mut self) {
        let Some(subscriptions) = self.subscriptions.upgrade() else {
            return;
        };
        subscriptions.lock().unwrap().remove(&self.subscription_id);
    }
}

/// 维护一组订阅者。
#[derive(Debug)]
pub(crate) struct SegmentBroadcaster {
    next_subscription_id: std::sync::atomic::AtomicU64,
    subscribers: Arc<Mutex<HashMap<u64, OwnedFd>>>,
}

impl Default for SegmentBroadcaster {
    fn default() -> Self {
        Self {
            next_subscription_id: std::sync::atomic::AtomicU64::new(1),
            subscribers: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl SegmentBroadcaster {
    /// 注册新的 eventfd 订阅者。
    pub(crate) fn subscribe(&self) -> std::io::Result<SegmentNotifier> {
        let subscription_id = self
            .next_subscription_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let notifier = SegmentNotifier::new(subscription_id, Arc::downgrade(&self.subscribers))?;
        self.subscribers
            .lock()
            .unwrap()
            .insert(subscription_id, notifier.try_clone_fd()?);
        Ok(notifier)
    }

    /// 广播一次事件给所有订阅者。
    pub(crate) fn notify_all(&self) -> std::io::Result<()> {
        let subscribers = self.subscribers.lock().unwrap();
        for fd in subscribers.values() {
            write_eventfd(fd, 1)?;
        }
        Ok(())
    }
}

fn write_eventfd(fd: &OwnedFd, value: u64) -> std::io::Result<()> {
    let bytes = value.to_ne_bytes();
    let written = io::write(fd, &bytes)?;
    if written == bytes.len() {
        return Ok(());
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::WriteZero,
        "short write to eventfd",
    ))
}

fn read_eventfd(fd: &OwnedFd) -> std::io::Result<u64> {
    let mut bytes = [0_u8; 8];
    let read = io::read(fd, &mut bytes)?;
    if read == bytes.len() {
        return Ok(u64::from_ne_bytes(bytes));
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "short read from eventfd",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn dropping_notifier_removes_subscription() {
        let broadcaster = SegmentBroadcaster::default();
        let notifier = broadcaster.subscribe().unwrap();
        assert_eq!(broadcaster.subscribers.lock().unwrap().len(), 1);

        drop(notifier);

        assert_eq!(broadcaster.subscribers.lock().unwrap().len(), 0);
    }

    #[test]
    fn notify_all_propagates_write_error() {
        let broadcaster = SegmentBroadcaster::default();
        let file = File::open("/dev/null").unwrap();
        let fd: OwnedFd = file.into();
        broadcaster.subscribers.lock().unwrap().insert(1, fd);

        assert!(broadcaster.notify_all().is_err());
    }
}
