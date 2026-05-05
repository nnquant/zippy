use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex, Weak},
    time::{Duration, Instant},
};

/// 最小 segment 通知器。
#[derive(Debug)]
pub struct SegmentNotifier {
    state: Arc<NotifyState>,
    subscription_id: u64,
    subscriptions: Weak<Mutex<HashMap<u64, Arc<NotifyState>>>>,
}

impl SegmentNotifier {
    fn new(
        subscription_id: u64,
        subscriptions: Weak<Mutex<HashMap<u64, Arc<NotifyState>>>>,
    ) -> Self {
        Self {
            state: Arc::new(NotifyState::default()),
            subscription_id,
            subscriptions,
        }
    }

    /// 发送一次通知。
    pub fn notify(&self) -> std::io::Result<()> {
        self.state.notify()
    }

    /// 在超时内等待通知。
    pub fn wait_timeout(&self, timeout: Duration) -> std::io::Result<bool> {
        self.state.wait_timeout(timeout)
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
    subscribers: Arc<Mutex<HashMap<u64, Arc<NotifyState>>>>,
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
    /// 注册新的订阅者。
    pub(crate) fn subscribe(&self) -> std::io::Result<SegmentNotifier> {
        let subscription_id = self
            .next_subscription_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let notifier = SegmentNotifier::new(subscription_id, Arc::downgrade(&self.subscribers));
        self.subscribers
            .lock()
            .unwrap()
            .insert(subscription_id, Arc::clone(&notifier.state));
        Ok(notifier)
    }

    /// 广播一次事件给所有订阅者。
    pub(crate) fn notify_all(&self) -> std::io::Result<()> {
        let subscribers = self.subscribers.lock().unwrap();
        for state in subscribers.values() {
            state.notify()?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct NotifyState {
    pending: Mutex<u64>,
    condvar: Condvar,
}

impl NotifyState {
    fn notify(&self) -> std::io::Result<()> {
        let mut pending = self.pending.lock().map_err(poison_error)?;
        *pending = pending.saturating_add(1);
        self.condvar.notify_all();
        Ok(())
    }

    fn wait_timeout(&self, timeout: Duration) -> std::io::Result<bool> {
        let mut pending = self.pending.lock().map_err(poison_error)?;
        if *pending > 0 {
            *pending -= 1;
            return Ok(true);
        }
        let deadline = Instant::now() + timeout;

        loop {
            if *pending > 0 {
                *pending -= 1;
                return Ok(true);
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_pending, wait_result) = self
                .condvar
                .wait_timeout(pending, remaining)
                .map_err(poison_error)?;
            pending = next_pending;
            if wait_result.timed_out() && *pending == 0 {
                return Ok(false);
            }
        }
    }
}

fn poison_error<T>(_: std::sync::PoisonError<T>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, "segment notifier lock poisoned")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_notifier_removes_subscription() {
        let broadcaster = SegmentBroadcaster::default();
        let notifier = broadcaster.subscribe().unwrap();
        assert_eq!(broadcaster.subscribers.lock().unwrap().len(), 1);

        drop(notifier);

        assert_eq!(broadcaster.subscribers.lock().unwrap().len(), 0);
    }

    #[test]
    fn notify_all_wakes_subscription() {
        let broadcaster = SegmentBroadcaster::default();
        let notifier = broadcaster.subscribe().unwrap();

        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(10));
                broadcaster.notify_all().unwrap();
            });

            assert!(notifier.wait_timeout(Duration::from_secs(1)).unwrap());
        });
    }

    #[test]
    fn wait_timeout_returns_false_when_idle() {
        let broadcaster = SegmentBroadcaster::default();
        let notifier = broadcaster.subscribe().unwrap();

        assert!(!notifier.wait_timeout(Duration::from_millis(1)).unwrap());
    }
}
