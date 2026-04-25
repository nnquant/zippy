use std::collections::VecDeque;

/// 增量维护滚动均值的最小状态。
pub struct RollingMeanState {
    window: usize,
    values: VecDeque<f64>,
    sum: f64,
}

impl RollingMeanState {
    /// 构造指定窗口长度的状态。
    pub fn new(window: usize) -> Self {
        assert!(window > 0, "rolling mean window must be greater than zero");
        Self {
            window,
            values: VecDeque::new(),
            sum: 0.0,
        }
    }

    /// 推入一个新值。
    pub fn push(&mut self, value: f64) {
        self.values.push_back(value);
        self.sum += value;
        if self.values.len() > self.window {
            self.sum -= self.values.pop_front().unwrap();
        }
    }

    /// 返回当前窗口均值。
    pub fn mean(&self) -> Option<f64> {
        if self.values.is_empty() {
            None
        } else {
            Some(self.sum / self.values.len() as f64)
        }
    }
}
