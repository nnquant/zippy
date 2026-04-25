use std::io::Write;
use std::{
    io::{Error, ErrorKind},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use parquet::arrow::ArrowWriter;

use crate::SealedSegmentHandle;

/// 最小持久化队列，占位异步 flush 接口。
#[derive(Debug, Clone)]
pub struct PersistenceQueue {
    sender: crossbeam_channel::Sender<SealedSegmentHandle>,
    receiver: crossbeam_channel::Receiver<SealedSegmentHandle>,
    worker_running: Arc<AtomicBool>,
}

/// 后台 parquet 持久化 worker。
#[derive(Debug)]
pub struct PersistenceWorker {
    stop_sender: crossbeam_channel::Sender<()>,
    completed: crossbeam_channel::Receiver<PersistenceFlushResult>,
    worker_running: Arc<AtomicBool>,
    metrics: Arc<PersistenceWorkerMetrics>,
    join_handle: Option<thread::JoinHandle<()>>,
}

type PersistenceFlushResult = Result<PathBuf, PersistenceFailure>;

/// 持久化失败重试策略。
#[derive(Debug, Clone, Copy)]
pub struct PersistenceRetryPolicy {
    pub max_attempts: usize,
    pub backoff: Duration,
}

impl Default for PersistenceRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff: Duration::from_millis(0),
        }
    }
}

/// 持久化 worker 指标快照。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PersistenceWorkerMetricsSnapshot {
    completed_flushes: u64,
    failed_flushes: u64,
    retry_attempts: u64,
    dead_lettered_segments: u64,
}

#[derive(Debug, Default)]
struct PersistenceWorkerMetrics {
    completed_flushes: AtomicU64,
    failed_flushes: AtomicU64,
    retry_attempts: AtomicU64,
    dead_lettered_segments: AtomicU64,
}

/// 单个 sealed segment 持久化失败事件。
#[derive(Debug, Clone)]
pub struct PersistenceFailure {
    sealed: SealedSegmentHandle,
    persistence_key: String,
    segment_id: u64,
    target_path: PathBuf,
    error: &'static str,
    attempts: usize,
}

/// 后台持久化 worker 关闭后的 flush 汇总。
#[derive(Debug, Default)]
pub struct PersistenceWorkerShutdownReport {
    completed_paths: Vec<PathBuf>,
    failed_segments: Vec<PersistenceFailure>,
    metrics: PersistenceWorkerMetricsSnapshot,
}

impl PersistenceQueue {
    /// 创建新的持久化队列。
    pub fn new() -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        Self {
            sender,
            receiver,
            worker_running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 入队一个 sealed segment。
    pub fn enqueue(&self, segment: SealedSegmentHandle) -> Result<(), &'static str> {
        self.sender.send(segment).map_err(|_| "send failed")
    }

    /// 启动后台 worker，异步消费 sealed segment 并写入 parquet。
    pub fn start_worker(&self, output_dir: PathBuf) -> Result<PersistenceWorker, std::io::Error> {
        self.start_worker_with_policy(output_dir, PersistenceRetryPolicy::default())
    }

    /// 按指定策略启动后台 worker，异步消费 sealed segment 并写入 parquet。
    pub fn start_worker_with_policy(
        &self,
        output_dir: PathBuf,
        policy: PersistenceRetryPolicy,
    ) -> Result<PersistenceWorker, std::io::Error> {
        self.worker_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::new(ErrorKind::AlreadyExists, "persistence worker is running"))?;
        if let Err(error) = std::fs::create_dir_all(&output_dir) {
            self.worker_running.store(false, Ordering::Release);
            return Err(error);
        }

        let receiver = self.receiver.clone();
        let worker_running = self.worker_running.clone();
        let metrics = Arc::new(PersistenceWorkerMetrics::default());
        let worker_metrics = metrics.clone();
        let (stop_sender, stop_receiver) = crossbeam_channel::bounded(1);
        let (completed_sender, completed) = crossbeam_channel::unbounded();
        let join_handle = thread::spawn(move || loop {
            crossbeam_channel::select! {
                recv(stop_receiver) -> _ => {
                    while let Ok(sealed) = receiver.try_recv() {
                        let result = flush_with_retry(sealed, &output_dir, policy, &worker_metrics);
                        let _ = completed_sender.send(result);
                    }
                    break;
                },
                recv(receiver) -> message => {
                    let Ok(sealed) = message else {
                        break;
                    };
                    let result = flush_with_retry(sealed, &output_dir, policy, &worker_metrics);
                    let _ = completed_sender.send(result);
                }
            }
        });

        Ok(PersistenceWorker {
            stop_sender,
            completed,
            worker_running,
            metrics,
            join_handle: Some(join_handle),
        })
    }

    /// 仅用于测试：同步消费一条并写出 parquet。
    pub fn flush_one_for_test(&self) -> Result<PathBuf, &'static str> {
        self.ensure_sync_flush_allowed()?;
        let sealed = self.receiver.recv().map_err(|_| "recv failed")?;
        flush_sealed_for_test(sealed)
    }

    /// 仅用于测试：在给定超时内消费一条并写出 parquet。
    pub fn flush_one_timeout_for_test(
        &self,
        timeout: Duration,
    ) -> Result<Option<PathBuf>, &'static str> {
        self.ensure_sync_flush_allowed()?;
        let sealed = match self.receiver.recv_timeout(timeout) {
            Ok(sealed) => sealed,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => return Ok(None),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return Err("recv failed"),
        };
        flush_sealed_for_test(sealed).map(Some)
    }

    fn ensure_sync_flush_allowed(&self) -> Result<(), &'static str> {
        if self.worker_running.load(Ordering::Acquire) {
            return Err("persistence worker is running");
        }
        Ok(())
    }
}

impl PersistenceWorker {
    /// 停止 worker，并在退出前 drain 已入队的 sealed segments。
    pub fn shutdown(mut self) -> PersistenceWorkerShutdownReport {
        self.stop_and_join();
        self.collect_shutdown_report()
    }

    /// 仅用于测试：等待后台 worker 完成一次 parquet flush。
    pub fn recv_flushed_path_timeout_for_test(
        &self,
        timeout: Duration,
    ) -> Result<Option<PathBuf>, &'static str> {
        match self.completed.recv_timeout(timeout) {
            Ok(Ok(path)) => Ok(Some(path)),
            Ok(Err(error)) => Err(error.error()),
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => Ok(None),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                Err("persistence worker stopped")
            }
        }
    }

    /// 返回 worker 当前指标快照。
    pub fn metrics_snapshot(&self) -> PersistenceWorkerMetricsSnapshot {
        self.metrics.snapshot()
    }

    fn stop_and_join(&mut self) {
        if let Some(join_handle) = self.join_handle.take() {
            let _ = self.stop_sender.send(());
            let _ = join_handle.join();
        }
        self.worker_running.store(false, Ordering::Release);
    }

    fn collect_shutdown_report(&self) -> PersistenceWorkerShutdownReport {
        let mut report = PersistenceWorkerShutdownReport::default();
        for result in self.completed.try_iter() {
            match result {
                Ok(path) => report.completed_paths.push(path),
                Err(error) => report.failed_segments.push(error),
            }
        }
        report.metrics = self.metrics.snapshot();
        report
    }
}

impl PersistenceWorkerMetricsSnapshot {
    /// 返回成功 flush 数。
    pub fn completed_flushes(&self) -> u64 {
        self.completed_flushes
    }

    /// 返回失败 flush 数。
    pub fn failed_flushes(&self) -> u64 {
        self.failed_flushes
    }

    /// 返回 retry 次数。
    pub fn retry_attempts(&self) -> u64 {
        self.retry_attempts
    }

    /// 返回 dead-letter segment 数。
    pub fn dead_lettered_segments(&self) -> u64 {
        self.dead_lettered_segments
    }
}

impl PersistenceWorkerMetrics {
    fn snapshot(&self) -> PersistenceWorkerMetricsSnapshot {
        PersistenceWorkerMetricsSnapshot {
            completed_flushes: self.completed_flushes.load(Ordering::Relaxed),
            failed_flushes: self.failed_flushes.load(Ordering::Relaxed),
            retry_attempts: self.retry_attempts.load(Ordering::Relaxed),
            dead_lettered_segments: self.dead_lettered_segments.load(Ordering::Relaxed),
        }
    }
}

impl PersistenceFailure {
    /// 返回失败 segment 的持久化 key。
    pub fn persistence_key(&self) -> &str {
        &self.persistence_key
    }

    /// 返回失败 segment id。
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// 返回本次 flush 的目标 parquet 路径。
    pub fn target_path(&self) -> &Path {
        &self.target_path
    }

    /// 返回失败原因。
    pub fn error(&self) -> &'static str {
        self.error
    }

    /// 返回本次失败前已尝试 flush 的次数。
    pub fn attempts(&self) -> usize {
        self.attempts
    }

    /// 将失败 segment 重新写入指定目录。
    pub fn retry_to_dir(
        &self,
        output_dir: impl AsRef<Path>,
    ) -> Result<PathBuf, PersistenceFailure> {
        flush_sealed_to_dir(self.sealed.clone(), output_dir.as_ref())
    }
}

impl PersistenceWorkerShutdownReport {
    /// 返回成功 flush 的 parquet 路径。
    pub fn completed_paths(&self) -> &[PathBuf] {
        &self.completed_paths
    }

    /// 返回 flush 失败原因。
    pub fn errors(&self) -> Vec<&'static str> {
        self.failed_segments
            .iter()
            .map(PersistenceFailure::error)
            .collect()
    }

    /// 返回 shutdown 时的 worker 指标快照。
    pub fn metrics(&self) -> PersistenceWorkerMetricsSnapshot {
        self.metrics
    }

    /// 返回结构化 flush 失败事件。
    pub fn failed_segments(&self) -> &[PersistenceFailure] {
        &self.failed_segments
    }
}

impl Drop for PersistenceWorker {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

fn flush_sealed_for_test(sealed: SealedSegmentHandle) -> Result<PathBuf, &'static str> {
    flush_sealed_to_dir(sealed, &std::env::temp_dir()).map_err(|failure| failure.error())
}

fn flush_with_retry(
    sealed: SealedSegmentHandle,
    output_dir: &Path,
    policy: PersistenceRetryPolicy,
    metrics: &PersistenceWorkerMetrics,
) -> PersistenceFlushResult {
    let max_attempts = policy.max_attempts.max(1);
    let mut last_failure = None;

    for attempt in 1..=max_attempts {
        match flush_sealed_to_dir(sealed.clone(), output_dir) {
            Ok(path) => {
                metrics.completed_flushes.fetch_add(1, Ordering::Relaxed);
                return Ok(path);
            }
            Err(failure) => {
                last_failure = Some(failure);
                if attempt < max_attempts {
                    metrics.retry_attempts.fetch_add(1, Ordering::Relaxed);
                    if !policy.backoff.is_zero() {
                        thread::sleep(policy.backoff);
                    }
                }
            }
        }
    }

    metrics.failed_flushes.fetch_add(1, Ordering::Relaxed);
    let mut failure = last_failure.expect("max_attempts is at least one");
    failure.attempts = max_attempts;
    write_dead_letter(output_dir, &failure);
    metrics
        .dead_lettered_segments
        .fetch_add(1, Ordering::Relaxed);
    Err(failure)
}

fn flush_sealed_to_dir(sealed: SealedSegmentHandle, output_dir: &Path) -> PersistenceFlushResult {
    let target_path = parquet_path_for_segment(&sealed, output_dir);
    let batch = sealed
        .as_record_batch()
        .map_err(|_| failure_for_segment(&sealed, target_path.clone(), "batch export failed"))?;
    std::fs::create_dir_all(output_dir).map_err(|_| {
        failure_for_segment(&sealed, target_path.clone(), "create parquet dir failed")
    })?;
    let temp_path = temp_parquet_path_for_target(&target_path);
    let file = std::fs::File::create(&temp_path)
        .map_err(|_| failure_for_segment(&sealed, target_path.clone(), "create parquet failed"))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)
        .map_err(|_| failure_for_segment(&sealed, target_path.clone(), "writer init failed"))?;
    writer
        .write(&batch)
        .map_err(|_| failure_for_segment(&sealed, target_path.clone(), "write parquet failed"))?;
    writer
        .close()
        .map_err(|_| failure_for_segment(&sealed, target_path.clone(), "close parquet failed"))?;
    if std::fs::rename(&temp_path, &target_path).is_err() {
        let _ = std::fs::remove_file(&temp_path);
        return Err(failure_for_segment(
            &sealed,
            target_path,
            "rename parquet failed",
        ));
    }
    Ok(target_path)
}

fn parquet_path_for_segment(sealed: &SealedSegmentHandle, output_dir: &Path) -> PathBuf {
    output_dir.join(format!(
        "segment-{}-{}.parquet",
        sanitize_path_component(sealed.persistence_key()),
        sealed.segment_id()
    ))
}

fn temp_parquet_path_for_target(target_path: &Path) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let file_name = target_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("segment.parquet");
    target_path.with_file_name(format!("{file_name}.tmp-{}-{}", std::process::id(), suffix))
}

fn failure_for_segment(
    sealed: &SealedSegmentHandle,
    target_path: PathBuf,
    error: &'static str,
) -> PersistenceFailure {
    PersistenceFailure {
        sealed: sealed.clone(),
        persistence_key: sealed.persistence_key().to_string(),
        segment_id: sealed.segment_id(),
        target_path,
        error,
        attempts: 1,
    }
}

fn write_dead_letter(output_dir: &Path, failure: &PersistenceFailure) {
    let path = output_dir.join("dead-letter.log");
    let line = format!(
        "persistence_key={} segment_id={} attempts={} target_path={} error={}\n",
        failure.persistence_key(),
        failure.segment_id(),
        failure.attempts(),
        failure.target_path().display(),
        failure.error(),
    );
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut file| file.write_all(line.as_bytes()));
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

impl Default for PersistenceQueue {
    fn default() -> Self {
        Self::new()
    }
}
