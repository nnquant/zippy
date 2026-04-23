use std::path::PathBuf;

use parquet::arrow::ArrowWriter;

use crate::SealedSegmentHandle;

/// 最小持久化队列，占位异步 flush 接口。
#[derive(Debug, Clone)]
pub struct PersistenceQueue {
    sender: crossbeam_channel::Sender<SealedSegmentHandle>,
    receiver: crossbeam_channel::Receiver<SealedSegmentHandle>,
}

impl PersistenceQueue {
    /// 创建新的持久化队列。
    pub fn new() -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        Self { sender, receiver }
    }

    /// 入队一个 sealed segment。
    pub fn enqueue(&self, segment: SealedSegmentHandle) -> Result<(), &'static str> {
        self.sender.send(segment).map_err(|_| "send failed")
    }

    /// 仅用于测试：同步消费一条并写出 parquet。
    pub fn flush_one_for_test(&self) -> Result<PathBuf, &'static str> {
        let sealed = self.receiver.recv().map_err(|_| "recv failed")?;
        let batch = sealed
            .as_record_batch()
            .map_err(|_| "batch export failed")?;
        let path = std::env::temp_dir().join(format!(
            "segment-{}-{}.parquet",
            sanitize_path_component(sealed.persistence_key()),
            sealed.segment_id()
        ));
        let file = std::fs::File::create(&path).map_err(|_| "create parquet failed")?;
        let mut writer =
            ArrowWriter::try_new(file, batch.schema(), None).map_err(|_| "writer init failed")?;
        writer.write(&batch).map_err(|_| "write parquet failed")?;
        writer.close().map_err(|_| "close parquet failed")?;
        Ok(path)
    }
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
