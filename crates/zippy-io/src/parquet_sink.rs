use std::fs::{File, create_dir_all};
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use zippy_core::{Result, ZippyError};

pub struct ParquetSink {
    root: PathBuf,
}

impl ParquetSink {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn write_batch(&self, file_name: &str, batch: &RecordBatch) -> Result<()> {
        create_dir_all(&self.root).map_err(|error| ZippyError::Io {
            reason: format!("failed to create parquet directory error=[{}]", error),
        })?;

        let file = File::create(self.root.join(file_name)).map_err(|error| ZippyError::Io {
            reason: format!("failed to create parquet file error=[{}]", error),
        })?;
        let mut writer =
            ArrowWriter::try_new(file, batch.schema(), None).map_err(|error| ZippyError::Io {
                reason: format!("failed to create parquet writer error=[{}]", error),
            })?;
        writer.write(batch).map_err(|error| ZippyError::Io {
            reason: format!("failed to write parquet batch error=[{}]", error),
        })?;
        writer.close().map_err(|error| ZippyError::Io {
            reason: format!("failed to close parquet writer error=[{}]", error),
        })?;
        Ok(())
    }
}
