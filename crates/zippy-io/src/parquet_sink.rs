use std::fs::{create_dir_all, File};
use std::path::PathBuf;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use zippy_core::{Result, ZippyError};

pub struct ParquetSink {
    root: PathBuf,
}

pub struct ParquetSinkWriter {
    writer: Option<ArrowWriter<File>>,
    schema: SchemaRef,
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

    pub fn create_writer(&self, file_name: &str, schema: SchemaRef) -> Result<ParquetSinkWriter> {
        create_dir_all(&self.root).map_err(|error| ZippyError::Io {
            reason: format!("failed to create parquet directory error=[{}]", error),
        })?;

        let file = File::create(self.root.join(file_name)).map_err(|error| ZippyError::Io {
            reason: format!("failed to create parquet file error=[{}]", error),
        })?;
        let writer = ArrowWriter::try_new(file, schema.clone(), None).map_err(|error| {
            ZippyError::Io {
                reason: format!("failed to create parquet writer error=[{}]", error),
            }
        })?;

        Ok(ParquetSinkWriter {
            writer: Some(writer),
            schema,
        })
    }
}

impl ParquetSinkWriter {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let Some(writer) = self.writer.as_mut() else {
            return Err(ZippyError::InvalidState {
                status: "parquet sink writer closed",
            });
        };

        writer.write(batch).map_err(|error| ZippyError::Io {
            reason: format!("failed to write parquet batch error=[{}]", error),
        })?;
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };

        writer.close().map_err(|error| ZippyError::Io {
            reason: format!("failed to close parquet writer error=[{}]", error),
        })?;
        Ok(())
    }
}
