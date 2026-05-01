use arrow::record_batch::RecordBatch;
use zippy_core::{Publisher, Result, SegmentTableView};

#[derive(Default)]
pub struct NullPublisher {
    published_batches: usize,
}

impl NullPublisher {
    pub fn published_batches(&self) -> usize {
        self.published_batches
    }
}

impl Publisher for NullPublisher {
    fn publish(&mut self, _batch: &RecordBatch) -> Result<()> {
        self.published_batches += 1;
        Ok(())
    }

    fn publish_table(&mut self, _table: &SegmentTableView) -> Result<()> {
        self.published_batches += 1;
        Ok(())
    }
}

#[derive(Default)]
pub struct FanoutPublisher {
    publishers: Vec<Box<dyn Publisher>>,
}

impl FanoutPublisher {
    pub fn new(publishers: Vec<Box<dyn Publisher>>) -> Self {
        Self { publishers }
    }

    pub fn len(&self) -> usize {
        self.publishers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.publishers.is_empty()
    }
}

impl Publisher for FanoutPublisher {
    fn publish(&mut self, batch: &RecordBatch) -> Result<()> {
        for publisher in &mut self.publishers {
            publisher.publish(batch)?;
        }

        Ok(())
    }

    fn publish_table(&mut self, table: &SegmentTableView) -> Result<()> {
        for publisher in &mut self.publishers {
            publisher.publish_table(table)?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        for publisher in &mut self.publishers {
            publisher.flush()?;
        }

        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        for publisher in &mut self.publishers {
            publisher.close()?;
        }

        Ok(())
    }
}
