use arrow::record_batch::RecordBatch;

use crate::{Result, SegmentTableView};

pub trait Publisher: Send + 'static {
    fn publish_table(&mut self, table: &SegmentTableView) -> Result<()>;

    fn publish(&mut self, batch: &RecordBatch) -> Result<()> {
        self.publish_table(&SegmentTableView::from_record_batch(batch.clone()))
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<P> Publisher for Box<P>
where
    P: Publisher + ?Sized,
{
    fn publish(&mut self, batch: &RecordBatch) -> Result<()> {
        (**self).publish(batch)
    }

    fn publish_table(&mut self, table: &SegmentTableView) -> Result<()> {
        (**self).publish_table(table)
    }

    fn flush(&mut self) -> Result<()> {
        (**self).flush()
    }

    fn close(&mut self) -> Result<()> {
        (**self).close()
    }
}
