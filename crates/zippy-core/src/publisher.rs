use arrow::record_batch::RecordBatch;

use crate::Result;

pub trait Publisher: Send + 'static {
    fn publish(&mut self, batch: &RecordBatch) -> Result<()>;

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

    fn flush(&mut self) -> Result<()> {
        (**self).flush()
    }

    fn close(&mut self) -> Result<()> {
        (**self).close()
    }
}
