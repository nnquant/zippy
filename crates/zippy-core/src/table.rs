use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::record_batch::RecordBatch;
use zippy_segment_store::RowSpanView;

use crate::{Result, ZippyError};

/// 内部 segment 行范围视图。
#[derive(Clone, Debug)]
pub struct SegmentRowView {
    span: RowSpanView,
}

impl SegmentRowView {
    pub fn new(span: RowSpanView) -> Self {
        Self { span }
    }

    pub fn schema(&self) -> ArrowSchemaRef {
        self.span.schema_ref()
    }

    pub fn num_rows(&self) -> usize {
        self.span.end_row() - self.span.start_row()
    }

    pub fn row_span(&self) -> &RowSpanView {
        &self.span
    }

    pub fn column(&self, name: &str) -> Result<ArrayRef> {
        self.span.column(name).map_err(arrow_zippy_error)
    }

    pub fn column_at(&self, index: usize) -> Result<ArrayRef> {
        let schema = self.schema();
        let field = schema
            .fields()
            .get(index)
            .ok_or_else(|| ZippyError::SchemaMismatch {
                reason: format!("table column index out of bounds index=[{}]", index),
            })?;
        self.column(field.name())
    }

    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        self.span.as_record_batch().map_err(arrow_zippy_error)
    }
}

/// 内部上下游 Engine 数据交换表视图。
#[derive(Clone, Debug)]
pub enum SegmentTableView {
    Segment(SegmentRowView),
    Memory(RecordBatch),
}

impl SegmentTableView {
    pub fn from_segment_row_view(row_view: SegmentRowView) -> Self {
        Self::Segment(row_view)
    }

    pub fn from_row_span(span: RowSpanView) -> Self {
        Self::Segment(SegmentRowView::new(span))
    }

    pub fn from_record_batch(batch: RecordBatch) -> Self {
        Self::Memory(batch)
    }

    pub fn schema(&self) -> ArrowSchemaRef {
        match self {
            Self::Segment(row_view) => row_view.schema(),
            Self::Memory(batch) => batch.schema(),
        }
    }

    pub fn num_rows(&self) -> usize {
        match self {
            Self::Segment(row_view) => row_view.num_rows(),
            Self::Memory(batch) => batch.num_rows(),
        }
    }

    pub fn as_segment_row_view(&self) -> Option<&SegmentRowView> {
        match self {
            Self::Segment(row_view) => Some(row_view),
            Self::Memory(_) => None,
        }
    }

    pub fn column(&self, name: &str) -> Result<ArrayRef> {
        match self {
            Self::Segment(row_view) => row_view.column(name),
            Self::Memory(batch) => {
                let index =
                    batch
                        .schema()
                        .index_of(name)
                        .map_err(|_| ZippyError::SchemaMismatch {
                            reason: format!("missing table column column=[{}]", name),
                        })?;
                Ok(Arc::clone(batch.column(index)))
            }
        }
    }

    pub fn column_at(&self, index: usize) -> Result<ArrayRef> {
        match self {
            Self::Segment(row_view) => row_view.column_at(index),
            Self::Memory(batch) => {
                if index >= batch.num_columns() {
                    return Err(ZippyError::SchemaMismatch {
                        reason: format!("table column index out of bounds index=[{}]", index),
                    });
                }
                Ok(Arc::clone(batch.column(index)))
            }
        }
    }

    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        match self {
            Self::Segment(row_view) => row_view.to_record_batch(),
            Self::Memory(batch) => Ok(batch.clone()),
        }
    }
}

fn arrow_zippy_error(error: arrow::error::ArrowError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}
