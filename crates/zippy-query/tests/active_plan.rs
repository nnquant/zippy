use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::memory::MemoryStream;
use zippy_query::{
    next_persisted_batch, prepare_persisted_plan, profile_persisted_plan, ActiveSegmentDescription,
    ActiveSegmentKey, ActiveSegmentOpenRequest, ActiveSegmentOpener, ActiveSegmentRole,
    BoxedAsyncFileReader, PersistedFileOpenRequest, PersistedFileOpener, PersistedPlanLimits,
    PersistedSnapshotDescription, PersistedTableBinding, PersistedTableDescription,
    QuerySessionPolicy,
};
use zippy_types::table::SourceSegmentIdentity;
use zippy_types::{ErrorCode, LogOffset, OffsetRange, Result, ServerError, TableId};

fn nonzero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

fn policy() -> QuerySessionPolicy {
    QuerySessionPolicy::new(NonZeroUsize::new(1).unwrap())
        .with_batch_size(NonZeroUsize::new(2).unwrap())
}

fn limits() -> PersistedPlanLimits {
    PersistedPlanLimits::try_new(4_096, 4, 4, 128, 128, 512).unwrap()
}

fn active_snapshot() -> (PersistedSnapshotDescription, RecordBatch) {
    let table_id = TableId::from_u128(10).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, false),
        Field::new("volume", DataType::Int64, false),
    ]));
    let offsets = OffsetRange::new(LogOffset::new(10), LogOffset::new(12)).unwrap();
    let active = ActiveSegmentDescription::try_new(
        ActiveSegmentKey::new(
            0,
            0,
            table_id,
            SourceSegmentIdentity {
                segment_id: 7,
                generation: 2,
                writer_epoch: 3,
            },
        ),
        ActiveSegmentRole::Active,
        nonzero(5),
        nonzero(2),
        offsets,
        nonzero(3),
    )
    .unwrap();
    let table = PersistedTableDescription::try_new_with_active_segments(
        0,
        table_id,
        nonzero(5),
        nonzero(2),
        Arc::clone(&schema),
        Vec::new(),
        vec![active],
    )
    .unwrap();
    let snapshot = PersistedSnapshotDescription::try_new(vec![table]).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
        ],
    )
    .unwrap();
    (snapshot, batch)
}

struct NoFileOpener {
    snapshot: PersistedSnapshotDescription,
}

impl PersistedFileOpener for NoFileOpener {
    fn snapshot_description(&self) -> &PersistedSnapshotDescription {
        &self.snapshot
    }

    fn open_reader(&self, _request: PersistedFileOpenRequest) -> Result<BoxedAsyncFileReader> {
        Err(ServerError::new(
            ErrorCode::Internal,
            "active-only test unexpectedly opened a persisted file",
        ))
    }
}

struct BatchActiveOpener {
    snapshot: PersistedSnapshotDescription,
    batch: RecordBatch,
    opens: Arc<AtomicUsize>,
}

impl ActiveSegmentOpener for BatchActiveOpener {
    fn snapshot_description(&self) -> &PersistedSnapshotDescription {
        &self.snapshot
    }

    fn open_segments(
        &self,
        request: ActiveSegmentOpenRequest,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let expected = self.snapshot.table(0).unwrap().active_segments();
        if request.descriptions() != expected {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                "active test request differs from its snapshot",
            ));
        }
        self.opens.fetch_add(1, Ordering::Relaxed);
        let batch = request
            .projection()
            .map_or_else(
                || Ok(self.batch.clone()),
                |projection| self.batch.project(projection),
            )
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Internal,
                    format!("active test projection failed error=[{error}]"),
                )
            })?;
        let schema = batch.schema();
        let stream = MemoryStream::try_new(vec![batch], schema, None).map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("active test stream failed error=[{error}]"),
            )
        })?;
        Ok(Box::pin(stream))
    }
}

#[tokio::test]
async fn active_scan_supports_fixed_width_filter_projection_and_limit() {
    let (snapshot, batch) = active_snapshot();
    let bindings = vec![PersistedTableBinding::try_new("ticks", 0).unwrap()];
    let sql = "select volume from ticks where price >= 2 limit 1";
    let profile = profile_persisted_plan(sql, &bindings, &snapshot, limits()).unwrap();
    let prepared = prepare_persisted_plan(policy(), sql, &bindings, &snapshot, profile)
        .await
        .unwrap();
    let file_opener: Arc<dyn PersistedFileOpener> = Arc::new(NoFileOpener {
        snapshot: snapshot.clone(),
    });
    let opens = Arc::new(AtomicUsize::new(0));
    let active_opener: Arc<dyn ActiveSegmentOpener> = Arc::new(BatchActiveOpener {
        snapshot: snapshot.clone(),
        batch,
        opens: Arc::clone(&opens),
    });
    let bound = prepared
        .bind_with_active(&file_opener, &active_opener)
        .unwrap();
    assert_eq!(bound.bound_scan_count(), 0);
    assert_eq!(bound.bound_active_scan_count(), 1);
    let admission = bound
        .streaming_output_admission(policy().batch_size())
        .unwrap();
    assert!(admission.active_decode_memory_bytes().unwrap().get() > 64 * 1024);

    let execution = policy()
        .create_execution_context(NonZeroU64::new(4 * 1024 * 1024).unwrap())
        .unwrap();
    let mut stream = bound.execute_stream(&execution).unwrap();
    let result = next_persisted_batch(&mut stream).await.unwrap().unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.num_columns(), 1);
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        20
    );
    assert!(next_persisted_batch(&mut stream).await.unwrap().is_none());
    assert_eq!(opens.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn active_plan_clone_cannot_outlive_its_weak_execution_authority() {
    let (snapshot, batch) = active_snapshot();
    let bindings = vec![PersistedTableBinding::try_new("ticks", 0).unwrap()];
    let sql = "select price from ticks";
    let profile = profile_persisted_plan(sql, &bindings, &snapshot, limits()).unwrap();
    let prepared = prepare_persisted_plan(policy(), sql, &bindings, &snapshot, profile)
        .await
        .unwrap();
    let file_opener: Arc<dyn PersistedFileOpener> = Arc::new(NoFileOpener {
        snapshot: snapshot.clone(),
    });
    let active_opener: Arc<dyn ActiveSegmentOpener> = Arc::new(BatchActiveOpener {
        snapshot,
        batch,
        opens: Arc::new(AtomicUsize::new(0)),
    });
    let bound = prepared
        .bind_with_active(&file_opener, &active_opener)
        .unwrap();
    drop(active_opener);
    drop(file_opener);

    let execution = policy()
        .create_execution_context(NonZeroU64::new(4 * 1024 * 1024).unwrap())
        .unwrap();
    let error = match bound.execute_stream(&execution) {
        Ok(_) => panic!("expired active execution authority must reject execution"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("execution lease has expired"));
}
