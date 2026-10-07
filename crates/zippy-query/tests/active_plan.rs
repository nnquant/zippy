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

#[tokio::test]
async fn key_value_view_resolves_latest_events_before_filter_projection_and_limit() {
    use arrow::array::StringArray;
    use zippy_types::ingest::KeyValueMutation;
    use zippy_types::key_value::{change_batch, log_schema};

    let table_id = TableId::from_u128(10).unwrap();
    let logical = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
        ],
        [("source".to_owned(), "query-fixture".to_owned())].into(),
    ));
    let physical = log_schema(&logical, &["key".to_owned()]).unwrap();
    let upserts = RecordBatch::try_new(
        logical.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "a", "c", "d"])),
            Arc::new(Int64Array::from(vec![99, 50, 1, 20, 30])),
        ],
    )
    .unwrap();
    let deletes = RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            vec![logical.field(0).clone()],
            logical.metadata().clone(),
        )),
        vec![Arc::new(StringArray::from(vec!["b", "missing"]))],
    )
    .unwrap();
    let events = [
        change_batch(
            physical.clone(),
            &KeyValueMutation::Upsert(upserts),
            table_id,
            1,
            1,
        )
        .unwrap(),
        change_batch(
            physical.clone(),
            &KeyValueMutation::Delete(deletes),
            table_id,
            2,
            6,
        )
        .unwrap(),
    ];
    let batch = arrow::compute::concat_batches(&physical, events.iter()).unwrap();
    let active = ActiveSegmentDescription::try_new(
        ActiveSegmentKey::new(
            0,
            0,
            table_id,
            SourceSegmentIdentity {
                segment_id: 7,
                generation: 1,
                writer_epoch: 1,
            },
        ),
        ActiveSegmentRole::Active,
        nonzero(1),
        nonzero(1),
        OffsetRange::new(LogOffset::new(0), LogOffset::new(6)).unwrap(),
        nonzero(7),
    )
    .unwrap()
    .with_utf8_value_bound(32);
    let table = PersistedTableDescription::try_new_with_active_segments(
        0,
        table_id,
        nonzero(1),
        nonzero(1),
        physical,
        vec![],
        vec![active],
    )
    .unwrap();
    let snapshot = PersistedSnapshotDescription::try_new(vec![table]).unwrap();
    let query_policy = QuerySessionPolicy::new(NonZeroUsize::new(2).unwrap())
        .with_batch_size(NonZeroUsize::new(2).unwrap());
    let bindings = vec![PersistedTableBinding::try_new("quotes", 0).unwrap()];
    for (sql, expected) in [
        ("select price from quotes order by price", vec![1, 20, 30]),
        ("select price from quotes where price >= 50", vec![]),
        (
            "select price from quotes where price >= 20 order by price limit 1",
            vec![20],
        ),
        ("select count(*) from quotes", vec![3]),
    ] {
        let profile = profile_persisted_plan(sql, &bindings, &snapshot, limits()).unwrap();
        let prepared = prepare_persisted_plan(query_policy, sql, &bindings, &snapshot, profile)
            .await
            .unwrap();
        let files: Arc<dyn PersistedFileOpener> = Arc::new(NoFileOpener {
            snapshot: snapshot.clone(),
        });
        let active: Arc<dyn ActiveSegmentOpener> = Arc::new(BatchActiveOpener {
            snapshot: snapshot.clone(),
            batch: batch.clone(),
            opens: Arc::new(AtomicUsize::new(0)),
        });
        let bound = prepared.bind_with_active(&files, &active).unwrap();
        let admission = bound
            .streaming_output_admission(NonZeroUsize::new(2).unwrap())
            .unwrap();
        assert!(admission.active_decode_memory_bytes().unwrap().get() < 8 * 1024 * 1024);
        let execution = query_policy
            .create_execution_context(nonzero(8 * 1024 * 1024))
            .unwrap();
        let mut stream = bound.execute_stream(&execution).unwrap();
        let mut actual: Vec<i64> = Vec::new();
        while let Some(batch) = next_persisted_batch(&mut stream).await.unwrap() {
            actual.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values(),
            );
        }
        assert_eq!(actual, expected, "{sql}");
    }
}

#[test]
fn native_key_value_schema_budget_uses_actual_relations() {
    let logical = Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("price", DataType::Int64, true),
    ]);
    let schema = zippy_types::key_value::log_schema(&logical, &["symbol".to_owned()]).unwrap();
    let table = PersistedTableDescription::try_new(
        0,
        TableId::from_u128(1).unwrap(),
        nonzero(1),
        nonzero(1),
        schema,
        vec![],
    )
    .unwrap();
    let snapshot = PersistedSnapshotDescription::try_new(vec![table]).unwrap();
    let profile = profile_persisted_plan(
        "SELECT * FROM \"zippy_schema_source\" LIMIT 0",
        &[PersistedTableBinding::try_new("zippy_schema_source", 0).unwrap()],
        &snapshot,
        PersistedPlanLimits::try_new(16 * 1024, 32, 64, 512, 1024, 4096).unwrap(),
    )
    .unwrap();
    assert!(
        profile.build_peak_bytes().get() < 512 * 1024 * 1024,
        "{profile:?}"
    );
}

#[tokio::test]
async fn active_utf8_admission_requires_snapshot_proof_and_rejects_expansion() {
    use arrow::array::StringArray;
    let table_id = TableId::from_u128(20).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    for (proof, sql, admitted) in [
        (false, "select key from ticks", false),
        (true, "select key from ticks", true),
        (true, "select cast(price as varchar) from ticks", false),
        (
            true,
            "select lag(key) over (order by price) from ticks",
            false,
        ),
        (true, "select min(key) from ticks", false),
        (true, "select key, count(*) from ticks group by key", true),
    ] {
        let active = ActiveSegmentDescription::try_new(
            ActiveSegmentKey::new(
                0,
                0,
                table_id,
                SourceSegmentIdentity {
                    segment_id: 1,
                    generation: 1,
                    writer_epoch: 1,
                },
            ),
            ActiveSegmentRole::Active,
            nonzero(1),
            nonzero(1),
            OffsetRange::new(LogOffset::new(0), LogOffset::new(1)).unwrap(),
            nonzero(2),
        )
        .unwrap();
        let active = if proof {
            active.with_utf8_value_bound(1)
        } else {
            active
        };
        let table = PersistedTableDescription::try_new_with_active_segments(
            0,
            table_id,
            nonzero(1),
            nonzero(1),
            schema.clone(),
            vec![],
            vec![active],
        )
        .unwrap();
        let snapshot = PersistedSnapshotDescription::try_new(vec![table]).unwrap();
        let bindings = [PersistedTableBinding::try_new("ticks", 0).unwrap()];
        let profile = profile_persisted_plan(sql, &bindings, &snapshot, limits()).unwrap();
        let prepared = prepare_persisted_plan(policy(), sql, &bindings, &snapshot, profile)
            .await
            .unwrap();
        let files: Arc<dyn PersistedFileOpener> = Arc::new(NoFileOpener {
            snapshot: snapshot.clone(),
        });
        let opens = Arc::new(AtomicUsize::new(0));
        let active: Arc<dyn ActiveSegmentOpener> = Arc::new(BatchActiveOpener {
            snapshot,
            batch: batch.clone(),
            opens: opens.clone(),
        });
        let bound = prepared.bind_with_active(&files, &active).unwrap();
        assert_eq!(
            bound
                .streaming_output_admission(NonZeroUsize::new(2).unwrap())
                .is_ok(),
            admitted,
            "{sql}, proof={proof}"
        );
        assert_eq!(
            opens.load(Ordering::Relaxed),
            0,
            "admission must precede any decoding"
        );
    }
}
