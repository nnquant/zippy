use std::sync::Arc;

use arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use zippy_core::{Engine, LateDataPolicy, SegmentTableView};
use zippy_engines::CrossSectionalEngine;
use zippy_operators::{CSDemeanSpec, CSRankSpec, CSZscoreSpec};

fn bench_crosssectional_pipeline(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("ret_1m", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                (0..1024)
                    .map(|index| format!("S{:03}", index % 32))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(vec![0_i64; 1024]).with_timezone("UTC")),
            Arc::new(Float64Array::from(
                (0..1024)
                    .map(|index| (index % 17) as f64 - 8.0)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    c.bench_function("crosssectional_pipeline_1024_rows", |b| {
        b.iter(|| {
            let mut engine = CrossSectionalEngine::new(
                "bench",
                schema.clone(),
                "symbol",
                "dt",
                60_000_000_000_i64,
                LateDataPolicy::Reject,
                vec![
                    CSRankSpec::new("ret_1m", "ret_rank").build().unwrap(),
                    CSZscoreSpec::new("ret_1m", "ret_z").build().unwrap(),
                    CSDemeanSpec::new("ret_1m", "ret_dm").build().unwrap(),
                ],
            )
            .unwrap();
            let _ = engine
                .on_data(SegmentTableView::from_record_batch(batch.clone()))
                .unwrap();
            let _ = engine.on_flush().unwrap();
        })
    });
}

fn crosssectional_batch(schema: Arc<Schema>, rows: usize, offset: usize) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|index| format!("S{:05}", (index + offset) % 8192))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(vec![0_i64; rows]).with_timezone("UTC")),
            Arc::new(Float64Array::from(
                (0..rows)
                    .map(|index| ((index + offset) % 97) as f64)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn bench_crosssectional_large_bucket_small_updates(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("ret_1m", DataType::Float64, false),
    ]));
    let initial = crosssectional_batch(Arc::clone(&schema), 8192, 0);
    let updates = (0..128)
        .map(|offset| crosssectional_batch(Arc::clone(&schema), 32, offset * 32))
        .collect::<Vec<_>>();

    c.bench_function("crosssectional_8192_symbols_32_row_updates", |b| {
        b.iter(|| {
            let mut engine = CrossSectionalEngine::new(
                "bench",
                Arc::clone(&schema),
                "symbol",
                "dt",
                60_000_000_000_i64,
                LateDataPolicy::Reject,
                vec![
                    CSRankSpec::new("ret_1m", "ret_rank").build().unwrap(),
                    CSZscoreSpec::new("ret_1m", "ret_z").build().unwrap(),
                    CSDemeanSpec::new("ret_1m", "ret_dm").build().unwrap(),
                ],
            )
            .unwrap();
            engine
                .on_data(SegmentTableView::from_record_batch(initial.clone()))
                .unwrap();
            for update in &updates {
                engine
                    .on_data(SegmentTableView::from_record_batch(update.clone()))
                    .unwrap();
            }
        })
    });
}

criterion_group!(
    benches,
    bench_crosssectional_pipeline,
    bench_crosssectional_large_bucket_small_updates
);
criterion_main!(benches);
