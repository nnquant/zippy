use std::sync::Arc;

use arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use zippy_core::{Engine, LateDataPolicy, SegmentTableView};
use zippy_engines::TimeSeriesEngine;
use zippy_operators::{AggFirstSpec, AggLastSpec, AggSumSpec};

fn bench_timeseries_pipeline(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A"; 1024])),
            Arc::new(
                TimestampNanosecondArray::from(
                    (0..1024)
                        .map(|index| index as i64 * 1_000_000_000)
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(Float64Array::from(vec![10.0; 1024])),
            Arc::new(Float64Array::from(vec![100.0; 1024])),
        ],
    )
    .unwrap();

    c.bench_function("timeseries_pipeline_1024_rows", |b| {
        b.iter(|| {
            let mut engine = TimeSeriesEngine::new(
                "bench",
                schema.clone(),
                "symbol",
                "dt",
                60_000_000_000_i64,
                LateDataPolicy::Reject,
                vec![
                    AggFirstSpec::new("price", "open").build().unwrap(),
                    AggLastSpec::new("price", "close").build().unwrap(),
                    AggSumSpec::new("volume", "volume").build().unwrap(),
                ],
                vec![],
                vec![],
            )
            .unwrap();
            let _ = engine
                .on_data(SegmentTableView::from_record_batch(batch.clone()))
                .unwrap();
            let _ = engine.on_flush().unwrap();
        })
    });
}

fn timeseries_batch(schema: Arc<Schema>, rows: usize, symbols: usize) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|index| format!("S{:05}", index % symbols))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampNanosecondArray::from(
                    (0..rows)
                        .map(|index| index as i64 * 1_000_000)
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(Float64Array::from(
                (0..rows)
                    .map(|index| 10.0 + (index % 97) as f64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(vec![100.0; rows])),
        ],
    )
    .unwrap()
}

fn bench_timeseries_many_symbols(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]));
    let batch_1024 = timeseries_batch(Arc::clone(&schema), 1024, 1024);
    let batch_8192 = timeseries_batch(Arc::clone(&schema), 8192, 8192);

    for (name, batch) in [
        ("timeseries_pipeline_1024_symbols", batch_1024),
        ("timeseries_pipeline_8192_symbols", batch_8192),
    ] {
        c.bench_function(name, |b| {
            b.iter(|| {
                let mut engine = TimeSeriesEngine::new(
                    "bench",
                    Arc::clone(&schema),
                    "symbol",
                    "dt",
                    60_000_000_000_i64,
                    LateDataPolicy::Reject,
                    vec![
                        AggFirstSpec::new("price", "open").build().unwrap(),
                        AggLastSpec::new("price", "close").build().unwrap(),
                        AggSumSpec::new("volume", "volume").build().unwrap(),
                    ],
                    vec![],
                    vec![],
                )
                .unwrap();
                engine
                    .on_data(SegmentTableView::from_record_batch(batch.clone()))
                    .unwrap();
                engine.on_flush().unwrap();
            })
        });
    }
}

criterion_group!(
    benches,
    bench_timeseries_pipeline,
    bench_timeseries_many_symbols
);
criterion_main!(benches);
