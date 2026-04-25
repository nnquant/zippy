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

criterion_group!(benches, bench_timeseries_pipeline);
criterion_main!(benches);
