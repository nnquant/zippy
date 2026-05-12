use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use zippy_core::{Engine, SegmentTableView};
use zippy_engines::ReactiveStateEngine;
use zippy_operators::{CastSpec, ExpressionSpec, TsEmaSpec, TsMeanSpec, TsStdSpec};

fn bench_reactive_pipeline(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A"; 1024])),
            Arc::new(Float64Array::from(vec![10.0; 1024])),
        ],
    )
    .unwrap();

    c.bench_function("reactive_pipeline_1024_rows", |b| {
        b.iter(|| {
            let mut engine = ReactiveStateEngine::new(
                "bench",
                schema.clone(),
                vec![TsEmaSpec::new("symbol", "price", 8, "ema_8")
                    .build()
                    .unwrap()],
            )
            .unwrap();
            let _ = engine
                .on_data(SegmentTableView::from_record_batch(batch.clone()))
                .unwrap();
        })
    });
}

fn bench_reactive_pipeline_multi_factor(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                (0..1024)
                    .map(|index| format!("S{:03}", index % 32))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..1024)
                    .map(|index| 10.0 + (index % 17) as f64)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    c.bench_function("reactive_pipeline_1024_rows_8_factors", |b| {
        b.iter(|| {
            let mut engine = ReactiveStateEngine::new(
                "bench",
                schema.clone(),
                vec![
                    TsEmaSpec::new("symbol", "price", 8, "ema_8")
                        .build()
                        .unwrap(),
                    TsEmaSpec::new("symbol", "price", 16, "ema_16")
                        .build()
                        .unwrap(),
                    TsMeanSpec::new("symbol", "price", 8, "mean_8")
                        .build()
                        .unwrap(),
                    TsStdSpec::new("symbol", "price", 8, "std_8")
                        .build()
                        .unwrap(),
                    CastSpec::new("symbol", "price", "int64", "price_i64")
                        .build()
                        .unwrap(),
                    ExpressionSpec::new("price + 1", "price_plus_one")
                        .build(schema.as_ref())
                        .unwrap(),
                    ExpressionSpec::new("abs(price)", "price_abs")
                        .build(schema.as_ref())
                        .unwrap(),
                    ExpressionSpec::new("clip(price, 10, 20)", "price_clip")
                        .build(schema.as_ref())
                        .unwrap(),
                ],
            )
            .unwrap();
            let _ = engine
                .on_data(SegmentTableView::from_record_batch(batch.clone()))
                .unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_reactive_pipeline,
    bench_reactive_pipeline_multi_factor
);
criterion_main!(benches);
