use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use zippy_core::{Engine, SegmentTableView};
use zippy_engines::ReactiveStateEngine;
use zippy_operators::TsEmaSpec;

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

criterion_group!(benches, bench_reactive_pipeline);
criterion_main!(benches);
