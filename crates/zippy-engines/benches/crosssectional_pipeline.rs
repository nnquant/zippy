use std::sync::Arc;

use arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use zippy_core::{Engine, LateDataPolicy};
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
            Arc::new(
                TimestampNanosecondArray::from(vec![0_i64; 1024]).with_timezone("UTC"),
            ),
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
            let _ = engine.on_data(batch.clone()).unwrap();
            let _ = engine.on_flush().unwrap();
        })
    });
}

criterion_group!(benches, bench_crosssectional_pipeline);
criterion_main!(benches);
