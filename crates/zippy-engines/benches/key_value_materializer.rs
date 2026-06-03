use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use zippy_core::{Engine, SegmentTableView};
use zippy_engines::KeyValueTableMaterializer;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("last_price", DataType::Float64, false),
    ]))
}

fn batch(row_count: usize, offset: usize) -> RecordBatch {
    let instrument_ids = (0..row_count)
        .map(|index| format!("S{:05}", (index + offset) % 8192))
        .collect::<Vec<_>>();
    let timestamps = (0..row_count)
        .map(|index| 1_710_000_000_000_000_000_i64 + index as i64 + offset as i64)
        .collect::<Vec<_>>();
    let prices = (0..row_count)
        .map(|index| 4000.0 + ((index + offset) % 97) as f64)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(instrument_ids)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")) as ArrayRef,
            Arc::new(Float64Array::from(prices)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn bench_key_value_existing_key_updates(c: &mut Criterion) {
    let input_schema = schema();
    let initial = batch(8192, 0);
    let updates = (0..128)
        .map(|offset| batch(32, offset * 32))
        .collect::<Vec<_>>();

    c.bench_function("key_value_8192_keys_32_existing_key_updates", |b| {
        b.iter(|| {
            let mut materializer = KeyValueTableMaterializer::new_with_row_capacity(
                "bench_latest",
                Arc::clone(&input_schema),
                vec!["instrument_id"],
                8192,
            )
            .unwrap();
            materializer
                .on_data(SegmentTableView::from_record_batch(initial.clone()))
                .unwrap();
            for update in &updates {
                materializer
                    .on_data(SegmentTableView::from_record_batch(update.clone()))
                    .unwrap();
            }
        })
    });
}

criterion_group!(benches, bench_key_value_existing_key_updates);
criterion_main!(benches);
