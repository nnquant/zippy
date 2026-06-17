use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use zippy_core::{Engine, SegmentTableView};
use zippy_engines::{ShardConfig, ShardedStreamTableMaterializer, StreamTableMaterializer};

fn tick_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

fn tick_batch(row_count: usize) -> RecordBatch {
    let row_ids = (0..row_count).map(|index| index as i64).collect::<Vec<_>>();
    let instrument_ids = (0..row_count)
        .map(|index| format!("S{:05}", index % 8192))
        .collect::<Vec<_>>();
    let prices = (0..row_count)
        .map(|index| 4000.0 + (index % 97) as f64)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        tick_schema(),
        vec![
            Arc::new(Int64Array::from(row_ids)) as ArrayRef,
            Arc::new(StringArray::from(instrument_ids)) as ArrayRef,
            Arc::new(Float64Array::from(prices)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn sharded_materializer(schema: Arc<Schema>, shard_nums: usize) -> ShardedStreamTableMaterializer {
    let config = ShardConfig::new(vec!["instrument_id".to_string()], shard_nums, 1).unwrap();
    ShardedStreamTableMaterializer::new_with_row_capacity(
        format!("bench_ticks_sharded_{shard_nums}"),
        schema,
        config,
        65_536,
    )
    .unwrap()
}

fn bench_stream_table_sharding(c: &mut Criterion) {
    let schema = tick_schema();
    let batch = tick_batch(16_384);
    let mut group = c.benchmark_group("stream_table_sharding");
    group.throughput(Throughput::Elements(batch.num_rows() as u64));

    group.bench_function(BenchmarkId::new("single", batch.num_rows()), |b| {
        b.iter(|| {
            let mut materializer = StreamTableMaterializer::new_with_row_capacity(
                "bench_ticks_single",
                Arc::clone(&schema),
                65_536,
            )
            .unwrap();
            materializer
                .on_data(SegmentTableView::from_record_batch(batch.clone()))
                .unwrap();
            materializer.on_flush().unwrap();
        })
    });

    for shard_nums in [4usize, 16usize] {
        group.bench_function(
            BenchmarkId::new(format!("sharded_{shard_nums}"), batch.num_rows()),
            |b| {
                b.iter(|| {
                    let mut materializer = sharded_materializer(Arc::clone(&schema), shard_nums);
                    materializer
                        .on_data(SegmentTableView::from_record_batch(batch.clone()))
                        .unwrap();
                    materializer.on_flush().unwrap();
                    let counts = materializer.shard_row_counts();
                    assert_eq!(counts.iter().sum::<usize>(), batch.num_rows());
                    assert!(materializer.failed_shards().is_empty());
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_stream_table_sharding);
criterion_main!(benches);
