use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use zippy_io::ParquetSink;

static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(0);

fn temp_dir_path() -> PathBuf {
    let path_id = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "zippy-io-parquet-test-pid-{}-{}",
        std::process::id(),
        path_id
    ))
}

#[test]
fn parquet_sink_writes_batch_that_can_be_read_back() {
    let root = temp_dir_path();
    let sink = ParquetSink::new(&root);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
        ],
    )
    .unwrap();

    sink.write_batch("bars.parquet", &batch).unwrap();

    let file = File::open(root.join("bars.parquet")).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let output = reader.next().unwrap().unwrap();

    assert_eq!(output.schema().as_ref(), schema.as_ref());
    assert_eq!(output.num_rows(), 2);

    drop(output);
    drop(reader);
    std::fs::remove_dir_all(&root).unwrap();
}

fn read_back_batch(path: PathBuf) -> RecordBatch {
    let file = File::open(path).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.next().unwrap().unwrap();

    drop(reader);
    batch
}

#[test]
fn parquet_sink_overwrites_existing_file_on_second_write() {
    let root = temp_dir_path();
    let sink = ParquetSink::new(&root);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let first = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Float64Array::from(vec![1.0])),
        ],
    )
    .unwrap();
    let second = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["b", "c"])),
            Arc::new(Float64Array::from(vec![2.0, 3.0])),
        ],
    )
    .unwrap();

    sink.write_batch("bars.parquet", &first).unwrap();
    sink.write_batch("bars.parquet", &second).unwrap();

    let output = read_back_batch(root.join("bars.parquet"));

    assert_eq!(output.schema().as_ref(), schema.as_ref());
    assert_eq!(output.num_rows(), 2);

    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn parquet_sink_writes_multiple_files_without_conflict() {
    let root = temp_dir_path();
    let sink = ParquetSink::new(&root);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let first = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Float64Array::from(vec![1.0])),
        ],
    )
    .unwrap();
    let second = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["b"])),
            Arc::new(Float64Array::from(vec![2.0])),
        ],
    )
    .unwrap();

    sink.write_batch("bars-1.parquet", &first).unwrap();
    sink.write_batch("bars-2.parquet", &second).unwrap();

    assert_eq!(read_back_batch(root.join("bars-1.parquet")).num_rows(), 1);
    assert_eq!(read_back_batch(root.join("bars-2.parquet")).num_rows(), 1);

    std::fs::remove_dir_all(&root).unwrap();
}

fn read_back_row_count(path: PathBuf) -> usize {
    let file = File::open(path).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next() {
        rows += batch.unwrap().num_rows();
    }
    rows
}

#[test]
fn parquet_sink_writer_appends_multiple_batches_into_single_file() {
    let root = temp_dir_path();
    let sink = ParquetSink::new(&root);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let first = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Float64Array::from(vec![1.0])),
        ],
    )
    .unwrap();
    let second = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["b", "c"])),
            Arc::new(Float64Array::from(vec![2.0, 3.0])),
        ],
    )
    .unwrap();

    let mut writer = sink.create_writer("bars.parquet", schema).unwrap();
    writer.write_batch(&first).unwrap();
    writer.write_batch(&second).unwrap();
    writer.close().unwrap();

    assert_eq!(read_back_row_count(root.join("bars.parquet")), 3);

    std::fs::remove_dir_all(&root).unwrap();
}
