use std::fs::{self, File};
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use zippy_core::OwnedDataDirectory;
use zippy_storage::{
    ParquetCandidateSpec, ParquetCandidateWriter, CANDIDATE_FOOTER_METADATA_KEY,
    PARQUET_CANDIDATE_FORMAT_VERSION,
};
use zippy_types::table::{
    DataFileLocation, DataFileProvenance, MetadataMap, MetadataScalar, NativeDataFileProvenance,
    SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages,
};
use zippy_types::{ErrorCode, FileId, LogOffset, OffsetRange, ServerErrorDetails, TableId};

const TABLE: u128 = 10;
const FILE: u128 = 20;

fn table_id(value: u128) -> TableId {
    TableId::from_u128(value).expect("test table id must be nonzero")
}

fn file_id(value: u128) -> FileId {
    FileId::from_u128(value).expect("test file id must be nonzero")
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, false),
        Field::new("instrument", DataType::Utf8, false),
    ]))
}

fn batch(schema: SchemaRef, prices: Vec<i64>, instruments: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(prices)),
            Arc::new(StringArray::from(instruments)),
        ],
    )
    .expect("valid test batch")
}

fn provenance(first: u64, last: u64) -> NativeDataFileProvenance {
    let offsets = OffsetRange::new(LogOffset::new(first), LogOffset::new(last))
        .expect("ordered test offsets");
    let source_lineage = SourceSegmentLineages::new(vec![SourceSegmentLineage {
        source: SourceSegmentIdentity {
            segment_id: 7,
            generation: 3,
            writer_epoch: 11,
        },
        offsets,
    }])
    .expect("valid test lineage");
    NativeDataFileProvenance::new(offsets, source_lineage)
        .expect("lineage exactly covers test offsets")
}

fn candidate_spec(file: u128, schema: SchemaRef, first: u64, last: u64) -> ParquetCandidateSpec {
    ParquetCandidateSpec {
        file_id: file_id(file),
        table_id: table_id(TABLE),
        schema_version: 5,
        schema,
        provenance: provenance(first, last),
        partition: MetadataMap::from([(
            "trade_date".to_owned(),
            MetadataScalar::String("2026-07-14".to_owned()),
        )]),
        metadata: MetadataMap::from([(
            "source".to_owned(),
            MetadataScalar::String("sim7x24".to_owned()),
        )]),
        created_at_ns: 1_752_492_600_000_000_000,
    }
}

fn candidate_writer(root: &TempDir) -> ParquetCandidateWriter {
    let owned = OwnedDataDirectory::acquire_or_initialize(root.path().join("data"))
        .expect("own test data directory");
    ParquetCandidateWriter::new(Arc::new(owned))
}

fn checksum(bytes: &[u8]) -> String {
    format!("sha256-v1:{:x}", Sha256::digest(bytes))
}

fn all_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };
    let mut files = Vec::new();
    for entry in entries {
        let path = entry.expect("read test directory entry").path();
        if path.is_dir() {
            files.extend(all_files(&path));
        } else {
            files.push(path);
        }
    }
    files
}

#[test]
fn publishes_exact_pending_descriptor_and_readable_parquet() {
    let root = TempDir::new().expect("create test root");
    let writer = candidate_writer(&root);
    let schema = schema();
    let expected_batch = batch(
        Arc::clone(&schema),
        vec![101, 102, 103],
        vec!["au2608", "ag2608", "cu2608"],
    );

    let candidate = writer
        .write_candidate(
            candidate_spec(FILE, Arc::clone(&schema), 40, 42),
            vec![expected_batch.clone()],
        )
        .expect("publish durable candidate");

    let expected_path = root.path().join("data").join(format!(
        "parquet/{}/{}.parquet",
        table_id(TABLE),
        file_id(FILE)
    ));
    assert_eq!(candidate.path(), expected_path);
    let bytes = fs::read(candidate.path()).expect("read published candidate");
    let pending = candidate.pending();
    assert_eq!(pending.file_id, file_id(FILE));
    assert_eq!(pending.table_id, table_id(TABLE));
    assert_eq!(pending.schema_version, 5);
    assert_eq!(pending.file_size_bytes, bytes.len() as u64);
    assert_eq!(pending.row_count.get(), 3);
    assert_eq!(pending.checksum, checksum(&bytes));
    assert!(matches!(
        pending.location,
        DataFileLocation::Managed(ref path)
            if path.as_str()
                == format!("parquet/{}/{}.parquet", table_id(TABLE), file_id(FILE))
    ));
    assert!(matches!(
        pending.provenance,
        DataFileProvenance::Native(ref value) if value.offsets().len() == 3
    ));
    assert_eq!(
        pending.statistics.get("zippy.parquet.row_group_count"),
        Some(&MetadataScalar::UInt64(1))
    );
    assert_eq!(
        pending.statistics.get("zippy.parquet.column_count"),
        Some(&MetadataScalar::UInt64(2))
    );

    let builder = ParquetRecordBatchReaderBuilder::try_new(
        File::open(candidate.path()).expect("open published candidate"),
    )
    .expect("open Parquet metadata");
    let footer_json = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("candidate footer metadata")
        .iter()
        .find(|entry| entry.key == CANDIDATE_FOOTER_METADATA_KEY)
        .and_then(|entry| entry.value.as_deref())
        .expect("Zippy candidate footer envelope");
    let footer: serde_json::Value =
        serde_json::from_str(footer_json).expect("decode candidate footer envelope");
    assert_eq!(footer["format_version"], PARQUET_CANDIDATE_FORMAT_VERSION);
    assert_eq!(footer["file_id"], file_id(FILE).to_string());
    assert_eq!(footer["table_id"], table_id(TABLE).to_string());
    assert_eq!(footer["schema_version"], 5);

    let mut reader = builder.build().expect("build candidate reader");
    assert_eq!(
        reader.next().expect("one batch").expect("read batch"),
        expected_batch
    );
    assert!(reader.next().is_none());
}

#[test]
fn byte_identical_replay_is_idempotent() {
    let root = TempDir::new().expect("create test root");
    let writer = candidate_writer(&root);
    let schema = schema();
    let spec = candidate_spec(FILE, Arc::clone(&schema), 0, 1);
    let values = batch(
        Arc::clone(&schema),
        vec![500, 501],
        vec!["IF2609", "IH2609"],
    );
    let first = writer
        .write_candidate(spec.clone(), vec![values.clone()])
        .expect("first publication");
    let original = fs::read(first.path()).expect("read first publication");
    #[cfg(unix)]
    let first_inode = {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(first.path()).expect("first metadata").ino()
    };

    let replay = writer
        .write_candidate(spec, vec![values])
        .expect("idempotent replay");

    assert_eq!(replay, first);
    assert_eq!(fs::read(replay.path()).expect("read replay"), original);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            fs::metadata(replay.path()).expect("replay metadata").ino(),
            first_inode
        );
    }
}

#[test]
fn same_identity_with_different_content_is_rejected_without_overwrite() {
    let root = TempDir::new().expect("create test root");
    let writer = candidate_writer(&root);
    let schema = schema();
    let spec = candidate_spec(FILE, Arc::clone(&schema), 0, 1);
    let first = writer
        .write_candidate(
            spec.clone(),
            vec![batch(
                Arc::clone(&schema),
                vec![1, 2],
                vec!["rb2610", "rb2610"],
            )],
        )
        .expect("first publication");
    let original = fs::read(first.path()).expect("read first publication");

    let error = writer
        .write_candidate(
            spec,
            vec![batch(
                Arc::clone(&schema),
                vec![9, 10],
                vec!["rb2610", "rb2610"],
            )],
        )
        .expect_err("identity collision must fail");

    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert_eq!(
        fs::read(first.path()).expect("read retained file"),
        original
    );
    assert!(matches!(
        error.details(),
        Some(ServerErrorDetails::DataCorruption {
            expected_checksum: Some(expected),
            actual_checksum: Some(actual),
            ..
        }) if expected != actual && actual == &checksum(&original)
    ));
}

#[test]
fn empty_candidate_is_rejected_and_temporary_file_is_removed() {
    let root = TempDir::new().expect("create test root");
    let writer = candidate_writer(&root);
    let error = writer
        .write_candidate(candidate_spec(FILE, schema(), 0, 0), Vec::new())
        .expect_err("empty candidates are invalid");

    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    assert!(all_files(&root.path().join("data/parquet")).is_empty());
}

#[test]
fn row_count_must_equal_exact_offset_coverage() {
    let root = TempDir::new().expect("create test root");
    let writer = candidate_writer(&root);
    let schema = schema();
    let error = writer
        .write_candidate(
            candidate_spec(FILE, Arc::clone(&schema), 0, 0),
            vec![batch(schema, vec![1, 2], vec!["a", "b"])],
        )
        .expect_err("offset mismatch must fail");

    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    assert!(all_files(&root.path().join("data/parquet")).is_empty());
}

#[test]
fn batch_schema_must_match_bound_schema_exactly() {
    let root = TempDir::new().expect("create test root");
    let writer = candidate_writer(&root);
    let bound_schema = schema();
    let other_schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int32, false),
        Field::new("instrument", DataType::Utf8, false),
    ]));
    let other_batch = RecordBatch::try_new(
        other_schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ],
    )
    .expect("valid mismatched batch");

    let error = writer
        .write_candidate(candidate_spec(FILE, bound_schema, 0, 0), vec![other_batch])
        .expect_err("schema mismatch must fail");

    assert_eq!(error.code(), ErrorCode::SchemaMismatch);
    assert!(all_files(&root.path().join("data/parquet")).is_empty());
}

#[test]
fn unsupported_parquet_schema_is_rejected_before_creating_a_candidate() {
    let root = TempDir::new().expect("create test root");
    let writer = candidate_writer(&root);
    let run_end_encoded = DataType::RunEndEncoded(
        Arc::new(Field::new("run_ends", DataType::Int32, false)),
        Arc::new(Field::new("values", DataType::Int64, true)),
    );
    let unsupported_schema = Arc::new(Schema::new(vec![Field::new(
        "encoded",
        run_end_encoded,
        false,
    )]));

    let error = writer
        .write_candidate(candidate_spec(FILE, unsupported_schema, 0, 0), Vec::new())
        .expect_err("unsupported schema must fail before writing");

    assert_eq!(error.code(), ErrorCode::SchemaMismatch);
    assert!(all_files(&root.path().join("data/parquet")).is_empty());
}

#[test]
fn oversized_footer_metadata_is_rejected_before_creating_a_candidate() {
    let root = TempDir::new().expect("create test root");
    let writer = candidate_writer(&root);
    let mut spec = candidate_spec(FILE, schema(), 0, 0);
    spec.metadata.insert(
        "oversized".to_owned(),
        MetadataScalar::String("x".repeat(1024 * 1024)),
    );

    let error = writer
        .write_candidate(spec, Vec::new())
        .expect_err("oversized footer metadata must fail before writing");

    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(all_files(&root.path().join("data/parquet")).is_empty());
}

#[cfg(unix)]
#[test]
fn existing_symlink_candidate_is_rejected_without_following_it() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let root = TempDir::new().expect("create test root");
    let writer = candidate_writer(&root);
    let schema = schema();
    let parent = root
        .path()
        .join("data/parquet")
        .join(table_id(TABLE).to_string());
    fs::create_dir_all(&parent).expect("create candidate parent");
    fs::set_permissions(&parent, fs::Permissions::from_mode(0o700))
        .expect("secure candidate parent");
    let source = root.path().join("symlink-source");
    fs::write(&source, b"must remain unchanged").expect("write symlink source");
    let target = parent.join(format!("{}.parquet", file_id(FILE)));
    symlink(&source, &target).expect("create candidate symlink");

    let error = writer
        .write_candidate(
            candidate_spec(FILE, Arc::clone(&schema), 0, 0),
            vec![batch(schema, vec![1], vec!["a"])],
        )
        .expect_err("symlink candidate must fail");

    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert_eq!(
        fs::read(&source).expect("read source"),
        b"must remain unchanged"
    );
    assert!(fs::symlink_metadata(&target)
        .expect("inspect target")
        .file_type()
        .is_symlink());
}
