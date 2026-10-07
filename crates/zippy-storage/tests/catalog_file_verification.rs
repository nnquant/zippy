use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::OwnedDataDirectory;
use zippy_storage::{
    ParquetCandidateSpec, ParquetCandidateWriter, StartupCatalogFileVerifier,
    StartupCatalogFileVerifierConfig,
};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogRead,
    CatalogWrite, CommitCompactionCommand, CommitDataFilesCommand, CreateNamespaceCommand,
    CreateTableCommand, VersionedCatalogCommand,
};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, DataFileLocation, ManagedDataPath, MetadataMap,
    NativeDataFileProvenance, PendingDataFile, SchemaIpcPayload, SourceSegmentIdentity,
    SourceSegmentLineage, SourceSegmentLineages, TableDefinition, TableState,
};
use zippy_types::{
    ErrorCode, FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, RequestId, TableId,
};

const NAMESPACE: u128 = 1;
const TABLE: u128 = 10;

fn namespace_id() -> NamespaceId {
    NamespaceId::from_u128(NAMESPACE).expect("nonzero test namespace ID")
}

fn table_id(value: u128) -> TableId {
    TableId::from_u128(value).expect("nonzero test table ID")
}

fn file_id(value: u128) -> FileId {
    FileId::from_u128(value).expect("nonzero test file ID")
}

fn request_id(value: u128) -> RequestId {
    RequestId::from_u128(value).expect("nonzero test request ID")
}

fn actor() -> CatalogActor {
    CatalogActor {
        kind: CatalogActorKind::Server,
        principal: "catalog-file-verification-test".to_owned(),
    }
}

fn command(
    request: u128,
    expected_revision: Option<u64>,
    body: CatalogCommandV1,
) -> VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(request_id(request), expected_revision, actor(), body)
        .expect("valid Catalog command")
        .into()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Int64,
        false,
    )]))
}

fn schema_ipc(schema: &Schema) -> SchemaIpcPayload {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, schema).expect("create schema writer");
    writer.finish().expect("finish schema writer");
    drop(writer);
    SchemaIpcPayload::new(bytes).expect("nonempty schema IPC")
}

fn provenance(first: u64, last: u64) -> NativeDataFileProvenance {
    let offsets = OffsetRange::new(LogOffset::new(first), LogOffset::new(last))
        .expect("ordered test offsets");
    NativeDataFileProvenance::new(
        offsets,
        SourceSegmentLineages::new(vec![SourceSegmentLineage {
            source: SourceSegmentIdentity {
                segment_id: 1,
                generation: 1,
                writer_epoch: 0,
            },
            offsets,
        }])
        .expect("valid test lineage"),
    )
    .expect("lineage covers test offsets")
}

struct Fixture {
    _temp: tempfile::TempDir,
    owned: Arc<OwnedDataDirectory>,
    catalog: Arc<SqliteCatalog>,
}

impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().expect("create test directory");
        let owned = Arc::new(
            OwnedDataDirectory::acquire_or_initialize(temp.path().join("database"))
                .expect("own test data directory"),
        );
        let catalog = Arc::new(
            SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open test Catalog"),
        );
        catalog
            .command(command(
                1,
                Some(0),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: namespace_id(),
                    name: CatalogName::new("market").expect("valid namespace name"),
                }),
            ))
            .await
            .expect("create test namespace");
        Self {
            _temp: temp,
            owned,
            catalog,
        }
    }

    async fn create_table(&self, table: u128, request: u128, expected_revision: u64) {
        let schema = schema();
        self.catalog
            .command(command(
                request,
                Some(expected_revision),
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: table_id(table),
                    namespace_id: namespace_id(),
                    name: CatalogName::new(format!("ticks_{table}")).expect("valid table name"),
                    definition: TableDefinition::Append(AppendTableDefinition::default()),
                    schema_ipc: schema_ipc(schema.as_ref()),
                    config: BTreeMap::new(),
                }),
            ))
            .await
            .expect("create test table");
    }

    fn write_candidate(
        &self,
        table: u128,
        file: u128,
        first: u64,
        values: Vec<i64>,
    ) -> zippy_storage::DurableParquetCandidate {
        let last = first + u64::try_from(values.len()).expect("small row count") - 1;
        let schema = schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .expect("valid test batch");
        ParquetCandidateWriter::new(Arc::clone(&self.owned))
            .write_candidate(
                ParquetCandidateSpec {
                    file_id: file_id(file),
                    table_id: table_id(table),
                    schema_version: 1,
                    schema,
                    provenance: provenance(first, last),
                    partition: MetadataMap::new(),
                    metadata: MetadataMap::new(),
                    created_at_ns: 1_752_492_600_000_000_000 + file as u64,
                },
                vec![batch],
            )
            .expect("write test candidate")
    }

    async fn commit_files(
        &self,
        request: u128,
        expected_revision: u64,
        table: u128,
        files: Vec<PendingDataFile>,
        committed_offset: u64,
    ) {
        self.catalog
            .command(command(
                request,
                Some(expected_revision),
                CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
                    table_id: table_id(table),
                    expected_snapshot_version: 1,
                    files,
                    committed_log_offset: OffsetWatermark::Known(LogOffset::new(committed_offset)),
                }),
            ))
            .await
            .expect("commit test files");
    }

    fn verifier(&self) -> StartupCatalogFileVerifier {
        let catalog: Arc<dyn CatalogRead> = self.catalog.clone();
        StartupCatalogFileVerifier::new(Arc::clone(&self.owned), catalog)
    }

    fn verifier_with(
        &self,
        config: StartupCatalogFileVerifierConfig,
    ) -> StartupCatalogFileVerifier {
        let catalog: Arc<dyn CatalogRead> = self.catalog.clone();
        StartupCatalogFileVerifier::with_config(config, Arc::clone(&self.owned), catalog)
            .expect("valid verifier config")
    }
}

#[tokio::test]
async fn verifies_current_visible_managed_candidate() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let candidate = fixture.write_candidate(TABLE, 20, 0, vec![100, 101]);
    let expected_bytes = candidate.pending().file_size_bytes;
    fixture
        .commit_files(3, 2, TABLE, vec![candidate.into_pending()], 1)
        .await;

    let report = fixture
        .verifier()
        .verify_with_bootstrap()
        .await
        .expect("verify files")
        .verification;

    assert_eq!(report.catalog_revision, 3);
    assert_eq!(report.table_count, 1);
    assert_eq!(report.managed_file_count, 1);
    assert_eq!(report.file_count, 1);
    assert_eq!(report.verified_bytes, expected_bytes);
}

#[tokio::test]
async fn external_legacy_location_is_rejected_before_physical_read() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let candidate = fixture.write_candidate(TABLE, 10, 0, vec![100]);
    fixture
        .commit_files(3, 2, TABLE, vec![candidate.into_pending()], 0)
        .await;
    let external = fixture._temp.path().join("must-not-be-opened.parquet");
    let catalog =
        rusqlite::Connection::open(SqliteCatalogConfig::new(&fixture.owned).path()).unwrap();
    let rejected = catalog
        .execute(
            "UPDATE data_files SET location_kind = 'external_legacy', file_path = ?1",
            [external.to_str().unwrap()],
        )
        .unwrap_err();
    assert_eq!(
        rejected.sqlite_error_code(),
        Some(rusqlite::ErrorCode::ConstraintViolation)
    );
    for sql in [
        "UPDATE data_files SET provenance_kind = 'legacy'",
        "UPDATE table_snapshots SET operation = 'import'",
        "UPDATE table_snapshots SET committed_log_offset = X'020000000000000000'",
    ] {
        assert_eq!(
            catalog.execute(sql, []).unwrap_err().sqlite_error_code(),
            Some(rusqlite::ErrorCode::ConstraintViolation)
        );
    }
    catalog
        .execute_batch("PRAGMA ignore_check_constraints = ON")
        .unwrap();
    catalog
        .execute(
            "UPDATE data_files SET location_kind = 'external_legacy', file_path = ?1",
            [external.to_str().unwrap()],
        )
        .unwrap();
    drop(catalog);
    let error = fixture
        .verifier()
        .verify_with_bootstrap()
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert!(!error.retryable());
    assert!(error
        .message()
        .contains("catalog data file location kind is invalid"));
    assert!(!error.message().contains(external.to_str().unwrap()));
    assert!(!external.exists());
}

#[tokio::test]
async fn native_tables_share_the_total_file_budget() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let first = fixture.write_candidate(TABLE, 10, 0, vec![100, 101, 102]);
    let first_bytes = first.pending().file_size_bytes;
    let DataFileLocation::Managed(first_relative) = &first.pending().location;
    let first_path = fixture
        .owned
        .directory()
        .root()
        .join(first_relative.as_str());
    fixture
        .commit_files(3, 2, TABLE, vec![first.into_pending()], 2)
        .await;
    fixture.create_table(TABLE + 1, 4, 3).await;
    let second = fixture.write_candidate(TABLE + 1, 20, 0, vec![200, 201]);
    let second_bytes = second.pending().file_size_bytes;
    fixture
        .commit_files(5, 4, TABLE + 1, vec![second.into_pending()], 1)
        .await;
    let aggregate_bytes = first_bytes + second_bytes;
    let report = fixture
        .verifier()
        .verify_with_bootstrap()
        .await
        .expect("verify both native tables")
        .verification;
    assert_eq!(report.managed_file_count, 2);
    assert_eq!(report.file_count, 2);
    assert_eq!(report.verified_bytes, aggregate_bytes);

    fs::remove_file(first_path).expect("remove native file after successful verification");
    let limited = fixture.verifier_with(StartupCatalogFileVerifierConfig::new().with_max_files(1));
    let error = limited
        .verify_with_bootstrap()
        .await
        .expect_err("aggregate file budget must fail before physical reads");
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
}

#[tokio::test]
async fn native_tables_share_the_verification_byte_budget() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let first = fixture.write_candidate(TABLE, 10, 0, vec![100, 101, 102]);
    let first_bytes = first.pending().file_size_bytes;
    fixture
        .commit_files(3, 2, TABLE, vec![first.into_pending()], 2)
        .await;
    fixture.create_table(TABLE + 1, 4, 3).await;
    let second = fixture.write_candidate(TABLE + 1, 20, 0, vec![200, 201]);
    let second_bytes = second.pending().file_size_bytes;
    fixture
        .commit_files(5, 4, TABLE + 1, vec![second.into_pending()], 1)
        .await;
    let aggregate_bytes = first_bytes + second_bytes;
    let verifier = fixture.verifier_with(
        StartupCatalogFileVerifierConfig::new().with_max_verification_bytes(aggregate_bytes - 1),
    );

    let error = verifier
        .verify_with_bootstrap()
        .await
        .expect_err("aggregate byte budget must cover both native tables");

    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
}

#[tokio::test]
async fn bootstrap_returns_current_runtime_descriptors_without_visible_files() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let candidate = fixture.write_candidate(TABLE, 20, 0, vec![100, 101]);
    let expected_bytes = candidate.pending().file_size_bytes;
    fixture
        .commit_files(3, 2, TABLE, vec![candidate.into_pending()], 1)
        .await;

    let bootstrap = fixture
        .verifier()
        .verify_with_bootstrap()
        .await
        .expect("verify files and load runtime descriptors");

    assert_eq!(bootstrap.verification.catalog_revision, 3);
    assert_eq!(bootstrap.verification.table_count, 1);
    assert_eq!(bootstrap.verification.managed_file_count, 1);
    assert_eq!(bootstrap.verification.verified_bytes, expected_bytes);
    assert_eq!(bootstrap.active_tables.len(), 1);
    assert_eq!(
        bootstrap
            .active_tables
            .iter()
            .map(|table| table.retained_metadata_bytes)
            .sum::<u64>(),
        bootstrap.runtime_metadata_bytes
    );
    let descriptor = &bootstrap.active_tables[0];
    assert_eq!(descriptor.table.table_id, table_id(TABLE));
    assert_eq!(descriptor.table.state, TableState::Active);
    assert_eq!(descriptor.table.current_schema_version, 1);
    assert_eq!(descriptor.table.current_snapshot_version, 2);
    assert_eq!(descriptor.schema.table_id, table_id(TABLE));
    assert_eq!(descriptor.schema.schema_version, 1);
    assert_eq!(descriptor.schema.schema_ipc, schema_ipc(schema().as_ref()));
    assert_eq!(descriptor.snapshot.table_id, table_id(TABLE));
    assert_eq!(descriptor.snapshot.snapshot_version, 2);
    assert!(
        bootstrap.runtime_metadata_bytes
            > u64::try_from(descriptor.schema.schema_ipc.as_bytes().len())
                .expect("schema IPC size fits u64")
    );
}

#[tokio::test]
async fn bootstrap_is_complete_and_ordered_across_pages_and_snapshot_batches() {
    let fixture = Fixture::new().await;
    for index in 0_u128..3 {
        fixture
            .create_table(TABLE + index, 2 + index, 1 + index as u64)
            .await;
    }
    let verifier = fixture.verifier_with(
        StartupCatalogFileVerifierConfig::new()
            .with_table_page_size(2)
            .with_snapshot_batch_size(1),
    );

    let bootstrap = verifier
        .verify_with_bootstrap()
        .await
        .expect("load all active tables across bounded reads");

    assert_eq!(bootstrap.verification.catalog_revision, 4);
    assert_eq!(bootstrap.verification.table_count, 3);
    assert_eq!(bootstrap.verification.managed_file_count, 0);
    assert_eq!(
        bootstrap
            .active_tables
            .iter()
            .map(|descriptor| descriptor.table.table_id)
            .collect::<Vec<_>>(),
        vec![table_id(TABLE), table_id(TABLE + 1), table_id(TABLE + 2)]
    );
}

#[tokio::test]
async fn aggregate_runtime_metadata_budget_fails_closed() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    fixture.create_table(TABLE + 1, 3, 2).await;
    let measured = fixture
        .verifier()
        .verify_with_bootstrap()
        .await
        .expect("measure default bootstrap metadata");
    assert!(measured.runtime_metadata_bytes > 1);
    let verifier = fixture.verifier_with(
        StartupCatalogFileVerifierConfig::new()
            .with_table_page_size(1)
            .with_max_runtime_metadata_bytes(measured.runtime_metadata_bytes - 1),
    );

    let error = verifier
        .verify_with_bootstrap()
        .await
        .expect_err("aggregate metadata must exceed the startup budget");

    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
}

#[tokio::test]
async fn later_physical_failure_returns_no_partial_bootstrap() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    fixture.create_table(TABLE + 1, 3, 2).await;
    let candidate = fixture.write_candidate(TABLE + 1, 20, 0, vec![100]);
    let path = candidate.path().to_path_buf();
    fixture
        .commit_files(4, 3, TABLE + 1, vec![candidate.into_pending()], 0)
        .await;
    fs::remove_file(path).expect("remove second-page committed candidate");
    let verifier = fixture.verifier_with(
        StartupCatalogFileVerifierConfig::new()
            .with_table_page_size(1)
            .with_snapshot_batch_size(1),
    );

    let error = verifier
        .verify_with_bootstrap()
        .await
        .expect_err("a later physical failure must discard earlier descriptors");

    assert_eq!(error.code(), ErrorCode::DataCorruption);
}

#[tokio::test]
async fn missing_current_visible_file_is_data_corruption() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let candidate = fixture.write_candidate(TABLE, 20, 0, vec![100]);
    let path = candidate.path().to_path_buf();
    fixture
        .commit_files(3, 2, TABLE, vec![candidate.into_pending()], 0)
        .await;
    fs::remove_file(path).expect("remove committed test candidate");

    let error = fixture
        .verifier()
        .verify_with_bootstrap()
        .await
        .expect_err("missing visible file must fail closed");

    assert_eq!(error.code(), ErrorCode::DataCorruption);
}

#[tokio::test]
async fn changed_current_visible_file_is_data_corruption() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let candidate = fixture.write_candidate(TABLE, 20, 0, vec![100]);
    let path = candidate.path().to_path_buf();
    fixture
        .commit_files(3, 2, TABLE, vec![candidate.into_pending()], 0)
        .await;
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open committed candidate for corruption test");
    file.seek(SeekFrom::Start(0)).expect("seek candidate");
    file.write_all(b"FAIL").expect("change candidate bytes");
    file.sync_all().expect("sync changed candidate");

    let error = fixture
        .verifier()
        .verify_with_bootstrap()
        .await
        .expect_err("changed visible file must fail closed");

    assert_eq!(error.code(), ErrorCode::DataCorruption);
}

#[tokio::test]
async fn noncanonical_catalog_path_is_data_corruption() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let candidate = fixture.write_candidate(TABLE, 20, 0, vec![100]);
    let mut pending = candidate.into_pending();
    pending.location = DataFileLocation::Managed(
        ManagedDataPath::new(format!("parquet/{TABLE}/alias.parquet"))
            .expect("syntactically valid but noncanonical candidate path"),
    );
    fixture.commit_files(3, 2, TABLE, vec![pending], 0).await;

    let error = fixture
        .verifier()
        .verify_with_bootstrap()
        .await
        .expect_err("Catalog path must encode the immutable file identity");

    assert_eq!(error.code(), ErrorCode::DataCorruption);
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_catalog_table_directory_is_data_corruption() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let candidate = fixture.write_candidate(TABLE, 20, 0, vec![100]);
    fixture
        .commit_files(3, 2, TABLE, vec![candidate.into_pending()], 0)
        .await;
    let root = fixture.owned.directory().root();
    let table_directory = root.join("parquet").join(table_id(TABLE).to_string());
    let relocated = root.join("tmp/relocated-table");
    fs::rename(&table_directory, &relocated).expect("relocate committed table directory");
    symlink(&relocated, &table_directory).expect("replace table directory with a symlink");

    let error = fixture
        .verifier()
        .verify_with_bootstrap()
        .await
        .expect_err("symlinked parent must not be followed");

    assert_eq!(error.code(), ErrorCode::DataCorruption);
}

#[cfg(unix)]
#[tokio::test]
async fn verification_uses_owned_root_descriptor_after_path_replacement() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let candidate = fixture.write_candidate(TABLE, 20, 0, vec![100]);
    fixture
        .commit_files(3, 2, TABLE, vec![candidate.into_pending()], 0)
        .await;
    let root = fixture.owned.directory().root().to_path_buf();
    let parent = root.parent().expect("test data root has a parent");
    let relocated = parent.join("relocated-database");
    let replacement = parent.join("replacement-database");
    fs::rename(&root, &relocated).expect("relocate owned data root");
    fs::create_dir(&replacement).expect("create replacement root");
    symlink(&replacement, &root).expect("replace data-root path with symlink");

    let report = fixture
        .verifier()
        .verify_with_bootstrap()
        .await
        .expect("verification remains bound to the owned root descriptor")
        .verification;

    assert_eq!(report.managed_file_count, 1);
}

#[tokio::test]
async fn ignores_removed_file_and_verifies_only_current_compaction_output() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let source = fixture.write_candidate(TABLE, 20, 0, vec![100]);
    let source_path = source.path().to_path_buf();
    fixture
        .commit_files(3, 2, TABLE, vec![source.into_pending()], 0)
        .await;
    let output = fixture.write_candidate(TABLE, 21, 0, vec![100]);
    let output_bytes = output.pending().file_size_bytes;
    fixture
        .catalog
        .command(command(
            4,
            Some(3),
            CatalogCommandV1::CommitCompaction(CommitCompactionCommand {
                table_id: table_id(TABLE),
                expected_snapshot_version: 2,
                input_file_ids: vec![file_id(20)],
                output_files: vec![output.into_pending()],
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(0)),
            }),
        ))
        .await
        .expect("commit test compaction");
    fs::remove_file(source_path).expect("remove historical source candidate");

    let report = fixture
        .verifier()
        .verify_with_bootstrap()
        .await
        .expect("historical removed file is outside current visibility")
        .verification;

    assert_eq!(report.catalog_revision, 4);
    assert_eq!(report.managed_file_count, 1);
    assert_eq!(report.verified_bytes, output_bytes);
}

#[tokio::test]
async fn table_count_budget_fails_closed() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    fixture.create_table(TABLE + 1, 3, 2).await;
    let verifier = fixture.verifier_with(
        StartupCatalogFileVerifierConfig::new()
            .with_max_tables(1)
            .with_table_page_size(1),
    );

    let error = verifier
        .verify_with_bootstrap()
        .await
        .expect_err("second table must exceed the startup budget");

    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
}

#[tokio::test]
async fn managed_file_count_budget_fails_before_verification() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let first = fixture.write_candidate(TABLE, 20, 0, vec![100]);
    let second = fixture.write_candidate(TABLE, 21, 1, vec![101]);
    fixture
        .commit_files(
            3,
            2,
            TABLE,
            vec![first.into_pending(), second.into_pending()],
            1,
        )
        .await;
    let verifier = fixture.verifier_with(StartupCatalogFileVerifierConfig::new().with_max_files(1));

    let error = verifier
        .verify_with_bootstrap()
        .await
        .expect_err("two files must exceed the startup budget");

    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
}

#[tokio::test]
async fn verification_byte_budget_fails_before_file_read() {
    let fixture = Fixture::new().await;
    fixture.create_table(TABLE, 2, 1).await;
    let candidate = fixture.write_candidate(TABLE, 20, 0, vec![100]);
    fixture
        .commit_files(3, 2, TABLE, vec![candidate.into_pending()], 0)
        .await;
    let verifier = fixture
        .verifier_with(StartupCatalogFileVerifierConfig::new().with_max_verification_bytes(1));

    let error = verifier
        .verify_with_bootstrap()
        .await
        .expect_err("candidate must exceed the checksum byte budget");

    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
}
