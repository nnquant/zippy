use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::num::NonZeroU64;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use zippy_catalog::{canonical_schema_hash_from_ipc, SqliteCatalog, SqliteCatalogConfig};
use zippy_core::OwnedDataDirectory;
use zippy_storage::{verify_legacy_parquet_import, LegacyParquetImportSpec};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogRead,
    CatalogWrite, CreateNamespaceCommand, LegacyCatalogImportCommandV2,
    LegacyCatalogImportEnvelopeV2,
};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, ExternalDataPath, MetadataMap, SchemaIpcPayload,
    SnapshotOperation, TableDefinition, TableRef,
};
use zippy_types::{ErrorCode, NamespaceId, OffsetWatermark, RequestId, TableId};

const NAMESPACE: u128 = 10;
const TABLE: u128 = 20;

fn namespace_id() -> NamespaceId {
    NamespaceId::from_u128(NAMESPACE).unwrap()
}

fn table_id() -> TableId {
    TableId::from_u128(TABLE).unwrap()
}

fn request_id(value: u128) -> RequestId {
    RequestId::from_u128(value).unwrap()
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("price", DataType::Int64, false).with_metadata(HashMap::from([
                ("currency".to_owned(), "CNY".to_owned()),
                ("unit".to_owned(), "price_tick".to_owned()),
            ])),
            Field::new("instrument", DataType::Utf8, false),
        ],
        HashMap::from([
            ("source".to_owned(), "legacy-registry".to_owned()),
            ("timezone".to_owned(), "UTC".to_owned()),
        ]),
    ))
}

fn schema_ipc(schema: &Schema) -> SchemaIpcPayload {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, schema).unwrap();
    writer.finish().unwrap();
    drop(writer);
    SchemaIpcPayload::new(bytes).unwrap()
}

fn write_parquet(path: &std::path::Path, schema: Arc<Schema>) {
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103])),
            Arc::new(StringArray::from(vec!["au", "ag", "cu"])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn ownership(root: &TempDir) -> Arc<OwnedDataDirectory> {
    Arc::new(OwnedDataDirectory::acquire_or_initialize(root.path().join("catalog-data")).unwrap())
}

fn migration_actor() -> CatalogActor {
    CatalogActor {
        kind: CatalogActorKind::Migration,
        principal: "zippy-storage/legacy-import-test".to_owned(),
    }
}

async fn create_namespace(catalog: &SqliteCatalog) {
    let command = CatalogCommandEnvelopeV1::new(
        request_id(1),
        Some(0),
        migration_actor(),
        CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
            namespace_id: namespace_id(),
            name: CatalogName::new("legacy").unwrap(),
        }),
    )
    .unwrap();
    catalog.command(command.into()).await.unwrap();
}

fn verified_file(
    legacy_root: &std::path::Path,
    path: &std::path::Path,
    schema: Arc<Schema>,
) -> zippy_types::catalog::LegacyCatalogImportFileV2 {
    verify_legacy_parquet_import(LegacyParquetImportSpec {
        table_id: table_id(),
        schema_version: 1,
        path: ExternalDataPath::new(path.to_str().unwrap()).unwrap(),
        allowed_root: Some(legacy_root.to_path_buf()),
        expected_schema: schema,
        expected_row_count: NonZeroU64::new(3),
        expected_checksum: None,
        discovery_order: 0,
        partition: MetadataMap::new(),
        statistics: MetadataMap::new(),
        metadata: MetadataMap::new(),
    })
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn imports_real_legacy_parquet_and_replays_exactly_after_reopen() {
    let root = TempDir::new().unwrap();
    let legacy_root = root.path().join("legacy");
    std::fs::create_dir(&legacy_root).unwrap();
    let parquet_path = legacy_root.join("events.parquet");
    let schema = schema();
    write_parquet(&parquet_path, Arc::clone(&schema));
    let schema_ipc = schema_ipc(&schema);
    let canonical_schema_hash = canonical_schema_hash_from_ipc(&schema_ipc).unwrap();
    let file = verified_file(&legacy_root, &parquet_path, schema);
    assert_eq!(
        file.file.checksum,
        format!(
            "sha256-v1:{:x}",
            Sha256::digest(std::fs::read(&parquet_path).unwrap())
        )
    );
    let command = LegacyCatalogImportEnvelopeV2::new(
        request_id(2),
        Some(1),
        migration_actor(),
        format!("sha256-v1:{}", "1".repeat(64)),
        7,
        LegacyCatalogImportCommandV2 {
            table_id: table_id(),
            namespace_id: namespace_id(),
            name: CatalogName::new("events").unwrap(),
            definition: TableDefinition::Append(AppendTableDefinition::default()),
            schema_ipc,
            canonical_schema_hash,
            config: BTreeMap::new(),
            writer_epoch: 9,
            files: vec![file.clone()],
            legacy_files_revision: 4,
        },
    )
    .unwrap();

    let owned = ownership(&root);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).unwrap();
    create_namespace(&catalog).await;
    let session = catalog.begin_legacy_import_session().unwrap();
    let first = session
        .import_legacy_table_v2(command.clone())
        .await
        .unwrap();
    assert_eq!(first.catalog_revision, 2);
    assert_eq!(first.table.current_snapshot_version, 2);
    assert_eq!(first.table.writer_epoch, 9);
    assert_eq!(first.snapshot.operation, SnapshotOperation::Import);
    assert_eq!(
        first.snapshot.committed_log_offset,
        OffsetWatermark::LegacyUnknown
    );
    assert_eq!(first.snapshot.parent_snapshot_version, Some(1));
    session.seal();
    catalog.close().unwrap();

    let reopened = SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).unwrap();
    let replay_session = reopened.begin_legacy_import_session().unwrap();
    let replay = replay_session
        .import_legacy_table_v2(command)
        .await
        .unwrap();
    assert_eq!(replay, first);
    assert_eq!(reopened.revision().await.unwrap(), 2);
    let table = reopened
        .get_table(TableRef::from(table_id()))
        .await
        .unwrap();
    assert_eq!(table.current_snapshot_version, 2);
    assert_eq!(table.writer_epoch, 9);
    replay_session.seal();

    let conflicting_v1 = CatalogCommandEnvelopeV1::new(
        request_id(2),
        Some(2),
        migration_actor(),
        CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
            namespace_id: NamespaceId::from_u128(11).unwrap(),
            name: CatalogName::new("conflicting-v1").unwrap(),
        }),
    )
    .unwrap();
    let conflict = reopened
        .command(conflicting_v1.into())
        .await
        .expect_err("V2 request identity must conflict in the V1 replay table");
    assert_eq!(conflict.code(), ErrorCode::Conflict);
    assert_eq!(reopened.revision().await.unwrap(), 2);
    reopened.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_schema_proof_mismatch_before_catalog_commit() {
    let root = TempDir::new().unwrap();
    let legacy_root = root.path().join("legacy");
    std::fs::create_dir(&legacy_root).unwrap();
    let parquet_path = legacy_root.join("events.parquet");
    let physical_schema = schema();
    write_parquet(&parquet_path, Arc::clone(&physical_schema));
    let file = verified_file(&legacy_root, &parquet_path, physical_schema);

    let catalog_schema = Arc::new(Schema::new(vec![Field::new(
        "different",
        DataType::Int64,
        false,
    )]));
    let schema_ipc = schema_ipc(&catalog_schema);
    let canonical_schema_hash = canonical_schema_hash_from_ipc(&schema_ipc).unwrap();
    let command = LegacyCatalogImportEnvelopeV2::new(
        request_id(2),
        Some(1),
        migration_actor(),
        format!("sha256-v1:{}", "4".repeat(64)),
        8,
        LegacyCatalogImportCommandV2 {
            table_id: table_id(),
            namespace_id: namespace_id(),
            name: CatalogName::new("events").unwrap(),
            definition: TableDefinition::Append(AppendTableDefinition::default()),
            schema_ipc,
            canonical_schema_hash,
            config: BTreeMap::new(),
            writer_epoch: 1,
            files: vec![file],
            legacy_files_revision: 1,
        },
    )
    .unwrap();

    let owned = ownership(&root);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).unwrap();
    create_namespace(&catalog).await;
    let session = catalog.begin_legacy_import_session().unwrap();
    let error = session
        .import_legacy_table_v2(command)
        .await
        .expect_err("physical Schema proof must bind the imported Catalog Schema");
    assert_eq!(error.code(), ErrorCode::SchemaMismatch);
    assert_eq!(catalog.revision().await.unwrap(), 1);
    session.seal();
    catalog.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v1_request_identity_conflicts_before_v2_import() {
    let root = TempDir::new().unwrap();
    let legacy_root = root.path().join("legacy");
    std::fs::create_dir(&legacy_root).unwrap();
    let parquet_path = legacy_root.join("events.parquet");
    let expected_schema = schema();
    write_parquet(&parquet_path, Arc::clone(&expected_schema));
    let schema_ipc = schema_ipc(&expected_schema);
    let canonical_schema_hash = canonical_schema_hash_from_ipc(&schema_ipc).unwrap();
    let file = verified_file(&legacy_root, &parquet_path, expected_schema);

    let owned = ownership(&root);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).unwrap();
    create_namespace(&catalog).await;
    let v1 = CatalogCommandEnvelopeV1::new(
        request_id(2),
        Some(1),
        migration_actor(),
        CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
            namespace_id: NamespaceId::from_u128(12).unwrap(),
            name: CatalogName::new("v1-first").unwrap(),
        }),
    )
    .unwrap();
    catalog.command(v1.into()).await.unwrap();

    let command = LegacyCatalogImportEnvelopeV2::new(
        request_id(2),
        Some(2),
        migration_actor(),
        format!("sha256-v1:{}", "5".repeat(64)),
        9,
        LegacyCatalogImportCommandV2 {
            table_id: table_id(),
            namespace_id: namespace_id(),
            name: CatalogName::new("events").unwrap(),
            definition: TableDefinition::Append(AppendTableDefinition::default()),
            schema_ipc,
            canonical_schema_hash,
            config: BTreeMap::new(),
            writer_epoch: 1,
            files: vec![file],
            legacy_files_revision: 1,
        },
    )
    .unwrap();
    let session = catalog.begin_legacy_import_session().unwrap();
    let error = session
        .import_legacy_table_v2(command)
        .await
        .expect_err("V1 request identity must conflict in the V2 replay table");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert_eq!(catalog.revision().await.unwrap(), 2);
    session.seal();
    catalog.close().unwrap();
}

#[test]
fn missing_legacy_file_is_blocked_before_catalog_admission() {
    let root = TempDir::new().unwrap();
    let schema = schema();
    let missing = root.path().join("missing.parquet");
    let error = verify_legacy_parquet_import(LegacyParquetImportSpec {
        table_id: table_id(),
        schema_version: 1,
        path: ExternalDataPath::new(missing.to_str().unwrap()).unwrap(),
        allowed_root: Some(root.path().to_path_buf()),
        expected_schema: schema,
        expected_row_count: None,
        expected_checksum: None,
        discovery_order: 0,
        partition: MetadataMap::new(),
        statistics: MetadataMap::new(),
        metadata: MetadataMap::new(),
    })
    .unwrap_err();
    assert_eq!(error.code(), ErrorCode::DataCorruption);
}

#[cfg(unix)]
#[test]
fn symbolic_link_legacy_file_is_blocked_before_catalog_admission() {
    use std::os::unix::fs::symlink;

    let root = TempDir::new().unwrap();
    let schema = schema();
    let target = root.path().join("target.parquet");
    write_parquet(&target, Arc::clone(&schema));
    let link = root.path().join("alias.parquet");
    symlink(&target, &link).unwrap();
    let error = verify_legacy_parquet_import(LegacyParquetImportSpec {
        table_id: table_id(),
        schema_version: 1,
        path: ExternalDataPath::new(link.to_str().unwrap()).unwrap(),
        allowed_root: Some(root.path().to_path_buf()),
        expected_schema: schema,
        expected_row_count: None,
        expected_checksum: None,
        discovery_order: 0,
        partition: MetadataMap::new(),
        statistics: MetadataMap::new(),
        metadata: MetadataMap::new(),
    })
    .unwrap_err();
    assert_eq!(error.code(), ErrorCode::DataCorruption);
}
