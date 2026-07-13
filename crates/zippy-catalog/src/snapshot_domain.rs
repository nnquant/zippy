//! Exact, revision-consistent Catalog snapshot reads.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU64;

use rusqlite::types::FromSql;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension, TransactionBehavior};
use zippy_types::catalog::{CatalogQuerySnapshot, CatalogSnapshotSelector, CatalogTableSnapshot};
use zippy_types::table::{
    DataFileDescriptor, DataFileLocation, DataFileProvenance, DataFileState, DataFiles,
    ExternalDataPath, ManagedDataPath, MetadataMap, NativeDataFileProvenance, PendingDataFile,
    SchemaCompatibility, SchemaDescriptor, SchemaIpcPayload, SnapshotOperation,
    SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages, TableDescriptor,
    TableSnapshot,
};
use zippy_types::{ErrorCode, FileId, LogOffset, OffsetRange, Result, ServerError, TableId};

use crate::encoding::{decode_offset_watermark, decode_optional_u64, decode_u64};
use crate::storage_codec::{
    canonical_schema_hash, decode_data_file_metadata_blob, decode_data_file_partition_blob,
    decode_data_file_statistics_blob, decode_schema_ipc_stream, decode_snapshot_summary_blob,
};

const MAX_SNAPSHOT_SELECTORS: usize = 4_096;
const MAX_VISIBLE_FILES: usize = 65_536;
const MAX_SOURCE_LINEAGE_ROWS: usize = 262_144;
const LINEAGE_QUERY_CHUNK_SIZE: usize = 512;
const MAX_SNAPSHOT_RAW_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const TABLE_ROW_LENGTHS_SQL: &str =
    "SELECT length(table_id), length(namespace_id), length(CAST(name AS BLOB)), \
            length(CAST(table_kind AS BLOB)), length(definition_blob), \
            length(current_schema_version), length(current_snapshot_version), \
            length(writer_epoch), length(CAST(state AS BLOB)), length(config_blob), \
            length(created_at_ns), length(updated_at_ns) \
       FROM tables WHERE table_id = ?1";
const GET_SCHEMA_SQL: &str =
    "SELECT length(table_id), length(schema_version), length(schema_ipc), \
            length(CAST(schema_hash AS BLOB)), length(CAST(compatibility AS BLOB)), \
            length(created_at_ns), table_id, schema_version, schema_ipc, schema_hash, \
            compatibility, created_at_ns \
       FROM table_schemas WHERE table_id = ?1 AND schema_version = ?2";
const GET_SNAPSHOT_SQL: &str =
    "SELECT length(table_id), length(snapshot_version), length(parent_snapshot_version), \
            length(CAST(operation AS BLOB)), length(commit_lsn), \
            length(committed_log_offset), length(created_at_ns), length(summary_blob), \
            table_id, snapshot_version, parent_snapshot_version, operation, commit_lsn, \
            committed_log_offset, created_at_ns, summary_blob \
       FROM table_snapshots WHERE table_id = ?1 AND snapshot_version = ?2";
const GET_VISIBLE_FILES_SQL: &str =
    "SELECT length(file_id), length(table_id), length(schema_version), \
            length(CAST(location_kind AS BLOB)), length(CAST(file_path AS BLOB)), \
            length(file_size_bytes), length(row_count), \
            length(CAST(provenance_kind AS BLOB)), length(min_log_offset), \
            length(max_log_offset), 1, length(legacy_discovery_order), \
            length(partition_blob), length(statistics_blob), length(metadata_blob), \
            length(CAST(checksum AS BLOB)), length(added_snapshot_version), \
            length(removed_snapshot_version), length(CAST(state AS BLOB)), \
            length(created_at_ns), file_id, table_id, schema_version, location_kind, \
            file_path, file_size_bytes, row_count, provenance_kind, min_log_offset, \
            max_log_offset, legacy_preserve_discovery_order, legacy_discovery_order, \
            partition_blob, statistics_blob, metadata_blob, checksum, \
            added_snapshot_version, removed_snapshot_version, state, created_at_ns \
       FROM data_files INDEXED BY data_files_visible_by_table \
      WHERE table_id = ?1 AND state = 'committed' \
        AND added_snapshot_version <= ?2 \
        AND (removed_snapshot_version = X'000000000000000000' \
             OR removed_snapshot_version > ?3)";

struct SnapshotReadBudget {
    raw_bytes: usize,
    files: usize,
    lineage_rows: usize,
}

impl SnapshotReadBudget {
    fn charge_bytes(&mut self, additional: usize) -> Result<()> {
        let actual = self.raw_bytes.checked_add(additional).ok_or_else(|| {
            resource_exhausted("catalog snapshot raw response byte count overflow")
        })?;
        if actual > MAX_SNAPSHOT_RAW_RESPONSE_BYTES {
            return Err(resource_exhausted(format!(
                "catalog snapshot raw response exceeds byte budget actual=[{}] maximum=[{}]",
                actual, MAX_SNAPSHOT_RAW_RESPONSE_BYTES
            )));
        }
        self.raw_bytes = actual;
        Ok(())
    }

    fn charge_file(&mut self, raw_bytes: usize) -> Result<()> {
        self.files = self
            .files
            .checked_add(1)
            .ok_or_else(|| resource_exhausted("catalog snapshot file count overflow"))?;
        if self.files > MAX_VISIBLE_FILES {
            return Err(resource_exhausted(format!(
                "catalog snapshot visible file count exceeds maximum actual=[{}] maximum=[{}]",
                self.files, MAX_VISIBLE_FILES
            )));
        }
        self.charge_bytes(raw_bytes)
    }

    fn charge_lineage(&mut self, raw_bytes: usize) -> Result<()> {
        self.lineage_rows = self
            .lineage_rows
            .checked_add(1)
            .ok_or_else(|| resource_exhausted("catalog snapshot lineage row count overflow"))?;
        if self.lineage_rows > MAX_SOURCE_LINEAGE_ROWS {
            return Err(resource_exhausted(format!(
                "catalog snapshot lineage row count exceeds maximum actual=[{}] maximum=[{}]",
                self.lineage_rows, MAX_SOURCE_LINEAGE_ROWS
            )));
        }
        self.charge_bytes(raw_bytes)
    }
}

struct PendingTableSnapshot {
    selector: CatalogSnapshotSelector,
    table: TableDescriptor,
    schema: SchemaDescriptor,
    snapshot: TableSnapshot,
    files: Vec<PendingVisibleFile>,
}

enum PendingFileProvenance {
    Native { offsets: OffsetRange },
    Legacy { offsets: Option<OffsetRange> },
}

struct PendingVisibleFile {
    file_id: FileId,
    table_id: TableId,
    schema_version: u64,
    location: DataFileLocation,
    file_size_bytes: u64,
    row_count: NonZeroU64,
    provenance: PendingFileProvenance,
    partition: MetadataMap,
    statistics: MetadataMap,
    metadata: MetadataMap,
    checksum: String,
    created_at_ns: u64,
    added_snapshot_version: u64,
    removed_snapshot_version: Option<u64>,
    state: DataFileState,
}

/// Validates deterministic selector ordering before a request is copied into the actor queue.
pub(crate) fn validate_snapshot_selectors(selectors: &[CatalogSnapshotSelector]) -> Result<()> {
    if selectors.len() > MAX_SNAPSHOT_SELECTORS {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "catalog snapshot selector count exceeds maximum actual=[{}] maximum=[{}]",
                selectors.len(),
                MAX_SNAPSHOT_SELECTORS
            ),
        ));
    }
    if let Some(selector) = selectors
        .iter()
        .find(|selector| selector.schema_version == 0 || selector.snapshot_version == 0)
    {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                concat!(
                    "catalog snapshot selector versions must be nonzero ",
                    "table_id=[{}] schema_version=[{}] snapshot_version=[{}]"
                ),
                selector.table_id, selector.schema_version, selector.snapshot_version
            ),
        ));
    }
    for pair in selectors.windows(2) {
        match pair[0].table_id.cmp(&pair[1].table_id) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => {
                return Err(ServerError::new(
                    ErrorCode::InvalidArgument,
                    format!(
                        "catalog snapshot selectors contain duplicate table id table_id=[{}]",
                        pair[0].table_id
                    ),
                ));
            }
            std::cmp::Ordering::Greater => {
                return Err(ServerError::new(
                    ErrorCode::InvalidArgument,
                    "catalog snapshot selectors must be strictly sorted by table id",
                ));
            }
        }
    }
    Ok(())
}

/// Reads exact selector-bound Catalog state from one SQLite read transaction.
pub(crate) fn read_snapshot_at(
    connection: &mut Connection,
    selectors: &[CatalogSnapshotSelector],
) -> Result<CatalogQuerySnapshot> {
    validate_snapshot_selectors(selectors)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|error| crate::map_sqlite_error("begin catalog snapshot read", error))?;
    let catalog_revision = crate::domain::revision(&transaction)?;
    let mut budget = SnapshotReadBudget {
        raw_bytes: 0,
        files: 0,
        lineage_rows: 0,
    };
    let mut pending_tables = Vec::with_capacity(selectors.len());
    let mut visible_file_ids = Vec::new();
    let mut unique_file_ids = HashSet::new();
    for selector in selectors {
        let table = read_table(&transaction, selector.table_id, &mut budget)?;
        validate_selector_bounds(selector, &table)?;
        let schema = read_schema(&transaction, selector, &mut budget)?;
        let snapshot = read_snapshot(&transaction, selector, catalog_revision, &mut budget)?;
        let files = read_visible_files(&transaction, selector, &mut budget)?;
        if snapshot.operation == SnapshotOperation::Create && !files.is_empty() {
            return Err(corruption(
                "initial create snapshot must not contain visible data files",
            ));
        }
        for file in &files {
            if !unique_file_ids.insert(file.file_id) {
                return Err(corruption(format!(
                    "catalog snapshot returned duplicate visible file id file_id=[{}]",
                    file.file_id
                )));
            }
            visible_file_ids.push(file.file_id);
        }
        pending_tables.push(PendingTableSnapshot {
            selector: selector.clone(),
            table,
            schema,
            snapshot,
            files,
        });
    }
    let mut lineage = read_source_lineage(&transaction, &visible_file_ids, &mut budget)?;
    let mut tables = Vec::with_capacity(pending_tables.len());
    for pending in pending_tables {
        let mut files = pending
            .files
            .into_iter()
            .map(|file| {
                let source_rows = lineage.remove(&file.file_id).unwrap_or_default();
                file.into_descriptor(source_rows)
            })
            .collect::<Result<Vec<_>>>()?;
        files.sort_unstable_by_key(DataFileDescriptor::file_id);
        let files = DataFiles::new(files)
            .map_err(|error| corruption(format!("invalid catalog data files error=[{error}]")))?;
        tables.push(
            CatalogTableSnapshot::new(
                pending.selector,
                pending.table,
                pending.schema,
                pending.snapshot,
                files,
            )
            .map_err(|error| {
                corruption(format!("invalid catalog table snapshot error=[{error}]"))
            })?,
        );
    }
    if !lineage.is_empty() {
        return Err(corruption(
            "catalog snapshot source lineage was not assigned to a visible file",
        ));
    }
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit catalog snapshot read", error))?;
    CatalogQuerySnapshot::new(catalog_revision, tables)
        .map_err(|error| corruption(format!("invalid catalog query snapshot error=[{error}]")))
}

fn read_table(
    connection: &Connection,
    table_id: TableId,
    budget: &mut SnapshotReadBudget,
) -> Result<TableDescriptor> {
    let encoded_table_id = table_id.into_bytes();
    let lengths = connection
        .query_row(
            TABLE_ROW_LENGTHS_SQL,
            [encoded_table_id.as_slice()],
            |row| {
                Ok([
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, i64>(11)?,
                ])
            },
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read catalog snapshot table lengths", error))?
        .ok_or_else(|| not_found("catalog snapshot table was not found"))?;
    let raw_bytes = sum_lengths(&lengths, "table")?;
    budget.charge_bytes(raw_bytes)?;

    let raw = connection
        .query_row(
            crate::table_domain::GET_TABLE_BY_ID_INCLUDING_INACTIVE_SQL,
            [encoded_table_id.as_slice()],
            crate::table_domain::table_row,
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read catalog snapshot table", error))?
        .ok_or_else(|| corruption("catalog snapshot table disappeared inside read transaction"))?;
    if raw.encoded_size_bytes() != raw_bytes {
        return Err(corruption(
            "catalog snapshot table byte length changed inside read transaction",
        ));
    }
    crate::table_domain::decode_table_row(raw)
}

fn validate_selector_bounds(
    selector: &CatalogSnapshotSelector,
    table: &TableDescriptor,
) -> Result<()> {
    if selector.schema_version > table.current_schema_version {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            format!(
                "catalog snapshot schema selector is ahead of table current selected=[{}] current=[{}]",
                selector.schema_version, table.current_schema_version
            ),
        ));
    }
    if selector.snapshot_version > table.current_snapshot_version {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            format!(
                "catalog snapshot selector is ahead of table current selected=[{}] current=[{}]",
                selector.snapshot_version, table.current_snapshot_version
            ),
        ));
    }
    Ok(())
}

fn read_schema(
    connection: &Connection,
    selector: &CatalogSnapshotSelector,
    budget: &mut SnapshotReadBudget,
) -> Result<SchemaDescriptor> {
    let table_id = selector.table_id.into_bytes();
    let schema_version = selector.schema_version.to_be_bytes();
    let mut statement = connection
        .prepare_cached(GET_SCHEMA_SQL)
        .map_err(|error| crate::map_sqlite_error("prepare exact catalog schema", error))?;
    let mut rows = statement
        .query([table_id.as_slice(), schema_version.as_slice()])
        .map_err(|error| crate::map_sqlite_error("query exact catalog schema", error))?;
    let row = rows
        .next()
        .map_err(|error| crate::map_sqlite_error("read exact catalog schema", error))?
        .ok_or_else(|| not_found("exact catalog schema was not found"))?;
    budget.charge_bytes(row_lengths(row, 0, 6, "schema")?)?;

    let raw_table_id: Vec<u8> = row
        .get(6)
        .map_err(|error| crate::map_sqlite_error("decode catalog schema table id", error))?;
    let raw_schema_version: Vec<u8> = row
        .get(7)
        .map_err(|error| crate::map_sqlite_error("decode catalog schema version", error))?;
    let schema_ipc: Vec<u8> = row
        .get(8)
        .map_err(|error| crate::map_sqlite_error("decode catalog schema IPC", error))?;
    let schema_hash: String = row
        .get(9)
        .map_err(|error| crate::map_sqlite_error("decode catalog schema hash", error))?;
    let compatibility: String = row
        .get(10)
        .map_err(|error| crate::map_sqlite_error("decode catalog schema compatibility", error))?;
    let created_at_ns: Vec<u8> = row
        .get(11)
        .map_err(|error| crate::map_sqlite_error("decode catalog schema timestamp", error))?;

    let decoded_table_id = decode_table_id(raw_table_id, "catalog schema table id")?;
    let decoded_version = decode_u64(&raw_schema_version)?;
    if decoded_table_id != selector.table_id || decoded_version != selector.schema_version {
        return Err(corruption(
            "exact catalog schema identity does not match selector",
        ));
    }
    if decoded_version == 0 {
        return Err(corruption("catalog schema version must be nonzero"));
    }
    let compatibility = decode_schema_compatibility(&compatibility)?;
    if (decoded_version == 1) != (compatibility == SchemaCompatibility::Initial) {
        return Err(corruption(
            "catalog schema initial compatibility does not match its version",
        ));
    }
    let decoded_schema = decode_schema_ipc_stream(&schema_ipc).map_err(|error| {
        corruption(format!(
            "persisted catalog schema IPC is invalid error=[{error}]"
        ))
    })?;
    let actual_hash = canonical_schema_hash(&decoded_schema).map_err(|error| {
        corruption(format!(
            "persisted catalog schema cannot be hashed error=[{error}]"
        ))
    })?;
    if actual_hash != schema_hash {
        return Err(corruption(format!(
            "catalog schema hash mismatch expected=[{}] actual=[{}]",
            schema_hash, actual_hash
        )));
    }
    let schema_ipc = SchemaIpcPayload::new(schema_ipc)
        .map_err(|error| corruption(format!("catalog schema IPC is invalid error=[{error}]")))?;
    Ok(SchemaDescriptor {
        table_id: decoded_table_id,
        schema_version: decoded_version,
        schema_ipc,
        schema_hash,
        compatibility,
        created_at_ns: decode_u64(&created_at_ns)?,
    })
}

fn read_snapshot(
    connection: &Connection,
    selector: &CatalogSnapshotSelector,
    catalog_revision: u64,
    budget: &mut SnapshotReadBudget,
) -> Result<TableSnapshot> {
    let table_id = selector.table_id.into_bytes();
    let snapshot_version = selector.snapshot_version.to_be_bytes();
    let mut statement = connection
        .prepare_cached(GET_SNAPSHOT_SQL)
        .map_err(|error| crate::map_sqlite_error("prepare exact catalog snapshot", error))?;
    let mut rows = statement
        .query([table_id.as_slice(), snapshot_version.as_slice()])
        .map_err(|error| crate::map_sqlite_error("query exact catalog snapshot", error))?;
    let row = rows
        .next()
        .map_err(|error| crate::map_sqlite_error("read exact catalog snapshot", error))?
        .ok_or_else(|| not_found("exact catalog snapshot was not found"))?;
    budget.charge_bytes(row_lengths(row, 0, 8, "snapshot")?)?;

    let raw_table_id: Vec<u8> = row
        .get(8)
        .map_err(|error| crate::map_sqlite_error("decode catalog snapshot table id", error))?;
    let raw_snapshot_version: Vec<u8> = row
        .get(9)
        .map_err(|error| crate::map_sqlite_error("decode catalog snapshot version", error))?;
    let parent_snapshot_version: Vec<u8> = row
        .get(10)
        .map_err(|error| crate::map_sqlite_error("decode catalog snapshot parent", error))?;
    let operation: String = row
        .get(11)
        .map_err(|error| crate::map_sqlite_error("decode catalog snapshot operation", error))?;
    let commit_lsn: Vec<u8> = row
        .get(12)
        .map_err(|error| crate::map_sqlite_error("decode catalog snapshot commit lsn", error))?;
    let committed_log_offset: Vec<u8> = row
        .get(13)
        .map_err(|error| crate::map_sqlite_error("decode catalog snapshot log offset", error))?;
    let created_at_ns: Vec<u8> = row
        .get(14)
        .map_err(|error| crate::map_sqlite_error("decode catalog snapshot timestamp", error))?;
    let summary_blob: Vec<u8> = row
        .get(15)
        .map_err(|error| crate::map_sqlite_error("decode catalog snapshot summary", error))?;

    let decoded_table_id = decode_table_id(raw_table_id, "catalog snapshot table id")?;
    let decoded_version = decode_u64(&raw_snapshot_version)?;
    if decoded_table_id != selector.table_id || decoded_version != selector.snapshot_version {
        return Err(corruption(
            "exact catalog snapshot identity does not match selector",
        ));
    }
    if decoded_version == 0 {
        return Err(corruption("catalog snapshot version must be nonzero"));
    }
    let parent_snapshot_version = decode_optional_u64(&parent_snapshot_version)?;
    let operation = decode_snapshot_operation(&operation)?;
    validate_snapshot_parent(decoded_version, parent_snapshot_version, operation)?;
    let commit_lsn = decode_u64(&commit_lsn)?;
    if commit_lsn == 0 {
        return Err(corruption("catalog snapshot commit lsn must be nonzero"));
    }
    if commit_lsn > catalog_revision {
        return Err(corruption(format!(
            "catalog snapshot commit lsn exceeds read revision commit_lsn=[{}] revision=[{}]",
            commit_lsn, catalog_revision
        )));
    }
    Ok(TableSnapshot {
        table_id: decoded_table_id,
        snapshot_version: decoded_version,
        parent_snapshot_version,
        operation,
        commit_lsn,
        committed_log_offset: decode_offset_watermark(&committed_log_offset)?,
        created_at_ns: decode_u64(&created_at_ns)?,
        summary: decode_snapshot_summary_blob(&summary_blob)?,
    })
}

fn read_visible_files(
    connection: &Connection,
    selector: &CatalogSnapshotSelector,
    budget: &mut SnapshotReadBudget,
) -> Result<Vec<PendingVisibleFile>> {
    let table_id = selector.table_id.into_bytes();
    let snapshot_version = selector.snapshot_version.to_be_bytes();
    let removed_after = crate::encoding::encode_optional_u64(Some(selector.snapshot_version));
    let mut statement = connection
        .prepare_cached(GET_VISIBLE_FILES_SQL)
        .map_err(|error| crate::map_sqlite_error("prepare visible catalog files", error))?;
    let mut rows = statement
        .query(params![
            table_id.as_slice(),
            snapshot_version.as_slice(),
            removed_after.as_slice()
        ])
        .map_err(|error| crate::map_sqlite_error("query visible catalog files", error))?;
    let mut files = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| crate::map_sqlite_error("read visible catalog file", error))?
    {
        budget.charge_file(row_lengths(row, 0, 20, "data file")?)?;
        let file = decode_visible_file_row(row)?;
        if file.table_id != selector.table_id {
            return Err(corruption(
                "visible catalog file table identity does not match selector",
            ));
        }
        files.push(file);
    }
    Ok(files)
}

fn decode_visible_file_row(row: &rusqlite::Row<'_>) -> Result<PendingVisibleFile> {
    let file_id = decode_file_id(row_value(row, 20, "decode catalog file id")?)?;
    let table_id = decode_table_id(
        row_value(row, 21, "decode catalog file table id")?,
        "catalog file table id",
    )?;
    let schema_version = decode_u64(&row_value::<Vec<u8>>(
        row,
        22,
        "decode catalog file schema version",
    )?)?;
    if schema_version == 0 {
        return Err(corruption(
            "catalog data file schema version must be nonzero",
        ));
    }
    let location_kind: String = row_value(row, 23, "decode catalog file location kind")?;
    let file_path: String = row_value(row, 24, "decode catalog file path")?;
    let location = match location_kind.as_str() {
        "managed" => {
            DataFileLocation::Managed(ManagedDataPath::new(file_path).map_err(|error| {
                corruption(format!(
                    "catalog managed data path is invalid error=[{error}]"
                ))
            })?)
        }
        "external_legacy" => {
            DataFileLocation::ExternalLegacy(ExternalDataPath::new(file_path).map_err(|error| {
                corruption(format!(
                    "catalog external data path is invalid error=[{error}]"
                ))
            })?)
        }
        _ => {
            return Err(corruption(format!(
                "catalog data file location kind is invalid value=[{location_kind}]"
            )));
        }
    };
    let file_size_bytes = decode_u64(&row_value::<Vec<u8>>(row, 25, "decode catalog file size")?)?;
    let row_count = decode_u64(&row_value::<Vec<u8>>(
        row,
        26,
        "decode catalog file row count",
    )?)?;
    let row_count = NonZeroU64::new(row_count)
        .ok_or_else(|| corruption("catalog data file row count must be nonzero"))?;
    let provenance_kind: String = row_value(row, 27, "decode catalog file provenance kind")?;
    let min_log_offset = decode_optional_u64(&row_value::<Vec<u8>>(
        row,
        28,
        "decode catalog file minimum offset",
    )?)?;
    let max_log_offset = decode_optional_u64(&row_value::<Vec<u8>>(
        row,
        29,
        "decode catalog file maximum offset",
    )?)?;
    let preserve_discovery_order: i64 =
        row_value(row, 30, "decode catalog legacy discovery policy")?;
    let preserve_discovery_order = match preserve_discovery_order {
        0 => false,
        1 => true,
        value => {
            return Err(corruption(format!(
                "catalog legacy discovery policy is invalid value=[{value}]"
            )));
        }
    };
    let legacy_discovery_order = decode_optional_u64(&row_value::<Vec<u8>>(
        row,
        31,
        "decode catalog legacy discovery order",
    )?)?;
    let provenance = decode_file_provenance(
        &provenance_kind,
        min_log_offset,
        max_log_offset,
        preserve_discovery_order,
        legacy_discovery_order,
    )?;
    let partition_blob: Vec<u8> = row_value(row, 32, "decode catalog file partition")?;
    let statistics_blob: Vec<u8> = row_value(row, 33, "decode catalog file statistics")?;
    let metadata_blob: Vec<u8> = row_value(row, 34, "decode catalog file metadata")?;
    let checksum: String = row_value(row, 35, "decode catalog file checksum")?;
    if checksum.is_empty() || checksum != checksum.to_lowercase() {
        return Err(corruption("catalog data file checksum is not canonical"));
    }
    let added_snapshot_version = decode_u64(&row_value::<Vec<u8>>(
        row,
        36,
        "decode catalog file added snapshot",
    )?)?;
    if added_snapshot_version == 0 {
        return Err(corruption(
            "catalog data file added snapshot version must be nonzero",
        ));
    }
    let removed_snapshot_version = decode_optional_u64(&row_value::<Vec<u8>>(
        row,
        37,
        "decode catalog file removed snapshot",
    )?)?;
    let state: String = row_value(row, 38, "decode catalog file state")?;
    let state = match state.as_str() {
        "committed" => DataFileState::Committed,
        "deleting" => DataFileState::Deleting,
        "deleted" => DataFileState::Deleted,
        _ => {
            return Err(corruption(format!(
                "catalog data file state is invalid value=[{state}]"
            )));
        }
    };
    if state != DataFileState::Committed {
        return Err(corruption(
            "visible catalog data file is not in committed state",
        ));
    }
    let created_at_ns = decode_u64(&row_value::<Vec<u8>>(
        row,
        39,
        "decode catalog file timestamp",
    )?)?;
    Ok(PendingVisibleFile {
        file_id,
        table_id,
        schema_version,
        location,
        file_size_bytes,
        row_count,
        provenance,
        partition: decode_data_file_partition_blob(&partition_blob)?,
        statistics: decode_data_file_statistics_blob(&statistics_blob)?,
        metadata: decode_data_file_metadata_blob(&metadata_blob)?,
        checksum,
        created_at_ns,
        added_snapshot_version,
        removed_snapshot_version,
        state,
    })
}

fn decode_file_provenance(
    kind: &str,
    min_log_offset: Option<u64>,
    max_log_offset: Option<u64>,
    preserve_discovery_order: bool,
    legacy_discovery_order: Option<u64>,
) -> Result<PendingFileProvenance> {
    match kind {
        "native" => {
            if preserve_discovery_order || legacy_discovery_order.is_some() {
                return Err(corruption(
                    "native catalog data file contains legacy discovery metadata",
                ));
            }
            let offsets = offset_range(min_log_offset, max_log_offset, "native data file")?
                .ok_or_else(|| corruption("native catalog data file lacks logical offsets"))?;
            Ok(PendingFileProvenance::Native { offsets })
        }
        "legacy" => {
            if preserve_discovery_order != legacy_discovery_order.is_some() {
                return Err(corruption(
                    "legacy catalog discovery policy does not match its order value",
                ));
            }
            if preserve_discovery_order {
                return Err(ServerError::new(
                    ErrorCode::UnsupportedPlan,
                    "catalog V1 snapshot cannot represent preserved legacy discovery order",
                ));
            }
            Ok(PendingFileProvenance::Legacy {
                offsets: offset_range(min_log_offset, max_log_offset, "legacy data file")?,
            })
        }
        _ => Err(corruption(format!(
            "catalog data file provenance kind is invalid value=[{kind}]"
        ))),
    }
}

fn offset_range(
    first: Option<u64>,
    last: Option<u64>,
    context: &str,
) -> Result<Option<OffsetRange>> {
    match (first, last) {
        (None, None) => Ok(None),
        (Some(first), Some(last)) => OffsetRange::new(LogOffset::new(first), LogOffset::new(last))
            .map(Some)
            .map_err(|error| {
                corruption(format!(
                    "catalog {context} offset range is invalid error=[{error}]"
                ))
            }),
        _ => Err(corruption(format!(
            "catalog {context} offset endpoints have mismatched presence"
        ))),
    }
}

fn read_source_lineage(
    connection: &Connection,
    file_ids: &[FileId],
    budget: &mut SnapshotReadBudget,
) -> Result<HashMap<FileId, Vec<SourceSegmentLineage>>> {
    let visible_ids = file_ids.iter().copied().collect::<HashSet<_>>();
    let mut lineage = HashMap::<FileId, Vec<SourceSegmentLineage>>::new();
    for chunk in file_ids.chunks(LINEAGE_QUERY_CHUNK_SIZE) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            concat!(
                "SELECT length(file_id), length(source_segment_id), length(source_generation), ",
                "length(writer_epoch), length(min_log_offset), length(max_log_offset), ",
                "file_id, source_segment_id, source_generation, writer_epoch, ",
                "min_log_offset, max_log_offset FROM data_file_sources ",
                "WHERE file_id IN ({})"
            ),
            placeholders
        );
        let encoded_ids = chunk
            .iter()
            .map(|file_id| file_id.into_bytes())
            .collect::<Vec<_>>();
        let mut statement = connection.prepare(&sql).map_err(|error| {
            crate::map_sqlite_error("prepare catalog file lineage batch", error)
        })?;
        let mut rows = statement
            .query(params_from_iter(
                encoded_ids.iter().map(|file_id| file_id.as_slice()),
            ))
            .map_err(|error| crate::map_sqlite_error("query catalog file lineage batch", error))?;
        while let Some(row) = rows
            .next()
            .map_err(|error| crate::map_sqlite_error("read catalog file lineage", error))?
        {
            budget.charge_lineage(row_lengths(row, 0, 6, "source lineage")?)?;
            let file_id = decode_file_id(row_value(row, 6, "decode lineage file id")?)?;
            if !visible_ids.contains(&file_id) {
                return Err(corruption(
                    "catalog source lineage references a non-visible file",
                ));
            }
            let source = SourceSegmentIdentity {
                segment_id: decode_u64(&row_value::<Vec<u8>>(
                    row,
                    7,
                    "decode lineage segment id",
                )?)?,
                generation: decode_u64(&row_value::<Vec<u8>>(
                    row,
                    8,
                    "decode lineage generation",
                )?)?,
                writer_epoch: decode_u64(&row_value::<Vec<u8>>(
                    row,
                    9,
                    "decode lineage writer epoch",
                )?)?,
            };
            let first = decode_u64(&row_value::<Vec<u8>>(
                row,
                10,
                "decode lineage minimum offset",
            )?)?;
            let last = decode_u64(&row_value::<Vec<u8>>(
                row,
                11,
                "decode lineage maximum offset",
            )?)?;
            let offsets =
                OffsetRange::new(LogOffset::new(first), LogOffset::new(last)).map_err(|error| {
                    corruption(format!("catalog source lineage is invalid error=[{error}]"))
                })?;
            lineage
                .entry(file_id)
                .or_default()
                .push(SourceSegmentLineage { source, offsets });
        }
    }
    for rows in lineage.values_mut() {
        rows.sort_unstable_by_key(|row| row.source);
    }
    Ok(lineage)
}

impl PendingVisibleFile {
    fn into_descriptor(
        self,
        source_lineage: Vec<SourceSegmentLineage>,
    ) -> Result<DataFileDescriptor> {
        let provenance = match self.provenance {
            PendingFileProvenance::Native { offsets } => {
                let source_lineage =
                    SourceSegmentLineages::new(source_lineage).map_err(|error| {
                        corruption(format!(
                            "catalog native source lineage is invalid error=[{error}]"
                        ))
                    })?;
                DataFileProvenance::Native(
                    NativeDataFileProvenance::new(offsets, source_lineage).map_err(|error| {
                        corruption(format!(
                            "catalog native file provenance is invalid error=[{error}]"
                        ))
                    })?,
                )
            }
            PendingFileProvenance::Legacy { offsets } => {
                if !source_lineage.is_empty() {
                    return Err(corruption(
                        "legacy catalog data file contains native source lineage",
                    ));
                }
                DataFileProvenance::Legacy {
                    offsets,
                    preserve_discovery_order: false,
                }
            }
        };
        let descriptor = DataFileDescriptor {
            file: PendingDataFile {
                file_id: self.file_id,
                table_id: self.table_id,
                schema_version: self.schema_version,
                location: self.location,
                file_size_bytes: self.file_size_bytes,
                row_count: self.row_count,
                provenance,
                partition: self.partition,
                statistics: self.statistics,
                metadata: self.metadata,
                checksum: self.checksum,
                created_at_ns: self.created_at_ns,
            },
            added_snapshot_version: self.added_snapshot_version,
            removed_snapshot_version: self.removed_snapshot_version,
            state: self.state,
        };
        descriptor.validate().map_err(|error| {
            corruption(format!(
                "catalog data file descriptor is invalid error=[{error}]"
            ))
        })?;
        Ok(descriptor)
    }
}

fn validate_snapshot_parent(
    version: u64,
    parent: Option<u64>,
    operation: SnapshotOperation,
) -> Result<()> {
    if version == 1 {
        if parent.is_some() || operation != SnapshotOperation::Create {
            return Err(corruption(
                "initial catalog snapshot must be create with no parent",
            ));
        }
    } else if parent != version.checked_sub(1) || operation == SnapshotOperation::Create {
        return Err(corruption(concat!(
            "non-initial catalog snapshot must have its immediately preceding parent ",
            "and non-create operation"
        )));
    }
    Ok(())
}

fn decode_schema_compatibility(value: &str) -> Result<SchemaCompatibility> {
    match value {
        "initial" => Ok(SchemaCompatibility::Initial),
        "full" => Ok(SchemaCompatibility::Full),
        "backward" => Ok(SchemaCompatibility::Backward),
        "forward" => Ok(SchemaCompatibility::Forward),
        "breaking" => Ok(SchemaCompatibility::Breaking),
        _ => Err(corruption(format!(
            "catalog schema compatibility is invalid value=[{value}]"
        ))),
    }
}

fn decode_snapshot_operation(value: &str) -> Result<SnapshotOperation> {
    match value {
        "create" => Ok(SnapshotOperation::Create),
        "persist" => Ok(SnapshotOperation::Persist),
        "compaction" => Ok(SnapshotOperation::Compaction),
        "import" => Ok(SnapshotOperation::Import),
        "retention" => Ok(SnapshotOperation::Retention),
        _ => Err(corruption(format!(
            "catalog snapshot operation is invalid value=[{value}]"
        ))),
    }
}

fn row_lengths(
    row: &rusqlite::Row<'_>,
    start: usize,
    count: usize,
    context: &str,
) -> Result<usize> {
    let mut total = 0_usize;
    for index in start..start + count {
        let length: i64 = row.get(index).map_err(|error| {
            crate::map_sqlite_error("decode catalog snapshot field length", error)
        })?;
        let length = usize::try_from(length).map_err(|_| {
            corruption(format!(
                "catalog {context} field length is outside supported domain value=[{length}]"
            ))
        })?;
        total = total.checked_add(length).ok_or_else(|| {
            resource_exhausted(format!("catalog {context} raw byte count overflow"))
        })?;
    }
    Ok(total)
}

fn sum_lengths<const N: usize>(lengths: &[i64; N], context: &str) -> Result<usize> {
    let mut total = 0_usize;
    for length in lengths {
        let length = usize::try_from(*length).map_err(|_| {
            corruption(format!(
                "catalog {context} field length is outside supported domain value=[{length}]"
            ))
        })?;
        total = total.checked_add(length).ok_or_else(|| {
            resource_exhausted(format!("catalog {context} raw byte count overflow"))
        })?;
    }
    Ok(total)
}

fn decode_table_id(bytes: Vec<u8>, context: &str) -> Result<TableId> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        corruption(format!(
            "{context} has invalid width actual=[{}]",
            bytes.len()
        ))
    })?;
    TableId::from_bytes(bytes)
        .map_err(|error| corruption(format!("{context} is invalid error=[{error}]")))
}

fn decode_file_id(bytes: Vec<u8>) -> Result<FileId> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        corruption(format!(
            "catalog file id has invalid width actual=[{}]",
            bytes.len()
        ))
    })?;
    FileId::from_bytes(bytes)
        .map_err(|error| corruption(format!("catalog file id is invalid error=[{error}]")))
}

fn row_value<T>(row: &rusqlite::Row<'_>, index: usize, context: &str) -> Result<T>
where
    T: FromSql,
{
    row.get(index)
        .map_err(|error| crate::map_sqlite_error(context, error))
}

fn not_found(message: &'static str) -> ServerError {
    ServerError::new(ErrorCode::NotFound, message)
}

fn resource_exhausted(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message)
}

fn corruption(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use rusqlite::{params, Connection};
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1,
        CatalogSnapshotSelector, CreateNamespaceCommand, CreateTableCommand,
    };
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, DataFileProvenance, MetadataMap, MetadataScalar,
        SchemaIpcPayload, TableDefinition,
    };
    use zippy_types::{
        ErrorCode, FileId, LogOffset, NamespaceId, OffsetWatermark, RequestId, TableId,
    };

    use super::{read_snapshot_at, GET_VISIBLE_FILES_SQL};
    use crate::encoding::{encode_offset_watermark, encode_optional_u64, encode_u64};
    use crate::storage_codec::{
        encode_data_file_metadata_blob, encode_data_file_partition_blob,
        encode_data_file_statistics_blob, encode_snapshot_summary_blob,
    };

    #[derive(Clone, Copy)]
    enum TestFileKind {
        Native,
        Legacy,
    }

    fn selector(table_id: u128) -> CatalogSnapshotSelector {
        CatalogSnapshotSelector {
            table_id: TableId::from_u128(table_id).expect("nonzero table id"),
            snapshot_version: 1,
            schema_version: 1,
        }
    }

    fn file_id(value: u128) -> FileId {
        FileId::from_u128(value).expect("nonzero file id")
    }

    fn schema_ipc() -> SchemaIpcPayload {
        let schema = Schema::new(vec![Field::new("value", DataType::UInt64, false)]);
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, &schema)
            .expect("create catalog test schema writer");
        writer.finish().expect("finish catalog test schema stream");
        drop(writer);
        SchemaIpcPayload::new(payload).expect("valid catalog test schema IPC")
    }

    fn actor() -> CatalogActor {
        CatalogActor {
            kind: CatalogActorKind::Server,
            principal: "snapshot-domain-test".to_owned(),
        }
    }

    fn migrated_catalog() -> (Connection, CatalogSnapshotSelector) {
        let mut connection = Connection::open_in_memory().expect("open in-memory catalog");
        crate::migration::migrate(&mut connection).expect("migrate in-memory catalog");
        let namespace_id = NamespaceId::from_u128(1).expect("nonzero namespace id");
        let create_namespace = CatalogCommandEnvelopeV1::new(
            RequestId::from_u128(1).expect("nonzero request id"),
            Some(0),
            actor(),
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id,
                name: CatalogName::new("market").expect("valid namespace name"),
            }),
        )
        .expect("create namespace envelope");
        crate::domain::execute_command(&mut connection, create_namespace.into())
            .expect("create namespace");
        let create_table = CatalogCommandEnvelopeV1::new(
            RequestId::from_u128(2).expect("nonzero request id"),
            Some(1),
            actor(),
            CatalogCommandV1::CreateTable(CreateTableCommand {
                table_id: selector(1).table_id,
                namespace_id,
                name: CatalogName::new("ticks").expect("valid table name"),
                definition: TableDefinition::Append(AppendTableDefinition::default()),
                schema_ipc: schema_ipc(),
                config: BTreeMap::new(),
            }),
        )
        .expect("create table envelope");
        crate::domain::execute_command(&mut connection, create_table.into()).expect("create table");
        (connection, selector(1))
    }

    fn add_second_snapshot(connection: &Connection) -> CatalogSnapshotSelector {
        let table_id = selector(1).table_id.into_bytes();
        let summary =
            MetadataMap::from([("persisted_files".to_owned(), MetadataScalar::UInt64(2))]);
        connection
            .execute(
                "INSERT INTO table_snapshots(\
                     table_id, snapshot_version, parent_snapshot_version, operation, commit_lsn, \
                     committed_log_offset, created_at_ns, summary_blob\
                 ) VALUES (?1, ?2, ?3, 'persist', ?4, ?5, ?6, ?7)",
                params![
                    table_id.as_slice(),
                    encode_u64(2).as_slice(),
                    encode_optional_u64(Some(1)).as_slice(),
                    encode_u64(2).as_slice(),
                    encode_offset_watermark(OffsetWatermark::Known(LogOffset::new(11))).as_slice(),
                    encode_u64(20).as_slice(),
                    encode_snapshot_summary_blob(&summary).expect("encode snapshot summary"),
                ],
            )
            .expect("insert second snapshot");
        connection
            .execute(
                "UPDATE tables SET current_snapshot_version = ?1 WHERE table_id = ?2",
                params![encode_u64(2).as_slice(), table_id.as_slice()],
            )
            .expect("advance table snapshot version");
        CatalogSnapshotSelector {
            snapshot_version: 2,
            ..selector(1)
        }
    }

    fn insert_file(
        connection: &Connection,
        value: u128,
        added_snapshot_version: u64,
        kind: TestFileKind,
        offsets: Option<(u64, u64)>,
        metadata_blob: Vec<u8>,
    ) {
        let id = file_id(value).into_bytes();
        let table_id = selector(1).table_id.into_bytes();
        let (location_kind, path, provenance_kind, preserve_order, discovery_order) = match kind {
            TestFileKind::Native => (
                "managed",
                format!("tables/1/{value}.parquet"),
                "native",
                0_i64,
                None,
            ),
            TestFileKind::Legacy => (
                "external_legacy",
                format!("/legacy/{value}.parquet"),
                "legacy",
                0_i64,
                None,
            ),
        };
        let row_count = offsets.map(|(first, last)| last - first + 1).unwrap_or(1);
        connection
            .execute(
                "INSERT INTO data_files(\
                     file_id, table_id, schema_version, location_kind, file_path, \
                     file_size_bytes, row_count, provenance_kind, min_log_offset, \
                     max_log_offset, legacy_preserve_discovery_order, legacy_discovery_order, \
                     partition_blob, statistics_blob, metadata_blob, checksum, \
                     added_snapshot_version, removed_snapshot_version, state, created_at_ns\
                 ) VALUES (\
                     ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, \
                     ?15, ?16, ?17, ?18, 'committed', ?19\
                 )",
                params![
                    id.as_slice(),
                    table_id.as_slice(),
                    encode_u64(1).as_slice(),
                    location_kind,
                    path,
                    encode_u64(128).as_slice(),
                    encode_u64(row_count).as_slice(),
                    provenance_kind,
                    encode_optional_u64(offsets.map(|value| value.0)).as_slice(),
                    encode_optional_u64(offsets.map(|value| value.1)).as_slice(),
                    preserve_order,
                    encode_optional_u64(discovery_order).as_slice(),
                    encode_data_file_partition_blob(&MetadataMap::new())
                        .expect("encode file partition"),
                    encode_data_file_statistics_blob(&MetadataMap::new())
                        .expect("encode file statistics"),
                    metadata_blob,
                    format!("sha256:{value:064x}"),
                    encode_u64(added_snapshot_version).as_slice(),
                    encode_optional_u64(None).as_slice(),
                    encode_u64(100 + value as u64).as_slice(),
                ],
            )
            .expect("insert catalog data file");
    }

    fn insert_lineage(connection: &Connection, value: u128, rows: &[(u64, u64, u64)]) {
        let id = file_id(value).into_bytes();
        for &(segment_id, first, last) in rows {
            connection
                .execute(
                    "INSERT INTO data_file_sources(\
                         file_id, source_segment_id, source_generation, writer_epoch, \
                         min_log_offset, max_log_offset\
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        id.as_slice(),
                        encode_u64(segment_id).as_slice(),
                        encode_u64(1).as_slice(),
                        encode_u64(1).as_slice(),
                        encode_u64(first).as_slice(),
                        encode_u64(last).as_slice(),
                    ],
                )
                .expect("insert catalog source lineage");
        }
    }

    fn encoded_metadata(file: u64) -> Vec<u8> {
        encode_data_file_metadata_blob(&MetadataMap::from([(
            "file".to_owned(),
            MetadataScalar::UInt64(file),
        )]))
        .expect("encode file metadata")
    }

    #[test]
    fn snapshot_reader_rebuilds_lineage_and_rejects_invalid_aggregates() {
        let (mut connection, _) = migrated_catalog();
        let selected = add_second_snapshot(&connection);
        insert_file(
            &connection,
            10,
            2,
            TestFileKind::Native,
            Some((0, 2)),
            encoded_metadata(10),
        );
        insert_lineage(&connection, 10, &[(2, 2, 2), (1, 0, 1)]);
        let snapshot = read_snapshot_at(&mut connection, &[selected]).expect("read snapshot");
        let file = &snapshot.tables()[0].files().as_slice()[0];
        assert_eq!(
            file.file.metadata.get("file"),
            Some(&MetadataScalar::UInt64(10))
        );
        let DataFileProvenance::Native(native) = &file.file.provenance else {
            panic!("modern file must retain native provenance");
        };
        assert_eq!(
            native
                .source_lineage()
                .as_slice()
                .iter()
                .map(|row| row.source.segment_id)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(
            native.offsets(),
            zippy_types::OffsetRange::new(LogOffset::new(0), LogOffset::new(2))
                .expect("valid expected offset range")
        );

        let (mut legacy_connection, _) = migrated_catalog();
        let selected = add_second_snapshot(&legacy_connection);
        insert_file(
            &legacy_connection,
            20,
            2,
            TestFileKind::Legacy,
            None,
            encoded_metadata(20),
        );
        insert_lineage(&legacy_connection, 20, &[(1, 0, 0)]);
        assert_eq!(
            read_snapshot_at(&mut legacy_connection, &[selected])
                .expect_err("legacy file with native lineage must fail")
                .code(),
            ErrorCode::DataCorruption
        );

        let (mut missing_connection, _) = migrated_catalog();
        let selected = add_second_snapshot(&missing_connection);
        insert_file(
            &missing_connection,
            30,
            2,
            TestFileKind::Native,
            Some((0, 0)),
            encoded_metadata(30),
        );
        assert_eq!(
            read_snapshot_at(&mut missing_connection, &[selected])
                .expect_err("native file without lineage must fail")
                .code(),
            ErrorCode::DataCorruption
        );

        let (mut initial_connection, initial) = migrated_catalog();
        insert_file(
            &initial_connection,
            40,
            1,
            TestFileKind::Native,
            Some((0, 0)),
            encoded_metadata(40),
        );
        insert_lineage(&initial_connection, 40, &[(1, 0, 0)]);
        assert_eq!(
            read_snapshot_at(&mut initial_connection, &[initial])
                .expect_err("initial create snapshot with a file must fail")
                .code(),
            ErrorCode::DataCorruption
        );
    }

    #[test]
    fn visible_file_plan_uses_v3_index_without_a_temporary_sort() {
        let (connection, selected) = migrated_catalog();
        let explain_sql = format!("EXPLAIN QUERY PLAN {GET_VISIBLE_FILES_SQL}");
        let mut statement = connection
            .prepare(&explain_sql)
            .expect("prepare visible-file query plan");
        let table_id = selected.table_id.into_bytes();
        let snapshot_version = encode_u64(selected.snapshot_version);
        let removed_after = encode_optional_u64(Some(selected.snapshot_version));
        let details = statement
            .query_map(
                params![
                    table_id.as_slice(),
                    snapshot_version.as_slice(),
                    removed_after.as_slice(),
                ],
                |row| row.get::<_, String>(3),
            )
            .expect("explain visible-file query")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect visible-file query plan");

        assert!(details
            .iter()
            .any(|detail| detail.contains("data_files_visible_by_table")));
        assert!(details
            .iter()
            .all(|detail| !detail.to_ascii_uppercase().contains("TEMP B-TREE")));
        assert!(!GET_VISIBLE_FILES_SQL
            .to_ascii_uppercase()
            .contains("ORDER BY"));
    }
}
