//! Transactional table creation and lightweight table discovery.

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use arrow::datatypes::SchemaRef;
use rusqlite::{params, Connection, OptionalExtension, Params, Row, TransactionBehavior};
use zippy_types::catalog::{CreateTableCommand, ListTablesRequest, ListTablesResponse};
use zippy_types::schema_contract::validate_catalog_table_schema;
use zippy_types::table::{
    CatalogName, MetadataMap, SchemaIpcPayload, TableDefinition, TableDescriptor, TableKind,
    TableRef, TableState,
};
use zippy_types::{ErrorCode, NamespaceId, OffsetWatermark, Result, ServerError, TableId};

use crate::encoding::{encode_offset_watermark, encode_optional_u64, encode_u64};
use crate::storage_codec::{
    canonical_schema_hash, decode_schema_ipc_stream, decode_table_config_blob,
    decode_table_definition_blob, encode_snapshot_summary_blob, encode_table_config_blob,
    encode_table_definition_blob,
};

const INITIAL_SCHEMA_VERSION: u64 = 1;
const INITIAL_SNAPSHOT_VERSION: u64 = 1;
const INITIAL_WRITER_EPOCH: u64 = 0;
const MAX_TABLE_PAGE_SIZE: u32 = 4_096;
const MAX_TABLE_RESOLUTION_COUNT: usize = 4_096;
const MAX_TABLE_RESOLUTION_REFERENCE_BYTES: usize = 1024 * 1024;
const MAX_TABLE_READ_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TABLE_CONFIG_ENTRIES: usize = 4_096;
const MAX_TABLE_CONFIG_TEXT_BYTES: usize = 1024 * 1024;

macro_rules! table_select {
    ($suffix:literal) => {
        concat!(
            "SELECT t.table_id, t.namespace_id, t.name, t.table_kind, t.definition_blob, ",
            "t.current_schema_version, t.current_snapshot_version, t.writer_epoch, t.state, ",
            "t.config_blob, t.created_at_ns, t.updated_at_ns ",
            $suffix
        )
    };
}

const GET_TABLE_BY_ID_SQL: &str =
    table_select!("FROM tables t WHERE t.table_id = ?1 AND t.state = 'active'");
pub(crate) const GET_TABLE_BY_ID_INCLUDING_INACTIVE_SQL: &str =
    table_select!("FROM tables t WHERE t.table_id = ?1");
const GET_TABLE_BY_NAME_SQL: &str = table_select!(
    "FROM tables t JOIN namespaces n ON n.namespace_id = t.namespace_id \
     WHERE n.name = ?1 AND t.name = ?2 AND t.state = 'active'"
);
const LIST_TABLES_SQL: &str =
    table_select!("FROM tables t WHERE t.state = 'active' ORDER BY t.table_id LIMIT ?1");
const LIST_TABLES_AFTER_SQL: &str = table_select!(
    "FROM tables t WHERE t.state = 'active' AND t.table_id > ?1 \
     ORDER BY t.table_id LIMIT ?2"
);
const LIST_NAMESPACE_TABLES_SQL: &str = table_select!(
    "FROM tables t WHERE t.namespace_id = ?1 AND t.state = 'active' \
     ORDER BY t.table_id LIMIT ?2"
);
const LIST_NAMESPACE_TABLES_AFTER_SQL: &str = table_select!(
    "FROM tables t WHERE t.namespace_id = ?1 AND t.state = 'active' AND t.table_id > ?2 \
     ORDER BY t.table_id LIMIT ?3"
);

/// A validated CreateTable command whose expensive work is complete before the writer lock.
pub(crate) struct PreparedCreateTable {
    table_id: TableId,
    namespace_id: NamespaceId,
    name: CatalogName,
    definition: TableDefinition,
    definition_blob: Vec<u8>,
    schema_ipc: SchemaIpcPayload,
    schema_hash: String,
    config: BTreeMap<String, String>,
    config_blob: Vec<u8>,
    initial_summary_blob: Vec<u8>,
}

impl PreparedCreateTable {
    pub(crate) fn schema_hash(&self) -> &str {
        &self.schema_hash
    }
}

/// Validates and encodes a CreateTable command without holding SQLite's writer lock.
pub(crate) fn prepare_create_table(command: CreateTableCommand) -> Result<PreparedCreateTable> {
    let CreateTableCommand {
        table_id,
        namespace_id,
        name,
        definition,
        schema_ipc,
        config,
    } = command;
    crate::domain::validate_new_catalog_name(&name, "table")?;
    validate_table_config(&config)?;
    let schema = decode_schema_ipc_stream(schema_ipc.as_bytes())?;
    validate_table_schema(&schema, &definition)?;
    let schema_hash = canonical_schema_hash(&schema)?;
    let definition_blob = encode_table_definition_blob(&definition)?;
    let config_blob = encode_table_config_blob(&config)?;
    let initial_summary_blob = encode_snapshot_summary_blob(&MetadataMap::new())?;
    validate_new_table_row_budget(&name, &definition, &definition_blob, &config_blob)?;
    Ok(PreparedCreateTable {
        table_id,
        namespace_id,
        name,
        definition,
        definition_blob,
        schema_ipc,
        schema_hash,
        config,
        config_blob,
        initial_summary_blob,
    })
}

/// Applies one prepared CreateTable mutation inside the caller's immediate transaction.
pub(crate) fn create_table(
    connection: &Connection,
    prepared: PreparedCreateTable,
    next_revision: u64,
    created_at_ns: u64,
) -> Result<TableDescriptor> {
    ensure_namespace_exists(connection, prepared.namespace_id)?;
    ensure_table_identity_available(
        connection,
        prepared.table_id,
        prepared.namespace_id,
        &prepared.name,
    )?;

    let descriptor = TableDescriptor {
        table_id: prepared.table_id,
        namespace_id: prepared.namespace_id,
        name: prepared.name,
        definition: prepared.definition,
        current_schema_version: INITIAL_SCHEMA_VERSION,
        current_snapshot_version: INITIAL_SNAPSHOT_VERSION,
        writer_epoch: INITIAL_WRITER_EPOCH,
        state: TableState::Active,
        config: prepared.config,
        created_at_ns,
        updated_at_ns: created_at_ns,
    };
    let schema_version = encode_u64(INITIAL_SCHEMA_VERSION);
    let snapshot_version = encode_u64(INITIAL_SNAPSHOT_VERSION);
    let writer_epoch = encode_u64(INITIAL_WRITER_EPOCH);
    let timestamp = encode_u64(created_at_ns);
    let table_id = descriptor.table_id.into_bytes();
    let namespace_id = descriptor.namespace_id.into_bytes();

    connection
        .execute(
            "INSERT INTO tables(\
                 table_id, namespace_id, name, table_kind, definition_blob, \
                 current_schema_version, current_snapshot_version, writer_epoch, state, \
                 config_blob, created_at_ns, updated_at_ns\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'active', ?9, ?10, ?10)",
            params![
                table_id.as_slice(),
                namespace_id.as_slice(),
                descriptor.name.as_str(),
                descriptor.kind().as_str(),
                prepared.definition_blob,
                schema_version.as_slice(),
                snapshot_version.as_slice(),
                writer_epoch.as_slice(),
                prepared.config_blob,
                timestamp.as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("insert catalog table", error))?;
    connection
        .execute(
            "INSERT INTO table_schemas(\
                 table_id, schema_version, schema_ipc, schema_hash, compatibility, created_at_ns\
             ) VALUES (?1, ?2, ?3, ?4, 'initial', ?5)",
            params![
                table_id.as_slice(),
                schema_version.as_slice(),
                prepared.schema_ipc.into_bytes(),
                prepared.schema_hash,
                timestamp.as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("insert initial catalog schema", error))?;
    connection
        .execute(
            "INSERT INTO table_snapshots(\
                 table_id, snapshot_version, parent_snapshot_version, operation, commit_lsn, \
                 committed_log_offset, created_at_ns, summary_blob\
             ) VALUES (?1, ?2, ?3, 'create', ?4, ?5, ?6, ?7)",
            params![
                table_id.as_slice(),
                snapshot_version.as_slice(),
                encode_optional_u64(None).as_slice(),
                encode_u64(next_revision).as_slice(),
                encode_offset_watermark(OffsetWatermark::Empty).as_slice(),
                timestamp.as_slice(),
                prepared.initial_summary_blob,
            ],
        )
        .map_err(|error| crate::map_sqlite_error("insert initial catalog snapshot", error))?;
    Ok(descriptor)
}

/// Resolves one active table without loading schema or manifest state.
pub(crate) fn get_table(connection: &Connection, table: TableRef) -> Result<TableDescriptor> {
    get_table_in_transaction(connection, &table)
}

/// Lists one deterministic, revision-consistent page of active tables.
pub(crate) fn list_tables(
    connection: &mut Connection,
    request: ListTablesRequest,
) -> Result<ListTablesResponse> {
    if request.limit.get() > MAX_TABLE_PAGE_SIZE {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "catalog table page limit exceeds maximum requested=[{}] maximum=[{}]",
                request.limit.get(),
                MAX_TABLE_PAGE_SIZE
            ),
        ));
    }
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|error| crate::map_sqlite_error("begin catalog table listing", error))?;
    let catalog_revision = crate::domain::revision(&transaction)?;
    let requested_limit = usize::try_from(request.limit.get()).map_err(|_| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            "catalog table page limit is outside the supported domain",
        )
    })?;
    let sql_limit = i64::from(request.limit.get()) + 1;
    let page = match (request.namespace_id, request.after) {
        (None, None) => query_tables(&transaction, LIST_TABLES_SQL, [sql_limit], requested_limit)?,
        (None, Some(after)) => query_tables(
            &transaction,
            LIST_TABLES_AFTER_SQL,
            params![after.into_bytes().as_slice(), sql_limit],
            requested_limit,
        )?,
        (Some(namespace_id), None) => query_tables(
            &transaction,
            LIST_NAMESPACE_TABLES_SQL,
            params![namespace_id.into_bytes().as_slice(), sql_limit],
            requested_limit,
        )?,
        (Some(namespace_id), Some(after)) => query_tables(
            &transaction,
            LIST_NAMESPACE_TABLES_AFTER_SQL,
            params![
                namespace_id.into_bytes().as_slice(),
                after.into_bytes().as_slice(),
                sql_limit
            ],
            requested_limit,
        )?,
    };
    let next_after = page.has_more.then(|| {
        page.tables
            .last()
            .expect("nonempty bounded table page")
            .table_id
    });
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit catalog table listing", error))?;
    Ok(ListTablesResponse {
        catalog_revision,
        tables: page.tables,
        next_after,
    })
}

/// Validates bounded table-resolution input before it is cloned into the actor queue.
pub(crate) fn validate_table_resolution_request(table_refs: &[TableRef]) -> Result<()> {
    if table_refs.len() > MAX_TABLE_RESOLUTION_COUNT {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "catalog table resolution exceeds maximum requested=[{}] maximum=[{}]",
                table_refs.len(),
                MAX_TABLE_RESOLUTION_COUNT
            ),
        ));
    }
    let reference_bytes = table_refs
        .iter()
        .map(table_reference_bytes)
        .try_fold(0_usize, |total, bytes| total.checked_add(bytes))
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "catalog table resolution reference bytes overflow",
            )
        })?;
    if reference_bytes > MAX_TABLE_RESOLUTION_REFERENCE_BYTES {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                concat!(
                    "catalog table resolution reference bytes exceed maximum ",
                    "actual=[{}] maximum=[{}]"
                ),
                reference_bytes, MAX_TABLE_RESOLUTION_REFERENCE_BYTES
            ),
        ));
    }
    Ok(())
}

/// Resolves active table descriptors in input order from one read transaction.
pub(crate) fn resolve_tables(
    connection: &mut Connection,
    table_refs: &[TableRef],
) -> Result<Vec<TableDescriptor>> {
    validate_table_resolution_request(table_refs)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|error| crate::map_sqlite_error("begin catalog table resolution", error))?;
    let _ = crate::domain::revision(&transaction)?;
    let mut response_bytes = 0_usize;
    let mut resolved = HashMap::<TableRef, (TableDescriptor, usize)>::new();
    let mut tables = Vec::with_capacity(table_refs.len());
    for table_ref in table_refs {
        let (descriptor, descriptor_bytes) = match resolved.get(table_ref) {
            Some((descriptor, descriptor_bytes)) => (descriptor.clone(), *descriptor_bytes),
            None => {
                let raw = get_raw_table_in_transaction(&transaction, table_ref)?;
                let descriptor_bytes = raw.encoded_size_bytes();
                let descriptor = decode_table_row(raw)?;
                resolved.insert(table_ref.clone(), (descriptor.clone(), descriptor_bytes));
                (descriptor, descriptor_bytes)
            }
        };
        let next_response_bytes = response_bytes.saturating_add(descriptor_bytes);
        if next_response_bytes > MAX_TABLE_READ_RESPONSE_BYTES {
            return Err(table_response_budget_exhausted(next_response_bytes));
        }
        response_bytes = next_response_bytes;
        tables.push(descriptor);
    }
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit catalog table resolution", error))?;
    Ok(tables)
}

fn validate_table_config(config: &BTreeMap<String, String>) -> Result<()> {
    if config.len() > MAX_TABLE_CONFIG_ENTRIES {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "catalog table config entry count exceeds maximum actual=[{}] maximum=[{}]",
                config.len(),
                MAX_TABLE_CONFIG_ENTRIES
            ),
        ));
    }
    let text_bytes = config
        .iter()
        .map(|(key, value)| key.len().saturating_add(value.len()))
        .fold(0_usize, usize::saturating_add);
    if text_bytes > MAX_TABLE_CONFIG_TEXT_BYTES {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "catalog table config text exceeds maximum actual=[{}] maximum=[{}]",
                text_bytes, MAX_TABLE_CONFIG_TEXT_BYTES
            ),
        ));
    }
    Ok(())
}

fn validate_new_table_row_budget(
    name: &CatalogName,
    definition: &TableDefinition,
    definition_blob: &[u8],
    config_blob: &[u8],
) -> Result<()> {
    let encoded_size = [
        std::mem::size_of::<[u8; 16]>(),
        std::mem::size_of::<[u8; 16]>(),
        name.as_str().len(),
        definition.kind().as_str().len(),
        definition_blob.len(),
        std::mem::size_of::<u64>(),
        std::mem::size_of::<u64>(),
        std::mem::size_of::<u64>(),
        "active".len(),
        config_blob.len(),
        std::mem::size_of::<u64>(),
        std::mem::size_of::<u64>(),
    ]
    .into_iter()
    .fold(0_usize, usize::saturating_add);
    if encoded_size > MAX_TABLE_READ_RESPONSE_BYTES {
        return Err(table_response_budget_exhausted(encoded_size));
    }
    Ok(())
}

fn table_reference_bytes(table_ref: &TableRef) -> usize {
    match table_ref {
        TableRef::Id { .. } => std::mem::size_of::<TableId>(),
        TableRef::Name { namespace, table } => namespace
            .as_str()
            .len()
            .saturating_add(table.as_str().len()),
    }
}

pub(crate) fn validate_table_schema(
    schema: &SchemaRef,
    definition: &TableDefinition,
) -> Result<()> {
    validate_catalog_table_schema(schema.as_ref(), definition).map_err(|error| {
        let code = if error.is_resource_exhausted() {
            ErrorCode::ResourceExhausted
        } else {
            ErrorCode::SchemaMismatch
        };
        ServerError::new(code, error.to_string())
    })
}

fn ensure_namespace_exists(connection: &Connection, namespace_id: NamespaceId) -> Result<()> {
    let exists = connection
        .query_row(
            "SELECT 1 FROM namespaces WHERE namespace_id = ?1",
            [namespace_id.into_bytes().as_slice()],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("check catalog table namespace", error))?
        .is_some();
    if exists {
        Ok(())
    } else {
        Err(ServerError::new(
            ErrorCode::NotFound,
            "catalog table namespace was not found",
        ))
    }
}

fn ensure_table_identity_available(
    connection: &Connection,
    table_id: TableId,
    namespace_id: NamespaceId,
    name: &CatalogName,
) -> Result<()> {
    let id_exists = connection
        .query_row(
            "SELECT 1 FROM tables WHERE table_id = ?1",
            [table_id.into_bytes().as_slice()],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("check catalog table identity", error))?
        .is_some();
    if id_exists {
        return Err(ServerError::new(
            ErrorCode::AlreadyExists,
            "catalog table identity already exists",
        ));
    }
    let name_exists = connection
        .query_row(
            "SELECT 1 FROM tables WHERE namespace_id = ?1 AND name = ?2",
            params![namespace_id.into_bytes().as_slice(), name.as_str()],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("check catalog table name", error))?
        .is_some();
    if name_exists {
        return Err(ServerError::new(
            ErrorCode::AlreadyExists,
            "catalog table name already exists in its namespace",
        ));
    }
    Ok(())
}

fn get_table_in_transaction(connection: &Connection, table: &TableRef) -> Result<TableDescriptor> {
    decode_table_row(get_raw_table_in_transaction(connection, table)?)
}

fn get_raw_table_in_transaction(connection: &Connection, table: &TableRef) -> Result<RawTableRow> {
    let raw = match table {
        TableRef::Id { table_id } => {
            let mut statement =
                connection
                    .prepare_cached(GET_TABLE_BY_ID_SQL)
                    .map_err(|error| {
                        crate::map_sqlite_error("prepare catalog table identity lookup", error)
                    })?;
            statement
                .query_row([table_id.into_bytes().as_slice()], table_row)
                .optional()
        }
        TableRef::Name { namespace, table } => {
            let mut statement =
                connection
                    .prepare_cached(GET_TABLE_BY_NAME_SQL)
                    .map_err(|error| {
                        crate::map_sqlite_error("prepare catalog table name lookup", error)
                    })?;
            statement
                .query_row(params![namespace.as_str(), table.as_str()], table_row)
                .optional()
        }
    }
    .map_err(|error| crate::map_sqlite_error("read catalog table", error))?;
    raw.ok_or_else(table_not_found)
}

struct TablePage {
    tables: Vec<TableDescriptor>,
    has_more: bool,
}

fn query_tables<P>(
    connection: &Connection,
    sql: &str,
    params: P,
    requested_limit: usize,
) -> Result<TablePage>
where
    P: Params,
{
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| crate::map_sqlite_error("prepare catalog table listing", error))?;
    let mut rows = statement
        .query(params)
        .map_err(|error| crate::map_sqlite_error("query catalog table listing", error))?;
    let mut tables = Vec::with_capacity(requested_limit.min(256));
    let mut response_bytes = 0_usize;
    while let Some(row) = rows
        .next()
        .map_err(|error| crate::map_sqlite_error("read catalog table page", error))?
    {
        let raw = table_row(row)
            .map_err(|error| crate::map_sqlite_error("decode catalog table row", error))?;
        if tables.len() == requested_limit {
            return Ok(TablePage {
                tables,
                has_more: true,
            });
        }
        let descriptor_bytes = raw.encoded_size_bytes();
        let next_response_bytes = response_bytes.saturating_add(descriptor_bytes);
        if next_response_bytes > MAX_TABLE_READ_RESPONSE_BYTES {
            if tables.is_empty() {
                return Err(table_response_budget_exhausted(next_response_bytes));
            }
            return Ok(TablePage {
                tables,
                has_more: true,
            });
        }
        response_bytes = next_response_bytes;
        tables.push(decode_table_row(raw)?);
    }
    Ok(TablePage {
        tables,
        has_more: false,
    })
}

pub(crate) struct RawTableRow {
    table_id: Vec<u8>,
    namespace_id: Vec<u8>,
    name: String,
    table_kind: String,
    definition_blob: Vec<u8>,
    current_schema_version: Vec<u8>,
    current_snapshot_version: Vec<u8>,
    writer_epoch: Vec<u8>,
    state: String,
    config_blob: Vec<u8>,
    created_at_ns: Vec<u8>,
    updated_at_ns: Vec<u8>,
}

impl RawTableRow {
    pub(crate) fn encoded_size_bytes(&self) -> usize {
        [
            self.table_id.len(),
            self.namespace_id.len(),
            self.name.len(),
            self.table_kind.len(),
            self.definition_blob.len(),
            self.current_schema_version.len(),
            self.current_snapshot_version.len(),
            self.writer_epoch.len(),
            self.state.len(),
            self.config_blob.len(),
            self.created_at_ns.len(),
            self.updated_at_ns.len(),
        ]
        .into_iter()
        .fold(0_usize, usize::saturating_add)
    }
}

pub(crate) fn table_row(row: &Row<'_>) -> rusqlite::Result<RawTableRow> {
    Ok(RawTableRow {
        table_id: row.get(0)?,
        namespace_id: row.get(1)?,
        name: row.get(2)?,
        table_kind: row.get(3)?,
        definition_blob: row.get(4)?,
        current_schema_version: row.get(5)?,
        current_snapshot_version: row.get(6)?,
        writer_epoch: row.get(7)?,
        state: row.get(8)?,
        config_blob: row.get(9)?,
        created_at_ns: row.get(10)?,
        updated_at_ns: row.get(11)?,
    })
}

pub(crate) fn decode_table_row(raw: RawTableRow) -> Result<TableDescriptor> {
    let RawTableRow {
        table_id,
        namespace_id,
        name,
        table_kind,
        definition_blob,
        current_schema_version,
        current_snapshot_version,
        writer_epoch,
        state,
        config_blob,
        created_at_ns,
        updated_at_ns,
    } = raw;
    let table_id = decode_table_id(table_id)?;
    let namespace_id = decode_namespace_id(namespace_id)?;
    let name = CatalogName::new(name)
        .map_err(|error| corruption(format!("catalog table name is invalid error=[{error}]")))?;
    let stored_kind = TableKind::from_str(&table_kind)
        .map_err(|error| corruption(format!("catalog table kind is invalid error=[{error}]")))?;
    let definition = decode_table_definition_blob(&definition_blob)?;
    if definition.kind() != stored_kind {
        return Err(corruption(format!(
            "catalog table kind does not match definition stored=[{}] decoded=[{}]",
            stored_kind,
            definition.kind()
        )));
    }
    let state = decode_table_state(&state)?;
    let config = decode_table_config_blob(&config_blob)?;
    Ok(TableDescriptor {
        table_id,
        namespace_id,
        name,
        definition,
        current_schema_version: crate::encoding::decode_u64(&current_schema_version)?,
        current_snapshot_version: crate::encoding::decode_u64(&current_snapshot_version)?,
        writer_epoch: crate::encoding::decode_u64(&writer_epoch)?,
        state,
        config,
        created_at_ns: crate::encoding::decode_u64(&created_at_ns)?,
        updated_at_ns: crate::encoding::decode_u64(&updated_at_ns)?,
    })
}

fn decode_table_id(bytes: Vec<u8>) -> Result<TableId> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        corruption(format!(
            "catalog table id has invalid width actual=[{}]",
            bytes.len()
        ))
    })?;
    TableId::from_bytes(bytes)
        .map_err(|error| corruption(format!("catalog table id is invalid error=[{error}]")))
}

fn decode_namespace_id(bytes: Vec<u8>) -> Result<NamespaceId> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        corruption(format!(
            "catalog table namespace id has invalid width actual=[{}]",
            bytes.len()
        ))
    })?;
    NamespaceId::from_bytes(bytes).map_err(|error| {
        corruption(format!(
            "catalog table namespace id is invalid error=[{error}]"
        ))
    })
}

fn decode_table_state(value: &str) -> Result<TableState> {
    match value {
        "active" => Ok(TableState::Active),
        "dropping" => Ok(TableState::Dropping),
        "dropped" => Ok(TableState::Dropped),
        "failed" => Ok(TableState::Failed),
        _ => Err(corruption(format!(
            "catalog table state is invalid value=[{value}]"
        ))),
    }
}

fn table_not_found() -> ServerError {
    ServerError::new(ErrorCode::NotFound, "catalog active table was not found")
}

fn table_response_budget_exhausted(actual_bytes: usize) -> ServerError {
    ServerError::new(
        ErrorCode::ResourceExhausted,
        format!(
            "catalog table response exceeds byte budget actual=[{}] maximum=[{}]",
            actual_bytes, MAX_TABLE_READ_RESPONSE_BYTES
        ),
    )
}

fn corruption(message: String) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
}
