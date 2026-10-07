//! Atomic native schema changes with immutable schema history and writer fencing.

use arrow::datatypes::SchemaRef;
use rusqlite::{params, Connection};
use zippy_types::catalog::{AlterTableSchemaCommand, CatalogActorKind};
use zippy_types::schema_evolution::classify_schema_change;
use zippy_types::table::{SchemaCompatibility, SchemaDescriptor, TableRef};
use zippy_types::{ErrorCode, Result, ServerError};

use crate::encoding::encode_u64;
use crate::storage_codec::{
    canonical_schema_hash, decode_persisted_table_schema, decode_schema_ipc_stream,
};

pub(crate) struct PreparedAlterTableSchema {
    command: AlterTableSchemaCommand,
    schema: SchemaRef,
    schema_hash: String,
}

/// Validates the proposed schema before acquiring the SQLite writer transaction.
pub(crate) fn prepare_alter_table_schema(
    actor: CatalogActorKind,
    command: AlterTableSchemaCommand,
) -> Result<PreparedAlterTableSchema> {
    if !matches!(actor, CatalogActorKind::User | CatalogActorKind::Server) {
        return Err(ServerError::new(
            ErrorCode::PermissionDenied,
            "schema changes require a user or server authority",
        )
        .with_retryable(false));
    }
    if command.expected_schema_version == 0 {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "schema changes require a nonzero expected schema version",
        )
        .with_retryable(false));
    }
    let schema = decode_schema_ipc_stream(command.schema_ipc.as_bytes())?;
    let schema_hash = canonical_schema_hash(&schema)?;
    Ok(PreparedAlterTableSchema {
        command,
        schema,
        schema_hash,
    })
}

/// Commits a new schema and writer epoch without changing immutable file visibility.
///
/// The caller owns the transaction. Online callers must drain and persist the previous
/// runtime schema before using this primitive, then replace its fenced runtime instances.
pub(crate) fn alter_table_schema(
    connection: &Connection,
    prepared: PreparedAlterTableSchema,
    committed_at_ns: u64,
) -> Result<SchemaDescriptor> {
    let command = prepared.command;
    let table = crate::table_domain::get_table(
        connection,
        TableRef::Id {
            table_id: command.table_id,
        },
    )?;
    if table.current_schema_version != command.expected_schema_version {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            "table schema changed since the caller observed it",
        )
        .with_table_id(table.table_id)
        .with_retryable(false));
    }
    let current = crate::snapshot_domain::read_schema_in_transaction(
        connection,
        table.table_id,
        table.current_schema_version,
    )?;
    let current = decode_persisted_table_schema(&table, &current)?.into_schema();
    let compatibility = classify_schema_change(&table.definition, &current, &prepared.schema)?;
    let exhausted = || {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "table schema version or writer epoch exhausted",
        )
        .with_table_id(table.table_id)
        .with_retryable(false)
    };
    let schema_version = table
        .current_schema_version
        .checked_add(1)
        .ok_or_else(exhausted)?;
    let writer_epoch = table.writer_epoch.checked_add(1).ok_or_else(exhausted)?;
    let identity = table.table_id.into_bytes();
    connection
        .execute(
            "INSERT INTO table_schemas(\
                 table_id, schema_version, schema_ipc, schema_hash, compatibility, created_at_ns\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                identity.as_slice(),
                encode_u64(schema_version).as_slice(),
                command.schema_ipc.as_bytes(),
                prepared.schema_hash,
                match compatibility {
                    SchemaCompatibility::Full => "full",
                    SchemaCompatibility::Backward => "backward",
                    _ => unreachable!("native schema changes only preserve readable history"),
                },
                encode_u64(committed_at_ns).as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("insert evolved table schema", error))?;
    let updated = connection
        .execute(
            "UPDATE tables SET current_schema_version = ?2, writer_epoch = ?3, updated_at_ns = ?4 \
             WHERE table_id = ?1 AND state = 'active' AND current_schema_version = ?5",
            params![
                identity.as_slice(),
                encode_u64(schema_version).as_slice(),
                encode_u64(writer_epoch).as_slice(),
                encode_u64(committed_at_ns).as_slice(),
                encode_u64(command.expected_schema_version).as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("advance table schema and writer epoch", error))?;
    if updated != 1 {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "schema change affected an unexpected table row count",
        ));
    }
    Ok(SchemaDescriptor {
        table_id: table.table_id,
        schema_version,
        schema_ipc: command.schema_ipc,
        schema_hash: prepared.schema_hash,
        compatibility,
        created_at_ns: committed_at_ns,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use zippy_types::catalog::{
        AcquireProducerCommand, CatalogActor, CatalogCommandEnvelopeV1, CatalogCommandV1,
        CatalogOutcomeV1, CatalogSnapshotSelector, CreateNamespaceCommand, CreateTableCommand,
        VersionedCatalogCommand, VersionedCatalogResponse,
    };
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, ColumnName, ColumnSet, FrameTableDefinition,
        KeyValueTableDefinition, SchemaIpcPayload, TableDefinition,
    };
    use zippy_types::{NamespaceId, ProducerId, RequestId, TableId};

    use super::*;
    use crate::domain::{execute_command, revision};

    fn table_id() -> TableId {
        TableId::from_u128(1).unwrap()
    }

    fn envelope(request: u128, command: CatalogCommandV1) -> VersionedCatalogCommand {
        CatalogCommandEnvelopeV1::new(
            RequestId::from_u128(request).unwrap(),
            None,
            CatalogActor {
                kind: CatalogActorKind::User,
                principal: "schema-test".to_owned(),
            },
            command,
        )
        .unwrap()
        .into()
    }

    fn ipc(schema: &Schema) -> SchemaIpcPayload {
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, schema).unwrap();
        writer.finish().unwrap();
        drop(writer);
        SchemaIpcPayload::new(bytes).unwrap()
    }

    fn setup(connection: &mut Connection, definition: TableDefinition, schema: &Schema) {
        crate::layout::initialize(connection).unwrap();
        let namespace_id = NamespaceId::from_u128(1).unwrap();
        execute_command(
            connection,
            envelope(
                1,
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id,
                    name: CatalogName::new("test").unwrap(),
                }),
            ),
        )
        .unwrap();
        execute_command(
            connection,
            envelope(
                2,
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: table_id(),
                    namespace_id,
                    name: CatalogName::new("events").unwrap(),
                    definition,
                    schema_ipc: ipc(schema),
                    config: BTreeMap::new(),
                }),
            ),
        )
        .unwrap();
    }

    fn alter(request: u128, expected: u64, schema: &Schema) -> VersionedCatalogCommand {
        envelope(
            request,
            CatalogCommandV1::AlterTableSchema(AlterTableSchemaCommand {
                table_id: table_id(),
                expected_schema_version: expected,
                schema_ipc: ipc(schema),
            }),
        )
    }

    fn schema(extra: bool) -> Schema {
        let mut fields = vec![Field::new("key", DataType::Utf8, false)];
        if extra {
            fields.push(Field::new("value", DataType::Int64, true));
        }
        Schema::new(fields)
    }

    #[test]
    fn schema_history_is_atomic_replayable_and_fences_all_table_kinds() {
        for definition in [
            TableDefinition::Append(AppendTableDefinition::default()),
            TableDefinition::Frame(FrameTableDefinition {
                by: ColumnName::new("key").unwrap(),
            }),
            TableDefinition::KeyValue(
                KeyValueTableDefinition::new(
                    ColumnSet::new(vec![ColumnName::new("key").unwrap()]).unwrap(),
                )
                .unwrap(),
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("catalog.sqlite");
            let mut connection = Connection::open(&path).unwrap();
            let (old, new) = if matches!(definition, TableDefinition::KeyValue(_)) {
                let keys = vec!["key".to_owned()];
                (
                    zippy_types::key_value::log_schema(&schema(false), &keys).unwrap(),
                    zippy_types::key_value::log_schema(&schema(true), &keys).unwrap(),
                )
            } else {
                (
                    std::sync::Arc::new(schema(false)),
                    std::sync::Arc::new(schema(true)),
                )
            };
            setup(&mut connection, definition, &old);
            let command = alter(3, 1, &new);
            let response = execute_command(&mut connection, command.clone()).unwrap();
            let VersionedCatalogResponse::V1(body) = &response;
            let CatalogOutcomeV1::Schema(descriptor) = &body.outcome else {
                panic!("schema response expected");
            };
            assert_eq!(descriptor.schema_version, 2);
            assert_eq!(descriptor.compatibility, SchemaCompatibility::Backward);
            assert_eq!(descriptor.schema_ipc, ipc(&new));
            let table = crate::table_domain::get_table(
                &connection,
                TableRef::Id {
                    table_id: table_id(),
                },
            )
            .unwrap();
            assert_eq!(table.current_snapshot_version, 1);
            assert_eq!(table.writer_epoch, 1);
            for (version, expected) in [(1, &old), (2, &new)] {
                let snapshot = crate::snapshot_domain::read_snapshot_at(
                    &mut connection,
                    &[CatalogSnapshotSelector {
                        table_id: table_id(),
                        snapshot_version: 1,
                        schema_version: version,
                    }],
                )
                .unwrap();
                assert_eq!(snapshot.tables()[0].schema().schema_ipc, ipc(expected));
            }
            let stale = envelope(
                4,
                CatalogCommandV1::AcquireProducer(AcquireProducerCommand {
                    table_id: table_id(),
                    producer_id: ProducerId::from_u128(1).unwrap(),
                    expected_writer_epoch: Some(0),
                }),
            );
            assert_eq!(
                execute_command(&mut connection, stale).unwrap_err().code(),
                ErrorCode::StaleProducerEpoch
            );
            assert_eq!(
                execute_command(&mut connection, alter(5, 1, &new))
                    .unwrap_err()
                    .code(),
                ErrorCode::Conflict
            );
            assert_eq!(revision(&connection).unwrap(), 3);
            drop(connection);
            let mut connection = Connection::open(path).unwrap();
            crate::layout::initialize(&mut connection).unwrap();
            assert_eq!(execute_command(&mut connection, command).unwrap(), response);
            assert_eq!(revision(&connection).unwrap(), 3);
        }
    }

    #[test]
    fn rejected_changes_and_partial_sql_failure_leave_no_history_or_receipt() {
        let mut connection = Connection::open_in_memory().unwrap();
        setup(
            &mut connection,
            TableDefinition::Append(AppendTableDefinition::default()),
            &schema(false),
        );
        for (expected, proposed, code) in [
            (0, schema(true), ErrorCode::InvalidArgument),
            (1, schema(false), ErrorCode::InvalidArgument),
            (
                1,
                Schema::new(vec![Field::new("renamed", DataType::Utf8, false)]),
                ErrorCode::SchemaMismatch,
            ),
        ] {
            assert_eq!(
                execute_command(&mut connection, alter(3, expected, &proposed))
                    .unwrap_err()
                    .code(),
                code
            );
            assert_eq!(revision(&connection).unwrap(), 2);
        }
        connection.execute_batch("CREATE TEMP TRIGGER fail_schema_update BEFORE UPDATE OF current_schema_version ON tables BEGIN SELECT RAISE(ABORT, 'injected schema update failure'); END;").unwrap();
        assert!(execute_command(&mut connection, alter(3, 1, &schema(true))).is_err());
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM table_schemas", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(revision(&connection).unwrap(), 2);
        connection
            .execute_batch("DROP TRIGGER fail_schema_update")
            .unwrap();
        execute_command(&mut connection, alter(3, 1, &schema(true))).unwrap();
        assert_eq!(revision(&connection).unwrap(), 3);
    }

    #[test]
    fn corrupt_current_schema_and_exhausted_epoch_cannot_publish_new_schema() {
        let mut connection = Connection::open_in_memory().unwrap();
        setup(
            &mut connection,
            TableDefinition::Append(AppendTableDefinition::default()),
            &schema(false),
        );
        // A proposal must not silently repair a corrupt definition/schema pairing by
        // adding the definition's previously missing event-time column.
        let invalid_definition = TableDefinition::Append(AppendTableDefinition {
            event_time_column: Some(ColumnName::new("value").unwrap()),
        });
        connection
            .execute(
                "UPDATE tables SET definition_blob = ?1",
                [
                    crate::storage_codec::encode_table_definition_blob(&invalid_definition)
                        .unwrap(),
                ],
            )
            .unwrap();
        assert_eq!(
            execute_command(&mut connection, alter(3, 1, &schema(true)))
                .unwrap_err()
                .code(),
            ErrorCode::DataCorruption
        );
        connection
            .execute(
                "UPDATE tables SET definition_blob = ?1",
                [
                    crate::storage_codec::encode_table_definition_blob(&TableDefinition::Append(
                        AppendTableDefinition::default(),
                    ))
                    .unwrap(),
                ],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE tables SET writer_epoch = ?1",
                [encode_u64(u64::MAX).as_slice()],
            )
            .unwrap();
        assert_eq!(
            execute_command(&mut connection, alter(3, 1, &schema(true)))
                .unwrap_err()
                .code(),
            ErrorCode::ResourceExhausted
        );
        connection
            .execute(
                "UPDATE table_schemas SET schema_hash = ?1",
                [format!("sha256-v1:{}", "0".repeat(64))],
            )
            .unwrap();
        assert_eq!(
            execute_command(&mut connection, alter(3, 1, &schema(true)))
                .unwrap_err()
                .code(),
            ErrorCode::DataCorruption
        );
        assert_eq!(revision(&connection).unwrap(), 2);
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM table_schemas", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
}
