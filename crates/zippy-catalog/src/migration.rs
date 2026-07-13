use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, TransactionBehavior};
use sha2::{Digest, Sha256};
use zippy_types::{ErrorCode, Result, ServerError};

use crate::{CATALOG_APPLICATION_ID, CATALOG_FORMAT, CATALOG_SCHEMA_VERSION};

const MIGRATION_V1_VERSION: u32 = 1;
const MIGRATION_V1_NAME: &str = "catalog_migration_foundation";

// Physical SQLite schema versions are independent of CatalogCommand wire
// versions. Physical v1 intentionally creates no domain tables: those must
// wait for a golden typed encoding that preserves the full zippy-types domain.
const CATALOG_MIGRATIONS_SQL: &str = r#"CREATE TABLE catalog_migrations (
    schema_version INTEGER PRIMARY KEY CHECK(schema_version > 0),
    migration_name TEXT NOT NULL UNIQUE CHECK(length(migration_name) > 0),
    migration_checksum BLOB NOT NULL CHECK(
        typeof(migration_checksum) = 'blob' AND length(migration_checksum) = 32
    ),
    applied_at_ns BLOB NOT NULL CHECK(
        typeof(applied_at_ns) = 'blob' AND length(applied_at_ns) = 8
    )
 )"#;

const CATALOG_META_SQL: &str = r#"CREATE TABLE catalog_meta (
    key TEXT PRIMARY KEY CHECK(length(key) > 0),
    value BLOB NOT NULL CHECK(typeof(value) = 'blob')
 )"#;

const MIGRATION_V1: &str = r#"
CREATE TABLE catalog_migrations (
    schema_version INTEGER PRIMARY KEY CHECK(schema_version > 0),
    migration_name TEXT NOT NULL UNIQUE CHECK(length(migration_name) > 0),
    migration_checksum BLOB NOT NULL CHECK(
        typeof(migration_checksum) = 'blob' AND length(migration_checksum) = 32
    ),
    applied_at_ns BLOB NOT NULL CHECK(
        typeof(applied_at_ns) = 'blob' AND length(applied_at_ns) = 8
    )
);

CREATE TABLE catalog_meta (
    key TEXT PRIMARY KEY CHECK(length(key) > 0),
    value BLOB NOT NULL CHECK(typeof(value) = 'blob')
);
"#;

struct ExpectedSchemaObject {
    object_type: &'static str,
    name: &'static str,
    table_name: &'static str,
    sql: Option<&'static str>,
}

const MIGRATION_V1_SCHEMA: [ExpectedSchemaObject; 4] = [
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_catalog_meta_1",
        table_name: "catalog_meta",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_catalog_migrations_1",
        table_name: "catalog_migrations",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "catalog_meta",
        table_name: "catalog_meta",
        sql: Some(CATALOG_META_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "catalog_migrations",
        table_name: "catalog_migrations",
        sql: Some(CATALOG_MIGRATIONS_SQL),
    },
];

struct Migration {
    version: u32,
    name: &'static str,
    sql: &'static str,
    expected_schema: &'static [ExpectedSchemaObject],
}

const MIGRATIONS: [Migration; 1] = [Migration {
    version: MIGRATION_V1_VERSION,
    name: MIGRATION_V1_NAME,
    sql: MIGRATION_V1,
    expected_schema: &MIGRATION_V1_SCHEMA,
}];

pub(crate) fn migrate(connection: &mut Connection) -> Result<()> {
    validate_migration_registry()?;
    let mut actual = schema_version(connection)?;
    reject_future_version(actual)?;
    if actual > 0 {
        let _ = validate_metadata_at_version(connection, actual)?;
    }
    while actual < CATALOG_SCHEMA_VERSION {
        let next = actual.checked_add(1).ok_or_else(|| {
            ServerError::new(ErrorCode::Internal, "catalog migration version overflow")
        })?;
        let migration = migration_by_version(next)?;
        apply_migration(connection, migration)?;
        actual = next;
        let _ = validate_metadata_at_version(connection, actual)?;
    }
    Ok(())
}

fn validate_migration_registry() -> Result<()> {
    for (index, migration) in MIGRATIONS.iter().enumerate() {
        let expected = u32::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .ok_or_else(|| {
                ServerError::new(
                    ErrorCode::Internal,
                    "catalog migration registry is too large",
                )
            })?;
        if migration.version != expected {
            return Err(ServerError::new(
                ErrorCode::Internal,
                format!(
                    "catalog migration registry is not contiguous expected=[{}] actual=[{}]",
                    expected, migration.version
                ),
            ));
        }
    }
    if MIGRATIONS.last().map(|migration| migration.version) != Some(CATALOG_SCHEMA_VERSION) {
        return Err(ServerError::new(
            ErrorCode::Internal,
            "catalog migration registry does not end at the supported schema version",
        ));
    }
    Ok(())
}

fn migration_by_version(version: u32) -> Result<&'static Migration> {
    MIGRATIONS
        .iter()
        .find(|migration| migration.version == version)
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::Internal,
                format!("catalog migration is missing version=[{version}]"),
            )
        })
}

fn reject_future_version(version: u32) -> Result<()> {
    if version > CATALOG_SCHEMA_VERSION {
        Err(ServerError::new(
            ErrorCode::UnsupportedPlan,
            format!(
                "catalog schema is newer than this server supported=[{}] actual=[{}]",
                CATALOG_SCHEMA_VERSION, version
            ),
        ))
    } else {
        Ok(())
    }
}

fn application_id(connection: &Connection) -> Result<u32> {
    let value: i64 = connection
        .query_row("PRAGMA application_id", [], |row| row.get(0))
        .map_err(|error| crate::map_sqlite_error("read catalog application id", error))?;
    u32::try_from(value).map_err(|_| {
        ServerError::new(
            ErrorCode::DataCorruption,
            format!("catalog application id is outside the supported domain actual=[{value}]"),
        )
    })
}

pub(crate) fn preflight(connection: &Connection) -> Result<()> {
    let application_id = application_id(connection)?;
    let version = schema_version(connection)?;
    let schema_objects: u64 = connection
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'",
            [],
            |row| row.get(0),
        )
        .map_err(|error| crate::map_sqlite_error("inspect catalog schema objects", error))?;

    if application_id == 0 && version == 0 && schema_objects == 0 {
        return Ok(());
    }
    if application_id != CATALOG_APPLICATION_ID {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!("database is not a Zippy catalog application_id=[{application_id}]"),
        ));
    }
    reject_future_version(version)?;
    if version == 0 {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "Zippy catalog application marker has no physical schema version",
        ));
    }
    let _ = validate_metadata_at_version(connection, version)?;
    Ok(())
}

pub(crate) fn schema_version(connection: &Connection) -> Result<u32> {
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|error| crate::map_sqlite_error("read catalog schema version", error))?;
    u32::try_from(version).map_err(|_| {
        ServerError::new(
            ErrorCode::DataCorruption,
            format!("catalog schema version is outside the supported domain actual=[{version}]"),
        )
    })
}

fn apply_migration(connection: &mut Connection, migration: &Migration) -> Result<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| crate::map_sqlite_error("begin catalog migration", error))?;
    transaction
        .execute_batch(migration.sql)
        .map_err(|error| crate::map_sqlite_error("apply catalog migration", error))?;
    if migration.version == MIGRATION_V1_VERSION {
        transaction
            .execute(
                "INSERT INTO catalog_meta(key, value) VALUES (?1, ?2)",
                params!["catalog_format", CATALOG_FORMAT.as_bytes()],
            )
            .map_err(|error| crate::map_sqlite_error("initialize catalog format", error))?;
        transaction
            .execute(
                "INSERT INTO catalog_meta(key, value) VALUES (?1, ?2)",
                params![
                    "schema_version",
                    encode_u64(u64::from(migration.version)).as_slice()
                ],
            )
            .map_err(|error| {
                crate::map_sqlite_error("initialize catalog schema metadata", error)
            })?;
        transaction
            .execute(
                "INSERT INTO catalog_meta(key, value) VALUES (?1, ?2)",
                params!["catalog_revision", encode_u64(0).as_slice()],
            )
            .map_err(|error| crate::map_sqlite_error("initialize catalog revision", error))?;
    } else {
        let updated = transaction
            .execute(
                "UPDATE catalog_meta SET value = ?1 WHERE key = 'schema_version'",
                [encode_u64(u64::from(migration.version)).as_slice()],
            )
            .map_err(|error| crate::map_sqlite_error("advance catalog schema metadata", error))?;
        if updated != 1 {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                format!(
                    "catalog schema metadata update affected unexpected rows actual=[{updated}]"
                ),
            ));
        }
    }
    transaction
        .execute(
            "INSERT INTO catalog_migrations(
                schema_version, migration_name, migration_checksum, applied_at_ns
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                migration.version,
                migration.name,
                migration_checksum(migration).as_slice(),
                encode_u64(unix_time_ns()?).as_slice()
            ],
        )
        .map_err(|error| crate::map_sqlite_error("record catalog migration", error))?;
    transaction
        .pragma_update(None, "application_id", CATALOG_APPLICATION_ID)
        .map_err(|error| crate::map_sqlite_error("record catalog application id", error))?;
    transaction
        .pragma_update(None, "user_version", migration.version)
        .map_err(|error| crate::map_sqlite_error("record catalog schema version", error))?;
    let _ =
        validate_metadata_at_version_with_migration(&transaction, migration.version, migration)?;
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit catalog migration", error))
}

pub(crate) fn validate_metadata(connection: &Connection) -> Result<(String, u32, u64)> {
    let physical_version = schema_version(connection)?;
    reject_future_version(physical_version)?;
    if physical_version != CATALOG_SCHEMA_VERSION {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog physical schema version mismatch expected=[{}] actual=[{}]",
                CATALOG_SCHEMA_VERSION, physical_version
            ),
        ));
    }
    validate_metadata_at_version(connection, physical_version)
}

fn validate_metadata_at_version(
    connection: &Connection,
    expected_version: u32,
) -> Result<(String, u32, u64)> {
    let migration = migration_by_version(expected_version)?;
    validate_metadata_at_version_with_migration(connection, expected_version, migration)
}

fn validate_metadata_at_version_with_migration(
    connection: &Connection,
    expected_version: u32,
    migration: &Migration,
) -> Result<(String, u32, u64)> {
    let actual_application_id = application_id(connection)?;
    if actual_application_id != CATALOG_APPLICATION_ID {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog application id mismatch expected=[{}] actual=[{}]",
                CATALOG_APPLICATION_ID, actual_application_id
            ),
        ));
    }
    let physical_version = schema_version(connection)?;
    if physical_version != expected_version {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog physical schema version mismatch expected=[{}] actual=[{}]",
                expected_version, physical_version
            ),
        ));
    }
    validate_schema_fingerprint_for_migration(connection, migration)?;
    let format = metadata_blob(connection, "catalog_format")?;
    if format != CATALOG_FORMAT.as_bytes() {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog format metadata mismatch",
        ));
    }
    let metadata_version = decode_u64(
        &metadata_blob(connection, "schema_version")?,
        "schema_version",
    )?;
    if metadata_version != u64::from(expected_version) {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog schema metadata mismatch expected=[{}] actual=[{}]",
                expected_version, metadata_version
            ),
        ));
    }
    let catalog_revision = catalog_revision(connection)?;
    validate_migration_audit(connection, physical_version)?;
    let format = String::from_utf8(format).map_err(|_| {
        ServerError::new(
            ErrorCode::DataCorruption,
            "catalog format metadata is not UTF-8",
        )
    })?;
    Ok((format, physical_version, catalog_revision))
}

fn validate_migration_audit(connection: &Connection, physical_version: u32) -> Result<()> {
    let mut statement = connection
        .prepare(
            "SELECT schema_version, migration_name, migration_checksum, applied_at_ns
             FROM catalog_migrations ORDER BY schema_version",
        )
        .map_err(|error| crate::map_sqlite_error("prepare catalog migration audit", error))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, u32>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        })
        .map_err(|error| crate::map_sqlite_error("read catalog migration audit", error))?;
    let mut actual = Vec::new();
    for row in rows {
        actual.push(
            row.map_err(|error| crate::map_sqlite_error("decode catalog migration audit", error))?,
        );
    }
    let expected = MIGRATIONS
        .iter()
        .filter(|migration| migration.version <= physical_version)
        .collect::<Vec<_>>();
    if actual.len() != expected.len() {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog migration audit length mismatch expected=[{}] actual=[{}]",
                expected.len(),
                actual.len()
            ),
        ));
    }
    for ((version, name, checksum, applied_at_ns), migration) in actual.iter().zip(expected) {
        if *version != migration.version
            || name != migration.name
            || checksum.as_slice() != migration_checksum(migration)
        {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                format!(
                    "catalog migration audit mismatch expected_version=[{}] actual_version=[{}]",
                    migration.version, version
                ),
            ));
        }
        let _ = decode_u64(applied_at_ns, "catalog_migrations.applied_at_ns")?;
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CanonicalSchemaObject {
    object_type: String,
    name: String,
    table_name: String,
    sql: Option<String>,
}

fn validate_schema_fingerprint_for_migration(
    connection: &Connection,
    migration: &Migration,
) -> Result<()> {
    let mut expected = migration
        .expected_schema
        .iter()
        .map(|object| {
            canonical_schema_object(
                object.object_type,
                object.name,
                object.table_name,
                object.sql,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    expected.sort();
    if expected.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ServerError::new(
            ErrorCode::Internal,
            format!(
                "catalog expected schema contains duplicate objects version=[{}]",
                migration.version
            ),
        ));
    }

    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, sql
             FROM sqlite_schema
             WHERE name NOT GLOB 'sqlite_*'
                OR (type = 'index' AND name GLOB 'sqlite_autoindex_*')",
        )
        .map_err(|error| crate::map_sqlite_error("prepare catalog schema inventory", error))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(|error| crate::map_sqlite_error("read catalog schema inventory", error))?;
    let mut actual = Vec::new();
    for row in rows {
        let (object_type, name, table_name, sql) =
            row.map_err(|error| crate::map_sqlite_error("decode catalog schema inventory", error))?;
        actual.push(canonical_schema_object(
            &object_type,
            &name,
            &table_name,
            sql.as_deref(),
        )?);
    }
    actual.sort();

    let expected_fingerprint = schema_fingerprint(&expected);
    let actual_fingerprint = schema_fingerprint(&actual);
    if expected_fingerprint != actual_fingerprint {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                concat!(
                    "catalog schema fingerprint mismatch version=[{}] ",
                    "expected_objects=[{}] actual_objects=[{}]"
                ),
                migration.version,
                expected.len(),
                actual.len()
            ),
        ));
    }
    Ok(())
}

fn canonical_schema_object(
    object_type: &str,
    name: &str,
    table_name: &str,
    sql: Option<&str>,
) -> Result<CanonicalSchemaObject> {
    Ok(CanonicalSchemaObject {
        object_type: object_type.to_owned(),
        name: name.to_owned(),
        table_name: table_name.to_owned(),
        sql: sql.map(normalize_schema_sql).transpose()?,
    })
}

fn schema_fingerprint(objects: &[CanonicalSchemaObject]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for object in objects {
        for field in [
            object.object_type.as_str(),
            object.name.as_str(),
            object.table_name.as_str(),
        ] {
            digest.update((field.len() as u64).to_be_bytes());
            digest.update(field.as_bytes());
        }
        match object.sql.as_deref() {
            Some(sql) => {
                digest.update([1]);
                digest.update((sql.len() as u64).to_be_bytes());
                digest.update(sql.as_bytes());
            }
            None => digest.update([0]),
        }
    }
    digest.finalize().into()
}

fn normalize_schema_sql(sql: &str) -> Result<String> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Quote {
        Single,
        Double,
        Backtick,
        Bracket,
    }

    let mut normalized = String::with_capacity(sql.len());
    let mut characters = sql.chars().peekable();
    let mut quote = None;
    let mut whitespace_pending = false;
    while let Some(character) = characters.next() {
        if let Some(active) = quote {
            normalized.push(character);
            let terminator = match active {
                Quote::Single => '\'',
                Quote::Double => '"',
                Quote::Backtick => '`',
                Quote::Bracket => ']',
            };
            if character == terminator {
                let doubled = !matches!(active, Quote::Bracket)
                    && characters.peek().is_some_and(|next| *next == terminator);
                if doubled {
                    normalized.push(characters.next().expect("peeked quote must exist"));
                } else {
                    quote = None;
                }
            }
            continue;
        }

        if character.is_whitespace() {
            whitespace_pending = !normalized.is_empty();
            continue;
        }
        if whitespace_pending {
            normalized.push(' ');
            whitespace_pending = false;
        }
        normalized.push(character);
        quote = match character {
            '\'' => Some(Quote::Single),
            '"' => Some(Quote::Double),
            '`' => Some(Quote::Backtick),
            '[' => Some(Quote::Bracket),
            _ => None,
        };
    }
    if quote.is_some() {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog schema SQL contains an unterminated quoted token",
        ));
    }
    Ok(normalized)
}

pub(crate) fn catalog_revision(connection: &Connection) -> Result<u64> {
    decode_u64(
        &metadata_blob(connection, "catalog_revision")?,
        "catalog_revision",
    )
}

fn metadata_blob(connection: &Connection, key: &str) -> Result<Vec<u8>> {
    connection
        .query_row(
            "SELECT value FROM catalog_meta WHERE key = ?1",
            [key],
            |row| row.get(0),
        )
        .map_err(|error| crate::map_sqlite_error("read catalog metadata", error))
}

fn migration_checksum(migration: &Migration) -> [u8; 32] {
    Sha256::digest(migration.sql.as_bytes()).into()
}

fn unix_time_ns() -> Result<u64> {
    let nanoseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("system clock precedes Unix epoch error=[{error}]"),
            )
        })?
        .as_nanos();
    u64::try_from(nanoseconds).map_err(|_| {
        ServerError::new(
            ErrorCode::Internal,
            "system time exceeds catalog timestamp representation",
        )
    })
}

const fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn decode_u64(bytes: &[u8], field: &str) -> Result<u64> {
    let encoded: [u8; 8] = bytes.try_into().map_err(|_| {
        ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog unsigned field has invalid width field=[{}] actual=[{}]",
                field,
                bytes.len()
            ),
        )
    })?;
    Ok(u64::from_be_bytes(encoded))
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use zippy_types::ErrorCode;

    use super::{
        apply_migration, decode_u64, encode_u64, validate_migration_registry, Migration,
        MIGRATION_V1, MIGRATION_V1_NAME, MIGRATION_V1_SCHEMA, MIGRATION_V1_VERSION,
    };

    #[test]
    fn physical_migration_registry_is_contiguous_and_complete() {
        assert_eq!(validate_migration_registry(), Ok(()));
    }

    #[test]
    fn unsigned_metadata_encoding_round_trips_extremes_and_preserves_order() {
        let values = [0, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX];
        for value in values {
            assert_eq!(decode_u64(&encode_u64(value), "test"), Ok(value));
        }
        for pair in values.windows(2) {
            assert!(encode_u64(pair[0]) < encode_u64(pair[1]));
        }
    }

    #[test]
    fn fresh_migration_manifest_drift_rolls_back_before_commit() {
        let mut connection = Connection::open_in_memory().expect("open in-memory catalog");
        let drifted_manifest = Migration {
            version: MIGRATION_V1_VERSION,
            name: MIGRATION_V1_NAME,
            sql: MIGRATION_V1,
            expected_schema: &[],
        };

        assert_migration_rolls_back(&mut connection, &drifted_manifest);
    }

    #[test]
    fn fresh_migration_ledger_drift_rolls_back_before_commit() {
        let mut connection = Connection::open_in_memory().expect("open in-memory catalog");
        let drifted_manifest = Migration {
            version: MIGRATION_V1_VERSION,
            name: "drifted_test_migration",
            sql: MIGRATION_V1,
            expected_schema: &MIGRATION_V1_SCHEMA,
        };

        assert_migration_rolls_back(&mut connection, &drifted_manifest);
    }

    fn assert_migration_rolls_back(connection: &mut Connection, migration: &Migration) {
        let error = apply_migration(connection, migration)
            .expect_err("migration drift must be rejected before commit");
        assert_eq!(error.code(), ErrorCode::DataCorruption);

        let schema_objects: u64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'",
                [],
                |row| row.get(0),
            )
            .expect("count schema objects after rollback");
        assert_eq!(schema_objects, 0);
        let user_version: u32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("read schema version after rollback");
        assert_eq!(user_version, 0);
        let application_id: u32 = connection
            .query_row("PRAGMA application_id", [], |row| row.get(0))
            .expect("read application id after rollback");
        assert_eq!(application_id, 0);
    }
}
