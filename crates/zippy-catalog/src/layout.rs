//! Exact current Catalog layout initialization and validation; no online format upgrades.

use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, Transaction, TransactionBehavior};
use sha2::{Digest, Sha256};
use zippy_types::{ErrorCode, Result, ServerError};

use crate::encoding::{decode_u64, encode_u64};
use crate::{CATALOG_APPLICATION_ID, CATALOG_FORMAT, CATALOG_SCHEMA_VERSION};

const LAYOUT_NAME: &str = "native_catalog_layout";
const LAYOUT_SQL: &str = include_str!("layout.sql");
// Frozen inventory includes constraints, generated columns, autoindexes and partial indexes.
const LAYOUT_FINGERPRINT: [u8; 32] = [
    197, 194, 185, 160, 249, 241, 238, 15, 17, 6, 67, 39, 176, 51, 154, 158, 225, 165, 56, 104,
    200, 179, 77, 229, 150, 226, 247, 89, 102, 86, 70, 190,
];

pub(crate) fn initialize(connection: &mut Connection) -> Result<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| crate::map_sqlite_error("begin catalog initialization", error))?;
    if validate_catalog_state(&transaction)? == 0 {
        create_layout(&transaction)?;
    }
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit catalog initialization", error))
}

fn create_layout(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(LAYOUT_SQL)
        .map_err(|error| crate::map_sqlite_error("create catalog layout", error))?;
    for (key, value) in [
        ("catalog_format", CATALOG_FORMAT.as_bytes().to_vec()),
        (
            "schema_version",
            encode_u64(u64::from(CATALOG_SCHEMA_VERSION)).to_vec(),
        ),
        ("catalog_revision", encode_u64(0).to_vec()),
    ] {
        connection
            .execute(
                "INSERT INTO catalog_meta(key,value) VALUES (?1,?2)",
                params![key, value],
            )
            .map_err(|error| crate::map_sqlite_error("initialize catalog metadata", error))?;
    }
    connection
        .execute(
            "INSERT INTO catalog_layout(
                schema_version, layout_name, layout_checksum, created_at_ns
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                CATALOG_SCHEMA_VERSION,
                LAYOUT_NAME,
                layout_checksum().as_slice(),
                encode_u64(unix_time_ns()?).as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("record catalog initialization", error))?;
    connection
        .pragma_update(None, "application_id", CATALOG_APPLICATION_ID)
        .map_err(|error| crate::map_sqlite_error("record catalog application id", error))?;
    connection
        .pragma_update(None, "user_version", CATALOG_SCHEMA_VERSION)
        .map_err(|error| crate::map_sqlite_error("record catalog schema version", error))?;
    let _ = validate_metadata(connection)?;
    Ok(())
}

fn require_current_version(version: u32) -> Result<()> {
    if version != CATALOG_SCHEMA_VERSION {
        return Err(ServerError::new(
            ErrorCode::UnsupportedPlan,
            format!(
                "unsupported catalog schema version supported=[{}] actual=[{version}]; \
                 create a new data directory",
                CATALOG_SCHEMA_VERSION,
            ),
        )
        .with_retryable(false));
    }
    Ok(())
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
    let transaction = Transaction::new_unchecked(connection, TransactionBehavior::Deferred)
        .map_err(|error| crate::map_sqlite_error("begin catalog preflight", error))?;
    let _ = validate_catalog_state(&transaction)?;
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit catalog preflight", error))
}

fn validate_catalog_state(connection: &Connection) -> Result<u32> {
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
        return Ok(version);
    }
    if application_id != CATALOG_APPLICATION_ID {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!("database is not a Zippy catalog application_id=[{application_id}]"),
        ));
    }
    if version == 0 {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "Zippy catalog application marker has no physical schema version",
        ));
    }
    require_current_version(version)?;
    let _ = validate_layout_contents(connection, version)?;
    Ok(version)
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

pub(crate) fn validate_metadata(connection: &Connection) -> Result<(String, u32, u64)> {
    let version = schema_version(connection)?;
    require_current_version(version)?;
    if application_id(connection)? != CATALOG_APPLICATION_ID {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog application id mismatch",
        ));
    }
    let revision = validate_layout_contents(connection, version)?;
    Ok((CATALOG_FORMAT.to_owned(), version, revision))
}

/// Validates layout contents after the SQLite header has been checked; returns the revision.
fn validate_layout_contents(connection: &Connection, version: u32) -> Result<u64> {
    validate_schema_fingerprint(connection)?;
    let format = metadata_blob(connection, "catalog_format")?;
    if format != CATALOG_FORMAT.as_bytes() {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog format metadata mismatch",
        ));
    }
    if decode_u64(&metadata_blob(connection, "schema_version")?)? != u64::from(version) {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog schema metadata mismatch",
        ));
    }
    validate_layout_record(connection)?;
    catalog_revision(connection)
}

fn validate_layout_record(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT schema_version, layout_name, layout_checksum, created_at_ns FROM catalog_layout",
    ).map_err(|error| crate::map_sqlite_error("prepare catalog layout record", error))?;
    let mut rows = statement
        .query([])
        .map_err(|error| crate::map_sqlite_error("read catalog layout record", error))?;
    let row = rows
        .next()
        .map_err(|error| crate::map_sqlite_error("read catalog layout record", error))?
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::DataCorruption,
                "catalog layout record is missing",
            )
        })?;
    let values = (|| -> rusqlite::Result<(u32, String, Vec<u8>, Vec<u8>)> {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    })()
    .map_err(|error| crate::map_sqlite_error("decode catalog layout record", error))?;
    if values.0 != CATALOG_SCHEMA_VERSION
        || values.1 != LAYOUT_NAME
        || values.2.as_slice() != layout_checksum()
    {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog layout record mismatch",
        ));
    }
    let _ = decode_u64(&values.3)?;
    if rows
        .next()
        .map_err(|error| crate::map_sqlite_error("read catalog layout record", error))?
        .is_some()
    {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog layout record is not a singleton",
        ));
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

fn validate_schema_fingerprint(connection: &Connection) -> Result<()> {
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

    if schema_fingerprint(&actual) != LAYOUT_FINGERPRINT {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog schema fingerprint mismatch version=[{}] actual_objects=[{}]",
                CATALOG_SCHEMA_VERSION,
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
    decode_u64(&metadata_blob(connection, "catalog_revision")?)
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

fn layout_checksum() -> [u8; 32] {
    // Source archives/checkouts may normalize text line endings on different platforms.
    // The same logical layout must keep its initialization checksum when moved between them.
    normalized_layout_checksum(LAYOUT_SQL)
}

fn normalized_layout_checksum(sql: &str) -> [u8; 32] {
    Sha256::digest(sql.replace("\r\n", "\n").as_bytes()).into()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsigned_metadata_encoding_round_trips_extremes_and_preserves_order() {
        let values = [0, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX];
        for value in values {
            assert_eq!(decode_u64(&encode_u64(value)), Ok(value));
        }
        for pair in values.windows(2) {
            assert!(encode_u64(pair[0]) < encode_u64(pair[1]));
        }
    }

    #[test]
    fn layout_checksum_is_independent_of_source_line_endings() {
        let canonical = LAYOUT_SQL.replace("\r\n", "\n");
        assert_eq!(layout_checksum(), normalized_layout_checksum(&canonical));
        assert_eq!(
            layout_checksum(),
            normalized_layout_checksum(&canonical.replace('\n', "\r\n"))
        );
    }

    #[test]
    fn new_layout_has_exact_inventory_and_initializes_once() {
        let mut connection = Connection::open_in_memory().unwrap();
        initialize(&mut connection).unwrap();
        assert_eq!(schema_version(&connection).unwrap(), CATALOG_SCHEMA_VERSION);
        assert_eq!(catalog_revision(&connection).unwrap(), 0);
        initialize(&mut connection).unwrap();
        validate_metadata(&connection).unwrap();
        let records: u64 = connection
            .query_row("SELECT count(*) FROM catalog_layout", [], |row| row.get(0))
            .unwrap();
        assert_eq!(records, 1);
        let migrations: u64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name='catalog_migrations'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(migrations, 0);
    }

    #[test]
    fn failed_bootstrap_validation_rolls_back_schema_and_header() {
        let mut connection = Connection::open_in_memory().unwrap();
        {
            let transaction = connection.transaction().unwrap();
            create_layout(&transaction).unwrap();
            transaction
                .execute_batch("DROP INDEX tables_reserved_name")
                .unwrap();
            assert_eq!(
                validate_metadata(&transaction).unwrap_err().code(),
                ErrorCode::DataCorruption
            );
        }
        assert_eq!(schema_version(&connection).unwrap(), 0);
        assert_eq!(application_id(&connection).unwrap(), 0);
        let objects: u64 = connection
            .query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))
            .unwrap();
        assert_eq!(objects, 0);
        initialize(&mut connection).unwrap();
    }
}
