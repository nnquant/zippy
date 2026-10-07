//! Bounded lightweight data-file identity probes for recovery and maintenance.

use std::collections::{HashMap, HashSet};

use rusqlite::{params_from_iter, Connection, TransactionBehavior};
use zippy_types::catalog::{
    CatalogDataFileProbeMatch, CatalogDataFileProbeResult, CatalogDataFileReference,
    CatalogDataFileReferenceSnapshot, ManagedDataFileProbe,
};
use zippy_types::table::{DataFileLocation, DataFileState, ManagedDataPath};
use zippy_types::{ErrorCode, FileId, Result, ServerError, TableId};

use crate::encoding::{decode_optional_u64, decode_u64};

const MAX_FILE_PROBES: usize = 4_096;
const FILE_PROBE_CHUNK_SIZE: usize = 512;
const MAX_MANAGED_PATH_BYTES: usize = 4 * 1024;
const MAX_PROBE_PATH_BYTES: usize = 1024 * 1024;
const MAX_FILE_PROBE_RAW_BYTES: usize = 4 * 1024 * 1024;

/// Rejects malformed or unbounded probes before actor queue admission.
pub(crate) fn validate_file_probes(probes: &[ManagedDataFileProbe]) -> Result<()> {
    if probes.len() > MAX_FILE_PROBES {
        return Err(resource_exhausted(format!(
            "catalog file probe count exceeds maximum actual=[{}] maximum=[{}]",
            probes.len(),
            MAX_FILE_PROBES
        ))
        .with_retryable(false));
    }
    let mut path_bytes = 0_usize;
    let mut paths = HashSet::with_capacity(probes.len());
    for probe in probes {
        let length = probe.path.as_str().len();
        if length > MAX_MANAGED_PATH_BYTES {
            return Err(resource_exhausted(format!(
                "catalog file probe path exceeds maximum actual=[{length}] maximum=[{MAX_MANAGED_PATH_BYTES}]"
            ))
            .with_retryable(false));
        }
        path_bytes = path_bytes.checked_add(length).ok_or_else(|| {
            resource_exhausted("catalog file probe aggregate path byte count overflow")
                .with_retryable(false)
        })?;
        if path_bytes > MAX_PROBE_PATH_BYTES {
            return Err(resource_exhausted(format!(
                "catalog file probe paths exceed byte budget actual=[{path_bytes}] maximum=[{MAX_PROBE_PATH_BYTES}]"
            ))
            .with_retryable(false));
        }
        if !paths.insert(probe.path.as_str()) {
            return Err(invalid(format!(
                "catalog file probes contain duplicate managed path path=[{}]",
                probe.path.as_str()
            )));
        }
    }
    for pair in probes.windows(2) {
        match pair[0].file_id.cmp(&pair[1].file_id) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => {
                return Err(invalid(format!(
                    "catalog file probes contain duplicate file id file_id=[{}]",
                    pair[0].file_id
                )));
            }
            std::cmp::Ordering::Greater => {
                return Err(invalid(
                    "catalog file probe identities must be strictly sorted",
                ));
            }
        }
    }
    Ok(())
}

/// Resolves both immutable lookup keys within one SQLite read transaction.
pub(crate) fn inspect_data_file_references(
    connection: &mut Connection,
    probes: &[ManagedDataFileProbe],
) -> Result<CatalogDataFileReferenceSnapshot> {
    validate_file_probes(probes)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|error| crate::map_sqlite_error("begin catalog file probe", error))?;
    let catalog_revision = crate::domain::revision(&transaction)?;
    let mut by_file_id = HashMap::with_capacity(probes.len());
    let mut by_path = HashMap::with_capacity(probes.len());
    let mut raw_bytes = 0_usize;

    for chunk in probes.chunks(FILE_PROBE_CHUNK_SIZE) {
        query_by_file_id(&transaction, chunk, &mut by_file_id, &mut raw_bytes)?;
        query_by_path(&transaction, chunk, &mut by_path, &mut raw_bytes)?;
    }

    let results = probes
        .iter()
        .cloned()
        .map(|probe| {
            let by_id = by_file_id.get(&probe.file_id).cloned();
            let by_location = by_path.get(probe.path.as_str()).cloned();
            let matched = match (&by_id, &by_location) {
                (None, None) => CatalogDataFileProbeMatch::Unreferenced,
                (Some(by_id), Some(by_path))
                    if by_id == by_path
                        && by_id.table_id == probe.table_id
                        && by_id.location == DataFileLocation::Managed(probe.path.clone()) =>
                {
                    CatalogDataFileProbeMatch::Exact {
                        reference: by_id.clone(),
                    }
                }
                _ => CatalogDataFileProbeMatch::IdentityConflict {
                    by_file_id: by_id,
                    by_path: by_location,
                },
            };
            CatalogDataFileProbeResult { probe, matched }
        })
        .collect();
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit catalog file probe", error))?;
    Ok(CatalogDataFileReferenceSnapshot {
        catalog_revision,
        results,
    })
}

fn query_by_file_id(
    connection: &Connection,
    probes: &[ManagedDataFileProbe],
    output: &mut HashMap<FileId, CatalogDataFileReference>,
    raw_bytes: &mut usize,
) -> Result<()> {
    let placeholders = placeholders(probes.len());
    let sql = format!(
        "{} WHERE file_id IN ({placeholders}) ORDER BY file_id",
        file_reference_select()
    );
    let encoded = probes
        .iter()
        .map(|probe| probe.file_id.into_bytes())
        .collect::<Vec<_>>();
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| crate::map_sqlite_error("prepare catalog file-id probe", error))?;
    let mut rows = statement
        .query(params_from_iter(
            encoded.iter().map(|file_id| file_id.as_slice()),
        ))
        .map_err(|error| crate::map_sqlite_error("query catalog file-id probe", error))?;
    while let Some(row) = rows
        .next()
        .map_err(|error| crate::map_sqlite_error("read catalog file-id probe", error))?
    {
        charge_raw_bytes(raw_bytes, row)?;
        let reference = decode_reference(row)?;
        let file_id = reference.file_id;
        if output.insert(file_id, reference).is_some() {
            return Err(corruption(format!(
                "catalog file-id probe returned a duplicate row file_id=[{file_id}]"
            )));
        }
    }
    Ok(())
}

fn query_by_path(
    connection: &Connection,
    probes: &[ManagedDataFileProbe],
    output: &mut HashMap<String, CatalogDataFileReference>,
    raw_bytes: &mut usize,
) -> Result<()> {
    let placeholders = placeholders(probes.len());
    let sql = format!(
        "{} WHERE file_path IN ({placeholders}) ORDER BY file_path",
        file_reference_select()
    );
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| crate::map_sqlite_error("prepare catalog file-path probe", error))?;
    let mut rows = statement
        .query(params_from_iter(
            probes.iter().map(|probe| probe.path.as_str()),
        ))
        .map_err(|error| crate::map_sqlite_error("query catalog file-path probe", error))?;
    while let Some(row) = rows
        .next()
        .map_err(|error| crate::map_sqlite_error("read catalog file-path probe", error))?
    {
        charge_raw_bytes(raw_bytes, row)?;
        let reference = decode_reference(row)?;
        let DataFileLocation::Managed(path) = &reference.location;
        let path = path.as_str().to_owned();
        if output.insert(path.clone(), reference).is_some() {
            return Err(corruption(format!(
                "catalog file-path probe returned a duplicate row path=[{path}]"
            )));
        }
    }
    Ok(())
}

fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

fn file_reference_select() -> &'static str {
    concat!(
        "SELECT length(file_id), length(table_id), ",
        "length(CAST(location_kind AS BLOB)), length(CAST(file_path AS BLOB)), ",
        "length(file_size_bytes), length(CAST(checksum AS BLOB)), ",
        "length(added_snapshot_version), length(removed_snapshot_version), ",
        "length(CAST(state AS BLOB)), file_id, table_id, location_kind, file_path, ",
        "file_size_bytes, checksum, added_snapshot_version, removed_snapshot_version, ",
        "state FROM data_files"
    )
}

fn charge_raw_bytes(current: &mut usize, row: &rusqlite::Row<'_>) -> Result<()> {
    let mut additional = 0_usize;
    for index in 0..9 {
        let length: i64 = row.get(index).map_err(|error| {
            crate::map_sqlite_error("decode catalog file probe field length", error)
        })?;
        let length = usize::try_from(length).map_err(|_| {
            corruption(format!(
                "catalog file probe field length is outside supported domain value=[{length}]"
            ))
        })?;
        additional = additional
            .checked_add(length)
            .ok_or_else(|| resource_exhausted("catalog file probe row byte count overflow"))?;
    }
    *current = current
        .checked_add(additional)
        .ok_or_else(|| resource_exhausted("catalog file probe response byte count overflow"))?;
    if *current > MAX_FILE_PROBE_RAW_BYTES {
        return Err(resource_exhausted(format!(
            "catalog file probe response exceeds byte budget actual=[{}] maximum=[{}]",
            *current, MAX_FILE_PROBE_RAW_BYTES
        ))
        .with_retryable(false));
    }
    Ok(())
}

fn decode_reference(row: &rusqlite::Row<'_>) -> Result<CatalogDataFileReference> {
    let file_id =
        decode_file_id(row.get(9).map_err(|error| {
            crate::map_sqlite_error("decode catalog file probe identity", error)
        })?)?;
    let table_id = decode_table_id(row.get(10).map_err(|error| {
        crate::map_sqlite_error("decode catalog file probe table identity", error)
    })?)?;
    let location_kind: String = row.get(11).map_err(|error| {
        crate::map_sqlite_error("decode catalog file probe location kind", error)
    })?;
    let file_path: String = row
        .get(12)
        .map_err(|error| crate::map_sqlite_error("decode catalog file probe path", error))?;
    let location = decode_location(&location_kind, file_path)?;
    let file_size_bytes = decode_u64(
        &row.get::<_, Vec<u8>>(13)
            .map_err(|error| crate::map_sqlite_error("decode catalog file probe size", error))?,
    )?;
    let checksum: String = row
        .get(14)
        .map_err(|error| crate::map_sqlite_error("decode catalog file probe checksum", error))?;
    if checksum.is_empty() || checksum != checksum.to_lowercase() {
        return Err(corruption(
            "catalog file probe checksum is not canonical lowercase text",
        ));
    }
    let added_snapshot_version = decode_u64(&row.get::<_, Vec<u8>>(15).map_err(|error| {
        crate::map_sqlite_error("decode catalog file probe added snapshot", error)
    })?)?;
    if added_snapshot_version == 0 {
        return Err(corruption(
            "catalog file probe added snapshot version must be nonzero",
        ));
    }
    let removed_snapshot_version =
        decode_optional_u64(&row.get::<_, Vec<u8>>(16).map_err(|error| {
            crate::map_sqlite_error("decode catalog file probe removed snapshot", error)
        })?)?;
    if removed_snapshot_version.is_some_and(|removed| removed <= added_snapshot_version) {
        return Err(corruption(
            "catalog file probe contains an invalid visibility interval",
        ));
    }
    let state: String = row.get(17).map_err(|error| {
        crate::map_sqlite_error("decode catalog file probe lifecycle state", error)
    })?;
    let state = decode_state(&state)?;
    if matches!(state, DataFileState::Deleting | DataFileState::Deleted)
        && removed_snapshot_version.is_none()
    {
        return Err(corruption(
            "catalog file probe removed state lacks a removal snapshot",
        ));
    }
    Ok(CatalogDataFileReference {
        file_id,
        table_id,
        location,
        file_size_bytes,
        checksum,
        added_snapshot_version,
        removed_snapshot_version,
        state,
    })
}

fn decode_location(kind: &str, path: String) -> Result<DataFileLocation> {
    match kind {
        "managed" => ManagedDataPath::new(path)
            .map(DataFileLocation::Managed)
            .map_err(|error| {
                corruption(format!(
                    "catalog file probe managed path is invalid error=[{error}]"
                ))
            }),
        _ => Err(corruption(format!(
            "catalog file probe location kind is invalid value=[{kind}]"
        ))),
    }
}

fn decode_state(value: &str) -> Result<DataFileState> {
    match value {
        "committed" => Ok(DataFileState::Committed),
        "deleting" => Ok(DataFileState::Deleting),
        "deleted" => Ok(DataFileState::Deleted),
        _ => Err(corruption(format!(
            "catalog file probe lifecycle state is invalid value=[{value}]"
        ))),
    }
}

fn decode_file_id(bytes: Vec<u8>) -> Result<FileId> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        corruption(format!(
            "catalog file probe identity has invalid width actual=[{}]",
            bytes.len()
        ))
    })?;
    FileId::from_bytes(bytes).map_err(|error| {
        corruption(format!(
            "catalog file probe identity is invalid error=[{error}]"
        ))
    })
}

fn decode_table_id(bytes: Vec<u8>) -> Result<TableId> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        corruption(format!(
            "catalog file probe table identity has invalid width actual=[{}]",
            bytes.len()
        ))
    })?;
    TableId::from_bytes(bytes).map_err(|error| {
        corruption(format!(
            "catalog file probe table identity is invalid error=[{error}]"
        ))
    })
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn resource_exhausted(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message)
}

fn corruption(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message).with_retryable(false)
}
