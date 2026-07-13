//! Transactional native manifest persistence and row-preserving compaction.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use zippy_types::catalog::{
    CatalogActorKind, CatalogCommandV1, CommitCompactionCommand, CommitDataFilesCommand,
    VersionedCatalogCommand, MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
};
use zippy_types::table::{
    DataFileLocation, DataFileProvenance, MetadataMap, MetadataScalar, NativeDataFileProvenance,
    PendingDataFile, SnapshotOperation, SourceSegmentIdentity, SourceSegmentLineage,
    SourceSegmentLineages, TableSnapshot,
};
use zippy_types::{
    ErrorCode, FileId, LogOffset, OffsetRange, OffsetWatermark, Result, ServerError,
    ServerErrorDetails, TableId,
};

use crate::encoding::{
    decode_offset_watermark, decode_optional_u64, decode_u64, encode_offset_watermark,
    encode_optional_u64, encode_u64,
};
use crate::storage_codec::{
    encode_data_file_metadata_blob, encode_data_file_partition_blob,
    encode_data_file_statistics_blob, encode_snapshot_summary_blob,
};

const MAX_MANIFEST_FILES: usize = 4_096;
const MAX_COMPACTION_INPUTS: usize = 4_096;
const MAX_LINEAGE_ROWS_PER_FILE: usize = 4_096;
const MAX_MANIFEST_LINEAGE_ROWS: usize = 65_536;
const MAX_ENCODED_METADATA_BYTES: usize = 64 * 1024 * 1024;
const MAX_METADATA_ENTRIES: usize = 4_096;
const MAX_FILE_PATH_BYTES: usize = 4 * 1024;
const MAX_CHECKSUM_BYTES: usize = 256;
const SQLITE_IN_CHUNK: usize = 512;

type CanonicalLineage = BTreeMap<SourceSegmentIdentity, Vec<OffsetRange>>;
type CanonicalSchemaCoverage = BTreeMap<u64, Vec<OffsetRange>>;

pub(crate) struct PreparedCommitDataFiles {
    table_id: TableId,
    expected_snapshot_version: u64,
    files: PreparedFiles,
    committed_log_offset: OffsetWatermark,
    summary: MetadataMap,
    summary_blob: Vec<u8>,
}

pub(crate) struct PreparedCommitCompaction {
    table_id: TableId,
    expected_snapshot_version: u64,
    input_file_ids: Vec<FileId>,
    output_files: PreparedFiles,
    committed_log_offset: OffsetWatermark,
    summary: MetadataMap,
    summary_blob: Vec<u8>,
}

struct PreparedFiles {
    files: Vec<PreparedDataFile>,
    schema_versions: BTreeSet<u64>,
    schema_coverage: CanonicalSchemaCoverage,
    lineage: CanonicalLineage,
    coverage: Vec<OffsetRange>,
    total_rows: u64,
    total_bytes: u64,
    lineage_rows: usize,
    max_writer_epoch: u64,
    encoded_metadata_bytes: usize,
}

struct PreparedDataFile {
    file: PendingDataFile,
    partition_blob: Vec<u8>,
    statistics_blob: Vec<u8>,
    metadata_blob: Vec<u8>,
}

struct InputManifest {
    schema_coverage: CanonicalSchemaCoverage,
    lineage: CanonicalLineage,
    coverage: Vec<OffsetRange>,
    total_rows: u64,
}

struct RawInputFile {
    table_id: Vec<u8>,
    schema_version: Vec<u8>,
    location_kind: String,
    row_count: Vec<u8>,
    provenance_kind: String,
    min_log_offset: Vec<u8>,
    max_log_offset: Vec<u8>,
    added_snapshot_version: Vec<u8>,
    removed_snapshot_version: Vec<u8>,
    state: String,
}

struct ManifestTableState {
    current_schema_version: u64,
    current_snapshot_version: u64,
    writer_epoch: u64,
}

/// Validates common actor bounds and estimates manifest memory before actor admission.
pub(crate) fn manifest_command_preflight_bytes(command: &VersionedCatalogCommand) -> Result<usize> {
    let VersionedCatalogCommand::V1(envelope) = command;
    let result = (|| {
        let principal_bytes = envelope.actor.principal.len();
        if principal_bytes > MAX_CATALOG_ACTOR_PRINCIPAL_BYTES {
            return Err(resource(format!(
                "catalog actor principal exceeds byte limit actual=[{principal_bytes}] maximum=[{MAX_CATALOG_ACTOR_PRINCIPAL_BYTES}]"
            )));
        }
        let command_bytes = match &envelope.command {
            CatalogCommandV1::CommitDataFiles(command) => {
                estimate_manifest_queue_bytes(&command.files, "persist")?
            }
            CatalogCommandV1::CommitCompaction(command) => {
                if command.input_file_ids.len() > MAX_COMPACTION_INPUTS {
                    return Err(resource(format!(
                        "compaction input file count exceeds maximum actual=[{}] maximum=[{}]",
                        command.input_file_ids.len(),
                        MAX_COMPACTION_INPUTS
                    )));
                }
                let output_bytes =
                    estimate_manifest_queue_bytes(&command.output_files, "compaction output")?;
                let input_bytes = command
                    .input_file_ids
                    .len()
                    .checked_mul(size_of::<FileId>())
                    .ok_or_else(|| resource("compaction input byte estimate overflow"))?;
                checked_add(output_bytes, input_bytes, "compaction queue byte estimate")?
            }
            _ => return Ok(0),
        };
        checked_add(
            command_bytes,
            principal_bytes,
            "manifest queue actor principal bytes",
        )
    })();
    result.map_err(|error: ServerError| error.with_request_id(envelope.request_id))
}

fn estimate_manifest_queue_bytes(files: &[PendingDataFile], context: &str) -> Result<usize> {
    if files.len() > MAX_MANIFEST_FILES {
        return Err(resource(format!(
            "{context} file count exceeds maximum actual=[{}] maximum=[{}]",
            files.len(),
            MAX_MANIFEST_FILES
        )));
    }
    let mut total = files
        .len()
        .checked_mul(size_of::<PendingDataFile>())
        .ok_or_else(|| resource("manifest queue file byte estimate overflow"))?;
    let mut admitted_payload = 0_usize;
    let mut lineage_rows = 0_usize;
    for file in files {
        let path = match &file.location {
            DataFileLocation::Managed(path) => path.as_str(),
            DataFileLocation::ExternalLegacy(path) => path.as_str(),
        };
        if path.len() > MAX_FILE_PATH_BYTES {
            return Err(resource(format!(
                "manifest file path exceeds byte limit file_id=[{}] actual=[{}] maximum=[{}]",
                file.file_id,
                path.len(),
                MAX_FILE_PATH_BYTES
            )));
        }
        if file.checksum.len() > MAX_CHECKSUM_BYTES {
            return Err(resource(format!(
                "manifest checksum exceeds byte limit file_id=[{}] actual=[{}] maximum=[{}]",
                file.file_id,
                file.checksum.len(),
                MAX_CHECKSUM_BYTES
            )));
        }
        let file_lineage = match &file.provenance {
            DataFileProvenance::Native(native) => native.source_lineage().as_slice().len(),
            DataFileProvenance::Legacy { .. } => 0,
        };
        if file_lineage > MAX_LINEAGE_ROWS_PER_FILE {
            return Err(resource(format!(
                "{context} file lineage count exceeds maximum file_id=[{}] actual=[{}] maximum=[{}]",
                file.file_id, file_lineage, MAX_LINEAGE_ROWS_PER_FILE
            )));
        }
        lineage_rows = checked_add(lineage_rows, file_lineage, "manifest lineage count")?;
        if lineage_rows > MAX_MANIFEST_LINEAGE_ROWS {
            return Err(resource(format!(
                "{context} lineage count exceeds maximum actual=[{}] maximum=[{}]",
                lineage_rows, MAX_MANIFEST_LINEAGE_ROWS
            )));
        }
        total = checked_add(total, path.len(), "manifest queue path bytes")?;
        total = checked_add(total, file.checksum.len(), "manifest queue checksum bytes")?;
        admitted_payload =
            checked_add(admitted_payload, path.len(), "manifest admitted path bytes")?;
        admitted_payload = checked_add(
            admitted_payload,
            file.checksum.len(),
            "manifest admitted checksum bytes",
        )?;
        for map in [&file.partition, &file.statistics, &file.metadata] {
            let metadata_bytes = estimate_metadata_bytes(map)?;
            total = checked_add(total, metadata_bytes, "manifest queue metadata bytes")?;
            admitted_payload = checked_add(
                admitted_payload,
                metadata_bytes,
                "manifest admitted metadata bytes",
            )?;
        }
        let lineage_bytes = file_lineage
            .checked_mul(size_of::<SourceSegmentLineage>())
            .ok_or_else(|| resource("manifest queue lineage byte estimate overflow"))?;
        total = checked_add(total, lineage_bytes, "manifest queue lineage bytes")?;
        if admitted_payload > MAX_ENCODED_METADATA_BYTES {
            return Err(resource(format!(
                "{context} payload estimate exceeds maximum actual=[{}] maximum=[{}]",
                admitted_payload, MAX_ENCODED_METADATA_BYTES
            )));
        }
    }
    Ok(total.max(1))
}

/// Validates and encodes a native persist command before taking the SQLite writer lock.
pub(crate) fn prepare_commit_data_files(
    actor: CatalogActorKind,
    command: CommitDataFilesCommand,
) -> Result<PreparedCommitDataFiles> {
    validate_actor(actor, "commit_data_files")?;
    validate_snapshot_version(command.expected_snapshot_version)?;
    validate_pending_files(&command.files, command.table_id, "persist")?;
    let committed_log_offset = require_known_watermark(command.committed_log_offset, "persist")?;
    let files = prepare_files(command.files)?;
    validate_contiguous_ranges(&files.coverage, "persist file")?;
    let last = files
        .coverage
        .last()
        .expect("nonempty manifest files have offset coverage")
        .last();
    if committed_log_offset != last {
        return Err(invalid(format!(
            "persist watermark does not equal file coverage end watermark=[{}] coverage_end=[{}]",
            committed_log_offset, last
        )));
    }
    let summary = manifest_summary(&files, 0)?;
    let summary_blob = encode_snapshot_summary_blob(&summary)?;
    ensure_encoded_budget(files.encoded_metadata_bytes, summary_blob.len())?;
    Ok(PreparedCommitDataFiles {
        table_id: command.table_id,
        expected_snapshot_version: command.expected_snapshot_version,
        files,
        committed_log_offset: command.committed_log_offset,
        summary,
        summary_blob,
    })
}

/// Validates and encodes a row-preserving compaction before taking the writer lock.
pub(crate) fn prepare_commit_compaction(
    actor: CatalogActorKind,
    command: CommitCompactionCommand,
) -> Result<PreparedCommitCompaction> {
    validate_actor(actor, "commit_compaction")?;
    validate_snapshot_version(command.expected_snapshot_version)?;
    validate_file_ids(
        &command.input_file_ids,
        MAX_COMPACTION_INPUTS,
        "compaction input",
    )?;
    validate_pending_files(&command.output_files, command.table_id, "compaction output")?;
    ensure_input_output_ids_disjoint(&command.input_file_ids, &command.output_files)?;
    require_known_watermark(command.committed_log_offset, "compaction")?;
    let output_files = prepare_files(command.output_files)?;
    let removed_files = u64::try_from(command.input_file_ids.len())
        .map_err(|_| resource("compaction input count is outside the supported domain"))?;
    let summary = manifest_summary(&output_files, removed_files)?;
    let summary_blob = encode_snapshot_summary_blob(&summary)?;
    ensure_encoded_budget(output_files.encoded_metadata_bytes, summary_blob.len())?;
    Ok(PreparedCommitCompaction {
        table_id: command.table_id,
        expected_snapshot_version: command.expected_snapshot_version,
        input_file_ids: command.input_file_ids,
        output_files,
        committed_log_offset: command.committed_log_offset,
        summary,
        summary_blob,
    })
}

fn validate_actor(actor: CatalogActorKind, operation: &str) -> Result<()> {
    if actor == CatalogActorKind::Server {
        Ok(())
    } else {
        Err(ServerError::new(
            ErrorCode::PermissionDenied,
            format!(
                "frozen V1 manifest command is server-internal operation=[{operation}] actor=[{actor:?}]"
            ),
        ))
    }
}

fn validate_snapshot_version(version: u64) -> Result<()> {
    if version == 0 {
        Err(invalid(
            "manifest expected snapshot version must be nonzero",
        ))
    } else {
        Ok(())
    }
}

fn require_known_watermark(value: OffsetWatermark, operation: &str) -> Result<LogOffset> {
    match value {
        OffsetWatermark::Known(offset) => Ok(offset),
        OffsetWatermark::Empty => Err(invalid(format!(
            "native {operation} requires a nonempty committed log watermark"
        ))),
        OffsetWatermark::LegacyUnknown => Err(unsupported(format!(
            "catalog.{operation}.legacy_unknown_watermark"
        ))),
    }
}

fn validate_pending_files(
    files: &[PendingDataFile],
    table_id: TableId,
    context: &str,
) -> Result<()> {
    if files.is_empty() {
        return Err(invalid(format!("{context} files must not be empty")));
    }
    if files.len() > MAX_MANIFEST_FILES {
        return Err(resource(format!(
            "{context} file count exceeds maximum actual=[{}] maximum=[{}]",
            files.len(),
            MAX_MANIFEST_FILES
        )));
    }
    for pair in files.windows(2) {
        if pair[0].file_id >= pair[1].file_id {
            return Err(invalid(format!(
                "{context} files must be strictly sorted by file id"
            )));
        }
    }

    let mut paths = HashSet::with_capacity(files.len());
    let mut lineage_rows = 0_usize;
    let mut estimated_bytes = 0_usize;
    for file in files {
        if file.table_id != table_id {
            return Err(invalid(format!(
                "{context} file belongs to another table file_id=[{}]",
                file.file_id
            )));
        }
        if file.schema_version == 0 {
            return Err(invalid(format!(
                "{context} file schema version must be nonzero file_id=[{}]",
                file.file_id
            )));
        }
        if file.file_size_bytes == 0 {
            return Err(invalid(format!(
                "{context} file size must be nonzero file_id=[{}]",
                file.file_id
            )));
        }
        let path = match &file.location {
            DataFileLocation::Managed(path) => path.as_str(),
            DataFileLocation::ExternalLegacy(_) => {
                return Err(unsupported(format!("catalog.{context}.external_legacy")));
            }
        };
        validate_managed_path(path, file.file_id)?;
        if !paths.insert(path) {
            return Err(invalid(format!(
                "{context} files contain duplicate managed path path=[{path}]"
            )));
        }
        validate_checksum(&file.checksum, file.file_id)?;
        let native = match &file.provenance {
            DataFileProvenance::Native(native) => native,
            DataFileProvenance::Legacy { .. } => {
                return Err(unsupported(format!("catalog.{context}.legacy")));
            }
        };
        if native.offsets().len() != u128::from(file.row_count.get()) {
            return Err(invalid(format!(
                "{context} file row count does not equal offset span file_id=[{}] rows=[{}] offsets=[{}]",
                file.file_id,
                file.row_count,
                native.offsets().len()
            )));
        }
        let file_lineage = native.source_lineage().as_slice().len();
        if file_lineage > MAX_LINEAGE_ROWS_PER_FILE {
            return Err(resource(format!(
                "{context} file lineage count exceeds maximum file_id=[{}] actual=[{}] maximum=[{}]",
                file.file_id, file_lineage, MAX_LINEAGE_ROWS_PER_FILE
            )));
        }
        lineage_rows = checked_add(lineage_rows, file_lineage, "manifest lineage count")?;
        if lineage_rows > MAX_MANIFEST_LINEAGE_ROWS {
            return Err(resource(format!(
                "{context} lineage count exceeds maximum actual=[{}] maximum=[{}]",
                lineage_rows, MAX_MANIFEST_LINEAGE_ROWS
            )));
        }
        for map in [&file.partition, &file.statistics, &file.metadata] {
            estimated_bytes = checked_add(
                estimated_bytes,
                estimate_metadata_bytes(map)?,
                "manifest metadata estimate",
            )?;
        }
        estimated_bytes = checked_add(estimated_bytes, path.len(), "manifest path bytes")?;
        estimated_bytes = checked_add(
            estimated_bytes,
            file.checksum.len(),
            "manifest checksum bytes",
        )?;
        if estimated_bytes > MAX_ENCODED_METADATA_BYTES {
            return Err(resource(format!(
                "{context} metadata estimate exceeds maximum actual=[{}] maximum=[{}]",
                estimated_bytes, MAX_ENCODED_METADATA_BYTES
            )));
        }
    }
    Ok(())
}

fn validate_file_ids(ids: &[FileId], maximum: usize, context: &str) -> Result<()> {
    if ids.is_empty() {
        return Err(invalid(format!("{context} files must not be empty")));
    }
    if ids.len() > maximum {
        return Err(resource(format!(
            "{context} file count exceeds maximum actual=[{}] maximum=[{}]",
            ids.len(),
            maximum
        )));
    }
    if ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid(format!(
            "{context} file ids must be strictly sorted"
        )));
    }
    Ok(())
}

fn ensure_input_output_ids_disjoint(inputs: &[FileId], outputs: &[PendingDataFile]) -> Result<()> {
    let input_ids = inputs.iter().copied().collect::<HashSet<_>>();
    if let Some(output) = outputs
        .iter()
        .find(|output| input_ids.contains(&output.file_id))
    {
        return Err(invalid(format!(
            "compaction output reuses an input file id file_id=[{}]",
            output.file_id
        )));
    }
    Ok(())
}

fn validate_managed_path(path: &str, file_id: FileId) -> Result<()> {
    if path.len() > MAX_FILE_PATH_BYTES {
        return Err(resource(format!(
            "manifest file path exceeds byte limit file_id=[{file_id}] actual=[{}] maximum=[{}]",
            path.len(),
            MAX_FILE_PATH_BYTES
        )));
    }
    if path.as_bytes().contains(&0) {
        return Err(invalid(format!(
            "manifest managed path contains a nul byte file_id=[{file_id}]"
        )));
    }
    let canonical = Path::new(path).components().collect::<PathBuf>();
    if canonical.to_str() != Some(path) {
        return Err(invalid(format!(
            "manifest managed path is not canonical file_id=[{file_id}] path=[{path}]"
        )));
    }
    Ok(())
}

fn validate_checksum(checksum: &str, file_id: FileId) -> Result<()> {
    if checksum.len() > MAX_CHECKSUM_BYTES {
        return Err(resource(format!(
            "manifest checksum exceeds byte limit file_id=[{file_id}] actual=[{}] maximum=[{}]",
            checksum.len(),
            MAX_CHECKSUM_BYTES
        )));
    }
    let Some((algorithm, digest)) = checksum.split_once(':') else {
        return Err(invalid(format!(
            "manifest checksum lacks an algorithm prefix file_id=[{file_id}]"
        )));
    };
    let valid_algorithm = algorithm
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_lowercase)
        && algorithm.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
        });
    let valid_digest = !digest.is_empty()
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase());
    if !valid_algorithm || !valid_digest {
        return Err(invalid(format!(
            "manifest checksum is not canonical file_id=[{file_id}]"
        )));
    }
    Ok(())
}

fn estimate_metadata_bytes(metadata: &MetadataMap) -> Result<usize> {
    if metadata.len() > MAX_METADATA_ENTRIES {
        return Err(resource(format!(
            "manifest metadata entry count exceeds maximum actual=[{}] maximum=[{}]",
            metadata.len(),
            MAX_METADATA_ENTRIES
        )));
    }
    let mut bytes = 0_usize;
    for (key, value) in metadata {
        bytes = checked_add(bytes, key.len(), "manifest metadata key bytes")?;
        let value_bytes = match value {
            MetadataScalar::Null => 0,
            MetadataScalar::Boolean(_) => 1,
            MetadataScalar::Int64(_) | MetadataScalar::UInt64(_) | MetadataScalar::Float64(_) => 8,
            MetadataScalar::String(value) => value.len(),
            MetadataScalar::Bytes(value) => value.len(),
        };
        bytes = checked_add(bytes, value_bytes, "manifest metadata value bytes")?;
        bytes = checked_add(bytes, 32, "manifest metadata encoding overhead")?;
    }
    Ok(bytes)
}

fn prepare_files(files: Vec<PendingDataFile>) -> Result<PreparedFiles> {
    let mut prepared = Vec::with_capacity(files.len());
    let mut schema_versions = BTreeSet::new();
    let mut schema_coverage = BTreeMap::<u64, Vec<OffsetRange>>::new();
    let mut lineage = BTreeMap::<SourceSegmentIdentity, Vec<OffsetRange>>::new();
    let mut coverage = Vec::with_capacity(files.len());
    let mut total_rows = 0_u64;
    let mut total_bytes = 0_u64;
    let mut lineage_rows = 0_usize;
    let mut max_writer_epoch = 0_u64;
    let mut encoded_metadata_bytes = 0_usize;
    for mut file in files {
        let DataFileProvenance::Native(native) = &file.provenance else {
            return Err(unsupported("catalog.manifest.legacy"));
        };
        coverage.push(native.offsets());
        schema_coverage
            .entry(file.schema_version)
            .or_default()
            .push(native.offsets());
        for source in native.source_lineage().as_slice() {
            max_writer_epoch = max_writer_epoch.max(source.source.writer_epoch);
            lineage
                .entry(source.source)
                .or_default()
                .push(source.offsets);
        }
        lineage_rows = checked_add(
            lineage_rows,
            native.source_lineage().as_slice().len(),
            "manifest lineage count",
        )?;
        total_rows = total_rows
            .checked_add(file.row_count.get())
            .ok_or_else(|| resource("manifest row count overflow"))?;
        total_bytes = total_bytes
            .checked_add(file.file_size_bytes)
            .ok_or_else(|| resource("manifest file byte count overflow"))?;
        schema_versions.insert(file.schema_version);
        let partition = std::mem::take(&mut file.partition);
        let partition_blob = encode_data_file_partition_blob(&partition)?;
        drop(partition);
        encoded_metadata_bytes =
            charge_encoded_metadata(encoded_metadata_bytes, partition_blob.len(), "partition")?;
        let statistics = std::mem::take(&mut file.statistics);
        let statistics_blob = encode_data_file_statistics_blob(&statistics)?;
        drop(statistics);
        encoded_metadata_bytes =
            charge_encoded_metadata(encoded_metadata_bytes, statistics_blob.len(), "statistics")?;
        let metadata = std::mem::take(&mut file.metadata);
        let metadata_blob = encode_data_file_metadata_blob(&metadata)?;
        drop(metadata);
        encoded_metadata_bytes =
            charge_encoded_metadata(encoded_metadata_bytes, metadata_blob.len(), "file metadata")?;
        prepared.push(PreparedDataFile {
            file,
            partition_blob,
            statistics_blob,
            metadata_blob,
        });
    }
    let coverage = canonicalize_ranges(coverage, ErrorCode::InvalidArgument, "manifest output")?;
    let schema_coverage = canonicalize_schema_coverage(
        schema_coverage,
        ErrorCode::InvalidArgument,
        "manifest output",
    )?;
    let lineage = canonicalize_lineage(lineage, ErrorCode::InvalidArgument, "manifest output")?;
    Ok(PreparedFiles {
        files: prepared,
        schema_versions,
        schema_coverage,
        lineage,
        coverage,
        total_rows,
        total_bytes,
        lineage_rows,
        max_writer_epoch,
        encoded_metadata_bytes,
    })
}

fn charge_encoded_metadata(current: usize, additional: usize, kind: &str) -> Result<usize> {
    let actual = checked_add(current, additional, "manifest encoded metadata bytes")?;
    if actual > MAX_ENCODED_METADATA_BYTES {
        return Err(resource(format!(
            "manifest encoded {kind} exceeds command byte budget actual=[{}] maximum=[{}]",
            actual, MAX_ENCODED_METADATA_BYTES
        )));
    }
    Ok(actual)
}

fn validate_contiguous_ranges(ranges: &[OffsetRange], context: &str) -> Result<()> {
    for pair in ranges.windows(2) {
        let Some(expected) = pair[0].last().checked_next() else {
            return Err(invalid(format!(
                "{context} coverage continues after the maximum log offset"
            )));
        };
        if pair[1].first() != expected {
            return Err(invalid(format!(
                "{context} coverage is not contiguous previous_end=[{}] next_start=[{}]",
                pair[0].last(),
                pair[1].first()
            )));
        }
    }
    Ok(())
}

fn canonicalize_ranges(
    mut ranges: Vec<OffsetRange>,
    error_code: ErrorCode,
    context: &str,
) -> Result<Vec<OffsetRange>> {
    ranges.sort_unstable_by_key(|range| (range.first(), range.last()));
    let mut canonical = Vec::<OffsetRange>::with_capacity(ranges.len());
    for range in ranges {
        let Some(previous) = canonical.last_mut() else {
            canonical.push(range);
            continue;
        };
        if range.first() <= previous.last() {
            return Err(ServerError::new(
                error_code,
                format!(
                    "{context} logical coverage overlaps previous=[{previous:?}] next=[{range:?}]"
                ),
            ));
        }
        if previous.last().checked_next() == Some(range.first()) {
            *previous = OffsetRange::new(previous.first(), range.last())
                .expect("adjacent ordered ranges form one valid range");
        } else {
            canonical.push(range);
        }
    }
    Ok(canonical)
}

fn canonicalize_lineage(
    mut lineage: BTreeMap<SourceSegmentIdentity, Vec<OffsetRange>>,
    error_code: ErrorCode,
    context: &str,
) -> Result<CanonicalLineage> {
    for ranges in lineage.values_mut() {
        ranges.sort_unstable_by_key(|range| (range.first(), range.last()));
        let mut canonical = Vec::<OffsetRange>::with_capacity(ranges.len());
        for range in ranges.drain(..) {
            let Some(previous) = canonical.last_mut() else {
                canonical.push(range);
                continue;
            };
            if range.first() <= previous.last() {
                return Err(ServerError::new(
                    error_code,
                    format!(
                        "{context} source lineage overlaps previous=[{previous:?}] next=[{range:?}]"
                    ),
                ));
            }
            if previous.last().checked_next() == Some(range.first()) {
                *previous = OffsetRange::new(previous.first(), range.last())
                    .expect("adjacent ordered ranges form one valid range");
            } else {
                canonical.push(range);
            }
        }
        *ranges = canonical;
    }
    Ok(lineage)
}

fn canonicalize_schema_coverage(
    mut coverage: BTreeMap<u64, Vec<OffsetRange>>,
    error_code: ErrorCode,
    context: &str,
) -> Result<CanonicalSchemaCoverage> {
    for (schema_version, ranges) in &mut coverage {
        let label = format!("{context} schema version {schema_version}");
        *ranges = canonicalize_ranges(std::mem::take(ranges), error_code, &label)?;
    }
    Ok(coverage)
}

fn manifest_summary(files: &PreparedFiles, removed_files: u64) -> Result<MetadataMap> {
    Ok(MetadataMap::from([
        (
            "added_bytes".to_owned(),
            MetadataScalar::UInt64(files.total_bytes),
        ),
        (
            "added_files".to_owned(),
            MetadataScalar::UInt64(
                u64::try_from(files.files.len())
                    .map_err(|_| resource("manifest file count is outside the supported domain"))?,
            ),
        ),
        (
            "added_rows".to_owned(),
            MetadataScalar::UInt64(files.total_rows),
        ),
        (
            "removed_files".to_owned(),
            MetadataScalar::UInt64(removed_files),
        ),
        (
            "source_lineage_rows".to_owned(),
            MetadataScalar::UInt64(
                u64::try_from(files.lineage_rows).map_err(|_| {
                    resource("manifest lineage count is outside the supported domain")
                })?,
            ),
        ),
    ]))
}

fn ensure_encoded_budget(metadata_bytes: usize, summary_bytes: usize) -> Result<()> {
    let actual = checked_add(
        metadata_bytes,
        summary_bytes,
        "manifest encoded metadata bytes",
    )?;
    if actual > MAX_ENCODED_METADATA_BYTES {
        return Err(resource(format!(
            "manifest encoded metadata exceeds maximum actual=[{}] maximum=[{}]",
            actual, MAX_ENCODED_METADATA_BYTES
        )));
    }
    Ok(())
}

fn checked_add(left: usize, right: usize, context: &str) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| resource(format!("{context} overflow")))
}

/// Publishes one contiguous native file set inside the caller's write transaction.
pub(crate) fn commit_data_files(
    connection: &Connection,
    prepared: PreparedCommitDataFiles,
    next_revision: u64,
    committed_at_ns: u64,
) -> Result<TableSnapshot> {
    let table = read_append_table(connection, prepared.table_id, "commit_data_files")?;
    validate_current_snapshot(
        prepared.expected_snapshot_version,
        table.current_snapshot_version,
    )?;
    validate_writer_epoch(prepared.files.max_writer_epoch, table.writer_epoch)?;
    validate_schema_versions(
        connection,
        prepared.table_id,
        table.current_schema_version,
        &prepared.files.schema_versions,
    )?;
    ensure_output_identities_available(connection, &prepared.files.files)?;

    let parent_watermark = read_parent_watermark(
        connection,
        prepared.table_id,
        prepared.expected_snapshot_version,
    )?;
    let first = prepared
        .files
        .coverage
        .first()
        .expect("nonempty prepared manifest has coverage")
        .first();
    validate_persist_parent_watermark(parent_watermark, first)?;
    let next_snapshot_version = next_snapshot_version(prepared.expected_snapshot_version)?;

    insert_snapshot(
        connection,
        prepared.table_id,
        next_snapshot_version,
        prepared.expected_snapshot_version,
        SnapshotOperation::Persist,
        next_revision,
        prepared.committed_log_offset,
        committed_at_ns,
        &prepared.summary_blob,
    )?;
    insert_output_files(
        connection,
        prepared.table_id,
        next_snapshot_version,
        &prepared.files.files,
    )?;
    advance_table_snapshot(
        connection,
        prepared.table_id,
        prepared.expected_snapshot_version,
        next_snapshot_version,
        committed_at_ns,
    )?;

    Ok(TableSnapshot {
        table_id: prepared.table_id,
        snapshot_version: next_snapshot_version,
        parent_snapshot_version: Some(prepared.expected_snapshot_version),
        operation: SnapshotOperation::Persist,
        commit_lsn: next_revision,
        committed_log_offset: prepared.committed_log_offset,
        created_at_ns: committed_at_ns,
        summary: prepared.summary,
    })
}

/// Replaces one exact append manifest subset inside the caller's write transaction.
pub(crate) fn commit_compaction(
    connection: &Connection,
    prepared: PreparedCommitCompaction,
    next_revision: u64,
    committed_at_ns: u64,
) -> Result<TableSnapshot> {
    let table = read_append_table(connection, prepared.table_id, "commit_compaction")?;
    validate_current_snapshot(
        prepared.expected_snapshot_version,
        table.current_snapshot_version,
    )?;
    validate_writer_epoch(prepared.output_files.max_writer_epoch, table.writer_epoch)?;
    validate_schema_versions(
        connection,
        prepared.table_id,
        table.current_schema_version,
        &prepared.output_files.schema_versions,
    )?;
    ensure_output_identities_available(connection, &prepared.output_files.files)?;

    let parent_watermark = read_parent_watermark(
        connection,
        prepared.table_id,
        prepared.expected_snapshot_version,
    )?;
    if parent_watermark != prepared.committed_log_offset {
        return Err(conflict(format!(
            "compaction watermark changed parent=[{parent_watermark:?}] requested=[{:?}]",
            prepared.committed_log_offset
        )));
    }
    let inputs = read_compaction_inputs(
        connection,
        prepared.table_id,
        prepared.expected_snapshot_version,
        table.current_schema_version,
        table.writer_epoch,
        &prepared.input_file_ids,
    )?;
    if inputs.total_rows != prepared.output_files.total_rows {
        return Err(invalid(format!(
            "compaction output row count differs from inputs input=[{}] output=[{}]",
            inputs.total_rows, prepared.output_files.total_rows
        )));
    }
    if inputs.coverage != prepared.output_files.coverage {
        return Err(invalid(
            "compaction output logical offset coverage differs from inputs",
        ));
    }
    if inputs.schema_coverage != prepared.output_files.schema_coverage {
        return Err(invalid(
            "compaction output schema coverage differs from inputs",
        ));
    }
    if inputs.lineage != prepared.output_files.lineage {
        return Err(invalid(
            "compaction output source lineage coverage differs from inputs",
        ));
    }

    let next_snapshot_version = next_snapshot_version(prepared.expected_snapshot_version)?;
    insert_snapshot(
        connection,
        prepared.table_id,
        next_snapshot_version,
        prepared.expected_snapshot_version,
        SnapshotOperation::Compaction,
        next_revision,
        prepared.committed_log_offset,
        committed_at_ns,
        &prepared.summary_blob,
    )?;
    insert_output_files(
        connection,
        prepared.table_id,
        next_snapshot_version,
        &prepared.output_files.files,
    )?;
    remove_compaction_inputs(
        connection,
        prepared.table_id,
        next_snapshot_version,
        &prepared.input_file_ids,
    )?;
    advance_table_snapshot(
        connection,
        prepared.table_id,
        prepared.expected_snapshot_version,
        next_snapshot_version,
        committed_at_ns,
    )?;

    Ok(TableSnapshot {
        table_id: prepared.table_id,
        snapshot_version: next_snapshot_version,
        parent_snapshot_version: Some(prepared.expected_snapshot_version),
        operation: SnapshotOperation::Compaction,
        commit_lsn: next_revision,
        committed_log_offset: prepared.committed_log_offset,
        created_at_ns: committed_at_ns,
        summary: prepared.summary,
    })
}

fn read_append_table(
    connection: &Connection,
    table_id: TableId,
    operation: &str,
) -> Result<ManifestTableState> {
    let encoded_table_id = table_id.into_bytes();
    let row = connection
        .query_row(
            "SELECT table_kind, current_schema_version, current_snapshot_version, writer_epoch, state \
             FROM tables WHERE table_id = ?1",
            params![encoded_table_id.as_slice()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read manifest table state", error))?
        .ok_or_else(|| not_found(format!("table was not found table_id=[{table_id}]")))?;
    let (table_kind, current_schema_version, current_snapshot_version, writer_epoch, state) = row;
    if state != "active" {
        return Err(conflict(format!(
            "manifest table is not active table_id=[{table_id}] state=[{state}]"
        )));
    }
    if table_kind != "append_table" {
        return Err(unsupported(format!(
            "catalog.{operation}.table_kind.{}",
            table_kind
        )));
    }
    let current_schema_version = decode_u64(&current_schema_version)?;
    let current_snapshot_version = decode_u64(&current_snapshot_version)?;
    let writer_epoch = decode_u64(&writer_epoch)?;
    if current_schema_version == 0 || current_snapshot_version == 0 {
        return Err(corruption(format!(
            "manifest table has a zero current version table_id=[{table_id}]"
        )));
    }
    Ok(ManifestTableState {
        current_schema_version,
        current_snapshot_version,
        writer_epoch,
    })
}

fn validate_current_snapshot(expected: u64, actual: u64) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(conflict(format!(
            "catalog snapshot changed expected=[{expected}] actual=[{actual}]"
        )))
    }
}

fn validate_writer_epoch(maximum_file_epoch: u64, current_table_epoch: u64) -> Result<()> {
    if maximum_file_epoch <= current_table_epoch {
        return Ok(());
    }
    Err(ServerError::new(
        ErrorCode::InvalidArgument,
        format!(
            "manifest lineage uses a future writer epoch current=[{current_table_epoch}] supplied=[{maximum_file_epoch}]"
        ),
    ))
}

fn read_parent_watermark(
    connection: &Connection,
    table_id: TableId,
    snapshot_version: u64,
) -> Result<OffsetWatermark> {
    let table_id = table_id.into_bytes();
    let snapshot_version = encode_u64(snapshot_version);
    let encoded = connection
        .query_row(
            "SELECT committed_log_offset FROM table_snapshots \
             WHERE table_id = ?1 AND snapshot_version = ?2",
            params![table_id.as_slice(), snapshot_version.as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read manifest parent watermark", error))?
        .ok_or_else(|| corruption("current table snapshot is missing"))?;
    decode_offset_watermark(&encoded)
}

fn validate_persist_parent_watermark(parent: OffsetWatermark, first: LogOffset) -> Result<()> {
    let expected = match parent {
        OffsetWatermark::Empty => LogOffset::new(0),
        OffsetWatermark::Known(offset) => offset
            .checked_next()
            .ok_or_else(|| conflict("cannot persist after the maximum committed log offset"))?,
        OffsetWatermark::LegacyUnknown => {
            return Err(unsupported(
                "catalog.commit_data_files.legacy_unknown_parent",
            ));
        }
    };
    if first != expected {
        return Err(conflict(format!(
            "persist file coverage does not continue parent watermark expected_start=[{expected}] actual_start=[{first}]"
        )));
    }
    Ok(())
}

fn next_snapshot_version(current: u64) -> Result<u64> {
    current
        .checked_add(1)
        .ok_or_else(|| resource("catalog snapshot version space is exhausted"))
}

fn validate_schema_versions(
    connection: &Connection,
    table_id: TableId,
    current_schema_version: u64,
    requested: &BTreeSet<u64>,
) -> Result<()> {
    if let Some(future) = requested
        .iter()
        .find(|version| **version > current_schema_version)
    {
        return Err(ServerError::new(
            ErrorCode::SchemaMismatch,
            format!(
                "manifest file schema version is newer than the table table_id=[{table_id}] current=[{current_schema_version}] requested=[{future}]"
            ),
        ));
    }
    let encoded_table_id = table_id.into_bytes();
    let mut found = BTreeSet::new();
    let versions = requested.iter().copied().collect::<Vec<_>>();
    for chunk in versions.chunks(SQLITE_IN_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT schema_version FROM table_schemas \
             WHERE table_id = ? AND schema_version IN ({placeholders})"
        );
        let encoded_versions = chunk.iter().copied().map(encode_u64).collect::<Vec<_>>();
        let parameters = std::iter::once(encoded_table_id.as_slice())
            .chain(encoded_versions.iter().map(|value| value.as_slice()));
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| crate::map_sqlite_error("prepare manifest schema lookup", error))?;
        let mut rows = statement
            .query(params_from_iter(parameters))
            .map_err(|error| crate::map_sqlite_error("query manifest schema lookup", error))?;
        while let Some(row) = rows
            .next()
            .map_err(|error| crate::map_sqlite_error("read manifest schema lookup", error))?
        {
            let encoded: Vec<u8> = row.get(0).map_err(|error| {
                crate::map_sqlite_error("decode manifest schema version", error)
            })?;
            found.insert(decode_u64(&encoded)?);
        }
    }
    if let Some(missing) = requested.iter().find(|version| !found.contains(version)) {
        return Err(ServerError::new(
            ErrorCode::SchemaMismatch,
            format!(
                "manifest file schema version does not exist table_id=[{table_id}] schema_version=[{missing}]"
            ),
        ));
    }
    Ok(())
}

fn ensure_output_identities_available(
    connection: &Connection,
    files: &[PreparedDataFile],
) -> Result<()> {
    for chunk in files.chunks(SQLITE_IN_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT 1 FROM data_files WHERE file_id IN ({placeholders}) LIMIT 1");
        let encoded = chunk
            .iter()
            .map(|file| file.file.file_id.into_bytes())
            .collect::<Vec<_>>();
        let found = connection
            .query_row(
                &sql,
                params_from_iter(encoded.iter().map(|value| value.as_slice())),
                |_| Ok(()),
            )
            .optional()
            .map_err(|error| crate::map_sqlite_error("check manifest file identities", error))?;
        if found.is_some() {
            return Err(already_exists(
                "manifest output file identity already exists",
            ));
        }
    }
    for chunk in files.chunks(SQLITE_IN_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT 1 FROM data_files WHERE file_path IN ({placeholders}) LIMIT 1");
        let paths = chunk.iter().map(prepared_managed_path).collect::<Vec<_>>();
        let found = connection
            .query_row(&sql, params_from_iter(paths.iter().copied()), |_| Ok(()))
            .optional()
            .map_err(|error| crate::map_sqlite_error("check manifest file paths", error))?;
        if found.is_some() {
            return Err(already_exists("manifest output file path already exists"));
        }
    }
    Ok(())
}

fn prepared_managed_path(file: &PreparedDataFile) -> &str {
    match &file.file.location {
        DataFileLocation::Managed(path) => path.as_str(),
        DataFileLocation::ExternalLegacy(_) => {
            unreachable!("prepared native manifest files are managed")
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_snapshot(
    connection: &Connection,
    table_id: TableId,
    snapshot_version: u64,
    parent_snapshot_version: u64,
    operation: SnapshotOperation,
    commit_lsn: u64,
    committed_log_offset: OffsetWatermark,
    created_at_ns: u64,
    summary_blob: &[u8],
) -> Result<()> {
    let table_id = table_id.into_bytes();
    let snapshot_version = encode_u64(snapshot_version);
    let parent_snapshot_version = encode_optional_u64(Some(parent_snapshot_version));
    let operation = match operation {
        SnapshotOperation::Persist => "persist",
        SnapshotOperation::Compaction => "compaction",
        SnapshotOperation::Create | SnapshotOperation::Import | SnapshotOperation::Retention => {
            unreachable!("manifest writer only creates persist and compaction snapshots")
        }
    };
    connection
        .execute(
            "INSERT INTO table_snapshots(\
                 table_id, snapshot_version, parent_snapshot_version, operation, commit_lsn, \
                 committed_log_offset, created_at_ns, summary_blob\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                table_id.as_slice(),
                snapshot_version.as_slice(),
                parent_snapshot_version.as_slice(),
                operation,
                encode_u64(commit_lsn).as_slice(),
                encode_offset_watermark(committed_log_offset).as_slice(),
                encode_u64(created_at_ns).as_slice(),
                summary_blob,
            ],
        )
        .map_err(|error| crate::map_sqlite_error("insert catalog manifest snapshot", error))?;
    Ok(())
}

fn insert_output_files(
    connection: &Connection,
    table_id: TableId,
    added_snapshot_version: u64,
    files: &[PreparedDataFile],
) -> Result<()> {
    let table_id = table_id.into_bytes();
    let added_snapshot_version = encode_u64(added_snapshot_version);
    let absent = encode_optional_u64(None);
    let mut insert_file = connection
        .prepare_cached(
            "INSERT INTO data_files(\
                 file_id, table_id, schema_version, location_kind, file_path, \
                 file_size_bytes, row_count, provenance_kind, min_log_offset, \
                 max_log_offset, legacy_preserve_discovery_order, legacy_discovery_order, \
                 partition_blob, statistics_blob, metadata_blob, checksum, \
                 added_snapshot_version, removed_snapshot_version, state, created_at_ns\
             ) VALUES (\
                 ?1, ?2, ?3, 'managed', ?4, ?5, ?6, 'native', ?7, ?8, 0, ?9, \
                 ?10, ?11, ?12, ?13, ?14, ?15, 'committed', ?16\
             )",
        )
        .map_err(|error| crate::map_sqlite_error("prepare manifest file insert", error))?;
    for prepared in files {
        let file = &prepared.file;
        let DataFileProvenance::Native(native) = &file.provenance else {
            unreachable!("prepared manifest files have native provenance");
        };
        let file_id = file.file_id.into_bytes();
        let offsets = native.offsets();
        let minimum = encode_optional_u64(Some(offsets.first().get()));
        let maximum = encode_optional_u64(Some(offsets.last().get()));
        insert_file
            .execute(params![
                file_id.as_slice(),
                table_id.as_slice(),
                encode_u64(file.schema_version).as_slice(),
                prepared_managed_path(prepared),
                encode_u64(file.file_size_bytes).as_slice(),
                encode_u64(file.row_count.get()).as_slice(),
                minimum.as_slice(),
                maximum.as_slice(),
                absent.as_slice(),
                prepared.partition_blob.as_slice(),
                prepared.statistics_blob.as_slice(),
                prepared.metadata_blob.as_slice(),
                file.checksum.as_str(),
                added_snapshot_version.as_slice(),
                absent.as_slice(),
                encode_u64(file.created_at_ns).as_slice(),
            ])
            .map_err(|error| crate::map_sqlite_error("insert catalog manifest file", error))?;
    }
    drop(insert_file);

    let mut insert_lineage = connection
        .prepare_cached(
            "INSERT INTO data_file_sources(\
                 file_id, source_segment_id, source_generation, writer_epoch, \
                 min_log_offset, max_log_offset\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .map_err(|error| crate::map_sqlite_error("prepare manifest lineage insert", error))?;
    for prepared in files {
        let file_id = prepared.file.file_id.into_bytes();
        let DataFileProvenance::Native(native) = &prepared.file.provenance else {
            unreachable!("prepared manifest files have native provenance");
        };
        for lineage in native.source_lineage().as_slice() {
            insert_lineage
                .execute(params![
                    file_id.as_slice(),
                    encode_u64(lineage.source.segment_id).as_slice(),
                    encode_u64(lineage.source.generation).as_slice(),
                    encode_u64(lineage.source.writer_epoch).as_slice(),
                    encode_u64(lineage.offsets.first().get()).as_slice(),
                    encode_u64(lineage.offsets.last().get()).as_slice(),
                ])
                .map_err(|error| {
                    crate::map_sqlite_error("insert catalog manifest lineage", error)
                })?;
        }
    }
    Ok(())
}

fn remove_compaction_inputs(
    connection: &Connection,
    table_id: TableId,
    removed_snapshot_version: u64,
    input_file_ids: &[FileId],
) -> Result<()> {
    let table_id = table_id.into_bytes();
    let removed_snapshot_version = encode_optional_u64(Some(removed_snapshot_version));
    let absent = encode_optional_u64(None);
    let mut updated = 0_usize;
    for chunk in input_file_ids.chunks(SQLITE_IN_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            concat!(
                "UPDATE data_files SET removed_snapshot_version = ? ",
                "WHERE table_id = ? AND state = 'committed' ",
                "AND removed_snapshot_version = ? AND file_id IN ({})"
            ),
            placeholders
        );
        let encoded_ids = chunk
            .iter()
            .map(|file_id| file_id.into_bytes())
            .collect::<Vec<_>>();
        let parameters = [
            removed_snapshot_version.as_slice(),
            table_id.as_slice(),
            absent.as_slice(),
        ]
        .into_iter()
        .chain(encoded_ids.iter().map(|value| value.as_slice()));
        let chunk_updated = connection
            .execute(&sql, params_from_iter(parameters))
            .map_err(|error| crate::map_sqlite_error("remove compaction input files", error))?;
        updated = updated
            .checked_add(chunk_updated)
            .ok_or_else(|| resource("compaction updated file count overflow"))?;
    }
    if updated != input_file_ids.len() {
        return Err(conflict(format!(
            "compaction input set changed expected=[{}] updated=[{}]",
            input_file_ids.len(),
            updated
        )));
    }
    Ok(())
}

fn advance_table_snapshot(
    connection: &Connection,
    table_id: TableId,
    expected_snapshot_version: u64,
    next_snapshot_version: u64,
    updated_at_ns: u64,
) -> Result<()> {
    let updated = connection
        .execute(
            "UPDATE tables \
             SET current_snapshot_version = ?1, updated_at_ns = ?2 \
             WHERE table_id = ?3 AND current_snapshot_version = ?4 AND state = 'active'",
            params![
                encode_u64(next_snapshot_version).as_slice(),
                encode_u64(updated_at_ns).as_slice(),
                table_id.into_bytes().as_slice(),
                encode_u64(expected_snapshot_version).as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("advance table manifest snapshot", error))?;
    if updated != 1 {
        return Err(conflict(
            "table snapshot changed while committing the manifest",
        ));
    }
    Ok(())
}

fn read_compaction_inputs(
    connection: &Connection,
    table_id: TableId,
    snapshot_version: u64,
    current_schema_version: u64,
    current_writer_epoch: u64,
    input_file_ids: &[FileId],
) -> Result<InputManifest> {
    let mut raw_files = read_raw_input_files(connection, input_file_ids)?;
    let mut source_rows = read_input_lineage(connection, input_file_ids)?;
    let mut schema_coverage = BTreeMap::<u64, Vec<OffsetRange>>::new();
    let mut lineage = BTreeMap::<SourceSegmentIdentity, Vec<OffsetRange>>::new();
    let mut coverage = Vec::with_capacity(input_file_ids.len());
    let mut total_rows = 0_u64;

    for file_id in input_file_ids {
        let raw = raw_files.remove(file_id).ok_or_else(|| {
            conflict(format!(
                "compaction input set is stale because a file was not found file_id=[{file_id}]"
            ))
        })?;
        let input_table_id = decode_table_id(raw.table_id)?;
        if input_table_id != table_id {
            return Err(conflict(format!(
                "compaction input belongs to another table file_id=[{file_id}]"
            )));
        }
        if raw.location_kind != "managed" || raw.provenance_kind != "native" {
            return Err(unsupported("catalog.commit_compaction.non_native_input"));
        }
        let schema_version = decode_u64(&raw.schema_version)?;
        if schema_version == 0 || schema_version > current_schema_version {
            return Err(corruption(format!(
                "compaction input schema version is outside the table cursor file_id=[{file_id}] current=[{current_schema_version}] actual=[{schema_version}]"
            )));
        }
        let added_snapshot_version = decode_u64(&raw.added_snapshot_version)?;
        let removed_snapshot_version = decode_optional_u64(&raw.removed_snapshot_version)?;
        if raw.state != "committed"
            || added_snapshot_version == 0
            || added_snapshot_version > snapshot_version
            || removed_snapshot_version.is_some()
        {
            return Err(conflict(format!(
                "compaction input is not visible in the current snapshot file_id=[{file_id}] snapshot=[{snapshot_version}]"
            )));
        }
        let first = decode_optional_u64(&raw.min_log_offset)?
            .ok_or_else(|| corruption("native compaction input lacks a minimum log offset"))?;
        let last = decode_optional_u64(&raw.max_log_offset)?
            .ok_or_else(|| corruption("native compaction input lacks a maximum log offset"))?;
        let offsets =
            OffsetRange::new(LogOffset::new(first), LogOffset::new(last)).map_err(|error| {
                corruption(format!(
                    "compaction input offset range is invalid error=[{error}]"
                ))
            })?;
        let row_count = decode_u64(&raw.row_count)?;
        if offsets.len() != u128::from(row_count) {
            return Err(corruption(format!(
                "compaction input row count does not equal offset span file_id=[{file_id}] rows=[{row_count}] offsets=[{}]",
                offsets.len()
            )));
        }
        coverage.push(offsets);
        schema_coverage
            .entry(schema_version)
            .or_default()
            .push(offsets);
        total_rows = total_rows
            .checked_add(row_count)
            .ok_or_else(|| resource("compaction input row count overflow"))?;

        let mut file_sources = source_rows.remove(file_id).unwrap_or_default();
        file_sources.sort_unstable_by_key(|source| source.source);
        if let Some(future) = file_sources
            .iter()
            .find(|source| source.source.writer_epoch > current_writer_epoch)
        {
            return Err(corruption(format!(
                "compaction input lineage has a future writer epoch file_id=[{file_id}] current=[{current_writer_epoch}] actual=[{}]",
                future.source.writer_epoch
            )));
        }
        let validated_sources = SourceSegmentLineages::new(file_sources).map_err(|error| {
            corruption(format!(
                "compaction input source lineage is invalid file_id=[{file_id}] error=[{error}]"
            ))
        })?;
        for source in validated_sources.as_slice() {
            lineage
                .entry(source.source)
                .or_default()
                .push(source.offsets);
        }
        NativeDataFileProvenance::new(offsets, validated_sources).map_err(|error| {
            corruption(format!(
                "compaction input provenance is invalid file_id=[{file_id}] error=[{error}]"
            ))
        })?;
    }
    if !raw_files.is_empty() || !source_rows.is_empty() {
        return Err(corruption(
            "compaction input query returned unassigned file metadata",
        ));
    }
    Ok(InputManifest {
        schema_coverage: canonicalize_schema_coverage(
            schema_coverage,
            ErrorCode::DataCorruption,
            "compaction input",
        )?,
        lineage: canonicalize_lineage(lineage, ErrorCode::DataCorruption, "compaction input")?,
        coverage: canonicalize_ranges(coverage, ErrorCode::DataCorruption, "compaction input")?,
        total_rows,
    })
}

fn read_raw_input_files(
    connection: &Connection,
    input_file_ids: &[FileId],
) -> Result<HashMap<FileId, RawInputFile>> {
    let expected = input_file_ids.iter().copied().collect::<HashSet<_>>();
    let mut files = HashMap::with_capacity(input_file_ids.len());
    for chunk in input_file_ids.chunks(SQLITE_IN_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            concat!(
                "SELECT file_id, table_id, schema_version, location_kind, row_count, provenance_kind, ",
                "min_log_offset, max_log_offset, added_snapshot_version, ",
                "removed_snapshot_version, state FROM data_files WHERE file_id IN ({})"
            ),
            placeholders
        );
        let encoded_ids = chunk
            .iter()
            .map(|file_id| file_id.into_bytes())
            .collect::<Vec<_>>();
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| crate::map_sqlite_error("prepare compaction input lookup", error))?;
        let mut rows = statement
            .query(params_from_iter(
                encoded_ids.iter().map(|value| value.as_slice()),
            ))
            .map_err(|error| crate::map_sqlite_error("query compaction inputs", error))?;
        while let Some(row) = rows
            .next()
            .map_err(|error| crate::map_sqlite_error("read compaction input", error))?
        {
            let file_id =
                decode_file_id(row.get(0).map_err(|error| {
                    crate::map_sqlite_error("decode compaction file id", error)
                })?)?;
            if !expected.contains(&file_id) || files.contains_key(&file_id) {
                return Err(corruption(
                    "compaction input lookup returned an unexpected duplicate file",
                ));
            }
            files.insert(
                file_id,
                RawInputFile {
                    table_id: row.get(1).map_err(|error| {
                        crate::map_sqlite_error("decode compaction input table id", error)
                    })?,
                    schema_version: row.get(2).map_err(|error| {
                        crate::map_sqlite_error("decode compaction input schema version", error)
                    })?,
                    location_kind: row.get(3).map_err(|error| {
                        crate::map_sqlite_error("decode compaction input location", error)
                    })?,
                    row_count: row.get(4).map_err(|error| {
                        crate::map_sqlite_error("decode compaction input row count", error)
                    })?,
                    provenance_kind: row.get(5).map_err(|error| {
                        crate::map_sqlite_error("decode compaction input provenance", error)
                    })?,
                    min_log_offset: row.get(6).map_err(|error| {
                        crate::map_sqlite_error("decode compaction input minimum offset", error)
                    })?,
                    max_log_offset: row.get(7).map_err(|error| {
                        crate::map_sqlite_error("decode compaction input maximum offset", error)
                    })?,
                    added_snapshot_version: row.get(8).map_err(|error| {
                        crate::map_sqlite_error("decode compaction input added snapshot", error)
                    })?,
                    removed_snapshot_version: row.get(9).map_err(|error| {
                        crate::map_sqlite_error("decode compaction input removed snapshot", error)
                    })?,
                    state: row.get(10).map_err(|error| {
                        crate::map_sqlite_error("decode compaction input state", error)
                    })?,
                },
            );
        }
    }
    Ok(files)
}

fn read_input_lineage(
    connection: &Connection,
    input_file_ids: &[FileId],
) -> Result<HashMap<FileId, Vec<SourceSegmentLineage>>> {
    let expected = input_file_ids.iter().copied().collect::<HashSet<_>>();
    let mut lineage = HashMap::<FileId, Vec<SourceSegmentLineage>>::new();
    let mut total_rows = 0_usize;
    for chunk in input_file_ids.chunks(SQLITE_IN_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            concat!(
                "SELECT file_id, source_segment_id, source_generation, writer_epoch, ",
                "min_log_offset, max_log_offset FROM data_file_sources ",
                "WHERE file_id IN ({})"
            ),
            placeholders
        );
        let encoded_ids = chunk
            .iter()
            .map(|file_id| file_id.into_bytes())
            .collect::<Vec<_>>();
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| crate::map_sqlite_error("prepare compaction lineage lookup", error))?;
        let mut rows = statement
            .query(params_from_iter(
                encoded_ids.iter().map(|value| value.as_slice()),
            ))
            .map_err(|error| crate::map_sqlite_error("query compaction lineage", error))?;
        while let Some(row) = rows
            .next()
            .map_err(|error| crate::map_sqlite_error("read compaction lineage", error))?
        {
            total_rows = checked_add(total_rows, 1, "compaction input lineage count")?;
            if total_rows > MAX_MANIFEST_LINEAGE_ROWS {
                return Err(resource(format!(
                    "compaction input lineage count exceeds maximum actual=[{}] maximum=[{}]",
                    total_rows, MAX_MANIFEST_LINEAGE_ROWS
                )));
            }
            let file_id = decode_file_id(row.get(0).map_err(|error| {
                crate::map_sqlite_error("decode compaction lineage file id", error)
            })?)?;
            if !expected.contains(&file_id) {
                return Err(corruption(
                    "compaction lineage references an unexpected input file",
                ));
            }
            let rows_for_file = lineage.entry(file_id).or_default();
            if rows_for_file.len() >= MAX_LINEAGE_ROWS_PER_FILE {
                return Err(resource(format!(
                    "compaction input lineage per-file maximum exceeded file_id=[{file_id}] maximum=[{MAX_LINEAGE_ROWS_PER_FILE}]"
                )));
            }
            rows_for_file.push(SourceSegmentLineage {
                source: SourceSegmentIdentity {
                    segment_id: decode_u64(&row.get::<_, Vec<u8>>(1).map_err(|error| {
                        crate::map_sqlite_error("decode compaction lineage segment id", error)
                    })?)?,
                    generation: decode_u64(&row.get::<_, Vec<u8>>(2).map_err(|error| {
                        crate::map_sqlite_error("decode compaction lineage generation", error)
                    })?)?,
                    writer_epoch: decode_u64(&row.get::<_, Vec<u8>>(3).map_err(|error| {
                        crate::map_sqlite_error("decode compaction lineage writer epoch", error)
                    })?)?,
                },
                offsets: OffsetRange::new(
                    LogOffset::new(decode_u64(&row.get::<_, Vec<u8>>(4).map_err(|error| {
                        crate::map_sqlite_error("decode compaction lineage minimum offset", error)
                    })?)?),
                    LogOffset::new(decode_u64(&row.get::<_, Vec<u8>>(5).map_err(|error| {
                        crate::map_sqlite_error("decode compaction lineage maximum offset", error)
                    })?)?),
                )
                .map_err(|error| {
                    corruption(format!(
                        "compaction lineage offset range is invalid error=[{error}]"
                    ))
                })?,
            });
        }
    }
    Ok(lineage)
}

fn decode_file_id(encoded: Vec<u8>) -> Result<FileId> {
    let bytes: [u8; 16] = encoded.try_into().map_err(|encoded: Vec<u8>| {
        corruption(format!(
            "compaction input file id has invalid width actual=[{}]",
            encoded.len()
        ))
    })?;
    FileId::from_bytes(bytes).map_err(|error| {
        corruption(format!(
            "compaction input file id is invalid error=[{error}]"
        ))
    })
}

fn decode_table_id(encoded: Vec<u8>) -> Result<TableId> {
    let bytes: [u8; 16] = encoded.try_into().map_err(|encoded: Vec<u8>| {
        corruption(format!(
            "compaction input table id has invalid width actual=[{}]",
            encoded.len()
        ))
    })?;
    TableId::from_bytes(bytes).map_err(|error| {
        corruption(format!(
            "compaction input table id is invalid error=[{error}]"
        ))
    })
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message)
}

fn resource(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message)
}

fn conflict(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Conflict, message)
}

fn not_found(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::NotFound, message)
}

fn already_exists(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::AlreadyExists, message)
}

fn corruption(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
}

fn unsupported(operator: impl Into<String>) -> ServerError {
    let operator = operator.into();
    ServerError::new(
        ErrorCode::UnsupportedPlan,
        format!("manifest operation is not supported operator=[{operator}]"),
    )
    .with_details(ServerErrorDetails::UnsupportedPlan { operator })
    .expect("unsupported-plan details match the unsupported-plan error code")
}
