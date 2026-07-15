//! Read-only catalog compatibility over persistent legacy registry metadata.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{self, Write};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use serde::Serialize;
use sha2::{Digest, Sha256};
use zippy_types::catalog::{
    compute_legacy_catalog_projection_digest_v4, CatalogCommandV1, CatalogQuerySnapshot,
    CatalogRead, CatalogSnapshotSelector, CatalogWrite, LegacyCatalogProjectionDigestTableV4,
    LegacyCatalogProjectionTableStateV4, LegacyCatalogSourceV4, LegacyCatalogValueV4,
    ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
    NamespaceRef, VersionedCatalogCommand, VersionedCatalogResponse,
    MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4, MAX_LEGACY_CATALOG_PROJECTION_FILES_V4,
    MAX_LEGACY_CATALOG_PROJECTION_SOURCES_V4, MAX_LEGACY_CATALOG_VALUE_DEPTH_V4,
    MAX_LEGACY_CATALOG_VALUE_NODES_V4,
};
use zippy_types::schema_contract::{validate_catalog_table_schema, MAX_CATALOG_SCHEMA_IPC_BYTES};
use zippy_types::schema_hash::canonical_catalog_schema_hash_v1;
use zippy_types::table::{
    AppendTableDefinition, CatalogName, ColumnName, ColumnSet, FiniteMetadataFloat,
    FrameTableDefinition, KeyValueTableDefinition, NamespaceDescriptor, TableDefinition,
    TableDescriptor, TableRef, TableState,
};
use zippy_types::{
    BoxFuture, ErrorCode, NamespaceId, Result, ServerError, ServerErrorDetails, TableId,
};

use crate::registry::Registry;

const LEGACY_ID_SCHEME: &[u8] = b"zippy.legacy_catalog.id.v1";
const TABLE_KIND_FIELD: &str = "zippy_table_kind";
const FRAME_MODE_FIELD: &str = "zippy_frame_mode";
const FRAME_BY_FIELD: &str = "zippy_frame_by";
const FRAME_PROTOCOL_VERSION_FIELD: &str = "zippy_frame_protocol_version";
const KEY_VALUE_KEYS_FIELD: &str = "zippy_key_value_keys";
const LEGACY_LATEST_BY_FIELD: &str = "zippy_latest_by";
const INTERNAL_TABLE_FIELD: &str = "zippy_internal";
const SEGMENT_PER_BATCH_FRAME_MODE: &str = "segment_per_batch";
const FRAME_PROTOCOL_VERSION: u64 = 1;
const LEGACY_MIGRATION_SNAPSHOT_HASH_SCHEME: &str = "sha256-v1";

/// One atomically captured, normalized view of the table-relevant legacy registry state.
///
/// The hash covers the normalized migration view rather than the byte layout of
/// `master-registry.json`. Process, source, and engine liveness are deliberately excluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyCatalogMigrationSnapshot {
    source_catalog_revision: u64,
    source_snapshot_hash: String,
    namespace: NamespaceDescriptor,
    tables: Vec<LegacyCatalogMigrationTable>,
}

impl LegacyCatalogMigrationSnapshot {
    /// Returns the legacy Catalog revision captured under the registry lock.
    #[must_use]
    pub const fn source_catalog_revision(&self) -> u64 {
        self.source_catalog_revision
    }

    /// Returns the deterministic hash of the normalized migration view.
    #[must_use]
    pub fn source_snapshot_hash(&self) -> &str {
        &self.source_snapshot_hash
    }

    /// Returns the stable default namespace represented by the legacy registry.
    #[must_use]
    pub const fn namespace(&self) -> &NamespaceDescriptor {
        &self.namespace
    }

    /// Returns public legacy tables in strict stable-table-id order.
    #[must_use]
    pub fn tables(&self) -> &[LegacyCatalogMigrationTable] {
        &self.tables
    }
}

/// One atomically captured complete V4 projection of the active public legacy Catalog.
///
/// This projection is deliberately independent from [`LegacyCatalogMigrationSnapshot`]. Its
/// digest binds only logical table state and stable source fields; the source Catalog revision is
/// returned as checkpoint evidence but is not part of that digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyCatalogProjectionSnapshotV4 {
    catalog_revision: u64,
    global_projection_digest: String,
    namespace: NamespaceDescriptor,
    tables: Vec<LegacyCatalogActivePublicTableV4>,
}

impl LegacyCatalogProjectionSnapshotV4 {
    /// Returns the legacy Catalog revision captured under the same registry lock as the snapshot.
    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    /// Returns the canonical digest of all active public tables and their stable sources.
    #[must_use]
    pub fn global_projection_digest(&self) -> &str {
        &self.global_projection_digest
    }

    /// Returns the stable default namespace represented by the legacy registry.
    #[must_use]
    pub const fn namespace(&self) -> &NamespaceDescriptor {
        &self.namespace
    }

    /// Returns active public legacy tables in strict stable-table-id order.
    #[must_use]
    pub fn tables(&self) -> &[LegacyCatalogActivePublicTableV4] {
        &self.tables
    }
}

/// Complete detached V4 input for one active public legacy table.
///
/// The DTO contains the schema and file evidence needed to build a bootstrap or adoption command,
/// together with the complete stable source set used by the V4 projection digest. Runtime source
/// ownership, process state, status, metrics, and per-process revisions are absent by type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyCatalogActivePublicTableV4 {
    descriptor: TableDescriptor,
    legacy_schema: serde_json::Value,
    legacy_schema_hash: String,
    canonical_schema_hash: String,
    schema_ipc: zippy_types::table::SchemaIpcPayload,
    schema_has_custom_metadata: bool,
    persisted_files_revision: u64,
    migration_files: Vec<LegacyMigrationFile>,
    sources: Vec<LegacyCatalogSourceV4>,
}

impl LegacyCatalogActivePublicTableV4 {
    /// Returns the stable normalized active table descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &TableDescriptor {
        &self.descriptor
    }

    /// Returns the original JSON Arrow schema metadata stored by the registry.
    #[must_use]
    pub const fn legacy_schema(&self) -> &serde_json::Value {
        &self.legacy_schema
    }

    /// Returns the legacy control-plane schema hash.
    #[must_use]
    pub fn legacy_schema_hash(&self) -> &str {
        &self.legacy_schema_hash
    }

    /// Returns the canonical hash recomputed from the parsed Arrow Schema.
    #[must_use]
    pub fn canonical_schema_hash(&self) -> &str {
        &self.canonical_schema_hash
    }

    /// Returns the schema-only Arrow IPC stream required by a V4 bootstrap command.
    #[must_use]
    pub const fn schema_ipc(&self) -> &zippy_types::table::SchemaIpcPayload {
        &self.schema_ipc
    }

    /// Returns whether custom metadata makes regenerated Arrow IPC byte ordering unstable.
    #[must_use]
    pub const fn schema_has_custom_metadata(&self) -> bool {
        self.schema_has_custom_metadata
    }

    /// Returns the exact legacy persisted-file view revision represented by this table.
    #[must_use]
    pub const fn persisted_files_revision(&self) -> u64 {
        self.persisted_files_revision
    }

    /// Returns validated persisted-file projections in Gateway-compatible scan order metadata.
    #[must_use]
    pub fn migration_files(&self) -> &[LegacyMigrationFile] {
        &self.migration_files
    }

    /// Returns complete stable source records in strict source-name order.
    #[must_use]
    pub fn sources(&self) -> &[LegacyCatalogSourceV4] {
        &self.sources
    }

    fn projection_digest_input(&self) -> LegacyCatalogProjectionDigestTableV4 {
        LegacyCatalogProjectionDigestTableV4 {
            table_id: self.descriptor.table_id,
            namespace_id: self.descriptor.namespace_id,
            name: self.descriptor.name.clone(),
            definition: self.descriptor.definition.clone(),
            schema_version: self.descriptor.current_schema_version,
            schema_hash: self.canonical_schema_hash.clone(),
            config: source_owned_table_config(&self.descriptor.config),
            legacy_files_revision: self.persisted_files_revision,
            writer_epoch: self.descriptor.writer_epoch,
            state: LegacyCatalogProjectionTableStateV4::Active,
            sources: self.sources.clone(),
        }
    }
}

/// Complete migration input retained for one public legacy table.
///
/// `persisted_files` preserves legacy discovery order. Frozen V1 Catalog commands cannot import
/// those files; the raw values are retained so a later typed V2 importer can validate paths,
/// derive stable file identities, and preserve ordering without rereading a moving registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyCatalogMigrationTable {
    descriptor: TableDescriptor,
    legacy_schema: serde_json::Value,
    legacy_schema_hash: String,
    schema_ipc: zippy_types::table::SchemaIpcPayload,
    schema_has_custom_metadata: bool,
    persisted_files_revision: u64,
    persisted_files: Vec<serde_json::Value>,
    migration_files: Vec<LegacyMigrationFile>,
}

impl LegacyCatalogMigrationTable {
    /// Returns the stable normalized table descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &TableDescriptor {
        &self.descriptor
    }

    /// Returns the original JSON Arrow schema metadata stored by the registry.
    #[must_use]
    pub const fn legacy_schema(&self) -> &serde_json::Value {
        &self.legacy_schema
    }

    /// Returns the legacy control-plane schema hash.
    #[must_use]
    pub fn legacy_schema_hash(&self) -> &str {
        &self.legacy_schema_hash
    }

    /// Returns the schema-only Arrow IPC stream required by `CreateTable`.
    #[must_use]
    pub const fn schema_ipc(&self) -> &zippy_types::table::SchemaIpcPayload {
        &self.schema_ipc
    }

    /// Returns whether schema or field metadata makes regenerated Arrow IPC byte ordering unstable.
    #[must_use]
    pub const fn schema_has_custom_metadata(&self) -> bool {
        self.schema_has_custom_metadata
    }

    /// Returns the legacy revision of the complete persisted-file view.
    #[must_use]
    pub const fn persisted_files_revision(&self) -> u64 {
        self.persisted_files_revision
    }

    /// Returns raw persisted-file metadata in legacy discovery order.
    #[must_use]
    pub fn persisted_files(&self) -> &[serde_json::Value] {
        &self.persisted_files
    }

    /// Returns typed persisted-file projections in original registry discovery order.
    ///
    /// Each projection retains its zero-based registry position and computed Gateway scan order.
    /// The raw values returned by [`Self::persisted_files`] remain the frozen V1 fingerprint input.
    #[must_use]
    pub fn migration_files(&self) -> &[LegacyMigrationFile] {
        &self.migration_files
    }
}

/// Gateway-compatible stable ordering key for one legacy persisted file.
///
/// Missing legacy numeric fields are represented as zero, exactly matching the current Gateway
/// persisted-file ordering contract. A field that is present with the wrong JSON type is rejected
/// while the migration snapshot is built rather than silently treated as missing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LegacyMigrationScanOrderKey {
    writer_epoch: u64,
    source_segment_id: u64,
    source_generation: u64,
    created_at: u64,
    file_path: String,
}

impl LegacyMigrationScanOrderKey {
    /// Returns the effective writer epoch, defaulting missing legacy metadata to zero.
    #[must_use]
    pub const fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    /// Returns the effective source segment identity, defaulting a missing field to zero.
    #[must_use]
    pub const fn source_segment_id(&self) -> u64 {
        self.source_segment_id
    }

    /// Returns the effective source generation, defaulting a missing field to zero.
    #[must_use]
    pub const fn source_generation(&self) -> u64 {
        self.source_generation
    }

    /// Returns the effective legacy creation timestamp, defaulting a missing field to zero.
    #[must_use]
    pub const fn created_at(&self) -> u64 {
        self.created_at
    }

    /// Returns the validated canonical absolute UTF-8 path used as the final ordering component.
    #[must_use]
    pub fn file_path(&self) -> &str {
        &self.file_path
    }
}

/// Strongly typed audit projection of one raw legacy persisted-file registry entry.
///
/// This projection validates metadata only. It does not canonicalize through the host filesystem,
/// open a file, follow a mount, verify Parquet contents, or prove that a checksum and row count are
/// physically true. Those operations remain the storage verifier's responsibility. The exact raw
/// JSON value is retained unchanged so opaque legacy extensions and the frozen V1 migration hash
/// remain available without reconstructing JSON from typed fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyMigrationFile {
    canonical_value: serde_json::Value,
    registry_index: u64,
    file_path: String,
    persist_data_root: Option<String>,
    row_count: Option<NonZeroU64>,
    checksum: Option<String>,
    stream_name: String,
    stream_name_was_declared: bool,
    schema_hash: String,
    schema_hash_was_declared: bool,
    writer_epoch: Option<u64>,
    source_segment_id: Option<u64>,
    source_generation: Option<u64>,
    created_at: Option<u64>,
    scan_order_key: LegacyMigrationScanOrderKey,
    effective_scan_order: u64,
}

impl LegacyMigrationFile {
    /// Returns the exact registry JSON value used by the frozen V1 snapshot fingerprint.
    #[must_use]
    pub const fn canonical_value(&self) -> &serde_json::Value {
        &self.canonical_value
    }

    /// Returns untyped extension fields retained from the registry entry.
    ///
    /// Known typed fields are excluded from this iterator. Nested values are borrowed directly
    /// from [`Self::canonical_value`] and are never normalized or reserialized.
    pub fn opaque_extras(&self) -> impl Iterator<Item = (&str, &serde_json::Value)> {
        self.canonical_value
            .as_object()
            .expect("validated legacy migration file metadata is an object")
            .iter()
            .filter(|(name, _)| !is_typed_legacy_migration_file_field(name))
            .map(|(name, value)| (name.as_str(), value))
    }

    /// Returns the zero-based position in the captured registry vector.
    #[must_use]
    pub const fn registry_index(&self) -> u64 {
        self.registry_index
    }

    /// Returns the validated canonical absolute UTF-8 file path.
    #[must_use]
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Returns the optional canonical absolute root declared by legacy metadata.
    ///
    /// The file path is guaranteed to be a strict lexical descendant when this value is present.
    #[must_use]
    pub fn persist_data_root(&self) -> Option<&str> {
        self.persist_data_root.as_deref()
    }

    /// Returns the optional nonzero row-count claim from the registry.
    #[must_use]
    pub const fn row_count(&self) -> Option<NonZeroU64> {
        self.row_count
    }

    /// Returns the optional canonical lowercase `sha256-v1` checksum claim.
    #[must_use]
    pub fn checksum(&self) -> Option<&str> {
        self.checksum.as_deref()
    }

    /// Returns the effective stream name after validating an optional raw declaration.
    #[must_use]
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Returns whether the raw file metadata explicitly declared its stream name.
    #[must_use]
    pub const fn stream_name_was_declared(&self) -> bool {
        self.stream_name_was_declared
    }

    /// Returns the effective legacy schema hash after validating an optional raw declaration.
    #[must_use]
    pub fn schema_hash(&self) -> &str {
        &self.schema_hash
    }

    /// Returns whether the raw file metadata explicitly declared its schema hash.
    #[must_use]
    pub const fn schema_hash_was_declared(&self) -> bool {
        self.schema_hash_was_declared
    }

    /// Returns the optional writer epoch exactly as declared by legacy metadata.
    #[must_use]
    pub const fn writer_epoch(&self) -> Option<u64> {
        self.writer_epoch
    }

    /// Returns the optional source segment identity exactly as declared by legacy metadata.
    #[must_use]
    pub const fn source_segment_id(&self) -> Option<u64> {
        self.source_segment_id
    }

    /// Returns the optional source generation exactly as declared by legacy metadata.
    #[must_use]
    pub const fn source_generation(&self) -> Option<u64> {
        self.source_generation
    }

    /// Returns the optional legacy creation timestamp exactly as declared by metadata.
    #[must_use]
    pub const fn created_at(&self) -> Option<u64> {
        self.created_at
    }

    /// Returns the exact key used by the current Gateway stable persisted-file sort.
    #[must_use]
    pub const fn scan_order_key(&self) -> &LegacyMigrationScanOrderKey {
        &self.scan_order_key
    }

    /// Returns the zero-based position after stable Gateway-compatible ordering.
    ///
    /// Equal keys retain registry discovery order through [`Self::registry_index`].
    #[must_use]
    pub const fn effective_scan_order(&self) -> u64 {
        self.effective_scan_order
    }
}

/// Read-only bridge from the legacy master registry to the persistent catalog contract.
#[derive(Clone)]
pub struct LegacyCatalogAdapter {
    registry: Arc<Mutex<Registry>>,
    running: Arc<AtomicBool>,
}

#[derive(Debug)]
struct LegacyCatalogView {
    catalog_revision: u64,
    streams: Vec<LegacyStreamView>,
}

#[derive(Debug)]
struct LegacyStreamView {
    stream_name: String,
    persisted_files_revision: u64,
    writer_epoch: u64,
    source_configs: Vec<serde_json::Value>,
}

#[derive(Debug)]
struct LegacyMigrationView {
    catalog_revision: u64,
    streams: Vec<LegacyMigrationStreamView>,
}

#[derive(Debug)]
struct LegacyMigrationStreamView {
    catalog: LegacyStreamView,
    schema: serde_json::Value,
    schema_hash: String,
    persisted_files: Vec<serde_json::Value>,
}

#[derive(Debug)]
struct LegacyProjectionViewV4 {
    catalog_revision: u64,
    streams: Vec<LegacyProjectionStreamViewV4>,
    sources: Vec<LegacyProjectionSourceViewV4>,
}

#[derive(Debug)]
struct LegacyProjectionStreamViewV4 {
    stream_name: String,
    schema: serde_json::Value,
    schema_hash: String,
    persisted_files_revision: u64,
    persisted_files: Vec<serde_json::Value>,
    writer_epoch: u64,
}

#[derive(Debug, Clone)]
struct LegacyProjectionSourceViewV4 {
    source_name: String,
    source_type: String,
    output_stream: String,
    config: serde_json::Value,
    writer_epoch: u64,
}

impl LegacyCatalogAdapter {
    pub(crate) fn new(registry: Arc<Mutex<Registry>>, running: Arc<AtomicBool>) -> Self {
        Self { registry, running }
    }

    fn lock_registry(&self) -> Result<std::sync::MutexGuard<'_, Registry>> {
        let registry = self.registry.lock().map_err(|_| {
            ServerError::new(
                ErrorCode::Internal,
                "legacy catalog registry lock is poisoned",
            )
        })?;
        if !self.running.load(Ordering::Acquire) {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "legacy catalog master lifecycle is stopped",
            ));
        }
        Ok(registry)
    }

    fn capture_view(&self) -> Result<LegacyCatalogView> {
        let registry = self.lock_registry()?;
        let mut source_configs = registry.source_configs_by_stream();
        let streams = registry
            .stream_records()
            .map(|stream| LegacyStreamView {
                stream_name: stream.stream_name.clone(),
                persisted_files_revision: stream.persisted_files_revision,
                writer_epoch: stream.writer_epoch,
                source_configs: source_configs
                    .remove(&stream.stream_name)
                    .unwrap_or_default(),
            })
            .collect();
        Ok(LegacyCatalogView {
            catalog_revision: registry.catalog_revision(),
            streams,
        })
    }

    fn capture_migration_view(&self) -> Result<LegacyMigrationView> {
        let registry = self.lock_registry()?;
        let mut source_configs = registry.source_configs_by_stream();
        let streams = registry
            .stream_records()
            .map(|stream| LegacyMigrationStreamView {
                catalog: LegacyStreamView {
                    stream_name: stream.stream_name.clone(),
                    persisted_files_revision: stream.persisted_files_revision,
                    writer_epoch: stream.writer_epoch,
                    source_configs: source_configs
                        .remove(&stream.stream_name)
                        .unwrap_or_default(),
                },
                schema: stream.schema.clone(),
                schema_hash: stream.schema_hash.clone(),
                persisted_files: stream.persisted_files.clone(),
            })
            .collect();
        Ok(LegacyMigrationView {
            catalog_revision: registry.catalog_revision(),
            streams,
        })
    }

    /// Captures all table-relevant migration data under one legacy registry lock.
    ///
    /// The returned value is detached from the live registry. Hashing and Arrow IPC encoding occur
    /// after releasing the lock, so migration work cannot extend the legacy control-plane critical
    /// section. Runtime liveness records are intentionally absent.
    pub fn capture_migration_snapshot(&self) -> Result<LegacyCatalogMigrationSnapshot> {
        let view = self.capture_migration_view()?;
        build_migration_snapshot(view)
    }

    fn capture_projection_view_v4(&self) -> Result<LegacyProjectionViewV4> {
        self.capture_projection_view_v4_with_metadata_budget(MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4)
    }

    fn capture_projection_view_v4_with_metadata_budget(
        &self,
        maximum_metadata_bytes: usize,
    ) -> Result<LegacyProjectionViewV4> {
        let registry = self.lock_registry()?;
        let mut metadata_budget = ProjectionMetadataBudgetWriter::new(maximum_metadata_bytes);
        let mut candidate_internal_markers = BTreeMap::<&str, Option<bool>>::new();
        for source in registry.source_records() {
            if source.source_name.is_empty()
                || source.source_type.is_empty()
                || source.output_stream.is_empty()
            {
                return Err(projection_corruption(
                    "sources",
                    "legacy projection contains an empty source identity",
                ));
            }
            if registry.get_stream(&source.output_stream).is_none() {
                return Err(projection_corruption(
                    &format!("source/{}", source.source_name),
                    &format!(
                        "legacy source is bound to an unknown output stream output_stream=[{}]",
                        source.output_stream
                    ),
                ));
            }
            if source_config_declares_table(&source.config) {
                candidate_internal_markers
                    .entry(source.output_stream.as_str())
                    .or_default();
            }
        }
        for source in registry.source_records() {
            if let Some(marker) = candidate_internal_markers.get_mut(source.output_stream.as_str())
            {
                merge_internal_table_marker(&source.output_stream, marker, &source.config)?;
            }
        }
        let mut public_streams = BTreeSet::new();
        for (stream_name, internal_marker) in candidate_internal_markers {
            let internal = internal_marker.unwrap_or_else(|| is_physical_shard_name(stream_name));
            if !internal {
                write_projection_metadata_budget_value_v4(
                    &mut metadata_budget,
                    stream_name,
                    stream_name,
                    "legacy table identity exceeds the global projection byte budget",
                )?;
                public_streams.insert((*stream_name).to_owned());
            }
        }
        let mut source_counts = BTreeMap::<&str, usize>::new();
        for source in registry
            .source_records()
            .filter(|source| public_streams.contains(&source.output_stream))
        {
            let count = source_counts
                .entry(source.output_stream.as_str())
                .or_default();
            *count = count.checked_add(1).ok_or_else(|| {
                projection_corruption(
                    &source.output_stream,
                    "legacy source projection count overflowed",
                )
            })?;
            validate_projection_source_count_v4(&source.output_stream, *count)?;
            write_projection_source_metadata_budget_v4(
                &mut metadata_budget,
                &source.output_stream,
                source.source_name.as_str(),
                source.source_type.as_str(),
                source.output_stream.as_str(),
                &source.config,
                source.writer_epoch,
            )?;
        }
        for stream in registry
            .stream_records()
            .filter(|stream| public_streams.contains(&stream.stream_name))
        {
            validate_projection_file_count_v4(&stream.stream_name, stream.persisted_files.len())?;
            write_projection_metadata_budget_parts_v4(
                &mut metadata_budget,
                &stream.stream_name,
                &stream.schema,
                std::iter::empty(),
                &stream.persisted_files,
            )?;
            write_projection_metadata_budget_value_v4(
                &mut metadata_budget,
                &stream.stream_name,
                &stream.schema_hash,
                "legacy Schema hash exceeds the global projection byte budget",
            )?;
        }
        // Do not clone any public schema, source config, or file metadata until the complete
        // projection has passed the shared byte-budget preflight above.
        let streams = registry
            .stream_records()
            .filter(|stream| public_streams.contains(&stream.stream_name))
            .map(|stream| LegacyProjectionStreamViewV4 {
                stream_name: stream.stream_name.clone(),
                schema: stream.schema.clone(),
                schema_hash: stream.schema_hash.clone(),
                persisted_files_revision: stream.persisted_files_revision,
                persisted_files: stream.persisted_files.clone(),
                writer_epoch: stream.writer_epoch,
            })
            .collect();
        let sources = registry
            .source_records()
            .filter(|source| public_streams.contains(&source.output_stream))
            .map(|source| LegacyProjectionSourceViewV4 {
                source_name: source.source_name.clone(),
                source_type: source.source_type.clone(),
                output_stream: source.output_stream.clone(),
                config: source.config.clone(),
                writer_epoch: source.writer_epoch,
            })
            .collect();
        Ok(LegacyProjectionViewV4 {
            catalog_revision: registry.catalog_revision(),
            streams,
            sources,
        })
    }

    /// Captures the complete stable V4 projection under one legacy registry lock.
    ///
    /// Borrowed identity, public-table, count, and byte-budget preflight occurs before cloning.
    /// Arrow IPC encoding, typed JSON conversion, and digest computation occur after releasing the
    /// lock. Any malformed identity, duplicate identity, or bounded canonicalization failure aborts
    /// the whole capture.
    pub fn capture_projection_snapshot_v4(&self) -> Result<LegacyCatalogProjectionSnapshotV4> {
        let view = self.capture_projection_view_v4()?;
        build_projection_snapshot_v4(view)
    }

    fn read_revision(&self) -> Result<u64> {
        Ok(self.lock_registry()?.catalog_revision())
    }
}

impl CatalogRead for LegacyCatalogAdapter {
    fn revision(&self) -> BoxFuture<'_, Result<u64>> {
        Box::pin(async move { self.read_revision() })
    }

    fn get_namespace(&self, namespace: NamespaceRef) -> BoxFuture<'_, Result<NamespaceDescriptor>> {
        Box::pin(async move {
            self.read_revision()?;
            let descriptor = default_namespace_descriptor();
            let found = match namespace {
                NamespaceRef::Id(namespace_id) => descriptor.namespace_id == namespace_id,
                NamespaceRef::Name(name) => descriptor.name == name,
            };
            if found {
                Ok(descriptor)
            } else {
                Err(ServerError::new(
                    ErrorCode::NotFound,
                    "legacy catalog namespace was not found",
                ))
            }
        })
    }

    fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> BoxFuture<'_, Result<ListNamespacesResponse>> {
        Box::pin(async move {
            let catalog_revision = self.read_revision()?;
            let descriptor = default_namespace_descriptor();
            let namespaces = if request
                .after
                .is_none_or(|after| descriptor.namespace_id > after)
            {
                vec![descriptor]
            } else {
                Vec::new()
            };
            Ok(ListNamespacesResponse {
                catalog_revision,
                namespaces,
                next_after: None,
            })
        })
    }

    fn get_table(&self, table: TableRef) -> BoxFuture<'_, Result<TableDescriptor>> {
        Box::pin(async move {
            let view = self.capture_view()?;
            let tables = map_tables(&view)?;
            resolve_table(&tables, &table).cloned().ok_or_else(|| {
                ServerError::new(ErrorCode::NotFound, "legacy catalog table was not found")
            })
        })
    }

    fn list_tables(&self, request: ListTablesRequest) -> BoxFuture<'_, Result<ListTablesResponse>> {
        Box::pin(async move {
            let view = self.capture_view()?;
            let namespace_id = legacy_namespace_id();
            let mut tables = if request
                .namespace_id
                .is_none_or(|requested| requested == namespace_id)
            {
                map_tables(&view)?
            } else {
                Vec::new()
            };
            if let Some(after) = request.after {
                tables.retain(|table| table.table_id > after);
            }
            let limit = usize::try_from(request.limit.get()).unwrap_or(usize::MAX);
            let has_more = tables.len() > limit;
            tables.truncate(limit);
            let next_after = has_more.then(|| tables.last().expect("nonzero page limit").table_id);
            Ok(ListTablesResponse {
                catalog_revision: view.catalog_revision,
                tables,
                next_after,
            })
        })
    }

    fn resolve_tables<'a>(
        &'a self,
        table_refs: &'a [TableRef],
    ) -> BoxFuture<'a, Result<Vec<TableDescriptor>>> {
        Box::pin(async move {
            let view = self.capture_view()?;
            let tables = map_tables(&view)?;
            table_refs
                .iter()
                .map(|table_ref| {
                    resolve_table(&tables, table_ref).cloned().ok_or_else(|| {
                        ServerError::new(ErrorCode::NotFound, "legacy catalog table was not found")
                    })
                })
                .collect()
        })
    }

    fn read_snapshot_at<'a>(
        &'a self,
        selectors: &'a [CatalogSnapshotSelector],
    ) -> BoxFuture<'a, Result<CatalogQuerySnapshot>> {
        Box::pin(async move {
            let revision = self.read_revision()?;
            if selectors.is_empty() {
                return Ok(CatalogQuerySnapshot::empty(revision));
            }
            Err(unsupported_plan("legacy_catalog.read_snapshot_at"))
        })
    }
}

impl CatalogWrite for LegacyCatalogAdapter {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        let VersionedCatalogCommand::V1(envelope) = command;
        let operator = match &envelope.command {
            CatalogCommandV1::CreateNamespace(_) => "legacy_catalog.command.create_namespace",
            CatalogCommandV1::CreateTable(_) => "legacy_catalog.command.create_table",
            CatalogCommandV1::AlterTableSchema(_) => "legacy_catalog.command.alter_table_schema",
            CatalogCommandV1::AcquireProducer(_) => "legacy_catalog.command.acquire_producer",
            CatalogCommandV1::CommitDataFiles(_) => "legacy_catalog.command.commit_data_files",
            CatalogCommandV1::CommitCompaction(_) => "legacy_catalog.command.commit_compaction",
            CatalogCommandV1::CommitConsumerOffset(_) => {
                "legacy_catalog.command.commit_consumer_offset"
            }
            CatalogCommandV1::AcquireSnapshotLease(_) => {
                "legacy_catalog.command.acquire_snapshot_lease"
            }
            CatalogCommandV1::ReleaseSnapshotLease(_) => {
                "legacy_catalog.command.release_snapshot_lease"
            }
            CatalogCommandV1::CreateMaintenanceJob(_) => {
                "legacy_catalog.command.create_maintenance_job"
            }
            CatalogCommandV1::TransitionMaintenanceJob(_) => {
                "legacy_catalog.command.transition_maintenance_job"
            }
            CatalogCommandV1::DropTable(_) => "legacy_catalog.command.drop_table",
        };
        let error = unsupported_plan(operator).with_request_id(envelope.request_id);
        Box::pin(async move { Err(error) })
    }
}

fn project_legacy_migration_files(
    stream_name: &str,
    schema_hash: &str,
    persisted_files: &[serde_json::Value],
) -> Result<Vec<LegacyMigrationFile>> {
    let mut files = persisted_files
        .iter()
        .enumerate()
        .map(|(registry_index, value)| {
            project_legacy_migration_file(stream_name, schema_hash, registry_index, value)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut effective_order = (0..files.len()).collect::<Vec<_>>();
    effective_order.sort_by(|left, right| {
        files[*left]
            .scan_order_key
            .cmp(&files[*right].scan_order_key)
            .then_with(|| {
                files[*left]
                    .registry_index
                    .cmp(&files[*right].registry_index)
            })
    });
    for (effective_scan_order, registry_index) in effective_order.into_iter().enumerate() {
        files[registry_index].effective_scan_order =
            u64::try_from(effective_scan_order).map_err(|_| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "legacy persisted-file scan order exceeds u64",
                )
                .with_retryable(false)
            })?;
    }
    Ok(files)
}

fn project_legacy_migration_file(
    stream_name: &str,
    schema_hash: &str,
    registry_index: usize,
    value: &serde_json::Value,
) -> Result<LegacyMigrationFile> {
    let object = value.as_object().ok_or_else(|| {
        migration_file_corruption(
            stream_name,
            registry_index,
            "entry",
            "metadata must be a JSON object",
        )
    })?;
    let file_path = required_string_field(stream_name, registry_index, object, "file_path")?;
    let file_components =
        canonical_absolute_path_components(file_path, false).map_err(|reason| {
            migration_file_corruption(stream_name, registry_index, "file_path", reason)
        })?;
    let persist_data_root =
        optional_string_field(stream_name, registry_index, object, "persist_data_root")?;
    if let Some(root) = persist_data_root {
        let root_components = canonical_absolute_path_components(root, true).map_err(|reason| {
            migration_file_corruption(stream_name, registry_index, "persist_data_root", reason)
        })?;
        if file_components.len() <= root_components.len()
            || !file_components.starts_with(&root_components)
        {
            return Err(migration_file_corruption(
                stream_name,
                registry_index,
                "persist_data_root",
                "file_path must be a strict lexical descendant of persist_data_root",
            ));
        }
    }

    let row_count = match optional_u64_field(stream_name, registry_index, object, "row_count")? {
        Some(value) => Some(NonZeroU64::new(value).ok_or_else(|| {
            migration_file_corruption(
                stream_name,
                registry_index,
                "row_count",
                "value must be nonzero",
            )
        })?),
        None => None,
    };
    let checksum = optional_string_field(stream_name, registry_index, object, "checksum")?;
    if let Some(checksum) = checksum {
        validate_legacy_sha256_checksum(checksum).map_err(|reason| {
            migration_file_corruption(stream_name, registry_index, "checksum", reason)
        })?;
    }

    let declared_stream_name =
        optional_string_field(stream_name, registry_index, object, "stream_name")?;
    if declared_stream_name.is_some_and(|declared| declared != stream_name) {
        return Err(migration_file_corruption(
            stream_name,
            registry_index,
            "stream_name",
            "value does not match the owning legacy table",
        ));
    }
    let declared_schema_hash =
        optional_string_field(stream_name, registry_index, object, "schema_hash")?;
    if declared_schema_hash.is_some_and(|declared| declared != schema_hash) {
        return Err(migration_file_corruption(
            stream_name,
            registry_index,
            "schema_hash",
            "value does not match the owning legacy table schema",
        ));
    }

    let writer_epoch = optional_u64_field(stream_name, registry_index, object, "writer_epoch")?;
    let source_segment_id =
        optional_u64_field(stream_name, registry_index, object, "source_segment_id")?;
    let source_generation =
        optional_u64_field(stream_name, registry_index, object, "source_generation")?;
    let created_at = optional_u64_field(stream_name, registry_index, object, "created_at")?;
    let registry_index_u64 = u64::try_from(registry_index).map_err(|_| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "legacy persisted-file registry index exceeds u64",
        )
        .with_retryable(false)
    })?;

    Ok(LegacyMigrationFile {
        canonical_value: value.clone(),
        registry_index: registry_index_u64,
        file_path: file_path.to_owned(),
        persist_data_root: persist_data_root.map(str::to_owned),
        row_count,
        checksum: checksum.map(str::to_owned),
        stream_name: stream_name.to_owned(),
        stream_name_was_declared: declared_stream_name.is_some(),
        schema_hash: schema_hash.to_owned(),
        schema_hash_was_declared: declared_schema_hash.is_some(),
        writer_epoch,
        source_segment_id,
        source_generation,
        created_at,
        scan_order_key: LegacyMigrationScanOrderKey {
            writer_epoch: writer_epoch.unwrap_or_default(),
            source_segment_id: source_segment_id.unwrap_or_default(),
            source_generation: source_generation.unwrap_or_default(),
            created_at: created_at.unwrap_or_default(),
            file_path: file_path.to_owned(),
        },
        effective_scan_order: 0,
    })
}

fn required_string_field<'a>(
    stream_name: &str,
    registry_index: usize,
    object: &'a serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<&'a str> {
    optional_string_field(stream_name, registry_index, object, field)?.ok_or_else(|| {
        migration_file_corruption(
            stream_name,
            registry_index,
            field,
            "required field is missing",
        )
    })
}

fn optional_string_field<'a>(
    stream_name: &str,
    registry_index: usize,
    object: &'a serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<Option<&'a str>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .map(Some)
        .ok_or_else(|| {
            migration_file_corruption(
                stream_name,
                registry_index,
                field,
                "present value must be a nonempty string",
            )
        })
}

fn optional_u64_field(
    stream_name: &str,
    registry_index: usize,
    object: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<Option<u64>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    value.as_u64().map(Some).ok_or_else(|| {
        migration_file_corruption(
            stream_name,
            registry_index,
            field,
            "present value must be an unsigned integer",
        )
    })
}

fn canonical_absolute_path_components(
    value: &str,
    allow_root: bool,
) -> std::result::Result<Vec<&str>, &'static str> {
    if !value.starts_with('/') {
        return Err("value must be an absolute POSIX path");
    }
    if value.chars().any(char::is_control) {
        return Err("value contains a control character");
    }
    if value == "/" {
        return allow_root
            .then(Vec::new)
            .ok_or("file path must name a component below the filesystem root");
    }
    if value.ends_with('/') {
        return Err("canonical path must not have a trailing separator");
    }
    let mut components = Vec::new();
    for component in value[1..].split('/') {
        if component.is_empty() {
            return Err("canonical path must not contain repeated separators");
        }
        if matches!(component, "." | "..") {
            return Err("canonical path must not contain traversal components");
        }
        components.push(component);
    }
    Ok(components)
}

fn validate_legacy_sha256_checksum(checksum: &str) -> std::result::Result<(), &'static str> {
    let digest = checksum
        .strip_prefix("sha256-v1:")
        .ok_or("checksum must use the sha256-v1 scheme")?;
    if digest.len() != 64
        || digest
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err("checksum must contain 64 canonical lowercase hexadecimal digits");
    }
    Ok(())
}

fn is_typed_legacy_migration_file_field(field: &str) -> bool {
    matches!(
        field,
        "file_path"
            | "persist_data_root"
            | "row_count"
            | "checksum"
            | "stream_name"
            | "schema_hash"
            | "writer_epoch"
            | "source_segment_id"
            | "source_generation"
            | "created_at"
    )
}

fn migration_file_corruption(
    stream_name: &str,
    registry_index: usize,
    field: &str,
    reason: &str,
) -> ServerError {
    data_corruption(
        stream_name,
        &format!(
            "legacy persisted-file metadata is invalid registry_index=[{registry_index}] \
             field=[{field}] reason=[{reason}]"
        ),
    )
}

fn build_migration_snapshot(view: LegacyMigrationView) -> Result<LegacyCatalogMigrationSnapshot> {
    let namespace = default_namespace_descriptor();
    let mut tables = Vec::new();
    for stream in view.streams {
        if !has_explicit_table_kind(&stream.catalog) || is_internal_table(&stream.catalog)? {
            continue;
        }
        let descriptor = map_table(&stream.catalog)?;
        let schema =
            arrow_schema_from_stream_metadata(&stream.catalog.stream_name, &stream.schema)?;
        let schema_has_custom_metadata = !schema.metadata().is_empty()
            || schema
                .fields()
                .iter()
                .any(|field| !field.metadata().is_empty());
        let schema_ipc = encode_schema_ipc(&stream.catalog.stream_name, &schema)?;
        let migration_files = project_legacy_migration_files(
            &stream.catalog.stream_name,
            &stream.schema_hash,
            &stream.persisted_files,
        )?;
        tables.push(LegacyCatalogMigrationTable {
            persisted_files_revision: stream.catalog.persisted_files_revision,
            descriptor,
            legacy_schema: stream.schema,
            legacy_schema_hash: stream.schema_hash,
            schema_ipc,
            schema_has_custom_metadata,
            persisted_files: stream.persisted_files,
            migration_files,
        });
    }
    tables.sort_by_key(|table| table.descriptor.table_id);
    if let Some(duplicate) = tables
        .windows(2)
        .find(|pair| pair[0].descriptor.table_id == pair[1].descriptor.table_id)
    {
        return Err(data_corruption(
            duplicate[0].descriptor.name.as_str(),
            "deterministic legacy table id collision",
        ));
    }
    let source_snapshot_hash = migration_snapshot_hash(view.catalog_revision, &namespace, &tables)?;
    Ok(LegacyCatalogMigrationSnapshot {
        source_catalog_revision: view.catalog_revision,
        source_snapshot_hash,
        namespace,
        tables,
    })
}

#[derive(Serialize)]
struct MigrationSnapshotFingerprint<'a> {
    format: &'static str,
    source_catalog_revision: u64,
    namespace: &'a NamespaceDescriptor,
    tables: Vec<MigrationTableFingerprint<'a>>,
}

#[derive(Serialize)]
struct MigrationTableFingerprint<'a> {
    descriptor: &'a TableDescriptor,
    legacy_schema: &'a serde_json::Value,
    legacy_schema_hash: &'a str,
    persisted_files_revision: u64,
    persisted_files: &'a [serde_json::Value],
}

fn migration_snapshot_hash(
    source_catalog_revision: u64,
    namespace: &NamespaceDescriptor,
    tables: &[LegacyCatalogMigrationTable],
) -> Result<String> {
    let fingerprint = MigrationSnapshotFingerprint {
        format: "zippy.legacy_catalog.migration_snapshot.v1",
        source_catalog_revision,
        namespace,
        tables: tables
            .iter()
            .map(|table| MigrationTableFingerprint {
                descriptor: &table.descriptor,
                legacy_schema: &table.legacy_schema,
                legacy_schema_hash: &table.legacy_schema_hash,
                persisted_files_revision: table.persisted_files_revision,
                persisted_files: &table.persisted_files,
            })
            .collect(),
    };
    let bytes = serde_json::to_vec(&fingerprint).map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("failed to encode legacy migration snapshot fingerprint error=[{error}]"),
        )
    })?;
    let digest = Sha256::digest(bytes);
    Ok(format!(
        "{LEGACY_MIGRATION_SNAPSHOT_HASH_SCHEME}:{digest:x}"
    ))
}

fn build_projection_snapshot_v4(
    mut view: LegacyProjectionViewV4,
) -> Result<LegacyCatalogProjectionSnapshotV4> {
    view.streams
        .sort_by(|left, right| left.stream_name.cmp(&right.stream_name));
    if view
        .streams
        .iter()
        .any(|stream| stream.stream_name.is_empty())
        || view
            .streams
            .windows(2)
            .any(|pair| pair[0].stream_name == pair[1].stream_name)
    {
        return Err(projection_corruption(
            "streams",
            "legacy projection contains an empty or duplicate stream identity",
        ));
    }
    let stream_names = view
        .streams
        .iter()
        .map(|stream| stream.stream_name.clone())
        .collect::<BTreeSet<_>>();

    view.sources
        .sort_by(|left, right| left.source_name.cmp(&right.source_name));
    if view.sources.iter().any(|source| {
        source.source_name.is_empty()
            || source.source_type.is_empty()
            || source.output_stream.is_empty()
    }) || view
        .sources
        .windows(2)
        .any(|pair| pair[0].source_name == pair[1].source_name)
    {
        return Err(projection_corruption(
            "sources",
            "legacy projection contains an empty or duplicate source identity",
        ));
    }

    let mut source_views_by_stream = BTreeMap::<String, Vec<LegacyProjectionSourceViewV4>>::new();
    for source in view.sources {
        if !stream_names.contains(&source.output_stream) {
            return Err(projection_corruption(
                &format!("source/{}", source.source_name),
                &format!(
                    "legacy source is bound to an unknown output stream output_stream=[{}]",
                    source.output_stream
                ),
            ));
        }
        source_views_by_stream
            .entry(source.output_stream.clone())
            .or_default()
            .push(source);
    }

    let namespace = default_namespace_descriptor();
    let mut tables = Vec::new();
    for stream in view.streams {
        let stream_name = stream.stream_name.clone();
        let source_views = source_views_by_stream
            .remove(&stream_name)
            .unwrap_or_default();
        let catalog = LegacyStreamView {
            stream_name: stream.stream_name,
            persisted_files_revision: stream.persisted_files_revision,
            writer_epoch: stream.writer_epoch,
            source_configs: source_views
                .iter()
                .map(|source| source.config.clone())
                .collect(),
        };
        if !has_explicit_table_kind(&catalog) || is_internal_table(&catalog)? {
            continue;
        }

        let mut descriptor = map_table(&catalog)?;
        descriptor.config = source_owned_table_config(&descriptor.config);
        let persisted_files_revision = catalog.persisted_files_revision;
        drop(catalog);
        validate_projection_metadata_budget_v4(
            &stream_name,
            &stream.schema,
            &source_views,
            &stream.persisted_files,
            MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4,
        )?;
        validate_projection_file_count_v4(&stream_name, stream.persisted_files.len())?;
        validate_projection_source_count_v4(&stream_name, source_views.len())?;
        let mut value_nodes = 0_usize;
        let sources = source_views
            .iter()
            .map(|source| {
                Ok(LegacyCatalogSourceV4 {
                    source_name: source.source_name.clone(),
                    source_type: source.source_type.clone(),
                    output_stream: source.output_stream.clone(),
                    config: project_legacy_catalog_value_v4(
                        &source.source_name,
                        &source.config,
                        1,
                        &mut value_nodes,
                    )?,
                    writer_epoch: source.writer_epoch,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(arrow_schema_from_stream_metadata(
            &stream_name,
            &stream.schema,
        )?);
        let schema_has_custom_metadata = !schema.metadata().is_empty()
            || schema
                .fields()
                .iter()
                .any(|field| !field.metadata().is_empty());
        validate_legacy_control_schema_hash(
            &stream_name,
            &stream.schema_hash,
            &schema,
            schema_has_custom_metadata,
        )?;
        validate_catalog_table_schema(schema.as_ref(), &descriptor.definition).map_err(
            |error| {
                data_corruption(
                    &stream_name,
                    &format!("legacy table Schema violates Catalog contract error=[{error}]"),
                )
            },
        )?;
        let canonical_schema_hash = canonical_catalog_schema_hash_v1(schema.as_ref());
        let schema_ipc = encode_schema_ipc(&stream_name, schema.as_ref())?;
        validate_projection_schema_ipc_size_v4(&stream_name, schema_ipc.as_bytes().len())?;
        let migration_files = project_legacy_migration_files(
            &stream_name,
            &stream.schema_hash,
            &stream.persisted_files,
        )?;
        validate_unique_projection_file_paths_v4(&stream_name, &migration_files)?;
        tables.push(LegacyCatalogActivePublicTableV4 {
            descriptor,
            legacy_schema: stream.schema,
            legacy_schema_hash: stream.schema_hash,
            canonical_schema_hash,
            schema_ipc,
            schema_has_custom_metadata,
            persisted_files_revision,
            migration_files,
            sources,
        });
    }
    tables.sort_by_key(|table| table.descriptor.table_id);
    if let Some(duplicate) = tables
        .windows(2)
        .find(|pair| pair[0].descriptor.table_id == pair[1].descriptor.table_id)
    {
        return Err(data_corruption(
            duplicate[0].descriptor.name.as_str(),
            "deterministic legacy table id collision",
        ));
    }

    let digest_inputs = tables
        .iter()
        .map(LegacyCatalogActivePublicTableV4::projection_digest_input)
        .collect::<Vec<_>>();
    let global_projection_digest = compute_legacy_catalog_projection_digest_v4(&digest_inputs)
        .map_err(|error| {
            projection_corruption(
                "digest",
                &format!("legacy V4 projection digest failed error=[{error}]"),
            )
        })?;
    Ok(LegacyCatalogProjectionSnapshotV4 {
        catalog_revision: view.catalog_revision,
        global_projection_digest,
        namespace,
        tables,
    })
}

fn project_legacy_catalog_value_v4(
    source_name: &str,
    value: &serde_json::Value,
    depth: usize,
    nodes: &mut usize,
) -> Result<LegacyCatalogValueV4> {
    *nodes = nodes.checked_add(1).ok_or_else(|| {
        projection_corruption(
            &format!("source/{source_name}"),
            "legacy source config node count overflowed",
        )
    })?;
    if *nodes > MAX_LEGACY_CATALOG_VALUE_NODES_V4 {
        return Err(projection_corruption(
            &format!("source/{source_name}"),
            &format!(
                "legacy source config exceeds node limit actual=[{}] maximum=[{}]",
                *nodes, MAX_LEGACY_CATALOG_VALUE_NODES_V4
            ),
        ));
    }
    if depth > MAX_LEGACY_CATALOG_VALUE_DEPTH_V4 {
        return Err(projection_corruption(
            &format!("source/{source_name}"),
            &format!(
                "legacy source config exceeds depth limit actual=[{depth}] maximum=[{}]",
                MAX_LEGACY_CATALOG_VALUE_DEPTH_V4
            ),
        ));
    }
    let next_depth = depth.saturating_add(1);
    match value {
        serde_json::Value::Null => Ok(LegacyCatalogValueV4::Null),
        serde_json::Value::Bool(value) => Ok(LegacyCatalogValueV4::Boolean(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                return Ok(LegacyCatalogValueV4::UInt64(value));
            }
            if let Some(value) = value.as_i64() {
                return Ok(LegacyCatalogValueV4::Int64(value));
            }
            let value = value.as_f64().ok_or_else(|| {
                projection_corruption(
                    &format!("source/{source_name}"),
                    "legacy source config contains an unsupported JSON number",
                )
            })?;
            let value = FiniteMetadataFloat::new(value).map_err(|_| {
                projection_corruption(
                    &format!("source/{source_name}"),
                    "legacy source config contains a non-finite JSON number",
                )
            })?;
            Ok(LegacyCatalogValueV4::Float64(value))
        }
        serde_json::Value::String(value) => Ok(LegacyCatalogValueV4::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| project_legacy_catalog_value_v4(source_name, value, next_depth, nodes))
            .collect::<Result<Vec<_>>>()
            .map(LegacyCatalogValueV4::Array),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                project_legacy_catalog_value_v4(source_name, value, next_depth, nodes)
                    .map(|value| (key.clone(), value))
            })
            .collect::<Result<BTreeMap<_, _>>>()
            .map(LegacyCatalogValueV4::Object),
    }
}

fn validate_legacy_control_schema_hash(
    stream_name: &str,
    legacy_schema_hash: &str,
    schema: &Arc<Schema>,
    schema_has_custom_metadata: bool,
) -> Result<()> {
    if legacy_schema_hash.len() != 16
        || legacy_schema_hash
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err(data_corruption(
            stream_name,
            "legacy Schema hash must contain 16 canonical lowercase hexadecimal digits",
        ));
    }
    if schema_has_custom_metadata {
        return Ok(());
    }
    let recomputed = zippy_core::canonical_schema_hash(schema);
    if legacy_schema_hash != recomputed {
        return Err(data_corruption(
            stream_name,
            &format!(
                "legacy Schema hash does not match parsed Arrow Schema expected=[{recomputed}] \
                 actual=[{legacy_schema_hash}]"
            ),
        ));
    }
    Ok(())
}

fn validate_projection_file_count_v4(stream_name: &str, actual: usize) -> Result<()> {
    if actual > MAX_LEGACY_CATALOG_PROJECTION_FILES_V4 {
        return Err(projection_corruption(
            stream_name,
            &format!(
                "legacy file projection exceeds file limit actual=[{actual}] maximum=[{}]",
                MAX_LEGACY_CATALOG_PROJECTION_FILES_V4
            ),
        ));
    }
    Ok(())
}

fn validate_projection_schema_ipc_size_v4(stream_name: &str, actual: usize) -> Result<()> {
    if actual > MAX_CATALOG_SCHEMA_IPC_BYTES {
        return Err(projection_corruption(
            stream_name,
            &format!(
                "legacy Schema IPC exceeds byte limit actual=[{actual}] maximum=[{}]",
                MAX_CATALOG_SCHEMA_IPC_BYTES
            ),
        ));
    }
    Ok(())
}

fn validate_projection_source_count_v4(stream_name: &str, actual: usize) -> Result<()> {
    if actual > MAX_LEGACY_CATALOG_PROJECTION_SOURCES_V4 {
        return Err(projection_corruption(
            stream_name,
            &format!(
                "legacy source projection exceeds source limit actual=[{actual}] maximum=[{}]",
                MAX_LEGACY_CATALOG_PROJECTION_SOURCES_V4
            ),
        ));
    }
    Ok(())
}

fn validate_unique_projection_file_paths_v4(
    stream_name: &str,
    files: &[LegacyMigrationFile],
) -> Result<()> {
    let mut registry_indexes_by_path = BTreeMap::new();
    for file in files {
        if let Some(first_registry_index) =
            registry_indexes_by_path.insert(file.file_path(), file.registry_index())
        {
            return Err(data_corruption(
                stream_name,
                &format!(
                    "legacy file projection contains duplicate canonical path path=[{}] \
                     first_registry_index=[{}] duplicate_registry_index=[{}]",
                    file.file_path(),
                    first_registry_index,
                    file.registry_index()
                ),
            ));
        }
    }
    Ok(())
}

fn validate_projection_metadata_budget_v4(
    stream_name: &str,
    schema: &serde_json::Value,
    sources: &[LegacyProjectionSourceViewV4],
    persisted_files: &[serde_json::Value],
    maximum: usize,
) -> Result<()> {
    validate_projection_metadata_budget_parts_v4(
        stream_name,
        schema,
        sources.iter().map(|source| {
            (
                source.source_name.as_str(),
                source.source_type.as_str(),
                source.output_stream.as_str(),
                &source.config,
                source.writer_epoch,
            )
        }),
        persisted_files,
        maximum,
    )
}

fn validate_projection_metadata_budget_parts_v4<'a>(
    stream_name: &str,
    schema: &serde_json::Value,
    sources: impl IntoIterator<Item = (&'a str, &'a str, &'a str, &'a serde_json::Value, u64)>,
    persisted_files: &[serde_json::Value],
    maximum: usize,
) -> Result<()> {
    let mut writer = ProjectionMetadataBudgetWriter::new(maximum);
    write_projection_metadata_budget_parts_v4(
        &mut writer,
        stream_name,
        schema,
        sources,
        persisted_files,
    )
}

fn write_projection_metadata_budget_parts_v4<'a>(
    writer: &mut ProjectionMetadataBudgetWriter,
    stream_name: &str,
    schema: &serde_json::Value,
    sources: impl IntoIterator<Item = (&'a str, &'a str, &'a str, &'a serde_json::Value, u64)>,
    persisted_files: &[serde_json::Value],
) -> Result<()> {
    serde_json::to_writer(&mut *writer, schema).map_err(|error| {
        projection_corruption(
            stream_name,
            &format!("legacy Schema metadata exceeds canonical byte budget error=[{error}]"),
        )
    })?;
    for (source_name, source_type, output_stream, config, writer_epoch) in sources {
        write_projection_source_metadata_budget_v4(
            writer,
            stream_name,
            source_name,
            source_type,
            output_stream,
            config,
            writer_epoch,
        )?;
    }
    serde_json::to_writer(&mut *writer, persisted_files).map_err(|error| {
        projection_corruption(
            stream_name,
            &format!("legacy file metadata exceeds canonical byte budget error=[{error}]"),
        )
    })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_projection_source_metadata_budget_v4(
    writer: &mut ProjectionMetadataBudgetWriter,
    stream_name: &str,
    source_name: &str,
    source_type: &str,
    output_stream: &str,
    config: &serde_json::Value,
    writer_epoch: u64,
) -> Result<()> {
    serde_json::to_writer(
        writer,
        &(
            source_name,
            source_type,
            output_stream,
            config,
            writer_epoch,
        ),
    )
    .map_err(|error| {
        projection_corruption(
            stream_name,
            &format!("legacy source metadata exceeds canonical byte budget error=[{error}]"),
        )
    })
}

fn write_projection_metadata_budget_value_v4(
    writer: &mut ProjectionMetadataBudgetWriter,
    stream_name: &str,
    value: &(impl Serialize + ?Sized),
    message: &'static str,
) -> Result<()> {
    serde_json::to_writer(writer, value)
        .map_err(|error| projection_corruption(stream_name, &format!("{message} error=[{error}]")))
}

struct ProjectionMetadataBudgetWriter {
    written: usize,
    maximum: usize,
}

impl ProjectionMetadataBudgetWriter {
    const fn new(maximum: usize) -> Self {
        Self {
            written: 0,
            maximum,
        }
    }
}

impl Write for ProjectionMetadataBudgetWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let next = self
            .written
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("legacy projection metadata byte count overflowed"))?;
        if next > self.maximum {
            return Err(io::Error::other(format!(
                "legacy projection metadata bytes exceed limit actual=[{next}] maximum=[{}]",
                self.maximum
            )));
        }
        self.written = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn source_owned_table_config(config: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    config
        .iter()
        .filter(|(key, _)| !key.starts_with("zippy.migration."))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn arrow_schema_from_stream_metadata(
    stream_name: &str,
    schema: &serde_json::Value,
) -> Result<Schema> {
    let fields = schema
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| data_corruption(stream_name, "stream schema metadata missing fields"))?;
    let fields = fields
        .iter()
        .map(|field| {
            let name = field
                .get("name")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| data_corruption(stream_name, "stream schema field missing name"))?;
            let segment_type = field
                .get("segment_type")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    data_corruption(stream_name, "stream schema field missing segment_type")
                })?;
            let nullable = field
                .get("nullable")
                .and_then(serde_json::Value::as_bool)
                .ok_or_else(|| {
                    data_corruption(stream_name, "stream schema field missing nullable")
                })?;
            let timezone = field.get("timezone").and_then(serde_json::Value::as_str);
            let data_type =
                parse_arrow_schema_metadata_data_type(stream_name, segment_type, timezone)?;
            let metadata =
                string_map_from_json_value(stream_name, field.get("metadata"), "field metadata")?;
            Ok(Field::new(name, data_type, nullable).with_metadata(metadata))
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata =
        string_map_from_json_value(stream_name, schema.get("metadata"), "schema metadata")?;
    Ok(Schema::new_with_metadata(fields, metadata))
}

fn parse_arrow_schema_metadata_data_type(
    stream_name: &str,
    segment_type: &str,
    timezone: Option<&str>,
) -> Result<DataType> {
    match segment_type {
        "int64" => Ok(DataType::Int64),
        "float64" => Ok(DataType::Float64),
        "utf8" => Ok(DataType::Utf8),
        "timestamp_ns_tz" => timezone
            .map(|timezone| {
                DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone.to_owned().into()))
            })
            .ok_or_else(|| {
                data_corruption(
                    stream_name,
                    "timestamp_ns_tz stream schema field missing timezone",
                )
            }),
        "timestamp_ns" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        _ => Err(data_corruption(
            stream_name,
            &format!("unsupported stream field type segment_type=[{segment_type}]"),
        )),
    }
}

fn string_map_from_json_value(
    stream_name: &str,
    value: Option<&serde_json::Value>,
    context: &str,
) -> Result<HashMap<String, String>> {
    let Some(value) = value else {
        return Ok(HashMap::new());
    };
    let object = value.as_object().ok_or_else(|| {
        data_corruption(
            stream_name,
            &format!("stream schema {context} must be an object"),
        )
    })?;
    object
        .iter()
        .map(|(key, value)| {
            value
                .as_str()
                .map(|value| (key.clone(), value.to_owned()))
                .ok_or_else(|| {
                    data_corruption(
                        stream_name,
                        &format!("stream schema {context} values must be strings"),
                    )
                })
        })
        .collect()
}

fn encode_schema_ipc(
    stream_name: &str,
    schema: &Schema,
) -> Result<zippy_types::table::SchemaIpcPayload> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, schema).map_err(|error| {
            data_corruption(
                stream_name,
                &format!("failed to encode legacy table schema IPC error=[{error}]"),
            )
        })?;
        writer.finish().map_err(|error| {
            data_corruption(
                stream_name,
                &format!("failed to finish legacy table schema IPC error=[{error}]"),
            )
        })?;
    }
    zippy_types::table::SchemaIpcPayload::new(bytes).map_err(|error| {
        data_corruption(
            stream_name,
            &format!("legacy table schema IPC is empty error=[{error}]"),
        )
    })
}

fn map_tables(view: &LegacyCatalogView) -> Result<Vec<TableDescriptor>> {
    let mut tables = Vec::new();
    for stream in &view.streams {
        if !has_explicit_table_kind(stream) || is_internal_table(stream)? {
            continue;
        }
        tables.push(map_table(stream)?);
    }
    tables.sort_by_key(|table| table.table_id);
    if let Some(duplicate) = tables
        .windows(2)
        .find(|pair| pair[0].table_id == pair[1].table_id)
    {
        return Err(data_corruption(
            duplicate[0].name.as_str(),
            "deterministic legacy table id collision",
        ));
    }
    Ok(tables)
}

fn map_table(stream: &LegacyStreamView) -> Result<TableDescriptor> {
    let kind = unique_metadata(stream, TABLE_KIND_FIELD)?
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| data_corruption(&stream.stream_name, "invalid zippy_table_kind"))?;
    let definition = match kind {
        "append_table" => TableDefinition::Append(AppendTableDefinition {
            event_time_column: None,
        }),
        "frame_table" => map_frame_definition(stream)?,
        "key_value_table" => map_key_value_definition(stream)?,
        _ => {
            return Err(data_corruption(
                &stream.stream_name,
                "unknown zippy_table_kind",
            ));
        }
    };
    let name = CatalogName::new(stream.stream_name.clone())
        .map_err(|_| data_corruption(&stream.stream_name, "invalid legacy table name"))?;
    Ok(TableDescriptor {
        table_id: legacy_table_id(&stream.stream_name),
        namespace_id: legacy_namespace_id(),
        name,
        definition,
        current_schema_version: 1,
        current_snapshot_version: stream.persisted_files_revision,
        writer_epoch: stream.writer_epoch,
        state: TableState::Active,
        config: BTreeMap::new(),
        created_at_ns: 0,
        updated_at_ns: 0,
    })
}

fn map_frame_definition(stream: &LegacyStreamView) -> Result<TableDefinition> {
    if let Some(mode) = unique_metadata(stream, FRAME_MODE_FIELD)? {
        if mode.as_str() != Some(SEGMENT_PER_BATCH_FRAME_MODE) {
            return Err(data_corruption(
                &stream.stream_name,
                "invalid zippy_frame_mode",
            ));
        }
    }
    if let Some(version) = unique_metadata(stream, FRAME_PROTOCOL_VERSION_FIELD)? {
        if version.as_u64() != Some(FRAME_PROTOCOL_VERSION) {
            return Err(data_corruption(
                &stream.stream_name,
                "invalid zippy_frame_protocol_version",
            ));
        }
    }
    let by = unique_metadata(stream, FRAME_BY_FIELD)?
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| data_corruption(&stream.stream_name, "missing or invalid zippy_frame_by"))?;
    let by = ColumnName::new(by.to_owned())
        .map_err(|_| data_corruption(&stream.stream_name, "invalid zippy_frame_by"))?;
    Ok(TableDefinition::Frame(FrameTableDefinition { by }))
}

fn map_key_value_definition(stream: &LegacyStreamView) -> Result<TableDefinition> {
    let keys = unique_metadata(stream, KEY_VALUE_KEYS_FIELD)?;
    let legacy_keys = unique_metadata(stream, LEGACY_LATEST_BY_FIELD)?;
    if keys.is_some() && legacy_keys.is_some() && keys != legacy_keys {
        return Err(data_corruption(
            &stream.stream_name,
            "conflicting key-value key metadata",
        ));
    }
    let values = keys
        .or(legacy_keys)
        .ok_or_else(|| data_corruption(&stream.stream_name, "missing key-value key metadata"))?;
    let values = values
        .as_array()
        .ok_or_else(|| data_corruption(&stream.stream_name, "invalid key-value key metadata"))?;
    let columns = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| data_corruption(&stream.stream_name, "invalid key-value key column"))
                .and_then(|column| {
                    ColumnName::new(column.to_owned()).map_err(|_| {
                        data_corruption(&stream.stream_name, "invalid key-value key column")
                    })
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let columns = ColumnSet::new(columns)
        .map_err(|_| data_corruption(&stream.stream_name, "invalid key-value key columns"))?;
    let definition = KeyValueTableDefinition::new(columns)
        .map_err(|_| data_corruption(&stream.stream_name, "invalid key-value key columns"))?;
    Ok(TableDefinition::KeyValue(definition))
}

fn has_explicit_table_kind(stream: &LegacyStreamView) -> bool {
    stream
        .source_configs
        .iter()
        .any(source_config_declares_table)
}

fn source_config_declares_table(config: &serde_json::Value) -> bool {
    config
        .as_object()
        .is_some_and(|object| object.contains_key(TABLE_KIND_FIELD))
}

fn is_internal_table(stream: &LegacyStreamView) -> Result<bool> {
    is_internal_table_from_configs(&stream.stream_name, stream.source_configs.iter())
}

fn is_internal_table_from_configs<'a>(
    stream_name: &str,
    source_configs: impl IntoIterator<Item = &'a serde_json::Value>,
) -> Result<bool> {
    let mut marker = None;
    for config in source_configs {
        merge_internal_table_marker(stream_name, &mut marker, config)?;
    }
    Ok(marker.unwrap_or_else(|| is_physical_shard_name(stream_name)))
}

fn merge_internal_table_marker(
    stream_name: &str,
    marker: &mut Option<bool>,
    config: &serde_json::Value,
) -> Result<()> {
    let Some(value) = config
        .as_object()
        .and_then(|object| object.get(INTERNAL_TABLE_FIELD))
    else {
        return Ok(());
    };
    let value = value
        .as_bool()
        .ok_or_else(|| data_corruption(stream_name, "invalid zippy_internal"))?;
    if marker.is_some_and(|current| current != value) {
        return Err(data_corruption(
            stream_name,
            &format!("conflicting {INTERNAL_TABLE_FIELD} metadata"),
        ));
    }
    *marker = Some(value);
    Ok(())
}

fn is_physical_shard_name(stream_name: &str) -> bool {
    let Some((parent, shard_index)) = stream_name.rsplit_once(".__shard_") else {
        return false;
    };
    !parent.is_empty()
        && shard_index.len() == 4
        && shard_index.chars().all(|value| value.is_ascii_digit())
}

fn unique_metadata<'a>(
    stream: &'a LegacyStreamView,
    key: &str,
) -> Result<Option<&'a serde_json::Value>> {
    let mut values = stream
        .source_configs
        .iter()
        .filter_map(|config| config.as_object().and_then(|object| object.get(key)));
    let Some(first) = values.next() else {
        return Ok(None);
    };
    if values.any(|value| value != first) {
        return Err(data_corruption(
            &stream.stream_name,
            &format!("conflicting {key} metadata"),
        ));
    }
    Ok(Some(first))
}

fn resolve_table<'a>(
    tables: &'a [TableDescriptor],
    table_ref: &TableRef,
) -> Option<&'a TableDescriptor> {
    match table_ref {
        TableRef::Id { table_id } => tables.iter().find(|table| table.table_id == *table_id),
        TableRef::Name { namespace, table } => {
            if namespace != &CatalogName::default_namespace() {
                return None;
            }
            tables.iter().find(|descriptor| descriptor.name == *table)
        }
    }
}

fn default_namespace_descriptor() -> NamespaceDescriptor {
    NamespaceDescriptor {
        namespace_id: legacy_namespace_id(),
        name: CatalogName::default_namespace(),
        created_at_ns: 0,
    }
}

fn legacy_namespace_id() -> NamespaceId {
    NamespaceId::from_bytes(derive_legacy_id("namespace", &[b"default"]))
        .expect("legacy namespace id is forced nonzero")
}

pub(crate) fn legacy_table_id(stream_name: &str) -> TableId {
    TableId::from_bytes(derive_legacy_id(
        "table",
        &[b"default", stream_name.as_bytes()],
    ))
    .expect("legacy table id is forced nonzero")
}

fn derive_legacy_id(domain: &str, components: &[&[u8]]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hash_component(&mut hasher, LEGACY_ID_SCHEME);
    hash_component(&mut hasher, domain.as_bytes());
    for component in components {
        hash_component(&mut hasher, component);
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    if bytes.iter().all(|byte| *byte == 0) {
        bytes[15] = 1;
    }
    bytes
}

fn hash_component(hasher: &mut Sha256, component: &[u8]) {
    hasher.update((component.len() as u64).to_be_bytes());
    hasher.update(component);
}

fn data_corruption(stream_name: &str, message: &str) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
        .with_details(ServerErrorDetails::DataCorruption {
            object: format!("legacy_catalog/table/{stream_name}"),
            expected_checksum: None,
            actual_checksum: None,
        })
        .expect("data corruption detail matches error code")
}

fn projection_corruption(object: &str, message: &str) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
        .with_details(ServerErrorDetails::DataCorruption {
            object: format!("legacy_catalog/projection/{object}"),
            expected_checksum: None,
            actual_checksum: None,
        })
        .expect("data corruption detail matches error code")
}

fn unsupported_plan(operator: &str) -> ServerError {
    ServerError::new(
        ErrorCode::UnsupportedPlan,
        "legacy catalog adapter does not implement this operation",
    )
    .with_details(ServerErrorDetails::UnsupportedPlan {
        operator: operator.to_owned(),
    })
    .expect("unsupported plan detail matches error code")
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::future::Future;
    use std::num::NonZeroU32;
    use std::pin::pin;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Wake, Waker};

    use arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;
    use zippy_core::{canonical_schema_hash, schema_metadata};
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogRead,
        CatalogSnapshotSelector, CatalogWrite, CreateNamespaceCommand, LegacyCatalogValueV4,
        ListNamespacesRequest, ListTablesRequest, NamespaceRef, VersionedCatalogCommand,
        MAX_LEGACY_CATALOG_PROJECTION_FILES_V4, MAX_LEGACY_CATALOG_VALUE_DEPTH_V4,
        MAX_LEGACY_CATALOG_VALUE_NODES_V4,
    };
    use zippy_types::schema_contract::{MAX_CATALOG_SCHEMA_FIELDS, MAX_CATALOG_SCHEMA_IPC_BYTES};
    use zippy_types::table::{CatalogName, TableRef};
    use zippy_types::{ErrorCode, NamespaceId, RequestId, ServerErrorDetails, TableId};

    use super::{
        legacy_namespace_id, map_tables, source_owned_table_config, LegacyCatalogAdapter,
        LegacyCatalogMigrationSnapshot, LegacyProjectionSourceViewV4, LegacyProjectionViewV4,
    };
    use crate::registry::Registry;
    use crate::server::MasterServer;
    use crate::snapshot::{
        RegistrySnapshot, SnapshotSourceRecord, SnapshotStore, SnapshotStreamRecord,
    };

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn add_stream(registry: &mut Registry, stream_name: &str, config: serde_json::Value) {
        registry
            .register_stream(stream_name, json!({"fields": []}), "schema", 64, 64)
            .unwrap();
        registry
            .register_source(
                &format!("{stream_name}_source"),
                "test",
                &format!("{stream_name}_process"),
                stream_name,
                config,
            )
            .unwrap();
    }

    fn add_migratable_stream(
        registry: &mut Registry,
        stream_name: &str,
        config: serde_json::Value,
        custom_metadata: bool,
    ) {
        add_migratable_stream_without_source(registry, stream_name, custom_metadata);
        registry
            .register_source(
                &format!("{stream_name}_source"),
                "test",
                &format!("{stream_name}_process"),
                stream_name,
                config,
            )
            .unwrap();
    }

    fn add_migratable_stream_without_source(
        registry: &mut Registry,
        stream_name: &str,
        custom_metadata: bool,
    ) {
        let field = Field::new("value", DataType::Int64, false).with_metadata(if custom_metadata {
            HashMap::from([("unit".to_owned(), "ticks".to_owned())])
        } else {
            HashMap::new()
        });
        let schema = Arc::new(Schema::new(vec![field]));
        registry
            .register_stream(
                stream_name,
                schema_metadata(&schema),
                &canonical_schema_hash(&schema),
                64,
                64,
            )
            .unwrap();
    }

    fn plain_migratable_schema_hash() -> String {
        canonical_schema_hash(&Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])))
    }

    fn capture_migration_snapshot_with_raw_files(
        persisted_files: Vec<serde_json::Value>,
    ) -> zippy_types::Result<LegacyCatalogMigrationSnapshot> {
        let mut registry = Registry::default();
        add_migratable_stream(
            &mut registry,
            "typed_events",
            json!({"zippy_table_kind": "append_table"}),
            false,
        );
        registry
            .set_stream_segment_metadata(
                "typed_events",
                1,
                Vec::new(),
                persisted_files,
                Vec::new(),
                1,
                1,
                Vec::new(),
                7,
            )
            .unwrap();
        LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(registry)),
            Arc::new(AtomicBool::new(true)),
        )
        .capture_migration_snapshot()
    }

    fn test_adapter() -> (LegacyCatalogAdapter, Arc<Mutex<Registry>>) {
        let mut registry = Registry::default();
        add_stream(
            &mut registry,
            "append_events",
            json!({"zippy_table_kind": "append_table"}),
        );
        add_stream(
            &mut registry,
            "cross_section",
            json!({
                "zippy_table_kind": "frame_table",
                "zippy_frame_mode": "segment_per_batch",
                "zippy_frame_by": "dt",
                "zippy_frame_protocol_version": 1
            }),
        );
        add_stream(
            &mut registry,
            "latest_factors",
            json!({
                "zippy_table_kind": "key_value_table",
                "zippy_key_value_keys": ["instrument_id"]
            }),
        );
        add_stream(
            &mut registry,
            "append_events.__shard_0000",
            json!({
                "zippy_table_kind": "append_table",
                "zippy_internal": true,
                "zippy_logical_parent": "append_events"
            }),
        );
        add_stream(
            &mut registry,
            "legacy_events.__shard_0001",
            json!({"zippy_table_kind": "append_table"}),
        );
        add_stream(&mut registry, "plain_stream", json!({"source": "test"}));
        let registry = Arc::new(Mutex::new(registry));
        (
            LegacyCatalogAdapter::new(Arc::clone(&registry), Arc::new(AtomicBool::new(true))),
            registry,
        )
    }

    #[test]
    fn discovery_exposes_only_explicit_tables_with_stable_ids() {
        let (adapter, _) = test_adapter();
        let namespace =
            block_on(adapter.get_namespace(NamespaceRef::Name(CatalogName::default_namespace())))
                .unwrap();
        assert_eq!(namespace.namespace_id, legacy_namespace_id());
        assert_eq!(
            namespace.namespace_id.to_string(),
            "2dd41c3d-f74d-fb68-e1e5-e58084d335d6"
        );

        let page = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: Some(namespace.namespace_id),
            after: None,
            limit: NonZeroU32::new(10).unwrap(),
        }))
        .unwrap();
        assert_eq!(page.tables.len(), 3);
        assert!(page
            .tables
            .windows(2)
            .all(|pair| pair[0].table_id < pair[1].table_id));
        assert!(page
            .tables
            .iter()
            .all(|table| table.name.as_str() != "plain_stream"));
        assert!(page
            .tables
            .iter()
            .all(|table| table.name.as_str() != "append_events.__shard_0000"));
        assert!(page
            .tables
            .iter()
            .all(|table| table.name.as_str() != "legacy_events.__shard_0001"));
        let append = page
            .tables
            .iter()
            .find(|table| table.name.as_str() == "append_events")
            .unwrap();
        assert_eq!(
            append.table_id.to_string(),
            "28b27ce0-4cb2-a769-efa9-2deb5cc3c29d"
        );
        let frame = page
            .tables
            .iter()
            .find(|table| table.name.as_str() == "cross_section")
            .unwrap();
        assert!(matches!(
            &frame.definition,
            zippy_types::table::TableDefinition::Frame(definition)
                if definition.by.as_str() == "dt"
        ));
        let key_value = page
            .tables
            .iter()
            .find(|table| table.name.as_str() == "latest_factors")
            .unwrap();
        assert!(matches!(
            &key_value.definition,
            zippy_types::table::TableDefinition::KeyValue(definition)
                if definition.keys().as_slice()[0].as_str() == "instrument_id"
        ));

        let second = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).unwrap(),
        }))
        .unwrap();
        assert_eq!(page.tables, second.tables);
        assert_ne!(
            namespace.namespace_id.as_u128(),
            page.tables[0].table_id.as_u128()
        );
    }

    #[test]
    fn table_pagination_uses_strict_stable_id_cursors() {
        let (adapter, _) = test_adapter();
        let mut after = None;
        let mut table_ids = Vec::new();
        loop {
            let page = block_on(adapter.list_tables(ListTablesRequest {
                namespace_id: None,
                after,
                limit: NonZeroU32::new(1).unwrap(),
            }))
            .unwrap();
            table_ids.extend(page.tables.iter().map(|table| table.table_id));
            let Some(next) = page.next_after else {
                break;
            };
            after = Some(next);
        }
        assert_eq!(table_ids.len(), 3);
        assert!(table_ids.windows(2).all(|pair| pair[0] < pair[1]));

        let exhausted = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: None,
            after: table_ids.last().copied(),
            limit: NonZeroU32::new(1).unwrap(),
        }))
        .unwrap();
        assert!(exhausted.tables.is_empty());
        assert_eq!(exhausted.next_after, None);
    }

    #[test]
    fn namespace_pagination_is_strict_and_bounded() {
        let (adapter, _) = test_adapter();
        let first = block_on(adapter.list_namespaces(ListNamespacesRequest {
            after: None,
            limit: NonZeroU32::new(1).unwrap(),
        }))
        .unwrap();
        assert_eq!(first.namespaces.len(), 1);
        assert_eq!(first.next_after, None);
        let exhausted = block_on(adapter.list_namespaces(ListNamespacesRequest {
            after: Some(first.namespaces[0].namespace_id),
            limit: NonZeroU32::new(1).unwrap(),
        }))
        .unwrap();
        assert!(exhausted.namespaces.is_empty());
    }

    #[test]
    fn resolution_preserves_input_order_and_reports_missing_tables() {
        let (adapter, _) = test_adapter();
        let page = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).unwrap(),
        }))
        .unwrap();
        let refs = [
            TableRef::from(page.tables[2].table_id),
            TableRef::from(page.tables[0].table_id),
        ];
        let resolved = block_on(adapter.resolve_tables(&refs)).unwrap();
        assert_eq!(resolved[0].table_id, page.tables[2].table_id);
        assert_eq!(resolved[1].table_id, page.tables[0].table_id);

        let missing = block_on(adapter.get_table(TableRef::from(TableId::from_u128(1).unwrap())))
            .unwrap_err();
        assert_eq!(missing.code(), ErrorCode::NotFound);
    }

    #[test]
    fn malformed_explicit_metadata_is_data_corruption() {
        let mut registry = Registry::default();
        add_stream(
            &mut registry,
            "broken_frame",
            json!({"zippy_table_kind": "frame_table"}),
        );
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(registry)),
            Arc::new(AtomicBool::new(true)),
        );
        let error = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).unwrap(),
        }))
        .unwrap_err();
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert!(matches!(
            error.details(),
            Some(ServerErrorDetails::DataCorruption { object, .. })
                if object == "legacy_catalog/table/broken_frame"
        ));
    }

    #[test]
    fn malformed_internal_marker_is_data_corruption() {
        let mut registry = Registry::default();
        add_stream(
            &mut registry,
            "broken_internal",
            json!({
                "zippy_table_kind": "append_table",
                "zippy_internal": "yes"
            }),
        );
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(registry)),
            Arc::new(AtomicBool::new(true)),
        );

        let error = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).unwrap(),
        }))
        .unwrap_err();

        assert_eq!(error.code(), ErrorCode::DataCorruption);
    }

    #[test]
    fn explicit_public_marker_overrides_physical_shard_name_fallback() {
        let mut registry = Registry::default();
        add_stream(
            &mut registry,
            "public.__shard_0000",
            json!({
                "zippy_table_kind": "append_table",
                "zippy_internal": false
            }),
        );
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(registry)),
            Arc::new(AtomicBool::new(true)),
        );

        let page = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).unwrap(),
        }))
        .unwrap();

        assert_eq!(page.tables.len(), 1);
        assert_eq!(page.tables[0].name.as_str(), "public.__shard_0000");
    }

    #[test]
    fn empty_snapshot_read_succeeds_and_nonempty_read_is_unsupported() {
        let (adapter, _) = test_adapter();
        let empty = block_on(adapter.read_snapshot_at(&[])).unwrap();
        assert!(empty.is_empty());
        let selectors = [CatalogSnapshotSelector {
            table_id: TableId::from_u128(1).unwrap(),
            snapshot_version: 0,
            schema_version: 1,
        }];
        let error = block_on(adapter.read_snapshot_at(&selectors)).unwrap_err();
        assert_eq!(error.code(), ErrorCode::UnsupportedPlan);
        assert!(matches!(
            error.details(),
            Some(ServerErrorDetails::UnsupportedPlan { operator })
                if operator == "legacy_catalog.read_snapshot_at"
        ));
    }

    #[test]
    fn stopped_master_rejects_every_catalog_read_entrypoint() {
        let master = MasterServer::default();
        let adapter = master.legacy_catalog_adapter();
        master.shutdown();

        let namespace = NamespaceRef::Name(CatalogName::default_namespace());
        let table = TableRef::Id {
            table_id: TableId::from_u128(1).unwrap(),
        };
        let selector = CatalogSnapshotSelector {
            table_id: TableId::from_u128(1).unwrap(),
            snapshot_version: 0,
            schema_version: 1,
        };
        let errors = [
            block_on(adapter.revision()).unwrap_err(),
            block_on(adapter.get_namespace(namespace)).unwrap_err(),
            block_on(adapter.list_namespaces(ListNamespacesRequest {
                after: None,
                limit: NonZeroU32::new(1).unwrap(),
            }))
            .unwrap_err(),
            block_on(adapter.get_table(table.clone())).unwrap_err(),
            block_on(adapter.list_tables(ListTablesRequest {
                namespace_id: None,
                after: None,
                limit: NonZeroU32::new(1).unwrap(),
            }))
            .unwrap_err(),
            block_on(adapter.resolve_tables(&[table])).unwrap_err(),
            block_on(adapter.read_snapshot_at(&[])).unwrap_err(),
            block_on(adapter.read_snapshot_at(&[selector])).unwrap_err(),
        ];

        assert!(errors
            .iter()
            .all(|error| error.code() == ErrorCode::Unavailable));
    }

    #[test]
    fn every_legacy_write_is_rejected_with_request_context() {
        let (adapter, _) = test_adapter();
        let request_id = RequestId::from_u128(7).unwrap();
        let envelope = CatalogCommandEnvelopeV1::new(
            request_id,
            None,
            CatalogActor {
                kind: CatalogActorKind::Migration,
                principal: "test".to_owned(),
            },
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id: NamespaceId::from_u128(9).unwrap(),
                name: CatalogName::new("unsupported").unwrap(),
            }),
        )
        .unwrap();
        let error = block_on(adapter.command(VersionedCatalogCommand::V1(envelope))).unwrap_err();
        assert_eq!(error.code(), ErrorCode::UnsupportedPlan);
        assert_eq!(error.request_id(), Some(request_id));
        assert!(matches!(
            error.details(),
            Some(ServerErrorDetails::UnsupportedPlan { operator })
                if operator == "legacy_catalog.command.create_namespace"
        ));
    }

    #[test]
    fn table_mapping_does_not_hold_or_reacquire_registry_lock() {
        let (adapter, registry) = test_adapter();
        let view = adapter.capture_view().unwrap();
        let _guard = registry.lock().unwrap();
        let tables = map_tables(&view).unwrap();
        assert_eq!(tables.len(), 3);
    }

    #[test]
    fn catalog_revision_ignores_runtime_only_control_changes() {
        let mut registry = Registry::default();
        assert_eq!(registry.catalog_revision(), 0);
        let process_id = registry.register_process("test");
        assert_eq!(registry.catalog_revision(), 0);

        registry
            .register_stream("events", json!({"fields": []}), "schema", 64, 64)
            .unwrap();
        assert_eq!(registry.catalog_revision(), 1);
        registry
            .register_source(
                "events_source",
                "test",
                &process_id,
                "events",
                json!({"zippy_table_kind": "append_table"}),
            )
            .unwrap();
        assert_eq!(registry.catalog_revision(), 2);

        registry
            .set_source_status("events_source", "running", Some(json!({"rows": 1})))
            .unwrap();
        registry.set_stream_status("events", "active").unwrap();
        registry.record_heartbeat(&process_id).unwrap();
        assert_eq!(registry.catalog_revision(), 2);

        registry.attach_writer("events", &process_id).unwrap();
        assert_eq!(registry.catalog_revision(), 3);
        registry.detach_writer("events").unwrap();
        assert_eq!(registry.catalog_revision(), 3);
    }

    #[test]
    fn snapshot_version_tracks_only_committed_file_view() {
        let mut registry = Registry::default();
        let process_id = registry.register_process("test");
        registry
            .register_stream("events", json!({"fields": []}), "schema", 64, 64)
            .unwrap();
        registry
            .register_source(
                "events_source",
                "test",
                &process_id,
                "events",
                json!({"zippy_table_kind": "append_table"}),
            )
            .unwrap();
        let registry = Arc::new(Mutex::new(registry));
        let adapter =
            LegacyCatalogAdapter::new(Arc::clone(&registry), Arc::new(AtomicBool::new(true)));
        let table_ref = TableRef::Name {
            namespace: CatalogName::default_namespace(),
            table: CatalogName::new("events").unwrap(),
        };
        assert_eq!(
            block_on(adapter.get_table(table_ref.clone()))
                .unwrap()
                .current_snapshot_version,
            0
        );

        registry
            .lock()
            .unwrap()
            .publish_persist_event(
                "events",
                &process_id,
                json!({"persist_event_type": "persist_failed"}),
            )
            .unwrap();
        assert_eq!(
            block_on(adapter.get_table(table_ref.clone()))
                .unwrap()
                .current_snapshot_version,
            0
        );

        registry
            .lock()
            .unwrap()
            .replace_persisted_files(
                "events",
                vec![json!({"file_path": "events.parquet", "created_at": 1})],
            )
            .unwrap();
        assert_eq!(
            block_on(adapter.get_table(table_ref.clone()))
                .unwrap()
                .current_snapshot_version,
            1
        );

        registry
            .lock()
            .unwrap()
            .publish_persist_event(
                "events",
                &process_id,
                json!({"persist_event_type": "persist_metadata_sync_failed"}),
            )
            .unwrap();
        assert_eq!(
            block_on(adapter.get_table(table_ref))
                .unwrap()
                .current_snapshot_version,
            1
        );
    }

    #[test]
    fn repeated_segment_metadata_does_not_advance_catalog_revision() {
        let mut registry = Registry::default();
        registry
            .register_stream("events", json!({"fields": []}), "schema", 64, 64)
            .unwrap();
        assert_eq!(registry.catalog_revision(), 1);

        let persisted_files = vec![json!({"file_path": "events.parquet"})];
        let persist_events = vec![json!({"persist_event_type": "flush"})];
        registry
            .set_stream_segment_metadata(
                "events",
                1,
                vec![json!({"source_segment_id": 1})],
                persisted_files.clone(),
                persist_events.clone(),
                5,
                4,
                vec![json!({"lease_id": "lease-1"})],
                3,
            )
            .unwrap();
        assert_eq!(registry.catalog_revision(), 2);

        registry
            .set_stream_segment_metadata(
                "events",
                2,
                vec![json!({"source_segment_id": 2})],
                persisted_files,
                persist_events,
                5,
                4,
                vec![json!({"lease_id": "lease-2"})],
                3,
            )
            .unwrap();
        assert_eq!(registry.catalog_revision(), 2);
    }

    #[test]
    fn catalog_revision_and_ids_survive_snapshot_restore() {
        let temp = tempfile::tempdir().unwrap();
        let snapshot_path = temp.path().join("master-registry.json");
        SnapshotStore::write(
            &snapshot_path,
            &RegistrySnapshot {
                catalog_revision: 41,
                streams: vec![SnapshotStreamRecord {
                    stream_name: "restored_frames".to_owned(),
                    schema: json!({"fields": []}),
                    schema_hash: "schema".to_owned(),
                    data_path: "segment".to_owned(),
                    descriptor_generation: 2,
                    sealed_segments: Vec::new(),
                    persisted_files: Vec::new(),
                    persist_events: Vec::new(),
                    persist_revision: 5,
                    persisted_files_revision: 5,
                    segment_reader_leases: Vec::new(),
                    writer_epoch: 7,
                    buffer_size: 64,
                    frame_size: 64,
                    status: "active".to_owned(),
                }],
                sources: vec![SnapshotSourceRecord {
                    source_name: "restored_frames_source".to_owned(),
                    source_type: "test".to_owned(),
                    process_id: "proc_1".to_owned(),
                    output_stream: "restored_frames".to_owned(),
                    config: json!({
                        "zippy_table_kind": "frame_table",
                        "zippy_frame_mode": "segment_per_batch",
                        "zippy_frame_by": "dt",
                        "zippy_frame_protocol_version": 1
                    }),
                    status: "running".to_owned(),
                    metrics: json!({}),
                    writer_epoch: 7,
                    revision: 9,
                }],
                engines: Vec::new(),
            },
        )
        .unwrap();

        let first = MasterServer::from_snapshot_path(&snapshot_path).unwrap();
        let second = MasterServer::from_snapshot_path(&snapshot_path).unwrap();
        let first_adapter = first.legacy_catalog_adapter();
        let second_adapter = second.legacy_catalog_adapter();
        assert_eq!(block_on(first_adapter.revision()).unwrap(), 41);
        assert_eq!(block_on(second_adapter.revision()).unwrap(), 41);

        let table_ref = TableRef::Name {
            namespace: CatalogName::default_namespace(),
            table: CatalogName::new("restored_frames").unwrap(),
        };
        let first_table = block_on(first_adapter.get_table(table_ref.clone())).unwrap();
        let second_table = block_on(second_adapter.get_table(table_ref)).unwrap();
        assert_eq!(first_table.table_id, second_table.table_id);
        assert_eq!(
            first_table.table_id.to_string(),
            "7e38750c-487e-853a-3e22-b0f7ed898aa7"
        );
        assert_eq!(first_table.current_snapshot_version, 5);
        assert_eq!(first_table.writer_epoch, 7);
    }

    #[test]
    fn migration_snapshot_is_atomic_deterministic_and_preserves_legacy_file_order() {
        let mut registry = Registry::default();
        add_migratable_stream(
            &mut registry,
            "events",
            json!({"zippy_table_kind": "append_table"}),
            false,
        );
        registry
            .replace_persisted_files(
                "events",
                vec![
                    json!({"file_path": "/legacy/second.parquet", "row_count": 2}),
                    json!({"file_path": "/legacy/first.parquet", "row_count": 1}),
                ],
            )
            .unwrap();
        let registry = Arc::new(Mutex::new(registry));
        let adapter =
            LegacyCatalogAdapter::new(Arc::clone(&registry), Arc::new(AtomicBool::new(true)));

        let first = adapter.capture_migration_snapshot().unwrap();
        let second = adapter.capture_migration_snapshot().unwrap();

        assert_eq!(first.source_snapshot_hash(), second.source_snapshot_hash());
        assert!(first.source_snapshot_hash().starts_with("sha256-v1:"));
        assert_eq!(first.tables().len(), 1);
        let table = &first.tables()[0];
        assert_eq!(
            table.descriptor().current_snapshot_version,
            table.persisted_files_revision()
        );
        assert_eq!(table.persisted_files().len(), 2);
        assert_eq!(
            table.persisted_files()[0]["file_path"],
            "/legacy/second.parquet"
        );
        assert_eq!(
            table.persisted_files()[1]["file_path"],
            "/legacy/first.parquet"
        );
        assert!(!table.schema_ipc().as_bytes().is_empty());
        assert!(!table.schema_has_custom_metadata());
    }

    #[test]
    fn migration_snapshot_marks_custom_schema_metadata_as_noncanonical_ipc() {
        let mut registry = Registry::default();
        add_migratable_stream(
            &mut registry,
            "metadata_events",
            json!({"zippy_table_kind": "append_table"}),
            true,
        );
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(registry)),
            Arc::new(AtomicBool::new(true)),
        );

        let snapshot = adapter.capture_migration_snapshot().unwrap();

        assert_eq!(snapshot.tables().len(), 1);
        assert!(snapshot.tables()[0].schema_has_custom_metadata());
    }

    #[test]
    fn migration_snapshot_v1_fixture_hash_does_not_drift_with_typed_projection() {
        let mut registry = Registry::default();
        add_migratable_stream(
            &mut registry,
            "fixture_events",
            json!({"zippy_table_kind": "append_table"}),
            false,
        );
        registry
            .replace_persisted_files(
                "fixture_events",
                vec![json!({
                    "file_path": "/legacy/fixture-events.parquet",
                    "persist_data_root": "/legacy",
                    "row_count": 3,
                    "checksum": format!("sha256-v1:{}", "1".repeat(64)),
                    "writer_epoch": 7,
                    "source_segment_id": 2,
                    "source_generation": 1,
                    "created_at": 1_234,
                    "vendor_extension": {"codec": "zstd"}
                })],
            )
            .unwrap();
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(registry)),
            Arc::new(AtomicBool::new(true)),
        );

        let snapshot = adapter.capture_migration_snapshot().unwrap();

        assert_eq!(
            snapshot.source_snapshot_hash(),
            "sha256-v1:9949fea4f921c8f404f477f3c2a51af6642bbe3aebefd50a5cf993bd89e31136"
        );
        let table = &snapshot.tables()[0];
        assert_eq!(table.persisted_files().len(), 1);
        assert_eq!(table.migration_files().len(), 1);
        assert_eq!(
            table.migration_files()[0].canonical_value(),
            &table.persisted_files()[0]
        );
        assert_eq!(
            serde_json::to_vec(table.migration_files()[0].canonical_value()).unwrap(),
            serde_json::to_vec(&table.persisted_files()[0]).unwrap()
        );
    }

    #[test]
    fn projection_v4_publishes_only_source_declared_tables_and_types_json_numbers() {
        let mut registry = Registry::default();
        add_migratable_stream_without_source(&mut registry, "projection_events", false);
        let registry = Arc::new(Mutex::new(registry));
        let adapter =
            LegacyCatalogAdapter::new(Arc::clone(&registry), Arc::new(AtomicBool::new(true)));

        let unpublished = adapter.capture_projection_snapshot_v4().unwrap();
        assert!(unpublished.tables().is_empty());

        registry
            .lock()
            .unwrap()
            .register_source(
                "projection_events_source",
                "test",
                "projection_process",
                "projection_events",
                json!({
                    "zippy_table_kind": "append_table",
                    "positive": u64::MAX,
                    "negative": i64::MIN,
                    "ratio": 1.5,
                    "nested": {"enabled": true}
                }),
            )
            .unwrap();

        let published = adapter.capture_projection_snapshot_v4().unwrap();
        assert_eq!(published.tables().len(), 1);
        assert!(published
            .global_projection_digest()
            .starts_with("sha256-v1:"));
        let table = &published.tables()[0];
        assert_eq!(table.legacy_schema_hash().len(), 16);
        assert!(table.canonical_schema_hash().starts_with("sha256-v1:"));
        assert_ne!(table.legacy_schema_hash(), table.canonical_schema_hash());
        assert!(!table.schema_ipc().as_bytes().is_empty());
        assert_eq!(table.sources().len(), 1);
        let source = &table.sources()[0];
        assert_eq!(source.source_name, "projection_events_source");
        assert_eq!(source.source_type, "test");
        assert_eq!(source.output_stream, "projection_events");
        assert_eq!(source.writer_epoch, table.descriptor().writer_epoch);
        let LegacyCatalogValueV4::Object(config) = &source.config else {
            panic!("source config must remain an object");
        };
        assert_eq!(config["positive"], LegacyCatalogValueV4::UInt64(u64::MAX));
        assert_eq!(config["negative"], LegacyCatalogValueV4::Int64(i64::MIN));
        assert!(matches!(config["ratio"], LegacyCatalogValueV4::Float64(_)));
    }

    #[test]
    fn projection_v4_digest_excludes_runtime_liveness_and_catalog_revision() {
        let mut registry = Registry::default();
        let process_id = registry.register_process("projection-test");
        add_migratable_stream_without_source(&mut registry, "runtime_events", false);
        registry
            .register_source(
                "runtime_events_source",
                "test",
                &process_id,
                "runtime_events",
                json!({"zippy_table_kind": "append_table", "endpoint": "source-a"}),
            )
            .unwrap();
        let registry = Arc::new(Mutex::new(registry));
        let adapter =
            LegacyCatalogAdapter::new(Arc::clone(&registry), Arc::new(AtomicBool::new(true)));

        let baseline = adapter.capture_projection_snapshot_v4().unwrap();
        registry
            .lock()
            .unwrap()
            .set_source_status(
                "runtime_events_source",
                "running",
                Some(json!({"rows": 17, "latency_us": 3})),
            )
            .unwrap();
        let runtime_changed = adapter.capture_projection_snapshot_v4().unwrap();
        assert_eq!(
            runtime_changed.global_projection_digest(),
            baseline.global_projection_digest()
        );
        assert_eq!(
            runtime_changed.catalog_revision(),
            baseline.catalog_revision()
        );

        registry
            .lock()
            .unwrap()
            .publish_persist_event(
                "runtime_events",
                &process_id,
                json!({
                    "persist_event_type": "flush_completed",
                    "persist_event_id": "event-1"
                }),
            )
            .unwrap();
        let event_changed = adapter.capture_projection_snapshot_v4().unwrap();
        assert!(event_changed.catalog_revision() > baseline.catalog_revision());
        assert_eq!(
            event_changed.global_projection_digest(),
            baseline.global_projection_digest()
        );

        registry
            .lock()
            .unwrap()
            .mark_records_lost_for_process(&process_id);
        let ownership_lost = adapter.capture_projection_snapshot_v4().unwrap();
        assert_eq!(
            ownership_lost.global_projection_digest(),
            baseline.global_projection_digest()
        );
        assert_eq!(
            ownership_lost.catalog_revision(),
            event_changed.catalog_revision()
        );
    }

    #[test]
    fn projection_v4_digest_tracks_stable_source_writer_and_file_revision() {
        let mut registry = Registry::default();
        add_migratable_stream(
            &mut registry,
            "stable_events",
            json!({"zippy_table_kind": "append_table", "endpoint": "source-a"}),
            false,
        );
        let registry = Arc::new(Mutex::new(registry));
        let adapter =
            LegacyCatalogAdapter::new(Arc::clone(&registry), Arc::new(AtomicBool::new(true)));
        let baseline = adapter.capture_projection_snapshot_v4().unwrap();
        let baseline_writer_epoch = baseline.tables()[0].descriptor().writer_epoch;

        {
            let mut registry = registry.lock().unwrap();
            registry.unregister_source("stable_events_source").unwrap();
            registry
                .register_source(
                    "stable_events_source",
                    "test",
                    "stable_events_process",
                    "stable_events",
                    json!({"zippy_table_kind": "append_table", "endpoint": "source-b"}),
                )
                .unwrap();
            registry
                .set_source_writer_epoch("stable_events_source", baseline_writer_epoch)
                .unwrap();
        }
        let source_changed = adapter.capture_projection_snapshot_v4().unwrap();
        assert_ne!(
            source_changed.global_projection_digest(),
            baseline.global_projection_digest()
        );
        assert_eq!(
            source_changed.tables()[0].descriptor().writer_epoch,
            baseline_writer_epoch
        );

        registry
            .lock()
            .unwrap()
            .set_source_writer_epoch(
                "stable_events_source",
                baseline_writer_epoch.saturating_add(1),
            )
            .unwrap();
        let writer_changed = adapter.capture_projection_snapshot_v4().unwrap();
        assert_ne!(
            writer_changed.global_projection_digest(),
            source_changed.global_projection_digest()
        );

        registry
            .lock()
            .unwrap()
            .set_stream_segment_metadata(
                "stable_events",
                0,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                0,
                1,
                Vec::new(),
                baseline_writer_epoch.saturating_add(1),
            )
            .unwrap();
        let files_changed = adapter.capture_projection_snapshot_v4().unwrap();
        assert_eq!(files_changed.tables()[0].persisted_files_revision(), 1);
        assert_ne!(
            files_changed.global_projection_digest(),
            writer_changed.global_projection_digest()
        );
    }

    #[test]
    fn projection_v4_fails_closed_for_unbound_duplicate_and_malformed_sources() {
        let mut unbound = Registry::default();
        unbound
            .register_source(
                "unbound_source",
                "test",
                "process",
                "missing_stream",
                json!({}),
            )
            .unwrap();
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(unbound)),
            Arc::new(AtomicBool::new(true)),
        );
        assert_eq!(
            adapter.capture_projection_snapshot_v4().unwrap_err().code(),
            ErrorCode::DataCorruption
        );

        let duplicate = LegacyProjectionSourceViewV4 {
            source_name: "duplicate".to_owned(),
            source_type: "test".to_owned(),
            output_stream: "events".to_owned(),
            config: json!({}),
            writer_epoch: 1,
        };
        let error = super::build_projection_snapshot_v4(LegacyProjectionViewV4 {
            catalog_revision: 1,
            streams: Vec::new(),
            sources: vec![duplicate.clone(), duplicate],
        })
        .unwrap_err();
        assert_eq!(error.code(), ErrorCode::DataCorruption);

        let mut malformed = Registry::default();
        add_migratable_stream_without_source(&mut malformed, "malformed_events", false);
        malformed
            .register_source(
                "malformed_events_source",
                "",
                "process",
                "malformed_events",
                json!({"zippy_table_kind": "append_table"}),
            )
            .unwrap();
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(malformed)),
            Arc::new(AtomicBool::new(true)),
        );
        assert_eq!(
            adapter.capture_projection_snapshot_v4().unwrap_err().code(),
            ErrorCode::DataCorruption
        );
    }

    #[test]
    fn projection_v4_fails_closed_for_schema_and_config_resource_corruption() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let mut mismatched = Registry::default();
        mismatched
            .register_stream(
                "mismatched_schema",
                schema_metadata(&schema),
                "0000000000000000",
                64,
                64,
            )
            .unwrap();
        mismatched
            .register_source(
                "mismatched_schema_source",
                "test",
                "process",
                "mismatched_schema",
                json!({"zippy_table_kind": "append_table"}),
            )
            .unwrap();
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(mismatched)),
            Arc::new(AtomicBool::new(true)),
        );
        assert_eq!(
            adapter.capture_projection_snapshot_v4().unwrap_err().code(),
            ErrorCode::DataCorruption
        );

        let mut deep_value = serde_json::Value::Null;
        for _ in 0..=MAX_LEGACY_CATALOG_VALUE_DEPTH_V4 {
            deep_value = serde_json::Value::Array(vec![deep_value]);
        }
        let mut deep = Registry::default();
        add_migratable_stream_without_source(&mut deep, "deep_config", false);
        deep.register_source(
            "deep_config_source",
            "test",
            "process",
            "deep_config",
            json!({"zippy_table_kind": "append_table", "deep": deep_value}),
        )
        .unwrap();
        let adapter =
            LegacyCatalogAdapter::new(Arc::new(Mutex::new(deep)), Arc::new(AtomicBool::new(true)));
        assert_eq!(
            adapter.capture_projection_snapshot_v4().unwrap_err().code(),
            ErrorCode::DataCorruption
        );

        let mut oversized = Registry::default();
        add_migratable_stream_without_source(&mut oversized, "oversized_config", false);
        oversized
            .register_source(
                "oversized_config_source",
                "test",
                "process",
                "oversized_config",
                json!({
                    "zippy_table_kind": "append_table",
                    "values": vec![serde_json::Value::Null; MAX_LEGACY_CATALOG_VALUE_NODES_V4]
                }),
            )
            .unwrap();
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(oversized)),
            Arc::new(AtomicBool::new(true)),
        );
        assert_eq!(
            adapter.capture_projection_snapshot_v4().unwrap_err().code(),
            ErrorCode::DataCorruption
        );
    }

    #[test]
    fn projection_v4_custom_metadata_uses_native_hash_and_validates_legacy_hash_format() {
        let field = Field::new("value", DataType::Int64, false).with_metadata(HashMap::from([
            ("currency".to_owned(), "CNY".to_owned()),
            ("scale".to_owned(), "10000".to_owned()),
        ]));
        let schema = Arc::new(Schema::new_with_metadata(
            vec![field],
            HashMap::from([
                ("source".to_owned(), "exchange".to_owned()),
                ("timezone".to_owned(), "Asia/Shanghai".to_owned()),
            ]),
        ));
        let mut registry = Registry::default();
        registry
            .register_stream(
                "metadata_projection",
                schema_metadata(&schema),
                "0123456789abcdef",
                64,
                64,
            )
            .unwrap();
        registry
            .register_source(
                "metadata_projection_source",
                "test",
                "process",
                "metadata_projection",
                json!({"zippy_table_kind": "append_table"}),
            )
            .unwrap();
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(registry)),
            Arc::new(AtomicBool::new(true)),
        );

        let first = adapter.capture_projection_snapshot_v4().unwrap();
        let second = adapter.capture_projection_snapshot_v4().unwrap();
        let table = &first.tables()[0];
        assert!(table.schema_has_custom_metadata());
        assert_eq!(table.legacy_schema_hash(), "0123456789abcdef");
        assert!(table.canonical_schema_hash().starts_with("sha256-v1:"));
        assert_eq!(
            first.global_projection_digest(),
            second.global_projection_digest()
        );

        let mut equivalent = Registry::default();
        equivalent
            .register_stream(
                "metadata_projection",
                schema_metadata(&schema),
                "fedcba9876543210",
                64,
                64,
            )
            .unwrap();
        equivalent
            .register_source(
                "metadata_projection_source",
                "test",
                "another_process",
                "metadata_projection",
                json!({"zippy_table_kind": "append_table"}),
            )
            .unwrap();
        let equivalent = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(equivalent)),
            Arc::new(AtomicBool::new(true)),
        )
        .capture_projection_snapshot_v4()
        .unwrap();
        assert_eq!(
            equivalent.global_projection_digest(),
            first.global_projection_digest()
        );

        let mut malformed = Registry::default();
        malformed
            .register_stream(
                "malformed_metadata_hash",
                schema_metadata(&schema),
                "ABCDEF0123456789",
                64,
                64,
            )
            .unwrap();
        malformed
            .register_source(
                "malformed_metadata_hash_source",
                "test",
                "process",
                "malformed_metadata_hash",
                json!({"zippy_table_kind": "append_table"}),
            )
            .unwrap();
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(malformed)),
            Arc::new(AtomicBool::new(true)),
        );
        assert_eq!(
            adapter.capture_projection_snapshot_v4().unwrap_err().code(),
            ErrorCode::DataCorruption
        );
    }

    #[test]
    fn projection_v4_file_count_boundary_and_duplicate_path_fail_closed() {
        let mut registry = Registry::default();
        add_migratable_stream(
            &mut registry,
            "file_boundary",
            json!({"zippy_table_kind": "append_table"}),
            false,
        );
        let at_limit = (0..MAX_LEGACY_CATALOG_PROJECTION_FILES_V4)
            .map(|index| json!({"file_path": format!("/legacy/file-{index:04}.parquet")}))
            .collect::<Vec<_>>();
        registry
            .set_stream_segment_metadata(
                "file_boundary",
                1,
                Vec::new(),
                at_limit.clone(),
                Vec::new(),
                1,
                1,
                Vec::new(),
                1,
            )
            .unwrap();
        let registry = Arc::new(Mutex::new(registry));
        let adapter =
            LegacyCatalogAdapter::new(Arc::clone(&registry), Arc::new(AtomicBool::new(true)));

        let snapshot = adapter.capture_projection_snapshot_v4().unwrap();
        assert_eq!(
            snapshot.tables()[0].migration_files().len(),
            MAX_LEGACY_CATALOG_PROJECTION_FILES_V4
        );

        let mut over_limit = at_limit;
        over_limit.push(json!({"file_path": "/legacy/file-over-limit.parquet"}));
        registry
            .lock()
            .unwrap()
            .set_stream_segment_metadata(
                "file_boundary",
                2,
                Vec::new(),
                over_limit,
                Vec::new(),
                2,
                2,
                Vec::new(),
                1,
            )
            .unwrap();
        assert_eq!(
            adapter.capture_projection_snapshot_v4().unwrap_err().code(),
            ErrorCode::DataCorruption
        );

        registry
            .lock()
            .unwrap()
            .set_stream_segment_metadata(
                "file_boundary",
                3,
                Vec::new(),
                vec![
                    json!({"file_path": "/legacy/duplicate.parquet"}),
                    json!({"file_path": "/legacy/duplicate.parquet"}),
                ],
                Vec::new(),
                3,
                3,
                Vec::new(),
                1,
            )
            .unwrap();
        assert_eq!(
            adapter.capture_projection_snapshot_v4().unwrap_err().code(),
            ErrorCode::DataCorruption
        );
    }

    #[test]
    fn projection_v4_metadata_budget_is_enforced_without_allocating_canonical_bytes() {
        let source = LegacyProjectionSourceViewV4 {
            source_name: "budget_source".to_owned(),
            source_type: "test".to_owned(),
            output_stream: "budget_stream".to_owned(),
            config: json!({"payload": "x".repeat(128)}),
            writer_epoch: 1,
        };

        assert_eq!(
            super::validate_projection_metadata_budget_v4(
                "budget_stream",
                &json!({"fields": []}),
                &[source],
                &[],
                64,
            )
            .unwrap_err()
            .code(),
            ErrorCode::DataCorruption
        );
        assert_eq!(
            super::validate_projection_schema_ipc_size_v4(
                "budget_stream",
                MAX_CATALOG_SCHEMA_IPC_BYTES + 1,
            )
            .unwrap_err()
            .code(),
            ErrorCode::DataCorruption
        );
    }

    #[test]
    fn projection_v4_metadata_budget_accumulates_across_public_tables() {
        type EmptySource<'a> = (&'a str, &'a str, &'a str, &'a serde_json::Value, u64);

        let schema = json!({"fields": []});
        let files = Vec::<serde_json::Value>::new();
        let per_table_bytes =
            serde_json::to_vec(&schema).unwrap().len() + serde_json::to_vec(&files).unwrap().len();
        let maximum = per_table_bytes * 2 - 1;
        super::validate_projection_metadata_budget_parts_v4(
            "first",
            &schema,
            std::iter::empty::<EmptySource<'_>>(),
            &files,
            maximum,
        )
        .expect("either table fits the budget in isolation");

        let mut shared = super::ProjectionMetadataBudgetWriter::new(maximum);
        super::write_projection_metadata_budget_parts_v4(
            &mut shared,
            "first",
            &schema,
            std::iter::empty::<EmptySource<'_>>(),
            &files,
        )
        .expect("first table fits the shared budget");
        assert_eq!(
            super::write_projection_metadata_budget_parts_v4(
                &mut shared,
                "second",
                &schema,
                std::iter::empty::<EmptySource<'_>>(),
                &files,
            )
            .unwrap_err()
            .code(),
            ErrorCode::DataCorruption
        );
    }

    #[test]
    fn projection_v4_capture_uses_one_budget_for_the_complete_public_snapshot() {
        fn adapter_for(stream_names: &[&str]) -> LegacyCatalogAdapter {
            let mut registry = Registry::default();
            for stream_name in stream_names {
                add_migratable_stream(
                    &mut registry,
                    stream_name,
                    json!({
                        "zippy_table_kind": "append_table",
                        "payload": "x".repeat(64),
                    }),
                    false,
                );
            }
            LegacyCatalogAdapter::new(
                Arc::new(Mutex::new(registry)),
                Arc::new(AtomicBool::new(true)),
            )
        }

        fn minimum_passing_budget(adapter: &LegacyCatalogAdapter) -> usize {
            let mut upper = 1_usize;
            while adapter
                .capture_projection_view_v4_with_metadata_budget(upper)
                .is_err()
            {
                upper = upper.checked_mul(2).expect("test budget must not overflow");
            }
            let mut lower = 0_usize;
            while lower + 1 < upper {
                let midpoint = lower + (upper - lower) / 2;
                if adapter
                    .capture_projection_view_v4_with_metadata_budget(midpoint)
                    .is_ok()
                {
                    upper = midpoint;
                } else {
                    lower = midpoint;
                }
            }
            upper
        }

        let first = adapter_for(&["first_events"]);
        let second = adapter_for(&["second_events"]);
        let combined = adapter_for(&["first_events", "second_events"]);
        let isolated_limit = minimum_passing_budget(&first).max(minimum_passing_budget(&second));

        first
            .capture_projection_view_v4_with_metadata_budget(isolated_limit)
            .expect("first table fits the isolated limit");
        second
            .capture_projection_view_v4_with_metadata_budget(isolated_limit)
            .expect("second table fits the isolated limit");
        assert_eq!(
            combined
                .capture_projection_view_v4_with_metadata_budget(isolated_limit)
                .unwrap_err()
                .code(),
            ErrorCode::DataCorruption
        );
    }

    #[test]
    fn projection_v4_ignores_oversized_config_on_non_public_stream() {
        let mut registry = Registry::default();
        add_migratable_stream(
            &mut registry,
            "public_events",
            json!({"zippy_table_kind": "append_table"}),
            false,
        );
        add_migratable_stream_without_source(&mut registry, "plain_events", false);
        registry
            .register_source(
                "plain_events_source",
                "test",
                "process",
                "plain_events",
                json!({
                    "payload": vec![
                        serde_json::Value::Null;
                        MAX_LEGACY_CATALOG_VALUE_NODES_V4 + 1
                    ]
                }),
            )
            .unwrap();
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(registry)),
            Arc::new(AtomicBool::new(true)),
        );

        let snapshot = adapter.capture_projection_snapshot_v4().unwrap();
        assert_eq!(snapshot.tables().len(), 1);
        assert_eq!(
            snapshot.tables()[0].descriptor().name.as_str(),
            "public_events"
        );
    }

    #[test]
    fn projection_v4_enforces_shared_table_schema_contract() {
        fn capture_error(schema: Arc<Schema>, config: serde_json::Value) -> ErrorCode {
            let mut registry = Registry::default();
            registry
                .register_stream(
                    "invalid_schema",
                    schema_metadata(&schema),
                    &canonical_schema_hash(&schema),
                    64,
                    64,
                )
                .unwrap();
            registry
                .register_source(
                    "invalid_schema_source",
                    "test",
                    "process",
                    "invalid_schema",
                    config,
                )
                .unwrap();
            LegacyCatalogAdapter::new(
                Arc::new(Mutex::new(registry)),
                Arc::new(AtomicBool::new(true)),
            )
            .capture_projection_snapshot_v4()
            .unwrap_err()
            .code()
        }

        let duplicate = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));
        assert_eq!(
            capture_error(duplicate, json!({"zippy_table_kind": "append_table"})),
            ErrorCode::DataCorruption
        );

        let missing_frame_by = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            true,
        )]));
        assert_eq!(
            capture_error(
                missing_frame_by,
                json!({
                    "zippy_table_kind": "frame_table",
                    "zippy_frame_by": "dt"
                }),
            ),
            ErrorCode::DataCorruption
        );

        let non_utf8_key = Arc::new(Schema::new(vec![Field::new(
            "instrument_id",
            DataType::Int64,
            false,
        )]));
        assert_eq!(
            capture_error(
                non_utf8_key,
                json!({
                    "zippy_table_kind": "key_value_table",
                    "zippy_key_value_keys": ["instrument_id"]
                }),
            ),
            ErrorCode::DataCorruption
        );

        let over_field_limit = Arc::new(Schema::new(
            (0..=MAX_CATALOG_SCHEMA_FIELDS)
                .map(|index| Field::new(format!("field_{index}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        ));
        assert_eq!(
            capture_error(
                over_field_limit,
                json!({"zippy_table_kind": "append_table"}),
            ),
            ErrorCode::DataCorruption
        );
    }

    #[test]
    fn projection_v4_table_config_excludes_all_migration_bookkeeping() {
        let config = BTreeMap::from([
            ("retention".to_owned(), "30d".to_owned()),
            (
                "zippy.migration.legacy_stream_name".to_owned(),
                "events".to_owned(),
            ),
            ("zippy.migration.version".to_owned(), "3".to_owned()),
        ]);

        assert_eq!(
            source_owned_table_config(&config),
            BTreeMap::from([("retention".to_owned(), "30d".to_owned())])
        );
    }

    #[test]
    fn migration_file_projection_accepts_mixed_legacy_and_audited_metadata() {
        let schema_hash = plain_migratable_schema_hash();
        let minimal = json!({
            "file_path": "/legacy/minimal.parquet",
            "opaque_legacy_flag": true
        });
        let audited = json!({
            "file_path": "/legacy/audited.parquet",
            "persist_data_root": "/legacy",
            "row_count": 9,
            "checksum": format!("sha256-v1:{}", "a".repeat(64)),
            "stream_name": "typed_events",
            "schema_hash": schema_hash,
            "writer_epoch": 8,
            "source_segment_id": 11,
            "source_generation": 2,
            "created_at": 77,
            "vendor_extension": {"compression": "zstd"}
        });

        let snapshot =
            capture_migration_snapshot_with_raw_files(vec![minimal.clone(), audited.clone()])
                .unwrap();
        let table = &snapshot.tables()[0];
        assert_eq!(table.persisted_files(), &[minimal, audited]);
        let files = table.migration_files();
        assert_eq!(files.len(), 2);

        assert_eq!(files[0].registry_index(), 0);
        assert_eq!(files[0].file_path(), "/legacy/minimal.parquet");
        assert_eq!(files[0].persist_data_root(), None);
        assert_eq!(files[0].row_count(), None);
        assert_eq!(files[0].checksum(), None);
        assert_eq!(files[0].stream_name(), "typed_events");
        assert!(!files[0].stream_name_was_declared());
        assert_eq!(files[0].schema_hash(), plain_migratable_schema_hash());
        assert!(!files[0].schema_hash_was_declared());
        assert_eq!(files[0].scan_order_key().writer_epoch(), 0);
        assert_eq!(files[0].effective_scan_order(), 0);
        let minimal_extras = files[0].opaque_extras().collect::<Vec<_>>();
        assert_eq!(minimal_extras.len(), 1);
        assert_eq!(minimal_extras[0].0, "opaque_legacy_flag");
        assert_eq!(minimal_extras[0].1, &json!(true));

        assert_eq!(files[1].registry_index(), 1);
        assert_eq!(files[1].persist_data_root(), Some("/legacy"));
        assert_eq!(files[1].row_count().unwrap().get(), 9);
        let expected_checksum = format!("sha256-v1:{}", "a".repeat(64));
        assert_eq!(files[1].checksum(), Some(expected_checksum.as_str()));
        assert!(files[1].stream_name_was_declared());
        assert!(files[1].schema_hash_was_declared());
        assert_eq!(files[1].writer_epoch(), Some(8));
        assert_eq!(files[1].source_segment_id(), Some(11));
        assert_eq!(files[1].source_generation(), Some(2));
        assert_eq!(files[1].created_at(), Some(77));
        assert_eq!(files[1].effective_scan_order(), 1);
        assert_eq!(
            files[1]
                .opaque_extras()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            vec!["vendor_extension"]
        );
    }

    #[test]
    fn malformed_migration_file_fails_the_complete_table_snapshot() {
        let malformed = [
            ("entry", json!("not-an-object")),
            ("file_path", json!({"file_path": "relative.parquet"})),
            (
                "file_path",
                json!({"file_path": "/legacy/../outside.parquet"}),
            ),
            (
                "persist_data_root",
                json!({
                    "file_path": "/outside/file.parquet",
                    "persist_data_root": "/legacy"
                }),
            ),
            (
                "row_count",
                json!({"file_path": "/legacy/a", "row_count": 0}),
            ),
            (
                "checksum",
                json!({"file_path": "/legacy/a", "checksum": "sha256-v1:ABC"}),
            ),
            (
                "stream_name",
                json!({"file_path": "/legacy/a", "stream_name": "other"}),
            ),
            (
                "schema_hash",
                json!({"file_path": "/legacy/a", "schema_hash": "other"}),
            ),
            (
                "writer_epoch",
                json!({"file_path": "/legacy/a", "writer_epoch": "7"}),
            ),
        ];

        for (field, malformed_file) in malformed {
            let error = capture_migration_snapshot_with_raw_files(vec![
                json!({"file_path": "/legacy/valid.parquet"}),
                malformed_file,
            ])
            .unwrap_err();
            assert_eq!(error.code(), ErrorCode::DataCorruption, "field={field}");
            assert!(
                error.message().contains("registry_index=[1]"),
                "field={field} error={error}"
            );
            assert!(
                error.message().contains(&format!("field=[{field}]")),
                "field={field} error={error}"
            );
        }
    }

    #[test]
    fn effective_scan_order_matches_gateway_key_and_registry_tie_breaking() {
        let raw_files = vec![
            json!({
                "file_path": "/legacy/epoch-2.parquet",
                "writer_epoch": 2,
                "source_segment_id": 1,
                "source_generation": 0,
                "created_at": 10
            }),
            json!({
                "file_path": "/legacy/epoch-1-segment-2.parquet",
                "writer_epoch": 1,
                "source_segment_id": 2,
                "source_generation": 0,
                "created_at": 10
            }),
            json!({
                "file_path": "/legacy/equal.parquet",
                "writer_epoch": 1,
                "source_segment_id": 1,
                "source_generation": 0,
                "created_at": 10,
                "copy": "first"
            }),
            json!({
                "file_path": "/legacy/equal.parquet",
                "writer_epoch": 1,
                "source_segment_id": 1,
                "source_generation": 0,
                "created_at": 10,
                "copy": "second"
            }),
        ];
        let snapshot = capture_migration_snapshot_with_raw_files(raw_files.clone()).unwrap();
        let table = &snapshot.tables()[0];

        assert_eq!(table.persisted_files(), raw_files);
        assert_eq!(
            table
                .migration_files()
                .iter()
                .map(|file| file.effective_scan_order())
                .collect::<Vec<_>>(),
            vec![3, 2, 0, 1]
        );
        let mut effective = table.migration_files().iter().collect::<Vec<_>>();
        effective.sort_by_key(|file| file.effective_scan_order());
        assert_eq!(
            effective
                .iter()
                .map(|file| file.registry_index())
                .collect::<Vec<_>>(),
            vec![2, 3, 1, 0]
        );
        assert_eq!(effective[0].scan_order_key(), effective[1].scan_order_key());
        assert_eq!(effective[0].opaque_extras().next().unwrap().1, "first");
        assert_eq!(effective[1].opaque_extras().next().unwrap().1, "second");
    }
}
