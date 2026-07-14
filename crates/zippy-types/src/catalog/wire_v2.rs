//! Private frozen V2 legacy-import wire mirror.

use std::collections::BTreeMap;
use std::fmt::Display;
use std::str::FromStr;

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Serialize, Serializer};

use super::{
    CatalogActor, CatalogActorKind, LegacyCatalogImportEnvelopeV2, LegacyCatalogImportResponseV2,
    LEGACY_CATALOG_IMPORT_FORMAT_VERSION_V2,
};
use crate::table::{
    AppendTableDefinition, DataFileLocation, DataFileProvenance, FrameTableDefinition,
    KeyValueTableDefinition, MetadataMap, MetadataScalar, PendingDataFile, SnapshotOperation,
    SourceSegmentIdentity, SourceSegmentLineage, TableDefinition, TableDescriptor, TableSnapshot,
    TableState,
};
use crate::{LogOffset, OffsetRange, OffsetWatermark};

type Result<T, E> = std::result::Result<T, E>;

#[derive(Serialize)]
pub(super) struct FingerprintInputRefV2<'a> {
    format_version: u32,
    expected_revision: Option<u64>,
    actor: ActorRefV2<'a>,
    source_snapshot_hash: &'a str,
    source_catalog_revision: u64,
    command: LegacyImportCommandRefV2<'a>,
}

impl<'a> From<&'a LegacyCatalogImportEnvelopeV2> for FingerprintInputRefV2<'a> {
    fn from(envelope: &'a LegacyCatalogImportEnvelopeV2) -> Self {
        Self {
            format_version: envelope.format_version.get(),
            expected_revision: envelope.expected_revision,
            actor: (&envelope.actor).into(),
            source_snapshot_hash: envelope.source_snapshot_hash.as_str(),
            source_catalog_revision: envelope.source_catalog_revision,
            command: (&envelope.command).into(),
        }
    }
}

#[derive(Serialize)]
pub(super) struct ActorRefV2<'a> {
    kind: &'static str,
    principal: &'a str,
}

impl<'a> From<&'a CatalogActor> for ActorRefV2<'a> {
    fn from(actor: &'a CatalogActor) -> Self {
        Self {
            kind: actor_kind(actor.kind),
            principal: actor.principal.as_str(),
        }
    }
}

#[derive(Serialize)]
struct LegacyImportCommandRefV2<'a> {
    table_id: DisplayRef<'a, crate::TableId>,
    namespace_id: DisplayRef<'a, crate::NamespaceId>,
    name: &'a str,
    definition: TableDefinitionRefV2<'a>,
    canonical_schema_hash: &'a str,
    config: StringMapRefV2<'a>,
    writer_epoch: u64,
    files: LegacyImportFilesRefV2<'a>,
    legacy_files_revision: u64,
}

impl<'a> From<&'a super::LegacyCatalogImportCommandV2> for LegacyImportCommandRefV2<'a> {
    fn from(command: &'a super::LegacyCatalogImportCommandV2) -> Self {
        Self {
            table_id: DisplayRef(&command.table_id),
            namespace_id: DisplayRef(&command.namespace_id),
            name: command.name.as_str(),
            definition: (&command.definition).into(),
            canonical_schema_hash: command.canonical_schema_hash.as_str(),
            config: StringMapRefV2(&command.config),
            writer_epoch: command.writer_epoch,
            files: LegacyImportFilesRefV2(&command.files),
            legacy_files_revision: command.legacy_files_revision,
        }
    }
}

struct DisplayRef<'a, T>(&'a T);

impl<T> Serialize for DisplayRef<'_, T>
where
    T: Display,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self.0)
    }
}

struct StringMapRefV2<'a>(&'a BTreeMap<String, String>);

impl Serialize for StringMapRefV2<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0 {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

pub(super) struct LegacyImportFilesRefV2<'a>(pub(super) &'a [super::LegacyCatalogImportFileV2]);

impl Serialize for LegacyImportFilesRefV2<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for file in self.0 {
            sequence.serialize_element(&LegacyImportFileRefV2::from(file))?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
struct LegacyImportFileRefV2<'a> {
    file: PendingDataFileRefV2<'a>,
    discovery_order: u64,
}

impl<'a> From<&'a super::LegacyCatalogImportFileV2> for LegacyImportFileRefV2<'a> {
    fn from(file: &'a super::LegacyCatalogImportFileV2) -> Self {
        Self {
            file: (&file.file).into(),
            discovery_order: file.discovery_order,
        }
    }
}

#[derive(Serialize)]
struct PendingDataFileRefV2<'a> {
    file_id: DisplayRef<'a, crate::FileId>,
    table_id: DisplayRef<'a, crate::TableId>,
    schema_version: u64,
    location: DataFileLocationRefV2<'a>,
    file_size_bytes: u64,
    row_count: u64,
    provenance: DataFileProvenanceRefV2<'a>,
    partition: MetadataMapRefV2<'a>,
    statistics: MetadataMapRefV2<'a>,
    metadata: MetadataMapRefV2<'a>,
    checksum: &'a str,
    created_at_ns: u64,
}

impl<'a> From<&'a PendingDataFile> for PendingDataFileRefV2<'a> {
    fn from(file: &'a PendingDataFile) -> Self {
        Self {
            file_id: DisplayRef(&file.file_id),
            table_id: DisplayRef(&file.table_id),
            schema_version: file.schema_version,
            location: (&file.location).into(),
            file_size_bytes: file.file_size_bytes,
            row_count: file.row_count.get(),
            provenance: (&file.provenance).into(),
            partition: MetadataMapRefV2(&file.partition),
            statistics: MetadataMapRefV2(&file.statistics),
            metadata: MetadataMapRefV2(&file.metadata),
            checksum: file.checksum.as_str(),
            created_at_ns: file.created_at_ns,
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "type", content = "path", rename_all = "snake_case")]
enum DataFileLocationRefV2<'a> {
    Managed(&'a str),
    ExternalLegacy(&'a str),
}

impl<'a> From<&'a DataFileLocation> for DataFileLocationRefV2<'a> {
    fn from(location: &'a DataFileLocation) -> Self {
        match location {
            DataFileLocation::Managed(path) => Self::Managed(path.as_str()),
            DataFileLocation::ExternalLegacy(path) => Self::ExternalLegacy(path.as_str()),
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DataFileProvenanceRefV2<'a> {
    Native(NativeDataFileProvenanceRefV2<'a>),
    Legacy {
        offsets: Option<OffsetRangeV2>,
        preserve_discovery_order: bool,
    },
}

impl<'a> From<&'a DataFileProvenance> for DataFileProvenanceRefV2<'a> {
    fn from(provenance: &'a DataFileProvenance) -> Self {
        match provenance {
            DataFileProvenance::Native(native) => Self::Native(NativeDataFileProvenanceRefV2 {
                offsets: native.offsets().into(),
                source_lineage: SourceLineagesRefV2(native.source_lineage().as_slice()),
            }),
            DataFileProvenance::Legacy {
                offsets,
                preserve_discovery_order,
            } => Self::Legacy {
                offsets: offsets.map(Into::into),
                preserve_discovery_order: *preserve_discovery_order,
            },
        }
    }
}

#[derive(Serialize)]
struct NativeDataFileProvenanceRefV2<'a> {
    offsets: OffsetRangeV2,
    source_lineage: SourceLineagesRefV2<'a>,
}

struct SourceLineagesRefV2<'a>(&'a [SourceSegmentLineage]);

impl Serialize for SourceLineagesRefV2<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for lineage in self.0 {
            sequence.serialize_element(&SourceSegmentLineageV2::from(lineage))?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
struct SourceSegmentLineageV2 {
    source: SourceSegmentIdentityV2,
    offsets: OffsetRangeV2,
}

impl From<&SourceSegmentLineage> for SourceSegmentLineageV2 {
    fn from(lineage: &SourceSegmentLineage) -> Self {
        Self {
            source: lineage.source.into(),
            offsets: lineage.offsets.into(),
        }
    }
}

#[derive(Serialize)]
struct SourceSegmentIdentityV2 {
    segment_id: u64,
    generation: u64,
    writer_epoch: u64,
}

impl From<SourceSegmentIdentity> for SourceSegmentIdentityV2 {
    fn from(identity: SourceSegmentIdentity) -> Self {
        Self {
            segment_id: identity.segment_id,
            generation: identity.generation,
            writer_epoch: identity.writer_epoch,
        }
    }
}

struct MetadataMapRefV2<'a>(&'a MetadataMap);

impl Serialize for MetadataMapRefV2<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0 {
            map.serialize_entry(key, &MetadataScalarRefV2::from(value))?;
        }
        map.end()
    }
}

#[derive(Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum MetadataScalarRefV2<'a> {
    Null,
    Boolean(&'a bool),
    Int64(&'a i64),
    #[serde(rename = "uint64")]
    UInt64(&'a u64),
    Float64(f64),
    String(&'a str),
    Bytes(&'a [u8]),
}

impl<'a> From<&'a MetadataScalar> for MetadataScalarRefV2<'a> {
    fn from(value: &'a MetadataScalar) -> Self {
        match value {
            MetadataScalar::Null => Self::Null,
            MetadataScalar::Boolean(value) => Self::Boolean(value),
            MetadataScalar::Int64(value) => Self::Int64(value),
            MetadataScalar::UInt64(value) => Self::UInt64(value),
            MetadataScalar::Float64(value) => Self::Float64(value.get()),
            MetadataScalar::String(value) => Self::String(value),
            MetadataScalar::Bytes(value) => Self::Bytes(value),
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "definition")]
enum TableDefinitionRefV2<'a> {
    #[serde(rename = "append_table")]
    Append(AppendDefinitionRefV2<'a>),
    #[serde(rename = "frame_table")]
    Frame(FrameDefinitionRefV2<'a>),
    #[serde(rename = "key_value_table")]
    KeyValue(KeyValueDefinitionRefV2<'a>),
}

impl<'a> From<&'a TableDefinition> for TableDefinitionRefV2<'a> {
    fn from(definition: &'a TableDefinition) -> Self {
        match definition {
            TableDefinition::Append(definition) => Self::Append(AppendDefinitionRefV2 {
                event_time_column: definition
                    .event_time_column
                    .as_ref()
                    .map(|column| column.as_str()),
            }),
            TableDefinition::Frame(definition) => Self::Frame(FrameDefinitionRefV2 {
                by: definition.by.as_str(),
            }),
            TableDefinition::KeyValue(definition) => Self::KeyValue(KeyValueDefinitionRefV2 {
                keys: ColumnNamesRefV2(definition.keys().as_slice()),
            }),
        }
    }
}

#[derive(Serialize)]
struct AppendDefinitionRefV2<'a> {
    event_time_column: Option<&'a str>,
}

#[derive(Serialize)]
struct FrameDefinitionRefV2<'a> {
    by: &'a str,
}

#[derive(Serialize)]
struct KeyValueDefinitionRefV2<'a> {
    keys: ColumnNamesRefV2<'a>,
}

struct ColumnNamesRefV2<'a>(&'a [crate::table::ColumnName]);

impl Serialize for ColumnNamesRefV2<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for name in self.0 {
            sequence.serialize_element(name.as_str())?;
        }
        sequence.end()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResponseEnvelopeV2 {
    format_version: u32,
    response: ResponseV2,
}

impl From<&LegacyCatalogImportResponseV2> for ResponseEnvelopeV2 {
    fn from(response: &LegacyCatalogImportResponseV2) -> Self {
        Self {
            format_version: LEGACY_CATALOG_IMPORT_FORMAT_VERSION_V2,
            response: response.into(),
        }
    }
}

impl ResponseEnvelopeV2 {
    pub(super) fn into_domain(self) -> Result<LegacyCatalogImportResponseV2, String> {
        if self.format_version != LEGACY_CATALOG_IMPORT_FORMAT_VERSION_V2 {
            return Err(format!(
                "invalid frozen V2 legacy import response version actual=[{}]",
                self.format_version
            ));
        }
        self.response.into_domain()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseV2 {
    request_id: String,
    catalog_revision: u64,
    table: TableDescriptorV2,
    snapshot: TableSnapshotV2,
}

impl From<&LegacyCatalogImportResponseV2> for ResponseV2 {
    fn from(response: &LegacyCatalogImportResponseV2) -> Self {
        Self {
            request_id: response.request_id.to_string(),
            catalog_revision: response.catalog_revision,
            table: (&response.table).into(),
            snapshot: (&response.snapshot).into(),
        }
    }
}

impl ResponseV2 {
    fn into_domain(self) -> Result<LegacyCatalogImportResponseV2, String> {
        Ok(LegacyCatalogImportResponseV2 {
            request_id: parse(self.request_id, "request_id")?,
            catalog_revision: self.catalog_revision,
            table: self.table.into_domain()?,
            snapshot: self.snapshot.into_domain()?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableDescriptorV2 {
    table_id: String,
    namespace_id: String,
    name: String,
    definition: TableDefinitionV2,
    current_schema_version: u64,
    current_snapshot_version: u64,
    writer_epoch: u64,
    state: String,
    config: BTreeMap<String, String>,
    created_at_ns: u64,
    updated_at_ns: u64,
}

impl From<&TableDescriptor> for TableDescriptorV2 {
    fn from(descriptor: &TableDescriptor) -> Self {
        Self {
            table_id: descriptor.table_id.to_string(),
            namespace_id: descriptor.namespace_id.to_string(),
            name: descriptor.name.as_str().to_owned(),
            definition: (&descriptor.definition).into(),
            current_schema_version: descriptor.current_schema_version,
            current_snapshot_version: descriptor.current_snapshot_version,
            writer_epoch: descriptor.writer_epoch,
            state: table_state(descriptor.state).to_owned(),
            config: descriptor.config.clone(),
            created_at_ns: descriptor.created_at_ns,
            updated_at_ns: descriptor.updated_at_ns,
        }
    }
}

impl TableDescriptorV2 {
    fn into_domain(self) -> Result<TableDescriptor, String> {
        Ok(TableDescriptor {
            table_id: parse(self.table_id, "table_id")?,
            namespace_id: parse(self.namespace_id, "namespace_id")?,
            name: crate::table::CatalogName::new(self.name).map_err(invalid)?,
            definition: self.definition.into_domain()?,
            current_schema_version: self.current_schema_version,
            current_snapshot_version: self.current_snapshot_version,
            writer_epoch: self.writer_epoch,
            state: parse_table_state(&self.state)?,
            config: self.config,
            created_at_ns: self.created_at_ns,
            updated_at_ns: self.updated_at_ns,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableSnapshotV2 {
    table_id: String,
    snapshot_version: u64,
    parent_snapshot_version: Option<u64>,
    operation: String,
    commit_lsn: u64,
    committed_log_offset: OffsetWatermarkV2,
    created_at_ns: u64,
    summary: BTreeMap<String, MetadataScalarV2>,
}

impl From<&TableSnapshot> for TableSnapshotV2 {
    fn from(snapshot: &TableSnapshot) -> Self {
        Self {
            table_id: snapshot.table_id.to_string(),
            snapshot_version: snapshot.snapshot_version,
            parent_snapshot_version: snapshot.parent_snapshot_version,
            operation: snapshot_operation(snapshot.operation).to_owned(),
            commit_lsn: snapshot.commit_lsn,
            committed_log_offset: snapshot.committed_log_offset.into(),
            created_at_ns: snapshot.created_at_ns,
            summary: metadata_map(&snapshot.summary),
        }
    }
}

impl TableSnapshotV2 {
    fn into_domain(self) -> Result<TableSnapshot, String> {
        Ok(TableSnapshot {
            table_id: parse(self.table_id, "table_id")?,
            snapshot_version: self.snapshot_version,
            parent_snapshot_version: self.parent_snapshot_version,
            operation: parse_snapshot_operation(&self.operation)?,
            commit_lsn: self.commit_lsn,
            committed_log_offset: self.committed_log_offset.into_domain(),
            created_at_ns: self.created_at_ns,
            summary: metadata_map_into_domain(self.summary)?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "definition", deny_unknown_fields)]
enum TableDefinitionV2 {
    #[serde(rename = "append_table")]
    Append(AppendDefinitionV2),
    #[serde(rename = "frame_table")]
    Frame(FrameDefinitionV2),
    #[serde(rename = "key_value_table")]
    KeyValue(KeyValueDefinitionV2),
}

impl From<&TableDefinition> for TableDefinitionV2 {
    fn from(definition: &TableDefinition) -> Self {
        match definition {
            TableDefinition::Append(definition) => Self::Append(AppendDefinitionV2 {
                event_time_column: definition
                    .event_time_column
                    .as_ref()
                    .map(|column| column.as_str().to_owned()),
            }),
            TableDefinition::Frame(definition) => Self::Frame(FrameDefinitionV2 {
                by: definition.by.as_str().to_owned(),
            }),
            TableDefinition::KeyValue(definition) => Self::KeyValue(KeyValueDefinitionV2 {
                keys: definition
                    .keys()
                    .as_slice()
                    .iter()
                    .map(|column| column.as_str().to_owned())
                    .collect(),
            }),
        }
    }
}

impl TableDefinitionV2 {
    fn into_domain(self) -> Result<TableDefinition, String> {
        Ok(match self {
            Self::Append(definition) => TableDefinition::Append(AppendTableDefinition {
                event_time_column: definition
                    .event_time_column
                    .map(crate::table::ColumnName::new)
                    .transpose()
                    .map_err(invalid)?,
            }),
            Self::Frame(definition) => TableDefinition::Frame(FrameTableDefinition {
                by: crate::table::ColumnName::new(definition.by).map_err(invalid)?,
            }),
            Self::KeyValue(definition) => {
                let keys = definition
                    .keys
                    .into_iter()
                    .map(crate::table::ColumnName::new)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(invalid)?;
                TableDefinition::KeyValue(
                    KeyValueTableDefinition::new(
                        crate::table::ColumnSet::new(keys).map_err(invalid)?,
                    )
                    .map_err(invalid)?,
                )
            }
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendDefinitionV2 {
    event_time_column: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrameDefinitionV2 {
    by: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyValueDefinitionV2 {
    keys: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "offset",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum OffsetWatermarkV2 {
    Empty,
    Known(u64),
    LegacyUnknown,
}

impl From<OffsetWatermark> for OffsetWatermarkV2 {
    fn from(watermark: OffsetWatermark) -> Self {
        match watermark {
            OffsetWatermark::Empty => Self::Empty,
            OffsetWatermark::Known(offset) => Self::Known(offset.get()),
            OffsetWatermark::LegacyUnknown => Self::LegacyUnknown,
        }
    }
}

impl OffsetWatermarkV2 {
    fn into_domain(self) -> OffsetWatermark {
        match self {
            Self::Empty => OffsetWatermark::Empty,
            Self::Known(offset) => OffsetWatermark::Known(LogOffset::new(offset)),
            Self::LegacyUnknown => OffsetWatermark::LegacyUnknown,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
struct OffsetRangeV2 {
    first: u64,
    last: u64,
}

impl From<OffsetRange> for OffsetRangeV2 {
    fn from(range: OffsetRange) -> Self {
        Self {
            first: range.first().get(),
            last: range.last().get(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum MetadataScalarV2 {
    Null,
    Boolean(bool),
    Int64(i64),
    #[serde(rename = "uint64")]
    UInt64(u64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl From<&MetadataScalar> for MetadataScalarV2 {
    fn from(value: &MetadataScalar) -> Self {
        match value {
            MetadataScalar::Null => Self::Null,
            MetadataScalar::Boolean(value) => Self::Boolean(*value),
            MetadataScalar::Int64(value) => Self::Int64(*value),
            MetadataScalar::UInt64(value) => Self::UInt64(*value),
            MetadataScalar::Float64(value) => Self::Float64(value.get()),
            MetadataScalar::String(value) => Self::String(value.clone()),
            MetadataScalar::Bytes(value) => Self::Bytes(value.clone()),
        }
    }
}

impl MetadataScalarV2 {
    fn into_domain(self) -> Result<MetadataScalar, String> {
        Ok(match self {
            Self::Null => MetadataScalar::Null,
            Self::Boolean(value) => MetadataScalar::Boolean(value),
            Self::Int64(value) => MetadataScalar::Int64(value),
            Self::UInt64(value) => MetadataScalar::UInt64(value),
            Self::Float64(value) => MetadataScalar::Float64(
                crate::table::FiniteMetadataFloat::new(value).map_err(invalid)?,
            ),
            Self::String(value) => MetadataScalar::String(value),
            Self::Bytes(value) => MetadataScalar::Bytes(value),
        })
    }
}

fn metadata_map(values: &MetadataMap) -> BTreeMap<String, MetadataScalarV2> {
    values
        .iter()
        .map(|(key, value)| (key.clone(), value.into()))
        .collect()
}

fn metadata_map_into_domain(
    values: BTreeMap<String, MetadataScalarV2>,
) -> Result<MetadataMap, String> {
    values
        .into_iter()
        .map(|(key, value)| Ok((key, value.into_domain()?)))
        .collect()
}

fn actor_kind(kind: CatalogActorKind) -> &'static str {
    match kind {
        CatalogActorKind::User => "user",
        CatalogActorKind::Server => "server",
        CatalogActorKind::Maintenance => "maintenance",
        CatalogActorKind::Migration => "migration",
    }
}

fn table_state(state: TableState) -> &'static str {
    match state {
        TableState::Active => "active",
        TableState::Dropping => "dropping",
        TableState::Dropped => "dropped",
        TableState::Failed => "failed",
    }
}

fn parse_table_state(value: &str) -> Result<TableState, String> {
    match value {
        "active" => Ok(TableState::Active),
        "dropping" => Ok(TableState::Dropping),
        "dropped" => Ok(TableState::Dropped),
        "failed" => Ok(TableState::Failed),
        value => Err(format!("invalid V2 table state value=[{value}]")),
    }
}

fn snapshot_operation(operation: SnapshotOperation) -> &'static str {
    match operation {
        SnapshotOperation::Create => "create",
        SnapshotOperation::Persist => "persist",
        SnapshotOperation::Compaction => "compaction",
        SnapshotOperation::Import => "import",
        SnapshotOperation::Retention => "retention",
    }
}

fn parse_snapshot_operation(value: &str) -> Result<SnapshotOperation, String> {
    match value {
        "create" => Ok(SnapshotOperation::Create),
        "persist" => Ok(SnapshotOperation::Persist),
        "compaction" => Ok(SnapshotOperation::Compaction),
        "import" => Ok(SnapshotOperation::Import),
        "retention" => Ok(SnapshotOperation::Retention),
        value => Err(format!("invalid V2 snapshot operation value=[{value}]")),
    }
}

fn parse<T>(value: String, field: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid V2 {field} error=[{error}]"))
}

fn invalid(error: impl Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use super::FingerprintInputRefV2;
    use crate::catalog::{
        CatalogActor, CatalogActorKind, LegacyCatalogImportCodecV2, LegacyCatalogImportCommandV2,
        LegacyCatalogImportEnvelopeV2, LegacyCatalogImportFileV2, LegacyCatalogImportResponseV2,
    };
    use crate::table::{
        AppendTableDefinition, CatalogName, ColumnName, ColumnSet, DataFileLocation,
        DataFileProvenance, ExternalDataPath, FiniteMetadataFloat, FrameTableDefinition,
        KeyValueTableDefinition, MetadataMap, MetadataScalar, PendingDataFile, SchemaIpcPayload,
        SnapshotOperation, TableDefinition, TableDescriptor, TableSnapshot, TableState,
    };
    use crate::{FileId, NamespaceId, OffsetWatermark, RequestId, TableId};

    const FINGERPRINT_BYTES_V2: &str = concat!(
        r#"{"format_version":2,"expected_revision":9,"#,
        r#""actor":{"kind":"migration","principal":"legacy-importer"},"#,
        r#""source_snapshot_hash":"sha256-v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"#,
        r#"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_catalog_revision":4,"command":{"#,
        r#""table_id":"00000000-0000-0000-0000-000000000003","#,
        r#""namespace_id":"00000000-0000-0000-0000-000000000001","#,
        r#""name":"ticks","definition":{"kind":"frame_table","definition":{"by":"symbol"}},"#,
        r#""canonical_schema_hash":"schema-v1:cccccccccccccccccccccccccccccccc"#,
        r#"cccccccccccccccccccccccccccccccc","config":{"retention":"forever"},"#,
        r#""writer_epoch":7,"files":[{"file":{"#,
        r#""file_id":"00000000-0000-0000-0000-000000000004","#,
        r#""table_id":"00000000-0000-0000-0000-000000000003","schema_version":1,"#,
        r#""location":{"type":"external_legacy","path":"/legacy/ticks.parquet"},"#,
        r#""file_size_bytes":200,"row_count":3,"#,
        r#""provenance":{"type":"legacy","offsets":null,"preserve_discovery_order":true},"#,
        r#""partition":{"trade_date":{"type":"string","value":"2026-07-14"}},"#,
        r#""statistics":{"rows":{"type":"uint64","value":3}},"metadata":{"#,
        r#""boolean":{"type":"boolean","value":true},"#,
        r#""bytes":{"type":"bytes","value":[1,2,3]},"#,
        r#""float64":{"type":"float64","value":1.5},"#,
        r#""int64":{"type":"int64","value":-4},"null":{"type":"null"},"#,
        r#""string":{"type":"string","value":"value"},"#,
        r#""uint64":{"type":"uint64","value":5}},"#,
        r#""checksum":"sha256-v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"#,
        r#"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","created_at_ns":99},"#,
        r#""discovery_order":0}],"legacy_files_revision":5}}"#,
    );
    const FINGERPRINT_SHA256_V2: &str =
        "f2b535dee1a3b56071349c77303c53862728f50f9297e70ae4a56e50d732eaa0";
    const RESPONSE_BYTES_V2: &str = concat!(
        r#"{"format_version":2,"response":{"#,
        r#""request_id":"00000000-0000-0000-0000-000000000002","catalog_revision":10,"#,
        r#""table":{"table_id":"00000000-0000-0000-0000-000000000003","#,
        r#""namespace_id":"00000000-0000-0000-0000-000000000001","name":"ticks","#,
        r#""definition":{"kind":"frame_table","definition":{"by":"symbol"}},"#,
        r#""current_schema_version":1,"current_snapshot_version":2,"writer_epoch":7,"#,
        r#""state":"active","config":{"retention":"forever"},"created_at_ns":100,"#,
        r#""updated_at_ns":100},"snapshot":{"#,
        r#""table_id":"00000000-0000-0000-0000-000000000003","snapshot_version":2,"#,
        r#""parent_snapshot_version":1,"operation":"import","commit_lsn":10,"#,
        r#""committed_log_offset":{"state":"legacy_unknown"},"created_at_ns":100,"#,
        r#""summary":{"boolean":{"type":"boolean","value":true},"#,
        r#""bytes":{"type":"bytes","value":[1,2,3]},"#,
        r#""float64":{"type":"float64","value":1.5},"#,
        r#""int64":{"type":"int64","value":-4},"null":{"type":"null"},"#,
        r#""string":{"type":"string","value":"value"},"#,
        r#""uint64":{"type":"uint64","value":5}}}}}"#,
    );

    fn table_id() -> TableId {
        TableId::from_u128(3).expect("nonzero table id")
    }

    fn namespace_id() -> NamespaceId {
        NamespaceId::from_u128(1).expect("nonzero namespace id")
    }

    fn request_id() -> RequestId {
        RequestId::from_u128(2).expect("nonzero request id")
    }

    fn metadata() -> MetadataMap {
        BTreeMap::from([
            ("boolean".to_owned(), MetadataScalar::Boolean(true)),
            ("bytes".to_owned(), MetadataScalar::Bytes(vec![1, 2, 3])),
            (
                "float64".to_owned(),
                MetadataScalar::Float64(FiniteMetadataFloat::new(1.5).unwrap()),
            ),
            ("int64".to_owned(), MetadataScalar::Int64(-4)),
            ("null".to_owned(), MetadataScalar::Null),
            (
                "string".to_owned(),
                MetadataScalar::String("value".to_owned()),
            ),
            ("uint64".to_owned(), MetadataScalar::UInt64(5)),
        ])
    }

    fn frame_definition() -> TableDefinition {
        TableDefinition::Frame(FrameTableDefinition {
            by: ColumnName::new("symbol").unwrap(),
        })
    }

    fn envelope() -> LegacyCatalogImportEnvelopeV2 {
        let file = PendingDataFile {
            file_id: FileId::from_u128(4).unwrap(),
            table_id: table_id(),
            schema_version: 1,
            location: DataFileLocation::ExternalLegacy(
                ExternalDataPath::new("/legacy/ticks.parquet").unwrap(),
            ),
            file_size_bytes: 200,
            row_count: NonZeroU64::new(3).unwrap(),
            provenance: DataFileProvenance::Legacy {
                offsets: None,
                preserve_discovery_order: true,
            },
            partition: BTreeMap::from([(
                "trade_date".to_owned(),
                MetadataScalar::String("2026-07-14".to_owned()),
            )]),
            statistics: BTreeMap::from([("rows".to_owned(), MetadataScalar::UInt64(3))]),
            metadata: metadata(),
            checksum: format!("sha256-v1:{}", "b".repeat(64)),
            created_at_ns: 99,
        };
        LegacyCatalogImportEnvelopeV2::new(
            request_id(),
            Some(9),
            CatalogActor {
                kind: CatalogActorKind::Migration,
                principal: "legacy-importer".to_owned(),
            },
            format!("sha256-v1:{}", "a".repeat(64)),
            4,
            LegacyCatalogImportCommandV2 {
                table_id: table_id(),
                namespace_id: namespace_id(),
                name: CatalogName::new("ticks").unwrap(),
                definition: frame_definition(),
                schema_ipc: SchemaIpcPayload::new(vec![1, 2, 3]).unwrap(),
                canonical_schema_hash: format!("schema-v1:{}", "c".repeat(64)),
                config: BTreeMap::from([("retention".to_owned(), "forever".to_owned())]),
                writer_epoch: 7,
                files: vec![LegacyCatalogImportFileV2 {
                    file,
                    discovery_order: 0,
                }],
                legacy_files_revision: 5,
            },
        )
        .unwrap()
    }

    fn response(definition: TableDefinition) -> LegacyCatalogImportResponseV2 {
        LegacyCatalogImportResponseV2 {
            request_id: request_id(),
            catalog_revision: 10,
            table: TableDescriptor {
                table_id: table_id(),
                namespace_id: namespace_id(),
                name: CatalogName::new("ticks").unwrap(),
                definition,
                current_schema_version: 1,
                current_snapshot_version: 2,
                writer_epoch: 7,
                state: TableState::Active,
                config: BTreeMap::from([("retention".to_owned(), "forever".to_owned())]),
                created_at_ns: 100,
                updated_at_ns: 100,
            },
            snapshot: TableSnapshot {
                table_id: table_id(),
                snapshot_version: 2,
                parent_snapshot_version: Some(1),
                operation: SnapshotOperation::Import,
                commit_lsn: 10,
                committed_log_offset: OffsetWatermark::LegacyUnknown,
                created_at_ns: 100,
                summary: metadata(),
            },
        }
    }

    #[test]
    fn legacy_import_v2_fingerprint_bytes_and_digest_are_frozen() {
        let envelope = envelope();
        let bytes = serde_json::to_vec(&FingerprintInputRefV2::from(&envelope)).unwrap();
        let actual = String::from_utf8(bytes).unwrap();
        if FINGERPRINT_BYTES_V2.starts_with("__UPDATE") {
            panic!(
                "FINGERPRINT_BYTES={actual}\nFINGERPRINT_SHA256={}",
                envelope.canonical_fingerprint
            );
        }
        assert_eq!(actual, FINGERPRINT_BYTES_V2);
        assert_eq!(
            envelope.canonical_fingerprint.to_string(),
            FINGERPRINT_SHA256_V2
        );

        let mut logically_identical = envelope.clone();
        logically_identical.command.schema_ipc = SchemaIpcPayload::new(vec![9, 8, 7]).unwrap();
        assert!(logically_identical.verify_fingerprint().is_ok());
    }

    #[test]
    fn legacy_import_v2_response_bytes_are_frozen_and_round_trip() {
        let response = response(frame_definition());
        let encoded = LegacyCatalogImportCodecV2::encode_response(&response).unwrap();
        let actual = String::from_utf8(encoded.clone()).unwrap();
        if RESPONSE_BYTES_V2.starts_with("__UPDATE") {
            panic!("RESPONSE_BYTES={actual}");
        }
        assert_eq!(actual, RESPONSE_BYTES_V2);
        assert_eq!(
            LegacyCatalogImportCodecV2::decode_response(&encoded).unwrap(),
            response
        );
    }

    #[test]
    fn legacy_import_v2_response_round_trips_every_table_definition() {
        let definitions = [
            TableDefinition::Append(AppendTableDefinition {
                event_time_column: Some(ColumnName::new("event_time").unwrap()),
            }),
            frame_definition(),
            TableDefinition::KeyValue(
                KeyValueTableDefinition::new(
                    ColumnSet::new(vec![
                        ColumnName::new("symbol").unwrap(),
                        ColumnName::new("venue").unwrap(),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ),
        ];
        for definition in definitions {
            let response = response(definition);
            let encoded = LegacyCatalogImportCodecV2::encode_response(&response).unwrap();
            assert_eq!(
                LegacyCatalogImportCodecV2::decode_response(&encoded).unwrap(),
                response
            );
        }
    }

    #[test]
    fn legacy_import_v2_response_rejects_unknown_top_and_deep_fields() {
        let encoded = LegacyCatalogImportCodecV2::encode_response(&response(frame_definition()))
            .expect("encode response fixture");
        let mut top: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        top["unexpected"] = serde_json::json!(true);
        assert!(
            LegacyCatalogImportCodecV2::decode_response(&serde_json::to_vec(&top).unwrap())
                .is_err()
        );

        let mut deep: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        deep["response"]["table"]["unexpected"] = serde_json::json!(true);
        assert!(
            LegacyCatalogImportCodecV2::decode_response(&serde_json::to_vec(&deep).unwrap())
                .is_err()
        );
    }

    #[test]
    fn legacy_import_v2_decodes_frozen_historical_response_bytes() {
        if RESPONSE_BYTES_V2.starts_with("__UPDATE") {
            return;
        }
        assert_eq!(
            LegacyCatalogImportCodecV2::decode_response(RESPONSE_BYTES_V2.as_bytes()).unwrap(),
            response(frame_definition())
        );
    }

    #[test]
    fn legacy_import_v2_response_rejects_unknown_version() {
        let encoded = LegacyCatalogImportCodecV2::encode_response(&response(frame_definition()))
            .expect("encode response fixture");
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        value["format_version"] = serde_json::json!(99);
        assert!(
            LegacyCatalogImportCodecV2::decode_response(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
    }
}
