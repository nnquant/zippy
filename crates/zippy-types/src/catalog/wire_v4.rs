//! Private frozen V4 legacy projection wire mirror.

use std::collections::BTreeMap;
use std::fmt::Display;
use std::str::FromStr;

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Serialize, Serializer};

use super::wire_v2::{ActorRefV2, LegacyImportFilesRefV2};
use super::{
    LegacyCatalogAdoptExistingTableCommandV4, LegacyCatalogAdvanceCheckpointCommandV4,
    LegacyCatalogBootstrapTableCommandV4, LegacyCatalogCheckpointModeV4,
    LegacyCatalogDropTableCommandV4, LegacyCatalogProjectionCommandV4,
    LegacyCatalogProjectionCompletenessV4, LegacyCatalogProjectionDigestTableV4,
    LegacyCatalogProjectionEnvelopeV4, LegacyCatalogProjectionOutcomeV4,
    LegacyCatalogProjectionPhaseV4, LegacyCatalogProjectionResponseV4,
    LegacyCatalogProjectionTableResultV4, LegacyCatalogProjectionTableStateV4,
    LegacyCatalogSourceV4, LegacyCatalogSyncTableMetadataCommandV4, LegacyCatalogValueV4,
    LEGACY_CATALOG_PROJECTION_FORMAT_VERSION_V4,
};
use crate::table::TableDefinition;

type Result<T, E> = std::result::Result<T, E>;

#[derive(Serialize)]
pub(super) struct FingerprintInputRefV4<'a> {
    format_version: u32,
    actor: ActorRefV2<'a>,
    expected_shadow_epoch: u64,
    source_projection_hash: &'a str,
    source_catalog_revision: u64,
    command: ProjectionCommandRefV4<'a>,
}

impl<'a> From<&'a LegacyCatalogProjectionEnvelopeV4> for FingerprintInputRefV4<'a> {
    fn from(envelope: &'a LegacyCatalogProjectionEnvelopeV4) -> Self {
        Self {
            format_version: envelope.format_version.get(),
            actor: (&envelope.actor).into(),
            expected_shadow_epoch: envelope.expected_shadow_epoch,
            source_projection_hash: envelope.source_projection_hash.as_str(),
            source_catalog_revision: envelope.source_catalog_revision,
            command: (&envelope.command).into(),
        }
    }
}

#[derive(Serialize)]
pub(super) struct ProjectionDigestInputRefV4<'a> {
    format_version: u32,
    tables: ProjectionDigestTablesRefV4<'a>,
}

impl<'a> From<&'a [LegacyCatalogProjectionDigestTableV4]> for ProjectionDigestInputRefV4<'a> {
    fn from(tables: &'a [LegacyCatalogProjectionDigestTableV4]) -> Self {
        Self {
            format_version: LEGACY_CATALOG_PROJECTION_FORMAT_VERSION_V4,
            tables: ProjectionDigestTablesRefV4(tables),
        }
    }
}

#[derive(Serialize)]
pub(super) struct SourceMetadataDigestInputRefV4<'a> {
    format_version: u32,
    sources: SourcesRefV4<'a>,
}

impl<'a> From<&'a [LegacyCatalogSourceV4]> for SourceMetadataDigestInputRefV4<'a> {
    fn from(sources: &'a [LegacyCatalogSourceV4]) -> Self {
        Self {
            format_version: LEGACY_CATALOG_PROJECTION_FORMAT_VERSION_V4,
            sources: SourcesRefV4(sources),
        }
    }
}

struct ProjectionDigestTablesRefV4<'a>(&'a [LegacyCatalogProjectionDigestTableV4]);

impl Serialize for ProjectionDigestTablesRefV4<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for table in self.0 {
            sequence.serialize_element(&ProjectionDigestTableRefV4::from(table))?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
struct ProjectionDigestTableRefV4<'a> {
    table_id: DisplayRef<'a, crate::TableId>,
    namespace_id: DisplayRef<'a, crate::NamespaceId>,
    name: &'a str,
    definition: TableDefinitionRefV4<'a>,
    schema_version: u64,
    schema_hash: &'a str,
    config: StringMapRefV4<'a>,
    legacy_files_revision: u64,
    writer_epoch: u64,
    state: &'static str,
    sources: SourcesRefV4<'a>,
}

impl<'a> From<&'a LegacyCatalogProjectionDigestTableV4> for ProjectionDigestTableRefV4<'a> {
    fn from(table: &'a LegacyCatalogProjectionDigestTableV4) -> Self {
        Self {
            table_id: DisplayRef(&table.table_id),
            namespace_id: DisplayRef(&table.namespace_id),
            name: table.name.as_str(),
            definition: (&table.definition).into(),
            schema_version: table.schema_version,
            schema_hash: table.schema_hash.as_str(),
            config: StringMapRefV4(&table.config),
            legacy_files_revision: table.legacy_files_revision,
            writer_epoch: table.writer_epoch,
            state: table_state(table.state),
            sources: SourcesRefV4(&table.sources),
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "type", content = "command", rename_all = "snake_case")]
enum ProjectionCommandRefV4<'a> {
    BootstrapTable(BootstrapTableRefV4<'a>),
    AdoptExistingTable(AdoptExistingTableRefV4<'a>),
    SyncTableMetadata(SyncTableMetadataRefV4<'a>),
    DropTable(DropTableRefV4<'a>),
    AdvanceCheckpoint(AdvanceCheckpointRefV4<'a>),
}

impl<'a> From<&'a LegacyCatalogProjectionCommandV4> for ProjectionCommandRefV4<'a> {
    fn from(command: &'a LegacyCatalogProjectionCommandV4) -> Self {
        match command {
            LegacyCatalogProjectionCommandV4::BootstrapTable(command) => {
                Self::BootstrapTable(command.into())
            }
            LegacyCatalogProjectionCommandV4::AdoptExistingTable(command) => {
                Self::AdoptExistingTable(command.into())
            }
            LegacyCatalogProjectionCommandV4::SyncTableMetadata(command) => {
                Self::SyncTableMetadata(command.into())
            }
            LegacyCatalogProjectionCommandV4::DropTable(command) => Self::DropTable(command.into()),
            LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(command) => {
                Self::AdvanceCheckpoint(command.into())
            }
        }
    }
}

#[derive(Serialize)]
struct AdoptExistingTableRefV4<'a> {
    table_id: DisplayRef<'a, crate::TableId>,
    namespace_id: DisplayRef<'a, crate::NamespaceId>,
    name: &'a str,
    definition: TableDefinitionRefV4<'a>,
    expected_schema_version: u64,
    expected_schema_hash: &'a str,
    config: StringMapRefV4<'a>,
    expected_snapshot_version: u64,
    expected_legacy_files_revision: u64,
    expected_writer_epoch: u64,
    sources: SourcesRefV4<'a>,
    files: LegacyImportFilesRefV2<'a>,
}

impl<'a> From<&'a LegacyCatalogAdoptExistingTableCommandV4> for AdoptExistingTableRefV4<'a> {
    fn from(command: &'a LegacyCatalogAdoptExistingTableCommandV4) -> Self {
        Self {
            table_id: DisplayRef(&command.table_id),
            namespace_id: DisplayRef(&command.namespace_id),
            name: command.name.as_str(),
            definition: (&command.definition).into(),
            expected_schema_version: command.expected_schema_version,
            expected_schema_hash: command.expected_schema_hash.as_str(),
            config: StringMapRefV4(&command.config),
            expected_snapshot_version: command.expected_snapshot_version,
            expected_legacy_files_revision: command.expected_legacy_files_revision,
            expected_writer_epoch: command.expected_writer_epoch,
            sources: SourcesRefV4(&command.sources),
            files: LegacyImportFilesRefV2(&command.files),
        }
    }
}

#[derive(Serialize)]
struct BootstrapTableRefV4<'a> {
    table_id: DisplayRef<'a, crate::TableId>,
    namespace_id: DisplayRef<'a, crate::NamespaceId>,
    name: &'a str,
    definition: TableDefinitionRefV4<'a>,
    canonical_schema_hash: &'a str,
    config: StringMapRefV4<'a>,
    writer_epoch: u64,
    sources: SourcesRefV4<'a>,
    files: LegacyImportFilesRefV2<'a>,
    legacy_files_revision: u64,
}

impl<'a> From<&'a LegacyCatalogBootstrapTableCommandV4> for BootstrapTableRefV4<'a> {
    fn from(command: &'a LegacyCatalogBootstrapTableCommandV4) -> Self {
        Self {
            table_id: DisplayRef(&command.table_id),
            namespace_id: DisplayRef(&command.namespace_id),
            name: command.name.as_str(),
            definition: (&command.definition).into(),
            canonical_schema_hash: command.canonical_schema_hash.as_str(),
            config: StringMapRefV4(&command.config),
            writer_epoch: command.writer_epoch,
            sources: SourcesRefV4(&command.sources),
            files: LegacyImportFilesRefV2(&command.files),
            legacy_files_revision: command.legacy_files_revision,
        }
    }
}

#[derive(Serialize)]
struct SyncTableMetadataRefV4<'a> {
    table_id: DisplayRef<'a, crate::TableId>,
    expected_projection_revision: u64,
    expected_schema_version: u64,
    expected_schema_hash: &'a str,
    expected_snapshot_version: u64,
    expected_legacy_files_revision: u64,
    expected_writer_epoch: u64,
    expected_source_metadata_hash: &'a str,
    writer_epoch: u64,
    sources: SourcesRefV4<'a>,
}

impl<'a> From<&'a LegacyCatalogSyncTableMetadataCommandV4> for SyncTableMetadataRefV4<'a> {
    fn from(command: &'a LegacyCatalogSyncTableMetadataCommandV4) -> Self {
        Self {
            table_id: DisplayRef(&command.table_id),
            expected_projection_revision: command.expected_projection_revision,
            expected_schema_version: command.expected_schema_version,
            expected_schema_hash: command.expected_schema_hash.as_str(),
            expected_snapshot_version: command.expected_snapshot_version,
            expected_legacy_files_revision: command.expected_legacy_files_revision,
            expected_writer_epoch: command.expected_writer_epoch,
            expected_source_metadata_hash: command.expected_source_metadata_hash.as_str(),
            writer_epoch: command.writer_epoch,
            sources: SourcesRefV4(&command.sources),
        }
    }
}

#[derive(Serialize)]
struct DropTableRefV4<'a> {
    table_id: DisplayRef<'a, crate::TableId>,
    expected_projection_revision: u64,
    expected_schema_version: u64,
    expected_schema_hash: &'a str,
    expected_snapshot_version: u64,
    expected_legacy_files_revision: u64,
    expected_writer_epoch: u64,
    expected_source_metadata_hash: Option<&'a str>,
}

impl<'a> From<&'a LegacyCatalogDropTableCommandV4> for DropTableRefV4<'a> {
    fn from(command: &'a LegacyCatalogDropTableCommandV4) -> Self {
        Self {
            table_id: DisplayRef(&command.table_id),
            expected_projection_revision: command.expected_projection_revision,
            expected_schema_version: command.expected_schema_version,
            expected_schema_hash: command.expected_schema_hash.as_str(),
            expected_snapshot_version: command.expected_snapshot_version,
            expected_legacy_files_revision: command.expected_legacy_files_revision,
            expected_writer_epoch: command.expected_writer_epoch,
            expected_source_metadata_hash: command.expected_source_metadata_hash.as_deref(),
        }
    }
}

#[derive(Serialize)]
struct AdvanceCheckpointRefV4<'a> {
    expected_checkpoint_generation: u64,
    expected_source_catalog_revision: u64,
    expected_source_projection_hash: &'a str,
    expected_phase: &'static str,
    shadow_epoch: u64,
    phase: &'static str,
    mode: &'static str,
    target_projection_digest: &'a str,
    observed_continuity_loss_epoch: u64,
}

impl<'a> From<&'a LegacyCatalogAdvanceCheckpointCommandV4> for AdvanceCheckpointRefV4<'a> {
    fn from(command: &'a LegacyCatalogAdvanceCheckpointCommandV4) -> Self {
        Self {
            expected_checkpoint_generation: command.expected_checkpoint_generation,
            expected_source_catalog_revision: command.expected_source_catalog_revision,
            expected_source_projection_hash: command.expected_source_projection_hash.as_str(),
            expected_phase: phase(command.expected_phase),
            shadow_epoch: command.shadow_epoch,
            phase: phase(command.phase),
            mode: checkpoint_mode(command.mode),
            target_projection_digest: command.target_projection_digest.as_str(),
            observed_continuity_loss_epoch: command.observed_continuity_loss_epoch,
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

struct StringMapRefV4<'a>(&'a BTreeMap<String, String>);

impl Serialize for StringMapRefV4<'_> {
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

struct SourcesRefV4<'a>(&'a [LegacyCatalogSourceV4]);

impl Serialize for SourcesRefV4<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for source in self.0 {
            sequence.serialize_element(&SourceRefV4::from(source))?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
struct SourceRefV4<'a> {
    source_name: &'a str,
    source_type: &'a str,
    output_stream: &'a str,
    config: ValueRefV4<'a>,
    writer_epoch: u64,
}

impl<'a> From<&'a LegacyCatalogSourceV4> for SourceRefV4<'a> {
    fn from(source: &'a LegacyCatalogSourceV4) -> Self {
        Self {
            source_name: source.source_name.as_str(),
            source_type: source.source_type.as_str(),
            output_stream: source.output_stream.as_str(),
            config: ValueRefV4(&source.config),
            writer_epoch: source.writer_epoch,
        }
    }
}

struct ValueRefV4<'a>(&'a LegacyCatalogValueV4);

impl Serialize for ValueRefV4<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            LegacyCatalogValueV4::Null => ValueWireRefV4::Null.serialize(serializer),
            LegacyCatalogValueV4::Boolean(value) => {
                ValueWireRefV4::Boolean(value).serialize(serializer)
            }
            LegacyCatalogValueV4::Int64(value) => {
                ValueWireRefV4::Int64(value).serialize(serializer)
            }
            LegacyCatalogValueV4::UInt64(value) => {
                ValueWireRefV4::UInt64(value).serialize(serializer)
            }
            LegacyCatalogValueV4::Float64(value) => {
                ValueWireRefV4::Float64(value.get()).serialize(serializer)
            }
            LegacyCatalogValueV4::String(value) => {
                ValueWireRefV4::String(value).serialize(serializer)
            }
            LegacyCatalogValueV4::Array(values) => {
                let values = values.iter().map(ValueRefV4).collect::<Vec<_>>();
                ValueWireRefV4::Array(values).serialize(serializer)
            }
            LegacyCatalogValueV4::Object(values) => {
                let values = values
                    .iter()
                    .map(|(key, value)| (key.as_str(), ValueRefV4(value)))
                    .collect::<BTreeMap<_, _>>();
                ValueWireRefV4::Object(values).serialize(serializer)
            }
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum ValueWireRefV4<'a> {
    Null,
    Boolean(&'a bool),
    Int64(&'a i64),
    #[serde(rename = "uint64")]
    UInt64(&'a u64),
    Float64(f64),
    String(&'a str),
    Array(Vec<ValueRefV4<'a>>),
    Object(BTreeMap<&'a str, ValueRefV4<'a>>),
}

#[derive(Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum ValueWireV4 {
    Null,
    Boolean(bool),
    Int64(i64),
    #[serde(rename = "uint64")]
    UInt64(u64),
    Float64(f64),
    String(String),
    Array(Vec<Self>),
    Object(BTreeMap<String, Self>),
}

impl ValueWireV4 {
    fn into_domain(self) -> Result<LegacyCatalogValueV4, String> {
        Ok(match self {
            Self::Null => LegacyCatalogValueV4::Null,
            Self::Boolean(value) => LegacyCatalogValueV4::Boolean(value),
            Self::Int64(value) => LegacyCatalogValueV4::Int64(value),
            Self::UInt64(value) => LegacyCatalogValueV4::UInt64(value),
            Self::Float64(value) => LegacyCatalogValueV4::Float64(
                crate::table::FiniteMetadataFloat::new(value)
                    .map_err(|error| format!("invalid V4 source float error=[{error}]"))?,
            ),
            Self::String(value) => LegacyCatalogValueV4::String(value),
            Self::Array(values) => LegacyCatalogValueV4::Array(
                values
                    .into_iter()
                    .map(Self::into_domain)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Object(values) => LegacyCatalogValueV4::Object(
                values
                    .into_iter()
                    .map(|(key, value)| Ok((key, value.into_domain()?)))
                    .collect::<Result<BTreeMap<_, _>, String>>()?,
            ),
        })
    }
}

#[cfg(test)]
pub(super) fn encode_value_v4(value: &LegacyCatalogValueV4) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&ValueRefV4(value))
}

pub(super) fn write_value_v4(
    writer: impl std::io::Write,
    value: &LegacyCatalogValueV4,
) -> serde_json::Result<()> {
    serde_json::to_writer(writer, &ValueRefV4(value))
}

pub(super) fn decode_value_v4(bytes: &[u8]) -> Result<LegacyCatalogValueV4, String> {
    serde_json::from_slice::<ValueWireV4>(bytes)
        .map_err(|error| format!("decode V4 source value error=[{error}]"))?
        .into_domain()
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "definition")]
enum TableDefinitionRefV4<'a> {
    #[serde(rename = "append_table")]
    Append { event_time_column: Option<&'a str> },
    #[serde(rename = "frame_table")]
    Frame { by: &'a str },
    #[serde(rename = "key_value_table")]
    KeyValue { keys: Vec<&'a str> },
}

impl<'a> From<&'a TableDefinition> for TableDefinitionRefV4<'a> {
    fn from(definition: &'a TableDefinition) -> Self {
        match definition {
            TableDefinition::Append(definition) => Self::Append {
                event_time_column: definition
                    .event_time_column
                    .as_ref()
                    .map(|column| column.as_str()),
            },
            TableDefinition::Frame(definition) => Self::Frame {
                by: definition.by.as_str(),
            },
            TableDefinition::KeyValue(definition) => Self::KeyValue {
                keys: definition
                    .keys()
                    .as_slice()
                    .iter()
                    .map(|column| column.as_str())
                    .collect(),
            },
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResponseEnvelopeV4 {
    format_version: u32,
    response: ResponseV4,
}

impl From<&LegacyCatalogProjectionResponseV4> for ResponseEnvelopeV4 {
    fn from(response: &LegacyCatalogProjectionResponseV4) -> Self {
        Self {
            format_version: LEGACY_CATALOG_PROJECTION_FORMAT_VERSION_V4,
            response: response.into(),
        }
    }
}

impl ResponseEnvelopeV4 {
    pub(super) fn into_domain(self) -> Result<LegacyCatalogProjectionResponseV4, String> {
        if self.format_version != LEGACY_CATALOG_PROJECTION_FORMAT_VERSION_V4 {
            return Err(format!(
                "invalid frozen V4 legacy projection response version actual=[{}]",
                self.format_version
            ));
        }
        self.response.into_domain()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseV4 {
    request_id: String,
    catalog_revision: u64,
    outcome: OutcomeV4,
}

impl From<&LegacyCatalogProjectionResponseV4> for ResponseV4 {
    fn from(response: &LegacyCatalogProjectionResponseV4) -> Self {
        Self {
            request_id: response.request_id.to_string(),
            catalog_revision: response.catalog_revision,
            outcome: (&response.outcome).into(),
        }
    }
}

impl ResponseV4 {
    fn into_domain(self) -> Result<LegacyCatalogProjectionResponseV4, String> {
        Ok(LegacyCatalogProjectionResponseV4 {
            request_id: parse(self.request_id, "request_id")?,
            catalog_revision: self.catalog_revision,
            outcome: self.outcome.into_domain()?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "outcome",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum OutcomeV4 {
    BootstrapTable(TableResultV4),
    AdoptExistingTable(TableResultV4),
    SyncTableMetadata(TableResultV4),
    DropTable(TableResultV4),
    AdvanceCheckpoint(CheckpointV4),
}

impl From<&LegacyCatalogProjectionOutcomeV4> for OutcomeV4 {
    fn from(outcome: &LegacyCatalogProjectionOutcomeV4) -> Self {
        match outcome {
            LegacyCatalogProjectionOutcomeV4::BootstrapTable(result) => {
                Self::BootstrapTable(result.into())
            }
            LegacyCatalogProjectionOutcomeV4::AdoptExistingTable(result) => {
                Self::AdoptExistingTable(result.into())
            }
            LegacyCatalogProjectionOutcomeV4::SyncTableMetadata(result) => {
                Self::SyncTableMetadata(result.into())
            }
            LegacyCatalogProjectionOutcomeV4::DropTable(result) => Self::DropTable(result.into()),
            LegacyCatalogProjectionOutcomeV4::AdvanceCheckpoint(checkpoint) => {
                Self::AdvanceCheckpoint(checkpoint.into())
            }
        }
    }
}

impl OutcomeV4 {
    fn into_domain(self) -> Result<LegacyCatalogProjectionOutcomeV4, String> {
        Ok(match self {
            Self::BootstrapTable(result) => {
                LegacyCatalogProjectionOutcomeV4::BootstrapTable(result.into_domain()?)
            }
            Self::AdoptExistingTable(result) => {
                LegacyCatalogProjectionOutcomeV4::AdoptExistingTable(result.into_domain()?)
            }
            Self::SyncTableMetadata(result) => {
                LegacyCatalogProjectionOutcomeV4::SyncTableMetadata(result.into_domain()?)
            }
            Self::DropTable(result) => {
                LegacyCatalogProjectionOutcomeV4::DropTable(result.into_domain()?)
            }
            Self::AdvanceCheckpoint(checkpoint) => {
                LegacyCatalogProjectionOutcomeV4::AdvanceCheckpoint(checkpoint.into_domain()?)
            }
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableResultV4 {
    table_id: String,
    projection_revision: u64,
    native_snapshot_version: u64,
    legacy_files_revision: u64,
    writer_epoch: u64,
    completeness: String,
    state: String,
}

impl From<&LegacyCatalogProjectionTableResultV4> for TableResultV4 {
    fn from(result: &LegacyCatalogProjectionTableResultV4) -> Self {
        Self {
            table_id: result.table_id.to_string(),
            projection_revision: result.projection_revision,
            native_snapshot_version: result.native_snapshot_version,
            legacy_files_revision: result.legacy_files_revision,
            writer_epoch: result.writer_epoch,
            completeness: completeness(result.completeness).to_owned(),
            state: table_state(result.state).to_owned(),
        }
    }
}

impl TableResultV4 {
    fn into_domain(self) -> Result<LegacyCatalogProjectionTableResultV4, String> {
        Ok(LegacyCatalogProjectionTableResultV4 {
            table_id: parse(self.table_id, "table_id")?,
            projection_revision: self.projection_revision,
            native_snapshot_version: self.native_snapshot_version,
            legacy_files_revision: self.legacy_files_revision,
            writer_epoch: self.writer_epoch,
            completeness: parse_completeness(&self.completeness)?,
            state: parse_table_state(&self.state)?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointV4 {
    checkpoint_generation: u64,
    shadow_epoch: u64,
    phase: String,
    source_catalog_revision: u64,
    source_projection_hash: String,
    projection_digest: String,
    mode: String,
    resolved_continuity_loss_epoch: u64,
    updated_at_ns: u64,
}

impl From<&super::LegacyCatalogProjectionCheckpointV4> for CheckpointV4 {
    fn from(checkpoint: &super::LegacyCatalogProjectionCheckpointV4) -> Self {
        Self {
            checkpoint_generation: checkpoint.checkpoint_generation,
            shadow_epoch: checkpoint.shadow_epoch,
            phase: phase(checkpoint.phase).to_owned(),
            source_catalog_revision: checkpoint.source_catalog_revision,
            source_projection_hash: checkpoint.source_projection_hash.clone(),
            projection_digest: checkpoint.projection_digest.clone(),
            mode: checkpoint_mode(checkpoint.mode).to_owned(),
            resolved_continuity_loss_epoch: checkpoint.resolved_continuity_loss_epoch,
            updated_at_ns: checkpoint.updated_at_ns,
        }
    }
}

impl CheckpointV4 {
    fn into_domain(self) -> Result<super::LegacyCatalogProjectionCheckpointV4, String> {
        Ok(super::LegacyCatalogProjectionCheckpointV4 {
            checkpoint_generation: self.checkpoint_generation,
            shadow_epoch: self.shadow_epoch,
            phase: parse_phase(&self.phase)?,
            source_catalog_revision: self.source_catalog_revision,
            source_projection_hash: self.source_projection_hash,
            projection_digest: self.projection_digest,
            mode: parse_checkpoint_mode(&self.mode)?,
            resolved_continuity_loss_epoch: self.resolved_continuity_loss_epoch,
            updated_at_ns: self.updated_at_ns,
        })
    }
}

fn phase(value: LegacyCatalogProjectionPhaseV4) -> &'static str {
    match value {
        LegacyCatalogProjectionPhaseV4::FoundationOnly => "foundation_only",
        LegacyCatalogProjectionPhaseV4::FileShadow => "file_shadow",
        LegacyCatalogProjectionPhaseV4::FullShadow => "full_shadow",
        LegacyCatalogProjectionPhaseV4::ReadCutoverReady => "read_cutover_ready",
        LegacyCatalogProjectionPhaseV4::NativeAuthoritative => "native_authoritative",
    }
}

fn parse_phase(value: &str) -> Result<LegacyCatalogProjectionPhaseV4, String> {
    match value {
        "foundation_only" => Ok(LegacyCatalogProjectionPhaseV4::FoundationOnly),
        "file_shadow" => Ok(LegacyCatalogProjectionPhaseV4::FileShadow),
        "full_shadow" => Ok(LegacyCatalogProjectionPhaseV4::FullShadow),
        "read_cutover_ready" => Ok(LegacyCatalogProjectionPhaseV4::ReadCutoverReady),
        "native_authoritative" => Ok(LegacyCatalogProjectionPhaseV4::NativeAuthoritative),
        value => Err(format!("invalid V4 projection phase value=[{value}]")),
    }
}

fn checkpoint_mode(value: LegacyCatalogCheckpointModeV4) -> &'static str {
    match value {
        LegacyCatalogCheckpointModeV4::Contiguous => "contiguous",
        LegacyCatalogCheckpointModeV4::FullReconcile => "full_reconcile",
    }
}

fn parse_checkpoint_mode(value: &str) -> Result<LegacyCatalogCheckpointModeV4, String> {
    match value {
        "contiguous" => Ok(LegacyCatalogCheckpointModeV4::Contiguous),
        "full_reconcile" => Ok(LegacyCatalogCheckpointModeV4::FullReconcile),
        value => Err(format!("invalid V4 checkpoint mode value=[{value}]")),
    }
}

fn table_state(value: LegacyCatalogProjectionTableStateV4) -> &'static str {
    match value {
        LegacyCatalogProjectionTableStateV4::Active => "active",
        LegacyCatalogProjectionTableStateV4::Dropped => "dropped",
    }
}

fn completeness(value: LegacyCatalogProjectionCompletenessV4) -> &'static str {
    match value {
        LegacyCatalogProjectionCompletenessV4::Seeded => "seeded",
        LegacyCatalogProjectionCompletenessV4::Complete => "complete",
    }
}

fn parse_completeness(value: &str) -> Result<LegacyCatalogProjectionCompletenessV4, String> {
    match value {
        "seeded" => Ok(LegacyCatalogProjectionCompletenessV4::Seeded),
        "complete" => Ok(LegacyCatalogProjectionCompletenessV4::Complete),
        value => Err(format!(
            "invalid V4 projection completeness value=[{value}]"
        )),
    }
}

fn parse_table_state(value: &str) -> Result<LegacyCatalogProjectionTableStateV4, String> {
    match value {
        "active" => Ok(LegacyCatalogProjectionTableStateV4::Active),
        "dropped" => Ok(LegacyCatalogProjectionTableStateV4::Dropped),
        value => Err(format!("invalid V4 projection table state value=[{value}]")),
    }
}

fn parse<T>(value: String, field: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid V4 {field} error=[{error}]"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::FingerprintInputRefV4;
    use crate::catalog::{
        CatalogActor, CatalogActorKind, CatalogCodecError, LegacyCatalogCheckpointModeV4,
        LegacyCatalogDropTableCommandV4, LegacyCatalogProjectionCheckpointV4,
        LegacyCatalogProjectionCodecV4, LegacyCatalogProjectionCommandV4,
        LegacyCatalogProjectionEnvelopeV4, LegacyCatalogProjectionOutcomeV4,
        LegacyCatalogProjectionPhaseV4, LegacyCatalogProjectionResponseV4, LegacyCatalogSourceV4,
        LegacyCatalogSyncTableMetadataCommandV4, LegacyCatalogValueV4,
        MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4, MAX_LEGACY_CATALOG_VALUE_DEPTH_V4,
        MAX_LEGACY_CATALOG_VALUE_NODES_V4,
    };
    use crate::{RequestId, TableId};

    fn envelope() -> LegacyCatalogProjectionEnvelopeV4 {
        LegacyCatalogProjectionEnvelopeV4::new(
            RequestId::from_u128(4).unwrap(),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "zippy-server/legacy-projection-v4".to_owned(),
            },
            2,
            format!("sha256-v1:{}", "a".repeat(64)),
            9,
            LegacyCatalogProjectionCommandV4::SyncTableMetadata(
                LegacyCatalogSyncTableMetadataCommandV4 {
                    table_id: TableId::from_u128(3).unwrap(),
                    expected_projection_revision: 5,
                    expected_schema_version: 1,
                    expected_schema_hash: format!("sha256-v1:{}", "b".repeat(64)),
                    expected_snapshot_version: 7,
                    expected_legacy_files_revision: 8,
                    expected_writer_epoch: 10,
                    expected_source_metadata_hash: format!("sha256-v1:{}", "c".repeat(64)),
                    writer_epoch: 11,
                    sources: vec![LegacyCatalogSourceV4 {
                        source_name: "ctp".to_owned(),
                        source_type: "market_data".to_owned(),
                        output_stream: "ticks".to_owned(),
                        config: LegacyCatalogValueV4::Object(BTreeMap::from([(
                            "front".to_owned(),
                            LegacyCatalogValueV4::String("tcp://127.0.0.1:1".to_owned()),
                        )])),
                        writer_epoch: 11,
                    }],
                },
            ),
        )
        .unwrap()
    }

    fn drop_envelope(
        expected_source_metadata_hash: Option<String>,
    ) -> LegacyCatalogProjectionEnvelopeV4 {
        LegacyCatalogProjectionEnvelopeV4::new(
            RequestId::from_u128(5).unwrap(),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "zippy-server/legacy-projection-v4".to_owned(),
            },
            2,
            format!("sha256-v1:{}", "a".repeat(64)),
            9,
            LegacyCatalogProjectionCommandV4::DropTable(LegacyCatalogDropTableCommandV4 {
                table_id: TableId::from_u128(3).unwrap(),
                expected_projection_revision: 5,
                expected_schema_version: 1,
                expected_schema_hash: format!("sha256-v1:{}", "b".repeat(64)),
                expected_snapshot_version: 7,
                expected_legacy_files_revision: 8,
                expected_writer_epoch: 10,
                expected_source_metadata_hash,
            }),
        )
        .unwrap()
    }

    #[test]
    fn projection_v4_fingerprint_has_a_frozen_golden() {
        let envelope = envelope();
        let bytes = serde_json::to_string(&FingerprintInputRefV4::from(&envelope)).unwrap();
        assert!(bytes.contains("\"sync_table_metadata\""));
        assert_eq!(
            envelope.canonical_fingerprint.to_string(),
            "878cd03fd2cac9650c056429c96095cfd979fcb1b99f8bae7fde9a109ada2f62"
        );
        assert!(envelope.verify_fingerprint().is_ok());
    }

    #[test]
    fn projection_v4_fingerprint_binds_epoch_source_and_complete_typed_state() {
        let original = envelope();
        let mut variants = Vec::new();
        let mut changed = original.clone();
        changed.expected_shadow_epoch += 1;
        variants.push(changed);
        let mut changed = original.clone();
        changed.source_catalog_revision += 1;
        variants.push(changed);
        let mut changed = original.clone();
        let LegacyCatalogProjectionCommandV4::SyncTableMetadata(command) = &mut changed.command
        else {
            unreachable!()
        };
        command.sources[0].writer_epoch += 1;
        variants.push(changed);
        for changed in variants {
            assert!(changed.verify_fingerprint().is_err());
        }
    }

    #[test]
    fn projection_v4_drop_source_hash_option_has_frozen_goldens() {
        let complete = drop_envelope(Some(format!("sha256-v1:{}", "c".repeat(64))));
        let seeded = drop_envelope(None);
        let complete_bytes =
            serde_json::to_string(&FingerprintInputRefV4::from(&complete)).unwrap();
        let seeded_bytes = serde_json::to_string(&FingerprintInputRefV4::from(&seeded)).unwrap();
        assert!(complete_bytes.contains(&format!(
            "\"expected_source_metadata_hash\":\"sha256-v1:{}\"",
            "c".repeat(64)
        )));
        assert!(seeded_bytes.contains("\"expected_source_metadata_hash\":null"));
        assert_eq!(
            vec![
                complete.canonical_fingerprint.to_string(),
                seeded.canonical_fingerprint.to_string(),
            ],
            vec![
                "d2b72a949dad38578e65bab066882a96687cda4ed2afd733897f544e8309f3d5",
                "fde3e1e2122e8621a04fece7658a44b0dcf9bb13be22be407edff887d3e89fe6",
            ]
        );
    }

    #[test]
    fn projection_v4_response_round_trips_and_rejects_unknown_fields() {
        let response = LegacyCatalogProjectionResponseV4 {
            request_id: RequestId::from_u128(4).unwrap(),
            catalog_revision: 10,
            outcome: LegacyCatalogProjectionOutcomeV4::AdvanceCheckpoint(
                LegacyCatalogProjectionCheckpointV4 {
                    checkpoint_generation: 3,
                    shadow_epoch: 2,
                    phase: LegacyCatalogProjectionPhaseV4::FullShadow,
                    source_catalog_revision: 9,
                    source_projection_hash: format!("sha256-v1:{}", "a".repeat(64)),
                    projection_digest: format!("sha256-v1:{}", "d".repeat(64)),
                    mode: LegacyCatalogCheckpointModeV4::FullReconcile,
                    resolved_continuity_loss_epoch: 4,
                    updated_at_ns: 12,
                },
            ),
        };
        let encoded = LegacyCatalogProjectionCodecV4::encode_response(&response).unwrap();
        assert_eq!(
            LegacyCatalogProjectionCodecV4::decode_response(&encoded).unwrap(),
            response
        );
        let mut value = serde_json::from_slice::<serde_json::Value>(&encoded).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unknown".to_owned(), serde_json::Value::Bool(true));
        assert!(LegacyCatalogProjectionCodecV4::decode_response(
            &serde_json::to_vec(&value).unwrap()
        )
        .is_err());
    }

    fn nested_array(depth: usize) -> LegacyCatalogValueV4 {
        let mut value = LegacyCatalogValueV4::Null;
        for _ in 1..depth {
            value = LegacyCatalogValueV4::Array(vec![value]);
        }
        value
    }

    fn assert_invalid_projection(error: CatalogCodecError) {
        assert!(matches!(
            error,
            CatalogCodecError::InvalidLegacyProjectionV4(_)
        ));
    }

    #[test]
    fn projection_v4_source_codec_enforces_value_depth_limit() {
        let boundary = nested_array(MAX_LEGACY_CATALOG_VALUE_DEPTH_V4);
        let encoded = LegacyCatalogProjectionCodecV4::encode_source_config(&boundary).unwrap();
        assert_eq!(
            LegacyCatalogProjectionCodecV4::decode_source_config(&encoded).unwrap(),
            boundary
        );

        let too_deep = nested_array(MAX_LEGACY_CATALOG_VALUE_DEPTH_V4 + 1);
        assert_invalid_projection(
            LegacyCatalogProjectionCodecV4::encode_source_config(&too_deep).unwrap_err(),
        );
        let encoded = super::encode_value_v4(&too_deep).unwrap();
        assert_invalid_projection(
            LegacyCatalogProjectionCodecV4::decode_source_config(&encoded).unwrap_err(),
        );
    }

    #[test]
    fn projection_v4_source_codec_enforces_value_node_limit() {
        let boundary = LegacyCatalogValueV4::Array(vec![
            LegacyCatalogValueV4::Null;
            MAX_LEGACY_CATALOG_VALUE_NODES_V4 - 1
        ]);
        let encoded = LegacyCatalogProjectionCodecV4::encode_source_config(&boundary).unwrap();
        assert_eq!(
            LegacyCatalogProjectionCodecV4::decode_source_config(&encoded).unwrap(),
            boundary
        );

        let too_many = LegacyCatalogValueV4::Array(vec![
            LegacyCatalogValueV4::Null;
            MAX_LEGACY_CATALOG_VALUE_NODES_V4
        ]);
        assert_invalid_projection(
            LegacyCatalogProjectionCodecV4::encode_source_config(&too_many).unwrap_err(),
        );
        let encoded = super::encode_value_v4(&too_many).unwrap();
        assert_invalid_projection(
            LegacyCatalogProjectionCodecV4::decode_source_config(&encoded).unwrap_err(),
        );
    }

    #[test]
    fn projection_v4_source_codec_enforces_canonical_byte_limit() {
        let escaped_bytes = MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4 / 6 + 1;
        let oversized = LegacyCatalogValueV4::String("\0".repeat(escaped_bytes));
        assert_invalid_projection(
            LegacyCatalogProjectionCodecV4::encode_source_config(&oversized).unwrap_err(),
        );
        drop(oversized);

        let oversized = vec![b' '; MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4 + 1];
        assert_invalid_projection(
            LegacyCatalogProjectionCodecV4::decode_source_config(&oversized).unwrap_err(),
        );
    }
}
