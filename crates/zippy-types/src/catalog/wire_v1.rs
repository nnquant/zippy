//! Private frozen V1 canonical wire mirror.

use std::collections::BTreeMap;
use std::fmt::Display;
use std::num::NonZeroU64;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::*;
use crate::table::{
    AppendTableDefinition, DataFileLocation, DataFileProvenance, FrameTableDefinition,
    KeyValueTableDefinition, MetadataMap, MetadataScalar, NamespaceDescriptor,
    NativeDataFileProvenance, PendingDataFile, SchemaCompatibility, SchemaDescriptor,
    SnapshotOperation, SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages,
    TableDefinition, TableDescriptor, TableSnapshot, TableState,
};
use crate::{
    CommittedOffset, ConsumerOffsetAuthority, OffsetRange, OffsetWatermark, SequenceWatermark,
};

type Result<T, E> = std::result::Result<T, E>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EnvelopeV1 {
    format_version: u32,
    request_id: String,
    expected_revision: Option<u64>,
    actor: ActorV1,
    command: CommandV1,
    canonical_fingerprint: String,
}

impl From<&CatalogCommandEnvelopeV1> for EnvelopeV1 {
    fn from(envelope: &CatalogCommandEnvelopeV1) -> Self {
        let CatalogCommandEnvelopeV1 {
            format_version,
            request_id,
            expected_revision,
            actor,
            command,
            canonical_fingerprint,
        } = envelope;
        Self {
            format_version: format_version.get(),
            request_id: request_id.to_string(),
            expected_revision: *expected_revision,
            actor: actor.into(),
            command: command.into(),
            canonical_fingerprint: canonical_fingerprint.to_string(),
        }
    }
}

impl EnvelopeV1 {
    pub(super) fn into_domain(self) -> Result<CatalogCommandEnvelopeV1, String> {
        Ok(CatalogCommandEnvelopeV1 {
            format_version: CatalogCommandFormatVersion::new(self.format_version),
            request_id: parse(self.request_id, "request_id")?,
            expected_revision: self.expected_revision,
            actor: self.actor.into_domain()?,
            command: self.command.into_domain()?,
            canonical_fingerprint: parse(self.canonical_fingerprint, "canonical_fingerprint")?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FingerprintInputV1 {
    format_version: u32,
    expected_revision: Option<u64>,
    actor: ActorV1,
    command: CommandV1,
}

impl From<&CatalogCommandFingerprintInputV1> for FingerprintInputV1 {
    fn from(input: &CatalogCommandFingerprintInputV1) -> Self {
        let CatalogCommandFingerprintInputV1 {
            format_version,
            expected_revision,
            actor,
            command,
        } = input;
        Self {
            format_version: format_version.get(),
            expected_revision: *expected_revision,
            actor: actor.into(),
            command: command.into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActorV1 {
    kind: String,
    principal: String,
}

impl From<&CatalogActor> for ActorV1 {
    fn from(actor: &CatalogActor) -> Self {
        let CatalogActor { kind, principal } = actor;
        Self {
            kind: match kind {
                CatalogActorKind::User => "user",
                CatalogActorKind::Server => "server",
                CatalogActorKind::Maintenance => "maintenance",
                CatalogActorKind::Migration => "migration",
            }
            .to_owned(),
            principal: principal.clone(),
        }
    }
}

impl ActorV1 {
    fn into_domain(self) -> Result<CatalogActor, String> {
        let kind = match self.kind.as_str() {
            "user" => CatalogActorKind::User,
            "server" => CatalogActorKind::Server,
            "maintenance" => CatalogActorKind::Maintenance,
            "migration" => CatalogActorKind::Migration,
            value => return Err(format!("invalid V1 actor kind value=[{value}]")),
        };
        Ok(CatalogActor {
            kind,
            principal: self.principal,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "command",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum CommandV1 {
    CreateNamespace(CreateNamespaceV1),
    CreateTable(CreateTableV1),
    AlterTableSchema(AlterTableSchemaV1),
    AcquireProducer(AcquireProducerV1),
    CommitDataFiles(CommitDataFilesV1),
    CommitCompaction(CommitCompactionV1),
    CommitConsumerOffset(CommitConsumerOffsetV1),
    AcquireSnapshotLease(AcquireSnapshotLeaseV1),
    ReleaseSnapshotLease(ReleaseSnapshotLeaseV1),
    CreateMaintenanceJob(CreateMaintenanceJobV1),
    TransitionMaintenanceJob(TransitionMaintenanceJobV1),
    DropTable(DropTableV1),
}

impl From<&CatalogCommandV1> for CommandV1 {
    fn from(command: &CatalogCommandV1) -> Self {
        match command {
            CatalogCommandV1::CreateNamespace(command) => Self::CreateNamespace(command.into()),
            CatalogCommandV1::CreateTable(command) => Self::CreateTable(command.into()),
            CatalogCommandV1::AlterTableSchema(command) => Self::AlterTableSchema(command.into()),
            CatalogCommandV1::AcquireProducer(command) => Self::AcquireProducer(command.into()),
            CatalogCommandV1::CommitDataFiles(command) => Self::CommitDataFiles(command.into()),
            CatalogCommandV1::CommitCompaction(command) => Self::CommitCompaction(command.into()),
            CatalogCommandV1::CommitConsumerOffset(command) => {
                Self::CommitConsumerOffset(command.into())
            }
            CatalogCommandV1::AcquireSnapshotLease(command) => {
                Self::AcquireSnapshotLease(command.into())
            }
            CatalogCommandV1::ReleaseSnapshotLease(command) => {
                Self::ReleaseSnapshotLease(command.into())
            }
            CatalogCommandV1::CreateMaintenanceJob(command) => {
                Self::CreateMaintenanceJob(command.into())
            }
            CatalogCommandV1::TransitionMaintenanceJob(command) => {
                Self::TransitionMaintenanceJob(command.into())
            }
            CatalogCommandV1::DropTable(command) => Self::DropTable(command.into()),
        }
    }
}

impl CommandV1 {
    fn into_domain(self) -> Result<CatalogCommandV1, String> {
        Ok(match self {
            Self::CreateNamespace(command) => {
                CatalogCommandV1::CreateNamespace(command.into_domain()?)
            }
            Self::CreateTable(command) => CatalogCommandV1::CreateTable(command.into_domain()?),
            Self::AlterTableSchema(command) => {
                CatalogCommandV1::AlterTableSchema(command.into_domain()?)
            }
            Self::AcquireProducer(command) => {
                CatalogCommandV1::AcquireProducer(command.into_domain()?)
            }
            Self::CommitDataFiles(command) => {
                CatalogCommandV1::CommitDataFiles(command.into_domain()?)
            }
            Self::CommitCompaction(command) => {
                CatalogCommandV1::CommitCompaction(command.into_domain()?)
            }
            Self::CommitConsumerOffset(command) => {
                CatalogCommandV1::CommitConsumerOffset(command.into_domain()?)
            }
            Self::AcquireSnapshotLease(command) => {
                CatalogCommandV1::AcquireSnapshotLease(command.into_domain()?)
            }
            Self::ReleaseSnapshotLease(command) => {
                CatalogCommandV1::ReleaseSnapshotLease(command.into_domain()?)
            }
            Self::CreateMaintenanceJob(command) => {
                CatalogCommandV1::CreateMaintenanceJob(command.into_domain()?)
            }
            Self::TransitionMaintenanceJob(command) => {
                CatalogCommandV1::TransitionMaintenanceJob(command.into_domain()?)
            }
            Self::DropTable(command) => CatalogCommandV1::DropTable(command.into_domain()?),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateNamespaceV1 {
    namespace_id: String,
    name: String,
}

impl From<&CreateNamespaceCommand> for CreateNamespaceV1 {
    fn from(command: &CreateNamespaceCommand) -> Self {
        let CreateNamespaceCommand { namespace_id, name } = command;
        Self {
            namespace_id: namespace_id.to_string(),
            name: name.as_str().to_owned(),
        }
    }
}

impl CreateNamespaceV1 {
    fn into_domain(self) -> Result<CreateNamespaceCommand, String> {
        Ok(CreateNamespaceCommand {
            namespace_id: parse(self.namespace_id, "namespace_id")?,
            name: crate::table::CatalogName::new(self.name).map_err(invalid)?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTableV1 {
    table_id: String,
    namespace_id: String,
    name: String,
    definition: TableDefinitionV1,
    schema_ipc: Vec<u8>,
    config: BTreeMap<String, String>,
}

impl From<&CreateTableCommand> for CreateTableV1 {
    fn from(command: &CreateTableCommand) -> Self {
        let CreateTableCommand {
            table_id,
            namespace_id,
            name,
            definition,
            schema_ipc,
            config,
        } = command;
        Self {
            table_id: table_id.to_string(),
            namespace_id: namespace_id.to_string(),
            name: name.as_str().to_owned(),
            definition: definition.into(),
            schema_ipc: schema_ipc.as_bytes().to_vec(),
            config: config.clone(),
        }
    }
}

impl CreateTableV1 {
    fn into_domain(self) -> Result<CreateTableCommand, String> {
        Ok(CreateTableCommand {
            table_id: parse(self.table_id, "table_id")?,
            namespace_id: parse(self.namespace_id, "namespace_id")?,
            name: crate::table::CatalogName::new(self.name).map_err(invalid)?,
            definition: self.definition.into_domain()?,
            schema_ipc: crate::table::SchemaIpcPayload::new(self.schema_ipc).map_err(invalid)?,
            config: self.config,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AlterTableSchemaV1 {
    table_id: String,
    expected_schema_version: u64,
    schema_ipc: Vec<u8>,
}

impl From<&AlterTableSchemaCommand> for AlterTableSchemaV1 {
    fn from(command: &AlterTableSchemaCommand) -> Self {
        let AlterTableSchemaCommand {
            table_id,
            expected_schema_version,
            schema_ipc,
        } = command;
        Self {
            table_id: table_id.to_string(),
            expected_schema_version: *expected_schema_version,
            schema_ipc: schema_ipc.as_bytes().to_vec(),
        }
    }
}

impl AlterTableSchemaV1 {
    fn into_domain(self) -> Result<AlterTableSchemaCommand, String> {
        Ok(AlterTableSchemaCommand {
            table_id: parse(self.table_id, "table_id")?,
            expected_schema_version: self.expected_schema_version,
            schema_ipc: crate::table::SchemaIpcPayload::new(self.schema_ipc).map_err(invalid)?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcquireProducerV1 {
    table_id: String,
    producer_id: String,
    expected_writer_epoch: Option<u64>,
}

impl From<&AcquireProducerCommand> for AcquireProducerV1 {
    fn from(command: &AcquireProducerCommand) -> Self {
        let AcquireProducerCommand {
            table_id,
            producer_id,
            expected_writer_epoch,
        } = command;
        Self {
            table_id: table_id.to_string(),
            producer_id: producer_id.to_string(),
            expected_writer_epoch: *expected_writer_epoch,
        }
    }
}

impl AcquireProducerV1 {
    fn into_domain(self) -> Result<AcquireProducerCommand, String> {
        Ok(AcquireProducerCommand {
            table_id: parse(self.table_id, "table_id")?,
            producer_id: parse(self.producer_id, "producer_id")?,
            expected_writer_epoch: self.expected_writer_epoch,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitDataFilesV1 {
    table_id: String,
    expected_snapshot_version: u64,
    files: Vec<PendingDataFileV1>,
    committed_log_offset: OffsetWatermarkV1,
}

impl From<&CommitDataFilesCommand> for CommitDataFilesV1 {
    fn from(command: &CommitDataFilesCommand) -> Self {
        let CommitDataFilesCommand {
            table_id,
            expected_snapshot_version,
            files,
            committed_log_offset,
        } = command;
        Self {
            table_id: table_id.to_string(),
            expected_snapshot_version: *expected_snapshot_version,
            files: files.iter().map(Into::into).collect(),
            committed_log_offset: (*committed_log_offset).into(),
        }
    }
}

impl CommitDataFilesV1 {
    fn into_domain(self) -> Result<CommitDataFilesCommand, String> {
        Ok(CommitDataFilesCommand {
            table_id: parse(self.table_id, "table_id")?,
            expected_snapshot_version: self.expected_snapshot_version,
            files: self
                .files
                .into_iter()
                .map(PendingDataFileV1::into_domain)
                .collect::<Result<_, _>>()?,
            committed_log_offset: self.committed_log_offset.into_domain(),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitCompactionV1 {
    table_id: String,
    expected_snapshot_version: u64,
    input_file_ids: Vec<String>,
    output_files: Vec<PendingDataFileV1>,
    committed_log_offset: OffsetWatermarkV1,
}

impl From<&CommitCompactionCommand> for CommitCompactionV1 {
    fn from(command: &CommitCompactionCommand) -> Self {
        let CommitCompactionCommand {
            table_id,
            expected_snapshot_version,
            input_file_ids,
            output_files,
            committed_log_offset,
        } = command;
        Self {
            table_id: table_id.to_string(),
            expected_snapshot_version: *expected_snapshot_version,
            input_file_ids: input_file_ids.iter().map(ToString::to_string).collect(),
            output_files: output_files.iter().map(Into::into).collect(),
            committed_log_offset: (*committed_log_offset).into(),
        }
    }
}

impl CommitCompactionV1 {
    fn into_domain(self) -> Result<CommitCompactionCommand, String> {
        Ok(CommitCompactionCommand {
            table_id: parse(self.table_id, "table_id")?,
            expected_snapshot_version: self.expected_snapshot_version,
            input_file_ids: self
                .input_file_ids
                .into_iter()
                .map(|id| parse(id, "input_file_id"))
                .collect::<Result<_, _>>()?,
            output_files: self
                .output_files
                .into_iter()
                .map(PendingDataFileV1::into_domain)
                .collect::<Result<_, _>>()?,
            committed_log_offset: self.committed_log_offset.into_domain(),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitConsumerOffsetV1 {
    consumer_id: String,
    table_id: String,
    offset_authority: ConsumerOffsetAuthorityV1,
    committed_log_offset: CommittedOffsetV1,
}

impl From<&CommitConsumerOffsetCommand> for CommitConsumerOffsetV1 {
    fn from(command: &CommitConsumerOffsetCommand) -> Self {
        let CommitConsumerOffsetCommand {
            consumer_id,
            table_id,
            offset_authority,
            committed_log_offset,
        } = command;
        Self {
            consumer_id: consumer_id.to_string(),
            table_id: table_id.to_string(),
            offset_authority: (*offset_authority).into(),
            committed_log_offset: (*committed_log_offset).into(),
        }
    }
}

impl CommitConsumerOffsetV1 {
    fn into_domain(self) -> Result<CommitConsumerOffsetCommand, String> {
        Ok(CommitConsumerOffsetCommand {
            consumer_id: parse(self.consumer_id, "consumer_id")?,
            table_id: parse(self.table_id, "table_id")?,
            offset_authority: self.offset_authority.into_domain()?,
            committed_log_offset: self.committed_log_offset.into_domain(),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcquireSnapshotLeaseV1 {
    lease_id: String,
    table_id: String,
    snapshot_version: u64,
    owner: String,
    expires_at_ns: u64,
}

impl From<&AcquireSnapshotLeaseCommand> for AcquireSnapshotLeaseV1 {
    fn from(command: &AcquireSnapshotLeaseCommand) -> Self {
        let AcquireSnapshotLeaseCommand {
            lease_id,
            table_id,
            snapshot_version,
            owner,
            expires_at_ns,
        } = command;
        Self {
            lease_id: lease_id.to_string(),
            table_id: table_id.to_string(),
            snapshot_version: *snapshot_version,
            owner: owner.clone(),
            expires_at_ns: *expires_at_ns,
        }
    }
}

impl AcquireSnapshotLeaseV1 {
    fn into_domain(self) -> Result<AcquireSnapshotLeaseCommand, String> {
        Ok(AcquireSnapshotLeaseCommand {
            lease_id: parse(self.lease_id, "lease_id")?,
            table_id: parse(self.table_id, "table_id")?,
            snapshot_version: self.snapshot_version,
            owner: self.owner,
            expires_at_ns: self.expires_at_ns,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseSnapshotLeaseV1 {
    lease_id: String,
    owner: String,
}

impl From<&ReleaseSnapshotLeaseCommand> for ReleaseSnapshotLeaseV1 {
    fn from(command: &ReleaseSnapshotLeaseCommand) -> Self {
        let ReleaseSnapshotLeaseCommand { lease_id, owner } = command;
        Self {
            lease_id: lease_id.to_string(),
            owner: owner.clone(),
        }
    }
}

impl ReleaseSnapshotLeaseV1 {
    fn into_domain(self) -> Result<ReleaseSnapshotLeaseCommand, String> {
        Ok(ReleaseSnapshotLeaseCommand {
            lease_id: parse(self.lease_id, "lease_id")?,
            owner: self.owner,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateMaintenanceJobV1 {
    job_id: String,
    kind: String,
    table_id: Option<String>,
    priority: i32,
    input: BTreeMap<String, String>,
}

impl From<&CreateMaintenanceJobCommand> for CreateMaintenanceJobV1 {
    fn from(command: &CreateMaintenanceJobCommand) -> Self {
        let CreateMaintenanceJobCommand {
            job_id,
            kind,
            table_id,
            priority,
            input,
        } = command;
        Self {
            job_id: job_id.to_string(),
            kind: maintenance_kind(*kind).to_owned(),
            table_id: table_id.map(|value| value.to_string()),
            priority: *priority,
            input: input.clone(),
        }
    }
}

impl CreateMaintenanceJobV1 {
    fn into_domain(self) -> Result<CreateMaintenanceJobCommand, String> {
        Ok(CreateMaintenanceJobCommand {
            job_id: parse(self.job_id, "job_id")?,
            kind: parse_maintenance_kind(&self.kind)?,
            table_id: self.table_id.map(|id| parse(id, "table_id")).transpose()?,
            priority: self.priority,
            input: self.input,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransitionMaintenanceJobV1 {
    job_id: String,
    expected_state: String,
    next_state: String,
    lease_owner: Option<String>,
    lease_expires_at_ns: Option<u64>,
    progress: BTreeMap<String, String>,
    error: Option<String>,
}

impl From<&TransitionMaintenanceJobCommand> for TransitionMaintenanceJobV1 {
    fn from(command: &TransitionMaintenanceJobCommand) -> Self {
        let TransitionMaintenanceJobCommand {
            job_id,
            expected_state,
            next_state,
            lease_owner,
            lease_expires_at_ns,
            progress,
            error,
        } = command;
        Self {
            job_id: job_id.to_string(),
            expected_state: maintenance_state(*expected_state).to_owned(),
            next_state: maintenance_state(*next_state).to_owned(),
            lease_owner: lease_owner.clone(),
            lease_expires_at_ns: *lease_expires_at_ns,
            progress: progress.clone(),
            error: error.clone(),
        }
    }
}

impl TransitionMaintenanceJobV1 {
    fn into_domain(self) -> Result<TransitionMaintenanceJobCommand, String> {
        Ok(TransitionMaintenanceJobCommand {
            job_id: parse(self.job_id, "job_id")?,
            expected_state: parse_maintenance_state(&self.expected_state)?,
            next_state: parse_maintenance_state(&self.next_state)?,
            lease_owner: self.lease_owner,
            lease_expires_at_ns: self.lease_expires_at_ns,
            progress: self.progress,
            error: self.error,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DropTableV1 {
    table_id: String,
    expected_snapshot_version: u64,
}

impl From<&DropTableCommand> for DropTableV1 {
    fn from(command: &DropTableCommand) -> Self {
        let DropTableCommand {
            table_id,
            expected_snapshot_version,
        } = command;
        Self {
            table_id: table_id.to_string(),
            expected_snapshot_version: *expected_snapshot_version,
        }
    }
}

impl DropTableV1 {
    fn into_domain(self) -> Result<DropTableCommand, String> {
        Ok(DropTableCommand {
            table_id: parse(self.table_id, "table_id")?,
            expected_snapshot_version: self.expected_snapshot_version,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResponseEnvelopeV1 {
    format_version: u32,
    response: ResponseV1,
}

impl From<&CatalogResponseV1> for ResponseEnvelopeV1 {
    fn from(response: &CatalogResponseV1) -> Self {
        Self {
            format_version: CATALOG_COMMAND_FORMAT_VERSION_V1,
            response: response.into(),
        }
    }
}

impl ResponseEnvelopeV1 {
    pub(super) fn into_domain(self) -> Result<CatalogResponseV1, String> {
        if self.format_version != CATALOG_COMMAND_FORMAT_VERSION_V1 {
            return Err(format!(
                "invalid frozen V1 response version actual=[{}]",
                self.format_version
            ));
        }
        self.response.into_domain()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseV1 {
    request_id: String,
    catalog_revision: u64,
    outcome: OutcomeV1,
}

impl From<&CatalogResponseV1> for ResponseV1 {
    fn from(response: &CatalogResponseV1) -> Self {
        let CatalogResponseV1 {
            request_id,
            catalog_revision,
            outcome,
        } = response;
        Self {
            request_id: request_id.to_string(),
            catalog_revision: *catalog_revision,
            outcome: outcome.into(),
        }
    }
}

impl ResponseV1 {
    fn into_domain(self) -> Result<CatalogResponseV1, String> {
        Ok(CatalogResponseV1 {
            request_id: parse(self.request_id, "request_id")?,
            catalog_revision: self.catalog_revision,
            outcome: self.outcome.into_domain()?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum OutcomeV1 {
    Namespace(NamespaceDescriptorV1),
    Table(TableDescriptorV1),
    Schema(SchemaDescriptorV1),
    Producer(ProducerDescriptorV1),
    Snapshot(TableSnapshotV1),
    ConsumerOffset(ConsumerOffsetOutcomeV1),
    SnapshotLease(SnapshotLeaseDescriptorV1),
    SnapshotLeaseReleased(SnapshotLeaseReleasedV1),
    MaintenanceJob(MaintenanceJobDescriptorV1),
    TableDropped(TableDroppedV1),
}

impl From<&CatalogOutcomeV1> for OutcomeV1 {
    fn from(outcome: &CatalogOutcomeV1) -> Self {
        match outcome {
            CatalogOutcomeV1::Namespace(descriptor) => Self::Namespace(descriptor.into()),
            CatalogOutcomeV1::Table(descriptor) => Self::Table(descriptor.into()),
            CatalogOutcomeV1::Schema(descriptor) => Self::Schema(descriptor.into()),
            CatalogOutcomeV1::Producer(descriptor) => Self::Producer(descriptor.into()),
            CatalogOutcomeV1::Snapshot(descriptor) => Self::Snapshot(descriptor.into()),
            CatalogOutcomeV1::ConsumerOffset {
                offset_authority,
                committed_log_offset,
            } => Self::ConsumerOffset(ConsumerOffsetOutcomeV1 {
                offset_authority: (*offset_authority).into(),
                committed_log_offset: (*committed_log_offset).into(),
            }),
            CatalogOutcomeV1::SnapshotLease(descriptor) => Self::SnapshotLease(descriptor.into()),
            CatalogOutcomeV1::SnapshotLeaseReleased { lease_id } => {
                Self::SnapshotLeaseReleased(SnapshotLeaseReleasedV1 {
                    lease_id: lease_id.to_string(),
                })
            }
            CatalogOutcomeV1::MaintenanceJob(descriptor) => Self::MaintenanceJob(descriptor.into()),
            CatalogOutcomeV1::TableDropped { table_id } => Self::TableDropped(TableDroppedV1 {
                table_id: table_id.to_string(),
            }),
        }
    }
}

impl OutcomeV1 {
    fn into_domain(self) -> Result<CatalogOutcomeV1, String> {
        Ok(match self {
            Self::Namespace(descriptor) => CatalogOutcomeV1::Namespace(descriptor.into_domain()?),
            Self::Table(descriptor) => CatalogOutcomeV1::Table(descriptor.into_domain()?),
            Self::Schema(descriptor) => CatalogOutcomeV1::Schema(descriptor.into_domain()?),
            Self::Producer(descriptor) => CatalogOutcomeV1::Producer(descriptor.into_domain()?),
            Self::Snapshot(descriptor) => CatalogOutcomeV1::Snapshot(descriptor.into_domain()?),
            Self::ConsumerOffset(outcome) => CatalogOutcomeV1::ConsumerOffset {
                offset_authority: outcome.offset_authority.into_domain()?,
                committed_log_offset: outcome.committed_log_offset.into_domain(),
            },
            Self::SnapshotLease(descriptor) => {
                CatalogOutcomeV1::SnapshotLease(descriptor.into_domain()?)
            }
            Self::SnapshotLeaseReleased(outcome) => CatalogOutcomeV1::SnapshotLeaseReleased {
                lease_id: parse(outcome.lease_id, "lease_id")?,
            },
            Self::MaintenanceJob(descriptor) => {
                CatalogOutcomeV1::MaintenanceJob(descriptor.into_domain()?)
            }
            Self::TableDropped(outcome) => CatalogOutcomeV1::TableDropped {
                table_id: parse(outcome.table_id, "table_id")?,
            },
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NamespaceDescriptorV1 {
    namespace_id: String,
    name: String,
    created_at_ns: u64,
}

impl From<&NamespaceDescriptor> for NamespaceDescriptorV1 {
    fn from(descriptor: &NamespaceDescriptor) -> Self {
        let NamespaceDescriptor {
            namespace_id,
            name,
            created_at_ns,
        } = descriptor;
        Self {
            namespace_id: namespace_id.to_string(),
            name: name.as_str().to_owned(),
            created_at_ns: *created_at_ns,
        }
    }
}

impl NamespaceDescriptorV1 {
    fn into_domain(self) -> Result<NamespaceDescriptor, String> {
        Ok(NamespaceDescriptor {
            namespace_id: parse(self.namespace_id, "namespace_id")?,
            name: crate::table::CatalogName::new(self.name).map_err(invalid)?,
            created_at_ns: self.created_at_ns,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableDescriptorV1 {
    table_id: String,
    namespace_id: String,
    name: String,
    definition: TableDefinitionV1,
    current_schema_version: u64,
    current_snapshot_version: u64,
    writer_epoch: u64,
    state: String,
    config: BTreeMap<String, String>,
    created_at_ns: u64,
    updated_at_ns: u64,
}

impl From<&TableDescriptor> for TableDescriptorV1 {
    fn from(descriptor: &TableDescriptor) -> Self {
        let TableDescriptor {
            table_id,
            namespace_id,
            name,
            definition,
            current_schema_version,
            current_snapshot_version,
            writer_epoch,
            state,
            config,
            created_at_ns,
            updated_at_ns,
        } = descriptor;
        Self {
            table_id: table_id.to_string(),
            namespace_id: namespace_id.to_string(),
            name: name.as_str().to_owned(),
            definition: definition.into(),
            current_schema_version: *current_schema_version,
            current_snapshot_version: *current_snapshot_version,
            writer_epoch: *writer_epoch,
            state: table_state(*state).to_owned(),
            config: config.clone(),
            created_at_ns: *created_at_ns,
            updated_at_ns: *updated_at_ns,
        }
    }
}

impl TableDescriptorV1 {
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
struct SchemaDescriptorV1 {
    table_id: String,
    schema_version: u64,
    schema_ipc: Vec<u8>,
    schema_hash: String,
    compatibility: String,
    created_at_ns: u64,
}

impl From<&SchemaDescriptor> for SchemaDescriptorV1 {
    fn from(descriptor: &SchemaDescriptor) -> Self {
        let SchemaDescriptor {
            table_id,
            schema_version,
            schema_ipc,
            schema_hash,
            compatibility,
            created_at_ns,
        } = descriptor;
        Self {
            table_id: table_id.to_string(),
            schema_version: *schema_version,
            schema_ipc: schema_ipc.as_bytes().to_vec(),
            schema_hash: schema_hash.clone(),
            compatibility: schema_compatibility(*compatibility).to_owned(),
            created_at_ns: *created_at_ns,
        }
    }
}

impl SchemaDescriptorV1 {
    fn into_domain(self) -> Result<SchemaDescriptor, String> {
        Ok(SchemaDescriptor {
            table_id: parse(self.table_id, "table_id")?,
            schema_version: self.schema_version,
            schema_ipc: crate::table::SchemaIpcPayload::new(self.schema_ipc).map_err(invalid)?,
            schema_hash: self.schema_hash,
            compatibility: parse_schema_compatibility(&self.compatibility)?,
            created_at_ns: self.created_at_ns,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProducerDescriptorV1 {
    producer_id: String,
    table_id: String,
    producer_epoch: u64,
    last_batch_sequence: SequenceWatermarkV1,
    last_committed_log_offset: OffsetWatermarkV1,
}

impl From<&ProducerDescriptor> for ProducerDescriptorV1 {
    fn from(descriptor: &ProducerDescriptor) -> Self {
        let ProducerDescriptor {
            producer_id,
            table_id,
            producer_epoch,
            last_batch_sequence,
            last_committed_log_offset,
        } = descriptor;
        Self {
            producer_id: producer_id.to_string(),
            table_id: table_id.to_string(),
            producer_epoch: *producer_epoch,
            last_batch_sequence: (*last_batch_sequence).into(),
            last_committed_log_offset: (*last_committed_log_offset).into(),
        }
    }
}

impl ProducerDescriptorV1 {
    fn into_domain(self) -> Result<ProducerDescriptor, String> {
        Ok(ProducerDescriptor {
            producer_id: parse(self.producer_id, "producer_id")?,
            table_id: parse(self.table_id, "table_id")?,
            producer_epoch: self.producer_epoch,
            last_batch_sequence: self.last_batch_sequence.into_domain(),
            last_committed_log_offset: self.last_committed_log_offset.into_domain(),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableSnapshotV1 {
    table_id: String,
    snapshot_version: u64,
    parent_snapshot_version: Option<u64>,
    operation: String,
    commit_lsn: u64,
    committed_log_offset: OffsetWatermarkV1,
    created_at_ns: u64,
    summary: BTreeMap<String, MetadataScalarV1>,
}

impl From<&TableSnapshot> for TableSnapshotV1 {
    fn from(descriptor: &TableSnapshot) -> Self {
        let TableSnapshot {
            table_id,
            snapshot_version,
            parent_snapshot_version,
            operation,
            commit_lsn,
            committed_log_offset,
            created_at_ns,
            summary,
        } = descriptor;
        Self {
            table_id: table_id.to_string(),
            snapshot_version: *snapshot_version,
            parent_snapshot_version: *parent_snapshot_version,
            operation: snapshot_operation(*operation).to_owned(),
            commit_lsn: *commit_lsn,
            committed_log_offset: (*committed_log_offset).into(),
            created_at_ns: *created_at_ns,
            summary: metadata_map(summary),
        }
    }
}

impl TableSnapshotV1 {
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
#[serde(deny_unknown_fields)]
struct ConsumerOffsetOutcomeV1 {
    offset_authority: ConsumerOffsetAuthorityV1,
    committed_log_offset: CommittedOffsetV1,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotLeaseDescriptorV1 {
    lease_id: String,
    table_id: String,
    snapshot_version: u64,
    owner: String,
    expires_at_ns: u64,
}

impl From<&SnapshotLeaseDescriptor> for SnapshotLeaseDescriptorV1 {
    fn from(descriptor: &SnapshotLeaseDescriptor) -> Self {
        let SnapshotLeaseDescriptor {
            lease_id,
            table_id,
            snapshot_version,
            owner,
            expires_at_ns,
        } = descriptor;
        Self {
            lease_id: lease_id.to_string(),
            table_id: table_id.to_string(),
            snapshot_version: *snapshot_version,
            owner: owner.clone(),
            expires_at_ns: *expires_at_ns,
        }
    }
}

impl SnapshotLeaseDescriptorV1 {
    fn into_domain(self) -> Result<SnapshotLeaseDescriptor, String> {
        Ok(SnapshotLeaseDescriptor {
            lease_id: parse(self.lease_id, "lease_id")?,
            table_id: parse(self.table_id, "table_id")?,
            snapshot_version: self.snapshot_version,
            owner: self.owner,
            expires_at_ns: self.expires_at_ns,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotLeaseReleasedV1 {
    lease_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceJobDescriptorV1 {
    job_id: String,
    kind: String,
    table_id: Option<String>,
    state: String,
    priority: i32,
    attempt: u32,
    lease_owner: Option<String>,
    lease_expires_at_ns: Option<u64>,
    input: BTreeMap<String, String>,
    progress: BTreeMap<String, String>,
    error: Option<String>,
    created_at_ns: u64,
    updated_at_ns: u64,
}

impl From<&MaintenanceJobDescriptor> for MaintenanceJobDescriptorV1 {
    fn from(descriptor: &MaintenanceJobDescriptor) -> Self {
        let MaintenanceJobDescriptor {
            job_id,
            kind,
            table_id,
            state,
            priority,
            attempt,
            lease_owner,
            lease_expires_at_ns,
            input,
            progress,
            error,
            created_at_ns,
            updated_at_ns,
        } = descriptor;
        Self {
            job_id: job_id.to_string(),
            kind: maintenance_kind(*kind).to_owned(),
            table_id: table_id.map(|value| value.to_string()),
            state: maintenance_state(*state).to_owned(),
            priority: *priority,
            attempt: *attempt,
            lease_owner: lease_owner.clone(),
            lease_expires_at_ns: *lease_expires_at_ns,
            input: input.clone(),
            progress: progress.clone(),
            error: error.clone(),
            created_at_ns: *created_at_ns,
            updated_at_ns: *updated_at_ns,
        }
    }
}

impl MaintenanceJobDescriptorV1 {
    fn into_domain(self) -> Result<MaintenanceJobDescriptor, String> {
        Ok(MaintenanceJobDescriptor {
            job_id: parse(self.job_id, "job_id")?,
            kind: parse_maintenance_kind(&self.kind)?,
            table_id: self.table_id.map(|id| parse(id, "table_id")).transpose()?,
            state: parse_maintenance_state(&self.state)?,
            priority: self.priority,
            attempt: self.attempt,
            lease_owner: self.lease_owner,
            lease_expires_at_ns: self.lease_expires_at_ns,
            input: self.input,
            progress: self.progress,
            error: self.error,
            created_at_ns: self.created_at_ns,
            updated_at_ns: self.updated_at_ns,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableDroppedV1 {
    table_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "definition", deny_unknown_fields)]
enum TableDefinitionV1 {
    #[serde(rename = "append_table")]
    Append(AppendDefinitionV1),
    #[serde(rename = "frame_table")]
    Frame(FrameDefinitionV1),
    #[serde(rename = "key_value_table")]
    KeyValue(KeyValueDefinitionV1),
}

impl From<&TableDefinition> for TableDefinitionV1 {
    fn from(definition: &TableDefinition) -> Self {
        match definition {
            TableDefinition::Append(definition) => Self::Append(definition.into()),
            TableDefinition::Frame(definition) => Self::Frame(definition.into()),
            TableDefinition::KeyValue(definition) => Self::KeyValue(definition.into()),
        }
    }
}

impl TableDefinitionV1 {
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
struct AppendDefinitionV1 {
    event_time_column: Option<String>,
}

impl From<&AppendTableDefinition> for AppendDefinitionV1 {
    fn from(definition: &AppendTableDefinition) -> Self {
        let AppendTableDefinition { event_time_column } = definition;
        Self {
            event_time_column: event_time_column
                .as_ref()
                .map(|column| column.as_str().to_owned()),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrameDefinitionV1 {
    by: String,
}

impl From<&FrameTableDefinition> for FrameDefinitionV1 {
    fn from(definition: &FrameTableDefinition) -> Self {
        let FrameTableDefinition { by } = definition;
        Self {
            by: by.as_str().to_owned(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyValueDefinitionV1 {
    keys: Vec<String>,
}

impl From<&KeyValueTableDefinition> for KeyValueDefinitionV1 {
    fn from(definition: &KeyValueTableDefinition) -> Self {
        Self {
            keys: definition
                .keys()
                .as_slice()
                .iter()
                .map(|column| column.as_str().to_owned())
                .collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingDataFileV1 {
    file_id: String,
    table_id: String,
    schema_version: u64,
    location: DataFileLocationV1,
    file_size_bytes: u64,
    row_count: u64,
    provenance: DataFileProvenanceV1,
    partition: BTreeMap<String, MetadataScalarV1>,
    statistics: BTreeMap<String, MetadataScalarV1>,
    metadata: BTreeMap<String, MetadataScalarV1>,
    checksum: String,
    created_at_ns: u64,
}

impl From<&PendingDataFile> for PendingDataFileV1 {
    fn from(file: &PendingDataFile) -> Self {
        let PendingDataFile {
            file_id,
            table_id,
            schema_version,
            location,
            file_size_bytes,
            row_count,
            provenance,
            partition,
            statistics,
            metadata,
            checksum,
            created_at_ns,
        } = file;
        Self {
            file_id: file_id.to_string(),
            table_id: table_id.to_string(),
            schema_version: *schema_version,
            location: location.into(),
            file_size_bytes: *file_size_bytes,
            row_count: row_count.get(),
            provenance: provenance.into(),
            partition: metadata_map(partition),
            statistics: metadata_map(statistics),
            metadata: metadata_map(metadata),
            checksum: checksum.clone(),
            created_at_ns: *created_at_ns,
        }
    }
}

impl PendingDataFileV1 {
    fn into_domain(self) -> Result<PendingDataFile, String> {
        Ok(PendingDataFile {
            file_id: parse(self.file_id, "file_id")?,
            table_id: parse(self.table_id, "table_id")?,
            schema_version: self.schema_version,
            location: self.location.into_domain()?,
            file_size_bytes: self.file_size_bytes,
            row_count: NonZeroU64::new(self.row_count)
                .ok_or_else(|| "invalid V1 zero row_count".to_owned())?,
            provenance: self.provenance.into_domain()?,
            partition: metadata_map_into_domain(self.partition)?,
            statistics: metadata_map_into_domain(self.statistics)?,
            metadata: metadata_map_into_domain(self.metadata)?,
            checksum: self.checksum,
            created_at_ns: self.created_at_ns,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "path",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum DataFileLocationV1 {
    Managed(String),
    ExternalLegacy(String),
}

impl From<&DataFileLocation> for DataFileLocationV1 {
    fn from(location: &DataFileLocation) -> Self {
        match location {
            DataFileLocation::Managed(path) => Self::Managed(path.as_str().to_owned()),
            DataFileLocation::ExternalLegacy(path) => {
                Self::ExternalLegacy(path.as_str().to_owned())
            }
        }
    }
}

impl DataFileLocationV1 {
    fn into_domain(self) -> Result<DataFileLocation, String> {
        match self {
            Self::Managed(path) => Ok(DataFileLocation::Managed(
                crate::table::ManagedDataPath::new(path).map_err(invalid)?,
            )),
            Self::ExternalLegacy(path) => Ok(DataFileLocation::ExternalLegacy(
                crate::table::ExternalDataPath::new(path).map_err(invalid)?,
            )),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum DataFileProvenanceV1 {
    Native(NativeDataFileProvenanceV1),
    Legacy {
        offsets: Option<OffsetRangeV1>,
        preserve_discovery_order: bool,
    },
}

impl From<&DataFileProvenance> for DataFileProvenanceV1 {
    fn from(provenance: &DataFileProvenance) -> Self {
        match provenance {
            DataFileProvenance::Native(native) => Self::Native(NativeDataFileProvenanceV1 {
                offsets: native.offsets().into(),
                source_lineage: native
                    .source_lineage()
                    .as_slice()
                    .iter()
                    .map(Into::into)
                    .collect(),
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

impl DataFileProvenanceV1 {
    fn into_domain(self) -> Result<DataFileProvenance, String> {
        match self {
            Self::Native(native) => {
                let lineage = native
                    .source_lineage
                    .into_iter()
                    .map(SourceSegmentLineageV1::into_domain)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DataFileProvenance::Native(
                    NativeDataFileProvenance::new(
                        native.offsets.into_domain()?,
                        SourceSegmentLineages::new(lineage).map_err(invalid)?,
                    )
                    .map_err(invalid)?,
                ))
            }
            Self::Legacy {
                offsets,
                preserve_discovery_order,
            } => Ok(DataFileProvenance::Legacy {
                offsets: offsets.map(OffsetRangeV1::into_domain).transpose()?,
                preserve_discovery_order,
            }),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeDataFileProvenanceV1 {
    offsets: OffsetRangeV1,
    source_lineage: Vec<SourceSegmentLineageV1>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceSegmentLineageV1 {
    source: SourceSegmentIdentityV1,
    offsets: OffsetRangeV1,
}

impl From<&SourceSegmentLineage> for SourceSegmentLineageV1 {
    fn from(lineage: &SourceSegmentLineage) -> Self {
        let SourceSegmentLineage { source, offsets } = lineage;
        Self {
            source: (*source).into(),
            offsets: (*offsets).into(),
        }
    }
}

impl SourceSegmentLineageV1 {
    fn into_domain(self) -> Result<SourceSegmentLineage, String> {
        Ok(SourceSegmentLineage {
            source: self.source.into_domain(),
            offsets: self.offsets.into_domain()?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceSegmentIdentityV1 {
    segment_id: u64,
    generation: u64,
    writer_epoch: u64,
}

impl From<SourceSegmentIdentity> for SourceSegmentIdentityV1 {
    fn from(identity: SourceSegmentIdentity) -> Self {
        let SourceSegmentIdentity {
            segment_id,
            generation,
            writer_epoch,
        } = identity;
        Self {
            segment_id,
            generation,
            writer_epoch,
        }
    }
}

impl SourceSegmentIdentityV1 {
    fn into_domain(self) -> SourceSegmentIdentity {
        SourceSegmentIdentity {
            segment_id: self.segment_id,
            generation: self.generation,
            writer_epoch: self.writer_epoch,
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
enum MetadataScalarV1 {
    Null,
    Boolean(bool),
    Int64(i64),
    #[serde(rename = "uint64")]
    UInt64(u64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl From<&MetadataScalar> for MetadataScalarV1 {
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

impl MetadataScalarV1 {
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

fn metadata_map(values: &MetadataMap) -> BTreeMap<String, MetadataScalarV1> {
    values
        .iter()
        .map(|(key, value)| (key.clone(), value.into()))
        .collect()
}

fn metadata_map_into_domain(
    values: BTreeMap<String, MetadataScalarV1>,
) -> Result<MetadataMap, String> {
    values
        .into_iter()
        .map(|(key, value)| Ok((key, value.into_domain()?)))
        .collect()
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OffsetRangeV1 {
    first: u64,
    last: u64,
}

impl From<OffsetRange> for OffsetRangeV1 {
    fn from(range: OffsetRange) -> Self {
        Self {
            first: range.first().get(),
            last: range.last().get(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "sequence",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum SequenceWatermarkV1 {
    Empty,
    Known(u64),
}

impl From<SequenceWatermark> for SequenceWatermarkV1 {
    fn from(sequence: SequenceWatermark) -> Self {
        match sequence {
            SequenceWatermark::Empty => Self::Empty,
            SequenceWatermark::Known(sequence) => Self::Known(sequence),
        }
    }
}

impl SequenceWatermarkV1 {
    fn into_domain(self) -> SequenceWatermark {
        match self {
            Self::Empty => SequenceWatermark::Empty,
            Self::Known(sequence) => SequenceWatermark::Known(sequence),
        }
    }
}

impl OffsetRangeV1 {
    fn into_domain(self) -> Result<OffsetRange, String> {
        OffsetRange::new(
            crate::LogOffset::new(self.first),
            crate::LogOffset::new(self.last),
        )
        .map_err(invalid)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "offset",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum OffsetWatermarkV1 {
    Empty,
    Known(u64),
    LegacyUnknown,
}

impl From<OffsetWatermark> for OffsetWatermarkV1 {
    fn from(offset: OffsetWatermark) -> Self {
        match offset {
            OffsetWatermark::Empty => Self::Empty,
            OffsetWatermark::Known(offset) => Self::Known(offset.get()),
            OffsetWatermark::LegacyUnknown => Self::LegacyUnknown,
        }
    }
}

impl OffsetWatermarkV1 {
    fn into_domain(self) -> OffsetWatermark {
        match self {
            Self::Empty => OffsetWatermark::Empty,
            Self::Known(offset) => OffsetWatermark::Known(crate::LogOffset::new(offset)),
            Self::LegacyUnknown => OffsetWatermark::LegacyUnknown,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "offset",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum CommittedOffsetV1 {
    Empty,
    Known(u64),
}

impl From<CommittedOffset> for CommittedOffsetV1 {
    fn from(offset: CommittedOffset) -> Self {
        match offset {
            CommittedOffset::Empty => Self::Empty,
            CommittedOffset::Known(offset) => Self::Known(offset.get()),
        }
    }
}

impl CommittedOffsetV1 {
    fn into_domain(self) -> CommittedOffset {
        match self {
            Self::Empty => CommittedOffset::Empty,
            Self::Known(offset) => CommittedOffset::Known(crate::LogOffset::new(offset)),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "id",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum ConsumerOffsetAuthorityV1 {
    Consumer(String),
    Group(String),
}

impl From<ConsumerOffsetAuthority> for ConsumerOffsetAuthorityV1 {
    fn from(authority: ConsumerOffsetAuthority) -> Self {
        match authority {
            ConsumerOffsetAuthority::Consumer(id) => Self::Consumer(id.to_string()),
            ConsumerOffsetAuthority::Group(id) => Self::Group(id.to_string()),
        }
    }
}

impl ConsumerOffsetAuthorityV1 {
    fn into_domain(self) -> Result<ConsumerOffsetAuthority, String> {
        match self {
            Self::Consumer(id) => Ok(ConsumerOffsetAuthority::Consumer(parse(id, "consumer_id")?)),
            Self::Group(id) => Ok(ConsumerOffsetAuthority::Group(parse(
                id,
                "consumer_group_id",
            )?)),
        }
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
        _ => Err(format!("invalid V1 table state value=[{value}]")),
    }
}

fn schema_compatibility(compatibility: SchemaCompatibility) -> &'static str {
    match compatibility {
        SchemaCompatibility::Initial => "initial",
        SchemaCompatibility::Full => "full",
        SchemaCompatibility::Backward => "backward",
        SchemaCompatibility::Forward => "forward",
        SchemaCompatibility::Breaking => "breaking",
    }
}

fn parse_schema_compatibility(value: &str) -> Result<SchemaCompatibility, String> {
    match value {
        "initial" => Ok(SchemaCompatibility::Initial),
        "full" => Ok(SchemaCompatibility::Full),
        "backward" => Ok(SchemaCompatibility::Backward),
        "forward" => Ok(SchemaCompatibility::Forward),
        "breaking" => Ok(SchemaCompatibility::Breaking),
        _ => Err(format!("invalid V1 schema compatibility value=[{value}]")),
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
        _ => Err(format!("invalid V1 snapshot operation value=[{value}]")),
    }
}

fn maintenance_kind(kind: MaintenanceJobKind) -> &'static str {
    match kind {
        MaintenanceJobKind::Compaction => "compaction",
        MaintenanceJobKind::Retention => "retention",
        MaintenanceJobKind::GarbageCollection => "garbage_collection",
        MaintenanceJobKind::Verify => "verify",
        MaintenanceJobKind::Backup => "backup",
        MaintenanceJobKind::Import => "import",
    }
}

fn parse_maintenance_kind(value: &str) -> Result<MaintenanceJobKind, String> {
    match value {
        "compaction" => Ok(MaintenanceJobKind::Compaction),
        "retention" => Ok(MaintenanceJobKind::Retention),
        "garbage_collection" => Ok(MaintenanceJobKind::GarbageCollection),
        "verify" => Ok(MaintenanceJobKind::Verify),
        "backup" => Ok(MaintenanceJobKind::Backup),
        "import" => Ok(MaintenanceJobKind::Import),
        _ => Err(format!("invalid V1 maintenance kind value=[{value}]")),
    }
}

fn maintenance_state(state: MaintenanceJobState) -> &'static str {
    match state {
        MaintenanceJobState::Pending => "pending",
        MaintenanceJobState::Running => "running",
        MaintenanceJobState::Succeeded => "succeeded",
        MaintenanceJobState::Failed => "failed",
        MaintenanceJobState::Cancelled => "cancelled",
    }
}

fn parse_maintenance_state(value: &str) -> Result<MaintenanceJobState, String> {
    match value {
        "pending" => Ok(MaintenanceJobState::Pending),
        "running" => Ok(MaintenanceJobState::Running),
        "succeeded" => Ok(MaintenanceJobState::Succeeded),
        "failed" => Ok(MaintenanceJobState::Failed),
        "cancelled" => Ok(MaintenanceJobState::Cancelled),
        _ => Err(format!("invalid V1 maintenance state value=[{value}]")),
    }
}

fn parse<T>(value: String, field: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid V1 {field} error=[{error}]"))
}

fn invalid(error: impl Display) -> String {
    format!("invalid V1 field error=[{error}]")
}
