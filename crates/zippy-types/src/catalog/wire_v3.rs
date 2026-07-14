//! Private V3 legacy file-view reconcile fingerprint mirror.

use serde::Serialize;

use super::wire_v2::{ActorRefV2, LegacyImportFilesRefV2};
use super::LegacyCatalogReconcileEnvelopeV3;

#[derive(Serialize)]
pub(super) struct FingerprintInputRefV3<'a> {
    format_version: u32,
    actor: ActorRefV2<'a>,
    source_snapshot_hash: &'a str,
    source_catalog_revision: u64,
    command: ReconcileCommandRefV3<'a>,
}

impl<'a> From<&'a LegacyCatalogReconcileEnvelopeV3> for FingerprintInputRefV3<'a> {
    fn from(envelope: &'a LegacyCatalogReconcileEnvelopeV3) -> Self {
        Self {
            format_version: envelope.format_version.get(),
            actor: (&envelope.actor).into(),
            source_snapshot_hash: envelope.source_snapshot_hash.as_str(),
            source_catalog_revision: envelope.source_catalog_revision,
            command: (&envelope.command).into(),
        }
    }
}

#[derive(Serialize)]
struct ReconcileCommandRefV3<'a> {
    table_id: String,
    expected_schema_version: u64,
    expected_schema_hash: &'a str,
    expected_writer_epoch: u64,
    writer_epoch: u64,
    expected_legacy_files_revision: u64,
    legacy_files_revision: u64,
    files: LegacyImportFilesRefV2<'a>,
}

impl<'a> From<&'a super::LegacyCatalogReconcileCommandV3> for ReconcileCommandRefV3<'a> {
    fn from(command: &'a super::LegacyCatalogReconcileCommandV3) -> Self {
        Self {
            table_id: command.table_id.to_string(),
            expected_schema_version: command.expected_schema_version,
            expected_schema_hash: command.expected_schema_hash.as_str(),
            expected_writer_epoch: command.expected_writer_epoch,
            writer_epoch: command.writer_epoch,
            expected_legacy_files_revision: command.expected_legacy_files_revision,
            legacy_files_revision: command.legacy_files_revision,
            files: LegacyImportFilesRefV2(&command.files),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::super::{
        CatalogActor, CatalogActorKind, CatalogCommandFormatVersion, CatalogFingerprintError,
        LegacyCatalogImportFileV2, LegacyCatalogReconcileCommandV3,
        LegacyCatalogReconcileEnvelopeV3, LEGACY_VERIFIED_SCHEMA_HASH_METADATA_KEY,
        MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
    };
    use crate::table::{
        DataFileLocation, DataFileProvenance, ExternalDataPath, MetadataMap, MetadataScalar,
        PendingDataFile,
    };
    use crate::{FileId, RequestId, TableId};

    fn envelope() -> LegacyCatalogReconcileEnvelopeV3 {
        let schema_hash = format!("sha256-v1:{}", "a".repeat(64));
        LegacyCatalogReconcileEnvelopeV3::new(
            RequestId::from_u128(3).unwrap(),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "zippy-server/legacy-reconcile-v3".to_owned(),
            },
            format!("sha256-v1:{}", "c".repeat(64)),
            7,
            LegacyCatalogReconcileCommandV3 {
                table_id: TableId::from_u128(1).unwrap(),
                expected_schema_version: 1,
                expected_schema_hash: schema_hash.clone(),
                expected_writer_epoch: 4,
                writer_epoch: 5,
                expected_legacy_files_revision: 6,
                legacy_files_revision: 8,
                files: vec![LegacyCatalogImportFileV2 {
                    file: PendingDataFile {
                        file_id: FileId::from_u128(2).unwrap(),
                        table_id: TableId::from_u128(1).unwrap(),
                        schema_version: 1,
                        location: DataFileLocation::ExternalLegacy(
                            ExternalDataPath::new("/srv/legacy/ticks-0001.parquet").unwrap(),
                        ),
                        file_size_bytes: 128,
                        row_count: NonZeroU64::new(9).unwrap(),
                        provenance: DataFileProvenance::Legacy {
                            offsets: None,
                            preserve_discovery_order: true,
                        },
                        partition: MetadataMap::new(),
                        statistics: MetadataMap::new(),
                        metadata: MetadataMap::from([(
                            LEGACY_VERIFIED_SCHEMA_HASH_METADATA_KEY.to_owned(),
                            MetadataScalar::String(schema_hash),
                        )]),
                        checksum: format!("sha256-v1:{}", "b".repeat(64)),
                        created_at_ns: 11,
                    },
                    discovery_order: 0,
                }],
            },
        )
        .unwrap()
    }

    #[test]
    fn reconcile_v3_fingerprint_has_a_frozen_golden() {
        let envelope = envelope();
        assert_eq!(
            envelope.canonical_fingerprint.to_string(),
            "307a0d662dde9293d19aff178ccdca0d69615e5048a6f608fd2d7e33a0ccc478"
        );
        assert!(envelope.verify_fingerprint().is_ok());
    }

    #[test]
    fn reconcile_v3_fingerprint_binds_revisions_epochs_and_file_order() {
        let original = envelope();
        let mut variants = Vec::new();
        let mut changed = original.clone();
        changed.source_catalog_revision += 1;
        variants.push(changed);
        let mut changed = original.clone();
        changed.command.expected_legacy_files_revision += 1;
        variants.push(changed);
        let mut changed = original.clone();
        changed.command.legacy_files_revision += 1;
        variants.push(changed);
        let mut changed = original.clone();
        changed.command.expected_writer_epoch += 1;
        variants.push(changed);
        let mut changed = original.clone();
        changed.command.writer_epoch += 1;
        variants.push(changed);
        let mut changed = original.clone();
        changed.command.files[0].discovery_order += 1;
        variants.push(changed);
        for changed in variants {
            assert!(matches!(
                changed.verify_fingerprint(),
                Err(CatalogFingerprintError::Mismatch)
            ));
        }
    }

    #[test]
    fn reconcile_v3_rejects_oversized_actor_and_wrong_format() {
        let mut command = envelope().command;
        command.files.clear();
        let oversized = LegacyCatalogReconcileEnvelopeV3::new(
            RequestId::from_u128(4).unwrap(),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "x".repeat(MAX_CATALOG_ACTOR_PRINCIPAL_BYTES + 1),
            },
            format!("sha256-v1:{}", "d".repeat(64)),
            1,
            command,
        )
        .unwrap_err();
        assert!(matches!(
            oversized,
            CatalogFingerprintError::ActorPrincipalTooLarge { .. }
        ));

        let mut wrong_format = envelope();
        wrong_format.format_version = CatalogCommandFormatVersion::new(2);
        assert!(matches!(
            wrong_format.verify_fingerprint(),
            Err(CatalogFingerprintError::UnsupportedFormatVersion { actual: 2 })
        ));
    }
}
