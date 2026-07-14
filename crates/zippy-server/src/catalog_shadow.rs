//! SQLite Catalog adapter for legacy Gateway persisted-read shadow comparisons.

use std::pin::Pin;

use zippy_catalog::{
    decode_persisted_schema, SqliteCatalogReadHandle, SqliteCatalogSnapshotReadLimits,
};
use zippy_core::{canonical_schema_hash, ZippyError};
use zippy_gateway::{
    PersistedReadShadowSnapshot, PersistedReadShadowSource, PersistedReadSnapshot,
};
use zippy_types::catalog::{CatalogRead, CatalogSnapshotSelector};
use zippy_types::table::{CatalogName, TableRef, TableState};
use zippy_types::{ErrorCode, ServerError};

pub(crate) const LEGACY_STREAM_NAME_CONFIG_KEY: &str = "zippy.migration.legacy_stream_name";

/// Resolves exact native Catalog snapshots for asynchronous Gateway comparisons.
pub(crate) struct SqliteCatalogPersistedReadShadowSource {
    catalog: SqliteCatalogReadHandle,
    read_limits: SqliteCatalogSnapshotReadLimits,
    max_encoded_metadata_bytes: usize,
}

impl SqliteCatalogPersistedReadShadowSource {
    pub(crate) fn new(
        catalog: SqliteCatalogReadHandle,
        max_files: usize,
        max_metadata_bytes: usize,
    ) -> zippy_core::Result<Self> {
        let read_limits = SqliteCatalogSnapshotReadLimits {
            max_visible_files: max_files,
            max_raw_response_bytes: max_metadata_bytes,
            ..SqliteCatalogSnapshotReadLimits::default()
        };
        read_limits.validate().map_err(map_catalog_error)?;
        Ok(Self {
            catalog,
            read_limits,
            max_encoded_metadata_bytes: max_metadata_bytes,
        })
    }
}

impl PersistedReadShadowSource for SqliteCatalogPersistedReadShadowSource {
    fn snapshot<'a>(
        &'a self,
        stream_name: &str,
        persisted_files_revision: u64,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = zippy_core::Result<PersistedReadShadowSnapshot>>
                + Send
                + 'a,
        >,
    > {
        let stream_name = stream_name.to_string();
        Box::pin(async move {
            let table_name = CatalogName::new(stream_name.clone()).map_err(|error| {
                ZippyError::InvalidConfig {
                    reason: format!(
                        "legacy stream name cannot be resolved as a Catalog table \
                         stream_name=[{}] error=[{}]",
                        stream_name, error
                    ),
                }
            })?;
            let table = self
                .catalog
                .get_table(TableRef::Name {
                    namespace: CatalogName::default_namespace(),
                    table: table_name,
                })
                .await
                .map_err(map_catalog_error)?;
            if table.state != TableState::Active {
                return Err(ZippyError::Conflict {
                    reason: format!(
                        "native shadow table is not active stream_name=[{}] state=[{:?}]",
                        stream_name, table.state
                    ),
                });
            }

            validate_legacy_stream_mapping(&stream_name, &table.config)?;
            let mapping = self
                .catalog
                .resolve_legacy_file_view_mapping(table.table_id, persisted_files_revision)
                .await
                .map_err(map_catalog_error)?;
            if mapping.table_id != table.table_id
                || mapping.legacy_files_revision != persisted_files_revision
            {
                return Err(ZippyError::DataCorruption {
                    reason: format!(
                        "native Catalog returned an inconsistent legacy mapping \
                         stream_name=[{}] requested_revision=[{}] mapped_table_id=[{}] \
                         mapped_revision=[{}]",
                        stream_name,
                        persisted_files_revision,
                        mapping.table_id,
                        mapping.legacy_files_revision
                    ),
                });
            }
            let native_snapshot_version = mapping.native_snapshot_version;
            if native_snapshot_version > table.current_snapshot_version {
                return Err(ZippyError::Conflict {
                    reason: format!(
                        "native shadow revision has not been committed stream_name=[{}] \
                         legacy_revision=[{}] native_snapshot=[{}] current_native_snapshot=[{}]",
                        stream_name,
                        persisted_files_revision,
                        native_snapshot_version,
                        table.current_snapshot_version
                    ),
                });
            }
            let selector = CatalogSnapshotSelector {
                table_id: table.table_id,
                snapshot_version: native_snapshot_version,
                schema_version: table.current_schema_version,
            };
            let query_snapshot = self
                .catalog
                .read_snapshot_at_with_limits(std::slice::from_ref(&selector), self.read_limits)
                .await
                .map_err(map_catalog_error)?;
            let native =
                query_snapshot
                    .tables()
                    .first()
                    .ok_or_else(|| ZippyError::DataCorruption {
                        reason: format!(
                            "native Catalog returned no table for exact shadow selector \
                         stream_name=[{}] native_snapshot=[{}]",
                            stream_name, native_snapshot_version
                        ),
                    })?;
            if query_snapshot.tables().len() != 1 || native.selector() != &selector {
                return Err(ZippyError::DataCorruption {
                    reason: format!(
                        "native Catalog returned an inconsistent shadow selector \
                         stream_name=[{}] native_snapshot=[{}]",
                        stream_name, native_snapshot_version
                    ),
                });
            }

            let schema = decode_persisted_schema(native.schema()).map_err(map_catalog_error)?;
            let legacy_schema_hash = canonical_schema_hash(&schema);
            let mut encoded_bytes = 2_usize;
            let mut persisted_files = Vec::with_capacity(native.files().as_slice().len());
            let mut ordered_files = native.files().as_slice().iter().collect::<Vec<_>>();
            ordered_files.sort_unstable_by(|left, right| {
                match (
                    left.legacy_discovery_order(),
                    right.legacy_discovery_order(),
                ) {
                    (Some(left_order), Some(right_order)) => left_order
                        .cmp(&right_order)
                        .then_with(|| left.file_id().cmp(&right.file_id())),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => left.file_id().cmp(&right.file_id()),
                }
            });
            for (index, file) in ordered_files.into_iter().enumerate() {
                if index % 32 == 0 {
                    tokio::task::yield_now().await;
                }
                let encoded =
                    serde_json::to_vec(file).map_err(|error| ZippyError::DataCorruption {
                        reason: format!(
                            "native Catalog file cannot be encoded for shadow comparison \
                             stream_name=[{}] file_id=[{}] error=[{}]",
                            stream_name,
                            file.file_id(),
                            error
                        ),
                    })?;
                encoded_bytes = encoded_bytes
                    .checked_add(encoded.len())
                    .and_then(|bytes| bytes.checked_add(usize::from(index != 0)))
                    .ok_or_else(|| ZippyError::ResourceExhausted {
                        reason: "native shadow metadata byte count overflow".to_string(),
                    })?;
                if encoded_bytes > self.max_encoded_metadata_bytes {
                    return Err(ZippyError::ResourceExhausted {
                        reason: format!(
                            "native shadow metadata exceeds per-comparison limit \
                             stream_name=[{}] actual=[{}] maximum=[{}]",
                            stream_name, encoded_bytes, self.max_encoded_metadata_bytes
                        ),
                    });
                }
                persisted_files.push(serde_json::from_slice(&encoded).map_err(|error| {
                    ZippyError::DataCorruption {
                        reason: format!(
                            "encoded native Catalog file cannot be normalized \
                             stream_name=[{}] file_id=[{}] error=[{}]",
                            stream_name,
                            file.file_id(),
                            error
                        ),
                    }
                })?);
            }
            Ok(PersistedReadShadowSnapshot::new(
                legacy_schema_hash,
                PersistedReadSnapshot::new(stream_name, persisted_files),
            ))
        })
    }
}

fn validate_legacy_stream_mapping(
    stream_name: &str,
    config: &std::collections::BTreeMap<String, String>,
) -> zippy_core::Result<()> {
    let mapped_stream = required_mapping(config, LEGACY_STREAM_NAME_CONFIG_KEY, stream_name)?;
    if mapped_stream != stream_name {
        return Err(ZippyError::Conflict {
            reason: format!(
                "native shadow mapping belongs to another legacy stream requested=[{}] \
                 mapped=[{}]",
                stream_name, mapped_stream
            ),
        });
    }
    Ok(())
}

fn required_mapping<'a>(
    config: &'a std::collections::BTreeMap<String, String>,
    key: &str,
    stream_name: &str,
) -> zippy_core::Result<&'a str> {
    config
        .get(key)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ZippyError::Conflict {
            reason: format!(
                "native table lacks an exact legacy revision mapping stream_name=[{}] key=[{}]",
                stream_name, key
            ),
        })
}

fn map_catalog_error(error: ServerError) -> ZippyError {
    let reason = format!(
        "native Catalog shadow read failed code=[{}] reason=[{}]",
        error.code(),
        error.message()
    );
    match error.code() {
        ErrorCode::InvalidArgument => ZippyError::InvalidConfig { reason },
        ErrorCode::NotFound => ZippyError::NotFound { reason },
        ErrorCode::AlreadyExists
        | ErrorCode::Conflict
        | ErrorCode::StaleProducerEpoch
        | ErrorCode::SequenceGap
        | ErrorCode::OffsetExpired
        | ErrorCode::UnsupportedPlan => ZippyError::Conflict { reason },
        ErrorCode::SchemaMismatch => ZippyError::SchemaMismatch { reason },
        ErrorCode::ResourceExhausted => ZippyError::ResourceExhausted { reason },
        ErrorCode::DeadlineExceeded => ZippyError::DeadlineExceeded { reason },
        ErrorCode::Cancelled => ZippyError::Cancelled { reason },
        ErrorCode::DataCorruption => ZippyError::DataCorruption { reason },
        ErrorCode::Unauthenticated
        | ErrorCode::PermissionDenied
        | ErrorCode::Unavailable
        | ErrorCode::Internal => ZippyError::Io { reason },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn mapping(stream_name: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(
            LEGACY_STREAM_NAME_CONFIG_KEY.to_string(),
            stream_name.to_string(),
        )])
    }

    #[test]
    fn exact_revision_mapping_requires_the_same_legacy_stream() {
        validate_legacy_stream_mapping("ticks", &mapping("ticks")).unwrap();
        assert!(validate_legacy_stream_mapping("ticks", &mapping("orders")).is_err());
    }

    #[test]
    fn exact_revision_mapping_rejects_missing_stream_identity() {
        let missing = BTreeMap::new();
        assert!(validate_legacy_stream_mapping("ticks", &missing).is_err());
    }
}
