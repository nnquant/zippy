//! Server-private log windows that need no persisted file capabilities.

use std::any::Any;
use std::mem::size_of;
use std::num::NonZeroU64;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use zippy_ingest::TableActorLiveSegmentSnapshot;
use zippy_segment_store::{ColumnLayout, RowSpanView};
use zippy_types::catalog::CatalogRead;
use zippy_types::query::QueryTableVersion;
use zippy_types::schema_memory::conservative_arrow_schema_retained_bytes;
use zippy_types::storage::{
    CatalogSnapshotMemoryLease, CatalogSnapshotMemoryLeaseDescriptor, QueryReadSnapshotDescriptor,
    QueryReadSnapshotGuard, TableReadSnapshotDescriptor,
};
use zippy_types::table::{TableKind, TableRef};
use zippy_types::{BoxFuture, ErrorCode, LogOffset, OffsetRange, QueryId, Result, ServerError};

use super::{
    canonicalize_tables, conservative_query_snapshot_planning_peak_bytes, corruption,
    project_live_read_descriptors, validate_table_boundaries, NativeQuerySnapshotCoordinator,
    TableBoundary, MAX_SNAPSHOT_COORDINATION_ATTEMPTS,
};
use crate::native_log::{
    log_error, select_sources_after, NativeLogLocation, NativeLogPlan, NativeLogSource,
};
use crate::query_service::{QueryPlanDraft, QueryPlanningToken};

/// Owns fixed live spans and their query reservation, without a historical file manifest.
pub(crate) struct LiveLogSnapshot {
    descriptor: QueryReadSnapshotDescriptor,
    segments: Arc<[TableActorLiveSegmentSnapshot]>,
    // Release physical ownership and metadata before releasing its accounting reservation.
    memory: CatalogSnapshotMemoryLease,
}

impl NativeQuerySnapshotCoordinator {
    /// Selects the live-only path only when the cursor is beyond the captured durable prefix.
    pub(crate) async fn pin_live_log(
        &self,
        planning: &QueryPlanningToken,
        expected: QueryTableVersion,
        start: LogOffset,
        schema: &SchemaRef,
        kind: TableKind,
    ) -> Result<Option<(LiveLogSnapshot, Arc<NativeLogPlan>)>> {
        let query_id = planning.instance_id().query_id();
        self.ensure_planning_active(planning)?;
        let _admission = self.admit_planning(query_id)?;
        let mut cancellation = planning.cancellation();
        for attempt in 0..MAX_SNAPSHOT_COORDINATION_ATTEMPTS {
            self.ensure_planning_active(planning)?;
            self.ensure_accepting(query_id)?;
            let outcome = tokio::select! {
                biased;
                terminal = cancellation.wait_terminal() => {
                    return Err(terminal?.error.unwrap_or_else(|| {
                        ServerError::new(ErrorCode::Cancelled, "query planning was cancelled")
                            .with_query_id(query_id)
                            .with_retryable(false)
                    }));
                }
                outcome = self.pin_live_log_once(planning, expected, start, schema, kind) => outcome,
            };
            self.ensure_planning_active(planning)?;
            match outcome {
                Err(error)
                    if error.code() == ErrorCode::Conflict
                        && error.retryable()
                        && attempt + 1 < MAX_SNAPSHOT_COORDINATION_ATTEMPTS =>
                {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        unreachable!("the final coordination attempt returns its outcome")
    }

    async fn pin_live_log_once(
        &self,
        planning: &QueryPlanningToken,
        expected: QueryTableVersion,
        start: LogOffset,
        schema: &SchemaRef,
        kind: TableKind,
    ) -> Result<Option<(LiveLogSnapshot, Arc<NativeLogPlan>)>> {
        let query_id = planning.instance_id().query_id();
        // Preserve the existing coordination peak while resolution and Actor snapshot creation
        // run. This inherits the full-snapshot path's fixed coordination allowance; it is not a
        // proven bound for every allowed combination of schema width and live-segment count.
        // Resolution keeps this lease if the awaiting query is cancelled. Compact only after
        // both retained owners have separate size-based reservations below.
        let bytes = conservative_query_snapshot_planning_peak_bytes(1)?;
        let memory = planning.reserve_snapshot_memory(bytes)?;
        let (resolved, memory) = self
            .inner
            .catalog
            .resolve_tables_with_memory_lease(
                query_id,
                &[TableRef::Id {
                    table_id: expected.table_id,
                }],
                memory,
            )
            .await?;
        if resolved.len() != 1 {
            return Err(corruption(
                query_id,
                "Catalog table resolution count does not match the requested reference count",
            ));
        }
        let resolved = canonicalize_tables(resolved)?;
        let actor = self.inner.table_actors.table(expected.table_id)?;
        let boundary = TableBoundary::from_actor(actor.try_snapshot()?.wait().await?);
        validate_table_boundaries(query_id, &resolved, std::slice::from_ref(&boundary))?;
        if boundary
            .actor_committed_log_offset
            .known()
            .is_some_and(|last| start <= last)
        {
            return Ok(None);
        }
        if boundary.schema_version != expected.schema_version
            || resolved[&expected.table_id].kind() != kind
        {
            return Err(ServerError::new(
                ErrorCode::SchemaMismatch,
                "event subscription schema changed",
            ));
        }
        // Read after the actor observation: the published persisted version is already durable,
        // so this actual Catalog revision cannot predate it. No file or schema text is loaded.
        let catalog_revision = self.inner.catalog.revision().await?;
        self.ensure_planning_active(planning)?;
        self.ensure_accepting(query_id)?;
        let schema_bytes = conservative_arrow_schema_retained_bytes(schema.as_ref())?.get();
        let snapshot_bytes = live_snapshot_retained_bytes(query_id, &boundary.live_segments)?;
        let snapshot_memory = planning.reserve_snapshot_memory(snapshot_bytes)?;
        let plan_bytes = (boundary.live_segments.len() as u64)
            .checked_mul(512)
            .and_then(|bytes| bytes.checked_add(schema_bytes.checked_mul(3)?))
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .and_then(NonZeroU64::new)
            .ok_or_else(|| log_error("event plan charge overflowed"))?;
        let plan_memory = planning.reserve_snapshot_memory(plan_bytes)?;
        // Structural validation remains over every current live span. UTF-8 payload bounds are
        // deliberately deferred until after cursor clipping; consumed live rows are not scanned.
        let (active_segment, retained_sealed_segments) =
            project_live_read_descriptors(query_id, &boundary, false)?;
        let mut sources = boundary
            .live_segments
            .iter()
            .enumerate()
            .map(|(index, segment)| NativeLogSource {
                location: NativeLogLocation::Live {
                    index,
                    max_utf8_value_bytes: 0,
                },
                physical: segment.offsets(),
                selected: segment.offsets(),
                frame: (kind == TableKind::Frame).then_some(segment.offsets()),
                frame_source: (kind == TableKind::Frame).then_some(segment.identity()),
            })
            .collect::<Vec<_>>();
        select_sources_after(
            &mut sources,
            boundary.live_high_watermark,
            Some(start),
            boundary.actor_committed_log_offset,
        )?;
        for source in &mut sources {
            self.ensure_planning_active(planning)?;
            let NativeLogLocation::Live {
                index,
                max_utf8_value_bytes,
            } = &mut source.location
            else {
                unreachable!("live-only sources have no file capability")
            };
            *max_utf8_value_bytes = selected_utf8_bound(
                query_id,
                boundary.live_segments[*index].span(),
                source.physical,
                source.selected,
            )?;
        }
        let descriptor = QueryReadSnapshotDescriptor {
            query_id,
            catalog_revision,
            tables: vec![TableReadSnapshotDescriptor {
                table_id: boundary.table_id,
                schema_version: boundary.schema_version,
                persisted_snapshot_version: boundary.persisted_snapshot_version,
                writer_epoch: boundary.writer_epoch,
                active_segment,
                retained_sealed_segments,
                max_log_offset: boundary.live_high_watermark,
                lease_id: self.next_lease_id(),
            }],
        };
        let plan = Arc::new(NativeLogPlan {
            schema: Arc::clone(schema),
            kind,
            watermark: boundary.live_high_watermark,
            sources,
            _memory: plan_memory,
        });
        drop(resolved);
        drop(memory);
        Ok(Some((
            LiveLogSnapshot {
                descriptor,
                segments: boundary.live_segments,
                memory: snapshot_memory,
            },
            plan,
        )))
    }
}

fn live_snapshot_retained_bytes(
    query_id: QueryId,
    segments: &[TableActorLiveSegmentSnapshot],
) -> Result<NonZeroU64> {
    let overflow = || corruption(query_id, "retained snapshot charge overflowed");
    let mut bytes = 64_u64 * 1024;
    for segment in segments {
        // Each Actor observation clones its RowSpanView. For an active backing (including a
        // retained sealed SHM segment), that deep-clones both LayoutPlan.columns and shm_os_id.
        // Validated layouts contain one ColumnLayout per compiled-schema column. Column names,
        // CompiledSchema and the SHM mapping stay shared; the fixed per-span allowance covers
        // inline snapshot/descriptor slots and allocator overhead, not these variable arrays.
        let owned_bytes = match segment.span().active_descriptor() {
            Some(descriptor) => descriptor
                .schema()
                .columns()
                .len()
                .checked_mul(size_of::<ColumnLayout>())
                .and_then(|bytes| bytes.checked_add(descriptor.shm_os_id().len()))
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(overflow)?,
            None => 0,
        };
        bytes = bytes
            .checked_add(512)
            .and_then(|bytes| bytes.checked_add(owned_bytes))
            .ok_or_else(overflow)?;
    }
    NonZeroU64::new(bytes).ok_or_else(overflow)
}

fn selected_utf8_bound(
    query_id: QueryId,
    span: &RowSpanView,
    physical: OffsetRange,
    selected: OffsetRange,
) -> Result<u64> {
    let start =
        usize::try_from(selected.first().get() - physical.first().get()).map_err(log_error)?;
    let rows = usize::try_from(selected.len()).map_err(log_error)?;
    span.slice_rows(start, start + rows)
        .map_err(log_error)?
        .max_utf8_value_bytes()
        .map_err(|error| {
            corruption(
                query_id,
                format!("cannot bound live UTF-8 values error=[{error}]"),
            )
        })
}

impl LiveLogSnapshot {
    pub(crate) fn memory_lease_descriptor(&self) -> CatalogSnapshotMemoryLeaseDescriptor {
        self.memory.descriptor()
    }

    pub(crate) fn plan_draft(&self) -> QueryPlanDraft {
        let table = &self.descriptor.tables[0];
        QueryPlanDraft::new(
            self.descriptor.query_id,
            self.descriptor.catalog_revision,
            vec![QueryTableVersion {
                table_id: table.table_id,
                snapshot_version: table.persisted_snapshot_version,
                schema_version: table.schema_version,
            }],
        )
    }

    pub(crate) fn log_span(&self, index: usize, offsets: OffsetRange) -> Result<&RowSpanView> {
        let segment = self.segments.get(index).ok_or_else(|| {
            corruption(
                self.descriptor.query_id,
                "event source index is out of bounds",
            )
        })?;
        if segment.offsets() != offsets {
            return Err(corruption(
                self.descriptor.query_id,
                "event source differs from the pinned window",
            ));
        }
        Ok(segment.span())
    }
}

impl QueryReadSnapshotGuard for LiveLogSnapshot {
    fn descriptor(&self) -> &QueryReadSnapshotDescriptor {
        &self.descriptor
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn renew(&mut self, _expires_at_ns: u64) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn release(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            drop(self);
            Ok(())
        })
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
    use zippy_core::{OwnedDataDirectory, DEFAULT_QUERY_MAX_MEMORY_BYTES};
    use zippy_ingest::{
        TableActorLiveSegmentPublication, TableActorLiveSegmentRole,
        TableActorLiveSegmentsPublication,
    };
    use zippy_segment_store::{
        compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan, SegmentStore,
        SegmentStoreConfig,
    };
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogWrite,
        CreateNamespaceCommand, CreateTableCommand,
    };
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, SchemaIpcPayload, SourceSegmentIdentity,
        TableDefinition,
    };
    use zippy_types::{NamespaceId, RequestId, SessionId, TableId};

    use crate::native_storage::{
        NativeStorageRuntime, NativeStorageRuntimeConfig, NativeStorageShutdownOutcome,
    };
    use crate::query_service::{QueryResourceBudget, QuerySubmissionMetadata};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_wide_snapshot_owns_layout_charge_after_plan_drop() {
        const COLUMNS: usize = 128;
        const SEGMENTS: usize = 32;
        let schema = Arc::new(Schema::new(
            (0..COLUMNS)
                .map(|index| Field::new(format!("column_{index}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        let mut ipc = Vec::new();
        let mut writer = StreamWriter::try_new(&mut ipc, schema.as_ref()).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let temp = tempfile::tempdir().unwrap();
        let ownership = Arc::new(
            OwnedDataDirectory::acquire_or_initialize(temp.path().join("database")).unwrap(),
        );
        let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
        let table_id = TableId::from_u128(10).unwrap();
        let namespace_id = NamespaceId::from_u128(1).unwrap();
        let commands = [
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id,
                name: CatalogName::new("memory_test").unwrap(),
            }),
            CatalogCommandV1::CreateTable(CreateTableCommand {
                table_id,
                namespace_id,
                name: CatalogName::new("wide").unwrap(),
                definition: TableDefinition::Append(AppendTableDefinition::default()),
                schema_ipc: SchemaIpcPayload::new(ipc).unwrap(),
                config: BTreeMap::new(),
            }),
        ];
        for (index, command) in commands.into_iter().enumerate() {
            catalog
                .command(
                    CatalogCommandEnvelopeV1::new(
                        RequestId::from_u128(index as u128 + 1).unwrap(),
                        Some(index as u64),
                        CatalogActor {
                            kind: CatalogActorKind::Server,
                            principal: "live-snapshot-memory-test".to_owned(),
                        },
                        command,
                    )
                    .unwrap()
                    .into(),
                )
                .await
                .unwrap();
        }
        catalog.close().unwrap();
        let mut runtime =
            NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
        let actor = runtime.table_actor(table_id).unwrap();
        let initial = actor.snapshot().await.unwrap();
        let compiled = compile_schema(
            &schema
                .fields()
                .iter()
                .map(|field| ColumnSpec::new(field.name().as_str(), ColumnType::Int64))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let store = SegmentStore::new(SegmentStoreConfig {
            default_row_capacity: 1,
        })
        .unwrap();
        let partition = store
            .open_partition_with_schema_and_writer_epoch(
                "wide",
                "all",
                compiled,
                initial.writer_epoch,
            )
            .unwrap();
        let mut live = Vec::new();
        for index in 0..SEGMENTS {
            partition
                .writer()
                .write_row(|row| {
                    for field in schema.fields() {
                        row.write_i64(field.name(), 7)?;
                    }
                    Ok(())
                })
                .unwrap();
            live.push(partition.active_descriptor());
            if index + 1 != SEGMENTS {
                partition.writer().rollover().unwrap();
            }
        }
        assert_eq!(live.len(), SEGMENTS);
        actor
            .publish_live_segments(TableActorLiveSegmentsPublication::new(
                table_id,
                live.iter()
                    .enumerate()
                    .map(|(index, descriptor)| {
                        TableActorLiveSegmentPublication::new(
                            if index + 1 != SEGMENTS {
                                TableActorLiveSegmentRole::RetainedSealed
                            } else {
                                TableActorLiveSegmentRole::Active
                            },
                            SourceSegmentIdentity {
                                segment_id: descriptor.segment_id(),
                                generation: descriptor.generation(),
                                writer_epoch: descriptor.writer_epoch(),
                            },
                            initial.schema_version,
                            OffsetRange::single(LogOffset::new(index as u64)),
                            descriptor.clone(),
                            0,
                            1,
                        )
                    })
                    .collect(),
            ))
            .await
            .unwrap();
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(SessionId::from_u128(1).unwrap());
        let planning = resources
            .submit(
                owner.as_ref(),
                QuerySubmissionMetadata {
                    query_id: QueryId::from_u128(1).unwrap(),
                    deadline_ns: None,
                },
                QueryResourceBudget {
                    max_active_tasks: 1,
                    max_cpu_slots: 1,
                    max_memory_bytes: DEFAULT_QUERY_MAX_MEMORY_BYTES,
                },
            )
            .unwrap();
        let (snapshot, plan) = runtime
            .query_snapshot_coordinator()
            .pin_live_log(
                &planning,
                QueryTableVersion {
                    table_id,
                    snapshot_version: initial.persisted_snapshot_version,
                    schema_version: initial.schema_version,
                },
                LogOffset::new(0),
                &schema,
                TableKind::Append,
            )
            .await
            .unwrap()
            .unwrap();
        // Measure actual layout entries through their public accessor, independently of the
        // production charge's compiled-schema column count. Every observed span owns a distinct
        // cloned layout vector, even though its schema and mapped payload stay shared.
        let mut layout_bytes = 0;
        let mut layout_addresses = std::collections::HashSet::new();
        for segment in snapshot.segments.iter() {
            let layout = segment.span().active_descriptor().unwrap().layout();
            layout_addresses.insert(layout.column_at(0).unwrap() as *const ColumnLayout);
            let mut index = 0;
            while let Some(column) = layout.column_at(index) {
                layout_bytes += std::mem::size_of_val(column) as u64;
                index += 1;
            }
            assert_eq!(index, COLUMNS);
        }
        assert_eq!(layout_addresses.len(), SEGMENTS);
        let snapshot_bytes = snapshot.memory_lease_descriptor().reserved_bytes().get();
        let previous_fixed_charge = 64 * 1024 + SEGMENTS as u64 * 512;
        assert!(layout_bytes > previous_fixed_charge);
        assert!(snapshot_bytes >= previous_fixed_charge + layout_bytes);
        let plan_bytes = plan._memory.descriptor().reserved_bytes().get();
        assert_eq!(
            resources.status().active_snapshot_memory_bytes,
            snapshot_bytes + plan_bytes
        );
        drop(plan);
        assert_eq!(
            resources.status().active_snapshot_memory_bytes,
            snapshot_bytes
        );
        assert_eq!(snapshot.segments.len(), SEGMENTS);
        eprintln!(
            "live wide snapshot columns=[{COLUMNS}] segments=[{SEGMENTS}] \
             layout_bytes=[{layout_bytes}] snapshot_bytes=[{snapshot_bytes}] \
             plan_bytes=[{plan_bytes}] coordination_peak=[{}]",
            conservative_query_snapshot_planning_peak_bytes(1).unwrap()
        );
        drop(snapshot);
        assert_eq!(resources.status().active_snapshot_memory_bytes, 0);
        drop(planning);
        drop(live);
        drop(partition);
        drop(store);
        assert_eq!(
            runtime.shutdown(Duration::from_secs(5)),
            NativeStorageShutdownOutcome::Complete
        );
    }

    #[test]
    fn live_utf8_bound_only_scans_the_selected_suffix() {
        let schema = compile_schema(&[ColumnSpec::new("symbol", ColumnType::Utf8)]).unwrap();
        let layout = LayoutPlan::for_schema(&schema, 128).unwrap();
        let mut writer = ActiveSegmentWriter::new_for_runtime(schema, layout, 17, 1).unwrap();
        for value in ["x".repeat(2048), "ok".to_owned()] {
            writer.begin_row().unwrap();
            writer.write_utf8("symbol", &value).unwrap();
            writer.commit_row().unwrap();
        }
        let span = RowSpanView::from_active_descriptor(writer.active_descriptor(), 0, 2).unwrap();
        assert_eq!(span.max_utf8_value_bytes().unwrap(), 2048);
        assert_eq!(
            selected_utf8_bound(
                QueryId::from_u128(1).unwrap(),
                &span,
                OffsetRange::new(LogOffset::new(100), LogOffset::new(101)).unwrap(),
                OffsetRange::single(LogOffset::new(101)),
            )
            .unwrap(),
            2
        );
    }
}
