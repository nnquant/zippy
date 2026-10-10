//! Real Catalog snapshots and restart bootstrap while checkpoint visibility is prepared.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::sync::oneshot;
use zippy_types::catalog::{CatalogRead, CatalogSnapshotSelector, CatalogTableSnapshot};
use zippy_types::table::{DataFileLocation, TableKind};
use zippy_types::{ErrorCode, LogOffset, OffsetWatermark};

use crate::{
    NativeIngestSink, NativeIngestTableBootstrap, TableActorRegistry, TableActorRegistryConfig,
    TableActorSnapshot, TableRuntimeBootstrap, TableRuntimeRegistry,
};

use super::tests::{apply_one, table_id, Fixture};
use super::{NativeTableMaterializerConfig, NativeTableMaterializerSink};

/// Pauses after durable Catalog commit and before actor publication or prefix reclamation.
pub(super) struct PreparedCheckpointPause {
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

impl PreparedCheckpointPause {
    pub(super) async fn wait(self) {
        self.reached.send(()).expect("announce prepared checkpoint");
        self.resume.await.expect("resume prepared checkpoint");
    }
}

fn prices(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn persisted_prices(fixture: &Fixture, snapshot: &CatalogTableSnapshot) -> Vec<i64> {
    let mut rows = Vec::new();
    for descriptor in snapshot.files().as_slice() {
        let DataFileLocation::Managed(path) = &descriptor.file.location;
        let file =
            std::fs::File::open(fixture._root.path().join("data").join(path.as_str())).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            rows.extend(prices(&batch.unwrap()));
        }
    }
    rows.sort_unstable();
    rows
}

fn combined_prices(
    fixture: &Fixture,
    persisted: &CatalogTableSnapshot,
    actor: &TableActorSnapshot,
) -> Vec<i64> {
    let mut rows = persisted_prices(fixture, persisted);
    for segment in actor.live_segments.iter() {
        rows.extend(prices(&segment.span().as_record_batch().unwrap()));
    }
    rows.sort_unstable();
    rows
}

async fn apply(fixture: &Fixture, kind: TableKind, offset: u64) {
    let sink = Arc::clone(&fixture.sink);
    tokio::task::spawn_blocking(move || apply_one(&sink, kind, offset))
        .await
        .unwrap()
        .unwrap();
}

async fn prepared_catalog_query_and_restart(kind: TableKind) {
    let fixture = Fixture::start(kind).await;
    apply(&fixture, kind, 0).await;
    let sink = Arc::clone(&fixture.sink);
    let initial = tokio::task::spawn_blocking(move || {
        sink.prepare_checkpoint(table_id(), OffsetWatermark::Known(LogOffset::new(0)))
    })
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    initial.await.unwrap();
    let table = fixture.sink.table(table_id()).unwrap();
    let initial_actor = table.actor.snapshot().await.unwrap();
    assert_eq!(initial_actor.persisted_snapshot_version, 2);
    assert_eq!(
        initial_actor.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(0))
    );
    apply(&fixture, kind, 1).await;
    let (reached, prepared) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    *table.prepared_pause.lock().unwrap() = Some(PreparedCheckpointPause {
        reached,
        resume: resumed,
    });
    let sink = Arc::clone(&fixture.sink);
    let checkpoint = tokio::task::spawn_blocking(move || {
        sink.prepare_checkpoint(table_id(), OffsetWatermark::Known(LogOffset::new(1)))
    })
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let checkpoint = tokio::spawn(checkpoint);
    tokio::time::timeout(Duration::from_secs(10), prepared)
        .await
        .expect("checkpoint reaches prepared stage after real Catalog commit")
        .unwrap();

    let retirement_error = match fixture
        .actors
        .registrar()
        .prepare_retirement(table_id())
        .await
    {
        Err(error) => error,
        Ok(retirement) => {
            drop(retirement);
            panic!("prepared persistence must exclude table retirement until publication");
        }
    };
    assert_eq!(retirement_error.code(), ErrorCode::ResourceExhausted);
    assert!(retirement_error.retryable());

    // Stage 1 is durably committed, but the actor and materializer still expose the old
    // persisted prefix. Writers and snapshot readers must continue making progress.
    apply(&fixture, kind, 2).await;
    let during = table.actor.snapshot().await.unwrap();
    assert_eq!(during.persisted_snapshot_version, 2);
    assert_eq!(
        during.committed_log_offset,
        initial_actor.committed_log_offset
    );
    assert_eq!(
        during.max_log_offset,
        OffsetWatermark::Known(LogOffset::new(2))
    );
    let descriptor = fixture._catalog.get_table(table_id().into()).await.unwrap();
    assert_eq!(descriptor.current_snapshot_version, 3);
    let old = fixture
        ._catalog
        .read_snapshot_at(&[CatalogSnapshotSelector {
            table_id: table_id(),
            schema_version: during.schema_version,
            snapshot_version: during.persisted_snapshot_version,
        }])
        .await
        .unwrap();
    let latest = fixture
        ._catalog
        .read_snapshot_at(&[CatalogSnapshotSelector {
            table_id: table_id(),
            schema_version: descriptor.current_schema_version,
            snapshot_version: descriptor.current_snapshot_version,
        }])
        .await
        .unwrap();
    assert_eq!(persisted_prices(&fixture, &old.tables()[0]), vec![0]);
    assert_eq!(persisted_prices(&fixture, &latest.tables()[0]), vec![0, 1]);
    assert_eq!(
        combined_prices(&fixture, &old.tables()[0], &during),
        vec![0, 1, 2],
        "actor-selected Catalog version plus live rows is complete and duplicate-free"
    );
    assert_eq!(
        combined_prices(&fixture, &latest.tables()[0], &during),
        vec![0, 1, 1, 2],
        "mixing the latest Catalog with the old actor would duplicate the prepared prefix"
    );
    {
        let state = table.state.lock().unwrap();
        assert_eq!(
            state.persisted_log_offset,
            OffsetWatermark::Known(LogOffset::new(0))
        );
        assert_eq!(state.retained_start, 1);
        assert_eq!(state.live_rows, 2);
    }

    // Simulate a restart at precisely this durable intermediate state. The fresh actor
    // uses authoritative Catalog version 3, and replay must begin at offset 2, not 1.
    let current = &latest.tables()[0];
    let bootstrap = NativeIngestTableBootstrap::from_catalog_snapshot(
        current.table(),
        Arc::clone(&table.schema),
        current.snapshot(),
    )
    .unwrap();
    assert_eq!(
        bootstrap.persisted_log_offset(),
        OffsetWatermark::Known(LogOffset::new(1))
    );
    if kind == TableKind::KeyValue {
        assert_eq!(bootstrap.persisted_state_version(), 2);
    }
    let runtime = TableRuntimeBootstrap::new(
        current.table().clone(),
        current.schema().clone(),
        Arc::clone(&table.schema),
        current.snapshot().clone(),
    )
    .unwrap();
    let recovered_actors = TableActorRegistry::start(
        TableActorRegistryConfig::new(),
        TableRuntimeRegistry::from_bootstraps(latest.catalog_revision(), vec![runtime]).unwrap(),
        fixture.persistence.handle(),
    )
    .unwrap();
    let recovered_sink = Arc::new(
        NativeTableMaterializerSink::start(
            NativeTableMaterializerConfig::new().with_row_capacity(8),
            &[bootstrap],
            &recovered_actors.handle(),
        )
        .unwrap(),
    );
    let recovered_table = recovered_sink.table(table_id()).unwrap();
    let recovered = recovered_table.actor.snapshot().await.unwrap();
    assert_eq!(recovered.persisted_snapshot_version, 3);
    assert_eq!(
        recovered.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(1))
    );
    assert!(recovered.live_segments.is_empty());
    assert_eq!(
        combined_prices(&fixture, current, &recovered),
        vec![0, 1],
        "restart exposes exactly the durable prefix before WAL suffix replay"
    );
    let sink = Arc::clone(&recovered_sink);
    tokio::task::spawn_blocking(move || apply_one(&sink, kind, 2))
        .await
        .unwrap()
        .unwrap();
    let recovered = recovered_table.actor.snapshot().await.unwrap();
    assert_eq!(
        combined_prices(&fixture, current, &recovered),
        vec![0, 1, 2]
    );
    if kind == TableKind::KeyValue {
        assert_eq!(recovered_table.state.lock().unwrap().key_value_version, 3);
    }
    recovered_actors.drain().await.unwrap();
    drop(recovered_sink);
    drop(recovered_table);

    resume.send(()).unwrap();
    checkpoint.await.unwrap().unwrap();
    let after = table.actor.snapshot().await.unwrap();
    assert_eq!(after.persisted_snapshot_version, 3);
    assert_eq!(
        after.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(1))
    );
    assert_eq!(combined_prices(&fixture, current, &after), vec![0, 1, 2]);
    let retirement = fixture
        .actors
        .registrar()
        .prepare_retirement(table_id())
        .await
        .expect("retirement may prepare after durable publication and reclamation finish");
    drop(retirement);
    fixture.drain().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_catalog_snapshot_and_restart_append() {
    prepared_catalog_query_and_restart(TableKind::Append).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_catalog_snapshot_and_restart_frame() {
    prepared_catalog_query_and_restart(TableKind::Frame).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_catalog_snapshot_and_restart_key_value() {
    prepared_catalog_query_and_restart(TableKind::KeyValue).await;
}
