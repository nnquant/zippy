#![cfg(unix)]

use std::fs;
use std::num::NonZeroUsize;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::datatypes::{DataType, Field, Schema};
use tempfile::TempDir;
use zippy_core::{MasterClient, ZippyConfig};
use zippy_master::legacy_catalog_commit::{
    LegacyCatalogCommitMutationKind, LegacyCatalogCommitPublisher,
};
use zippy_master::server::MasterServer;

static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn unique_socket_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sequence = SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "zippy-master-commit-events-{}-{nanos}-{sequence}.sock",
        std::process::id()
    ))
}

fn spawn_server(
    server: &MasterServer,
    socket_path: &Path,
) -> thread::JoinHandle<zippy_core::Result<()>> {
    let serving = server.clone();
    let socket_path = socket_path.to_path_buf();
    let wait_path = socket_path.clone();
    let handle = thread::spawn(move || serving.serve(&socket_path));
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match UnixStream::connect(&wait_path) {
            Ok(stream) => {
                drop(stream);
                return handle;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound
                        | std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionReset
                ) =>
            {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("wait for master socket failed error=[{error}]"),
        }
    }
    panic!("master socket did not become ready");
}

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn register_table(client: &mut MasterClient, root: Option<&Path>) {
    client.register_process("commit-event-test").unwrap();
    client
        .register_stream("events", test_schema(), 64, 4096)
        .unwrap();
    let mut source_config = serde_json::json!({"zippy_table_kind": "append_table"});
    if let Some(root) = root {
        source_config.as_object_mut().unwrap().insert(
            "persist_data_root".to_string(),
            serde_json::Value::String(root.to_string_lossy().into_owned()),
        );
    }
    client
        .register_source("events-source", "test", "events", source_config)
        .unwrap();
}

#[test]
fn committed_events_preserve_catalog_revision_order() {
    let temp = TempDir::new().unwrap();
    let persist_root = temp.path().join("persist");
    fs::create_dir(&persist_root).unwrap();
    let snapshot_path = temp.path().join("master-registry.json");
    let mut config = ZippyConfig::default();
    config.table.persist.data_dir = persist_root.to_string_lossy().into_owned();
    let server = MasterServer::with_runtime_config_and_config(
        Some(snapshot_path),
        Duration::from_secs(10),
        Duration::from_secs(2),
        config,
    );
    let (publisher, receiver) =
        LegacyCatalogCommitPublisher::bounded(NonZeroUsize::new(16).unwrap());
    let metrics = publisher.metrics();
    server
        .install_legacy_catalog_commit_publisher(publisher)
        .unwrap();
    let socket_path = unique_socket_path();
    let handle = spawn_server(&server, &socket_path);
    let mut client = MasterClient::connect(&socket_path).unwrap();
    register_table(&mut client, Some(&persist_root));

    let file_path = persist_root.join("events.parquet");
    fs::write(&file_path, b"event-test").unwrap();
    let persisted_file = serde_json::json!({
        "file_path": file_path,
        "persist_file_id": "events-1",
        "row_count": 1,
    });
    client
        .publish_persisted_file("events", persisted_file.clone())
        .unwrap();
    let replacement_path = persist_root.join("events-replacement.parquet");
    fs::write(&replacement_path, b"replacement-event-test").unwrap();
    client
        .replace_persisted_files(
            "events",
            vec![serde_json::json!({
                "file_path": replacement_path,
                "persist_file_id": "events-2",
                "row_count": 2,
            })],
        )
        .unwrap();
    client
        .publish_persist_event(
            "events",
            serde_json::json!({
                "persist_event_id": "persist-event-1",
                "persist_event_type": "flush",
            }),
        )
        .unwrap();
    client.unregister_source("events-source").unwrap();
    assert!(client.drop_table("events", false).unwrap().dropped);

    let events = (0..6)
        .map(|_| receiver.recv_timeout(Duration::from_secs(1)).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .map(|event| event.mutation_kind())
            .collect::<Vec<_>>(),
        vec![
            LegacyCatalogCommitMutationKind::RegisterStream,
            LegacyCatalogCommitMutationKind::RegisterSource,
            LegacyCatalogCommitMutationKind::PublishPersistedFile,
            LegacyCatalogCommitMutationKind::ReplacePersistedFiles,
            LegacyCatalogCommitMutationKind::PersistEventWatermark,
            LegacyCatalogCommitMutationKind::DropTable,
        ]
    );
    assert_eq!(events[0].previous_revision(), 0);
    assert!(events
        .windows(2)
        .all(|pair| pair[0].committed_revision() == pair[1].previous_revision()));
    let table_id = events[0].stream().unwrap().table_id();
    assert!(events
        .iter()
        .all(|event| event.stream().unwrap().table_id() == table_id));

    server.shutdown();
    handle.join().unwrap().unwrap();
    assert!(matches!(
        receiver.recv_timeout(Duration::from_millis(50)),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
    ));
    let publisher_metrics = metrics.snapshot();
    assert!(publisher_metrics.sealed);
    assert_eq!(
        publisher_metrics.final_revision,
        events.last().unwrap().committed_revision()
    );
}

#[test]
fn installation_seeds_baseline_and_shutdown_seals_empty_queue() {
    let server = MasterServer::default();
    server
        .registry()
        .lock()
        .unwrap()
        .register_stream(
            "baseline-events",
            serde_json::json!({"fields": [], "metadata": {}}),
            "baseline-schema",
            64,
            4096,
        )
        .unwrap();
    let (publisher, receiver) =
        LegacyCatalogCommitPublisher::bounded(NonZeroUsize::new(2).unwrap());
    let metrics = publisher.metrics();
    server
        .install_legacy_catalog_commit_publisher(publisher)
        .unwrap();
    let installed = metrics.snapshot();
    assert_eq!(installed.baseline_revision, 1);
    assert_eq!(installed.latest_committed_revision, 1);

    let socket_path = unique_socket_path();
    let handle = spawn_server(&server, &socket_path);
    server.shutdown();
    handle.join().unwrap().unwrap();

    assert!(matches!(
        receiver.recv_timeout(Duration::from_millis(50)),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
    ));
    let sealed = metrics.snapshot();
    assert!(sealed.sealed);
    assert_eq!(sealed.final_revision, 1);
}

#[test]
fn full_queue_does_not_change_primary_response_and_latches_gap() {
    let server = MasterServer::default();
    let (publisher, receiver) =
        LegacyCatalogCommitPublisher::bounded(NonZeroUsize::new(1).unwrap());
    let metrics = publisher.metrics();
    server
        .install_legacy_catalog_commit_publisher(publisher)
        .unwrap();
    let socket_path = unique_socket_path();
    let handle = spawn_server(&server, &socket_path);
    let mut client = MasterClient::connect(&socket_path).unwrap();

    register_table(&mut client, None);

    let first = receiver.try_recv().unwrap();
    assert_eq!(
        first.mutation_kind(),
        LegacyCatalogCommitMutationKind::RegisterStream
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.attempts_total, 2);
    assert_eq!(snapshot.published_total, 1);
    assert_eq!(snapshot.queue_full_total, 1);
    assert_eq!(snapshot.latest_committed_revision, 2);
    assert_eq!(snapshot.last_lost_revision, 2);
    assert_eq!(snapshot.continuity_loss_epoch, 1);
    assert!(snapshot.continuity_lost);

    server.shutdown();
    handle.join().unwrap().unwrap();
    let sealed = metrics.snapshot();
    assert!(sealed.sealed);
    assert_eq!(sealed.final_revision, 2);
    assert_eq!(sealed.latest_observed_revision, 2);
    assert_eq!(sealed.continuity_loss_epoch, 1);
}

#[test]
fn snapshot_write_failure_rolls_back_without_publishing() {
    let temp = TempDir::new().unwrap();
    let snapshot_path = temp.path().join("snapshot-is-a-directory");
    fs::create_dir(&snapshot_path).unwrap();
    let server = MasterServer::with_runtime_config(
        Some(snapshot_path),
        Duration::from_secs(10),
        Duration::from_secs(2),
    );
    let (publisher, receiver) =
        LegacyCatalogCommitPublisher::bounded(NonZeroUsize::new(4).unwrap());
    let metrics = publisher.metrics();
    server
        .install_legacy_catalog_commit_publisher(publisher)
        .unwrap();
    let socket_path = unique_socket_path();
    let handle = spawn_server(&server, &socket_path);
    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("snapshot-failure-test").unwrap();

    assert!(client
        .register_stream("events", test_schema(), 64, 4096)
        .is_err());
    assert!(server
        .registry()
        .lock()
        .unwrap()
        .get_stream("events")
        .is_none());
    assert!(matches!(
        receiver.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    assert_eq!(metrics.snapshot().attempts_total, 0);

    server.shutdown();
    assert!(handle.join().unwrap().is_err());
}
