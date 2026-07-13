use std::fs;
use std::path::PathBuf;

use serde_json::Value;
use zippy_master::server::MasterServer;
use zippy_master::snapshot::SnapshotStore;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/contracts")
        .join(name)
}

fn json_fixture(name: &str) -> Value {
    let bytes = fs::read(fixture_path(name)).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[test]
fn legacy_registry_v1_preserves_table_and_persisted_metadata() {
    let snapshot = SnapshotStore::load(&fixture_path("legacy_registry_v1.json")).unwrap();
    let table_contract = json_fixture("table_semantics_v1.json");
    let persisted_contract = json_fixture("persisted_file_v1.json");

    assert_eq!(snapshot.streams.len(), 3);
    assert_eq!(snapshot.sources.len(), 3);
    assert_eq!(snapshot.streams[0].stream_name, "factor_events");
    assert_eq!(snapshot.streams[0].buffer_size, 1024);
    assert_eq!(snapshot.streams[0].frame_size, 1024);
    assert_eq!(
        snapshot.streams[0].persisted_files,
        vec![persisted_contract]
    );
    assert_eq!(snapshot.streams[0].segment_reader_leases.len(), 1);

    let tables = table_contract["tables"].as_array().unwrap();
    for source in &snapshot.sources {
        let table = tables
            .iter()
            .find(|table| table["name"] == source.output_stream)
            .unwrap();
        assert_eq!(source.config, table["descriptor"]);
    }
}

#[test]
fn legacy_registry_v1_round_trip_is_canonical_and_lossless() {
    let snapshot = SnapshotStore::load(&fixture_path("legacy_registry_v1.json")).unwrap();
    let expected = serde_json::to_value(&snapshot).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let canonical_path = temp.path().join("master-registry.json");

    SnapshotStore::write(&canonical_path, &snapshot).unwrap();
    let canonical_text = fs::read_to_string(&canonical_path).unwrap();
    let reloaded = SnapshotStore::load(&canonical_path).unwrap();

    assert!(!canonical_text.contains("ring_capacity"));
    assert!(canonical_text.contains("\"buffer_size\": 1024"));
    assert!(canonical_text.contains("\"frame_size\": 1024"));
    assert_eq!(serde_json::to_value(reloaded).unwrap(), expected);
}

#[test]
fn legacy_registry_v1_restores_durable_state_without_runtime_leases() {
    let server =
        MasterServer::from_snapshot_path(&fixture_path("legacy_registry_v1.json")).unwrap();
    let registry = server.registry();
    let registry = registry.lock().unwrap();
    let append = registry.get_stream("factor_events").unwrap();

    assert_eq!(append.status, "restored");
    assert_eq!(append.writer_epoch, 7);
    assert_eq!(append.persist_revision, 3);
    assert_eq!(
        append.persisted_files,
        vec![json_fixture("persisted_file_v1.json")]
    );
    assert!(append.segment_reader_leases.is_empty());
    assert!(append.active_segment_descriptor.is_none());

    for source in registry.list_sources() {
        assert_eq!(source.status, "restored");
    }
}
