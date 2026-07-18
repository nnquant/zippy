use std::fs;
use std::sync::{Mutex, OnceLock};

use zippy_core::{
    ZippyCatalogBackend, ZippyConfig, ZippyRuntimeConfig,
    DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_PINS,
    DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_TABLES, MAX_CATALOG_RUNTIME_SNAPSHOT_PINS,
    MAX_QUERY_PLAN_TTL_MS,
};

const CONFIG_ENV_KEYS: &[&str] = &[
    "HOME",
    "ZIPPY_SERVER_DATA_DIR",
    "ZIPPY_SERVER_IMPORT_LEGACY_SNAPSHOT",
    "ZIPPY_SERVER_NATIVE_STORAGE",
    "ZIPPY_CATALOG_BACKEND",
    "ZIPPY_CATALOG_RUNTIME_SNAPSHOT_PINS_MAX_ACTIVE_PINS",
    "ZIPPY_CATALOG_RUNTIME_SNAPSHOT_PINS_MAX_ACTIVE_TABLES",
    "ZIPPY_CATALOG_SHADOW_QUEUE_CAPACITY",
    "ZIPPY_CATALOG_SHADOW_METADATA_BUDGET_BYTES",
    "ZIPPY_CATALOG_SHADOW_MAX_FILES_PER_COMPARISON",
    "ZIPPY_CATALOG_SHADOW_MAX_METADATA_BYTES_PER_COMPARISON",
    "ZIPPY_CATALOG_SHADOW_COMPARISON_TIMEOUT_MS",
    "ZIPPY_CATALOG_SHADOW_DEDUP_CAPACITY",
    "ZIPPY_LOG_LEVEL",
    "ZIPPY_MAX_CPU_CORES",
    "ZIPPY_MAX_PARALLELISM",
    "ZIPPY_MAX_QUERY_BUFFER_BYTES",
    "ZIPPY_QUERY_MAX_ACTIVE_TASKS",
    "ZIPPY_QUERY_MAX_BUFFERED_BYTES",
    "ZIPPY_QUERY_MAX_CONCURRENT_QUERIES",
    "ZIPPY_QUERY_MAX_MEMORY_BYTES",
    "ZIPPY_QUERY_NATIVE_ENABLED",
    "ZIPPY_QUERY_OPERATOR_MEMORY_BYTES",
    "ZIPPY_QUERY_PARALLELISM",
    "ZIPPY_QUERY_PLAN_TTL_MS",
    "ZIPPY_QUERY_CANARY_ENABLED",
    "ZIPPY_QUERY_CANARY_MAX_ROWS",
    "ZIPPY_QUERY_CANARY_MAX_BATCHES",
    "ZIPPY_QUERY_CANARY_MAX_OUTPUT_BYTES",
    "ZIPPY_QUERY_CANARY_OPERATOR_MEMORY_BYTES",
    "ZIPPY_TABLE_ROW_CAPACITY",
    "ZIPPY_TABLE_RETENTION_SEGMENTS",
    "ZIPPY_TABLE_PERSIST",
    "ZIPPY_TABLE_PERSIST_METHOD",
    "ZIPPY_TABLE_PERSIST_DATA_DIR",
    "ZIPPY_TABLE_PERSIST_PARTITION_DT_COLUMN",
    "ZIPPY_TABLE_PERSIST_PARTITION_ID_COLUMN",
    "ZIPPY_TABLE_PERSIST_PARTITION_DT_PART",
    "ZIPPY_TABLE_PERSIST_COMPACTION",
    "ZIPPY_TABLE_PERSIST_COMPACTION_INTERVAL_SEC",
    "ZIPPY_TABLE_PERSIST_COMPACTION_MIN_FILES",
    "ZIPPY_TABLE_PERSIST_COMPACTION_DELETE_SOURCES",
    "ZIPPY_TABLE_PERSIST_COMPACTION_SORT_COLUMN",
    "ZIPPY_GATEWAY",
    "ZIPPY_GATEWAY_ENDPOINT",
    "ZIPPY_GATEWAY_HOST",
    "ZIPPY_GATEWAY_PORT",
    "ZIPPY_GATEWAY_TOKEN",
    "ZIPPY_GATEWAY_PROTOCOL_VERSION",
    "ZIPPY_MASTER_HOST",
    "ZIPPY_MASTER_PORT",
    "ZIPPY_MASTER_SHUTDOWN_TIMEOUT_MS",
    "ZIPPY_MASTER_TOKEN",
];

#[test]
fn zippy_config_loads_toml_and_env_overrides() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]
data_dir = "server-data-from-file"
import_legacy_endpoint_snapshot = false

[log]
level = "debug"

[runtime]
max_cpu_cores = 10
max_query_buffer_bytes = 268435456

[query]
parallelism = 6
max_buffered_bytes = 67108864
plan_ttl_ms = 45000

[query.resources]
max_concurrent_queries = 32
max_active_tasks = 40
max_memory_bytes = 402653184

[table]
row_capacity = 1024
retention_segments = 3

[table.persist]
enabled = false
data_dir = "from-file"

[table.persist.partition]
dt_column = "dt"
id_column = "instrument_id"
dt_part = "%Y%m%d"

[table.persist.compaction]
enabled = false
interval_sec = 600.0
min_files = 10
delete_sources = true
sort_column = "dt"

[gateway]
enabled = false
endpoint = "127.0.0.1:17666"
token = "from-file-token"
protocol_version = 1

[master]
host = "127.0.0.1"
port = 17690
shutdown_timeout_ms = 3000
"#,
    )
    .unwrap();

    with_env(
        &[
            ("ZIPPY_SERVER_DATA_DIR", "/tmp/zippy-server-data-from-env"),
            ("ZIPPY_SERVER_IMPORT_LEGACY_SNAPSHOT", "true"),
            ("ZIPPY_LOG_LEVEL", "warn"),
            ("ZIPPY_MAX_CPU_CORES", "9"),
            ("ZIPPY_MAX_QUERY_BUFFER_BYTES", "805306368"),
            ("ZIPPY_QUERY_MAX_ACTIVE_TASKS", "36"),
            ("ZIPPY_QUERY_MAX_BUFFERED_BYTES", "100663296"),
            ("ZIPPY_QUERY_MAX_CONCURRENT_QUERIES", "24"),
            ("ZIPPY_QUERY_MAX_MEMORY_BYTES", "603979776"),
            ("ZIPPY_QUERY_PARALLELISM", "8"),
            ("ZIPPY_QUERY_PLAN_TTL_MS", "20000"),
            ("ZIPPY_TABLE_ROW_CAPACITY", "2048"),
            ("ZIPPY_TABLE_RETENTION_SEGMENTS", "5"),
            ("ZIPPY_TABLE_PERSIST", "true"),
            ("ZIPPY_TABLE_PERSIST_DATA_DIR", "from-env"),
            ("ZIPPY_TABLE_PERSIST_PARTITION_DT_COLUMN", "recv_ts"),
            ("ZIPPY_TABLE_PERSIST_PARTITION_ID_COLUMN", "symbol"),
            ("ZIPPY_TABLE_PERSIST_PARTITION_DT_PART", "%Y%m"),
            ("ZIPPY_TABLE_PERSIST_COMPACTION", "true"),
            ("ZIPPY_TABLE_PERSIST_COMPACTION_INTERVAL_SEC", "120.5"),
            ("ZIPPY_TABLE_PERSIST_COMPACTION_MIN_FILES", "6"),
            ("ZIPPY_TABLE_PERSIST_COMPACTION_DELETE_SOURCES", "false"),
            ("ZIPPY_TABLE_PERSIST_COMPACTION_SORT_COLUMN", "recv_ts"),
            ("ZIPPY_GATEWAY", "true"),
            ("ZIPPY_GATEWAY_ENDPOINT", "127.0.0.1:27666"),
            ("ZIPPY_GATEWAY_TOKEN", "from-env-token"),
            ("ZIPPY_GATEWAY_PROTOCOL_VERSION", "2"),
            ("ZIPPY_MASTER_HOST", "0.0.0.0"),
            ("ZIPPY_MASTER_PORT", "27690"),
            ("ZIPPY_MASTER_SHUTDOWN_TIMEOUT_MS", "7000"),
        ],
        || {
            let config = ZippyConfig::load_from_path(Some(&config_path)).unwrap();

            assert_eq!(
                config.server.data_dir.as_deref(),
                Some(std::path::Path::new("/tmp/zippy-server-data-from-env"))
            );
            assert!(config.server.import_legacy_endpoint_snapshot);
            assert_eq!(config.master.host.as_deref(), Some("0.0.0.0"));
            assert_eq!(config.master.port, Some(27690));
            assert_eq!(config.master.shutdown_timeout_ms, 7000);
            assert_eq!(config.log.level, "warn");
            assert_eq!(config.runtime.max_cpu_cores, 9);
            assert_eq!(config.runtime.max_query_buffer_bytes, 805306368);
            assert_eq!(config.query.parallelism, Some(8));
            assert_eq!(config.query.max_buffered_bytes, Some(100663296));
            assert_eq!(config.query.plan_ttl_ms, 20000);
            assert_eq!(config.query.resources.max_concurrent_queries, 24);
            assert_eq!(config.query.resources.max_active_tasks, 36);
            assert_eq!(config.query.resources.max_memory_bytes, 603979776);
            assert_eq!(config.query_parallelism(), 8);
            assert_eq!(config.query_max_buffered_bytes(), 100663296);
            assert_eq!(config.table.row_capacity, 2048);
            assert_eq!(config.table.retention_segments, Some(5));
            assert!(config.table.persist.enabled);
            assert_eq!(config.table.persist.method, "parquet");
            assert_eq!(config.table.persist.data_dir, "from-env");
            assert_eq!(
                config.table.persist.partition.dt_column.as_deref(),
                Some("recv_ts")
            );
            assert_eq!(
                config.table.persist.partition.id_column.as_deref(),
                Some("symbol")
            );
            assert_eq!(
                config.table.persist.partition.dt_part.as_deref(),
                Some("%Y%m")
            );
            assert!(config.table.persist.compaction.enabled);
            assert_eq!(config.table.persist.compaction.interval_sec, 120.5);
            assert_eq!(config.table.persist.compaction.min_files, 6);
            assert!(!config.table.persist.compaction.delete_sources);
            assert_eq!(
                config.table.persist.compaction.sort_column.as_deref(),
                Some("recv_ts")
            );
            assert!(config.gateway.enabled);
            assert_eq!(config.gateway.endpoint.as_deref(), Some("127.0.0.1:27666"));
            assert_eq!(config.gateway.token.as_deref(), Some("from-env-token"));
            assert_eq!(config.gateway.protocol_version, 2);
        },
    );
}

#[test]
fn zippy_config_defaults_query_parallelism_to_four() {
    let config = with_env(&[], || {
        ZippyConfig::load_from_path(None::<&std::path::Path>)
    })
    .unwrap();

    assert_eq!(config.runtime.max_cpu_cores, 4);
    assert_eq!(config.runtime.max_query_buffer_bytes, 536870912);
    assert_eq!(config.query.parallelism, None);
    assert!(!config.query.native_enabled);
    assert_eq!(config.query.max_buffered_bytes, None);
    assert_eq!(config.query.plan_ttl_ms, 30000);
    assert_eq!(config.query_parallelism(), 4);
    assert_eq!(config.query_max_buffered_bytes(), 134217728);
    assert_eq!(config.query.resources.max_concurrent_queries, 64);
    assert_eq!(config.query.resources.max_active_tasks, 16);
    assert_eq!(config.query.resources.max_memory_bytes, 536870912);
    assert!(!config.query.canary.enabled);
    assert_eq!(config.query.canary.max_rows, 1_000_000);
    assert_eq!(config.query.canary.max_batches, 4_096);
    assert_eq!(config.query.canary.max_output_bytes, 268_435_456);
    assert_eq!(config.query.canary.operator_memory_bytes, None);
    assert_eq!(config.query_canary_operator_memory_bytes(), 67_108_864);
    assert_eq!(config.server.data_dir, None);
    assert!(!config.server.import_legacy_endpoint_snapshot);
    assert!(!config.server.native_storage_enabled);
    assert_eq!(config.catalog.backend, ZippyCatalogBackend::Legacy);
    assert_eq!(
        config.catalog.runtime_snapshot_pins.max_active_pins,
        DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_PINS
    );
    assert_eq!(
        config.catalog.runtime_snapshot_pins.max_active_tables,
        DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_TABLES
    );
    assert_eq!(
        config.server_data_dir("endpoint-data"),
        std::path::PathBuf::from("endpoint-data")
    );
}

#[test]
fn zippy_config_json_overlay_ignores_environment_and_derives_active_tasks() {
    let base = std::env::temp_dir().join("zippy-json-overlay-base");
    let config = with_env(
        &[
            ("ZIPPY_MAX_CPU_CORES", "2"),
            ("ZIPPY_QUERY_MAX_ACTIVE_TASKS", "999"),
        ],
        || {
            ZippyConfig::from_json_overlay(
                serde_json::json!({
                    "runtime": {"max_cpu_cores": 32},
                    "query": {"parallelism": 3},
                }),
                &base,
            )
        },
    )
    .unwrap();

    assert_eq!(config.runtime.max_cpu_cores, 32);
    assert_eq!(config.query_parallelism(), 3);
    assert_eq!(config.query.resources.max_active_tasks, 12);
}

#[test]
fn zippy_config_json_overlay_preserves_explicit_query_resources() {
    let base = std::env::temp_dir().join("zippy-json-overlay-base");
    let config = ZippyConfig::from_json_overlay(
        serde_json::json!({
            "runtime": {"max_cpu_cores": 8},
            "query": {
                "parallelism": 6,
                "resources": {
                    "max_concurrent_queries": 20,
                    "max_active_tasks": 24,
                    "max_memory_bytes": 268435456,
                },
            },
        }),
        &base,
    )
    .unwrap();

    assert_eq!(config.query.resources.max_concurrent_queries, 20);
    assert_eq!(config.query.resources.max_active_tasks, 24);
    assert_eq!(config.query.resources.max_memory_bytes, 268435456);
}

#[test]
fn zippy_config_json_overlay_resolves_relative_data_dir_from_explicit_base() {
    let temp = tempfile::tempdir().unwrap();
    let config = ZippyConfig::from_json_overlay(
        serde_json::json!({
            "server": {"data_dir": "relative/database"},
        }),
        temp.path(),
    )
    .unwrap();

    let expected = temp.path().join("relative/database");
    assert_eq!(config.server.data_dir.as_deref(), Some(expected.as_path()));

    let error = ZippyConfig::from_json_overlay(serde_json::json!({}), "relative/base").unwrap_err();
    assert!(error
        .to_string()
        .contains("JSON config overlay base must be absolute"));
}

#[test]
fn zippy_config_json_overlay_accepts_legacy_cpu_alias_and_rejects_conflict() {
    let base = std::env::temp_dir().join("zippy-json-overlay-base");
    let config = ZippyConfig::from_json_overlay(
        serde_json::json!({
            "runtime": {"max_parallelism": 7},
        }),
        &base,
    )
    .unwrap();
    assert_eq!(config.runtime.max_cpu_cores, 7);
    assert_eq!(config.query.resources.max_active_tasks, 28);

    let error = ZippyConfig::from_json_overlay(
        serde_json::json!({
            "runtime": {"max_cpu_cores": 7, "max_parallelism": 6},
        }),
        &base,
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("conflicting runtime CPU limits layer=[JSON overlay [runtime]]"));
}

#[test]
fn zippy_config_json_overlay_rejects_invalid_cpu_query_and_task_limits() {
    let base = std::env::temp_dir().join("zippy-json-overlay-base");
    let cases = [
        (
            serde_json::json!({"runtime": {"max_cpu_cores": 0}}),
            "runtime max_cpu_cores must be between 1 and 256",
        ),
        (
            serde_json::json!({
                "runtime": {"max_cpu_cores": 4},
                "query": {"parallelism": 5},
            }),
            "query parallelism must be between 1 and runtime max_cpu_cores",
        ),
        (
            serde_json::json!({
                "runtime": {"max_cpu_cores": 8},
                "query": {
                    "parallelism": 6,
                    "resources": {"max_active_tasks": 5},
                },
            }),
            "max_active_tasks must not be less than effective query parallelism",
        ),
    ];

    for (overlay, expected) in cases {
        let error = ZippyConfig::from_json_overlay(overlay, &base).unwrap_err();
        assert!(error.to_string().contains(expected), "expected={expected}");
    }
}

#[test]
fn zippy_config_json_overlay_rejects_null_for_non_nullable_fields() {
    let base = std::env::temp_dir().join("zippy-json-overlay-base");
    let cases = [
        (serde_json::json!({"runtime": null}), "runtime"),
        (
            serde_json::json!({"query": {"resources": null}}),
            "query.resources",
        ),
        (
            serde_json::json!({
                "query": {"resources": {"max_memory_bytes": null}},
            }),
            "query.resources.max_memory_bytes",
        ),
        (
            serde_json::json!({"runtime": {"max_parallelism": null}}),
            "runtime.max_parallelism",
        ),
    ];

    for (overlay, path) in cases {
        let error = ZippyConfig::from_json_overlay(overlay, &base).unwrap_err();
        assert!(
            error.to_string().contains(&format!(
                "JSON config overlay field must not be null path=[{path}]"
            )),
            "path={path} error={error}"
        );
    }
}

#[test]
fn zippy_config_json_overlay_accepts_null_for_nullable_fields() {
    let base = std::env::temp_dir().join("zippy-json-overlay-base");
    let config = ZippyConfig::from_json_overlay(
        serde_json::json!({
            "server": {"data_dir": null},
            "query": {"parallelism": null},
            "gateway": {"endpoint": null},
        }),
        &base,
    )
    .unwrap();

    assert_eq!(config.server.data_dir, None);
    assert_eq!(config.query.parallelism, None);
    assert_eq!(config.gateway.endpoint, None);
}

#[test]
fn zippy_config_loads_and_validates_runtime_snapshot_pin_limits() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[catalog.runtime_snapshot_pins]
max_active_pins = 4096
max_active_tables = 1024
"#,
    )
    .unwrap();

    let from_toml = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();
    assert_eq!(
        from_toml.catalog.runtime_snapshot_pins.max_active_pins,
        4_096
    );
    assert_eq!(
        from_toml.catalog.runtime_snapshot_pins.max_active_tables,
        1_024
    );

    let from_env = with_env(
        &[
            (
                "ZIPPY_CATALOG_RUNTIME_SNAPSHOT_PINS_MAX_ACTIVE_PINS",
                "8192",
            ),
            (
                "ZIPPY_CATALOG_RUNTIME_SNAPSHOT_PINS_MAX_ACTIVE_TABLES",
                "2048",
            ),
        ],
        || ZippyConfig::load_from_path(Some(&config_path)),
    )
    .unwrap();
    assert_eq!(
        from_env.catalog.runtime_snapshot_pins.max_active_pins,
        8_192
    );
    assert_eq!(
        from_env.catalog.runtime_snapshot_pins.max_active_tables,
        2_048
    );

    let zero_error = with_env(
        &[("ZIPPY_CATALOG_RUNTIME_SNAPSHOT_PINS_MAX_ACTIVE_PINS", "0")],
        || ZippyConfig::load_from_path(None::<&std::path::Path>),
    )
    .expect_err("zero runtime snapshot pin limit must be rejected");
    assert!(zero_error
        .to_string()
        .contains("catalog.runtime_snapshot_pins.max_active_pins must be between 1"));

    let oversized = (MAX_CATALOG_RUNTIME_SNAPSHOT_PINS + 1).to_string();
    let oversized_error = with_env(
        &[(
            "ZIPPY_CATALOG_RUNTIME_SNAPSHOT_PINS_MAX_ACTIVE_PINS",
            oversized.as_str(),
        )],
        || ZippyConfig::load_from_path(None::<&std::path::Path>),
    )
    .expect_err("oversized runtime snapshot pin limit must be rejected");
    assert!(oversized_error
        .to_string()
        .contains("catalog.runtime_snapshot_pins.max_active_pins must be between 1"));
}

#[test]
fn zippy_config_loads_sqlite_shadow_backend_from_toml_and_environment() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]
native_storage_enabled = true

[catalog]
backend = "legacy"
"#,
    )
    .unwrap();

    let from_toml = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();
    assert_eq!(from_toml.catalog.backend, ZippyCatalogBackend::Legacy);

    let from_env = with_env(&[("ZIPPY_CATALOG_BACKEND", "sqlite_shadow")], || {
        ZippyConfig::load_from_path(Some(&config_path))
    })
    .unwrap();
    assert_eq!(from_env.catalog.backend, ZippyCatalogBackend::SqliteShadow);
}

#[test]
fn zippy_config_loads_and_validates_bounded_catalog_shadow_resources() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]
native_storage_enabled = true

[catalog]
backend = "sqlite_shadow"

[catalog.shadow]
queue_capacity = 12
metadata_budget_bytes = 33554432
max_files_per_comparison = 2048
max_metadata_bytes_per_comparison = 4194304
comparison_timeout_ms = 2500
dedup_capacity = 256
"#,
    )
    .unwrap();

    let from_toml = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();
    assert_eq!(from_toml.catalog.shadow.queue_capacity, 12);
    assert_eq!(from_toml.catalog.shadow.metadata_budget_bytes, 33_554_432);
    assert_eq!(from_toml.catalog.shadow.max_files_per_comparison, 2_048);
    assert_eq!(
        from_toml.catalog.shadow.max_metadata_bytes_per_comparison,
        4_194_304
    );
    assert_eq!(from_toml.catalog.shadow.comparison_timeout_ms, 2_500);
    assert_eq!(from_toml.catalog.shadow.dedup_capacity, 256);

    let from_env = with_env(
        &[
            ("ZIPPY_CATALOG_SHADOW_QUEUE_CAPACITY", "24"),
            ("ZIPPY_CATALOG_SHADOW_METADATA_BUDGET_BYTES", "67108864"),
            ("ZIPPY_CATALOG_SHADOW_MAX_FILES_PER_COMPARISON", "4096"),
            (
                "ZIPPY_CATALOG_SHADOW_MAX_METADATA_BYTES_PER_COMPARISON",
                "8388608",
            ),
            ("ZIPPY_CATALOG_SHADOW_COMPARISON_TIMEOUT_MS", "7500"),
            ("ZIPPY_CATALOG_SHADOW_DEDUP_CAPACITY", "512"),
        ],
        || ZippyConfig::load_from_path(Some(&config_path)),
    )
    .unwrap();
    assert_eq!(from_env.catalog.shadow.queue_capacity, 24);
    assert_eq!(from_env.catalog.shadow.metadata_budget_bytes, 67_108_864);
    assert_eq!(from_env.catalog.shadow.max_files_per_comparison, 4_096);
    assert_eq!(
        from_env.catalog.shadow.max_metadata_bytes_per_comparison,
        8_388_608
    );
    assert_eq!(from_env.catalog.shadow.comparison_timeout_ms, 7_500);
    assert_eq!(from_env.catalog.shadow.dedup_capacity, 512);

    let error = with_env(
        &[
            ("ZIPPY_CATALOG_SHADOW_METADATA_BUDGET_BYTES", "1024"),
            (
                "ZIPPY_CATALOG_SHADOW_MAX_METADATA_BYTES_PER_COMPARISON",
                "2048",
            ),
        ],
        || ZippyConfig::load_from_path(None::<&std::path::Path>),
    )
    .expect_err("per-comparison shadow budget must fit the total budget");
    assert!(error
        .to_string()
        .contains("per-comparison metadata budget must not exceed total budget"));
}

#[test]
fn zippy_config_rejects_sqlite_shadow_without_native_storage() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[catalog]
backend = "sqlite_shadow"
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path)))
        .expect_err("shadow mode must not run without native storage");
    assert!(error
        .to_string()
        .contains("sqlite_shadow backend requires server native_storage_enabled"));
}

#[test]
fn zippy_config_rejects_sqlite_authority_before_cutover() {
    let error = with_env(
        &[
            ("ZIPPY_SERVER_NATIVE_STORAGE", "true"),
            ("ZIPPY_CATALOG_BACKEND", "sqlite"),
        ],
        || ZippyConfig::load_from_path(None::<&std::path::Path>),
    )
    .expect_err("sqlite authority must remain closed until cutover");
    assert!(error
        .to_string()
        .contains("sqlite authority is not available before explicit cutover"));
}

#[test]
fn zippy_config_rejects_unknown_catalog_backend_environment_value() {
    let error = with_env(&[("ZIPPY_CATALOG_BACKEND", "dual")], || {
        ZippyConfig::load_from_path(None::<&std::path::Path>)
    })
    .expect_err("unknown Catalog backend must be rejected");
    assert!(error
        .to_string()
        .contains("name=[ZIPPY_CATALOG_BACKEND] value=[dual]"));
}

#[test]
fn zippy_config_merges_native_storage_toml_and_environment_override() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]
native_storage_enabled = true
"#,
    )
    .unwrap();

    let from_toml = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();
    assert!(from_toml.server.native_storage_enabled);

    let from_env = with_env(&[("ZIPPY_SERVER_NATIVE_STORAGE", "false")], || {
        ZippyConfig::load_from_path(Some(&config_path))
    })
    .unwrap();
    assert!(!from_env.server.native_storage_enabled);
}

#[test]
fn zippy_config_rejects_invalid_native_storage_environment_value() {
    let error = with_env(&[("ZIPPY_SERVER_NATIVE_STORAGE", "enabled")], || {
        ZippyConfig::load_from_path(None::<&std::path::Path>)
    })
    .expect_err("native storage environment value must be a supported boolean");

    assert!(error
        .to_string()
        .contains("env var must parse as bool name=[ZIPPY_SERVER_NATIVE_STORAGE] value=[enabled]"));
}

#[test]
fn zippy_config_serializes_native_storage_and_round_trips_equality() {
    let mut config = ZippyConfig::default();
    config.server.native_storage_enabled = true;

    let value = config.to_json_value();
    assert_eq!(value["server"]["native_storage_enabled"], true);
    assert_eq!(value["runtime"]["max_cpu_cores"], 4);
    assert_eq!(value["runtime"]["max_parallelism"], 4);
    assert_eq!(value["query"]["plan_ttl_ms"], 30000);

    let decoded = serde_json::from_value::<ZippyConfig>(value.clone()).unwrap();
    assert_eq!(decoded, config);

    let mut legacy_value = value;
    legacy_value["query"]
        .as_object_mut()
        .expect("query config object")
        .remove("plan_ttl_ms");
    let legacy_decoded = serde_json::from_value::<ZippyConfig>(legacy_value).unwrap();
    assert_eq!(legacy_decoded.query.plan_ttl_ms, 30000);
}

#[test]
fn zippy_config_wire_json_redacts_master_token() {
    let mut config = ZippyConfig::default();
    config.master.token = Some("must-not-leak".to_string());

    let value = config.to_json_value();

    assert!(value["master"].get("token").is_none());
}

#[test]
fn zippy_config_expands_explicit_server_data_dir() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]
data_dir = "~/.zippy/database"
"#,
    )
    .unwrap();

    let config = with_env(&[("HOME", "/tmp/zippy-config-home")], || {
        ZippyConfig::load_from_path(Some(&config_path))
    })
    .unwrap();

    assert_eq!(
        config.server_data_dir("unused"),
        std::path::PathBuf::from("/tmp/zippy-config-home/.zippy/database")
    );
}

#[test]
fn zippy_config_resolves_relative_server_data_dir_from_config_parent() {
    let temp = tempfile::tempdir().unwrap();
    let config_dir = temp.path().join("config");
    fs::create_dir_all(&config_dir).unwrap();
    let config_path = config_dir.join("zippy.toml");
    fs::write(
        &config_path,
        r#"
[server]
data_dir = "../database"
"#,
    )
    .unwrap();

    let config = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();

    assert_eq!(
        config.server_data_dir("unused"),
        config_dir.join("../database")
    );
}

#[test]
fn zippy_config_rejects_relative_server_data_dir_from_environment() {
    let error = with_env(&[("ZIPPY_SERVER_DATA_DIR", "relative/database")], || {
        ZippyConfig::load_from_path(None::<&std::path::Path>)
    })
    .expect_err("environment paths must not depend on process cwd");

    assert!(error.to_string().contains("env path must be absolute"));
}

#[test]
fn zippy_runtime_config_deserializes_cpu_aliases_without_buffer_budget() {
    for value in [
        serde_json::json!({"max_cpu_cores": 4}),
        serde_json::json!({"max_parallelism": 4}),
        serde_json::json!({"max_cpu_cores": 4, "max_parallelism": 4}),
    ] {
        let runtime = serde_json::from_value::<ZippyRuntimeConfig>(value).unwrap();
        assert_eq!(runtime.max_cpu_cores, 4);
        assert_eq!(runtime.max_query_buffer_bytes, 512 * 1024 * 1024);
    }
}

#[test]
fn zippy_runtime_config_rejects_conflicting_cpu_aliases() {
    let error = serde_json::from_value::<ZippyRuntimeConfig>(serde_json::json!({
        "max_cpu_cores": 4,
        "max_parallelism": 3,
    }))
    .unwrap_err();

    assert!(error.to_string().contains(
        "conflicting runtime CPU limits layer=[serialized runtime] max_cpu_cores=[4] max_parallelism=[3]"
    ));
}

#[test]
fn zippy_runtime_config_rejects_null_cpu_aliases() {
    for value in [
        serde_json::json!({"max_cpu_cores": null, "max_parallelism": 4}),
        serde_json::json!({"max_cpu_cores": 4, "max_parallelism": null}),
    ] {
        let error = serde_json::from_value::<ZippyRuntimeConfig>(value).unwrap_err();
        assert!(error.to_string().contains("invalid type: null"));
    }
}

#[test]
fn zippy_config_accepts_equal_runtime_cpu_aliases_in_toml() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[runtime]
max_cpu_cores = 6
max_parallelism = 6
"#,
    )
    .unwrap();

    let config = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();

    assert_eq!(config.runtime.max_cpu_cores, 6);
}

#[test]
fn zippy_config_rejects_conflicting_runtime_cpu_aliases_in_toml() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[runtime]
max_cpu_cores = 6
max_parallelism = 5
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error.to_string().contains(
        "conflicting runtime CPU limits layer=[TOML [runtime]] max_cpu_cores=[6] max_parallelism=[5]"
    ));
}

#[test]
fn zippy_config_legacy_cpu_environment_alias_overrides_toml_canonical_value() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[runtime]
max_cpu_cores = 6
"#,
    )
    .unwrap();

    let config = with_env(&[("ZIPPY_MAX_PARALLELISM", "7")], || {
        ZippyConfig::load_from_path(Some(&config_path))
    })
    .unwrap();

    assert_eq!(config.runtime.max_cpu_cores, 7);
}

#[test]
fn zippy_config_rejects_conflicting_runtime_cpu_aliases_in_environment() {
    let error = with_env(
        &[("ZIPPY_MAX_CPU_CORES", "6"), ("ZIPPY_MAX_PARALLELISM", "5")],
        || ZippyConfig::load_from_path(None::<&std::path::Path>),
    )
    .unwrap_err();

    assert!(error.to_string().contains(
        "conflicting runtime CPU limits layer=[environment] max_cpu_cores=[6] max_parallelism=[5]"
    ));
}

#[test]
fn zippy_config_accepts_equal_runtime_cpu_aliases_in_environment() {
    let config = with_env(
        &[("ZIPPY_MAX_CPU_CORES", "6"), ("ZIPPY_MAX_PARALLELISM", "6")],
        || ZippyConfig::load_from_path(None::<&std::path::Path>),
    )
    .unwrap();

    assert_eq!(config.runtime.max_cpu_cores, 6);
}

#[test]
fn zippy_config_derives_tasks_for_high_legacy_runtime_cpu_limit() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(&config_path, "[runtime]\nmax_parallelism = 32\n").unwrap();

    let config = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();

    assert_eq!(config.runtime.max_cpu_cores, 32);
    assert_eq!(config.query_parallelism(), 32);
    assert_eq!(config.query.resources.max_active_tasks, 128);
}

#[test]
fn zippy_config_derives_tasks_from_tighter_query_parallelism() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[runtime]
max_cpu_cores = 32

[query]
parallelism = 3
"#,
    )
    .unwrap();

    let config = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();

    assert_eq!(config.query_parallelism(), 3);
    assert_eq!(config.query.resources.max_active_tasks, 12);
}

#[test]
fn zippy_config_environment_explicit_task_limit_overrides_toml_value() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[runtime]
max_cpu_cores = 8

[query.resources]
max_active_tasks = 32
"#,
    )
    .unwrap();

    let config = with_env(&[("ZIPPY_QUERY_MAX_ACTIVE_TASKS", "48")], || {
        ZippyConfig::load_from_path(Some(&config_path))
    })
    .unwrap();

    assert_eq!(config.query.resources.max_active_tasks, 48);
}

#[test]
fn zippy_config_query_parallelism_inherits_runtime_limit() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[runtime]
max_parallelism = 7
max_query_buffer_bytes = 67108864
"#,
    )
    .unwrap();

    let config = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();

    assert_eq!(config.query.parallelism, None);
    assert_eq!(config.runtime.max_cpu_cores, 7);
    assert_eq!(config.query_parallelism(), 7);
    assert_eq!(config.query.max_buffered_bytes, None);
    assert_eq!(config.query_max_buffered_bytes(), 67108864);
}

#[test]
fn zippy_config_rejects_query_parallelism_above_runtime_limit() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[query]
parallelism = 5
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("query parallelism must be between 1 and runtime max_cpu_cores"));
}

#[test]
fn zippy_config_rejects_invalid_runtime_max_cpu_cores() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[runtime]
max_cpu_cores = 0
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("runtime max_cpu_cores must be between 1 and 256"));
}

#[test]
fn zippy_config_rejects_zero_native_query_resource_limits() {
    let cases = [
        (
            "max_concurrent_queries",
            "query resources max_concurrent_queries must be between 1 and 4096 value=[0]",
        ),
        (
            "max_active_tasks",
            "query resources max_active_tasks must be greater than zero",
        ),
        (
            "max_memory_bytes",
            "query resources max_memory_bytes must be greater than zero",
        ),
    ];

    for (field, message) in cases {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("config.toml");
        fs::write(&config_path, format!("[query.resources]\n{field} = 0\n")).unwrap();

        let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

        assert!(error.to_string().contains(message), "field={field}");
    }
}

#[test]
fn zippy_config_loads_native_query_canary_toml_and_env_overrides() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]
native_storage_enabled = true

[query.canary]
enabled = true
max_rows = 1200
max_batches = 12
max_output_bytes = 1048576
operator_memory_bytes = 2097152
"#,
    )
    .unwrap();

    let config = with_env(
        &[
            ("ZIPPY_QUERY_CANARY_MAX_ROWS", "2400"),
            ("ZIPPY_QUERY_CANARY_MAX_BATCHES", "24"),
            ("ZIPPY_QUERY_CANARY_MAX_OUTPUT_BYTES", "2097152"),
            ("ZIPPY_QUERY_CANARY_OPERATOR_MEMORY_BYTES", "4194304"),
        ],
        || ZippyConfig::load_from_path(Some(&config_path)),
    )
    .unwrap();

    assert!(config.query.canary.enabled);
    assert_eq!(config.query.canary.max_rows, 2400);
    assert_eq!(config.query.canary.max_batches, 24);
    assert_eq!(config.query.canary.max_output_bytes, 2097152);
    assert_eq!(config.query.canary.operator_memory_bytes, Some(4194304));
    assert_eq!(config.query_canary_operator_memory_bytes(), 4194304);
}

#[test]
fn zippy_config_rejects_enabled_query_canary_without_native_storage() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(&config_path, "[query.canary]\nenabled = true\n").unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("query canary requires server native_storage_enabled"));
}

#[test]
fn zippy_config_loads_native_query_frontend_gate() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]
native_storage_enabled = true

[catalog]
backend = "sqlite_shadow"

[gateway]
enabled = true
endpoint = "127.0.0.1:17666"

[query]
native_enabled = true
operator_memory_bytes = 16777216
"#,
    )
    .unwrap();

    let config = with_env(&[("ZIPPY_QUERY_OPERATOR_MEMORY_BYTES", "33554432")], || {
        ZippyConfig::load_from_path(Some(&config_path))
    })
    .unwrap();

    assert!(config.query.native_enabled);
    assert_eq!(config.query.operator_memory_bytes, Some(33554432));
    assert_eq!(config.query_operator_memory_bytes(), 33554432);
}

#[test]
fn zippy_config_rejects_invalid_native_query_operator_memory() {
    for (value, expected) in [
        (0, "query operator_memory_bytes must be greater than zero"),
        (
            2_097_152,
            "query operator_memory_bytes must not exceed query resources memory",
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("config.toml");
        fs::write(
            &config_path,
            format!(
                "[query]\noperator_memory_bytes = {value}\n\
                 [query.resources]\nmax_memory_bytes = 1048576\n"
            ),
        )
        .unwrap();

        let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();
        assert!(error.to_string().contains(expected), "value={value}");
    }
}

#[test]
fn zippy_config_rejects_incomplete_native_query_frontend_dependencies() {
    let cases = [
        (
            r#"
[query]
native_enabled = true
"#,
            "native query frontend requires server native_storage_enabled",
        ),
        (
            r#"
[server]
native_storage_enabled = true

[catalog]
backend = "sqlite_shadow"

[query]
native_enabled = true
"#,
            "native query frontend requires gateway enabled",
        ),
        (
            r#"
[server]
native_storage_enabled = true

[gateway]
enabled = true
endpoint = "127.0.0.1:17666"

[query]
native_enabled = true
"#,
            "native query frontend requires catalog sqlite_shadow backend",
        ),
    ];

    for (source, expected) in cases {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("config.toml");
        fs::write(&config_path, source).unwrap();

        let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

        assert!(error.to_string().contains(expected), "source={source}");
    }
}

#[test]
fn zippy_config_rejects_invalid_query_canary_limits() {
    let cases = [
        (
            "max_rows",
            "query canary max_rows must be greater than zero",
        ),
        (
            "max_batches",
            "query canary max_batches must be greater than zero",
        ),
        (
            "max_output_bytes",
            "query canary max_output_bytes must be greater than zero",
        ),
        (
            "operator_memory_bytes",
            "query canary operator_memory_bytes must be greater than zero",
        ),
    ];

    for (field, message) in cases {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("config.toml");
        fs::write(&config_path, format!("[query.canary]\n{field} = 0\n")).unwrap();

        let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

        assert!(error.to_string().contains(message), "field={field}");
    }
}

#[test]
fn zippy_config_rejects_canary_operator_memory_above_query_budget() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[query.resources]
max_memory_bytes = 1048576

[query.canary]
operator_memory_bytes = 2097152
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error.to_string().contains(
        "operator_memory_bytes must not exceed query resources memory operator_memory_bytes=[2097152] max_memory_bytes=[1048576]"
    ));
}

#[test]
fn zippy_config_rejects_zero_query_plan_ttl() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(&config_path, "[query]\nplan_ttl_ms = 0\n").unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("query plan_ttl_ms must be between 1 and 86400000 value=[0]"));
}

#[test]
fn zippy_config_rejects_query_plan_ttl_above_one_day() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        format!("[query]\nplan_ttl_ms = {}\n", MAX_QUERY_PLAN_TTL_MS + 1),
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("query plan_ttl_ms must be between 1 and 86400000 value=[86400001]"));
}

#[test]
fn zippy_config_rejects_native_query_concurrency_above_registry_capacity() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        "[query.resources]\nmax_concurrent_queries = 4097\n",
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("max_concurrent_queries must be between 1 and 4096 value=[4097]"));
}

#[test]
fn zippy_config_rejects_native_task_budget_below_effective_query_parallelism() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[runtime]
max_cpu_cores = 8

[query]
parallelism = 6

[query.resources]
max_active_tasks = 5
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error.to_string().contains(
        "max_active_tasks must not be less than effective query parallelism value=[5] query_parallelism=[6]"
    ));
}

#[test]
fn zippy_config_rejects_query_buffer_budget_above_runtime_limit() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[runtime]
max_query_buffer_bytes = 67108864

[query]
max_buffered_bytes = 134217728
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("query max_buffered_bytes must not exceed runtime max_query_buffer_bytes"));
}

#[test]
fn zippy_config_builds_gateway_endpoint_from_host_and_port() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[gateway]
enabled = true
host = "0.0.0.0"
port = 17666
"#,
    )
    .unwrap();

    let config = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();

    assert_eq!(config.gateway.endpoint.as_deref(), Some("0.0.0.0:17666"));
}

#[test]
fn zippy_config_derives_gateway_endpoint_from_master_port() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[master]
host = "127.0.0.1"
port = 17690

[gateway]
enabled = true
"#,
    )
    .unwrap();

    let config = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();

    assert_eq!(config.gateway.endpoint.as_deref(), Some("127.0.0.1:17691"));
}

#[test]
fn zippy_config_does_not_derive_disabled_gateway_endpoint() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[master]
host = "127.0.0.1"
port = 17690
"#,
    )
    .unwrap();

    let config = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();

    assert!(!config.gateway.enabled);
    assert_eq!(config.gateway.endpoint, None);
    assert_eq!(config.gateway.host, None);
    assert_eq!(config.gateway.port, None);
}

#[test]
fn zippy_config_rejects_enabled_gateway_without_endpoint_or_master_port() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[gateway]
enabled = true
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("gateway endpoint must be set when enabled or derivable from master port"));
}

#[test]
fn zippy_config_rejects_compaction_source_deletion_without_snapshot_leases() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[table.persist.compaction]
enabled = true
delete_sources = true
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("delete_sources requires persisted snapshot leases"));
}

#[test]
fn zippy_config_rejects_legacy_partition_dt_part_format() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[table.persist.partition]
dt_column = "dt"
dt_part = "YYYYMM"
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("unsupported table persist partition dt_part"));
}

#[test]
fn zippy_config_rejects_invalid_persist_method_when_enabled() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[table.persist]
enabled = true
method = "csv"
"#,
    )
    .unwrap();

    let error = ZippyConfig::load_from_path(Some(&config_path)).unwrap_err();

    assert!(error
        .to_string()
        .contains("unsupported table persist method"));
}

#[test]
fn zippy_config_loads_default_home_config_path() {
    let temp = tempfile::tempdir().unwrap();
    let config_dir = temp.path().join(".zippy");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(
        config_dir.join("config.toml"),
        r#"
[table]
row_capacity = 4096
"#,
    )
    .unwrap();

    with_env(&[("HOME", temp.path().to_str().unwrap())], || {
        let config = ZippyConfig::load_default().unwrap();

        assert_eq!(config.table.row_capacity, 4096);
    });
}

fn with_env<T>(vars: &[(&str, &str)], body: impl FnOnce() -> T) -> T {
    let _guard = env_lock().lock().unwrap();
    let previous = CONFIG_ENV_KEYS
        .iter()
        .map(|key| (*key, std::env::var_os(key)))
        .collect::<Vec<_>>();
    for key in CONFIG_ENV_KEYS {
        std::env::remove_var(key);
    }
    for (key, value) in vars {
        std::env::set_var(key, value);
    }

    let result = body();

    for (key, value) in previous {
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }
    result
}

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
