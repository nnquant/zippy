use std::fs;
use std::sync::{Mutex, OnceLock};

use zippy_core::{
    ZippyConfig, ZippyRuntimeConfig, DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_PINS,
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
    "ZIPPY_FLIGHT_ENABLED",
    "ZIPPY_FLIGHT_ENDPOINT",
    "ZIPPY_FLIGHT_PROTOCOL_VERSION",
    "ZIPPY_FLIGHT_MAX_MESSAGE_BYTES",
    "ZIPPY_FLIGHT_MAX_PENDING_TICKETS",
    "ZIPPY_FLIGHT_TLS_ENABLED",
    "ZIPPY_FLIGHT_TLS_CERT_FILE",
    "ZIPPY_FLIGHT_TLS_KEY_FILE",
    "ZIPPY_FLIGHT_TLS_CLIENT_CA_FILE",
    "ZIPPY_FLIGHT_TLS_REQUIRE_CLIENT_CERTIFICATE",
    "ZIPPY_FLIGHT_AUTH_ENABLED",
    "ZIPPY_FLIGHT_AUTH_TOKEN_FILE",
    "ZIPPY_FLIGHT_AUTH_TOKEN",
    "ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN_FILE",
    "ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN",
    "ZIPPY_FLIGHT_AUTH_WRITER_TOKEN_FILE",
    "ZIPPY_FLIGHT_AUTH_WRITER_TOKEN",
    "ZIPPY_FLIGHT_AUTH_SESSION_TTL_MS",
    "ZIPPY_FLIGHT_AUTH_MAX_SESSIONS",
    "ZIPPY_FLIGHT_METADATA_MAX_ROWS",
    "ZIPPY_FLIGHT_METADATA_MAX_BYTES",
    "ZIPPY_FLIGHT_METADATA_TICKET_TTL_MS",
    "ZIPPY_FLIGHT_INGEST_ENABLED",
    "ZIPPY_FLIGHT_INGEST_MAX_CONCURRENT_PUTS",
    "ZIPPY_FLIGHT_INGEST_MAX_BATCHES_PER_PUT",
    "ZIPPY_FLIGHT_INGEST_MAX_ROWS_PER_BATCH",
    "ZIPPY_FLIGHT_INGEST_MAX_ROWS_PER_PUT",
    "ZIPPY_FLIGHT_INGEST_MAX_BATCH_BYTES",
    "ZIPPY_FLIGHT_INGEST_MAX_BYTES_PER_PUT",
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

[log]
level = "debug"

[runtime]
max_cpu_cores = 10
max_query_buffer_bytes = 268435456

[query]
parallelism = 6
max_buffered_bytes = 67108864
plan_ttl_ms = 45000

[flight]
max_pending_tickets = 16

[query.resources]
max_concurrent_queries = 32
max_active_tasks = 40
max_memory_bytes = 402653184

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
            ("ZIPPY_LOG_LEVEL", "warn"),
            ("ZIPPY_MAX_CPU_CORES", "9"),
            ("ZIPPY_MAX_QUERY_BUFFER_BYTES", "805306368"),
            ("ZIPPY_QUERY_MAX_ACTIVE_TASKS", "36"),
            ("ZIPPY_QUERY_MAX_BUFFERED_BYTES", "100663296"),
            ("ZIPPY_QUERY_MAX_CONCURRENT_QUERIES", "24"),
            ("ZIPPY_QUERY_MAX_MEMORY_BYTES", "603979776"),
            ("ZIPPY_QUERY_PARALLELISM", "8"),
            ("ZIPPY_QUERY_PLAN_TTL_MS", "20000"),
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
            "flight": {"max_pending_tickets": 20},
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
fn zippy_config_json_overlay_rejects_removed_cpu_alias() {
    let base = std::env::temp_dir().join("zippy-json-overlay-base");
    for runtime in [
        serde_json::json!({"max_parallelism": 7}),
        serde_json::json!({"max_cpu_cores": 7, "max_parallelism": 7}),
    ] {
        let error = ZippyConfig::from_json_overlay(serde_json::json!({"runtime": runtime}), &base)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("unknown field `max_parallelism`"));
    }
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
        (
            serde_json::json!({"flight": {"endpoint": null}}),
            "flight.endpoint",
        ),
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
            serde_json::json!({"runtime": {"max_cpu_cores": null}}),
            "runtime.max_cpu_cores",
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
        }),
        &base,
    )
    .unwrap();

    assert_eq!(config.server.data_dir, None);
    assert_eq!(config.query.parallelism, None);
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
fn zippy_config_serializes_native_defaults_without_feature_switches() {
    let config = ZippyConfig::default();

    let value = config.to_json_value();
    assert!(value["server"].get("native_storage_enabled").is_none());
    assert!(value["query"].get("native_enabled").is_none());
    assert!(value["catalog"].get("backend").is_none());
    assert_eq!(value["runtime"]["max_cpu_cores"], 4);
    assert!(value["runtime"].get("max_parallelism").is_none());
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
fn zippy_runtime_config_accepts_only_canonical_cpu_config() {
    let runtime =
        serde_json::from_value::<ZippyRuntimeConfig>(serde_json::json!({"max_cpu_cores": 4}))
            .unwrap();
    assert_eq!(runtime.max_cpu_cores, 4);
    assert_eq!(runtime.max_query_buffer_bytes, 512 * 1024 * 1024);
    for value in [
        serde_json::json!({"max_parallelism": 4}),
        serde_json::json!({"max_cpu_cores": 4, "max_parallelism": 4}),
        serde_json::json!({"max_cpu_cores": null}),
    ] {
        assert!(serde_json::from_value::<ZippyRuntimeConfig>(value).is_err());
    }
}

#[test]
fn zippy_config_rejects_removed_runtime_cpu_name_in_toml() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("config.toml");
    for source in [
        "[runtime]\nmax_parallelism = 6\n",
        "[runtime]\nmax_cpu_cores = 6\nmax_parallelism = 6\n",
    ] {
        fs::write(&path, source).unwrap();
        let error = with_env(&[], || ZippyConfig::load_from_path(Some(&path))).unwrap_err();
        assert!(error
            .to_string()
            .contains("unknown field `max_parallelism`"));
    }
}

#[test]
fn zippy_config_rejects_removed_cpu_environment_even_with_canonical_value() {
    for canonical in [None, Some("6")] {
        let mut values = vec![("ZIPPY_MAX_PARALLELISM", "6")];
        if let Some(value) = canonical {
            values.push(("ZIPPY_MAX_CPU_CORES", value));
        }
        let error = with_env(&values, || {
            ZippyConfig::load_from_path(None::<&std::path::Path>)
        })
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("removed environment variable [ZIPPY_MAX_PARALLELISM]"));
    }
}

#[test]
fn zippy_config_derives_tasks_for_high_runtime_cpu_limit() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(&config_path, "[runtime]\nmax_cpu_cores = 32\n").unwrap();

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
max_cpu_cores = 7
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
fn zippy_config_loads_native_query_operator_budget() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]


[catalog]


[flight]
enabled = true
endpoint = "127.0.0.1:17666"

[query]

operator_memory_bytes = 16777216
"#,
    )
    .unwrap();

    let config = with_env(&[("ZIPPY_QUERY_OPERATOR_MEMORY_BYTES", "33554432")], || {
        ZippyConfig::load_from_path(Some(&config_path))
    })
    .unwrap();

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
fn zippy_config_loads_loopback_flight_frontend_and_environment_limits() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]


[catalog]


[query]


[flight]
enabled = true
endpoint = "127.0.0.1:17692"
max_message_bytes = 1048576
max_pending_tickets = 32
"#,
    )
    .unwrap();

    let config = with_env(
        &[
            ("ZIPPY_FLIGHT_MAX_MESSAGE_BYTES", "2097152"),
            ("ZIPPY_FLIGHT_MAX_PENDING_TICKETS", "16"),
        ],
        || ZippyConfig::load_from_path(Some(&config_path)),
    )
    .unwrap();

    assert!(config.flight.enabled);
    assert_eq!(config.flight.endpoint.as_str(), "127.0.0.1:17692");
    assert_eq!(config.flight.max_message_bytes, 2_097_152);
    assert_eq!(config.flight.max_pending_tickets, 16);
}

#[test]
fn zippy_config_keeps_flight_endpoint_independent_from_master() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]


[catalog]


[master]
host = "127.0.0.1"
port = 17690

[query]


[flight]
enabled = true
"#,
    )
    .unwrap();

    let config = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();

    assert_eq!(config.flight.endpoint.as_str(), "127.0.0.1:17667");
}

#[test]
fn zippy_config_rejects_non_loopback_flight_without_tls_and_authentication() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]


[catalog]


[query]


[flight]
enabled = true
endpoint = "0.0.0.0:17692"
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("non-loopback flight endpoint requires both TLS and authentication"));
}

#[test]
fn zippy_config_accepts_non_loopback_flight_with_tls_and_file_authentication() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]


[catalog]


[query]


[flight]
enabled = true
endpoint = "0.0.0.0:17692"

[flight.tls]
enabled = true
cert_file = "credentials/server.pem"
key_file = "credentials/server.key"

[flight.auth]
enabled = true
token_file = "credentials/flight.token"
session_ttl_ms = 60000
max_sessions = 17
"#,
    )
    .unwrap();

    let config = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap();

    assert_eq!(
        config.flight.tls.cert_file.as_deref(),
        Some(temp.path().join("credentials/server.pem").as_path())
    );
    assert_eq!(
        config.flight.auth.token_file.as_deref(),
        Some(temp.path().join("credentials/flight.token").as_path())
    );
    assert_eq!(config.flight.auth.max_sessions, 17);
}

#[test]
fn zippy_config_keeps_environment_flight_token_out_of_debug_and_json() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]


[catalog]


[query]


[flight]
enabled = true
endpoint = "127.0.0.1:17692"

[flight.auth]
enabled = true
"#,
    )
    .unwrap();
    let token = "test-only-flight-bootstrap-token-0001";

    let config = with_env(&[("ZIPPY_FLIGHT_AUTH_TOKEN", token)], || {
        ZippyConfig::load_from_path(Some(&config_path))
    })
    .unwrap();

    assert_eq!(config.flight.auth.bootstrap_token(), Some(token));
    assert!(!format!("{config:?}").contains(token));
    let json = config.to_json_value().to_string();
    assert!(!json.contains(token));
    assert!(!json.contains("bootstrap_token"));
}

#[test]
fn zippy_config_rejects_plaintext_flight_token_in_toml() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[flight.auth]
enabled = true
token = "test-only-flight-bootstrap-token-0001"
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error.to_string().contains("unknown field `token`"));
}

#[test]
fn zippy_config_rejects_flight_session_shorter_than_query_plan() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]


[catalog]


[query]

plan_ttl_ms = 30000

[flight]
enabled = true
endpoint = "127.0.0.1:17692"

[flight.auth]
enabled = true
token_file = "flight.token"
session_ttl_ms = 29999
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("session_ttl_ms must cover query plan and metadata ticket TTLs"));
}

#[test]
fn zippy_config_rejects_local_read_session_shorter_than_query_plan() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[flight.auth]
enabled = false
session_ttl_ms = 1
"#,
    )
    .unwrap();
    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();
    assert!(error
        .to_string()
        .contains("session_ttl_ms must cover query plan and metadata ticket TTLs"));
}

#[test]
fn zippy_config_loads_distinct_admin_auth_and_metadata_environment_limits() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]


[catalog]


[query]


[flight]
enabled = true
endpoint = "127.0.0.1:17692"

[flight.auth]
enabled = true
"#,
    )
    .unwrap();
    let read_token = "test-only-flight-bootstrap-token-0001";
    let admin_token = "test-only-flight-admin-token-00000001";

    let config = with_env(
        &[
            ("ZIPPY_FLIGHT_AUTH_TOKEN", read_token),
            ("ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN", admin_token),
            ("ZIPPY_FLIGHT_METADATA_MAX_ROWS", "512"),
            ("ZIPPY_FLIGHT_METADATA_MAX_BYTES", "1048576"),
            ("ZIPPY_FLIGHT_METADATA_TICKET_TTL_MS", "15000"),
        ],
        || ZippyConfig::load_from_path(Some(&config_path)),
    )
    .unwrap();

    assert_eq!(config.flight.auth.bootstrap_token(), Some(read_token));
    assert_eq!(
        config.flight.auth.admin_bootstrap_token(),
        Some(admin_token)
    );
    assert_eq!(config.flight.metadata.max_rows, 512);
    assert_eq!(config.flight.metadata.max_bytes, 1_048_576);
    assert_eq!(config.flight.metadata.ticket_ttl_ms, 15_000);
    let debug = format!("{config:?}");
    let json = config.to_json_value().to_string();
    assert!(!debug.contains(read_token));
    assert!(!debug.contains(admin_token));
    assert!(!json.contains(read_token));
    assert!(!json.contains(admin_token));
}

#[test]
fn zippy_config_rejects_identical_read_and_admin_credentials() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[flight.auth]
enabled = true
"#,
    )
    .unwrap();
    let token = "test-only-flight-bootstrap-token-0001";

    let error = with_env(
        &[
            ("ZIPPY_FLIGHT_AUTH_TOKEN", token),
            ("ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN", token),
        ],
        || ZippyConfig::load_from_path(Some(&config_path)),
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("read and administrator bootstrap tokens must be distinct"));
}

#[test]
fn zippy_config_loads_explicit_flight_ingest_and_redacts_writer_token() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]


[catalog]


[query]


[flight]
enabled = true
endpoint = "127.0.0.1:17692"

[flight.auth]
enabled = true

[flight.ingest]
enabled = true
"#,
    )
    .unwrap();
    let read_token = "test-only-flight-bootstrap-token-0001";
    let writer_token = "test-only-flight-writer-token-000000001";

    let config = with_env(
        &[
            ("ZIPPY_FLIGHT_AUTH_TOKEN", read_token),
            ("ZIPPY_FLIGHT_AUTH_WRITER_TOKEN", writer_token),
            ("ZIPPY_FLIGHT_INGEST_MAX_CONCURRENT_PUTS", "3"),
            ("ZIPPY_FLIGHT_INGEST_MAX_BATCHES_PER_PUT", "9"),
            ("ZIPPY_FLIGHT_INGEST_MAX_ROWS_PER_BATCH", "1000"),
            ("ZIPPY_FLIGHT_INGEST_MAX_ROWS_PER_PUT", "5000"),
            ("ZIPPY_FLIGHT_INGEST_MAX_BATCH_BYTES", "1048576"),
            ("ZIPPY_FLIGHT_INGEST_MAX_BYTES_PER_PUT", "4194304"),
        ],
        || ZippyConfig::load_from_path(Some(&config_path)),
    )
    .unwrap();

    assert!(config.flight.ingest.enabled);
    assert_eq!(
        config.flight.auth.writer_bootstrap_token(),
        Some(writer_token)
    );
    assert_eq!(config.flight.ingest.max_concurrent_puts, 3);
    assert_eq!(config.flight.ingest.max_batches_per_put, 9);
    assert_eq!(config.flight.ingest.max_rows_per_batch, 1_000);
    assert_eq!(config.flight.ingest.max_rows_per_put, 5_000);
    assert_eq!(config.flight.ingest.max_batch_bytes, 1_048_576);
    assert_eq!(config.flight.ingest.max_bytes_per_put, 4_194_304);
    assert!(!format!("{config:?}").contains(writer_token));
    assert!(!config.to_json_value().to_string().contains(writer_token));
}

#[test]
fn zippy_config_rejects_flight_ingest_without_distinct_writer_credential() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]


[catalog]


[query]


[flight]
enabled = true
endpoint = "127.0.0.1:17692"

[flight.auth]
enabled = true

[flight.ingest]
enabled = true
"#,
    )
    .unwrap();

    let error = with_env(
        &[(
            "ZIPPY_FLIGHT_AUTH_TOKEN",
            "test-only-flight-bootstrap-token-0001",
        )],
        || ZippyConfig::load_from_path(Some(&config_path)),
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("requires exactly one writer bootstrap token source"));
}

#[test]
fn zippy_config_rejects_metadata_result_larger_than_shared_transport_budget() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[server]


[catalog]


[runtime]
max_query_buffer_bytes = 1048576

[query]

max_buffered_bytes = 1048576

[flight]
enabled = true
endpoint = "127.0.0.1:17692"
max_message_bytes = 1048576

[flight.metadata]
max_bytes = 2097152
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("metadata max_bytes must not exceed the shared transport budget"));
}

#[test]
fn zippy_config_rejects_removed_compaction_configuration() {
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

    assert!(error.to_string().contains("unknown field `table`"));
}

#[test]
fn zippy_config_loads_default_home_config_path() {
    let temp = tempfile::tempdir().unwrap();
    let config_dir = temp.path().join(".zippy");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(
        config_dir.join("config.toml"),
        r#"
[query]
plan_ttl_ms = 45000
"#,
    )
    .unwrap();

    with_env(&[("HOME", temp.path().to_str().unwrap())], || {
        let config = ZippyConfig::load_default().unwrap();

        assert_eq!(config.query.plan_ttl_ms, 45000);
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

#[test]
fn zippy_config_rejects_removed_catalog_shadow_configuration() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("config.toml");
    for source in [
        "[catalog]\nbackend = \"sqlite_shadow\"\n",
        "[catalog.shadow]\nqueue_capacity = 4\n",
    ] {
        fs::write(&path, source).unwrap();
        assert!(with_env(&[], || ZippyConfig::load_from_path(Some(&path))).is_err());
    }
    fs::write(&path, "").unwrap();
    for (key, value) in [
        ("ZIPPY_CATALOG_BACKEND", "sqlite_shadow"),
        ("ZIPPY_CATALOG_SHADOW_QUEUE_CAPACITY", "4"),
    ] {
        assert!(with_env(&[(key, value)], || ZippyConfig::load_from_path(Some(&path))).is_err());
    }
}

#[test]
fn zippy_config_rejects_removed_endpoint_snapshot_import() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("config.toml");
    fs::write(&path, "[server]\nimport_legacy_endpoint_snapshot = false\n").unwrap();
    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&path))).unwrap_err();
    assert!(error.to_string().contains("unknown field"));
    fs::write(&path, "").unwrap();
    for value in ["true", "false"] {
        let error = with_env(&[("ZIPPY_SERVER_IMPORT_LEGACY_SNAPSHOT", value)], || {
            ZippyConfig::load_from_path(Some(&path))
        })
        .unwrap_err();
        assert!(error.to_string().contains("removed environment variable"));
    }
}

#[test]
fn zippy_config_rejects_removed_gateway_configuration() {
    for value in ["false", "true"] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("removed.toml");
        fs::write(&path, format!("[gateway]\nenabled = {value}\n")).unwrap();
        let error = with_env(&[], || ZippyConfig::load_from_path(Some(&path))).unwrap_err();
        assert!(error.to_string().contains("unknown field `gateway`"));
    }
    for name in [
        "ZIPPY_GATEWAY",
        "ZIPPY_GATEWAY_ENDPOINT",
        "ZIPPY_GATEWAY_TOKEN",
    ] {
        let error = with_env(&[(name, "removed")], || {
            ZippyConfig::load_from_path(None::<&std::path::Path>)
        })
        .unwrap_err();
        assert!(error.to_string().contains("removed gateway configuration"));
    }
}

#[test]
fn zippy_config_rejects_unknown_fields_in_every_nested_section() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("config.toml");
    for (section, field) in [
        ("catalog.runtime_snapshot_pins", "old_pin_limit"),
        ("query.resources", "old_memory_limit"),
        ("query.canary", "old_enabled"),
        ("log", "old_level"),
    ] {
        fs::write(&path, format!("[{section}]\n{field} = 1\n")).unwrap();
        let error = with_env(&[], || ZippyConfig::load_from_path(Some(&path))).unwrap_err();
        assert!(
            error.to_string().contains("unknown field"),
            "{section}: {error}"
        );
        let mut overlay = serde_json::json!({});
        overlay[section.split('.').next().unwrap()] = serde_json::json!({});
        let mut cursor = &mut overlay;
        for part in section.split('.') {
            cursor = &mut cursor[part];
        }
        cursor[field] = serde_json::json!(1);
        assert!(ZippyConfig::from_json_overlay(overlay, temp.path()).is_err());
    }

    for (section, field) in [
        ("catalog", "removed_pin_policy"),
        ("catalog.runtime_snapshot_pins", "removed_pin_limit"),
        ("log", "removed_sink"),
        ("flight", "removed_gateway_fallback"),
        ("flight.tls", "removed_tls_mode"),
        ("flight.auth", "bootstrap_token"),
        ("flight.metadata", "removed_metadata_limit"),
        ("flight.ingest", "removed_ingest_route"),
        ("query.resources", "removed_query_budget"),
        ("query.canary", "removed_canary_mode"),
    ] {
        let mut value = ZippyConfig::default().to_json_value();
        let mut cursor = &mut value;
        for part in section.split('.') {
            cursor = &mut cursor[part];
        }
        cursor[field] = serde_json::json!(1);
        assert!(
            serde_json::from_value::<ZippyConfig>(value).is_err(),
            "direct config accepted [{section}.{field}]"
        );
    }
}

#[test]
fn zippy_config_rejects_removed_compaction_environment() {
    for key in [
        "ZIPPY_TABLE_PERSIST_COMPACTION",
        "ZIPPY_TABLE_PERSIST_COMPACTION_INTERVAL_SEC",
        "ZIPPY_TABLE_PERSIST_COMPACTION_SORT_COLUMN",
    ] {
        let error = with_env(&[(key, "unused")], || {
            ZippyConfig::load_from_path(None::<&std::path::Path>)
        })
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("removed legacy table configuration"));
    }
}

#[test]
fn zippy_config_native_defaults_enable_one_explicit_flight_endpoint() {
    let config = with_env(&[], || {
        ZippyConfig::load_from_path(None::<&std::path::Path>)
    })
    .unwrap();
    assert!(config.flight.enabled);
    assert_eq!(config.flight.endpoint, "127.0.0.1:17667");
    let json = config.to_json_value();
    assert!(json["catalog"].get("backend").is_none());
    assert!(json["server"].get("native_storage_enabled").is_none());
    assert!(json["query"].get("native_enabled").is_none());
}

#[test]
fn zippy_config_rejects_removed_backend_and_native_switches() {
    for (section, field, values) in [
        (
            "server",
            "native_storage_enabled",
            vec![serde_json::json!(true), serde_json::json!(false)],
        ),
        (
            "query",
            "native_enabled",
            vec![serde_json::json!(true), serde_json::json!(false)],
        ),
        (
            "catalog",
            "backend",
            vec![
                serde_json::json!("legacy"),
                serde_json::json!("sqlite"),
                serde_json::json!("sqlite_shadow"),
            ],
        ),
    ] {
        for value in values {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("removed.toml");
            fs::write(&path, format!("[{section}]\n{field} = {value}\n")).unwrap();
            let error = with_env(&[], || ZippyConfig::load_from_path(Some(&path))).unwrap_err();
            assert!(error.to_string().contains("unknown field"));
            let mut overlay = serde_json::json!({});
            overlay[section] = serde_json::json!({});
            overlay[section][field] = value.clone();
            assert!(ZippyConfig::from_json_overlay(overlay, temp.path()).is_err());
            let mut wire = ZippyConfig::default().to_json_value();
            wire[section][field] = value;
            assert!(serde_json::from_value::<ZippyConfig>(wire).is_err());
        }
    }
    for key in [
        "ZIPPY_SERVER_NATIVE_STORAGE",
        "ZIPPY_QUERY_NATIVE_ENABLED",
        "ZIPPY_CATALOG_BACKEND",
    ] {
        for value in ["true", "false", "legacy", "sqlite"] {
            let error = with_env(&[(key, value)], || {
                ZippyConfig::load_from_path(None::<&std::path::Path>)
            })
            .unwrap_err();
            assert!(error
                .to_string()
                .contains("removed backend or native feature switch"));
        }
    }
}

#[test]
fn zippy_config_rejects_removed_table_and_master_token_configuration() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("config.toml");
    for (toml, overlay) in [
        (
            "[table]\nrow_capacity = 4096\n",
            serde_json::json!({"table":{"row_capacity":4096}}),
        ),
        (
            "[table.persist]\nenabled = true\n",
            serde_json::json!({"table":{"persist":{"enabled":true}}}),
        ),
        (
            "[master]\ntoken = \"retired-secret\"\n",
            serde_json::json!({"master":{"token":"retired-secret"}}),
        ),
    ] {
        fs::write(&path, toml).unwrap();
        assert!(with_env(&[], || ZippyConfig::load_from_path(Some(&path))).is_err());
        assert!(ZippyConfig::from_json_overlay(overlay.clone(), temp.path()).is_err());
        let mut wire = ZippyConfig::default().to_json_value();
        for (key, value) in overlay.as_object().unwrap() {
            if key == "master" {
                wire["master"]["token"] = value["token"].clone();
            } else {
                wire[key] = value.clone();
            }
        }
        assert!(serde_json::from_value::<ZippyConfig>(wire).is_err());
    }
    for key in CONFIG_ENV_KEYS
        .iter()
        .copied()
        .filter(|key| key.starts_with("ZIPPY_TABLE_") || *key == "ZIPPY_MASTER_TOKEN")
    {
        assert!(
            with_env(&[(key, "retired")], || ZippyConfig::load_from_path(
                None::<&std::path::Path>
            ))
            .is_err(),
            "{key}"
        );
    }
}
