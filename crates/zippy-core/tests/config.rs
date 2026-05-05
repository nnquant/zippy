use std::fs;
use std::sync::{Mutex, OnceLock};

use zippy_core::ZippyConfig;

const CONFIG_ENV_KEYS: &[&str] = &[
    "HOME",
    "ZIPPY_LOG_LEVEL",
    "ZIPPY_TABLE_ROW_CAPACITY",
    "ZIPPY_TABLE_RETENTION_SEGMENTS",
    "ZIPPY_TABLE_PERSIST",
    "ZIPPY_TABLE_PERSIST_METHOD",
    "ZIPPY_TABLE_PERSIST_DATA_DIR",
    "ZIPPY_TABLE_PERSIST_PARTITION_DT_COLUMN",
    "ZIPPY_TABLE_PERSIST_PARTITION_ID_COLUMN",
    "ZIPPY_TABLE_PERSIST_PARTITION_DT_PART",
    "ZIPPY_REMOTE_GATEWAY",
    "ZIPPY_REMOTE_GATEWAY_ENDPOINT",
    "ZIPPY_REMOTE_GATEWAY_TOKEN",
    "ZIPPY_REMOTE_GATEWAY_PROTOCOL_VERSION",
];

#[test]
fn zippy_config_loads_toml_and_env_overrides() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[log]
level = "debug"

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

[remote_gateway]
enabled = false
endpoint = "127.0.0.1:17666"
token = "from-file-token"
protocol_version = 1
"#,
    )
    .unwrap();

    with_env(
        &[
            ("ZIPPY_LOG_LEVEL", "warn"),
            ("ZIPPY_TABLE_ROW_CAPACITY", "2048"),
            ("ZIPPY_TABLE_RETENTION_SEGMENTS", "5"),
            ("ZIPPY_TABLE_PERSIST", "true"),
            ("ZIPPY_TABLE_PERSIST_DATA_DIR", "from-env"),
            ("ZIPPY_TABLE_PERSIST_PARTITION_DT_COLUMN", "recv_ts"),
            ("ZIPPY_TABLE_PERSIST_PARTITION_ID_COLUMN", "symbol"),
            ("ZIPPY_TABLE_PERSIST_PARTITION_DT_PART", "%Y%m"),
            ("ZIPPY_REMOTE_GATEWAY", "true"),
            ("ZIPPY_REMOTE_GATEWAY_ENDPOINT", "127.0.0.1:27666"),
            ("ZIPPY_REMOTE_GATEWAY_TOKEN", "from-env-token"),
            ("ZIPPY_REMOTE_GATEWAY_PROTOCOL_VERSION", "2"),
        ],
        || {
            let config = ZippyConfig::load_from_path(Some(&config_path)).unwrap();

            assert_eq!(config.log.level, "warn");
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
            assert!(config.remote_gateway.enabled);
            assert_eq!(
                config.remote_gateway.endpoint.as_deref(),
                Some("127.0.0.1:27666")
            );
            assert_eq!(
                config.remote_gateway.token.as_deref(),
                Some("from-env-token")
            );
            assert_eq!(config.remote_gateway.protocol_version, 2);
        },
    );
}

#[test]
fn zippy_config_rejects_enabled_remote_gateway_without_endpoint() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[remote_gateway]
enabled = true
"#,
    )
    .unwrap();

    let error = with_env(&[], || ZippyConfig::load_from_path(Some(&config_path))).unwrap_err();

    assert!(error
        .to_string()
        .contains("remote_gateway endpoint must be set when enabled"));
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
