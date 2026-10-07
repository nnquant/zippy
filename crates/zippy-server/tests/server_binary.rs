#![cfg(unix)]

use std::fs;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use zippy_core::{ControlEndpoint, MasterClient};

#[test]
fn server_binary_writes_startup_logs_and_socket() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let log_dir = temp.path().join("logs");
    let binary = env!("CARGO_BIN_EXE_zippy-server");

    let mut child = Command::new(binary)
        .env("ZIPPY_FLIGHT_ENABLED", "false")
        .arg(&socket_path)
        .arg("--log-dir")
        .arg(&log_dir)
        .arg("--log-level")
        .arg("info")
        .arg("--no-console-log")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let log_file = wait_for_single_log_file(&log_dir);
    let before_socket = fs::read_to_string(&log_file).unwrap();
    assert!(before_socket.contains("\"event\":\"master_start\""));
    assert!(before_socket.contains("\"status\":\"starting\""));

    wait_for_path(&socket_path);
    let after_socket = fs::read_to_string(&log_file).unwrap();
    assert!(after_socket.contains("\"event\":\"master_listening\""));
    assert!(after_socket.contains("\"status\":\"ready\""));

    child.kill().unwrap();
    let _ = child.wait();

    let contents = fs::read_to_string(log_file).unwrap();
    assert!(contents.contains("\"event\":\"master_start\""));
    assert!(contents.contains("\"event\":\"master_listening\""));
    assert!(contents.contains("\"component\":\"master\""));
    assert!(contents.contains("\"status\":\"starting\""));
    assert!(contents.contains("\"status\":\"ready\""));
}

#[test]
fn server_binary_rejects_missing_log_dir_value() {
    let binary = env!("CARGO_BIN_EXE_zippy-server");
    let mut child = Command::new(binary)
        .env("ZIPPY_FLIGHT_ENABLED", "false")
        .arg("--log-dir")
        .arg("--no-console-log")
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();

    let status = wait_for_exit(&mut child, Duration::from_secs(1)).unwrap();
    assert!(!status.success());

    let stderr = read_stderr(&mut child);
    assert!(stderr.contains("missing value for server flag flag=[--log-dir]"));
}

#[test]
fn server_binary_rejects_multiple_control_endpoints() {
    let temp = tempfile::tempdir().unwrap();
    let binary = env!("CARGO_BIN_EXE_zippy-server");
    let mut child = Command::new(binary)
        .env("ZIPPY_FLIGHT_ENABLED", "false")
        .arg(temp.path().join("first.sock"))
        .arg(temp.path().join("second.sock"))
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();

    let status = wait_for_exit(&mut child, Duration::from_secs(1)).unwrap();
    assert!(!status.success());

    let stderr = read_stderr(&mut child);
    assert!(stderr.contains("multiple control endpoint values"));
}

#[test]
fn server_binary_reads_explicit_config_file() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let log_dir = temp.path().join("logs");
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        r#"
[log]
level = "debug"

[query]
plan_ttl_ms = 45000
"#,
    )
    .unwrap();
    let binary = env!("CARGO_BIN_EXE_zippy-server");

    let mut child = Command::new(binary)
        .env("ZIPPY_FLIGHT_ENABLED", "false")
        .arg(&socket_path)
        .arg("--config")
        .arg(&config_path)
        .arg("--log-dir")
        .arg(&log_dir)
        .arg("--no-console-log")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_path(&socket_path);
    let client = MasterClient::connect(&socket_path).unwrap();
    let config = client.get_config().unwrap();

    assert_eq!(config.log.level, "debug");
    assert_eq!(config.query.plan_ttl_ms, 45000);

    send_sigint(&child);
    let exit = child.wait().unwrap();
    assert!(exit.success());
}

#[test]
fn server_binary_serves_tcp_control_endpoint() {
    let temp = tempfile::tempdir().unwrap();
    let addr = unused_loopback_addr();
    let log_dir = temp.path().join("logs");
    let binary = env!("CARGO_BIN_EXE_zippy-server");

    let mut child = Command::new(binary)
        .env("ZIPPY_FLIGHT_ENABLED", "false")
        .arg(format!("tcp://{addr}"))
        .arg("--log-dir")
        .arg(&log_dir)
        .arg("--log-level")
        .arg("info")
        .arg("--no-console-log")
        .env("HOME", temp.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_log_contains(&log_dir, "\"event\":\"master_listening\"");
    let mut client = MasterClient::connect_endpoint(ControlEndpoint::Tcp(addr)).unwrap();
    let process_id = client.register_process("tcp_binary").unwrap();

    assert!(process_id.starts_with("proc_"));
    client.unregister_process().unwrap();

    send_sigint(&child);
    let exit = child.wait().unwrap();
    assert!(exit.success());
}

#[test]
fn server_binary_uses_configured_master_host_and_port() {
    let temp = tempfile::tempdir().unwrap();
    let addr = unused_loopback_addr();
    let log_dir = temp.path().join("logs");
    let config_path = temp.path().join("config.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[master]
host = "127.0.0.1"
port = {}
"#,
            addr.port()
        ),
    )
    .unwrap();
    let binary = env!("CARGO_BIN_EXE_zippy-server");

    let mut child = Command::new(binary)
        .env("ZIPPY_FLIGHT_ENABLED", "false")
        .arg("--config")
        .arg(&config_path)
        .arg("--log-dir")
        .arg(&log_dir)
        .arg("--log-level")
        .arg("info")
        .arg("--no-console-log")
        .env("HOME", temp.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_log_contains(&log_dir, "\"event\":\"master_listening\"");
    let log_file = wait_for_single_log_file(&log_dir);
    let log_contents = fs::read_to_string(log_file).unwrap();
    assert!(log_contents.contains("\"host\":\"127.0.0.1\""));
    assert!(log_contents.contains(&format!("\"port\":\"{}\"", addr.port())));

    let mut client = MasterClient::connect_endpoint(ControlEndpoint::Tcp(addr)).unwrap();
    let process_id = client.register_process("tcp_config_binary").unwrap();
    assert!(process_id.starts_with("proc_"));
    client.unregister_process().unwrap();

    send_sigint(&child);
    let exit = child.wait().unwrap();
    assert!(exit.success());
}

#[test]
fn server_binary_handles_sigint_and_cleans_socket() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let log_dir = temp.path().join("logs");
    let binary = env!("CARGO_BIN_EXE_zippy-server");

    let mut child = Command::new(binary)
        .env("ZIPPY_FLIGHT_ENABLED", "false")
        .arg(&socket_path)
        .arg("--log-dir")
        .arg(&log_dir)
        .arg("--log-level")
        .arg("info")
        .arg("--no-console-log")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_path(&socket_path);

    send_sigint(&child);

    let exit = child.wait().unwrap();
    assert!(exit.success());
    assert!(!socket_path.exists());

    let contents = fs::read_to_string(wait_for_single_log_file(&log_dir)).unwrap();
    assert!(contents.contains("\"event\":\"server_shutdown_requested\""));
    assert!(contents.contains("\"event\":\"master_stopped\""));
}

#[test]
fn server_binary_rejects_second_master_before_rebinding_the_active_data_root() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let first_log_dir = temp.path().join("logs-first");
    let second_log_dir = temp.path().join("logs-second");
    let binary = env!("CARGO_BIN_EXE_zippy-server");

    let mut first = Command::new(binary)
        .env("ZIPPY_FLIGHT_ENABLED", "false")
        .arg(&socket_path)
        .arg("--log-dir")
        .arg(&first_log_dir)
        .arg("--log-level")
        .arg("info")
        .arg("--no-console-log")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_path(&socket_path);

    let mut second = Command::new(binary)
        .env("ZIPPY_FLIGHT_ENABLED", "false")
        .arg(&socket_path)
        .arg("--log-dir")
        .arg(&second_log_dir)
        .arg("--log-level")
        .arg("info")
        .arg("--no-console-log")
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();

    let status = wait_for_exit(&mut second, Duration::from_secs(2)).unwrap();
    assert!(!status.success());

    let stderr = read_stderr(&mut second);
    assert!(stderr.contains("data directory is already owned by another process"));
    assert!(!second_log_dir.exists());
    assert!(socket_path.exists());

    send_sigint(&first);
    let exit = first.wait().unwrap();
    assert!(exit.success());
}

#[test]
fn server_binary_does_not_delete_new_socket_after_old_instance_exits() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let first_log_dir = temp.path().join("logs-first");
    let second_log_dir = temp.path().join("logs-second");
    let cleanup_ready_marker = temp.path().join("cleanup-ready");
    let binary = env!("CARGO_BIN_EXE_zippy-server");

    let mut first = Command::new(binary)
        .env("ZIPPY_FLIGHT_ENABLED", "false")
        .arg(&socket_path)
        .arg("--log-dir")
        .arg(&first_log_dir)
        .arg("--log-level")
        .arg("info")
        .arg("--no-console-log")
        .env(
            "ZIPPY_MASTER_TEST_CLEANUP_READY_FILE",
            &cleanup_ready_marker,
        )
        .env("ZIPPY_MASTER_TEST_PAUSE_BEFORE_CLEANUP", "1")
        .env("ZIPPY_SERVER_DATA_DIR", temp.path().join("data-first"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_path(&socket_path);

    send_sigint(&first);
    wait_for_path(&cleanup_ready_marker);

    let mut second = Command::new(binary)
        .env("ZIPPY_FLIGHT_ENABLED", "false")
        .arg(&socket_path)
        .arg("--log-dir")
        .arg(&second_log_dir)
        .arg("--log-level")
        .arg("info")
        .arg("--no-console-log")
        .env("ZIPPY_SERVER_DATA_DIR", temp.path().join("data-second"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_log_contains(&second_log_dir, "\"event\":\"master_listening\"");

    let first_exit = first.wait().unwrap();
    assert!(first_exit.success());
    assert!(socket_path.exists());

    send_sigint(&second);
    let second_exit = second.wait().unwrap();
    assert!(second_exit.success());
}

fn wait_for_path(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("socket was not created path=[{}]", path.display());
}

fn wait_for_single_log_file(log_dir: &Path) -> PathBuf {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let app_dir = log_dir.join("zippy-master");
        if app_dir.exists() {
            let mut entries = fs::read_dir(&app_dir)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<Vec<_>>();
            if entries.len() == 1 {
                entries.sort();
                return entries.remove(0);
            }
        }
        thread::sleep(Duration::from_millis(20));
    }

    panic!("log file was not created under [{}]", log_dir.display());
}

fn wait_for_log_contains(log_dir: &Path, needle: &str) {
    let log_file = wait_for_single_log_file(log_dir);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let contents = fs::read_to_string(&log_file).unwrap();
        if contents.contains(needle) {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }

    panic!(
        "log file under [{}] did not contain [{}]",
        log_dir.display(),
        needle
    );
}

fn wait_for_exit(
    child: &mut std::process::Child,
    timeout: Duration,
) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        thread::sleep(Duration::from_millis(20));
    }

    let _ = child.kill();
    None
}

fn unused_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    listener.local_addr().unwrap()
}

fn read_stderr(child: &mut std::process::Child) -> String {
    let mut stderr = child.stderr.take().unwrap();
    let mut output = String::new();
    stderr.read_to_string(&mut output).unwrap();
    output
}

fn send_sigint(child: &std::process::Child) {
    let pid = rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::INT).unwrap();
}
