use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn master_binary_writes_startup_logs_and_socket() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let log_dir = temp.path().join("logs");
    let binary = env!("CARGO_BIN_EXE_zippy-master");

    let mut child = Command::new(binary)
        .arg(&socket_path)
        .arg("--log-dir")
        .arg(&log_dir)
        .arg("--log-level")
        .arg("info")
        .arg("--no-console-log")
        .env("ZIPPY_MASTER_TEST_PAUSE_BEFORE_SERVE", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let log_file = wait_for_single_log_file(&log_dir);
    let before_socket = fs::read_to_string(&log_file).unwrap();
    assert!(before_socket.contains("\"event\":\"master_start\""));
    assert!(before_socket.contains("\"status\":\"starting\""));
    assert!(!before_socket.contains("\"event\":\"master_listening\""));
    assert!(!socket_path.exists());

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
fn master_binary_rejects_missing_log_dir_value() {
    let binary = env!("CARGO_BIN_EXE_zippy-master");
    let mut child = Command::new(binary)
        .arg("--log-dir")
        .arg("--no-console-log")
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();

    let status = wait_for_exit(&mut child, Duration::from_secs(1)).unwrap();
    assert!(!status.success());

    let stderr = read_stderr(&mut child);
    assert!(stderr.contains("missing value for master flag flag=[--log-dir]"));
}

#[test]
fn master_binary_rejects_multiple_control_endpoints() {
    let temp = tempfile::tempdir().unwrap();
    let binary = env!("CARGO_BIN_EXE_zippy-master");
    let mut child = Command::new(binary)
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
fn master_binary_handles_sigint_and_cleans_socket() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let log_dir = temp.path().join("logs");
    let binary = env!("CARGO_BIN_EXE_zippy-master");

    let mut child = Command::new(binary)
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

    let status = unsafe { libc::kill(child.id() as i32, libc::SIGINT) };
    assert_eq!(status, 0);

    let exit = child.wait().unwrap();
    assert!(exit.success());
    assert!(!socket_path.exists());

    let contents = fs::read_to_string(wait_for_single_log_file(&log_dir)).unwrap();
    assert!(contents.contains("\"event\":\"master_shutdown_requested\""));
    assert!(contents.contains("\"event\":\"master_stopped\""));
}

#[test]
fn master_binary_exits_before_listening_when_sigint_arrives_during_startup_pause() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let log_dir = temp.path().join("logs");
    let binary = env!("CARGO_BIN_EXE_zippy-master");

    let mut child = Command::new(binary)
        .arg(&socket_path)
        .arg("--log-dir")
        .arg(&log_dir)
        .arg("--log-level")
        .arg("info")
        .arg("--no-console-log")
        .env("ZIPPY_MASTER_TEST_PAUSE_BEFORE_SERVE", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let log_file = wait_for_single_log_file(&log_dir);
    wait_for_log_contains(&log_dir, "\"event\":\"master_start\"");
    assert!(!socket_path.exists());

    let status = unsafe { libc::kill(child.id() as i32, libc::SIGINT) };
    assert_eq!(status, 0);

    let exit = child.wait().unwrap();
    assert!(exit.success());
    assert!(!socket_path.exists());

    let contents = fs::read_to_string(log_file).unwrap();
    assert!(contents.contains("\"event\":\"master_shutdown_requested\""));
    assert!(contents.contains("\"event\":\"master_stopped\""));
    assert!(!contents.contains("\"event\":\"master_listening\""));
}

#[test]
fn master_binary_rejects_second_master_when_socket_is_active() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let first_log_dir = temp.path().join("logs-first");
    let second_log_dir = temp.path().join("logs-second");
    let binary = env!("CARGO_BIN_EXE_zippy-master");

    let mut first = Command::new(binary)
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
    assert!(stderr.contains("control endpoint socket is already active"));
    assert!(socket_path.exists());

    let status = unsafe { libc::kill(first.id() as i32, libc::SIGINT) };
    assert_eq!(status, 0);
    let exit = first.wait().unwrap();
    assert!(exit.success());
}

#[test]
fn master_binary_does_not_delete_new_socket_after_old_instance_exits() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("master.sock");
    let first_log_dir = temp.path().join("logs-first");
    let second_log_dir = temp.path().join("logs-second");
    let cleanup_ready_marker = temp.path().join("cleanup-ready");
    let binary = env!("CARGO_BIN_EXE_zippy-master");

    let mut first = Command::new(binary)
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
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_path(&socket_path);

    let status = unsafe { libc::kill(first.id() as i32, libc::SIGINT) };
    assert_eq!(status, 0);
    wait_for_path(&cleanup_ready_marker);

    let mut second = Command::new(binary)
        .arg(&socket_path)
        .arg("--log-dir")
        .arg(&second_log_dir)
        .arg("--log-level")
        .arg("info")
        .arg("--no-console-log")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    wait_for_log_contains(&second_log_dir, "\"event\":\"master_listening\"");

    let first_exit = first.wait().unwrap();
    assert!(first_exit.success());
    assert!(socket_path.exists());

    let status = unsafe { libc::kill(second.id() as i32, libc::SIGINT) };
    assert_eq!(status, 0);
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

fn read_stderr(child: &mut std::process::Child) -> String {
    let mut stderr = child.stderr.take().unwrap();
    let mut output = String::new();
    stderr.read_to_string(&mut output).unwrap();
    output
}
