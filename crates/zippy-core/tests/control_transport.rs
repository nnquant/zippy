use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use zippy_core::{
    control_response_result, send_control_line_request_cancellable_with_deadline,
    send_control_line_request_with_timeout, ControlEndpoint, ControlErrorCode, ControlRequest,
    ControlResponse, RegisterProcessRequest, ZippyError,
};

#[test]
fn tcp_control_transport_roundtrips_json_line() {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request_line = String::new();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        reader.read_line(&mut request_line).unwrap();
        let request = serde_json::from_str::<ControlRequest>(request_line.trim_end()).unwrap();
        assert!(matches!(request, ControlRequest::RegisterProcess(_)));
        let response = serde_json::to_string(&ControlResponse::ProcessRegistered {
            process_id: "proc_test".to_string(),
            process_token: "token_test".to_string(),
        })
        .unwrap();
        stream.write_all(response.as_bytes()).unwrap();
        stream.write_all(b"\n").unwrap();
    });

    let response = send_control_line_request_with_timeout(
        &ControlEndpoint::Tcp(addr),
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "pytest".to_string(),
        }),
        Duration::from_secs(2),
    )
    .unwrap();

    match response {
        ControlResponse::ProcessRegistered { process_id, .. } => {
            assert_eq!(process_id, "proc_test");
        }
        other => panic!("unexpected response: {other:?}"),
    }
    handle.join().unwrap();
}

#[test]
fn tcp_control_transport_cancellation_interrupts_stalled_response() {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let (request_seen_tx, request_seen_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream);
        let mut request_line = String::new();
        reader.read_line(&mut request_line).unwrap();
        request_seen_tx.send(()).unwrap();

        let mut stream = reader.into_inner();
        let mut closed = [0_u8; 1];
        assert_eq!(stream.read(&mut closed).unwrap(), 0);
    });
    let cancelled = Arc::new(AtomicBool::new(false));
    let canceller_flag = Arc::clone(&cancelled);
    let canceller = thread::spawn(move || {
        request_seen_rx.recv().unwrap();
        canceller_flag.store(true, Ordering::Release);
    });

    let started = Instant::now();
    let error = send_control_line_request_cancellable_with_deadline(
        &ControlEndpoint::Tcp(addr),
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "cancel_test".to_string(),
        }),
        Instant::now() + Duration::from_secs(5),
        cancelled.as_ref(),
    )
    .unwrap_err();
    let elapsed = started.elapsed();

    assert!(matches!(
        error,
        ZippyError::Cancelled { ref reason } if reason == "control request cancelled"
    ));
    assert!(
        elapsed < Duration::from_millis(500),
        "elapsed=[{elapsed:?}]"
    );
    canceller.join().unwrap();
    server.join().unwrap();
}

#[test]
fn tcp_control_transport_trickle_response_cannot_extend_deadline() {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request_line = String::new();
        BufReader::new(stream.try_clone().unwrap())
            .read_line(&mut request_line)
            .unwrap();
        let response = serde_json::to_vec(&ControlResponse::ProcessRegistered {
            process_id: "proc_trickle".to_string(),
            process_token: "token_trickle".to_string(),
        })
        .unwrap();
        for byte in response.into_iter().take(20) {
            if stream.write_all(&[byte]).is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(15));
        }
    });

    let timeout = Duration::from_millis(100);
    let started = Instant::now();
    let error = send_control_line_request_with_timeout(
        &ControlEndpoint::Tcp(addr),
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "trickle_test".to_string(),
        }),
        timeout,
    )
    .unwrap_err();
    let elapsed = started.elapsed();

    assert!(matches!(
        error,
        ZippyError::DeadlineExceeded { ref reason }
            if reason == "control request deadline exceeded"
    ));
    assert!(
        elapsed >= Duration::from_millis(50),
        "elapsed=[{elapsed:?}]"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "elapsed=[{elapsed:?}]"
    );
    server.join().unwrap();
}

#[test]
fn tcp_control_transport_timeout_api_roundtrips_json_line() {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request_line = String::new();
        BufReader::new(stream.try_clone().unwrap())
            .read_line(&mut request_line)
            .unwrap();
        let response = serde_json::to_vec(&ControlResponse::ProcessRegistered {
            process_id: "proc_timeout".to_string(),
            process_token: "token_timeout".to_string(),
        })
        .unwrap();
        stream.write_all(&response).unwrap();
        stream.write_all(b"\n").unwrap();
    });

    let response = send_control_line_request_with_timeout(
        &ControlEndpoint::Tcp(addr),
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "timeout_test".to_string(),
        }),
        Duration::from_secs(1),
    )
    .unwrap();

    assert!(matches!(
        response,
        ControlResponse::ProcessRegistered { ref process_id, .. }
            if process_id == "proc_timeout"
    ));
    server.join().unwrap();
}

#[test]
fn tcp_control_transport_preserves_structured_not_found() {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request_line = String::new();
        BufReader::new(stream.try_clone().unwrap())
            .read_line(&mut request_line)
            .unwrap();
        let response = ControlResponse::Error {
            code: ControlErrorCode::NotFound,
            retryable: false,
            reason: "stream not found stream_name=[missing]".to_string(),
        };
        let payload = serde_json::to_string(&response).unwrap();
        stream.write_all(payload.as_bytes()).unwrap();
        stream.write_all(b"\n").unwrap();
    });

    let error = send_control_line_request_with_timeout(
        &ControlEndpoint::Tcp(addr),
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "typed_error_test".to_string(),
        }),
        Duration::from_secs(2),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        ZippyError::NotFound { ref reason }
            if reason == "stream not found stream_name=[missing]"
    ));
    handle.join().unwrap();
}

#[test]
fn control_transport_preserves_structured_terminal_codes() {
    let cancelled = control_response_result(ControlResponse::Error {
        code: ControlErrorCode::Cancelled,
        retryable: false,
        reason: "query cancelled".to_string(),
    })
    .unwrap_err();
    let deadline = control_response_result(ControlResponse::Error {
        code: ControlErrorCode::DeadlineExceeded,
        retryable: false,
        reason: "query deadline exceeded".to_string(),
    })
    .unwrap_err();
    let conflict = control_response_result(ControlResponse::Error {
        code: ControlErrorCode::Conflict,
        retryable: false,
        reason: "data root is already owned".to_string(),
    })
    .unwrap_err();

    assert!(matches!(
        cancelled,
        ZippyError::Cancelled { ref reason } if reason == "query cancelled"
    ));
    assert!(matches!(
        deadline,
        ZippyError::DeadlineExceeded { ref reason }
            if reason == "query deadline exceeded"
    ));
    assert!(matches!(
        conflict,
        ZippyError::Conflict { ref reason } if reason == "data root is already owned"
    ));
}

#[test]
fn tcp_control_transport_rejects_uncoded_error() {
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request_line = String::new();
        BufReader::new(stream.try_clone().unwrap())
            .read_line(&mut request_line)
            .unwrap();
        stream
            .write_all(b"{\"Error\":{\"reason\":\"legacy failure\"}}\n")
            .unwrap();
    });

    let error = send_control_line_request_with_timeout(
        &ControlEndpoint::Tcp(addr),
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "legacy_error_test".to_string(),
        }),
        Duration::from_secs(2),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        ZippyError::Io { ref reason } if reason.contains("missing field")
    ));
    handle.join().unwrap();
}

#[test]
fn control_transport_preserves_unmapped_code_and_explicit_retry_policy() {
    let error = control_response_result(ControlResponse::Error {
        code: ControlErrorCode::PermissionDenied,
        reason: "denied".to_owned(),
        retryable: true,
    })
    .unwrap_err();
    assert_eq!(error.code(), ControlErrorCode::PermissionDenied);
    assert!(error.retryable());
    for payload in [
        r#"{"Error":{"reason":"missing","code":"NOT_FOUND"}}"#,
        r#"{"Error":{"reason":"missing","code":null,"retryable":false}}"#,
    ] {
        assert!(serde_json::from_str::<ControlResponse>(payload).is_err());
    }
}
