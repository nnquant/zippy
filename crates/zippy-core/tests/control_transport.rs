use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::thread;

use zippy_core::{
    send_control_line_request, ControlEndpoint, ControlRequest, ControlResponse,
    RegisterProcessRequest,
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
        })
        .unwrap();
        stream.write_all(response.as_bytes()).unwrap();
        stream.write_all(b"\n").unwrap();
    });

    let response = send_control_line_request(
        &ControlEndpoint::Tcp(addr),
        ControlRequest::RegisterProcess(RegisterProcessRequest {
            app: "pytest".to_string(),
        }),
    )
    .unwrap();

    match response {
        ControlResponse::ProcessRegistered { process_id } => {
            assert_eq!(process_id, "proc_test");
        }
        other => panic!("unexpected response: {other:?}"),
    }
    handle.join().unwrap();
}
