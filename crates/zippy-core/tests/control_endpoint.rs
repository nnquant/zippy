use std::net::{IpAddr, Ipv4Addr, SocketAddr};
#[cfg(unix)]
use std::path::PathBuf;

use zippy_core::{
    resolve_control_endpoint, resolve_control_endpoint_with_home, ControlEndpoint,
    ControlEndpointKind,
};

#[test]
#[cfg(unix)]
fn resolves_logical_default_to_unix_socket_on_unix() {
    let home = PathBuf::from("/tmp/zippy-home");
    let endpoint = resolve_control_endpoint_with_home("zippy://default", &home).unwrap();

    assert_eq!(endpoint.kind(), ControlEndpointKind::Unix);
    assert_eq!(
        endpoint,
        ControlEndpoint::Unix(PathBuf::from(
            "/tmp/zippy-home/.zippy/control_endpoints/default/master.sock"
        ))
    );
    assert_eq!(
        endpoint.display_string(),
        "/tmp/zippy-home/.zippy/control_endpoints/default/master.sock"
    );
}

#[test]
fn resolves_explicit_tcp_endpoint() {
    let endpoint = resolve_control_endpoint("tcp://127.0.0.1:17777").unwrap();

    assert_eq!(endpoint.kind(), ControlEndpointKind::Tcp);
    assert_eq!(
        endpoint,
        ControlEndpoint::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 17777))
    );
    assert_eq!(endpoint.display_string(), "tcp://127.0.0.1:17777");
}

#[test]
#[cfg(unix)]
fn unix_endpoint_state_directories_include_the_socket_name() {
    let first = ControlEndpoint::Unix(PathBuf::from("/tmp/zippy/a.sock"));
    let second = ControlEndpoint::Unix(PathBuf::from("/tmp/zippy/b.sock"));

    assert_eq!(
        first.snapshot_dir(),
        PathBuf::from("/tmp/zippy/a.sock.zippy-state")
    );
    assert_eq!(
        second.snapshot_dir(),
        PathBuf::from("/tmp/zippy/b.sock.zippy-state")
    );
    assert_ne!(first.snapshot_dir(), second.snapshot_dir());
    assert_eq!(
        first.legacy_snapshot_path(),
        PathBuf::from("/tmp/zippy/master-registry.json")
    );
}

#[test]
fn tcp_endpoint_state_directories_include_the_full_address() {
    let first = ControlEndpoint::Tcp("127.0.0.1:17690".parse().unwrap());
    let second = ControlEndpoint::Tcp("127.0.0.2:17690".parse().unwrap());

    assert_ne!(first.snapshot_dir(), second.snapshot_dir());
    assert!(first.snapshot_dir().ends_with("tcp-v4-127-0-0-1-17690"));
    assert!(second.snapshot_dir().ends_with("tcp-v4-127-0-0-2-17690"));
}

#[test]
fn resolves_remote_zippy_uri_to_tcp_endpoint() {
    let endpoint = resolve_control_endpoint("zippy://127.0.0.1:17777/default").unwrap();

    assert_eq!(endpoint.kind(), ControlEndpointKind::Tcp);
    assert_eq!(
        endpoint,
        ControlEndpoint::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 17777))
    );
    assert_eq!(endpoint.display_string(), "tcp://127.0.0.1:17777");
}

#[test]
fn resolves_localhost_remote_zippy_uri_to_tcp_endpoint() {
    let endpoint = resolve_control_endpoint("zippy://localhost:17777").unwrap();

    assert_eq!(endpoint.kind(), ControlEndpointKind::Tcp);
    assert_eq!(
        endpoint,
        ControlEndpoint::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 17777))
    );
    assert_eq!(endpoint.display_string(), "tcp://127.0.0.1:17777");
}

#[test]
fn rejects_tcp_endpoint_without_port() {
    let error = resolve_control_endpoint("tcp://127.0.0.1").unwrap_err();

    assert!(error
        .to_string()
        .contains("invalid tcp control endpoint uri=[tcp://127.0.0.1]"));
}

#[test]
fn rejects_legacy_remote_gateway_uri() {
    let error = resolve_control_endpoint("zippy+tcp://127.0.0.1:17666/default").unwrap_err();

    assert!(error.to_string().contains("zippy://host:port/profile"));
}

#[test]
#[cfg(not(unix))]
fn resolves_logical_default_to_tcp_loopback_on_windows() {
    let endpoint = resolve_control_endpoint_with_home(
        "zippy://default",
        std::path::Path::new("C:\\Users\\zippy"),
    )
    .unwrap();

    assert_eq!(endpoint.kind(), ControlEndpointKind::Tcp);
    assert_eq!(endpoint.display_string(), "tcp://127.0.0.1:17690");
}

#[test]
#[cfg(not(unix))]
fn rejects_unix_socket_paths_on_windows() {
    let error = resolve_control_endpoint(r"C:\zippy\master.sock").unwrap_err();

    assert!(error
        .to_string()
        .contains("unix control endpoint is not supported"));
}
