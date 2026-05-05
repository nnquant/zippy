use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::{Result, ZippyError};

pub const DEFAULT_CONTROL_ENDPOINT_URI: &str = "zippy://default";
pub const DEFAULT_WINDOWS_CONTROL_ADDR: &str = "127.0.0.1:17690";

const CONTROL_ENDPOINT_ROOT: &str = ".zippy/control_endpoints";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlEndpointKind {
    Unix,
    Tcp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlEndpoint {
    Unix(PathBuf),
    Tcp(SocketAddr),
}

impl From<PathBuf> for ControlEndpoint {
    fn from(path: PathBuf) -> Self {
        Self::Unix(path)
    }
}

impl From<SocketAddr> for ControlEndpoint {
    fn from(addr: SocketAddr) -> Self {
        Self::Tcp(addr)
    }
}

impl ControlEndpoint {
    pub fn kind(&self) -> ControlEndpointKind {
        match self {
            Self::Unix(_) => ControlEndpointKind::Unix,
            Self::Tcp(_) => ControlEndpointKind::Tcp,
        }
    }

    pub fn display_string(&self) -> String {
        match self {
            Self::Unix(path) => path.display().to_string(),
            Self::Tcp(addr) => format!("tcp://{addr}"),
        }
    }

    pub fn snapshot_dir(&self) -> PathBuf {
        match self {
            Self::Unix(path) => path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(".")),
            Self::Tcp(addr) => home_dir()
                .join(CONTROL_ENDPOINT_ROOT)
                .join(format!("tcp-{}", addr.port())),
        }
    }

    pub fn unix_path(&self) -> Option<&Path> {
        match self {
            Self::Unix(path) => Some(path.as_path()),
            Self::Tcp(_) => None,
        }
    }
}

pub fn default_control_endpoint() -> ControlEndpoint {
    resolve_control_endpoint(DEFAULT_CONTROL_ENDPOINT_URI)
        .expect("default control endpoint must be valid")
}

pub fn resolve_control_endpoint(uri: impl AsRef<str>) -> Result<ControlEndpoint> {
    resolve_control_endpoint_with_home(uri.as_ref(), &home_dir())
}

pub fn resolve_control_endpoint_with_home(uri: &str, home: &Path) -> Result<ControlEndpoint> {
    if uri.starts_with("zippy+tcp://") {
        return Err(ZippyError::InvalidConfig {
            reason: "zippy+tcp:// uri is no longer supported; use zippy://host:port/profile"
                .to_string(),
        });
    }
    if let Some(addr) = uri.strip_prefix("tcp://") {
        return parse_tcp_endpoint(uri, addr);
    }
    if let Some(addr) = remote_zippy_authority(uri) {
        return parse_tcp_endpoint(uri, addr);
    }

    #[cfg(not(unix))]
    let _ = home;

    #[cfg(unix)]
    {
        Ok(ControlEndpoint::Unix(
            resolve_control_endpoint_uri_with_home(uri, home),
        ))
    }

    #[cfg(not(unix))]
    {
        if uri.starts_with("unix://") || uri.starts_with("file://") || looks_like_path(uri) {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "unix control endpoint is not supported on this platform uri=[{uri}]"
                ),
            });
        }
        parse_tcp_endpoint(
            &format!("tcp://{DEFAULT_WINDOWS_CONTROL_ADDR}"),
            DEFAULT_WINDOWS_CONTROL_ADDR,
        )
    }
}

pub fn default_control_endpoint_path() -> PathBuf {
    resolve_control_endpoint_uri(DEFAULT_CONTROL_ENDPOINT_URI)
}

pub fn resolve_control_endpoint_uri(uri: impl AsRef<str>) -> PathBuf {
    resolve_control_endpoint_uri_with_home(uri.as_ref(), &home_dir())
}

fn resolve_control_endpoint_uri_with_home(uri: &str, home: &Path) -> PathBuf {
    if let Some(path) = uri.strip_prefix("unix://") {
        return expand_path(path, home);
    }
    if let Some(path) = uri.strip_prefix("file://") {
        return expand_path(path, home);
    }
    if let Some(name) = uri.strip_prefix("zippy://") {
        return logical_endpoint_path(name, home);
    }
    if looks_like_path(uri) {
        return expand_path(uri, home);
    }

    logical_endpoint_path(uri, home)
}

fn logical_endpoint_path(name: &str, home: &Path) -> PathBuf {
    let endpoint_name = if name.is_empty() { "default" } else { name };
    home.join(CONTROL_ENDPOINT_ROOT)
        .join(endpoint_name)
        .join("master.sock")
}

fn looks_like_path(uri: &str) -> bool {
    if uri.starts_with("zippy://") {
        return false;
    }

    uri.starts_with('/')
        || uri.starts_with("~/")
        || uri.starts_with("~\\")
        || uri.starts_with("./")
        || uri.starts_with("../")
        || uri.contains('\\')
        || uri.contains('/')
        || uri.as_bytes().get(1) == Some(&b':')
        || uri.ends_with(".sock")
}

fn remote_zippy_authority(uri: &str) -> Option<&str> {
    let body = uri.strip_prefix("zippy://")?;
    let authority = body.split('/').next().unwrap_or_default();
    if authority.contains(':') {
        Some(authority)
    } else {
        None
    }
}

fn expand_path(path: &str, home: &Path) -> PathBuf {
    if let Some(relative) = path.strip_prefix("~/") {
        return home.join(relative);
    }
    if let Some(relative) = path.strip_prefix("~\\") {
        return home.join(relative);
    }
    if path == "~" {
        return home.to_path_buf();
    }
    Path::new(path).to_path_buf()
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .or_else(|| {
            let drive = std::env::var_os("HOMEDRIVE")?;
            let path = std::env::var_os("HOMEPATH")?;
            let mut home = std::ffi::OsString::from(drive);
            home.push(path);
            Some(home)
        })
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}

fn parse_tcp_endpoint(original_uri: &str, addr: &str) -> Result<ControlEndpoint> {
    let socket_addr = addr
        .parse::<SocketAddr>()
        .map_err(|error| ZippyError::InvalidConfig {
            reason: format!("invalid tcp control endpoint uri=[{original_uri}] error=[{error}]"),
        })?;
    Ok(ControlEndpoint::Tcp(socket_addr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_logical_default_endpoint() {
        let home = PathBuf::from("/tmp/zippy-home");
        let path = resolve_control_endpoint_uri_with_home("zippy://default", &home);

        assert_eq!(
            path,
            PathBuf::from("/tmp/zippy-home/.zippy/control_endpoints/default/master.sock")
        );
    }

    #[test]
    fn resolves_bare_logical_name_and_explicit_paths() {
        let home = PathBuf::from("/tmp/zippy-home");

        assert_eq!(
            resolve_control_endpoint_uri_with_home("sim", &home),
            PathBuf::from("/tmp/zippy-home/.zippy/control_endpoints/sim/master.sock")
        );
        assert_eq!(
            resolve_control_endpoint_uri_with_home("~/custom/master.sock", &home),
            PathBuf::from("/tmp/zippy-home/custom/master.sock")
        );
        assert_eq!(
            resolve_control_endpoint_uri_with_home("unix:///tmp/custom-master.sock", &home),
            PathBuf::from("/tmp/custom-master.sock")
        );
        assert_eq!(
            resolve_control_endpoint_uri_with_home("/tmp/custom-master.sock", &home),
            PathBuf::from("/tmp/custom-master.sock")
        );
    }

    #[test]
    fn treats_bare_names_as_logical_endpoints() {
        assert!(!looks_like_path("default"));
        assert!(!looks_like_path("sim"));
        assert!(!looks_like_path("zippy://default"));
        assert!(looks_like_path("/tmp/master.sock"));
        assert!(looks_like_path("~/master.sock"));
        assert!(looks_like_path("./master.sock"));
        assert!(looks_like_path("../master.sock"));
        assert!(looks_like_path("runtime/master.sock"));
        assert!(looks_like_path(r"runtime\master.sock"));
        assert!(looks_like_path(r"C:\zippy\master.sock"));
        assert!(looks_like_path("master.sock"));
    }
}
