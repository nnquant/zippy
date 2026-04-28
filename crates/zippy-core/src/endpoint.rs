use std::path::{Path, PathBuf};

pub const DEFAULT_CONTROL_ENDPOINT_URI: &str = "zippy://default";

const CONTROL_ENDPOINT_ROOT: &str = ".zippy/control_endpoints";

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
    uri.starts_with('/')
        || uri.starts_with("~/")
        || uri.starts_with("./")
        || uri.starts_with("../")
        || uri.contains('/')
        || uri.ends_with(".sock")
}

fn expand_path(path: &str, home: &Path) -> PathBuf {
    if let Some(relative) = path.strip_prefix("~/") {
        return home.join(relative);
    }
    if path == "~" {
        return home.to_path_buf();
    }
    Path::new(path).to_path_buf()
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
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
        assert!(looks_like_path("/tmp/master.sock"));
        assert!(looks_like_path("~/master.sock"));
        assert!(looks_like_path("./master.sock"));
        assert!(looks_like_path("../master.sock"));
        assert!(looks_like_path("runtime/master.sock"));
        assert!(looks_like_path("master.sock"));
    }
}
