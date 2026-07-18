use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

use zippy_core::config::ZippyFlightConfig;
use zippy_protocol::auth::FlightBearerAuthConfig;
use zippy_protocol::runtime::FlightServerTlsConfig;
use zippy_types::{ErrorCode, ServerError};

const MAX_CERTIFICATE_BYTES: usize = 1024 * 1024;
const MAX_PRIVATE_KEY_BYTES: usize = 256 * 1024;
const MAX_CLIENT_CA_BYTES: usize = 1024 * 1024;
const MAX_BOOTSTRAP_TOKEN_BYTES: usize = 4 * 1024;

pub(crate) struct LoadedFlightSecurity {
    pub(crate) bearer_auth: Option<FlightBearerAuthConfig>,
    pub(crate) tls: Option<FlightServerTlsConfig>,
}

pub(crate) fn load_flight_security(
    config: &ZippyFlightConfig,
) -> Result<LoadedFlightSecurity, ServerError> {
    let bearer_auth = if config.auth.enabled {
        let mut token = match (
            config.auth.bootstrap_token(),
            config.auth.token_file.as_deref(),
        ) {
            (Some(token), None) => token.as_bytes().to_vec(),
            (None, Some(path)) => {
                read_bounded_file(path, MAX_BOOTSTRAP_TOKEN_BYTES, true, "bootstrap token")?
            }
            _ => {
                return Err(invalid(
                    "validated Flight auth configuration has an invalid credential source count",
                ));
            }
        };
        strip_one_line_ending(&mut token);
        let auth_result = FlightBearerAuthConfig::try_new(
            &token,
            Duration::from_millis(config.auth.session_ttl_ms),
            config.auth.max_sessions,
        );
        token.fill(0);
        let mut auth = auth_result?;
        let mut admin_token = match (
            config.auth.admin_bootstrap_token(),
            config.auth.admin_token_file.as_deref(),
        ) {
            (Some(token), None) => Some(token.as_bytes().to_vec()),
            (None, Some(path)) => Some(read_bounded_file(
                path,
                MAX_BOOTSTRAP_TOKEN_BYTES,
                true,
                "administrator bootstrap token",
            )?),
            (None, None) => None,
            (Some(_), Some(_)) => {
                return Err(invalid(
                    "validated Flight auth has multiple administrator credential sources",
                ));
            }
        };
        if let Some(admin_token) = admin_token.as_mut() {
            strip_one_line_ending(admin_token);
            let admin_result = auth.with_admin_token(admin_token);
            admin_token.fill(0);
            auth = admin_result?;
        }
        let mut writer_token = match (
            config.auth.writer_bootstrap_token(),
            config.auth.writer_token_file.as_deref(),
        ) {
            (Some(token), None) => Some(token.as_bytes().to_vec()),
            (None, Some(path)) => Some(read_bounded_file(
                path,
                MAX_BOOTSTRAP_TOKEN_BYTES,
                true,
                "writer bootstrap token",
            )?),
            (None, None) => None,
            (Some(_), Some(_)) => {
                return Err(invalid(
                    "validated Flight auth has multiple writer credential sources",
                ));
            }
        };
        if let Some(writer_token) = writer_token.as_mut() {
            strip_one_line_ending(writer_token);
            let writer_result = auth.with_writer_token(writer_token);
            writer_token.fill(0);
            auth = writer_result?;
        }
        Some(auth)
    } else {
        None
    };

    let tls = if config.tls.enabled {
        let cert_file =
            config.tls.cert_file.as_deref().ok_or_else(|| {
                invalid("validated Flight TLS configuration has no certificate file")
            })?;
        let key_file =
            config.tls.key_file.as_deref().ok_or_else(|| {
                invalid("validated Flight TLS configuration has no private key file")
            })?;
        let certificate =
            read_bounded_file(cert_file, MAX_CERTIFICATE_BYTES, false, "certificate")?;
        let private_key = read_bounded_file(key_file, MAX_PRIVATE_KEY_BYTES, true, "private key")?;
        let client_ca = config
            .tls
            .client_ca_file
            .as_deref()
            .map(|path| read_bounded_file(path, MAX_CLIENT_CA_BYTES, false, "client CA"))
            .transpose()?;
        Some(FlightServerTlsConfig::try_new(
            certificate,
            private_key,
            client_ca,
            config.tls.require_client_certificate,
        )?)
    } else {
        None
    };

    Ok(LoadedFlightSecurity { bearer_auth, tls })
}

fn strip_one_line_ending(value: &mut Vec<u8>) {
    if value.ends_with(b"\r\n") {
        value.truncate(value.len() - 2);
    } else if value.ends_with(b"\n") {
        value.truncate(value.len() - 1);
    }
}

#[cfg(unix)]
fn read_bounded_file(
    path: &Path,
    maximum: usize,
    secret: bool,
    purpose: &str,
) -> Result<Vec<u8>, ServerError> {
    use rustix::fs::{fstat, open, FileType, Mode, OFlags};

    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(|error| credential_io_error("open", purpose, path, error))?;
    let stat =
        fstat(&descriptor).map_err(|error| credential_io_error("inspect", purpose, path, error))?;
    if !FileType::from_raw_mode(stat.st_mode).is_file() {
        return Err(invalid(format!(
            "Flight {purpose} must be a regular file path=[{}]",
            path.display()
        )));
    }
    if stat.st_size < 0 || usize::try_from(stat.st_size).map_or(true, |size| size > maximum) {
        return Err(invalid(format!(
            "Flight {purpose} exceeds its size limit maximum=[{maximum}]"
        )));
    }
    if secret {
        if stat.st_uid != rustix::process::geteuid().as_raw() {
            return Err(invalid(format!(
                "Flight {purpose} must be owned by the server user"
            )));
        }
        if stat.st_nlink != 1 {
            return Err(invalid(format!(
                "Flight {purpose} must have exactly one filesystem link"
            )));
        }
        if stat.st_mode & 0o077 != 0 {
            return Err(invalid(format!(
                "Flight {purpose} permissions must not grant group or other access"
            )));
        }
    }
    read_from_file(File::from(descriptor), maximum, purpose)
}

#[cfg(not(unix))]
fn read_bounded_file(
    path: &Path,
    maximum: usize,
    _secret: bool,
    purpose: &str,
) -> Result<Vec<u8>, ServerError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| credential_io_error("inspect", purpose, path, error))?;
    if !metadata.file_type().is_file() || metadata.len() > maximum as u64 {
        return Err(invalid(format!(
            "Flight {purpose} must be a bounded regular file"
        )));
    }
    let file =
        File::open(path).map_err(|error| credential_io_error("open", purpose, path, error))?;
    read_from_file(file, maximum, purpose)
}

fn read_from_file(file: File, maximum: usize, purpose: &str) -> Result<Vec<u8>, ServerError> {
    let mut value = Vec::with_capacity(maximum.min(8 * 1024));
    file.take((maximum as u64).saturating_add(1))
        .read_to_end(&mut value)
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Unavailable,
                format!("read Flight {purpose} failed error=[{error}]"),
            )
            .with_retryable(false)
        })?;
    if value.len() > maximum {
        return Err(invalid(format!(
            "Flight {purpose} exceeds its size limit maximum=[{maximum}]"
        )));
    }
    Ok(value)
}

fn credential_io_error(
    operation: &str,
    purpose: &str,
    path: &Path,
    error: impl std::fmt::Display,
) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        format!(
            "{operation} Flight {purpose} failed path=[{}] error=[{error}]",
            path.display()
        ),
    )
    .with_retryable(false)
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(unix)]
    #[test]
    fn secret_files_must_be_owner_only_regular_files() {
        let temp = tempfile::tempdir().unwrap();
        let token = temp.path().join("token");
        fs::write(&token, b"test-only-flight-bootstrap-token-0001\n").unwrap();
        fs::set_permissions(&token, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(read_bounded_file(&token, 4_096, true, "bootstrap token")
            .unwrap_err()
            .message()
            .contains("permissions"));
        fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            read_bounded_file(&token, 4_096, true, "bootstrap token").unwrap(),
            b"test-only-flight-bootstrap-token-0001\n"
        );
    }

    #[cfg(unix)]
    #[test]
    fn credential_reader_rejects_symbolic_links() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("target");
        let link = temp.path().join("link");
        fs::write(&target, b"certificate").unwrap();
        symlink(&target, &link).unwrap();
        assert!(read_bounded_file(&link, 4_096, false, "certificate").is_err());
    }
}
