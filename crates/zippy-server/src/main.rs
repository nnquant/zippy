//! Foreground CLI for the same owning runtime used by the Python server launcher.

use std::path::PathBuf;

use zippy_core::{default_control_endpoint, resolve_control_endpoint, ControlEndpoint, ZippyError};
use zippy_master::daemon::MasterDaemonConfig;
use zippy_server::{run_server_daemon, ServerRuntimeConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = ServerArgs::parse(std::env::args().skip(1))?;
    let mut config = MasterDaemonConfig::new(args.control_endpoint);
    if args.control_endpoint_explicit {
        config = config.with_explicit_control_endpoint();
    }
    if let Some(config_path) = args.config_path {
        config = config.with_config_path(config_path);
    }
    if let Some(log_level) = args.log_level {
        config = config.with_logging(args.log_dir, log_level, args.console_log);
    } else {
        config.log_dir = args.log_dir;
        config.console_log = args.console_log;
    }
    run_server_daemon(ServerRuntimeConfig::new(config))?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerArgs {
    control_endpoint: ControlEndpoint,
    control_endpoint_explicit: bool,
    config_path: Option<PathBuf>,
    log_dir: PathBuf,
    log_level: Option<String>,
    console_log: bool,
}

impl ServerArgs {
    fn parse(args: impl IntoIterator<Item = String>) -> zippy_core::Result<Self> {
        let mut control_endpoint = None;
        let mut config_path = None;
        let mut log_dir = PathBuf::from("logs");
        let mut log_level = None;
        let mut console_log = true;
        let mut iter = args.into_iter();

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--log-dir" => {
                    log_dir = PathBuf::from(next_value(&mut iter, "--log-dir")?);
                }
                "--log-level" => {
                    log_level = Some(next_value(&mut iter, "--log-level")?);
                }
                "-c" | "--config" => {
                    config_path = Some(PathBuf::from(next_value(&mut iter, arg.as_str())?));
                }
                "--no-console-log" => {
                    console_log = false;
                }
                value if value.starts_with('-') => {
                    return Err(ZippyError::InvalidConfig {
                        reason: format!("unknown server flag flag=[{}]", value),
                    });
                }
                value => {
                    if control_endpoint.is_some() {
                        return Err(ZippyError::InvalidConfig {
                            reason: format!("multiple control endpoint values value=[{}]", value),
                        });
                    }
                    control_endpoint = Some(resolve_control_endpoint(value)?);
                }
            }
        }

        Ok(Self {
            control_endpoint_explicit: control_endpoint.is_some(),
            control_endpoint: control_endpoint.unwrap_or_else(default_control_endpoint),
            config_path,
            log_dir,
            log_level,
            console_log,
        })
    }
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> zippy_core::Result<String> {
    args.next()
        .filter(|value| !value.starts_with('-'))
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: format!("missing value for server flag flag=[{}]", flag),
        })
}
