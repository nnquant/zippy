use std::path::PathBuf;

use zippy_core::{default_control_endpoint_path, resolve_control_endpoint_uri, ZippyError};
use zippy_master::daemon::{run_master_daemon, MasterDaemonConfig};

fn main() -> zippy_core::Result<()> {
    let args = MasterArgs::parse(std::env::args().skip(1))?;
    let mut config = MasterDaemonConfig::new(args.control_endpoint);
    if let Some(config_path) = args.config_path {
        config = config.with_config_path(config_path);
    }
    if let Some(log_level) = args.log_level {
        config = config.with_logging(args.log_dir, log_level, args.console_log);
    } else {
        config.log_dir = args.log_dir;
        config.console_log = args.console_log;
    }
    run_master_daemon(config)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MasterArgs {
    control_endpoint: PathBuf,
    config_path: Option<PathBuf>,
    log_dir: PathBuf,
    log_level: Option<String>,
    console_log: bool,
}

impl MasterArgs {
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
                        reason: format!("unknown master flag flag=[{}]", value),
                    });
                }
                value => {
                    if control_endpoint.is_some() {
                        return Err(ZippyError::InvalidConfig {
                            reason: format!("multiple control endpoint values value=[{}]", value),
                        });
                    }
                    control_endpoint = Some(resolve_control_endpoint_uri(value));
                }
            }
        }

        Ok(Self {
            control_endpoint: control_endpoint.unwrap_or_else(default_control_endpoint_path),
            config_path,
            log_dir,
            log_level,
            console_log,
        })
    }
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> zippy_core::Result<String> {
    match args.next() {
        Some(value) if value.starts_with('-') => Err(ZippyError::InvalidConfig {
            reason: format!("missing value for master flag flag=[{}]", flag),
        }),
        Some(value) => Ok(value),
        None => Err(ZippyError::InvalidConfig {
            reason: format!("missing value for master flag flag=[{}]", flag),
        }),
    }
}
