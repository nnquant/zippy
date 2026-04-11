use std::path::PathBuf;

use zippy_core::ZippyError;
use zippy_master::daemon::{run_master_daemon, MasterDaemonConfig};

fn main() -> zippy_core::Result<()> {
    let args = MasterArgs::parse(std::env::args().skip(1))?;
    let config = MasterDaemonConfig::new(args.control_endpoint).with_logging(
        args.log_dir,
        args.log_level,
        args.console_log,
    );
    run_master_daemon(config)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MasterArgs {
    control_endpoint: PathBuf,
    log_dir: PathBuf,
    log_level: String,
    console_log: bool,
}

impl MasterArgs {
    fn parse(args: impl IntoIterator<Item = String>) -> zippy_core::Result<Self> {
        let mut control_endpoint = None;
        let mut log_dir = PathBuf::from("logs");
        let mut log_level = String::from("info");
        let mut console_log = true;
        let mut iter = args.into_iter();

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--log-dir" => {
                    log_dir = PathBuf::from(next_value(&mut iter, "--log-dir")?);
                }
                "--log-level" => {
                    log_level = next_value(&mut iter, "--log-level")?;
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
                    control_endpoint = Some(expand_control_endpoint(value.to_string()));
                }
            }
        }

        Ok(Self {
            control_endpoint: control_endpoint.unwrap_or_else(default_control_endpoint),
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

fn default_control_endpoint() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".zippy")
        .join("master.sock")
}

fn expand_control_endpoint(path: String) -> PathBuf {
    if let Some(stripped) = path.strip_prefix("~/") {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join(stripped);
    }

    PathBuf::from(path)
}
