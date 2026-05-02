use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use crate::{ControlEndpoint, ControlRequest, ControlResponse, Result, ZippyError};

pub enum ControlStream {
    #[cfg(unix)]
    Unix(UnixStream),
    Tcp(TcpStream),
}

impl Read for ControlStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.read(buf),
            Self::Tcp(stream) => stream.read(buf),
        }
    }
}

impl Write for ControlStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.write(buf),
            Self::Tcp(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.flush(),
            Self::Tcp(stream) => stream.flush(),
        }
    }
}

pub fn connect_control_endpoint(endpoint: &ControlEndpoint) -> Result<ControlStream> {
    match endpoint {
        #[cfg(unix)]
        ControlEndpoint::Unix(path) => UnixStream::connect(path)
            .map(ControlStream::Unix)
            .map_err(io_error),
        #[cfg(not(unix))]
        ControlEndpoint::Unix(path) => Err(ZippyError::InvalidConfig {
            reason: format!(
                "unix control endpoint is not supported on this platform path=[{}]",
                path.display()
            ),
        }),
        ControlEndpoint::Tcp(addr) => {
            let stream =
                TcpStream::connect_timeout(addr, Duration::from_secs(1)).map_err(io_error)?;
            stream.set_nodelay(true).map_err(io_error)?;
            Ok(ControlStream::Tcp(stream))
        }
    }
}

pub fn send_control_line_request(
    endpoint: &ControlEndpoint,
    request: ControlRequest,
) -> Result<ControlResponse> {
    let mut stream = connect_control_endpoint(endpoint)?;
    let payload = serde_json::to_string(&request).map_err(json_error)?;
    stream.write_all(payload.as_bytes()).map_err(io_error)?;
    stream.write_all(b"\n").map_err(io_error)?;
    stream.flush().map_err(io_error)?;

    let mut response_line = String::new();
    let mut reader = BufReader::new(stream);
    reader.read_line(&mut response_line).map_err(io_error)?;
    let response: ControlResponse =
        serde_json::from_str(response_line.trim_end()).map_err(json_error)?;

    match response {
        ControlResponse::Error { reason } => Err(ZippyError::Io { reason }),
        other => Ok(other),
    }
}

fn io_error(error: std::io::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn json_error(error: serde_json::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}
