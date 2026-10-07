use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use crate::{ControlEndpoint, ControlRequest, ControlResponse, Result, ZippyError};
use zippy_types::ErrorCode;

const CONTROL_IO_POLL_INTERVAL: Duration = Duration::from_millis(20);
/// Maximum JSON response-line payload accepted by control clients, excluding the newline.
pub const MAX_CONTROL_RESPONSE_LINE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy)]
struct RequestBudget<'a> {
    deadline: Option<Instant>,
    cancelled: Option<&'a AtomicBool>,
}

impl<'a> RequestBudget<'a> {
    const fn unbounded() -> Self {
        Self {
            deadline: None,
            cancelled: None,
        }
    }

    const fn until(deadline: Instant, cancelled: Option<&'a AtomicBool>) -> Self {
        Self {
            deadline: Some(deadline),
            cancelled,
        }
    }

    fn check(self) -> Result<()> {
        if self
            .cancelled
            .is_some_and(|cancelled| cancelled.load(Ordering::Acquire))
        {
            return Err(cancelled_error());
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(deadline_error());
        }
        Ok(())
    }

    fn poll_timeout(self) -> Result<Option<Duration>> {
        self.check()?;
        let Some(deadline) = self.deadline else {
            return Ok(None);
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(deadline_error());
        }
        Ok(Some(remaining.min(CONTROL_IO_POLL_INTERVAL)))
    }

    fn prefer_terminal(self, error: ZippyError) -> ZippyError {
        self.check().err().unwrap_or(error)
    }
}

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
    connect_control_endpoint_inner(endpoint, RequestBudget::unbounded())
}

fn connect_control_endpoint_inner(
    endpoint: &ControlEndpoint,
    budget: RequestBudget<'_>,
) -> Result<ControlStream> {
    budget.check()?;
    match endpoint {
        #[cfg(unix)]
        ControlEndpoint::Unix(path) => {
            let stream = UnixStream::connect(path)
                .map_err(io_error)
                .map_err(|error| budget.prefer_terminal(error))?;
            budget.check()?;
            Ok(ControlStream::Unix(stream))
        }
        #[cfg(not(unix))]
        ControlEndpoint::Unix(path) => Err(ZippyError::InvalidConfig {
            reason: format!(
                "unix control endpoint is not supported on this platform path=[{}]",
                path.display()
            ),
        }),
        ControlEndpoint::Tcp(addr) => loop {
            let connect_timeout = budget
                .poll_timeout()?
                .unwrap_or(Duration::from_secs(1))
                .min(Duration::from_secs(1));
            match TcpStream::connect_timeout(addr, connect_timeout) {
                Ok(stream) => {
                    budget.check()?;
                    stream
                        .set_nodelay(true)
                        .map_err(io_error)
                        .map_err(|error| budget.prefer_terminal(error))?;
                    return Ok(ControlStream::Tcp(stream));
                }
                Err(error)
                    if budget.deadline.is_some()
                        && matches!(
                            error.kind(),
                            ErrorKind::Interrupted | ErrorKind::TimedOut | ErrorKind::WouldBlock
                        ) =>
                {
                    continue;
                }
                Err(error) => return Err(budget.prefer_terminal(io_error(error))),
            }
        },
    }
}

/// Sends one control request under one monotonic timeout budget.
///
/// TCP connection setup and all connected socket reads and writes share the same deadline. A Unix
/// domain connection is checked immediately before and after `connect`, but the synchronous
/// standard-library `UnixStream::connect` call itself cannot be interrupted.
pub fn send_control_line_request_with_timeout(
    endpoint: &ControlEndpoint,
    request: ControlRequest,
    timeout: Duration,
) -> Result<ControlResponse> {
    if timeout.is_zero() {
        return Err(ZippyError::InvalidConfig {
            reason: "control request timeout must be positive".to_string(),
        });
    }
    let deadline = checked_deadline(timeout)?;
    send_control_line_request_inner(endpoint, request, RequestBudget::until(deadline, None))
}

/// Sends one control request with cancellation and one monotonic absolute deadline.
///
/// The cancellation flag and deadline cover TCP connection setup, request writes, and response
/// reads. Blocking socket operations are polled in short bounded intervals so cancellation cannot
/// be lost between the caller's initial check and the first I/O operation. A Unix domain connection
/// is checked immediately before and after `connect`, but the synchronous standard-library
/// `UnixStream::connect` call itself cannot be interrupted.
pub fn send_control_line_request_cancellable_with_deadline(
    endpoint: &ControlEndpoint,
    request: ControlRequest,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<ControlResponse> {
    send_control_line_request_inner(
        endpoint,
        request,
        RequestBudget::until(deadline, Some(cancelled)),
    )
}

fn send_control_line_request_inner(
    endpoint: &ControlEndpoint,
    request: ControlRequest,
    budget: RequestBudget<'_>,
) -> Result<ControlResponse> {
    budget.check()?;
    let mut payload = serde_json::to_vec(&request).map_err(json_error)?;
    payload.push(b'\n');
    budget.check()?;

    let mut stream = connect_control_endpoint_inner(endpoint, budget)?;
    write_all_with_budget(&mut stream, &payload, budget)?;
    flush_with_budget(&mut stream, budget)?;

    let response_line = read_line_with_budget(&mut stream, budget)?;
    budget.check()?;
    let response = parse_control_response(&response_line, budget)?;

    control_response_result(response)
}

fn parse_control_response(
    response_line: &[u8],
    budget: RequestBudget<'_>,
) -> Result<ControlResponse> {
    let response = serde_json::from_slice(response_line)
        .map_err(json_error)
        .map_err(|error| budget.prefer_terminal(error))?;
    budget.check()?;
    Ok(response)
}

fn checked_deadline(timeout: Duration) -> Result<Instant> {
    Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "control request timeout exceeds monotonic clock range".to_string(),
        })
}

fn write_all_with_budget(
    stream: &mut ControlStream,
    mut payload: &[u8],
    budget: RequestBudget<'_>,
) -> Result<()> {
    while !payload.is_empty() {
        set_control_stream_write_timeout(stream, budget.poll_timeout()?)
            .map_err(|error| budget.prefer_terminal(error))?;
        match stream.write(payload) {
            Ok(0) => {
                return Err(budget.prefer_terminal(io_error(std::io::Error::new(
                    ErrorKind::WriteZero,
                    "failed to write control request",
                ))))
            }
            Ok(written) => payload = &payload[written..],
            Err(error) if retryable_io(&error) => continue,
            Err(error) => return Err(budget.prefer_terminal(io_error(error))),
        }
    }
    budget.check()
}

fn flush_with_budget(stream: &mut ControlStream, budget: RequestBudget<'_>) -> Result<()> {
    loop {
        set_control_stream_write_timeout(stream, budget.poll_timeout()?)
            .map_err(|error| budget.prefer_terminal(error))?;
        match stream.flush() {
            Ok(()) => return budget.check(),
            Err(error) if retryable_io(&error) => continue,
            Err(error) => return Err(budget.prefer_terminal(io_error(error))),
        }
    }
}

fn read_line_with_budget(stream: &mut ControlStream, budget: RequestBudget<'_>) -> Result<Vec<u8>> {
    let mut response = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        set_control_stream_read_timeout(stream, budget.poll_timeout()?)
            .map_err(|error| budget.prefer_terminal(error))?;
        match stream.read(&mut chunk) {
            Ok(0) => return Ok(response),
            Ok(read) => {
                let newline = chunk[..read].iter().position(|byte| *byte == b'\n');
                let line_bytes = newline.unwrap_or(read);
                ensure_response_line_capacity(response.len(), line_bytes)
                    .map_err(|error| budget.prefer_terminal(error))?;
                response.extend_from_slice(&chunk[..line_bytes]);
                if newline.is_some() {
                    budget.check()?;
                    return Ok(response);
                }
            }
            Err(error) if retryable_io(&error) => continue,
            Err(error) => return Err(budget.prefer_terminal(io_error(error))),
        }
    }
}

fn ensure_response_line_capacity(current_bytes: usize, additional_bytes: usize) -> Result<()> {
    if current_bytes > MAX_CONTROL_RESPONSE_LINE_BYTES
        || additional_bytes > MAX_CONTROL_RESPONSE_LINE_BYTES - current_bytes
    {
        return Err(ZippyError::ResourceExhausted {
            reason: format!(
                "control response line exceeds limit max_bytes=[{}]",
                MAX_CONTROL_RESPONSE_LINE_BYTES
            ),
        });
    }
    Ok(())
}

fn retryable_io(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::Interrupted | ErrorKind::TimedOut | ErrorKind::WouldBlock
    )
}

/// Converts a decoded control response into the corresponding typed client result.
///
/// Every error requires a stable code and explicit retryability. Unstructured legacy
/// responses fail deserialization instead of being downgraded to an IO error.
pub fn control_response_result(response: ControlResponse) -> Result<ControlResponse> {
    match response {
        ControlResponse::Error {
            code,
            reason,
            retryable,
        } => Err(control_error(code, reason, retryable)),
        other => Ok(other),
    }
}

fn control_error(code: ErrorCode, reason: String, retryable: bool) -> ZippyError {
    if retryable != code.retryable_by_default() {
        return ZippyError::Service {
            code,
            reason,
            retryable,
        };
    }
    match code {
        ErrorCode::Unauthenticated => ZippyError::Unauthenticated { reason },
        ErrorCode::InvalidArgument => ZippyError::InvalidConfig { reason },
        ErrorCode::ResourceExhausted => ZippyError::ResourceExhausted { reason },
        ErrorCode::NotFound => ZippyError::NotFound { reason },
        ErrorCode::Conflict => ZippyError::Conflict { reason },
        ErrorCode::DataCorruption => ZippyError::DataCorruption { reason },
        ErrorCode::SchemaMismatch => ZippyError::SchemaMismatch { reason },
        ErrorCode::Cancelled => ZippyError::Cancelled { reason },
        ErrorCode::DeadlineExceeded => ZippyError::DeadlineExceeded { reason },
        code => ZippyError::Service {
            code,
            reason,
            retryable,
        },
    }
}

fn set_control_stream_read_timeout(
    stream: &ControlStream,
    timeout: Option<Duration>,
) -> Result<()> {
    match stream {
        #[cfg(unix)]
        ControlStream::Unix(stream) => stream.set_read_timeout(timeout).map_err(io_error)?,
        ControlStream::Tcp(stream) => stream.set_read_timeout(timeout).map_err(io_error)?,
    }
    Ok(())
}

fn set_control_stream_write_timeout(
    stream: &ControlStream,
    timeout: Option<Duration>,
) -> Result<()> {
    match stream {
        #[cfg(unix)]
        ControlStream::Unix(stream) => stream.set_write_timeout(timeout).map_err(io_error)?,
        ControlStream::Tcp(stream) => stream.set_write_timeout(timeout).map_err(io_error)?,
    }
    Ok(())
}

fn cancelled_error() -> ZippyError {
    ZippyError::Cancelled {
        reason: "control request cancelled".to_string(),
    }
}

fn deadline_error() -> ZippyError {
    ZippyError::DeadlineExceeded {
        reason: "control request deadline exceeded".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_line_limit_rejects_growth_without_allocating_the_limit() {
        ensure_response_line_capacity(MAX_CONTROL_RESPONSE_LINE_BYTES, 0).unwrap();

        let error = ensure_response_line_capacity(MAX_CONTROL_RESPONSE_LINE_BYTES, 1).unwrap_err();
        let oversized_chunk =
            ensure_response_line_capacity(0, MAX_CONTROL_RESPONSE_LINE_BYTES + 1).unwrap_err();

        assert!(matches!(error, ZippyError::ResourceExhausted { .. }));
        assert!(matches!(
            oversized_chunk,
            ZippyError::ResourceExhausted { .. }
        ));
    }

    #[test]
    fn parse_error_prefers_cancelled_and_deadline_terminal_states() {
        let cancelled = AtomicBool::new(true);
        let cancelled_error = parse_control_response(
            b"not json",
            RequestBudget::until(Instant::now() + Duration::from_secs(1), Some(&cancelled)),
        )
        .unwrap_err();
        let deadline_error =
            parse_control_response(b"not json", RequestBudget::until(Instant::now(), None))
                .unwrap_err();

        assert!(matches!(cancelled_error, ZippyError::Cancelled { .. }));
        assert!(matches!(
            deadline_error,
            ZippyError::DeadlineExceeded { .. }
        ));
    }
}
