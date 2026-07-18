//! Bearer bootstrap authentication and bounded server-issued Flight sessions.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tonic::metadata::MetadataMap;
use zippy_types::{ErrorCode, Result, ServerError, SessionId};

use crate::AUTHORIZATION_METADATA_KEY;

const MIN_BOOTSTRAP_TOKEN_BYTES: usize = 32;
const MAX_BOOTSTRAP_TOKEN_BYTES: usize = 4 * 1024;
const MAX_SESSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const SESSION_TOKEN_RANDOM_BYTES: usize = 32;
const SESSION_TOKEN_PREFIX: &str = "zfs1_";

/// Validated bootstrap credential and bounded lifetime policy for Flight sessions.
#[derive(Clone, PartialEq, Eq)]
pub struct FlightBearerAuthConfig {
    bootstrap_digest: [u8; 32],
    admin_bootstrap_digest: Option<[u8; 32]>,
    session_ttl: Duration,
    max_sessions: usize,
}

impl fmt::Debug for FlightBearerAuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FlightBearerAuthConfig")
            .field("bootstrap_token", &"[REDACTED]")
            .field(
                "admin_bootstrap_token_configured",
                &self.admin_bootstrap_digest.is_some(),
            )
            .field("session_ttl", &self.session_ttl)
            .field("max_sessions", &self.max_sessions)
            .finish()
    }
}

impl FlightBearerAuthConfig {
    /// Validates and hashes one bootstrap token without retaining the plaintext credential.
    pub fn try_new(
        bootstrap_token: &[u8],
        session_ttl: Duration,
        max_sessions: usize,
    ) -> Result<Self> {
        validate_bootstrap_token(bootstrap_token)?;
        if session_ttl.is_zero() || session_ttl > MAX_SESSION_TTL {
            return Err(invalid(
                "Flight session TTL must be positive and no longer than 24 hours",
            ));
        }
        if max_sessions == 0 {
            return Err(invalid("Flight session capacity must be positive"));
        }
        Ok(Self {
            bootstrap_digest: digest(bootstrap_token),
            admin_bootstrap_digest: None,
            session_ttl,
            max_sessions,
        })
    }

    /// Adds a distinct administrator bootstrap credential.
    pub fn with_admin_token(mut self, admin_bootstrap_token: &[u8]) -> Result<Self> {
        validate_bootstrap_token(admin_bootstrap_token)?;
        let admin_digest = digest(admin_bootstrap_token);
        if bool::from(self.bootstrap_digest.ct_eq(&admin_digest)) {
            return Err(invalid(
                "Flight read and administrator bootstrap tokens must be distinct",
            ));
        }
        self.admin_bootstrap_digest = Some(admin_digest);
        Ok(self)
    }

    /// Returns the configured server-issued session lifetime.
    #[must_use]
    pub const fn session_ttl(&self) -> Duration {
        self.session_ttl
    }

    /// Returns the maximum number of simultaneously valid sessions.
    #[must_use]
    pub const fn max_sessions(&self) -> usize {
        self.max_sessions
    }

    /// Returns whether administrator sessions may be issued.
    #[must_use]
    pub const fn administrator_enabled(&self) -> bool {
        self.admin_bootstrap_digest.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlightSessionRole {
    Read,
    Administrator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AuthenticatedFlightSession {
    pub(crate) session_id: SessionId,
    pub(crate) role: FlightSessionRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionAuthorityStatus {
    pub(crate) admission_open: bool,
    pub(crate) active_sessions: usize,
    pub(crate) active_administrator_sessions: usize,
    pub(crate) opened_sessions: u64,
    pub(crate) expired_sessions: u64,
    pub(crate) auth_failures: u64,
    pub(crate) capacity_rejections: u64,
}

pub(crate) struct OpenedSession {
    pub(crate) session_id: SessionId,
    pub(crate) role: FlightSessionRole,
    pub(crate) bearer_token: String,
    pub(crate) expires_at_ns: u64,
}

impl fmt::Debug for OpenedSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenedSession")
            .field("session_id", &self.session_id)
            .field("role", &self.role)
            .field("bearer_token", &"[REDACTED]")
            .field("expires_at_ns", &self.expires_at_ns)
            .finish()
    }
}

pub(crate) struct SessionAuthority {
    config: FlightBearerAuthConfig,
    state: Mutex<SessionAuthorityState>,
    metrics: SessionAuthorityMetrics,
}

struct SessionAuthorityState {
    admission_open: bool,
    entries: HashMap<[u8; 32], SessionEntry>,
}

#[derive(Clone, Copy)]
struct SessionEntry {
    session_id: SessionId,
    role: FlightSessionRole,
    expires_at_ns: u64,
}

#[derive(Default)]
struct SessionAuthorityMetrics {
    opened_sessions: AtomicU64,
    expired_sessions: AtomicU64,
    auth_failures: AtomicU64,
    capacity_rejections: AtomicU64,
}

impl SessionAuthority {
    pub(crate) fn new(config: FlightBearerAuthConfig) -> Self {
        Self {
            state: Mutex::new(SessionAuthorityState {
                admission_open: true,
                entries: HashMap::with_capacity(config.max_sessions.min(1_024)),
            }),
            config,
            metrics: SessionAuthorityMetrics::default(),
        }
    }

    pub(crate) fn session_ttl(&self) -> Duration {
        self.config.session_ttl
    }

    pub(crate) fn open(&self, metadata: &MetadataMap, now_ns: u64) -> Result<OpenedSession> {
        let supplied = bearer_token(metadata).ok_or_else(|| self.authentication_failed())?;
        let supplied_digest = digest(supplied.as_bytes());
        let read_match = bool::from(self.config.bootstrap_digest.ct_eq(&supplied_digest));
        let administrator_match = self
            .config
            .admin_bootstrap_digest
            .as_ref()
            .is_some_and(|digest| bool::from(digest.ct_eq(&supplied_digest)));
        if !read_match && !administrator_match {
            return Err(self.authentication_failed());
        }
        let role = if administrator_match {
            FlightSessionRole::Administrator
        } else {
            FlightSessionRole::Read
        };
        let expired = self.sweep_expired(now_ns);
        drop(expired);

        let mut state = self.lock();
        if !state.admission_open {
            return Err(unavailable("Flight session admission is closed"));
        }
        if state.entries.len() >= self.config.max_sessions {
            self.metrics
                .capacity_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                "Flight session capacity is exhausted",
            )
            .with_retryable(true));
        }

        let expires_at_ns = now_ns
            .saturating_add(u64::try_from(self.config.session_ttl.as_nanos()).unwrap_or(u64::MAX));
        for _ in 0..16 {
            let session_id = random_session_id()?;
            if state
                .entries
                .values()
                .any(|entry| entry.session_id == session_id)
            {
                continue;
            }
            let bearer_token = random_session_token()?;
            let token_digest = digest(bearer_token.as_bytes());
            if state.entries.contains_key(&token_digest) {
                continue;
            }
            state.entries.insert(
                token_digest,
                SessionEntry {
                    session_id,
                    role,
                    expires_at_ns,
                },
            );
            drop(state);
            self.metrics.opened_sessions.fetch_add(1, Ordering::Relaxed);
            return Ok(OpenedSession {
                session_id,
                role,
                bearer_token,
                expires_at_ns,
            });
        }
        Err(internal("could not allocate a unique Flight session"))
    }

    pub(crate) fn authenticate(
        &self,
        metadata: &MetadataMap,
        now_ns: u64,
    ) -> Result<AuthenticatedFlightSession> {
        let supplied = bearer_token(metadata).ok_or_else(|| self.authentication_failed())?;
        if !valid_session_token_shape(supplied) {
            return Err(self.authentication_failed());
        }
        let supplied_digest = digest(supplied.as_bytes());
        let mut state = self.lock();
        let Some(entry) = state.entries.get(&supplied_digest).copied() else {
            drop(state);
            return Err(self.authentication_failed());
        };
        if entry.expires_at_ns <= now_ns {
            state.entries.remove(&supplied_digest);
            drop(state);
            self.metrics
                .expired_sessions
                .fetch_add(1, Ordering::Relaxed);
            return Err(self.authentication_failed());
        }
        Ok(AuthenticatedFlightSession {
            session_id: entry.session_id,
            role: entry.role,
        })
    }

    pub(crate) fn sweep_expired(&self, now_ns: u64) -> Vec<SessionId> {
        let expired = {
            let mut state = self.lock();
            let tokens = state
                .entries
                .iter()
                .filter_map(|(token, entry)| (entry.expires_at_ns <= now_ns).then_some(*token))
                .collect::<Vec<_>>();
            tokens
                .into_iter()
                .filter_map(|token| state.entries.remove(&token))
                .map(|entry| entry.session_id)
                .collect::<Vec<_>>()
        };
        self.metrics
            .expired_sessions
            .fetch_add(expired.len() as u64, Ordering::Relaxed);
        expired
    }

    pub(crate) fn close(&self) -> Vec<SessionId> {
        let entries = {
            let mut state = self.lock();
            state.admission_open = false;
            std::mem::take(&mut state.entries)
        };
        entries
            .into_values()
            .map(|entry| entry.session_id)
            .collect()
    }

    pub(crate) fn status(&self) -> SessionAuthorityStatus {
        let state = self.lock();
        SessionAuthorityStatus {
            admission_open: state.admission_open,
            active_sessions: state.entries.len(),
            active_administrator_sessions: state
                .entries
                .values()
                .filter(|entry| entry.role == FlightSessionRole::Administrator)
                .count(),
            opened_sessions: self.metrics.opened_sessions.load(Ordering::Relaxed),
            expired_sessions: self.metrics.expired_sessions.load(Ordering::Relaxed),
            auth_failures: self.metrics.auth_failures.load(Ordering::Relaxed),
            capacity_rejections: self.metrics.capacity_rejections.load(Ordering::Relaxed),
        }
    }

    fn authentication_failed(&self) -> ServerError {
        self.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
        ServerError::new(
            ErrorCode::Unauthenticated,
            "Flight bearer credential is missing or invalid",
        )
        .with_retryable(false)
    }

    fn lock(&self) -> MutexGuard<'_, SessionAuthorityState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn validate_bootstrap_token(token: &[u8]) -> Result<()> {
    if !(MIN_BOOTSTRAP_TOKEN_BYTES..=MAX_BOOTSTRAP_TOKEN_BYTES).contains(&token.len())
        || !token.iter().all(|byte| (0x21..=0x7e).contains(byte))
    {
        return Err(invalid(format!(
            "Flight bootstrap token must contain {MIN_BOOTSTRAP_TOKEN_BYTES}..={MAX_BOOTSTRAP_TOKEN_BYTES} visible ASCII bytes"
        )));
    }
    Ok(())
}

fn bearer_token(metadata: &MetadataMap) -> Option<&str> {
    metadata
        .get(AUTHORIZATION_METADATA_KEY)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty())
}

fn valid_session_token_shape(token: &str) -> bool {
    token.len() == SESSION_TOKEN_PREFIX.len() + SESSION_TOKEN_RANDOM_BYTES * 2
        && token.starts_with(SESSION_TOKEN_PREFIX)
        && token[SESSION_TOKEN_PREFIX.len()..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn random_session_id() -> Result<SessionId> {
    for _ in 0..16 {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(randomness_error)?;
        if let Ok(session_id) = SessionId::from_bytes(bytes) {
            return Ok(session_id);
        }
    }
    Err(internal(
        "operating system returned repeated nil session IDs",
    ))
}

fn random_session_token() -> Result<String> {
    let mut random = [0_u8; SESSION_TOKEN_RANDOM_BYTES];
    getrandom::fill(&mut random).map_err(randomness_error)?;
    let mut token = String::with_capacity(SESSION_TOKEN_PREFIX.len() + random.len() * 2);
    token.push_str(SESSION_TOKEN_PREFIX);
    for byte in random {
        use std::fmt::Write;
        write!(&mut token, "{byte:02x}").expect("writing hexadecimal bytes to String cannot fail");
    }
    Ok(token)
}

fn digest(value: &[u8]) -> [u8; 32] {
    Sha256::digest(value).into()
}

fn randomness_error(error: getrandom::Error) -> ServerError {
    internal(format!(
        "operating system randomness failed error=[{error}]"
    ))
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn unavailable(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Unavailable, message).with_retryable(true)
}

fn internal(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Internal, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    const BOOTSTRAP_TOKEN: &str = "test-only-flight-bootstrap-token-0001";

    #[test]
    fn bootstrap_token_is_redacted_and_compared_before_session_issue() {
        let config =
            FlightBearerAuthConfig::try_new(BOOTSTRAP_TOKEN.as_bytes(), Duration::from_secs(60), 2)
                .unwrap();
        assert!(!format!("{config:?}").contains(BOOTSTRAP_TOKEN));
        let authority = SessionAuthority::new(config);
        let mut wrong = MetadataMap::new();
        wrong.insert(
            AUTHORIZATION_METADATA_KEY,
            MetadataValue::try_from("Bearer test-only-flight-bootstrap-token-0002").unwrap(),
        );
        assert_eq!(
            authority.open(&wrong, 10).unwrap_err().code(),
            ErrorCode::Unauthenticated
        );
        assert_eq!(authority.status().active_sessions, 0);
    }

    #[test]
    fn sessions_are_server_issued_bounded_and_expire() {
        let authority = SessionAuthority::new(
            FlightBearerAuthConfig::try_new(
                BOOTSTRAP_TOKEN.as_bytes(),
                Duration::from_nanos(100),
                1,
            )
            .unwrap(),
        );
        let mut bootstrap = MetadataMap::new();
        bootstrap.insert(
            AUTHORIZATION_METADATA_KEY,
            MetadataValue::try_from(format!("Bearer {BOOTSTRAP_TOKEN}")).unwrap(),
        );
        let opened = authority.open(&bootstrap, 10).unwrap();
        assert_eq!(
            authority.open(&bootstrap, 11).unwrap_err().code(),
            ErrorCode::ResourceExhausted
        );
        let mut session = MetadataMap::new();
        session.insert(
            AUTHORIZATION_METADATA_KEY,
            MetadataValue::try_from(format!("Bearer {}", opened.bearer_token)).unwrap(),
        );
        let authenticated = authority.authenticate(&session, 109).unwrap();
        assert_eq!(authenticated.session_id, opened.session_id);
        assert_eq!(authenticated.role, FlightSessionRole::Read);
        assert_eq!(
            authority.authenticate(&session, 110).unwrap_err().code(),
            ErrorCode::Unauthenticated
        );
        assert_eq!(authority.status().active_sessions, 0);
    }

    #[test]
    fn administrator_token_is_distinct_and_issues_an_explicit_role() {
        const ADMIN_TOKEN: &str = "test-only-flight-administrator-token-0001";
        let config =
            FlightBearerAuthConfig::try_new(BOOTSTRAP_TOKEN.as_bytes(), Duration::from_secs(60), 2)
                .unwrap()
                .with_admin_token(ADMIN_TOKEN.as_bytes())
                .unwrap();
        assert!(config.administrator_enabled());
        assert!(!format!("{config:?}").contains(ADMIN_TOKEN));
        let authority = SessionAuthority::new(config);
        let mut bootstrap = MetadataMap::new();
        bootstrap.insert(
            AUTHORIZATION_METADATA_KEY,
            MetadataValue::try_from(format!("Bearer {ADMIN_TOKEN}")).unwrap(),
        );

        let opened = authority.open(&bootstrap, 10).unwrap();

        assert_eq!(opened.role, FlightSessionRole::Administrator);
        assert_eq!(authority.status().active_administrator_sessions, 1);
        assert!(FlightBearerAuthConfig::try_new(
            BOOTSTRAP_TOKEN.as_bytes(),
            Duration::from_secs(60),
            2,
        )
        .unwrap()
        .with_admin_token(BOOTSTRAP_TOKEN.as_bytes())
        .is_err());
    }
}
