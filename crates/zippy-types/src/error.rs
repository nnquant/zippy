//! Structured server errors shared by every service boundary.

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{OffsetWatermark, QueryId, RequestId, ResourceLimit, TableId};

/// A result returned by stable Zippy service contracts.
pub type Result<T> = std::result::Result<T, ServerError>;

/// A stable machine-readable server error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    /// A request field or combination of fields is invalid.
    InvalidArgument,
    /// No valid authentication identity was provided.
    Unauthenticated,
    /// The authenticated identity is not authorized.
    PermissionDenied,
    /// A requested resource does not exist.
    NotFound,
    /// A resource with the requested identity already exists.
    AlreadyExists,
    /// The request conflicts with current state or an expected revision.
    Conflict,
    /// An ingest producer used an obsolete fencing epoch.
    StaleProducerEpoch,
    /// An ingest producer skipped a required sequence number.
    SequenceGap,
    /// A schema does not satisfy the table or operation contract.
    SchemaMismatch,
    /// A requested subscription offset is no longer retained.
    OffsetExpired,
    /// Admission or execution exceeded a configured resource budget.
    ResourceExhausted,
    /// The request exceeded its deadline.
    DeadlineExceeded,
    /// The request was explicitly cancelled.
    Cancelled,
    /// The service is temporarily unavailable.
    Unavailable,
    /// Persistent data or metadata failed integrity validation.
    DataCorruption,
    /// An unexpected internal invariant failed.
    Internal,
    /// The request uses a valid feature that this server cannot execute.
    UnsupportedPlan,
}

impl ErrorCode {
    /// Returns the stable uppercase protocol representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::NotFound => "NOT_FOUND",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::Conflict => "CONFLICT",
            Self::StaleProducerEpoch => "STALE_PRODUCER_EPOCH",
            Self::SequenceGap => "SEQUENCE_GAP",
            Self::SchemaMismatch => "SCHEMA_MISMATCH",
            Self::OffsetExpired => "OFFSET_EXPIRED",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::Cancelled => "CANCELLED",
            Self::Unavailable => "UNAVAILABLE",
            Self::DataCorruption => "DATA_CORRUPTION",
            Self::Internal => "INTERNAL",
            Self::UnsupportedPlan => "UNSUPPORTED_PLAN",
        }
    }

    /// Returns the conservative default retry policy for this code.
    ///
    /// Individual errors may override this policy through
    /// [`ServerError::retryable`].
    #[must_use]
    pub const fn retryable_by_default(self) -> bool {
        matches!(self, Self::ResourceExhausted | Self::Unavailable)
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable, typed context for errors that clients may act on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerErrorDetails {
    /// An optimistic catalog revision did not match current state.
    RevisionConflict {
        /// The revision required by the caller.
        expected: u64,
        /// The authoritative revision observed by the server.
        actual: u64,
    },
    /// A producer used a fenced writer epoch.
    ProducerEpoch {
        /// The currently active epoch.
        expected: u64,
        /// The stale epoch supplied by the producer.
        actual: u64,
    },
    /// A producer skipped the next contiguous sequence.
    SequenceGap {
        /// The required next sequence.
        expected: u64,
        /// The supplied sequence.
        actual: u64,
    },
    /// A request used a schema version other than the bound version.
    SchemaVersion {
        /// The bound schema version.
        expected: u64,
        /// The supplied schema version.
        actual: u64,
    },
    /// A requested log boundary is no longer available.
    OffsetExpired {
        /// The requested replay boundary.
        requested: OffsetWatermark,
        /// The earliest retained boundary.
        earliest: OffsetWatermark,
        /// The current committed boundary.
        latest: OffsetWatermark,
        /// Whether an exact table snapshot can recover state before tailing.
        snapshot_recovery_available: bool,
    },
    /// A measured resource demand exceeded its explicit budget.
    ResourceLimit {
        /// Stable resource name such as `memory_bytes`.
        resource: String,
        /// The server-default, disabled, or explicit nonzero byte policy.
        limit: ResourceLimit,
        /// The measured or requested amount in the resource's documented unit.
        requested: u64,
    },
    /// Persistent content failed an integrity check.
    DataCorruption {
        /// Stable object locator or identity.
        object: String,
        /// Expected algorithm-prefixed checksum, when available.
        expected_checksum: Option<String>,
        /// Observed algorithm-prefixed checksum, when available.
        actual_checksum: Option<String>,
    },
    /// A valid logical operator has no executable implementation.
    UnsupportedPlan {
        /// Stable logical operator name.
        operator: String,
    },
}

impl ServerErrorDetails {
    /// Returns the sole error code compatible with this detail variant.
    #[must_use]
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::RevisionConflict { .. } => ErrorCode::Conflict,
            Self::ProducerEpoch { .. } => ErrorCode::StaleProducerEpoch,
            Self::SequenceGap { .. } => ErrorCode::SequenceGap,
            Self::SchemaVersion { .. } => ErrorCode::SchemaMismatch,
            Self::OffsetExpired { .. } => ErrorCode::OffsetExpired,
            Self::ResourceLimit { .. } => ErrorCode::ResourceExhausted,
            Self::DataCorruption { .. } => ErrorCode::DataCorruption,
            Self::UnsupportedPlan { .. } => ErrorCode::UnsupportedPlan,
        }
    }
}

/// A detail payload was attached to an incompatible error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("server error detail/code mismatch code=[{code}] expected=[{expected}]")]
pub struct InvalidServerErrorDetails {
    /// The error code supplied by the caller or wire payload.
    pub code: ErrorCode,
    /// The sole code required by the detail variant.
    pub expected: ErrorCode,
}

/// A structured error that is safe to translate across process boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Error)]
#[error("{code}: {message}")]
pub struct ServerError {
    /// The machine-readable error code.
    code: ErrorCode,
    /// A concise human-readable explanation that clients must not parse.
    message: String,
    /// Whether retrying the same logical operation may succeed.
    retryable: bool,
    /// The related request identifier, when known.
    request_id: Option<RequestId>,
    /// The related query identifier, when known.
    query_id: Option<QueryId>,
    /// The related table identifier, when known.
    table_id: Option<TableId>,
    /// Typed context whose variant is selected by the error code.
    details: Option<Box<ServerErrorDetails>>,
}

impl ServerError {
    /// Creates an error using the code's conservative retry default.
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: code.retryable_by_default(),
            request_id: None,
            query_id: None,
            table_id: None,
            details: None,
        }
    }

    /// Returns the machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    /// Borrows the human-readable, non-parseable message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns whether retrying the logical operation may succeed.
    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    /// Returns the related request identity, when known.
    #[must_use]
    pub const fn request_id(&self) -> Option<RequestId> {
        self.request_id
    }

    /// Returns the related query identity, when known.
    #[must_use]
    pub const fn query_id(&self) -> Option<QueryId> {
        self.query_id
    }

    /// Returns the related table identity, when known.
    #[must_use]
    pub const fn table_id(&self) -> Option<TableId> {
        self.table_id
    }

    /// Borrows validated typed details, when present.
    #[must_use]
    pub fn details(&self) -> Option<&ServerErrorDetails> {
        self.details.as_deref()
    }

    /// Associates the error with a request.
    #[must_use]
    pub fn with_request_id(mut self, request_id: RequestId) -> Self {
        self.request_id = Some(request_id);
        self
    }

    /// Associates the error with a query.
    #[must_use]
    pub fn with_query_id(mut self, query_id: QueryId) -> Self {
        self.query_id = Some(query_id);
        self
    }

    /// Associates the error with a table.
    #[must_use]
    pub fn with_table_id(mut self, table_id: TableId) -> Self {
        self.table_id = Some(table_id);
        self
    }

    /// Overrides whether the operation may be retried.
    #[must_use]
    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    /// Attaches one stable machine-readable detail payload.
    pub fn with_details(
        mut self,
        details: ServerErrorDetails,
    ) -> std::result::Result<Self, InvalidServerErrorDetails> {
        let expected = details.error_code();
        if self.code != expected {
            return Err(InvalidServerErrorDetails {
                code: self.code,
                expected,
            });
        }
        self.details = Some(Box::new(details));
        Ok(self)
    }
}

impl<'de> Deserialize<'de> for ServerError {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawError {
            code: ErrorCode,
            message: String,
            retryable: bool,
            request_id: Option<RequestId>,
            query_id: Option<QueryId>,
            table_id: Option<TableId>,
            details: Option<Box<ServerErrorDetails>>,
        }
        let raw = RawError::deserialize(deserializer)?;
        let mut error = Self::new(raw.code, raw.message).with_retryable(raw.retryable);
        error.request_id = raw.request_id;
        error.query_id = raw.query_id;
        error.table_id = raw.table_id;
        if let Some(details) = raw.details {
            error = error
                .with_details(*details)
                .map_err(serde::de::Error::custom)?;
        }
        Ok(error)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::{ErrorCode, ServerError, ServerErrorDetails};
    use crate::{RequestId, ResourceLimit, TableId};

    #[test]
    fn structured_error_preserves_machine_readable_context() {
        let error = ServerError::new(ErrorCode::Conflict, "catalog revision changed")
            .with_request_id(RequestId::from_u128(1).unwrap())
            .with_table_id(TableId::from_u128(2).unwrap())
            .with_details(ServerErrorDetails::RevisionConflict {
                expected: 4,
                actual: 5,
            })
            .unwrap();
        let encoded = serde_json::to_string(&error).expect("serialize server error");
        let decoded: ServerError =
            serde_json::from_str(&encoded).expect("deserialize server error");

        assert_eq!(decoded, error);
        assert_eq!(decoded.code().to_string(), "CONFLICT");
        assert!(!decoded.retryable());
        assert!(encoded.contains(r#""type":"revision_conflict""#));
    }

    #[test]
    fn transient_codes_have_conservative_retry_defaults() {
        assert!(ServerError::new(ErrorCode::Unavailable, "starting").retryable());
        assert!(!ServerError::new(ErrorCode::DeadlineExceeded, "too late").retryable());
        assert!(!ServerError::new(ErrorCode::DataCorruption, "checksum mismatch").retryable());
    }

    #[test]
    fn error_code_has_a_golden_protocol_form() {
        assert_eq!(
            serde_json::to_string(&ErrorCode::StaleProducerEpoch).unwrap(),
            "\"STALE_PRODUCER_EPOCH\""
        );
    }

    #[test]
    fn detail_variants_reject_unstructured_or_incomplete_context() {
        assert!(serde_json::from_str::<ServerErrorDetails>(
            r#"{"type":"revision_conflict","expected":4}"#
        )
        .is_err());
        assert!(ServerError::new(ErrorCode::Conflict, "wrong details")
            .with_details(ServerErrorDetails::SequenceGap {
                expected: 2,
                actual: 3,
            })
            .is_err());
        assert!(serde_json::from_str::<ServerError>(concat!(
            r#"{"code":"CONFLICT","message":"wrong details","retryable":false,"#,
            r#""request_id":null,"query_id":null,"table_id":null,"details":{#,
            r#""type":"sequence_gap","expected":2,"actual":3}}"#,
        ))
        .is_err());
        assert!(serde_json::from_str::<ServerErrorDetails>(
            r#"{"type":"arbitrary","key":"value"}"#
        )
        .is_err());
        let details = ServerErrorDetails::ResourceLimit {
            resource: "memory_bytes".to_owned(),
            limit: ResourceLimit::Bytes(NonZeroU64::new(1024).unwrap()),
            requested: 2048,
        };
        assert_eq!(
            serde_json::to_value(details).unwrap()["limit"],
            serde_json::json!({"type": "bytes", "value": 1024})
        );
        assert!(serde_json::from_str::<ServerErrorDetails>(concat!(
            r#"{"type":"resource_limit","resource":"memory_bytes","#,
            r#""limit":{"type":"bytes","value":0},"requested":1}"#,
        ))
        .is_err());
    }

    #[test]
    fn every_detail_variant_has_one_authoritative_error_code() {
        let watermark = crate::OffsetWatermark::Empty;
        let details = [
            ServerErrorDetails::RevisionConflict {
                expected: 1,
                actual: 2,
            },
            ServerErrorDetails::ProducerEpoch {
                expected: 1,
                actual: 2,
            },
            ServerErrorDetails::SequenceGap {
                expected: 1,
                actual: 2,
            },
            ServerErrorDetails::SchemaVersion {
                expected: 1,
                actual: 2,
            },
            ServerErrorDetails::OffsetExpired {
                requested: watermark,
                earliest: watermark,
                latest: watermark,
                snapshot_recovery_available: false,
            },
            ServerErrorDetails::ResourceLimit {
                resource: "memory_bytes".to_owned(),
                limit: ResourceLimit::Disabled,
                requested: 1,
            },
            ServerErrorDetails::DataCorruption {
                object: "file".to_owned(),
                expected_checksum: None,
                actual_checksum: None,
            },
            ServerErrorDetails::UnsupportedPlan {
                operator: "future_operator".to_owned(),
            },
        ];
        assert_eq!(
            details.map(|details| details.error_code()),
            [
                ErrorCode::Conflict,
                ErrorCode::StaleProducerEpoch,
                ErrorCode::SequenceGap,
                ErrorCode::SchemaMismatch,
                ErrorCode::OffsetExpired,
                ErrorCode::ResourceExhausted,
                ErrorCode::DataCorruption,
                ErrorCode::UnsupportedPlan,
            ]
        );
    }
}
