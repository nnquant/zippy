//! Stable conversion between Zippy errors and gRPC status values.

use std::num::NonZeroU64;

use bytes::Bytes;
use prost::Message;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Code, Status};
use zippy_types::{
    ErrorCode, LogOffset, OffsetWatermark, RequestId, ResourceLimit, ServerError,
    ServerErrorDetails, TableId,
};

use crate::wire::error_envelope::Details;
use crate::wire::offset_watermark::State as OffsetState;
use crate::wire::resource_limit::Kind as ResourceLimitKind;
use crate::wire::{
    DataCorruption, ErrorEnvelope, ExpectedActual, OffsetExpired, ResourceLimitExceeded,
    RevisionConflict, UnsupportedPlan,
};
use crate::{CURRENT_PROTOCOL_VERSION, ERROR_METADATA_KEY};

/// Converts one structured server error to a gRPC status without relying on
/// human-readable message parsing.
#[must_use]
pub fn server_error_to_status(error: &ServerError) -> Status {
    let encoded = encode_error(error).encode_to_vec();
    let mut metadata = MetadataMap::new();
    metadata.insert_bin(ERROR_METADATA_KEY, MetadataValue::from_bytes(&encoded));
    // Arrow C++ only reconstructs grpc-status-details-bin when x-arrow-status is present.
    // Use Arrow IOError (5); the original gRPC code remains authoritative, and the binary
    // envelope carries Zippy's exact code/retryability for all Python FlightError variants.
    metadata.insert("x-arrow-status", MetadataValue::from_static("5"));
    metadata.insert_bin(
        "x-arrow-status-message-bin",
        MetadataValue::from_bytes(error.message().as_bytes()),
    );
    Status::with_details_and_metadata(
        grpc_code(error.code()),
        error.message().to_owned(),
        Bytes::from(encoded),
        metadata,
    )
}

/// Decodes a structured Zippy error from a gRPC status.
///
/// Malformed or unknown details fall back conservatively to the standard gRPC
/// code. The human-readable message is retained for diagnostics but never
/// parsed to recover machine state.
#[must_use]
pub fn status_to_server_error(status: &Status) -> ServerError {
    let details = status.details();
    let metadata = status
        .metadata()
        .get_bin(ERROR_METADATA_KEY)
        .and_then(|value| value.to_bytes().ok());
    let bytes = match (details.is_empty(), metadata.as_ref()) {
        (false, Some(metadata)) if metadata.as_ref() == details => Some(details),
        (false, Some(_)) => None,
        (false, None) => Some(details),
        (true, Some(metadata)) => Some(metadata.as_ref()),
        (true, None) => None,
    };
    bytes
        .and_then(|bytes| ErrorEnvelope::decode(bytes).ok())
        .and_then(decode_error)
        .unwrap_or_else(|| {
            ServerError::new(error_code_from_grpc(status.code()), status.message()).with_retryable(
                matches!(status.code(), Code::ResourceExhausted | Code::Unavailable),
            )
        })
}

fn encode_error(error: &ServerError) -> ErrorEnvelope {
    ErrorEnvelope {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        code: error.code().as_str().to_owned(),
        message: error.message().to_owned(),
        retryable: error.retryable(),
        request_id: error
            .request_id()
            .map(|value| value.into_bytes().to_vec().into()),
        query_id: error
            .query_id()
            .map(|value| value.into_bytes().to_vec().into()),
        table_id: error
            .table_id()
            .map(|value| value.into_bytes().to_vec().into()),
        details: error.details().map(details_to_wire),
    }
}

fn decode_error(wire: ErrorEnvelope) -> Option<ServerError> {
    if wire.protocol_version != CURRENT_PROTOCOL_VERSION {
        return None;
    }
    let code = error_code_from_str(&wire.code)?;
    let mut error = ServerError::new(code, wire.message).with_retryable(wire.retryable);
    if let Some(value) = wire.request_id {
        error = error.with_request_id(decode_id(&value, RequestId::from_bytes)?);
    }
    if let Some(value) = wire.query_id {
        error = error.with_query_id(decode_id(&value, zippy_types::QueryId::from_bytes)?);
    }
    if let Some(value) = wire.table_id {
        error = error.with_table_id(decode_id(&value, TableId::from_bytes)?);
    }
    if let Some(details) = wire.details {
        error = error.with_details(details_from_wire(details)?).ok()?;
    }
    Some(error)
}

fn decode_id<T>(
    bytes: &[u8],
    constructor: impl FnOnce([u8; 16]) -> Result<T, zippy_types::id::NilStableIdError>,
) -> Option<T> {
    constructor(bytes.try_into().ok()?).ok()
}

fn details_to_wire(details: &ServerErrorDetails) -> Details {
    match details {
        ServerErrorDetails::RevisionConflict { expected, actual } => {
            Details::RevisionConflict(RevisionConflict {
                expected: *expected,
                actual: *actual,
            })
        }
        ServerErrorDetails::ProducerEpoch { expected, actual } => {
            Details::ProducerEpoch(ExpectedActual {
                expected: *expected,
                actual: *actual,
            })
        }
        ServerErrorDetails::SequenceGap { expected, actual } => {
            Details::SequenceGap(ExpectedActual {
                expected: *expected,
                actual: *actual,
            })
        }
        ServerErrorDetails::SchemaVersion { expected, actual } => {
            Details::SchemaVersion(ExpectedActual {
                expected: *expected,
                actual: *actual,
            })
        }
        ServerErrorDetails::OffsetExpired {
            requested,
            earliest,
            latest,
            snapshot_recovery_available,
        } => Details::OffsetExpired(OffsetExpired {
            requested: Some(watermark_to_wire(*requested)),
            earliest: Some(watermark_to_wire(*earliest)),
            latest: Some(watermark_to_wire(*latest)),
            snapshot_recovery_available: *snapshot_recovery_available,
        }),
        ServerErrorDetails::ResourceLimit {
            resource,
            limit,
            requested,
        } => Details::ResourceLimit(ResourceLimitExceeded {
            resource: resource.clone(),
            limit: Some(resource_limit_to_wire(*limit)),
            requested: *requested,
        }),
        ServerErrorDetails::DataCorruption {
            object,
            expected_checksum,
            actual_checksum,
        } => Details::DataCorruption(DataCorruption {
            object: object.clone(),
            expected_checksum: expected_checksum.clone(),
            actual_checksum: actual_checksum.clone(),
        }),
        ServerErrorDetails::UnsupportedPlan { operator } => {
            Details::UnsupportedPlan(UnsupportedPlan {
                operator: operator.clone(),
            })
        }
    }
}

fn details_from_wire(details: Details) -> Option<ServerErrorDetails> {
    Some(match details {
        Details::RevisionConflict(value) => ServerErrorDetails::RevisionConflict {
            expected: value.expected,
            actual: value.actual,
        },
        Details::ProducerEpoch(value) => ServerErrorDetails::ProducerEpoch {
            expected: value.expected,
            actual: value.actual,
        },
        Details::SequenceGap(value) => ServerErrorDetails::SequenceGap {
            expected: value.expected,
            actual: value.actual,
        },
        Details::SchemaVersion(value) => ServerErrorDetails::SchemaVersion {
            expected: value.expected,
            actual: value.actual,
        },
        Details::OffsetExpired(value) => ServerErrorDetails::OffsetExpired {
            requested: watermark_from_wire(value.requested?)?,
            earliest: watermark_from_wire(value.earliest?)?,
            latest: watermark_from_wire(value.latest?)?,
            snapshot_recovery_available: value.snapshot_recovery_available,
        },
        Details::ResourceLimit(value) => ServerErrorDetails::ResourceLimit {
            resource: value.resource,
            limit: resource_limit_from_wire(value.limit?)?,
            requested: value.requested,
        },
        Details::DataCorruption(value) => ServerErrorDetails::DataCorruption {
            object: value.object,
            expected_checksum: value.expected_checksum,
            actual_checksum: value.actual_checksum,
        },
        Details::UnsupportedPlan(value) => ServerErrorDetails::UnsupportedPlan {
            operator: value.operator,
        },
    })
}

pub(crate) fn watermark_to_wire(value: OffsetWatermark) -> crate::wire::OffsetWatermark {
    let (state, offset) = match value {
        OffsetWatermark::Empty => (OffsetState::Empty, 0),
        OffsetWatermark::Known(offset) => (OffsetState::Known, offset.get()),
    };
    crate::wire::OffsetWatermark {
        state: state as i32,
        offset,
    }
}

fn watermark_from_wire(value: crate::wire::OffsetWatermark) -> Option<OffsetWatermark> {
    match OffsetState::try_from(value.state).ok()? {
        OffsetState::Empty if value.offset == 0 => Some(OffsetWatermark::Empty),
        OffsetState::Known => Some(OffsetWatermark::Known(LogOffset::new(value.offset))),
        OffsetState::Unspecified | OffsetState::Empty => None,
    }
}

pub(crate) fn resource_limit_to_wire(value: ResourceLimit) -> crate::wire::ResourceLimit {
    let (kind, bytes) = match value {
        ResourceLimit::ServerDefault => (ResourceLimitKind::ServerDefault, 0),
        ResourceLimit::Disabled => (ResourceLimitKind::Disabled, 0),
        ResourceLimit::Bytes(bytes) => (ResourceLimitKind::Bytes, bytes.get()),
    };
    crate::wire::ResourceLimit {
        kind: kind as i32,
        bytes,
    }
}

pub(crate) fn resource_limit_from_wire(value: crate::wire::ResourceLimit) -> Option<ResourceLimit> {
    match ResourceLimitKind::try_from(value.kind).ok()? {
        ResourceLimitKind::ServerDefault if value.bytes == 0 => Some(ResourceLimit::ServerDefault),
        ResourceLimitKind::Disabled if value.bytes == 0 => Some(ResourceLimit::Disabled),
        ResourceLimitKind::Bytes => NonZeroU64::new(value.bytes).map(ResourceLimit::Bytes),
        ResourceLimitKind::Unspecified
        | ResourceLimitKind::ServerDefault
        | ResourceLimitKind::Disabled => None,
    }
}

fn grpc_code(code: ErrorCode) -> Code {
    match code {
        ErrorCode::InvalidArgument => Code::InvalidArgument,
        ErrorCode::Unauthenticated => Code::Unauthenticated,
        ErrorCode::PermissionDenied => Code::PermissionDenied,
        ErrorCode::NotFound => Code::NotFound,
        ErrorCode::AlreadyExists => Code::AlreadyExists,
        ErrorCode::Conflict => Code::Aborted,
        ErrorCode::StaleProducerEpoch | ErrorCode::SequenceGap | ErrorCode::SchemaMismatch => {
            Code::FailedPrecondition
        }
        ErrorCode::OffsetExpired => Code::OutOfRange,
        ErrorCode::ResourceExhausted => Code::ResourceExhausted,
        ErrorCode::DeadlineExceeded => Code::DeadlineExceeded,
        ErrorCode::Cancelled => Code::Cancelled,
        ErrorCode::Unavailable => Code::Unavailable,
        ErrorCode::DataCorruption => Code::DataLoss,
        ErrorCode::Internal => Code::Internal,
        ErrorCode::UnsupportedPlan => Code::Unimplemented,
    }
}

fn error_code_from_grpc(code: Code) -> ErrorCode {
    match code {
        Code::InvalidArgument => ErrorCode::InvalidArgument,
        Code::Unauthenticated => ErrorCode::Unauthenticated,
        Code::PermissionDenied => ErrorCode::PermissionDenied,
        Code::NotFound => ErrorCode::NotFound,
        Code::AlreadyExists => ErrorCode::AlreadyExists,
        Code::Aborted => ErrorCode::Conflict,
        Code::FailedPrecondition => ErrorCode::Conflict,
        Code::OutOfRange => ErrorCode::OffsetExpired,
        Code::ResourceExhausted => ErrorCode::ResourceExhausted,
        Code::DeadlineExceeded => ErrorCode::DeadlineExceeded,
        Code::Cancelled => ErrorCode::Cancelled,
        Code::Unavailable => ErrorCode::Unavailable,
        Code::DataLoss => ErrorCode::DataCorruption,
        Code::Unimplemented => ErrorCode::UnsupportedPlan,
        Code::Unknown | Code::Ok | Code::Internal => ErrorCode::Internal,
    }
}

fn error_code_from_str(value: &str) -> Option<ErrorCode> {
    Some(match value {
        "INVALID_ARGUMENT" => ErrorCode::InvalidArgument,
        "UNAUTHENTICATED" => ErrorCode::Unauthenticated,
        "PERMISSION_DENIED" => ErrorCode::PermissionDenied,
        "NOT_FOUND" => ErrorCode::NotFound,
        "ALREADY_EXISTS" => ErrorCode::AlreadyExists,
        "CONFLICT" => ErrorCode::Conflict,
        "STALE_PRODUCER_EPOCH" => ErrorCode::StaleProducerEpoch,
        "SEQUENCE_GAP" => ErrorCode::SequenceGap,
        "SCHEMA_MISMATCH" => ErrorCode::SchemaMismatch,
        "OFFSET_EXPIRED" => ErrorCode::OffsetExpired,
        "RESOURCE_EXHAUSTED" => ErrorCode::ResourceExhausted,
        "DEADLINE_EXCEEDED" => ErrorCode::DeadlineExceeded,
        "CANCELLED" => ErrorCode::Cancelled,
        "UNAVAILABLE" => ErrorCode::Unavailable,
        "DATA_CORRUPTION" => ErrorCode::DataCorruption,
        "INTERNAL" => ErrorCode::Internal,
        "UNSUPPORTED_PLAN" => ErrorCode::UnsupportedPlan,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_error_round_trips_without_message_parsing() {
        let source = ServerError::new(ErrorCode::ResourceExhausted, "query budget reached")
            .with_query_id(zippy_types::QueryId::from_u128(7).unwrap())
            .with_details(ServerErrorDetails::ResourceLimit {
                resource: "memory_bytes".to_owned(),
                limit: ResourceLimit::Bytes(NonZeroU64::new(1024).unwrap()),
                requested: 2048,
            })
            .unwrap();
        let status = server_error_to_status(&source);

        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(status.metadata().get("x-arrow-status").unwrap(), "5");
        assert_eq!(
            status
                .metadata()
                .get_bin("x-arrow-status-message-bin")
                .unwrap()
                .to_bytes()
                .unwrap(),
            source.message().as_bytes(),
        );
        assert_eq!(status_to_server_error(&status), source);
    }

    #[test]
    fn wire_watermarks_reject_removed_state_and_preserve_empty_vs_zero() {
        assert_eq!(
            super::watermark_from_wire(crate::wire::OffsetWatermark {
                state: 3,
                offset: 0
            }),
            None
        );
        for watermark in [
            zippy_types::OffsetWatermark::Empty,
            zippy_types::OffsetWatermark::Known(zippy_types::LogOffset::new(0)),
            zippy_types::OffsetWatermark::Known(zippy_types::LogOffset::new(u64::MAX)),
        ] {
            assert_eq!(
                super::watermark_from_wire(super::watermark_to_wire(watermark)),
                Some(watermark)
            );
        }
    }

    #[test]
    fn mismatched_metadata_and_details_fail_closed() {
        let source = ServerError::new(ErrorCode::Unavailable, "server starting");
        let mut status = server_error_to_status(&source);
        status
            .metadata_mut()
            .insert_bin(ERROR_METADATA_KEY, MetadataValue::from_bytes(b"tampered"));

        let decoded = status_to_server_error(&status);
        assert_eq!(decoded.code(), ErrorCode::Unavailable);
        assert_eq!(decoded.message(), "server starting");
    }
}
