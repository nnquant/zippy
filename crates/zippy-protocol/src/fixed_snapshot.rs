//! Bounded session-owned cross-query snapshot actions.

use crate::query::{binding_from_wire, MAX_QUERY_BINDINGS, MAX_QUERY_COMMAND_BYTES};
use crate::wire::{CreateFixedSnapshotRequest, ReleaseFixedSnapshotRequest};
use crate::CURRENT_PROTOCOL_VERSION;
use bytes::Bytes;
use prost::Message;
use std::collections::HashSet;
use zippy_types::table::TableRef;
use zippy_types::{ErrorCode, QueryId, Result, ServerError, SessionId};

/// Creates a new immutable multi-table snapshot lease.
pub const CREATE_FIXED_SNAPSHOT_ACTION: &str = "zippy.create_fixed_snapshot.v1";
/// Releases a session-owned snapshot lease without cancelling queries already prepared from it.
pub const RELEASE_FIXED_SNAPSHOT_ACTION: &str = "zippy.release_fixed_snapshot.v1";
/// Advertised only when capture, query binding, expiry and release share one native authority.
pub const FIXED_SNAPSHOT_CAPABILITY: &str = "query.fixed_snapshot.v1";
/// Maximum requested lifetime, further bounded by the authenticated session expiry.
pub const MAX_FIXED_SNAPSHOT_TTL_MS: u64 = 300_000;

/// Validated capture request with transport-authenticated ownership and a server-issued identity.
#[derive(Debug, Clone)]
pub struct FlightFixedSnapshotRequest {
    /// Boot-scoped server-issued opaque identity, also used for query resource accounting.
    pub snapshot_id: QueryId,
    /// Authenticated owning session; never supplied by the action payload.
    pub session_id: SessionId,
    /// Catalog references captured together.
    pub tables: Vec<TableRef>,
    /// Absolute UTC expiry, bounded by the session authority.
    pub expires_at_ns: u64,
}

pub(crate) fn decode_create(
    body: Bytes,
    snapshot_id: QueryId,
    session_id: SessionId,
    session_expiry: u64,
    now_ns: u64,
) -> Result<FlightFixedSnapshotRequest> {
    if body.is_empty() || body.len() > MAX_QUERY_COMMAND_BYTES {
        return Err(invalid("fixed snapshot request exceeds protocol bounds"));
    }
    let request = CreateFixedSnapshotRequest::decode(body)
        .map_err(|_| invalid("malformed fixed snapshot request"))?;
    if request.protocol_version != CURRENT_PROTOCOL_VERSION
        || request.ttl_ms == 0
        || request.ttl_ms > MAX_FIXED_SNAPSHOT_TTL_MS
        || request.bindings.is_empty()
        || request.bindings.len() > MAX_QUERY_BINDINGS
    {
        return Err(invalid(
            "fixed snapshot version, lifetime, or binding count is invalid",
        ));
    }
    let mut names = HashSet::new();
    let tables = request
        .bindings
        .into_iter()
        .map(|binding| {
            let binding = binding_from_wire(binding)?;
            if !names.insert(binding.sql_name) {
                return Err(invalid("fixed snapshot binding names must be unique"));
            }
            Ok(binding.table)
        })
        .collect::<Result<Vec<_>>>()?;
    let expires_at_ns = now_ns
        .checked_add(request.ttl_ms * 1_000_000)
        .ok_or_else(|| invalid("fixed snapshot expiry overflowed"))?
        .min(session_expiry);
    if expires_at_ns <= now_ns {
        return Err(ServerError::new(
            ErrorCode::Unauthenticated,
            "fixed snapshot session expired",
        ));
    }
    Ok(FlightFixedSnapshotRequest {
        snapshot_id,
        session_id,
        tables,
        expires_at_ns,
    })
}

pub(crate) fn decode_release(body: Bytes) -> Result<QueryId> {
    if body.is_empty() || body.len() > 128 {
        return Err(invalid("fixed snapshot release exceeds protocol bounds"));
    }
    let request = ReleaseFixedSnapshotRequest::decode(body)
        .map_err(|_| invalid("malformed fixed snapshot release"))?;
    if request.protocol_version != CURRENT_PROTOCOL_VERSION {
        return Err(invalid("fixed snapshot release version is unsupported"));
    }
    let bytes: [u8; 16] = request
        .snapshot_id
        .as_ref()
        .try_into()
        .map_err(|_| invalid("fixed snapshot identity must contain 16 bytes"))?;
    QueryId::from_bytes(bytes).map_err(|_| invalid("fixed snapshot identity must be nonzero"))
}

fn invalid(message: &str) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{table_binding, TableBinding};

    fn request() -> CreateFixedSnapshotRequest {
        CreateFixedSnapshotRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            bindings: vec![TableBinding {
                sql_name: "source".to_owned(),
                table: Some(table_binding::Table::TableId(Bytes::from_static(&[3; 16]))),
            }],
            ttl_ms: 100,
        }
    }

    fn decode(request: CreateFixedSnapshotRequest) -> Result<FlightFixedSnapshotRequest> {
        decode_create(
            request.encode_to_vec().into(),
            QueryId::from_bytes([1; 16]).unwrap(),
            SessionId::from_bytes([2; 16]).unwrap(),
            10_000_000,
            1,
        )
    }

    #[test]
    fn capture_is_bounded_by_authenticated_session_and_validates_bindings() {
        let capture = decode(request()).unwrap();
        assert_eq!(capture.expires_at_ns, 10_000_000);
        assert_eq!(capture.session_id, SessionId::from_bytes([2; 16]).unwrap());
        for ttl in [0, MAX_FIXED_SNAPSHOT_TTL_MS + 1] {
            let mut invalid = request();
            invalid.ttl_ms = ttl;
            assert_eq!(
                decode(invalid).unwrap_err().code(),
                ErrorCode::InvalidArgument
            );
        }
        let mut duplicate = request();
        duplicate.bindings.push(duplicate.bindings[0].clone());
        assert_eq!(
            decode(duplicate).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );
        let mut empty = request();
        empty.bindings.clear();
        assert_eq!(
            decode(empty).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );
        let mut version = request();
        version.protocol_version += 1;
        assert_eq!(
            decode(version).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );
        let expired = decode_create(
            request().encode_to_vec().into(),
            capture.snapshot_id,
            capture.session_id,
            1,
            1,
        )
        .unwrap_err();
        assert_eq!(expired.code(), ErrorCode::Unauthenticated);
    }

    #[test]
    fn release_rejects_missing_nil_oversized_and_unversioned_identities() {
        for bytes in [vec![], vec![1; 15], vec![1; 17], vec![0; 16]] {
            let request = ReleaseFixedSnapshotRequest {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                snapshot_id: bytes.into(),
            };
            assert_eq!(
                decode_release(request.encode_to_vec().into())
                    .unwrap_err()
                    .code(),
                ErrorCode::InvalidArgument
            );
        }
        let mut release = ReleaseFixedSnapshotRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            snapshot_id: Bytes::from_static(&[1; 16]),
        };
        assert_eq!(
            decode_release(release.encode_to_vec().into()).unwrap(),
            QueryId::from_bytes([1; 16]).unwrap()
        );
        release.protocol_version += 1;
        assert!(decode_release(release.encode_to_vec().into()).is_err());
        assert!(decode_release(Bytes::from(vec![1; 129])).is_err());
        assert!(decode_release(Bytes::from_static(b"\xff")).is_err());
    }
}
