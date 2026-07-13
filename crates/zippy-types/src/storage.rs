//! Three-phase coordination of exact catalog versions with live storage state.

use std::any::Any;

use serde::{Deserialize, Serialize};

use crate::catalog::{CatalogQuerySnapshot, CatalogSnapshotSelector, CatalogTableSnapshot};
use crate::table::{SourceSegmentIdentity, TableDescriptor};
use crate::{BoxFuture, LeaseId, OffsetWatermark, QueryId, Result, TableId};

/// A read-only segment descriptor fixed at a committed high watermark.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentDescriptor {
    /// The identity used for persisted/live lineage comparison.
    pub identity: SourceSegmentIdentity,
    /// The schema version encoded by the segment.
    pub schema_version: u64,
    /// The implementation-specific local segment locator.
    pub location: String,
    /// The number of committed rows visible to this snapshot.
    pub committed_rows: u64,
    /// The greatest logical offset visible in this segment prefix.
    pub committed_log_offset: OffsetWatermark,
}

/// A request to pin live table-actor state before reading file manifests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinLiveSnapshotsRequest {
    /// The query that will own all resulting in-memory leases.
    pub query_id: QueryId,
    /// Catalog-resolved table descriptors in deterministic table-id order.
    pub tables: Vec<TableDescriptor>,
}

/// Cloneable data describing one table actor's live pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveTableSnapshotPinDescriptor {
    /// The in-memory lease identity protecting segment resources.
    pub lease_id: LeaseId,
    /// The pinned table.
    pub table_id: TableId,
    /// The exact persisted snapshot version observed in actor write order.
    pub observed_persisted_snapshot_version: u64,
    /// The schema version observed in the same actor turn.
    pub observed_schema_version: u64,
    /// The producer fencing epoch observed in the same actor turn.
    pub writer_epoch: u64,
    /// The active segment and fixed committed prefix, when present.
    pub active_segment: Option<SegmentDescriptor>,
    /// Sealed segments retained at the actor boundary.
    pub retained_sealed_segments: Vec<SegmentDescriptor>,
    /// The greatest logical offset visible across the pinned live sources.
    pub live_high_watermark: OffsetWatermark,
    /// The lease expiration time as Unix nanoseconds in UTC.
    pub expires_at_ns: u64,
}

/// Cloneable audit data for all live pins acquired by one query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveSnapshotPinsDescriptor {
    /// The query that owns the pins.
    pub query_id: QueryId,
    /// Pins ordered by immutable table identity.
    pub tables: Vec<LiveTableSnapshotPinDescriptor>,
}

impl LiveSnapshotPinsDescriptor {
    /// Builds exact historical catalog selectors from actor-observed versions.
    #[must_use]
    pub fn catalog_selectors(&self) -> Vec<CatalogSnapshotSelector> {
        self.tables
            .iter()
            .map(|pin| CatalogSnapshotSelector {
                table_id: pin.table_id,
                snapshot_version: pin.observed_persisted_snapshot_version,
                schema_version: pin.observed_schema_version,
            })
            .collect()
    }
}

/// Exclusive ownership of unfinalized live snapshot leases.
///
/// This guard is intentionally non-cloneable and non-serializable. `release`
/// consumes ownership. Implementations must enqueue release exactly once on
/// drop, and `renew` must atomically validate current lease generation and
/// expiration before extending it. A storage engine consumes `into_any`,
/// downcasts to its private guard type, and validates a private instance nonce;
/// the concrete type alone cannot distinguish two storage-engine instances.
pub trait LiveSnapshotPinGuard: Any + Send {
    /// Borrows cloneable audit and catalog-selection data.
    fn descriptor(&self) -> &LiveSnapshotPinsDescriptor;

    /// Renews every owned live lease to an absolute Unix-nanosecond expiry.
    fn renew(&mut self, expires_at_ns: u64) -> BoxFuture<'_, Result<()>>;

    /// Explicitly releases every owned live lease exactly once.
    fn release(self: Box<Self>) -> BoxFuture<'static, Result<()>>;

    /// Consumes the guard into the storage engine's origin-check boundary.
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send>;
}

/// A type-erased exclusive live-pin guard.
///
/// ```compile_fail
/// fn duplicate(guard: zippy_types::storage::OpaqueLiveSnapshotPins) {
///     let _second_owner = guard.clone();
/// }
/// ```
pub type OpaqueLiveSnapshotPins = Box<dyn LiveSnapshotPinGuard>;

/// Cloneable data for an immutable persisted and live table boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableReadSnapshotDescriptor {
    /// The exact historical catalog descriptor, schema, snapshot, and files.
    pub catalog: CatalogTableSnapshot,
    /// The producer fencing epoch validated against the table actor.
    pub writer_epoch: u64,
    /// The active segment and committed high watermark, when present.
    pub active_segment: Option<SegmentDescriptor>,
    /// Pinned sealed segments not represented by persisted file lineage.
    pub retained_sealed_segments: Vec<SegmentDescriptor>,
    /// The greatest logical offset visible across persisted and live sources.
    pub max_log_offset: OffsetWatermark,
    /// The in-memory lease protecting live resources in this table snapshot.
    pub lease_id: LeaseId,
}

impl TableReadSnapshotDescriptor {
    /// Returns the immutable table identifier.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.catalog.table().table_id
    }
}

/// Cloneable audit data for finalized table read boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryReadSnapshotDescriptor {
    /// The query that owns the snapshot leases.
    pub query_id: QueryId,
    /// The catalog revision shared by all exact historical reads.
    pub catalog_revision: u64,
    /// Table snapshots in deterministic table-id order.
    pub tables: Vec<TableReadSnapshotDescriptor>,
}

/// Exclusive ownership of finalized query snapshot leases.
///
/// The guard transfers from the planner into the one-time query execution
/// token, then into the result stream. It is intentionally non-cloneable and
/// non-serializable. Drop must enqueue an idempotent release exactly once. A
/// consumer may use `into_any` to validate concrete origin and a private
/// storage-instance nonce before taking ownership.
pub trait QueryReadSnapshotGuard: Any + Send {
    /// Borrows cloneable query snapshot audit data.
    fn descriptor(&self) -> &QueryReadSnapshotDescriptor;

    /// Renews every owned snapshot lease to an absolute Unix-nanosecond expiry.
    fn renew(&mut self, expires_at_ns: u64) -> BoxFuture<'_, Result<()>>;

    /// Explicitly releases every owned snapshot lease exactly once.
    fn release(self: Box<Self>) -> BoxFuture<'static, Result<()>>;

    /// Consumes the guard into a concrete origin-check boundary.
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send>;
}

/// A type-erased exclusive finalized snapshot guard.
///
/// ```compile_fail
/// fn duplicate(guard: zippy_types::storage::OpaqueQueryReadSnapshot) {
///     let _second_owner = guard.clone();
/// }
/// ```
pub type OpaqueQueryReadSnapshot = Box<dyn QueryReadSnapshotGuard>;

/// Coordinates live segment pins with exact historical catalog manifests.
///
/// The required protocol is:
///
/// 1. `pin_live_snapshots` fixes actor state and owns its live leases.
/// 2. [`crate::catalog::CatalogRead::read_snapshot_at`] reads exact persisted
///    and schema versions selected by the guard descriptor.
/// 3. `validate_and_finalize` consumes the live guard and transfers lease
///    ownership into a finalized query guard.
///
/// A mismatch must release consumed pins before returning a retryable conflict.
/// If phase two fails, the caller explicitly releases or drops the live guard.
pub trait StorageEngine: Send + Sync {
    /// Pins actor-owned live boundaries in stable table-id order.
    fn pin_live_snapshots(
        &self,
        request: PinLiveSnapshotsRequest,
    ) -> BoxFuture<'_, Result<OpaqueLiveSnapshotPins>>;

    /// Validates exact catalog versions and transfers lease ownership.
    fn validate_and_finalize(
        &self,
        pins: OpaqueLiveSnapshotPins,
        catalog: CatalogQuerySnapshot,
    ) -> BoxFuture<'_, Result<OpaqueQueryReadSnapshot>>;
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::{
        LiveSnapshotPinGuard, LiveSnapshotPinsDescriptor, LiveTableSnapshotPinDescriptor,
        OpaqueLiveSnapshotPins, OpaqueQueryReadSnapshot, QueryReadSnapshotDescriptor,
        QueryReadSnapshotGuard, StorageEngine,
    };
    use crate::catalog::CatalogSnapshotSelector;
    use crate::{BoxFuture, LeaseId, OffsetWatermark, QueryId, Result, TableId};

    struct TestLiveGuard {
        descriptor: LiveSnapshotPinsDescriptor,
        instance_nonce: u128,
    }

    impl LiveSnapshotPinGuard for TestLiveGuard {
        fn descriptor(&self) -> &LiveSnapshotPinsDescriptor {
            &self.descriptor
        }

        fn renew(&mut self, _expires_at_ns: u64) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn release(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }

    struct ForeignLiveGuard(TestLiveGuard);

    impl LiveSnapshotPinGuard for ForeignLiveGuard {
        fn descriptor(&self) -> &LiveSnapshotPinsDescriptor {
            &self.0.descriptor
        }

        fn renew(&mut self, _expires_at_ns: u64) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn release(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }

    struct TestQueryGuard {
        descriptor: QueryReadSnapshotDescriptor,
        instance_nonce: u128,
    }

    impl QueryReadSnapshotGuard for TestQueryGuard {
        fn descriptor(&self) -> &QueryReadSnapshotDescriptor {
            &self.descriptor
        }

        fn renew(&mut self, _expires_at_ns: u64) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn release(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }

    struct ForeignQueryGuard(TestQueryGuard);

    impl QueryReadSnapshotGuard for ForeignQueryGuard {
        fn descriptor(&self) -> &QueryReadSnapshotDescriptor {
            &self.0.descriptor
        }

        fn renew(&mut self, _expires_at_ns: u64) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn release(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }

    fn empty_pin_descriptor() -> LiveSnapshotPinsDescriptor {
        LiveSnapshotPinsDescriptor {
            query_id: QueryId::from_u128(1).unwrap(),
            tables: Vec::new(),
        }
    }

    fn live_guard_is_local(guard: OpaqueLiveSnapshotPins, instance_nonce: u128) -> bool {
        guard
            .into_any()
            .downcast::<TestLiveGuard>()
            .is_ok_and(|guard| guard.instance_nonce == instance_nonce)
    }

    fn query_guard_is_local(guard: OpaqueQueryReadSnapshot, instance_nonce: u128) -> bool {
        guard
            .into_any()
            .downcast::<TestQueryGuard>()
            .is_ok_and(|guard| guard.instance_nonce == instance_nonce)
    }

    fn assert_storage_object_safe(_: Option<&dyn StorageEngine>) {}
    fn assert_live_guard_object_safe(_: Option<OpaqueLiveSnapshotPins>) {}
    fn assert_query_guard_object_safe(_: Option<OpaqueQueryReadSnapshot>) {}
    fn assert_live_trait_object_safe(_: Option<&dyn LiveSnapshotPinGuard>) {}
    fn assert_query_trait_object_safe(_: Option<&dyn QueryReadSnapshotGuard>) {}

    #[test]
    fn storage_and_exclusive_guards_are_object_safe() {
        assert_storage_object_safe(None);
        assert_live_guard_object_safe(None);
        assert_query_guard_object_safe(None);
        assert_live_trait_object_safe(None);
        assert_query_trait_object_safe(None);
    }

    #[test]
    fn storage_origin_check_rejects_foreign_guard_type_and_nonce() {
        let guard = |instance_nonce| TestLiveGuard {
            descriptor: empty_pin_descriptor(),
            instance_nonce,
        };

        assert!(live_guard_is_local(Box::new(guard(7)), 7));
        assert!(!live_guard_is_local(Box::new(guard(8)), 7));
        assert!(!live_guard_is_local(
            Box::new(ForeignLiveGuard(guard(7))),
            7
        ));

        let query_guard = |instance_nonce| TestQueryGuard {
            descriptor: QueryReadSnapshotDescriptor {
                query_id: QueryId::from_u128(2).unwrap(),
                catalog_revision: 3,
                tables: Vec::new(),
            },
            instance_nonce,
        };
        assert!(query_guard_is_local(Box::new(query_guard(7)), 7));
        assert!(!query_guard_is_local(Box::new(query_guard(8)), 7));
        assert!(!query_guard_is_local(
            Box::new(ForeignQueryGuard(query_guard(7))),
            7
        ));
    }

    #[test]
    fn live_pin_selector_binds_snapshot_and_schema_versions() {
        let pins = LiveSnapshotPinsDescriptor {
            query_id: QueryId::from_u128(1).unwrap(),
            tables: vec![LiveTableSnapshotPinDescriptor {
                lease_id: LeaseId::from_u128(2).unwrap(),
                table_id: TableId::from_u128(3).unwrap(),
                observed_persisted_snapshot_version: 4,
                observed_schema_version: 5,
                writer_epoch: 6,
                active_segment: None,
                retained_sealed_segments: Vec::new(),
                live_high_watermark: OffsetWatermark::Empty,
                expires_at_ns: 7,
            }],
        };

        let selectors = pins.catalog_selectors();
        assert_eq!(selectors[0].snapshot_version, 4);
        assert_eq!(selectors[0].schema_version, 5);
        assert!(serde_json::from_str::<CatalogSnapshotSelector>(
            r#"{"table_id":"00000000-0000-0000-0000-000000000003","snapshot_version":4}"#
        )
        .is_err());
    }
}
