//! Three-phase coordination of exact catalog versions with live storage state.

use std::any::Any;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::catalog::CatalogSnapshotSelector;
use crate::query::QueryInstanceId;
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

/// Cloneable audit data for a pinned segment prefix.
///
/// Physical segment locators intentionally remain behind an opaque storage guard. Query plans,
/// status responses, and logs may clone this value without gaining authority to open a segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentReadSnapshotDescriptor {
    /// The immutable producer lineage identity.
    pub identity: SourceSegmentIdentity,
    /// The schema version encoded by the segment.
    pub schema_version: u64,
    /// The number of committed rows visible to this snapshot.
    pub committed_rows: u64,
    /// The greatest logical offset visible in this segment prefix.
    pub committed_log_offset: OffsetWatermark,
}

impl From<&SegmentDescriptor> for SegmentReadSnapshotDescriptor {
    fn from(segment: &SegmentDescriptor) -> Self {
        Self {
            identity: segment.identity,
            schema_version: segment.schema_version,
            committed_rows: segment.committed_rows,
            committed_log_offset: segment.committed_log_offset,
        }
    }
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
    pub active_segment: Option<SegmentReadSnapshotDescriptor>,
    /// Sealed segments retained at the actor boundary.
    pub retained_sealed_segments: Vec<SegmentReadSnapshotDescriptor>,
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

/// Cloneable audit data for a Catalog snapshot whose physical files remain pinned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSnapshotPinDescriptor {
    /// The Catalog revision observed by the exact multi-table read transaction.
    pub catalog_revision: u64,
    /// Exact table versions in deterministic table-ID order.
    pub selectors: Vec<CatalogSnapshotSelector>,
}

/// Exclusive ownership of exact Catalog manifests protected against physical GC.
///
/// Raw file locations remain inside the concrete implementation. Storage consumes this guard,
/// validates its private origin, and merges it with live table leases. Dropping a guard before
/// finalization releases every process-local Catalog pin it owns.
pub trait CatalogSnapshotPinGuard: Any + Send {
    /// Borrows path-free audit data for the protected Catalog snapshot.
    fn descriptor(&self) -> &CatalogSnapshotPinDescriptor;

    /// Consumes the guard into the Catalog implementation's origin-check boundary.
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send>;
}

/// A type-erased, non-serializable Catalog snapshot pin capability.
pub type OpaqueCatalogSnapshotPin = Box<dyn CatalogSnapshotPinGuard>;

static NEXT_CATALOG_SNAPSHOT_MEMORY_AUTHORITY_ID: AtomicU64 = AtomicU64::new(1);

/// Process-unique identity for one Catalog snapshot memory reservation.
///
/// The authority component distinguishes Catalog instances and the sequence component is allocated
/// monotonically by that authority. It is an accounting identity, not a standalone capability:
/// Catalog also verifies the private authority retained by the concrete lease before accepting it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CatalogSnapshotMemoryReservationId {
    authority_id: NonZeroU64,
    sequence: NonZeroU64,
}

impl CatalogSnapshotMemoryReservationId {
    /// Returns the process-unique Catalog authority component.
    #[must_use]
    pub const fn authority_id(self) -> NonZeroU64 {
        self.authority_id
    }

    /// Returns the nonzero sequence allocated within the Catalog authority.
    #[must_use]
    pub const fn sequence(self) -> NonZeroU64 {
        self.sequence
    }
}

/// Copyable audit descriptor for one exclusive Catalog snapshot memory reservation.
///
/// This value may be stored in a query admission ledger and compared with a pinned snapshot. It
/// does not authorize a Catalog operation without the concrete, non-cloneable lease that owns the
/// private issuing authority and accounting payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CatalogSnapshotMemoryLeaseDescriptor {
    reservation_id: CatalogSnapshotMemoryReservationId,
    reserved_bytes: NonZeroU64,
    query_instance_id: Option<QueryInstanceId>,
}

impl CatalogSnapshotMemoryLeaseDescriptor {
    /// Returns the issuer-assigned reservation identity.
    #[must_use]
    pub const fn reservation_id(self) -> CatalogSnapshotMemoryReservationId {
        self.reservation_id
    }

    /// Returns the admission-accounting bytes retained by this reservation.
    #[must_use]
    pub const fn reserved_bytes(self) -> NonZeroU64 {
        self.reserved_bytes
    }

    /// Returns the exact query incarnation charged for this reservation, when attributed.
    #[must_use]
    pub const fn query_instance_id(self) -> Option<QueryInstanceId> {
        self.query_instance_id
    }
}

#[derive(Debug)]
struct CatalogSnapshotMemoryLeaseAuthority {
    authority_id: NonZeroU64,
    next_reservation_id: AtomicU64,
}

impl CatalogSnapshotMemoryLeaseAuthority {
    fn new(authority_id: NonZeroU64) -> Self {
        Self {
            authority_id,
            next_reservation_id: AtomicU64::new(1),
        }
    }

    fn allocate_reservation_id(&self) -> crate::Result<CatalogSnapshotMemoryReservationId> {
        let value = self
            .next_reservation_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                crate::ServerError::new(
                    crate::ErrorCode::ResourceExhausted,
                    "catalog snapshot memory reservation identity space is exhausted",
                )
                .with_retryable(false)
            })?;
        let value = NonZeroU64::new(value).expect("reservation identity starts at one");
        Ok(CatalogSnapshotMemoryReservationId {
            authority_id: self.authority_id,
            sequence: value,
        })
    }
}

fn allocate_catalog_snapshot_memory_authority_id() -> crate::Result<NonZeroU64> {
    let value = NEXT_CATALOG_SNAPSHOT_MEMORY_AUTHORITY_ID
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map_err(|_| {
            crate::ServerError::new(
                crate::ErrorCode::ResourceExhausted,
                "catalog snapshot memory authority identity space is exhausted",
            )
            .with_retryable(false)
        })?;
    Ok(NonZeroU64::new(value).expect("authority identity starts at one"))
}

/// Process-local capability that issues memory reservations for one Catalog instance.
///
/// The issuer is intentionally returned only by the Catalog's explicit native-runtime startup API.
/// Clones share one authority and monotonically allocated reservation identity space.
#[derive(Clone)]
pub struct CatalogSnapshotMemoryLeaseIssuer {
    authority: Arc<CatalogSnapshotMemoryLeaseAuthority>,
}

impl fmt::Debug for CatalogSnapshotMemoryLeaseIssuer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogSnapshotMemoryLeaseIssuer")
            .finish_non_exhaustive()
    }
}

impl CatalogSnapshotMemoryLeaseIssuer {
    /// Creates a fresh issuer/verifier authority pair.
    ///
    /// A Catalog instance retains only the verifier. Its native query runtime receives the paired
    /// issuer and uses [`Self::issue_with_owner`] after memory admission succeeds.
    ///
    /// :returns: Fresh process-local issuer and its matching origin verifier.
    /// :raises ServerError: If the process-wide authority identity space is exhausted.
    pub fn new_authority() -> crate::Result<(Self, CatalogSnapshotMemoryLeaseVerifier)> {
        let authority = Arc::new(CatalogSnapshotMemoryLeaseAuthority::new(
            allocate_catalog_snapshot_memory_authority_id()?,
        ));
        Ok((
            Self {
                authority: Arc::clone(&authority),
            },
            CatalogSnapshotMemoryLeaseVerifier { authority },
        ))
    }

    /// Issues one exclusive reservation and constructs its accounting owner from the exact ID.
    ///
    /// The owner factory observes the final descriptor before producing the payload. This permits
    /// the caller to bind the same reservation identity into its memory ledger atomically; the
    /// resulting payload remains structurally owned by the lease until Catalog releases it.
    ///
    /// :param reserved_bytes: Conservative owned-memory peak admitted by the caller.
    /// :param query_instance_id: Exact charged query incarnation, or `None` for maintenance work.
    /// :param owner_factory: Factory that consumes the final descriptor and returns the accounting
    ///     owner retained by the lease.
    /// :returns: Non-cloneable lease accepted only by the paired verifier.
    /// :raises ServerError: If reservation identities are exhausted or the owner factory rejects
    ///     admission.
    pub fn issue_with_owner<F>(
        &self,
        reserved_bytes: NonZeroU64,
        query_instance_id: Option<QueryInstanceId>,
        owner_factory: F,
    ) -> crate::Result<CatalogSnapshotMemoryLease>
    where
        F: FnOnce(
            CatalogSnapshotMemoryLeaseDescriptor,
        ) -> crate::Result<Box<dyn Any + Send + Sync>>,
    {
        let descriptor = CatalogSnapshotMemoryLeaseDescriptor {
            reservation_id: self.authority.allocate_reservation_id()?,
            reserved_bytes,
            query_instance_id,
        };
        let owner = owner_factory(descriptor)?;
        Ok(CatalogSnapshotMemoryLease {
            authority: Arc::clone(&self.authority),
            descriptor,
            _owner: owner,
        })
    }
}

/// Catalog-internal verifier paired with one snapshot memory lease issuer.
///
/// A verifier can validate origin but cannot issue a lease or recover its private owner payload.
#[derive(Clone)]
pub struct CatalogSnapshotMemoryLeaseVerifier {
    authority: Arc<CatalogSnapshotMemoryLeaseAuthority>,
}

impl fmt::Debug for CatalogSnapshotMemoryLeaseVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogSnapshotMemoryLeaseVerifier")
            .finish_non_exhaustive()
    }
}

impl CatalogSnapshotMemoryLeaseVerifier {
    /// Returns whether the lease was issued by this verifier's paired authority.
    #[must_use]
    pub fn accepts(&self, lease: &CatalogSnapshotMemoryLease) -> bool {
        Arc::ptr_eq(&self.authority, &lease.authority)
    }
}

/// An exclusive memory reservation for one owned Catalog snapshot.
///
/// The reservation has private fields, is non-cloneable and non-serializable, and cannot be
/// implemented by callers. Catalog carries it across worker boundaries and retains it beside the
/// final pinned snapshot owner. Dropping the lease drops its accounting payload only after all
/// preceding owned snapshot data has been released.
///
/// ```compile_fail
/// fn duplicate(lease: zippy_types::storage::CatalogSnapshotMemoryLease) {
///     let _second_owner = lease.clone();
/// }
/// ```
pub struct CatalogSnapshotMemoryLease {
    authority: Arc<CatalogSnapshotMemoryLeaseAuthority>,
    descriptor: CatalogSnapshotMemoryLeaseDescriptor,
    _owner: Box<dyn Any + Send + Sync>,
}

impl CatalogSnapshotMemoryLease {
    /// Returns the copyable accounting descriptor bound at issuance.
    #[must_use]
    pub const fn descriptor(&self) -> CatalogSnapshotMemoryLeaseDescriptor {
        self.descriptor
    }

    /// Returns the admission-accounting bytes retained by this reservation.
    #[must_use]
    pub const fn reserved_bytes(&self) -> NonZeroU64 {
        self.descriptor.reserved_bytes()
    }

    /// Returns the exact query incarnation charged for this reservation, when attributed.
    #[must_use]
    pub const fn query_instance_id(&self) -> Option<QueryInstanceId> {
        self.descriptor.query_instance_id()
    }
}

impl fmt::Debug for CatalogSnapshotMemoryLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogSnapshotMemoryLease")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

/// Cloneable data for an immutable persisted and live table boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableReadSnapshotDescriptor {
    /// The immutable table identity.
    pub table_id: TableId,
    /// The exact schema version used for planning and decoding.
    pub schema_version: u64,
    /// The exact persisted manifest version protected by the guard.
    pub persisted_snapshot_version: u64,
    /// The producer fencing epoch validated against the table actor.
    pub writer_epoch: u64,
    /// The active segment and committed high watermark, when present.
    pub active_segment: Option<SegmentReadSnapshotDescriptor>,
    /// Pinned sealed segments not represented by persisted file lineage.
    pub retained_sealed_segments: Vec<SegmentReadSnapshotDescriptor>,
    /// The greatest logical offset visible across persisted and live sources.
    pub max_log_offset: OffsetWatermark,
    /// The in-memory lease protecting persisted and, when present, live resources.
    pub lease_id: LeaseId,
}

impl TableReadSnapshotDescriptor {
    /// Returns the immutable table identifier.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
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

    /// Borrows the concrete implementation for an origin-checked executor.
    fn as_any(&self) -> &dyn Any;

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
/// 2. The Catalog reads and pins the exact persisted and schema versions selected by the guard
///    descriptor, returning [`OpaqueCatalogSnapshotPin`]. A raw metadata snapshot is insufficient
///    because it permits Safe-GC to claim files before finalization.
/// 3. `validate_and_finalize` consumes the live guard and transfers lease
///    and Catalog-pin ownership into a finalized query guard.
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
        catalog: OpaqueCatalogSnapshotPin,
    ) -> BoxFuture<'_, Result<OpaqueQueryReadSnapshot>>;
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::{
        CatalogSnapshotMemoryLeaseIssuer, CatalogSnapshotPinDescriptor, CatalogSnapshotPinGuard,
        LiveSnapshotPinGuard, LiveSnapshotPinsDescriptor, LiveTableSnapshotPinDescriptor,
        OpaqueCatalogSnapshotPin, OpaqueLiveSnapshotPins, OpaqueQueryReadSnapshot,
        QueryReadSnapshotDescriptor, QueryReadSnapshotGuard, StorageEngine,
    };
    use crate::catalog::CatalogSnapshotSelector;
    use crate::query::QueryInstanceId;
    use crate::{BoxFuture, LeaseId, OffsetWatermark, QueryId, Result, TableId};

    struct TestLiveGuard {
        descriptor: LiveSnapshotPinsDescriptor,
        instance_nonce: u128,
    }

    struct TestCatalogGuard {
        descriptor: CatalogSnapshotPinDescriptor,
    }

    impl CatalogSnapshotPinGuard for TestCatalogGuard {
        fn descriptor(&self) -> &CatalogSnapshotPinDescriptor {
            &self.descriptor
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
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

        fn as_any(&self) -> &dyn Any {
            self
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

        fn as_any(&self) -> &dyn Any {
            self
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
    fn assert_catalog_guard_object_safe(_: Option<OpaqueCatalogSnapshotPin>) {}
    fn assert_catalog_memory_lease_thread_safe<T: Send + Sync>(_: T) {}
    fn assert_live_guard_object_safe(_: Option<OpaqueLiveSnapshotPins>) {}
    fn assert_query_guard_object_safe(_: Option<OpaqueQueryReadSnapshot>) {}
    fn assert_live_trait_object_safe(_: Option<&dyn LiveSnapshotPinGuard>) {}
    fn assert_query_trait_object_safe(_: Option<&dyn QueryReadSnapshotGuard>) {}

    #[test]
    fn storage_and_exclusive_guards_are_object_safe() {
        assert_storage_object_safe(None);
        assert_catalog_guard_object_safe(None);
        assert_live_guard_object_safe(None);
        assert_query_guard_object_safe(None);
        assert_live_trait_object_safe(None);
        assert_query_trait_object_safe(None);
        let (issuer, verifier) = CatalogSnapshotMemoryLeaseIssuer::new_authority()
            .expect("create test memory lease authority");
        let lease = issuer
            .issue_with_owner(
                NonZeroU64::new(7).expect("nonzero test memory reservation"),
                None,
                |_| Ok(Box::new(())),
            )
            .expect("issue test memory reservation");
        assert!(verifier.accepts(&lease));
        assert_eq!(lease.reserved_bytes().get(), 7);
        assert_eq!(lease.query_instance_id(), None);
        assert_catalog_memory_lease_thread_safe(lease);
        assert_catalog_memory_lease_thread_safe(issuer);
        assert_catalog_memory_lease_thread_safe(verifier);
    }

    #[test]
    fn catalog_memory_lease_binds_descriptor_owner_and_private_authority() {
        struct DropCounter(Arc<AtomicUsize>);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }

        let (issuer, verifier) = CatalogSnapshotMemoryLeaseIssuer::new_authority()
            .expect("create test memory lease authority");
        let (foreign_issuer, foreign_verifier) = CatalogSnapshotMemoryLeaseIssuer::new_authority()
            .expect("create foreign test memory lease authority");
        let query_instance_id = QueryInstanceId::new(
            QueryId::from_u128(17).expect("nonzero test query ID"),
            NonZeroU64::new(3).expect("nonzero test query generation"),
        );
        let drops = Arc::new(AtomicUsize::new(0));
        let owner_drops = Arc::clone(&drops);
        let observed_descriptor = Arc::new(std::sync::Mutex::new(None));
        let owner_descriptor = Arc::clone(&observed_descriptor);
        let lease = issuer
            .issue_with_owner(
                NonZeroU64::new(11).expect("nonzero test memory reservation"),
                Some(query_instance_id),
                move |descriptor| {
                    *owner_descriptor.lock().expect("lock owner descriptor") = Some(descriptor);
                    Ok(Box::new(DropCounter(owner_drops)))
                },
            )
            .expect("issue test memory reservation");
        let descriptor = lease.descriptor();
        assert_eq!(
            *observed_descriptor
                .lock()
                .expect("lock observed descriptor"),
            Some(descriptor)
        );
        assert_eq!(descriptor.reserved_bytes().get(), 11);
        assert_eq!(descriptor.query_instance_id(), Some(query_instance_id));
        assert!(verifier.accepts(&lease));
        assert!(!foreign_verifier.accepts(&lease));

        let foreign_lease = foreign_issuer
            .issue_with_owner(
                NonZeroU64::new(11).expect("nonzero foreign reservation"),
                Some(query_instance_id),
                |_| Ok(Box::new(())),
            )
            .expect("issue foreign reservation");
        assert!(!verifier.accepts(&foreign_lease));
        assert_ne!(
            descriptor.reservation_id(),
            foreign_lease.descriptor().reservation_id()
        );
        assert_eq!(drops.load(Ordering::Acquire), 0);
        drop(lease);
        assert_eq!(drops.load(Ordering::Acquire), 1);
    }

    #[test]
    fn catalog_memory_reservation_ids_are_unique_within_one_authority() {
        let (issuer, _verifier) = CatalogSnapshotMemoryLeaseIssuer::new_authority()
            .expect("create test memory lease authority");
        let first = issuer
            .issue_with_owner(NonZeroU64::new(1).unwrap(), None, |_| Ok(Box::new(())))
            .expect("issue first reservation");
        let second = issuer
            .issue_with_owner(NonZeroU64::new(1).unwrap(), None, |_| Ok(Box::new(())))
            .expect("issue second reservation");
        assert_ne!(
            first.descriptor().reservation_id(),
            second.descriptor().reservation_id()
        );
    }

    #[test]
    fn failed_owner_admission_does_not_create_a_memory_lease() {
        let (issuer, _verifier) = CatalogSnapshotMemoryLeaseIssuer::new_authority()
            .expect("create test memory lease authority");
        let error = issuer
            .issue_with_owner(NonZeroU64::new(1).unwrap(), None, |_| {
                Err(crate::ServerError::new(
                    crate::ErrorCode::ResourceExhausted,
                    "injected owner admission failure",
                )
                .with_retryable(false))
            })
            .expect_err("failed owner admission must not produce a lease");
        assert_eq!(error.code(), crate::ErrorCode::ResourceExhausted);
    }

    #[test]
    fn catalog_pin_guard_exposes_only_path_free_selection_audit_data() {
        let selector = CatalogSnapshotSelector {
            table_id: TableId::from_u128(3).unwrap(),
            snapshot_version: 4,
            schema_version: 5,
        };
        let guard: OpaqueCatalogSnapshotPin = Box::new(TestCatalogGuard {
            descriptor: CatalogSnapshotPinDescriptor {
                catalog_revision: 6,
                selectors: vec![selector.clone()],
            },
        });
        assert_eq!(guard.descriptor().catalog_revision, 6);
        assert_eq!(guard.descriptor().selectors, vec![selector]);
        assert!(guard.into_any().downcast::<TestCatalogGuard>().is_ok());
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
