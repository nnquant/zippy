//! Subscription view, replay, progress, acknowledgement, and ownership contracts.

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::query::LogicalPlanEnvelope;
use crate::table::{TableDefinition, TableKind};
use crate::{
    BoxFuture, CommittedOffset, ConnectionId, ConsumerOffsetAuthority, DurableConsumerId,
    LogOffset, OffsetRange, OffsetWatermark, Result, TableId,
};

/// The initial logical position of a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "offset", rename_all = "snake_case")]
pub enum SubscriptionStart {
    /// Deliver only positions committed after the subscription opens.
    Latest,
    /// Replay from the earliest position still retained.
    EarliestRetained,
    /// Replay beginning at this exact logical offset.
    Offset(LogOffset),
    /// Resume after the durable consumer or group committed offset.
    Committed,
    /// Emit a table snapshot, then tail immediately after its committed watermark.
    SnapshotThenTail,
}

/// The failure and replay guarantees of subscription delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    /// Slow consumers may lose data and cannot persist acknowledgements.
    BestEffort,
    /// Unacknowledged scanned progress is replayed after reconnect.
    AtLeastOnce,
}

/// The logical shape requested from a table subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionView {
    /// Append rows or key-value changelog mutations.
    Rows,
    /// Complete atomic frames from a frame table.
    Frames,
    /// Table-semantic batches, frames, or materialized snapshots.
    Table,
}

impl SubscriptionView {
    /// Validates that this view has unambiguous semantics for a table definition.
    pub fn validate(
        self,
        definition: &TableDefinition,
    ) -> std::result::Result<(), InvalidSubscriptionRequest> {
        let supported = matches!(
            (self, definition),
            (
                Self::Rows,
                TableDefinition::Append(_) | TableDefinition::KeyValue(_)
            ) | (Self::Frames, TableDefinition::Frame(_))
                | (Self::Table, _)
        );
        if supported {
            Ok(())
        } else {
            Err(InvalidSubscriptionRequest::UnsupportedView {
                view: self,
                table_kind: definition.kind(),
            })
        }
    }
}

/// Consumer identity independent of the current network connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConsumerIdentity {
    /// No offset or membership survives the current connection.
    Ephemeral,
    /// A durable consumer whose replay authority is explicit.
    Durable {
        /// The durable consumer identity and group member identity.
        consumer_id: DurableConsumerId,
        /// The individual or group record that governs committed replay.
        offset_authority: ConsumerOffsetAuthority,
    },
}

impl ConsumerIdentity {
    /// Returns true when committed replay state survives reconnects.
    #[must_use]
    pub const fn is_durable(self) -> bool {
        matches!(self, Self::Durable { .. })
    }

    fn validate(self) -> std::result::Result<(), InvalidSubscriptionRequest> {
        if let Self::Durable {
            consumer_id,
            offset_authority: ConsumerOffsetAuthority::Consumer(authority_id),
        } = self
        {
            if consumer_id != authority_id {
                return Err(InvalidSubscriptionRequest::ConsumerAuthorityMismatch);
            }
        }
        Ok(())
    }
}

/// A validated request to open one live subscription connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OpenSubscriptionRequest {
    connection_id: ConnectionId,
    consumer: ConsumerIdentity,
    table_id: TableId,
    view: SubscriptionView,
    start: SubscriptionStart,
    delivery: DeliveryMode,
    filter: Option<LogicalPlanEnvelope>,
    batch_rows: Option<u32>,
    throttle_ms: Option<u64>,
}

impl OpenSubscriptionRequest {
    /// Creates a request while enforcing durable replay invariants.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connection_id: ConnectionId,
        consumer: ConsumerIdentity,
        table_id: TableId,
        view: SubscriptionView,
        start: SubscriptionStart,
        delivery: DeliveryMode,
        filter: Option<LogicalPlanEnvelope>,
        batch_rows: Option<u32>,
        throttle_ms: Option<u64>,
    ) -> std::result::Result<Self, InvalidSubscriptionRequest> {
        consumer.validate()?;
        if matches!(start, SubscriptionStart::Committed) && !consumer.is_durable() {
            return Err(InvalidSubscriptionRequest::CommittedStartRequiresDurableConsumer);
        }
        if delivery == DeliveryMode::AtLeastOnce && !consumer.is_durable() {
            return Err(InvalidSubscriptionRequest::AtLeastOnceRequiresDurableConsumer);
        }
        if matches!(start, SubscriptionStart::SnapshotThenTail) && view != SubscriptionView::Table {
            return Err(InvalidSubscriptionRequest::SnapshotStartRequiresTableView);
        }
        if batch_rows == Some(0) {
            return Err(InvalidSubscriptionRequest::ZeroBatchRows);
        }
        Ok(Self {
            connection_id,
            consumer,
            table_id,
            view,
            start,
            delivery,
            filter,
            batch_rows,
            throttle_ms,
        })
    }

    /// Validates the requested view after resolving the catalog table definition.
    pub fn validate_table_definition(
        &self,
        definition: &TableDefinition,
    ) -> std::result::Result<(), InvalidSubscriptionRequest> {
        self.view.validate(definition)
    }

    /// Returns the boot-scoped connection identity.
    #[must_use]
    pub const fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    /// Returns the connection-independent consumer identity.
    #[must_use]
    pub const fn consumer(&self) -> ConsumerIdentity {
        self.consumer
    }

    /// Returns the subscribed table identity.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the requested subscription view.
    #[must_use]
    pub const fn view(&self) -> SubscriptionView {
        self.view
    }

    /// Returns the requested starting position.
    #[must_use]
    pub const fn start(&self) -> SubscriptionStart {
        self.start
    }

    /// Returns the delivery guarantee.
    #[must_use]
    pub const fn delivery(&self) -> DeliveryMode {
        self.delivery
    }

    /// Borrows the optional typed filter plan.
    #[must_use]
    pub fn filter(&self) -> Option<&LogicalPlanEnvelope> {
        self.filter.as_ref()
    }

    /// Returns the preferred batch row limit.
    #[must_use]
    pub const fn batch_rows(&self) -> Option<u32> {
        self.batch_rows
    }

    /// Returns the optional delivery throttle in milliseconds.
    #[must_use]
    pub const fn throttle_ms(&self) -> Option<u64> {
        self.throttle_ms
    }
}

impl<'de> Deserialize<'de> for OpenSubscriptionRequest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawRequest {
            connection_id: ConnectionId,
            consumer: ConsumerIdentity,
            table_id: TableId,
            view: SubscriptionView,
            start: SubscriptionStart,
            delivery: DeliveryMode,
            filter: Option<LogicalPlanEnvelope>,
            batch_rows: Option<u32>,
            throttle_ms: Option<u64>,
        }

        let raw = RawRequest::deserialize(deserializer)?;
        Self::new(
            raw.connection_id,
            raw.consumer,
            raw.table_id,
            raw.view,
            raw.start,
            raw.delivery,
            raw.filter,
            raw.batch_rows,
            raw.throttle_ms,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// An invalid subscription request invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidSubscriptionRequest {
    /// Committed replay requires persistent consumer state.
    #[error("committed subscription start requires a durable consumer")]
    CommittedStartRequiresDurableConsumer,
    /// At-least-once delivery requires a durable acknowledgement authority.
    #[error("at-least-once delivery requires a durable consumer")]
    AtLeastOnceRequiresDurableConsumer,
    /// An individual authority must name the same durable consumer.
    #[error("consumer offset authority does not match durable consumer")]
    ConsumerAuthorityMismatch,
    /// Snapshot-then-tail is defined only for the table-semantic view.
    #[error("snapshot-then-tail requires table subscription view")]
    SnapshotStartRequiresTableView,
    /// The requested view is not defined for the resolved table kind.
    #[error("unsupported subscription view view=[{view:?}] table_kind=[{table_kind:?}]")]
    UnsupportedView {
        /// The requested logical view.
        view: SubscriptionView,
        /// The resolved persistent table kind.
        table_kind: TableKind,
    },
    /// A requested result batch must allow at least one row.
    #[error("subscription batch_rows must be greater than zero")]
    ZeroBatchRows,
}

/// Scanned log progress and optionally delivered offset bounds.
///
/// Filters may advance `scanned_through` while delivering zero rows. Delivered
/// bounds describe the least and greatest delivered offsets, not a claim that
/// every offset inside the range was delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SubscriptionLogProgress {
    scanned_through: CommittedOffset,
    delivered_offsets: Option<OffsetRange>,
}

impl<'de> Deserialize<'de> for SubscriptionLogProgress {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawProgress {
            scanned_through: CommittedOffset,
            delivered_offsets: Option<OffsetRange>,
        }

        let raw = RawProgress::deserialize(deserializer)?;
        Self::new(raw.scanned_through, raw.delivered_offsets).map_err(serde::de::Error::custom)
    }
}

impl SubscriptionLogProgress {
    /// Creates progress and verifies delivered offsets were already scanned.
    pub fn new(
        scanned_through: CommittedOffset,
        delivered_offsets: Option<OffsetRange>,
    ) -> std::result::Result<Self, InvalidSubscriptionProgress> {
        match (scanned_through, delivered_offsets) {
            (CommittedOffset::Empty, Some(_)) => {
                return Err(InvalidSubscriptionProgress::DeliveredWithoutScan);
            }
            (CommittedOffset::Known(scanned), Some(delivered)) if delivered.last() > scanned => {
                return Err(InvalidSubscriptionProgress::DeliveredBeyondScan);
            }
            _ => {}
        }
        Ok(Self {
            scanned_through,
            delivered_offsets,
        })
    }

    /// Returns the durable progress safe to acknowledge after processing the batch.
    #[must_use]
    pub const fn scanned_through(self) -> CommittedOffset {
        self.scanned_through
    }

    /// Returns delivered offset bounds, absent for a filtered empty delivery.
    #[must_use]
    pub const fn delivered_offsets(self) -> Option<OffsetRange> {
        self.delivered_offsets
    }
}

/// An invalid relationship between scanned and delivered offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidSubscriptionProgress {
    /// Empty scan progress cannot contain delivered log rows.
    #[error("subscription delivered offsets without scanned progress")]
    DeliveredWithoutScan,
    /// Delivered offsets cannot extend past scanned progress.
    #[error("subscription delivered offsets extend beyond scanned progress")]
    DeliveredBeyondScan,
}

/// The logical position represented by a subscription delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SubscriptionPosition {
    /// A materialized snapshot covering a durable log boundary.
    ///
    /// `SnapshotThenTail` starts log delivery at
    /// `committed_through.next_offset()` and never replays a position already
    /// represented by the snapshot.
    Snapshot {
        /// The greatest log position represented by the snapshot.
        committed_through: CommittedOffset,
    },
    /// Scanned log progress, which may have delivered no rows after filtering.
    Log(SubscriptionLogProgress),
}

/// One validated native Arrow subscription delivery.
#[derive(Debug, Clone)]
pub struct SubscriptionBatch {
    delivery_sequence: u64,
    position: SubscriptionPosition,
    watermark: OffsetWatermark,
    batch: RecordBatch,
    replayed: bool,
}

impl SubscriptionBatch {
    /// Creates a snapshot delivery whose tail starts after `committed_through`.
    #[must_use]
    pub fn new_snapshot(
        delivery_sequence: u64,
        committed_through: CommittedOffset,
        batch: RecordBatch,
        replayed: bool,
    ) -> Self {
        let watermark = match committed_through {
            CommittedOffset::Empty => OffsetWatermark::Empty,
            CommittedOffset::Known(offset) => OffsetWatermark::Known(offset),
        };
        Self {
            delivery_sequence,
            position: SubscriptionPosition::Snapshot { committed_through },
            watermark,
            batch,
            replayed,
        }
    }

    /// Creates a log delivery and validates progress, rows, and table watermark.
    pub fn new_log(
        delivery_sequence: u64,
        progress: SubscriptionLogProgress,
        watermark: OffsetWatermark,
        batch: RecordBatch,
        replayed: bool,
    ) -> std::result::Result<Self, InvalidSubscriptionBatch> {
        match (batch.num_rows(), progress.delivered_offsets()) {
            (0, None) => {}
            (0, Some(_)) => return Err(InvalidSubscriptionBatch::EmptyBatchWithOffsets),
            (_, None) => return Err(InvalidSubscriptionBatch::RowsWithoutOffsets),
            (_, Some(_)) => {}
        }
        match (progress.scanned_through(), watermark) {
            (CommittedOffset::Empty, OffsetWatermark::Empty) => {}
            (CommittedOffset::Known(scanned), OffsetWatermark::Known(table))
                if scanned <= table => {}
            _ => return Err(InvalidSubscriptionBatch::InvalidTableWatermark),
        }
        Ok(Self {
            delivery_sequence,
            position: SubscriptionPosition::Log(progress),
            watermark,
            batch,
            replayed,
        })
    }

    /// Returns the connection-local delivery sequence.
    #[must_use]
    pub const fn delivery_sequence(&self) -> u64 {
        self.delivery_sequence
    }

    /// Returns the snapshot or log progress represented by this batch.
    #[must_use]
    pub const fn position(&self) -> SubscriptionPosition {
        self.position
    }

    /// Returns the table watermark observed when materializing the batch.
    #[must_use]
    pub const fn watermark(&self) -> OffsetWatermark {
        self.watermark
    }

    /// Borrows the native Arrow batch.
    #[must_use]
    pub const fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Returns whether this log progress was produced by replay.
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

/// An invalid native subscription delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidSubscriptionBatch {
    /// A zero-row Arrow batch must not claim delivered offsets.
    #[error("empty subscription batch must not contain delivered offsets")]
    EmptyBatchWithOffsets,
    /// A non-empty Arrow batch must identify its delivered offset bounds.
    #[error("non-empty subscription batch must contain delivered offsets")]
    RowsWithoutOffsets,
    /// Scanned progress must not exceed the known table watermark.
    #[error("subscription progress is incompatible with table watermark")]
    InvalidTableWatermark,
}

/// A durable at-least-once acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionAck {
    /// The connection-local delivery sequence being acknowledged.
    pub delivery_sequence: u64,
    /// The scanned position completely processed by the consumer.
    pub processed_through: CommittedOffset,
}

/// An open subscription stream and acknowledgement channel.
///
/// Dropping this handle releases the boot-scoped connection and group
/// membership, but never deletes durable consumer identity or committed group
/// offsets. At-least-once replay may repeat the final unacknowledged progress
/// but must not skip retained scanned positions.
pub trait SubscriptionHandle: Send {
    /// Returns the current boot-scoped connection identity.
    fn connection_id(&self) -> ConnectionId;

    /// Polls the next native Arrow delivery.
    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<SubscriptionBatch>>>;

    /// Commits a monotonic durable acknowledgement against the bound authority.
    ///
    /// Best-effort handles must reject this operation with `INVALID_ARGUMENT`.
    fn acknowledge(
        self: Pin<&mut Self>,
        acknowledgement: SubscriptionAck,
    ) -> BoxFuture<'_, Result<()>>;
}

/// A sendable, type-erased subscription handle.
pub type SendableSubscription = Pin<Box<dyn SubscriptionHandle>>;

/// Opens subscription connections against stable logical table offsets.
pub trait SubscriptionService: Send + Sync {
    /// Resolves the table, validates its view, retention, and offset authority,
    /// then opens a subscription stream.
    fn open(&self, request: OpenSubscriptionRequest)
        -> BoxFuture<'_, Result<SendableSubscription>>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::record_batch::RecordBatch;

    use super::{
        ConsumerIdentity, DeliveryMode, OpenSubscriptionRequest, SubscriptionBatch,
        SubscriptionLogProgress, SubscriptionPosition, SubscriptionService, SubscriptionStart,
        SubscriptionView,
    };
    use crate::table::{AppendTableDefinition, ColumnName, FrameTableDefinition, TableDefinition};
    use crate::{
        CommittedOffset, ConnectionId, ConsumerOffsetAuthority, DurableConsumerId, LogOffset,
        OffsetRange, OffsetWatermark, TableId,
    };

    fn connection_id() -> ConnectionId {
        ConnectionId::from_u128(1).unwrap()
    }

    fn table_id() -> TableId {
        TableId::from_u128(2).unwrap()
    }

    fn durable_consumer() -> ConsumerIdentity {
        let consumer_id = DurableConsumerId::from_u128(3).unwrap();
        ConsumerIdentity::Durable {
            consumer_id,
            offset_authority: ConsumerOffsetAuthority::Consumer(consumer_id),
        }
    }

    fn batch(rows: usize) -> RecordBatch {
        let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64));
        RecordBatch::try_from_iter([("value", values)]).unwrap()
    }

    fn assert_object_safe(_: Option<&dyn SubscriptionService>) {}

    #[test]
    fn subscription_service_is_object_safe() {
        assert_object_safe(None);
    }

    #[test]
    fn committed_and_at_least_once_require_durable_identity() {
        for (start, delivery) in [
            (SubscriptionStart::Committed, DeliveryMode::BestEffort),
            (SubscriptionStart::Latest, DeliveryMode::AtLeastOnce),
        ] {
            assert!(OpenSubscriptionRequest::new(
                connection_id(),
                ConsumerIdentity::Ephemeral,
                table_id(),
                SubscriptionView::Rows,
                start,
                delivery,
                None,
                Some(128),
                None,
            )
            .is_err());
        }
    }

    #[test]
    fn individual_offset_authority_must_match_consumer() {
        assert!(OpenSubscriptionRequest::new(
            connection_id(),
            ConsumerIdentity::Durable {
                consumer_id: DurableConsumerId::from_u128(3).unwrap(),
                offset_authority: ConsumerOffsetAuthority::Consumer(
                    DurableConsumerId::from_u128(4).unwrap(),
                ),
            },
            table_id(),
            SubscriptionView::Rows,
            SubscriptionStart::Committed,
            DeliveryMode::AtLeastOnce,
            None,
            None,
            None,
        )
        .is_err());
    }

    #[test]
    fn subscription_view_is_validated_against_table_definition() {
        let append = TableDefinition::Append(AppendTableDefinition::default());
        let frame = TableDefinition::Frame(FrameTableDefinition {
            by: ColumnName::new("symbol").unwrap(),
        });

        assert!(SubscriptionView::Rows.validate(&append).is_ok());
        assert!(SubscriptionView::Rows.validate(&frame).is_err());
        assert!(SubscriptionView::Frames.validate(&frame).is_ok());
        assert!(SubscriptionView::Frames.validate(&append).is_err());
        assert!(SubscriptionView::Table.validate(&append).is_ok());
    }

    #[test]
    fn filtered_empty_delivery_advances_only_scanned_progress() {
        let progress =
            SubscriptionLogProgress::new(CommittedOffset::Known(LogOffset::new(7)), None).unwrap();
        let delivery = SubscriptionBatch::new_log(
            1,
            progress,
            OffsetWatermark::Known(LogOffset::new(9)),
            batch(0),
            false,
        )
        .unwrap();

        assert_eq!(delivery.batch().num_rows(), 0);
        let SubscriptionPosition::Log(progress) = delivery.position() else {
            panic!("expected log position");
        };
        assert_eq!(
            progress.scanned_through(),
            CommittedOffset::Known(LogOffset::new(7))
        );
        assert_eq!(progress.delivered_offsets(), None);
    }

    #[test]
    fn subscription_progress_and_batch_reject_impossible_offsets() {
        assert!(SubscriptionLogProgress::new(
            CommittedOffset::Empty,
            Some(OffsetRange::single(LogOffset::new(0))),
        )
        .is_err());
        assert!(serde_json::from_str::<SubscriptionLogProgress>(
            r#"{"scanned_through":{"state":"empty"},"delivered_offsets":{"first":0,"last":0}}"#
        )
        .is_err());
        let progress = SubscriptionLogProgress::new(
            CommittedOffset::Known(LogOffset::new(1)),
            Some(OffsetRange::single(LogOffset::new(1))),
        )
        .unwrap();
        assert!(SubscriptionBatch::new_log(
            1,
            progress,
            OffsetWatermark::Known(LogOffset::new(1)),
            batch(0),
            false,
        )
        .is_err());
    }

    #[test]
    fn snapshot_position_defines_the_first_tail_offset() {
        let delivery = SubscriptionBatch::new_snapshot(
            1,
            CommittedOffset::Known(LogOffset::new(7)),
            batch(1),
            false,
        );
        let SubscriptionPosition::Snapshot { committed_through } = delivery.position() else {
            panic!("expected snapshot position");
        };

        assert_eq!(committed_through.next_offset(), Some(LogOffset::new(8)));
    }

    #[test]
    fn durable_subscription_has_a_golden_authority_form() {
        let request = OpenSubscriptionRequest::new(
            connection_id(),
            durable_consumer(),
            table_id(),
            SubscriptionView::Rows,
            SubscriptionStart::Committed,
            DeliveryMode::AtLeastOnce,
            None,
            Some(128),
            None,
        )
        .unwrap();
        let json = serde_json::to_value(request).unwrap();

        assert_eq!(json["view"], "rows");
        assert_eq!(json["consumer"]["offset_authority"]["type"], "consumer");
        assert_eq!(
            json["consumer"]["offset_authority"]["id"],
            "00000000-0000-0000-0000-000000000003"
        );
    }
}
