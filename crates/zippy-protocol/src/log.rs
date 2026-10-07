//! Ordered native event fragments over Flight DoExchange, with explicit offset metadata.

use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::{FlightData, FlightDescriptor};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use zippy_types::query::{QueryInstanceId, QueryResultBatch};
use zippy_types::subscription::SubscriptionAck;
use zippy_types::table::{CatalogName, TableKind, TableRef};
use zippy_types::{
    BoxFuture, CommittedOffset, ConsumerGroupId, ConsumerOffsetAuthority, DurableConsumerId,
    ErrorCode, LogOffset, OffsetRange, OffsetWatermark, QueryId, RequestId, ResourceLimit, Result,
    ServerError, SessionId, TableId,
};

use crate::error::{resource_limit_from_wire, server_error_to_status, watermark_to_wire};
use crate::wire::{self, log_command, log_metadata, table_binding};
use crate::CURRENT_PROTOCOL_VERSION;

/// Capability for raw ordered fragments, without durable ACK or atomic frame assembly.
pub const LOG_CAPABILITY: &str = "subscription.log_fragments.v1";
/// Capability for committed replay and exact durable atomic-unit acknowledgements.
pub const DURABLE_LOG_CAPABILITY: &str = "subscription.durable_log.v1";
/// Maximum serialized opening command and control message size.
pub const MAX_LOG_COMMAND_BYTES: usize = 16 * 1024;

/// Connection-independent consumer identity and individual or group offset authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlightLogConsumer {
    /// Durable consumer or group member identity.
    pub consumer_id: DurableConsumerId,
    /// Persistent record governing replay.
    pub offset_authority: ConsumerOffsetAuthority,
}

/// Session-bound ACK endpoint, retained only for this open subscription.
pub trait FlightLogAcknowledger: Send + Sync {
    /// Persists an exact delivery and returns only after its Catalog commit is known.
    fn acknowledge(&self, request_id: RequestId, ack: SubscriptionAck)
        -> BoxFuture<'_, Result<()>>;
}

/// Validated client control on an existing log connection.
pub enum FlightLogControl {
    /// Stop the connection and its native query.
    Cancel,
    /// Confirm an entirely emitted atomic delivery.
    Acknowledge {
        /// Stable idempotency key for the confirmation.
        request_id: RequestId,
        /// Connection-local delivery and exact processed boundary.
        acknowledgement: SubscriptionAck,
    },
}

/// Validated opening request for a single native table log.
#[derive(Debug, Clone)]
pub struct FlightLogRequest {
    /// Query identity used throughout the stream.
    pub query_id: QueryId,
    /// Authenticated owning session.
    pub session_id: SessionId,
    /// Catalog name or immutable table identity.
    pub table: TableRef,
    /// Inclusive offset, or the latest boundary when absent.
    pub start: Option<LogOffset>,
    /// Absolute UTC Unix-nanosecond deadline.
    pub deadline_ns: Option<u64>,
    /// Memory admission requested from the native query coordinator.
    pub memory_limit_bytes: ResourceLimit,
    /// Preferred maximum rows per native fragment.
    pub batch_rows: Option<NonZeroU32>,
    /// When present, start must be committed and Catalog determines the replay boundary.
    pub consumer: Option<FlightLogConsumer>,
}

/// An owned fragment whose Arrow buffers remain charged during transport encoding.
pub struct FlightLogBatch {
    /// Native result ownership and memory lease.
    pub batch: QueryResultBatch,
    /// Original row offsets, inclusive.
    pub offsets: OffsetRange,
    /// Complete original frame boundary, when present.
    pub frame: Option<OffsetRange>,
    /// Highest event visible in this fragment's pinned window.
    pub watermark: OffsetWatermark,
    /// Complete delivery identity, retained on every transport split of this fragment.
    pub acknowledgement: Option<SubscriptionAck>,
}

/// A native reader-driven subscription; dropping its stream cancels and releases ownership.
pub struct FlightLogSubscription {
    /// Stable incarnation across all refreshed windows.
    pub instance_id: QueryInstanceId,
    /// Resolved immutable table identity.
    pub table_id: TableId,
    /// Logical table kind.
    pub kind: TableKind,
    /// Stable raw event schema, including key-value mutation control columns.
    pub schema: SchemaRef,
    /// Opening boundary, even when the initial stream is empty.
    pub watermark: OffsetWatermark,
    /// Sequential native fragments, with no background result queue.
    pub stream: BoxStream<'static, Result<FlightLogBatch>>,
    /// Durable identity echoed by the opening message.
    pub consumer: Option<FlightLogConsumer>,
    /// Inclusive committed replay boundary; required exactly when consumer is present.
    pub next_offset: Option<LogOffset>,
    /// Durable endpoint, absent for raw explicit-cursor streams.
    pub acknowledger: Option<Arc<dyn FlightLogAcknowledger>>,
}

/// Validates a descriptor without accepting implicit start positions or SQL aliases.
pub fn decode_log_descriptor(descriptor: &FlightDescriptor) -> Result<FlightLogRequest> {
    if DescriptorType::try_from(descriptor.r#type).ok() != Some(DescriptorType::Cmd)
        || !descriptor.path.is_empty()
        || descriptor.cmd.is_empty()
        || descriptor.cmd.len() > MAX_LOG_COMMAND_BYTES
    {
        return Err(invalid("log descriptor must contain one bounded command"));
    }
    let command = wire::LogCommand::decode(descriptor.cmd.clone())
        .map_err(|_| invalid("log command protobuf is malformed"))?;
    if command.protocol_version != CURRENT_PROTOCOL_VERSION {
        return Err(invalid("log protocol version is unsupported"));
    }
    let query_id = id(&command.query_id, QueryId::from_bytes)?;
    let session_id = id(&command.session_id, SessionId::from_bytes)?;
    let binding = command
        .table
        .ok_or_else(|| invalid("log table is missing"))?;
    if !binding.sql_name.is_empty() {
        return Err(invalid("log table must not have a SQL alias"));
    }
    let table = match binding.table {
        Some(table_binding::Table::Name(name)) => {
            if name.namespace.len() > 512 || name.table.len() > 512 {
                return Err(invalid("log table name exceeds protocol limit"));
            }
            TableRef::Name {
                namespace: CatalogName::new(name.namespace)
                    .map_err(|_| invalid("log namespace is invalid"))?,
                table: CatalogName::new(name.table)
                    .map_err(|_| invalid("log table name is invalid"))?,
            }
        }
        Some(table_binding::Table::TableId(value)) => TableRef::Id {
            table_id: id(&value, TableId::from_bytes)?,
        },
        None => return Err(invalid("log table reference is missing")),
    };
    let consumer = command.consumer.map(decode_consumer).transpose()?;
    let start = match (command.start, consumer.is_some()) {
        (Some(log_command::Start::Latest(true)), false) => None,
        (Some(log_command::Start::Offset(value)), false) => Some(LogOffset::new(value)),
        (Some(log_command::Start::Committed(true)), true) => None,
        _ => {
            return Err(invalid(
                "log start requires an explicit cursor or committed=true with a consumer",
            ))
        }
    };
    let memory_limit_bytes = command
        .memory_limit
        .and_then(resource_limit_from_wire)
        .filter(|limit| *limit != ResourceLimit::Disabled)
        .ok_or_else(|| invalid("log memory limit is missing, disabled or invalid"))?;
    let batch_rows = command
        .batch_rows
        .map(|rows| NonZeroU32::new(rows).ok_or_else(|| invalid("log batch rows must be positive")))
        .transpose()?;
    Ok(FlightLogRequest {
        query_id,
        session_id,
        table,
        start,
        deadline_ns: command.deadline_ns,
        memory_limit_bytes,
        batch_rows,
        consumer,
    })
}

/// Validates bounded cancellation or an exact durable acknowledgement.
pub fn decode_log_control(data: &FlightData) -> Result<FlightLogControl> {
    if data.flight_descriptor.is_some()
        || !data.data_header.is_empty()
        || !data.data_body.is_empty()
        || data.app_metadata.is_empty()
        || data.app_metadata.len() > MAX_LOG_COMMAND_BYTES
    {
        return Err(invalid("log control must contain only bounded metadata"));
    }
    let control = wire::LogControl::decode(data.app_metadata.clone())
        .map_err(|_| invalid("log control protobuf is malformed"))?;
    if control.protocol_version != CURRENT_PROTOCOL_VERSION {
        return Err(invalid("log control version is unsupported"));
    }
    match control.control {
        Some(wire::log_control::Control::Cancel(true)) => Ok(FlightLogControl::Cancel),
        Some(wire::log_control::Control::Acknowledge(ack)) if ack.delivery_sequence > 0 => {
            Ok(FlightLogControl::Acknowledge {
                request_id: id(&ack.request_id, RequestId::from_bytes)?,
                acknowledgement: SubscriptionAck {
                    delivery_sequence: ack.delivery_sequence,
                    processed_through: CommittedOffset::Known(LogOffset::new(
                        ack.processed_through,
                    )),
                },
            })
        }
        _ => Err(invalid(
            "log control must request cancellation or a nonzero delivery ACK",
        )),
    }
}

fn decode_consumer(consumer: wire::LogConsumer) -> Result<FlightLogConsumer> {
    let consumer_id = id(&consumer.consumer_id, DurableConsumerId::from_bytes)?;
    let offset_authority = match consumer.authority {
        Some(wire::log_consumer::Authority::Individual(true)) => {
            ConsumerOffsetAuthority::Consumer(consumer_id)
        }
        Some(wire::log_consumer::Authority::GroupId(group)) => {
            ConsumerOffsetAuthority::Group(id(&group, ConsumerGroupId::from_bytes)?)
        }
        _ => {
            return Err(invalid(
                "log consumer requires individual=true or a non-nil group id",
            ))
        }
    };
    Ok(FlightLogConsumer {
        consumer_id,
        offset_authority,
    })
}

fn consumer_to_wire(consumer: FlightLogConsumer) -> wire::LogConsumer {
    wire::LogConsumer {
        consumer_id: Bytes::copy_from_slice(&consumer.consumer_id.into_bytes()),
        authority: Some(match consumer.offset_authority {
            ConsumerOffsetAuthority::Consumer(_) => wire::log_consumer::Authority::Individual(true),
            ConsumerOffsetAuthority::Group(group) => {
                wire::log_consumer::Authority::GroupId(Bytes::copy_from_slice(&group.into_bytes()))
            }
        }),
    }
}

fn id<T>(
    bytes: &[u8],
    constructor: impl FnOnce([u8; 16]) -> std::result::Result<T, zippy_types::id::NilStableIdError>,
) -> Result<T> {
    constructor(
        bytes
            .try_into()
            .map_err(|_| invalid("log identity must contain 16 bytes"))?,
    )
    .map_err(|_| invalid("log identity must not be nil"))
}

fn invalid(message: &str) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message)
}

struct RetainedFragment {
    owner: FlightLogBatch,
    encoded_rows: u64,
}

struct LogInput {
    stream: BoxStream<'static, Result<FlightLogBatch>>,
    current: Arc<Mutex<Option<RetainedFragment>>>,
}

impl Stream for LogInput {
    type Item = arrow_flight::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let previous = self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(previous) = previous {
            if u128::from(previous.encoded_rows) != previous.owner.offsets.len() {
                return Poll::Ready(Some(Err(protocol_error("log encoder lost fragment rows"))));
            }
        }
        match self.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(owner))) => {
                let batch = owner.batch.record_batch().clone();
                if batch.num_rows() == 0
                    || batch.num_rows() as u128 != owner.offsets.len()
                    || owner.frame.is_some_and(|frame| {
                        !frame.contains(owner.offsets.first())
                            || !frame.contains(owner.offsets.last())
                    })
                    || owner
                        .watermark
                        .known()
                        .is_none_or(|end| end < owner.offsets.last())
                {
                    return Poll::Ready(Some(Err(protocol_error(
                        "native log fragment boundary is invalid",
                    ))));
                }
                *self
                    .current
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(RetainedFragment {
                    owner,
                    encoded_rows: 0,
                });
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(FlightError::Tonic(
                server_error_to_status(&error),
            )))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn protocol_error(message: &str) -> FlightError {
    FlightError::Tonic(server_error_to_status(&ServerError::new(
        ErrorCode::Internal,
        message,
    )))
}

/// Encodes a schema followed by offset-tagged record batches, preserving row offsets on splitting.
/// The encoder drains all output chunks before polling the next native fragment.
pub(crate) fn encode_log(
    subscription: FlightLogSubscription,
    max_bytes: usize,
) -> BoxStream<'static, arrow_flight::error::Result<FlightData>> {
    let opened = wire::LogOpened {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        query_id: Bytes::copy_from_slice(&subscription.instance_id.query_id().into_bytes()),
        generation: subscription.instance_id.generation().get(),
        table_id: Bytes::copy_from_slice(&subscription.table_id.into_bytes()),
        table_kind: match subscription.kind {
            TableKind::Append => "append",
            TableKind::Frame => "frame",
            TableKind::KeyValue => "key_value",
        }
        .to_owned(),
        watermark: Some(watermark_to_wire(subscription.watermark)),
        consumer: subscription.consumer.map(consumer_to_wire),
        next_offset: subscription.next_offset.map(LogOffset::get),
    };
    let current = Arc::new(Mutex::new(None));
    let input = LogInput {
        stream: subscription.stream,
        current: current.clone(),
    };
    let opening = FlightData {
        app_metadata: Bytes::from(
            wire::LogMetadata {
                payload: Some(log_metadata::Payload::Opened(opened)),
            }
            .encode_to_vec(),
        ),
        ..FlightData::default()
    };
    let encoder = FlightDataEncoderBuilder::new()
        .with_schema(subscription.schema)
        .with_max_flight_data_size(max_bytes)
        .build(input)
        .and_then(move |mut data| {
            let result = (|| {
                let message = arrow::ipc::root_as_message(&data.data_header)
                    .map_err(|_| protocol_error("encoded log IPC header is invalid"))?;
                if let Some(batch) = message.header_as_record_batch() {
                    let rows = u64::try_from(batch.length())
                        .map_err(|_| protocol_error("encoded log row count is invalid"))?;
                    let mut guard = current
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let fragment = guard
                        .as_mut()
                        .ok_or_else(|| protocol_error("encoded log has no native owner"))?;
                    let first = fragment
                        .owner
                        .offsets
                        .first()
                        .get()
                        .checked_add(fragment.encoded_rows)
                        .ok_or_else(|| protocol_error("encoded log offset overflow"))?;
                    let last = rows
                        .checked_sub(1)
                        .and_then(|n| first.checked_add(n))
                        .filter(|end| *end <= fragment.owner.offsets.last().get())
                        .ok_or_else(|| {
                            protocol_error("encoded log row count exceeds its fragment")
                        })?;
                    fragment.encoded_rows += rows;
                    data.app_metadata = Bytes::from(
                        wire::LogMetadata {
                            payload: Some(log_metadata::Payload::Progress(wire::LogProgress {
                                first_offset: first,
                                last_offset: last,
                                frame_first_offset: fragment
                                    .owner
                                    .frame
                                    .map(|range| range.first().get()),
                                frame_last_offset: fragment
                                    .owner
                                    .frame
                                    .map(|range| range.last().get()),
                                watermark: Some(watermark_to_wire(fragment.owner.watermark)),
                                delivery_sequence: fragment
                                    .owner
                                    .acknowledgement
                                    .map(|ack| ack.delivery_sequence),
                                delivery_last_offset: fragment.owner.acknowledgement.and_then(
                                    |ack| match ack.processed_through {
                                        CommittedOffset::Known(offset) => Some(offset.get()),
                                        CommittedOffset::Empty => None,
                                    },
                                ),
                            })),
                        }
                        .encode_to_vec(),
                    );
                }
                Ok(data)
            })();
            futures::future::ready(result)
        })
        .boxed();
    // C++/PyArrow readers consume schema messages without exposing their app_metadata.
    // A standalone opening message is observable before GetSchema on every client.
    futures::stream::once(async move { Ok(opening) })
        .chain(encoder)
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct DropCounter(Arc<AtomicUsize>);
    struct TestAcknowledger;
    impl FlightLogAcknowledger for TestAcknowledger {
        fn acknowledge(&self, _: RequestId, _: SubscriptionAck) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn command() -> wire::LogCommand {
        wire::LogCommand {
            consumer: None,
            protocol_version: CURRENT_PROTOCOL_VERSION,
            query_id: QueryId::from_u128(1).unwrap().into_bytes().to_vec().into(),
            session_id: SessionId::from_u128(2)
                .unwrap()
                .into_bytes()
                .to_vec()
                .into(),
            table: Some(wire::TableBinding {
                sql_name: String::new(),
                table: Some(table_binding::Table::TableId(
                    TableId::from_u128(3).unwrap().into_bytes().to_vec().into(),
                )),
            }),
            start: Some(log_command::Start::Offset(0)),
            deadline_ns: None,
            memory_limit: Some(crate::error::resource_limit_to_wire(
                ResourceLimit::ServerDefault,
            )),
            batch_rows: Some(32),
        }
    }

    #[test]
    fn command_requires_explicit_cursor_session_table_and_admission() {
        let parse = |command: wire::LogCommand| {
            decode_log_descriptor(&FlightDescriptor::new_cmd(command.encode_to_vec()))
        };
        assert_eq!(parse(command()).unwrap().start, Some(LogOffset::new(0)));
        let mut latest = command();
        latest.start = Some(log_command::Start::Latest(true));
        assert!(parse(latest).unwrap().start.is_none());
        for bad in 0..8 {
            let mut value = command();
            match bad {
                0 => value.start = None,
                1 => value.start = Some(log_command::Start::Latest(false)),
                2 => value.session_id = Bytes::from_static(&[0; 16]),
                3 => value.table.as_mut().unwrap().sql_name = "alias".to_owned(),
                4 => value.memory_limit = None,
                5 => value.batch_rows = Some(0),
                6 => value.protocol_version += 1,
                _ => {
                    value.memory_limit = Some(crate::error::resource_limit_to_wire(
                        ResourceLimit::Disabled,
                    ))
                }
            }
            assert_eq!(parse(value).unwrap_err().code(), ErrorCode::InvalidArgument);
        }
        let cancel = FlightData {
            app_metadata: wire::LogControl {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                control: Some(wire::log_control::Control::Cancel(true)),
            }
            .encode_to_vec()
            .into(),
            ..FlightData::default()
        };
        decode_log_control(&cancel).unwrap();
        assert!(decode_log_control(&FlightData::default()).is_err());
    }

    #[test]
    fn durable_descriptor_and_ack_require_explicit_identity_and_delivery() {
        let mut valid = command();
        valid.start = Some(log_command::Start::Committed(true));
        valid.consumer = Some(wire::LogConsumer {
            consumer_id: DurableConsumerId::from_u128(7)
                .unwrap()
                .into_bytes()
                .to_vec()
                .into(),
            authority: Some(wire::log_consumer::Authority::Individual(true)),
        });
        let decode = |command: wire::LogCommand| {
            decode_log_descriptor(&FlightDescriptor::new_cmd(command.encode_to_vec()))
        };
        assert!(decode(valid.clone()).unwrap().consumer.is_some());
        for change in 0..5 {
            let mut invalid = valid.clone();
            match change {
                0 => invalid.start = Some(log_command::Start::Offset(0)),
                1 => invalid.consumer = None,
                2 => invalid.consumer.as_mut().unwrap().consumer_id = Bytes::from_static(&[0; 16]),
                3 => {
                    invalid.consumer.as_mut().unwrap().authority =
                        Some(wire::log_consumer::Authority::Individual(false))
                }
                _ => {
                    invalid.consumer.as_mut().unwrap().authority = Some(
                        wire::log_consumer::Authority::GroupId(Bytes::from_static(&[0; 16])),
                    )
                }
            }
            assert_eq!(
                decode(invalid).unwrap_err().code(),
                ErrorCode::InvalidArgument
            );
        }
        let ack = wire::LogAcknowledgement {
            request_id: RequestId::from_u128(8)
                .unwrap()
                .into_bytes()
                .to_vec()
                .into(),
            delivery_sequence: 1,
            processed_through: 0,
        };
        let decode = |ack| {
            decode_log_control(&FlightData {
                app_metadata: wire::LogControl {
                    protocol_version: 1,
                    control: Some(wire::log_control::Control::Acknowledge(ack)),
                }
                .encode_to_vec()
                .into(),
                ..FlightData::default()
            })
        };
        assert!(
            matches!(decode(ack.clone()).unwrap(), FlightLogControl::Acknowledge {
            acknowledgement: SubscriptionAck { processed_through: CommittedOffset::Known(offset), .. }, ..
        } if offset == LogOffset::new(0))
        );
        let mut invalid = ack.clone();
        invalid.delivery_sequence = 0;
        assert!(decode(invalid).is_err());
        let mut invalid = ack;
        invalid.request_id = Bytes::new();
        assert!(decode(invalid).is_err());
    }

    #[tokio::test]
    async fn split_transport_batches_preserve_offsets_frame_and_owned_memory() {
        let drops = Arc::new(AtomicUsize::new(0));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]));
        let mut fragments = Vec::new();
        for first in [0, 4000] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from_iter_values(first..first + 4000))],
            )
            .unwrap();
            let offsets = OffsetRange::new(
                LogOffset::new(first as u64),
                LogOffset::new(first as u64 + 3999),
            )
            .unwrap();
            fragments.push(Ok(FlightLogBatch {
                acknowledgement: Some(SubscriptionAck {
                    delivery_sequence: 3,
                    processed_through: CommittedOffset::Known(LogOffset::new(7999)),
                }),
                batch: QueryResultBatch::with_owner(batch, DropCounter(drops.clone())),
                offsets,
                frame: Some(OffsetRange::new(LogOffset::new(0), LogOffset::new(7999)).unwrap()),
                watermark: OffsetWatermark::Known(LogOffset::new(7999)),
            }));
        }
        let subscription = FlightLogSubscription {
            consumer: Some(FlightLogConsumer {
                consumer_id: DurableConsumerId::from_u128(7).unwrap(),
                offset_authority: ConsumerOffsetAuthority::Consumer(
                    DurableConsumerId::from_u128(7).unwrap(),
                ),
            }),
            next_offset: Some(LogOffset::new(0)),
            acknowledger: Some(Arc::new(TestAcknowledger)),
            instance_id: QueryInstanceId::new(
                QueryId::from_u128(1).unwrap(),
                std::num::NonZeroU64::new(1).unwrap(),
            ),
            table_id: TableId::from_u128(2).unwrap(),
            kind: TableKind::Frame,
            schema,
            watermark: OffsetWatermark::Known(LogOffset::new(7999)),
            stream: futures::stream::iter(fragments).boxed(),
        };
        let mut stream = encode_log(subscription, 4096);
        let schema = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            wire::LogMetadata::decode(schema.app_metadata)
                .unwrap()
                .payload,
            Some(log_metadata::Payload::Opened(_))
        ));
        let schema = stream.next().await.unwrap().unwrap();
        assert!(arrow::ipc::root_as_message(&schema.data_header)
            .unwrap()
            .header_as_schema()
            .is_some());
        let mut next = 0;
        let mut pieces = 0;
        while let Some(data) = stream.next().await {
            let data = data.unwrap();
            let Some(log_metadata::Payload::Progress(progress)) =
                wire::LogMetadata::decode(data.app_metadata)
                    .unwrap()
                    .payload
            else {
                panic!("missing progress")
            };
            assert_eq!(progress.first_offset, next);
            assert_eq!(progress.frame_first_offset, Some(0));
            assert_eq!(progress.frame_last_offset, Some(7999));
            assert_eq!(progress.delivery_sequence, Some(3));
            assert_eq!(progress.delivery_last_offset, Some(7999));
            assert_eq!(drops.load(Ordering::SeqCst), usize::from(next >= 4000));
            let header = arrow::ipc::root_as_message(&data.data_header).unwrap();
            assert_eq!(
                header.header_as_record_batch().unwrap().length() as u64,
                progress.last_offset - progress.first_offset + 1
            );
            next = progress.last_offset + 1;
            pieces += 1;
        }
        assert_eq!(next, 8000);
        assert!(pieces > 2);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn dropping_partial_encoding_releases_native_owner() {
        let drops = Arc::new(AtomicUsize::new(0));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..4000))],
        )
        .unwrap();
        let offsets = OffsetRange::new(LogOffset::new(0), LogOffset::new(3999)).unwrap();
        let subscription = FlightLogSubscription {
            consumer: None,
            next_offset: None,
            acknowledger: None,
            instance_id: QueryInstanceId::new(
                QueryId::from_u128(1).unwrap(),
                std::num::NonZeroU64::new(1).unwrap(),
            ),
            table_id: TableId::from_u128(2).unwrap(),
            kind: TableKind::Append,
            schema,
            watermark: OffsetWatermark::Known(offsets.last()),
            stream: futures::stream::iter(vec![Ok(FlightLogBatch {
                acknowledgement: None,
                batch: QueryResultBatch::with_owner(batch, DropCounter(drops.clone())),
                offsets,
                frame: None,
                watermark: OffsetWatermark::Known(offsets.last()),
            })])
            .chain(futures::stream::pending())
            .boxed(),
        };
        let mut encoded = encode_log(subscription, 4096);
        encoded.next().await.unwrap().unwrap();
        encoded.next().await.unwrap().unwrap();
        encoded.next().await.unwrap().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(encoded);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
