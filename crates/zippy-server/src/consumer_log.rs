//! Durable native event delivery: one complete unit, one exact ACK, one Catalog authority.

use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, Int64Array, StringArray};
use futures::StreamExt;
use zippy_catalog::{SqliteCatalogReadHandle, SqliteCatalogWriteHandle};
use zippy_ingest::NativeIngestHandle;
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1,
    CatalogConsumerRead, CatalogRead, CatalogWrite, CommitConsumerOffsetCommand,
    ReadConsumerOffsetRequest,
};
use zippy_types::query::{QueryInstanceId, QueryOwnerCapability};
use zippy_types::subscription::SubscriptionAck;
use zippy_types::table::{TableKind, TableRef};
use zippy_types::{
    CommittedOffset, ConnectionId, ConsumerOffsetAuthority, ErrorCode, LogOffset, RequestId,
    Result, ServerError, TableId,
};

use crate::{NativeLogBatch, NativeLogRequest, NativeLogStream, NativeQueryEngine};

type AuthorityKey = (TableId, ConsumerOffsetAuthority);

#[derive(Default)]
struct Memberships {
    connections: HashSet<ConnectionId>,
    authorities: HashSet<AuthorityKey>,
}

struct Membership {
    registry: Arc<Mutex<Memberships>>,
    connection: ConnectionId,
    authority: AuthorityKey,
}

impl Drop for Membership {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().unwrap();
        registry.connections.remove(&self.connection);
        registry.authorities.remove(&self.authority);
    }
}

/// Runtime-owned durable subscription capability; transports authorize access before calling.
///
/// A consumer or group has one active connection. The shared registry survives handle clones,
/// while the Catalog is the sole persistent offset authority. Connections replay from the
/// last acknowledged boundary and allow only one unacknowledged atomic delivery.
#[derive(Clone)]
pub struct NativeConsumerLogService {
    engine: NativeQueryEngine,
    read: SqliteCatalogReadHandle,
    write: SqliteCatalogWriteHandle,
    ingest: NativeIngestHandle,
    memberships: Arc<Mutex<Memberships>>,
}

impl NativeConsumerLogService {
    pub(crate) fn new(
        engine: NativeQueryEngine,
        read: SqliteCatalogReadHandle,
        write: SqliteCatalogWriteHandle,
        ingest: NativeIngestHandle,
    ) -> Self {
        Self {
            engine,
            read,
            write,
            ingest,
            memberships: Arc::default(),
        }
    }

    /// Opens committed replay using the authenticated session's query ownership capability.
    ///
    /// `request.table` must identify `consumer.table_id`, and `request.start` must be absent:
    /// durable replay always derives its position from Catalog. Frame and key-value mutations
    /// are delivered completely or fail under the existing query memory budget.
    pub async fn open(
        &self,
        owner: &dyn QueryOwnerCapability,
        connection: ConnectionId,
        consumer: ReadConsumerOffsetRequest,
        mut request: NativeLogRequest,
    ) -> Result<NativeConsumerLog> {
        if request.table
            != (TableRef::Id {
                table_id: consumer.table_id,
            })
            || request.start.is_some()
        {
            return Err(invalid(
                "durable log requires an exact table id and committed start",
            ));
        }
        let membership = {
            let mut registry = self.memberships.lock().unwrap();
            let authority = (consumer.table_id, consumer.offset_authority);
            if registry.connections.contains(&connection)
                || registry.authorities.contains(&authority)
            {
                return Err(ServerError::new(
                    ErrorCode::Conflict,
                    "consumer authority is active",
                ));
            }
            // Bound opening reservations as well as active subscriptions. Query admission applies
            // its stricter configured resource limits after metadata resolution.
            if registry.connections.len() >= 1024 {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "native consumer connection limit reached",
                ));
            }
            registry.connections.insert(connection);
            registry.authorities.insert(authority);
            Arc::new(Membership {
                registry: Arc::clone(&self.memberships),
                connection,
                authority,
            })
        };
        let offset = self.read.read_consumer_offset(consumer).await?;
        let table = self.read.get_table(request.table.clone()).await?;
        let start = offset.committed_log_offset.next_offset().ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "consumer offset domain is exhausted",
            )
        })?;
        let kind = table.definition.kind();
        let predecessor = if kind == TableKind::KeyValue {
            // One physical row per fragment avoids cloning unaccounted Arrow slices when a
            // physical segment contains several mutations. Each row retains its query permit.
            request.batch_rows = NonZeroU32::new(1);
            start.get().checked_sub(1).map(LogOffset::new)
        } else {
            None
        };
        request.start = Some(predecessor.unwrap_or(start));
        let log = self.engine.subscribe_log(owner, request).await?;
        if log.schema_version() != offset.schema_version {
            return Err(ServerError::new(
                ErrorCode::SchemaMismatch,
                "consumer opening schema changed",
            ));
        }
        Ok(NativeConsumerLog {
            instance_id: log.instance_id(),
            schema: log.schema(),
            opening_watermark: log.watermark(),
            cancellation: log.cancellation(),
            changes: log.changes(),
            stream: Some(log.into_stream()),
            service: self.clone(),
            membership,
            consumer,
            schema_version: offset.schema_version,
            kind,
            committed: offset.committed_log_offset,
            next: start,
            predecessor,
            sequence: 0,
            fragments: Vec::new(),
            unit_end: None,
            kv_identity: None,
            previous_kv_version: 0,
            pending: None,
            last_ack: None,
            commit: None,
        })
    }
}

/// Complete atomic delivery retaining every fragment's query memory and snapshot ownership.
///
/// Fragments are borrowed or moved with this non-cloneable owner; retaining this value retains
/// its query charges even after the subscription closes. No concatenation copy is required.
pub struct NativeConsumerDelivery {
    /// Complete append batch, frame, or key-value mutation in original order.
    pub fragments: Vec<NativeLogBatch>,
    /// Exact acknowledgement accepted after every fragment has been processed.
    pub acknowledgement: SubscriptionAck,
}

/// One durable connection with cancellation-independent ACK persistence.
pub struct NativeConsumerLog {
    instance_id: QueryInstanceId,
    schema: arrow::datatypes::SchemaRef,
    opening_watermark: zippy_types::OffsetWatermark,
    cancellation: crate::query_service::QueryCancellation,
    changes: zippy_ingest::TableActorChanges,
    stream: Option<NativeLogStream>,
    service: NativeConsumerLogService,
    membership: Arc<Membership>,
    consumer: ReadConsumerOffsetRequest,
    schema_version: u64,
    kind: TableKind,
    committed: CommittedOffset,
    next: LogOffset,
    predecessor: Option<LogOffset>,
    sequence: u64,
    fragments: Vec<NativeLogBatch>,
    unit_end: Option<LogOffset>,
    kv_identity: Option<(i64, String)>,
    previous_kv_version: i64,
    pending: Option<SubscriptionAck>,
    last_ack: Option<SubscriptionAck>,
    commit: Option<(RequestId, tokio::task::JoinHandle<Result<()>>)>,
}

impl NativeConsumerLog {
    pub(crate) fn cancellation(&self) -> crate::query_service::QueryCancellation {
        self.cancellation.clone()
    }

    /// Returns the stable raw event schema of this connection.
    pub fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Returns the event semantics used for atomic delivery.
    pub const fn kind(&self) -> TableKind {
        self.kind
    }

    /// Returns the fixed watermark captured when opening this connection.
    pub const fn opening_watermark(&self) -> zippy_types::OffsetWatermark {
        self.opening_watermark
    }

    /// Returns the first event not represented by the durable committed position.
    pub fn committed_next_offset(&self) -> Option<LogOffset> {
        self.committed.next_offset()
    }

    /// Returns the query incarnation used for status, deadline and cancellation.
    pub const fn instance_id(&self) -> QueryInstanceId {
        self.instance_id
    }

    /// Reads one complete atomic unit. A previous delivery must be acknowledged first.
    ///
    /// Cancelling this future retains partial assembly in the handle. Dropping the handle
    /// releases the stream; reconnection replays every unacknowledged unit from Catalog.
    pub async fn next_delivery(&mut self) -> Result<NativeConsumerDelivery> {
        if self.pending.is_some() {
            return Err(invalid(
                "previous consumer delivery must be acknowledged first",
            ));
        }
        let result = self.read_delivery().await;
        if result.is_err() {
            self.stream = None;
            self.fragments.clear();
        }
        result
    }

    async fn read_delivery(&mut self) -> Result<NativeConsumerDelivery> {
        loop {
            let fragment = self
                .stream
                .as_mut()
                .ok_or_else(|| ServerError::new(ErrorCode::Cancelled, "consumer stream is closed"))?
                .next()
                .await
                .transpose()?
                .ok_or_else(|| {
                    ServerError::new(ErrorCode::Unavailable, "consumer log ended unexpectedly")
                })?;
            let end = match self.kind {
                TableKind::Append => fragment.offsets.last(),
                TableKind::Frame => fragment
                    .frame
                    .ok_or_else(|| corrupt("missing frame boundary"))?
                    .last(),
                TableKind::KeyValue => {
                    let (end, version, operation) = validate_kv(&fragment, self.consumer.table_id)?;
                    if let Some(predecessor) = self.predecessor.take() {
                        if fragment.offsets.first() != predecessor || end != predecessor {
                            return Err(corrupt(
                                "committed consumer offset splits a key-value mutation",
                            ));
                        }
                        self.previous_kv_version = version;
                        continue;
                    }
                    if let Some(identity) = &self.kv_identity {
                        if identity != &(version, operation.clone()) {
                            return Err(corrupt(
                                "key-value mutation identity changed within delivery",
                            ));
                        }
                    } else {
                        if self.previous_kv_version.checked_add(1) != Some(version) {
                            return Err(corrupt("key-value mutation version is not consecutive"));
                        }
                        self.kv_identity = Some((version, operation));
                    }
                    end
                }
            };
            if fragment.offsets.first() != self.next
                || fragment.offsets.last() > end
                || self.unit_end.is_some_and(|expected| expected != end)
                || fragment
                    .watermark
                    .known()
                    .is_none_or(|watermark| end > watermark)
            {
                return Err(corrupt(
                    "consumer log contains a gap or inconsistent atomic boundary",
                ));
            }
            if self.fragments.is_empty()
                && self.kind == TableKind::Frame
                && fragment
                    .frame
                    .is_some_and(|frame| frame.first() != self.next)
            {
                return Err(corrupt("consumer frame starts inside its atomic boundary"));
            }
            self.unit_end = Some(end);
            let complete = fragment.offsets.last() == end;
            self.next = fragment.offsets.last().checked_next().ok_or_else(|| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "consumer offset domain is exhausted",
                )
            })?;
            self.fragments.push(fragment);
            if complete {
                self.sequence = self.sequence.checked_add(1).ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::ResourceExhausted,
                        "consumer delivery sequence exhausted",
                    )
                })?;
                let acknowledgement = SubscriptionAck {
                    delivery_sequence: self.sequence,
                    processed_through: CommittedOffset::Known(end),
                };
                self.pending = Some(acknowledgement);
                self.unit_end = None;
                if let Some((version, _)) = self.kv_identity.take() {
                    self.previous_kv_version = version;
                }
                return Ok(NativeConsumerDelivery {
                    fragments: std::mem::take(&mut self.fragments),
                    acknowledgement,
                });
            }
        }
    }

    /// Persists the delivered prefix and atomically commits the exact delivery's offset.
    ///
    /// The request ID must remain stable when retrying an interrupted ACK. Cancelling the
    /// caller's wait does not cancel the commit or release group membership early. Repeating
    /// the last successful ACK is harmless; fabricated or partial delivery positions fail.
    pub async fn acknowledge(&mut self, request_id: RequestId, ack: SubscriptionAck) -> Result<()> {
        if self.last_ack == Some(ack) {
            return Ok(());
        }
        if self.pending != Some(ack) {
            return Err(invalid(
                "acknowledgement differs from the complete delivered unit",
            ));
        }
        if self
            .commit
            .as_ref()
            .is_some_and(|(id, _)| *id != request_id)
        {
            return Err(invalid(
                "pending acknowledgement requires its original request id",
            ));
        }
        if self.commit.is_none() {
            if let Some(terminal) = self.cancellation.terminal_signal() {
                self.stream = None;
                return Err(terminal.error.unwrap_or_else(|| {
                    ServerError::new(ErrorCode::Cancelled, "consumer query is closed")
                }));
            }
            let service = self.service.clone();
            let membership = Arc::clone(&self.membership);
            let consumer = self.consumer;
            let schema_version = self.schema_version;
            let command = CatalogCommandEnvelopeV1::new(
                request_id,
                None,
                CatalogActor {
                    kind: CatalogActorKind::Server,
                    principal: "native-consumer".to_owned(),
                },
                CatalogCommandV1::CommitConsumerOffset(CommitConsumerOffsetCommand {
                    consumer_id: consumer.consumer_id,
                    table_id: consumer.table_id,
                    offset_authority: consumer.offset_authority,
                    expected_schema_version: schema_version,
                    expected_committed_log_offset: self.committed,
                    committed_log_offset: ack.processed_through,
                }),
            )
            .map_err(|error| invalid(&format!("invalid consumer command error=[{error}]")))?;
            self.commit = Some((
                request_id,
                tokio::spawn(async move {
                    let _membership = membership;
                    let _checkpoint = service
                        .ingest
                        .checkpoint_table(consumer.table_id, schema_version)
                        .await?;
                    service.write.command(command.into()).await?;
                    Ok(())
                }),
            ));
        }
        let result = (&mut self.commit.as_mut().expect("admitted commit").1).await;
        self.commit = None;
        result.map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("consumer commit task failed error=[{error}]"),
            )
        })??;
        self.committed = ack.processed_through;
        self.last_ack = Some(ack);
        self.pending = None;
        Ok(())
    }
}

fn validate_kv(fragment: &NativeLogBatch, table_id: TableId) -> Result<(LogOffset, i64, String)> {
    use zippy_types::key_value::{BATCH_END, OPERATION, SEQUENCE, STREAM_ID, VERSION};
    let batch = fragment.batch.record_batch();
    if batch.num_rows() != 1 || fragment.offsets.first() != fragment.offsets.last() {
        return Err(corrupt(
            "key-value atomic reader requires one-row owned fragments",
        ));
    }
    let integer = |name| -> Result<i64> {
        let column = batch
            .column_by_name(name)
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| corrupt("invalid key-value integer control"))?;
        if column.is_null(0) {
            return Err(corrupt("null key-value integer control"));
        }
        Ok(column.value(0))
    };
    let string = |name| -> Result<&str> {
        let column = batch
            .column_by_name(name)
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| corrupt("invalid key-value string control"))?;
        if column.is_null(0) {
            return Err(corrupt("null key-value string control"));
        }
        Ok(column.value(0))
    };
    let sequence = integer(SEQUENCE)?;
    let end = integer(BATCH_END)?;
    let version = integer(VERSION)?;
    let operation = string(OPERATION)?;
    if sequence <= 0
        || end < sequence
        || version <= 0
        || sequence as u64 - 1 != fragment.offsets.first().get()
        || !matches!(operation, "upsert" | "delete")
        || string(STREAM_ID)? != format!("{:032x}", table_id.as_u128())
    {
        return Err(corrupt("inconsistent key-value mutation controls"));
    }
    Ok((
        LogOffset::new(end as u64 - 1),
        version,
        operation.to_owned(),
    ))
}

fn invalid(message: &str) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message)
}
fn corrupt(message: &str) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
}

struct FlightConsumerAcknowledgement {
    log: Arc<tokio::sync::Mutex<NativeConsumerLog>>,
    confirmed: tokio::sync::watch::Sender<u64>,
}

impl zippy_protocol::log::FlightLogAcknowledger for FlightConsumerAcknowledgement {
    fn acknowledge(
        &self,
        request_id: RequestId,
        ack: SubscriptionAck,
    ) -> zippy_types::BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.log.lock().await.acknowledge(request_id, ack).await?;
            self.confirmed.send_modify(|sequence| {
                *sequence = (*sequence).max(ack.delivery_sequence);
            });
            Ok(())
        })
    }
}

impl NativeConsumerLog {
    pub(crate) fn into_flight(self) -> zippy_protocol::log::FlightLogSubscription {
        use std::collections::VecDeque;
        use zippy_protocol::log::{FlightLogBatch, FlightLogConsumer, FlightLogSubscription};

        let instance_id = self.instance_id();
        let table_id = self.consumer.table_id;
        let kind = self.kind();
        let schema = self.schema();
        let watermark = self.opening_watermark();
        let next_offset = self.committed_next_offset();
        let consumer = Some(FlightLogConsumer {
            consumer_id: self.consumer.consumer_id,
            offset_authority: self.consumer.offset_authority,
        });
        let cancellation = self.cancellation();
        let changes = self.changes.clone();
        let shared = Arc::new(tokio::sync::Mutex::new(self));
        let (confirmed, received) = tokio::sync::watch::channel(0);
        let acknowledger = Arc::new(FlightConsumerAcknowledgement {
            log: Arc::clone(&shared),
            confirmed,
        });
        let stream = futures::stream::try_unfold(
            (shared, received, VecDeque::<NativeLogBatch>::new(), None::<SubscriptionAck>, cancellation, changes),
            |(shared, mut received, mut fragments, mut acknowledgement, mut cancellation, mut changes)| async move {
                if fragments.is_empty() {
                    if let Some(ack) = acknowledgement {
                        while *received.borrow_and_update() < ack.delivery_sequence {
                            tokio::select! {
                                error = changes.wait_for_stop() => return Err(error),
                                terminal = cancellation.wait_terminal() => {
                                    return Err(terminal?.error.unwrap_or_else(||
                                        ServerError::new(ErrorCode::Cancelled, "consumer query is closed")));
                                }
                                changed = received.changed() => {
                                    changed.map_err(|_| ServerError::new(ErrorCode::Unavailable,
                                        "consumer acknowledgement channel closed"))?;
                                }
                            }
                        }
                    }
                    // Never retain this lock while waiting for ACK: the peer's control handler
                    // must be able to commit even when the encoder polls ahead of the next unit.
                    let delivery = shared.lock().await.next_delivery().await?;
                    acknowledgement = Some(delivery.acknowledgement);
                    fragments = delivery.fragments.into();
                }
                let fragment = fragments.pop_front().ok_or_else(|| corrupt("empty atomic delivery"))?;
                Ok(Some((FlightLogBatch {
                    batch: fragment.batch,
                    offsets: fragment.offsets,
                    frame: fragment.frame,
                    watermark: fragment.watermark,
                    acknowledgement,
                }, (shared, received, fragments, acknowledgement, cancellation, changes))))
            },
        ).boxed();
        FlightLogSubscription {
            instance_id,
            table_id,
            kind,
            schema,
            watermark,
            stream,
            consumer,
            next_offset,
            acknowledger: Some(acknowledger),
        }
    }
}
