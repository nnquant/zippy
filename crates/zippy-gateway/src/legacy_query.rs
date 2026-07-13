use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use serde_json::{json, Value};
use zippy_core::{Result, ZippyError};

use super::{
    collect_stream_plan_from_header, GatewayCollectStreamPlan, GatewayCollectStreamProducer,
    GatewayState,
};

/// A validated request for the legacy blocking collect adapter.
#[derive(Clone)]
pub struct LegacyCollectRequest {
    stream_plan: GatewayCollectStreamPlan,
}

impl LegacyCollectRequest {
    /// Build and validate a streaming collect request.
    ///
    /// Only plans that can be fully pushed into the gateway streaming path are accepted.
    pub fn new(source: impl Into<String>, plan: Vec<Value>, chunk_rows: usize) -> Result<Self> {
        let source = source.into();
        if source.trim().is_empty() {
            return Err(ZippyError::InvalidConfig {
                reason: "legacy collect source must not be empty".to_string(),
            });
        }
        if chunk_rows == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "legacy collect chunk_rows must be positive".to_string(),
            });
        }
        Self::from_header(json!({
            "source": source,
            "plan": plan,
            "chunk_rows": chunk_rows,
        }))
    }

    /// Return the source stream name.
    pub fn source(&self) -> &str {
        &self.stream_plan.source
    }

    /// Return the validated query plan.
    pub fn plan(&self) -> &[Value] {
        &self.stream_plan.plan
    }

    /// Return the maximum row count emitted by one cursor batch.
    pub fn chunk_rows(&self) -> usize {
        self.stream_plan.chunk_rows
    }

    fn from_header(header: Value) -> Result<Self> {
        let stream_plan = collect_stream_plan_from_header(header)?;
        if stream_plan.source.trim().is_empty() {
            return Err(ZippyError::InvalidConfig {
                reason: "legacy collect source must not be empty".to_string(),
            });
        }
        if stream_plan.snapshot_id.is_some() {
            return Err(ZippyError::InvalidConfig {
                reason: "legacy collect facade does not support snapshots".to_string(),
            });
        }
        if stream_plan.row_range_pushdown.is_some() {
            return Err(ZippyError::InvalidConfig {
                reason: concat!(
                    "legacy collect facade does not support row-range operations; ",
                    "use a streaming filter/projection plan"
                )
                .to_string(),
            });
        }
        Ok(Self { stream_plan })
    }

    fn into_stream_plan(self) -> GatewayCollectStreamPlan {
        self.stream_plan
    }
}

/// A cloneable, state-hiding entry point for legacy query servers.
#[derive(Clone)]
pub struct LegacyCollectFacade {
    state: Arc<GatewayState>,
}

impl LegacyCollectFacade {
    pub(crate) fn new(state: Arc<GatewayState>) -> Self {
        Self { state }
    }

    /// Validate a request without opening files or contacting master.
    pub fn validate(&self, request: &LegacyCollectRequest) -> Result<()> {
        if self.state.stopped.load(Ordering::Acquire) {
            return Err(ZippyError::InvalidState { status: "stopped" });
        }
        if request.source().trim().is_empty() {
            return Err(ZippyError::InvalidConfig {
                reason: "legacy collect source must not be empty".to_string(),
            });
        }
        if request.chunk_rows() == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "legacy collect chunk_rows must be positive".to_string(),
            });
        }
        Ok(())
    }

    /// Open a blocking cursor without allowing a materialized query fallback.
    ///
    /// Global persisted-buffer capacity is reserved before the first file scan. If capacity is
    /// unavailable, this method returns immediately so callers can retry or cancel safely.
    pub fn open(&self, request: LegacyCollectRequest) -> Result<LegacyCollectCursor> {
        self.open_cancellable(request, Arc::new(AtomicBool::new(false)))
    }

    /// Open a blocking cursor controlled by a caller-owned cancellation flag.
    ///
    /// The cursor and persisted scanner share this flag. The method checks cancellation before and
    /// after synchronous setup; dropping or cancelling the cursor also sets the flag.
    pub fn open_cancellable(
        &self,
        request: LegacyCollectRequest,
        cancelled: Arc<AtomicBool>,
    ) -> Result<LegacyCollectCursor> {
        self.open_cancellable_with_timeout(request, cancelled, self.state.write_timeout)
    }

    /// Open a blocking cursor with a caller-provided control-plane I/O timeout.
    ///
    /// The effective timeout cannot exceed the gateway's configured write timeout.
    pub fn open_cancellable_with_timeout(
        &self,
        request: LegacyCollectRequest,
        cancelled: Arc<AtomicBool>,
        timeout: Duration,
    ) -> Result<LegacyCollectCursor> {
        if timeout.is_zero() {
            return Err(ZippyError::InvalidConfig {
                reason: "legacy collect timeout must be positive".to_string(),
            });
        }
        if cancelled.load(Ordering::Acquire) {
            return Err(legacy_collect_cancelled_error());
        }
        self.validate(&request)?;
        let effective_timeout = timeout.min(self.state.write_timeout);
        let control_deadline = Instant::now()
            .checked_add(effective_timeout)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "legacy collect timeout exceeds monotonic clock range".to_string(),
            })?;
        let mut producer = self.state.collect_stream_producer_direct(
            request.into_stream_plan(),
            control_deadline,
            cancelled.as_ref(),
        )?;
        if cancelled.load(Ordering::Acquire) {
            return Err(legacy_collect_cancelled_error());
        }
        let schema = producer.schema();
        let reservation_bytes = producer.global_buffer_reservation_bytes();
        if reservation_bytes > 0 {
            let permits =
                u32::try_from(reservation_bytes).map_err(|_| ZippyError::ResourceExhausted {
                    reason: format!(
                        "gateway query buffer reservation exceeds supported range bytes=[{}]",
                        reservation_bytes
                    ),
                })?;
            let reservation_started = Instant::now();
            let reservation = Arc::clone(&self.state.persisted_buffer_limit)
                .try_acquire_many_owned(permits)
                .map_err(|error| ZippyError::ResourceExhausted {
                    reason: format!(
                        concat!(
                            "gateway query buffer reservation unavailable bytes=[{}] ",
                            "error=[{}]"
                        ),
                        reservation_bytes, error
                    ),
                })?;
            producer.attach_global_buffer_reservation(reservation)?;
            producer.metrics_mut().global_buffer_wait_elapsed_ms +=
                reservation_started.elapsed().as_secs_f64() * 1000.0;
        }
        if cancelled.load(Ordering::Acquire) {
            return Err(legacy_collect_cancelled_error());
        }
        producer.attach_cancellation_flag(Arc::clone(&cancelled));
        let metrics = producer.metrics();
        Ok(LegacyCollectCursor {
            schema,
            producer: Some(producer),
            last_metrics: metrics,
            cancelled,
        })
    }
}

/// An opaque blocking cursor over a gateway streaming producer.
pub struct LegacyCollectCursor {
    schema: SchemaRef,
    producer: Option<GatewayCollectStreamProducer>,
    last_metrics: Value,
    cancelled: Arc<AtomicBool>,
}

impl LegacyCollectCursor {
    /// Return the result schema without advancing the cursor.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Read the next bounded record batch.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(legacy_collect_cancelled_error());
        }
        let producer = self
            .producer
            .as_mut()
            .ok_or_else(legacy_collect_cancelled_error)?;
        let result = producer.next_batch();
        self.last_metrics = producer.metrics();
        if self.cancelled.load(Ordering::Acquire) {
            return Err(legacy_collect_cancelled_error());
        }
        result
    }

    /// Return a snapshot of the producer metrics.
    pub fn metrics(&self) -> Value {
        self.producer
            .as_ref()
            .map(GatewayCollectStreamProducer::metrics)
            .unwrap_or_else(|| self.last_metrics.clone())
    }

    /// Cancel the cursor and release all scan and buffer resources.
    pub fn cancel(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(producer) = self.producer.take() {
            self.last_metrics = producer.metrics();
            drop(producer);
        }
    }

    #[cfg(test)]
    pub(crate) fn persisted_cancellation_flag(&self) -> Option<Arc<AtomicBool>> {
        self.producer
            .as_ref()
            .and_then(GatewayCollectStreamProducer::persisted_cancellation_flag)
    }
}

impl Drop for LegacyCollectCursor {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn legacy_collect_cancelled_error() -> ZippyError {
    ZippyError::Cancelled {
        reason: "legacy collect cancelled".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{BufRead, BufReader, Read};
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn request_rejects_invalid_or_residual_plans() {
        let empty_source = LegacyCollectRequest::new("", Vec::new(), 1).err().unwrap();
        assert!(empty_source
            .to_string()
            .contains("source must not be empty"));

        let zero_chunk = LegacyCollectRequest::new("ticks", Vec::new(), 0)
            .err()
            .unwrap();
        assert!(zero_chunk
            .to_string()
            .contains("chunk_rows must be positive"));

        let residual = LegacyCollectRequest::new(
            "ticks",
            vec![json!({"op": "sort", "columns": ["seq"]})],
            128,
        )
        .err()
        .unwrap();
        assert!(residual.to_string().contains("fully streamable plan"));

        let row_range =
            LegacyCollectRequest::new("ticks", vec![json!({"op": "head", "n": 1})], 128)
                .err()
                .unwrap();
        assert!(row_range.to_string().contains("row-range operations"));
    }

    #[test]
    fn request_explicitly_rejects_snapshots() {
        let error = LegacyCollectRequest::from_header(json!({
            "source": "ticks",
            "snapshot_id": "snapshot-1",
            "plan": [],
            "chunk_rows": 128,
        }))
        .err()
        .unwrap();

        assert!(error.to_string().contains("does not support snapshots"));
    }

    #[test]
    fn request_accepts_streamable_filter_and_projection() {
        let request = LegacyCollectRequest::new(
            "ticks",
            vec![
                json!({
                    "op": "filter",
                    "expr": {
                        "kind": "binary",
                        "op": "gt",
                        "args": [
                            {"kind": "col", "value": "seq"},
                            {"kind": "literal", "value": 0},
                        ],
                    }
                }),
                json!({
                    "op": "select",
                    "exprs": [{"kind": "col", "value": "seq"}],
                }),
            ],
            128,
        )
        .unwrap();

        assert_eq!(request.source(), "ticks");
        assert_eq!(request.plan().len(), 2);
        assert_eq!(request.chunk_rows(), 128);
    }

    #[test]
    fn facade_rejects_work_after_gateway_shutdown() {
        let gateway = super::super::GatewayServer::new(super::super::GatewayServerConfig {
            endpoint: "127.0.0.1:0".to_string(),
            master_endpoint: zippy_core::ControlEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
            token: None,
            max_write_rows: None,
            max_connections: None,
            max_subscribers: None,
            max_blocking_requests: None,
            write_timeout_ms: None,
        })
        .unwrap();
        let facade = gateway.legacy_collect_facade();
        drop(gateway);
        let request = LegacyCollectRequest::new("ticks", Vec::new(), 128).unwrap();

        let error = facade.validate(&request).unwrap_err();

        assert!(error.to_string().contains("status=[stopped]"));
    }

    #[test]
    fn facade_pre_cancel_returns_typed_cancelled() {
        let gateway = super::super::GatewayServer::new(super::super::GatewayServerConfig {
            endpoint: "127.0.0.1:0".to_string(),
            master_endpoint: zippy_core::ControlEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
            token: None,
            max_write_rows: None,
            max_connections: None,
            max_subscribers: None,
            max_blocking_requests: None,
            write_timeout_ms: None,
        })
        .unwrap();
        let cancelled = Arc::new(AtomicBool::new(true));

        let error = gateway
            .legacy_collect_facade()
            .open_cancellable(
                LegacyCollectRequest::new("pre_cancelled", Vec::new(), 128).unwrap(),
                cancelled,
            )
            .err()
            .unwrap();

        assert!(matches!(
            error,
            ZippyError::Cancelled { ref reason } if reason == "legacy collect cancelled"
        ));
    }

    #[test]
    fn facade_cancellation_interrupts_stalled_master_lookup() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let master_endpoint = zippy_core::ControlEndpoint::Tcp(listener.local_addr().unwrap());
        let (request_seen_tx, request_seen_rx) = mpsc::channel();
        let master = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();
            let request: zippy_core::ControlRequest =
                serde_json::from_str(request_line.trim_end()).unwrap();
            assert!(matches!(request, zippy_core::ControlRequest::GetStream(_)));
            request_seen_tx.send(()).unwrap();

            let mut stream = reader.into_inner();
            let mut closed = [0_u8; 1];
            assert_eq!(stream.read(&mut closed).unwrap(), 0);
        });
        let gateway = super::super::GatewayServer::new(super::super::GatewayServerConfig {
            endpoint: "127.0.0.1:0".to_string(),
            master_endpoint,
            token: None,
            max_write_rows: None,
            max_connections: None,
            max_subscribers: None,
            max_blocking_requests: None,
            write_timeout_ms: Some(5_000),
        })
        .unwrap();
        let facade = gateway.legacy_collect_facade();
        let cancelled = Arc::new(AtomicBool::new(false));
        let canceller_flag = Arc::clone(&cancelled);
        let canceller = thread::spawn(move || {
            request_seen_rx.recv().unwrap();
            canceller_flag.store(true, Ordering::Release);
        });

        let started = Instant::now();
        let error = facade
            .open_cancellable_with_timeout(
                LegacyCollectRequest::new("stalled", Vec::new(), 128).unwrap(),
                cancelled,
                Duration::from_secs(5),
            )
            .err()
            .unwrap();
        let elapsed = started.elapsed();

        assert!(matches!(
            error,
            ZippyError::Cancelled { ref reason } if reason == "control request cancelled"
        ));
        assert!(
            elapsed < Duration::from_millis(500),
            "elapsed=[{elapsed:?}]"
        );
        canceller.join().unwrap();
        master.join().unwrap();
    }
}
