use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use arrow::ipc::convert::IpcSchemaEncoder;
use arrow::record_batch::RecordBatch;

use crate::{Result, SchemaRef, ZippyError};

type SourceStopFn = Box<dyn FnMut() -> Result<()> + Send>;

struct SourceHandleState {
    join_handle: Option<JoinHandle<Result<()>>>,
    join_result: Option<Result<()>>,
    joining: bool,
    stop_fn: Option<SourceStopFn>,
    stop_requested: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceMode {
    Pipeline,
    Consumer,
}

#[derive(Clone, Debug)]
pub struct StreamHello {
    pub protocol_version: u16,
    pub stream_name: String,
    pub schema: SchemaRef,
    pub schema_hash: String,
}

impl StreamHello {
    pub fn new(stream_name: &str, schema: SchemaRef, protocol_version: u16) -> Result<Self> {
        if stream_name.is_empty() {
            return Err(ZippyError::InvalidConfig {
                reason: "stream name must not be empty".to_string(),
            });
        }
        let schema_hash = canonical_schema_hash(&schema);

        Ok(Self {
            protocol_version,
            stream_name: stream_name.to_string(),
            schema,
            schema_hash,
        })
    }
}

pub enum SourceEvent {
    Hello(StreamHello),
    Data(RecordBatch),
    Flush,
    Stop,
    Error(String),
}

pub trait SourceSink: Send + Sync + 'static {
    fn emit(&self, event: SourceEvent) -> Result<()>;
}

#[derive(Clone)]
pub struct SourceHandle {
    inner: Arc<(Mutex<SourceHandleState>, Condvar)>,
}

impl SourceHandle {
    pub fn new(join_handle: JoinHandle<Result<()>>) -> Self {
        Self::new_with_stop_internal(join_handle, None)
    }

    pub fn new_with_stop(join_handle: JoinHandle<Result<()>>, stop_fn: SourceStopFn) -> Self {
        Self::new_with_stop_internal(join_handle, Some(stop_fn))
    }

    fn new_with_stop_internal(
        join_handle: JoinHandle<Result<()>>,
        stop_fn: Option<SourceStopFn>,
    ) -> Self {
        Self {
            inner: Arc::new((
                Mutex::new(SourceHandleState {
                    join_handle: Some(join_handle),
                    join_result: None,
                    joining: false,
                    stop_fn,
                    stop_requested: false,
                }),
                Condvar::new(),
            )),
        }
    }

    pub fn stop(&self) -> Result<()> {
        let (lock, _) = &*self.inner;
        let mut state = lock.lock().unwrap();
        if state.stop_requested {
            return Ok(());
        }
        match state.stop_fn.as_mut() {
            Some(stop_fn) => {
                stop_fn()?;
                state.stop_requested = true;
                Ok(())
            }
            None => {
                state.stop_requested = true;
                Ok(())
            }
        }
    }

    pub fn stop_requested(&self) -> bool {
        let (lock, _) = &*self.inner;
        lock.lock().unwrap().stop_requested
    }

    pub fn join(&self) -> Result<()> {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().unwrap();

        loop {
            if let Some(join_result) = state.join_result.clone() {
                return join_result;
            }

            if let Some(join_handle) = state.join_handle.take() {
                state.joining = true;
                drop(state);

                let join_result = match join_handle.join() {
                    Ok(result) => result,
                    Err(_) => Err(ZippyError::Io {
                        reason: "source thread panicked".to_string(),
                    }),
                };

                let mut state = lock.lock().unwrap();
                state.joining = false;
                state.join_result = Some(join_result.clone());
                condvar.notify_all();
                return join_result;
            }

            if state.joining {
                state = condvar.wait(state).unwrap();
                continue;
            }

            panic!("source handle join state invalid");
        }
    }
}

pub trait Source: Send + 'static {
    fn name(&self) -> &str;
    fn output_schema(&self) -> SchemaRef;
    fn mode(&self) -> SourceMode;
    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle>;
}

fn canonical_schema_hash(schema: &SchemaRef) -> String {
    let mut encoder = IpcSchemaEncoder::new();
    let flatbuffer = encoder.schema_to_fb(schema.as_ref());
    let hash = fnv1a64(flatbuffer.finished_data());
    format!("{hash:016x}")
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x00000100000001b3;

    let mut hash = OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}
