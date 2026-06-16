use std::ffi::{CStr, CString};
use std::io::Cursor;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyCapsuleMethods};
use zippy_core::{SegmentTableView, SourceEvent, SourceSink, StreamHello, ZippyError};
use zippy_segment_store::{ActiveSegmentDescriptor, CompiledSchema, LayoutPlan, RowSpanView};

pub const NATIVE_SOURCE_SINK_CAPSULE_NAME: &str = "zippy.native_source_sink.v2";

fn native_source_latency_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("ZIPPY_RUNTIME_LATENCY_TRACE") {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => false,
    })
}

fn write_latency_trace_line(mut line: String) {
    use std::io::Write;

    line.push('\n');
    let mut stderr = std::io::stderr().lock();
    let _ = stderr.write_all(line.as_bytes());
}

fn instant_delta_us(start: Instant, end: Instant) -> f64 {
    end.saturating_duration_since(start).as_secs_f64() * 1_000_000.0
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NativeSourceSinkAbi {
    pub ctx: *mut c_void,
    pub emit_hello: unsafe extern "C" fn(*mut c_void, *const c_char, u16) -> c_int,
    pub emit_data_ipc: unsafe extern "C" fn(*mut c_void, *const u8, usize) -> c_int,
    pub emit_data_segment:
        unsafe extern "C" fn(*mut c_void, *const u8, usize, u64, u64, u64) -> c_int,
    pub emit_flush: unsafe extern "C" fn(*mut c_void) -> c_int,
    pub emit_stop: unsafe extern "C" fn(*mut c_void) -> c_int,
    pub emit_error: unsafe extern "C" fn(*mut c_void, *const c_char, usize) -> c_int,
}

unsafe impl Send for NativeSourceSinkAbi {}

struct NativeSourceSinkState {
    sink: Arc<dyn SourceSink>,
    schema: Arc<Schema>,
    segment_schema: CompiledSchema,
}

pub fn create_native_source_sink_capsule(
    py: Python<'_>,
    sink: Arc<dyn SourceSink>,
    schema: Arc<Schema>,
    segment_schema: CompiledSchema,
) -> PyResult<Bound<'_, PyCapsule>> {
    let state = Box::new(NativeSourceSinkState {
        sink,
        schema,
        segment_schema,
    });
    let ctx = Box::into_raw(state) as *mut c_void;
    let capsule = PyCapsule::new_bound_with_destructor(
        py,
        NativeSourceSinkAbi {
            ctx,
            emit_hello: native_emit_hello,
            emit_data_ipc: native_emit_data_ipc,
            emit_data_segment: native_emit_data_segment,
            emit_flush: native_emit_flush,
            emit_stop: native_emit_stop,
            emit_error: native_emit_error,
        },
        Some(CString::new(NATIVE_SOURCE_SINK_CAPSULE_NAME).unwrap()),
        |_, capsule_ctx| unsafe {
            if !capsule_ctx.is_null() {
                drop(Box::from_raw(capsule_ctx.cast::<NativeSourceSinkState>()));
            }
        },
    )?;
    capsule.set_context(ctx)?;
    Ok(capsule)
}

fn state_from_ctx<'a>(ctx: *mut c_void) -> Result<&'a NativeSourceSinkState, ZippyError> {
    if ctx.is_null() {
        return Err(ZippyError::Io {
            reason: "native source sink context is null".to_string(),
        });
    }

    Ok(unsafe { &*(ctx.cast::<NativeSourceSinkState>()) })
}

unsafe extern "C" fn native_emit_hello(
    ctx: *mut c_void,
    stream_name: *const c_char,
    protocol_version: u16,
) -> c_int {
    emit_hello_impl(ctx, stream_name, protocol_version).map_or(1, |_| 0)
}

unsafe extern "C" fn native_emit_data_ipc(ctx: *mut c_void, data: *const u8, len: usize) -> c_int {
    emit_data_ipc_impl(ctx, data, len).map_or(1, |_| 0)
}

unsafe extern "C" fn native_emit_data_segment(
    ctx: *mut c_void,
    descriptor: *const u8,
    descriptor_len: usize,
    start_row: u64,
    end_row: u64,
    row_capacity: u64,
) -> c_int {
    emit_data_segment_impl(
        ctx,
        descriptor,
        descriptor_len,
        start_row,
        end_row,
        row_capacity,
    )
    .map_or(1, |_| 0)
}

unsafe extern "C" fn native_emit_flush(ctx: *mut c_void) -> c_int {
    emit_simple_event(ctx, SourceEvent::Flush).map_or(1, |_| 0)
}

unsafe extern "C" fn native_emit_stop(ctx: *mut c_void) -> c_int {
    emit_simple_event(ctx, SourceEvent::Stop).map_or(1, |_| 0)
}

unsafe extern "C" fn native_emit_error(
    ctx: *mut c_void,
    reason: *const c_char,
    len: usize,
) -> c_int {
    emit_error_impl(ctx, reason, len).map_or(1, |_| 0)
}

fn emit_hello_impl(
    ctx: *mut c_void,
    stream_name: *const c_char,
    protocol_version: u16,
) -> Result<(), ZippyError> {
    let state = state_from_ctx(ctx)?;
    if stream_name.is_null() {
        return Err(ZippyError::Io {
            reason: "native source sink stream name is null".to_string(),
        });
    }
    let stream_name = unsafe { CStr::from_ptr(stream_name) }
        .to_str()
        .map_err(|error| ZippyError::Io {
            reason: format!("native source sink stream name decode failed error=[{error}]"),
        })?;
    let hello = StreamHello::new(stream_name, Arc::clone(&state.schema), protocol_version)?;
    state.sink.emit(SourceEvent::Hello(hello))
}

fn emit_data_ipc_impl(ctx: *mut c_void, data: *const u8, len: usize) -> Result<(), ZippyError> {
    let state = state_from_ctx(ctx)?;
    if data.is_null() || len == 0 {
        return Err(ZippyError::Io {
            reason: "native source sink ipc payload is empty".to_string(),
        });
    }
    let bytes = unsafe { std::slice::from_raw_parts(data, len) };
    let batch = decode_ipc_batch(bytes)?;
    state
        .sink
        .emit(SourceEvent::Data(SegmentTableView::from_record_batch(
            batch,
        )))
}

fn emit_data_segment_impl(
    ctx: *mut c_void,
    descriptor: *const u8,
    descriptor_len: usize,
    start_row: u64,
    end_row: u64,
    row_capacity: u64,
) -> Result<(), ZippyError> {
    let state = state_from_ctx(ctx)?;
    if descriptor.is_null() || descriptor_len == 0 {
        return Err(ZippyError::Io {
            reason: "native source sink segment descriptor is empty".to_string(),
        });
    }
    let trace_enabled = native_source_latency_trace_enabled();
    let emit_started_at = trace_enabled.then(Instant::now);
    let descriptor_bytes = unsafe { std::slice::from_raw_parts(descriptor, descriptor_len) };
    let row_capacity = usize::try_from(row_capacity).map_err(|_| ZippyError::Io {
        reason: "native source sink segment row capacity overflows usize".to_string(),
    })?;
    let start_row = usize::try_from(start_row).map_err(|_| ZippyError::Io {
        reason: "native source sink segment start row overflows usize".to_string(),
    })?;
    let end_row = usize::try_from(end_row).map_err(|_| ZippyError::Io {
        reason: "native source sink segment end row overflows usize".to_string(),
    })?;
    let layout_started_at = trace_enabled.then(Instant::now);
    let layout = LayoutPlan::for_schema(&state.segment_schema, row_capacity).map_err(|error| {
        ZippyError::Io {
            reason: format!("native source sink segment layout failed error=[{error}]"),
        }
    })?;
    let layout_done_at = trace_enabled.then(Instant::now);
    let descriptor_decode_started_at = trace_enabled.then(Instant::now);
    let descriptor = ActiveSegmentDescriptor::from_envelope_bytes(
        descriptor_bytes,
        state.segment_schema.clone(),
        layout,
    )
    .map_err(|error| ZippyError::Io {
        reason: format!("native source sink segment descriptor decode failed error=[{error}]"),
    })?;
    let descriptor_decode_done_at = trace_enabled.then(Instant::now);
    let attach_started_at = trace_enabled.then(Instant::now);
    let span =
        RowSpanView::from_active_descriptor(descriptor, start_row, end_row).map_err(|error| {
            ZippyError::Io {
                reason: format!("native source sink segment attach failed error=[{error}]"),
            }
        })?;
    let attach_done_at = trace_enabled.then(Instant::now);
    let table_view_started_at = trace_enabled.then(Instant::now);
    let table = SegmentTableView::from_row_span(span);
    let table_view_done_at = trace_enabled.then(Instant::now);
    let sink_emit_started_at = trace_enabled.then(Instant::now);
    let result = state.sink.emit(SourceEvent::Data(table));
    let sink_emit_done_at = trace_enabled.then(Instant::now);
    if let (
        Some(emit_started_at),
        Some(layout_started_at),
        Some(layout_done_at),
        Some(descriptor_decode_started_at),
        Some(descriptor_decode_done_at),
        Some(attach_started_at),
        Some(attach_done_at),
        Some(table_view_started_at),
        Some(table_view_done_at),
        Some(sink_emit_started_at),
        Some(sink_emit_done_at),
    ) = (
        emit_started_at,
        layout_started_at,
        layout_done_at,
        descriptor_decode_started_at,
        descriptor_decode_done_at,
        attach_started_at,
        attach_done_at,
        table_view_started_at,
        table_view_done_at,
        sink_emit_started_at,
        sink_emit_done_at,
    ) {
        write_latency_trace_line(format!(
            "zippy_native_source_latency_trace event=[bridge_data_segment] rows=[{}] descriptor_len=[{}] layout_us=[{:.3}] descriptor_decode_us=[{:.3}] attach_us=[{:.3}] table_view_us=[{:.3}] sink_emit_us=[{:.3}] total_us=[{:.3}]",
            end_row.saturating_sub(start_row),
            descriptor_len,
            instant_delta_us(layout_started_at, layout_done_at),
            instant_delta_us(descriptor_decode_started_at, descriptor_decode_done_at),
            instant_delta_us(attach_started_at, attach_done_at),
            instant_delta_us(table_view_started_at, table_view_done_at),
            instant_delta_us(sink_emit_started_at, sink_emit_done_at),
            instant_delta_us(emit_started_at, sink_emit_done_at),
        ));
    }
    result
}

fn emit_simple_event(ctx: *mut c_void, event: SourceEvent) -> Result<(), ZippyError> {
    let state = state_from_ctx(ctx)?;
    state.sink.emit(event)
}

fn emit_error_impl(ctx: *mut c_void, reason: *const c_char, len: usize) -> Result<(), ZippyError> {
    let state = state_from_ctx(ctx)?;
    if reason.is_null() {
        return Err(ZippyError::Io {
            reason: "native source sink error reason is null".to_string(),
        });
    }
    let bytes = unsafe { std::slice::from_raw_parts(reason.cast::<u8>(), len) };
    let reason = std::str::from_utf8(bytes).map_err(|error| ZippyError::Io {
        reason: format!("native source sink error reason decode failed error=[{error}]"),
    })?;
    state.sink.emit(SourceEvent::Error(reason.to_string()))
}

fn decode_ipc_batch(bytes: &[u8]) -> Result<RecordBatch, ZippyError> {
    let mut reader =
        StreamReader::try_new(Cursor::new(bytes), None).map_err(|error| ZippyError::Io {
            reason: format!("native source sink failed to open ipc stream error=[{error}]"),
        })?;
    let Some(batch_result) = reader.next() else {
        return Err(ZippyError::Io {
            reason: "native source sink ipc stream contained no batch".to_string(),
        });
    };
    let batch = batch_result.map_err(|error| ZippyError::Io {
        reason: format!("native source sink failed to decode ipc batch error=[{error}]"),
    })?;
    if reader.next().is_some() {
        return Err(ZippyError::Io {
            reason: "native source sink ipc stream contained multiple batches".to_string(),
        });
    }
    Ok(batch)
}
