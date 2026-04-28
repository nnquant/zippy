use arrow::datatypes::{DataType, TimeUnit};
use arrow::ipc::convert::IpcSchemaEncoder;
use serde_json::{json, Value};

use crate::engine::SchemaRef;

/// Return the canonical schema hash used by stream control-plane metadata.
pub fn canonical_schema_hash(schema: &SchemaRef) -> String {
    let mut encoder = IpcSchemaEncoder::new();
    let flatbuffer = encoder.schema_to_fb(schema.as_ref());
    let hash = fnv1a64(flatbuffer.finished_data());
    format!("{hash:016x}")
}

/// Convert an Arrow schema into stable, human-readable stream metadata.
pub fn schema_metadata(schema: &SchemaRef) -> Value {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let segment_type = segment_type_metadata(field.data_type());
            json!({
                "name": field.name(),
                "data_type": format!("{:?}", field.data_type()),
                "segment_type": segment_type.0,
                "timezone": segment_type.1,
                "nullable": field.is_nullable(),
                "metadata": field.metadata(),
            })
        })
        .collect::<Vec<_>>();

    json!({
        "fields": fields,
        "metadata": schema.metadata(),
    })
}

fn segment_type_metadata(data_type: &DataType) -> (&'static str, Value) {
    match data_type {
        DataType::Int64 => ("int64", Value::Null),
        DataType::Float64 => ("float64", Value::Null),
        DataType::Utf8 => ("utf8", Value::Null),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)) => {
            ("timestamp_ns_tz", Value::String(timezone.to_string()))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => ("timestamp_ns", Value::Null),
        _ => ("unsupported", Value::Null),
    }
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
