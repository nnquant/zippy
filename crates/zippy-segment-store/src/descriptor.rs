use serde::{Deserialize, Serialize};

use crate::{
    segment::{ActiveSegmentDescriptor, SHM_COMMITTED_ROW_COUNT_OFFSET, SHM_PAYLOAD_OFFSET},
    CompiledSchema, LayoutPlan,
};

const ACTIVE_DESCRIPTOR_ENVELOPE_MAGIC: &str = "zippy.segment.active";
const ACTIVE_DESCRIPTOR_ENVELOPE_VERSION: u32 = 2;

#[derive(Debug, Deserialize, Serialize)]
struct ActiveSegmentDescriptorEnvelope {
    magic: String,
    version: u32,
    schema_id: u64,
    row_capacity: usize,
    shm_os_id: String,
    payload_offset: usize,
    committed_row_count_offset: usize,
    segment_id: u64,
    generation: u64,
    writer_epoch: u64,
    descriptor_generation: u64,
}

impl ActiveSegmentDescriptor {
    /// 将 active segment 描述符编码为跨进程传输 envelope。
    pub fn to_envelope_bytes(&self) -> Result<Vec<u8>, &'static str> {
        let envelope = ActiveSegmentDescriptorEnvelope {
            magic: ACTIVE_DESCRIPTOR_ENVELOPE_MAGIC.to_string(),
            version: ACTIVE_DESCRIPTOR_ENVELOPE_VERSION,
            schema_id: self.schema().schema_id(),
            row_capacity: self.layout().row_capacity(),
            shm_os_id: self.shm_os_id().to_string(),
            payload_offset: self.payload_offset(),
            committed_row_count_offset: self.committed_row_count_offset(),
            segment_id: self.segment_id(),
            generation: self.generation(),
            writer_epoch: self.writer_epoch(),
            descriptor_generation: self.descriptor_generation(),
        };

        serde_json::to_vec(&envelope).map_err(|_| "failed to encode active segment descriptor")
    }

    /// 从跨进程传输 envelope 重建 active segment 描述符。
    pub fn from_envelope_bytes(
        bytes: &[u8],
        schema: CompiledSchema,
        layout: LayoutPlan,
    ) -> Result<Self, &'static str> {
        let envelope: ActiveSegmentDescriptorEnvelope = serde_json::from_slice(bytes)
            .map_err(|_| "failed to decode active segment descriptor")?;

        if envelope.magic != ACTIVE_DESCRIPTOR_ENVELOPE_MAGIC {
            return Err("active segment descriptor magic mismatch");
        }
        if envelope.version != ACTIVE_DESCRIPTOR_ENVELOPE_VERSION {
            return Err("active segment descriptor version mismatch");
        }
        if envelope.schema_id != schema.schema_id() {
            return Err("active segment descriptor schema id mismatch");
        }
        if envelope.row_capacity != layout.row_capacity() {
            return Err("active segment descriptor row capacity mismatch");
        }
        if envelope.payload_offset != SHM_PAYLOAD_OFFSET {
            return Err("active segment descriptor payload offset mismatch");
        }
        if envelope.committed_row_count_offset != SHM_COMMITTED_ROW_COUNT_OFFSET {
            return Err("active segment descriptor committed row count offset mismatch");
        }

        Ok(Self {
            schema,
            layout,
            shm_os_id: envelope.shm_os_id,
            payload_offset: envelope.payload_offset,
            committed_row_count_offset: envelope.committed_row_count_offset,
            segment_id: envelope.segment_id,
            generation: envelope.generation,
            writer_epoch: envelope.writer_epoch,
            descriptor_generation: envelope.descriptor_generation,
        })
    }
}
