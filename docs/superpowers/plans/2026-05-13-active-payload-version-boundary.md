# Active Payload Version Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to
> implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** add an explicit active payload version boundary so active shared-memory reads can prove
they observed one stable payload version.

**Architecture:** Keep append-only writes cheap, and only version operations that may rewrite
committed payload. Add `payload_version` to the active shm header and descriptor capability,
then wrap active Arrow/scalar reads in a seqlock-style retry check.

**Tech Stack:** Rust, `zippy-segment-store`, serde descriptor envelopes, Arrow RecordBatch tests,
shared-memory acquire/release atomics.

---

## File Structure

- Modify: `crates/zippy-segment-store/src/segment.rs`
  - Add `SHM_PAYLOAD_VERSION_OFFSET`, expose payload version in control snapshots and descriptors.
- Modify: `crates/zippy-segment-store/src/builder.rs`
  - Initialize payload version and add writer mutation helpers.
- Modify: `crates/zippy-segment-store/src/descriptor.rs`
  - Add optional `payload_version_offset` to active descriptor envelopes.
- Modify: `crates/zippy-segment-store/src/active_reader.rs`
  - Validate payload version capability and include it in control snapshots.
- Modify: `crates/zippy-segment-store/src/view.rs`
  - Add active payload stable-read helper and wrap active scalar reads.
- Modify: `crates/zippy-segment-store/src/arrow_bridge.rs`
  - Wrap active Arrow array projection in the stable-read helper.
- Modify: `crates/zippy-segment-store/tests/arrow_bridge.rs`
  - Add descriptor and active Arrow retry tests.
- Modify: `crates/zippy-segment-store/tests/active_segment.rs`
  - Add writer payload mutation tests.
- Modify: `crates/zippy-segment-store/tests/active_segment_reader.rs`
  - Add control snapshot and descriptor compatibility tests.

## Task 1: Header And Descriptor Capability

**Files:**
- Modify: `crates/zippy-segment-store/src/segment.rs`
- Modify: `crates/zippy-segment-store/src/builder.rs`
- Modify: `crates/zippy-segment-store/src/descriptor.rs`
- Modify: `crates/zippy-segment-store/src/active_reader.rs`
- Test: `crates/zippy-segment-store/tests/arrow_bridge.rs`
- Test: `crates/zippy-segment-store/tests/active_segment_reader.rs`

- [ ] **Step 1: Write failing descriptor envelope tests**

Add this helper near the top of `crates/zippy-segment-store/tests/arrow_bridge.rs` if the file does
not already have it:

```rust
fn tick_schema_and_layout() -> (CompiledSchema, LayoutPlan) {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    (schema, layout)
}
```

Also extend the test imports with `CompiledSchema`:

```rust
use zippy_segment_store::{
    compile_schema, debug_snapshot_record_batch_for_test, ActiveSegmentDescriptor,
    ActiveSegmentWriter, ColumnSpec, ColumnType, CompiledSchema, LayoutPlan, RowSpanView,
    ShmRegion,
};
```

Then add these tests to `crates/zippy-segment-store/tests/arrow_bridge.rs`:

```rust
#[test]
fn active_descriptor_envelope_includes_payload_version_offset() {
    let (schema, layout) = tick_schema_and_layout();
    let mut writer = ActiveSegmentWriter::new_for_test(schema.clone(), layout.clone()).unwrap();
    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();

    let bytes = writer.active_descriptor().to_envelope_bytes().unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(envelope["payload_version_offset"], serde_json::json!(88));

    let descriptor = ActiveSegmentDescriptor::from_envelope_bytes(&bytes, schema, layout).unwrap();
    assert_eq!(descriptor.payload_version_offset(), Some(88));
}

#[test]
fn active_descriptor_envelope_accepts_legacy_payload_version_absence() {
    let (schema, layout) = tick_schema_and_layout();
    let mut writer = ActiveSegmentWriter::new_for_test(schema.clone(), layout.clone()).unwrap();
    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();

    let bytes = writer.active_descriptor().to_envelope_bytes().unwrap();
    let mut envelope: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    envelope
        .as_object_mut()
        .unwrap()
        .remove("payload_version_offset");
    let legacy = serde_json::to_vec(&envelope).unwrap();

    let descriptor = ActiveSegmentDescriptor::from_envelope_bytes(&legacy, schema, layout).unwrap();
    assert_eq!(descriptor.payload_version_offset(), None);
}
```

Add this test to `crates/zippy-segment-store/tests/active_segment_reader.rs`:

```rust
#[test]
fn active_segment_reader_rejects_v4_header_without_payload_version_capability() {
    let schema = tick_schema();
    let layout = LayoutPlan::for_schema(&schema, 32).unwrap();
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let partition = store
        .open_partition_with_schema("openctp_ticks", "all", schema.clone())
        .unwrap();
    {
        let writer = partition.writer();
        writer.append_tick_for_test(1, "IF2606", 4112.5).unwrap();
    }

    let bytes = partition.active_descriptor_envelope_bytes().unwrap();
    let mut envelope: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    envelope
        .as_object_mut()
        .unwrap()
        .remove("payload_version_offset");
    let legacy = serde_json::to_vec(&envelope).unwrap();

    let err = ActiveSegmentReader::from_descriptor_envelope(&legacy, schema, layout).unwrap_err();
    assert!(err
        .to_string()
        .contains("active segment payload version capability missing"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p zippy-segment-store --test arrow_bridge active_descriptor_envelope -- --nocapture
cargo test -p zippy-segment-store --test active_segment_reader payload_version -- --nocapture
```

Expected: FAIL because `payload_version_offset()` and descriptor field do not exist.

- [ ] **Step 3: Add header constants and descriptor fields**

In `crates/zippy-segment-store/src/segment.rs`, add:

```rust
pub(crate) const SHM_PAYLOAD_VERSION_OFFSET: usize = 88;
```

Extend `SegmentControlSnapshot`:

```rust
pub struct SegmentControlSnapshot {
    pub magic: u32,
    pub layout_version: u32,
    pub schema_id: u64,
    pub segment_id: u64,
    pub generation: u64,
    pub writer_epoch: u64,
    pub descriptor_generation: u64,
    pub capacity_rows: usize,
    pub row_count: usize,
    pub committed_row_count: usize,
    pub payload_version: Option<u64>,
    pub notify_seq: u32,
    pub waiter_count: u32,
    pub sealed: bool,
    pub payload_offset: usize,
    pub committed_row_count_offset: usize,
}
```

Extend `ActiveSegmentDescriptor` and add accessors:

```rust
pub struct ActiveSegmentDescriptor {
    pub(crate) schema: CompiledSchema,
    pub(crate) layout: LayoutPlan,
    pub(crate) shm_os_id: String,
    pub(crate) payload_offset: usize,
    pub(crate) committed_row_count_offset: usize,
    pub(crate) payload_version_offset: Option<usize>,
    pub(crate) segment_id: u64,
    pub(crate) generation: u64,
    pub(crate) writer_epoch: u64,
    pub(crate) descriptor_generation: u64,
}

impl ActiveSegmentDescriptor {
    pub fn payload_version_offset(&self) -> Option<usize> {
        self.payload_version_offset
    }

    pub fn with_payload_version_offset_for_test(mut self, offset: Option<usize>) -> Self {
        self.payload_version_offset = offset;
        self
    }
}
```

- [ ] **Step 4: Initialize the shm header and descriptor capability**

In `crates/zippy-segment-store/src/builder.rs`, import `SHM_PAYLOAD_VERSION_OFFSET` and initialize
it in `new_with_origin_for_test()`:

```rust
shm_region
    .store_u64_release(SHM_PAYLOAD_VERSION_OFFSET, 0)
    .map_err(|_| "failed to initialize shared memory payload version")?;
```

Set descriptor capability in `active_descriptor()`:

```rust
payload_version_offset: Some(SHM_PAYLOAD_VERSION_OFFSET),
```

- [ ] **Step 5: Add descriptor envelope field**

In `crates/zippy-segment-store/src/descriptor.rs`, import `SHM_PAYLOAD_VERSION_OFFSET` and extend
the envelope:

```rust
#[derive(Debug, Deserialize, Serialize)]
struct ActiveSegmentDescriptorEnvelope {
    magic: String,
    version: u32,
    schema_id: u64,
    row_capacity: usize,
    shm_os_id: String,
    payload_offset: usize,
    committed_row_count_offset: usize,
    #[serde(default)]
    payload_version_offset: Option<usize>,
    segment_id: u64,
    generation: u64,
    writer_epoch: u64,
    descriptor_generation: u64,
}
```

When encoding:

```rust
payload_version_offset: self.payload_version_offset(),
```

When decoding:

```rust
if let Some(offset) = envelope.payload_version_offset {
    if offset != SHM_PAYLOAD_VERSION_OFFSET {
        return Err("active segment descriptor payload version offset mismatch");
    }
}
```

Set the descriptor field:

```rust
payload_version_offset: envelope.payload_version_offset,
```

- [ ] **Step 6: Validate capability during active attach/control snapshot**

In `crates/zippy-segment-store/src/active_reader.rs`, import `SHM_PAYLOAD_VERSION_OFFSET` and add
this helper:

```rust
fn read_payload_version_capability(
    descriptor: &ActiveSegmentDescriptor,
    shm_region: &ShmRegion,
) -> Result<Option<u64>, ZippySegmentStoreError> {
    let layout_version = read_u32_header(shm_region, SHM_LAYOUT_VERSION_OFFSET)?;
    if layout_version == SHM_LAYOUT_VERSION {
        let Some(offset) = descriptor.payload_version_offset() else {
            return Err(ZippySegmentStoreError::Layout(
                "active segment payload version capability missing",
            ));
        };
        if offset != SHM_PAYLOAD_VERSION_OFFSET {
            return Err(ZippySegmentStoreError::Layout(
                "active segment payload version offset mismatch",
            ));
        }
        let version = shm_region.load_u64_acquire(offset)?;
        return Ok(Some(version));
    }
    Ok(None)
}
```

Call it in `read_control_snapshot()` and set:

```rust
payload_version,
```

Mirror the same capability check in `validate_active_descriptor_header()` in
`crates/zippy-segment-store/src/view.rs`.

- [ ] **Step 7: Run focused tests**

Run:

```bash
cargo test -p zippy-segment-store --test arrow_bridge active_descriptor_envelope -- --nocapture
cargo test -p zippy-segment-store --test active_segment_reader payload_version -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/zippy-segment-store/src/segment.rs \
  crates/zippy-segment-store/src/builder.rs \
  crates/zippy-segment-store/src/descriptor.rs \
  crates/zippy-segment-store/src/active_reader.rs \
  crates/zippy-segment-store/src/view.rs \
  crates/zippy-segment-store/tests/arrow_bridge.rs \
  crates/zippy-segment-store/tests/active_segment_reader.rs
git commit -m "feat: add active payload version capability"
```

## Task 2: Writer Payload Mutation Primitives

**Files:**
- Modify: `crates/zippy-segment-store/src/builder.rs`
- Test: `crates/zippy-segment-store/tests/active_segment.rs`

- [ ] **Step 1: Write failing writer tests**

Add this helper near the top of `crates/zippy-segment-store/tests/active_segment.rs` if the file
does not already have it:

```rust
fn tick_schema_and_layout() -> (CompiledSchema, LayoutPlan) {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 8).unwrap();
    (schema, layout)
}
```

Also extend the test imports with `CompiledSchema`:

```rust
use zippy_segment_store::{
    compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, CompiledSchema, LayoutPlan,
    ShmRegion,
};
```

Then add these tests to `crates/zippy-segment-store/tests/active_segment.rs`:

```rust
#[test]
fn payload_mutation_marks_odd_then_even_version() {
    let (schema, layout) = tick_schema_and_layout();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    assert_eq!(writer.payload_version_for_test().unwrap(), 0);
    assert_eq!(writer.begin_payload_mutation_for_test().unwrap(), 1);
    assert_eq!(writer.payload_version_for_test().unwrap(), 1);
    assert_eq!(writer.finish_payload_mutation_for_test().unwrap(), 2);
    assert_eq!(writer.payload_version_for_test().unwrap(), 2);
}

#[test]
fn payload_mutation_rejects_open_row() {
    let (schema, layout) = tick_schema_and_layout();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.begin_row().unwrap();
    let err = writer.begin_payload_mutation_for_test().unwrap_err();

    assert_eq!(err, "open row exists during payload mutation");
}

#[test]
fn payload_mutation_rejects_nested_mutation() {
    let (schema, layout) = tick_schema_and_layout();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.begin_payload_mutation_for_test().unwrap();
    let err = writer.begin_payload_mutation_for_test().unwrap_err();

    assert_eq!(err, "payload mutation already in progress");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p zippy-segment-store --test active_segment payload_mutation -- --nocapture
```

Expected: FAIL because mutation helpers do not exist.

- [ ] **Step 3: Add payload version helpers**

In `crates/zippy-segment-store/src/builder.rs`, add private helpers:

```rust
fn payload_version(&self) -> Result<u64, &'static str> {
    self.shm_region
        .load_u64_acquire(SHM_PAYLOAD_VERSION_OFFSET)
        .map_err(|_| "failed to read shared memory payload version")
}

fn publish_payload_version(&mut self, version: u64) -> Result<(), &'static str> {
    self.shm_region
        .store_u64_release(SHM_PAYLOAD_VERSION_OFFSET, version)
        .map_err(|_| "failed to publish shared memory payload version")
}
```

Add test-only accessor:

```rust
pub fn payload_version_for_test(&self) -> Result<u64, &'static str> {
    self.payload_version()
}
```

- [ ] **Step 4: Split committed-prefix publish from notification**

Replace `publish_committed_prefix()` internals with:

```rust
pub(crate) fn publish_committed_prefix(&mut self) -> Result<(), &'static str> {
    self.publish_committed_prefix_without_notify()?;
    self.notify_readers()?;
    Ok(())
}

fn publish_committed_prefix_without_notify(&mut self) -> Result<(), &'static str> {
    self.header
        .committed_row_count
        .store(self.row_cursor, Ordering::Release);
    write_u64_header(
        &mut self.shm_region,
        SHM_ROW_COUNT_OFFSET,
        self.header.row_count as u64,
    )?;
    self.shm_region
        .store_u64_release(SHM_COMMITTED_ROW_COUNT_OFFSET, self.row_cursor as u64)
        .map_err(|_| "failed to publish shared memory committed row count")?;
    Ok(())
}
```

- [ ] **Step 5: Implement mutation primitives**

Add:

```rust
fn begin_payload_mutation(&mut self) -> Result<u64, &'static str> {
    if self.header.sealed {
        return Err("segment is sealed");
    }
    if self.current_row_open {
        return Err("open row exists during payload mutation");
    }

    let current = self.payload_version()?;
    if current % 2 != 0 {
        return Err("payload mutation already in progress");
    }
    let odd = current
        .checked_add(1)
        .ok_or("payload version overflow")?;
    self.publish_payload_version(odd)?;
    Ok(odd)
}

fn finish_payload_mutation(&mut self) -> Result<u64, &'static str> {
    let current = self.payload_version()?;
    if current % 2 == 0 {
        return Err("payload mutation is not in progress");
    }
    self.publish_committed_prefix_without_notify()?;
    let even = current
        .checked_add(1)
        .ok_or("payload version overflow")?;
    self.publish_payload_version(even)?;
    self.notify_readers()?;
    Ok(even)
}

fn abort_payload_mutation(&mut self) -> Result<u64, &'static str> {
    let current = self.payload_version()?;
    if current % 2 == 0 {
        return Err("payload mutation is not in progress");
    }
    let even = current - 1;
    self.publish_payload_version(even)?;
    self.notify_readers()?;
    Ok(even)
}
```

Expose test-only wrappers:

```rust
pub fn begin_payload_mutation_for_test(&mut self) -> Result<u64, &'static str> {
    self.begin_payload_mutation()
}

pub fn finish_payload_mutation_for_test(&mut self) -> Result<u64, &'static str> {
    self.finish_payload_mutation()
}

pub fn abort_payload_mutation_for_test(&mut self) -> Result<u64, &'static str> {
    self.abort_payload_mutation()
}
```

- [ ] **Step 6: Run focused writer tests**

Run:

```bash
cargo test -p zippy-segment-store --test active_segment payload_mutation -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/zippy-segment-store/src/builder.rs \
  crates/zippy-segment-store/tests/active_segment.rs
git commit -m "feat: add active payload mutation primitives"
```

## Task 3: Stable Active Payload Read Helper

**Files:**
- Modify: `crates/zippy-segment-store/src/view.rs`
- Modify: `crates/zippy-segment-store/src/arrow_bridge.rs`
- Test: `crates/zippy-segment-store/tests/arrow_bridge.rs`
- Test: `crates/zippy-segment-store/tests/active_segment_reader.rs`

- [ ] **Step 1: Write failing stable-read tests**

Add this test to `crates/zippy-segment-store/tests/arrow_bridge.rs`:

```rust
#[test]
fn active_record_batch_read_rejects_odd_payload_version() {
    let (schema, layout) = tick_schema_and_layout();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();
    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.begin_payload_mutation_for_test().unwrap();

    let span = RowSpanView::from_active_descriptor(writer.active_descriptor(), 0, 1).unwrap();
    let err = span.as_record_batch().unwrap_err();

    assert!(err
        .to_string()
        .contains("active payload changed during read"));
}
```

Extend the imports in `crates/zippy-segment-store/tests/active_segment_reader.rs` with
`ActiveSegmentWriter`:

```rust
use zippy_segment_store::{
    compile_schema, ActiveSegmentReader, ActiveSegmentWriter, ColumnSpec, ColumnType,
    CompiledSchema, LayoutPlan, SegmentCellValue, SegmentStore, SegmentStoreConfig,
};
```

Then add this test to `crates/zippy-segment-store/tests/active_segment_reader.rs`:

```rust
#[test]
fn control_snapshot_reports_payload_version() {
    let schema = tick_schema();
    let layout = LayoutPlan::for_schema(&schema, 32).unwrap();
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let partition = store
        .open_partition_with_schema("openctp_ticks", "all", schema.clone())
        .unwrap();
    {
        let writer = partition.writer();
        writer.append_tick_for_test(1, "IF2606", 4112.5).unwrap();
    }

    let envelope = partition.active_descriptor_envelope_bytes().unwrap();
    let reader = ActiveSegmentReader::from_descriptor_envelope(&envelope, schema, layout).unwrap();
    let snapshot = reader.control_snapshot().unwrap();

    assert_eq!(snapshot.payload_version, Some(0));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p zippy-segment-store --test arrow_bridge \
  active_record_batch_read_rejects -- --nocapture
cargo test -p zippy-segment-store --test active_segment_reader \
  control_snapshot_reports_payload_version -- --nocapture
```

Expected: first test FAIL because reads ignore odd payload version. The second test may already pass
after Task 1.

- [ ] **Step 3: Add stable-read helper**

In `crates/zippy-segment-store/src/view.rs`, add:

```rust
const ACTIVE_PAYLOAD_READ_MAX_RETRIES: usize = 8;

pub(crate) fn read_active_payload_consistent<T, E, F, M>(
    attachment: &ActiveSegmentAttachment,
    mut read_fn: F,
    map_error: M,
) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
    M: Fn(ZippySegmentStoreError) -> E,
{
    let Some(offset) = attachment.descriptor.payload_version_offset() else {
        return read_fn();
    };

    for _ in 0..ACTIVE_PAYLOAD_READ_MAX_RETRIES {
        let before = attachment
            .shm_region
            .load_u64_acquire(offset)
            .map_err(|error| map_error(ZippySegmentStoreError::Shmem(error.to_string())))?;
        if before % 2 != 0 {
            continue;
        }

        let result = read_fn()?;
        let after = attachment
            .shm_region
            .load_u64_acquire(offset)
            .map_err(|error| map_error(ZippySegmentStoreError::Shmem(error.to_string())))?;
        if before == after && after % 2 == 0 {
            return Ok(result);
        }
    }

    Err(map_error(ZippySegmentStoreError::Lifecycle(
        "active payload changed during read",
    )))
}
```

- [ ] **Step 4: Wrap Arrow active projection**

In `crates/zippy-segment-store/src/arrow_bridge.rs`, import the helper:

```rust
use crate::view::{read_active_payload_consistent, ActiveSegmentAttachment, RowSpanBacking};
```

Change the active branch:

```rust
RowSpanBacking::Active(attachment) => read_active_payload_consistent(
    attachment,
    || self.project_active_array_unchecked(attachment, field_name, spec),
    |error| ArrowError::ParseError(error.to_string()),
),
```

Rename the existing active function:

```rust
fn project_active_array_unchecked(
    &self,
    attachment: &ActiveSegmentAttachment,
    field_name: &str,
    spec: &crate::ColumnSpec,
) -> Result<ArrayRef, ArrowError> {
    let layout = attachment
        .descriptor
        .layout
        .column(field_name)
        .ok_or_else(|| ArrowError::SchemaError(format!("missing column [{field_name}]")))?;

    match &spec.data_type {
        ColumnType::Int64 => {
            if spec.nullable {
                let values = (self.start_row..self.end_row)
                    .map(|row| {
                        let is_valid = read_active_validity(attachment, layout, row)?;
                        if !is_valid {
                            return Ok(None);
                        }
                        Ok(Some(read_active_i64(attachment, layout, row)?))
                    })
                    .collect::<Result<Vec<_>, ArrowError>>()?;
                Ok(Arc::new(Int64Array::from(values)))
            } else {
                let values = (self.start_row..self.end_row)
                    .map(|row| read_active_i64(attachment, layout, row))
                    .collect::<Result<Vec<_>, ArrowError>>()?;
                Ok(Arc::new(Int64Array::from(values)))
            }
        }
        ColumnType::Float64 => {
            if spec.nullable {
                let values = (self.start_row..self.end_row)
                    .map(|row| {
                        let is_valid = read_active_validity(attachment, layout, row)?;
                        if !is_valid {
                            return Ok(None);
                        }
                        Ok(Some(read_active_f64(attachment, layout, row)?))
                    })
                    .collect::<Result<Vec<_>, ArrowError>>()?;
                Ok(Arc::new(Float64Array::from(values)))
            } else {
                let values = (self.start_row..self.end_row)
                    .map(|row| read_active_f64(attachment, layout, row))
                    .collect::<Result<Vec<_>, ArrowError>>()?;
                Ok(Arc::new(Float64Array::from(values)))
            }
        }
        ColumnType::Utf8 => {
            if spec.nullable {
                let values = (self.start_row..self.end_row)
                    .map(|row| {
                        let is_valid = read_active_validity(attachment, layout, row)?;
                        if !is_valid {
                            return Ok(None);
                        }
                        Ok(Some(read_active_utf8(attachment, layout, row)?))
                    })
                    .collect::<Result<Vec<_>, ArrowError>>()?;
                Ok(Arc::new(StringArray::from(values)))
            } else {
                let values = (self.start_row..self.end_row)
                    .map(|row| read_active_utf8(attachment, layout, row))
                    .collect::<Result<Vec<_>, ArrowError>>()?;
                Ok(Arc::new(StringArray::from(values)))
            }
        }
        ColumnType::TimestampNsTz(timezone) => {
            if spec.nullable {
                let values = (self.start_row..self.end_row)
                    .map(|row| {
                        let is_valid = read_active_validity(attachment, layout, row)?;
                        if !is_valid {
                            return Ok(None);
                        }
                        Ok(Some(read_active_i64(attachment, layout, row)?))
                    })
                    .collect::<Result<Vec<_>, ArrowError>>()?;
                Ok(Arc::new(
                    TimestampNanosecondArray::from(values)
                        .with_timezone((*timezone).to_string()),
                ))
            } else {
                let values = (self.start_row..self.end_row)
                    .map(|row| read_active_i64(attachment, layout, row))
                    .collect::<Result<Vec<_>, ArrowError>>()?;
                Ok(Arc::new(
                    TimestampNanosecondArray::from(values)
                        .with_timezone((*timezone).to_string()),
                ))
            }
        }
    }
}
```

- [ ] **Step 5: Wrap owned scalar reads**

In `crates/zippy-segment-store/src/view.rs`, change the active branch in `cell_value()`:

```rust
RowSpanBacking::Active(attachment) => read_active_payload_consistent(
    attachment,
    || self.active_cell_value(attachment, spec, absolute_row),
    |error| error,
),
```

Leave `utf8_cell_value()` borrowed active reads unchanged in this task. They remain an append-only
hot-filter helper until the later rewrite implementation decides whether to return owned values.

- [ ] **Step 6: Run focused stable-read tests**

Run:

```bash
cargo test -p zippy-segment-store --test arrow_bridge \
  active_record_batch_read_rejects -- --nocapture
cargo test -p zippy-segment-store --test active_segment_reader \
  control_snapshot_reports_payload_version -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/zippy-segment-store/src/view.rs \
  crates/zippy-segment-store/src/arrow_bridge.rs \
  crates/zippy-segment-store/tests/arrow_bridge.rs \
  crates/zippy-segment-store/tests/active_segment_reader.rs
git commit -m "feat: guard active payload reads with version checks"
```

## Task 4: Compatibility And Regression Sweep

**Files:**
- Modify: `crates/zippy-segment-store/src/active_reader.rs`
- Modify: `crates/zippy-segment-store/src/view.rs`
- Test: `crates/zippy-segment-store/tests/active_segment_reader.rs`
- Test: `crates/zippy-segment-store/tests/partition_api.rs`

- [ ] **Step 1: Write missing compatibility regression tests**

Add this test to `crates/zippy-segment-store/tests/partition_api.rs`:

```rust
#[test]
fn active_descriptor_control_snapshot_keeps_append_only_reads_unchanged() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store
        .open_partition_with_schema("ticks", "source", schema)
        .unwrap();
    {
        let writer = handle.writer();
        writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
        writer.append_tick_for_test(2, "rb2505", 4125.0).unwrap();
    }

    let span = handle.active_rows(0, 2).unwrap();
    let batch = span.as_record_batch().unwrap();

    assert_eq!(batch.num_rows(), 2);
    let instrument = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(instrument.value(1), "rb2505");
}
```

Add this test to `crates/zippy-segment-store/tests/active_segment_reader.rs`:

```rust
#[test]
fn reader_read_available_keeps_cursor_when_payload_read_later_fails() {
    let schema = tick_schema();
    let layout = LayoutPlan::for_schema(&schema, 32).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema.clone(), layout.clone()).unwrap();
    writer.append_tick_for_test(1, "IF2606", 4112.5).unwrap();

    let envelope = writer.active_descriptor().to_envelope_bytes().unwrap();
    writer.begin_payload_mutation_for_test().unwrap();
    let mut reader = ActiveSegmentReader::from_descriptor_envelope(&envelope, schema, layout)
        .unwrap();
    let span = reader.read_available().unwrap().expect("expected row");
    assert!(span.as_record_batch().unwrap_err().to_string().contains(
        "active payload changed during read"
    ));
    assert!(reader.read_available().unwrap().is_none());
}
```

- [ ] **Step 2: Run compatibility tests**

Run:

```bash
cargo test -p zippy-segment-store --test partition_api \
  active_descriptor_control_snapshot -- --nocapture
cargo test -p zippy-segment-store --test active_segment_reader \
  reader_read_available_keeps_cursor -- --nocapture
```

Expected: PASS after Tasks 1-3.

- [ ] **Step 3: Run existing active/arrow suites**

Run:

```bash
cargo test -p zippy-segment-store --test active_segment -- --nocapture
cargo test -p zippy-segment-store --test active_segment_reader -- --nocapture
cargo test -p zippy-segment-store --test arrow_bridge -- --nocapture
cargo test -p zippy-segment-store --test partition_api -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Fix regressions locally**

If a failure is only a changed expected snapshot field, update the assertion explicitly. If a
failure shows a read path not covered by `read_active_payload_consistent()`, wrap that path before
changing the test.

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-segment-store/src/active_reader.rs \
  crates/zippy-segment-store/src/view.rs \
  crates/zippy-segment-store/tests/active_segment_reader.rs \
  crates/zippy-segment-store/tests/partition_api.rs
git commit -m "test: cover active payload version compatibility"
```

## Task 5: Final Verification

**Files:**
- Verify only.

- [ ] **Step 1: Run package tests**

Run:

```bash
cargo test -p zippy-segment-store --lib -- --nocapture
cargo test -p zippy-segment-store --tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 2: Run clippy**

Run:

```bash
cargo clippy -p zippy-segment-store --all-targets -- -D warnings
```

Expected: PASS.

- [ ] **Step 3: Run formatting and whitespace checks**

Run:

```bash
cargo fmt --check
git diff --check
```

Expected: PASS.

- [ ] **Step 4: Inspect git status**

Run:

```bash
git status --short
```

Expected: clean, unless the next KeyValue rewrite task has already started.

## Self-Review Notes

- The plan does not implement KeyValue snapshot rewrite; it only creates the segment-store safety
  primitive required before that work.
- The plan deliberately leaves append-only commit without per-row `payload_version` bumps.
- The plan wraps owned Arrow/scalar reads. Borrowed `utf8_cell_value()` remains append-only scoped;
  it must be revisited before exposing committed-payload rewrite to hot-filter callers.
- Descriptor compatibility is split: envelope parsing can accept legacy absence, but v4 active
  header attach rejects missing payload version capability.
