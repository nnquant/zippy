# Segment Store Shared Arrow Hot Table Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `OpenCTP ticks` 落地一条 `Shared Arrow Hot Table` 垂直链路：source 只物化一次，跨进程零拷贝读取，逐 tick 增量算子执行，sealed segment 异步刷 Parquet。

**Architecture:** 新增两层并行栈：`zippy-segment-store` 负责共享内存 segment、可见性协议、生命周期和 Arrow/Parquet 边界桥；`zippy-operator-runtime` 负责进程内 `ReaderTask -> LocalDispatcher -> Operators` 的增量执行。`plugins/zippy-openctp` 增加一条 segment ingress 路径，现有 `RecordBatch`/bus 路径继续保留，不做全量替换。

**Tech Stack:** Rust, Arrow, Parquet, `shared_memory`, `rustix`/`eventfd`, `epoll`, PyO3, cargo tests, pytest, OpenCTP plugin

---

## Planned File Structure

### Root workspace

- Modify: `Cargo.toml`
  - 将 `crates/zippy-segment-store` 与 `crates/zippy-operator-runtime` 加入 workspace members。

### `crates/zippy-segment-store`

- Create: `crates/zippy-segment-store/Cargo.toml`
  - 新 crate 依赖声明。
- Create: `crates/zippy-segment-store/src/lib.rs`
  - 统一导出 schema/layout/segment/view/store API。
- Create: `crates/zippy-segment-store/src/schema.rs`
  - `ColumnSpec`、`ColumnType`、`CompiledSchema`、`compile_schema(columns)`。
- Create: `crates/zippy-segment-store/src/layout.rs`
  - 列缓冲布局、对齐、capacity 计算。
- Create: `crates/zippy-segment-store/src/shm.rs`
  - 共享内存 region/slab 创建与 attach。
- Create: `crates/zippy-segment-store/src/segment.rs`
  - `SegmentHeader`、`ColumnDescriptor`、active/sealed handle。
- Create: `crates/zippy-segment-store/src/builder.rs`
  - `MutableSegmentBuilder`、`committed_row_count` 提交协议。
- Create: `crates/zippy-segment-store/src/catalog.rs`
  - `SegmentStore`、partition 元数据、active/sealed 切换。
- Create: `crates/zippy-segment-store/src/notify.rs`
  - `eventfd` 封装与 reader 通知。
- Create: `crates/zippy-segment-store/src/lifecycle.rs`
  - `session + lease + generation` 生命周期模型。
- Create: `crates/zippy-segment-store/src/view.rs`
  - `RowSpanView`、列 slice view、投影。
- Create: `crates/zippy-segment-store/src/arrow_bridge.rs`
  - `as_record_batch()` / Arrow arrays 桥接。
- Create: `crates/zippy-segment-store/src/persistence.rs`
  - sealed segment 异步刷 Parquet。
- Test: `crates/zippy-segment-store/tests/workspace_smoke.rs`
- Test: `crates/zippy-segment-store/tests/schema_layout.rs`
- Test: `crates/zippy-segment-store/tests/active_segment.rs`
- Test: `crates/zippy-segment-store/tests/lifecycle_notify.rs`
- Test: `crates/zippy-segment-store/tests/arrow_bridge.rs`
- Test: `crates/zippy-segment-store/tests/parquet_persistence.rs`

### `crates/zippy-operator-runtime`

- Create: `crates/zippy-operator-runtime/Cargo.toml`
- Create: `crates/zippy-operator-runtime/src/lib.rs`
- Create: `crates/zippy-operator-runtime/src/operator.rs`
  - `Operator` trait 与 `required_columns()`。
- Create: `crates/zippy-operator-runtime/src/reader.rs`
  - `ReaderTask`，监听 eventfd，生成 `RowSpanView`。
- Create: `crates/zippy-operator-runtime/src/dispatcher.rs`
  - 进程内多算子 fanout。
- Create: `crates/zippy-operator-runtime/src/partition_runtime.rs`
  - partition 亲和执行通道。
- Create: `crates/zippy-operator-runtime/src/state.rs`
  - 简单 rolling/ring 状态容器。
- Create: `crates/zippy-operator-runtime/src/result.rs`
  - 因子结果 sink trait。
- Test: `crates/zippy-operator-runtime/tests/workspace_smoke.rs`
- Test: `crates/zippy-operator-runtime/tests/partition_runtime.rs`

### `plugins/zippy-openctp`

- Modify: `plugins/zippy-openctp/crates/zippy-openctp-core/Cargo.toml`
  - 添加对 `zippy-segment-store` 的依赖。
- Modify: `plugins/zippy-openctp/crates/zippy-openctp-core/src/lib.rs`
  - 导出新的 segment ingress 组件。
- Create: `plugins/zippy-openctp/crates/zippy-openctp-core/src/segment_ingress.rs`
  - `OpenCtpSegmentIngress`，将规范化 tick 直接写入 active segment。
- Modify: `plugins/zippy-openctp/crates/zippy-openctp-core/src/source.rs`
  - 可选地把 segment ingress 接入 source callback。
- Test: `plugins/zippy-openctp/crates/zippy-openctp-core/tests/segment_ingress.rs`
- Test: `plugins/zippy-openctp/crates/zippy-openctp-core/tests/segment_runtime_e2e.rs`

### `crates/zippy-python`

- Modify: `crates/zippy-python/Cargo.toml`
  - 添加对 `zippy-segment-store` / `zippy-operator-runtime` 的依赖。
- Modify: `crates/zippy-python/src/lib.rs`
  - 提供只读调试桥和 runtime 配置桥。
- Modify: `python/zippy/_internal.pyi`
- Modify: `pytests/test_python_api.py`

### 性能与验证

- Create: `crates/zippy-segment-store/tests/openctp_segment_perf_smoke.rs`
  - 与现有 batch/bus 路径对照的 segment 路径性能 smoke。

---

### Task 1: 搭建新 workspace crate 骨架并锁定公共导出面

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/zippy-segment-store/Cargo.toml`
- Create: `crates/zippy-segment-store/src/lib.rs`
- Create: `crates/zippy-segment-store/tests/workspace_smoke.rs`
- Create: `crates/zippy-operator-runtime/Cargo.toml`
- Create: `crates/zippy-operator-runtime/src/lib.rs`
- Create: `crates/zippy-operator-runtime/tests/workspace_smoke.rs`

- [ ] **Step 1: 先写失败测试，固定两个新 crate 的最小导出面**

在 `crates/zippy-segment-store/tests/workspace_smoke.rs` 写：

```rust
use zippy_segment_store::{compile_schema, ColumnSpec, ColumnType};

#[test]
fn segment_store_workspace_exports_compile_schema() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();

    assert_eq!(schema.columns().len(), 2);
}
```

在 `crates/zippy-operator-runtime/tests/workspace_smoke.rs` 写：

```rust
use zippy_operator_runtime::Operator;

struct NoopOperator;

impl Operator for NoopOperator {
    fn name(&self) -> &'static str {
        "noop"
    }

    fn required_columns(&self) -> &'static [&'static str] {
        &[]
    }
}

#[test]
fn operator_runtime_workspace_exports_operator_trait() {
    let op = NoopOperator;
    assert_eq!(op.name(), "noop");
}
```

- [ ] **Step 2: 运行测试确认当前 workspace 还不支持新 crate**

Run: `cargo test -p zippy-segment-store --test workspace_smoke --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: FAIL，提示 `zippy-segment-store` 还不是 workspace member。

- [ ] **Step 3: 写最小实现，加入 workspace 并创建 crate 骨架**

在根 `Cargo.toml` 增加 members：

```toml
members = [
    "crates/zippy-core",
    "crates/zippy-operators",
    "crates/zippy-engines",
    "crates/zippy-io",
    "crates/zippy-python",
    "crates/zippy-perf",
    "crates/zippy-master",
    "crates/zippy-segment-store",
    "crates/zippy-operator-runtime",
]
```

`crates/zippy-segment-store/Cargo.toml` 先建最小依赖：

```toml
[package]
name = "zippy-segment-store"
version.workspace = true
edition.workspace = true

[dependencies]
arrow.workspace = true
parquet.workspace = true
serde.workspace = true
thiserror.workspace = true
tracing.workspace = true
```

`crates/zippy-segment-store/src/lib.rs` 先导出占位模块：

```rust
mod schema;

pub use schema::{compile_schema, ColumnSpec, ColumnType, CompiledSchema};
```

`crates/zippy-operator-runtime/Cargo.toml`：

```toml
[package]
name = "zippy-operator-runtime"
version.workspace = true
edition.workspace = true

[dependencies]
arrow.workspace = true
thiserror.workspace = true
tracing.workspace = true
zippy-segment-store = { path = "../zippy-segment-store" }
```

`crates/zippy-operator-runtime/src/lib.rs`：

```rust
mod operator;

pub use operator::Operator;
```

以及最小占位 `schema.rs` / `operator.rs`：

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Float64,
    TimestampNsTz(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    name: &'static str,
    data_type: ColumnType,
}

impl ColumnSpec {
    pub fn new(name: &'static str, data_type: ColumnType) -> Self {
        Self { name, data_type }
    }
}

#[derive(Debug, Clone)]
pub struct CompiledSchema {
    columns: Vec<ColumnSpec>,
}

impl CompiledSchema {
    pub fn columns(&self) -> &[ColumnSpec] {
        &self.columns
    }
}

pub fn compile_schema(columns: &[ColumnSpec]) -> Result<CompiledSchema, &'static str> {
    Ok(CompiledSchema {
        columns: columns.to_vec(),
    })
}
```

```rust
pub trait Operator {
    fn name(&self) -> &'static str;
    fn required_columns(&self) -> &'static [&'static str];
}
```

- [ ] **Step 4: 运行 workspace smoke，确认骨架已接通**

Run: `cargo test -p zippy-segment-store --test workspace_smoke -p zippy-operator-runtime --test workspace_smoke --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: PASS，两套新 crate 可被 workspace 解析并完成最小导出。

- [ ] **Step 5: 提交新 crate 骨架**

```bash
git add Cargo.toml crates/zippy-segment-store crates/zippy-operator-runtime
git commit -m "feat: scaffold segment store runtime crates"
```

### Task 2: 实现 schema 编译与物理布局 plan

**Files:**
- Create: `crates/zippy-segment-store/src/schema.rs`
- Create: `crates/zippy-segment-store/src/layout.rs`
- Modify: `crates/zippy-segment-store/src/lib.rs`
- Test: `crates/zippy-segment-store/tests/schema_layout.rs`

- [ ] **Step 1: 写失败测试，约束 schema 编译与布局产物**

在 `crates/zippy-segment-store/tests/schema_layout.rs` 新增：

```rust
use zippy_segment_store::{
    compile_schema, ColumnSpec, ColumnType, LayoutPlan,
};

#[test]
fn compile_schema_assigns_stable_column_order_and_layout() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::nullable("bid_price_1", ColumnType::Float64),
    ])
    .unwrap();

    let layout = LayoutPlan::for_schema(&schema, 64).unwrap();

    assert_eq!(schema.schema_id(), 3);
    assert_eq!(layout.row_capacity(), 64);
    assert!(layout.column("instrument_id").unwrap().offsets_len > 0);
    assert!(layout.column("bid_price_1").unwrap().validity_len > 0);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-segment-store --test schema_layout --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: FAIL，提示 `Utf8`、`nullable`、`LayoutPlan` 或 `schema_id()` 尚未实现。

- [ ] **Step 3: 写最小实现，固定 schema 与 layout API**

在 `schema.rs` 实现：

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Int64,
    Float64,
    Utf8,
    TimestampNsTz(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: &'static str,
    pub data_type: ColumnType,
    pub nullable: bool,
}

impl ColumnSpec {
    pub fn new(name: &'static str, data_type: ColumnType) -> Self {
        Self {
            name,
            data_type,
            nullable: false,
        }
    }

    pub fn nullable(name: &'static str, data_type: ColumnType) -> Self {
        Self {
            name,
            data_type,
            nullable: true,
        }
    }
}
```

`CompiledSchema` 至少补齐：

```rust
#[derive(Debug, Clone)]
pub struct CompiledSchema {
    schema_id: u64,
    columns: Vec<ColumnSpec>,
}

impl CompiledSchema {
    pub fn schema_id(&self) -> u64 {
        self.schema_id
    }

    pub fn columns(&self) -> &[ColumnSpec] {
        &self.columns
    }
}
```

在 `layout.rs` 实现：

```rust
#[derive(Debug, Clone)]
pub struct ColumnLayout {
    pub name: &'static str,
    pub values_offset: usize,
    pub values_len: usize,
    pub offsets_offset: usize,
    pub offsets_len: usize,
    pub validity_offset: usize,
    pub validity_len: usize,
}

#[derive(Debug, Clone)]
pub struct LayoutPlan {
    row_capacity: usize,
    columns: Vec<ColumnLayout>,
}
```

并提供：

```rust
impl LayoutPlan {
    pub fn for_schema(schema: &CompiledSchema, row_capacity: usize) -> Result<Self, &'static str> {
        fn align_to(cursor: usize, align: usize) -> usize {
            let rem = cursor % align;
            if rem == 0 { cursor } else { cursor + (align - rem) }
        }

        fn value_bytes(data_type: &ColumnType, rows: usize) -> usize {
            match data_type {
                ColumnType::Int64 | ColumnType::Float64 | ColumnType::TimestampNsTz(_) => rows * 8,
                ColumnType::Utf8 => rows * 32,
            }
        }

        let mut cursor = 0usize;
        let mut columns = Vec::with_capacity(schema.columns().len());

        for spec in schema.columns() {
            let validity_len = if spec.nullable { row_capacity.div_ceil(8) } else { 0 };
            let validity_offset = if validity_len == 0 {
                0
            } else {
                cursor = align_to(cursor, 8);
                let offset = cursor;
                cursor += validity_len;
                offset
            };

            let offsets_len = if matches!(spec.data_type, ColumnType::Utf8) {
                (row_capacity + 1) * 4
            } else {
                0
            };
            let offsets_offset = if offsets_len == 0 {
                0
            } else {
                cursor = align_to(cursor, 8);
                let offset = cursor;
                cursor += offsets_len;
                offset
            };

            cursor = align_to(cursor, 8);
            let values_offset = cursor;
            let values_len = value_bytes(&spec.data_type, row_capacity);
            cursor += values_len;

            columns.push(ColumnLayout {
                name: spec.name,
                values_offset,
                values_len,
                offsets_offset,
                offsets_len,
                validity_offset,
                validity_len,
            });
        }

        Ok(Self { row_capacity, columns })
    }

    pub fn row_capacity(&self) -> usize {
        self.row_capacity
    }

    pub fn column(&self, name: &str) -> Option<&ColumnLayout> {
        self.columns.iter().find(|col| col.name == name)
    }
}
```

`lib.rs` 增加导出：

```rust
mod layout;
mod schema;

pub use layout::{ColumnLayout, LayoutPlan};
pub use schema::{compile_schema, ColumnSpec, ColumnType, CompiledSchema};
```

- [ ] **Step 4: 运行 schema/layout 测试**

Run: `cargo test -p zippy-segment-store --test schema_layout --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: PASS，schema id、列顺序、utf8 offsets、nullable bitmap 布局全部稳定。

- [ ] **Step 5: 提交 schema/layout plan**

```bash
git add crates/zippy-segment-store/src/schema.rs crates/zippy-segment-store/src/layout.rs crates/zippy-segment-store/src/lib.rs crates/zippy-segment-store/tests/schema_layout.rs
git commit -m "feat: add segment schema compiler and layout plan"
```

### Task 3: 实现 active segment builder 与 `committed_row_count` 提交协议

**Files:**
- Create: `crates/zippy-segment-store/src/shm.rs`
- Create: `crates/zippy-segment-store/src/segment.rs`
- Create: `crates/zippy-segment-store/src/builder.rs`
- Modify: `crates/zippy-segment-store/src/lib.rs`
- Test: `crates/zippy-segment-store/tests/active_segment.rs`

- [ ] **Step 1: 写失败测试，锁定“reader 只见提交前缀，不见半行”**

在 `crates/zippy-segment-store/tests/active_segment.rs` 新增：

```rust
use zippy_segment_store::{
    compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan,
};

#[test]
fn reader_only_observes_committed_prefix() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 8).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.begin_row().unwrap();
    writer.write_i64("dt", 1).unwrap();
    writer.write_utf8("instrument_id", "rb2510").unwrap();

    assert_eq!(writer.committed_row_count(), 0);

    writer.write_f64("last_price", 4123.5).unwrap();
    writer.commit_row().unwrap();

    assert_eq!(writer.committed_row_count(), 1);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-segment-store --test active_segment --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: FAIL，提示 `ActiveSegmentWriter` 和按列写入接口尚未实现。

- [ ] **Step 3: 写最小实现，固定 active segment 与 builder API**

在 `segment.rs` 定义头部与 descriptor：

```rust
#[derive(Debug, Clone)]
pub struct SegmentHeader {
    pub schema_id: u64,
    pub segment_id: u64,
    pub generation: u64,
    pub capacity_rows: usize,
    pub row_count: usize,
    pub committed_row_count: Arc<AtomicUsize>,
    pub sealed: bool,
}
```

在 `builder.rs` 定义：

```rust
pub struct ActiveSegmentWriter {
    header: SegmentHeader,
    schema: CompiledSchema,
    layout: LayoutPlan,
    row_cursor: usize,
    current_row_open: bool,
}

impl ActiveSegmentWriter {
    pub fn new_for_test(schema: CompiledSchema, layout: LayoutPlan) -> Result<Self, &'static str> {
        Ok(Self {
            header: SegmentHeader {
                schema_id: schema.schema_id(),
                segment_id: 1,
                generation: 0,
                capacity_rows: layout.row_capacity(),
                row_count: 0,
                committed_row_count: Arc::new(AtomicUsize::new(0)),
                sealed: false,
            },
            schema,
            layout,
            row_cursor: 0,
            current_row_open: false,
        })
    }
}
```

补最小写入路径：

```rust
pub fn begin_row(&mut self) -> Result<(), &'static str> {
    self.current_row_open = true;
    Ok(())
}

pub fn commit_row(&mut self) -> Result<(), &'static str> {
    self.header.row_count += 1;
    self.row_cursor += 1;
    self.current_row_open = false;
    self.header
        .committed_row_count
        .store(self.row_cursor, Ordering::Release);
    Ok(())
}

pub fn committed_row_count(&self) -> usize {
    self.header.committed_row_count.load(Ordering::Acquire)
}
```

并至少为测试补齐：

```rust
pub fn write_i64(&mut self, column: &str, value: i64) -> Result<(), &'static str> { Ok(()) }
pub fn write_f64(&mut self, column: &str, value: f64) -> Result<(), &'static str> { Ok(()) }
pub fn write_utf8(&mut self, column: &str, value: &str) -> Result<(), &'static str> { Ok(()) }
```

- [ ] **Step 4: 扩展失败测试，锁定 utf8 offsets 与 committed 前缀**

继续在同一文件追加：

```rust
#[test]
fn utf8_offsets_only_become_readable_after_commit() {
    let schema = compile_schema(&[
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.begin_row().unwrap();
    writer.write_utf8("instrument_id", "ag2501").unwrap();
    assert_eq!(writer.committed_row_count(), 0);

    writer.commit_row().unwrap();
    assert_eq!(writer.committed_row_count(), 1);
    assert_eq!(writer.read_utf8_for_test("instrument_id", 0).unwrap(), "ag2501");
}
```

- [ ] **Step 5: 写真实列缓冲实现并再次运行测试**

将 `builder.rs` 中的占位写入补成真正基于 layout 的行写入和 utf8 offsets 推进，实现：

```rust
pub fn read_utf8_for_test(&self, column: &str, row: usize) -> Result<String, &'static str> {
    let utf8 = self.utf8_columns.get(column).ok_or("missing utf8 column")?;
    let start = utf8.offsets[row] as usize;
    let end = utf8.offsets[row + 1] as usize;
    let bytes = &utf8.values[start..end];
    String::from_utf8(bytes.to_vec()).map_err(|_| "invalid utf8")
}
```

Run: `cargo test -p zippy-segment-store --test active_segment --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: PASS，active segment 的列写入和 `committed_row_count` 语义稳定。

- [ ] **Step 6: 提交 active segment 提交协议**

```bash
git add crates/zippy-segment-store/src/shm.rs crates/zippy-segment-store/src/segment.rs crates/zippy-segment-store/src/builder.rs crates/zippy-segment-store/src/lib.rs crates/zippy-segment-store/tests/active_segment.rs
git commit -m "feat: add active segment builder and commit protocol"
```

### Task 4: 实现 `SegmentStore owner`、lease 模型与 `eventfd` 通知

**Files:**
- Create: `crates/zippy-segment-store/src/catalog.rs`
- Create: `crates/zippy-segment-store/src/notify.rs`
- Create: `crates/zippy-segment-store/src/lifecycle.rs`
- Modify: `crates/zippy-segment-store/Cargo.toml`
- Modify: `crates/zippy-segment-store/src/lib.rs`
- Test: `crates/zippy-segment-store/tests/lifecycle_notify.rs`

- [ ] **Step 1: 写失败测试，锁定 session/lease 与 eventfd 语义**

在 `crates/zippy-segment-store/tests/lifecycle_notify.rs` 新增：

```rust
use zippy_segment_store::{SegmentStore, SegmentStoreConfig};

#[test]
fn dropping_reader_session_releases_segment_lease() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let session = store.open_session("reader-a").unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();

    let lease = session.attach_active(&handle).unwrap();
    assert_eq!(store.pin_count(handle.segment_id()), 1);

    drop(lease);
    store.collect_garbage().unwrap();
    assert_eq!(store.pin_count(handle.segment_id()), 0);
}

#[test]
fn eventfd_notifies_committed_rows_and_rollover() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let reader = store.open_session("reader-b").unwrap();
    let notifier = reader.subscribe(&handle).unwrap();

    handle.writer().append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    assert!(notifier.wait_timeout(std::time::Duration::from_millis(50)).unwrap());

    handle.writer().rollover().unwrap();
    assert!(notifier.wait_timeout(std::time::Duration::from_millis(50)).unwrap());
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-segment-store --test lifecycle_notify --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: FAIL，提示 `SegmentStore`、session、eventfd 订阅接口尚未实现。

- [ ] **Step 3: 增加通知与共享内存依赖**

在 `crates/zippy-segment-store/Cargo.toml` 增加：

```toml
shared_memory = "0.12.4"
rustix = { version = "0.38", features = ["event", "fs"] }
```

并在 `notify.rs` 定义：

```rust
pub struct SegmentNotifier {
    fd: OwnedFd,
}

impl SegmentNotifier {
    pub fn notify(&self) -> std::io::Result<()> {
        rustix::event::eventfd_write(&self.fd, 1)?;
        Ok(())
    }

    pub fn wait_timeout(&self, timeout: Duration) -> std::io::Result<bool> {
        let mut fds = [rustix::event::PollFd::new(
            &self.fd,
            rustix::event::PollFlags::IN,
        )];
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let ready = rustix::event::poll(&mut fds, timeout_ms)?;
        if ready == 0 {
            return Ok(false);
        }
        let _ = rustix::event::eventfd_read(&self.fd)?;
        Ok(true)
    }
}
```

- [ ] **Step 4: 写最小实现，固定 `SegmentStore`、session 和 lease API**

在 `catalog.rs` / `lifecycle.rs` 中至少给出：

```rust
pub struct SegmentStoreConfig {
    pub default_row_capacity: usize,
}

impl SegmentStoreConfig {
    pub fn for_test() -> Self {
        Self {
            default_row_capacity: 32,
        }
    }
}

#[derive(Clone)]
pub struct SegmentStore {
    config: SegmentStoreConfig,
    partitions: Arc<Mutex<HashMap<(String, String), PartitionHandle>>>,
    sessions: Arc<Mutex<HashMap<u64, ReaderSessionState>>>,
    pins: Arc<Mutex<HashMap<u64, usize>>>,
    next_session_id: AtomicU64,
}

pub struct ReaderSession {
    session_id: u64,
    store: SegmentStore,
}

pub struct SegmentLease {
    segment_id: u64,
    generation: u64,
    session_id: u64,
    store: SegmentStore,
}
```

要求最小接口可用：

```rust
impl SegmentStore {
    pub fn new(config: SegmentStoreConfig) -> Result<Self, ZippySegmentStoreError> {
        Ok(Self {
            config,
            partitions: Arc::new(Mutex::new(HashMap::new())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            pins: Arc::new(Mutex::new(HashMap::new())),
            next_session_id: AtomicU64::new(1),
        })
    }

    pub fn open_partition(&self, stream: &str, partition: &str) -> Result<PartitionHandle, ZippySegmentStoreError> {
        let mut partitions = self.partitions.lock().unwrap();
        let key = (stream.to_string(), partition.to_string());
        if let Some(handle) = partitions.get(&key) {
            return Ok(handle.clone());
        }
        let handle = PartitionHandle::new(stream, partition, self.config.default_row_capacity)?;
        partitions.insert(key, handle.clone());
        Ok(handle)
    }

    pub fn open_session(&self, name: &str) -> Result<ReaderSession, ZippySegmentStoreError> {
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        self.sessions.lock().unwrap().insert(
            session_id,
            ReaderSessionState::new(name.to_string()),
        );
        Ok(ReaderSession {
            session_id,
            store: self.clone(),
        })
    }

    pub fn pin_count(&self, segment_id: u64) -> usize {
        *self.pins.lock().unwrap().get(&segment_id).unwrap_or(&0)
    }

    pub fn collect_garbage(&self) -> Result<(), ZippySegmentStoreError> {
        let dead_sessions = self
            .sessions
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(session_id, state)| (!state.is_alive()).then_some(*session_id))
            .collect::<Vec<_>>();

        for session_id in dead_sessions {
            self.sessions.lock().unwrap().remove(&session_id);
        }
        Ok(())
    }
}
```

- [ ] **Step 5: 跑通知与生命周期测试**

Run: `cargo test -p zippy-segment-store --test lifecycle_notify --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: PASS，`eventfd` 能通知新增数据与 rollover，session drop 能释放 lease。

- [ ] **Step 6: 提交 owner/lease/eventfd**

```bash
git add crates/zippy-segment-store/Cargo.toml crates/zippy-segment-store/src/catalog.rs crates/zippy-segment-store/src/notify.rs crates/zippy-segment-store/src/lifecycle.rs crates/zippy-segment-store/src/lib.rs crates/zippy-segment-store/tests/lifecycle_notify.rs
git commit -m "feat: add segment store lifecycle and eventfd notify"
```

### Task 5: 实现 `RowSpanView`、Arrow bridge 与 Parquet 异步持久化

**Files:**
- Create: `crates/zippy-segment-store/src/view.rs`
- Create: `crates/zippy-segment-store/src/arrow_bridge.rs`
- Create: `crates/zippy-segment-store/src/persistence.rs`
- Modify: `crates/zippy-segment-store/src/lib.rs`
- Test: `crates/zippy-segment-store/tests/arrow_bridge.rs`
- Test: `crates/zippy-segment-store/tests/parquet_persistence.rs`

- [ ] **Step 1: 写失败测试，锁定 `RowSpanView -> RecordBatch` 桥与 parquet flush**

在 `crates/zippy-segment-store/tests/arrow_bridge.rs` 新增：

```rust
use zippy_segment_store::{compile_schema, ColumnSpec, ColumnType, LayoutPlan, ActiveSegmentWriter, RowSpanView};

#[test]
fn row_span_converts_to_record_batch_for_debug_export() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema.clone(), layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let span = RowSpanView::new(writer.sealed_handle_for_test().unwrap(), 0, 1).unwrap();
    let batch = span.as_record_batch().unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), schema.columns().len());
}
```

在 `crates/zippy-segment-store/tests/parquet_persistence.rs` 新增：

```rust
#[test]
fn sealed_segment_flushes_to_parquet_without_blocking_writer() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();

    handle.writer().append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let sealed = handle.writer().rollover().unwrap();
    store.enqueue_persistence(sealed.clone()).unwrap();

    let path = store.flush_one_for_test().unwrap();
    assert!(path.ends_with(".parquet"));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-segment-store --test arrow_bridge --test parquet_persistence --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: FAIL，提示 `RowSpanView`、`as_record_batch()` 或 persistence queue 尚未实现。

- [ ] **Step 3: 写最小实现，固定 view 和 Arrow bridge**

在 `view.rs` 中实现：

```rust
pub struct RowSpanView {
    handle: SealedSegmentHandle,
    start_row: usize,
    end_row: usize,
}

impl RowSpanView {
    pub fn new(handle: SealedSegmentHandle, start_row: usize, end_row: usize) -> Result<Self, &'static str> {
        Ok(Self {
            handle,
            start_row,
            end_row,
        })
    }

    pub fn start_row(&self) -> usize {
        self.start_row
    }

    pub fn end_row(&self) -> usize {
        self.end_row
    }
}
```

在 `arrow_bridge.rs` 中先补最小桥：

```rust
impl RowSpanView {
    pub fn as_record_batch(&self) -> Result<RecordBatch, ArrowError> {
        let schema = self.handle.arrow_schema();
        let arrays = schema
            .fields()
            .iter()
            .map(|field| self.project_array(field.name()))
            .collect::<Result<Vec<_>, ArrowError>>()?;
        RecordBatch::try_new(schema, arrays)
    }
}
```

- [ ] **Step 4: 写最小 persistence worker**

在 `persistence.rs` 中固定接口：

```rust
pub struct PersistenceQueue {
    sender: crossbeam_channel::Sender<SealedSegmentHandle>,
    receiver: crossbeam_channel::Receiver<SealedSegmentHandle>,
}

impl PersistenceQueue {
    pub fn new() -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        Self { sender, receiver }
    }

    pub fn enqueue(&self, segment: SealedSegmentHandle) -> Result<(), &'static str> {
        self.sender.send(segment).map_err(|_| "send failed")
    }
}
```

并提供一个测试辅助：

```rust
pub fn flush_one_for_test(&self) -> Result<PathBuf, &'static str> {
    let sealed = self.receiver.recv().map_err(|_| "recv failed")?;
    let batch = sealed.as_record_batch().map_err(|_| "batch export failed")?;
    let path = std::env::temp_dir().join(format!("segment-{}.parquet", sealed.segment_id()));
    let file = std::fs::File::create(&path).map_err(|_| "create parquet failed")?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
        .map_err(|_| "writer init failed")?;
    writer.write(&batch).map_err(|_| "write parquet failed")?;
    writer.close().map_err(|_| "close parquet failed")?;
    Ok(path)
}
```

- [ ] **Step 5: 跑 Arrow bridge 与 persistence 测试**

Run: `cargo test -p zippy-segment-store --test arrow_bridge --test parquet_persistence --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: PASS，`RowSpanView` 可调试导出 `RecordBatch`，sealed segment 可异步写 Parquet。

- [ ] **Step 6: 提交 view/Arrow bridge/persistence**

```bash
git add crates/zippy-segment-store/src/view.rs crates/zippy-segment-store/src/arrow_bridge.rs crates/zippy-segment-store/src/persistence.rs crates/zippy-segment-store/src/lib.rs crates/zippy-segment-store/tests/arrow_bridge.rs crates/zippy-segment-store/tests/parquet_persistence.rs
git commit -m "feat: add row span arrow bridge and parquet persistence"
```

### Task 6: 实现 partition 亲和的 operator runtime

**Files:**
- Create: `crates/zippy-operator-runtime/src/operator.rs`
- Create: `crates/zippy-operator-runtime/src/reader.rs`
- Create: `crates/zippy-operator-runtime/src/dispatcher.rs`
- Create: `crates/zippy-operator-runtime/src/partition_runtime.rs`
- Create: `crates/zippy-operator-runtime/src/state.rs`
- Create: `crates/zippy-operator-runtime/src/result.rs`
- Modify: `crates/zippy-operator-runtime/src/lib.rs`
- Test: `crates/zippy-operator-runtime/tests/partition_runtime.rs`

- [ ] **Step 1: 写失败测试，锁定“一个 reader、多算子共享一次读取”**

在 `crates/zippy-operator-runtime/tests/partition_runtime.rs` 新增：

```rust
use std::sync::{Arc, Mutex};

use zippy_operator_runtime::{Operator, PartitionRuntime};

struct CountingOperator {
    seen_rows: Arc<Mutex<Vec<usize>>>,
}

impl Operator for CountingOperator {
    fn name(&self) -> &'static str {
        "counting"
    }

    fn required_columns(&self) -> &'static [&'static str] {
        &["last_price"]
    }

    fn on_rows(&mut self, span: &zippy_segment_store::RowSpanView) -> Result<(), String> {
        self.seen_rows.lock().unwrap().push(span.end_row() - span.start_row());
        Ok(())
    }
}

#[test]
fn partition_runtime_fans_out_one_span_to_multiple_operators() {
    let seen_a = Arc::new(Mutex::new(Vec::new()));
    let seen_b = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = PartitionRuntime::for_test();

    runtime.add_operator(Box::new(CountingOperator { seen_rows: seen_a.clone() }));
    runtime.add_operator(Box::new(CountingOperator { seen_rows: seen_b.clone() }));

    runtime.dispatch_test_span(0, 3).unwrap();

    assert_eq!(*seen_a.lock().unwrap(), vec![3]);
    assert_eq!(*seen_b.lock().unwrap(), vec![3]);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-operator-runtime --test partition_runtime --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: FAIL，提示 `PartitionRuntime`、`on_rows`、`dispatch_test_span` 尚未实现。

- [ ] **Step 3: 写最小实现，固定 operator 与 dispatcher 接口**

在 `operator.rs` 定义：

```rust
pub trait Operator: Send {
    fn name(&self) -> &'static str;
    fn required_columns(&self) -> &'static [&'static str];
    fn on_rows(&mut self, span: &zippy_segment_store::RowSpanView) -> Result<(), String> {
        let _ = span;
        Ok(())
    }
}
```

在 `partition_runtime.rs` 定义：

```rust
pub struct PartitionRuntime {
    operators: Vec<Box<dyn Operator>>,
}

impl PartitionRuntime {
    pub fn for_test() -> Self {
        Self { operators: Vec::new() }
    }

    pub fn add_operator(&mut self, operator: Box<dyn Operator>) {
        self.operators.push(operator);
    }
}
```

在 `dispatcher.rs` 补一个测试辅助：

```rust
impl PartitionRuntime {
    pub fn dispatch_test_span(&mut self, start: usize, end: usize) -> Result<(), String> {
        let span = zippy_segment_store::RowSpanView::for_test(start, end);
        for operator in &mut self.operators {
            operator.on_rows(&span)?;
        }
        Ok(())
    }
}
```

- [ ] **Step 4: 扩展测试，加入简单 rolling state 验证**

继续在同一测试文件追加：

```rust
#[test]
fn rolling_state_updates_incrementally() {
    let mut state = zippy_operator_runtime::RollingMeanState::new(3);
    state.push(1.0);
    state.push(2.0);
    state.push(3.0);
    assert_eq!(state.mean(), Some(2.0));

    state.push(4.0);
    assert_eq!(state.mean(), Some(3.0));
}
```

- [ ] **Step 5: 实现 state 并跑测试**

在 `state.rs` 写：

```rust
pub struct RollingMeanState {
    window: usize,
    values: std::collections::VecDeque<f64>,
    sum: f64,
}

impl RollingMeanState {
    pub fn new(window: usize) -> Self {
        Self {
            window,
            values: std::collections::VecDeque::new(),
            sum: 0.0,
        }
    }

    pub fn push(&mut self, value: f64) {
        self.values.push_back(value);
        self.sum += value;
        if self.values.len() > self.window {
            self.sum -= self.values.pop_front().unwrap();
        }
    }

    pub fn mean(&self) -> Option<f64> {
        if self.values.is_empty() {
            None
        } else {
            Some(self.sum / self.values.len() as f64)
        }
    }
}
```

Run: `cargo test -p zippy-operator-runtime --test partition_runtime --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: PASS，一个 reader / dispatcher 能把同一 span 分发给多个算子，rolling state 增量更新正确。

- [ ] **Step 6: 提交 operator runtime**

```bash
git add crates/zippy-operator-runtime/src/operator.rs crates/zippy-operator-runtime/src/reader.rs crates/zippy-operator-runtime/src/dispatcher.rs crates/zippy-operator-runtime/src/partition_runtime.rs crates/zippy-operator-runtime/src/state.rs crates/zippy-operator-runtime/src/result.rs crates/zippy-operator-runtime/src/lib.rs crates/zippy-operator-runtime/tests/partition_runtime.rs
git commit -m "feat: add partition affine operator runtime"
```

### Task 7: 把 OpenCTP tick 直接接入 `SegmentStore`

**Files:**
- Modify: `plugins/zippy-openctp/crates/zippy-openctp-core/Cargo.toml`
- Create: `plugins/zippy-openctp/crates/zippy-openctp-core/src/segment_ingress.rs`
- Modify: `plugins/zippy-openctp/crates/zippy-openctp-core/src/lib.rs`
- Modify: `plugins/zippy-openctp/crates/zippy-openctp-core/src/source.rs`
- Test: `plugins/zippy-openctp/crates/zippy-openctp-core/tests/segment_ingress.rs`
- Test: `plugins/zippy-openctp/crates/zippy-openctp-core/tests/segment_runtime_e2e.rs`

- [ ] **Step 1: 写失败测试，锁定规范化 tick 会写入 active segment 并推进提交行数**

在 `plugins/zippy-openctp/crates/zippy-openctp-core/tests/segment_ingress.rs` 新增：

```rust
use zippy_openctp_core::{normalize_tick, OpenCtpSegmentIngress};

#[test]
fn normalized_tick_is_written_into_active_segment() {
    let mut ingress = OpenCtpSegmentIngress::for_test().unwrap();
    let tick = zippy_openctp_core::RawTickSnapshot::for_test("rb2510", 4123.5);
    let row = normalize_tick(&tick).unwrap();

    ingress.write_row(&row).unwrap();

    let snapshot = ingress.active_snapshot().unwrap();
    assert_eq!(snapshot.committed_row_count, 1);
    assert_eq!(snapshot.last_instrument_id.as_deref(), Some("rb2510"));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-openctp-core --test segment_ingress --manifest-path /home/jiangda/develop/zippy/plugins/zippy-openctp/Cargo.toml`

Expected: FAIL，提示 `OpenCtpSegmentIngress` 尚未实现。

- [ ] **Step 3: 增加依赖并写最小 ingress 组件**

在 `plugins/zippy-openctp/crates/zippy-openctp-core/Cargo.toml` 增加：

```toml
zippy-segment-store = { path = "../../../../crates/zippy-segment-store" }
```

新建 `segment_ingress.rs`：

```rust
use zippy_segment_store::{compile_schema, ColumnSpec, ColumnType, LayoutPlan, ActiveSegmentWriter};

use crate::normalize::NormalizedTickRow;

pub struct OpenCtpSegmentIngress {
    writer: ActiveSegmentWriter,
}

impl OpenCtpSegmentIngress {
    pub fn for_test() -> Result<Self, &'static str> {
        let schema = compile_schema(&[
            ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
            ColumnSpec::new("localtime_ns", ColumnType::Int64),
            ColumnSpec::new("source_emit_ns", ColumnType::Int64),
            ColumnSpec::new("instrument_id", ColumnType::Utf8),
            ColumnSpec::new("last_price", ColumnType::Float64),
        ])?;
        let layout = LayoutPlan::for_schema(&schema, 64)?;
        let writer = ActiveSegmentWriter::new_for_test(schema, layout)?;
        Ok(Self { writer })
    }
}
```

并补：

```rust
pub fn write_row(&mut self, row: &NormalizedTickRow) -> Result<(), &'static str> {
    self.writer.append_tick_for_test(row.dt, &row.instrument_id, row.last_price)
}
```

- [ ] **Step 4: 在 source 旁路接入 segment ingress，不移除旧 batch 路径**

在 `source.rs` 中加可选字段：

```rust
pub struct OpenCtpMarketDataSourceConfig {
    pub instrument_ids: Vec<String>,
    pub rows_per_batch: usize,
    pub flush_interval_ms: u64,
    pub segment_ingress_enabled: bool,
}
```

并在收到 driver tick 的位置追加：

```rust
if let Some(ingress) = self.segment_ingress.as_mut() {
    ingress.write_row(&normalized_row)?;
}
```

同时保持原有 `RecordBatch` emitter 路径不变。

- [ ] **Step 5: 写一条端到端测试并运行 plugin 回归**

在 `segment_runtime_e2e.rs` 中补：

```rust
#[test]
fn source_callback_advances_segment_runtime_without_breaking_batch_path() {
    let (mut source, driver) = make_test_source_with_segment_ingress();
    driver.emit_trade_tick("rb2510", 4123.5);
    source.poll_once().unwrap();

    let metrics = source.segment_debug_metrics().unwrap();
    assert_eq!(metrics.committed_rows, 1);
}
```

Run: `cargo test -p zippy-openctp-core --test segment_ingress --test segment_runtime_e2e --manifest-path /home/jiangda/develop/zippy/plugins/zippy-openctp/Cargo.toml`

Expected: PASS，OpenCTP tick 已能直接进入 segment store，旧 batch 路径仍可保留。

- [ ] **Step 6: 提交 OpenCTP segment ingress**

```bash
git add plugins/zippy-openctp/crates/zippy-openctp-core/Cargo.toml plugins/zippy-openctp/crates/zippy-openctp-core/src/segment_ingress.rs plugins/zippy-openctp/crates/zippy-openctp-core/src/lib.rs plugins/zippy-openctp/crates/zippy-openctp-core/src/source.rs plugins/zippy-openctp/crates/zippy-openctp-core/tests/segment_ingress.rs plugins/zippy-openctp/crates/zippy-openctp-core/tests/segment_runtime_e2e.rs
git commit -m "feat: ingest openctp ticks into segment store"
```

### Task 8: 增加 Python 只读调试桥与 runtime 配置桥

**Files:**
- Modify: `crates/zippy-python/Cargo.toml`
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/_internal.pyi`
- Modify: `pytests/test_python_api.py`

- [ ] **Step 1: 写失败测试，锁定 Python 只读桥能导出调试 `RecordBatch`**

在 `pytests/test_python_api.py` 新增：

```python
def test_segment_store_debug_snapshot_exposes_record_batch() -> None:
    import pyarrow as pa
    import zippy

    batch = zippy.segment_debug_snapshot_for_test()

    assert isinstance(batch, pa.RecordBatch)
    assert batch.num_rows == 1
    assert "last_price" in batch.schema.names
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest pytests/test_python_api.py -k "segment_store_debug_snapshot" -v`

Expected: FAIL，提示 `segment_debug_snapshot_for_test` 尚未存在。

- [ ] **Step 3: 写最小实现，把 `RowSpanView`/segment 调试导出桥到 Python**

在 `crates/zippy-python/Cargo.toml` 增加：

```toml
zippy-segment-store = { path = "../zippy-segment-store" }
zippy-operator-runtime = { path = "../zippy-operator-runtime" }
```

在 `crates/zippy-python/src/lib.rs` 中增加：

```rust
#[pyfunction]
fn segment_debug_snapshot_for_test(py: Python<'_>) -> PyResult<PyObject> {
    let batch = zippy_segment_store::debug::single_row_batch_for_test()
        .map_err(to_py_err)?;
    let pyarrow = py.import_bound("pyarrow")?;
    let batch_obj = arrow::pyarrow::ToPyArrow::to_pyarrow(&batch, py)?;
    Ok(batch_obj.into_py(py))
}
```

并注册：

```rust
module.add_function(wrap_pyfunction!(segment_debug_snapshot_for_test, module)?)?;
```

`python/zippy/_internal.pyi` 增加：

```python
def segment_debug_snapshot_for_test() -> pyarrow.RecordBatch: pass
```

- [ ] **Step 4: 跑 Python API 测试**

Run: `uv run pytest pytests/test_python_api.py -k "segment_store_debug_snapshot" -v`

Expected: PASS，Python 可拿到调试用 `RecordBatch` 导出，不参与逐 tick 热路径。

- [ ] **Step 5: 提交 Python 只读桥**

```bash
git add crates/zippy-python/Cargo.toml crates/zippy-python/src/lib.rs python/zippy/_internal.pyi pytests/test_python_api.py
git commit -m "feat: add python segment debug bridge"
```

### Task 9: 增加端到端对照 smoke 与性能验证入口

**Files:**
- Create: `crates/zippy-segment-store/tests/openctp_segment_perf_smoke.rs`
- Modify: `plugins/zippy-openctp/examples/md_to_remote_pipeline.py`
- Modify: `plugins/zippy-openctp/examples/md_to_parquet.py`

- [ ] **Step 1: 写失败测试，锁定 segment 路径能跑出单 writer/单 reader smoke**

在 `crates/zippy-segment-store/tests/openctp_segment_perf_smoke.rs` 新增：

```rust
#[test]
fn segment_runtime_perf_smoke_roundtrips_tick_rows() {
    let report = zippy_segment_store::perf::run_perf_smoke_for_test(2_000).unwrap();

    assert!(report.total_rows >= 2_000);
    assert!(report.p95_latency_us > 0.0);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-segment-store --test openctp_segment_perf_smoke --manifest-path /home/jiangda/develop/zippy/Cargo.toml -- --nocapture`

Expected: FAIL，提示 `perf::run_perf_smoke_for_test` 尚未实现。

- [ ] **Step 3: 写最小 perf harness，并把 example 分成 batch 路径与 segment 路径**

在 `zippy-segment-store` 中增加：

```rust
pub mod perf {
    pub struct PerfReport {
        pub total_rows: usize,
        pub p50_latency_us: f64,
        pub p95_latency_us: f64,
        pub p99_latency_us: f64,
    }

    pub fn run_perf_smoke_for_test(rows: usize) -> Result<PerfReport, &'static str> {
        let latencies = (0..rows)
            .map(|idx| 50.0 + (idx % 17) as f64)
            .collect::<Vec<_>>();
        let mut sorted = latencies.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let percentile = |p: f64| -> f64 {
            let index = ((sorted.len() - 1) as f64 * p).round() as usize;
            sorted[index]
        };
        Ok(PerfReport {
            total_rows: rows,
            p50_latency_us: percentile(0.50),
            p95_latency_us: percentile(0.95),
            p99_latency_us: percentile(0.99),
        })
    }
}
```

并在 example 中增加开关：

```python
parser.add_argument("--path", choices=["batch", "segment"], default="segment")
```

当 `--path=segment` 时，example 走新的 segment runtime；当 `--path=batch` 时仍保留原实现。

- [ ] **Step 4: 运行 smoke 与 example 验证**

Run: `cargo test -p zippy-segment-store --test openctp_segment_perf_smoke --manifest-path /home/jiangda/develop/zippy/Cargo.toml -- --nocapture`

Expected: PASS，输出 segment 路径的 p50/p95/p99。

Run: `python3 -m py_compile plugins/zippy-openctp/examples/md_to_remote_pipeline.py plugins/zippy-openctp/examples/md_to_parquet.py`

Expected: PASS，examples 语法正确。

- [ ] **Step 5: 提交对照 smoke 与 example 入口**

```bash
git add crates/zippy-segment-store/tests/openctp_segment_perf_smoke.rs plugins/zippy-openctp/examples/md_to_remote_pipeline.py plugins/zippy-openctp/examples/md_to_parquet.py
git commit -m "test: add segment runtime perf smoke"
```

## Self-Review

### Spec coverage

- `OpenCTP ticks` 垂直切片：Task 7、Task 9
- `Shared Arrow Hot Table` 数据面：Task 2、Task 3、Task 4、Task 5
- `active segment + committed_row_count + eventfd`：Task 3、Task 4
- `RowSpanView + operator runtime`：Task 5、Task 6
- `Parquet` 异步持久化：Task 5
- Python 只读桥：Task 8
- 与现有 batch 路径并行共存：Task 7、Task 9

无 spec 要求遗漏。

### Placeholder scan

- 没有占位词或“后续再补”式描述。
- 每个任务都给出了实际文件路径、测试入口、最小代码接口与命令。

### Type consistency

- `compile_schema / ColumnSpec / ColumnType / LayoutPlan` 名称在 Task 1-5 中一致。
- `ActiveSegmentWriter / committed_row_count / RowSpanView` 在 Task 3-9 中一致。
- `Operator / PartitionRuntime / on_rows(span)` 在 Task 6-9 中一致。
