# Active Payload Version Boundary Design

日期：2026-05-13

## 背景

`KeyValueTableMaterializer` 的 snapshot churn 优化暴露了一个更底层的问题：active segment
当前只有 committed-prefix 边界，没有 payload 版本边界。

现在的协议是：

- writer 写 row payload 时直接 mirror 到共享内存。
- writer 在 `publish_committed_prefix()` 中发布 `row_count` 和 `committed_row_count`。
- reader attach active segment 时校验 header，再读取 `committed_row_count` 作为可见行数上界。
- `RowSpanView`、`arrow_bridge` 和 `cell_value()` 后续读取 payload 时不再校验同一个 payload 版本。

这个协议对 append-only committed prefix 是足够的：writer 不会改写已经 committed 的旧行 payload。
但它不支持同 active segment snapshot rewrite、row-slot overwrite 或任何会改写 committed payload 的能力。

## 问题

如果 writer 在同一个 active segment 内修改已经 committed 的 payload，reader 可能看到跨版本混合：

- 先读到旧 `committed_row_count`。
- writer 开始改写 validity、fixed-width values、utf8 offsets/value bytes。
- reader 按旧行数读取 payload，却读到部分旧数据、部分新数据。

这类错误不会必然触发越界。最危险的情况是 reader 得到一张形状合法但内容不一致的 batch。

## 目标

- 为 active segment payload 提供显式版本边界。
- 让 reader 能证明一次 active payload 读取来自同一个 stable version。
- 为后续 KeyValue snapshot rewrite 和 row-slot overwrite 提供安全前置原语。
- 保持 append-only 写入的低成本路径。
- 保持旧 descriptor / 旧 layout 的兼容边界清晰。

## 非目标

- 不在本专项实现 KeyValue snapshot rewrite。
- 不改变 sealed segment 的内存模型。
- 不改变 Python / Gateway 查询 API。
- 不引入 reader-side latest-by-key collapse。
- 不在第一阶段优化全量 snapshot materialization。

## 设计选项

### 方案 A：active payload seqlock（推荐）

在 active segment header 中新增 `payload_version`：

- even version 表示 payload 稳定。
- odd version 表示 writer 正在改写 payload。
- reader 读取 active payload 前后各读一次 version。
- 如果 version 为 odd 或前后不一致，reader retry。

优点：

- 直接表达“这次 payload 读取是否来自同一个版本”。
- 不需要复制整块 payload。
- 可以同时覆盖 snapshot rewrite 和 row-slot overwrite。
- 和现有 shared-memory header 模型一致。

缺点：

- 需要 layout version 迁移。
- active reader 的 batch materialization 需要 retry wrapper。
- 高频 writer 下 reader 可能 retry，需要上限和错误语义。

### 方案 B：double-buffer payload

active segment 分成两个 payload buffer：

- writer 写 inactive buffer。
- 写完后原子切换 active buffer index。
- reader 根据 buffer index 读取完整 payload。

优点：

- reader 不需要复制后再验证，逻辑直观。
- writer 失败不会污染当前 active buffer。

缺点：

- shared memory 占用接近翻倍。
- `LayoutPlan`、payload offset、descriptor contract 都要变复杂。
- append-only 场景也要承担更重布局。

### 方案 C：descriptor-only RCU

继续通过新 active segment + descriptor publish 表达每个 snapshot version。

优点：

- 已经是当前安全机制。
- 不需要改 reader。

缺点：

- 不能解决 KeyValue 高频小更新的 segment churn。
- 不能支持 row-slot overwrite。

## 采用方案

采用方案 A：active payload seqlock。

这个方案把一致性边界放在 segment-store，而不是让 engines 自己猜测 active shm 是否可安全改写。
只有 segment-store 提供稳定版本读取后，engines 才能使用同 active segment rewrite。

## Header Layout

当前 header 使用 `0..128` 作为固定区域：

```text
0   schema_id
8   segment_id
16  generation
24  capacity_rows
32  row_count
40  committed_row_count
48  sealed
52  notify_seq
56  magic
60  layout_version
64  writer_epoch
72  descriptor_generation
80  waiter_count
128 payload_start
```

新增：

```text
88 payload_version
```

布局要求：

- `SHM_LAYOUT_VERSION` 从 3 升到 4。
- `SHM_PAYLOAD_OFFSET` 保持 128，不改变现有 payload 起点。
- `payload_version` 初始为 0。
- `ActiveSegmentDescriptor` 增加 `payload_version_offset: Option<usize>` 或等价 capability。
- layout v3 descriptor 不声明 payload version capability，reader 按旧 append-only 语义读取。
- layout v4 descriptor 必须声明并校验 `payload_version_offset == 88`。

## Writer Protocol

新增 writer 原语：

```text
begin_payload_mutation()
finish_payload_mutation()
abort_payload_mutation()
```

语义：

- `begin_payload_mutation()`：
  - 要求 active segment 未 sealed。
  - 要求不存在 open row。
  - 读取当前 even `payload_version`。
  - 发布 odd `payload_version = old + 1`。
- `finish_payload_mutation()`：
  - 发布 `row_count`、`committed_row_count` 和所有 header 变更。
  - 发布 even `payload_version = odd + 1`。
  - notify readers。
- `abort_payload_mutation()`：
  - 第一阶段只允许在没有改写 committed payload 的 append-only 失败路径中恢复 even version。
  - 对已经改写 committed payload 的 rewrite 失败，不允许假装恢复旧 snapshot。
  - 如果无法恢复，必须 seal/retire 当前 segment 并让上层走新 segment replace。

普通 append-only 写入不需要每行 bump `payload_version`，因为它不改写已经 committed 的 payload。
只有会改写 committed payload 的 API 必须进入 mutation protocol。

## Reader Protocol

新增 active payload stable-read helper：

```text
read_active_payload_consistent(max_retries, read_fn)
```

流程：

1. 读取 `payload_version`，必须是 even。
2. 读取 `committed_row_count`，校验 requested range。
3. 执行 `read_fn` 读取 payload。
4. 再次读取 `payload_version`。
5. 如果两次 version 相同且为 even，返回结果。
6. 否则 retry；超过 `max_retries` 后返回 `active payload changed during read`。

接入点：

- `SegmentTableView` / Arrow batch materialization 的 active 读取。
- `RowSpanView::cell_value()` 的 active scalar 读取。
- `ActiveSegmentReader::read_from()` 返回 view 时，至少要把 capability 带到 view；
  真正的 payload 校验发生在 materialize / scalar read 阶段。

第一阶段不要求 `RowSpanView` 创建时冻结 payload。`RowSpanView` 是 live view，稳定性由每次读取操作保证。

## Descriptor Compatibility

descriptor envelope 增加 payload version capability：

```text
payload_version_offset: 88
```

兼容规则：

- v3 descriptor 没有这个字段，只允许 append-only committed-prefix 语义。
- v4 descriptor 必须带这个字段。
- 如果 descriptor 声明 offset 但 header layout version 不支持，attach 失败。
- 如果 header layout version 支持但 descriptor 缺字段，attach 失败，避免读端误判能力。

## KeyValue Snapshot Rewrite 接入边界

只有上述能力落地后，才能回到 KeyValue：

```text
begin_payload_mutation()
rewrite full snapshot payload
finish_payload_mutation()
```

第一版仍应保守：

- snapshot row count 超过 capacity 时 fallback `replace_with_table()`。
- persist_config 场景先 fallback，避免 rewrite 与 sealed parquet 任务交叉。
- rewrite 失败后如果无法恢复旧完整 payload，fallback 必须使用新 segment，不允许发布同 segment descriptor。

## 测试计划

`zippy-segment-store`：

- layout v4 初始化时写入 `payload_version = 0`。
- descriptor envelope 包含 `payload_version_offset`。
- v4 descriptor 缺少 offset 时 attach 失败。
- begin mutation 后 version 为 odd，reader stable-read 会 retry 或返回明确错误。
- finish mutation 后 version 为 even，reader stable-read 返回完整 payload。
- active Arrow batch materialization 在 version 改变时 retry。
- active `cell_value()` 在 version 改变时 retry。

`zippy-engines`：

- 在 segment-store 原语完成前，不改 `KeyValueTableMaterializer`。
- 后续实现 KeyValue rewrite 时，再补不 rollover segment 的 engine 测试。

验证命令：

```text
cargo test -p zippy-segment-store --lib payload_version -- --nocapture
cargo test -p zippy-segment-store --lib active -- --nocapture
cargo clippy -p zippy-segment-store --all-targets -- -D warnings
cargo fmt --check
```

## 风险

- 只在 `RowSpanView::from_active_descriptor()` 校验 version 不够，payload 读取必须被 retry wrapper 包住。
- `read_active_utf8_slice()` 当前返回 borrowed `&str`，如果底层 payload 后续被 writer 改写，
  这个引用无法长期保证稳定；后续 rewrite 能力接入前，需要决定 active scalar 读取是否改为 owned value。
- reader retry 上限太低会在高频 writer 下误报，太高会增加 tail latency。
- layout v4 rollout 必须避免旧 reader attach 新 segment 后误读 payload。

## 决策记录

- 2026-05-13：KeyValue same-segment rewrite 被判定为当前内存模型下不安全。
- 2026-05-13：采用 active payload seqlock 作为后续 rewrite / overwrite 的前置设计。
- 2026-05-13：普通 append-only 写入不强制 bump payload version。
