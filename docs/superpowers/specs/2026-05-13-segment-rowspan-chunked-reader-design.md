# Segment RowSpan Chunked Reader Design

日期：2026-05-13

## 背景

P002/P009 已经让 `collect(stream=True)` 在 persisted parquet 路径上走多帧 streaming
collect，并且支持 bounded parallel scan。当前缺口在 P008：live / sealed segment 读侧仍然会把一个
`RowSpanView` 一次性导出成完整 `RecordBatch`，再由 Gateway 按 `chunk_rows` 切片发送。

现有路径的主要问题：

- `RowSpanView::as_record_batch()` 会对整个行范围逐列创建 Arrow array。
- `GatewayCollectStreamProducer::Materialized` 持有完整 `RecordBatch`，`next_batch()` 只是对它
  `slice()`。
- `collect(stream=True)` 对 live / segment stream 的首包延迟和峰值内存仍受完整 materialization
  影响。

本专项先解决“逐块读取 segment 行范围”，不直接做 Arrow zero-copy。

## 目标

- 为 `RowSpanView` 增加 chunked batch reader，让调用方按固定 `chunk_rows` 逐块导出行范围。
- 让 Gateway `collect(stream=True)` 的 live / sealed segment 路径不再先 concat 成单个大 batch。
- 保持现有 projection、filter、row range pushdown 和 deterministic row order 语义。
- 保持 `collect()` 默认路径不变。
- 在 metrics 中区分真实 segment chunked streaming 和 materialized fallback。

## 非目标

- 不实现 Arrow buffer zero-copy。
- 不改变 sealed segment 的内存布局。
- 不改变 active segment payload version protocol。
- 不改变 Python API 或 `collect(stream=True)` 的用户契约。
- 不把 Gateway 查询执行器重写成统一 iterator framework。
- 不支持含 residual plan 的 streaming collect；仍按现有规则在 start 前拒绝。

## 设计选项

### 方案 A：RowSpan chunked reader（采用）

做法：

- `RowSpanView` 增加 `slice(start, end)` 或等价内部 helper。
- 新增 `RowSpanBatchReader`，持有一个 span、projection columns、`chunk_rows` 和当前 offset。
- 每次 `next_batch()` 只对当前 chunk 行范围调用已有 projection / Arrow materialization。
- Gateway segment streaming producer 持有多个 row-span reader，并逐个 reader 拉取 chunk。

优点：

- 改动范围清楚，复用现有 Arrow projection 和 active payload consistent-read wrapper。
- 对 sealed / active segment 都适用。
- 首包可以在第一个 chunk materialize 后返回，不需要等完整 segment。
- 为后续 zero-copy reader 留出同一个 producer 边界。

缺点：

- 每个 chunk 仍会分配 Arrow array。
- filter 仍是 chunk 读后执行，不能利用 segment-level statistics 跳过行组。

### 方案 B：直接 Arrow zero-copy（不采用）

做法：

- 为 sealed / active payload 构造 borrowed Arrow buffers。
- Utf8 offsets、validity bitmap 和 fixed-width values 直接映射到底层内存。

优点：

- 读侧拷贝最少。

缺点：

- active shm 生命周期、payload version、Arrow buffer ownership 和 UTF8 offsets 都要一起设计。
- 风险超过 P008 第一阶段需要。

### 方案 C：只优化 Gateway materialized fallback（不采用）

做法：

- 继续先拿完整 `RecordBatch`，只减少 concat 或 slice 的额外分配。

优点：

- 改动最小。

缺点：

- 没有解决完整 materialization 的首包延迟和峰值内存问题。

## 采用方案

采用方案 A：RowSpan chunked reader。

核心边界是：本专项把“segment 行范围怎么逐块导出”下沉到 `zippy-segment-store`，
Gateway 只消费 reader，不再自己把完整 segment batch materialize 后切片。

## Segment Store API

新增内部公开类型：

```text
RowSpanBatchReader
```

建议接口：

```text
RowSpanView::batch_reader(chunk_rows, projection_columns) -> Result<RowSpanBatchReader>
RowSpanBatchReader::next_batch() -> Result<Option<RecordBatch>>
```

语义：

- `chunk_rows` 小于 1 时按 1 处理。
- projection 为 `None` 时导出全部列。
- projection 为 `Some(columns)` 时只导出请求列，列顺序按请求顺序。
- 每个 chunk 的行序保持原 span 行序。
- 空 span 返回 schema 正确、无 chunk 的 reader；是否需要发送空 batch 由上层决定。

active segment 读取仍通过现有 `read_active_payload_consistent()` 包裹每个 chunk 的 payload 读取。
这意味着每个 chunk 是一致 payload version，不要求整个 stream 的所有 chunk 来自同一个 active
payload version。Gateway 如果需要 snapshot high watermark，仍由它在构造 span 时限定 end row。

## Gateway Producer

`GatewayCollectStreamProducer` 新增 segment reader 形态：

```text
Segment {
    schema,
    readers,
    current_reader,
    metrics,
}
```

每个 reader 对应一个 live row span：

- sealed segment span。
- active segment span，end row 限定为构造 producer 时的 committed high watermark。

`next_batch()` 流程：

1. 从当前 reader 拉取一个 chunk。
2. 对 chunk 应用 pushed filters。
3. 过滤后为空则继续拉取下一个 chunk。
4. 返回非空 chunk，并累加 `returned_rows`。
5. 当前 reader 结束后切到下一个 reader。

projection 应在 `RowSpanBatchReader` 中执行，避免读取未请求列。filter 仍在 chunk batch 上执行。

## Row Range

row range pushdown 继续保持当前语义：

- `tail(n)`：先选择需要覆盖的 sealed / active 尾部 row spans，再逐 span chunk 输出。
- `head(n)`：从最早 sealed segment / active span 开始，最多构造覆盖 `n` 行的 spans。
- `slice(offset, length)`：按全 stream 行号选择 spans。

有 persisted 文件的 mixed stream 保持现有安全边界：

- `tail(n)` 可以继续组合 persisted tail 和 live tail。
- `head` / `slice` 在 mixed persisted + live 场景下仍可先走现有 persisted producer 或 fallback，
  不在本专项强行重写全局 query planner。

## Metrics

`collect_end.metrics` 增加或更新：

- `streaming=true`
- `materialized_live_batches=0` 表示没有走完整 live batch fallback。
- `segment_streamed_batches`：从 segment row spans 产出的 chunk 数。
- `segment_streamed_rows`：segment row spans 扫描行数，filter 前计数。

已有 `returned_rows` 继续表示 filter 后返回给客户端的行数。

## 错误处理

- projection column 不存在时，在 `collect_start` 前或 producer 构造阶段返回普通错误。
- producer 开始后发生 segment attach、payload read、filter 或 Arrow 构造错误，返回
  `collect_error` frame。
- active payload 在 chunk 读取期间频繁变化并超过 retry 上限时，沿用 segment-store 的错误信息。
- stale stream 和缺失 descriptor 继续沿用当前 Gateway 错误语义。

## 测试策略

Segment-store tests:

- `RowSpanBatchReader` 按 `chunk_rows` 切分 sealed span。
- projection 只输出请求列，并保持请求列顺序。
- active span chunk reader 能读取多个 chunk，且结果和 `as_record_batch()` 一致。

Gateway tests:

- `collect(stream=True)` 对 live segment 按 `chunk_rows` 产出多个 `collect_chunk`。
- live segment streaming metrics 中 `segment_streamed_batches > 1` 且
  `materialized_live_batches == 0`。
- projection + filter 在 segment chunked producer 上语义和默认 `collect()` 一致。
- `tail(n)` live segment streaming 不先 materialize 全量 active segment。

## 实施顺序

1. 在 segment-store 写 `RowSpanBatchReader` sealed RED test。
2. 实现 sealed chunk reader。
3. 扩展 active chunk reader，并用 active span 对比完整 batch。
4. 在 Gateway 增加 live segment streaming RED test，确认当前仍走 materialized fallback。
5. 增加 `GatewayCollectStreamProducer::Segment` 并接入 live segment path。
6. 更新 P008 状态文档和验证记录。

## 后续专项

- Arrow zero-copy / borrowed buffer reader。
- segment-level filter statistics 或 row-group-like pruning。
- mixed persisted + live 的统一 streaming query planner。
- streaming collect 限流参数配置化。
