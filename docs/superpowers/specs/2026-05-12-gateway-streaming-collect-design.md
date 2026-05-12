# Gateway Streaming Collect Design

日期：2026-05-12

## 背景

当前 Gateway 的 `collect_stream` 协议已经存在，但服务端内部仍复用 full
`handle_collect()`：先扫描、拼接、执行 residual plan、编码成完整 Arrow IPC payload，再按
`chunk_rows` 切帧返回。这个实现只改变了 wire protocol，没有真正降低大查询的首包延迟和峰值
内存。

本设计覆盖 `docs/performance_audit_findings_2026-05-12.md` 中 P002 和 P009 的剩余项：

- `collect(stream=True)` 必须成为真实 streaming collect。
- persisted parquet 扫描需要 bounded parallelism，同时保持 deterministic output order。

## 已确认决策

- 只影响 `collect(stream=True)`；默认 `collect()` 保持旧协议和旧行为。
- `collect(stream=True)` 遇到不能流式执行的 residual plan 时直接报错，提示改用默认
  `collect()`。
- persisted parquet 并行扫描保持 deterministic output order，按现有文件顺序输出。
- 本专项先实现 persisted parquet 真 streaming + bounded parallel scan。
- live active/sealed segment 本轮继续沿用现有 batch materialization，再按 `chunk_rows` 切 chunk
  输出；真正 chunked/zero-copy segment reader 留给 P008 专项。

## 非目标

- 不改变默认 `Table.collect()` 行为。
- 不把所有 query plan 都改成统一查询执行器。
- 不实现 live segment zero-copy Arrow reader。
- 不支持 unordered streaming 输出。
- 不在本专项实现 Python async writer 或 Gateway subscribe notification。

## 查询契约

`collect(stream=True)` 表示“服务端必须全程使用可流式执行路径”。支持范围：

- `head`
- `tail`
- Gateway 当前可安全下推的 leading filter
- scan projection

不支持范围：

- 任何需要完整 `RecordBatch` 的 residual plan。
- 当前 Gateway 不能安全下推的表达式、复杂 select、with_columns、join 等。

入口校验必须发生在发送 `collect_start` 前。若 residual plan 非空，Gateway 返回普通错误帧：

```text
collect(stream=True) requires a fully streamable plan; use collect() for residual operations
```

这样可以避免“用户以为在 streaming，服务端实际全量 materialize”的假 streaming。

## 执行结构

在 `crates/zippy-gateway/src/lib.rs` 中为 streaming collect 保持独立路径：

- `handle_collect_stream_async()`
  - 负责 async socket protocol。
  - 发送 `collect_start`。
  - 循环拉取 next batch，编码并发送 `collect_chunk`。
  - 正常结束发送 `collect_end`。
  - 扫描中失败发送 `collect_error` 后结束。
- `build_collect_stream_plan(header)`
  - 解析 `source`、`snapshot_id`、`plan`、`chunk_rows`。
  - 复用现有 `collect_plan_row_range_prefix()`、
    `collect_plan_leading_filter_count()`、`collect_plan_scan_projection_columns()`。
  - 构造 `GatewayScanPushdown`。
  - 校验 residual plan 必须为空。
- `GatewayCollectStreamProducer`
  - blocking 侧 batch producer。
  - 每次调用返回下一批 `RecordBatch` 或结束状态。
  - 初版可用内部 enum 表达：
    - `PersistedParallel`
    - `LiveMaterialized`
    - `SnapshotMaterialized`

不直接引入复杂 async stream trait。当前 parquet scan、filter、projection 都是同步代码，先用
“blocking producer + async writer”的边界，符合现有 Gateway 架构。

## Persisted Parquet Streaming

persisted parquet 是本专项的真实 streaming 主路径。

### 文件顺序

使用 `non_overlapping_persisted_files(stream)` 的顺序作为 canonical order。并行扫描不改变输出
顺序。

### 扫描任务

新增内部任务结构：

```text
GatewayPersistedScanTask {
    file_index,
    file_path,
    projection_columns,
    scan_pushdown,
    tail_rows,
}
```

每个 worker 对单文件执行：

- 打开 parquet reader。
- 使用 `PERSISTED_PARQUET_SCAN_BATCH_SIZE` 逐 batch 读取。
- 应用 projection/filter pushdown。
- 生成该文件的 filtered batches。

### Bounded Parallelism

初版使用内部常量：

- `DEFAULT_GATEWAY_STREAMING_COLLECTS = 8`
- `DEFAULT_GATEWAY_PERSISTED_SCAN_PARALLELISM = 4`
- `DEFAULT_GATEWAY_STREAMING_PENDING_FILE_RESULTS = 8`

后续可把这些常量暴露为 Gateway config。

### Ordered Output

输出端维护：

- `next_file_index`
- `pending_file_results`

当 worker 完成某个文件：

- 如果 `file_index == next_file_index`，立即输出该文件 batches。
- 如果不是下一个 index，暂存到 bounded pending buffer。
- pending buffer 达到上限时，worker/collector 侧阻塞，避免慢客户端导致内存无限增长。

### Head/Tail

`head(n)`：

- 输出累计达到 `n` 行后停止继续调度新文件。
- 已在跑的 worker 可以自然结束，但不再把后续结果发送给客户端。

`tail(n)`：

- 继续沿用当前“从后往前选择可能命中文件”的语义，避免扫描无关旧文件。
- 输出前恢复正序，保证和默认 `collect()` 语义一致。

## Live And Snapshot Path

本专项不重写 live segment reader。

- in-process writer active batch：仍取 `active_record_batch()`，再按 `chunk_rows` 切 chunk 输出。
- snapshot / sealed / active segment：初版仍可复用现有 materialized batch 路径，再切 chunk 输出。
- 这些路径必须在 metrics 中标记为 `materialized_live_batches` 或等价字段，避免误认为已经
  zero-copy streaming。

后续 P008 专项再把 live/sealed segment 改成 chunked/zero-copy reader。

## 资源隔离

耗时 `collect(stream=True)` 会长期占用一个 TCP connection 和一个 async request task，但不应阻塞
整个 Gateway。

资源隔离规则：

- async socket accept/read/write 不执行重扫描或 parquet 解码。
- parquet scan/filter/encode 的 blocking 工作通过 bounded producer 执行。
- 写 socket 时不持有全局 `blocking_limit` permit。
- 慢客户端通过 socket write backpressure 限速 producer。
- pending result buffer 有上限，达到上限后 scan worker 等待消费。
- 客户端断开后停止调度新文件，并丢弃尚未发送的结果。

初版增加或复用以下限额：

- `max_streaming_collects`
- `max_persisted_scan_parallelism_per_collect`
- `max_streaming_pending_file_results`

这些限额初版可为内部常量，后续配置化。

## 错误处理

- plan 校验错误发生在 `collect_start` 前，返回普通 error frame。
- `collect_start` 已发送后，如果后续 scan/filter/encode 失败，发送：

```json
{"status": "error", "kind": "collect_error", "reason": "..."}
```

- 客户端断开后，async write 返回错误；服务端停止继续拉取 batch。
- token/auth、payload size、header timeout 沿用现有 Gateway frame 防护。

## Metrics

`collect_end.metrics` 增加 streaming 维度：

- `streaming=true`
- `scanned_files`
- `scanned_rows`
- `returned_rows`
- `scan_elapsed_ms`
- `filter_elapsed_ms`
- `encode_elapsed_ms`
- `write_elapsed_ms`
- `max_pending_file_results`
- `materialized_live_batches`

普通 `collect()` metrics 不需要在本专项中改名或改变语义。

## 测试计划

Rust unit tests:

- residual plan 非空时 `collect(stream=True)` 报错。
- persisted scan producer 即使后一个文件先完成，也按 deterministic file order 输出。
- `head(n)` 达到行数后停止输出。
- `tail(n)` 输出顺序和默认 `collect()` 一致。
- pending buffer 上限生效，不随文件数无限增长。

Native Gateway integration tests:

- 多 persisted parquet 文件下，`collect(stream=True)` 返回多个 chunk。
- streaming 输出顺序和默认 `collect()` 一致。
- `chunk_rows=1` 仍返回 start/chunk/end 多帧协议。
- streaming metrics 包含 scan/filter/encode/write 拆分字段。
- residual plan 对 `collect(stream=True)` 报错，默认 `collect()` 仍成功。

Python tests:

- `Table.collect(stream=True)` 对 streamable plan 成功。
- residual plan 抛出明确异常。
- 默认 `collect()` 对同一 residual plan 仍成功。

Verification commands:

```bash
cargo test -p zippy-gateway --lib
cargo test -p zippy-gateway --test native_gateway -- --test-threads=1
cargo clippy -p zippy-gateway --all-targets -- -D warnings
uv run pytest pytests/test_python_api.py -q -k "remote_collect_stream"
cargo fmt --check
```

## Rollout

1. 先实现 plan 校验和 residual error，确保 `collect(stream=True)` 契约清楚。
2. 再引入 persisted parquet streaming producer，先串行输出，保证协议正确。
3. 增加 bounded parallel scan，并保持 deterministic output order。
4. 增加 metrics 和慢客户端/断开路径测试。
5. 更新 `docs/performance_audit_fix_status_2026-05-12.md`，把 P002/P009 的剩余项改为已处理或
   明确保留的后续边界。

## 自审结论

- 本设计没有改变默认 `collect()`。
- 本设计不承诺 live segment zero-copy，避免和 P008 专项混在一起。
- streaming residual plan 明确报错，不隐藏 full materialization。
- 资源隔离明确要求 bounded concurrency 和 bounded pending results，避免耗时 collect 阻塞整个
  Gateway。
