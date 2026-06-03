# Zippy 复杂度热点待修复 Checklist

日期：2026-05-29

依据：本轮复杂度审计报告。范围为当前主工作区的 `crates/`、`python/`、`pytests/`、
`examples/` 和 `docs/zippy-documentation/assets/`，不包含 `.worktrees/` 内旧工作树。

本文件只记录待修复问题和验收口径，不代表已实现。所有条目默认保持现有行为、API、输出顺序、
错误语义和持久化语义不变。

## 总体原则

- [ ] 每个修复先补或确认 focused regression test，再改实现。
- [ ] 涉及 Python native extension 的改动，先运行
  `uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml`。
- [ ] 先跑窄测试，再跑相关 crate / Python API 测试。
- [ ] 性能收益不明显或涉及热路径时，补 micro-benchmark 或 `zippy-perf` profile。
- [ ] 不把 scanner 误报当成必须修复项；只处理真实热路径或长期运行风险。

## P0：长期运行内存风险

### CHK-001 移除 schema 编译路径中的非受控 `Box::leak`

- [x] 明确 `ColumnSpec` / `ColumnType::TimestampNsTz` 的 owned string 设计。
- [x] 将字段名和 timezone 从 `&'static str` 泄漏模式迁移到 `Arc<str>`、`Box<str>` 或
  `Cow<'static, str>`。
- [x] 覆盖 Gateway schema metadata 编译路径：
  `crates/zippy-gateway/src/lib.rs`。
- [x] 覆盖 Python / PyO3 schema 编译路径：
  `crates/zippy-python/src/lib.rs`。
- [x] 覆盖 StreamTable Arrow schema 编译路径：
  `crates/zippy-engines/src/stream_table.rs`。
- [x] 当前类型面已迁移为 owned schema，不再需要用 schema-hash 缓存掩盖泄漏。

验收：

- [x] 增加循环编译同一 schema 的 regression / smoke，确认无持续内存增长。
- [x] `cargo test -p zippy-segment-store -p zippy-engines -p zippy-gateway` 通过。
- [x] `PYO3_PYTHON=/home/jiangda/develop/zippy/.venv/bin/python cargo test -p zippy-python`
  或对应 Python API 测试通过。

## P1：高频查询与状态更新复杂度

### CHK-002 将 Gateway `is_in` literal list 编译为 membership index

- [x] 为 string `is_in` 使用 `HashSet<String>` 或等价预编译结构。
- [x] 为 int `is_in` 使用 `HashSet<i64>`。
- [x] 为 bool `is_in` 使用专用判断，避免每行扫 Vec。
- [x] 为 float `is_in` 明确 NaN、`-0.0`、精确相等语义，再选择 ordered wrapper、
  bit-level key 或排序后二分。
- [x] 避免每行重建或线性扫描 literal membership；当前实现按 batch 编译一次
  `GatewayIsInMembership`，再按行 O(1) 查询。

验收：

- [x] Gateway filter 单测覆盖 string / int / bool 的 membership 行为；float 语义通过
  `GatewayIsInMembership::float_key()` 固定 `-0.0` 与 `0.0` 等价，JSON literal 不支持 NaN。
- [x] 增加大 literal list 的微基准，确认从 `O(rows * literal_count)` 收敛到
  `O(rows + literal_count)`；`gateway_is_in_large_literal_membership_profile`
  显式 profile `literal_count=50000`、`row_count=200000`。
- [x] `cargo test -p zippy-gateway` 通过。
- [x] `cargo fmt --check --package zippy-gateway` 通过。
- [x] `cargo clippy -p zippy-gateway --all-targets -- -D warnings` 通过。

### CHK-003 KeyValue 表从全量 snapshot rewrite 迁移到增量/slot-level update

- [x] 梳理 `LatestColumnarState` 的 slot 生命周期和 key 顺序稳定性。
- [x] 保持 changelog 仍按更新 key 输出增量。
- [x] 将常规 snapshot 更新从 `materialize_snapshot()` + `replace_with_table()` 改为
  active slot 原地更新或等价增量路径；已有 key 走 slot-level update，新增 key 容量足够时
  在当前 active segment 内重写 snapshot，不换 segment。
- [x] 仅在容量不足、schema 变化、persist / sealed snapshot 边界等情况下回退全量 replace。
- [x] 明确 descriptor generation、snapshot version、changelog seq 的更新规则。

验收：

- [x] 覆盖同 key 多次更新、乱序 key、删除/覆盖边界、snapshot + changelog 一致性。
- [x] 增加大 key 数、小批更新 benchmark，确认常规更新接近 `O(updated_keys * columns)`。
- [x] `cargo test -p zippy-engines --test stream_table_materializer` 通过。
- [x] 相关 `pytests/test_python_api.py` key-value/latest 用例通过。

### CHK-004 TimeSeries / Reactive state 引入 id interning 或 slot id

- [x] 设计共享 id registry，避免每行 `id.to_string()` 进入 state map。
- [x] TimeSeries `open_windows` / `last_dt_by_id` 从 string key 迁移为 intern id 或 slot。
- [x] Reactive stateful factor 的 `state_by_id` / `history_by_id` 从 string key 迁移为 intern id。
- [x] 保持 flush 输出的 deterministic order。
- [x] 保持 rollback / late-data policy / id_filter 语义不变。
- [x] 明确 registry 的生命周期和清理策略，避免 id 空间无界膨胀。

验收：

- [x] TimeSeries late data、rollback、flush、id_filter regression 通过。
- [x] Reactive EMA / rolling / delay / diff / return regression 通过。
- [x] 增加 1024 / 8192 symbols 的 benchmark，对比 allocator 和吞吐。
- [x] `cargo test -p zippy-engines -p zippy-operators` 通过。

### CHK-005 CrossSectionalEngine 用 touched-slot undo log 替代每批 clone bucket

- [x] 将 `current_rows.clone()` 事务边界改为 touched-slot undo log。
- [x] 保留 bucket rollover、late row、同 bucket 同 id 最后一行覆盖语义。
- [x] 保留输出 id 顺序和 replay deterministic 语义。
- [x] 明确 finalize 失败时如何恢复 touched slot。

验收：

- [x] 覆盖乱序 bucket、重复 id、错误 rollback、flush 输出顺序。
- [x] 增加大截面、小批更新 benchmark，确认每批成本从 `O(active_bucket_rows * columns)`
  降为 `O(touched_rows * columns)`。
- [x] `cargo test -p zippy-engines --test cross_sectional_engine` 通过。

## P2：读侧复制和 Python 慢路径

### CHK-006 Segment 到 Arrow 导出减少复制，优先 sealed numeric zero-copy

- [x] 为 sealed numeric 列实现 Arrow buffer slice / zero-copy 导出；新增 sealed
  non-nullable / nullable numeric buffer 指针复用 regression。
- [x] 为 sealed Utf8 评估 offsets / values 复用方案，避免逐行 `to_owned()`；sealed
  Utf8 snapshot offsets 改为 Arrow `Utf8` 兼容的 `i32` offsets，并复用 values buffer。
- [x] 为 active segment 评估批量 column buffer 读取，减少逐行 `read_active_*` 调用；
  numeric / timestamp / Utf8 active 导出改为按列范围批量读共享内存后构造 Arrow array。
- [x] 保留 projection batch 和 chunked reader 现有语义。
- [x] 严格处理 mmap 生命周期、payload version、nullable bitmap 和 Arrow buffer ownership；
  sealed Arrow buffer 通过 custom allocation 持有 sealed segment `Arc`，active 仍复制出
  mmap 读快照并保留 payload version 一致性检查。

验收：

- [x] active / sealed consistency tests 通过：
  `cargo test -p zippy-segment-store --test arrow_bridge`。
- [x] 并发读写版本变化测试通过：`active_record_batch_read_rejects_odd_payload_version`
  仍通过。
- [x] Gateway `collect(stream=True)` live segment profile 通过：
  `native_gateway_collect_stream_reports_streaming_metrics` 覆盖 live segment streaming metrics，
  并验证 `materialized_live_batches == 0`、`segment_streamed_batches > 0`。
- [x] `cargo test -p zippy-segment-store -p zippy-gateway` 通过。

### CHK-007 Python row callback / replay 明确降级为低频兼容路径

- [x] 文档和 docstring 明确高频订阅优先使用 table callback / batch callback。
- [x] 为 replay 增加 batch callback 或批量 emit 模式：`ParquetReplayEngine`、
  `TableReplayEngine` 和 `replay()` 支持 `table_callback=True`。
- [x] 保留现有 row callback API 兼容性，默认仍是逐行 `Row` callback。
- [x] 给 row callback 加可观测指标或 profile 示例，避免被误用为高频路径；callback
  replay metrics 暴露 `callback_mode`、`rows_delivered_total`、`batches_delivered_total`、
  `elapsed_ns` 和 `rows_per_callback`。

验收：

- [x] remote subscribe / replay smoke 通过：
  `uv run pytest pytests/test_python_api.py -k "replay or subscribe_table" -q`。
- [x] row callback 与 table callback 行数、顺序、异常传播一致；新增
  `test_parquet_replay_engine_table_callback_matches_row_callback_order`。
- [x] 补一组 row callback vs table callback profile，证明批量路径对象分配更低；同一测试
  验证 `table_callback=True` 下 3 行按 2 个 Arrow table callback 发出，并通过 metrics 暴露。
- [x] `uv run --extra dev pytest pytests -v` 中相关 Python API 用例通过；当前已跑相关
  replay / subscribe_table 子集。

## 暂不纳入本轮主线的低优先级项

- [ ] `python/zippy/pm_bridge.py` TOML 排序和小数据结构循环：主要是 CLI 配置路径，先不优化。
- [ ] `docs/zippy-documentation/assets/site.js` 离线高亮器嵌套扫描：文档站小输入，先不优化。
- [ ] `examples/07_ops/02_table_perf_probe.py` 重复扫描：性能探针本身，除非要扩展为正式 benchmark。

## 建议修复顺序

1. [x] CHK-001：先解决无界内存泄漏风险。
2. [x] CHK-002：低风险、高收益，适合作为首个可落地优化。
3. [x] CHK-003：收益大，但语义边界宽，建议单独分支。
4. [x] CHK-004 和 CHK-005：同属 state key / rollback 专项，可共享 id registry 设计。
5. [x] CHK-006：读侧 zero-copy 专项，需要更完整 benchmark。
6. [x] CHK-007：API/documentation 和 profile 收口，可与 Python 文档一起做。
