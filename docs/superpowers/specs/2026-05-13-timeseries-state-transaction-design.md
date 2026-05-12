# TimeSeries State Transaction Design

日期：2026-05-13

## 背景

`TimeSeriesEngine` 的 P004 已经先完成了 row selection fast path：无 `id_filter` 的常见路径不再
物化全量 row index `Vec<u32>`。剩余热点集中在状态提交方式：

```text
let mut next_open_windows = self.open_windows.clone();
let mut next_last_dt_by_id = self.last_dt_by_id.clone();
```

这保证了 `on_data()` 在聚合更新或 finalize 出错时不会污染 engine 状态，但代价是每批都 clone
整个 active id/window 状态。大截面、长窗口或大量活跃 id 场景下，这会把批处理成本绑到
`active_ids`，而不是只和本批 touched ids 相关。

本设计只替换 `TimeSeriesEngine::on_data()` 的状态提交机制，不改变聚合语义、late-data 语义、
输出 schema、public API 或 `OpenWindow` 聚合逻辑。

## 目标

- 去掉 `on_data()` 对 `open_windows` 和 `last_dt_by_id` 的批级全量 clone。
- 引入内部 transaction / undo log，只记录本批 touched id 的原始状态。
- `on_data()` 成功时保留原地更新结果。
- `on_data()` 出错时恢复 touched id 的 `open_windows` 和 `last_dt_by_id`。
- 保持当前错误边界：late data reject、aggregation update error、finalize output error 都不污染状态。
- 保持现有输出顺序和窗口完成语义。
- 不改变 `on_flush()` 语义。

## 非目标

- 不改 `BTreeMap<String, OpenWindow>` / `BTreeMap<String, i64>` 容器类型。
- 不做 id interning、borrowed key、HashMap / IndexMap 迁移。
- 不改变 `OpenWindow` 内部 columnar/slot 结构。
- 不改变 pre/post factor evaluation。
- 不改变 `DropWithMetric` 对 late-row metric 的当前行为。
- 不改变 completed windows 的排序策略。

## 设计选项

### 方案 1：`TimeSeriesStateTxn` touched-id undo log（采用）

做法：

- 新增内部 `TimeSeriesStateTxn`，包装对 `open_windows` 和 `last_dt_by_id` 的可变借用。
- 第一次 touch 某个 id 时，记录：
  - `previous_open_window: Option<OpenWindow>`
  - `previous_last_dt: Option<i64>`
- 后续对同一 id 的更新不重复记录 undo。
- `commit()` 成功时丢弃 undo。
- `rollback()` 出错时按 undo 恢复 touched ids。

优点：

- 成本从 `O(active_ids clone)` 降到 `O(touched_ids clone)`。
- 保留当前错误不污染状态的行为。
- 不改 public API，也不改聚合结果语义。

缺点：

- `on_data()` 状态机需要从 clone-on-write 改成 transaction 风格。
- undo log 仍会 clone touched `OpenWindow`，只是范围大幅收窄。

### 方案 2：继续全量 clone，但优化 `OpenWindow` clone 成本

做法：

- 保留 `next_open_windows = self.open_windows.clone()`。
- 尝试压缩 `OpenWindow` 内部 Vec 或共享不可变 spec 元数据。

优点：

- 改动小。

缺点：

- 仍然每批复制所有 active ids。
- 不能解决 P004 的主要复杂度来源。

### 方案 3：直接改成 slot state / id registry

做法：

- 把 `BTreeMap<String, OpenWindow>` 改成 interned id + slot arena。
- last dt 和 open window 都按 slot 存储。

优点：

- 长期性能上限更高。

缺点：

- 会同时改变 key model、状态容器、rollback 和输出排序，范围过大。
- 更适合作为后续 P004/P005 shared id registry 专项。

## 采用方案

采用方案 1。

第一阶段只替换批级 clone 为 touched-id undo log。它直接对应当前代码里的全量 clone 热点，且不会把
id interning、slot state 和容器替换混入同一次变更。

## 核心设计

### 1. Transaction 类型

内部类型放在 `crates/zippy-engines/src/timeseries.rs`：

```text
TimeSeriesStateTxn<'a> {
    open_windows: &'a mut BTreeMap<String, OpenWindow>,
    last_dt_by_id: &'a mut BTreeMap<String, i64>,
    undo: BTreeMap<String, TimeSeriesStateUndo>,
    committed: bool,
}

TimeSeriesStateUndo {
    open_window: Option<OpenWindow>,
    last_dt: Option<i64>,
}
```

`BTreeMap` 保持 deterministic behavior。undo 只按 touched id 记录一次。

### 2. Touch 与 Update

transaction 提供方法：

```text
touch(id)
take_open_window(id) -> Option<OpenWindow>
insert_open_window(id, window)
set_last_dt(id, dt)
restore()
commit()
```

任何会修改该 id 状态的方法都先调用 `touch(id)`：

- `take_open_window()` 之前记录原 `open_windows[id]` 和 `last_dt_by_id[id]`。
- `insert_open_window()` 之前记录原状态。
- `set_last_dt()` 之前记录原状态。

这可以覆盖同一批中同一 id 多次更新、同一窗口更新、窗口 rollover、新 id 插入和 last dt 更新。

### 3. `on_data()` 接入

当前逻辑：

```text
let mut next_open_windows = self.open_windows.clone();
let mut next_last_dt_by_id = self.last_dt_by_id.clone();
for row ...
    mutate next_open_windows / next_last_dt_by_id
if completed.is_empty()
    assign next state
else
    finalize then assign next state
```

改为：

```text
let mut completed = Vec::new();
let mut txn = TimeSeriesStateTxn::new(&mut self.open_windows, &mut self.last_dt_by_id);

let result = process rows with txn;
if result.is_err() {
    txn.rollback();
    return result;
}

let output = finalize completed if needed;
if output.is_err() {
    txn.rollback();
    return output;
}

txn.commit();
return outputs;
```

Rust 实现上需要用 block 收窄 `txn` 的可变借用生命周期。`finalize_windows()` 需要 `&mut self`
来运行 post factors，所以应在 finalize 前结束或显式 drop transaction，同时保留一个 rollback
handle 是不可行的。为避免 borrow 冲突，第一版采用 helper：

```text
apply_timeseries_rows(open_windows, last_dt_by_id, ...) -> Result<TimeSeriesTxnResult>
```

该 helper 返回：

```text
TimeSeriesTxnResult {
    completed: Vec<OpenWindow>,
    rollback: Option<TimeSeriesRollback>,
}
```

其中 `TimeSeriesRollback` 持有 undo log，但不再持有 map borrow；如果 finalize 失败，
`rollback.restore(&mut self.open_windows, &mut self.last_dt_by_id)`。

### 4. 错误边界

需要保持：

- Late-data reject 在进入 state mutation 前已经完成校验；行为不变。
- `OpenWindow::update()` 如果返回 error，恢复 touched ids。
- `finalize_windows(completed)` 如果返回 error，恢复 touched ids。
- `processed_input` / pre factor 失败发生在 transaction 之前；状态自然不变。

`DropWithMetric` 的 late-row metric 当前会在 state mutation 前累计；本设计不改变这个行为。

### 5. 输出顺序

completed windows 的收集顺序仍按 accepted rows 处理顺序决定：

- 同一 id 窗口 rollover 时，把旧 window push 到 `completed`。
- 多 id 混合输入时，顺序与当前 clone-on-write 实现一致。

rollback 只影响失败路径；成功路径输出顺序不变。

## 测试计划

新增/扩展测试：

- `TimeSeriesStateTxn` 单元测试：
  - 新 id 插入后 rollback 会删除新 id。
  - 已有 id 更新后 rollback 会恢复旧 `OpenWindow` 和 old `last_dt`。
  - 同一 id 多次 touch 只恢复到批前状态。
- `TimeSeriesEngine` 集成测试：
  - 当前 22 个 timeseries tests 不变。
  - 新增 `on_data` finalize error 不污染状态测试：
    - 先写入 zero-weight VWAP window。
    - 再写入下一窗口触发旧 window finalize，返回 `vwap denominator is zero`。
    - 随后 `on_flush()` 仍对原旧 window 返回相同 zero denominator error，证明失败的下一窗口没有提交。

验证命令：

```text
cargo test -p zippy-engines --lib timeseries::tests::time_series_state_txn -- --nocapture
cargo test -p zippy-engines --test timeseries_engine -- --nocapture
cargo clippy -p zippy-engines --all-targets -- -D warnings
cargo fmt --check
```

## 后续扩展

- shared id registry / id interning。
- `BTreeMap<String, ...>` 迁移到 slot arena，同时保留 deterministic output order。
- 输入按 id/window 排序时的 no-remove fast path。
- spec input value downcast 缓存，减少每个 row/spec 重复 downcast。
