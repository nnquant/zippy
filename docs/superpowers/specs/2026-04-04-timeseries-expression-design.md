# TimeSeries Expression Design

## Goal

把表达式能力从 `ReactiveStateEngine` 扩展到 `TimeSeriesEngine`，同时保持窗口语义、可复现性和错误边界清晰。

## Scope

本次扩展只为 `TimeSeriesEngine` 增加两个显式表达式阶段：

- `pre_factors`
- `post_factors`

执行路径固定为：

1. 原始输入 batch
2. `pre_factors`
3. `AGG_*` 窗口聚合
4. `post_factors`
5. target / source / parquet output

`ReactiveStateEngine` 的现有表达式语义保持不变。

## API Shape

Python 侧扩展后的 `TimeSeriesEngine` 形状如下：

```python
bars = zippy.TimeSeriesEngine(
    name="bar_1m",
    input_schema=tick_schema,
    id_column="symbol",
    dt_column="dt",
    window=zippy.Duration.minutes(1),
    window_type=zippy.WindowType.TUMBLING,
    late_data_policy=zippy.LateDataPolicy.REJECT,
    pre_factors=[
        zippy.EXPR(expression="price * volume", output="turnover"),
    ],
    factors=[
        zippy.AGG_FIRST(column="price", output="open"),
        zippy.AGG_LAST(column="price", output="close"),
        zippy.AGG_SUM(column="volume", output="volume"),
        zippy.AGG_SUM(column="turnover", output="turnover"),
    ],
    post_factors=[
        zippy.EXPR(expression="close / open - 1.0", output="ret_1m"),
        zippy.EXPR(expression="turnover / volume", output="vwap_1m"),
    ],
    target=zippy.NullPublisher(),
)
```

参数约束：

- `pre_factors` 默认空列表
- `post_factors` 默认空列表
- `factors` 仍然必填
- `factors` 只接受 `AGG_*`
- `pre_factors` 和 `post_factors` 当前只接受 `EXPR(...)`

本次不允许把 `TS_*` 或 Python UDF 混入时序引擎的表达式阶段。

## Execution Model

`TimeSeriesEngine.on_data(batch)` 固定执行顺序如下：

1. 对输入 batch 做事件时间合法性检查
2. 丢弃或拒绝晚到行
3. 对剩余合法行执行 `pre_factors`
4. 把预处理后的行送入窗口状态
5. 对本次确认关闭的窗口执行 `AGG_*`
6. 对聚合结果执行 `post_factors`
7. 输出最终 `RecordBatch`

这个顺序是本设计最核心的约束：

- `pre_factors` 是逐输入行执行
- `post_factors` 是逐聚合结果行执行
- `post_factors` 不参与窗口状态，只参与结果整形

## Schemas

`TimeSeriesEngine` 需要维护四份 schema：

### 1. input_schema

用户写入引擎的原始输入 schema。

### 2. pre_schema

`input_schema + pre_factors` 逐个追加后的内部 schema。

规则：

- 每个 `pre_factor` 按声明顺序编译
- 后一个 `pre_factor` 可以引用前一个 `pre_factor` 输出
- `AGG_*` 的输入字段校验基于 `pre_schema`

### 3. agg_schema

窗口关闭后、`post_factors` 之前的中间结果 schema：

- `id_column`
- `window_start`
- `window_end`
- 所有 `AGG_*` 输出列

### 4. output_schema

`agg_schema + post_factors` 逐个追加后的最终 schema。

`output_schema()` 返回这份 schema。

## Expression Type Rules

`pre_factors` 和 `post_factors` 复用当前 `ReactiveStateEngine` 的表达式系统，不引入第二套规则。

当前类型规则：

- 四则运算统一产出 `float64`
- `abs` / `log` / `clip` 产出 `float64`
- `cast` 是唯一允许显式改 dtype 的入口
- 可空性按保守传播处理

也就是说：

- 任一输入可空，输出就标记为可空
- 不做激进的非空收紧

## Construction-Time Validation

以下错误必须在 `TimeSeriesEngine(...)` 构造阶段直接报错：

1. `pre_factors` 引用不存在的输入列
2. `post_factors` 引用不存在的聚合输出列
3. 不支持的函数名
4. 参数个数错误
5. 重复输出字段名
6. `post_factors` 引用原始输入列但没有经过聚合显式产出

例如以下写法必须直接拒绝：

```python
post_factors=[
    zippy.EXPR(expression="price * 2", output="bad"),
]
```

因为 `price` 不属于 `agg_schema`。

## Late Data Semantics

晚到数据判定点不变，仍然基于每个 `id` 已观察到的最新事件时间。

顺序必须固定为：

1. 先做 late data 判定
2. 再对被接纳的行执行 `pre_factors`
3. 再进入窗口状态

这样做的原因有两个：

1. 避免无效晚到行污染 `pre_factors` 的状态或结果
2. 避免在最终要丢弃的行上浪费表达式计算成本

指标约束：

- `late_rows_total` 只统计原始输入晚到行
- 不统计表达式产物数量

## Flush and Stop

`flush()` 的语义扩展为：

1. 强制关闭当前所有 open windows
2. 为每个窗口产出一行 `agg_schema`
3. 对这些结果执行 `post_factors`
4. 输出最终 batch

`flush()` 不会重新执行输入侧逻辑：

- 不会对历史输入重算 `pre_factors`
- 不会对未产出的“原始输入行”单独做表达式补算

原因是：

- `pre_factors` 在输入阶段已经完成
- `flush()` 只负责结算窗口

`stop()` 仍然等价于隐式执行一次 `flush()`。

## Source, Target, and Archiving

该扩展不改变 `source` / `target` / `parquet_sink` 的原有边界：

- `source` 仍然接收最终输出 batch
- `target` 仍然发布最终输出 batch
- `parquet_sink(write_input=True)` 归档原始输入 batch
- `parquet_sink(write_output=True)` 归档包含 `post_factors` 结果的最终输出 batch

本次不新增“归档 pre_schema 中间结果”能力。

## Error Semantics

构造期解决的是“能不能编译表达式”，运行时仍然会有数据值错误：

- 除零
- `log(x)` 中 `x <= 0`
- `clip(min, max)` 出现动态 `min > max`
- `cast(...)` 在某一行上转换失败

这些错误继续保持当前 expression runtime 语义：

- 首次出现即返回 worker error
- engine 状态切到 `failed`
- 后续 `write()` / `flush()` / `stop()` 按已有 failed 语义处理

本次不引入“跳过坏行继续跑”的 best-effort 模式。

## Python Binding

该扩展只增加新的显式参数与构造期校验，不引入 Python 回调。

GIL 约束保持：

- 参数解析时短暂持有 GIL
- Arrow 转换和 runtime 计算无 GIL
- `pre_factors` / `post_factors` 的执行都发生在 Rust 侧

## Testing

至少补这六类测试：

1. `pre_factors` 改变聚合输入
- 例如 `turnover = price * volume`
- 再 `AGG_SUM(turnover)`，验证数值正确

2. `post_factors` 引用聚合输出
- 例如 `ret_1m = close / open - 1.0`
- 校验 `output_schema()` 和实际结果

3. `post_factors` 可引用前序 `post_factors`
- 例如先算 `mid`，再算 `delta`

4. 构造期拒绝非法引用
- `post_factors` 引用 `price`
- `pre_factors` 引用缺失字段
- 输出名冲突

5. late data 不污染 `pre_factors`
- 晚到行不会进入聚合，也不会影响最终窗口结果

6. `flush()` 触发 `post_factors`
- open windows 在 `flush()` 结算时，`post_factors` 也必须执行

## Non-Goals

本次不做：

- `CrossSectionalEngine`
- 聚合表达式直接嵌入 `AGG_*`
- `TS_*` 进入 `pre_factors` / `post_factors`
- Python UDF
- 公共子表达式消除
- planner / optimizer
- 中间 schema 归档

## Conclusion

这次扩展的核心取舍只有一句话：

把时序表达式能力拆成显式的 `pre_factors` 和 `post_factors` 两个阶段，而不是把所有逻辑塞进一个隐式 planner。

这样做的收益是：

- 执行语义清楚
- schema 边界清楚
- late data 与 flush 的行为清楚
- 可在不改动 Rust 运行时基本模型的前提下稳定落地
