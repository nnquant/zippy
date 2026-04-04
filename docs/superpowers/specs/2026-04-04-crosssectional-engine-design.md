# CrossSectionalEngine Design

## Goal

为 Zippy 增加一个可复现、事件时间驱动的 `CrossSectionalEngine`，用于在固定时间桶上对截面数据执行最小可用的 `CS_*` 算子。

本设计只解决首版可交付能力：

- `ClockTrigger`
- 事件时间对齐
- `latest_timestamp_only`
- `CS_RANK`
- `CS_ZSCORE`
- `CS_DEMEAN`

本设计不试图一次性覆盖动态 universe、分组截面、字符串表达式、Python UDF、状态恢复或分布式执行。

## Scope

本次首版 `CrossSectionalEngine` 的范围如下：

### 包含

- 事件时间驱动的截面桶对齐
- 当前时间桶内每个 `id` 仅保留最后一条记录
- 看见更大事件时间桶时关闭旧桶并产出结果
- `flush()` 强制产出当前未关闭桶
- `LateDataPolicy::Reject`
- `CS_RANK`
- `CS_ZSCORE`
- `CS_DEMEAN`
- Rust 引擎实现
- Python 绑定
- `source=TimeSeriesEngine` 级联
- `target=NullPublisher/ZmqPublisher`
- `ParquetSink(write_output=True)` 输出归档

### 不包含

- 动态 universe 管理
- 静态显式 universe 声明
- `AllReady` / `Timeout`
- 系统时间触发
- 前值填充或补空行
- `group_by` / 行业中性化
- 表达式进入 `CrossSectionalEngine`
- Python UDF
- 状态快照与故障恢复
- 分布式执行

## Core Semantics

首版采用以下固定语义：

- 触发模型：`ClockTrigger`
- 时钟基准：事件时间
- 截面模式：`latest_timestamp_only`
- 缺失规则：某个时间桶里没有出现的 `id` 不属于该截面
- 关闭规则：看到更大的时间桶时立即关闭旧桶
- 晚到规则：已关闭桶的数据按 late data 处理，首版默认 `Reject`
- 空桶规则：没有输入的时间桶不产出空截面

这意味着 `CrossSectionalEngine` 的结果代表：

- “在某个事件时间桶上，针对该桶实际到达的 `id` 集合做截面计算”

而不是：

- “针对一个稳定 universe 做完整面板快照”

## Time Alignment

用户显式提供：

- `trigger_interval`

对每条输入记录，按以下方式对齐：

```text
bucket_start = align(dt, trigger_interval)
```

其中：

- `dt` 必须是 UTC 纳秒时间戳
- `bucket_start` 是该条记录所属截面的时间戳

首版沿用 `TimeSeriesEngine` 的整除对齐语义：

```text
bucket_start = dt.div_euclid(trigger_interval) * trigger_interval
```

## Bucket State Machine

内部状态只保留最小集合：

- `current_bucket_start: Option<i64>`
- `current_rows: BTreeMap<String, OwnedRow>`
- `last_closed_bucket_start: Option<i64>`

其中：

- `current_rows` 存当前时间桶内每个 `id` 的最后一条记录
- `BTreeMap` 保证稳定排序，便于可复现输出

### on_data(batch)

`on_data(batch)` 固定执行如下：

1. 校验输入 schema
2. 逐行读取 `id` 和 `dt`
3. 将 `dt` 对齐到 `bucket_start`
4. 根据 `bucket_start` 与 `current_bucket_start` 比较，执行以下分支：

#### Case 1: 当前桶为空

- 初始化 `current_bucket_start`
- 将当前行写入 `current_rows[id]`

#### Case 2: 行属于当前桶

- 用当前行覆盖 `current_rows[id]`
- 即：同一个桶内，同一个 `id` 多次出现时，只保留最后一条

#### Case 3: 行属于更大的桶

- 先对 `current_rows` 结算一次截面输出
- 更新 `last_closed_bucket_start`
- 清空当前桶状态
- 用当前行开启新桶

中间跳过的空桶：

- 不补产出
- 不补空 batch

#### Case 4: 行属于已关闭桶

- 按 late data 处理
- 首版默认返回 `ZippyError::LateData`

### flush()

`flush()` 的固定语义：

- 如果当前没有 open bucket，返回空输出
- 如果当前有 open bucket，立即对它做一次截面结算
- 结算结果与正常关桶走同一条输出路径
- 结算成功后清空当前桶状态

### stop()

- `stop()` 仍等价于隐式执行一次 `flush()`

## API Shape

Python 首版 API 形状如下：

```python
cs = zippy.CrossSectionalEngine(
    name="cs_1m",
    input_schema=bar_schema,
    id_column="symbol",
    dt_column="dt",
    trigger_interval=zippy.Duration.minutes(1),
    late_data_policy=zippy.LateDataPolicy.REJECT,
    factors=[
        zippy.CS_RANK(column="ret_1m", output="ret_rank"),
        zippy.CS_ZSCORE(column="ret_1m", output="ret_z"),
        zippy.CS_DEMEAN(column="ret_1m", output="ret_dm"),
    ],
    target=zippy.NullPublisher(),
)
```

参数约束：

- `input_schema` 必须显式声明
- `id_column` 必须是 utf8/string
- `dt_column` 必须是 UTC 纳秒时间戳
- `trigger_interval` 必须为正整数 duration
- `late_data_policy` 首版只接受 `Reject`
- `factors` 必填
- `factors` 首版只接受 `CS_RANK`、`CS_ZSCORE`、`CS_DEMEAN`

## Input and Output Schema

### input_schema

用户输入的原始 schema。

### output_schema

首版输出 schema 固定为：

- `id_column`
- `dt_column`
- 所有 `CS_*` 输出列

这里的 `dt_column` 含义不是原始逐条记录时间，而是：

- 对齐后的 `bucket_start`

这样做的原因：

- 输出可继续作为下游事件时间列使用
- 与 `source` 级联更自然
- 结果可复现且易于解释

## CS_* Operators

首版只交付 3 个算子。

### CS_RANK

```python
zippy.CS_RANK(column="ret_1m", output="ret_rank")
```

规则：

- 只对非 null 样本排名
- 非有限数值（`NaN`、`inf`、`-inf`）按缺失样本处理
- ties 使用 average rank
- 输出 dtype 为 `float64`
- 不做归一化到 `[0, 1]`
- null 行输出 null

### CS_DEMEAN

```python
zippy.CS_DEMEAN(column="ret_1m", output="ret_dm")
```

规则：

- 只对非 null 样本求均值
- 非有限数值（`NaN`、`inf`、`-inf`）按缺失样本处理
- 输出为 `value - mean`
- 输出 dtype 为 `float64`
- null 行输出 null

### CS_ZSCORE

```python
zippy.CS_ZSCORE(column="ret_1m", output="ret_z")
```

规则：

- 只对非 null 样本求均值和标准差
- 非有限数值（`NaN`、`inf`、`-inf`）按缺失样本处理
- 使用总体标准差
- 输出 dtype 为 `float64`
- 样本数为 0 时整列输出 null
- 标准差为 0 时整列输出 `0.0`
- null 行输出 null

## Null and Missing Semantics

首版把“该桶里没有这个 `id`”和“该 `id` 的某列为 null”严格区分：

- 没有这个 `id`：不属于该截面
- 存在这个 `id`，但算子输入列为 null：该行属于输出 batch，但该算子结果为 null
- 存在这个 `id`，但算子输入列为非有限数值：按缺失样本处理，该算子结果为 null

不同 `CS_*` 算子各自根据自己的输入列决定有效样本集合。

因此：

- 同一个输出 batch 中，不同 `CS_*` 的有效样本数可以不同

## Warmup

`CrossSectionalEngine` 首版没有跨桶状态，因此没有时间序列意义上的 warmup。

每个时间桶都是独立截面计算。

首版不额外提供：

- `min_samples`
- warmup 配置

## Error Semantics

### Construction-Time Errors

以下错误必须在构造阶段直接报错：

- 缺失输入列
- 输入列 dtype 不匹配
- 重复输出列名
- 非法算子配置
- 不支持的 `late_data_policy`
- 非法 `trigger_interval`

### Runtime Errors

首版目标是把常见坏值吸收为稳定语义，而不是整桶失败。

因此：

- null 值通过“该算子输出 null”处理
- 非有限数值按缺失样本处理，不传播为整列 `NaN`
- 零方差 `zscore` 输出 `0.0`

理论上首版这 3 个 `CS_*` 不应有常规数值错误把整桶打死。

### Late Data

若某条记录落入已关闭桶：

- 返回 `ZippyError::LateData`
- engine 进入现有 late-data 失败路径

## Source, Target, and Archiving

首版不改变现有边界：

- `source` 接收上游最终输出 batch
- `target` 发布当前引擎最终输出 batch
- `parquet_sink(write_output=True)` 归档最终输出 batch
- `parquet_sink(write_input=True)` 归档原始输入 batch

首版不新增：

- 中间桶状态归档
- 截面 evaluator 中间结果归档

## Determinism

首版可复现性依赖以下硬规则：

- 输入按批次内顺序读取
- 同桶内同 `id` 仅保留最后一条
- 输出行按 `id` 稳定排序
- 输出列按 factor 声明顺序稳定追加
- 看到更大 bucket 后立即关闭旧 bucket
- 已关闭 bucket 的数据按 late data 处理

只要输入事件顺序一致，输出就应一致。

## Integration with Existing Engines

首版最自然的链路是：

```text
ReactiveStateEngine -> TimeSeriesEngine -> CrossSectionalEngine
```

用途：

- `ReactiveStateEngine`：时间序列状态特征
- `TimeSeriesEngine`：bar 或时间桶聚合
- `CrossSectionalEngine`：同时间桶截面标准化/排名

首版不在 `CrossSectionalEngine` 内再次引入：

- `pre_factors`
- `post_factors`
- 字符串表达式

## Testing

首版至少需要以下测试：

1. 同桶内同一 `id` 多次出现时，仅保留最后一条
2. 看到更大 bucket 时旧 bucket 立刻产出
3. `flush()` 产出当前未关闭 bucket
4. 已关闭 bucket 的晚到数据返回 `LateData`
5. `CS_RANK` ties 使用 average rank
6. `CS_ZSCORE` 零方差输出 `0.0`
7. null 行不参与样本，但该行输出 null
8. 输出行按 `id` 稳定排序
9. `source=TimeSeriesEngine` 级联端到端测试
10. `ZmqPublisher/ZmqSubscriber` roundtrip 测试
11. `ParquetSink(write_output=True)` 输出 schema 测试

## Milestone

建议首个实现里程碑只包含：

- Rust `CrossSectionalEngine`
- `CS_RANK`
- `CS_DEMEAN`
- `CS_ZSCORE`
- Python 基本绑定
- `source=TimeSeriesEngine`
- `NullPublisher/ZmqPublisher`
- Rust/Python 端到端测试

以下能力延后：

- 动态 universe
- `group_by`
- industry neutral
- 表达式接入 `CrossSectionalEngine`
- Python UDF
- snapshot/recovery

## Conclusion

这份首版设计的核心取舍只有一句话：

- 先把 `CrossSectionalEngine` 做成“事件时间驱动、latest_timestamp_only、无补齐、强可解释”的最小闭环，再考虑更复杂的 universe 与分组语义。
