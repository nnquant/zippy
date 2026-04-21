# OpenCTP Local Receive Time And Shanghai DT Design

## Goal

调整 `zippy-openctp` 的稳定 tick schema：

- 将 `dt` 从 `timestamp(ns, UTC)` 改为 `timestamp(ns, Asia/Shanghai)`
- 新增 `localtime_ns: int64`

其中：

- `dt` 表示交易所事件时间
- `localtime_ns` 表示当前进程收到并准备写出该条行情时采样到的本地接收时间，单位为 Unix epoch 纳秒

本次设计的目标是同时满足：

1. 让 `dt` 更符合国内用户的直接阅读习惯。
2. 提供一个可跨进程直接比较的接收时间戳，用于延迟观测、链路比对和排障。

## Scope

本次设计覆盖：

- `zippy-openctp` Rust core schema
- Python `TickDataSchema()`
- tick 标准化与 batch 构造逻辑
- Rust / Python 契约测试
- README 中涉及 tick schema 的说明

本次设计不覆盖：

- 运行时动态切换时区
- 通过 monotonic clock 暴露单机进程内单调时间
- 对历史 parquet 或旧 stream 自动迁移
- 同时保留旧版 `dt(UTC)` 兼容列

## Required Semantics

### `dt`

- 字段名保持不变：`dt`
- 类型调整为：`timestamp(ns, Asia/Shanghai)`
- 语义保持为交易所事件时间
- 底层数值仍然是 Unix epoch 纳秒，只变更 Arrow 的时区标注

这意味着：

- 原有时间组合逻辑继续基于 `action_day + update_time + update_millisec`
- 原有数值计算不需要切换到“本地纪元”
- 下游看到的 wall-clock 展示将变成北京时间

### `localtime_ns`

- 新字段名：`localtime_ns`
- 类型：`int64`
- 含义：source 线程在接收到一条原始 tick 并准备将其写入内部缓冲前，使用系统实时时钟采样得到的 Unix epoch 纳秒时间戳

约束与解释：

- 必须是跨进程可比较的时间基准，因此选择 Unix epoch 纳秒
- 不使用 monotonic clock，因为 monotonic 值通常无法跨进程直接比较
- 允许受 NTP 校时影响，因此它适合做跨进程时刻对齐和链路延迟估算，不承诺严格单调

## Recommended Architecture

推荐把“事件时间”和“接收时间”分在两个阶段生成：

1. normalize 层只负责把原始 OpenCTP 时间字段组合成事件时间纳秒值
2. source 层在真正接收到 tick 并准备进入批量缓冲时补采 `localtime_ns`
3. batch 构造层把事件时间输出为 `dt(timestamp(ns, Asia/Shanghai))`，并同时输出 `localtime_ns(int64)`

推荐原因：

1. normalize 继续保持“纯输入到纯输出”的职责，不依赖本机时钟。
2. `localtime_ns` 的采样点定义清晰，后续更容易做端到端延迟分析。
3. 事件时间与接收时间来源分离，便于测试和代码审计。

## Schema Contract

时间相关字段调整为：

- `dt`
  - 类型：`timestamp(ns, Asia/Shanghai)`
  - 含义：交易所事件时间
- `localtime_ns`
  - 类型：`int64`
  - 含义：source 写入前采样的本地接收时间，Unix epoch 纳秒

字段顺序要求：

- `localtime_ns` 紧跟在 `dt` 后面

新的局部顺序为：

- `instrument_id`
- `exchange_id`
- `trading_day`
- `action_day`
- `dt`
- `localtime_ns`
- `last_price`
- 其余行情字段

## Normalization Strategy

标准化阶段继续只维护交易所事件时间的基础数值：

- `event_time_ns`

计算规则保持为：

1. 使用 `action_day + update_time + update_millisec`
2. 按交易所本地时间 `UTC+8` 解释
3. 转成 Unix epoch 纳秒

然后：

- 在 batch 构造时，将该数值以 `timestamp(ns, Asia/Shanghai)` 的 `dt` 输出
- 在 source 接收点单独采样 `localtime_ns`

不建议在 normalize 层直接生成 `localtime_ns`，因为它不属于原始行情内容，而属于本地接收上下文。

## Compatibility Impact

这次变更是显式 schema breaking change：

1. `dt` 的时区 metadata 会从 `UTC` 变成 `Asia/Shanghai`
2. tick schema 会新增 `localtime_ns`

影响包括：

- 依赖旧版 `dt(UTC)` 的下游需要同步调整
- 新版本写出的 parquet 会包含 `localtime_ns`
- 新版本 bus stream schema 会包含 `localtime_ns`
- 旧数据文件不会自动补列

这次变更不提供兼容层，原因是用户需求已经明确要求直接把 `dt` 改为本地时区。

## Implementation Notes

### Rust Core

- 在 `schema.rs` 中新增 `TimestampNsShanghai` 枚举分支
- 将 `dt` 的 schema 类型改为 `timestamp(ns, Asia/Shanghai)`
- 在 `TICK_SCHEMA_FIELDS` 中插入 `localtime_ns`

### Row Model

- normalize 层的行结构保留事件时间字段
- source 层在进入缓冲前补充 `localtime_ns`
- 如现有 `NormalizedTickRow` 已直接被 batch 构造使用，可将其扩展为同时承载 `event_time_ns` 与 `localtime_ns`

核心要求不是结构名，而是职责边界：

- 事件时间来自原始行情字段
- 接收时间来自 source 线程采样

### Python Bridge

- Python internal schema 枚举映射需要新增 `timestamp_ns_shanghai`
- `TickDataSchema()` 需要返回 `pa.timestamp("ns", tz="Asia/Shanghai")`
- `localtime_ns` 需要暴露为 `pa.int64()`

## Testing

至少补齐以下测试：

1. Rust schema 契约测试
   - `dt` 的类型是 `timestamp(ns, Asia/Shanghai)`
   - `localtime_ns` 存在且类型为 `int64`

2. Rust normalize / batching 测试
   - 输出 batch 中存在 `dt` 与 `localtime_ns`
   - `localtime_ns > 0`
   - `dt` 的 timezone metadata 为 `Asia/Shanghai`

3. Python schema 测试
   - `TickDataSchema().field("dt").type` 为 `timestamp[ns, tz=Asia/Shanghai]`
   - `TickDataSchema().field("localtime_ns").type` 为 `int64`

测试中不应对 `localtime_ns` 断言固定值，因为它依赖运行时采样；应断言存在性、正值和合理的相对关系。

## Rollout

建议实施顺序：

1. 先补 schema 契约测试
2. 让测试在当前实现上失败
3. 更新 Rust schema 与 Python schema 映射
4. 在 source batching 路径补 `localtime_ns`
5. 更新 README 与示例说明
6. 跑 Rust / Python 相关测试确认契约收敛

## Non-Goals

以下内容明确不在本次范围内：

- 增加 `dt_utc`
- 增加 `dt_local`
- 提供用户可配置时区
- 使用单调时钟替代 `localtime_ns`
- 对旧 parquet 做离线重写
