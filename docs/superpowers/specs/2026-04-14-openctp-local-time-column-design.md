# OpenCTP Local Time Column Design

## Goal

在 `zippy-openctp` 的稳定 tick schema 中保留现有 `dt` UTC 事件时间列，同时新增一个更符合国内用户阅读习惯的本地时间列 `dt_local`，其时区固定为 `Asia/Shanghai`。

本次变更只扩展 schema，不改变已有 `dt` 的语义、字段名或类型契约。

## Scope

本次设计覆盖：

- `zippy-openctp` Rust core schema
- `zippy_openctp.schemas.TickDataSchema()`
- tick 标准化输出的 `RecordBatch`
- 对应 Rust / Python schema 契约测试

本次设计不覆盖：

- 运行时按用户配置切换时区
- 替换 `dt`
- 对历史 Parquet 数据做迁移
- 引入多地区时区视角

## Current State

当前 `zippy-openctp` 的时间语义如下：

- 原始时间来自 `action_day + update_time + update_millisec`
- 标准化阶段先按交易所本地时间 `UTC+8` 解释
- 再转换成 UTC 纳秒
- 输出列 `dt` 固定为 `timestamp(ns, UTC)`

这套语义已经被：

- Rust schema 契约
- Python `TickDataSchema()`
- `master/bus` 远端流
- Parquet 输出

共同依赖，因此不应直接改写。

## Recommended Approach

保留：

- `dt: timestamp(ns, UTC)`

新增：

- `dt_local: timestamp(ns, Asia/Shanghai)`

两列表示同一个事件时刻，只是显示时区不同。

推荐原因：

1. 不破坏现有稳定契约。
2. 保留统一 UTC 事件时间，方便系统内部排序、回放、聚合和跨源对齐。
3. 给用户和运维提供本地时区视角，减少日常观察成本。

## Schema Contract

最终时间列语义：

- `dt`
  - 类型：`timestamp(ns, UTC)`
  - 含义：标准 UTC 事件时间
- `dt_local`
  - 类型：`timestamp(ns, Asia/Shanghai)`
  - 含义：与 `dt` 对应的上海时区显示视角

字段顺序要求：

- `dt_local` 紧跟在 `dt` 后面

这样对时间相关列最直观，也能把 schema 变更收在局部。

## Normalization Strategy

标准化阶段继续只维护一个基础时间值：

- `dt_ns`

即：

1. 用 `action_day + update_time + update_millisec`
2. 按交易所本地时间 `UTC+8` 解释
3. 转成 UTC 纳秒 `dt_ns`

然后在 batch 构造阶段：

- `dt` 使用同一组 `dt_ns`，标注时区为 `UTC`
- `dt_local` 使用同一组 `dt_ns`，标注时区为 `Asia/Shanghai`

不额外维护第二份“本地纳秒值”，避免两套数值来源漂移。

## Compatibility

兼容性策略：

- 现有依赖 `dt` 的逻辑无需修改
- 新增列只会扩展 schema，不会改写旧列
- 依赖精确 schema 的下游需要重新对齐字段集合

这意味着：

- 运行在新包版本上的 `master/bus` stream schema 会包含 `dt_local`
- 新写出的 Parquet 会包含 `dt_local`
- 老数据文件不会自动补列

## Testing

至少补齐这些验证：

1. Rust schema 契约测试
   - `TickDataSchema` 包含 `dt_local`
   - `dt_local` 类型为 `timestamp(ns, Asia/Shanghai)`

2. Rust normalize / source batching 测试
   - 输出 batch 中同时存在 `dt` 和 `dt_local`
   - 两列底层纳秒值一致，仅时区标注不同

3. Python schema 测试
   - `zippy_openctp.schemas.TickDataSchema()` 返回包含 `dt_local` 的 `pyarrow.Schema`

## Rollout

实施顺序：

1. 更新 Rust schema 常量
2. 更新 Python schema 映射
3. 在 batch 构造逻辑中新增 `dt_local`
4. 更新 Rust / Python 契约测试
5. 更新 `zippy-openctp` README 或示例中的 schema 展示

## Non-Goals

以下内容明确不在本次范围内：

- `dt` 改为本地时区
- 根据用户机器本地时区动态输出
- 对 `OpenCtpMarketDataSource` 增加 `timezone` 参数
- 对已有远端流做兼容层转换
