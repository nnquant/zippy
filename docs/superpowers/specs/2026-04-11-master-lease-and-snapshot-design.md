# Master Lease And Snapshot Design

## Summary

`zippy-master v1` 已经具备本机控制面和 bus 管理能力，但仍缺少两项关键能力：

- worker 异常退出后的脏 `writer/reader` 自动回收
- `master` 重启后的控制面元数据恢复

本设计定义 `zippy-master v2` 的最小恢复能力范围：

- 引入 **process lease / heartbeat**
- 引入 **registry snapshot**
- 只恢复 **控制面元数据**
- 不恢复 bus 数据、reader 位点或 shm ring 内容

## Goals

- 在 worker 进程失联后自动回收脏 `writer/reader`
- 保留 `stream/source/engine/sink` 元数据，避免 `master` 重启后拓扑丢失
- 为后续更强的恢复与编排能力打基础

## Non-Goals

本次明确不做：

- shm ring 数据恢复
- `write_seq/read_seq` 恢复
- reader 位点恢复
- 自动重新附着 writer/reader
- 自动重放或 replay
- 多机部署
- HA / 主从切换

## Approach Options

### Option 1: snapshot + heartbeat lease

做法：

- `master` 周期性维护 process heartbeat lease
- registry 变更时写控制面快照文件
- 重启时读取快照恢复 `stream/source/engine/sink`

优点：

- 与当前单机 Unix domain socket 架构最匹配
- 实现边界清楚
- 足以解决当前最关键的可运维性缺口

缺点：

- 不能恢复数据面状态
- 不能恢复活跃 reader/writer

### Option 2: append-only journal

做法：

- 控制面每次变更追加事件日志
- 重启时重放 journal 重建 registry

优点：

- 审计和恢复更强

缺点：

- 版本化、幂等和重放复杂度明显更高
- 超出 V2 需要解决的问题

### Option 3: lease only

做法：

- 只做 heartbeat 和脏句柄清理
- 不做 registry 持久化

优点：

- 实现最快

缺点：

- `master` 重启后仍然丢拓扑
- 只解决一半问题

## Recommendation

选择 **Option 1: snapshot + heartbeat lease**。

原因：

- 能覆盖当前最关键的两类故障
- 实现成本可控
- 不会过早把系统推向复杂的事件溯源架构

## Architecture

### Control Plane

`zippy-master` 继续作为唯一常驻 daemon，新增两套机制：

1. `ProcessLeaseManager`
- 记录 process 注册时间、最近 heartbeat 时间、lease 状态
- 周期性扫描过期 process

2. `RegistrySnapshotStore`
- 将控制面 registry 原子写入本地 snapshot 文件
- `master` 启动时先尝试加载

### Data Plane

`zippy-bus` 仍然只负责每个 stream 的 shm/ring 数据面，不承担恢复逻辑。

V2 中：

- 数据面 handle 不持久化
- writer/reader 附着关系不持久化
- 遇到 process 失联时，只做 detach，不做恢复

## Persisted State

### Persisted

持久化以下控制面实体：

1. `streams`
- `stream_name`
- `ring_capacity`
- `schema`
- `status`

2. `sources`
- `source_name`
- `source_type`
- `config`
- `output_stream`
- `desired_state`
- 最近一次已知状态

3. `engines`
- `engine_name`
- `engine_type`
- `config`
- `input_stream`
- `output_stream`
- `sink_names`
- `desired_state`
- 最近一次已知状态

4. `sinks`
- `sink_name`
- `sink_type`
- `config`
- `input_stream`
- 最近一次已知状态

### Not Persisted

以下内容明确不持久化：

- `process` 记录
- writer/reader handle
- `write_seq/read_seq`
- shm ring 内容
- bus 数据文件内容

## Recovery Semantics

`master` 启动恢复 snapshot 后：

- `stream/source/engine/sink` 会重新出现在 registry 中
- 这些对象的恢复状态统一标成 `restored`
- 不允许恢复后仍显示 `running`

恢复的含义是：

- 配置和拓扑被恢复
- 活跃运行态没有被恢复
- 活着的 worker 需要重新注册 process 并重新附着

## Process Lease Model

### Process Record

`ProcessRecord` 增加：

- `registered_at`
- `last_heartbeat_at`
- `lease_status`

`lease_status` 最小集合：

- `alive`
- `expired`

### Timing

V2 默认值：

- `heartbeat_interval = 2s`
- `lease_timeout = 10s`

### Heartbeat API

新增控制面请求：

- `Heartbeat(process_id)`

语义：

- 如果 process 存在且 lease 有效，则更新 `last_heartbeat_at`
- 如果 process 不存在，返回错误

### Lease Expiration Handling

当 process lease 超时：

1. detach 该 process 持有的所有 writer
2. detach 该 process 持有的所有 readers
3. 将该 process 名下的 `source/engine/sink` 状态改为 `lost`
4. 记录结构化日志：
   - `process_lease_expired`
   - `writer_reclaimed`
   - `reader_reclaimed`

### Why Process Records Are Not Restored

`process` 是瞬时实体，不具备稳定恢复语义：

- pid 会变
- 旧 `process_id` 不再可信
- 活着的 worker 应在恢复后重新注册

因此：

- snapshot 中不存 `process`
- 恢复后所有活跃租约从空开始

## Snapshot Format

首版采用 JSON 文件。

建议默认路径：

- `~/.zippy/master-registry.json`

写入方式：

1. 写到临时文件
2. `fsync` 临时文件
3. rename 覆盖正式文件

这样避免半写入损坏正式 snapshot。

## Master Lifecycle

### Startup

1. 初始化日志
2. 加载 snapshot
3. 用恢复出的 `stream/source/engine/sink` 构建 registry
4. 将恢复对象状态标成 `restored`
5. 启动 lease reaper
6. 开始接收控制面请求

### Shutdown

1. 停止接收新请求
2. 尽量写出最新 snapshot
3. 退出

## Runtime State Transitions

### Streams

- 新建 stream: `registered`
- writer 附着: `writer_attached`
- reader 附着: `reader_attached`
- writer + reader 并存: `active`
- 从 snapshot 恢复: `restored`

### Sources / Engines / Sinks

- 新注册但未运行: `registered`
- 正常运行: `running`
- 所属 process lease 过期: `lost`
- 从 snapshot 恢复且尚未重新附着: `restored`

## Logging

新增的关键事件：

- `snapshot_load_start`
- `snapshot_load_success`
- `snapshot_load_failure`
- `snapshot_write_success`
- `snapshot_write_failure`
- `heartbeat`
- `process_lease_expired`
- `writer_reclaimed`
- `reader_reclaimed`

日志字段至少包含：

- `component`
- `event`
- `status`
- `process_id`（如适用）
- `stream_name`（如适用）
- `message`

## Validation

至少补这些测试：

1. process 不发 heartbeat，writer 被自动回收
2. process 不发 heartbeat，reader 被自动回收
3. registry 写 snapshot，`master` 重启后 `stream/source/engine/sink` 恢复
4. 恢复后的状态是 `restored`，不是 `running`
5. 恢复后的 stream 允许新 writer 重新附着

## Rollout Order

推荐按以下顺序实现：

1. heartbeat / lease
2. writer/reader reclaim
3. snapshot 写入
4. snapshot 加载
5. 恢复状态语义与查询接口对齐

## Rationale

这个顺序能把问题拆开：

- 先解决运行时脏句柄问题
- 再解决重启后元数据丢失问题

这样定位和验证都更直接，也不会把 V2 复杂度拉到 journal/replay 那一级别。
