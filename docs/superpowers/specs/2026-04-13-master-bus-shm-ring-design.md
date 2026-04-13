# Zippy Master Bus SHM Ring Design

## Goal

将 `zippy-master` 的数据面从当前基于 `seq_*.ipc` 批次文件的伪总线，迁移为真正的共享内存 ring。

目标如下：

- 完全移除 `seq_*.ipc` 文件批次热路径
- 每个逻辑 `stream` 对应一段真正的共享内存
- 数据面保持单写者、多读者、每个 reader 独立游标
- 数据面热路径必须 lock-free
- 控制面继续保留 `zippy-master + MasterClient`
- 保持现有 `write_to/read_from` 上层使用方式基本不变

本设计优先解决：

- 单机多进程 tick 数据中心
- 本机低延迟流传输
- 避免文件系统写放大和目录扫描
- 为后续高吞吐 stream 计算链路提供正确的数据面基础

## Scope

### 包含

- 完全移除 `seq_*.ipc` 文件批次数据面
- 每个 stream 一个 shm ring
- 每个 shm ring 使用固定数量 frame
- 共享内存热路径 lock-free
- 单写者
- 多读者
- 每个 reader 独立读游标
- `buffer_size` 作为 frame 数量
- `frame_size` 作为单个 frame payload 区大小
- `Writer.write(batch)` 直接写 shm frame
- `Reader.read()` 直接从 shm frame 读
- `Reader.seek_latest()`
- lagged reader 显式失败
- 控制面 descriptor 和 stream metadata 改为 `buffer_size/frame_size`
- 现有 `StreamTableEngine -> bus`
- 现有 `bus -> ReactiveStateEngine`

### 不包含

- 控制面改造成无锁
- 多写者
- frame 跨槽分片
- 动态扩容 shm ring
- 数据面 snapshot/replay
- reader offset 恢复
- 自动重放历史 batch
- Arrow C Data Interface 零拷贝方案
- 多机数据面

## Motivation

当前 `master bus` 的数据面虽然逻辑上叫 bus/ring，但热路径实际上仍然是：

1. writer 把 `RecordBatch` 编码成 Arrow IPC bytes
2. 写入 `seq_*.ipc.tmp`
3. rename 成 `seq_*.ipc`
4. reader 扫目录
5. reader 读取文件
6. reader 再做 IPC 解码

这条路径的问题是结构性的：

1. 文件系统开销重  
每个 batch 都要写文件、rename、扫目录、删旧文件。

2. 不是 lock-free  
热路径依赖文件系统操作，而不是共享内存与原子操作。

3. 对 tick 场景不适合  
在 `rows_per_batch=1` 或小 batch 场景下，会显著增加延迟和抖动。

4. 和前面 `master/bus` 设计目标不一致  
我们已经明确将其设计为本机共享内存总线，继续沿用文件批次会把后续性能和一致性工作都拖偏。

因此这次迁移不是优化，而是把数据面从“临时实现”纠正成“正确实现”。

## Core Principles

- 控制面和数据面继续严格分离
- 数据面只服务于单机多进程
- 数据面热路径 lock-free
- 只支持单写者
- 多 reader 互不影响
- 新 reader 默认从最新位置之后开始读取
- 慢 reader 落后被覆盖时必须显式失败
- 不为 V1 数据面迁移保留文件 fallback
- 恢复边界继续维持“只恢复控制面，不恢复数据面”

## Terminology

本设计统一采用以下术语：

- `buffer_size`
  - ring 中 frame 的数量
- `frame_size`
  - 单个 frame payload 区允许的最大字节数
- `frame`
  - ring 中的单个固定大小写入单元
- `payload`
  - 写入某个 frame 的 Arrow IPC bytes

原有设计中的：

- `slot_count` -> `buffer_size`
- `slot_payload_bytes` -> `frame_size`

后续控制面、descriptor、文档、CLI 均应统一使用 `buffer_size/frame_size/frame`。

## Data Plane Architecture

每个 stream 对应一个 shm 区域，布局固定。

```text
[global header]
[frame header 0][frame header 1]...[frame header N-1]
[payload 0][payload 1]...[payload N-1]
```

### Global Header

建议最小字段：

- `magic`
- `layout_version`
- `buffer_size`
- `frame_size`
- `write_seq`
- `writer_attached`

其中：

- `write_seq` 是全局单调递增的最新已发布序号
- `writer_attached` 仅作为调试/观测辅助，不作为控制面唯一真相

### Frame Header

每个 frame 最少包含：

- `seq`
- `payload_len`
- `state`

`state` 最少定义：

- `EMPTY`
- `WRITING`
- `READY`

首版不强制引入 checksum。  
后续如果需要排障增强，可在 frame header 追加 `checksum`，但不作为本设计范围。

### Frame Payload

每个 frame 有固定大小 payload 区，大小为 `frame_size`。  
一个 `RecordBatch` 编码后的 Arrow IPC bytes 必须完整落在一个 frame 内，不允许跨 frame 分片。

如果单个 batch 编码后超过 `frame_size`：

- `Writer.write(batch)` 必须直接返回错误
- 错误中必须带：
  - `stream_name`
  - `payload_bytes`
  - `frame_size`

## Lock-Free Semantics

### 热路径约束

只有数据面热路径必须 lock-free：

- writer 写 frame
- reader 读 frame

控制面不要求 lock-free：

- `register_stream`
- `write_to`
- `read_from`
- registry
- heartbeat / lease
- snapshot

这些仍可继续使用锁。

### 原子字段

共享内存数据面至少使用以下原子字段：

- global header:
  - `write_seq: AtomicU64`
- frame header:
  - `seq: AtomicU64`
  - `payload_len: AtomicU32`
  - `state: AtomicU8`

### Writer Publish Order

writer 写单个 frame 的顺序必须固定：

1. 根据目标 `target_seq` 计算 `frame_index = target_seq % buffer_size`
2. `state = WRITING`
3. 拷贝 payload bytes 到 frame payload 区
4. `payload_len = len`
5. `seq = target_seq`
6. `state = READY`
7. 更新 global `write_seq`

reader 只认 `state == READY` 且 `seq == next_read_seq` 的 frame。

这样即使 writer 在中途崩溃：

- 未完成 frame 仍停留在 `WRITING`
- reader 不会把半帧当成有效数据

### Reader Read Semantics

reader 拿本地 `next_read_seq` 计算目标 frame：

1. 读取 frame header
2. 判断：
   - `state == READY && seq == next_read_seq`
     - 读取 payload
     - 解码 Arrow IPC
     - 本地 `next_read_seq += 1`
   - `seq < next_read_seq`
     - writer 还没写到目标序号，等待或超时
   - `seq > next_read_seq`
     - 目标序号已被覆盖，reader lagged

reader 不修改共享写指针，也不修改其他 reader 状态。

### Reader Join Semantics

新 reader 调用 `read_from(stream_name)` 时：

- 默认 `next_read_seq = current_write_seq + 1`
- 因此不会回放历史 frame
- 只会读到自己加入之后的新数据

这是本次迁移必须保证的硬语义。

### Lagged Reader Semantics

如果 reader 需要的 `next_read_seq` 已经被覆盖：

- `read()` 直接返回显式 lagged 错误
- 不能静默跳过
- reader 可显式：
  - `seek_latest()`
  - 或重新 `read_from(stream_name)`

`seek_latest()` 的语义：

- 将本地 `next_read_seq` 设置到 `current_write_seq + 1`

## Stream Configuration

创建 stream 时，控制面必须显式提供：

- `buffer_size`
- `frame_size`

不再继续使用单一 `ring_capacity` 模糊表示 ring 配置。

### Recommended Defaults

对 tick 类 stream，推荐首版默认值：

- `buffer_size = 131072`
- `frame_size = 65536`

这不是全局最优值，只是足够稳的默认起点。  
实际部署时仍应按：

- schema 列数
- 字符串列长度
- source `rows_per_batch`
- 目标延迟与容忍积压深度

进行调优。

### Fixed-at-Creation Rule

`buffer_size` 和 `frame_size` 在 stream 创建后固定：

- 不支持运行时修改
- 不支持在线 resize

因为这两个值直接决定 shm layout，在线修改会使一致性复杂度失控。

## API Compatibility

### 保持不变

对上层用户与 engine 而言，以下接口语义应尽量保持不变：

- `MasterClient.write_to(stream_name) -> Writer`
- `MasterClient.read_from(stream_name) -> Reader`
- `Writer.write(batch)`
- `Writer.flush()`
- `Writer.close()`
- `Reader.read(timeout_ms)`
- `Reader.seek_latest()`
- `Reader.close()`

也就是说，上层 `StreamTableEngine`、`ReactiveStateEngine`、`openctp` examples 的使用模式不应因为数据面迁移而整体重写。

### 必须变化

以下数据结构和控制面字段必须从旧语义迁到新语义：

1. `RegisterStream`
- 从 `ring_capacity`
- 改为：
  - `buffer_size`
  - `frame_size`

2. `StreamInfo`
- 返回：
  - `buffer_size`
  - `frame_size`
- 不再继续暴露旧的 `ring_capacity`

3. `WriterDescriptor` / `ReaderDescriptor`
- 必须携带：
  - `stream_name`
  - `buffer_size`
  - `frame_size`
  - `layout_version`
  - `shm_path` 或等价 shm 标识
  - `writer_id` / `reader_id`
  - `next_write_seq` / `next_read_seq`

4. CLI 与文档
- `stream ls/show`
- examples
- README
- 全部改成 `buffer_size/frame_size`

## Recovery Semantics

本次迁移后，恢复边界保持不变：

- 只恢复控制面
- 不恢复数据面

### master Restart

`master` 重启后：

- 恢复 `stream/source/engine/sink` 控制面元数据
- 恢复状态仍记为 `restored`
- 不恢复：
  - frame 内容
  - `write_seq`
  - reader 位点
  - 活跃 writer/reader attach

### writer Crash

writer 崩溃后：

- 由 lease timeout 回收 writer attach
- 新 writer 重新 `write_to(stream)` 后，从最新可写位置继续
- 未完成 frame 由于仍处于 `WRITING`，不会被 reader 读取

### reader Crash

reader 崩溃后：

- lease timeout 回收 reader attach
- 其他 reader 不受影响
- 新 reader 再加入时，继续从最新位置之后开始读

## Migration Plan

### Phase 1: Introduce SHM Frame Ring

首先在底层实现新的 shm frame ring：

- `zippy-master::ring`
- `zippy-master::bus`
- `zippy-core::Writer/Reader`
- descriptor 字段扩展

先做通：

- 单写者
- 多读者
- lock-free 热路径
- 新 reader 从最新开始
- lagged 显式失败

### Phase 2: Remove File-Based Data Plane

完成后直接移除旧方案：

- `Writer.write()` 不再写 `seq_*.ipc`
- `Reader.read()` 不再扫目录
- `next_storage_sequence(...)`
- `parse_seq_file_name(...)`
- 旧文件批次相关逻辑全部删除

不保留文件 fallback。

### Phase 3: Align Control Plane

统一把控制面语义改正：

- `ring_capacity` -> `buffer_size`
- 新增 `frame_size`
- Python 绑定、CLI、tests、examples 全部对齐

### Phase 4: Revalidate Main Pipelines

至少回归两条主链：

1. `StreamTableEngine -> bus stream`
2. `bus stream -> ReactiveStateEngine`

这是本次迁移的最低业务验收线。

## Testing Matrix

### Ring Basics

- writer 写一个 frame，reader 正常读到
- 新 reader 加入后从最新开始，不读旧 frame
- writer 关闭后可重新附着
- 多 reader 同时读取互不影响

### Consistency

- writer 崩在 `WRITING` 状态，reader 不读取半帧
- `payload_bytes > frame_size` 返回明确错误
- 覆盖后 reader 返回 lagged

### Control Plane

- `register_stream(buffer_size, frame_size)` 正常
- duplicate register 幂等仍成立
- `stream ls/show` 返回新字段

### End-to-End

- `StreamTableEngine -> bus -> direct reader`
- `bus -> ReactiveStateEngine -> bus`
- Python API 和 CLI 路径都通过

### Performance Smoke

至少补一条本机持续写读 smoke：

- 目标不是这次就定最终性能结论
- 而是确认：
  - 数据面已经不再依赖文件系统热路径
  - 不出现明显锁竞争或异常退化

## Risks

1. `frame_size` 配小后，真实 batch 超限  
需要通过明确错误暴露，而不是静默截断或回退。

2. 原子发布顺序实现不当  
会导致 reader 看到脏 frame。  
这部分必须靠单元测试和并发测试锁死。

3. 控制面字段迁移不彻底  
如果仍残留 `ring_capacity` 语义，后续用户和示例会混乱。

4. 恢复边界被误扩展  
本次不做数据面恢复，不能在实现里偷偷引入半恢复状态。

## Success Criteria

满足以下条件即认为本次设计成功：

- `seq_*.ipc` 文件数据面被彻底移除
- 新 reader 默认只读新数据，不回放历史
- writer/reader 热路径不再依赖文件系统
- 数据面使用真正的 shm frame ring
- 数据面热路径 lock-free
- `StreamTableEngine -> bus -> ReactiveStateEngine` 主链继续可用
- `stream` 控制面配置统一采用 `buffer_size/frame_size`
