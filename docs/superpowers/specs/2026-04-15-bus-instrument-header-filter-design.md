# Bus Instrument Header Filter Design

## 背景

当前 `zippy-master` bus 的共享内存 ring 只承载原始 Arrow IPC frame。下游消费者即使只关心少量
`instrument_id`，也必须逐帧读取、逐帧解码 Arrow、再在消费端判断是否命中目标合约。

对 Python 消费者来说，这条路径的主要开销包括：

- 从共享内存读取 frame payload
- 将 Arrow IPC payload 解码为 `RecordBatch`
- 将 Rust 侧 batch 转换为 Python `pyarrow` 对象

在“目标 instrument 很少、非目标 frame 很多”的场景下，这种模式效率很低。当前需求是为 bus 增加
原生过滤能力，使 reader 能在进入 Arrow 解码前跳过绝大多数无关 frame。

## 目标

- 为 bus frame 增加轻量级 instrument 目录头，使 reader 可在解码前判断该 frame 是否可能命中目标
  `instrument_id`
- 保持现有单 stream 对应单 ring 的模型，不引入新的 bus 分片或子 ring
- 在 reader attach 时固定声明过滤条件，运行中不支持动态更新
- 对不带过滤条件的 reader 保持现有行为不变
- 为后续更强的 bus 内索引方案保留演进空间

## 非目标

- 不在第一阶段实现 bus 级 `instrument_id -> seq` 二级索引
- 不在 bus 层对 batch 做按行裁剪
- 不支持 reader 运行时更新过滤条件
- 不追求第一阶段零拷贝优化

## 设计概览

bus 写入的 frame 从“纯 Arrow IPC payload”升级为“frame header + Arrow IPC payload”。

新注册并需要支持过滤订阅的 stream，统一使用该 envelope 格式。历史 stream 保持 legacy 格式不变，
仅支持无过滤读取。

reader 行为分为两类：

- 无过滤 reader：行为与现有一致，读取 frame 后直接解码 Arrow payload
- 带过滤 reader：先解析 frame header 中的 instrument 目录；若不命中则直接跳过该 seq，若命中再对
  Arrow payload 执行解码

这个设计的关键收益是：在高比例不命中的场景下，绝大多数 frame 不再进入 Arrow 解码和 Python 转换。

## Frame Envelope

### 总体格式

```text
[magic][version][flags][header_len][payload_offset][instrument_count][instrument entries...][arrow payload]
```

### 字段定义

- `magic`
  用于识别新 frame envelope，避免与历史“纯 Arrow payload”格式混淆。
- `version`
  envelope 协议版本，初始值为 `1`。
- `flags`
  按 bit 位表示 frame 特性。
  - bit 0: `has_instrument_directory`
  - bit 1: `legacy_arrow_only`
- `header_len`
  header 总长度，便于后续增加字段时保持向前兼容。
- `payload_offset`
  Arrow payload 起始位置。
- `instrument_count`
  当前 frame 中去重后的 `instrument_id` 数量。
- `instrument entries`
  每个 entry 使用 `u16 length + utf8 bytes` 编码，按出现值去重后写入。
- `arrow payload`
  原有 Arrow IPC stream payload，不修改现有 Arrow 编解码逻辑。

### 目录语义

instrument 目录表达的是“该 batch 中可能包含哪些 instrument”，而不是“如何从该 batch 中裁出目标行”。

因此：

- header 命中时，reader 返回整个 batch
- header 不命中时，reader 直接跳过该 frame
- 第一阶段不在 bus 层做按行过滤

例如一个 batch 同时包含 `IF2506` 与 `IH2506`，当 reader 订阅 `IF2506` 时，该 frame 命中并返回整批
数据，而不是只返回 `IF2506` 的子集。

## 写路径设计

### instrument 提取

`Writer.write(batch)` 在生成 Arrow IPC payload 之前，先从 batch 中提取 `instrument_id` 列的去重值。

约束如下：

- 流 schema 必须包含 `instrument_id`
- `instrument_id` 列类型必须可稳定转换为 UTF-8 文本
- 空 batch 允许写入，此时 `instrument_count = 0`

如果 schema 不包含 `instrument_id`，则对使用 envelope 的 stream 直接报错，不允许静默跳过目录生成。

原因是过滤能力依赖该列，如果允许无目录写入，会让过滤语义变得不确定。

### frame 写入

写入过程为：

1. 从 batch 提取去重后的 `instrument_id`
2. 生成 frame header
3. 编码 Arrow payload
4. 将 `header + payload` 作为单个 frame 写入共享内存 ring

对应地，stream 的 `frame_size` 需要覆盖“header 最大长度 + Arrow payload 长度”的总和。

## 读路径设计

### reader attach

过滤条件在 attach reader 时固定指定，不支持运行中更新。

控制面需要为 attach 请求新增可选 `instrument_ids` 字段。未提供时表示无过滤 reader。

### reader read

带过滤 reader 的读流程：

1. 从 ring 读取 frame bytes
2. 解析 frame header
3. 判断 header 中的 instrument 集合是否与 reader filter 有交集
4. 若无交集，推进 `next_read_seq` 并继续读取下一帧
5. 若有交集，仅对 payload 区域执行 Arrow 解码并返回 batch

无过滤 reader 的读流程：

- 可直接走现有逻辑
- 或统一复用 envelope 解析逻辑，但不做 instrument 匹配

第一阶段重点是减少 Arrow 解码，不修改 `SharedFrameRing` 的内存读取方式。当前实现仍会先把整帧 payload
复制到 `Vec<u8>`，但这部分成本明显低于对每个无关 frame 执行 Arrow IPC 解码。

## 控制面与 API

### 控制面协议

对 `AttachStreamRequest` 增加可选字段：

```json
{
  "stream_name": "ticks",
  "process_id": "proc_x",
  "instrument_ids": ["IF2506", "IH2506"]
}
```

对 `ReaderDescriptor` 增加回显字段，用于调试和观测：

- `instrument_filter: Option<Vec<String>>`

### Rust API

推荐保留原接口，并增加显式过滤版本：

```rust
read_from("ticks")
read_from_filtered("ticks", vec!["IF2506".to_string(), "IH2506".to_string()])
```

这样可以避免无过滤调用出现语义变化，同时让过滤订阅更显式。

### Python API

Python 直接扩展现有接口为可选参数形式：

```python
master.read_from("ticks")
master.read_from("ticks", instrument_ids=["IF2506", "IH2506"])
```

不新增新的 Python 方法名，减少迁移成本。

## 兼容策略

### legacy stream

历史 stream 中的 frame 只有 Arrow payload，没有 instrument header。对这类 stream：

- 无过滤 reader：继续允许读取
- 带过滤 reader：直接报错

不允许静默降级为“解 Arrow 后再过滤”，原因如下：

- 用户启用过滤能力的核心目标就是规避 Arrow 解码成本
- 静默降级会制造性能假象，难以排查
- 对量化系统而言，显式失败比隐式降级更安全

### 版本演进

通过 `magic + version + header_len` 保证 envelope 后续可扩展，例如未来增加：

- 哈希目录
- Bloom filter
- 扩展标签列
- 更紧凑的 instrument 编码

## mixed-instrument batch 语义

必须明确以下规则：

- 过滤条件是“frame 级命中”，不是“行级命中”
- 一个 frame 只要包含任一目标 instrument，就返回整个 batch
- 业务侧如需进一步裁行，应在消费后自行处理

这个语义保持了 bus 的传输职责边界，避免将 bus 演化为数据重写层。

## 错误处理

以下情况应返回明确错误：

- 带过滤 reader 读取 legacy stream
- frame header 魔数非法
- frame version 不支持
- `header_len` 与 `payload_offset` 非法
- header 声称存在 instrument 目录，但目录内容损坏
- 开启 envelope 的 stream 写入 batch 时缺少 `instrument_id` 列

错误消息应遵循现有日志规范，使用简洁英文并附上关键变量值。

## 测试计划

至少覆盖以下场景：

- 无过滤 reader 读取新格式 frame，行为与现有一致
- 带过滤 reader 遇到不命中的 frame，可连续跳过并继续等待下一命中帧
- 带过滤 reader 命中单 instrument frame，返回正确 batch
- 一个 batch 包含多个 instrument，命中任一 instrument 均返回整个 batch
- 空 batch 的目录生成与读取
- 缺少 `instrument_id` 列时的写入错误
- 带过滤 reader 订阅 legacy stream 时返回明确错误
- Python `read_from(..., instrument_ids=...)` 可正确创建过滤 reader

性能 smoke test 需要覆盖两类工况：

- 大比例不命中：验证 CPU 和端到端耗时相对现状明显下降
- 高命中率：验证 header 解析带来的额外开销在可接受范围内

## 演进路线

### 第一阶段

- 引入 frame envelope
- reader 支持基于 header 的固定 instrument 过滤
- Python API 支持 attach 时传入 `instrument_ids`

### 第二阶段

视压测结果决定是否继续优化：

- ring 只读 header，避免无关 frame 的整帧复制
- 进一步消除 `decode_batch()` 中的多余拷贝

### 第三阶段

如果后续 reader 数量很多、且“扫描所有 frame”本身成为瓶颈，再演进到 bus 内部维护
`instrument_id -> seq` 的二级索引方案。第一阶段的 header 目录可以直接复用为索引构建输入。

## 决策总结

本设计选择方案 3，即“轻量 header + 命中前不解 Arrow”，原因是：

- 当前主要热点是 Arrow 解码与 Python 转换，而非 header 扫描
- 改动范围集中，能快速交付收益
- 不破坏现有 stream/ring 模型
- 为未来更强的 bus 原生索引能力保留演进空间
