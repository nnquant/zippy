# Master Bus Benchmark Design

## 背景

`zippy-master` 数据面已经从 `seq_*.ipc` 文件批次迁移到 shm frame ring。当前缺的不是更多接口，而是一组稳定、可重复的性能基线，用来回答两个问题：

1. 共享内存 frame ring 本身的热路径开销是多少。
2. 接入 `MasterClient -> zippy-master bus -> Reader` 之后，集成层是否引入了明显额外损耗。

这次设计只补 benchmark 与性能 smoke，不改控制面协议和用户接口。

## 目标

- 为 `vendor/zippy-shm-bridge` 提供可重复运行的 `criterion` benchmark。
- 为 `zippy-master` 提供一组 bus 集成性能 smoke，覆盖 `MasterClient.write_to/read_from` 的最小 roundtrip。
- 建立后续优化可对照的基线，不在首版硬编码脆弱的性能阈值。

## 非目标

- 不做真实 OpenCTP 接入压测。
- 不做跨机压测。
- 不做长时间 soak test。
- 不为 benchmark 新增复杂报表系统。
- 不在本次 benchmark 中评估 Python 解释器开销。

## 方案概览

本次分两层：

### 1. shm bridge microbench

在 `vendor/zippy-shm-bridge` 中新增 `criterion` 基准，只测共享内存 frame ring 热路径。

输入直接使用固定大小的 `Vec<u8>` frame，不掺入 Arrow schema 构建或 Python 调用。这样 benchmark 结果代表 ring 自身的数据面成本，而不是上层序列化和运行时成本。

### 2. master bus perf smoke

在 `zippy-master` 中新增一条非严格阈值的性能 smoke，覆盖：

- `MasterServer`
- `MasterClient.write_to`
- `Writer.write`
- `Reader.read`

这条 smoke 主要用于确认集成路径在多次 roundtrip 下行为稳定，不用于输出严格性能数字。

## benchmark 范围

### shm bridge benchmark

文件：

- `vendor/zippy-shm-bridge/benches/frame_ring.rs`

基准项：

- `publish_256b`
- `publish_4kb`
- `publish_64kb`
- `read_ready_256b`
- `read_ready_4kb`
- `read_ready_64kb`
- `seek_latest`
- `publish_read_roundtrip_4kb`

### master bus smoke

文件：

- `crates/zippy-master/tests/master_bus_perf_smoke.rs`

覆盖内容：

- 起本地 `MasterServer`
- `MasterClient.register_process`
- `MasterClient.register_stream`
- `write_to/read_from`
- 循环多次 roundtrip
- 断言全程无错误

## 输入规模

`criterion` 统一使用三档 frame 大小：

- `256 B`
- `4 KB`
- `64 KB`

理由：

- `256 B` 代表很小的 tick 或控制消息负载。
- `4 KB` 代表更现实的小批次 frame。
- `64 KB` 代表接近较大 frame 的负载。

## 指标

依赖 `criterion` 默认输出：

- `ops/s`
- `ns/op` 或 `us/op`
- 稳定样本统计

不额外自定义复杂指标体系。

对 `master bus perf smoke`：

- 只打印总轮次与总耗时
- 不做硬阈值断言

## 实现要求

### shm bridge benchmark

- setup 只做一次，不在每轮 benchmark 内重新创建 shm 布局。
- benchmark 闭包内只做目标操作：
  - `publish`
  - `read`
  - `seek_latest`
- `read_ready_*` 基准必须确保读取的是已发布 frame，而不是 `Pending` 路径。
- `publish_read_roundtrip_4kb` 必须包含一次 publish 和对应 read。

### master bus perf smoke

- 只测试 Rust 侧链路，不引入 Python。
- 不把 benchmark 逻辑塞进普通功能测试文件中。
- 循环次数固定且适中，避免 CI 过慢。
- 不做 flaky 的时间阈值断言。

## 文件变更

- 修改 `vendor/zippy-shm-bridge/Cargo.toml`
  - 增加 `criterion` benchmark 配置
- 新增 `vendor/zippy-shm-bridge/benches/frame_ring.rs`
- 新增 `crates/zippy-master/tests/master_bus_perf_smoke.rs`

如有必要，可少量修改 `crates/zippy-master/tests` 里的共享测试辅助，但不扩散到生产代码。

## 运行方式

### 运行 shm bridge benchmark

```bash
cargo bench --manifest-path vendor/zippy-shm-bridge/Cargo.toml
```

### 运行 master bus smoke

```bash
cargo test -p zippy-master --test master_bus_perf_smoke -v -- --nocapture
```

## 验证标准

完成标准：

1. `criterion` benchmark 全部能跑通。
2. `master bus perf smoke` 全部通过。
3. 不引入新的 `clippy -D warnings` 失败。
4. 不修改现有用户接口语义。

## 风险与约束

### 风险 1：把序列化开销和 ring 开销混在一起

控制方式：

- shm bridge benchmark 只测原始 bytes。
- master smoke 只作为集成验证，不拿来解释纯 ring 成本。

### 风险 2：benchmark 结果不稳定

控制方式：

- 只在 `criterion` 中做 microbench。
- 不设置脆弱的固定阈值。

### 风险 3：benchmark 影响 CI 时间

控制方式：

- `criterion` 作为显式 `cargo bench` 运行，不塞进默认 `cargo test` 主路径。
- smoke 测试轮次保持适中。

## 后续扩展

这次完成后，后续可以自然扩展：

- `MasterClient` roundtrip 的 `criterion` benchmark
- Arrow IPC bytes 负载下的 bus benchmark
- 多 reader 情况下的读性能对比
- `openctp` 实际 tick 回放压测
