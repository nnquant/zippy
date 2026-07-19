# Stage 7.1 原生写入权威性能回归

日期：2026-07-19

## 目标与边界

本报告验证 Stage 7.1 把兼容写入迁移到 `zippy server` 所有的统一 memory ingest runtime 后，
Gateway 与 Flight 是否仍满足既有协议性能门禁。被测提交为
`ac5999dfafea8be62e1148694c7a815846132d7d`，对照基线为 Stage 7.0 的
`9967f226b1866ba98a3106a3ac219cb5125ca288`。

本阶段改变的是写入所有权和服务生命周期，不改变 ACK 的持久性语义。Gateway 与 Flight 共享同一个
`MemoryIngestHandle`，由 server 负责启动、准入、排空和关闭；成功回执仍是 process-local memory ACK，
不代表 WAL 已落盘。producer fencing、幂等序列、稳定 offset、崩溃恢复和 durable ACK 属于 Stage 7.2。

## 测试方法

基准启动真实 `LegacyServerRuntime`、Gateway TCP listener 和 Flight gRPC listener。查询路径共享
SQLite shadow Catalog、Parquet 数据和 native query engine，写入路径共享 server-owned ingest
authority。每个场景交替执行两条协议，并校验查询行数、写入回执以及统一 ingest 计数闭合。

- 源数据：262,144 行；
- 查询与 memory-ACK 写入规模：1、1,024、65,536 行；
- 每个场景：10 次预热、100 次计量；
- 环境：WSL2 本机 loopback、release profile、单 benchmark 进程、无 TLS。

执行命令：

```text
CARGO_TARGET_DIR=/tmp/zippy-target-stage7 \
ZIPPY_TRANSPORT_PERF_SOURCE_ROWS=262144 \
ZIPPY_TRANSPORT_PERF_WARMUP_SAMPLES=10 \
ZIPPY_TRANSPORT_PERF_MEASURED_SAMPLES=100 \
ZIPPY_TRANSPORT_PERF_REPORT_JSON=/tmp/zippy-transport-stage7-1-final.json \
cargo test --release -p zippy-server --test server_runtime \
  'transport_performance::flight_gateway_transport_performance_baseline' -- \
  --ignored --exact --nocapture --test-threads=1
```

原始报告和环境信息位于
[`evidence/2026-07-19-native-ingest-authority`](evidence/2026-07-19-native-ingest-authority)。

## 最终结果

| 场景 | Gateway p50 / p99 | Flight p50 / p99 | Flight 吞吐比 | Flight p99 比 |
| --- | ---: | ---: | ---: | ---: |
| 查询 1 行 | 3.826 / 4.771 ms | 4.099 / 5.374 ms | 0.928 | 1.126 |
| 查询 1,024 行 | 3.833 / 5.068 ms | 4.134 / 4.911 ms | 0.928 | 0.969 |
| 查询 65,536 行 | 8.067 / 10.101 ms | 8.097 / 11.215 ms | 0.985 | 1.110 |
| 写入 1 行 | 0.769 / 1.043 ms | 0.559 / 0.816 ms | 1.358 | 0.782 |
| 写入 1,024 行 | 0.832 / 1.184 ms | 0.630 / 0.838 ms | 1.302 | 0.708 |
| 写入 65,536 行 | 6.383 / 8.962 ms | 5.629 / 8.390 ms | 1.186 | 0.936 |

Flight read session 建立耗时 1.230 ms，writer session 建立耗时 0.895 ms。65,536 行查询的
Flight/Gateway 吞吐分别为 60.462/61.400 MiB/s；65,536 行写入分别为
83.768/70.657 MiB/s。

自动门禁要求每个场景的 Flight 吞吐不低于 Gateway 的 90%，且 Flight p99 不高于 Gateway 的
1.20 倍。六个场景全部通过，测试以退出码 0 完成。

## 与 Stage 7.0 对比

下表比较相同主机、数据规模、预热和计量次数下的 p50。负数表示 Stage 7.1 更快。

| 场景 | Gateway p50 变化 | Flight p50 变化 |
| --- | ---: | ---: |
| 查询 1 行 | -12.1% | -11.6% |
| 查询 1,024 行 | -10.9% | -11.1% |
| 查询 65,536 行 | -3.5% | -3.3% |
| 写入 1 行 | -7.0% | -5.2% |
| 写入 1,024 行 | -3.2% | -7.9% |
| 写入 65,536 行 | -0.9% | +1.2% |

唯一的 p50 上升是 65,536 行 Flight 写入的 1.2%，其余场景下降 0.9%--12.1%。这个结果不足以
宣称迁移本身带来加速，但可以排除可见的 transport 性能回归。Flight 相对 Gateway 的小查询固定
开销仍约为 7%，大查询基本持平；memory-ACK 写入仍比 Gateway 高 18.6%--35.8%。

## 写入权威闭合

同一次基准结束时，协议层和统一 runtime 的指标满足以下恒等关系：

- Gateway：330 个 batch、7,321,710 行；
- Flight：330 个 batch、7,321,710 行；
- server-owned memory ingest：660 个 batch、14,643,420 行；
- active tables 为 6，rejected operations 为 0，writer lock wait 为 0。

因此两条协议没有各自维护第二套 materializer；Gateway 暴露的嵌套 `memory_ingest` 指标也是同一个
server-owned runtime 的快照。这个计数闭合同时覆盖三个写入规模的预热与正式样本。

## 内存与结论

进程 RSS 从 29,988 KiB 增至 301,764 KiB，采样 peak 同为 301,764 KiB；Stage 7.0 对照为
30,012 KiB 到 302,712 KiB。基准累计保留约 1,464 万行进程内数据，因此该结果只表明迁移没有增加
可见的峰值 RSS，不能替代长时间回收与泄漏测试。

Stage 7.1 的结论是：统一写入所有权、Flight-only 启动和有序关闭已经落地，既有 Flight/Gateway
性能门禁保持通过。下一阶段必须围绕 durable ingest 建立新的正确性与性能基线，重点测量 WAL append、
fsync 策略、队列预算、producer epoch/sequence、重放恢复以及 durable ACK 的 p50/p99/p999。

## 功能验证

- `cargo test -p zippy-core`：完整通过；
- `cargo test -p zippy-ingest --lib`：23 项通过；
- `cargo test -p zippy-gateway -- --test-threads=1`：116 项通过，1 项忽略；
- `cargo test -p zippy-server -- --test-threads=1`：229 项通过，1 项手工基准忽略；
- `cargo clippy -p zippy-core -p zippy-ingest -p zippy-gateway -p zippy-server \
  --all-targets -- -D warnings`：通过；
- Python launcher：Black、Ruff、Mypy 通过，`test_server_process.py` 49 项通过；
- 100 样本 release Flight/Gateway 性能门禁：通过。
