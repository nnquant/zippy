# Flight 与 Gateway 真实协议性能基线

日期：2026-07-19

## 目标与方法

本报告验证 Arrow Flight 替代 Gateway 查询与内存写入 transport 后的实际性能边界。基准启动真实
`LegacyServerRuntime`、Gateway TCP listener 和 Flight gRPC listener；两条路径共享同一 SQLite
shadow Catalog、Parquet 数据、native query engine 和进程内 materializer。每个场景交替执行两条
transport，校验查询行数和写入回执，避免只测序列化函数或 mock backend。

正式修复后基准参数：

- 源数据：262,144 行；
- 查询/写入规模：1、1,024、65,536 行；
- 每场景：10 次预热、100 次计量；
- 查询结果 batch：最多 8,192 行；
- Flight 写入语义：append-only、memory ACK；
- 环境：WSL2 本机 loopback，release profile，单 benchmark 进程。

执行命令：

```text
CARGO_TARGET_DIR=/tmp/zippy-target-stage7 \
ZIPPY_TRANSPORT_PERF_SOURCE_ROWS=262144 \
ZIPPY_TRANSPORT_PERF_WARMUP_SAMPLES=10 \
ZIPPY_TRANSPORT_PERF_MEASURED_SAMPLES=100 \
ZIPPY_TRANSPORT_PERF_REPORT_JSON=/tmp/zippy-transport-post-gated-final.json \
cargo test --release -p zippy-server --test server_runtime \
  flight_gateway_transport_performance_baseline -- \
  --ignored --nocapture --test-threads=1
```

原始证据位于
[`evidence/2026-07-19-flight-gateway-transport`](evidence/2026-07-19-flight-gateway-transport)。

## 根因与修复

修复前 Flight 每次 RPC 稳定增加约 50--100 ms。运行时把已绑定的 `TcpListener` 包装为自定义
`TcpListenerStream` 后调用 tonic `serve_with_incoming`；这条路径不会应用 tonic 在内部 accept 时设置的
socket 选项，因此 accepted socket 保持 Nagle 开启。小型 request/response 在 loopback 上也会等待
delayed ACK，形成稳定的阶梯延迟。

`zippy-protocol` 现改用 `TcpIncoming::from_listener(listener, true, None)`，明确对每个 accepted Flight
socket 设置 `TCP_NODELAY`。同时新增确定性测试，直接检查 server 侧 accepted socket 的 `nodelay()`。

## 修复前后

修复前报告对应生产代码提交 `a36ebdf`，基准 harness 当时尚未提交；它使用 3 次预热和 20 次计量。
修复后报告对应干净提交 `9967f22`，使用 10 次预热和 100 次计量。两者场景和数据规模一致，但后者统计
置信度更高；修复前的旧百分位实现会把 20 样本 p99 取为最大值，因此下表主要比较 p50。

| 场景 | 修复前 Flight p50 | 修复后 Flight p50 | 降幅 |
| --- | ---: | ---: | ---: |
| 查询 1 行 | 100.417 ms | 4.638 ms | 95.4% |
| 查询 1,024 行 | 51.257 ms | 4.650 ms | 90.9% |
| 查询 65,536 行 | 52.625 ms | 8.373 ms | 84.1% |
| 写入 1 行 | 49.904 ms | 0.589 ms | 98.8% |
| 写入 1,024 行 | 49.934 ms | 0.684 ms | 98.6% |
| 写入 65,536 行 | 49.830 ms | 5.560 ms | 88.8% |

Flight read session 建立从 42.751 ms 降至 1.325 ms，writer session 从 50.031 ms 降至
1.149 ms，分别下降 96.9% 和 97.7%。

## 修复后对标

| 场景 | Gateway p50 / p99 | Flight p50 / p99 | Flight 吞吐比 | Flight p99 比 |
| --- | ---: | ---: | ---: | ---: |
| 查询 1 行 | 4.354 / 6.004 ms | 4.638 / 6.242 ms | 0.931 | 1.040 |
| 查询 1,024 行 | 4.303 / 6.795 ms | 4.650 / 6.639 ms | 0.931 | 0.977 |
| 查询 65,536 行 | 8.357 / 10.501 ms | 8.373 / 10.626 ms | 0.998 | 1.012 |
| 写入 1 行 | 0.827 / 0.983 ms | 0.589 / 0.691 ms | 1.411 | 0.703 |
| 写入 1,024 行 | 0.859 / 1.118 ms | 0.684 / 1.187 ms | 1.218 | 1.062 |
| 写入 65,536 行 | 6.442 / 7.652 ms | 5.560 / 6.832 ms | 1.202 | 0.893 |

65,536 行查询的 Flight/Gateway 吞吐分别为 59.173/59.301 MiB/s，基本持平；65,536 行写入
分别为 85.688/71.311 MiB/s，Flight 高约 20%。小查询仍有约 7% 的固定协议吞吐损失，但绝对 p50
差值只有约 0.3 ms。

## 自动门禁

基准报告 format version 2 对每个场景执行两项门禁：

- Flight rows/s 不低于 Gateway 的 90%；
- Flight p99 不高于 Gateway 的 1.20 倍。

最终 6 个场景全部通过。默认计量样本已提升到 100，p50/p95/p99 使用 nearest-rank；
`ZIPPY_TRANSPORT_PERF_MEASURED_SAMPLES` 是正式参数，旧的 `..._SAMPLES` 仅作为兼容别名。

## 内存与边界

最终运行 RSS 从 30,012 KiB 增至 302,712 KiB，采样 peak 同为 302,712 KiB。基准累计向六个
memory-ACK writer 写入约 732 万行，materializer 按语义保留这些进程内数据，因此该增量不能解释为泄漏，
也不能与只运行 20 样本的修复前 RSS 直接比较。持久化写入、回收和长时间内存稳定性由 Phase 7 的独立
soak 覆盖。

当前结论只适用于 WSL2 loopback、单客户端、无 TLS 的协议对比。Windows 客户端跨 WSL、TLS、远程网络、
高并发混合读写、慢消费者和 durable ingest 不在本报告覆盖范围。进入 Phase 7.1 后，应继续使用同一门禁
防止 native ingest backend 替换造成 transport 回归，并新增 backend queue、WAL 与 durable ACK 指标。

Stage 7.1 已完成 server-owned ingest authority 替换并复跑相同门禁，六个场景全部通过；详见
[`2026-07-19-native-ingest-authority.md`](2026-07-19-native-ingest-authority.md)。

Stage 7.3 将 Flight 写入切到 SQLite authoritative native ingest 后，该组合 harness 已升级为 report
format v3，并只保留 Gateway/Flight query 对比；authoritative 模式禁止 legacy Gateway 与 Flight ingest
并存。后续写入门禁由
[`2026-07-19-flight-native-ingest.md`](2026-07-19-flight-native-ingest.md) 的独立 harness 承担。

## 验证

- `cargo test -p zippy-protocol`：15 项 unit、19 项 Flight integration 通过；
- `cargo test -p zippy-server --test server_runtime flight_ -- --test-threads=1`：3 项通过；
- nearest-rank 百分位契约测试通过；
- `cargo clippy -p zippy-protocol -p zippy-server --all-targets -- -D warnings` 通过；
- 最终 release 性能门禁通过。
