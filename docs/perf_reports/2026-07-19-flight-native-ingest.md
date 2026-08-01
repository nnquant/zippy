# Flight 原生 durable ingest 性能基线

日期：2026-07-19

## 目标与方法

本报告记录 Stage 7.3 基线及其低延迟修订后的公开 Flight 写路径，而不是单独测 WAL codec
或 materializer。基准启动真实
SQLite authoritative Catalog、native storage、`NativeIngestRuntime`、TableActor 和 Flight listener，
关闭 Gateway，并对同一 append table 顺序写入 Arrow batch。

Stage 7.3 每个场景执行 10 次预热和 100 次计量，batch 行数为 1、1,024、65,536，分别请求 `Memory` 和
`Wal`。基准复用同一个 TCP Flight client 和 Writer session；每个样本建立新的 DoPut 和 logical
producer connection、acquire 更高 epoch、写一个 record batch，并等待权威 receipt。因此耗时包含
DoPut framing、Catalog producer acquire、fencing、fingerprint/IPC、WAL append、native materialization、
TableActor publication 和 receipt；不包含 TCP/TLS/Handshake 建连。

低延迟修订保持相同数据规模、顺序和样本数，但固定一个 producer/connection，并让每个短 DoPut 使用
连续 sequence 和同一个 epoch。服务端首次 acquire 后，后续 open 直接 resume，不再执行 SQLite
Catalog 写入、epoch fence、WAL segment 切换和 transition fsync。指纹计算生成的 Arrow IPC payload
由 WAL 直接复用，避免第二次序列化。另一个专用 Rust Flight 基准在单个 DoPut 内连续发送 110 个 1 行
batch，逐笔等待 receipt，用于测量 Python `FlightWriter` 跨 `flush()` 保持开放 stream 时采用的等价
wire/server 形态；它不包含 Python dict-to-Arrow、GIL 或 Windows 跨 WSL 开销。

执行命令：

```text
CARGO_TARGET_DIR=/tmp/zippy-target-stage7-3 \
ZIPPY_FLIGHT_NATIVE_INGEST_PERF_WARMUP_SAMPLES=10 \
ZIPPY_FLIGHT_NATIVE_INGEST_PERF_MEASURED_SAMPLES=100 \
ZIPPY_FLIGHT_NATIVE_INGEST_PERF_REPORT_JSON=/tmp/zippy-flight-native-ingest-stage7-3.json \
cargo test --release -p zippy-server --test server_runtime \
  native_flight_ingest_performance_baseline -- \
  --ignored --nocapture --test-threads=1
```

低延迟修订使用同一命令生成 short-put 报告；持久 stream 使用：

```text
CARGO_TARGET_DIR=/tmp/zippy-target-stage7-4 \
ZIPPY_FLIGHT_NATIVE_INGEST_PERF_WARMUP_SAMPLES=10 \
ZIPPY_FLIGHT_NATIVE_INGEST_PERF_MEASURED_SAMPLES=100 \
ZIPPY_FLIGHT_PERSISTENT_INGEST_PERF_REPORT_JSON=/tmp/zippy-stage7-4-persistent.json \
cargo test --release -p zippy-server --test server_runtime \
  native_flight_persistent_ingest_performance_baseline -- \
  --ignored --nocapture --test-threads=1
```

原始报告与环境位于
[`evidence/2026-07-19-flight-native-ingest`](evidence/2026-07-19-flight-native-ingest)。
JSON 中的 `git_sha=0d81c0a` 是测量时工作树的 base commit；Stage 7.3 及低延迟修订均尚未提交，
因此不能把该 hash 单独当成可复现实验源码标识。

## Stage 7.3 原始结果

Writer session Handshake 为 1.447 ms。公开写路径结果如下：

| Durability | 行/操作 | p50 | p95 | p99 | 最大值 | 吞吐 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 1 | 8.010 ms | 10.241 ms | 11.671 ms | 13.033 ms | 124 rows/s |
| Memory | 1,024 | 12.457 ms | 15.395 ms | 19.807 ms | 25.223 ms | 79,966 rows/s |
| Memory | 65,536 | 24.553 ms | 28.427 ms | 31.484 ms | 31.589 ms | 2.628 M rows/s |
| Wal | 1 | 23.150 ms | 26.659 ms | 27.455 ms | 29.110 ms | 43 rows/s |
| Wal | 1,024 | 28.823 ms | 32.945 ms | 33.899 ms | 34.282 ms | 35,553 rows/s |
| Wal | 65,536 | 41.912 ms | 49.543 ms | 56.573 ms | 73.210 ms | 1.533 M rows/s |

共完成 660 个 batch、14,643,420 行和 119,067,520 transport bytes；Flight 与 native ingest 的
batch/row 计数完全一致，duplicate、rejected 和 truncated-tail 均为 0。所有 660 次写入都创建新 epoch，
因此同时验证跨 epoch logical-offset 连续性和 live-segment publication。

## 低延迟修订结果

短 DoPut 仍然每个样本建立一次 RPC stream，但 660 次操作只发生 1 次 Catalog acquisition 和 659 次
内存 resume。结果如下：

| Durability | 行/操作 | p50 | p95 | p99 | 最大值 | 吞吐 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 1 | 0.768 ms | 0.923 ms | 1.012 ms | 1.012 ms | 1,276 rows/s |
| Memory | 1,024 | 0.855 ms | 1.053 ms | 1.158 ms | 1.183 ms | 1.167 M rows/s |
| Memory | 65,536 | 7.415 ms | 9.054 ms | 9.454 ms | 10.048 ms | 8.585 M rows/s |
| Wal | 1 | 4.507 ms | 5.433 ms | 6.009 ms | 7.848 ms | 217 rows/s |
| Wal | 1,024 | 4.634 ms | 5.642 ms | 5.959 ms | 7.310 ms | 213,168 rows/s |
| Wal | 65,536 | 12.749 ms | 14.347 ms | 17.271 ms | 21.838 ms | 5.073 M rows/s |

持久 DoPut 每种 durability 只建立一个 stream；每批仍独立完成并等待相同 durability receipt：

| Durability | 行/操作 | p50 | p95 | p99 | 最大值 | 吞吐 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 1 | 0.557 ms | 0.669 ms | 0.728 ms | 0.802 ms | 1,772 rows/s |
| Wal | 1 | 1.884 ms | 2.173 ms | 2.861 ms | 2.984 ms | 521 rows/s |

与 Stage 7.3 相比，持久 DoPut 的 1 行 p50 分别降低 93.0% 和 91.9%；p99 分别降低 93.8% 和
89.6%。65,536 行 short-put 吞吐分别提升至原来的 3.27 倍和 3.31 倍。完整 short-put 与 persistent
JSON 均位于 evidence 目录；其中 acquisition/resume、Flight/native batch/row 计数和资源水位均已核对。

## 评价

旧基线的主要固定成本已证实不是 loopback 网络，而是每笔重复 producer acquire 和 writer transition。
低延迟修订在不放宽 epoch fencing、严格 sequence、稳定 offset 或 ACK 边界的情况下消除了该成本。
持久 DoPut 比 epoch resume 的短 DoPut 继续节省约 0.21 ms Memory p50；WAL 仍由真实 `sync_data`
主导，1.88 ms 是本机文件系统上的确定恢复边界，不应与 `Memory` ACK 混为一谈。

CTA 逐 tick 若要求每笔崩溃可恢复，应使用长生命周期 `FlightWriter(durability="wal")`；若数据源可重放
且业务明确接受进程崩溃丢失未提交尾部，可使用 `memory` 获得约 0.56 ms 的公开路径 p50。吞吐型行情和
截面数据仍应保留 5--10 ms 有界聚合，以摊薄 Arrow 构造、IPC 和 fsync。后续 group commit 主要改善
多 writer 并发吞吐，无法消除单个同步 WAL 请求的物理持久化下限。

## FD 修复

第一次正式运行在常见的 `ulimit -n=1024` 下未完成，TableActor pin snapshot 时无法重新打开 SHM
backing file。根因是 Unix `mmap` 成功后仍为每个 owner 和 attachment 长期保留原始 FD。修复后
`MappedFile` 在映射建立后立即关闭 FD，mapping、跨进程路径和 owner 删除语义不变；Linux 回归测试
验证 64 个 owner 加 64 个 reader 不会线性增加 `/proc/self/fd`。相同 660 次 release 基准随后完整通过。

## 内存与边界

低延迟修订完整 short-put 基准的 RSS 从 25,612 KiB 增至 286,684 KiB，采样 peak 为 290,528 KiB。
该基准故意不执行 manifest
checkpoint，并保留约 1,464 万行的 WAL-visible live segments，因此这不是空载泄漏证据；但它证明
Stage 7.4 的 persisted checkpoint、WAL/live-segment 回收是必要的，不能长期依赖最多 1,024 个 live
segment 的保护上限。

本结论只适用于 WSL2 loopback、release、单客户端、顺序写、单 Int64 列、无 TLS。Windows 跨 WSL、
真实行情 schema、并发 producer、网络抖动、磁盘型号差异、24 小时 RSS 和恢复耗时不在本报告覆盖范围。

## 验证

- short-put release benchmark：660/660 batch 通过；
- persistent release benchmark：220/220 batch 通过；
- native Flight E2E：WAL 写入、立即查询、干净关停、重启 replay、再次查询通过；
- `zippy-types`：99 passed；
- `zippy-ingest`：44 passed、1 ignored；
- `zippy-protocol`：17 unit 与 19 integration 通过；
- `zippy-server`：191 unit、4 legacy runtime、12 native storage、22 server runtime 通过；
- 相关 Rust crates `cargo clippy --all-targets -- -D warnings` 通过。
