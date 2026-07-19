# Stage 7.2 原生 ingest WAL 性能基线

日期：2026-07-19

## 目标与边界

本报告测量 Stage 7.2 `NativeIngestRuntime` 在 producer fencing、严格 sequence、stable log offset 和
WAL v1 生效后的核心写入成本。被测提交为
`7e9a9142d3f859c89c0898fe53e4ccd7dabb2243`。

这是 backend authority 基准，不是 Flight 端到端基准。输入已经是进程内 Arrow `RecordBatch`；计时
包含 canonical SHA-256 mutation fingerprint、WAL Arrow IPC 编码、data record append 和 sink 调用。
`Wal` 还包含 commit marker、`sync_data`、异步 ACK 唤醒；测试 sink 只记录 receipt 元数据，不包含
native segment materializer、Flight/gRPC、TLS 或网络。

## 测试方法

每个 durability/行数组合使用独立 runtime、table、producer epoch 和 WAL 目录。每次 mutation 使用
连续 sequence 和唯一 request ID。先预热 20 次，再计量 200 次：

- `Memory`：调用 `write_memory`，不 fsync；计量结束后的 shutdown 才统一提交完整尾部。
- `Wal`：逐 batch 调用 `submit_durable(..., Wal)` 并等待 ACK，因此每个样本执行一次真实 fsync。
- 行数：1、1,024、65,536；单列 `Int64`，逻辑字节数分别为 8 B、8 KiB、512 KiB。
- 环境：WSL2 本机 ext 文件系统、release profile、单 benchmark test、Tokio 2 worker threads。

执行命令：

```text
CARGO_TARGET_DIR=/tmp/zippy-target-stage7-2 \
ZIPPY_NATIVE_INGEST_PERF_WARMUP_SAMPLES=20 \
ZIPPY_NATIVE_INGEST_PERF_MEASURED_SAMPLES=200 \
ZIPPY_NATIVE_INGEST_PERF_REPORT_JSON=/tmp/zippy-native-ingest-stage7-2.json \
cargo test --release -p zippy-ingest \
  native::tests::native_ingest_memory_and_wal_performance_baseline -- \
  --ignored --exact --nocapture --test-threads=1
```

原始 JSON 和环境信息位于
[`evidence/2026-07-19-native-ingest-wal`](evidence/2026-07-19-native-ingest-wal)。

## 结果

| Durability | 行数 | p50 | p99 | p999 | max | p50 rows/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 1 | 4.840 us | 12.795 us | 13.507 us | 81.561 us | 0.207 M |
| Wal | 1 | 1.217 ms | 1.952 ms | 1.968 ms | 2.228 ms | 0.001 M |
| Memory | 1,024 | 22.765 us | 43.816 us | 54.257 us | 70.960 us | 44.981 M |
| Wal | 1,024 | 1.277 ms | 1.478 ms | 1.485 ms | 1.513 ms | 0.802 M |
| Memory | 65,536 | 1.244 ms | 1.513 ms | 1.518 ms | 1.791 ms | 52.666 M |
| Wal | 65,536 | 2.699 ms | 3.672 ms | 6.167 ms | 7.344 ms | 24.282 M |

逐 batch fsync 相对 `Memory` 的 p50 增量分别为 1.213、1.254 和 1.455 ms。小 batch 主要受固定
fsync/调度成本支配；65,536 行时 Arrow IPC 编码和写入放大开始占据明显比例。

`Wal` 每个场景共记录 220 个非空 commit，等于 20 次预热加 200 次计量，证明本次结果没有通过
并发或隐式批处理摊薄 fsync。`Memory` 计量期间没有 commit，shutdown 后只有 1 个 commit，符合
memory ACK 的崩溃语义。

## 对 Flight 切流的含义

Stage 7.1 Flight memory write 的 p50 为 0.559、0.630、5.629 ms。由于该数据包含 transport 和旧
materializer，而本报告使用 no-op sink，两组数据不能直接相减或替换。把 authority 核心成本简单加到
Stage 7.1 只可作为保守量级估算：

| 行数 | Memory 粗略上界 | Wal 粗略上界 |
| --- | ---: | ---: |
| 1 | 0.564 ms | 1.776 ms |
| 1,024 | 0.653 ms | 1.907 ms |
| 65,536 | 6.873 ms | 8.328 ms |

这些值不是性能承诺。Stage 7.3 必须接入真实 native table sink 后复跑现有 100-sample Flight/Gateway
门禁，并新增 WAL p50/p99/p999、commit group size、queue wait 和恢复耗时门禁。

## 结论与下一步

Stage 7.2 的 memory authority 热路径在小 batch 下是微秒级，65,536 行仍可达到约 52.7 M rows/s；
逐 ACK WAL durability 的主要固定成本约为 1.2--1.5 ms。这个水平可用于明确要求单批落盘的写入，但
不适合作为高频小 batch 的唯一策略。

下一步不是放松 durability，而是增加有界 group-commit coordinator：设置最大等待时间、最大 group
records/bytes 和 pending ACK 预算，让并发 producer request 共享 fsync，同时保持 ACK 只在覆盖自身
sequence 的 commit marker 落盘后返回。默认窗口必须由真实 Flight + table sink 基准决定。

## 功能验证

- `cargo test -p zippy-types -p zippy-catalog -p zippy-ingest`：全部通过；
- `cargo test -p zippy-ingest --lib`：41 项通过，1 项手工基准忽略；
- `cargo clippy -p zippy-types -p zippy-catalog -p zippy-ingest --all-targets -- -D warnings`：通过；
- release benchmark：6 个场景、每场景 200 个计量样本，退出码 0。
