# Native persisted query Step 7 validation

日期：2026-07-16

## 范围

本报告验证 persisted query 的 planning、snapshot projection、physical-plan binding、deferred
Parquet reader、资源账本和 execution lease 生命周期。验证版本为：

- 运行时版本：`0.5.0.dev496+gaea8da8`
- 竞态与故障矩阵提交：`9f20722`
- 设计文档提交：`aea8da8`

当前边界尚未对外开放 public native result stream。decoded Arrow batch、operator arena、
result delivery、DataFusion global memory pool、spill 和 temporary-disk quota 不属于本轮
验证范围。因此，LDC 结果是组合链路和低延迟热路径非回归 smoke，不是 native persisted
query 吞吐基准。

## 静态检查与自动化测试

对干净代码提交执行（运行时仅新增本报告文档）：

```text
cargo fmt --all -- --check
git diff --check
cargo check --workspace --locked
cargo test -p zippy-query -p zippy-server --locked
cargo clippy -p zippy-query -p zippy-server --all-targets --all-features --locked -- -D warnings
```

结果：

- `zippy-query`：43 项测试通过。
- `zippy-server` library：169 项测试通过。
- `zippy-server` integration：4 项 legacy、12 项 native storage、15 项 server runtime
  测试通过。
- format、diff check、workspace check 和两个 crate 的 clippy 均通过。

故障矩阵与测试名记录在
[`single-node-server-refactor-design.md`](../single-node-server-refactor-design.md) 的
Stage 5.4f 小节。矩阵覆盖 prepare/bind 拒绝、资源原子回滚、两道 reader admission、
finish/fail/drop/admin cancel 竞争、真实 Parquet footer 扩容、预算拒绝和 option 篡改。

## 干净提交运行验证

运行环境：

- LDC broker profile：`tts_7_24`
- 行情：`au2607`
- 持续时间：60 秒
- Server：隔离的 `127.0.0.1:17990`，全新 catalog/data/run/log 目录
- native storage：启用
- catalog：`sqlite_shadow`
- persistence：关闭，避免把本轮 smoke 混同为 persisted query throughput

启动健康检查：

- Server 和 Gateway 均为 healthy。
- runtime version 精确为 `0.5.0.dev496+gaea8da8`。
- native catalog、storage reconciliation 和 commit recovery 均 ready。
- 初始 snapshot pin、query pin 和 shared-memory bytes 均为 0。

60 秒结果：

| 指标 | 结果 |
| --- | --- |
| source status | `running` |
| TTS snapshot ticks | 1 |
| login / subscribe / reconnect failures | 0 / 0 / 0 |
| normalize failures / callback errors | 0 / 0 |
| query health alerts | 0 |
| tail query latency | 10.919--23.518 ms；除一次抖动外为 10.919--14.080 ms |
| 首个 tick queue delay | 1.448 ms |
| 首个 tick process delay | 1.437 ms |
| 首个 tick total delay | 2.885 ms |
| open FD | 通常为 15，一次采样为 16 |
| longrun process RSS | 3,082,760 KiB，采样期间保持不变 |
| shared-memory bytes | 0 |

退出后 Server 仍为 healthy，`active_pins=0`、`active_query_pins=0`、`active_tables=0`。
随后 `zippy server stop` 返回 `graceful=true`、`forced=false`；PID、状态探针和
`/dev/shm` 均确认无残留。

TTS 在该窗口只推送了一个快照。该结果能证明真实 broker 登录、订阅、callback、热查询、
生命周期和清理链路没有明显回归，但不能证明高吞吐、长时间内存稳定性或 24 小时 soak。

## 非节流生成器容量边界

最终 smoke 前的探索性测试曾使用 LDC 的 sim generator：

- 64 symbols columnar 模式约 30 秒生成约 3940 万 ticks，FD 接近 930，queue delay
  达到约 7.8 秒，随后出现 shared-memory region 创建失败。
- 1 symbol raw 模式约 20 秒生成约 690 万 ticks，FD 达到约 726，queue delay 约
  14 秒，并出现 `Too many open files`。

这不是按 `interval_ms` 限速的市场仿真。`zippy-openctp` 的 raw、normalized 和 columnar
generator 主循环没有 wall-clock sleep；`interval_ms` 只推进生成数据的模拟时间戳。因此这两次
运行记录为无界 producer 的过载边界，不作为 nominal sim7x24 失败，也不作为本阶段通过证据。

后续容量测试需要先给 generator 增加明确的 wall-clock rate limiter，分别配置 steady state、
burst 和 overload 三档速率，再观察 query latency、FD、shared memory、RSS 和 backpressure。

## 结论与剩余边界

本轮 persisted planning/binding/reader lease 的资源和竞态边界通过自动化故障矩阵及真实
TTS 短跑验证。下一阶段仍需完成：

1. public native result stream 与慢消费者背压；
2. decoded batch、operator arena 和 result delivery 的独立预算及所有权移交；
3. DataFusion global memory pool、spill 和 temporary-disk quota；
4. client disconnect、deadline 和 cancel 到实际 operator task 的传播；
5. 有节流的高吞吐与至少 24 小时 soak 验证。
