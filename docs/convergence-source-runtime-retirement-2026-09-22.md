# 第五十批：删除旧后台运行时与 ZeroMQ 数据路径

日期：2026-09-22。承接[第49批](convergence-process-control-2026-09-22.md)。
累计 **32/34**；F08/F28 保持开放。

## 计算内核与旧执行器分离

删除 core 的 Source、SourceSink、SourceHandle、StreamHello、Publisher、EngineHandle、
spawn 工厂、EngineConfig/Status、OverflowPolicy、BoundedQueue/SpscDataQueue 和专属原子指标。
Engine 的同步计算、事务、flush、指标增量契约保留，NativeProcessor 继续调用同一组内核。
删除整个 zippy-io crate 及 ZeroMQ Rust/native 构建依赖、CI 专属测试。
迟到行和过滤行测试直接验证当前内核，Reactive 指标额外确认 drain 后清零。

旧 runtime lifecycle/fast-path/source/queue/ZeroMQ 专属测试随实现退役；保留原生 actor、
写入完整性、事务回滚、服务生命周期、真实 Flight 订阅与重启恢复测试。测试退役清单逐项记录。
Python 的旧 IO 静态导出测试也随整个 crate 删除。

## 性能边界明确化

退役 remote-pipeline-upstream/downstream 与 runtime-stream-table-e2e，删除 endpoint、
buffer_capacity、overflow_policy、max_queue_depth 及 Source/queue/publisher 指标。
旧 profile/config 明确拒绝；延迟 sample_count/max_micros 必填，清单格式更新为 v2。

新 timeseries-kernel 同步执行实际 TimeSeriesEngine，延迟包括输入转换、内核与输出计数。
输入必须全部完成；吞吐统计保留半开稳态窗口、有理数 deadline、最终 flush 超时计入分母。
输出和真实内核指标均现场统计，不以恒为零的旧执行器指标代替。
新增回归覆盖测量顺序、预热排除、失败不产生成功样本及旧性能配置拒绝。

旧入队 p95 ≤ 1 ms 门禁不再代表该边界。首次实测约 100 万行/秒、p95 2.036 ms、
p99 2.211 ms，旧阈值失败记录保留。新 profile 门禁为 p95 ≤ 5 ms、p99 ≤ 10 ms：
4096 行/批、100 万行/秒对应 4.096 ms 批间隔，持续吞吐仍要求目标的 90%，尾部留调度余量。
连续五次 10 秒测量、2 秒预热的 release 基线另行归档；不把本机基线解释为跨平台 SLA。

## 验证与剩余项

全 Rust workspace **1243 次通过记录、7 个原有手动性能忽略项**，包含子进程复验。
全 workspace/all-targets/all-features Clippy -D warnings、Rustfmt、diff 检查通过。
重建服务端与 Python 扩展后，全量 Python **543 项通过**；mypy 91 文件、客户端自身配置
27 文件、Ruff 和 Black 92 文件通过。原生矩阵继续验证三类表、固定快照、实时订阅/ACK、
计算、schema/drop、断连与重启恢复。扩展动态库依赖可解析。

[结构化证据](perf_reports/evidence/2026-09-22-source-runtime-retirement/summary.json)与
[测试退役清单](perf_reports/evidence/2026-09-22-source-runtime-retirement/test-retirement.json)
记录本批源码和二进制摘要、最终验证日志及性能基线。

F08 尚余 KV/shard materializer、descriptor forwarding 及专属基准；原生 KV 状态消费器保留。
F28 尚缺 Windows/macOS 实机和当前发行矩阵；本机 Linux CPython 3.11/Arrow 23.0.1 的
仓库构建验收不替代发行验收。未提交、推送、部署或改写旧数据目录。
