# 审计问题修复续报

> 历史记录：以下结论和测量属于文中日期，不作为当前实现状态。当前事实见
> [能力表](current-capabilities-2026-09-21.md)，后续工作见[收敛方案](single-trunk-convergence-plan.md)。

这是兼容契约阶段的历史记录。后续用户授权的新契约改造与当前验收状态见
[新契约续报](audit-2026-09-06-v2.md)。

日期：2026-09-06，Asia/Shanghai。基线仍为 `f1df4201e1faf179b1dd928ab15cfeb57b9b38b1`，
本轮叠加在首轮未提交修改上，未提交、未发布、未部署。

## 已修复

1. **原生 append 容量预检的重复扫描。**
   `StreamTableMaterializer::append_live_segment_count` 直接读取封存段数与活跃段行数，
   `preflight_live_segment_budget` 不再构造所有描述符、编码 JSON 或 attach SHM。
   预检从 O(S) 降为 O(E)，其中 S 为历史段数，E 为保留 writer epoch 数。
   保留原有容量、epoch handoff、零行和溢出校验。

2. **封存段重复解码和 attach。**
   每个封存描述符携带一个惰性类型化缓存，首次查询时验证并解析。封存前缀不可变，
   缓存随描述符回收或快照替换一起释放；失败结果不缓存，活跃段的行数仍实时读取。
   输出仍是完整有序列表，整体仍为 O(S)，没有宣称完成增量 TableActor 发布。
   回归覆盖重复查询、回收后的行数/顺序、部分段在 writer handoff 后变为 sealed。

3. **rolling 事务复制整份历史窗口。**
   undo entry 改为原长度、统计量快照与本事务实际淘汰的旧值；回滚时删除新值并还原旧前缀。
   不记录会被回滚丢弃的新值，不复制未触及的历史窗口。记录开销从 O(K×W) 降为
   O(B)，额外空间为 O(K + 实际淘汰旧值数)，其中 B 为本批有效更新数。
   测试覆盖五种窗口因子、空/半满/满窗口、一次事务更新次数超过窗口长度、新标的、null、
   回滚后继续写入。提交路径不再随 W 线性复制。

4. **rolling 标准差数值错误。**
   新增测试确认 `1e12 + 1, 1e12 + 2, ...` 的四元素窗口无法用原来的
   `E[x²] - E[x]²` 正确计算。改为预热阶段 Welford 中心矩与定长窗口替换公式；
   单元素窗口显式置零，仍返回总体标准差，原始 sum 的计算顺序保持不变。
   回滚同时精确还原 mean/M2，不使用逆浮点运算恢复统计量。

5. **异步描述符发布与源注销竞态、闲置线程残留。**
   Session/Pipeline 停止引擎后先排空描述符发布队列并 join，再注销 source。
   worker 改为首个任务到达时启动，stop 后退出，重启时可由原回调重新启动。
   保留正常运行期间发布失败的告警，不通过过滤告警掩盖问题。
   两种 owner 的回归检查最终发布先于注销、重复 stop 和重新启动；原真实快照替换告警
   用例单独复验已无告警通过。该修改没有实现队列字节预算或跨批次发布合并。

6. **压测的标的覆盖错误。**
   TickBatchFactory 原来只更新时间戳，symbols 列一直是首批标的；segment-copy/forward
   与 stream-table subscriber 同样复用固定标的。现在缓存一轮标的加跨界尾部，按累计输入
   行号取切片，保证小批次跨批覆盖全部配置标的，避免每批重新格式化标的字符串。
   回归逐行验证 `(64,10000)`、`(8,3)`、`(1,1)` 的两个完整轮转周期。
   inproc 时间戳继续单调前进；segment 微基准仍复用预生成时间戳，不是实盘时序回放。

## 性能与证据

使用 complexity-optimizer 的先测量、后修改、再回归流程。环境与首轮相同，未绑核。
局部 release 基准是单次计时，不是 p99，也不能当作整体 ingest 的速度倍率。

| 局部基准 | 修改前 | 修改后 |
| --- | ---: | ---: |
| 窗口 1024，单标的每事务更新一行 | 200.20 ns | 87.62 ns |
| 窗口 65536，单标的每事务更新一行 | 6328.83 ns | 87.25 ns |
| 64 个 sealed 段，构造完整描述符列表 | 287.51 μs | 6.34 μs |
| 512 个 sealed 段，构造完整描述符列表 | 2409.18 μs | 51.26 μs |

原始日志与修复后真正覆盖 1 万标的的测量已归档至
[followup-measurements.json](perf_reports/evidence/2026-09-06-audit/followup-measurements.json)。
首轮测量原件保留，但已标注实际只覆盖首批标的，撤回“1 万标的容量证明”。

| batch | 目标速率 | 稳态时长 | 实际平均速率 | 入队 p99 | 处理行数（含 2 秒预热） | 最大采样队列深度 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1024 | 1,000,000 行/秒 | 30 秒 | 999,857.9 行/秒 | 28 μs | 32,000,000 | 10 |
| 64 | 500,000 行/秒 | 10 秒 | 499,804.5 行/秒 | 22 μs | 6,000,000 | 41 |

两组 input_rows_total 均等于 processed_rows_total，最终 queue_depth=0，
dropped_batches_total=0。p99 是 write 入队时间，不含策略完成、网络或持久化。

```bash
cargo test --offline --release -p zippy-operators --lib \
  window_transaction_performance -- --ignored --nocapture
cargo test --offline --release -p zippy-engines --lib \
  append_descriptor_performance -- --ignored --nocapture
cargo run --offline --release -p zippy-perf -- inproc-timeseries \
  --symbols 10000 --rows-per-batch 1024 --target-rows-per-sec 1000000 \
  --duration-sec 30 --warmup-sec 2 --max-p95-micros 1000 --max-p99-micros 5000 \
  --max-queue-depth 64 --report-json /tmp/inproc-corrected-10000-1024.json
```

## 尚未关闭的契约与架构事项

- **全天持续原生写入尚未完成。** 1024 个 live segment、每 epoch 100 万精确回执、WAL
  启动扫描限额仍然存在。仓库 ADR 0005 也将 manifest checkpoint 集成明确列为后续阶段。
  本轮没有删除未持久化数据、提高上限或隐式淘汰回执。
- 需要确定旧批次精确回执的保留期限：永久兼容需要磁盘回执索引；有界窗口则需要明确
  窗口外重试的错误和客户端恢复方式。两者都不能只对内存 BTreeMap 做随意淘汰。
- 回收必须遵守：封存段提交 Parquet 与 manifest → 确认完整 mutation 边界水位 → 持久化
  checkpoint 与回执保留信息 → 更新 actor 的可见前缀 → 按 snapshot pin 安全退役 SHM →
  回收已覆盖 WAL。需增加各阶段崩溃重启、慢盘、超时重试与小限额持续写入测试。
- **KeyValue 完整快照输出仍为 O(N×C)。** 现有 `Engine::on_data` 每批返回完整且独立的
  Arrow 快照，不能直接改成部分行或会随下一次写入变化的 SHM view。需要显式增量模式，
  保留默认完整输出，明确 changelog、flush/stop 和订阅者的一致性契约。
- 本轮缓存仅消除重复解析，TableActor 的完整列表发布仍为 O(S)；与生命周期一致的增量
  publication 需要独立的版本和失效协议。
- 即使短时 1 万标的吞吐通过，也不等于全天 Level2 验收。真实宽表、开收盘突发、磁盘
  限速、持久化/恢复、多订阅者、30 分钟以上长跑与 p99.9 仍需完成。

## 验证

- Rust 全 workspace 测试、Clippy all-targets/all-features `-D warnings`：通过。
- 相关 engines/ingest/operators 测试：通过；修复前标准差回归实际失败，修复后通过。
- 性能工具测试在允许本地回环 socket 的环境通过；沙箱内 ZeroMQ 初始化会 abort，未把
  该环境失败误判为业务回归。
- Python 描述符定向测试与原真实快照替换用例：4 项通过，无原 RuntimeWarning。
- 重新构建 editable 扩展后的 Python 完整套件：603 passed、1 skipped，无 warning；
  客户端：67 passed，覆盖率 80.03%。maturin 的本地 patchelf 缺失警告仍存在，不将
  本地 editable 测试等同于可分发 wheel 验收。
- cargo fmt、git diff --check、两侧各自配置的 Black/Ruff 检查：通过。
- 按 CI 配置运行 mypy：主项目 14 个文件、客户端 24 个文件通过。配置不覆盖全部 Python
  实现文件；额外扩大到 `python pytests` 时有 1035 个类型错误。已将 HEAD 原件导出到
  独立临时目录作同口径复验：同为 1035 个，去除行号后的错误文本完全一致。本轮没有
  增加这些错误，也未扩大范围修复全部历史类型债务，不宣称全仓类型检查零错误。
