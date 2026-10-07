# 单主干收敛第十批：Flight 原生日志分片

日期：2026-09-22。继续推进 F11/F16，原 34 项仍为 **21/34**，剩余 **13 项**。
本批接通三类表的真实 Flight DoExchange，尚未替代 Gateway 产品订阅。
前一批见[持续原生日志](convergence-live-log-2026-09-22.md)。

## 协议与实现

新增能力 `subscription.log_fragments.v1`，只由原生 frontend 宣告。
`zippy-protocol/src/log.rs` 定义 transport 边界，`NativeFlightQueryFrontend` 转交
`NativeQueryEngine::subscribe_log`，没有额外日志缓存、Catalog 或写入权威。

- 首条 FlightData 只携带 `LogCommand` descriptor，包含协议版本、query/session ID、单表引用、
  显式 latest 或 inclusive offset、内存限制和可选 deadline/batch_rows。命令最大 16 KiB，
  开流等待最多 10 秒并占用现有 pending admission；拒绝 SQL alias、缺失起点和 disabled memory。
- 读取 RPC 复用 read session 校验；请求 session 必须与认证会话相同。原本受限的本机匿名部署
  仍按既有配置处理，F26 尚未关闭。
- schema 消息携带 `LogOpened`：精确 query generation、不可变 table ID、table kind、起始水位。
- 每个 record batch 携带 `LogProgress`：本批首尾 offset、完整原帧边界和当前窗口水位。
  Arrow 再次切分网络批次时，按实际 IPC row count 递增 offset，不为每个分片重复完整区间。
  KV 保留 v2 mutation 控制列；frame 分片尚须上层组装后才可原子交付。
- 输入半关闭表示客户端不再发送控制，输出仍持续。后续控制仅接受版本化 cancel；不把 ACK
  或其他未知控制当作成功。响应流丢弃时，原生 reader 和未消费结果所有权一起释放。
- 复用 Flight message/transport budget；编码期间保留原生结果 lease，输出逐条拉取，无新增
  后台 producer 队列。订阅占用现有 native query 资源上限。

canonical proto 同步生成 Rust 和 Python 绑定。Python 高层订阅接口、完整 frame/table view、
durable ACK、group membership 和 SnapshotThenTail 恢复尚未完成，能力名明确反映这一边界。

## 补充修复

### F42：Flight 取消重新签发 owner 导致自身拒绝

原 `NativeFlightQueryFrontend::cancel_owned` 新建 owner capability，其 nonce 不同于创建查询时的
nonce。同一 session 因而也不能取消自己的原生查询。现在 Flight 认证后调用 crate 内部专用取消
入口，核对原 session 与精确 query generation；本地 owner capability 检查仍验证 nonce，
外部 Rust 调用没有新增可绕过 capability 的公开 engine 方法。其他会话和旧 generation 均拒绝。

### 多订阅窗口构造峰值冲突

真实网络订阅与两个本地订阅并发刷新时，多个 Catalog 构造峰值同时预留，复现默认全局预算耗尽。
事件窗口现在经同一 coordinator 的公平 semaphore 顺序构造，直到驻留 lease 缩容后释放准入。
等待继承同一查询的取消/deadline，等待数受 native active-query 限额约束。
普通查询与持续日志仍共享全局内存账本；实际预算不足仍明确报错，没有提高默认预算或绕过计费。

## 验证

命令、结果及源码哈希见
[结构化证据](perf_reports/evidence/2026-09-22-flight-log/summary.json)。

- 三类表各运行真实本机 TCP DoExchange，与历史/latest 本地订阅并发；8 次续写和交替 checkpoint，
  解码核对全部 18 行的值、连续 offset、frame 范围；同会话取消、缺失/不匹配 session 拒绝，
  latest 半关闭后持续等待并按 deadline 终止。
- 分片测试强制 8,000 行切为多个网络消息，核对 IPC row count、offset 和完整 frame 范围；
  读到 EOF 或中途丢弃 encoder 后，结果 lease 恰好释放。
- 取消测试验证本地 nonce 不变、跨 session/错误 generation 拒绝、认证会话取消幂等。
- 窗口准入被占用时取消等待者，立即结束，不以超时轮询或无限等待代替取消。
- Rust workspace 全套、全目标全特性 Clippy（禁止 warning）、rustfmt、diff 及协议再生成校验通过。
- 全仓 Black/Ruff、根配置 14 文件及客户端 24 文件 mypy 门禁通过；全仓 F29 类型债务仍开放。
  Black 沙箱内进程阻塞，最终在沙箱外以单工作进程、临时缓存检查 83 个文件通过，
  已定点终止两个阻塞检查及其子进程。
- Python 客户端全套 95 项通过，覆盖率 80.86%。本批只改变其生成协议绑定，未新增高层订阅 API。

Gateway、旧主包订阅和本地计算路径仍保留，不能据此宣称 F11/F16 或整体主干切换已完成。
