# 单主干收敛第十一批：Python 原子日志交付与续读游标

日期：2026-09-22。原 34 项仍为 **21/34**，剩余 **13 项**；继续推进 F11/F16。
[第十批](convergence-flight-log-2026-09-22.md)的原生 Flight 日志已接入轻量 Python 客户端。

## 单一原生读取入口

新增 `Client.open_log()` / `FlightTransport.open_log()`，返回 `FlightLogReader` 同步迭代器。
它只使用原生 Flight，不探测或降级到 Gateway/SHM。默认从 latest 开始，`start="earliest"`
从 offset 0 回放；显式 `cursor=` 按不可变 table ID 续读，禁止再混用表名和起点。

- append：交付原始有序批次。
- frame：保留原始 frame 范围，收齐才交付一个 `LogEvent`；网络 batch_rows 不改变帧边界。
- KV：按 v2 mutation 结束序号拆分或拼接片段；验证连续 sequence/version、稳定 stream ID 和
  mutation 范围，保留 upsert/delete 控制列。一个事件代表一次完整 mutation，不等于完整表状态。
- `LogEvent.table` 使用 Arrow 分块引用，不把整个原子单元复制成连续数组。
- `max_pending_bytes` 约束内部保留的 Arrow buffers 和每片段固定开销；超过预算明确失败。
  这是组装缓存限制，不是整个 Python 进程 RSS 或调用者持有的历史事件预算。
- `LogCursor` 保存 table ID、下一 offset、kind、schema 和最后完整 KV version。只有完整交付才
  更新 cursor；缺口、schema 变化、越界、EOF 或断连都停止并清除未完成单元。
- 续读验证身份和 schema，防止把同名重建表混入原流。初始化 latest 本身固定完整提交边界。
- reader/context/client/transport 的 close 会取消网络读取；另一线程可中断阻塞的 next，
  不会把部分 frame 当成完整交付。没有新增后台消费队列或隐式自动重连。

游标表示已交付，不表示业务副作用已经成功或消费位置已经写入服务端。应用应处理完成后保存
`event.cursor`，再决定是否重连；durable consumer ACK 和自动 SnapshotThenTail 状态恢复仍待实现。
已有 callback `subscribe`/StateSubscription 和本地计算仍需迁移，Gateway 尚未删除。

## F43：PyArrow 丢弃 schema 附带的开流元数据

真实 PyArrow 23.0.1 对 Rust DoExchange 的探针显示：无论先读 schema 还是直接 read_chunk，
schema FlightData 的 app_metadata 都不会交给调用方。空流因而只等待到超时，无法读取起始水位。

开流信息现改成 **独立 metadata-only FlightData，随后发送 schema**。客户端先读取并验证
`LogOpened`，再读取 schema。只保留这一种开流方式，不增加双格式兼容分支。
Rust 分片/网络测试同步更新；真实 PyArrow 探针及 Python/Rust 产品验收证明开流信息可见。
F43 是原 34 项之外的新修复，不改变原清单关闭数。

## 验证

Rust workspace 全套、全目标全特性 Clippy（禁止 warning）、rustfmt/diff 均通过。
客户端 112 项通过，覆盖率 81.58%；全仓 Black（85 文件）/Ruff、根配置 14 文件及客户端 26 文件
mypy 门禁通过。全仓类型债务 F29 与跨平台矩阵仍未关闭。

命令、日志与源码 SHA-256 见
[结构化证据](perf_reports/evidence/2026-09-22-client-log/summary.json)。

- 客户端全套覆盖原子 frame/KV、跨网络批次拆合、断流保留旧 cursor、identity/schema/offset/version
  错误、预算、阻塞读取取消、client/transport 统一关闭及原有接口。
- `scripts/check_native_client.py` 从空目录启动带独立 read/writer/admin 凭据的真实 Rust 服务，
  经公开 API 建表、写入，执行四次启动（首次启动和三次重启）；每次验证三类日志回放、
  frame 跨片段重组、完整 KV mutation、按 table ID 续读和 latest 起始水位。
- 原子帧验收明确区分 `[10, null]` 与 `[12]` 两帧，不把两个帧拼成一个。
- 最终 KV 日志 sequence=10、version=6，与删除后空状态快照游标一致。

剩余主干收敛仍包含 durable ACK、状态订阅自动恢复、统一 SubscriptionService、旧回调/计算输出
迁移、alter/drop、唯一目录权威与旧路径删除、Python 合并、类型债务及平台矩阵。
