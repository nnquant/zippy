# 单主干收敛第二十二批：原生 callback 与恢复边界

日期：2026-09-22。推进 F11/F22；原 34 项仍为 **25/34 已关闭、9 项开放**。
本批迁移独立客户端 callback，主包 `zippy.subscribe*` 与本地计算尚未切换，不宣称旧主干已删除。

## 独立客户端只有一个订阅入口实现

`Client.subscribe()` / `subscribe_table()` 统一通过原生 Flight 日志，删除这两个入口的
Gateway/SHM 选择、旧 descriptor 探测、合成 KV changelog 名称转换及轮询参数校验。
路由器删除“Flight 尚未实现”的错误分支；订阅不再访问旧控制目录。明确指定旧 transport
直接失败，不暗中降级。查询默认 transport 的 SHM→AUTO 特例也删除。

配置由不可变 `SubscriptionOptions` 承载：namespace、固定 table ID、latest/earliest/committed、
游标、consumer/group、资源上限、重连和回调筛选。append 可用 Arrow 原生表达式或 instrument_ids
筛选，保留明确的每批尾部 count；不新增通用查询表达式解释器。batch_rows 控制传输片段，
throttle_ms 在完整事件间施加背压，不等待无限凑批。frame/KV 仅允许完整 table callback，
拒绝逐行、过滤和截断，避免把局部结果误当完整 mutation。

回调串行执行，无预读队列。普通游标只在整批回调完成后前进；持久 ACK 也必须在回调成功后。
筛选掉的 append 行属于显式处理策略，可以推进原始事件位点。回调失败直接终止，即使用户代码
抛出连接异常也不自动重试；异步回调或返回 awaitable 明确拒绝，不能未执行就 ACK。
ACK 丢回执自动以同一 consumer/group 重开，以 Catalog 决定是否重放，副作用仍需幂等。

重连绑定原表 ID、kind 和 schema，并重新签发 read session；不依赖重启前缓存的 bearer。
等待上一个持久提交释放 consumer ownership 的冲突，仅在已经建立过的订阅重连时重试。
初次打开时的组冲突、schema/连续性错误、鉴权拒绝仍显式失败。

`stop()` 取消实际 transport 并等待回调退出；回调内停止自身不会自我 join，也不会确认本批。
打开连接有有限超时；停止过程中稍后才返回的 reader 会被关闭。节流和重连等待均可立即打断。
成功交付后释放 worker 对最后一个事件的引用，避免空闲时无故保留 Arrow 数据。

## F52：已持久化旧 WAL 阻碍恢复后的 schema 切换

真实完整矩阵曾在第二次 schema 变更时报：
`SCHEMA_MISMATCH: recovered WAL schema version mismatch expected=[3] actual=[2]`。
失败日志保留在 `/tmp/zippy-flight-callback-schema-first-failure.log`。

恢复前缀在没有活动 WAL writer 时可以由原生 checkpoint 写入 manifest，但没有对应 writer
拥有回收权，因此旧 WAL 继续保留。随后 Catalog 推进 schema/epoch，重新注册 ingest 表时，
原流程仍对 manifest 已覆盖的旧记录执行“必须等于当前 schema”的校验，错误拒绝启动/变更。

现在只有同时满足以下条件的历史记录不再参与恢复：schema 严格早于当前版本、producer epoch
已被 Catalog 隔离、manifest 完整覆盖该记录、原子边界检查确认无需重放。WAL 的 checksum 与
mutation fingerprint 校验仍由原 WAL 解码器执行；不重解释旧数据，也不提前删除 WAL。
未覆盖、部分覆盖、epoch 未隔离及未来 epoch 仍拒绝。新增负例测试覆盖这些拒绝路径；已有三类
表 schema 集成测试扩展了“无 producer 重连时再次变更 schema，随后重启读取”的回归。

## F53：新 epoch 尚未写入时，ACK 跳过旧前缀持久化

同句柄停启验收又暴露一处独立时序问题：writer 切换后当前 epoch 的 sequence 为空，但表的
已提交 offset 仍包含上一 epoch 的数据。旧 checkpoint 因当前 sequence 为空直接返回，
消费 ACK 随后被 Catalog 拒绝为超过 persisted watermark。

writer handoff 已刷净上一 epoch 的 WAL；现在这一状态与纯恢复状态共用完整前缀的 checkpoint，
不依赖新 writer 必须先写第一批。新增确定性测试：先 Memory 写入，再切到尚未写入的 epoch，
确认消费 checkpoint 实际进入 sink、等待持久化完成才返回。真实失败保留在
`/tmp/zippy-flight-callback-ack-first-failure.log`，不以重跑通过代替修复。

## 验证与未完成边界

最终结果见 `docs/perf_reports/evidence/2026-09-22-flight-callback/summary.json`：

- Rust workspace 全部通过；stdout 汇总 1763 passed / 10 ignored（包含嵌套子进程输出，
  以 cargo exit 0 为准），Clippy workspace/all-targets/all-features/-D warnings 通过。
- Python 全量 773 passed / 1 skipped；独立客户端 163 passed，覆盖率 82.96%，新 callback 模块 98%。
- Black 全仓 88 文件、Ruff、Rustfmt、diff、协议再生检查通过；配置 mypy 根 14 / 客户端 28 文件通过。
- 完整 `mypy .` 仍失败：991 errors / 12 files / 87 checked，F29 不关闭。
- Arrow 23.0.1 与 25.0.1 的最终完整矩阵通过；未新增 Windows/macOS 或干净发行包验收。
真实服务矩阵包含三类表 callback、ACK、四代恢复、实际取消 gRPC 流后的续读，以及同一个
KV callback 句柄跨真实服务停启继续消费。Arrow 23/25 分别验收，不据此外推全部平台。

独立客户端的旧 GatewaySubscriber/ShmSubscriber 组件及其组件测试仍在；主包也仍有自己的
旧 subscriber、查询解释器与计算/producer 链路。这些不是 Client callback 的 fallback，
但必须在主包迁移验收后继续删除。统一 Rust subscription 合同与 KV 吞吐也尚未完成，F11 开放。
全仓 mypy 的既有债务仍归 F29，本批不加 ignore 或缩小门禁覆盖来掩盖失败。

验收脚本还修正了 schema 管理调用对合法背压的处理：只有明确标为 retryable 的
RESOURCE_EXHAUSTED 才用原 request ID 在五秒内重试。背景 checkpoint 持有转换 gate 时的
拒绝不会改变表；脚本不应把一次忙碌当成永久 schema 失败。其他错误和超时仍原样失败，
原始背压日志保留在 `/tmp/zippy-flight-callback-management-backpressure.log`。
