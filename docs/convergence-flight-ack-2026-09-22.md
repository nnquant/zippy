# 单主干收敛第二十一批：公开 Flight 持久消费与确认恢复

日期：2026-09-22。推进 F11，原 34 项仍为 **25/34 已关闭、9 项开放**。
公开 ACK 已接通；旧 callback、完整统一订阅接口及本地计算迁移仍未完成，不能删除旧主干。

## 对外接口

服务新增 `subscription.durable_log.v1` 能力声明。`Client.open_log` 支持固定 `consumer_id`、
可选 `group_id` 和显式 `start="committed"`，返回的完整 `LogEvent` 带连接内交付序号。
调用 `events.acknowledge(event)`，收到持久回执后再读取下一批。个人及组位点仍只保存在 Catalog，
没有新的消费状态库、客户端自动补位点或 Gateway 回退。消费者不能用交付游标覆盖已确认位置。

复用现有 Flight DoExchange、会话鉴权、结果编码器和运输内存额度。每个分片及编码器进一步拆分的
消息都携带完整交付序号和结束位置；服务端只接受已经完整发出的单元。伪造、半批及超前 ACK
在进入原生确认任务前被拒绝。重复最后一个已确认 ACK 直接返回已知成功，避免与等待未来数据的
读者锁相互等待。原始显式游标流仍支持半关闭请求通道；持久流必须保持 ACK 通道开放。

SDK 不自动确认业务处理。`AmbiguousAcknowledgementError` 表示回执缺失、结果未知；使用同一
consumer/group 重连后，以 Catalog 为准决定是否重放。收到明确的 schema、身份或权限拒绝时保留
结构化错误，即使该错误首先出现在 PyArrow 的请求 writer 上。业务副作用仍需承受重复执行，
这里提供至少一次恢复，不宣称跨外部业务系统的恰好一次事务。

## 恢复与切换修复

- **F50：恢复 WAL 不应依赖 producer 重连才能持久化。** 原 checkpoint 在没有活动 WAL writer 时
  直接返回；恢复的数据可读，却无法确认到尚未发布的 manifest 前沿。现在恢复前缀直接通过同一
  materializer/persistence 流程发布，校验完整水位；不伪造 producer 身份，不创建空 writer。
  未归档的原 WAL 保留到后续有权回收的 writer checkpoint，消费确认不能擅自删日志。
- **F51：临时 quiescence 不应抢先终止持续日志。** schema/drop 提交前暂停 actor 命令时，读者可能
  在刷新快照中收到 RESOURCE_EXHAUSTED，掩盖最后的 schema/drop 结果。增加有通知的可逆切换等待：
  回滚恢复读取；提交返回精确的 SCHEMA_MISMATCH / NOT_FOUND。查询取消和期限仍可结束等待。
  持久订阅等待 ACK 时也监听 actor 退出，不能因停止预读而错过 schema/drop。

## 验收

`check_native_client.py` 在真实 Server、Catalog、WAL、Flight 和 Python 客户端间验证三类表：

1. 完整交付与未确认禁止预读；服务端拒绝直接发送的伪造 ACK。
2. 同组第二成员被拒绝；取消旧连接后按原持久位点恢复。
3. 读取并丢弃真实的已提交回执，SDK 报结果未知；重连不重放已经确认的数据。
4. 多轮重启恢复未确认数据，包括尚未打开 producer 的 WAL 恢复前缀。
5. 等待 ACK 时 schema 变化返回明确错误；按新 schema 重开后补空历史列并确认。
6. 无后续数据时，重复发送真实网络 ACK 仍立即得到回执。

Arrow 23.0.1 和 25.0.1 均运行同一真实服务脚本。协议测试补齐 durable descriptor/control 拒绝，
并验证大帧经过编码器拆分后交付身份不变且原生内存所有权正确释放；客户端覆盖完整单元、回执
一致性、丢回执及 writer 端结构化拒绝。actor 回归验证可逆等待在回滚和退休两种结局都能唤醒。
完整门禁与代码指纹见[结构化证据](perf_reports/evidence/2026-09-22-flight-ack/summary.json)。

最终 Rust workspace 全部通过（10 ignored）；重建扩展后的 Python 为 734 passed、1 skipped，
独立客户端 124 项通过，覆盖率 81.51%。Clippy、Black、Ruff、Rustfmt、协议再生成及配置范围
mypy 均通过。全仓 `mypy .` 仍有 991 errors in 12 files，F29 不关闭。
一次并行验收期间的完整回归在旧 Gateway 注册测试中触发 5 秒超时；单独复验和最终完整回归均
通过。失败日志已保留，具体瞬时原因尚未证明，没有通过修改断言或放宽超时掩盖它。

## 仍待完成

F11 尚需迁移旧 callback，并清理未实现的规划接口，形成统一的订阅服务契约。KV 当前逐行计费组装
完整 mutation，仍需吞吐验收；单活动成员组不等于分区再平衡。其余主干切换、本地计算、maintenance、
全仓类型债务和平台发行验收继续保持开放。没有新增 Windows/macOS 或干净发行包验收结论。
