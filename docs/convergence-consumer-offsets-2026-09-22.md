# 单主干收敛第十九批：持久消费位点的唯一 Catalog 权威

日期：2026-09-22。推进 F11，原 34 项仍为 **25/34 已关闭、9 项开放**。
本批完成持久消费确认的存储前提；尚未宣称统一 SubscriptionService、Flight ACK 或旧回调迁移完成。

## 确认记录与表身份一致

`CommitConsumerOffset` 不再返回未实现错误。确认携带 consumer ID、不可变 table ID、个人或
消费组 authority、预期 schema version、预期旧位点及新位点。首次成功提交建立 consumer 身份，
consumer ID 此后不能切换到另一张表；相同名称的新 table ID 也不能继承旧消费进度。

个人 authority 必须等于自己的 consumer ID；消费组以 table ID 与 group ID 为共享位点键。
多个成员并发提交同一旧位点时仅一个 CAS 成功，另一个明确冲突，不用“取较大值”掩盖竞争。
Catalog 不负责选举消费组成员或确认连接所有权；这些仍是订阅服务的职责。

空位点和 offset 0 严格区分，Empty → Empty 可以持久登记身份而不伪造已消费数据。
确认禁止回退，禁止跨越当前已发布快照的持久水位，禁止使用过期 schema。首次登记、位点、
Catalog revision 和幂等回执在同一个事务中提交。精确重放返回原回执，不把较新位点退回去；
表删除后已提交请求仍可重放，但新的确认与位点读取明确失败。

## 只读查询与写入使用同一状态

新增 `CatalogConsumerRead`，由现有 SqliteCatalog 及其只读 handle 实现；使用原有有界控制读队列
和只读工作线程。表 schema、持久水位、consumer 身份及个人/组位点来自同一个读事务，
不建立额外状态库，不加载文件 manifest。

位点及身份 BLOB 在 SQL 投影中先限制类型与宽度，再交给解码器。过大 BLOB、非法标记、孤儿身份、
越过持久水位的已有记录均返回 DATA_CORRUPTION，不能被降级成 Empty 并从头重放。
目录仍为 v9，没有新增物理迁移或兼容读取分支。新确认命令此前未实现，本批为它加入明确的
schema/CAS 必填契约，没有为旧缺字段请求补默认值。

## 尚未越过的服务边界

该命令只接受 Server actor 合约；actor 是内部审计信息，不替代 RPC 鉴权。
订阅服务仍必须验证连接所有权、交付序号、已处理的连续前沿，以及 frame/KV 原子批次边界。
Catalog 仅验证身份、schema、CAS 和持久化上界，不能凭客户端任意声明的 offset 确认交付成功。

当前原生日志可能包含尚未写入 manifest 的数据，因此未来 durable ACK 通道还须协调持久化，
再提交对应前沿。不能只因事件已发送，就把消费位点持久化到可恢复数据之外。
现有 `Client.open_log()` 尚未公布 durable ACK 能力；旧 Gateway/SHM callback 路径暂时保留。

## 验证

新增专项回归覆盖三类表的个人位点、Empty/0、只读查询无登记副作用、共享组 CAS 并发唯一胜者、
事务中途故障回滚且同 request ID 可重试、跨表身份冲突、未持久化/回退/过期 schema/错误 actor 拒绝、
schema 变更后重新确认、损坏及过大 BLOB、重启精确重放和删除后重放。持久水位通过 Catalog
manifest 命令建立；这组测试验证 Catalog 合约，不代替真实订阅处理后 ACK 的端到端验收。

最终完整 Rust workspace 为 1,759 passed、10 ignored；重建扩展后 Python 为 730 passed、
1 skipped。Clippy、Black、Ruff、Rustfmt、协议再生成及配置范围 mypy 均通过。
全仓 mypy 仍为 991 errors in 12 files（85 files checked），没有缩小范围或增加忽略规则。
完整门禁结果与代码指纹见[结构化证据](perf_reports/evidence/2026-09-22-consumer-offsets/summary.json)。
本批没有新增公开 Flight ACK、Arrow 版本矩阵或干净发行包验收；这些不能从 Catalog 测试推断。
F29 的全仓类型债务仍开放。

下一步将原生日志与这些位点接口接入同一 SubscriptionService，并验证 ACK 丢失、重连、
重启、慢读者及完整 frame/KV 交付前沿，再迁移旧回调与本地计算消费。
