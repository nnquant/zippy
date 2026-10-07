# 单主干收敛第十四批：原生删除事务与写入隔离基础

日期：2026-09-22。继续推进 F09/F10，关闭数仍为 **23/34**，剩余 **11 项**。
本批补内部生命周期基础，尚未开放 Flight/Python drop 接口，不把 Catalog 原语当作在线删除闭环。

## 原子目录删除

`CatalogCommandV1::DropTable` 现在有原生实现。User/Server authority 以明确的当前 snapshot version
执行 CAS；不匹配时整个事务回滚。成功时在同一 Catalog 事务中：

1. 创建下一版本的空 retention snapshot，保留此前持久化日志水位，标注 reason=table_drop。
2. 将仍可见的文件标为在该新快照中移除，不删除物理文件。
3. 将表标记 dropped，推进 writer epoch、snapshot version、Catalog revision，并记录幂等回执。

普通 get/list/resolve 不再返回该表；新 producer 和 manifest 提交被拒绝。table ID、历史 schema、
历史快照和 lineage 保留，原 request ID 的重试及重启后的重试返回完全一致的回执。
文件仍受现有查询 pin 和 durable snapshot lease 保护；二者都释放后，现有 GC 才能领取回收任务。

这是逻辑删除及文件可见性边界，不是清空目录或绕过 GC。历史 snapshot handle 持有的身份不被
新表复用。当前 SQLite 名称唯一约束仍保留已删名称；同名新 ID 重建将在目录格式收敛时补齐，
不能宣称本批已有完整 DROP/CREATE 产品语义。

## 可回滚的写入隔离

新增 Server 专用 `NativeIngestHandle.begin_table_drop()` 和 `NativeTableDropGuard`：

- 先取得生命周期许可和该表的 producer/checkpoint gate，再检查未完成持久回执。
- 忙碌返回可重试 RESOURCE_EXHAUSTED，不改变表。持有 guard 时新 memory/WAL/Persisted mutation
  不能进入，已开始的同步 mutation 由表状态锁串行化。
- Catalog 未提交时释放 guard 恢复原写入；提交后 `retire()` 永久撤销旧写入者并移除 ingest lookup。
  已捕获旧表引用的 checkpoint 看到退休状态后停止，不重新激活该表。
- guard 计入停机排空；必须留在不受 RPC 取消影响的 Server 管理任务中直到目录提交结果明确。
- 不隐式删除 WAL、回执目录或查询持有的文件。物理清理由后续生命周期协调承担。

## 验证与尚缺工作

测试覆盖 Catalog CAS 失败无副作用、幂等重试和重启重试、保留历史快照、阻止新 producer/manifest、
查询 pin 和 durable lease 的回收保护；写入测试覆盖可逆隔离、拒绝新 mutation、等待真实未完成
WAL ACK、永久撤销旧写入者及持有删除 guard 时停机不得提前完成。

Rust workspace 和全目标全特性 Clippy 的最终结果见
[结构化证据](perf_reports/evidence/2026-09-22-drop-foundation/summary.json)。本批未修改 Python 协议/API，
未以此前 Python 产品验收替代尚不存在的在线 drop 验收。

接下来的必需工作：协调 actor 和 materializer 的退出及持续日志终止；接入不受取消打断的 Server
管理任务；同名新 ID 重建；公开管理协议和客户端；删除后重启与文件回收；alter/schema 变更。
以上完成前 F09/F10 不关闭，也不删除仍承接计算/订阅能力的旧运行路径。
