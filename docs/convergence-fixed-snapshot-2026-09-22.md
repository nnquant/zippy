# 第二十六批：原生跨查询固定快照

日期：2026-09-22。推进 F14/F22/F29；原审计仍为 **25/34 已关闭、9 项开放**。
本批补齐 Catalog、原生查询、Flight 和独立客户端的固定快照链路。主包与 Gateway 的旧查询入口
尚未整体迁移，因此不把这项能力的验收等同于旧数据主干已删除。

## 固定边界与资源所有权

原生捕获同时保留有界表集合的 Catalog manifest、schema、TableActor live spans 及其 pin。
后续查询从该冻结集合创建自己的元数据视图，不重新解析当前同名表或向 actor 取最新边界。
支持查询捕获集合的子集；表被改 schema、删除或以同名新 ID 重建后，旧快照仍指向旧 ID、旧 schema
和原数据。未捕获的表明确拒绝，不能把新 ID 混入旧快照。

每个派生查询先在自己的 QueryInstanceId 下预留元数据内存，再复制描述信息。Catalog 子视图
保留父 owner，实际文件 reader 保留子 owner，因此释放原始句柄不会提前释放已准备查询的 GC pin。
原始与子查询计费分别保留，最后一个 reader 退出后才能全部归还。预留内存不在原快照所有权锁内进行，
避免与资源回收形成锁顺序冲突。固定租约从 planning 进入 execution 状态，使用明确到期时间管理。

## Flight 与客户端契约

服务公布 `query.fixed_snapshot.v1`，提供 `zippy.create_fixed_snapshot.v1` 和
`zippy.release_fixed_snapshot.v1`。查询命令显式携带 `fixed_snapshot_id`；未实现该能力的 frontend
在创建 ticket 和规划前拒绝，不能忽略字段再读当前数据。

- snapshot ID 由服务端生成，归属认证 session；Read/Administrator 可用，Writer 不可管理快照。
- TTL 请求范围 1–300000 毫秒，实际到期不晚于认证会话到期；默认客户端请求 60000 毫秒。
- 每个 frontend 最多 32 个捕获中或已持有的租约，仍受查询与进程内存预算约束。
- 过期、会话撤销、关闭 admission 都清理租约。停服同时关闭在途捕获的发布入口。
- status 暴露 `active_fixed_snapshots`；显式释放已有句柄返回 true，已不存在返回 false。
- 固定查询快照不与 KV 原子状态游标请求混用；后者仍是独立的快照加日志游标契约。
- 租约是进程内短期资源，重启失效。任何失效都明确报错，不自动重新捕获最新数据。

独立客户端的 `read_table(..., snapshot=True)` 在句柄创建时捕获。`wait=True` 先等待表可用，
再捕获。`Client.open_snapshot([(namespace, table), ...])` 支持多表共同边界及 join；两个固定表
必须来自同一作用域。作用域、表、查询克隆独立持有引用，最后一个引用关闭时释放服务端句柄。
join 保留独立的右表视图，关闭原始右表不会破坏派生查询。

固定表 schema 通过同一快照执行 `SELECT * LIMIT 0` 获取，不再从当前 Catalog 元数据重新观察。
客户端保存捕获时的认证连接，避免会话提前换新使尚未到期的快照提前失效；会话实际到期后不会
重新捕获。普通表的 `snapshot()` 仅返回独立捕获的元数据，不改变其 per-query 查询模式。

## 验证

- Catalog 回归验证父子查询独立计费、GC pin 保持及最后 reader 释放；拒绝错误 issuer、无查询归属、
  不足内存和非法表序号。
- 原生运行时验证 live 边界、checkpoint 后重复读取、子集查询、三类表的 schema/drop/同名替换隔离、
  释放原租约后已准备查询继续执行、取消和过期拒绝。
- 真实 Flight/SDK 矩阵验证三类表、多表 join、独立句柄生命周期、认证会话换新、跨会话查询和释放拒绝、
  Writer 权限拒绝、TTL、32 个租约上限、停服回收及重启后旧 ID 拒绝。另用原始 GetFlightInfo/DoGet
  验证：准备查询后释放根租约，再写入数据，已准备查询仍返回原结果。
- 完整 Rust 工作区、协议回归及 Clippy；重建原生扩展后的主包与客户端 Python 回归。
- Arrow 23.0.1/25.0.1 从仓库外独立构建安装纯 Python 客户端，固定快照模块、生成协议与覆盖率门禁通过。
- 配置范围 mypy 通过；扩展全仓类型检查仍有 978 个错误，F29 继续开放。

命令结果与日志哈希见[验收证据](perf_reports/evidence/2026-09-22-fixed-snapshot/summary.json)。
本机环境为 Linux x86_64 / Python 3.11；未执行远端 Windows/macOS CI，也未重跑完整主包的隔离发行矩阵。

## 剩余收敛

F07/F08/F11/F13/F14/F15/F22/F28/F29 继续开放。下一步仍须切换主包查询与回调、旧
source/Session/Pipeline 和控制面，补齐 KV 计算状态输入、计算输出会话恢复及算子 checkpoint，
再删除仍承接这些能力的 Gateway/Legacy/shadow。派生查询预算偏保守和全仓类型检查缺口没有在本批关闭。
