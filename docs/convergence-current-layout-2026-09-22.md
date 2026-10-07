# 单主干收敛第十六批：唯一目录布局与同名重建

日期：2026-09-22。原 34 项仍为 **24/34 已关闭、10 项开放**。本批推进 F07/F09/F15/F28，
不把目录布局收敛等同于旧运行时和计算链路已经退出。

## 删除在线物理格式兼容分支

删除 `zippy-catalog/src/migration.rs` 中物理 v1–v7 的迁移注册表、逐版 DDL、回填和迁移历史检查。
当前仅保留 `layout.sql` 和 `layout.rs`：空 Catalog 在一个 BEGIN IMMEDIATE 事务中直接创建物理布局
v8；已有目录必须严格匹配 v8。没有 v7→v8 在线升级，也不保留旧格式读取后自动重写分支。

- 旧 v1–v7 和未来版本返回不可重试 UNSUPPORTED_PLAN，提示使用新数据目录。
- 版本检查发生在启用 WAL 前。拒绝测试逐一验证 v1–v7 的数据库文件字节、journal_mode 和 sidecar
  状态不变，保留哨兵历史 payload；这些测试替代此前“旧版本自动升级成功”的已废弃契约。
- 新目录只有一个 `catalog_layout` 初始化记录，不再生成 `catalog_migrations` 历史。
  单一冻结的 schema fingerprint 仍覆盖表定义、生成列、约束、隐式索引及部分索引。
- 保留初始化事务回滚、并发首次打开、quick_check、外键检查、篡改检查和有界读取。
  初始化记录的 SQL checksum 规范化 CRLF/LF，防止源码检出的换行方式改变同一布局的身份。
- 源码发行包已检查包含 `layout.rs`/`layout.sql`，且不包含已删除的 `migration.rs`。
  本批不以 sdist 文件检查替代全平台 wheel 验收。

物理布局版本与 wire/schema 版本分别承担不同契约。原生 CatalogCommand V1、Arrow schema version、
文件 checksum 格式继续有效；它们不构成旧目录的兼容分支。

## 同名新 ID 重建

旧的全表 UNIQUE(namespace_id, name) 改为仅覆盖 state != dropped 的部分唯一索引。
名称只有在逻辑删除提交后才释放；仍活动、正在删除或失败状态的表继续占用名称。table ID 的全局唯一性
不变，已删除的 ID 永不可复用；tombstone、历史 schema、快照、文件和回执不重命名、不删除。

Catalog/Runtime 测试验证：

- 活动名称冲突、新请求复用旧 ID 均被拒绝；名称解析指向新 ID。
- 旧删除请求精确重放不会删除同名新表；历史快照仍读取原 ID、原名称和原文件。
- 原表的 query pin / durable lease 在新表存在时仍保护旧文件；释放保护后 GC 才能领取旧文件。
- append/frame/key-value 各重复同名创建、删除 64 次，表数量和 metadata 额度均能回收。
- 新表占满 max_tables=1 时，旧 create 请求仍精确重放。Server 先通过 Catalog 有界只读回执通道
  核对已提交结果，再对真正的新表申请预算；不会复活旧表，也不会消耗新表额度。
- Catalog owner 关闭后，保留的只读回执 handle 明确返回不可重试 UNAVAILABLE，不重新打开数据库。

真实 Python/Flight 验收对三类表先写入 a，重启后删除，再用相同名称和新 ID 写入 b。
后续重启查询只能得到 b 和新 ID；旧 create/drop 请求返回原结果；旧日志游标返回 NOT_FOUND，
不能附着同名新表。新日志从自己的 offset 和新 ID 开始。Arrow 23.0.1、25.0.1 均通过六次服务启动。

## 回归中修复的测试竞争

首轮 workspace 回归在 legacy runtime 测试遇到控制端口被占用。测试夹具原先 bind(0) 取端口后立即
释放，再由另一个线程绑定，存在端口被其他并发连接占用的窗口。
MasterHarness 现在直接持有已绑定的 TcpListener 并移交给同一服务循环；固定地址重启也直接绑定后移交。
服务的 readiness、正常关闭及端口释放仍由原集成测试验证，不通过增加重试次数掩盖冲突。
这不是新增一套服务运行逻辑，两个入口共用同一个监听循环。

## 验证与未完成边界

Rust workspace 1,742 passed、10 ignored；重建扩展后 Python 共 726 passed、1 skipped。
Clippy、Rustfmt、Black、Ruff、配置范围类型检查及 sdist 文件核对通过。

最终计数和日志指纹见[结构化证据](perf_reports/evidence/2026-09-22-current-layout/summary.json)。
删除旧升级契约导致测试数量减少；保留当前格式的完整性/事务/查询/恢复测试，并新增旧格式拒绝、
同名身份隔离、满容量回执重放及换行无关校验。未删除仍有效的能力测试来满足门禁。

全仓 mypy 仍为 **991 errors in 12 files，checked 85 source files**；当前配置和新增验收脚本的检查
单独记录，F29 不关闭。

F07/F08 的默认配置和运行权威切换、F09 的 alter/schema、F11 的统一订阅与 durable ACK、
F13/F14/F22 的写入/查询/主包统一、F15 的旧 import/shadow/projection adapter、F28 的平台矩阵与
F29 的完整类型门禁仍需继续。新布局暂时保留旧 adapter 使用的表结构；在线物理迁移删除不等于
这些运行桥接已经删除。WAL/receipt 物理回收也尚未完成。
