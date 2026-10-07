# 单主干收敛第十八批：公开 schema 管理与确定的重放契约

日期：2026-09-22。关闭 F09，原 34 项累计 **25/34 已关闭、9 项开放**。
本批补齐第十七批原生实现的公开管理接口，修复新增发现 F48、F49；没有把订阅与本地计算
仍依赖旧路径的问题计为完成。

## 公开管理只有一种 schema 表示

Flight `zippy.alter_table_schema.v1` 使用 `catalog.schema.v1` 能力协商，并复用管理员认证、
请求大小上限和管理任务生命周期。Python `Client.alter_table_schema()` 接收不可变 table ID、
逻辑用户 schema、非零 expected schema version 和稳定 request ID。KV 显式携带原有有序 keys，
无需通过活表查询补全请求，因此表删除后仍可以重放原请求。

建表和 schema 变更共用逻辑 schema 到存储 schema 的转换函数；已有物理事件 envelope
不能作为第二种管理请求格式。Catalog、TableActor、ingest 和 materializer 继续沿用第十七批
的隔离、checkpoint、事务提交、退休和安装顺序。允许追加可空字段、普通字段放宽可空性、
描述元数据更新；不隐式重命名、删列、类型转换或改变主键/分组/事件时间字段。

## 旧句柄的结果必须明确

原生 actor 与 writer 的退休原因统一为 Dropped / SchemaChanged。删除返回 NOT_FOUND，
schema 变化返回 SCHEMA_MISMATCH，旧 fixed snapshot 不受影响。writer 持有小型共享终态信号，
在旧 TableAuthority 被释放后仍能返回准确原因，无需强引用整个 WAL/schema/回执集合。
客户端必须重新读取 schema 并打开句柄，旧 schema 游标不会静默适配新版本。

F49 来自真实 PyArrow DoPut：回执读取先返回 EOF，结构化 schema 拒绝只在 writer.close()
中暴露。客户端现在补读最终状态；明确的 SCHEMA_MISMATCH 保留 ServerError，无法证明结果
的 EOF 或内部错误仍为 AmbiguousWriteError。前几批已经确认时，ServerError.receipts 与
FlightWriter.last_receipts 保留本次调用的已确认前缀，避免丢失依据或盲目重写整表。

## F48：幂等身份不能依赖随机 IPC 编码

KV 的服务端 schema 包装会重新编码 Arrow IPC。schema 同时存在 keys 和描述元数据时，
Rust HashMap 的键顺序使相同逻辑请求得到不同 IPC 字节，原指纹因此偶发冲突。

建表和 schema 变更现在使用规范逻辑 schema hash 参与命令指纹，目标、定义、配置、预期版本
及审计主体仍参与身份；字段和元数据内容变化仍冲突。Arrow IPC 解码器下移到 zippy-types，
指纹和 Catalog 存储共用同一套 framing、FlatBuffer、字段递归与元数据预算校验。
畸形 IPC、尾随数据不能通过“逻辑等价”绕过校验；没有失败后退回原字节指纹的兼容分支。
编码对齐不同但逻辑相同的 schema 指纹相同，内容变化指纹不同，均有确定性回归。

这是持久幂等记录契约的变化，当前目录版本推进至 v9；v1–v8 及未来目录被明确拒绝，
不原地迁移或改写。目录边界与用户授权的放弃旧数据格式保持一致。

## 验收与剩余边界

`scripts/check_native_client.py` 在三类独立表上验证新增字段、纯元数据变更、错误 CAS、
不支持类型、read 会话越权、旧日志/写入句柄终止、旧 schema 游标拒绝、混合历史读取、
四轮服务启动、WAL 与 Persisted 恢复、较新版本原请求重放、删除后重放。
frame 保留追加的完整帧；验收不把它误作仅保留最新帧的状态表。

Arrow 23.0.1 和 25.0.1 均已通过真实服务验收，仍包含既有的同名新 ID 重建、状态订阅跨进程恢复
和本地只读会话检查。本批未新增干净 sdist/wheel 安装或 Windows/macOS 实机结果。
最终完整 Rust workspace 为 1,754 passed、10 ignored；重建扩展后 Python 为 730 passed、
1 skipped。独立客户端 120 passed，覆盖率 81.67%。Clippy、Black、Ruff、Rustfmt、协议再生成
与配置范围类型检查均通过；全仓 mypy 仍为 991 errors in 12 files（85 files checked）。
完整门禁结果与代码指纹见[结构化证据](perf_reports/evidence/2026-09-22-schema-api/summary.json)。
全仓类型债务继续计入 F29，不以配置范围 mypy 通过替代全仓通过。

剩余原项为 F07/F08 默认配置与权威切换、F11 统一订阅与持久消费确认、F13/F14/F22 主包
写入查询及本地计算统一、F15 旧 adapter 退出、F28 支持平台矩阵、F29 全量类型检查。
以上仍未完成，整体目标保持进行中。
