# 第三十二批：任意 SQL 入口统一到显式 Client

日期：2026-09-22。推进 F14/F22/F28/F29；原审计仍为 **25/34 已关闭、9 项开放**。
本批删除实际旧路径，不增加转发旧 API 的兼容包装。

主包删除 `zippy.read_sql()` 和 `_normalize_native_sql_bindings()`，不再通过正则从 SQL 猜测表名。
主包导出独立客户端的同一个 `Client` 类，任意 SQL 使用
`Client.read_sql(sql, bindings=...)`。命名空间、身份、结果批次、期限及预算沿用原生 Flight 契约。

独立客户端删除 GatewayTransport.read_sql、Gateway SQL 流式请求与响应分支、
`_gateway_query_sql_enabled` 与相应实例字段。SQL 与订阅都只允许 Flight；仅 Gateway 的旧
`query_sql=true` 广告不能启用 SQL，显式 Gateway 选择也不能重开该路径。
删除仅为 Gateway 保留的 SQL `parameters` 参数，以及在 Flight 中被直接忽略的 SQL `stream`
参数；传输始终采用 Flight 批次流，公开返回值仍为完整 Arrow Table。

主包旧 SQL 的假 Gateway 聚合测试被迁入真实原生验收：通过主包导出的 Client，显式将 ticks
绑定到 market.sql_left，执行 count/sum，在每次服务重启后断言 rows=2、total=30，并明确
设置规划内存和单行结果批次。客户端保留无 bindings、无 Flight、显式 Gateway、已删除参数
和旧能力广告的拒绝测试；SQL 编译、表查询及其快照恢复测试继续运行。

`docs/native-query-service.md` 已改为当前 Flight/SQLite/显式 Client 用法，删除旧灰度回退、
自动猜绑定及主包全局连接示例。客户端 README 的 SQL 路由表同步更新。发行验证器检查客户端
wheel 中 Gateway SQL 方法不存在，以及两包安装后 Client 类相同、主包旧函数不存在。

## 验证

- 最终仓库默认 pytest：861 passed、1 skipped。比上一批少一个仅验证旧 Gateway SQL/自动
  推断绑定的测试；其聚合功能已由上述真实原生服务验收覆盖，没有以删除有效行为断言规避失败。
- 真实原生矩阵通过：主包 Client 的显式命名空间绑定、聚合与逐代重启结果均正确；三类表、
  固定快照、订阅 ACK、计算输出恢复和帧边界的原断言全部保留并通过。
- 完整客户端 211 passed，覆盖率 84.95%；Arrow 23.0.1 和 25.0.1 的仓库外 sdist/wheel 构建、
  纯 Python 安装及测试各 206 passed、1 skipped，覆盖率均为 81.78%，均没有主包或 SHM 扩展。
- Black 全仓、Ruff、Rust fmt、diff 检查通过。根 mypy 16 文件及客户端/测试/脚本 31 文件通过；
  扩展全仓检查为 971 errors / 12 files / 98 checked，比上一批减少 5 个，未新增 ignore。
- 本批没有修改 Rust，不重复上一批 workspace/Clippy；没有重跑主包完整隔离发行或跨平台实机。
  两包发行安装后的 Client 身份/旧函数删除断言已加入验证器，不能把新增断言当作已完成运行证据。

日志及源码哈希见[验收证据](perf_reports/evidence/2026-09-22-sql-entry/summary.json)。

边界：服务端 Gateway adapter、主包旧 Table/Session/Pipeline、普通 Table 查询的旧 transport
分支仍未整体退出。当前主包仍有 10804 行；这次 SQL 删除不等于 F14 或 F22 全项关闭。
完整平台门禁及全仓类型债务也未完成。原九项保持开放。
