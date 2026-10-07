# 第四十七批：固定原生默认主干

日期：2026-09-22。承接[第46批](convergence-master-persistence-retirement-2026-09-22.md)。
关闭 F07，累计 **32/34**；F08/F28 保持开放。

## 唯一启动路径

删除 Catalog backend 枚举、catalog.backend、server.native_storage_enabled、query.native_enabled
和对应环境变量。SQLite、NativeStorage 和 NativeQueryEngine 无条件启动；旧字段在 TOML、
JSON overlay、wire serde 及环境变量入口都明确拒绝，不保留“旧值恰好等于新值”的兼容分支。
负向回归发现并修复 PartialQueryConfig 静默忽略旧字段的问题。

Flight 默认启用，固定监听 127.0.0.1:17667；endpoint 不再可空，也不再从 Master 地址派生。
flight.enabled=false 只关闭监听器，原生存储、恢复与查询引擎仍启动。写入仍要求显式启用
DoPut 和独立凭据。原生生命周期中的 Option 表示停服后的资源释放，不再表示可选后端。

ready schema 升级为 v11，删除 native_storage_configured；四个原生恢复事实无条件参与
Rust/Python 就绪判断。control_only 也必须完成恢复；Flight 启用时额外检查监听器和协议。
资源检查保持严格，测试配置显式协调 ticket 上限和查询并发预算，不做隐式缩小。

## 验证

- Core/Master/Server：472 次通过记录、4 个原有忽略项，包含子进程复验记录。
- 全 workspace/all-targets/all-features Clippy -D warnings、Rustfmt 和 diff 检查通过。
- 重建服务端与扩展，Python 全量 544 项通过；mypy 91 文件、客户端自身配置检查 27 文件通过；
  Ruff、Black 92 文件及扩展动态库依赖检查通过。
- 真正读取原生快照的 Flight 测试不再写 enabled=true，覆盖默认启动与 ticket 回收。
- 原生矩阵移除后端/native/Flight 启用开关，三类表、schema/drop、固定快照、实时订阅/ACK、
  本地计算及多次重启全部通过；确认不生成旧 Master registry。

[测试退役清单](perf_reports/evidence/2026-09-22-native-default/test-retirement.json)
列明 7 个旧契约专属测试退役及 5 个保留语义的迁移；
[结构化证据](perf_reports/evidence/2026-09-22-native-default/summary.json)
包含本批源码、验证日志和二进制摘要。

F08 尚有临时旧表/生产者控制请求、内部 source/materializer；F28 尚缺当前版本完整平台实测。
本批 Rust 执行范围是三个受影响 crate，全 workspace 执行证据属于第45批。
真实矩阵为 Linux CPython 3.11 / Arrow 23.0.1 的仓库构建，不代替新发行包或 Windows/macOS 验收。
未提交、推送、部署或迁移用户数据。
