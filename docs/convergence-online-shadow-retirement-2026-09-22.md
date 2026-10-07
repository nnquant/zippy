# 第三十九批：删除在线影子迁移链

日期：2026-09-22。前置证据见[第38批](convergence-main-python-trunk-2026-09-22.md)。
本批继续删除服务端真实执行的旧路径，累计仍为 **27/34**；F07/F08/F11/F13/F14/F15/F28
保持开放，未将部分拆除计为关闭。

## 实现变化

- 删除 Server 的旧 Catalog 启动导入、命令影子同步与 Gateway 持久文件对照模块；
  同时删除 Gateway 到原生查询引擎的迁移适配器。五个完整模块共 7,781 行，包括模块内测试。
  运行时和原生存储不再创建迁移状态、影子线程或 Catalog reconcile handle。
- 删除 `sqlite_shadow` 枚举值、`catalog.shadow` 配置及六个资源参数。
  TOML/直接反序列化拒绝未知 Catalog 字段，环境中的 `ZIPPY_CATALOG_SHADOW_*` 明确失败。
  旧选项不会被解析后静默忽略，也不会自动回退到另一权威。
- 查询统一捕获 TableActor 快照后固定 Catalog manifest。删除影子表绕开 actor 的分支，
  内部边界的 committed offset 和 live high watermark 不再是可缺失字段。
  原生恢复不再按旧 stream 标记跳过表。
- 停服仍按协议、ingest、控制监听器、原生存储的依赖顺序执行，共用一个绝对截止时间；
  删除影子发布者封口、队列排空及最终投影同步等待。尚未停止的依赖继续保留目录所有权。
- 重写当前运行时说明，移除已失效的影子部署配置与 Gateway 原生查询示例。

## 测试迁移与修正

原 Flight 查询集成测试通过旧 JSON registry 导入准备数据，无法证明删除导入后的原生恢复。
现改用 SQLite 建表、原生 TableActor 持久化，再启动 Flight 读取 2,048 行；保留逐值查询、
元数据、ticket 单次消费、资源释放和干净停服断言。

多表快照测试将原来的影子表改为第二张原生表，并断言两张表均有 actor；保留排序、审计、
Catalog pin 与租约生命周期检查。原生查询执行测试也不再构造旧 stream 标记。

删除的旧接口及迁移模块对应 52 个测试名称（含 1 个手动忽略的 Gateway 对照基准）；
另有 1 个多表测试改名，未删除其行为验证。新增旧配置拒绝测试。
逐项名称、文件和原生替代说明见
[测试退役清单](perf_reports/evidence/2026-09-22-online-shadow-retirement/test-retirement.json)。
原生 Flight 写入性能基准继续保留。

验证过程发现并修正两处测试前提：缺 writer 凭据测试还同时启用了不再允许的 Gateway；
多表测试还要求只有一张表存在 actor。保留首轮失败日志，最终验证通过后才记录结果。

## 验证结果

- `cargo test --offline -p zippy-core -p zippy-server -p zippy-gateway`：534 通过，5 个已有忽略项。
- 全工作区、全部 target 的 Clippy：零警告；Rustfmt 和 `git diff --check` 通过。
- 从当前源码重建服务端二进制和 Python 扩展；公开原生 API 重启矩阵通过，Arrow 23.0.1。
  覆盖三类表、WAL/Persisted、schema/drop、固定快照、订阅恢复与 ACK、计算和全部原生示例。
- Python 全量：595 通过；全仓 mypy：91 文件通过；客户端严格 mypy：27 文件通过；
  Ruff 与 Black：通过。

结构化结果、源码/二进制摘要和日志摘要见
[验收证据](perf_reports/evidence/2026-09-22-online-shadow-retirement/summary.json)。
本批未重新宣称 Arrow 25 发行包验证，也未宣称 Windows/macOS 实机通过。
第38批的归档发行记录是当时源码的证据，不能替代本批二进制摘要。

## 剩余边界

默认 Legacy 模式、Master 持久 registry、Gateway 读写/订阅与 memory ingest、旧查询解释器、
Rust/Python 内部 source/materializer 仍需删除。Master 还保留显式 endpoint snapshot 导入选项；
Catalog/Storage/Master 的底层旧导入、投影和事件类型仍存在，所以 **F15 未关闭**。
SQLite/native storage/native query 的默认主干切换及平台矩阵也未完成。

frame、KV、远端实时订阅和本地计算已有原生接替证据，后续以拆除上述旧实现为重点。
本批没有提交、推送或部署，也没有迁移、清空用户数据目录。
