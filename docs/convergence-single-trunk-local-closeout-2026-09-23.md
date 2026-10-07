# 第五十四批：本地单主干收敛验收

后续[第55批](convergence-native-commit-retirement-2026-09-23.md)进一步删除了本批暂留的
`CommitDataFilesIntent` 和预发布文件提交入口；下文保留第54批时点的原始判断与验收数字。

日期：2026-09-23。承接[第53批](convergence-actor-contract-2026-09-23.md)。
原审计 34 项中，F08 的本地代码收敛完成；F28 的 Windows/macOS 实机验收按用户指示延期，
因此当前为 **33 项关闭、1 项延期**。延期项没有通过记录。

## 唯一权威链

服务端仍保留 Master 的进程租约和配置发现，但表目录、请求幂等与文件可见性归 SQLite；
写入由原生 ingest、WAL、TableActor 和 PersistController 排序、提交、恢复。Flight 提供
远端写入、查询和三类表订阅；本地计算通过相同原生入口写回。空目录启动和重启恢复
均不使用旧 Master 持久 registry、Gateway 或旧 materializer。

本批从 segment-store 删除通用 Parquet 队列、后台 worker、重试和 dead-letter、自动入队
rollover、专用 failpoint 及测试。rollover 现在只封存 segment、保留被 pin 的旧内存并通知
读者；文件持久化及清理由原生控制器与 Catalog 的同一发布权限负责。删除旧 worker 的
直接测试，保留租约、通知、行视图、Parquet 候选、意图恢复和原生重启测试；证据清单
记录退休测试名称。

Master 的进程认证 token 与 boot identity 改用跨平台 OS 随机源。取随机失败会拒绝
注册或停止启动，不再以时间戳和哈希生成可预测 token。进程记录的 token、revision
字段改为必填；新增失败注入测试确保注册失败不会修改租约表或控制版本。

`CatalogCommitCoordinator::start` 现在必须接收具备幂等读取、文件引用读取及发布互斥的
`CatalogPersistence`；旧 `start_with_persistence` 双入口及可缺失持久能力的运行分支退出。
测试中的故障注入写入器复用真实 SQLite 读侧和发布边界，保持队列、故障、意图与孤儿
文件场景可验；只针对“无发布控制器也能启动”旧状态的负向用例退休。

复核了仍公开的 `write_candidate()` 与 `CommitDataFilesIntent`：它们用于构造已发布物理文件、
保留意图与故意损坏文件的低层测试，不参与服务端在线写入路由，也不能自行使文件成为
Catalog 可见数据。`write_candidate()` 已从公开文档中标为低层测试夹具；生产写入只能经
`PersistController` 的 staged publication。此处保留恢复故障注入能力，不将它解释为
另一套旧格式或旧服务端主干。

## 验证边界

全 Rust workspace **1196 条通过记录、7 个原有忽略项**；全目标全 feature Clippy
`-D warnings` 通过。重建最终服务端和 Python 扩展后，Python 全量 **543 项**、原生
append/frame/key-value、订阅、计算、固定快照与重启恢复矩阵通过。mypy、Ruff、Black、
Rustfmt 和 diff 检查通过。完整日志、测试退休清单、最终二进制摘要与矩阵结果见
[结构化证据](perf_reports/evidence/2026-09-23-single-trunk-local-closeout/summary.json)。

这些结论仅覆盖本机 Linux x86_64、CPython 3.11 和当前本地构建。F28 的 Windows/macOS
实机与更宽 Python/Arrow 版本发行矩阵未执行，不能据此宣称全平台兼容。没有提交、推送、
部署或迁移旧数据。
