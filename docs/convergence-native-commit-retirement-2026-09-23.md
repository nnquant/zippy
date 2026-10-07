# 第五十五批：移除预发布文件提交入口

后续[第56批](convergence-query-alias-control-deadline-2026-09-23.md)移除了残留查询兼容别名与
无期限控制请求入口；下文保留第55批时点的验证数字和判断。

日期：2026-09-23。承接[第54批本地收敛记录](convergence-single-trunk-local-closeout-2026-09-23.md)。
原审计仍为 **33/34 项本地关闭**；F28 Windows/macOS 实机验收依用户要求延期，未计为通过。

`CatalogCommitCoordinator` 已不再公开 `CommitDataFilesIntent` 和 `commit()`：原入口可接收
预先发布的 Parquet 文件，并在没有原生 staged publication permit 的情况下形成 Catalog
可见数据。现在在线持久化只从 `PersistController` 接收 `PersistCandidateInput`，先探查请求幂等，
再取得发布权限、发布文件和提交相同意图。重启恢复仍会读取已有的持久意图，并以相同
Catalog 命令重放；该恢复路径没有当前进程的 staged 候选，属于同一主干的恢复阶段。

测试也已换到这个入口。服务端的三项启动/多文件/缺文件回归，以及存储层的三项保留
意图/多文件/部分提交回归，继续保留原断言，并由原生控制器产生意图。原先只调用旧
`commit()` 的 `catalog_commit.rs` 退休，其场景由下表中的现存或新增测试接替：

| 退休测试场景 | 原生路径验证 |
| --- | --- |
| SQLite 快照提交 | `persisted_receipts_cover_real_manifests_and_survive_restart_exactly` |
| 明确拒绝后继续处理 | `definitive_post_publication_rejection_cleans_owned_candidate_and_continues` |
| 回执未知，原样重放 | `native_commit_unknown_replays_the_identical_catalog_envelope` |
| 等待者取消但已接收作业排空 | `dropped_ticket_does_not_cancel_and_drain_closes_admission` |
| 终止失败保留意图与启动恢复 | `startup_replays_retained_intent_before_publishing_final_reconciliation` |
| 启动丢弃明确被拒意图并接收新工作 | `native_startup_discards_definitively_rejected_intent_and_accepts_new_work` |
| 文件清理失败关闭写入 | `rejected_candidate_replacement_fails_cleanup_closed` |
| 候选篡改、回执身份不符 | `native_replay_rejects_tampered_retained_candidate_before_catalog`、`native_mismatched_catalog_response_retains_intent_and_fails_closed` |
| 未轮询/超额请求不入队 | `admission_distinguishes_oversized_and_temporarily_exhausted_input`、`config_and_request_validation_rejects_work_before_startup` |
| 同表提交顺序 | `same_table_admission_is_single_flight_until_commit_completes` |
| 意图临时文件与符号链接目录 | `native_startup_removes_partial_intent_and_rejects_symlink_directory` |

`write_candidate()` 仍是低层物理文件测试工具，用于孤儿与文件校验用例，不会使文件
成为 Catalog 可见数据。Master 仅保留进程租约与配置发现，SQLite、原生 ingest/WAL、
TableActor、PersistController、Flight 和本地计算是当前权威链。

最终全 Rust workspace 为 **1190 条通过记录、7 个忽略项**；全目标全 feature Clippy
`-D warnings` 通过。重建服务端和 Python 扩展后，Python 全量 **543 项**通过，原生矩阵
58 个结果字段中 49 个布尔验收项全部为真；其余是运行环境或实际恢复值，没有失败字段。
Rustfmt、diff、Black、Ruff 与两套 mypy 门禁通过。日志、源码与二进制 SHA-256 见
[结构化证据](perf_reports/evidence/2026-09-23-native-commit-retirement/summary.json)。
这些验收覆盖本机 Linux x86_64、CPython 3.11、Arrow 23.0.1，不能替代 F28 的
Windows/macOS 实机验证；没有提交、推送、部署或迁移旧数据。
