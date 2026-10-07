# 第二十八批：KV 事件计算与启动契约校验

日期：2026-09-22。推进 F11/F13/F22/F29；原审计仍为 **25/34 已关闭、9 项开放**。
本批验证旧 KV 计算的完整变更日志契约在原生路径上成立，并修复空源无法提前发现 schema
不匹配的问题。没有把事件计算验收描述为 KV 状态视图的价格因子适配已经完成。

## 输入契约和生命周期

源码核查确认，旧 `KeyValueTableMaterializer.on_data()` 返回协议 v2 changelog，快照由独立
接口提供。原生日志同样携带更新、删除、版本边界和表身份，不能简单移除控制字段后作为行情。
`SubscriptionOptions.expected_schema` 现在可声明精确 Arrow schema，首次打开源和重新连接
时都检查 metadata、字段类型和可空性；不匹配时关闭 reader，不启动 callback worker。

`ComputePipeline` 自动使用当前内核输入 schema 约束订阅。调用方配置与内核矛盾时，在转移
内核所有权之前拒绝，原内核仍可使用；源表为空也会同步拒绝不匹配，而非等待首次数据到达。
单批处理中的 schema 校验继续保留。失败启动的 subscriber 从 Client 生命周期集合移除，
调用方捕获错误后关闭 Client 不会再次收到相同启动错误。

## 真实服务验收

新脚本 `scripts/check_compute_kv_input.py` 已接入原生完整验收矩阵：

- 空 KV 表配普通逻辑 schema 内核，同步拒绝且输出计数为零。
- 通过真实日志获取完整事件 schema，在计算启动前写入 A/B，再从 earliest 回放。
- batch_rows=1 拆分传输，两行 KV mutation 仍只处理一次并形成一个输出批次。
- 对每键事件序号显式转换为 float64 后求两期均值；原始 int64 序号和控制字段仍保留。
- 保持同一个 Rust 内核，正常停止并重启存储服务，再依次删除 B、删除 A、重插 A。
  输入续读原处理游标；输出六行的原序号严格为 1–6，均值为 null/null/2/3/4/5.5。
- 把计算输出中的原事件列交回 Rust KeyValueState，确认删除后状态为空，重插后仅有 A=9。
- 测试输入生产者显式启用 WAL/Persisted 恢复和不变表身份；普通 SDK 写入默认仍不自动重试。

这里滚动的是事件序号，删除事件仍属于事件历史。它不表示价格因子能自动忽略删除或清除历史，
也不表示计算进程重启后具备 checkpoint。earliest 是日志回放，latest 不是自动状态快照。
测试重启为正常停服，不是 SIGKILL 或断电。用户文档同时纠正了第 27 批完成后仍遗留的
“输出会话不能恢复”说明；旧批次记录作为历史证据保留。

## 门禁与剩余项

- 主包与客户端完整 Python 回归：820 passed、1 skipped。
- 最终客户端门禁：192 passed，覆盖率 84.83%。
- Arrow 23.0.1 / 25.0.1 仓库外 sdist/wheel 独立安装：各 187 passed、1 skipped，覆盖率 81.64%。
- 真实原生完整矩阵、Black 全仓 93 文件、Ruff、配置范围 mypy 与本轮扩展 26 文件通过。
- 全仓扩展 mypy 仍为 978 个错误，涉及 12 文件；检查 92 文件，F29 不关闭。
- 本批没有修改 Rust，未重复运行上一批已通过的 Rust workspace/Clippy。

日志摘要和哈希见[验收证据](perf_reports/evidence/2026-09-22-compute-kv-input/summary.json)。
本机仍为 Linux x86_64 / Python 3.11，不能代替跨平台 CI 或完整主包隔离发行。

KV 状态视图到价格/窗口/截面因子的更新和删除语义、算子 checkpoint、旧 source/Session/Pipeline、
主包查询与回调、旧 Gateway/Legacy/shadow 和控制面退出仍需继续实现；
F07/F08/F11/F13/F14/F15/F22/F28/F29 保持开放。
