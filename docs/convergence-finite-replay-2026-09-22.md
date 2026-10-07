# 第三十三批：有限回放与原生发布收敛

日期：2026-09-22。推进 F14/F22/F29，原审计仍为 **25/34 已关闭、9 项开放**。

ParquetReplaySource 独立为有类型检查的模块，读完后 flush、stop 并自然结束，不再等待外部
stop 才释放线程。单一 replay_rate 参数接受有限正数，取消能打断长时间节拍等待；删除重复的
mode 参数。启动前校验完整 schema（含 metadata/nullability）和每个文件 fragment，不再隐式
转换类型。一个源同时只有一个 owner，完成后可以重新启动；失败保留首个异常，取消不伪装完成。
SourceInput 修复启动前停止不释放 join，以及取得 handle 期间取消后遗漏 stop 的两处竞态。

ParquetReplayEngine 只保留显式文件的 row/table 回调，复用相同有限源，不再另建生命周期实现。
删除旧 output_stream/master/persist 等发布参数、TableReplayEngine、全局 replay() 和按旧
registry.persisted_files 找文件的读取逻辑。命名输出由原生建表、NativeProcessor 和
ComputePipeline 承接；历史表输入使用 Flight 日志，固定比较使用 Client 多表 snapshot。
主包从 10804 行降至 9915 行，没有保留旧名称转发包装。

七个旧回放发布测试随被删除路径退出，其有效行为迁移如下：文件到三类表、任务停止后查询及
重启恢复由真实原生矩阵验收；开始回放前先订阅及完整 frame 批次由 check_compute_plugin
验收；跨表结果一致性与日志驱动 latest 由重写后的 examples/06_replay 三个公开示例实际运行。
回调顺序、时间闭区间、四种 timestamp 单位的整数精度、原异常传播和超时取消继续保留测试。
旧伪时钟测试改为直接测试绝对节拍和取消事件，避免 monkeypatch 全局 time 影响线程。

## 验证

- 最终全仓 pytest：876 passed、1 skipped。首次全仓 4 个失败来自调用已删除私有行转换函数，
  已迁至新回调入口，保留原精度断言，最终全部通过。
- 回放、输入生命周期、计算与共享行值 56 passed，覆盖率 89.45%；回放模块单独 90%。
- 三个公开示例四次真实执行通过；原生三类表、预先订阅、有限源自行退出、任务停止后结果查询、
  固定快照、ACK 和重启恢复矩阵全部通过。
- 全仓 Black（102 文件）、Ruff、Rust fmt、diff 通过；根 mypy 17 文件、本批 12 文件通过。
  扩展全仓 mypy 为 961 errors / 10 files / 101 checked，比上一批减少 10 个错误。
- 本批没有修改 Rust 或独立客户端，不重复前批 Rust workspace/Clippy 与独立客户端发行矩阵；
  主包完整隔离发行、跨平台与剩余全仓类型错误仍未完成。

证据见[验收记录](perf_reports/evidence/2026-09-22-finite-replay/summary.json)。旧主包
Table/Session/Pipeline、Gateway adapter 与双数据权威仍存在，不能据此关闭整体收敛项。
