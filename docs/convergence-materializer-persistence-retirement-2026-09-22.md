# 第四十八批：删除 materializer 的旧持久写入路径

日期：2026-09-22。承接[第47批](convergence-native-default-2026-09-22.md)。
累计 **32/34**；F08/F28 保持开放。

## 删除范围与保留边界

删除 StreamTableMaterializer 的旧 Parquet worker、任务队列、完成队列、重试发布器、commit 状态、
sidecar 元数据、独立分区文件写入，以及 descriptor forwarding 专属持久化支路。
删除三个公开持久配置/发布类型和仅供旧 worker 使用的方法，engines 不再直接依赖 Parquet。
源文件与专属测试均删除，未留下空实现或兼容转发。

原生 ingest 仍复用 StreamTableMaterializer 的共享内存、原子 frame、writer epoch 和段生命周期。
WAL、PersistController、TableActor 与 SQLite manifest 继续负责原生持久化；Memory/WAL/Persisted
承诺不变。持久前缀回收只接受原生 authority 提供的行数，不再维护第二套 worker 完成集合。
共享内存原生能力、计算内核与其他原生表实现均保留。

原有 retention 用例迁移为原生 manifest 前缀用例，验证不拆分段、不释放未持久尾段。
新增真实 mmap/reader lease 用例，验证读者 pin 阻止回收、释放后仅回收完整持久段，活跃尾段保留。
15 个仅覆盖旧 worker、sidecar 和旧发布器的测试退役；对应的新主干恢复验证继续执行。

## 验证

- engines/ingest/storage/server：480 次通过记录、6 个原有忽略项，包含子进程复验记录。
- 全 workspace/all-targets/all-features Clippy -D warnings、Rustfmt、diff 检查通过。
- 重建后 Python 全量 544 项通过；mypy 91 文件及客户端自身配置 27 文件、Ruff、Black 92 文件通过。
- 当前服务端/扩展通过三类表、schema/drop、固定快照、实时订阅/ACK、计算和重启恢复矩阵；
  扩展动态库依赖完整。

最终原生恢复与检查日志、测试退役映射和源码摘要见
[结构化证据](perf_reports/evidence/2026-09-22-materializer-persistence-retirement/summary.json)。
本批不关闭 F08：旧控制面和内部 source/publisher、旧 KV/shard materializer 等仍待退出。
平台验收仍限本机 Linux；不将仓库构建当作新的跨平台发行证明。未提交、推送或部署。
