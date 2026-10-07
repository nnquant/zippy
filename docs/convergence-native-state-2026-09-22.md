# 单主干收敛第十二批：原生状态订阅与完整恢复

日期：2026-09-22。原 34 项累计关闭 **22/34**，剩余 **12 项**；本批关闭 F16，推进 F11/F22/F28。
[第十一批](convergence-client-log-2026-09-22.md)的原子日志交付现已支撑主包状态订阅。

## F16 关闭依据

原问题要求原生 KV 持久化与 delete、跨机器一致快照和游标，以及摆脱自定义 snapshot_loader 的
状态恢复。前批已验收 WAL/Persisted、原子 upsert/delete 和完整快照 RPC；本批完成最后的状态消费链路：

- `StateSubscription(source, client=..., namespace=...)` 只调用 `zippy-client` 的原生 Flight。
  删除旧 `snapshot_loader`/`master` 构造路径；本地计算引擎及低层 KeyValueState 保留。
- 先附着日志固定水位，再按不可变 table ID 请求完整状态和一致游标。快照覆盖的完整 mutation
  去重，后续 mutation 仍由同一 Rust 状态引擎原子应用，半个版本不可见。
- 断连立即撤销可读状态；恢复重新认证、按原 table ID 开流并取新快照，验证身份和 schema。
  不以同名新表或最新增量冒充完整恢复。metrics 显式记录重建代次、重试和最后错误。
- 有界开流/快照截止、组装预算、可取消退避与重试次数限制；stop 取消网络读取并等待线程。
  开流超时仅作用于建立过程，不把持续流寿命限制为几秒。
- 协议/权限错误及管理员取消不自动重连。服务整体停机返回可重试 UNAVAILABLE，显式取消仍为
  不可重试 CANCELLED；保持这两种操作的不同语义。

真实服务验收从空目录、公开 API 建表开始，包含五次启动。原生 StateSubscription 完成 upsert、
delete、停机期间拒绝旧状态、同目录重启重新认证、恢复原表完整状态，然后继续 delete/upsert。
原三类表回放、原子帧边界、已交付游标续读、删除后空状态恢复仍全部保留。

自动重建恢复当前状态，不承诺中间事件逐条交付或业务 exactly-once。服务端 durable ACK、旧 callback
迁移和统一 SubscriptionService 仍属 F11；这些未实现能力不会被本次 F16 关闭掩盖。

## F44：Flight Arrow 缓冲区对齐导致原生进程中止

首次真实验收在 Rust arrow-array 53.4.1 FFI 导入字符串 offsets 时触发 misaligned pointer dereference，
Python 进程以 134 退出。普通 pa.array 单测没有覆盖 Flight 解码缓冲区的这一布局。

FlightCallOptions 统一指定 `IpcReadOptions(ensure_alignment=Alignment.DataTypeSpecific)`，由 Arrow
按需要对齐接收缓冲区，避免额外 Python 逐行转换或不同版本的 fallback。相同真实验收在
PyArrow 23.0.1 和 25.0.1 均通过。两包最低 Arrow 版本收敛为 23；未宣称所有未来版本均兼容。
F44 是原清单之外的新增修复，不单独增加原 34 项关闭数。

## 包依赖与验证

主包正式依赖 `zippy-client==0.1.0`，uv workspace 和 lock 同步，避免状态 API 依赖偶然安装的客户端。
两包 sdist 在无 Git 元数据的目录构建 wheel，再在新 venv 安装并验证导入和 CLI。构建过程中
Maturin 对客户端目录打印一次找不到默认 Cargo.toml 的探测信息，随后按显式 manifest-path 成功；
整个归档检查退出 0，并非编译失败被忽略。

- 主包 Python：610 passed，1 skipped（既有跳过项）。
- 客户端：113 passed，覆盖率 81.67%。
- Rust workspace 全套、全目标全特性 Clippy（-D warnings）、rustfmt 和 diff 检查通过。
- Black 全仓 86 文件、Ruff、根配置 mypy 14 文件和客户端 mypy 26 文件通过。
- 本机 Linux x86_64 / CPython 3.11；真实恢复分别通过 Arrow 23.0.1 和 25.0.1。

命令结果、日志和源码哈希见[结构化证据](perf_reports/evidence/2026-09-22-native-state/summary.json)。
当前使用方式见[状态订阅文档](key-value-v2-migration.md)。

剩余 F07/F08/F09/F10/F11/F13/F14/F15/F22/F26/F28/F29 仍开放：目录和写入权威切换、alter/drop、
订阅持久确认与旧回调、原生计算输出、重复查询/客户端删除、认证会话统一、类型债务及平台矩阵。
Gateway 和旧目录尚未删除，整体任务未完成。
