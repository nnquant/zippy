# ADR 0007：新版本契约与兼容分支退出

决策：2026-09-21，用户明确授权放弃旧 API、配置及数据格式。
实施状态：本地单主干已切换；原审计 33/34 项关闭。F28 Windows/macOS 实机验收按用户要求延期，
不计为通过。当前验收范围见[第五十六批实施记录](../convergence-query-alias-control-deadline-2026-09-23.md)。

## 决策

- 服务生命周期由 Server 持有；Python `zippy server` 和 Rust `zippy-server` 调用同一运行时。
  不再保留独立 Master daemon、Gateway run 或进程管理器的旧服务任务模板。
- 控制请求只走 typed enum dispatch；旧 Envelope/verb/inner 入口删除。
- CPU 配置只有 `max_cpu_cores` / `ZIPPY_MAX_CPU_CORES`，ready 状态只有 v11。
- SQLite、native storage 和 native query 固定启用，删除后端选择及 native 开关。
  Flight 默认监听 `127.0.0.1:17667`，端口不从 Master 派生；关闭监听器不关闭原生恢复。
- 轻量客户端以显式 `Client` 持有连接，删除默认全局连接与模块级兼容调用。
- 查询、列表、schema、等待就绪使用同一 transport 的 Catalog，不跨目录回退。
  Flight 缺少 bindings、发现失败或固定跨查询快照能力不足时直接失败。
- 默认写入要求 WAL；只有显式 `durability="memory"` 才能选择 Memory 确认。
  `Persisted` 仅在服务端公布对应 capability 时可请求；必须等待 manifest 覆盖和回执持久归档，
  不得用 WAL 成功或后台最终落盘代替确认。
- proto 是 Rust/Python wire 类型的唯一来源；生成器版本固定，再生、编译和打包资源必须受 CI 检查。
- 正式 Python 包不依赖相邻源码库；进程管理器原生服务通过既有 RPC 独立提供。

## 数据与功能边界

只接受新数据格式和空目录初始化，不开发旧目录自动导入或迁移工具。
新格式已通过公开 API 与重启恢复验收，旧目录保持原样并明确拒绝。
Master 持久 registry、旧 Catalog 适配器和 compaction worker 已删除；临时 registry
只保留进程租约，不再提供旧表/生产者控制接口。内部 Source/Publisher、双队列、
ZeroMQ IO、KV/shard materializer、descriptor forwarding、底层旧持久 worker、
只读 actor 兼容入口、shadow、导入通道、Gateway 数据协议和旧 memory ingest 已退出。

append/frame/key-value、远端订阅与本地计算是有效产品能力。必须补齐原生建表、删除、写入、
查询/订阅和重启恢复，才能移除仍承载这些能力的旧实现。单次查询快照与跨查询固定租约必须区分；
当前 Flight 已支持固定跨查询快照租约，过期和重启必须明确失败。

## 与历史决策的关系

ADR-0004 的目标仍是 Flight 统一远端协议，但其早期阶段描述以当前能力为准；已实现的 WAL、
checkpoint 与回收参见 ADR-0005/0006。ADR-0006 的 KeyValue v2 增量契约保持有效。
不同格式的版本空间彼此独立，不能为了收敛把 SQLite、WAL、SHM 和 Flight 的版本混为一个数字。

验证结果和未关闭事项见 [实施记录](../current-capabilities-2026-09-21.md)。

## 2026-09-21 补充确认

用户进一步确认：frame 表、key-value 表、远端实时订阅、本地计算引擎全部保留，补齐原生实现及
恢复测试后再删除旧路径。这是后续切换的硬性验收条件，不再作为待定产品范围。

主包必须加载原生扩展，不再提供 `ZIPPY_FORCE_PURE_PYTHON` 降级；远端独立应用使用
`zippy-client`。主包与轻量客户端已共用查询编译器和显式 Client，F22 已关闭。

发行版本由包清单决定：主包运行时 `__version__`、`version()`、Server ready 的版本字段均为
发行版本。`__build_revision__` 单独记录可选的完整 Git HEAD；它只标识构建工作树的基线，不证明
工作树干净。无 Git 的源码包中该字段为 `None`，不得把它伪造成当前仓库的 revision。

Flight 与控制错误契约包含结构化错误码和 retryable；缺少必要字段的旧响应明确拒绝。Gateway 错误协议随实现删除，F25 已关闭。
