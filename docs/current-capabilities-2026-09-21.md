# 当前能力与单主干切换边界

最终范围：原审计的 33 项本地收敛事项已完成；用户明确决定跳过 F28 跨平台实机验收。
F28 未验收，不计作通过；本页不宣称 Windows/macOS 实机或全部后续 Python/Arrow 版本兼容。

本页更新于 2026-09-23，以[第五十六批实施记录](convergence-query-alias-control-deadline-2026-09-23.md)
、[控制入口后续复核](convergence-control-registry-bounds-2026-09-23.md)和
[快照绑定复核](convergence-table-snapshot-binding-2026-09-23.md)中的验证结果为证据。
首次建表 WAL 目录同步补强见[持久性复核](convergence-wal-directory-durability-2026-09-23.md)。
联接查询直接调用 `snapshot()` 的全部来源捕获与独立克隆见
[联接快照复核](convergence-joined-snapshot-2026-09-23.md)。
嵌套旧配置键的静默忽略已修复，客户端旧控制端点环境变量也已明确拒绝，见
[严格配置复核](convergence-strict-config-2026-09-23.md)。
历史设计和审计用于追溯，不替代本页的当前能力说明。
现行查询、canary、计算和单节点运行文档已按本页修正，见
[当前文档边界复核](convergence-current-documentation-2026-09-23.md)；
`server-owned-auxiliary-services.md` 等早期方案保留历史设计标识，不能当作现行部署步骤。

| 能力 | 当前实现 | 已清理路径与现存边界 |
| --- | --- | --- |
| 查询编译与派生表 | 两包共用表达式和 SQL 编译器；任意 SQL 统一到同一个显式 Client 的 Flight 方法，主包旧函数/正则绑定推断与独立客户端全部 Gateway 数据路径已删除；原生派生表、外连接键、纳秒过滤及排序后 head/slice/tail 重启恢复已验收 | 主包 Table、Gateway residual、本地 Parquet 查询及快照入口已删除；Server 旧 QueryEngineAdapter、Gateway collect facade 和组合层已删除，Gateway crate 及其协议内查询解释器已删除；复杂派生查询预留量偏保守，验收明确使用 4 GiB，生产默认 512 MiB 未改 |
| 独立客户端发行 | 纯 Python wheel；SHM Rust crate/扩展/reader lease 已删除；Arrow 23/25 的仓库外独立安装与 80% 覆盖率门禁通过 | 跨平台实机矩阵按用户 2026-09-22 指示延期，尚无 Windows/macOS/Python 3.14 新验收结果；客户端查询/写入/订阅均仅走 Flight；主包 Gateway Python 实现已删除，服务端 Gateway 已删除 |
| 服务生命周期 | Rust `zippy-server` / Python `zippy server` 共用 Server 运行时 | Master 持久快照和旧 Catalog adapter 已退出；控制 registry 已仅保留配置发现与进程租约 |
| 原生三类表写入与查询 | 空目录管理员在线建表、动态注册；Flight → native ingest → WAL / TableActor；DataFusion 查询；命名空间贯穿写入和重启恢复；公开 Flight/Python schema 变更 | 主包旧 Session/Pipeline 已删除；旧数据控制面已删除；Rust 旧 Source/Publisher、KV/shard materializer 与 descriptor forwarding 均已删除 |
| 元数据与等待 | 客户端、table CLI 与 WebUI 共用原生 Catalog；CLI/WebUI 共用固定快照元数据检查；WebUI 同名表按命名空间区分、显式排序预览、精确数值显示与 HTTP 重启已验收；重复 stream 命令和 WebUI 旧 registry/Gateway 路径已删除 | 表控制面已统一 Flight；Rust 旧 Source/Publisher 已删除 |
| 确认持久性 | 原生 append/frame/key-value 支持 Memory/WAL/Persisted；默认 WAL；Persisted 等待 manifest 覆盖及回执持久归档 | 主包读写入口已统一 Client；旧 Gateway memory ingest 已删除，唯一原生 ingest 权威 |
| frame | 原生完整帧写入、查询、WAL/manifest 恢复及回执重试已验收；修复同 producer epoch 重启后 source identity 复用导致的帧合并，确定性回归及原真实恢复矩阵通过；主包旧本地回调入口已删 | Rust 内部旧 source 与订阅绑定退出 |
| key-value | 原生在线建表、upsert/delete、逻辑查询、WAL/Persisted、恢复及一致状态快照与游标 RPC 已验收；StateSubscription 原生快照+日志及重启恢复已验收 | Master 旧持久目录、快照恢复和 compaction worker 已退出；临时旧控制请求也已删除 |
| 远端实时订阅 | Client.open_log 支持原生三类表、原子 frame/KV 交付和显式游标；Catalog 位点、Flight/Python 原子交付及显式持久 ACK 已接通；Client.subscribe/subscribe_table 已统一原生 callback、处理后 ACK、固定身份重连；独立客户端旧 Gateway/SHM subscriber 已删除；主包旧 callback 已删除 | Gateway 旧远端订阅已删除，F11 关闭；KV 吞吐验收 |
| 本地计算引擎 | NativeProcessor 复用五类 Rust 内核，增加原生顺序 chain 与无因子 identity；ComputePipeline 通过 Flight 写三类表；有限 Parquet 源自行结束；旧表回放与 SHM 发布入口已删，直接回调复用相同源；Python source、串联状态与尾批、三类结果重启恢复、取消与失败传播已验收；同一内核输出恢复及 KV 事件计算已验收；Python Session/Pipeline、扩展中的旧后台引擎、source/materializer、SHM 订阅及 ZeroMQ 绑定已删 | Rust 内部旧 source/materializer 退出；KV 状态视图到价格因子的自动适配及跨进程持久算子 checkpoint 尚未提供，恢复依赖显式重放。这两项是新增能力边界：旧 KV materializer 输出的是变更日志，旧内核也没有跨进程持久 checkpoint |
| 固定跨查询快照 | Catalog pin/live 边界、独立查询计费、原生 Flight 租约与 SDK 多表 scope 已验收；普通 `Table.snapshot()` 会绑定后续查询与克隆到返回的快照；schema/drop/同名替换仍读原边界；过期和重启明确失败 | Python 旧快照入口已删；服务端旧 Gateway 租约已删除；短期租约不等于持久 checkpoint |
| Flight 会话 | Handshake 签发短期 bearer；Client.close 显式撤销全部持有会话并回收未消费 ticket、固定快照和名额；32 会话上限下连续 40 次短客户端关闭已验收；本地只读握手仅限 loopback | 网络不可达时本地关闭不等于远端立即回收，最终由 TTL 清理；生产 TLS/bootstrap 配置保持必需 |
| 控制错误契约 | code / retryable 必填；拒绝旧 uncoded 错误；客户端按错误码分类；进程注册标签最多 256 字节、租约记录最多 4096 条 | 旧表/生产者控制错误映射和 debug listener 的测试命令已删除；满额只回收已过期租约，活跃租约不逐出 |
| 独立客户端 | `zippy-client.Client` 显式持有连接；无全局默认客户端 | 主包 StateSubscription 已正式依赖客户端，独立客户端 callback 已统一原生；客户端与主包 Gateway 查询解释器及帧协议已删除；主包入口仅 136 行，因子配置和 Arrow 回放比较独立成模块，连接/查询/订阅只有显式 Client |
| 数据目录 | SQLite 只接受 v11 布局，旧 v1–v10 拒绝且不改写；在线影子链、Catalog 三代导入/投影通道、Storage 外部旧文件导入、endpoint snapshot 导入、Master 投影事件及 V2/V3/V4 协议已删除；旧配置明确拒绝；原生所有表都由 TableActor 提供查询边界 | 外部旧文件、导入快照、额外排序和 unknown 位点已删除，F15 关闭；Master 持久目录已删除，旧 registry 文件明确拒绝且不改写；SQLite、原生存储与查询固定启用，默认 Flight 已验收，F07 关闭；临时旧表/生产者接口已删除；旧 Source/Publisher、双队列与 ZeroMQ IO、旧 KV/shard materializer 和 descriptor forwarding 已删除；描述符版本字段已强制；只读 actor 入口、验证包装、底层旧持久 worker 和 Catalog 提交器双入口已退出；Master token 回退已删除，F08 关闭 |

## 包与平台证据

- Server / 主包发行版本为 `0.5.0`；独立客户端为 `0.1.0`；Flight wire 协议为 v1。
  两包版本独立，协议版本承担连接契约，不再将 Git commit count 混入发行版本。
- 当前本机验收为 Linux x86_64、CPython 3.11；开发测试环境使用 Arrow 23.0.1；干净环境
  分别构建安装两包 sdist/wheel；Arrow 23.0.1 和 25.0.1 均已验证真实状态恢复。
  第 38 批完整发行验收从源码归档重建主包；独立客户端两种 Arrow 版本各 219 项通过，覆盖率
  85.19%。主包原生恢复由安装后 CLI 启动，不借用仓库二进制；本轮新增七个公开示例的真实验收。
  包声明的 Python `>=3.11` 和 Arrow `>=23` 依赖范围不等于后续所有 Python/Arrow 版本均已验收。
- 2026-09-23 当前源码复验：两包从无 `.git` 的 sdist 构建并隔离安装，独立客户端
  239 项通过、覆盖率 85.18%；由安装版 CLI 启动的三类表原生恢复矩阵 49/49 项通过。
  当前 proto 再生无差异，协议生成器格式与 Clippy、console 构建及三项打包资源一致性检查通过。
  上一条第 38 批数字保留为历史节点，本条为本机最近一次发行证据。
- 后续端点契约收敛后，本机 Rust workspace tests、全目标全 feature Clippy、
  仓库 Python 571 项、独立客户端隔离发行 247 项通过；后者覆盖率 85.34%。
  这些本机测试不能替代已跳过的 F28 实机验收。
- Windows CI 有 Rust 编译和客户端契约检查；没有 Windows/macOS 实机结果，F28 按用户决定跳过。
- `rust-toolchain.toml` 是开发和 CI 的工具链版本来源；协议生成器有固定版本和再生无差异门禁。
- CLI 整个 `gateway` 命令组、旧性能辅助模块和手写 Gateway 协议示例已删除。远端示例只依赖
  独立客户端，使用原生固定快照查询、persisted 写入和本订阅句柄的交付计数，见
  [原生远端客户端](native-remote-client.md)。主包和服务端 Gateway 均已删除。

## 原生表管理边界

namespace/append/frame/key-value 在线创建已经协调 Catalog 提交、actor 创建、materializer 和 ingest 注册；
三处表集合支持动态安装，并纳入同一停服排空。空目录的公开 API 建表、写入、查询和重启已验收。
公开 Flight/Python drop 已接 Catalog、ingest、actor、materializer 和持续日志退出；三类表删除后重启
及幂等重试已验收，运行时表数量及逐表元数据额度可回收。旧固定快照仍受 pin/lease 保护。
同名新 ID 重建已验收，旧 ID、请求、游标和历史快照仍绑定原表；满容量下旧建表请求精确重放不占新额度。
原生 Catalog/Server 已支持追加可空列、普通列放宽可空性及描述元数据变更；先刷净旧 WAL，再原子
推进 schema/epoch 并替换运行时。历史文件按当前 schema 补空，旧固定快照继续持有原 schema。
建表和变更在提交前复用原生 materializer 的真实布局编译器校验类型，不能把通用 Arrow
可解码类型全部当作可写类型。公开 Flight/Python schema 变更已覆盖三类表；KV 显式携带不变主键。
旧 writer/日志以 SCHEMA_MISMATCH 结束，旧 schema 游标不静默续读；旧请求在较新 schema 或删除后
仍重放原结果。schema 指纹只依据有界校验后的逻辑内容，不受 IPC 对齐或元数据键顺序影响。
F07/F08/F09/F11/F13/F14/F15/F22/F29 已关闭，原审计本地事项累计 33 项完成；
F28 跨平台实机验收按用户决定跳过，未计为通过。Catalog 持久消费确认已接通身份/schema/CAS/持久水位校验，
只读位点与写入共用同一 Catalog；原生消费服务及公开 Flight/Python 已验证
原子交付、ACK 持久化、组互斥、回执丢失和重启恢复；独立客户端 callback 已原生化，主包旧 callback 已删除；Gateway 旧远端订阅已删除，F11 关闭。
WAL/receipt 物理回收按原生生命周期治理；Master 旧 compaction worker 和 Gateway 固定快照实现已删除。

用户要求保留的 frame、key-value、远端实时订阅和本地计算已有原生接替及恢复证据。
旧 source/publisher、KV/shard materializer、底层持久 worker、预发布文件提交入口、
独立客户端查询兼容别名、core 旧 stream schema/FNV 工具与无期限控制请求入口已退出；
本地因子的批量与上下文执行已归并为一个上下文内核；专属基准也已删除。
Master 的进程租约仍在，认证 token 使用 OS 随机源，持久表目录只由 SQLite 持有。
默认 mypy 已从文件白名单扩大到全部 Python 源码、测试、脚本、示例和客户端，当前 91 文件通过；
客户端自身配置启用 check_untyped_defs，27 文件通过。负向输入测试仍执行原来的运行时拒绝断言。
