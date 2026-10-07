# 单一主干收敛方案

状态：2026-09-23 本地权威切换完成，原审计 33 项本地事项关闭；用户决定跳过 F28
跨平台实机验收，未计为通过。本次目标按调整后的范围完成。下文保留实施前的阶段方案，
当前事实见[能力表](current-capabilities-2026-09-21.md)。
依据：[2026-09-18 审计](architecture-convergence-audit-2026-09-18.md)。

当前完成情况、实施证据与仍未通过的仓库门禁统一见[当前能力表](current-capabilities-2026-09-21.md)。

## 目标

正常运行只存在一套持久元数据、一套写入及恢复权威、一套通用查询语义。历史兼容不能借助自动
fallback 重新进入主运行路径。旧 API、配置和数据格式均不再承诺兼容，不开发离线迁移工具。

| 职责 | 唯一实现 | 其他代码的最终位置 |
| --- | --- | --- |
| 启停、配置、资源、健康 | `zippy-server` | Python CLI 是启动器；不再有独立 master/gateway 产品生命周期 |
| 持久表目录、schema、manifest、producer fencing | `zippy-catalog` / SQLite | 旧 JSON registry 的持久目录职责删除；进程/租约仍属运行时 registry |
| 写入、offset、幂等、ACK、恢复 | `zippy-ingest` + TableActor + WAL/checkpoint | GatewayMemoryIngest 不再持有第二份表状态 |
| 通用查询与 snapshot lease | `zippy-query` + Server QueryService | 删除 Gateway 运算解释器和 Python residual；因子引擎保留 |
| 远端数据协议 | `zippy-protocol` / Flight | 完成订阅后删除 Gateway 数据协议及双套 framing |
| 本机热数据访问 | `zippy-segment-store` / SHM | 作为相同 snapshot/offset 契约的优化，不形成独立 Catalog |
| Python 连接与查询 | `zippy-client` 的显式 Client | 主包持有 server/engine/producer，复用客户端，不复制 SQL 编译器 |
| KeyValue 状态消费 | v2 增量 + 显式快照/游标 | 不恢复旧每批全量输出模式 |
| 数据目录 | 只打开新版本格式 | 拒绝旧目录，不导入、不覆盖；删除 legacy primary/shadow/native 三选一 |

## 边界决策

用户已明确选择：“完全面向新版本，可放弃旧 API、配置及数据格式”。据此删除兼容接收分支，
不实现旧数据 migrator，也不保留在线 shadow 作为长期选项。旧目录必须明确拒绝，不能自动清空或
改写版本标记。此决定不意味着取消 append/frame/key-value、远端订阅或本地计算功能，也不授权
修改生产目录或部署。

## 阶段 A：先冻结当前契约，修复已经复现的问题

- 删除重复的 Control Envelope/verb/payload 路由及响应缓存；控制请求统一走现有 typed enum 的鉴权与 dispatch。Watch 客户端同步迁移。写入幂等仍由原生 ingest 的 producer/sequence 契约承担。
- 控制 listener 加连接许可、字节预算及整体 deadline。
- 删除 Flight 发现失败的自动降级；默认写入持久性固定，失败明确返回。
- 统一列举/等待/schema 与查询的 Catalog 来源。
- 区分单次 query snapshot 和跨 query snapshot handle；旧 `snapshot=True` 不再暗含不同保证。
- 让发行构建不依赖 `.git`；使用同一解释器构建并测试 wheel/sdist。

验收：旧 Envelope 被拒绝且无注册副作用；协议错误不切换写入持久性；Flight-only 表可列举和等待；
没有 `.git` 的源码包可以构建；正向行为与错误码都有回归用例。

## 阶段 B：删除已废弃的公开表面

1. 配置只接收 `runtime.max_cpu_cores` 和 `ZIPPY_MAX_CPU_CORES`；旧名称明确报错。
2. 同版本 daemon readiness 只接收当前格式，删除补齐旧字段的状态转换。
3. 对外服务入口统一 `zippy server`；更新全部示例、stub、测试和运维文档后删除 daemon 别名。
4. 发布 API 只保留 table builder；删除 deprecated 方法和以“仍然兼容”为目标的测试。
5. 移除无引用的旧 vendor/fixture；旧格式只作为拒绝输入的测试样本保留。
6. 拆分全量 Python 巨型模块，连接/查询复用 `zippy-client`；保留显式生命周期所有权。

验收：静态搜索没有旧入口的生产调用；旧配置/API 给出明确失败；新入口覆盖同样的有效功能。
删除测试时按新契约迁移断言，不能靠减少覆盖规避行为差异。

## 阶段 C：补齐原生产品闭环

需要先解决以下缺口，之后才具备删除旧 Catalog/Gateway 的条件：

- 空数据目录通过正式管理 API 创建 namespace/table/schema，不依赖 seed 或离线直接写 SQLite。
- drop/alter/schema 变更与 producer epoch、snapshot pin、历史文件 GC 保持一致。
- 本地 producer 和计算引擎注册到同一 Catalog/RuntimeRegistry，并由统一 ingest 发布数据。
- append、frame、key-value 分别定义真实支持的持久性及恢复；尚未支持的操作明确拒绝。
- Flight DoExchange 承接订阅，规定游标、gap、ACK、背压、重连、取消和 shutdown。
- KeyValue 初始化有可远端调用的一致 snapshot+cursor；raw append 缺口必须从事件历史补齐。
- WebUI、CLI、list_tables、wait 与 SQL metadata 全部读取相同的表身份和状态。

验收场景必须覆盖：空目录 → 公开 API 建表 → 写入 → 查询/订阅 → 正常停服 → 重启恢复 → 删除。
另外覆盖慢读者、磁盘故障、回执丢失、producer fencing、空批次以及 snapshot pin 未释放的停服。
不能用只测 append 的结果替代全部产品能力验收。

## 阶段 D：确立新数据格式边界

1. 权威切换时发布新 managed data root 格式版本，只允许新格式和空目录初始化。
2. 旧版本、无版本的非空目录、旧 endpoint snapshot 均明确拒绝；不提供自动 import 或就地转换。
3. SQLite 当前物理布局独立建库；删除仅为旧 registry 导入、projection/reconcile 保存的表与代码。
4. WAL/checkpoint/SHM 的格式各自显式校验，不把不同版本空间混成一个递增数字。
5. 旧目录保持原样；部署使用新目录。回滚使用对应二进制和目录，不混写格式。

不因“允许放弃兼容性”自动删除任何真实数据。格式边界在实现和恢复验收完成时切换，不能先改
FORMAT_VERSION 数字就声称已完成主干迁移。

## 阶段 E：切换唯一权威并删除迁移桥接

按依赖顺序删除，而非按文件名批量删除：

1. 固定 native storage + SQLite + native query；删除 `catalog.backend` 的 Legacy/Shadow 分支。
2. 停止向旧 registry 发布持久目录信息，删除在线 import/projection/reconcile/shadow worker。
3. 控制请求保留一个 typed enum dispatcher；删除 Envelope/inner/verb 入口及旧缓存，同时迁移客户端。
4. Gateway 订阅完全由 Flight 接管后，删除 Gateway listener、协议 codec、writer、subscriber、
   旧 query adapter 和旧 memory ingest；旧协议仅保留拒绝输入测试。
5. 删除 Python residual 和重复 SQL compiler，删旧 runtime 名称及临时 allow(dead_code)。
6. 移除影子指标、旧 capability、旧健康字段和临时开关；更新当前架构文档。

删除候选核心文件：

- `crates/zippy-gateway/src/legacy_ingest.rs`、`legacy_query.rs`、`persisted_read_shadow.rs`。
- `crates/zippy-server/src/legacy_query.rs`、`catalog_shadow.rs`、`catalog_command_shadow.rs`、
  `legacy_catalog_migration.rs`、`memory_ingest.rs`。
- `crates/zippy-master/src/legacy_catalog.rs`、`legacy_catalog_commit.rs` 中迁移桥接职责。
- `crates/zippy-catalog/src/legacy_*` 与 `crates/zippy-storage/src/legacy_import.rs` 中的旧数据迁移逻辑，连同对应持久字段从新版本依赖图移除。
- `clients/python/src/zippy_client/transports/gateway.py` 与 `protocol.py`，以及主 Python 包中的
  RemoteGatewayWriter/RemoteStreamSubscriber/_RemoteQuery 的重复远端实现。

这些是依赖解除后的删除候选，不是当前即可执行的 `rm` 列表。尤其不能删除 Master 内仍承担进程租约、
引擎注册和 compaction 生命周期的功能而不提供接替实现。

## 阶段 F：发布门禁与关账

- Rust 固定工具链检查、workspace tests、all-targets/all-features Clippy、format。
- Python 两包完整测试，核心覆盖率至少 80%；主包新模块逐步全部进入 mypy，不能只保留现有 15 文件。
- 从源码构建 wheel/sdist，在干净环境安装；没有 sibling repo、没有 `.git` 也能安装运行。
- Python/Arrow/Windows 客户端使用明确支持矩阵；Linux server 与 Windows client 的职责分开验证。
- proto 再生无差异；console build 和已打包资源一致；新安装包内版本及构建 revision 可追溯。
- 针对旧配置、旧协议、旧目录做“明确拒绝”的反向测试；不进行自动迁移或格式降级。
- 跑真实数据宽度、慢盘、多表多订阅者和持续回收长测，不能用短时合成吞吐替代生产容量结论。

每个阶段记录已删除入口、仍保留入口、未满足的产品能力和验证证据。最终关账时，旧运行分支应已从
依赖图消失，不能只把默认值改为 native 或把 Legacy 类重新命名为 Native。
