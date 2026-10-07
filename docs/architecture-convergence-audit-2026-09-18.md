# 历史分支、兼容契约与单一主干收敛审计

2026-09-23 快照契约复核：[显式 Table 快照绑定修复](convergence-table-snapshot-binding-2026-09-23.md)。
F05 的普通 `Table.snapshot()` 曾返回未参与后续查询的固定快照；现在一次捕获同时固定该表和
派生查询的读取边界，真实 Flight 重启矩阵通过。原审计仍为 33/34，F28 实机验收延期。

2026-09-23 后续复核：[控制入口与租约容量修复](convergence-control-registry-bounds-2026-09-23.md)。
F01/F02/F24 的当前实现还移除了 debug listener 上无凭据的测试请求，并限制进程租约
与注册标签的容量。原审计累计仍为 33/34；F28 实机验收延期，不能算通过。

2026-09-23 最新进展：[第五十六批实施记录](convergence-query-alias-control-deadline-2026-09-23.md)。累计关闭 33/34；F08 的本地单主干代码已收敛，F28 实机验收按用户要求延期，不能算通过。旧 segment-store 持久 worker、Catalog 提交器双入口、预发布文件提交入口、查询兼容别名、core 旧 stream schema/FNV 工具、无期限控制请求和 Master 随机 token 回退已退出。以下保留审计时点结论。

2026-09-22 范围调整：用户明确要求跨平台验收先不做。F28 实机验收延期，不计为本次实施阻塞；本地兼容性和安全问题继续修复。

日期：2026-09-18。基线：`1d1be4d`，开始审计时工作树干净。

2026-09-21 更新：用户已授权放弃旧 API、配置和数据格式。实施结果见
[收敛实施记录](convergence-implementation-2026-09-21.md)。以下保留审计基线，已修项以实施记录为准。

## 结论与边界

项目尚处于多阶段迁移共存状态，不能把所有 `legacy` 文件视为死代码。旧 Master 仍提供控制面、
注册、租约和部分持久化职责；Gateway 仍是远端订阅的实际实现。SQLite、DataFusion、Flight 和
原生 ingest 已有可运行链路，但没有覆盖旧路径的全部产品能力。

建议的唯一主干是：Server 持有生命周期；SQLite 持有持久元数据；TableActor/原生 ingest 持有写入
顺序、回执和恢复；DataFusion 执行通用查询；Flight 提供远端数据面；SHM 仅作为本机数据访问优化。
计算引擎与 append/frame/key-value 三种表语义应保留，不能为了删旧代码而默默删掉产品能力。

本报告记录 **34 项发现**，区分已经复现的问题、源码可确认的契约分歧、以及需要额外环境验证的
发布风险。具体实施顺序见 [主干收敛方案](single-trunk-convergence-plan.md)。本轮未修改业务实现，
未删除旧数据、未提交、未推送、未部署；删除边界需要结合旧 API/配置/数据兼容要求落实。

“全仓扫描”表示覆盖所有受版本控制区域、依赖清单和跨模块调用关系，不表示逐行人工证明或已经
找尽所有缺陷。历史报告中的问题只有经当前源码核查仍存在，才列为当前问题。

## 扫描范围

审计起点有 505 个跟踪文件，18 个 Rust workspace crate、两个 Python 发行包、一个 React console。
跟踪代码行数包含测试：Rust crate 区域 244,167；主 Python 17,953；Python 主测试 24,394；
轻量客户端及测试 7,946；前端 2,030；示例 3,363。生成资源、vendor 与历史文档单独检查，
不把 `.worktrees`、`target`、虚拟环境、日志和发行目录当作当前源码。

| 区域 | 核查内容 | 主要结论 |
| --- | --- | --- |
| types/core | 版本、控制协议、配置、URI、资源预算、目录所有权 | 格式版本应保留；控制协议和配置别名尚未收敛 |
| master/gateway/server | 启动、权威来源、迁移桥接、查询、停服 | 旧实现仍在真实链路中，不能按名称删除 |
| catalog/storage/ingest | SQLite 迁移、manifest、WAL、checkpoint、回执、回收 | 新 append 恢复链路已存在；表管理和其他表类型尚未完全接入 |
| query/protocol | DataFusion、Flight、认证、元数据、订阅能力 | 查询和 append 写入可用；DoExchange 尚未实现 |
| segment-store/operator-runtime | SHM 布局、读者保护、引擎接口 | 属于主干基础设施，不能因“多路径”误删 |
| engines/operators/io | 三类表、KeyValue v2、计算、ZeroMQ | 数据语义差异是有效功能，不等于历史兼容分支 |
| python/client-py | 全量 Python API、独立客户端、配置和快照行为 | 两套查询/连接实现并存，存在可见语义分歧 |
| perf/tests/examples | 门禁、golden、历史性能报告、示例入口 | 正向测试较完整，但未覆盖若干跨版本矛盾 |
| app/packaging/CI/docs | 构建、发行、版本来源、平台和文档状态 | 有可复现源码包构建问题及多处文档滞后 |

## 优先处理的正确性与契约问题

### F01 / P1：控制响应缓存绕过版本和身份检查，且没有容量边界

位置：`crates/zippy-master/src/server.rs`，`handle_stream` 的 `ControlRequest::Envelope` 分支，
约 1733 行；字段 `envelope_responses`。

缓存仅以调用者传入的字符串 `request_id` 为键，在检查 `envelope.version`、verb、payload 和进程
身份之前返回。已有请求的 ID 搭配不同版本/身份/操作，仍拿到原响应。缓存包含
`ProcessRegistered.process_token`，因此这不仅是返回旧结果的问题。也没有请求指纹、身份隔离、
TTL 或条目/字节预算；并发首次同 ID 还缺少“占位—执行—发布”串行化。

本机临时 Master 的最小探针复现：先用 version=2 注册测试进程，再以同 ID、version=999、不同操作、
无效身份请求，得到完全相同的注册响应，响应仍包含测试进程 token。探针只记录布尔值，未输出 token。
探针最初使用本地已安装扩展；随后从当前源码重新构建扩展，再次复现相同结果。

收敛：保留一个 typed dispatch；版本与身份验证先于缓存；幂等键绑定主体、操作、请求指纹；不同内容
复用 ID 明确冲突。定义回执有效期及资源上限，不能随意逐出后重新执行有副作用的操作。

### F02 / P1：旧控制服务缺少请求字节和连接线程上限

位置：`crates/zippy-master/src/server.rs`，Unix/TCP accept 循环、`handle_stream`。

每个连接直接 `thread::spawn`，服务端使用没有字节上限的 `BufRead::read_line`。虽然 socket 有一秒
读超时，但它不是整次请求的绝对截止时间，持续小量发送可以延长请求寿命。客户端已有响应行上限，
并不能保护服务端请求读取。这里是源码确认，未做耗尽机器资源的压力试验。

收敛：在统一控制入口设置连接许可、请求字节上限、绝对 deadline；沿用有界资源设计，不新增第二套
“旧端口特例”。

### F03 / P1：Flight 失败自动切换 Gateway，会改变未显式指定的写入持久性

位置：`clients/python/src/zippy_client/client.py:143`、`router.py:90`、`client.py:344`。

构造 Flight transport 遇到连接、协议或 capability 错误，只要 Gateway 存在就记录警告并移除 Flight
endpoint。自动写入随后选择 Gateway。`durability=None` 在 Flight writer 上被解释为 `wal`，在
Gateway 上则是 Memory；同一调用可能因发现失败改变 ACK 契约。显式指定 `durability="wal"` 时有
拒绝逻辑，这个保护应保留，不能据此认为默认调用没有问题。

收敛：已配置 Flight 的错误直接返回；默认 durability 独立于 transport 固定；自动路由不得切换到
更弱的写入承诺。旧 Gateway 的临时使用必须显式选择，并纳入删除清单。

### F04 / P1：Flight 原生表和客户端列表示例使用不同元数据来源

位置：`clients/python/src/zippy_client/client.py:249` 的 `list_tables` 和 `_wait_for_source`；
`crates/zippy-server/src/flight_query.rs`、`crates/zippy-protocol/src/sql.rs`。

`Client.list_tables()` 只读 Gateway/旧 control registry，没有走 Flight SQL metadata。原生表可以
存在于 SQLite 并被 Flight 查询，但未必注册到旧 registry。`wait=True` 的就绪判断也沿旧 control
路径。这是可由调用链确认的来源不一致；尚未对所有混合部署组合做端到端复现。

收敛：建表、列举、schema、wait 与查询 binding 使用同一 Catalog；进程 registry 只负责运行时租约。

### F05 / P1：相同 snapshot API 在不同 transport 下含义不同

位置：`clients/python/src/zippy_client/client.py:254`、`query.py:208`。

`read_table` 默认 `snapshot=True`。Gateway 分支持有 `_SnapshotLease`，多次 collect 复用固定快照；
Flight 的 `snapshot()` 返回 `mode="per_query"` 或上次查询元数据，并不获得同样的固定快照租约。
路由切换会改变可重复读取语义。

收敛：明确区分“单次查询一致快照”和“显式跨查询 snapshot handle”。后者未实现时明确拒绝，
不能用同名参数悄悄表达不同保证。

### F06 / P1：源码归档缺少 `.git` 时构建崩溃

位置：`crates/zippy-core/build.rs:28`。

`.git` 不是目录时，构建脚本直接把它当 worktree 指向文件读取并 `expect`。没有 `.git` 的 source
archive/sdist 因而 panic，虽然 `version.rs` 本身提供了没有 Git 信息时的基础版本分支。
用独立临时 crate 目录执行同一 build script，复现退出码 101 和
`.git indirection file should be readable`。这是构建脚本级复现，不冒充完整 wheel 发布验收。

收敛：发行版本来源明确且不依赖 Git；Git revision 仅作为可选构建信息。增加无 `.git` 的源码包构建
门禁，而非再增加“已安装包版本兼容”分支。

## 架构遗留与删除阻塞点

| 编号 / 优先级 | 当前证据与矛盾 | 保留的主干 / 删除条件 |
| --- | --- | --- |
| F07 / P1 | `core/config.rs` 默认 `catalog=Legacy`、native storage/query 关闭；SQLite、shadow、旧 registry 三套模式并存。目标 ADR 与默认启动路径尚未一致。 | 原生功能补齐后固定 SQLite；导入成为离线工具，删除线上 backend 选择与 shadow fan-out。 |
| F08 / P1 | `server/runtime.rs` 即使 SQLite 模式仍创建 `MasterServer` 和 `LegacyServiceSet`；`catalog()` 返回旧 adapter。旧进程注册与持久元数据职责没有彻底拆开。 | 保留一套运行时 registry，持久表目录迁入 SQLite；禁止再向旧 registry 写持久元数据。 |
| F09 / P1 | `catalog/domain.rs::prepare_command` 仅实现部分命令；当前已支持 AcquireProducer，但 alter/drop/schema 等命令仍走 UnsupportedPlan。Flight 也没有完整 DDL 管理入口。 | 补齐正式表管理命令及客户端，再删旧 RegisterStream/DropTable 的目录职责。 |
| F10 / P1 | `server/tests/server_runtime.rs::flight_writer_wal_put_uses_native_ingest_without_gateway` 通过离线 `seed_native_append_table` 建表后启动；证明 append 链路可用，不证明空目录用户建表闭环完成。 | 验收从空目录启动、经公开 API 建表、写入、重启、查询和删除，全程不调用测试 seed。 |
| F11 / P1 | `protocol/server.rs:1459` 对 DoExchange 返回 UnsupportedPlan；客户端远端订阅仍走 Gateway。SQLite authority 配置同时禁止 Gateway。 | 补齐同一 SubscriptionService 和 Flight DoExchange 后才能删 Gateway；否则必须明确宣布取消远端订阅。 |
| F12 / P1 | `ingest/materializer.rs` 只为 Append 建 native materializer，其他表定义跳过；Flight v1 只支持 append。引擎已有 frame/key-value 能力，typed mutation 存在不等于真实落地链路可用。 | 为三种表明确写入、快照、持久化、恢复契约；不能只保留 append 后声称完成等价迁移。 |
| F13 / P1 | `server/memory_ingest.rs` 与 `ingest/native.rs` 分别承接旧 Gateway 和新 Flight 写入，确认边界及恢复能力不同。 | 一个 IngestService 权威；transport 只解析请求和编码回执。删除旧 memory authority 的重复表状态。 |
| F14 / P1 | `python/zippy/__init__.py::_collect_query` 同时保留本地读取、Gateway 部分执行加 Python residual、原生 SQL；Gateway crate 还有自己的 Arrow 运算路径。 | 通用查询统一 DataFusion；SHM 只负责供数，不再保留第二套通用查询解释器。引擎内因子运算不在删除范围。 |
| F15 / P2 | `catalog/legacy_*`、`storage/legacy_import.rs`、`server/catalog_*shadow.rs`、`legacy_catalog_migration.rs`、`gateway/persisted_read_shadow.rs` 形成多代导入/投影/对照体系。 | 完成主干切换后把历史读入集中到一次性离线迁移；先验证文件顺序、checksum、lineage 和 snapshot 对应，再去掉在线桥接。 |
| F16 / P1 | KeyValue v2 增量已成为唯一输出，但跨机器快照 RPC、KeyValue 持久化和 delete 未形成产品闭环；主包 StateSubscription 依赖显式 snapshot_loader。 | 保持 v2 缺口检测，补统一快照与游标接口；不能用“从最新一条继续”冒充完整状态恢复。 |
| F17 / P2 | Flight 定义了 Persisted 枚举但 ingest decoder 明确拒绝；后台 checkpoint/manifest 落盘已经实现。两者常被旧文档混为一谈。 | 保留 Memory/Wal 的实际承诺；Persisted 未交付前明确 capability=false。不能以后台最终落盘替代请求确认。 |

## 可以独立收敛的兼容表面

| 编号 / 优先级 | 证据 | 收敛动作 |
| --- | --- | --- |
| F18 / P2 | Rust `config.rs` 与 Python `_resolve_parallelism_alias_pair` 同时接收 `max_cpu_cores/max_parallelism` 及两组环境变量；GetConfig 还输出旧名称。文档称只保留一个兼容周期。 | 唯一公开名称为 `max_cpu_cores`；旧配置明确报迁移错误；不把内部 query parallelism 一并重命名。 |
| F19 / P2 | `python/zippy/server_process.py` 当前 ready schema=7，接受 2–6；status 还把 v1 缺失字段填 False 后校验。 | 同版本启动仅接收 v7；旧进程识别与停服若需保留，应作为独立维护工具，不能伪造完整健康事实。 |
| F20 / P2 | `cli.py` 同时注册 master/server/gateway；Python 与 Rust 还导出 run_master_daemon 别名。 | 对外保留 `zippy server`；维护/诊断可以保留子命令，但不应再产生独立旧服务生命周期。 |
| F21 / P2 | `python/zippy/__init__.py` 的 publish_append_table / publish_key_value_table / source.publish_key_value_table 已发 DeprecationWarning，测试还刻意维持旧调用。 | 仓库示例和调用全部迁到 table builder，再删除旧方法、stub 和别名测试；保留新行为测试。 |
| F22 / P2 | 全量 Python 包约 11,964 行的 `__init__.py` 含连接、SQL 编译、时间过滤、writer/subscriber；轻量客户端又实现一套同类逻辑。 | `zippy-client` 持有连接/远端协议/查询编译；主包只持有服务器、计算和生产者能力，并调用共享客户端。 |
| F23 / P2 | `client.py` 存在进程级 `_DEFAULT_CLIENT` 与兼容 `master=` 参数；两个包的 connect/session 使用方式并存。 | 以显式 Client/Session 为唯一连接所有者；便利函数如果保留，只做薄调用，不再建立独立状态和兼容选择规则。 |
| F24 / P2 | `bus_protocol.rs` 同时暴露旧外层枚举、Envelope.inner、verb/payload；master 对这些入口分别 dispatch，支持集合不同。 | 固定一种 versioned typed envelope；鉴权、幂等、错误和 admission 共用一条路径。F01 应先修，再迁移下游。 |
| F25 / P2 | Gateway 客户端用 reason 文本中的 unauthorized/authentication 分类异常；旧 control 大量 `code: None`；Flight 使用结构化 ErrorCode。 | 服务端统一错误码与 retryable；删除文本猜测兼容。保留原始 cause 供诊断。 |
| F26 / P2 | 无认证 Flight 使用客户端自选 session ID，认证 Flight 使用服务端 session；客户端也有两套握手状态。非 loopback 已有 TLS/auth 校验。 | 产品主干固定认证 session；如果保留本地开发匿名模式，明确它是受限部署配置，而非向旧协议静默回退。当前不能把这个受限模式直接描述为公网认证漏洞。 |

## 构建、门禁、文档和仓库维护

| 编号 / 优先级 | 证据 | 处理方式 |
| --- | --- | --- |
| F27 / P1 | 主包 uv source 固定 `../rspm/python` editable；Linux CI 另 checkout 固定 commit 并创建 sibling 链接，Windows job 没有同样的准备。 | 发布依赖与开发 override 分离；干净目录安装主包/客户端并验证。Windows 是否因 workspace 解析触发 sibling 缺失尚需 Windows CI 实测，不能当作已复现。 |
| F28 / P2 | `rust-toolchain.toml` 固定 1.86.0，CI action 选择 stable；两个 Python 包只声明 >=3.11 且 CI 仅 3.11；PyArrow 等依赖开放较宽范围。 | 明确受支持矩阵，测最低/最高支持版本及 pin 的 Rust；没有证据时不宣称已兼容所有后续版本。 |
| F29 / P1 | 根 mypy 只覆盖 15 个文件，未覆盖巨型 `__init__.py`；扩展检查到 python+pytests 得到 1,029 个错误、涉及 7 个文件，其中生产代码 263 项、测试 766 项。当前 CI 通过不等于全量类型检查通过，类型错误也不等于同等数量的运行时缺陷。 | 先拆模块、统一类型和 stubs，再逐块纳入门禁；不要加 ignore 或删测试来宣称零债务。 |
| F30 / P2 | 工作区/主包/console 为 0.5.0，client 为 0.1.0；运行时又用 Git count/SHA 构造版本。本机扩展报旧 SHA，造成源码与已安装产物不一致。 | 版本独立演进可以保留，但须给出发行兼容矩阵和构建元信息；测试前构建目标产物，不把旧 binary 的结果当当前源码证明。 |
| F31 / P2 | proto、手写/已生成 Rust `wire.rs`、Python generated 代码同时入库；现有 scripts/CI 中没有发现统一再生及无差异检查。前端构建也未纳入当前 CI。 | schema 单一来源，固定生成器并做再生 diff；前端增加 build 与打包资源一致性门禁。本机 console 构建成功且资源文件名与打包目录一致，未发现当前 bundle 漂移证据。 |
| F32 / P2 | `server/lib.rs` 对多个已被 native_query 调用的模块保留“等待接线”的注释和整模块 allow(dead_code)；catalog README 仍称 SQLite authority 禁用、列出未完成 AcquireProducer。 | 删除已失效的阶段描述；缩小 allow 到实际需保留项；建立当前能力表。不能直接删除模块，因为它们已被使用。 |
| F33 / P2 | ADR 索引遗漏 0006；ADR-0004 和旧审计续报仍把已完成的 WAL checkpoint/回收、KeyValue v2 列为未来事项；ROADMAP 混用旧 Gateway 阶段。 | 历史报告保留原结论并加 superseded 导航；当前入口只链接最新能力表和迁移指南，不重写历史测量。 |
| F34 / P3 | `vendor/shared_memory-0.12.4` 未出现在当前 workspace 依赖和 Cargo.lock；复核更正：`table_semantics_v1.json` 仍由 Master golden 恢复测试引用，v2 只覆盖新表语义，不能判为无引用。 | 删除孤立 vendor；v1 fixture 与 legacy_registry_v1 暂随旧恢复测试保留，在新目录契约落地后改为拒绝输入测试。 |

## 不应误判为兼容垃圾的内容

1. SQLite physical schema v1–v7、WAL format、SHM layout、Flight protocol、KeyValue delta 版本属于不同
   版本空间。数字不一致不是缺陷。仍有旧持久数据时，不能改写已冻结 migration checksum 或直接删 DDL。
2. SHM、Parquet、WAL 是不同存储职责；本地与远端 transport 也可以共用同一业务权威。单一主干不要求
   每个操作只剩一个物理通道。
3. append 原始事件、frame 完整帧、KeyValue 状态增量不是三代兼容模式。删除其中一种属于产品能力变更。
4. descriptor forwarding 的 copy 分支用于 retention、历史段等条件下保持正确性，不等于旧版 fallback。
5. `SpscDataQueue` 已换成安全 crossbeam 实现，旧名称值得调整，但不能重新报告旧 UnsafeCell/丢唤醒
   问题为当前缺陷。普通控制队列与数据队列的关闭语义也需比较后再合并。
6. 9 月 6 日首轮报告中的无回收累计限额和每批全量 KeyValue 问题已被 v2 工作处理；当前待验证的是
   真实负载长跑、总内存/磁盘容量以及跨机器能力，不是重新提高旧限额。

## 验证记录

所有测试使用当前工作区，未连接生产服务。可复现命令中的 `.venv` 由 uv 管理。

| 检查 | 结果 / 限制 |
| --- | --- |
| `PYO3_PYTHON=$PWD/.venv/bin/python cargo test --offline --workspace` | 在允许本机 socket/SHM 的环境通过：1,710 passed、10 ignored，114 个结果块；包含 doctest。 |
| `cargo clippy --offline --workspace --all-targets --all-features -- -D warnings` | 通过，指定相同 PYO3_PYTHON。 |
| `cargo fmt --all -- --check` | 通过。 |
| uv / pytest 主包+客户端，已安装扩展基线 | 672 passed、1 skipped、1 failed；唯一失败为 Git 派生版本落后一个提交。后续重建验证单独记录，不隐藏此基线。 |
| uv/maturin 从当前源码重建后复验 | 主包 605 passed、1 skipped；客户端 68 passed，覆盖率 80.12%；控制缓存问题仍可复现。 |
| 根/客户端各自配置 Black、Ruff、mypy | Black 与 Ruff 通过；mypy 分别检查 15/24 个文件，通过。 |
| `uv run --no-sync mypy python pytests` | 1,029 errors in 7 files，checked 26 source files；是当前扩大范围后的实际结果。 |
| `npm run build` | TypeScript 与 Vite 构建通过。未进行浏览器交互或 Windows/macOS 实测。 |
| 无 `.git` 构建脚本探针 | 退出码 101，已复现。 |
| 控制缓存探针 | 不同版本、身份和操作使用相同 ID 返回旧注册响应，已复现；只保留布尔证据。 |

初次沙箱内失败来自 TCP/socket 权限，ZeroMQ 初始化会 abort；初次 Rust 未指定 PYO3_PYTHON 时错误地
链接系统 Python 3.8。重新选择项目解释器并允许本机通信后，Rust 完整回归通过。这些环境失败不计入
业务缺陷数量。

本机 editable 构建提示缺少可选 `patchelf`，仍成功安装并通过上述测试；这不等于可分发 manylinux
wheel 验收。无 Git 的发行构建问题、干净安装和目标平台验证仍待处理。

原始执行日志保存在本机 `/tmp/zippy-audit-*.log`，不是永久仓库证据。审计摘要见
[`perf_reports/evidence/2026-09-18-convergence-audit/summary.json`](perf_reports/evidence/2026-09-18-convergence-audit/summary.json)。

## 完成标准

不能用“删了多少 legacy 行”验收。完成应同时满足：从空目录建表到恢复只走一条权威链；有数据升级
通过显式离线迁移；同一 API 不因自动路由改变快照/持久性；所有支持的表类型、订阅和计算入口保留或
明确下线；旧配置/协议被清晰拒绝；构建和测试来自同一源码版本；门禁覆盖真实安装产物。

## 2026-09-21 回归补充：F35 / P1 原生停服依赖顺序竞争

完整回归复现 `native_checkpoint_worker_persists_without_an_explicit_flush` 的 shutdown 断言失败。
`NativeStorageRuntime::begin_shutdown` 先关闭 TableActor admission 并可能通知整个 storage supervisor
退出，而 `NativeIngestRuntime` 的已接受 checkpoint 此时仍需要 actor 与持久化服务。正常停服因此
会把下游服务提前关闭产生的错误记成 terminal_error；延长超时不能修正这个顺序。

修复：立即关闭新 ingest/query admission，但先完成 ingest/checkpoint drain，再关闭 actor admission
和通知 storage supervisor。没有 ingest 的启动路径仍立即关闭 actor admission。Drop 保持尽力清理，
不把省略或失败的显式 shutdown 伪装成成功。

回归在 checkpoint 完成后显式调用 begin_shutdown，验证写入 admission 已关闭，但 actor 依赖仍可用，
然后要求 shutdown 返回 Complete。这使旧顺序确定性失败，而不依赖偶发调度。最终结果见实施记录。
