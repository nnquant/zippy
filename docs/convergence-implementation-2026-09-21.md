# 单主干收敛实施记录（2026-09-21）

最新进展见[第三批实施记录](convergence-implementation-2026-09-22.md)；以下保留前两批的历史状态。

状态：两批代码收敛已实施并验证；**整个项目尚未完成单主干切换，也未达到全仓合并门禁**。
用户已授权放弃旧 API、配置和数据格式，不需要为这些旧契约保留兼容层或开发迁移工具。
本次未提交、推送、部署或打开/修改生产数据目录。

当前结果以文末“第二批”与[当前能力表](current-capabilities-2026-09-21.md)为准。

依据：[34 项审计](architecture-convergence-audit-2026-09-18.md)、
[完整收敛方案](single-trunk-convergence-plan.md)。代码基线为 `1d1be4d`。

## 第一批已关闭的发现

| 编号 | 实施结果 | 验证重点 |
| --- | --- | --- |
| F01、F24 | 删除 Control Envelope、verb/inner 分发和按 request_id 缓存；Watch 统一走 typed 请求分发 | 旧 Envelope 通过真实 socket 被拒绝，无注册副作用；原有鉴权、Watch、状态更新回归通过 |
| F02 | Unix/TCP 控制入口共用连接许可、请求读取预算；最多 256 个并发连接，请求正文最多 4 MiB，完整读取期限 1 秒，必须以换行结束 | 连接许可共享与释放、边界大小、截断请求、持续小量发送不能延长期限；响应 socket 另设 1 秒写超时 |
| F03 | 删除 Flight 发现失败后的 Gateway 降级；轻量客户端默认写入要求 WAL | 连接/协议/capability 错误原样返回；Gateway 仅在显式 `durability="memory"` 时接受写入 |
| F06 | 无 `.git` 时跳过可选 Git 构建元数据，使用既有基础版本 | 在无 `.git` 的临时源码布局编译并执行 build.rs 成功；当前扩展重新构建安装成功 |
| F18 | 配置仅接受 `runtime.max_cpu_cores` / `ZIPPY_MAX_CPU_CORES`，GetConfig 不输出旧字段 | TOML、JSON、wire、环境变量旧名称均拒绝，包含同时设置相同新旧值的情况 |
| F19 | ready 状态只接受 v7，删除 v1–v6 补字段/升格分支 | 旧 ready 版本拒绝；当前 generation 身份和停服竞争测试保留。starting/stopping/failed 的 state v1 属于另一协议，仍有效 |
| F21 | 删除 Session 和 source binding 的公开 `publish_*_table` 旧方法 | 有效发布测试迁移到 `.append_table(...).publish()` / `.key_value_table(...).publish()`；旧方法不存在 |
| F34 | 删除未被 Cargo workspace/lock 引用的 15 个 tracked vendor 文件 | 依赖与全仓编译通过；纠正旧 fixture 无引用的误判，详见下文 |

以上共关闭 9 个审计编号。配置 CPU 名称收敛不涉及内部 Gateway 的并行预算参数或 metrics 字段；
它们仍表达执行并行度，不再是配置解析别名。

控制请求期限仅限制读取完整请求，不能理解为 Watch 执行只能持续一秒。控制连接超额时直接关闭，
请求超长、未换行或超时时不进入业务分发。本批没有增加第二套控制协议或响应缓存。

## 第一批结束时已推进但未关闭

- F20：Python CLI 删除 `master` 命令，Python 与扩展删除 `run_master_daemon` 别名，移除
  `cli_master.py`；进程管理器任务改为 `server run --foreground`，示例、stub、帮助和测试同步更新。
  独立 Rust `zippy-master` daemon 和 Gateway 启动入口仍存在，不能把生命周期统一标成完成。
- F33：当前 server、辅助服务、Gateway、发布 API 和客户端说明已同步本批新契约，并为旧审计添加
  实施导航。全量 ADR、历史设计和 ROADMAP 尚未全部收敛。

保留了发布、控制鉴权、进程恢复、SIGINT 清理、URI/config 解析等有效功能测试；删除的测试针对
已删除的重复入口。另修正订阅重启测试中的时序竞争：回调事件发生在原生投递计数更新前，测试改为
有界等待指标发布，没有修改运行时投递语义或放宽计数断言。

审计更正：`tests/fixtures/contracts/table_semantics_v1.json` 仍由
`crates/zippy-master/tests/golden_contracts.rs` 引用，不能当作孤立文件删除；它将随旧目录恢复链路
退出，而不是通过删掉仍在运行的恢复测试来消除引用。被删除 vendor 目录中原有的未跟踪
`Cargo.lock` 保持原样，根 `.gitignore` 接续其既有忽略规则。

## 第一批结束时未关闭的架构问题

另外 23 项尚未关闭：F04、F05、F07–F17、F22、F23、F25–F32。
它们的证据及建议继续以审计和方案为准，尤其包括：

1. SQLite 与旧 registry 的双目录来源；Flight 查询与 list/wait/schema 的 Catalog 来源不一致。
2. 原生公开建表、alter/drop、三种表的写入和恢复闭环尚未齐全。
3. Flight DoExchange 尚未承接远端订阅，Gateway 仍有实际调用者。
4. 新旧 ingest、查询解释器、Python 客户端及全局连接状态仍重复；跨 transport 的 snapshot 保证不同。
5. 在线 shadow、旧数据导入和 backend 开关尚未从依赖图删除；新数据目录格式边界尚未发布。
6. 全仓类型检查、发行依赖、支持矩阵、生成代码和前端发布门禁仍有债务。

用户已放弃旧格式兼容，并不意味着当前新路径已经具备旧路径的全部有效功能。后续必须按方案
补齐空目录建表 → 写入 → 查询/订阅 → 停服 → 恢复 → 删除的公开 API 验收，再删除依赖链。
本批没有通过改默认开关、重命名 Legacy 或提高格式版本号来宣称权威已切换，也没有迁移或删除数据。

## 第一批验证结果与限制

| 检查 | 结果 |
| --- | --- |
| `cargo test --offline --workspace` | 通过；日志汇总 1705 passed、10 ignored（含测试子进程结果块） |
| Clippy workspace / all-targets / all-features / `-D warnings` | 通过 |
| `cargo fmt --all --check`、`git diff --check` | 通过 |
| 重建主包扩展后 `pytest pytests -q` | 604 passed、1 skipped |
| 轻量客户端完整 pytest | 72 passed，覆盖率 80.37% |
| `ruff check .` | 通过 |
| 两包按各自配置 Black | 主包 25 文件、客户端 22 文件通过 |
| 配置范围 mypy | 主包 14 文件、客户端 24 文件通过；删除 cli_master 后主包范围减少一个文件 |
| 仓库级 `mypy .` | **失败：77 个源文件中 11 文件有 1034 个错误**；主包 `__init__.py` 仍有 258 项 |
| 仓库级 `black --check --line-length 100 .` | **失败：7 文件需格式化**，其中 2 个 protobuf 生成文件、5 个示例文件 |
| 无 `.git` 构建脚本探针 | 通过；不等同于干净环境的完整 sdist/wheel 安装验收 |

整仓 mypy 和 Black 与现有 CI 限定检查范围存在差异，因此不能声明 AGENTS.md 的全部门禁通过。
没有通过添加类型忽略或手工改写生成协议文件来掩盖这些差异。主包总覆盖率和 Rust 覆盖率本次未测；
80.37% 仅指轻量客户端。Windows、干净发行环境、持续压力/磁盘故障及生产链路未在本机验收。
Maturin 已生成并安装本机 wheel，但报告缺少 patchelf 的 rpath 提示，不能替代可移植 wheel 验收。

完整日志位于 `/tmp/zippy-convergence-*`；机器可读摘要见
[验证摘要](perf_reports/evidence/2026-09-21-convergence/summary.json)。


## 第二批收敛（本次继续实施）

用户进一步明确：**frame、key-value、远端实时订阅与本地计算引擎全部保留，补齐原生实现和恢复
测试后才删除旧路径**。这个范围已经确定，不再需要以旧 API/旧数据兼容为由保留桥接。

| 编号 | 当前结果 | 本批验证 |
| --- | --- | --- |
| F04 | Flight 的 list/schema/wait/query 使用原生 Catalog；不再查旧 registry/Gateway 判断原生表就绪；等待期限传入实际查询 RPC | Flight SQL GetTables 真实往返；精确 namespace 过滤；旧目录调用设为测试失败；鉴权错误不被轮询吞掉 |
| F05 | 默认读取不请求跨查询固定快照；Flight 的 `snapshot=True` 和 `snapshot()` 明确拒绝未实现能力 | Gateway 现有快照测试保留，Flight 能力拒绝回归 |
| F20 | Rust 二进制迁入 zippy-server；删旧 daemon 实现、独立 `gateway run`、PM master/gateway 任务；启动示例统一 Server | Server 二进制 10 项集成测试；CLI、PM、SIGINT/目录锁/ready 清理回归 |
| F23 | 独立客户端删除模块级 connect/read/get_writer 等兼容入口、全局默认 Client、local 兼容参数 | 显式两个 Client 生命周期相互独立；仓库调用迁移 |
| F27 | 主包删除 rspm Python 依赖和 sibling uv source；进程表在主包渲染，原生 RSPM RPC 保留；CI 删除 sibling checkout | 两包 sdist → 无 Git 目录 → 独立 Cargo cache → wheel → 新 venv → 隔离导入/CLI 帮助通过 |
| F30 | 发行版本与运行时/ready 一致；Git 完整 revision 单独暴露，无 Git 时为 None | 源码构建检查 HEAD，归档构建检查版本等于 package metadata 且 revision 为空 |
| F31 | 固定生成器从 proto 再生 Rust/Python；CI 校验无差异、前端构建及包内资源逐字节一致 | 再生检查通过；3 个前端资源一致；格式化生成文件由 generator 完成 |
| F32 | 删原生查询模块整模块 dead_code 豁免和失效“未接线”描述；测试辅助项以 cfg(test) 精确隔离；Catalog README 更新 | workspace Clippy all-targets/all-features 严格检查通过 |

原审计累计关闭 17 项：F01–F06、F18–F21、F23–F24、F27、F30–F32、F34。

本批还推进但没有关闭：

- F22：主包删除缺失扩展时的纯 Python 降级、占位原生类型和静默日志实现；远端 smoke 迁到独立
  Client，并验证子进程不导入主包。主包旧远端查询、全局连接和 Python residual 仍在，不能标记整体完成。
- F21 补充：删除只会抛“已不支持”的 `ParquetSink` 和 `MasterClient.register_sink`；保留正常
  append builder 的持久化测试。原生符号、stub 与公开 Python API 同步移除。
- F25：核心错误到服务码的映射合并；Gateway 通常错误、流式错误和连接拒绝携带结构化字段，
  客户端不再猜测 reason 文本。旧控制协议仍有可空错误码，整体未关闭。
- F28：CI 与 performance 从 rust-toolchain.toml 安装固定 Rust；Python/Arrow 多版本及 Windows
  实机覆盖仍未齐全，不能根据开放依赖范围宣称所有后续版本兼容。
- F29：删除虚假的原生降级类型赋值，补部分真实扩展 stub；整仓 mypy 错误由 1034 降至 982，
  仍有 11 个文件报错。没有通过新增 ignore 或缩小既有门禁范围隐藏它们。
- F33：补 ADR 0006 索引、新增 ADR 0007 与当前能力表；当前启动/客户端指南同步。旧 Windows
  手工验收保留日期与事实，并明确标记过期及当前入口，不伪造新的 Windows 验收结果。

## 第二批之后仍未完成的工作

F07–F17、F22、F25–F26、F28–F29、F33 共 17 项尚未关闭。其中最大依赖链是：

1. 在线建表协调 Catalog、TableActor、materializer 与 WAL authority 的运行时注册；当前三处表集合
   都在启动时固定。仅对 SQLite 发 CreateTable 不能形成可写、可查询的表。
2. 三类表的原生写入/查询/重启恢复、alter/drop 与 fencing、snapshot pin、GC 闭环。
3. Flight 订阅及 KeyValue 一致快照/游标；本地计算输出接入同一 Catalog 与 ingest。
4. 完成以上验收后切换新数据目录边界，删除 legacy registry、shadow、在线导入、Gateway 和重复查询。
5. 全仓类型检查和支持矩阵、长期压力/故障验收。

本次没有完成以上原生功能，也没有切换默认数据主干；不能把已关闭的入口与发行问题描述为项目
已经只剩单一主干。详见[当前能力表](current-capabilities-2026-09-21.md)。

## 完整回归发现并修复的额外问题 F35

最终回归曾在 `native_checkpoint_worker_persists_without_an_explicit_flush` 失败，原因是 Storage
关闭 TableActor 和 persistence 的时机早于 ingest/checkpoint 排空。已改为先关新写入/查询 admission，
等待 ingest 排空后才关闭下游服务；新增断言强制覆盖这一停服顺序。原失败日志保留在
`/tmp/zippy-rust-final.log`，修复后的完整回归另存，不将重跑成功覆盖为“从未失败”。

## 第二批最终验证

- Python 两包合跑：681 passed、1 skipped；独立客户端单独运行：77 passed，覆盖率 81.30%，
  通过自身 80% 门禁。
- 整仓 Black：82 文件通过；整仓 Ruff、协议再生、前端包校验通过。
- 配置范围 mypy：主包 14 文件、客户端 24 文件通过；整仓 mypy：982 errors / 11 files / 81 checked。
- Rust workspace tests：1704 passed、10 ignored；Clippy、格式检查与干净发行检查通过。
  原生存储针对性回归：14 passed、1 ignored；生成器独立 Clippy/fmt 也通过。
- 当前最终日志：`/tmp/zippy-rust-verified.log`、`/tmp/zippy-python-verified.log`、
  `/tmp/zippy-distributions-verified.log`、`/tmp/zippy-clippy-verified.log`、`/tmp/zippy-*-final.log`。
- 独立安装环境以 Arrow 25.0.1 完成真实写入/查询；开发测试使用 Arrow 23.0.1。
- 两包归档、wheel 与独立安装环境位于 `/tmp/zippy-distributions-verified`。归档构建因 Maturin
  裁剪 workspace 成员，需要离线重算 lock 中的成员集合；使用独立 Cargo cache，不能复用仓库
  内带 Git 元数据的构建结果冒充无 Git 验收。

没有提交、推送、部署，也没有打开或修改生产目录。整仓 mypy 和原生产品闭环仍未通过，因此仍不
满足合并和最终关账条件。
