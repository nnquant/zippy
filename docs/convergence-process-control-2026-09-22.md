# 第四十九批：控制面只保留进程租约

日期：2026-09-22。承接[第48批](convergence-materializer-persistence-retirement-2026-09-22.md)。
累计 **32/34**；F08/F28 保持开放。

## 删除第二份表目录

Master registry 删除 stream/source/engine 记录、旧 schema/文件目录、tombstone、writer epoch、
段描述符、读者租约、数据 watch 和文件发布/替换/删除逻辑。共享 ControlRequest/ControlResponse、
MasterClient 及错误映射同步精简，只保留配置发现、进程注册/心跳/注销及 Process/Shutdown Watch。
旧请求类型无法再反序列化，也不保留跳转至 Flight 的迁移桥接。

ProcessRuntimeRegistry 替代 LegacyRuntimeRegistryAdapter，删除旧 source/engine/connection 镜像。
进程 lease 与运行时记录仍共享一把锁、boot identity 和 revision；CAS、ABA 防护、admission、
心跳通知、进程过期回收与有界停服继续有效。控制监听器的字节、连接和绝对读取期限限制保留。
Master 不再直接依赖 Arrow 或 segment-store，临时文件库仅用于测试。

ServerRuntime/ServerRuntimeConfig、health/readiness/capability 等公开 Rust 类型移除 Legacy 前缀，
不保留类型别名；原生 Server 直接持有 SQLite、ingest、DataFusion、Flight 与进程控制服务。

## 删除无执行者配置

删除整个 `[table]` 及其默认常量、持久分区配置、环境覆盖；这些字段已不控制原生 TableActor。
删除 `master.token` 及其生成、校验与日志路径。TOML、JSON overlay、wire serde 和环境变量入口
明确拒绝旧配置，PartialMasterConfig 同样拒绝未知字段。
Flight 的独立读、写、管理员凭据保持原契约，控制进程仍使用注册时签发的进程 token。

原生 Flight 测试不再建立无效的外部 persist_root，全部使用托管目录；ready 文件保密测试改为
验证当前进程能力凭据不出现在记录中，保留 owner-only 文件权限和生命周期断言。

## 验证与剩余项

全 Rust workspace 回归 **1323 次通过记录、8 个原有手动性能忽略项**，包含子进程复验记录。
新回归覆盖旧数据控制请求的真实 listener 拒绝、旧 watch 类型拒绝、拒绝后无状态变更且继续服务、
真实 lease reaper 撤销进程后继续支持配置发现，以及旧配置各入口拒绝。

全 workspace/all-targets/all-features Clippy -D warnings、Rustfmt、diff 检查通过。
重建后 Python 全量 544 项通过；mypy 91 文件、客户端自身配置 27 文件，Ruff 和 Black 92 文件通过。
当前二进制通过原生三类表、schema/drop、固定快照、实时订阅/ACK、计算与重启恢复矩阵；
新增断言确认不创建旧外部 persist 目录，扩展动态库依赖可解析。

[结构化证据](perf_reports/evidence/2026-09-22-process-control/summary.json)记录最终验证日志、
本批源码与二进制摘要；[测试退役清单](perf_reports/evidence/2026-09-22-process-control/test-retirement.json)
区分 67 个旧数据协议/配置用例退役、2 个进程语义迁移和同名运行时测试移动。
本批相对第48批 Rust 净减 9192 行。

F08 仍有 Rust 内部旧 source/publisher、KV/shard materializer 及其专属基准待删，三类表、订阅和
计算的原生接替路径继续保留。F28 的 Windows/macOS 实机验证尚未获得，已请求可用 runner 信息。
本机 Linux 仓库构建不代替跨平台发行验收。未提交、推送、部署或迁移用户数据。
