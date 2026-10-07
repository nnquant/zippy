# 第四十一批：删除 Master 投影与旧 Catalog 协议

日期：2026-09-22。承接[第40批](convergence-catalog-migration-retirement-2026-09-22.md)。
累计仍为 **27/34**；F07/F08/F11/F13/F14/F15/F28 保持开放。

## 删除内容

- Master 的迁移快照、V4 投影、文件排序、摘要构造与对应资源预算实现。
  `legacy_catalog.rs` 从 4,255 行减至 1,177 行；仍使用的旧目录 adapter 尚未删除。
- `legacy_catalog_commit.rs` 整个模块及事件集成测试；移除发布者安装、影子观察队列、
  指标与迁移封口。正常控制变更不再发布影子事件，停服不等待影子同步。
- 共享 `wire_v2.rs`、`wire_v3.rs`、`wire_v4.rs` 与 Catalog 中对应的导入、reconcile、
  projection 命令、回执、DTO 和有界规范化实现。原生 v1 指纹契约保留。
- 更新 Catalog、Master、Storage README，清除已失效的导入操作和“原生能力尚未补齐”说明。

上述十个 Rust 文件相对第40批净减 7,831 行（包括测试）。移除 46 个专属于旧接口的测试，
逐项见[退役清单](perf_reports/evidence/2026-09-22-master-projection-retirement/test-retirement.json)。
保留目录恢复集成测试，改为直接检查目录描述、持久 source 定义和 drop tombstone，
不再经迁移投影间接验证。v1 请求与回执的往返测试增加对版本 2、3、4 的明确拒绝断言。

## 原生验收暴露的并发问题

首次真实重启矩阵在本地计算运行时遇到 Catalog revision 318→319：Flight 表元数据分别
读取命名空间、表、schema 后发现版本变化，正确拒绝了混读，但直接向客户端返回失败。

服务端现只对内部识别的 revision 冲突重新执行完整元数据捕获，最多三次。
每次丢弃前次全部结果并保留同一请求资源许可；耗尽后仍返回可重试 Unavailable。
资源不足、数据损坏和一般 Unavailable 原样返回，不被笼统重试。
新增确定性测试覆盖冲突后成功、持续冲突到达上限，以及其他错误不重试。
首次失败日志保留，未修改验收脚本以忽略错误。

## 验证与边界

- Master/Types：221 项通过，另有 4 次日志子进程复验通过；46 个旧接口测试已登记退役。
- Server：223 项通过、3 个原有忽略项，包含新增的三个元数据重试测试。
- 全工作区、全部 target 的 Clippy 零警告；Rustfmt 与 diff 检查通过。
- 最终源码重建的服务端和 Python 扩展通过原生恢复矩阵：三类表、固定快照、
  schema/drop、订阅与 ACK、本地计算、公开示例及 v10 当前布局检查全部通过。
- Python 全量 595 项通过；mypy 91 文件、客户端严格 mypy 27 文件、Ruff 与 Black 通过。

命令结果、源码和二进制摘要见
[结构化证据](perf_reports/evidence/2026-09-22-master-projection-retirement/summary.json)。
平台和发行证据继续区分：本批本机 Arrow 23 验收不替代 Arrow 25 发行包或 Windows/macOS
实机验收。全工作区 Rust 运行证据属于第40批；本批对变更涉及的三个 crate 回归，
同时对全工作区全部 target 编译检查，不把两者混称为再次执行所有 Rust 测试。

剩余重点是删除外部旧文件类型、排序与编解码，再退出 Master 持久 registry、默认 Legacy
和 Gateway/memory ingest/旧查询订阅/内部 source-materializer。旧文件分支仍存在，
因此 F15 未关闭，整体目标也未完成。本批没有提交、推送、部署或改写用户旧数据目录。
