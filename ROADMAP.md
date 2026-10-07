# Zippy 当前路线图

更新：2026-09-23。实施状态统一见[当前能力表](docs/current-capabilities-2026-09-21.md)，
新版本边界与验收标准见[单一主干收敛方案](docs/single-trunk-convergence-plan.md)。
旧 API、配置与数据格式不再承诺兼容；旧数据目录保持原样，不自动迁移或清空。

## 本地收敛状态

1. 在线影子链、Catalog/Storage 导入提交通道、endpoint snapshot 导入、Master 投影事件与 V2/V3/V4 协议已删除；外部旧文件类型、导入快照和 unknown 位点已删除；Master 持久目录及旧 Catalog adapter 也已删除；SQLite、native storage 和 native query 已固定为唯一主干，Flight 默认启动且地址不再派生。
2. Server 旧查询适配器、Gateway crate、旧 memory ingest、重复查询解释器与远端订阅 listener 已删除；materializer 的旧持久 worker/sidecar 已删除，Rust 内部旧 KV/shard materializer 也已删除，Python 扩展的旧绑定已删除。
3. 临时 registry 的旧表/生产者控制请求已删除，只保留配置发现与进程租约。Source/Publisher 后台运行时、专属双队列、ZeroMQ IO crate 已删除，性能工具改用同步计算基准。KV/shard materializer、descriptor forwarding 和专属基准已删除；描述符旧缺省字段和 core 并行度别名已删除；只读 actor 入口与验证包装、底层旧持久 worker、Catalog 提交器双入口、预发布文件提交入口和 Master 随机 token 回退均已退出。旧持久 registry 文件已拒绝且不改写。
4. 保持本地格式、类型、测试和归档发行门禁。按用户 2026-09-22 指示，跨平台实机验收先延期，本次不因此阻塞。

原生三类表、公开管理、订阅 ACK、计算和重启恢复已验收；主包旧数据 API 已删除，全量 mypy
已纳入默认门禁。原审计 34 项中本地关闭 33 项；F28 的跨平台实机验收按用户指示延期，未计为通过。
独立客户端查询兼容别名与无期限控制请求入口也已退出，控制 Watch 长轮询有明确 30 秒上限。
本地因子的批量与上下文执行复用同一内核，不再维护物化 fallback 与重复表达式求值器。

三种表、远端订阅和本地计算均为必须保留的产品能力。每项先实现并验证接替路径，再删除旧实现。
实现细节、测试证据及未关闭编号统一记录在当前能力表链接的实施报告，避免多份清单漂移。
