# 第四十四批：删除服务端旧查询适配链

日期：2026-09-22。承接[第43批](convergence-python-native-bindings-2026-09-22.md)。
累计 **28/34**；F07/F08/F11/F13/F14/F28 保持开放。

## 删除范围

删除 Server `legacy_query.rs`、`composition.rs` 和 Gateway `legacy_query.rs`，退出
LegacyQueryEngineAdapter、LegacyGatewayPlan、LegacyCollectFacade/Request/Cursor 和
LegacyServerServices。旧查询计划缓存、执行登记、流包装、adapter 取消转发及组合层不再存在。
连同失去调用者的 direct collect、取消控制请求和专用 fixture，10 个 Rust 文件净减 4,304 行。

Server 不再按是否启用 Gateway 创建两套服务组合；只保留 Master 的目录和运行时 registry
句柄。对外原生查询句柄继续使用 NativeQueryEngine，其 DataFusion 计划、资源预算、
固定快照、结果流、取消和超时实现保留。旧 runtime.query() 接口随 adapter 删除，没有兼容转发。

Gateway 自身的协议 listener、内部 collect 解释器与 memory ingest 尚未删除，因此 F14 和
其他旧主干编号继续开放。本批不把“删除一层适配器”当作 Gateway 已退出的证据。

## 健康与就绪契约

健康检查原来通过构造 collect 请求调用 facade.validate() 判断 Gateway 状态。
现在直接读取 GatewayServer.is_running()，同时观察停止标记和 listener 线程是否退出。
健康字段改为 gateway_listener_running；ready 记录升级到 **v8**，Rust/Python 同步。
旧 ready 版本明确拒绝；v8 内只有旧 facade 字段也不能伪造新 listener 事实。
starting/stopping/failed 的 v1 是独立生命周期格式，不随 ready 版本改变。

原有主监听器依赖探测、启动失败回滚、进程租约排空、统一截止时间及存储所有权保留逻辑不变。
保留 Gateway 意外退出后 is_running 为 false 的测试，以及 Server readiness/停服和原生恢复测试。
原生计划 TTL 测试继续执行，只删除其中对已移除旧适配器的断言。

## 验证与测试退役

- Server/Gateway Rust 回归：282 次通过、4 个原有忽略项。
- Clippy：全 workspace/all-targets、`-D warnings` 通过；Rustfmt 与 diff 检查通过。
- 重建服务端和 Python 扩展后，Python 全量 552 项通过；mypy 91 文件、客户端严格配置
  27 文件、Ruff 与 Black 92 文件通过。
- 当前二进制通过原生三类表、schema/drop、固定快照、订阅/ACK、计算、插件与重启矩阵。
- 动态库依赖全部可解析。

退役 45 个专属旧查询桥接的 Rust 测试；一个原生 TTL 测试改名并保留有效断言。
新增旧 facade 就绪字段拒绝测试，并将 v7 加入旧 ready 版本拒绝矩阵。
逐项见[测试清单](perf_reports/evidence/2026-09-22-server-query-adapter/test-retirement.json)，
源码、日志和二进制摘要见[结构化证据](perf_reports/evidence/2026-09-22-server-query-adapter/summary.json)。

全 Rust workspace 的运行证据仍来自第42批；本批执行两个变更 crate 的测试并编译检查所有 target。
本机 Linux/CPython 3.11/Arrow 23.0.1 验收不替代其他平台或新的发行包验收。
没有提交、推送、部署或改写用户数据目录。下一步仍需删除默认 Legacy、Master 持久 registry、
Gateway 协议及 memory ingest、Rust 旧 source/materializer，并完成 F28 平台矩阵。
