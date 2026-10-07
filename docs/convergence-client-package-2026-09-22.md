# 第二十四批：删除客户端 SHM 构建链路与隔离发行验收

日期：2026-09-22。推进 F22/F28；原审计仍为 **25/34 已关闭、9 项开放**。
上一批删除独立客户端旧订阅后，剩余 `_shm` 扩展已无运行调用方。本批删除其全部构建链路，
不把“代码不再调用扩展”当成“发行包已无扩展”的证据。

## 删除与构建

删除 `crates/zippy-client-py`、对应 Cargo workspace/lock 成员、客户端 `_shm.pyi`，
以及 ControlClient 的 SHM reader lease 获取/释放接口。现有生成的本地 `.so` 已移出源码树，
不让 editable 环境继续意外导入旧模块。客户端改用固定 setuptools 83.0.0 构建纯 Python
包，显式保留 `py.typed` 和生成协议 `.pyi`，排除本地扩展和缓存产物。

完整 `zippy` 包仍用 Rust 原生扩展，构建后端与 dev CLI 统一固定为当前已验收的 maturin
1.12.6，避免隔离构建无意选取另一个版本。依赖锁文件已同步。

CI 不再对客户端运行 maturin develop；Linux 安装客户端时保留已构建的完整包和测试依赖。
另增加独立客户端发行矩阵：Linux/Windows/macOS，Python 3.11 + Arrow 23.0.1、
Python 3.14 + Arrow 25.0.1。该矩阵不安装 Rust 工具链或完整 `zippy`，但本地仅获得
Linux x86_64 / Python 3.11 的实机结果，不能把 CI 配置视作 Windows/macOS/3.14 验收完成。

## 从发行包验证实际安装

`scripts/check_distributions.py` 使用 uv 的 PEP 517 构建，先创建 sdist，再解压到仓库外，
从解压目录生成 wheel。在两个独立新环境中分别安装客户端、完整包及客户端。

客户端验证要求：

- wheel 为 `py3-none-any`、`Root-Is-Purelib: true`；源码包和 wheel 均不含旧扩展。
- 类型标记、生成 protobuf 实现和类型桩均在 wheel 中。
- `python -I` 导入路径位于隔离环境，`zippy` 和 `zippy_client._shm` 均不可导入。
- 完整客户端测试从已安装 wheel 运行，保留原有 80% 覆盖率门禁。
- client-only 模式不产生 Cargo target。

完整包验证额外运行真实原生服务矩阵：三类表、写入确认、callback/ACK、表身份隔离、
schema 变更、服务重启恢复以及原生计算结果。测试 harness 来自当前仓库，但 Python 包
使用 `-I` 从隔离环境导入，不以工作区 editable 包代替发行包验收。

首次 client-only 测试只有 77.48% 覆盖率，揭示公开查询 API 测试依赖完整包的旧集成服务。
新增独立 TCP 发现与真实 Flight RPC 测试，覆盖命名空间、查询组合、流式结果、profile、
固定快照拒绝、非法参数和关闭后拒绝。Flight peer 提供明确的结果 fixture，不冒充 DataFusion
解释器；真正 SQL 执行仍由 Rust/真实服务矩阵验证。补测试后原包覆盖率达到 80.09%，
随后删除无调用方 SHM lease 接口；最终两个 Arrow 环境均为 159 passed / 1 skipped、80.30%。
跳过的是明确需要完整包的旧 Master/Gateway 集成模块，不影响原生客户端协议测试。

## 全量回归暴露的两处测试竞态

Rust 旧 Gateway 慢连接测试在建表首批响应阶段失败。配置中的 10ms 同时限制 Master RPC，
不是单独的 socket 背压预算；源码证据见 `GatewayAsyncMasterClient::new(..., write_timeout)`。
测试改为 250ms 有界预算，保留慢连接必须超时、关闭原因及资源释放断言，并在建表失败时
输出真实响应。原始失败未记录具体响应，不能把调度归因说成已捕获了服务端超时错误码。

Python 旧 ZMQ source 过滤测试在单次 publish 后未收到结果。原测试用固定 100ms sleep
假定 PUB/SUB 两端就绪。现在先通过无状态表达式链路观察独立探针，确认可收发后仅发送一次
业务批次；收取时区分探针，保留实际过滤值和 filtered_rows_total 断言，失败时也释放资源。
不以重复发布业务数据来隐藏可能的消息丢失。

首次失败、针对性回归和最终全量日志均收录在
[验收证据](perf_reports/evidence/2026-09-22-client-package/summary.json)。

## 仍未关闭

主包旧 API、source/Session/Pipeline、Catalog/服务权威切换、Gateway 查询写入退出、
在线迁移桥接、实际跨平台矩阵及全仓类型债务仍需继续处理。全仓 mypy 仍有 991 errors，
不因配置门禁或独立发行测试通过而关闭 F29。F07/F08/F11/F13/F14/F15/F22/F28/F29 保持开放。
