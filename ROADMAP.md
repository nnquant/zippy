# Zippy Roadmap

本文件记录已经从当前审计修复中拆出的后续架构和性能工作。这里的项目不作为本轮
`2026-05-08` 审计关账阻塞项，除非后续压测或线上运行证明其已经升级为正确性问题。

## Gateway Query Execution

### Streaming collect

当前 remote gateway `collect` 仍采用一次性扫描、拼接、执行 residual plan、编码 Arrow IPC
并写回。后续应改成 streaming collect：

- 分批扫描和编码 Arrow IPC。
- 首包尽早返回，降低大查询首包延迟。
- 降低大结果集峰值内存。
- 慢客户端不占住业务 worker。
- 客户端断开后尽早取消后续 scan/encode。

### Parallel persisted scan

当前 persisted parquet scan 仍是同步路径。后续应在 bounded scan pool 中并行扫描多个文件：

- 保持文件级并发上限。
- 保持 projection/filter/tail/head pushdown。
- 保留 deterministic output order。
- 对 scan、decode、filter、encode 分别记录 metrics。

### Native expression coverage

Gateway 当前只原生支持 remote query 的安全子集。后续可逐步扩展：

- `with_columns`
- 复杂 `select` 表达式
- 更多 predicate 表达式
- join 的 remote-side capability

任何扩展都必须保持 local/remote 语义一致。无法安全下推的 plan 必须继续留在 residual path。

## Gateway Runtime And Configuration

### Resource limit configuration

当前 Gateway 连接、subscriber、blocking request 上限是内部默认值。后续可暴露为配置：

- `[gateway].max_connections`
- `[gateway].max_subscribers`
- `[gateway].max_blocking_requests`
- `[gateway].header_timeout_ms`
- `[gateway].payload_timeout_ms`
- `[gateway].write_timeout_ms`

默认值应保持保守，并在 metrics 中暴露 active/rejected/timeout 计数。

### Subscribe notification-driven mode

当前 subscribe socket/timer 已异步化，但数据检测仍以轮询为主。后续可接入 segment committed-row
notification：

- idle subscriber 不做无意义 batch fetch。
- 新行 commit 后唤醒订阅。
- 支持 shutdown 和 descriptor rollover 唤醒。
- 保持 best-effort live 订阅语义，不隐式补历史数据。

## Master Control Plane

### Async master server

Gateway 已经有专用 async master client，但 master server 仍是同步一请求一连接模型。后续如果
control plane 成为瓶颈，再推进 async master server：

- Tokio accept/read/write。
- bounded request execution。
- request timeout 和 slow client 防护。
- 保持现有 JSON-line wire protocol，或单独设计 versioned long-connection protocol。

### Long-lived control sessions

当前 control request 是一请求一连接。后续可评估长连接 session：

- 减少高频 heartbeat/get_stream 的 connect 成本。
- 支持 request multiplexing 前必须定义严格 ordering、timeout 和 reconnect 语义。
- 不能影响现有同步 `MasterClient` API 的兼容性。

## Diagnostics And Performance Baseline

### Gateway performance profiles

后续应增加可重复的 gateway benchmark：

- remote `write_batch` latency/throughput。
- remote `collect` 小结果、大结果、persisted parquet、多文件场景。
- remote `subscribe` idle、active、filtered 场景。
- 高并发 remote collect 下 master control request latency。

### Audit closure maintenance

每轮审计修复后维护单独 closure 文档，逐条标记：

- `fixed`
- `mitigated`
- `deferred`
- `not_applicable`

关账文档必须附带验证命令和剩余风险，不用测试全绿替代风险说明。
