# Arrow Flight 查询服务

## 定位

Stage 6.1 为 Phase 5 `NativeQueryEngine` 增加标准 Arrow Flight 查询入口。它只负责协议、listener、
ticket 和 Arrow batch 传输，不复制 Catalog、DataFusion 或资源调度逻辑。一次查询仍由 Server 固定
Catalog revision、table snapshot/schema version，并由原有 QueryService 负责内存、任务、取消和结果
batch permit。

当前适用场景：

- 在单机或同一受信主机上验证标准 Flight 查询链路；
- 为 Rust/Python connector 的后续迁移提供稳定 wire contract；
- 在不启动 legacy Gateway 时运行 persisted native SQL 查询。

当前不适用场景：公网/局域网裸连、写入、实时订阅、Flight SQL 元数据发现或通用 BI 客户端直连。

## 启用

Flight 依赖 native storage、SQLite shadow Catalog 和 native query：

```toml
[server]
native_storage_enabled = true

[catalog]
backend = "sqlite_shadow"

[runtime]
max_cpu_cores = 4
max_query_buffer_bytes = 536870912

[query]
native_enabled = true
operator_memory_bytes = 67108864
max_buffered_bytes = 67108864
plan_ttl_ms = 30000

[query.resources]
max_concurrent_queries = 64
max_active_tasks = 16
max_memory_bytes = 536870912

[flight]
enabled = true
endpoint = "127.0.0.1:17667"
protocol_version = 1
max_message_bytes = 2097152
max_pending_tickets = 64
```

若 `endpoint` 省略，且 master 配置了 TCP host/port，则使用相同 host 和 `master.port + 2`。
Stage 6.1 强制使用 loopback IP；TLS 和认证未完成前，`0.0.0.0` 或其他非 loopback 地址会在配置加载时
失败。`max_pending_tickets` 不得超过 `query.resources.max_concurrent_queries`，
`max_message_bytes` 不得超过 query transport byte budget。

对应环境变量：

```text
ZIPPY_FLIGHT_ENABLED
ZIPPY_FLIGHT_ENDPOINT
ZIPPY_FLIGHT_PROTOCOL_VERSION
ZIPPY_FLIGHT_MAX_MESSAGE_BYTES
ZIPPY_FLIGHT_MAX_PENDING_TICKETS
```

默认值是 disabled、protocol version 1、2 MiB message、64 个 pending ticket。

## 查询协议

1. 客户端调用 `DoAction("zippy.capabilities.v1")` 读取版本、消息上限和 capability。
2. 客户端生成非零 `query_id`/`session_id`，在 version 1 `QueryCommand` 中提交 SQL、明确 table
   bindings 和资源限制。
3. `GetFlightInfo` 请求 metadata 必须携带相同的 `x-zippy-session-id`。Server 在返回前已经完成
   Catalog 解析和物理计划固定，响应包含 Arrow schema、`QueryInfo` 和一个有期限 ticket。
4. 同一 session 使用 ticket 调用 `DoGet`。ticket 只可消费一次；错误 session 不会消耗它。
5. 客户端断开、stream drop、deadline、ticket 过期或 Server shutdown 都会释放所持 query、snapshot、
   operator memory 和 transport permit。

`x-zippy-session-id` 当前是生命周期绑定标识，不是认证凭据。它不能替代后续 auth middleware。

Version 1 只接受 SQL input；typed logical plan 会以 `UNSUPPORTED_PLAN` 明确失败，不会编码成空 SQL。
binding 必须非空、有界且名称唯一。Server 不会根据 SQL 隐式猜测 Catalog table。

## 背压和预算

Flight encoder 的目标 batch 大小会预留 gRPC/protobuf 开销；实际 header、body、metadata 和固定开销之和
必须小于 `max_message_bytes`。每条待写消息还需取得全局 transport byte permit。上一批编码数据在客户端
消费前保留 QueryService result permit，因此慢客户端只会施加背压，不会触发无界预取。

`max_pending_tickets` 同时计算正在 prepare 和已经发布但未消费的 ticket。后台每 100 ms 清理过期
ticket；清理和 shutdown 会在 registry 锁外 drop prepared query，避免资源释放阻塞其他请求。

## 错误契约

业务错误使用稳定 `ErrorCode` 映射到 gRPC status，并在 status details 与
`x-zippy-error-bin` 中携带同一份 versioned protobuf envelope。客户端应解析结构化 envelope，不依赖
message 文本。两份 envelope 不一致时客户端必须 fail closed。

未实现 RPC（包括 `DoPut`、`DoExchange`、`ListFlights` 和 `GetSchema`）返回结构化
`UNSUPPORTED_PLAN`，不会静默降级到 legacy collect。

## Readiness 与关停

Server ready record schema version 3 增加 Flight enabled/endpoint/protocol 字段。进程 readiness 需要
`flight_configured`、`flight_listener_ready` 和 `flight_protocol_ready` 同时满足。listener 异常退出会撤销
动态 readiness。

关停顺序为：关闭 Flight admission、释放 pending ticket、停止 Flight listener，再关闭 Gateway、drain
legacy process 与 Catalog shadow、停止 master listener，最后关闭 native storage/Catalog。所有阶段共享
Server 的绝对 shutdown deadline；若 Flight listener 超时，Server 返回不完整结果并让已发出 shutdown 的
listener 在后台继续清理，不会用无界 `join` 突破调用方 deadline。

## 后续阶段

1. Stage 6.2：TLS、认证与 session authority，开放受控非 loopback endpoint。
2. Stage 6.3：query cancel/admin action 与 Flight SQL Catalog metadata。
3. Stage 6.4：`DoPut` ingest，并接入后续 durable producer epoch/sequence。
4. Stage 6.5：Python/Rust connector 切换 Flight query，legacy Gateway 变为 compatibility adapter。
5. Phase 7：`DoExchange` durable subscription、offset/ack 和 replay。
