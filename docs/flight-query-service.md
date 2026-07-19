# Arrow Flight 查询与内存写入服务

## 定位

Stage 6.4 在标准 Arrow Flight 查询入口上增加了有界、append-only 的 `DoPut`。Flight 仍只负责协议、
listener、session、ticket 和 Arrow batch 传输，不复制 Catalog、DataFusion 或资源调度逻辑。一次查询仍由 Server 固定
Catalog revision、table snapshot/schema version，并由原有 QueryService 负责内存、任务、取消和结果
batch permit；一次 Stage 6.4 写入则复用现有 Gateway materializer，回执只表示进程内存已接收。

当前适用场景：

- 在单机、局域网或受控远程网络上运行加密的 Flight 查询链路；
- 为 Rust/Python connector 的后续迁移提供稳定 wire contract；
- 在不启动 legacy Gateway 时运行 persisted native SQL 查询。
- 使用标准 Flight SQL 客户端发现 catalog、namespace、table 和 Arrow schema；
- 由查询 owner 取消自己的查询，或由控制面管理员精确取消跨 session 查询。
- 从 Rust/Python 等 Arrow 客户端批量追加实时数据，并接受“进程退出可能丢失”的明确语义；
- 在 Phase 7 durable ingest 上线前，先迁移远端批量写入的 transport、鉴权和资源治理。

当前不适用场景：公网裸露、要求 WAL/persisted durability 的写入、网络重试去重、实时订阅、完整
Flight SQL statement 执行、细粒度表级 RBAC 或未经兼容性验证的通用 BI 客户端直连。管理员凭据是
control-only，不具备查询、metadata 读取或写入权限；Writer 凭据也不能读取或取消查询。

## 启用

Flight 依赖 native storage、SQLite shadow Catalog 和 native query：

```toml
[server]
native_storage_enabled = true

[catalog]
backend = "sqlite_shadow"

# Stage 6.4 DoPut 复用 Gateway materializer，因此启用写入时 Gateway 必须运行。
[gateway]
enabled = true
endpoint = "127.0.0.1:17666"

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
endpoint = "0.0.0.0:17667"
protocol_version = 1
max_message_bytes = 2097152
max_pending_tickets = 64

[flight.tls]
enabled = true
cert_file = "/etc/zippy/flight/server.pem"
key_file = "/etc/zippy/flight/server.key"
# 可选：配置后验证客户端证书；require=true 时强制 mTLS。
# client_ca_file = "/etc/zippy/flight/client-ca.pem"
require_client_certificate = false

[flight.auth]
enabled = true
token_file = "/etc/zippy/flight/query.token"
# 可选；必须与 query.token 使用不同文件和内容。
admin_token_file = "/etc/zippy/flight/admin.token"
# 启用 DoPut 时必填；必须与 read/admin 使用不同文件和内容。
writer_token_file = "/etc/zippy/flight/writer.token"
session_ttl_ms = 3600000
max_sessions = 1024

[flight.metadata]
max_rows = 4096
max_bytes = 8388608
ticket_ttl_ms = 30000

[flight.ingest]
enabled = true
max_concurrent_puts = 4
max_batches_per_put = 1024
max_rows_per_batch = 65536
max_rows_per_put = 1048576
max_batch_bytes = 8388608
max_bytes_per_put = 268435456
```

若 `endpoint` 省略，且 master 配置了 TCP host/port，则使用相同 host 和 `master.port + 2`。
非 loopback endpoint 必须同时启用 TLS 和认证，缺少任一项都会在 bind 前失败。loopback 可继续使用
6.1 的无认证兼容模式，也可启用相同安全策略。`max_pending_tickets` 不得超过
`query.resources.max_concurrent_queries`，
`max_message_bytes` 不得超过 query transport byte budget。

对应环境变量：

```text
ZIPPY_FLIGHT_ENABLED
ZIPPY_FLIGHT_ENDPOINT
ZIPPY_FLIGHT_PROTOCOL_VERSION
ZIPPY_FLIGHT_MAX_MESSAGE_BYTES
ZIPPY_FLIGHT_MAX_PENDING_TICKETS
ZIPPY_FLIGHT_TLS_ENABLED
ZIPPY_FLIGHT_TLS_CERT_FILE
ZIPPY_FLIGHT_TLS_KEY_FILE
ZIPPY_FLIGHT_TLS_CLIENT_CA_FILE
ZIPPY_FLIGHT_TLS_REQUIRE_CLIENT_CERTIFICATE
ZIPPY_FLIGHT_AUTH_ENABLED
ZIPPY_FLIGHT_AUTH_TOKEN_FILE
ZIPPY_FLIGHT_AUTH_TOKEN
ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN_FILE
ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN
ZIPPY_FLIGHT_AUTH_WRITER_TOKEN_FILE
ZIPPY_FLIGHT_AUTH_WRITER_TOKEN
ZIPPY_FLIGHT_AUTH_SESSION_TTL_MS
ZIPPY_FLIGHT_AUTH_MAX_SESSIONS
ZIPPY_FLIGHT_METADATA_MAX_ROWS
ZIPPY_FLIGHT_METADATA_MAX_BYTES
ZIPPY_FLIGHT_METADATA_TICKET_TTL_MS
ZIPPY_FLIGHT_INGEST_ENABLED
ZIPPY_FLIGHT_INGEST_MAX_CONCURRENT_PUTS
ZIPPY_FLIGHT_INGEST_MAX_BATCHES_PER_PUT
ZIPPY_FLIGHT_INGEST_MAX_ROWS_PER_BATCH
ZIPPY_FLIGHT_INGEST_MAX_ROWS_PER_PUT
ZIPPY_FLIGHT_INGEST_MAX_BATCH_BYTES
ZIPPY_FLIGHT_INGEST_MAX_BYTES_PER_PUT
```

默认值是 disabled、protocol version 1、2 MiB message、64 个 pending ticket、1 小时 session TTL、
1024 个 active session、4096 行 metadata、8 MiB 单结果 metadata 和 30 秒 metadata ticket TTL。
ingest 默认关闭；启用后的默认上限为 4 个并发 put、每个 put 1024 个 batch/1,048,576 行/256 MiB、
单 batch 65,536 行/8 MiB。
环境变量中的凭据文件路径必须是绝对路径；TOML 相对路径按配置文件目录解析。

## 查询协议

1. 客户端调用 `DoAction("zippy.capabilities.v1")` 读取版本、消息上限、认证要求和 capability。
2. 认证模式下，查询客户端以 `Authorization: Bearer <query-bootstrap-token>` 调用 Flight
   `Handshake`，发送 version 1 `SessionOpenRequest`。Server 返回随机 `session_id`、`zfs1_...`
   session bearer 和固定
   `expires_at_ns`。bootstrap token 不再用于查询 RPC。
3. 客户端生成非零 `query_id`，在 version 1 `QueryCommand` 中使用 Server 返回的 `session_id`，提交
   SQL、明确 table bindings 和资源限制。
4. `GetFlightInfo` 与 `DoGet` metadata 必须携带
   `Authorization: Bearer <session-bearer>`。Server 不信任客户端自报的 session；command 中的
   `session_id` 必须与 bearer 解析出的 authority 一致。Server 在返回前已经完成
   Catalog 解析和物理计划固定，响应包含 Arrow schema、`QueryInfo` 和一个有期限 ticket。
5. 同一 session 使用 ticket 调用 `DoGet`。ticket 只可消费一次；错误 bearer 不会消耗它。
6. 客户端断开、stream drop、query deadline、ticket 过期、session 过期时撤销未消费 ticket，或 Server
   shutdown，都会在对应生命周期边界释放 query、snapshot、operator memory 和 transport permit。

只有显式关闭 `[flight.auth]` 的 loopback 兼容模式继续读取 `x-zippy-session-id`。该 header 在认证模式下
不会参与 authority 判定，不能用来伪造或续期 session。

Version 1 只接受 SQL input；typed logical plan 会以 `UNSUPPORTED_PLAN` 明确失败，不会编码成空 SQL。
binding 必须非空、有界且名称唯一。Server 不会根据 SQL 隐式猜测 Catalog table。

## `DoPut` 内存写入协议

1. 客户端先用独立 writer bootstrap token 调用 `Handshake`，得到 `Writer` role、随机
   `session_id` 和短期 session bearer。read/admin token 均不能建立 Writer session。
2. 客户端创建标准 Flight `DoPut` stream。第一条消息必须是唯一的 Arrow schema，并携带 command
   descriptor；后续消息只能是相同 schema 的 record batch。
3. version 1 `PutCommand` 必须提供非零 `put_id`、与 bearer 一致的 `session_id`、`stream_name`、
   `first_batch_sequence`，并显式选择 `Append + Memory`。其他 mutation/durability 返回
   `UNSUPPORTED_PLAN`。
4. Server 按顺序解码和提交 batch。每个成功 batch 返回一个标准 `PutResult`，其 `app_metadata` 是
   `PutBatchResult`，包含 `put_id`、`batch_sequence`、`row_count` 和实际达到的 `Memory` durability。
5. 回执 channel 容量为 1；客户端不读取回执时，Server 不会继续无限解码。单 batch 在 backend
   返回前占用共享 transport byte permit，返回后才释放。

`put_id` 和 `batch_sequence` 在 Stage 6.4 只用于关联请求和回执，**不提供服务端去重或 producer
fencing**。若连接在部分 batch 成功后断开，客户端可能已经收到部分回执，也可能无法确定最后一批是否
生效。此时不能盲目重试并期待 exactly-once。需要网络重试去重、重启恢复或 WAL/persisted ACK 的生产
写入，应继续使用既有受控链路。Stage 7.2 原生核心已经具备 fencing、幂等和 WAL ACK，但在 Flight
durable command 完成切流前，不能把该能力推断到当前 v1 `DoPut`。

Stage 7.1 backend 使用 server-owned `MemoryIngestRuntime`。Gateway 和 Flight 只持有同一个 cloneable
handle，每个 stream 的 segment materializer 仅由该 runtime 创建一次，并在每批后完成现有 flush。
因此 `[flight.ingest]` 不再要求 `[gateway]` 启用；Gateway 关闭时 Flight writer 仍可独立运行。
该 backend 不会写 ingest WAL、登记 producer epoch、推进 durable sequence，也不会把 memory ACK 冒充
磁盘提交。Stage 7.2 已在 `zippy-ingest::NativeIngestRuntime` 实现这些原生能力，但尚未接入当前 Flight
command 和 table materializer。后续 Stage 7.3 会显式扩展 durable command 并切换 backend，同时保留
Writer role 和已有资源边界。若仍需旧 remote client，可继续启用 Gateway compatibility listener；
否则可直接关闭。

## 查询取消与管理员权限

客户端使用 Arrow Flight 标准 `CancelFlightInfo` action，并原样提交 `GetFlightInfo` 返回的
`FlightInfo`。取消目标由 opaque ticket 中的 `query_id + generation + server_instance_id` 唯一确定：

- read session 只能取消自己拥有的查询；跨 session 返回 `PERMISSION_DENIED`；
- admin bootstrap token 经 `Handshake` 签发 `Administrator` session，可取消任意 owner 的精确查询；
- administrator session 是 control-only，调用 `GetFlightInfo` 或 `DoGet` 返回 `PERMISSION_DENIED`；
- 旧进程 ticket、篡改 generation 或错误 server instance 不会命中新查询，避免 query ID 复用导致 ABA；
- 尚未消费的 query ticket 被取消时直接 drop prepared plan；已经进入 `DoGet` 的查询交给统一
  QueryService cancellation authority；metadata ticket 返回 `NOT_CANCELLABLE`。

`admin_token_file`/`ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN` 是可选项；启用时必须与 read token 来源和内容不同。
管理员取消会记录结构化审计日志，并分别累计 owner/admin cancellation 指标。

## Flight SQL metadata

Stage 6.3 支持标准 `GetFlightInfo -> DoGet` 形式的四类 Flight SQL 命令：

| 命令 | Zippy 映射 |
| --- | --- |
| `CommandGetCatalogs` | 单一 catalog `zippy` |
| `CommandGetDbSchemas` | active Catalog namespaces |
| `CommandGetTables` | active tables；`include_schema=true` 时返回 Arrow IPC schema |
| `CommandGetTableTypes` | `TABLE` |

filter 由 Arrow Flight SQL builder 按标准语义执行，但 `max_rows` 限制的是一次请求最多检查和保留的
Catalog 行数，不允许通过高选择性 filter 绕过扫描上限。`GetTables(include_schema=true)` 使用单独的
轻量 Catalog schema batch API，只读取 `table_schemas`，不会加载 snapshot manifest、data files 或
source lineage。

Catalog list、schema batch 和发布前复核必须看到同一 revision；若准备期间 revision 改变，请求返回
可重试的 `UNAVAILABLE`，不会把不同 revision 的 namespace/table/schema 拼成一个结果。metadata ticket
与查询 ticket 一样绑定 session、server instance、固定过期时间且只能消费一次。

## 背压和预算

Flight encoder 的目标 batch 大小会预留 gRPC/protobuf 开销；实际 header、body、metadata 和固定开销之和
必须小于 `max_message_bytes`。每条待写消息还需取得全局 transport byte permit。metadata prepare 先按
`max_bytes` 从同一个共享 transport budget 预留工作内存，构建完成后归还未使用部分，待消费 ticket
只保留 Arrow 实际内存；预算不足时 `GetFlightInfo` 快速返回可重试的 `RESOURCE_EXHAUSTED`。ticket 被
消费、过期、session 撤销或 Server 关闭时 permit 自动归还。上一批
编码数据在客户端消费前保留 QueryService result permit，因此慢客户端只会施加背压，不会触发无界预取。

`max_pending_tickets` 同时计算正在 prepare 和已经发布但未消费的 query/metadata ticket。后台每
100 ms 清理过期 ticket；清理和 shutdown 会在 registry 锁外 drop payload，避免资源释放阻塞其他请求。
`flight.metadata.max_bytes` 既是单结果上限，也不得超过共享 transport budget。状态面暴露该预算的
capacity/in-use gauge，以及 metadata issued/consumed、owner/admin cancellation 计数。

session registry 同样有 `max_sessions` 硬上限和固定 TTL，不做滑动续期。session 过期时，仍未消费的
ticket 会被撤销并释放 prepared query。已经通过 `DoGet` 完成认证的运行中查询不会被 session TTL 中途
终止，仍受 query deadline、客户端断开和 Server shutdown 约束。

ingest 另有 `max_concurrent_puts` semaphore，以及每个 put 的 batch、row、byte 累计上限和单 batch
row/byte 上限。并发槽耗尽时在读取请求体前快速返回可重试的 `RESOURCE_EXHAUSTED`；解码后超限的 batch
不会调用 backend。`max_batch_bytes` 不得超过共享 query transport budget，避免写入绕过进程级内存账本。
每个 put 内顺序提交，默认最多只有 4 条活跃写入流，因此 `spawn_blocking` 的进程内 memory authority
调用也是有界的。
capability response 和 ready record 同时公开六项有效上限，客户端应在发送前按较小值切 batch。

## 凭据与 TLS 约束

- read/admin/writer bootstrap token 都必须是 32 到 4096 个可见 ASCII 字节，且两两不同。TOML 不接受明文 token 字段；
  只能使用 owner-only 文件或对应环境变量。
- token file 和 private key 必须由 Server 当前用户拥有、只有一个硬链接，且 group/other 权限为零；
  推荐 `chmod 600`。credential reader 使用 no-follow 打开，符号链接、目录和超限文件会失败。
- certificate/client CA 是公开材料，不要求 `0600`，但仍必须是有界 regular file 且拒绝符号链接。
- `client_ca_file` 配置后会验证客户端证书；`require_client_certificate=true` 会拒绝未携带可信证书的
  客户端。Bearer auth 仍然必需，两者是叠加关系。
- token、private key 和 Authorization 不进入 ready record、JSON config 或 Debug 输出。ready record 只
  公布 TLS/auth 开关、client certificate 要求和 certificate-chain PEM 的 SHA-256 指纹。

### 证书轮换

6.2 使用明确的重启加载契约，尚不做进程内热加载：

1. 将新证书和新私钥写入新的 regular file，并把私钥权限设为 `0600`；
2. 原子替换配置或更新配置路径；
3. 执行有界 `zippy server restart`；
4. 等待 schema version 7 ready record，再核对 `flight.certificate_sha256`；
5. 客户端信任库切换到新 CA 后建立新 session。旧 session 随旧 listener 退出，不跨进程复用。

自动化测试覆盖旧 CA 在重启轮换后失败、新 CA 成功，以及 mTLS 无 client identity 失败。进程内无损
证书热轮换需要双 identity overlap、失败回退和连接 drain，留到后续独立阶段。

## 错误契约

业务错误使用稳定 `ErrorCode` 映射到 gRPC status，并在 status details 与
`x-zippy-error-bin` 中携带同一份 versioned protobuf envelope。客户端应解析结构化 envelope，不依赖
message 文本。两份 envelope 不一致时客户端必须 fail closed。

未实现 RPC（包括 `DoExchange`、`ListFlights` 和 `GetSchema`）返回结构化
`UNSUPPORTED_PLAN`，不会静默降级到 legacy collect。

## Readiness 与关停

Server ready record schema version 7 在既有 Flight enabled/endpoint/protocol、TLS/auth、capability、
limit 和非敏感证书指纹之外，新增 `memory_ingest_configured/ready` 以及 bounded ingest 状态。`zippy
server run/status` 摘要会显示 Flight endpoint、TLS、memory ingest 和 Writer 鉴权状态。进程 readiness 需要
`flight_configured`、`flight_listener_ready` 和 `flight_protocol_ready` 同时满足。listener 异常退出会撤销
动态 readiness；配置 Gateway 或 Flight ingest 时，共享 memory ingest authority 也必须仍在接受请求。

关停顺序为：统一关闭 Flight 与 memory ingest admission、释放 pending ticket、停止 Flight listener，
再关闭 Gateway、drain memory writers/publisher process、legacy process 与 Catalog shadow、停止 master
listener，最后关闭 native storage/Catalog。所有阶段共享
Server 的绝对 shutdown deadline；若 Flight listener 超时，Server 返回不完整结果并让已发出 shutdown 的
listener 在后台继续清理，不会用无界 `join` 突破调用方 deadline。

## 性能基线与门禁

真实 Gateway/Flight release 基准会启动两种 listener，并让它们共享同一 Catalog、Parquet 数据、
native query engine 和写入 materializer。默认对 1、1,024、65,536 行的查询与 memory-ACK 写入分别
采集 100 个样本；每个场景要求 Flight 吞吐不低于 Gateway 的 90%，且 p99 不超过 Gateway 的 1.20 倍。

2026-07-19 的 WSL2 loopback 基线中，Flight 小查询吞吐约为 Gateway 的 93%，65,536 行查询基本持平；
三档写入吞吐均高于 Gateway。accepted Flight socket 必须保持 `TCP_NODELAY`，对应回归测试会直接验证
server 侧 socket 选项。完整方法、结果和适用边界见
[`perf_reports/2026-07-19-flight-gateway-transport.md`](perf_reports/2026-07-19-flight-gateway-transport.md)。

手动生成 JSON 报告时设置 `ZIPPY_TRANSPORT_PERF_REPORT_JSON`，并运行 ignored test
`flight_gateway_transport_performance_baseline`。可用
`ZIPPY_TRANSPORT_PERF_MEASURED_SAMPLES` 调整计量次数；低于 100 的样本只适合快速 smoke，不应作为
p99 发布依据。

## 后续阶段

1. Stage 6.5 Python connector 已完成：`zippy-client` 默认使用 Flight query/DoPut，解析稳定错误，
   并保留 legacy Gateway compatibility adapter。Rust connector 后续单独发布。
2. Phase 7.1 已完成 Gateway/Flight server-owned memory authority cutover；下一步把该 backend 切到
   producer epoch/sequence + WAL，并实现 `DoExchange` durable subscription、offset/ack 和 replay。
