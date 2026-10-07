# Arrow Flight 查询与可恢复写入服务

## 定位

标准 Arrow Flight `DoPut` 使用原生 durable ingest authority，支持 append、frame 和 key-value。
Flight 只负责协议、listener、session 和 Arrow batch 传输，不复制 Catalog、DataFusion、producer
fencing、WAL 或资源调度逻辑。查询由 Server 固定 Catalog revision、table snapshot/schema version；
写入由 SQLite Catalog 颁发 producer epoch，经 ingest WAL 和 native materializer 原子发布到
`TableActor`，回执携带权威 epoch、逻辑 offset、水位和实际 durability。

当前适用场景：

- 在单机、局域网或受控远程网络上运行加密的 Flight 查询链路；
- 为 Rust/Python connector 的后续迁移提供稳定 wire contract；
- 通过唯一 Flight 数据入口运行原生 SQL 查询和三类表写入；
- 使用标准 Flight SQL 客户端发现 catalog、namespace、table 和 Arrow schema；
- 由查询 owner 取消自己的查询，或由控制面管理员精确取消跨 session 查询。
- 从 Rust/Python 等 Arrow 客户端批量追加实时数据，按请求选择 `Memory`、可重启恢复的 `Wal` 或 manifest 确认的 `Persisted` ACK；
- 使用 producer fencing、严格 batch sequence、服务端幂等回执和跨重启稳定 log offset。

当前不适用场景：公网裸露、没有稳定请求身份的歧义写入重试、完整
Flight SQL statement 执行、细粒度表级 RBAC 或未经兼容性验证的通用 BI 客户端直连。管理员凭据是
control-only，不具备查询、metadata 读取或写入权限；Writer 凭据也不能读取或取消查询。

## 原生表管理

管理员 action `zippy.create_namespace.v1`、`zippy.create_table.v1` 对应 `catalog.create.v1`；
`zippy.drop_table.v1` 对应 `catalog.drop.v1`。删除携带 request ID、不可变 table ID 和非零
expected snapshot version，成功返回 `CatalogMutationResult`，其中 schema_version=0 表示删除结果。
相同请求在响应丢失或重启后返回原回执；修改已有请求的内容被拒绝。read/writer 会话不能调用管理 action。

删除先隔离写入并排空 actor 命令，提交 Catalog tombstone 后终止该表的运行时和持续日志。
固定历史快照继续受 pin/lease 保护；删除后可用新 table ID 创建同名表并复用运行时容量。
旧请求和旧游标不会跳转到新表。
`zippy.alter_table_schema.v1` 对应 `catalog.schema.v1`，携带逻辑 schema IPC、非零
expected schema version、table ID 和 request ID；KV 必须带显式且不变的有序 keys。
返回 `CatalogMutationResult` 的新 schema version。变更先隔离新写入并持久化旧水位，
提交 Catalog 后替换运行时；旧 writer/日志以 `SCHEMA_MISMATCH` 终止。
允许追加可空列、普通列放宽可空性及描述元数据更新，历史读取补 null；固定快照保持原 schema。
精确重放不回退新版本或复活已删除表。建表/变更指纹使用有界校验后的逻辑 schema 内容，
不依赖 IPC 元数据键顺序；当前目录 v11 不接受旧指纹契约目录。

具体 Python 用法和删除边界见[客户端说明](../clients/python/README.md#原生在线删除)。

## 原生日志 DoExchange

原生 frontend 宣告 `subscription.log_fragments.v1`，提供 append/frame/key-value 的持续日志分片。
`LogCommand` 显式选择 latest 或 inclusive offset；独立 metadata-only 消息的 `LogOpened` 返回身份与水位，随后发送 schema，
每批 `LogProgress` 返回 offset 和完整 frame 范围。客户端可半关闭输入并继续读取，也可发送
`LogControl(cancel=true)` 终止；丢弃输出流会释放原生读取所有权。

wire 提供原始事件分片；Python `Client.open_log` 已实现完整 frame/KV mutation 交付和显式游标续读。
主包 StateSubscription 已支持完整状态重建与重启恢复；独立客户端已接通持久 ACK、消费组
位点和原生 callback，主包旧 callback 已删除。
切换边界见[当前能力表](current-capabilities-2026-09-21.md)。

## 启用

SQLite、native storage 和 native query 是唯一主干，无需启用开关。Flight 默认启用，
监听 `127.0.0.1:17667`；下面展示显式配置远端认证与写入的部署方式：

```toml
[runtime]
max_cpu_cores = 4
max_query_buffer_bytes = 536870912

[query]
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

若 `endpoint` 省略，固定使用 `127.0.0.1:17667`，不从 Master host/port 派生。
`flight.enabled=false` 只关闭远端监听器，仍启动原生存储并完成恢复。
旧后端选择及 native 开关在 TOML、JSON 和环境变量入口均明确拒绝。
非 loopback endpoint 必须同时启用 TLS 和认证，缺少任一项都会在 bind 前失败。loopback 可显式使用
无需 bootstrap 凭据的只读握手，也可启用相同安全策略。两种部署共用服务端会话 authority。`max_pending_tickets` 不得超过
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
6. 查询 stream drop、deadline、ticket/session 过期和 Server shutdown 在各自生命周期边界释放
   query、snapshot、operator memory 和 transport permit。单纯关闭 TCP 通道不撤销 bearer 会话；
   客户端应发送下述显式关闭 action，避免短连接耗尽会话名额。

显式关闭 `[flight.auth]` 时，仅 loopback listener 提供 `auth.local_read_handshake.v1`：同样调用
Handshake，省略 bootstrap header，并取得随机、短期的只读 session bearer。后续 RPC 与认证部署
完全使用同一会话校验、容量、过期、撤销和 ticket 归属规则。旧 `x-zippy-session-id` 不再被解析，
仅持有该 header 的请求返回 UNAUTHENTICATED。已配置 bootstrap token 的客户端若遇到本地只读服务，
明确报错，不静默忽略凭据。writer/admin 必须使用认证部署。

Version 1 只接受 SQL input；typed logical plan 会以 `UNSUPPORTED_PLAN` 明确失败，不会编码成空 SQL。
binding 必须非空、有界且名称唯一。Server 不会根据 SQL 隐式猜测 Catalog table。

## `DoPut` 可恢复写入协议

1. 客户端先用独立 writer bootstrap token 调用 `Handshake`，得到 `Writer` role、随机
   `session_id` 和短期 session bearer。read/admin token 均不能建立 Writer session。
2. 客户端创建标准 Flight `DoPut` stream。第一条消息必须是唯一的 Arrow schema，并携带 command
   descriptor；后续消息只能是相同 schema 的 record batch。
3. version 1 `PutCommand` 必须提供非零 `put_id`、`producer_id`、`connection_id`、与 bearer 一致的
   `session_id`、`stream_name`、`first_batch_sequence`，并显式选择 `Append + Memory` 或
   `Append + Wal`。已持有 writer 时还应发送 `expected_writer_epoch`，防止另一个连接已经接管表。
4. Server 按顺序解码和提交 batch。每个成功 batch 返回一个标准 `PutResult`，其 `app_metadata` 是
   `PutBatchResult`，包含确定性 batch `request_id`、`batch_sequence`、`row_count`、producer epoch、
   连续 log-offset range、前后水位和实际达到的 durability。
5. 回执 channel 容量为 1；客户端不读取回执时，Server 不会继续无限解码。单 batch 在 backend
   返回前占用共享 transport byte permit，返回后才释放。

每个不同的新 `connection_id` 都通过 Catalog acquire 获得更高的 table-wide producer epoch，旧
connection 随后被明确 fence。同一 producer/connection 携带当前 `expected_writer_epoch` 再次打开
DoPut 时直接恢复现有 epoch，不重复写 Catalog、切换 WAL segment 或 fence TableActor。相同
epoch/sequence/request/payload 的底层重放返回原 receipt；身份或 payload
冲突、sequence 缺口和 stale epoch 都明确失败。`Memory` 在 WAL data record 已追加并原子发布到
TableActor 后返回，但未被 commit marker 覆盖的尾部在崩溃后允许丢失；`Wal` 只有在 commit marker
完成 `sync_data` 后返回，可在重启时确定恢复。原生 append 的 `Persisted` 进一步等待
Parquet/manifest 覆盖该批水位，并把包含 snapshot 版本的回执持久归档。重启后的精确重试返回
第一次已确认的 snapshot 版本。只有支持 checkpoint 的 backend 才公布 `ingest.append.persisted.v1`，
其他 backend 在写入前明确拒绝该等级。

Python 高层 `FlightWriter` 会稳定复用 producer/connection ID、串联服务端 epoch，并在服务端配额与
RPC deadline 内跨多次 `flush()` 保持一个 DoPut stream；达到 batch/row/byte/time 上限后才关闭并以
同一 epoch 恢复新 stream。它逐批校验 receipt 的 request ID、offset、水位和 durability。若网络在
提交与回执之间中断，它会携带已确认 receipts 进入 fail-closed，
不会用新身份盲目重放不确定 batch。需要业务级自动恢复时，调用方必须先按稳定 offset 或业务主键核对，
再创建新 writer；当前高层 API 不宣称 exactly-once workflow。

Stage 7.3 backend 是 server-owned `NativeIngestRuntime`，WAL 位于 Server 数据目录的
`wal/ingest/<table-id>/`。SQLite 是唯一 Catalog；Gateway 和旧 memory ingest 已删除，只有一套原生写入权威。`sqlite_shadow` 及在线影子导入已删除；
原生查询和写入只接受 SQLite authority，不再提供 Gateway 到原生引擎的迁移桥接。

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

`session.close.v1` 宣告 `zippy.close_session.v1` action。请求为 `SessionCloseRequest`，仅携带
`protocol_version=1`；通过当前 bearer 认证，无目标 session 参数。三种角色都只能撤销自己的
会话，响应 `SessionCloseResponse.closed=true` 表示已撤销。版本错误或非法请求不得产生回收副作用。
服务端同时释放该会话名额、未消费 ticket 与固定快照租约；新握手可立即复用容量。
SDK 关闭写入流和日志 reader 后撤销全部持有会话，包括刷新后保留的旧会话。失败时仍关闭其他
本地通道；网络不可达则记录警告，远端最终由 TTL 回收。已通过认证的在途 RPC 仍按原有取消和
deadline 契约终止，action 不承诺强制取消所有在途查询。

ingest 另有 `max_concurrent_puts` semaphore，以及每个 put 的 batch、row、byte 累计上限和单 batch
row/byte 上限。并发槽耗尽时在读取请求体前快速返回可重试的 `RESOURCE_EXHAUSTED`；解码后超限的 batch
不会调用 backend。`max_batch_bytes` 不得超过共享 query transport budget，避免写入绕过进程级内存账本。
每个 put 内顺序提交，默认最多只有 4 条活跃写入流；native ingest 的全局 operation budget、per-table
pending ACK budget 和 TableActor mailbox 继续提供第二层有界 admission。
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
4. 等待 schema version 8 ready record，再核对 `flight.certificate_sha256`；
5. 客户端信任库切换到新 CA 后建立新 session。旧 session 随旧 listener 退出，不跨进程复用。

自动化测试覆盖旧 CA 在重启轮换后失败、新 CA 成功，以及 mTLS 无 client identity 失败。进程内无损
证书热轮换需要双 identity overlap、失败回退和连接 drain，留到后续独立阶段。

## 错误契约

业务错误使用稳定 `ErrorCode` 映射到 gRPC status，并在 status details 与
`x-zippy-error-bin` 中携带同一份 versioned protobuf envelope。客户端应解析结构化 envelope，不依赖
message 文本。两份 envelope 不一致时客户端必须 fail closed。

未实现 RPC（包括 `ListFlights` 和 `GetSchema`）返回结构化
`UNSUPPORTED_PLAN`，不会静默降级到 legacy collect。

## Readiness 与关停

Server ready record schema version 11 在既有 Flight enabled/endpoint/protocol、TLS/auth、capability、
limit 和非敏感证书指纹之外，公开 native ingest 状态。`zippy server run/status` 摘要会显示 Flight
endpoint、TLS、native ingest 和 Writer 鉴权状态。四个原生恢复条件无条件参与进程 readiness；启用 Flight 时还需要
`flight_configured`、`flight_listener_ready` 和 `flight_protocol_ready` 同时满足。listener 异常退出会撤销
动态 readiness；配置 Flight ingest 时，native ingest authority 也必须仍在接受请求。

关停顺序为：关闭 Flight admission、释放 pending ticket、停止 Flight listener，排空进程租约并停止 master listener，再 drain native ingest
WAL/worker、TableActor 和 PersistController，最后关闭 native storage/Catalog。所有阶段共享
Server 的绝对 shutdown deadline；若 Flight listener 超时，Server 返回不完整结果并让已发出 shutdown 的
listener 在后台继续清理，不会用无界 `join` 突破调用方 deadline。

## 性能基线与门禁

Gateway 已删除，原 Gateway/Flight 对照基准仅作为历史记录；当前性能入口只保留原生 Flight ingest 基准。

2026-07-19 的 WSL2 loopback 基线中，Flight 小查询吞吐约为 Gateway 的 93%，65,536 行查询基本持平；
三档写入吞吐均高于 Gateway。accepted Flight socket 必须保持 `TCP_NODELAY`，对应回归测试会直接验证
server 侧 socket 选项。完整方法、结果和适用边界见
[`perf_reports/2026-07-19-flight-gateway-transport.md`](perf_reports/2026-07-19-flight-gateway-transport.md)。

原 Gateway 对照测试已随实现退役，不再提供该测试入口。

`native_flight_ingest_performance_baseline` 直接测量 WAL、native materializer、TableActor 和 Flight
receipt 的完整公开路径。低延迟修订在 WSL2 loopback release 上把 1 行短 DoPut p50 降至 0.768 ms
（Memory）和 4.507 ms（Wal）；Python 高层采用的持久 DoPut 目标形态进一步达到 0.557 ms 和
1.884 ms。65,536 行短 DoPut 吞吐为 8.585 M rows/s 和 5.073 M rows/s。方法、原基线、完整百分位、
RSS 和适用边界见
[`perf_reports/2026-07-19-flight-native-ingest.md`](perf_reports/2026-07-19-flight-native-ingest.md)。

## 后续阶段

1. Python connector 的查询、写入和订阅只有 Flight，解析稳定错误；客户端 Gateway adapter
   和主包重复实现均已删除。Rust connector 的独立发行另行规划。
2. 原生 append/frame/key-value 已接通 producer fencing、WAL、checkpoint 和 manifest `Persisted` ACK；
   DoExchange 已支持带 offset 的原生日志分片。在线 drop、状态恢复、同名新 ID 重建、schema
   演进、消费 ACK 和 callback 已验收；Gateway 旧订阅已经退出，剩余 Master 持久目录与内部 source/materializer 待删除。
   最新验收状态统一见[当前能力表](current-capabilities-2026-09-21.md)。
