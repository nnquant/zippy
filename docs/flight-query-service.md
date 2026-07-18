# Arrow Flight 查询服务

## 定位

Stage 6.2 在标准 Arrow Flight 查询入口上增加 TLS、可选 mTLS、Bearer 引导认证和服务端 session
authority。Flight 仍只负责协议、listener、ticket 和 Arrow batch 传输，不复制 Catalog、DataFusion
或资源调度逻辑。一次查询仍由 Server 固定
Catalog revision、table snapshot/schema version，并由原有 QueryService 负责内存、任务、取消和结果
batch permit。

当前适用场景：

- 在单机、局域网或受控远程网络上运行加密的 Flight 查询链路；
- 为 Rust/Python connector 的后续迁移提供稳定 wire contract；
- 在不启动 legacy Gateway 时运行 persisted native SQL 查询。

当前不适用场景：公网裸露、写入、实时订阅、Flight SQL 元数据发现、细粒度 RBAC 或通用 BI 客户端
直连。管理员和写入权限不会复用本阶段的 query bearer。

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
session_ttl_ms = 3600000
max_sessions = 1024
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
ZIPPY_FLIGHT_AUTH_SESSION_TTL_MS
ZIPPY_FLIGHT_AUTH_MAX_SESSIONS
```

默认值是 disabled、protocol version 1、2 MiB message、64 个 pending ticket、1 小时 session TTL 和
1024 个 active session。环境变量中的凭据文件路径必须是绝对路径；TOML 相对路径按配置文件目录解析。

## 查询协议

1. 客户端调用 `DoAction("zippy.capabilities.v1")` 读取版本、消息上限、认证要求和 capability。
2. 认证模式下，客户端以 `Authorization: Bearer <bootstrap-token>` 调用 Flight `Handshake`，发送
   version 1 `SessionOpenRequest`。Server 返回随机 `session_id`、`zfs1_...` session bearer 和固定
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

## 背压和预算

Flight encoder 的目标 batch 大小会预留 gRPC/protobuf 开销；实际 header、body、metadata 和固定开销之和
必须小于 `max_message_bytes`。每条待写消息还需取得全局 transport byte permit。上一批编码数据在客户端
消费前保留 QueryService result permit，因此慢客户端只会施加背压，不会触发无界预取。

`max_pending_tickets` 同时计算正在 prepare 和已经发布但未消费的 ticket。后台每 100 ms 清理过期
ticket；清理和 shutdown 会在 registry 锁外 drop prepared query，避免资源释放阻塞其他请求。

session registry 同样有 `max_sessions` 硬上限和固定 TTL，不做滑动续期。session 过期时，仍未消费的
ticket 会被撤销并释放 prepared query。已经通过 `DoGet` 完成认证的运行中查询不会被 session TTL 中途
终止，仍受 query deadline、客户端断开和 Server shutdown 约束。

## 凭据与 TLS 约束

- bootstrap token 必须是 32 到 4096 个可见 ASCII 字节。TOML 不接受明文 `token` 字段；只能使用
  `token_file` 或环境变量 `ZIPPY_FLIGHT_AUTH_TOKEN`。
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
4. 等待 schema version 4 ready record，再核对 `flight.certificate_sha256`；
5. 客户端信任库切换到新 CA 后建立新 session。旧 session 随旧 listener 退出，不跨进程复用。

自动化测试覆盖旧 CA 在重启轮换后失败、新 CA 成功，以及 mTLS 无 client identity 失败。进程内无损
证书热轮换需要双 identity overlap、失败回退和连接 drain，留到后续独立阶段。

## 错误契约

业务错误使用稳定 `ErrorCode` 映射到 gRPC status，并在 status details 与
`x-zippy-error-bin` 中携带同一份 versioned protobuf envelope。客户端应解析结构化 envelope，不依赖
message 文本。两份 envelope 不一致时客户端必须 fail closed。

未实现 RPC（包括 `DoPut`、`DoExchange`、`ListFlights` 和 `GetSchema`）返回结构化
`UNSUPPORTED_PLAN`，不会静默降级到 legacy collect。

## Readiness 与关停

Server ready record schema version 4 包含 Flight enabled/endpoint/protocol、TLS/auth 状态、客户端证书
要求和非敏感证书指纹。进程 readiness 需要
`flight_configured`、`flight_listener_ready` 和 `flight_protocol_ready` 同时满足。listener 异常退出会撤销
动态 readiness。

关停顺序为：关闭 Flight admission、释放 pending ticket、停止 Flight listener，再关闭 Gateway、drain
legacy process 与 Catalog shadow、停止 master listener，最后关闭 native storage/Catalog。所有阶段共享
Server 的绝对 shutdown deadline；若 Flight listener 超时，Server 返回不完整结果并让已发出 shutdown 的
listener 在后台继续清理，不会用无界 `join` 突破调用方 deadline。

## 后续阶段

1. Stage 6.3：query cancel/admin action 与 Flight SQL Catalog metadata。
2. Stage 6.4：`DoPut` ingest，并接入后续 durable producer epoch/sequence。
3. Stage 6.5：Python/Rust connector 切换 Flight query，legacy Gateway 变为 compatibility adapter。
4. Phase 7：`DoExchange` durable subscription、offset/ack 和 replay。
