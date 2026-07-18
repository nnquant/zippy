# ADR-0004：Arrow Flight 作为远端数据协议

- 日期：2026-07-13
- 状态：已接受，Stage 6.2 查询安全边界已实施
- 决策所有者：Zippy core maintainers

## 背景

现有 Gateway 自定义 framing 已能完成远端 write、subscribe 和 collect，但查询、取消、schema、
批量 Arrow 数据和跨语言客户端需要持续扩展协议。继续维护自定义数据面会增加复制、兼容和工具
成本。

## 决策

远端数据协议使用 Arrow Flight，并按职责映射：

| Flight RPC | Zippy 语义 |
| --- | --- |
| `GetFlightInfo` | 解析查询并生成 ticket/schema |
| `DoGet` | 流式查询结果 |
| `DoPut` | Arrow batch ingest |
| `DoExchange` | subscription、ack 和 backpressure |
| `DoAction` | cancel、snapshot 和受控管理动作 |
| Flight SQL metadata | namespace、table 和 schema discovery |

旧 Gateway endpoint 在迁移期作为 compatibility adapter，内部调用统一的 QueryService、
IngestService 和 SubscriptionService，不保留第二套业务实现。

所有业务错误映射到稳定 `ErrorCode`，客户端不得依赖 message 文本解析。协议提供版本协商和
capability discovery。非 loopback endpoint 必须启用 TLS；query/ingest/admin 分开授权。

Flight frontend 必须在 QueryService 已拥有 query lifecycle 后实施，禁止先用 Flight 包装 legacy
全量 collect 形成临时协议。

## 结果

截至 2026-07-18，`GetFlightInfo`/`DoGet`、version 1 query/ticket/error envelope、capability
discovery、session-bound single-use ticket、有界 transport 和 Server 生命周期接线已经实现。TLS、可选
mTLS、Bearer Handshake、服务端签发 session、凭据文件约束和重启证书轮换也已完成；非 loopback
endpoint 只有同时配置 TLS 与认证才会 bind。`DoPut`、`DoExchange`、Flight SQL、cancel/admin 和
Python client cutover 仍待后续阶段。

正向结果：

- Arrow batch 可以跨语言、流式、低复制传输。
- 查询 ticket、取消和 schema discovery 有标准协议基础。
- Python 和 Rust 客户端共享一致的服务端查询行为。

代价与约束：

- Flight、gRPC、TLS 和认证增加部署与测试面。
- durable subscription 的 offset/ack 仍是 Zippy 业务协议，Flight 不自动提供其正确性。
- compatibility adapter 必须有删除条件，不能长期演化为第二个 frontend。

## 替代方案

继续扩展自定义 Gateway framing：迁移成本低，但跨语言支持、错误映射、流控和工具生态成本持续
增加。

HTTP/JSON + Arrow IPC：适合管理面和小请求，不适合作为统一的双向流式数据协议。

直接采用数据库 wire protocol：SQL 工具兼容性更强，但难以自然表达 Zippy 的 Arrow batch ingest
和 durable subscription。

## 重新评估条件

若 Flight 在目标平台无法满足认证、双向流控或尾延迟门槛，可在 Phase 6 前重新评估 transport。
业务服务接口和稳定错误模型不得随 transport 一起推翻。
