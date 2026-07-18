# zippy-protocol

Zippy 的 versioned Arrow Flight wire contract。crate 负责：

- canonical protobuf schema 与 checked-in Prost 类型；
- query descriptor、plan metadata 和 single-use ticket 校验；
- 稳定 `ErrorCode` 与 gRPC status/error envelope 映射；
- 有界 Flight query service、TLS/mTLS、Bearer handshake、服务端 session authority 和独立 listener 生命周期。

Catalog、DataFusion 执行和资源 authority 不属于本 crate；实现位于 `zippy-server`，通过
`FlightQueryFrontend`/`PreparedFlightQuery` 接入。Stage 6.2 的配置、协议流程和限制见
[`docs/flight-query-service.md`](../../docs/flight-query-service.md)。
