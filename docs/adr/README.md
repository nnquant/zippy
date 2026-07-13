# Zippy Architecture Decision Records

本目录记录会跨模块约束实现、迁移和运维的架构决策。ADR 一经接受，实施代码应遵守其中的
约束；若事实或前提发生变化，应新增 ADR 取代旧决策，不直接改写历史结论。

当前决策：

- [ADR-0001：单机数据库服务器边界](0001-single-node-server-boundary.md)
- [ADR-0002：SQLite WAL 作为 Catalog](0002-sqlite-wal-catalog.md)
- [ADR-0003：DataFusion 作为通用查询引擎](0003-datafusion-query-engine.md)
- [ADR-0004：Arrow Flight 作为远端数据协议](0004-arrow-flight-protocol.md)

相关总设计：

- [Zippy 单机数据库服务器完整重构方案](../single-node-server-refactor-design.md)
