# zippy-client Python 连接器

轻量客户端的安装、显式 `Client` API、Flight 认证、写入确认与原生建表说明统一维护在
[客户端使用指南](../clients/python/README.md)，避免重复示例产生不同契约。

- 不再提供模块级默认连接、`zp.connect()` 或 `zp.read_table()` 兼容入口。
- Flight 发现或协议失败直接返回错误；默认写入要求 WAL，Persisted 按服务端 capability 显式选择。
- Frame、KeyValue 和实时订阅的迁移边界见[当前能力表](current-capabilities-2026-09-21.md)。
- KeyValue 消费者升级遵守[v2 增量契约](key-value-v2-migration.md)，不把缺口后的最新增量当作完整状态。
- 平台与依赖的实际验收范围以能力表为准，旧 Windows 手工记录不代表本版本已验收。
