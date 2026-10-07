# 原生表管理命令

`zippy table` 使用共享 Client，经 Flight 读取原生 Catalog。读、写及管理员凭据分别沿用
`ZIPPY_FLIGHT_AUTH_TOKEN`、`ZIPPY_FLIGHT_AUTH_WRITER_TOKEN`、
`ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN`；连接由 `--uri` 指定的控制端点发现。

```bash
uv run zippy table ls --uri zippy://default --namespace market
uv run zippy table info ticks --namespace market --json
uv run zippy table schema ticks --namespace market --json
```

`ls` 显示 namespace、表名和目录对象类型。省略 `--namespace` 时列出所有可见表。
同名表通过命名空间区分，不再与旧进程 registry 的 stream 集合混合。

`info` 在一个固定快照中读取 schema 和查询快照元数据，`snapshots` 包含不可变表 ID、
snapshot version 等原生信息；命令结束即释放租约。`schema` 显示 Arrow 字段类型、可空性和
metadata。JSON 的 `metadata_base64` 对键和值分别编码，保留非 UTF-8 元数据。

删除操作使用不可变表 ID，必须提供先前观测到的快照版本和稳定请求 ID：

```text
zippy table drop TABLE_UUID --expected-snapshot-version VERSION --request-id REQUEST_UUID --yes --json
```

版本已改变时服务拒绝删除。丢失响应后以相同表 ID、版本和请求 ID 重试。使用管理员 Flight
凭据；`--yes` 省略时会提示确认。删除是原生逻辑删除，既有固定快照仍保留其边界内数据，
物理文件回收与快照租约分开管理。

原生管理请求遇到明确可重试的资源背压时最多尝试三次，保持同一请求内容与总超时预算。
版本冲突、权限错误及其它永久错误直接返回；重试耗尽后仍由调用者保留请求 ID 决定后续动作。

旧 `stream` 命令、`--control-endpoint` 隐藏别名、按表名删除、`--keep-persisted` 和旧
Master `--token` 已删除。当前表操作没有旧 registry 回退。

实现及真实服务验收见[第 35 批记录](convergence-native-table-cli-2026-09-22.md)。
