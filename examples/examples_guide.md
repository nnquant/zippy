# Zippy 原生示例指南

所有表操作都使用显式 `Client`，经 Flight 访问同一个原生 Catalog。主包只承载服务器、本地
计算和回放；不再提供 `connect/master/ops/Table/Session/Pipeline` 或模块级读写订阅函数。
旧示例已删除，不支持旧 API、旧配置名称和 Gateway 数据路径。

## 服务与认证

先按[Flight 服务配置](../docs/flight-query-service.md)启用 native storage、SQLite Catalog、
native query 和 Flight ingest，再用 `uv run zippy server run --config <config.toml>` 启动。
当前服务器默认权威尚未收敛，不能省略原生配置后假定示例可用。
读取使用 read 凭据，建表使用 administrator 凭据，写入使用 writer 凭据；远端还需要 TLS。
独立客户端从相应 `ZIPPY_FLIGHT_AUTH_*` 环境变量读取凭据，不写入示例源码。

创建专用命名空间，保存其 UUID；后续 `--namespace` 与 `--namespace-id` 必须对应：

```python
import uuid
from zippy_client import Client

namespace_id, request_id = uuid.uuid4(), uuid.uuid4()
with Client("zippy://default") as client:
    client.create_namespace("market", namespace_id=namespace_id, request_id=request_id)
print(namespace_id)
```

这些示例使用新表名，遇到同名表明确失败，不会删除或覆盖既有数据。管理请求重试需复用原
UUID 和原参数；重新运行脚本会生成新请求，不等于请求级幂等重试。

## 运行顺序

将下列 `<namespace-uuid>` 替换为上一步结果；默认控制端点为 `zippy://default`，可用 `--uri`
覆盖。默认 namespace 为 `market`，输入时间均为明确的 UTC epoch 纳秒。

```bash
uv run examples/01_quickstart/01_create_write_read.py --namespace-id <namespace-uuid>
uv run examples/03_query/01_fixed_query.py --table demo_ticks
uv run examples/04_subscribe/01_native_callbacks.py --table demo_ticks --mode rows --rows 4
uv run examples/04_subscribe/01_native_callbacks.py --table demo_ticks --mode tables --rows 4
uv run examples/02_stream_table/01_native_state_tables.py --namespace-id <namespace-uuid>
uv run examples/05_engines/01_native_local_chain.py
uv run examples/05_engines/02_native_table_compute.py --namespace-id <namespace-uuid> \
  --source demo_ticks --output demo_latest --kind latest --through-offset 4
uv run examples/05_engines/02_native_table_compute.py --namespace-id <namespace-uuid> \
  --source demo_ticks --output demo_bars --kind timeseries --window-ns 2 --through-offset 4
uv run examples/05_engines/02_native_table_compute.py --namespace-id <namespace-uuid> \
  --source demo_ticks --output demo_ranks --kind cross_sectional --window-ns 2 --through-offset 4
uv run examples/07_ops/01_wait_for_table.py --table demo_ticks --timeout 10
```

- quickstart 有限写入 4 行，使用 persisted 确认；查询显式按 dt 排序。
- 查询示例使用 `snapshot=True`，重复 collect 保持固定快照；普通 `read_table` 默认每次查询
  使用新边界。需要跨表一致性时使用 `Client.open_snapshot`。
- 回调示例明确选择 earliest/latest。rows 模式用于 append，tables 模式保持完整 frame/KV
  mutation；目标行数是最低值，不切片破坏原子交付。超时和后台异常退出失败。
- 状态表示例写两帧、upsert 后 delete，并读取逻辑 KV 当前状态；KV 日志增量不等于状态快照。
- 原生本地 chain 不需要服务器。表驱动计算通过 `ComputePipeline` 写 append/frame/KV，输出
  绑定不可变表 UUID，停止时刷出窗口尾部。时序示例将整数 volume 显式转为 float64 后聚合。
- `--through-offset` 是原生日志的绝对游标阈值，处理到至少该位置后停止；完整批次可能超过
  阈值。输入应是专用有限数据，不能把该参数当作精确截断工具。不指定时持续处理直到信号退出。
- 计算进程状态尚无持久算子 checkpoint；重启计算进程需要显式重放重建，不能把持久 ACK
  或输出恢复误当作内核状态自动恢复。
- 等待示例只承诺表已创建，不承诺已有行；可先启动它，再由另一个进程创建目标表。

## 回放、远端与运维

`06_replay/` 提供有限 Parquet 源、同一固定快照内的回放结果比较，以及日志驱动 latest 计算。
`compare_replay` 只接收已经 collect 的 Arrow Table；排序键由调用方指定，不再隐式连接表名。
`08_remote_flight/` 仅依赖 `zippy-client`，见[远端指南](../docs/native-remote-client.md)。

表列表、元数据、schema 和删除统一使用 `zippy table`，预览使用原生 WebUI。删除必须携带
不可变 table ID、观测到的 snapshot version 和稳定 request ID；不再使用旧 `drop_persisted`
开关。旧 SHM 私有扫描吞吐、Gateway 指标及 registry 健康推断示例已退出；服务真实故障必须
根据当前组件状态和请求错误判断，不能从表存在推断写入健康。

`scripts/check_native_client.py` 实际运行上述示例，并在每次服务重启后核对数据、三类计算
结果与回调。仓库测试仍保留原生内核细节验证。Linux 本机验收不替代跨平台实机结果。
