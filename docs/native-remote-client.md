# 原生远端客户端与交付观察

远端使用独立 `zippy-client`，主包 `zippy` 只在运行服务或本地计算时需要。控制端点用于发现
Flight；读写和订阅都通过 Flight。认证、TLS 和短期会话配置见[客户端指南](../clients/python/README.md)。
`zippy gateway` 命令、旧 Gateway 性能辅助模块和手写协议 fallback 示例已删除。

新示例位于 `examples/08_remote_flight/`，只导入独立客户端：

- `01_write_ticks.py`：向已存在的 append 表写有限模拟数据，明确使用 persisted 确认。
- `02_read_and_subscribe.py`：只读固定快照查询，再从明确日志起点观察行或表回调。

服务端启用原生存储、SQLite Catalog、native query 与 Flight/ingest，使用统一
`zippy server run --config <config.toml>` 启动。已有服务的表可先通过
`zippy table ls --uri <control-endpoint> --namespace <namespace>` 核对。

写入示例要求一张具有以下 schema 的原生 append 表；用管理员 Client 创建 namespace 和表，
为创建请求指定稳定 UUID，不在连接失败或表缺失时自动切换目录、建旧 stream：

```python
import uuid
import pyarrow as pa
from zippy_client import Client

namespace_id = uuid.UUID("00000000-0000-0000-0000-000000001001")
table_id = uuid.UUID("00000000-0000-0000-0000-000000001002")
schema = pa.schema([
    ("instrument_id", pa.string()), ("dt", pa.timestamp("ns", tz="UTC")),
    ("last_price", pa.float64()), ("volume", pa.int64()),
])
with Client("tcp://localhost:17690") as client:
    client.create_namespace("remote_demo", namespace_id=namespace_id,
                            request_id=uuid.UUID("00000000-0000-0000-0000-000000001003"))
    client.create_append_table("ticks", schema, namespace_id=namespace_id, table_id=table_id,
                              request_id=uuid.UUID("00000000-0000-0000-0000-000000001004"))
```

上面的 UUID 仅用于专用演示目录。同一创建请求重试复用原 UUID 和原参数；新演示使用新名字和
新 UUID。创建完成后只读操作无需管理员凭据。

```bash
python examples/08_remote_flight/01_write_ticks.py \
  --uri tcp://localhost:17690 --namespace remote_demo --table ticks --rows 10 --start-ns 1
python examples/08_remote_flight/02_read_and_subscribe.py \
  --uri tcp://localhost:17690 --namespace remote_demo --table ticks \
  --start earliest --mode tables --rows 10 --batch-rows 1 --order-by dt
```

`--start-ns` 使用 UTC epoch 纳秒，默认 0，生成确定性模拟数据而非当前行情。每次写入调用会
追加新数据；它不是整个脚本级的幂等重试工具。`persisted` 返回表示 manifest/Catalog 已覆盖，
不能将它与 memory 可见或仅 WAL 确认混为一谈。

读取示例中的 `--order-by` 可重复，明确排序后读取尾部五行；同一固定快照重复查询应完全一致，
结果报告包含原生表身份和快照版本。订阅 `--start earliest` 可以使用已有日志完成验收；
`latest` 等待后续写入，超时或认证失败会退出失败，不把“没有数据”报告成成功。

`--mode rows` 用于 append；`--mode tables` 适用于 append、frame 和 key-value。frame/KV 保持
原子回调，即使 `--batch-rows 1` 也不拆开一帧/一次 mutation。`--rows` 是最低观察行数，最后一个
原子单位可能使实际行数超过目标。KV 回调是增量变更，查询结果是逻辑当前状态，两者不能比较
成同一份数据；完整 KV 状态消费使用 `StateSubscription`。

报告中的计数来自本订阅句柄，停止并等待回调退出后再核对观察结果；不会读取 Gateway 全局
metrics，也不会用缺失值补零。首次回调等待时间包含连接和等待数据的时间，观察吞吐包含本机
回调成本，不表示行情事件到客户端的端到端延迟。只保留前 32 个批次大小作为样本，报告大小
不随整段订阅历史无界增长。

`scripts/check_native_client.py` 在 Linux/CPython 3.11/Arrow 23 的真实服务中执行这些脚本，
并验证独立子进程未导入 `zippy`、只读凭据拒绝写入、两种回调、完整帧、KV 和四代重启恢复。
这不替代 Windows/macOS 的实机验收；相关平台审计项继续开放。
