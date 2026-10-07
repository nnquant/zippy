# KeyValue v2：原生状态订阅

更新：2026-09-22。仅支持新契约；`StateSubscription` 的 `snapshot_loader` 和 `master`
入口已删除。主包正式依赖 `zippy-client`，两包均要求 PyArrow >=23。

## 完整状态与增量

原生 key-value 表支持 upsert/delete、WAL/Persisted 确认、重启恢复以及一致快照与游标 RPC。
`Client.snapshot_state()` 返回完整状态及其原子游标；`Client.open_log()` 返回完整 mutation，
单个 mutation 不代表完整市场。`KeyValueState` 继续复用 Rust 列式状态引擎。

增量控制列依次为：

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `_zippy_kv_seq` | int64 | 从 1 开始的连续行序号 |
| `_zippy_kv_op` | utf8 | upsert 或 delete |
| `_zippy_kv_snapshot_version` | int64 | 非空 mutation 的版本 |
| `_zippy_kv_batch_last_seq` | int64 | 本次完整 mutation 的结束序号 |
| `_zippy_kv_stream_id` | utf8 | 原生 Catalog 不可变 table ID 的十六进制字符串 |

其后为业务列。upsert 替换整行；delete 按键移除状态。空批次不推进版本。
传输可以拆分或合并批次，消费者按版本和结束序号识别原子边界，不能按回调次数判断。
输入业务 schema 不得占用 `_zippy_kv_` 前缀。

## 推荐接法

```python
from zippy import StateSubscription

# client 是调用方持有的 zippy_client.Client，已配置原生 Flight 地址及读取凭据。
with StateSubscription("quotes", client=client, namespace="market", timeout=5.0) as state:
    selected = state.select([["000001.SZ"], ["600000.SH"]])
    full = state.snapshot()  # 展示或导出时才构造全量
    metrics = state.metrics()
```

启动先附着日志、固定表身份和水位，再按同一 table ID 请求完整快照；安装快照后跳过快照
已经覆盖的完整 mutation，继续应用后续连续版本。开流和快照 RPC 均有截止，组装缓存受
`max_pending_bytes` 约束。只在完整版本提交后对外暴露状态。

断连或连续性丢失时，旧状态立即失效，`select`/`snapshot`/`sequence` 明确失败。恢复会重新
认证、按原 table ID 附着新日志、重新获取完整快照；表身份和 schema 必须一致，不按同名表
重新绑定。`metrics()` 显示 recovering/failed、重建次数和最后错误。
`wait_for_sequence()` 可等待已应用水位。指数退避可取消，重试次数有上限；权限、协议错误及
不可重试的管理员取消直接终止。`stop()` 取消读取并等待工作线程，不关闭调用方的 Client。

这是当前状态恢复：重建期间的中间事件不保证逐条交付。逐笔消费需使用日志游标和持久化事件
回放，不能用状态快照代替；游标也不等于业务处理成功或服务端 durable consumer ACK。

## 本地计算边界

`Session.snapshot_state()` 和本地计算引擎的 v2 输出仍保留；它们尚未迁入原生 Catalog/ingest。
此类进程内消费者可显式使用 `KeyValueState(snapshot)`/`apply(delta)`，自行保证先附着增量再
取得一致快照。新 `StateSubscription` 只接受原生 Client；不再为旧生产者 RPC 提供 loader 桥接。
旧 callback 订阅及 Gateway 的整体删除属于后续主干迁移，不意味着本地计算能力被删除。

## 使用约束

- 状态重建前不得 filter、截断或丢弃增量；过滤应在完整状态或验证后的增量应用之后进行。
- 普通 SQL 结果不携带原子状态游标，不能替代 `snapshot_state()`。
- 缺口、未知操作、版本跳变和缓存超限必须明确失败；不能把缺失的股票视为自动删除。
- 快照覆盖的完整旧版本允许重复交付，未完成版本中的重复或乱序片段失败。
- 连续性不等于行情新鲜度。使用方仍应检查源时间戳、心跳和可接受的延迟。

实现与恢复证据见[第十二批实施记录](convergence-native-state-2026-09-22.md)。
