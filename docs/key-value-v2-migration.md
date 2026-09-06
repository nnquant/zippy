# KeyValue v2：下游迁移

本次直接切换新契约，没有“每批全量”兼容开关。需一起更新生产者、Python 扩展和消费者，
重新发布表描述符；不能把旧进程的增量流接到新状态对象上。

## 现在收到什么

| 接口 | 新行为 |
| --- | --- |
| 原始 append 订阅 | 完整保留事件与顺序，不按股票合并逐笔 |
| `KeyValueTableMaterializer` 主输出 | 本批更新的键，每个键携带最新整行；同批同键只保留最后值 |
| `subscribe()` / `subscribe_table()` 订阅 KeyValue 名称 | 路由到增量日志，保留全部协议字段，不再自动发完整快照 |
| KeyValue / ReactiveLatest 的 flush/stop | 不再附送完整状态；flush 仍刷新内部日志与描述符 |
| `Session.snapshot_state(name)` | 显式获取本 Session 拥有的状态及一致游标 |
| `KeyValueState.select(keys)` | 只读取指定键，不构造整个市场快照 |
| `KeyValueState.snapshot()` | 显式构造完整状态，适合低频展示、导出，不要每个 tick 调用 |

增量前五列固定为：

```
_zippy_kv_seq                 int64   从 1 开始的连续行序号
_zippy_kv_op                  utf8    当前实现为 upsert
_zippy_kv_snapshot_version    int64   每个非空更新批次递增一次
_zippy_kv_batch_last_seq      int64   本原子批次的最后一个序号
_zippy_kv_stream_id           utf8    本次生产者实例的身份
```

其后是业务列。`upsert` 是替换该键的整行，不是仅更新非空字段。空批次不推进版本。
不支持 delete；消费者遇到未知操作必须失败，不能当成 upsert。
批次可能因传输而拆分或合并：以版本和 `batch_last_seq` 判断完整性，不能以回调次数判断。
输入业务 schema 不能占用 `_zippy_kv_` 前缀。

## 推荐接法

```python
import zippy as zp

# producer_session 是发布 quotes 的 Session。
subscription = zp.StateSubscription(
    "quotes",
    snapshot_loader=lambda: producer_session.snapshot_state("quotes"),
    master=master,
    max_pending_bytes=64 * 1024 * 1024,
).start()

try:
    # 组合键按 by 声明顺序给出。缺少键会明确报错，不返回伪造行情。
    selected = subscription.select([["000001.SZ"], ["600000.SH"]])
    # 展示/导出时才取全量：
    full = subscription.snapshot()
finally:
    subscription.stop()
```

启动顺序由 `StateSubscription` 实现：先挂增量读者并等待 ready → 有界缓存增量 → 调用
快照加载器 → 安装快照并跳过已覆盖序号 → 连续应用后续完整版本。半个版本不可见。

跨进程/远端场景下，`snapshot_loader` 通过你的控制 RPC 调用生产者的
`Session.snapshot_state()`，传回 Arrow 数据并保留 schema metadata。加载器自身应有超时。
这次没有新增跨机器快照 RPC 服务；不能拿普通 `read_table().collect()` 代替它：普通查询
没有与数据原子绑定的增量游标，描述符中的异步版本号也不能代替这个证明。

如果你已有自己的订阅/队列，可以直接使用 `KeyValueState(snapshot)` 和 `state.apply(delta)`。
本地和远端增量应用在 Rust 内按列进行，不会先把整批变成 Python 行字典。
轻量 `zippy_client` 已同步 KeyValue 路由与协议列保留，但它仍是传输客户端，不内置这套
状态引擎；可把它的 Arrow 批次接入 `zippy.KeyValueState`，或按上述协议维护自己的状态。

## 必须改变的使用习惯

- 不再把每次回调直接覆盖成“整个市场”。增量里没有出现的股票，保留原状态。
- 不在状态重建前使用 `filter`、`instrument_ids`、`count` 或允许丢数据的订阅选项。
  过滤应发生在完整状态或已验证的增量应用之后。传输分片可以，但不能丢行。
- 序号缺口、版本跳变、实例变化、缓存超限，均停止使用当前状态并重新 bootstrap。
  `StateSubscription` 还会拒绝把发生重连的传输当作连续状态；不会静默补接尾部。
- 快照覆盖的完整旧版本可以重复投递；尚未完成版本中的乱序/重复片段直接失败。
- 原始逐笔缺口不能用 KeyValue 快照修补，须从持久化事件重放。
- 连续性校验不等于行情新鲜度校验。交易端仍须检查源时间戳、心跳和可接受的延迟，不能
  仅因本地状态可读就认定生产者仍在更新。

KeyValue 仍是进程内状态物化，不提供 KeyValue Parquet 持久化或 delete。生产者重启后，
应先按业务需求重建完整状态，再对外提供初始化快照。原生 append 的持久化回收与这一点不同。

查询用 SHM 最新状态视图仍维护：已有键的定长值更新走原位写；新增键、非键字符串值或
容量变化可能重建该视图。这不是旧全量订阅模式，但这些边界仍可能有 O(N) 成本，需纳入压测。
