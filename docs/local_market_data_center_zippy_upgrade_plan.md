# local-market-data-center Zippy 升级改造方案

## 背景

Zippy 侧已经完成以下基础能力：

- `Pipeline.run_forever()`：长期运行 source 到 StreamTable 的推荐入口。
- `zippy.read_table(..., wait=True)` / `zippy.subscribe(..., wait=True)`：consumer 可以先启动，
  producer 后注册 stream。
- `subscribe_table()`：按 `pyarrow.Table` 批量回调，适合在 Python 侧用 Arrow/Polars
  做批量过滤，避免每条非命中 tick 都构造 Python Row。
- `zippy.ops.compact_table()` / `compact_tables()` / `start_compaction_worker()`：
  persisted parquet 小文件合并属于低频 ops，不应进入行情写入热路径。

本方案只描述 `~/services/local-market-data-center` 侧应如何接入和验证；Zippy 仓库不直接修改
LDC 代码。

## 目标

1. 增加一套可重复的 OpenCTP native 长跑验收脚本。
2. 收盘环境优先使用 SimNow 7x24，无法稳定连接时 fallback 到
   `zippy_openctp.OpenCtpMarketGeneratorSource`。
3. 将 LDC 订阅监控路径从逐行 Python 过滤升级为批量过滤，降低非关注合约带来的 GIL 和对象构造成本。
4. 输出统一指标，能区分 source 写入、Zippy 队列、LDC callback 处理、persist 和查询的延迟。

## 推荐文件边界

- `src/local_market_data_center/tick_watch.py`
  - 保留现有 `zippy.subscribe()` 路径作为兼容入口。
  - 新增 `subscribe_table()` 批量路径，作为默认实现。
- `src/local_market_data_center/zippy_longrun.py`
  - 新增长跑 runner，负责启动 openctp/generator source、stream table、监控和定期汇总。
- `scripts/longrun_zippy_openctp.py`
  - CLI 包装层，只解析参数并调用 `zippy_longrun.py`。
- `configs/zippy_longrun.toml`
  - 记录 SimNow、fallback sim、instrument、interval、table、persist 等参数。

## 长跑验收路径

### 模式一：SimNow 7x24

优先使用真实 OpenCTP native source：

```python
source = zippy_openctp.OpenCtpMarketDataSource(
    front=front,
    broker_id=broker_id,
    user_id=user_id,
    password=password,
    instruments=instruments,
    flow_path=flow_path,
)

(
    zippy.Pipeline("ldc_openctp_longrun")
    .source(source)
    .stream_table(
        "ldc_ctp_ticks",
        dt_column="dt",
        id_column="instrument_id",
        dt_part="%Y%m",
        persist="parquet",
    )
    .run_forever()
)
```

### 模式二：自带 sim fallback

当 SimNow 不稳定、认证失败或无法覆盖目标交易时段时，使用生成器 source：

```python
source = zippy_openctp.OpenCtpMarketGeneratorSource(
    instruments=instruments,
    interval_ms=interval_ms,
)
```

fallback 的目标不是替代真实 CTP 行为，而是验证 Zippy/LDC 链路的生命周期、吞吐、订阅和查询能力。

## tick_watch 订阅改造

### 当前问题

逐行订阅时，Zippy subscriber 会为 stream 内每条 tick 构造 `zp.Row` 并进入 Python callback。
如果 LDC 只关心少数 instrument，绝大多数非命中 tick 会在 Python 里完成对象构造、字段校验和
dict 拷贝后才被丢弃。

### 推荐实现

默认改为 `subscribe_table()`：

```python
def on_table(table: pa.Table) -> None:
    mask = pc.is_in(table["instrument_id"], value_set=pa.array(sorted(instrument_ids)))
    matched = table.filter(mask)
    if matched.num_rows == 0:
        return

    for row in matched.to_pylist():
        process_tick(row)


subscriber = zippy.subscribe_table(
    "ldc_ctp_ticks",
    callback=on_table,
    wait=True,
    timeout="30s",
    xfast=True,
)
```

关键原则：

- 先按 `instrument_id` 过滤，再做字段完整性校验。
- 非命中行不转换成 `dict` / `zp.Row`。
- 命中行才进入 LDC 原有格式化、打印、策略回调。
- 如果后续需要更低延迟，可以在 LDC 侧保留行级路径作为小流量调试模式。

## 指标与验收标准

长跑脚本每 10 秒输出窗口指标，每 5 分钟输出累计指标：

- source：收到 tick 数、写入成功数、写入错误数、source status。
- zippy：active segment generation、rollover 次数、persist 成功/失败事件、query tail 延迟。
- subscribe：queue min/avg/p95/p99/max、process min/avg/p95/max、total min/avg/p95/max。
- LDC：命中 tick 数、非命中 tick 数、instrument filter 命中率、callback 异常数。
- 系统：RSS、CPU、open fd、`/dev/shm` 或 mmap 数据目录占用。

建议验收门槛：

- 30 分钟连续运行无 `process lease expired`。
- SimNow 模式下无 subscriber 重启和 master 连接重建。
- 自带 sim 模式下 100k/s 持续 10 分钟无积压；压力测试可单独提高到 500k/s 或 1M/s。
- `queue_delay_ms` 不应出现固定 50ms 轮询长尾；偶发 OS 调度抖动需要和 WSL2/宿主负载分开记录。
- persist 打开时，query 能读取 active + sealed + persisted 数据，且 `Table.health()` 无
  `persist_failed` alert。

## 迁移顺序

1. 增加 `zippy_longrun.py`，先用 generator source 打通全链路。
2. 接入 SimNow 7x24 配置，长跑 30 分钟。
3. 将 `tick_watch.py` 默认订阅路径切到 `subscribe_table()`。
4. 保留旧 row callback 路径为 `--row-mode` 或 debug 配置。
5. 增加 nightly/手工压测命令，记录 100k/s、500k/s、1M/s 三档结果。
6. 将验收结果写入 LDC 文档，明确当前机器、是否 WSL2、CPU、instrument 数量和 sim 模式。

## 风险点

- `to_pylist()` 只能用于命中后的少量行；不要对整批 table 做 `to_pylist()`。
- Arrow compute 过滤前需要确认 `instrument_id` 列存在，否则应快速报错。
- SimNow 账号、front、broker 等敏感配置不要写入仓库。
- `persist="parquet"` 会引入后台写盘和 compaction 运维成本；纯监控场景可以默认 `persist=None`。
- compaction worker 不应该和行情 source 放在同一个高优先级热路径线程里运行。
