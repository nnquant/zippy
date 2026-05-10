# BarGeneratorEngine 设计

日期：2026-05-08

## 背景

Zippy 已有 `TimeSeriesEngine`，可以按固定窗口做通用聚合，但 tick 合成 K 线不是普通
窗口聚合。它需要同时处理市场交易时段、集合竞价、累计成交量/成交额差分、跨交易日状态
重置，以及标准 K 线输出 schema。把这些语义塞进通用聚合因子会让用户 API 变长，也会让
边界情况难以审计。

因此新增一个专门的 `BarGeneratorEngine`，用于从 tick 流生成 1 分钟 K 线。后续更高周期
K 线由下游从 1 分钟 K 线继续生成。

## 目标

- 支持多标的 tick 输入，按标的独立生成 1 分钟 K 线。
- 输出标的列以及标准 K 线列：
  `instrument/dt/open/high/low/close/volume/total_turnover/num_trades/limit_up/limit_down/start_dt/close_dt`。
- 支持输入缺少 `num_trades`、`limit_up`、`limit_down` 字段时输出对应 nullable 列。
- 丢弃非交易时段无效 tick，不生成 K 线，也不污染累计成交基线。
- 支持集合竞价归属策略。
- 支持 `dt` 标签策略，避免输出 tick 毫秒级时间作为 K 线标签。
- 支持 CTP 这类累计成交字段，正确处理交易日切换、夜盘和盘中冷启动。
- API 通过 `profile` 收敛复杂策略，不把大量 policy 平铺到 engine 构造函数上。

## 非目标

- v1 只生成 1 分钟 K 线，不直接生成 5m、15m、日线等周期。
- v1 不内置完整交易所日历和节假日表；profile 可以携带交易时段，节假日停盘由上游不发
  有效 tick 或插件 profile 后续增强。
- v1 不实现外部历史回补或跨进程状态恢复；重启后按 profile 的 bootstrap 规则重新建基线。
- v1 不把 OpenCTP 插件放进 zippy 主包；主包只定义通用 profile 协议和 engine。

## 推荐 API

`BarGeneratorEngine` 是 zippy 顶层 runtime engine。它只接受 `profile`，不接受一串细碎
policy 参数：

```python
import zippy as zp
import zippy_openctp

profile = zippy_openctp.OpenCtpBarGeneratorProfile.cn_futures_1m()

session = (
    zp.Session("bar_session")
    .engine(
        zp.BarGeneratorEngine,
        name="ctp_bars_1m",
        source="ldc_ctp_ticks",
        input_schema=tick_schema,
        profile=profile,
    )
    .stream_table("ctp_bars_1m")
    .run()
)
```

主包中的自定义 profile API 放在 `zp.bar` 命名空间，不污染 `zippy` 顶层：

```python
profile = zp.bar.BarGeneratorProfile(
    frequency="1m",
    columns=zp.bar.InputColumns(
        instrument="instrument_id",
        dt="dt",
        price="last_price",
        volume="volume",
        total_turnover="turnover",
        trading_day="trading_day",
        num_trades=None,
        limit_up="upper_limit_price",
        limit_down="lower_limit_price",
    ),
    sessions=zp.bar.TradingSessions(
        timezone="Asia/Shanghai",
        regular=[
            ("09:00:00", "10:15:00"),
            ("10:30:00", "11:30:00"),
            ("13:30:00", "15:00:00"),
            ("21:00:00", "02:30:00"),
        ],
        auction=[],
    ),
    volume=zp.bar.Volume.cumulative(
        trading_day_column="trading_day",
        bootstrap="skip_first_delta",
    ),
    auction=zp.bar.Auction.drop(),
    dt_label="close_dt",
)
```

插件 profile 使用插件自己的领域命名，例如：

```python
profile = zippy_openctp.OpenCtpBarGeneratorProfile.cn_futures_1m()
profile = zippy_cnstock.CnStockBarGeneratorProfile.a_share_1m(
    auction="merge_to_first_regular_bar",
)
```

## Profile 协议

`BarGeneratorEngine` 接受两类 profile：

- `zp.bar.BarGeneratorProfile` 实例。
- 实现 `to_bar_generator_spec()` 的插件 profile 对象。

协议示意：

```python
class BarGeneratorProfileProtocol(Protocol):
    def to_bar_generator_spec(self) -> dict[str, object]: ...
```

Python 层负责把 profile 规范化为稳定的 native spec。Rust 层只消费规范化后的 spec，不依赖
具体 Python 插件类型。

规范化 spec 至少包含：

- `frequency`: 目前只允许 `"1m"`。
- `columns`: 输入列映射。
- `sessions`: 时区、普通交易时段、集合竞价时段。
- `volume`: `cumulative` 或 `delta`，以及累计模式需要的 `trading_day_column` 和
  `bootstrap`。
- `auction`: `drop`、`merge_to_first_regular_bar` 或 `emit_separate_bar`。
- `dt_label`: `close_dt` 或 `start_dt`。

## 数据语义

### 输出 schema

输出列固定且顺序稳定。第一列是标的列，列名来自 profile 的 `columns.instrument` 输出名；
其后是标准 K 线列：

```text
instrument, dt, open, high, low, close, volume, total_turnover,
num_trades, limit_up, limit_down, start_dt, close_dt
```

其中：

- `instrument` 使用输入标的列的值和类型，v1 要求为 `utf8`。
- `dt/start_dt/close_dt` 为 timezone-aware nanosecond timestamp，时区来自 profile。
- `open/high/low/close/total_turnover/limit_up/limit_down` 为 `float64`。
- `volume` v1 使用 `float64`，兼容不同市场的数量单位。
- `num_trades` 为 nullable `int64`；输入无对应字段时整列 null。
- `limit_up/limit_down` 输入无对应字段时整列 null。

### 分钟边界

普通 1 分钟 K 线使用左开右闭的市场标签习惯：

```text
start_dt = 09:30:00
close_dt = 09:31:00
```

默认 `dt_label="close_dt"`，即 `dt=09:31:00`。若 profile 指定 `start_dt`，则
`dt=09:30:00`。无论哪种策略，`dt` 都是标准分钟时间，不携带 tick 毫秒。

### 非交易时段 tick

每条 tick 先经过 profile sessions 判断：

- 普通交易时段：参与 K 线生成。
- 集合竞价时段：交给 auction policy。
- 其他时段：丢弃，不更新 K 线，不更新累计成交基线。

这样可以避免 CTP 在早上 8 点、晚上 6 点等时间发出的无用 tick 污染后续差分。

### 集合竞价

`auction` policy：

- `drop`：不输出竞价 K 线，但可以更新累计成交基线，避免开盘第一根普通 K 线误吃竞价成交。
- `merge_to_first_regular_bar`：竞价成交归入下一根普通分钟 K 线，例如 A 股 9:25 开盘价归入
  9:31 第一根分钟 K。
- `emit_separate_bar`：为竞价单独输出一根 bar，`start_dt/close_dt` 来自 profile 的竞价区间。

OpenCTP 期货默认使用 `drop`。A 股插件可以默认或显式选择 `merge_to_first_regular_bar`。

### 累计成交量和成交额

`volume.mode="cumulative"` 时，engine 维护每个标的的累计状态：

```text
instrument -> {
  trading_day,
  last_volume,
  last_total_turnover,
}
```

处理规则：

- 优先使用 `trading_day_column` 判断业务交易日。
- 交易日变化时重置该标的累计基线，不跨交易日相减。
- 盘中冷启动默认 `bootstrap="skip_first_delta"`：第一条有效 tick 只建立基线，不把盘中累计值
  塞进当前分钟。
- 完整历史 replay 可以使用 `bootstrap="from_zero"`，从交易日第一条有效 tick 开始把累计值
  视为从 0 增加。
- 如果当前累计值小于上一条累计值，且交易日未变，按数据重置处理：丢弃该 tick 的增量并
  重建基线，同时增加异常计数。v1 不主动修复为负成交。

`volume.mode="delta"` 时，输入 `volume` 和 `total_turnover` 直接视为单 tick 增量。

## 架构

### Rust

新增 `crates/zippy-engines/src/bar_generator.rs`：

- `BarGeneratorEngine` 实现 `zippy_core::Engine`。
- `BarGeneratorSpec` 表示规范化后的 profile。
- `InputColumns`、`SessionSpec`、`VolumeSpec`、`AuctionPolicy`、`DtLabelPolicy` 等为显式结构。
- 内部状态按 instrument 分离，包含 open bar、累计成交基线、待合并竞价状态。

`crates/zippy-engines/src/lib.rs` 导出 `BarGeneratorEngine`。

### Python bridge

`crates/zippy-python/src/lib.rs` 新增 PyO3 wrapper：

- `BarGeneratorEngine` pyclass。
- 构造函数参数包括 `name/input_schema/profile/target`，并沿用现有 runtime options：
  `source/master/parquet_sink/buffer_capacity/overflow_policy/archive_buffer_capacity/xfast`。
- `config()` 返回 engine 基础配置和规范化后的 profile 摘要。
- `register_source()` 和 downstream linking 支持 `BarGeneratorEngine`。

### Python facade

`python/zippy/__init__.py`：

- 顶层导出 `BarGeneratorEngine`。
- 新增 `zp.bar` 命名空间对象，承载 `BarGeneratorProfile`、`InputColumns`、`TradingSessions`、
  `Volume`、`Auction` 等自定义 profile 构造器。
- 不在 `zippy` 顶层导出 `CumulativeVolumeProfile` 这类配置类。

## 错误处理和指标

构造期校验：

- `frequency` 必须为 `"1m"`。
- 必需输入列存在且类型兼容。
- `cumulative` 模式必须有 `trading_day_column`，除非 profile 显式声明可由 sessions 推断。
- session 时段必须合法，允许跨午夜时段。
- `auction` 和 `sessions.auction` 的组合必须一致；例如选择 `emit_separate_bar` 时必须提供
  可定位竞价区间的信息。

运行期指标 v1 复用现有 `EngineMetricsDelta`，不扩展核心指标结构：

- 非交易时段丢弃计入 filtered rows。
- id 过滤或无效行计入 filtered rows。
- 累计值异常重置丢弃的增量计入 filtered rows。

错误信息遵循仓库现有风格：小写英文开头，变量值用 `[]`。

## 测试计划

Rust 测试：

- 单标的 1 分钟 OHLC、成交量、成交额生成。
- 多标的独立 open bar 和累计基线。
- 非交易时段 tick 丢弃且不更新累计基线。
- `cumulative` 跨交易日重置。
- 盘中冷启动 `skip_first_delta` 不制造假成交量。
- `from_zero` replay 模式从第一条 tick 产生增量。
- 集合竞价三种 policy。
- `dt_label` 的 `close_dt` 和 `start_dt`。
- 缺少 `num_trades/limit_up/limit_down` 时输出 nullable 列。
- 输入 schema 错误和 profile 错误。

Python 测试：

- `zp.bar.BarGeneratorProfile` 能规范化为 native spec。
- 插件式对象只要实现 `to_bar_generator_spec()` 就能被 engine 接受。
- `BarGeneratorEngine.config()` 暴露稳定 profile 摘要。
- `Session.engine(...).stream_table(...)` 可物化 bar 输出。

验证命令：

```bash
cargo test -p zippy-engines bar_generator
cargo test -p zippy-python
UV_CACHE_DIR=/tmp/uv-cache uv run maturin develop -m crates/zippy-python/Cargo.toml --uv
UV_CACHE_DIR=/tmp/uv-cache uv run pytest pytests/test_python_api.py -k bar_generator
```

如果 sandbox 环境阻塞 master/socket/shared-memory 类测试，应拆分为纯 engine 验证和 live e2e
验证，明确说明阻塞点。

## 开发流程

- 在隔离 worktree 中创建 `feature/bar-generator-engine`。
- 保留当前 `main` 上已有的 gateway 未提交改动，不纳入本功能提交。
- 先写 Rust engine 行为测试，再实现 engine。
- 再补 Python bridge 和 facade 测试。
- 验收通过后，只合并本功能相关提交回主工作区。
