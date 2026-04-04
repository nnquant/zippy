# Python Policy Constants Design

## Goal

把 Python API 里的离散策略参数从裸字符串切成预定义常量，减少拼写错误和类别混用。

## Scope

本次只收口三类 Python 参数：

- `window_type`
- `late_data_policy`
- `overflow_policy`

Rust 内核枚举与运行时语义不变，只改 Python 输入层、类型桩、测试和示例。

## API Shape

Python 顶层模块新增三组常量容器：

```python
zippy.WindowType.TUMBLING
zippy.LateDataPolicy.REJECT
zippy.LateDataPolicy.DROP_WITH_METRIC
zippy.OverflowPolicy.BLOCK
zippy.OverflowPolicy.REJECT
zippy.OverflowPolicy.DROP_OLDEST
```

调用方式改成：

```python
bars = zippy.TimeSeriesEngine(
    name="bar_1m",
    input_schema=schema,
    id_column="symbol",
    dt_column="dt",
    window=zippy.Duration.minutes(1),
    window_type=zippy.WindowType.TUMBLING,
    late_data_policy=zippy.LateDataPolicy.REJECT,
    factors=[zippy.AGG_FIRST(column="price", output="open")],
    target=zippy.NullPublisher(),
)

reactive = zippy.ReactiveStateEngine(
    name="tick_factors",
    input_schema=schema,
    id_column="symbol",
    factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
    target=zippy.NullPublisher(),
    overflow_policy=zippy.OverflowPolicy.BLOCK,
)
```

## Compatibility

不保留字符串兼容：

- `window_type="tumbling"` 必须报错
- `late_data_policy="reject"` 必须报错
- `overflow_policy="block"` 必须报错

同时拒绝错类别常量：

- `window_type=zippy.OverflowPolicy.BLOCK`
- `late_data_policy=zippy.WindowType.TUMBLING`
- `overflow_policy=zippy.LateDataPolicy.REJECT`

## Constant Implementation

不引入真正的 Python `Enum`，而是使用轻量常量对象：

- 常量对象持有 `_zippy_constant_kind`
- 常量对象持有 `_zippy_constant_value`
- `repr()` 返回稳定名字，例如 `WindowType.TUMBLING`

理由：

1. 约束足够强
2. 比 `Enum` 更轻
3. PyO3 绑定只需要读取属性，不需要桥接完整枚举系统

## Parsing Rules

Python 绑定层新增统一解析逻辑：

1. 检查对象是否带 `_zippy_constant_kind`
2. 校验其种类是否匹配目标参数
3. 读取 `_zippy_constant_value`
4. 再映射到 Rust 枚举或内部字符串

默认值也改成常量语义：

- `window_type` 省略时等价于 `WindowType.TUMBLING`
- `overflow_policy` 省略时等价于 `OverflowPolicy.BLOCK`

`late_data_policy` 仍然由调用者显式提供，不新增默认值。

## Config Output

`engine.config()` 继续返回稳定字符串值，而不是常量对象：

- `window_type`: `"tumbling"`
- `late_data_policy`: `"reject"`
- `overflow_policy`: `"block"`

这样日志、序列化、断言和已有调试输出都更稳定。

## Files

需要修改：

- `crates/zippy-python/src/lib.rs`
- `python/zippy/__init__.py`
- `python/zippy/_internal.pyi`
- `pytests/test_python_api.py`
- `examples/python/*.py`
- `examples/python/README.md`

## Testing

至少覆盖：

1. 新常量调用可正常构造与运行
2. 旧字符串调用直接报错
3. 错类别常量直接报错
4. `config()` 继续输出稳定字符串值
5. 示例脚本改为新写法后可通过 `py_compile`

## Non-Goals

本次不做：

- 把所有字符串配置全部常量化
- Rust 内核枚举重构
- Python `Enum`
- `CrossSectionalEngine`
- 表达式系统扩展

## Conclusion

这次改动的目标不是“语法更花”，而是把 Python API 中最容易拼错的策略参数改成强约束接口，同时保持底层实现和外部配置输出稳定。
