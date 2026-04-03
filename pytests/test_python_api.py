import polars as pl
import pyarrow as pa
import pytest

import zippy


def test_reactive_engine_accepts_polars_and_flushes() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A", "A"], "price": [10.0, 11.0]}))
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_pyarrow_record_batch() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["A", "A"]),
            pa.array([10.0, 11.0]),
        ],
        schema=schema,
    )

    engine.start()
    engine.write(batch)
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_pyarrow_table() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    table = pa.table(
        {
            "symbol": ["A", "A"],
            "price": [10.0, 11.0],
        },
        schema=schema,
    )

    engine.start()
    engine.write(table)
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_columnar_dict_input() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write({"symbol": ["A", "A"], "price": [10.0, 11.0]})
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_scalar_dict_input() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write({"symbol": "A", "price": 10.0})
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_row_oriented_input() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(
        [
            {"symbol": "A", "price": 10.0},
            {"symbol": "A", "price": 11.0},
        ]
    )
    engine.flush()
    engine.stop()


def test_write_after_stop_raises_runtime_error() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.stop()

    with pytest.raises(RuntimeError):
        engine.write(pl.DataFrame({"symbol": ["A"], "price": [11.0]}))


def test_reactive_engine_exposes_output_schema_before_and_after_start() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
            pa.field("ema_2", pa.float64(), nullable=False),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()

    assert engine.output_schema() == expected_output_schema

    engine.stop()


def test_reactive_engine_accepts_target_list() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=[zippy.NullPublisher(), zippy.NullPublisher()],
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.stop()


def test_reactive_engine_accepts_zmq_target() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.ZmqPublisher(endpoint="tcp://127.0.0.1:*"),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.stop()
