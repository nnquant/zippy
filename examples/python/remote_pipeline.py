from __future__ import annotations

import argparse
from datetime import datetime, timezone
import time

import pyarrow as pa

import zippy

DEFAULT_SOURCE_ENDPOINT = "tcp://127.0.0.1:5560"
DEFAULT_OUTPUT_ENDPOINT = "tcp://127.0.0.1:5561"


def tick_schema() -> pa.Schema:
    return pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )


def bar_schema() -> pa.Schema:
    schema_probe = zippy.TimeSeriesEngine(
        name="bars_probe",
        input_schema=tick_schema(),
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="price", output="close")],
        target=zippy.NullPublisher(),
    )
    return schema_probe.output_schema()


def run_upstream(endpoint: str) -> None:
    stream_target = zippy.ZmqStreamPublisher(
        endpoint=endpoint,
        stream_name="bars",
        schema=bar_schema(),
    )
    engine = zippy.TimeSeriesEngine(
        name="bars_upstream",
        input_schema=tick_schema(),
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="price", output="close")],
        target=stream_target,
    )
    actual_endpoint = stream_target.last_endpoint()
    print(f"upstream engine publishing stream frames to {actual_endpoint}")
    engine.start()
    time.sleep(0.5)

    first_bucket = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)
    second_bucket = datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc)

    engine.write(
        {
            "symbol": ["A", "B"],
            "dt": [first_bucket, first_bucket],
            "price": [10.0, 20.0],
        }
    )
    engine.flush()
    print("upstream flushed first bucket")

    time.sleep(0.5)

    engine.write(
        {
            "symbol": ["A", "B"],
            "dt": [second_bucket, second_bucket],
            "price": [11.0, 19.0],
        }
    )
    engine.flush()
    print("upstream flushed second bucket")

    time.sleep(0.2)
    engine.stop()
    print("upstream stopped timeseries engine")


def run_downstream(source_endpoint: str, output_endpoint: str, mode: object) -> None:
    source = zippy.ZmqSource(
        endpoint=source_endpoint,
        expected_schema=bar_schema(),
        mode=mode,
    )
    engine = zippy.CrossSectionalEngine(
        name="remote_cs",
        source=source,
        input_schema=bar_schema(),
        id_column="symbol",
        dt_column="window_start",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="close", output="close_rank")],
        target=zippy.ZmqPublisher(endpoint=output_endpoint),
    )

    engine.start()
    print(f"downstream consuming {source_endpoint} and publishing ranks to {output_endpoint}")

    if mode is zippy.SourceMode.CONSUMER:
        time.sleep(2.0)
        engine.flush()
        print("downstream consumer mode: local flush executed")
        engine.stop()
        print("downstream consumer mode: local stop executed")
        return

    deadline = time.time() + 10.0
    while time.time() < deadline:
        status = engine.status()
        if status == "stopped":
            break
        if status == "failed":
            raise RuntimeError(f"downstream engine failed metrics={engine.metrics()}")
        time.sleep(0.1)

    print(f"downstream finished status={engine.status()} metrics={engine.metrics()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="run a remote ZmqSource pipeline demo")
    parser.add_argument("role", choices=["upstream", "downstream"])
    parser.add_argument("--source-endpoint", default=DEFAULT_SOURCE_ENDPOINT)
    parser.add_argument("--output-endpoint", default=DEFAULT_OUTPUT_ENDPOINT)
    parser.add_argument(
        "--mode",
        choices=["pipeline", "consumer"],
        default="pipeline",
    )
    args = parser.parse_args()

    mode = (
        zippy.SourceMode.PIPELINE
        if args.mode == "pipeline"
        else zippy.SourceMode.CONSUMER
    )

    if args.role == "upstream":
        run_upstream(args.source_endpoint)
    else:
        run_downstream(args.source_endpoint, args.output_endpoint, mode)


if __name__ == "__main__":
    main()
