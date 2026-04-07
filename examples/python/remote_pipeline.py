from __future__ import annotations

import argparse
from datetime import datetime, timezone
import time

import pyarrow as pa

import zippy

DEFAULT_SOURCE_ENDPOINT = "tcp://127.0.0.1:5560"
DEFAULT_OUTPUT_ENDPOINT = "tcp://127.0.0.1:5561"


def bar_schema() -> pa.Schema:
    return pa.schema(
        [
            ("symbol", pa.string()),
            ("window_start", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )


def run_upstream(endpoint: str) -> None:
    publisher = zippy.ZmqStreamPublisher(
        endpoint=endpoint,
        stream_name="bars",
        schema=bar_schema(),
    )
    actual_endpoint = publisher.last_endpoint()
    print(f"upstream publishing stream frames to {actual_endpoint}")
    time.sleep(0.5)

    first_bucket = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)
    second_bucket = datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc)

    publisher.publish(
        {
            "symbol": ["A", "B"],
            "window_start": [first_bucket, first_bucket],
            "ret_1m": [0.10, 0.20],
        }
    )
    publisher.flush()
    print("upstream flushed first bucket")

    time.sleep(0.5)

    publisher.publish(
        {
            "symbol": ["A", "B"],
            "window_start": [second_bucket, second_bucket],
            "ret_1m": [0.05, -0.02],
        }
    )
    publisher.flush()
    print("upstream flushed second bucket")

    time.sleep(0.2)
    publisher.stop()
    print("upstream stopped stream publisher")


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
        factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
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
