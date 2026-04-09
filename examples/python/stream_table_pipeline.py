from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow as pa

import zippy

STREAM_ENDPOINT = "tcp://127.0.0.1:5562"
ARCHIVE_ROOT = Path("/tmp/zippy-stream-table-demo")


def tick_schema() -> pa.Schema:
    return pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )


def main() -> None:
    schema = tick_schema()
    run_archive_root = ARCHIVE_ROOT / datetime.now(timezone.utc).strftime(
        "run-%Y%m%dT%H%M%S"
    )
    engine = zippy.StreamTableEngine(
        name="tick_table",
        input_schema=schema,
        target=zippy.ZmqStreamPublisher(
            endpoint=STREAM_ENDPOINT,
            stream_name="tick_table",
            schema=schema,
        ),
        sink=zippy.ParquetSink(
            path=str(run_archive_root),
            rotation="none",
            write_input=False,
            write_output=True,
        ),
        buffer_capacity=64,
        overflow_policy=zippy.OverflowPolicy.BLOCK,
        archive_buffer_capacity=16,
    )

    first_batch = pl.DataFrame(
        {
            "symbol": ["IF2606", "IH2606"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 0, 500000, tzinfo=timezone.utc),
            ],
            "price": [3898.2, 2675.4],
            "volume": [12.0, 8.0],
        }
    )
    second_batch = pl.DataFrame(
        {
            "symbol": ["IF2606", "IH2606"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, 500000, tzinfo=timezone.utc),
            ],
            "price": [3898.6, 2675.1],
            "volume": [6.0, 10.0],
        }
    )

    print("stream table demo: raw stream ingress for data-center style fanout")
    print("stream table demo: input batches are passed through unchanged")
    print("stream table demo: output batches are archived once to sink and published to target")
    print("config:", engine.config())
    print("status before start:", engine.status())

    engine.start()
    print("status after start:", engine.status())

    engine.write(first_batch)
    print("wrote first batch rows=", first_batch.height)
    engine.write(second_batch)
    print("wrote second batch rows=", second_batch.height)

    engine.flush()
    print("status after flush:", engine.status())
    print("metrics after flush:", engine.metrics())

    engine.stop()
    print("status after stop:", engine.status())
    print("metrics after stop:", engine.metrics())
    print("published stream to", STREAM_ENDPOINT)
    print("archived parquet files under", run_archive_root)

    for path in sorted(run_archive_root.rglob("*.parquet")):
        print(path)


if __name__ == "__main__":
    main()
