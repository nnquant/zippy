"""Write deterministic sample ticks to an existing native append table.

:param --uri: Control endpoint used for Flight discovery.
:param --namespace: Exact Catalog namespace, defaulting to default.
:param --table: Existing table with the schema returned by build_schema.
:raises RuntimeError: The server rejects a write; no compatibility path is attempted.
:example: python 01_write_ticks.py --uri tcp://localhost:17690 --table demo_ticks
"""

from __future__ import annotations

import argparse
import json

import pyarrow as pa
from zippy_client import Client


def build_schema() -> pa.Schema:
    """Return the exact synthetic tick schema.

    :returns: Instrument, UTC nanoseconds, price and volume fields.
    """
    return pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
            ("volume", pa.int64()),
        ]
    )


def main() -> None:
    """Write one finite batch sequence using explicit persisted durability.

    :raises ValueError: Row count or batch size is not positive.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", required=True)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--table", required=True)
    parser.add_argument("--rows", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--start-ns", type=int, default=0)
    args = parser.parse_args()
    if args.rows <= 0 or args.batch_size <= 0:
        parser.error("rows and batch-size must be positive")
    with Client(args.uri) as client:
        with client.get_writer(
            args.table,
            namespace=args.namespace,
            schema=build_schema(),
            batch_size=args.batch_size,
            durability="persisted",
        ) as writer:
            for index in range(args.rows):
                writer.write(
                    {
                        "instrument_id": "IF2606",
                        "dt": args.start_ns + index,
                        "last_price": 4100.0 + index * 0.2,
                        "volume": index + 1,
                    }
                )
    print(
        json.dumps(
            {
                "namespace": args.namespace,
                "table": args.table,
                "rows": args.rows,
                "durability": "persisted",
            }
        )
    )


if __name__ == "__main__":
    main()
