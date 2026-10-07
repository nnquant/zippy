"""Create an append table, persist finite ticks, and query with explicit ordering.

:example: python 01_create_write_read.py --namespace-id <UUID> --uri <control-endpoint>
"""

import argparse
import json
import uuid

import pyarrow as pa
from zippy_client import Client


def main() -> None:
    """Create a new table in an existing namespace; never replace an existing table."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="zippy://default")
    parser.add_argument("--namespace", default="market")
    parser.add_argument("--namespace-id", type=uuid.UUID, required=True)
    parser.add_argument("--table", default="demo_ticks")
    args = parser.parse_args()
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
            ("volume", pa.int64()),
        ]
    )
    table_id = uuid.uuid4()
    with Client(args.uri) as client:
        client.create_append_table(
            args.table,
            schema,
            namespace_id=args.namespace_id,
            table_id=table_id,
            request_id=uuid.uuid4(),
        )
        with client.get_writer(
            args.table,
            namespace=args.namespace,
            schema=schema,
            durability="persisted",
            batch_size=2,
        ) as writer:
            for index in range(4):
                writer.write(
                    {
                        "instrument_id": "A" if index % 2 == 0 else "B",
                        "dt": index,
                        "last_price": 10.0 + index,
                        "volume": index + 1,
                    }
                )
        with client.read_table(args.table, namespace=args.namespace) as table:
            result = table.sort("dt").collect()
            assert result.column("dt").cast(pa.int64()).to_pylist() == [0, 1, 2, 3]
    print(
        json.dumps(
            {
                "table_id": str(table_id),
                "rows": result.num_rows,
                "prices": result.column("last_price").to_pylist(),
            }
        )
    )


if __name__ == "__main__":
    main()
