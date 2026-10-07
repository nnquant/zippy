"""Wait for Catalog table creation before reading a fixed native snapshot.

:example: python 01_wait_for_table.py --table demo_ticks --timeout 10
"""

import argparse
import json

from zippy_client import Client


def main() -> None:
    """Wait for table existence; this does not imply that a producer has written rows."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="zippy://default")
    parser.add_argument("--namespace", default="market")
    parser.add_argument("--table", required=True)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    with (
        Client(args.uri) as client,
        client.read_table(
            args.table, namespace=args.namespace, wait=True, timeout=args.timeout, snapshot=True
        ) as table,
    ):
        result = table.collect()
        print(
            json.dumps(
                {"table": args.table, "rows": result.num_rows, "columns": result.column_names}
            )
        )


if __name__ == "__main__":
    main()
