"""Filter and compute columns under a fixed native snapshot.

:example: python 01_fixed_query.py --table demo_ticks --min-price 11
"""

import argparse
import json

from zippy_client import Client, col


def main() -> None:
    """Query the quickstart tick schema without reading server-side files."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="zippy://default")
    parser.add_argument("--namespace", default="market")
    parser.add_argument("--table", default="demo_ticks")
    parser.add_argument("--min-price", type=float, default=11.0)
    args = parser.parse_args()
    with (
        Client(args.uri) as client,
        client.read_table(args.table, namespace=args.namespace, snapshot=True) as source,
    ):
        with (
            source.filter(col("last_price") >= args.min_price)
            .select("instrument_id", "last_price", notional=col("last_price") * col("volume"))
            .sort("last_price") as query
        ):
            result = query.collect()
            assert result.equals(query.collect(), check_metadata=True)
            print(json.dumps(result.to_pydict()))


if __name__ == "__main__":
    main()
