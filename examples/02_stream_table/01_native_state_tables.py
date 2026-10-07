"""Create and persist complete frames and key-value mutations in one native Catalog.

:example: python 01_native_state_tables.py --namespace-id <UUID>
"""

import argparse
import json
import uuid

import pyarrow as pa
from zippy_client import Client


def main() -> None:
    """Write two complete frames and a KV upsert/delete; preserve created tables."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="zippy://default")
    parser.add_argument("--namespace", default="market")
    parser.add_argument("--namespace-id", type=uuid.UUID, required=True)
    parser.add_argument("--prefix", default="demo")
    args = parser.parse_args()
    frame_name, kv_name = args.prefix + "_frames", args.prefix + "_latest"
    schema = pa.schema([("symbol", pa.string()), ("dt", pa.int64()), ("price", pa.float64())])
    with Client(args.uri) as client:
        client.create_frame_table(
            frame_name,
            schema,
            by="dt",
            namespace_id=args.namespace_id,
            table_id=uuid.uuid4(),
            request_id=uuid.uuid4(),
        )
        client.create_key_value_table(
            kv_name,
            schema,
            keys=["symbol"],
            namespace_id=args.namespace_id,
            table_id=uuid.uuid4(),
            request_id=uuid.uuid4(),
        )
        for dt in (0, 1):
            data = pa.Table.from_pydict(
                {"symbol": ["A", "B"], "dt": [dt, dt], "price": [10.0 + dt, 20.0 + dt]},
                schema=schema,
            )
            client.write_frame(frame_name, data, namespace=args.namespace, durability="persisted")
            client.upsert_key_value(kv_name, data, namespace=args.namespace, durability="persisted")
        deleted = pa.Table.from_pydict({"symbol": ["B"], "dt": [1], "price": [21.0]}, schema=schema)
        client.delete_key_value(
            kv_name, deleted.select(["symbol"]), namespace=args.namespace, durability="persisted"
        )
        with client.open_snapshot(
            [(args.namespace, frame_name), (args.namespace, kv_name)]
        ) as snap:
            frame = (
                snap.read_table(frame_name, namespace=args.namespace)
                .sort(["dt", "symbol"])
                .collect()
            )
            latest = snap.read_table(kv_name, namespace=args.namespace).collect()
            assert frame.num_rows == 4 and latest.column("symbol").to_pylist() == ["A"]
            print(json.dumps({"frame": frame.to_pydict(), "key_value": latest.to_pydict()}))


if __name__ == "__main__":
    main()
