"""Replay native history into a local latest kernel and persist its key-value output.

:param --namespace-id: UUID of an existing namespace; read/write/admin credentials come from env.
:example: uv run examples/06_replay/03_replay_to_reactive_latest_engine.py --namespace-id UUID
"""

from __future__ import annotations

import argparse
import time
import uuid

import pyarrow as pa
from zippy_client import SubscriptionOptions

import zippy as zp


def main() -> None:
    """Publish deterministic history and replay its exact committed prefix through Flight."""
    parser = argparse.ArgumentParser(description="原生日志驱动 latest 内核")
    parser.add_argument("--uri", default="zippy://default")
    parser.add_argument("--namespace", default="market")
    parser.add_argument("--namespace-id", type=uuid.UUID, required=True)
    parser.add_argument("--live-table", default="replay_live_ticks")
    parser.add_argument("--latest-table", default="replay_latest_ticks")
    args = parser.parse_args()
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    data = pa.table(
        {
            "instrument_id": ["IF2606", "IH2606", "IF2606", "IH2606", "IC2606"],
            "dt": [1777017600000000000 + index * 1000 for index in range(5)],
            "last_price": [3898.0, 2675.0, 3899.0, 2676.0, 6123.0],
        },
        schema=schema,
    )
    input_id, output_id = uuid.uuid4(), uuid.uuid4()
    with zp.Client(args.uri) as client:
        client.create_append_table(
            args.live_table,
            schema,
            namespace_id=args.namespace_id,
            table_id=input_id,
            request_id=uuid.uuid4(),
        )
        with zp.ComputePipeline(
            client,
            zp.NativeProcessor.identity("seed", schema),
            output=args.live_table,
            output_table_id=input_id,
            output_namespace=args.namespace,
            durability="persisted",
        ) as seed:
            seed.write(data)
        kernel = zp.NativeProcessor.latest("latest", schema, ["instrument_id"])
        client.create_key_value_table(
            args.latest_table,
            kernel.output_schema(),
            keys=["instrument_id"],
            namespace_id=args.namespace_id,
            table_id=output_id,
            request_id=uuid.uuid4(),
        )
        with zp.ComputePipeline(
            client,
            kernel,
            source=args.live_table,
            input_options=SubscriptionOptions(namespace=args.namespace, start="earliest"),
            output=args.latest_table,
            output_table_id=output_id,
            output_kind="key_value",
            output_namespace=args.namespace,
            durability="persisted",
        ) as replay:
            deadline = time.monotonic() + 10
            while replay.cursor is None or replay.cursor.next_offset < data.num_rows:
                replay.join(0)
                if time.monotonic() >= deadline:
                    raise TimeoutError("native history replay did not reach its expected offset")
                time.sleep(0.002)
        result = (
            client.read_table(args.latest_table, namespace=args.namespace)
            .sort("instrument_id")
            .collect()
        )
        assert result.column("last_price").to_pylist() == [6123.0, 3899.0, 2676.0]
        print(result)


if __name__ == "__main__":
    main()
