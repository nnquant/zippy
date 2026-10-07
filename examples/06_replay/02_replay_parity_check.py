"""Compare two native tables under one explicit fixed snapshot.

:param --by: Comma-separated deterministic ordering keys.
:example: uv run examples/06_replay/02_replay_parity_check.py --live-table a --replay-table b
"""

from __future__ import annotations

import argparse

import zippy as zp


def main() -> None:
    """Compare a fixed original/result pair without accessing server filesystem paths."""
    parser = argparse.ArgumentParser(description="原生 live/replay 一致性检查")
    parser.add_argument("--uri", default="zippy://default")
    parser.add_argument("--namespace", default="market")
    parser.add_argument("--live-table", required=True)
    parser.add_argument("--replay-table", required=True)
    parser.add_argument("--by", default="instrument_id,dt")
    args = parser.parse_args()
    names = list(dict.fromkeys([args.live_table, args.replay_table]))
    with zp.Client(args.uri) as client:
        with client.open_snapshot([(args.namespace, name) for name in names]) as snapshot:
            expected = snapshot.read_table(args.live_table, namespace=args.namespace).collect()
            actual = snapshot.read_table(args.replay_table, namespace=args.namespace).collect()
        keys = [key.strip() for key in args.by.split(",") if key.strip()]
        comparison = zp.compare_replay(expected, actual, by=keys or None)
        print(comparison)
        if not comparison["equal"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
