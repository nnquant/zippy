"""
比较 live persisted 数据和 replay 输出是否一致。

这个脚本假设：

1. live 表已经开启 parquet persist，并且 master 能看到 persisted_files。
2. replay 表已经由 ParquetReplaySource 回放生成。

:example:

    uv run python examples/06_replay/02_replay_parity_check.py \
        --live-table live_ticks --replay-table replay_ticks --by instrument_id,dt
"""

from __future__ import annotations

import argparse

import zippy as zp


def parse_keys(value: str) -> list[str]:
    """
    解析逗号分隔的比较 key。

    :param value: 逗号分隔字段名
    :type value: str
    :returns: 字段名列表
    :rtype: list[str]
    """
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    """
    读取 persisted 和 replay 两侧数据，并输出对齐结果。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="live/replay 一致性检查")
    parser.add_argument("--uri", default="default", help="master URI")
    parser.add_argument("--live-table", required=True, help="原始 live 表")
    parser.add_argument("--replay-table", required=True, help="回放输出表")
    parser.add_argument("--by", default="instrument_id,dt", help="比较 key，逗号分隔")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_replay_parity_check")

    # persisted 侧只取已经落盘的不可变数据，避免 active segment 尚未落盘造成误差。
    expected = zp.read_table(args.live_table).scan_persisted().to_table()
    actual = zp.read_table(args.replay_table).collect()
    comparison = zp.compare_replay(expected, actual, by=parse_keys(args.by))

    print(comparison)
    if not comparison["equal"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
