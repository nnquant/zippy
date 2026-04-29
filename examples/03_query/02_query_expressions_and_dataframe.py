"""
演示 zp.col 表达式过滤、选择列，以及转换为 pandas/polars。

:example:

    uv run python examples/03_query/02_query_expressions_and_dataframe.py \
        --table demo_ticks --instrument IF2606
"""

from __future__ import annotations

import argparse

import zippy as zp


def main() -> None:
    """
    对表做表达式查询，并展示常见 DataFrame 转换方式。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Zippy 查询表达式示例")
    parser.add_argument("--uri", default="default", help="master URI")
    parser.add_argument("--table", default="demo_ticks", help="表名")
    parser.add_argument("--instrument", action="append", default=[], help="过滤合约")
    parser.add_argument("--min-price", type=float, default=0.0, help="最低价格过滤")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_query_expressions")
    query = zp.read_table(args.table)

    predicate = zp.col("last_price") >= args.min_price
    if args.instrument:
        predicate = predicate & zp.col("instrument_id").is_in(args.instrument)

    result = (
        query.where(predicate)
        .select(
            [
                "instrument_id",
                "dt",
                "last_price",
                (zp.col("last_price") * zp.col("volume")).alias("notional"),
            ]
        )
        .collect()
    )

    print("pyarrow.Table:")
    print(result)

    try:
        print("pandas.DataFrame:")
        print(query.where(predicate).to_pandas())
    except ImportError:
        print("pandas 未安装，跳过 pandas.DataFrame 转换示例")

    print("polars.DataFrame:")
    print(query.where(predicate).to_polars())


if __name__ == "__main__":
    main()
