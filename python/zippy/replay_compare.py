"""Compare explicit Arrow replay results without opening implicit connections."""

from __future__ import annotations

from collections import Counter

import pyarrow as pa


def compare_replay(
    left: pa.Table,
    right: pa.Table,
    *,
    by: str | list[str] | tuple[str, ...] | None = None,
) -> dict[str, object]:
    """Compare two Arrow results, including duplicate row multiplicities.

    :param left: Expected result collected under the caller's explicit snapshot.
    :param right: Actual replay result collected under that same snapshot.
    :param by: Optional sort keys; absent keys preserve input row order.
    :returns: Schema/data equality and missing/extra row counts, ignoring schema metadata.
    :raises TypeError: An input is not an Arrow table.
    :raises ValueError: A sort key does not exist in either input.
    :example: compare_replay(pa.table({"x": [1]}), pa.table({"x": [1]}))["equal"]
    """
    if not isinstance(left, pa.Table) or not isinstance(right, pa.Table):
        raise TypeError("replay comparison requires explicit Arrow tables")
    if by is not None:
        keys = [by] if isinstance(by, str) else list(by)
        if not keys or any(
            key not in left.column_names or key not in right.column_names for key in keys
        ):
            raise ValueError("replay comparison requires nonempty keys present in both inputs")
        left = left.sort_by([(key, "ascending") for key in keys])
        right = right.sort_by([(key, "ascending") for key in keys])
    schema_equal = left.schema.equals(right.schema, check_metadata=False)
    equal = schema_equal and left.equals(right, check_metadata=False)
    if equal:
        missing, extra = 0, 0
    elif not schema_equal:
        missing, extra = left.num_rows, right.num_rows
    else:
        schema = left.schema.remove_metadata()
        left_rows, right_rows = _row_counts(left, schema), _row_counts(right, schema)
        missing = sum((left_rows - right_rows).values())
        extra = sum((right_rows - left_rows).values())
    return {
        "equal": equal,
        "left_rows": left.num_rows,
        "right_rows": right.num_rows,
        "schema_equal": schema_equal,
        "missing_rows": missing,
        "extra_rows": extra,
        "mismatch_rows": max(missing, extra),
    }


def _row_counts(table: pa.Table, schema: pa.Schema) -> Counter[bytes]:
    # Canonical single-row IPC preserves nanoseconds, nulls and duplicate multiplicity.
    # Rebuilding each row removes chunk/slice offsets; this is a diagnostic comparison.
    rows: Counter[bytes] = Counter()
    for index in range(table.num_rows):
        arrays = [
            pa.array([column[index]], type=field.type)
            for column, field in zip(table.columns, schema)
        ]
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))
        rows[sink.getvalue().to_pybytes()] += 1
    return rows
