"""Normalize Python writer values to Arrow batches."""

from __future__ import annotations

from typing import Any, TypeGuard


def value_to_table(value: object, schema: object | None = None) -> Any:
    """Normalize common batched Python values to a PyArrow table."""
    import pyarrow as pa

    if isinstance(value, pa.Table):
        return value.cast(schema) if schema is not None and value.schema != schema else value
    if isinstance(value, pa.RecordBatch):
        table = pa.Table.from_batches([value])
        return table.cast(schema) if schema is not None else table
    if isinstance(value, dict):
        if is_scalar_row(value):
            return pa.Table.from_pylist([value], schema=schema)
        return pa.table(value, schema=schema)
    if isinstance(value, list):
        return pa.Table.from_pylist(value, schema=schema)
    to_arrow = getattr(value, "to_arrow", None)
    if callable(to_arrow):
        return value_to_table(to_arrow(), schema)
    raise TypeError(
        "writer value must be a row dict, list[dict], pyarrow.Table, or pyarrow.RecordBatch"
    )


def is_scalar_row(value: object) -> TypeGuard[dict[str, object]]:
    """Return whether a dictionary represents one scalar row."""
    if not isinstance(value, dict):
        return False
    return not any(isinstance(item, (list, tuple)) for item in value.values())
