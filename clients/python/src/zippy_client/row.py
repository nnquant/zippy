"""Subscription row values with exact nanosecond timestamp conversion.

Arrow timestamps are returned as integers in the schema's declared unit; no datetime
round-trip can truncate nanosecond precision.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping

import pyarrow as pa


class Row(Mapping[str, object]):
    """
    Represent one row delivered by a Zippy subscription.

    :param values: Row values keyed by column name.
    :type values: dict[str, object]
    """

    __slots__ = ("_values",)

    def __init__(self, values: dict[str, object]) -> None:
        self._values = values if type(values) is dict else dict(values)

    def __getitem__(self, key: str) -> object:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"zippy_client.Row({self._values!r})"

    def to_dict(self) -> dict[str, object]:
        """Return a shallow dictionary copy."""
        return dict(self._values)


def _table_rows(table: pa.Table) -> Iterator[dict[str, object]]:
    fields = list(table.schema)
    columns = [table.column(index) for index in range(table.num_columns)]
    for row_index in range(table.num_rows):
        values: dict[str, object] = {}
        for field, column in zip(fields, columns, strict=True):
            scalar = column[row_index]
            if not scalar.is_valid:
                values[field.name] = None
            elif pa.types.is_timestamp(field.type):
                values[field.name] = int(scalar.value)
            else:
                values[field.name] = scalar.as_py()
        yield values
