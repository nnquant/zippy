"""Explicit v2 mutation wire schema used by state-consumer failure fixtures."""

import pyarrow as pa


def changelog_schema(schema: pa.Schema) -> pa.Schema:
    """Prepend protocol controls to a fixture's logical fields."""
    return pa.schema(
        [
            pa.field("_zippy_kv_seq", pa.int64(), nullable=False),
            pa.field("_zippy_kv_op", pa.string(), nullable=False),
            pa.field("_zippy_kv_snapshot_version", pa.int64(), nullable=False),
            pa.field("_zippy_kv_batch_last_seq", pa.int64(), nullable=False),
            pa.field("_zippy_kv_stream_id", pa.string(), nullable=False),
            *schema,
        ]
    )
