"""Inspect native Catalog identity and Arrow schema within a single fixed snapshot."""

from __future__ import annotations

import base64

import pyarrow as pa
from zippy_client import Client


def schema_info(schema: pa.Schema) -> dict[str, object]:
    """Represent Arrow fields and arbitrary metadata without lossy decoding.

    :param schema: Exact logical Arrow schema.
    :returns: Fields and base64 encoded metadata, preserving arbitrary bytes.
    :example: schema_info(pa.schema([("id", pa.int64())]))["fields"]
    """

    def metadata(value: dict[bytes, bytes] | None) -> list[dict[str, str]]:
        return [
            {
                "key": base64.b64encode(key).decode("ascii"),
                "value": base64.b64encode(item).decode("ascii"),
            }
            for key, item in sorted((value or {}).items())
        ]

    return {
        "fields": [
            {
                "name": field.name,
                "data_type": str(field.type),
                "nullable": field.nullable,
                "metadata_base64": metadata(field.metadata),
            }
            for field in schema
        ],
        "metadata_base64": metadata(schema.metadata),
    }


def inspect_table(client: Client, name: str, namespace: str) -> dict[str, object]:
    """Read fixed native metadata without scanning table rows.

    :param client: Authenticated Flight client owned by the caller.
    :param name: Exact table name.
    :param namespace: Exact Catalog namespace.
    :returns: Identity, logical schema and authoritative snapshot versions.
    :raises RuntimeError: Query metadata does not contain snapshot identity.
    """
    with client.read_table(name, namespace=namespace, snapshot=True) as table:
        info = table.info()
        info["schema"] = schema_info(table.schema())
        with table.head(0) as query:
            info["snapshots"] = query_snapshots(query.profile())
    return info


def query_snapshots(profile: dict[str, object]) -> list[dict[str, object]]:
    """Extract required native query identities from an executed profile.

    :param profile: Flight query result and metrics.
    :returns: Nonempty immutable table identities and snapshot versions.
    :raises RuntimeError: Native query snapshot metadata is absent or malformed.
    """
    metrics = profile.get("metrics")
    query = metrics.get("query") if isinstance(metrics, dict) else None
    snapshots = query.get("snapshots") if isinstance(query, dict) else None
    if (
        not isinstance(snapshots, list)
        or not snapshots
        or not all(isinstance(item, dict) for item in snapshots)
    ):
        raise RuntimeError("native query response is missing snapshot metadata")
    return snapshots
