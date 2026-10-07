"""Inspect and retire native Catalog tables through the shared Flight client."""

from __future__ import annotations

from typing import Any
import uuid

import click
from zippy_client import Client, ZippyClientError

from .native_inspection import inspect_table, schema_info
from .cli_common import DEFAULT_CONTROL_ENDPOINT, cli_error, echo_json


@click.group("table")
def table_group() -> None:
    """Inspect and delete native Catalog tables."""


@table_group.command("ls")
@click.option(
    "--uri",
    default=DEFAULT_CONTROL_ENDPOINT,
    show_default=True,
    help="Server control endpoint used for Flight discovery.",
)
@click.option("--namespace", default=None, help="Restrict listing to one Catalog namespace.")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON output.")
def list_tables(uri: str, namespace: str | None, as_json: bool) -> None:
    """List native tables with explicit namespaces.

    :param uri: Server discovery endpoint.
    :param namespace: Optional namespace filter.
    :param as_json: Emit structured output instead of a text table.
    :raises click.ClickException: Discovery, authorization or catalog access fails.
    """
    try:
        with Client(uri) as client:
            tables = client.list_tables(namespace=namespace)
    except (ZippyClientError, OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    tables.sort(key=lambda item: (str(item["namespace"]), str(item["name"])))
    if as_json:
        echo_json(tables)
        return
    click.echo("NAMESPACE                TABLE                         TYPE")
    for table in tables:
        click.echo(f"{str(table['namespace']):<24} {str(table['name']):<29} {table['table_type']}")


@table_group.command("info")
@click.argument("table_name")
@click.option("--uri", default=DEFAULT_CONTROL_ENDPOINT, show_default=True)
@click.option("--namespace", default="default", show_default=True)
@click.option("--json", "as_json", is_flag=True)
def table_info(table_name: str, uri: str, namespace: str, as_json: bool) -> None:
    """Show authoritative table identity by name and logical Arrow schema.

    :param table_name: Native table name.
    :param uri: Server discovery endpoint.
    :param namespace: Explicit Catalog namespace.
    :param as_json: Emit structured output.
    :raises click.ClickException: The table is missing or access fails.
    """
    try:
        with Client(uri) as client:
            info = inspect_table(client, table_name, namespace)
    except (ZippyClientError, OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    if as_json:
        echo_json(info)
        return
    for name in ("namespace", "name", "catalog", "table_type"):
        click.echo(f"{name}: {info[name]}")
    echo_json(info["schema"])
    echo_json(info["snapshots"])


@table_group.command("schema")
@click.argument("table_name")
@click.option("--uri", default=DEFAULT_CONTROL_ENDPOINT, show_default=True)
@click.option("--namespace", default="default", show_default=True)
@click.option("--json", "as_json", is_flag=True)
def table_schema(table_name: str, uri: str, namespace: str, as_json: bool) -> None:
    """Show the exact native Arrow field types and metadata.

    :param table_name: Native table name.
    :param uri: Server discovery endpoint.
    :param namespace: Explicit Catalog namespace.
    :param as_json: Emit fields and base64 metadata as JSON.
    :raises click.ClickException: Schema discovery or authentication fails.
    """
    try:
        with Client(uri) as client, client.read_table(table_name, namespace=namespace) as table:
            schema = table.schema()
    except (ZippyClientError, OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    if as_json:
        echo_json(schema_info(schema))
        return
    click.echo(str(schema))


@table_group.command("drop")
@click.argument("table_id", type=click.UUID)
@click.option("--uri", default=DEFAULT_CONTROL_ENDPOINT, show_default=True)
@click.option(
    "--expected-snapshot-version",
    type=click.IntRange(min=1),
    required=True,
    help="Exact snapshot version observed in a native receipt or query metadata.",
)
@click.option(
    "--request-id",
    type=click.UUID,
    required=True,
    help="Stable non-nil request UUID; reuse unchanged after a lost response.",
)
@click.option("-y", "--yes", is_flag=True, help="Skip the confirmation prompt.")
@click.option("--json", "as_json", is_flag=True)
def drop_table(
    table_id: uuid.UUID,
    uri: str,
    expected_snapshot_version: int,
    request_id: uuid.UUID,
    yes: bool,
    as_json: bool,
) -> None:
    """Logically delete one immutable table with native concurrency protection.

    :param table_id: Immutable native table UUID; names cannot target replacements.
    :param uri: Server discovery endpoint.
    :param expected_snapshot_version: Positive version previously observed by the caller.
    :param request_id: Stable UUID reused for identical retries.
    :param yes: Bypass the interactive confirmation.
    :param as_json: Emit the committed response.
    :raises click.ClickException: Authorization, version validation or retirement fails.
    """
    if not yes and not click.confirm(
        f"drop native table [{table_id}] at snapshot [{expected_snapshot_version}]?", default=False
    ):
        click.echo("drop aborted")
        return
    try:
        with Client(uri) as client:
            result: dict[str, Any] = client.drop_table(
                table_id, expected_snapshot_version=expected_snapshot_version, request_id=request_id
            )
    except (ZippyClientError, OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    if as_json:
        echo_json(result)
        return
    click.echo(f"dropped table [{table_id}] request=[{request_id}]")
