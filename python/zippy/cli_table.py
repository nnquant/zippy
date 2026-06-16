"""
Table management commands for the zippy CLI.
"""

from __future__ import annotations

from collections.abc import Mapping

import click

import zippy

from .cli_common import DEFAULT_CONTROL_ENDPOINT, cli_error, echo_json


def _master_client(uri: str, *, token: str | None = None) -> zippy.MasterClient:
    """
    Create a master client for table management commands.

    :param uri: Master URI or explicit Unix control socket path.
    :type uri: str
    :param token: Optional master control token for privileged operations.
    :type token: str | None
    :returns: Connected master client.
    :rtype: zippy.MasterClient
    """
    client = zippy.connect(uri=uri)
    if token is not None:
        set_token = getattr(client, "set_token", None)
        if not callable(set_token):
            raise RuntimeError("master client does not support control tokens")
        set_token(token)
    return client


@click.group("table")
def table_group() -> None:
    """
    Manage master-registered tables.
    """


@table_group.command("ls")
@click.option(
    "--uri",
    "uri",
    default=DEFAULT_CONTROL_ENDPOINT,
    show_default=True,
    help="zippy-master URI",
)
@click.option(
    "--control-endpoint",
    "uri",
    hidden=True,
)
@click.option("--json", "as_json", is_flag=True, default=False, help="emit JSON output")
def list_tables(uri: str, as_json: bool) -> None:
    """
    List all registered tables.
    """
    try:
        tables = zippy.ops.list_tables(master=_master_client(uri))
    except RuntimeError as error:
        cli_error(str(error))

    if as_json:
        echo_json(tables)
        return

    click.echo(
        "TABLE NAME                  STATUS           ROWS  BUFFER SIZE  FRAME SIZE  SCHEMA HASH"
    )
    for table in tables:
        row_count = _table_row_count(table)
        click.echo(
            f"{str(table['stream_name']):<27} "
            f"{str(table['status']):<16} "
            f"{row_count:<5} "
            f"{table['buffer_size']:<12} "
            f"{table['frame_size']:<11} "
            f"{table.get('schema_hash', '')}"
        )


@table_group.command("info")
@click.argument("table_name")
@click.option(
    "--uri",
    "uri",
    default=DEFAULT_CONTROL_ENDPOINT,
    show_default=True,
    help="zippy-master URI",
)
@click.option(
    "--control-endpoint",
    "uri",
    hidden=True,
)
@click.option("--json", "as_json", is_flag=True, default=False, help="emit JSON output")
def table_info(table_name: str, uri: str, as_json: bool) -> None:
    """
    Show metadata for one registered table.
    """
    try:
        info = zippy.ops.table_info(table_name, master=_master_client(uri))
    except (RuntimeError, ValueError) as error:
        cli_error(str(error))

    if as_json:
        echo_json(info)
        return

    for key in [
        "stream_name",
        "status",
        "schema_hash",
        "buffer_size",
        "frame_size",
        "write_seq",
        "writer_process_id",
        "writer_epoch",
        "reader_count",
        "descriptor_generation",
    ]:
        click.echo(f"{key}: {info.get(key)}")
    click.echo(f"persisted_file_count: {len(info.get('persisted_files') or [])}")
    click.echo(f"sealed_segment_count: {len(info.get('sealed_segments') or [])}")


@table_group.command("schema")
@click.argument("table_name")
@click.option(
    "--uri",
    "uri",
    default=DEFAULT_CONTROL_ENDPOINT,
    show_default=True,
    help="zippy-master URI",
)
@click.option(
    "--control-endpoint",
    "uri",
    hidden=True,
)
@click.option("--json", "as_json", is_flag=True, default=False, help="emit JSON output")
def table_schema(table_name: str, uri: str, as_json: bool) -> None:
    """
    Show the registered Arrow schema for one table.
    """
    try:
        info = zippy.ops.table_info(table_name, master=_master_client(uri))
    except (RuntimeError, ValueError) as error:
        cli_error(str(error))

    schema = info.get("schema") or {}
    if as_json:
        echo_json(schema)
        return

    fields = _schema_fields(schema)
    click.echo(
        "FIELD                      DATA TYPE                 SEGMENT TYPE       NULLABLE  TIMEZONE"
    )
    for field in fields:
        click.echo(
            f"{str(field.get('name', '')):<26} "
            f"{str(field.get('data_type', '')):<25} "
            f"{str(field.get('segment_type', '')):<18} "
            f"{str(field.get('nullable', '')):<9} "
            f"{_display_value(field.get('timezone'))}"
        )


@table_group.command("drop")
@click.argument("table_name")
@click.option(
    "--uri",
    "uri",
    default=DEFAULT_CONTROL_ENDPOINT,
    show_default=True,
    help="zippy-master URI",
)
@click.option(
    "--control-endpoint",
    "uri",
    hidden=True,
)
@click.option(
    "--keep-persisted",
    is_flag=True,
    default=False,
    help="remove table metadata but keep registered persisted parquet files",
)
@click.option("-y", "--yes", is_flag=True, default=False, help="skip confirmation prompt")
@click.option(
    "--token",
    envvar="ZIPPY_MASTER_TOKEN",
    default=None,
    help="master control token; defaults to ZIPPY_MASTER_TOKEN",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="emit JSON output")
def drop_table(
    table_name: str,
    uri: str,
    keep_persisted: bool,
    yes: bool,
    token: str | None,
    as_json: bool,
) -> None:
    """
    Drop one registered table from master.
    """
    drop_persisted = not keep_persisted
    if not yes:
        persisted_note = (
            "and delete persisted files" if drop_persisted else "and keep persisted files"
        )
        confirmed = click.confirm(
            f"drop table [{table_name}] {persisted_note}?",
            default=False,
        )
        if not confirmed:
            click.echo("drop aborted")
            return

    try:
        result = zippy.ops.drop_table(
            table_name,
            drop_persisted=drop_persisted,
            master=_master_client(uri, token=token),
        )
    except (RuntimeError, ValueError) as error:
        cli_error(str(error))

    if as_json:
        echo_json(result)
        return

    click.echo(
        "dropped table "
        f"{result.get('table_name')} "
        f"dropped={result.get('dropped')} "
        f"sources_removed={result.get('sources_removed')} "
        f"engines_removed={result.get('engines_removed')} "
        f"persisted_files_deleted={result.get('persisted_files_deleted')}"
    )


def _schema_fields(schema: object) -> list[Mapping[str, object]]:
    if not isinstance(schema, Mapping):
        return []
    fields = schema.get("fields") or []
    return [field for field in fields if isinstance(field, Mapping)]


def _table_row_count(table: Mapping[str, object]) -> int:
    active = table.get("active_segment_descriptor")
    row_count = 0
    if isinstance(active, Mapping):
        row_count += _int_value(active.get("committed_row_count"))
    for segment in table.get("sealed_segments") or []:
        if isinstance(segment, Mapping):
            row_count += _int_value(segment.get("committed_row_count"))
    return row_count


def _int_value(value: object) -> int:
    if isinstance(value, int):
        return value
    return 0


def _display_value(value: object) -> str:
    if value is None:
        return "-"
    return str(value)
