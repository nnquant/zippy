"""
Stream inspection commands for the zippy CLI.
"""

from __future__ import annotations

import click

import zippy

from .cli_common import DEFAULT_CONTROL_ENDPOINT, cli_error, echo_json, resolve_control_endpoint


def _master_client(uri: str) -> zippy.MasterClient:
    """
    Create a master client for stream inspection commands.

    :param uri: Master URI or explicit Unix control socket path.
    :type uri: str
    :returns: Connected master client.
    :rtype: zippy.MasterClient
    """
    return zippy.MasterClient(uri=resolve_control_endpoint(uri))


@click.group("stream")
def stream_group() -> None:
    """
    Inspect master-managed streams.
    """


@stream_group.command("ls")
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
def list_streams(uri: str, as_json: bool) -> None:
    """
    List all registered streams.
    """
    try:
        streams = _master_client(uri).list_streams()
    except RuntimeError as error:
        cli_error(str(error))

    if as_json:
        echo_json(streams)
        return

    click.echo("STREAM NAME                 BUFFER SIZE  FRAME SIZE  STATUS")
    for stream in streams:
        click.echo(
            f"{stream['stream_name']:<27} "
            f"{stream['buffer_size']:<12} "
            f"{stream['frame_size']:<11} "
            f"{stream['status']}"
        )


@stream_group.command("show")
@click.argument("stream_name")
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
def show_stream(stream_name: str, uri: str, as_json: bool) -> None:
    """
    Show a single registered stream.
    """
    try:
        stream = _master_client(uri).get_stream(stream_name)
    except RuntimeError as error:
        cli_error(str(error))

    if as_json:
        echo_json(stream)
        return

    for key in [
        "stream_name",
        "buffer_size",
        "frame_size",
        "writer_process_id",
        "reader_count",
        "status",
    ]:
        click.echo(f"{key}: {stream[key]}")
