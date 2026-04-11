"""
Stream inspection commands for the zippy CLI.
"""

from __future__ import annotations

import click

import zippy

from .cli_common import DEFAULT_CONTROL_ENDPOINT, cli_error, echo_json, resolve_control_endpoint


def _master_client(control_endpoint: str) -> zippy.MasterClient:
    """
    Create a master client for stream inspection commands.

    :param control_endpoint: Unix control socket path.
    :type control_endpoint: str
    :returns: Connected master client.
    :rtype: zippy.MasterClient
    """
    return zippy.MasterClient(control_endpoint=resolve_control_endpoint(control_endpoint))


@click.group("stream")
def stream_group() -> None:
    """
    Inspect master-managed streams.
    """


@stream_group.command("ls")
@click.option(
    "--control-endpoint",
    default=DEFAULT_CONTROL_ENDPOINT,
    show_default=True,
    help="zippy-master control endpoint",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="emit JSON output")
def list_streams(control_endpoint: str, as_json: bool) -> None:
    """
    List all registered streams.
    """
    try:
        streams = _master_client(control_endpoint).list_streams()
    except RuntimeError as error:
        cli_error(str(error))

    if as_json:
        echo_json(streams)
        return

    click.echo("STREAM NAME                 RING CAPACITY  STATUS")
    for stream in streams:
        click.echo(
            f"{stream['stream_name']:<27} {stream['ring_capacity']:<14} {stream['status']}"
        )


@stream_group.command("show")
@click.argument("stream_name")
@click.option(
    "--control-endpoint",
    default=DEFAULT_CONTROL_ENDPOINT,
    show_default=True,
    help="zippy-master control endpoint",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="emit JSON output")
def show_stream(stream_name: str, control_endpoint: str, as_json: bool) -> None:
    """
    Show a single registered stream.
    """
    try:
        stream = _master_client(control_endpoint).get_stream(stream_name)
    except RuntimeError as error:
        cli_error(str(error))

    if as_json:
        echo_json(stream)
        return

    for key in ["stream_name", "ring_capacity", "writer_process_id", "reader_count", "status"]:
        click.echo(f"{key}: {stream[key]}")
