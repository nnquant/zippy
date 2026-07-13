"""
Single-node server management commands for the zippy CLI.
"""

from __future__ import annotations

import click

import zippy

from .cli_common import DEFAULT_CONTROL_ENDPOINT
from .cli_master import _run_daemon


@click.group("server", help="Manage the local single-node zippy server.")
def server_group() -> None:
    """
    Register commands that manage the local single-node server.

    :returns: None.
    :rtype: None
    """


@server_group.command(
    "run",
    help=f"Run the local single-node zippy server. Default URI: {DEFAULT_CONTROL_ENDPOINT}",
)
@click.argument("uri", required=False, default=None)
@click.option(
    "-c",
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=str),
    help="Path to zippy config TOML file.",
)
def run_server(uri: str | None, config_path: str | None) -> None:
    """
    Run the server-owned master, optional Gateway, and service adapters.

    :param uri: Optional master control endpoint. The configured or default endpoint is used when
        omitted.
    :type uri: str | None
    :param config_path: Optional zippy TOML configuration path.
    :type config_path: str | None
    :returns: None after a clean shutdown.
    :rtype: None
    :raises click.ClickException: If endpoint preparation or native runtime execution fails.
    :example:

        Run using the default endpoint and configuration::

            zippy server run
    """
    _run_daemon("server", zippy.run_server_daemon, uri, config_path)
