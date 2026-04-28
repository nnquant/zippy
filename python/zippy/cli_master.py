"""
Master management commands for the zippy CLI.
"""

from __future__ import annotations

import click

import zippy

from .cli_common import (
    DEFAULT_CONTROL_ENDPOINT,
    cli_error,
    ensure_control_parent_dir,
    resolve_control_endpoint,
)


@click.group("master")
def master_group() -> None:
    """
    Manage the local zippy-master daemon.
    """


@master_group.command("run")
@click.argument("uri", required=False, default=DEFAULT_CONTROL_ENDPOINT)
@click.option(
    "-c",
    "--config",
    "config_path",
    type=click.Path(dir_okay=False, path_type=str),
    help="Path to zippy config TOML file.",
)
def run_master(uri: str, config_path: str | None) -> None:
    """
    Run the local zippy-master daemon.

    Default URI: zippy://default
    """
    try:
        resolved_control_endpoint = resolve_control_endpoint(uri)
        ensure_control_parent_dir(resolved_control_endpoint)
        if config_path is None:
            zippy.run_master_daemon(resolved_control_endpoint)
        else:
            zippy.run_master_daemon(resolved_control_endpoint, config=config_path)
    except KeyboardInterrupt:
        return
    except (OSError, RuntimeError) as error:
        cli_error(str(error))
