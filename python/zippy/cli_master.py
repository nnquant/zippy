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
@click.argument("control_endpoint", required=False, default=DEFAULT_CONTROL_ENDPOINT)
def run_master(control_endpoint: str) -> None:
    """
    Run the local zippy-master daemon.

    Default control endpoint: ~/.zippy/master.sock
    """
    try:
        resolved_control_endpoint = resolve_control_endpoint(control_endpoint)
        ensure_control_parent_dir(resolved_control_endpoint)
        zippy.run_master_daemon(resolved_control_endpoint)
    except KeyboardInterrupt:
        return
    except (OSError, RuntimeError) as error:
        cli_error(str(error))
