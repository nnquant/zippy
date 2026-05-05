"""
Master management commands for the zippy CLI.
"""

from __future__ import annotations

import click
import tomllib

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
@click.argument("uri", required=False, default=None)
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
        config = _load_config(config_path)
        effective_uri = uri or _master_uri_from_config(config) or DEFAULT_CONTROL_ENDPOINT
        resolved_control_endpoint = resolve_control_endpoint(effective_uri)
        ensure_control_parent_dir(resolved_control_endpoint)
        host, port = _endpoint_host_port(resolved_control_endpoint)
        if port:
            click.echo(
                f"master starting host=[{host}] port=[{port}] "
                f"endpoint=[{resolved_control_endpoint}]"
            )
        if config_path is None:
            zippy.run_master_daemon(resolved_control_endpoint)
        else:
            zippy.run_master_daemon(resolved_control_endpoint, config=config_path)
    except KeyboardInterrupt:
        return
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))


def _load_config(path: str | None) -> dict[str, object]:
    if path is None:
        return {}
    from pathlib import Path

    if not Path(path).exists():
        return {}
    with open(path, "rb") as handle:
        return tomllib.load(handle)


def _master_uri_from_config(config: dict[str, object]) -> str | None:
    master = config.get("master", {})
    if not isinstance(master, dict):
        return None
    host = master.get("host")
    port = master.get("port")
    if host is None and port is None:
        return None
    if host is None or port is None:
        raise ValueError("master host and port must be set together")
    return f"tcp://{host}:{int(port)}"


def _endpoint_host_port(endpoint: str) -> tuple[str, str]:
    if endpoint.startswith("tcp://"):
        authority = endpoint.removeprefix("tcp://").split("/", 1)[0]
        host, port = authority.rsplit(":", 1)
        return host, port
    return endpoint, ""
