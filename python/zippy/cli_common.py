"""
Shared helpers for the zippy Python CLI.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import NoReturn

import click

DEFAULT_CONTROL_ENDPOINT = "zippy://default"


def echo_json(payload: object) -> None:
    """
    Emit a JSON payload to stdout.

    :param payload: JSON-serializable Python object.
    :type payload: object
    """
    click.echo(json.dumps(payload, ensure_ascii=False, indent=2))


def resolve_control_endpoint(control_endpoint: str) -> str:
    """
    Resolve a user-facing control endpoint URI.

    :param control_endpoint: Raw CLI control endpoint URI or explicit path.
    :type control_endpoint: str
    :returns: Resolved control endpoint URI or filesystem path.
    :rtype: str
    """
    from zippy import _resolve_uri

    return _resolve_uri(control_endpoint)


def ensure_control_parent_dir(control_endpoint: str) -> None:
    """
    Ensure the parent directory for a filesystem control endpoint exists.

    :param control_endpoint: Expanded control endpoint URI or path.
    :type control_endpoint: str
    """
    if control_endpoint.startswith(("tcp://", "zippy://")):
        return
    parent = Path(control_endpoint).parent
    parent.mkdir(parents=True, exist_ok=True)


def cli_error(message: str) -> NoReturn:
    """
    Raise a CLI-friendly error.

    :param message: Human-readable error message.
    :type message: str
    :raises click.ClickException: Always.
    """
    raise click.ClickException(message)
