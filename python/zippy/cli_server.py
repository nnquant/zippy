"""
Single-node server management commands for the zippy CLI.
"""

from __future__ import annotations

from collections.abc import Callable
import json
import math
import os
from pathlib import Path
import time

import click

import zippy

from .cli_common import DEFAULT_CONTROL_ENDPOINT, cli_error, ensure_control_parent_dir
from .cli_common import resolve_control_endpoint
from .cli_master import _endpoint_host_port
from .server_process import (
    _ServerProcessError,
    _cleanup_child_state_from_environment,
    _format_server_summary,
    _launch_server,
    _publish_child_shutdown_outcome,
    _server_status,
    _stop_server,
    _validate_child_starting_state,
    _validate_inherited_lock,
)


@click.group("server", help="Manage the local single-node zippy server.")
def server_group() -> None:
    """
    Register commands that manage the local single-node server.

    :returns: None.
    :rtype: None
    """


def _common_endpoint_options(function):
    function = click.option(
        "-c",
        "--config",
        "config_path",
        type=click.Path(dir_okay=False, path_type=str),
        help="Path to zippy config TOML file.",
    )(function)
    function = click.argument("uri", required=False, default=None)(function)
    return function


@server_group.command(
    "run",
    help=(
        "Run the local single-node zippy server. It starts in the background by default. "
        f"Default URI: {DEFAULT_CONTROL_ENDPOINT}"
    ),
)
@_common_endpoint_options
@click.option(
    "--foreground",
    is_flag=True,
    default=False,
    help="Keep the server attached for systemd, RSPM, containers, or debugging.",
)
@click.option(
    "--startup-timeout-sec",
    type=click.FloatRange(min=0.001),
    default=15.0,
    show_default=True,
    help="Maximum time to wait for native readiness and protocol health probes.",
)
@click.option(
    "--run-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Owner-only directory for server state and locks.",
)
@click.option(
    "--log-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Owner-only directory for bootstrap and native logs.",
)
@click.option(
    "--webui-url",
    default=None,
    help="WebUI origin URL to report and probe; paths and credentials are rejected.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit startup JSON.")
@click.option("--ready-file", type=click.Path(dir_okay=False, path_type=Path), hidden=True)
@click.option("--launch-nonce", hidden=True)
def run_server(
    uri: str | None,
    config_path: str | None,
    foreground: bool,
    startup_timeout_sec: float,
    run_dir: Path | None,
    log_dir: Path | None,
    webui_url: str | None,
    as_json: bool,
    ready_file: Path | None,
    launch_nonce: str | None,
) -> None:
    """
    Start the server-owned master, optional Gateway, and service adapters.

    :param uri: Optional master control endpoint. The configured or default endpoint is used when
        omitted.
    :type uri: str | None
    :param config_path: Optional zippy TOML configuration path.
    :type config_path: str | None
    :param foreground: Keep the server attached to the invoking terminal or supervisor.
    :type foreground: bool
    :param startup_timeout_sec: Detached startup readiness deadline in seconds.
    :type startup_timeout_sec: float
    :param run_dir: Optional state and lock directory.
    :type run_dir: pathlib.Path | None
    :param log_dir: Optional bootstrap and native log directory.
    :type log_dir: pathlib.Path | None
    :param webui_url: Optional independent WebUI URL to report and probe.
    :type webui_url: str | None
    :param as_json: Emit a machine-readable startup summary.
    :type as_json: bool
    :param ready_file: Internal native readiness record path.
    :type ready_file: pathlib.Path | None
    :param launch_nonce: Internal launcher identity nonce.
    :type launch_nonce: str | None
    :returns: None after startup or a clean foreground shutdown.
    :rtype: None
    :raises click.ClickException: If validation, startup, or health verification fails.
    :example:

        Start a detached server and wait for readiness::

            zippy server run --config /path/to/zippy.toml
    """
    try:
        control_endpoint, normalized_config = _resolve_server_inputs(uri, config_path)
        ensure_control_parent_dir(control_endpoint)
        if foreground:
            if ready_file is None and (
                run_dir is not None
                or webui_url is not None
                or as_json
                or startup_timeout_sec != 15.0
            ):
                raise _ServerProcessError(
                    "--run-dir, --webui-url, --json, and --startup-timeout-sec "
                    "apply only to detached mode"
                )
            _run_foreground_server(
                control_endpoint,
                config_path=normalized_config,
                log_dir=log_dir,
                ready_file=ready_file,
                launch_nonce=launch_nonce,
                startup_timeout_sec=startup_timeout_sec,
            )
            return
        if ready_file is not None or launch_nonce is not None:
            raise _ServerProcessError("ready-file and launch-nonce are internal foreground options")
        payload = _launch_server(
            control_endpoint,
            config_path=normalized_config,
            run_dir=run_dir,
            log_dir=log_dir,
            webui_url=webui_url,
            startup_timeout_sec=startup_timeout_sec,
        )
        _emit_payload(payload, as_json=as_json, action="run")
    except KeyboardInterrupt:
        return
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))


@server_group.command("status", help="Probe one detached single-node server.")
@_common_endpoint_options
@click.option(
    "--run-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Owner-only directory containing server state and locks.",
)
@click.option(
    "--webui-url",
    default=None,
    help="Override the independent WebUI origin URL reported and probed by status.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit status JSON.")
@click.pass_context
def server_status(
    context: click.Context,
    uri: str | None,
    config_path: str | None,
    run_dir: Path | None,
    webui_url: str | None,
    as_json: bool,
) -> None:
    """
    Verify daemon ownership and perform bounded Master, Gateway, and WebUI probes.

    :param context: Active Click context used to return a health-sensitive exit status.
    :type context: click.Context
    :param uri: Optional master control endpoint.
    :type uri: str | None
    :param config_path: Optional zippy TOML configuration used to resolve the endpoint.
    :type config_path: str | None
    :param run_dir: Optional state and lock directory.
    :type run_dir: pathlib.Path | None
    :param webui_url: Optional independent WebUI URL override.
    :type webui_url: str | None
    :param as_json: Emit a machine-readable status summary.
    :type as_json: bool
    :returns: None.
    :rtype: None
    :raises click.ClickException: If endpoint or state validation fails.
    """
    try:
        control_endpoint, _ = _resolve_server_inputs(uri, config_path)
        payload = _server_status(
            control_endpoint,
            run_dir=run_dir,
            webui_url=webui_url,
        )
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    _emit_payload(payload, as_json=as_json, action="status")
    if payload.get("status") != "healthy":
        context.exit(1)


@server_group.command("stop", help="Gracefully stop one detached single-node server.")
@_common_endpoint_options
@click.option(
    "--run-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Owner-only directory containing server state and locks.",
)
@click.option(
    "--timeout-sec",
    type=click.FloatRange(min=0.001),
    default=15.0,
    show_default=True,
    help="Maximum time to wait for graceful shutdown.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Send SIGKILL only after graceful shutdown exceeds the timeout.",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit stop JSON.")
def stop_server(
    uri: str | None,
    config_path: str | None,
    run_dir: Path | None,
    timeout_sec: float,
    force: bool,
    as_json: bool,
) -> None:
    """
    Stop a detached server after verifying its lock and process identity.

    :param uri: Optional master control endpoint.
    :type uri: str | None
    :param config_path: Optional zippy TOML configuration used to resolve the endpoint.
    :type config_path: str | None
    :param run_dir: Optional state and lock directory.
    :type run_dir: pathlib.Path | None
    :param timeout_sec: Graceful shutdown deadline in seconds.
    :type timeout_sec: float
    :param force: Permit SIGKILL after the graceful deadline expires.
    :type force: bool
    :param as_json: Emit a machine-readable shutdown summary.
    :type as_json: bool
    :returns: None after shutdown completes.
    :rtype: None
    :raises click.ClickException: If ownership validation or shutdown fails.
    """
    try:
        control_endpoint, _ = _resolve_server_inputs(uri, config_path)
        payload = _stop_server(
            control_endpoint,
            run_dir=run_dir,
            timeout_sec=timeout_sec,
            force=force,
        )
        _emit_payload(payload, as_json=as_json, action="stop")
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))


def _resolve_server_inputs(uri: str | None, config_path: str | None) -> tuple[str, str | None]:
    normalized_config: str | None = None
    if config_path is not None:
        path = Path(config_path).expanduser()
        if not path.is_file():
            raise ValueError(f"config file does not exist path=[{path}]")
        normalized_config = str(path.resolve())
    resolved = zippy.resolve_server_control_endpoint(
        uri or DEFAULT_CONTROL_ENDPOINT,
        config=normalized_config,
        endpoint_explicit=uri is not None,
    )
    return resolve_control_endpoint(resolved), normalized_config


def _run_foreground_server(
    control_endpoint: str,
    *,
    config_path: str | None,
    log_dir: Path | None,
    ready_file: Path | None,
    launch_nonce: str | None,
    startup_timeout_sec: float,
) -> None:
    if (ready_file is None) != (launch_nonce is None):
        raise ValueError("ready-file and launch-nonce must be set together")
    detached_child = ready_file is not None and launch_nonce is not None
    lock_path = None
    if detached_child:
        raw_lock_path = os.environ.get("ZIPPY_SERVER_LOCK_FILE")
        if raw_lock_path:
            lock_path = Path(raw_lock_path)
    _validate_inherited_lock(lock_path, required=detached_child)
    if ready_file is None:
        host, port = _endpoint_host_port(control_endpoint)
        if port:
            click.echo(
                f"server starting host=[{host}] port=[{port}] "
                f"endpoint=[{control_endpoint}] version=[{zippy.version()}]"
            )
        else:
            click.echo(f"server starting endpoint=[{control_endpoint}] version=[{zippy.version()}]")
    arguments: dict[str, object] = {"endpoint_explicit": True}
    if config_path is not None:
        arguments["config"] = config_path
    if log_dir is not None:
        arguments["log_dir"] = str(log_dir.expanduser().resolve())
    if ready_file is not None and launch_nonce is not None:
        _validate_child_starting_state(
            state_file=ready_file,
            launch_nonce=launch_nonce,
        )
        arguments["ready_file"] = str(ready_file)
        arguments["launch_nonce"] = launch_nonce
    try:
        if detached_child:
            arguments["startup_timeout_sec"] = _remaining_child_startup_timeout(startup_timeout_sec)
        native_runner: Callable[..., None] = zippy.run_server_daemon
        native_runner(control_endpoint, **arguments)
    except BaseException:
        if detached_child:
            _publish_child_shutdown_outcome(
                clean=False,
                error_code="runtime_error",
            )
        raise
    else:
        if detached_child:
            _publish_child_shutdown_outcome(clean=True, error_code=None)
    finally:
        _cleanup_child_state_from_environment()
        if ready_file is not None:
            try:
                ready_file.unlink()
            except OSError:
                pass


def _remaining_child_startup_timeout(fallback_timeout_sec: float) -> float:
    raw_deadline = os.environ.get("ZIPPY_SERVER_STARTUP_DEADLINE_MONOTONIC")
    if raw_deadline is None:
        return fallback_timeout_sec
    try:
        deadline = float(raw_deadline)
    except ValueError as error:
        raise _ServerProcessError("detached server startup deadline is invalid") from error
    if not math.isfinite(deadline):
        raise _ServerProcessError("detached server startup deadline is invalid")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise _ServerProcessError("detached server startup deadline expired before native startup")
    return remaining


def _emit_payload(payload: dict[str, object], *, as_json: bool, action: str) -> None:
    if as_json:
        click.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    click.echo(_format_server_summary(payload, action=action))
