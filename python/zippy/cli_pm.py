"""
Process-manager bridge commands for the zippy CLI.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import click

from .cli_common import cli_error
from .pm_bridge import (
    DEFAULT_MASTER_URI,
    DEFAULT_PM_CONFIG_FILE,
    add_task,
    custom_task,
    gateway_task,
    load_pm_config,
    master_task,
)


@click.group("pm")
def pm_group() -> None:
    """
    Manage local zippy processes through zippy-rspm.
    """


@pm_group.command("validate")
@click.option(
    "-f",
    "--file",
    "config_file",
    default=DEFAULT_PM_CONFIG_FILE,
    show_default=True,
    type=click.Path(dir_okay=False, path_type=str),
    help="Path to zippy process-manager TOML config.",
)
def validate_config(config_file: str) -> None:
    """
    Validate a local zippy process-manager config.
    """
    try:
        config = load_pm_config(config_file)
        tasks = config.get("tasks", {})
        if not isinstance(tasks, dict):
            raise TypeError("config tasks must be a table")
    except (OSError, TypeError, ValueError) as error:
        cli_error(str(error))
    click.echo(f"valid [{config_file}] tasks=[{len(tasks)}]")


@pm_group.command("apply")
@click.option(
    "-f",
    "--file",
    "config_file",
    default=DEFAULT_PM_CONFIG_FILE,
    show_default=True,
    type=click.Path(dir_okay=False, path_type=str),
    help="Path to zippy process-manager TOML config.",
)
@click.option("--dry-run", is_flag=True, default=False, help="Validate config without applying.")
def apply_config(config_file: str, dry_run: bool) -> None:
    """
    Apply a local zippy process-manager config.
    """
    try:
        config = load_pm_config(config_file)
        tasks = config.get("tasks", {})
        if not isinstance(tasks, dict):
            raise TypeError("config tasks must be a table")
        if dry_run:
            click.echo(f"dry-run [{config_file}] tasks=[{len(tasks)}]")
            return
        runner_cls = _pm_command_runner()
        if runner_cls is None:
            raise NotImplementedError("pm apply runner is not implemented yet")
        runner_cls(config_file).apply()
    except (AttributeError, NotImplementedError, OSError, TypeError, ValueError) as error:
        cli_error(str(error))


@pm_group.group("add")
def add_group() -> None:
    """
    Add tasks to the local zippy process-manager config.
    """


@add_group.command("master")
@click.option(
    "--master-uri",
    default=DEFAULT_MASTER_URI,
    show_default=True,
    help="TCP master URI passed to zippy master run.",
)
@click.option(
    "-f",
    "--file",
    "config_file",
    default=DEFAULT_PM_CONFIG_FILE,
    show_default=True,
    type=click.Path(dir_okay=False, path_type=str),
    help="Path to zippy process-manager TOML config.",
)
def add_master(master_uri: str, config_file: str) -> None:
    """
    Add the default zippy master task.
    """
    try:
        add_task(config_file, "master", master_task(master_uri))
    except (OSError, TypeError, ValueError) as error:
        cli_error(str(error))
    click.echo(f"added [master] file=[{config_file}]")


@add_group.command("gateway")
@click.option(
    "--master-uri",
    default=DEFAULT_MASTER_URI,
    show_default=True,
    help="Master URI used by the gateway.",
)
@click.option("--endpoint", default=None, help="Gateway endpoint.")
@click.option("--token", default=None, help="Gateway access token.")
@click.option(
    "-f",
    "--file",
    "config_file",
    default=DEFAULT_PM_CONFIG_FILE,
    show_default=True,
    type=click.Path(dir_okay=False, path_type=str),
    help="Path to zippy process-manager TOML config.",
)
def add_gateway(
    master_uri: str,
    endpoint: str | None,
    token: str | None,
    config_file: str,
) -> None:
    """
    Add the default zippy gateway task.
    """
    try:
        task = gateway_task(master_uri, endpoint=endpoint, token=token)
        add_task(config_file, "gateway", task)
    except (OSError, TypeError, ValueError) as error:
        cli_error(str(error))
    click.echo(f"added [gateway] file=[{config_file}]")


@add_group.command("custom")
@click.argument("command")
@click.option("--name", required=True, help="Task name to write under [tasks].")
@click.option("--cwd", default=None, type=click.Path(file_okay=False, path_type=str))
@click.option("--env", "env_entries", multiple=True, help="Environment entry as KEY=VALUE.")
@click.option(
    "-f",
    "--file",
    "config_file",
    default=DEFAULT_PM_CONFIG_FILE,
    show_default=True,
    type=click.Path(dir_okay=False, path_type=str),
    help="Path to zippy process-manager TOML config.",
)
def add_custom(
    command: str,
    name: str,
    cwd: str | None,
    env_entries: tuple[str, ...],
    config_file: str,
) -> None:
    """
    Add a custom task from an explicit command.
    """
    try:
        task = custom_task(command, cwd=cwd, env=env_entries)
        add_task(config_file, name, task)
    except (OSError, TypeError, ValueError) as error:
        cli_error(str(error))
    click.echo(f"added [{name}] file=[{config_file}]")


@pm_group.command("start")
def start() -> None:
    """
    Start managed zippy process-manager tasks.
    """
    _runtime_command("start")


@pm_group.command("stop")
def stop() -> None:
    """
    Stop managed zippy process-manager tasks.
    """
    _runtime_command("stop")


@pm_group.command("restart")
def restart() -> None:
    """
    Restart managed zippy process-manager tasks.
    """
    _runtime_command("restart")


@pm_group.command("doctor")
def doctor() -> None:
    """
    Inspect local zippy process-manager health.
    """
    _runtime_command("doctor")


def _runtime_command(command: str) -> None:
    runner_cls = _pm_command_runner()
    if runner_cls is None:
        cli_error(f"pm {command} runner is not implemented yet")
    try:
        getattr(runner_cls(DEFAULT_PM_CONFIG_FILE), command)()
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as error:
        cli_error(str(error))


def _pm_command_runner() -> Callable[[str | Path], object] | None:
    try:
        from .pm_bridge import PmCommandRunner
    except ImportError:
        return None
    return PmCommandRunner
