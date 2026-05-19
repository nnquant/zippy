"""
Process-manager bridge commands for the zippy CLI.
"""

from __future__ import annotations

import json

import click

from .cli_common import cli_error
from .pm_bridge import (
    DEFAULT_MASTER_URI,
    DEFAULT_PM_CONFIG_FILE,
    PmCommandRunner,
    PmTaskInfo,
    add_task,
    custom_task,
    gateway_task,
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
        message = PmCommandRunner.default().validate(config_file)
    except (OSError, TypeError, ValueError) as error:
        cli_error(str(error))
    click.echo(message)


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
        message = PmCommandRunner.default().apply(config_file, dry_run=dry_run)
    except (ConnectionError, OSError, RuntimeError, TimeoutError, TypeError, ValueError) as error:
        cli_error(str(error))
    click.echo(message)


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
@click.argument("name", required=False)
def start(name: str | None) -> None:
    """
    Start managed zippy process-manager tasks.
    """
    _task_action("start", name)


@pm_group.command("stop")
@click.argument("name", required=False)
def stop(name: str | None) -> None:
    """
    Stop managed zippy process-manager tasks.
    """
    _task_action("stop", name)


@pm_group.command("restart")
@click.argument("name", required=False)
def restart(name: str | None) -> None:
    """
    Restart managed zippy process-manager tasks.
    """
    _task_action("restart", name)


@pm_group.command("status")
def status() -> None:
    """
    Show managed task status.
    """
    try:
        _print_task_table(PmCommandRunner.default().status())
    except (ConnectionError, OSError, RuntimeError, TimeoutError, TypeError, ValueError) as error:
        cli_error(str(error))


@pm_group.command("ls")
def list_tasks() -> None:
    """
    List managed tasks.
    """
    try:
        _print_task_table(PmCommandRunner.default().list_tasks())
    except (ConnectionError, OSError, RuntimeError, TimeoutError, TypeError, ValueError) as error:
        cli_error(str(error))


@pm_group.command("logs")
@click.argument("name")
@click.option("--lines", type=int, default=None, help="Number of trailing log lines to print.")
@click.option("--follow", is_flag=True, default=False, help="Follow logs.")
def logs(name: str, lines: int | None, follow: bool) -> None:
    """
    Print task logs.
    """
    if follow:
        cli_error("log follow is not implemented in the first bridge version")
    try:
        click.echo(PmCommandRunner.default().logs(name, lines=lines))
    except (ConnectionError, OSError, RuntimeError, TimeoutError, TypeError, ValueError) as error:
        cli_error(str(error))


@pm_group.command("events")
def events() -> None:
    """
    Print daemon events.
    """
    try:
        for event in PmCommandRunner.default().events():
            click.echo(json.dumps(event, ensure_ascii=False, sort_keys=True))
    except (ConnectionError, OSError, RuntimeError, TimeoutError, TypeError, ValueError) as error:
        cli_error(str(error))


@pm_group.command("doctor")
def doctor() -> None:
    """
    Inspect local zippy process-manager health.
    """
    try:
        info = PmCommandRunner.default().doctor()
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        cli_error(str(error))
    for key in ("daemon", "endpoint", "log_dir", "state_dir", "run_dir", "socket_path"):
        click.echo(f"{key}: {info[key]}")


def _task_action(action: str, name: str | None) -> None:
    try:
        tasks = getattr(PmCommandRunner.default(), action)(name)
    except (ConnectionError, OSError, RuntimeError, TimeoutError, TypeError, ValueError) as error:
        cli_error(str(error))
    for task in tasks:
        click.echo(f"{action} [{task.name}] status=[{task.status}] pid=[{task.pid}]")


def _print_task_table(tasks: list[PmTaskInfo]) -> None:
    click.echo(f"{'name':<24} {'status':<14} {'pid':<10} command")
    for task in tasks:
        pid = "" if task.pid is None else str(task.pid)
        click.echo(f"{task.name:<24} {task.status:<14} {pid:<10} {task.cmd}")
