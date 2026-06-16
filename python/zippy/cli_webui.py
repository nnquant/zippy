"""
Web UI command for the zippy CLI.
"""

from __future__ import annotations

from pathlib import Path

import click

from .cli_common import DEFAULT_CONTROL_ENDPOINT, cli_error, echo_json
from .webui import DashboardService, WebuiConfig, run_webui, webui_url


@click.command("webui")
@click.option(
    "--uri",
    "uri",
    default=DEFAULT_CONTROL_ENDPOINT,
    show_default=True,
    help="zippy-master URI",
)
@click.option("--host", default="127.0.0.1", show_default=True, help="Web UI bind host")
@click.option("--port", type=int, default=17688, show_default=True, help="Web UI bind port")
@click.option(
    "--log-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("logs"),
    show_default=True,
    help="Directory containing zippy JSONL logs",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="emit dashboard JSON")
@click.option("--debug", is_flag=True, default=False, help="enable debug logs and auto reload")
@click.option("--once", is_flag=True, hidden=True)
def webui_command(
    uri: str,
    host: str,
    port: int,
    log_dir: Path,
    as_json: bool,
    debug: bool,
    once: bool,
) -> None:
    """
    Start the local Zippy Web UI.
    """
    config = WebuiConfig(uri=uri, host=host, port=port, log_dir=log_dir, debug=debug)
    try:
        if once or as_json:
            echo_json(DashboardService(config).dashboard())
            return
        click.echo(_startup_message(config))
        run_webui(config)
    except KeyboardInterrupt:
        return
    except OSError as error:
        cli_error(str(error))


def _startup_message(config: WebuiConfig) -> str:
    url = webui_url(config.host, config.port)
    return "\n".join(
        [
            "zippy webui starting",
            f"url: {url}",
            f"master: {config.uri}",
            f"log dir: {config.log_dir}",
            f"dashboard api: {url}api/dashboard",
            "access logs: enabled",
            f"debug: {'enabled' if config.debug else 'disabled'}",
            f"reload: {'enabled' if config.debug else 'disabled'}",
            "stop: Ctrl+C",
        ]
    )
