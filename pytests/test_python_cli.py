from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from click.testing import CliRunner
from fastapi import FastAPI
from fastapi.routing import APIRoute
import pytest

import zippy
import zippy.cli_webui as cli_webui
from zippy.cli import main
from zippy.cli_common import DEFAULT_CONTROL_ENDPOINT, ensure_control_parent_dir
from zippy.pm_bridge import PmCommandRunner, PmTaskInfo
from zippy.webui import DashboardService, WebuiConfig, create_app


def expected_logical_endpoint(fake_home: Path, name: str = "default") -> str:
    if os.name == "nt":
        return f"zippy://{name or 'default'}"
    return str(fake_home / ".zippy" / "control_endpoints" / name / "master.sock")


def unused_loopback_uri() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        host, port = sock.getsockname()
    return f"tcp://{host}:{port}"


def fastapi_route(app: FastAPI, path: str) -> APIRoute:
    for route in app.routes:
        if isinstance(route, APIRoute) and route.path == path:
            return route
    raise AssertionError(f"missing route path=[{path}]")


def test_server_run_help() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["server", "run", "--help"])
    assert result.exit_code == 0
    assert "Run the local single-node zippy server." in result.output
    assert DEFAULT_CONTROL_ENDPOINT in result.output
    assert "URI" in result.output


def test_public_master_daemon_alias_is_removed() -> None:
    assert not hasattr(zippy, "run_master_daemon")


def test_native_master_daemon_symbol_alias_is_removed() -> None:
    from zippy import _internal

    assert not hasattr(_internal, "run_master_daemon")


def test_cli_root_help() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "master" not in main.commands
    assert "server" in result.output
    assert "gateway" not in result.output
    assert "\n  stream " not in result.output
    assert "table" in result.output
    assert "pm" in result.output
    assert "webui" in result.output


def test_python_module_entrypoint_shows_root_help() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "zippy", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "zippy management CLI." in result.stdout
    assert "\n  master " not in result.stdout
    assert "server" in result.stdout
    assert "gateway" not in result.stdout
    assert "\n  stream " not in result.stdout
    assert "table" in result.stdout
    assert "pm" in result.stdout
    assert "webui" in result.stdout


def test_webui_once_emits_dashboard_json_for_unreachable_master() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["webui", "--uri", unused_loopback_uri(), "--once"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["catalog"]["status"] == "error"
    assert payload["catalog"]["error"]
    assert payload["webui"]["port"] == 17688
    assert payload["tables"] == []


def test_webui_startup_prints_operator_hints(monkeypatch: pytest.MonkeyPatch) -> None:
    started: list[WebuiConfig] = []

    def fake_run_webui(config: WebuiConfig) -> None:
        started.append(config)

    monkeypatch.setattr(cli_webui, "run_webui", fake_run_webui)

    runner = CliRunner()
    result = runner.invoke(main, ["webui", "--uri", "tcp://127.0.0.1:17690"])

    assert result.exit_code == 0
    assert started == [
        WebuiConfig(
            uri="tcp://127.0.0.1:17690",
            host="127.0.0.1",
            port=17688,
            log_dir=Path("logs"),
        )
    ]
    assert "zippy webui starting" in result.output
    assert "url: http://127.0.0.1:17688/" in result.output
    assert "server: tcp://127.0.0.1:17690" in result.output
    assert "log dir: logs" in result.output
    assert "dashboard api: http://127.0.0.1:17688/api/dashboard" in result.output
    assert "access logs: enabled" in result.output
    assert "stop: Ctrl+C" in result.output


def test_webui_debug_flag_enables_reload(monkeypatch: pytest.MonkeyPatch) -> None:
    started: list[WebuiConfig] = []

    def fake_run_webui(config: WebuiConfig) -> None:
        started.append(config)

    monkeypatch.setattr(cli_webui, "run_webui", fake_run_webui)

    runner = CliRunner()
    result = runner.invoke(main, ["webui", "--uri", "tcp://127.0.0.1:17690", "--debug"])

    assert result.exit_code == 0
    assert started == [
        WebuiConfig(
            uri="tcp://127.0.0.1:17690",
            host="127.0.0.1",
            port=17688,
            log_dir=Path("logs"),
            debug=True,
        )
    ]
    assert "debug: enabled" in result.output
    assert "reload: enabled" in result.output


def test_webui_fastapi_app_serves_index_dashboard_and_assets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def list_tables(self):
            return []

    class FakeRunner:
        def list_tasks(self) -> list[PmTaskInfo]:
            return []

    monkeypatch.setattr("zippy.webui.Client", lambda uri: FakeClient())
    monkeypatch.setattr(PmCommandRunner, "default", classmethod(lambda cls: FakeRunner()))

    app = create_app(
        WebuiConfig(
            uri="tcp://127.0.0.1:17690",
            host="127.0.0.1",
            port=17688,
            log_dir=tmp_path / "logs",
        )
    )

    index_response = fastapi_route(app, "/").endpoint()
    assert index_response.status_code == 200
    assert "text/html" in index_response.headers["content-type"]
    assert '<div id="root"></div>' in index_response.body.decode("utf-8")

    dashboard = fastapi_route(app, "/api/dashboard").endpoint(uri=None)
    assert dashboard["webui"]["port"] == 17688
    assert dashboard["pm"]["status"] == "ok"
    assert dashboard["tasks"] == []

    asset_response = fastapi_route(app, "/assets/zippy-logo.png").endpoint()
    assert asset_response.status_code == 200
    assert asset_response.headers["content-type"] == "image/png"
    assert asset_response.body


def test_webui_fastapi_app_serves_react_static_shell(tmp_path: Path) -> None:
    static_dir = tmp_path / "webui_static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text(
        '<div id="root"></div><script type="module" src="/assets/index.js"></script>',
        encoding="utf-8",
    )
    assets_dir = static_dir / "assets"
    assets_dir.mkdir()
    (assets_dir / "index.js").write_text("console.log('zippy')", encoding="utf-8")

    app = create_app(
        WebuiConfig(
            uri="tcp://127.0.0.1:17690",
            host="127.0.0.1",
            port=17688,
            log_dir=tmp_path / "logs",
            static_dir=static_dir,
        )
    )

    index_response = fastapi_route(app, "/").endpoint()
    body = index_response.body.decode("utf-8")

    assert index_response.status_code == 200
    assert '<div id="root"></div>' in body
    assert "/assets/index.js" in body


def test_webui_run_uses_uvicorn_with_access_logs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    started: list[dict[str, object]] = []

    def fake_uvicorn_run(
        app: object,
        *,
        host: str,
        port: int,
        access_log: bool,
        log_level: str,
    ) -> None:
        started.append(
            {
                "app": app,
                "host": host,
                "port": port,
                "access_log": access_log,
                "log_level": log_level,
            }
        )

    monkeypatch.setattr("zippy.webui.uvicorn.run", fake_uvicorn_run)

    from zippy.webui import run_webui

    run_webui(
        WebuiConfig(
            uri="tcp://127.0.0.1:17690",
            host="127.0.0.1",
            port=17688,
            log_dir=tmp_path / "logs",
        )
    )

    assert len(started) == 1
    assert isinstance(started[0]["app"], FastAPI)
    assert started[0]["host"] == "127.0.0.1"
    assert started[0]["port"] == 17688
    assert started[0]["access_log"] is True
    assert started[0]["log_level"] == "info"


def test_webui_debug_run_uses_uvicorn_reload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    started: list[dict[str, object]] = []

    def fake_uvicorn_run(
        app: object,
        *,
        host: str,
        port: int,
        access_log: bool,
        log_level: str,
        reload: bool,
        reload_dirs: list[str],
        factory: bool,
    ) -> None:
        started.append(
            {
                "app": app,
                "host": host,
                "port": port,
                "access_log": access_log,
                "log_level": log_level,
                "reload": reload,
                "reload_dirs": reload_dirs,
                "factory": factory,
            }
        )

    monkeypatch.setattr("zippy.webui.uvicorn.run", fake_uvicorn_run)

    from zippy.webui import run_webui

    run_webui(
        WebuiConfig(
            uri="tcp://127.0.0.1:17690",
            host="127.0.0.1",
            port=17688,
            log_dir=tmp_path / "logs",
            debug=True,
        )
    )

    assert len(started) == 1
    assert started[0]["app"] == "zippy.webui:create_app_from_env"
    assert started[0]["host"] == "127.0.0.1"
    assert started[0]["port"] == 17688
    assert started[0]["access_log"] is True
    assert started[0]["log_level"] == "debug"
    assert started[0]["reload"] is True
    assert started[0]["reload_dirs"]
    assert started[0]["factory"] is True


def test_webui_fastapi_app_serves_packaged_react_shell() -> None:
    app = create_app(
        WebuiConfig(
            uri="tcp://127.0.0.1:17690",
            host="127.0.0.1",
            port=17688,
            log_dir=Path("logs"),
        )
    )

    index_response = fastapi_route(app, "/").endpoint()
    body = index_response.body.decode("utf-8")

    assert index_response.status_code == 200
    assert '<div id="root"></div>' in body
    assert 'type="module"' in body
    assert "/assets/" in body
    assert "Zippy Console" in body
    assert "const DEFAULT_URI" not in body


def test_webui_packaged_react_shell_does_not_override_master_uri() -> None:
    asset_dir = Path("python/zippy/webui_static/assets")
    javascript = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(asset_dir.glob("*.js"))
    )

    assert "zippy://default" not in javascript
    assert "/api/dashboard?uri=" not in javascript
    assert "/api/table/" in javascript


def test_webui_react_dashboard_table_layout_contracts() -> None:
    app = Path("app/zippy-console/src/App.tsx").read_text(encoding="utf-8")
    dashboard = Path("app/zippy-console/src/views/DashboardView.tsx").read_text(encoding="utf-8")
    metric_card = Path("app/zippy-console/src/components/MetricCard.tsx").read_text(
        encoding="utf-8"
    )
    tables_view = Path("app/zippy-console/src/views/TablesView.tsx").read_text(encoding="utf-8")
    css = Path("app/zippy-console/src/styles/globals.css").read_text(encoding="utf-8")
    chip_block = css.split(".chip {", 1)[1].split("}", 1)[0]
    banner_block = css.split(".problem-banner.show {", 1)[1].split("}", 1)[0]
    warning_banner_block = css.split(".problem-banner.warning {", 1)[1].split("}", 1)[0]
    error_banner_block = css.split(".problem-banner.error,", 1)[1].split("}", 1)[0]
    badge_warning_block = css.split(".ui-badge-warning {", 1)[1].split("}", 1)[0]

    assert "--warning: #ffad00;" in css
    assert "--warning: #e59c02;" not in css
    assert "background: var(--warning-soft);" in badge_warning_block
    assert "color: var(--warning);" in badge_warning_block
    assert "border: 0;" in banner_block
    assert "color: #fff;" in banner_block
    assert "background: var(--warning);" in warning_banner_block
    assert "background: var(--error);" in error_banner_block
    assert "X" in app
    assert 'aria-label="Close alert banner"' in app
    assert "Retry" not in app
    assert "Dismiss" not in app
    assert "problem-close-button" in app
    assert "Loading dashboard..." not in app
    assert "function DashboardLoading" in app
    assert 'aria-label="Loading dashboard"' in app
    assert 'className="dashboard-loading"' in app
    assert 'className="dashboard-loading-ring"' in app
    assert ".dashboard-loading {" in css
    assert ".dashboard-loading-ring {" in css
    assert "@keyframes dashboard-loading-spin" in css
    assert ".problem-close-button {" in css
    assert ".problem-close-button:hover {" in css
    assert "problem-title" not in app
    assert "issue.message" in app
    assert "issue.title" not in app
    assert "Catalog query failed" in app
    assert "Process query failed" in app
    assert "tableKey(table) === current" in app
    assert "tone={statusTone(overallStatus)}" in dashboard
    assert 'label="Catalog access"' in dashboard
    assert 'label="Namespaces"' in dashboard
    assert "key={tableKey(table)}" in dashboard
    assert "reader_count" not in dashboard
    assert "gateway" not in app.lower()
    assert "Search table, ID, or description" not in dashboard
    assert 'className="action-column" aria-label="Open table detail"' in dashboard
    assert 'className="dashboard-table tables-table"' in dashboard
    assert 'className="dashboard-table"' in dashboard
    assert '<td className="strong table-name-cell"' in dashboard
    assert ".dashboard-table {" in css
    assert "font-size: 14px;" in css.split(".dashboard-table {", 1)[1].split("}", 1)[0]
    assert ".dashboard-table td {" in css
    assert "font-size: 14px;" in css.split(".dashboard-table td {", 1)[1].split("}", 1)[0]
    assert ".dashboard-table th {" in css
    assert "font-size: 12px;" in css.split(".dashboard-table th {", 1)[1].split("}", 1)[0]
    assert ".tables-table-wrap {" in css
    assert "overflow-x: auto;" in css
    assert ".table-name-cell {" in css
    assert "white-space: nowrap;" in css
    assert ".message-detail {" in css
    assert "font-weight: 600;" in css
    assert "-webkit-line-clamp: 2;" in css
    assert ".action-column {" in css
    assert "width: 42px;" in css
    assert 'font-family: "Noto Sans", system-ui' in css
    assert "body,\nbutton,\ninput,\nselect,\ntextarea {" in css
    assert ".tables-table th {" not in css
    assert "font-weight: 900;" not in css
    assert "900&display=swap" not in css
    assert "font-weight: 800;" in css
    assert ".metric-icon {" in css
    assert "width: 52px;" in css
    assert "height: 52px;" in css
    assert "background: transparent;" in css
    assert "color: var(--ink-muted);" in css
    assert ".metric-icon.ok" not in css
    assert ".metric-icon.warning" not in css
    assert ".metric-icon.error" not in css
    assert "<Icon size={34} />" in metric_card
    assert "grid-template-columns: 320px minmax(0, 1fr);" in css
    assert "grid-template-columns: 260px minmax(0, 1fr);" not in css
    assert "min-height: 44px;" in css
    assert "padding: 10px 16px;" in css
    assert "min-height: 62px;" not in css
    assert "padding: 16px 18px;" not in css
    assert "onRefresh:" not in dashboard
    assert "onRefresh," not in dashboard
    assert "onRefresh={() => void refresh()}" not in app
    assert 'aria-label="Refresh selected table"' not in tables_view
    assert "border: 0;" in chip_block
    assert "background: transparent;" in chip_block
    assert "box-shadow: none;" in chip_block
    assert "border: 1px solid var(--hairline);" not in chip_block


def test_webui_react_logs_view_contracts() -> None:
    app = Path("app/zippy-console/src/App.tsx").read_text(encoding="utf-8")
    sidebar = Path("app/zippy-console/src/components/Sidebar.tsx").read_text(encoding="utf-8")
    logs_view = Path("app/zippy-console/src/views/LogsView.tsx").read_text(encoding="utf-8")
    css = Path("app/zippy-console/src/styles/globals.css").read_text(encoding="utf-8")

    assert 'export type ConsoleView = "dashboard" | "tables" | "logs";' in sidebar
    assert "Workflow" not in sidebar
    assert ">Processes<" not in sidebar
    assert "v0.5.0" not in sidebar
    assert "sidebar-footer" not in sidebar
    assert 'activeView === "logs"' in sidebar
    assert 'onViewChange("logs")' in sidebar
    assert "LogsView" in app
    assert 'view === "logs"' in app
    assert "data={data}" in app

    assert "function parseLogEntry" in logs_view
    assert "function isAlertLog" in logs_view
    assert "function visibleLogs" in logs_view
    assert "function exportLogs" in logs_view
    assert "function copyLogs" in logs_view
    assert "function logJson" in logs_view
    assert "function JsonValue" in logs_view
    assert "function JsonScalar" in logs_view
    assert "function isJsonRecord" in logs_view
    assert "Only Alerts" in logs_view
    assert "Auto Refresh" in logs_view
    assert "Export JSONL" in logs_view
    assert "Copy visible logs" in logs_view
    assert "More" in logs_view
    assert "Raw JSON" in logs_view
    assert "levelFilter" in logs_view
    assert "sourceFilter" in logs_view
    assert "searchQuery" in logs_view
    assert 'className="tab-panel data-tab-panel logs-panel"' in logs_view
    assert 'className="data-toolbar logs-toolbar"' in logs_view
    assert 'className="data-toolbar-group"' in logs_view
    assert 'className="data-search-input"' in logs_view
    assert 'className="data-scroll logs-table-wrap"' in logs_view
    assert 'className="data-table logs-table"' in logs_view
    assert 'className="data-column-type"' not in logs_view
    assert "logColumns.map((column) => <th key={column}>{column}</th>)" in logs_view
    assert '<pre className="log-json">{logJson(selectedLog)}</pre>' not in logs_view
    assert '<div className="log-json" role="tree">' in logs_view
    assert "<JsonValue value={selectedLog.raw} />" in logs_view
    assert 'className="json-key"' in logs_view
    assert "className={`json-scalar ${scalarClass}`}" in logs_view
    assert 'aria-label="Toggle search"' in logs_view
    assert 'aria-label="Show only alert logs"' in logs_view

    assert ".logs-layout {" in css
    assert ".sidebar-footer {" not in css
    assert ".logs-table-wrap {" in css
    assert ".logs-table {" in css
    assert "min-width: 100%;" in css.split(".logs-table {", 1)[1].split("}", 1)[0]
    assert "table-layout: auto;" in css.split(".logs-table {", 1)[1].split("}", 1)[0]
    assert ".logs-table th," in css
    assert ".logs-table td {" in css
    assert "font-size: 13px;" in css.split(".logs-table th,", 1)[1].split("}", 1)[0]
    assert ".log-message-text {" in css
    assert ".log-message-text.expanded {" in css
    assert ".log-detail-panel {" in css
    assert ".log-json {" in css
    assert ".json-line {" in css
    assert ".json-key {" in css
    assert ".json-string {" in css
    assert ".json-number {" in css
    assert ".json-boolean {" in css
    assert ".json-null {" in css
    assert ".json-punctuation {" in css
    assert ".logs-search {" not in css


def test_webui_react_tables_data_grid_contracts() -> None:
    tables_view = Path("app/zippy-console/src/views/TablesView.tsx").read_text(encoding="utf-8")
    css = Path("app/zippy-console/src/styles/globals.css").read_text(encoding="utf-8")
    data_scroll_block = css.split(".data-scroll {", 1)[1].split("}", 1)[0]
    tables_list_scroll_block = css.split(".tables-list-scroll {", 1)[1].split("}", 1)[0]
    table_name_list_block = css.split(".table-name-list {", 1)[1].split("}", 1)[0]
    table_name_item_block = css.split(".table-name-item {", 1)[1].split("}", 1)[0]
    table_name_block = css.split(".table-name {", 1)[1].split("}", 1)[0]
    toolbar_icon_block = css.split(".toolbar-icon-button {", 1)[1].split("}", 1)[0]

    assert 'tab !== "data"' in tables_view
    assert "controller.abort()" in tables_view
    assert "controller.signal.aborted" in tables_view
    assert "detail={detail}" in tables_view
    assert 'className="schema-table"' in tables_view
    assert '"tab-panel data-tab-panel"' in tables_view
    assert 'fullscreen && "fullscreen"' in tables_view
    assert 'className="data-column-name"' in tables_view
    assert 'className="data-column-type"' in tables_view
    assert "function dataTypeByColumn" in tables_view
    assert "ArrowDownToLine" in tables_view
    assert "ArrowUpToLine" in tables_view
    assert "RefreshCw" in tables_view
    assert "Play" in tables_view
    assert "Pause" in tables_view
    assert "Search" in tables_view
    assert "Columns3" in tables_view
    assert "WrapText" in tables_view
    assert "Maximize2" in tables_view
    assert "Minimize2" in tables_view
    assert "Download" in tables_view
    assert "Copy" in tables_view
    assert "ClipboardList" in tables_view
    assert 'aria-label="Load tail rows"' in tables_view
    assert 'aria-label="Load head rows"' in tables_view
    assert 'aria-label="Refresh data"' in tables_view
    assert 'aria-label={autoRefresh ? "Pause auto refresh" : "Start auto refresh"}' in tables_view
    assert 'aria-label="Toggle search"' in tables_view
    assert 'aria-label="Toggle text wrap"' in tables_view
    assert 'aria-label={fullscreen ? "Exit fullscreen" : "Fullscreen"}' in tables_view
    assert 'aria-label="Copy visible rows"' in tables_view
    assert 'aria-label="Copy column names"' in tables_view
    assert (
        'className="ui-button ui-button-outline ui-button-size-icon toolbar-icon-button"'
        in tables_view
    )
    assert "Export CSV" in tables_view
    assert "Export JSON" in tables_view
    assert "Loaded" not in tables_view
    assert "lastLoaded" not in tables_view
    assert "data-loaded-at" not in tables_view
    assert "Density" not in tables_view
    assert "Rows3" not in tables_view
    assert "AlignJustify" not in tables_view
    assert "function visibleRows" in tables_view
    assert "function exportRows" in tables_view
    assert "function copyRows" in tables_view
    assert "function copyColumns" in tables_view
    assert "function downloadTextFile" in tables_view
    assert "function toCsv" in tables_view
    assert ".data-scroll {" in css
    assert "overflow: auto;" in data_scroll_block
    assert "flex: 1;" in data_scroll_block
    assert "border: 1px solid var(--hairline);" in data_scroll_block
    assert "border-radius: 0;" in data_scroll_block
    assert "overflow-x: hidden;" in tables_list_scroll_block
    assert "scrollbar-gutter" not in tables_list_scroll_block
    assert "padding: 8px 10px;" in tables_list_scroll_block
    assert "min-width: 0;" in table_name_list_block
    assert "overflow: hidden;" in table_name_list_block
    assert "overflow: hidden;" in table_name_item_block
    assert "max-width: 100%;" in table_name_item_block
    assert "flex: 1;" in table_name_block
    assert "min-width: 0;" in table_name_block
    assert ".data-tab-panel {" in css
    assert "overflow: hidden;" in css
    assert ".data-tab-panel.fullscreen {" in css
    assert ".data-toolbar-group {" in css
    assert ".toolbar-icon-button {" in css
    assert "border: 0;" in toolbar_icon_block
    assert "background: transparent;" in toolbar_icon_block
    assert ".toolbar-icon-button.active {" in css
    assert ".data-search-input {" in css
    assert ".toolbar-menu {" in css
    assert ".toolbar-menu-panel {" in css
    assert ".data-loaded-at {" not in css
    assert ".schema-table {" in css
    assert "font-size: 14px;" in css.split(".schema-table {", 1)[1].split("}", 1)[0]
    assert ".schema-table th {" in css
    assert "font-size: 12px;" in css.split(".schema-table th {", 1)[1].split("}", 1)[0]
    assert ".schema-table td {" in css
    assert "font-size: 14px;" in css.split(".schema-table td {", 1)[1].split("}", 1)[0]
    assert ".data-table {" in css
    assert "table-layout: auto;" in css
    assert "width: max-content;" in css
    assert "min-width: 100%;" in css
    assert ".data-table th," in css
    assert "text-transform: none;" in css
    assert "white-space: nowrap;" in css
    assert "overflow-wrap: normal;" in css
    assert "padding: 7px 10px;" in css
    assert ".data-table th {" in css
    assert "position: sticky;" in css
    assert ".data-column-name {" in css
    assert "color: var(--ink);" in css
    assert "font-weight: 700;" in css
    assert ".data-column-type {" in css
    assert "color: var(--ink-muted);" in css
    assert "font-weight: 500;" in css
    assert ".data-table.wrap-cells .data-cell {" in css
    assert "white-space: normal;" in css
    assert ".data-table tbody tr:nth-child(even)" in css
    assert "background: var(--surface-soft);" in css


def test_webui_dashboard_redacts_sensitive_log_fields(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs" / "zippy-master"
    log_dir.mkdir(parents=True)
    (log_dir / "master.jsonl").write_text(
        json.dumps(
            {
                "event": "master_token_generated",
                "message": "master generated control token",
                "token": "super-secret-token",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = DashboardService(
        WebuiConfig(
            uri=unused_loopback_uri(),
            host="127.0.0.1",
            port=17888,
            log_dir=tmp_path / "logs",
        )
    ).dashboard()

    logs = payload["log_tail"]
    assert isinstance(logs, list)
    assert logs[0]["token"] == "[redacted]"
    assert "super-secret-token" not in json.dumps(payload, ensure_ascii=False)


def test_webui_dashboard_log_tail_keeps_latest_logs_chronological(
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / "logs" / "zippy-master"
    log_dir.mkdir(parents=True)
    old_path = log_dir / "old.jsonl"
    new_path = log_dir / "new.jsonl"
    old_path.write_text(
        json.dumps(
            {
                "timestamp": "2026-06-12T09:00:00Z",
                "level": "info",
                "message": "old event",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    new_path.write_text(
        json.dumps(
            {
                "timestamp": "2026-06-12T09:01:00Z",
                "level": "warning",
                "message": "new event",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    os.utime(old_path, (100.0, 100.0))
    os.utime(new_path, (200.0, 200.0))

    payload = DashboardService(
        WebuiConfig(
            uri=unused_loopback_uri(),
            host="127.0.0.1",
            port=17888,
            log_dir=tmp_path / "logs",
        )
    ).dashboard()

    logs = payload["log_tail"]
    assert isinstance(logs, list)
    assert [row["message"] for row in logs] == ["old event", "new event"]


def test_webui_dashboard_processes_use_pm_ls_not_registry_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "master-registry.json").write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "source_name": "stale_registry_source",
                        "status": "lost",
                        "process_id": "proc_1",
                        "output_stream": "ldc_ctp_ticks",
                    }
                ],
                "engines": [],
            }
        ),
        encoding="utf-8",
    )

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def list_tables(self):
            return []

    class FakeRunner:
        def list_tasks(self) -> list[PmTaskInfo]:
            return [
                PmTaskInfo(
                    task_id=7,
                    name="ldc-master",
                    run_mode="long",
                    pid=12345,
                    status="online",
                    health="healthy",
                    started_at="2026-06-10T08:30:00Z",
                    uptime_ms=12_000,
                    memory_bytes=64 * 1024 * 1024,
                    restart_count=2,
                    cmd=".venv/bin/ldc-master",
                    schedule_state=None,
                )
            ]

    monkeypatch.setattr("zippy.webui.Client", lambda uri: FakeClient())
    monkeypatch.setattr(PmCommandRunner, "default", classmethod(lambda cls: FakeRunner()))

    payload = DashboardService(
        WebuiConfig(
            uri=str(tmp_path / "master.sock"),
            host="127.0.0.1",
            port=17688,
            log_dir=tmp_path / "logs",
        )
    ).dashboard()

    assert payload["tasks"] == [
        {
            "kind": "long",
            "name": "ldc-master",
            "status": "online",
            "process_id": 12345,
            "input_stream": None,
            "output_stream": None,
            "writer_epoch": None,
            "metrics": {
                "task_id": 7,
                "health": "healthy",
                "uptime_ms": 12_000,
                "memory_bytes": 64 * 1024 * 1024,
                "restart_count": 2,
                "last_exit_code": None,
                "cpu_percent": None,
                "schedule_state": None,
                "started_at": "2026-06-10T08:30:00Z",
                "stopped_at": None,
            },
            "cmd": ".venv/bin/ldc-master",
            "cwd": None,
            "dependencies": [],
            "dependents": [],
        }
    ]
    totals = payload["totals"]
    assert isinstance(totals, dict)
    assert totals["tasks"] == 1
    assert "snapshot" not in payload


def test_server_run_returns_click_error_when_runtime_start_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failing_run_server_daemon(
        control_endpoint: str,
        *,
        endpoint_explicit: bool = False,
    ) -> None:
        assert control_endpoint
        assert endpoint_explicit
        raise RuntimeError("failed to compose server runtime")

    monkeypatch.setattr(zippy, "run_server_daemon", failing_run_server_daemon, raising=False)

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["server", "run", "/tmp/zippy-server-fail.sock", "--foreground"],
    )

    assert result.exit_code != 0
    assert "failed to compose server runtime" in result.output


def test_cli_default_control_endpoint_uses_local_tcp_master() -> None:
    assert DEFAULT_CONTROL_ENDPOINT == "tcp://127.0.0.1:17690"


def test_server_run_uses_default_tcp_control_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str]] = []

    def recording_run_server_daemon(
        control_endpoint: str,
        *,
        endpoint_explicit: bool = False,
    ) -> None:
        assert endpoint_explicit
        events.append(("run_server_daemon", control_endpoint))

    def unexpected_run_master_daemon(control_endpoint: str) -> None:
        raise AssertionError(
            f"server run must not dispatch through Python master alias endpoint=[{control_endpoint}]"
        )

    monkeypatch.setattr(
        zippy,
        "run_server_daemon",
        recording_run_server_daemon,
        raising=False,
    )
    monkeypatch.setattr(
        zippy,
        "run_master_daemon",
        unexpected_run_master_daemon,
        raising=False,
    )

    runner = CliRunner()
    result = runner.invoke(main, ["server", "run", "--foreground"])

    assert result.exit_code == 0
    assert events == [("run_server_daemon", "tcp://127.0.0.1:17690")]
    assert "server starting host=[127.0.0.1] port=[17690]" in result.output


def test_control_parent_dir_skips_tcp_uri(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    ensure_control_parent_dir("tcp://127.0.0.1:17690")

    assert not (tmp_path / "tcp:").exists()


def test_server_run_forwards_config_path_and_configured_master_endpoint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    events: list[tuple[str, str]] = []
    config_path = tmp_path / "zippy-config.toml"
    config_path.write_text(
        """
[master]
host = "127.0.0.1"
port = 28690

""",
        encoding="utf-8",
    )

    def recording_run_server_daemon(
        control_endpoint: str,
        *,
        config: str | None = None,
        endpoint_explicit: bool = False,
    ) -> None:
        assert endpoint_explicit
        events.append((control_endpoint, config or ""))

    monkeypatch.setattr(
        zippy,
        "run_server_daemon",
        recording_run_server_daemon,
        raising=False,
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["server", "run", "--foreground", "--config", str(config_path)],
    )

    assert result.exit_code == 0
    assert events == [("tcp://127.0.0.1:28690", str(config_path))]
    assert (
        "server starting host=[127.0.0.1] port=[28690] endpoint=[tcp://127.0.0.1:28690]"
        in result.output
    )


def test_server_run_exits_cleanly_on_sigint(tmp_path: Path) -> None:
    if os.name == "nt":
        pytest.skip("SIGINT subprocess semantics are Unix-specific in this test")

    control_endpoint = tmp_path / "zippy-master-cli-signal.sock"
    owner_path = Path(f"{control_endpoint}.owner")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "zippy.cli",
            "server",
            "run",
            "--foreground",
            str(control_endpoint),
        ],
        cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        deadline = time.time() + 10
        while time.time() < deadline:
            if control_endpoint.exists():
                break
            if process.poll() is not None:
                break
            time.sleep(0.05)

        assert control_endpoint.exists(), "master socket was not created before SIGINT"

        process.send_signal(signal.SIGINT)
        stdout, stderr = process.communicate(timeout=10)

        assert process.returncode == 0
        assert not control_endpoint.exists()
        assert not owner_path.exists()
        assert "server_shutdown_requested" in stdout
        assert "master_stopped" in stdout
        assert "Aborted!" not in stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_removed_master_command_is_rejected() -> None:
    result = CliRunner().invoke(main, ["master", "run"])
    assert result.exit_code == 2
    assert "No such command" in result.output


@pytest.mark.parametrize("uri", ["sim", "/tmp/custom-server.sock", "tcp://127.0.0.1:17690"])
def test_server_run_resolves_explicit_endpoints(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, uri: str
) -> None:
    events: list[str] = []
    monkeypatch.setenv("HOME", str(tmp_path))

    def recording_run_server_daemon(endpoint: str, *, endpoint_explicit: bool = False) -> None:
        assert endpoint_explicit
        events.append(endpoint)

    monkeypatch.setattr(zippy, "run_server_daemon", recording_run_server_daemon)
    result = CliRunner().invoke(main, ["server", "run", "--foreground", uri])
    assert result.exit_code == 0, result.output
    expected = expected_logical_endpoint(tmp_path, "sim") if uri == "sim" else uri
    assert events == [expected]


def test_server_run_wraps_control_parent_creation_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_parent_creation(endpoint: str) -> None:
        raise PermissionError(f"cannot create control socket directory path=[{endpoint}]")

    monkeypatch.setattr("zippy.cli_server.ensure_control_parent_dir", fail_parent_creation)
    result = CliRunner().invoke(main, ["server", "run", "--foreground", "/tmp/forbidden.sock"])
    assert result.exit_code != 0
    assert "cannot create control socket directory" in result.output
    assert "Traceback" not in result.output


def test_gateway_command_group_is_removed() -> None:
    result = CliRunner().invoke(main, ["gateway", "--help"])
    assert result.exit_code == 2
    assert "No such command" in result.output
