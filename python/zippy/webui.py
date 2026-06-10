"""
Lightweight local Web UI for Zippy runtime observability.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import time
from urllib.parse import parse_qs, unquote, urlparse

import zippy


@dataclass(frozen=True)
class WebuiConfig:
    uri: str
    host: str
    port: int
    log_dir: Path


def run_webui(config: WebuiConfig) -> None:
    """
    Start the local Web UI HTTP server.
    """
    service = DashboardService(config)

    class Handler(ZippyWebuiHandler):
        dashboard_service = service

    server = ThreadingHTTPServer((config.host, config.port), Handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()


class DashboardService:
    def __init__(self, config: WebuiConfig) -> None:
        self.config = config
        self.started_at = time.time()

    def dashboard(self, uri: str | None = None) -> dict[str, object]:
        query_uri = uri or self.config.uri
        now = time.time()
        snapshot = _load_registry_snapshot(query_uri)
        logs = _read_recent_logs(self.config.log_dir)
        master = _master_health(
            uri=query_uri,
            started_at=self.started_at,
            logs=logs,
        )
        try:
            client = zippy.connect(uri=query_uri)
            tables = list(zippy.ops.list_tables(master=client))
            table_details = [_table_summary(table, client) for table in tables]
            alerts = [
                alert
                for table in table_details
                for alert in table.get("alerts", [])
                if isinstance(alert, dict)
            ]
            master.update(
                {
                    "status": _overall_status(table_details, alerts),
                    "table_count": len(table_details),
                    "error": None,
                }
            )
        except Exception as error:
            table_details = []
            alerts = []
            master.update(
                {
                    "status": "error",
                    "table_count": 0,
                    "error": str(error),
                }
            )

        tasks = _task_summaries(snapshot, table_details)
        events = _recent_events(logs, snapshot, alerts)
        stale_error_tables = [
            table
            for table in table_details
            if table.get("status") in {"stale", "error", "failed"}
            or table.get("health_status") in {"warning", "error"}
        ]
        totals = {
            "tables": len(table_details),
            "tasks": len(tasks),
            "alerts": len(alerts),
            "sealed_segments": sum(_int_value(table.get("sealed_count")) for table in table_details),
            "persisted_files": sum(
                _int_value(table.get("persisted_count")) for table in table_details
            ),
            "readers": sum(_int_value(table.get("reader_count")) for table in table_details),
        }
        return {
            "generated_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "webui": {
                "pid": os.getpid(),
                "uptime_sec": int(now - self.started_at),
                "host": self.config.host,
                "port": self.config.port,
            },
            "master": master,
            "totals": totals,
            "tasks": tasks,
            "tables": table_details,
            "stale_error_tables": stale_error_tables,
            "recent_events": events,
            "log_tail": logs[-80:],
            "snapshot": {
                "path": snapshot.get("path"),
                "loaded": bool(snapshot.get("loaded")),
                "error": snapshot.get("error"),
                "sources": len(snapshot.get("sources", [])),
                "engines": len(snapshot.get("engines", [])),
                "sinks": len(snapshot.get("sinks", [])),
            },
        }


class ZippyWebuiHandler(BaseHTTPRequestHandler):
    dashboard_service: DashboardService

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_html(INDEX_HTML)
            return
        if parsed.path == "/assets/zippy-logo.png":
            self._send_asset(Path(__file__).with_name("assets") / "zippy-logo.png", "image/png")
            return
        if parsed.path == "/api/dashboard":
            query = parse_qs(parsed.query)
            uri = query.get("uri", [None])[0]
            self._send_json(self.dashboard_service.dashboard(uri=uri))
            return
        if parsed.path.startswith("/api/table/"):
            table_name = unquote(parsed.path.removeprefix("/api/table/"))
            query = parse_qs(parsed.query)
            uri = query.get("uri", [self.dashboard_service.config.uri])[0]
            self._send_json(_table_detail(table_name, uri))
            return
        self.send_error(HTTPStatus.NOT_FOUND, "not found")

    def log_message(self, format: str, *args: object) -> None:
        return

    def _send_html(self, body: str) -> None:
        data = body.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, payload: object, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_asset(self, path: Path, content_type: str) -> None:
        try:
            data = path.read_bytes()
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND, "asset not found")
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "public, max-age=3600")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def _table_detail(table_name: str, uri: str) -> dict[str, object]:
    try:
        client = zippy.connect(uri=uri)
        info = zippy.ops.table_info(table_name, master=client)
        health = zippy.ops.table_health(table_name, master=client)
        return {"ok": True, "table": info, "health": health}
    except Exception as error:
        return {"ok": False, "error": str(error), "table_name": table_name}


def _table_summary(table: dict[str, object], client: object) -> dict[str, object]:
    table_name = str(table.get("stream_name") or "")
    try:
        info = zippy.ops.table_info(table_name, master=client)
    except Exception:
        info = table
    try:
        health = zippy.ops.table_health(table_name, master=client)
    except Exception as error:
        health = {
            "status": "error",
            "alert_count": 1,
            "alerts": [
                {
                    "severity": "error",
                    "kind": "health_query_failed",
                    "table_name": table_name,
                    "message": str(error),
                }
            ],
        }

    schema = info.get("schema") if isinstance(info, dict) else {}
    fields = _schema_fields(schema)
    active = info.get("active_segment_descriptor") if isinstance(info, dict) else None
    return {
        "stream_name": table_name,
        "status": info.get("status"),
        "health_status": health.get("status"),
        "schema_hash": info.get("schema_hash"),
        "schema": schema,
        "fields": fields,
        "field_count": len(fields),
        "buffer_size": info.get("buffer_size"),
        "frame_size": info.get("frame_size"),
        "write_seq": info.get("write_seq"),
        "writer_process_id": info.get("writer_process_id"),
        "writer_epoch": info.get("writer_epoch"),
        "reader_count": info.get("reader_count"),
        "descriptor_generation": info.get("descriptor_generation"),
        "sealed_count": len(info.get("sealed_segments") or []),
        "persisted_count": len(info.get("persisted_files") or []),
        "persist_event_count": len(info.get("persist_events") or []),
        "active_rows": _descriptor_rows(active),
        "row_count": _table_row_count(info),
        "alerts": health.get("alerts") or [],
    }


def _master_health(
    *,
    uri: str,
    started_at: float,
    logs: list[dict[str, object]],
) -> dict[str, object]:
    process = _find_master_process()
    master_start = next(
        (item for item in reversed(logs) if item.get("event") == "master_start"),
        None,
    )
    return {
        "uri": uri,
        "status": "unknown",
        "pid": process.get("pid"),
        "process": process,
        "uptime_sec": int(time.time() - started_at),
        "started_at": master_start.get("timestamp") if isinstance(master_start, dict) else None,
        "table_count": 0,
        "error": None,
    }


def _find_master_process() -> dict[str, object]:
    if os.name == "nt":
        return {"pid": None, "command": None, "source": "not_available"}
    try:
        result = subprocess.run(
            ["pgrep", "-af", "zippy.*master|python.*zippy.*master"],
            check=False,
            capture_output=True,
            text=True,
            timeout=1.0,
        )
    except Exception as error:
        return {"pid": None, "command": None, "source": "pgrep", "error": str(error)}
    current_pid = str(os.getpid())
    for line in result.stdout.splitlines():
        parts = line.strip().split(maxsplit=1)
        if not parts or parts[0] == current_pid:
            continue
        command = parts[1] if len(parts) > 1 else ""
        if "webui" in command:
            continue
        return {"pid": parts[0], "command": command, "source": "pgrep"}
    return {"pid": None, "command": None, "source": "pgrep"}


def _task_summaries(
    snapshot: dict[str, object],
    tables: list[dict[str, object]],
) -> list[dict[str, object]]:
    tasks: list[dict[str, object]] = []
    for kind, key in [
        ("source", "sources"),
        ("engine", "engines"),
        ("sink", "sinks"),
    ]:
        for item in snapshot.get(key, []) or []:
            if not isinstance(item, dict):
                continue
            name = item.get(f"{kind}_name") or item.get("name")
            tasks.append(
                {
                    "kind": kind,
                    "name": name,
                    "status": item.get("status"),
                    "process_id": item.get("process_id"),
                    "input_stream": item.get("input_stream"),
                    "output_stream": item.get("output_stream"),
                    "writer_epoch": item.get("writer_epoch"),
                    "metrics": item.get("metrics"),
                }
            )
    if tasks:
        return tasks
    for table in tables:
        tasks.append(
            {
                "kind": "writer",
                "name": table.get("stream_name"),
                "status": table.get("status"),
                "process_id": table.get("writer_process_id"),
                "input_stream": None,
                "output_stream": table.get("stream_name"),
                "writer_epoch": table.get("writer_epoch"),
                "metrics": {"reader_count": table.get("reader_count")},
            }
        )
    return tasks


def _recent_events(
    logs: list[dict[str, object]],
    snapshot: dict[str, object],
    alerts: list[dict[str, object]],
) -> list[dict[str, object]]:
    events = []
    for alert in alerts[-20:]:
        events.append(
            {
                "timestamp": None,
                "level": "error" if alert.get("severity") == "error" else "warning",
                "event": alert.get("kind"),
                "message": alert.get("message"),
                "table_name": alert.get("table_name"),
            }
        )
    for item in logs[-80:]:
        if item.get("event") or item.get("message"):
            events.append(item)
    if snapshot.get("error"):
        events.append(
            {
                "timestamp": None,
                "level": "warning",
                "event": "snapshot_load_failed",
                "message": snapshot.get("error"),
            }
        )
    return events[-40:]


def _read_recent_logs(log_dir: Path) -> list[dict[str, object]]:
    files = sorted((log_dir / "zippy-master").glob("*.jsonl"), key=_mtime, reverse=True)
    if not files:
        files = sorted(log_dir.glob("**/*.jsonl"), key=_mtime, reverse=True)[:3]
    rows: list[dict[str, object]] = []
    for path in files[:3]:
        for line in _tail_lines(path, 80):
            rows.append(_parse_log_line(line, path))
    return rows[-160:]


def _parse_log_line(line: str, path: Path) -> dict[str, object]:
    try:
        payload = json.loads(line)
        if isinstance(payload, dict):
            payload.setdefault("log_file", str(path))
            return payload
    except json.JSONDecodeError:
        pass
    return {"message": line, "log_file": str(path)}


def _tail_lines(path: Path, limit: int) -> list[str]:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            lines = handle.readlines()
    except OSError:
        return []
    return [line.rstrip("\n") for line in lines[-limit:] if line.strip()]


def _load_registry_snapshot(uri: str) -> dict[str, object]:
    path = _registry_snapshot_path(uri)
    result: dict[str, object] = {
        "path": str(path) if path is not None else None,
        "loaded": False,
        "streams": [],
        "sources": [],
        "engines": [],
        "sinks": [],
        "error": None,
    }
    if path is None or not path.exists():
        return result
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as error:
        result["error"] = str(error)
        return result
    if not isinstance(payload, dict):
        result["error"] = "snapshot root is not an object"
        return result
    result.update(
        {
            "loaded": True,
            "streams": payload.get("streams") or [],
            "sources": payload.get("sources") or [],
            "engines": payload.get("engines") or [],
            "sinks": payload.get("sinks") or [],
        }
    )
    return result


def _registry_snapshot_path(uri: str) -> Path | None:
    try:
        resolved = zippy._resolve_uri(uri)  # type: ignore[attr-defined]
    except Exception:
        resolved = uri
    if resolved.startswith("tcp://"):
        authority = resolved.removeprefix("tcp://").split("/", 1)[0]
        try:
            port = int(authority.rsplit(":", 1)[1])
        except (IndexError, ValueError):
            return None
        return Path.home() / ".zippy" / "control_endpoints" / f"tcp-{port}" / "master-registry.json"
    if resolved.startswith("zippy://"):
        name = resolved.removeprefix("zippy://") or "default"
        return Path.home() / ".zippy" / "control_endpoints" / name / "master-registry.json"
    path = Path(resolved).expanduser()
    return path.parent / "master-registry.json"


def _schema_fields(schema: object) -> list[dict[str, object]]:
    if not isinstance(schema, dict):
        return []
    fields = schema.get("fields") or []
    return [field for field in fields if isinstance(field, dict)]


def _table_row_count(info: dict[str, object]) -> int:
    total = _descriptor_rows(info.get("active_segment_descriptor"))
    for segment in info.get("sealed_segments") or []:
        total += _descriptor_rows(segment)
    return total


def _descriptor_rows(descriptor: object) -> int:
    if not isinstance(descriptor, dict):
        return 0
    return _int_value(descriptor.get("committed_row_count"))


def _overall_status(tables: list[dict[str, object]], alerts: list[dict[str, object]]) -> str:
    if any(alert.get("severity") == "error" for alert in alerts):
        return "error"
    if any(alert.get("severity") == "warning" for alert in alerts):
        return "warning"
    if any(table.get("status") in {"stale", "error", "failed"} for table in tables):
        return "warning"
    return "ok"


def _int_value(value: object) -> int:
    return value if isinstance(value, int) else 0


def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Zippy WebUI</title>
  <style>
    :root {
      --canvas: #ffffff;
      --sidebar: #f3f4f6;
      --surface: #ffffff;
      --surface-soft: #fafafa;
      --ink: #111827;
      --ink-secondary: #374151;
      --ink-muted: #6b7280;
      --ink-faint: #9ca3af;
      --hairline: #e5e7eb;
      --shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
      --accent: #0075de;
      --accent-soft: #eef6ff;
      --red: #dc2626;
      --red-soft: #fff1f2;
      --orange: #f97316;
      --orange-soft: #fff7ed;
      --blue: #2563eb;
      --blue-soft: #eff6ff;
      --green: #16a34a;
      --green-soft: #ecfdf3;
      --purple: #7c3aed;
      --cyan: #0891b2;
    }
    * { box-sizing: border-box; }
    html {
      width: 100%;
      height: 100%;
      overflow-x: hidden;
    }
    body {
      margin: 0;
      width: 100%;
      height: 100%;
      max-width: 100%;
      overflow-x: hidden;
      overflow-y: hidden;
      background: var(--canvas);
      color: var(--ink);
      font-family: "Noto Sans", "Noto Sans SC", -apple-system, BlinkMacSystemFont,
        "Segoe UI", Helvetica, Arial, sans-serif;
      letter-spacing: 0;
    }
    button, input { font: inherit; }
    button { cursor: pointer; }
    .lucide {
      width: 18px;
      height: 18px;
      stroke-width: 2.3;
      flex: 0 0 auto;
    }
    .sidebar {
      position: fixed;
      inset: 0 auto 0 0;
      width: 230px;
      display: flex;
      flex-direction: column;
      padding: 22px 16px;
      border-right: 1px solid var(--hairline);
      background: var(--sidebar);
    }
    .brand {
      display: flex;
      align-items: center;
      justify-content: center;
      height: 54px;
      margin-bottom: 22px;
    }
    .brand-logo {
      display: block;
      width: 106px;
      max-width: 78%;
      height: auto;
      image-rendering: pixelated;
    }
    .nav-item {
      display: flex;
      align-items: center;
      gap: 14px;
      height: 48px;
      padding: 0 16px;
      border-radius: 8px;
      color: var(--ink-secondary);
      font-size: 16px;
      font-weight: 700;
      margin-bottom: 8px;
    }
    .nav-item.active {
      background: var(--accent);
      color: #ffffff;
    }
    .nav-icon {
      width: 21px;
      height: 21px;
      text-align: center;
      line-height: 1;
    }
    .sidebar-footer {
      margin-top: auto;
      display: grid;
      gap: 14px;
    }
    .user-card {
      border: 1px solid var(--hairline);
      border-radius: 8px;
      background: var(--surface);
      padding: 12px 14px;
    }
    .small-label {
      color: var(--ink-muted);
      font-size: 12px;
      font-weight: 700;
    }
    .user-card {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .avatar {
      display: grid;
      place-items: center;
      width: 30px;
      height: 30px;
      border-radius: 999px;
      background: #818cf8;
      color: #fff;
      font-size: 14px;
      font-weight: 900;
    }
    main {
      margin-left: 230px;
      padding: 20px 26px 32px;
      width: calc(100% - 230px);
      max-width: calc(100vw - 230px);
      height: 100vh;
      min-width: 0;
      overflow-x: hidden;
      overflow-y: auto;
    }
    .topline {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
    }
    .top-left, .top-right {
      display: flex;
      align-items: center;
      gap: 12px;
      min-width: 0;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      height: 38px;
      padding: 0 14px;
      border: 0;
      border-radius: 8px;
      background: var(--surface);
      color: var(--ink);
      font-size: 14px;
      font-weight: 800;
      box-shadow: none;
      white-space: nowrap;
    }
    .dot {
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: var(--ink-faint);
    }
    .dot.ok { background: var(--green); }
    .dot.error { background: var(--red); }
    .btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      height: 38px;
      padding: 0 14px;
      border: 1px solid var(--hairline);
      border-radius: 8px;
      background: var(--surface);
      color: var(--ink);
      font-size: 14px;
      font-weight: 800;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.03);
    }
    .btn.danger {
      border-color: var(--accent);
      background: var(--accent);
      color: #fff;
    }
    .problem-banner {
      display: none;
      position: relative;
      align-items: center;
      gap: 18px;
      min-height: 100px;
      padding: 20px 22px;
      margin-bottom: 24px;
      border: 1px solid var(--red);
      border-radius: 8px;
      background: var(--red-soft);
    }
    .problem-banner.show { display: flex; }
    .problem-banner.warning {
      border-color: var(--orange);
      background: #ffedd5;
    }
    .problem-banner.error {
      border-color: var(--red);
      background: #fee2e2;
    }
    .problem-banner.critical {
      border-color: #ef4444;
      background: #ffe4e6;
    }
    .problem-icon {
      display: grid;
      place-items: center;
      flex: 0 0 auto;
      width: 38px;
      height: 38px;
      color: var(--red);
      font-weight: 900;
    }
    .problem-icon .lucide {
      width: 36px;
      height: 36px;
      stroke-width: 2.4;
    }
    .problem-content {
      min-width: 0;
      flex: 1;
    }
    .problem-title {
      color: var(--ink);
      font-size: 16px;
      font-weight: 900;
      line-height: 1.45;
    }
    .problem-message {
      margin-top: 8px;
      color: var(--ink-muted);
      font-size: 14px;
      line-height: 1.5;
    }
    .problem-actions {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .problem-action-icon {
      display: grid;
      place-items: center;
      width: 34px;
      height: 34px;
      padding: 0;
      border: 0;
      border-radius: 8px;
      background: transparent;
      color: var(--ink);
      box-shadow: none;
    }
    .problem-action-icon:hover {
      background: rgba(17, 24, 39, 0.06);
    }
    .problem-action-icon .lucide {
      width: 20px;
      height: 20px;
      stroke-width: 2.4;
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(5, minmax(140px, 1fr));
      gap: 14px;
      margin-bottom: 26px;
    }
    .metric {
      min-height: 96px;
      display: grid;
      grid-template-columns: minmax(0, 1fr) 34px;
      gap: 14px;
      align-items: center;
      padding: 20px 16px 18px;
      border: 1px solid var(--hairline);
      border-radius: 8px;
      background: var(--surface);
      box-shadow: var(--shadow);
      min-width: 0;
      text-align: left;
    }
    .metric-icon {
      display: grid;
      place-items: center;
      width: 42px;
      height: 42px;
      justify-self: end;
      color: var(--ink-faint);
    }
    .metric-icon .lucide {
      width: 32px;
      height: 32px;
      stroke-width: 2.2;
    }
    .metric-label {
      color: var(--ink-secondary);
      font-size: 14px;
      font-weight: 800;
      line-height: 1.2;
    }
    .metric-value {
      margin-top: 12px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      color: var(--ink);
      font-size: 28px;
      line-height: 1;
      font-weight: 900;
    }
    .metric-value.red { color: var(--red); }
    .metric-value.green { color: var(--green); }
    .workspace {
      display: grid;
      grid-template-columns: minmax(0, 1fr);
      gap: 18px;
      align-items: start;
      min-width: 0;
    }
    .panel {
      overflow: hidden;
      min-width: 0;
      background: var(--surface);
      border: 1px solid var(--hairline);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }
    .panel-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      min-height: 52px;
      padding: 0 16px;
      border-bottom: 1px solid var(--hairline);
    }
    .panel-head h2 {
      margin: 0;
      font-size: 17px;
      line-height: 1.3;
      font-weight: 900;
    }
    .panel-tools {
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
    }
    .search-box {
      width: 200px;
      height: 30px;
      padding: 0 10px;
      border: 1px solid var(--hairline);
      border-radius: 6px;
      color: var(--ink-muted);
      background: var(--surface);
      font-size: 12px;
      font-weight: 700;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      font-size: 13px;
    }
    th {
      padding: 10px 14px;
      background: var(--surface-soft);
      color: var(--ink-secondary);
      font-size: 14px;
      font-weight: 900;
      text-align: left;
      border-bottom: 1px solid var(--hairline);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    td {
      padding: 12px 14px;
      border-bottom: 1px solid var(--hairline);
      vertical-align: top;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    tr.clickable { cursor: pointer; }
    tr.clickable:hover { background: var(--surface-soft); }
    tr.selected { background: var(--blue-soft); }
    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      min-height: 23px;
      padding: 3px 8px;
      border-radius: 999px;
      background: var(--surface-soft);
      color: var(--ink-secondary);
      font-size: 11px;
      font-weight: 900;
    }
    .status-pill.error {
      background: #fee2e2;
      color: var(--red);
    }
    .status-pill.warning {
      background: #ffedd5;
      color: var(--orange);
    }
    .status-pill.ok {
      background: var(--green-soft);
      color: var(--green);
    }
    .empty-state {
      display: grid;
      place-items: center;
      min-height: 230px;
      padding: 28px;
      text-align: center;
    }
    .empty-icon {
      display: grid;
      place-items: center;
      width: 68px;
      height: 68px;
      margin: 0 auto 14px;
      border-radius: 18px;
      background: #f3f4f6;
      color: #d1d5db;
      font-weight: 900;
    }
    .empty-icon .lucide {
      width: 36px;
      height: 36px;
      stroke-width: 1.8;
    }
    .empty-title {
      font-size: 18px;
      font-weight: 900;
      line-height: 1.35;
    }
    .events-link {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      color: var(--blue);
      font-size: 13px;
      font-weight: 900;
      text-decoration: none;
    }
    .diag-body {
      padding: 16px;
    }
    .diag-card {
      padding: 16px;
      border: 1px solid var(--hairline);
      border-radius: 8px;
      background: var(--surface);
    }
    .diag-block {
      display: grid;
      grid-template-columns: 18px minmax(0, 1fr);
      gap: 10px;
      padding-bottom: 18px;
      margin-bottom: 18px;
      border-left: 2px solid transparent;
    }
    .diag-block:last-child {
      padding-bottom: 0;
      margin-bottom: 0;
    }
    .diag-block.red { border-left-color: var(--red); }
    .diag-block.orange { border-left-color: var(--orange); }
    .diag-block.blue { border-left-color: var(--blue); }
    .diag-dot {
      display: grid;
      place-items: center;
      width: 14px;
      height: 14px;
      border-radius: 999px;
      color: #fff;
      font-weight: 900;
      margin-left: -8px;
      margin-top: 2px;
    }
    .diag-dot .lucide {
      width: 12px;
      height: 12px;
      stroke-width: 3;
    }
    .row-action-icon {
      width: 16px;
      height: 16px;
      color: var(--ink-muted);
    }
    .diag-dot.red { background: var(--red); }
    .diag-dot.orange { background: var(--orange); }
    .diag-dot.blue { background: var(--blue); }
    .diag-title {
      font-size: 14px;
      font-weight: 900;
      line-height: 1.4;
    }
    .diag-text {
      margin-top: 8px;
      color: var(--ink-secondary);
      font-size: 13px;
      line-height: 1.6;
    }
    .diag-text ul, .diag-text ol {
      margin: 6px 0 0 18px;
      padding: 0;
    }
    pre {
      margin: 0;
      max-height: 260px;
      overflow: auto;
      padding: 14px;
      border-radius: 6px;
      background: #111820;
      color: #f8fafc;
      font-size: 12px;
      line-height: 1.6;
    }
    .log-error { color: #f87171; font-weight: 900; }
    .log-info { color: #e5e7eb; font-weight: 900; }
    .muted { color: var(--ink-muted); }
    .strong { font-weight: 900; }
    @media (max-width: 1400px) {
      .metrics { grid-template-columns: repeat(4, minmax(160px, 1fr)); }
      .workspace { grid-template-columns: 1fr; }
    }
    @media (max-width: 860px) {
      .sidebar { display: none; }
      main { margin-left: 0; padding: 16px; width: 100%; max-width: 100vw; }
      .topline, .top-left, .top-right { align-items: stretch; flex-direction: column; }
      .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .problem-banner { align-items: flex-start; flex-direction: column; padding: 18px; }
      .panel-head { align-items: flex-start; flex-direction: column; padding: 14px 16px; }
      .search-box { width: 100%; }
    }
    @media (max-width: 520px) {
      .metrics { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <aside class="sidebar">
    <div class="brand"><img class="brand-logo" src="/assets/zippy-logo.png" alt="zippy" /></div>
    <div class="nav-item active"><i data-lucide="layout-dashboard" class="nav-icon" aria-hidden="true"></i><span>Dashborad</span></div>
    <div class="nav-item"><i data-lucide="table" class="nav-icon" aria-hidden="true"></i><span>Tables</span></div>
    <div class="nav-item"><i data-lucide="workflow" class="nav-icon" aria-hidden="true"></i><span>Processes</span></div>
    <div class="nav-item"><i data-lucide="scroll-text" class="nav-icon" aria-hidden="true"></i><span>Logs</span></div>
    <div class="sidebar-footer">
      <div class="user-card">
        <div class="avatar">A</div>
        <div><div class="strong">admin</div><div class="small-label">超级管理员</div></div>
      </div>
    </div>
  </aside>
  <main>
    <section class="topline">
      <div class="top-left">
        <div id="masterChip" class="chip"><span class="dot"></span><span>Master</span></div>
        <div id="gatewayChip" class="chip"><span class="dot"></span><span>Gateway</span></div>
      </div>
      <div class="top-right">
        <div id="lastRefresh" class="chip"><i data-lucide="clock" aria-hidden="true"></i><span>最后刷新 --:--:--</span></div>
      </div>
    </section>

    <section id="problemBanner" class="problem-banner"></section>
    <section id="metricStrip" class="metrics"></section>

    <section class="workspace">
      <section class="panel">
        <div class="panel-head">
          <h2>Tables</h2>
          <div class="panel-tools">
            <input class="search-box" value="搜索表名、ID 或描述" readonly />
            <button class="btn"><i data-lucide="list-filter" aria-hidden="true"></i><span>筛选</span></button>
            <button id="tableRefreshBtn" class="btn" aria-label="刷新表列表"><i data-lucide="refresh-cw" aria-hidden="true"></i></button>
          </div>
        </div>
        <table>
          <thead>
            <tr>
              <th style="width:18%">表名</th>
              <th style="width:12%">状态</th>
              <th style="width:20%">Writer Process ID</th>
              <th style="width:13%">Writer Epoch</th>
              <th style="width:11%">Readers</th>
              <th style="width:10%">已封板</th>
              <th style="width:10%">已持久化</th>
              <th style="width:6%">操作</th>
            </tr>
          </thead>
          <tbody id="tableRows"></tbody>
        </table>
      </section>

      <section class="panel">
        <div class="panel-head"><h2>Processes</h2></div>
        <table>
          <thead><tr><th>任务名称</th><th>类型</th><th>状态</th><th>Process</th><th>Stream</th><th>更新时间</th></tr></thead>
          <tbody id="taskRows"></tbody>
        </table>
      </section>

      <section id="logPanel" class="panel">
        <div class="panel-head"><h2>Logs</h2></div>
        <table>
          <thead><tr><th style="width:14%">时间</th><th style="width:12%">级别</th><th style="width:18%">来源</th><th>消息</th></tr></thead>
          <tbody id="logRows"></tbody>
        </table>
      </section>
    </section>
  </main>
  <script src="https://unpkg.com/lucide@latest/dist/umd/lucide.js"></script>
  <script>
    const DEFAULT_URI = "zippy://default";
    const state = { data: null, selected: null, timer: null, bannerHidden: false };
    const $ = (id) => document.getElementById(id);
    const value = (v) => (v === null || v === undefined || v === "" ? "-" : String(v));
    const number = (v) => Number.isFinite(Number(v)) ? Number(v).toLocaleString() : value(v);
    const escapeHtml = (text) => value(text).replace(/[&<>"']/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" }[m]));
    const statusClass = (status) => ["ok", "ready", "registered", "running"].includes(status) ? "ok" : (["warning", "stale"].includes(status) ? "warning" : (["error", "failed"].includes(status) ? "error" : ""));
    const healthClass = (status) => statusClass(String(status || "").toLowerCase()) === "ok" ? "ok" : "error";
    const icon = (name, className = "") => `<i data-lucide="${name}" class="${className}" aria-hidden="true"></i>`;
    function hydrateIcons() {
      if (window.lucide) window.lucide.createIcons();
    }
    async function refresh() {
      const res = await fetch(`/api/dashboard?uri=${encodeURIComponent(DEFAULT_URI)}`, { cache: "no-store" });
      state.data = await res.json();
      if (!state.selected && state.data.tables.length) state.selected = state.data.tables[0].stream_name;
      if (!state.data.tables.find((item) => item.stream_name === state.selected)) state.selected = state.data.tables[0]?.stream_name || null;
      render();
    }
    function issueSummary() {
      const data = state.data;
      if (data.master?.error) {
        return {
          level: "critical",
          title: "MasterClient 初始化失败，缺少平台对应的 zippy wheel，影响表注册和读写连接。",
          message: "当前 Master 无法提供元数据服务，Tables 无法注册，读写请求可能失败。",
        };
      }
      const bad = data.stale_error_tables || [];
      if (bad.length) {
        return {
          level: "error",
          title: `${bad.length} 张表存在 stale/error 状态。`,
          message: "请优先检查 writer ownership、descriptor_generation 和 persist 事件。",
        };
      }
      return null;
    }
    function render() {
      const data = state.data;
      if (!data) return;
      const master = data.master;
      const masterStatus = master.status || "unknown";
      const gateway = data.gateway || {};
      const gatewayStatus = gateway.status || data.gateway_status || (gateway.healthy === true ? "ok" : "error");
      $("masterChip").className = "chip";
      $("masterChip").innerHTML = `<span class="dot ${healthClass(masterStatus)}"></span><span>Master</span>`;
      $("gatewayChip").className = "chip";
      $("gatewayChip").innerHTML = `<span class="dot ${healthClass(gatewayStatus)}"></span><span>Gateway</span>`;
      $("lastRefresh").innerHTML = `${icon("clock")}<span>最后刷新 ${new Date().toLocaleTimeString()}</span>`;
      renderProblemBanner();
      renderMetrics();
      renderTables();
      renderTasks();
      renderLogs();
      hydrateIcons();
    }
    function metricCard(label, val, iconName, color = "") {
      return `<div class="metric"><div><div class="metric-label">${escapeHtml(label)}</div><div class="metric-value ${color === "red" ? "red" : color === "green" ? "green" : ""}" title="${escapeHtml(val)}">${escapeHtml(value(val))}</div></div><div class="metric-icon ${color}">${icon(iconName)}</div></div>`;
    }
    function renderMetrics() {
      const data = state.data;
      const writers = new Set((data.tables || []).map((table) => table.writer_process_id).filter(Boolean)).size;
      const masterHealthy = healthClass(data.master.status) === "ok";
      $("metricStrip").innerHTML = [
        metricCard("Status", masterHealthy ? "Health" : "Error", "server", masterHealthy ? "green" : "red"),
        metricCard("Tables", number(data.totals.tables), "table"),
        metricCard("Readers", number(data.totals.readers), "users-round"),
        metricCard("Writers", number(writers), "pen-line"),
        metricCard("Processes", number(data.totals.tasks), "list-checks"),
      ].join("");
    }
    function renderProblemBanner() {
      const issue = issueSummary();
      if (!issue || state.bannerHidden) {
        $("problemBanner").className = "problem-banner";
        $("problemBanner").innerHTML = "";
        return;
      }
      $("problemBanner").className = `problem-banner show ${issue.level || "error"}`;
      $("problemBanner").innerHTML = `
        <div class="problem-icon">${icon("triangle-alert")}</div>
        <div class="problem-content">
          <div class="problem-title">${escapeHtml(issue.title)}</div>
          <div class="problem-message">${escapeHtml(issue.message)}</div>
        </div>
        <div class="problem-actions">
          <button class="problem-action-icon" onclick="scrollToDiagnostics()" aria-label="查看诊断">${icon("stethoscope")}</button>
          <button class="problem-action-icon" onclick="copyProblem()" aria-label="复制错误">${icon("copy")}</button>
          <button class="problem-action-icon" onclick="refresh()" aria-label="重试连接">${icon("refresh-cw")}</button>
          <button class="problem-action-icon banner-close" onclick="state.bannerHidden = true; renderProblemBanner()" aria-label="关闭错误横幅">${icon("x")}</button>
        </div>`;
    }
    function renderTables() {
      const rows = state.data.tables || [];
      $("tableRows").innerHTML = rows.length ? rows.map((table) => `
        <tr class="clickable ${table.stream_name === state.selected ? "selected" : ""}" onclick="selectTable('${encodeURIComponent(table.stream_name)}')">
          <td class="strong" title="${escapeHtml(table.stream_name)}">${escapeHtml(table.stream_name)}</td>
          <td><span class="status-pill ${statusClass(table.health_status || table.status)}">${escapeHtml(table.health_status || table.status)}</span></td>
          <td title="${escapeHtml(table.writer_process_id)}">${escapeHtml(table.writer_process_id)}</td>
          <td>${escapeHtml(table.writer_epoch)}</td>
          <td>${number(table.reader_count)}</td>
          <td>${number(table.sealed_count)}</td>
          <td>${number(table.persisted_count)}</td>
          <td>${icon("chevron-right", "row-action-icon")}</td>
        </tr>`).join("") : `<tr><td colspan="8">${emptyState("No tables")}</td></tr>`;
    }
    function emptyState(title) {
      return `<div class="empty-state"><div><div class="empty-icon">${icon("package-open")}</div><div class="empty-title">${escapeHtml(title)}</div></div></div>`;
    }
    function selectTable(encoded) {
      state.selected = decodeURIComponent(encoded);
      renderTables();
      hydrateIcons();
    }
    function renderTasks() {
      const tasks = state.data.tasks || [];
      $("taskRows").innerHTML = tasks.length ? tasks.slice(0, 80).map((task) => `<tr><td class="strong">${escapeHtml(task.name)}</td><td>${escapeHtml(task.kind)}</td><td>${escapeHtml(task.status)}</td><td>${escapeHtml(task.process_id)}</td><td>${escapeHtml(task.output_stream || task.input_stream)}</td><td>-</td></tr>`).join("") : `<tr><td colspan="6">${emptyState("No processes")}</td></tr>`;
    }
    function renderLogs() {
      const rows = logRows();
      $("logRows").innerHTML = rows.length ? rows.map((row) => `<tr><td>${escapeHtml(row.time)}</td><td><span class="status-pill ${statusClass(row.level)}">${escapeHtml(row.level.toUpperCase())}</span></td><td>${escapeHtml(row.source)}</td><td title="${escapeHtml(row.message)}">${escapeHtml(row.message)}</td></tr>`).join("") : `<tr><td colspan="4">${emptyState("No Logs")}</td></tr>`;
    }
    function logRows() {
      const lines = (state.data.log_tail || []).slice(-80);
      if (!lines.length && state.data.master?.error) {
        const now = new Date().toLocaleTimeString();
        return [
          { time: now, level: "error", source: "MasterClient", message: "Failed to initialize MasterClient" },
          { time: now, level: "error", source: "MasterClient", message: state.data.master.error },
          { time: now, level: "error", source: "Schema", message: "Master unavailable, cannot fetch schema" },
          { time: now, level: "info", source: "System", message: "Waiting for reconnect..." },
        ];
      }
      return lines.map(normalizeLogEntry).filter(Boolean);
    }
    function normalizeLogEntry(entry) {
      if (entry && typeof entry === "object" && !Array.isArray(entry)) {
        const message = entry.message || entry.msg || entry.summary || JSON.stringify(entry);
        return {
          time: entry.time || entry.timestamp || entry.ts || "-",
          level: String(entry.level || entry.severity || inferLevel(message)).toLowerCase(),
          source: entry.source || entry.component || entry.logger || inferSource(message),
          message,
        };
      }
      const text = value(entry);
      return {
        time: inferTime(text),
        level: inferLevel(text).toLowerCase(),
        source: inferSource(text),
        message: text,
      };
    }
    function inferTime(text) {
      return (String(text).match(/\b\d{1,2}:\d{2}:\d{2}\b/) || [])[0] || "-";
    }
    function inferLevel(text) {
      return (String(text).match(/\b(CRITICAL|ERROR|WARNING|WARN|INFO|DEBUG)\b/i) || ["info"])[0].replace(/^warn$/i, "warning");
    }
    function inferSource(text) {
      return (String(text).match(/\[([^\]]+)\]/) || [null, "-"])[1];
    }
    function scrollToDiagnostics() {
      document.querySelector("#logPanel").scrollIntoView({ behavior: "smooth", block: "start" });
    }
    async function copyProblem() {
      const issue = issueSummary();
      if (!issue) return;
      try { await navigator.clipboard.writeText(`${issue.title}\n${issue.message}`); } catch (error) {}
    }
    $("tableRefreshBtn").addEventListener("click", refresh);
    refresh().catch((error) => {
      $("metricStrip").innerHTML = metricCard("WebUI request failed", error.message, "wifi-off", "blue");
      hydrateIcons();
    });
    state.timer = setInterval(refresh, 3000);
    window.addEventListener("load", hydrateIcons);
  </script>
</body>
</html>
"""


def webui_url(host: str, port: int) -> str:
    display_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    return f"http://{display_host}:{port}/"
