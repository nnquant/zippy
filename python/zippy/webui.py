"""
Lightweight local Web UI for Zippy runtime observability.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import mimetypes
import os
from pathlib import Path
import subprocess
import time
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, Response
import uvicorn

import zippy
from .pm_bridge import PmCommandRunner, PmTaskInfo

SENSITIVE_LOG_FIELD_NAMES = {
    "api_key",
    "access_key",
    "credential",
    "credentials",
    "password",
    "secret",
    "secret_key",
    "token",
}


@dataclass(frozen=True)
class WebuiConfig:
    uri: str
    host: str
    port: int
    log_dir: Path
    debug: bool = False
    static_dir: Path | None = None


def run_webui(config: WebuiConfig) -> None:
    """
    Start the local Web UI HTTP server.
    """
    if config.debug:
        os.environ["ZIPPY_WEBUI_URI"] = config.uri
        os.environ["ZIPPY_WEBUI_HOST"] = config.host
        os.environ["ZIPPY_WEBUI_PORT"] = str(config.port)
        os.environ["ZIPPY_WEBUI_LOG_DIR"] = str(config.log_dir)
        uvicorn.run(
            "zippy.webui:create_app_from_env",
            host=config.host,
            port=config.port,
            access_log=True,
            log_level="debug",
            reload=True,
            reload_dirs=[str(Path(__file__).resolve().parents[1])],
            factory=True,
        )
        return

    uvicorn.run(
        create_app(config),
        host=config.host,
        port=config.port,
        access_log=True,
        log_level="info",
    )


def create_app_from_env() -> FastAPI:
    """
    Create a Web UI app from environment variables for uvicorn reload workers.
    """
    return create_app(
        WebuiConfig(
            uri=os.environ.get("ZIPPY_WEBUI_URI", "zippy://default"),
            host=os.environ.get("ZIPPY_WEBUI_HOST", "127.0.0.1"),
            port=int(os.environ.get("ZIPPY_WEBUI_PORT", "17688")),
            log_dir=Path(os.environ.get("ZIPPY_WEBUI_LOG_DIR", "logs")),
            debug=True,
        )
    )


def create_app(config: WebuiConfig) -> FastAPI:
    """
    Create the read-only FastAPI application for the Zippy Web UI.
    """
    app = FastAPI(title="Zippy WebUI")
    service = DashboardService(config)
    app.state.dashboard_service = service

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        return HTMLResponse(_read_webui_index(config))

    @app.get("/assets/zippy-logo.png")
    def zippy_logo() -> Response:
        path = Path(__file__).with_name("assets") / "zippy-logo.png"
        try:
            data = path.read_bytes()
        except OSError:
            return Response("asset not found", status_code=404, media_type="text/plain")
        return Response(
            content=data,
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=3600"},
        )

    @app.get("/assets/{asset_path:path}")
    def webui_asset(asset_path: str) -> Response:
        path = (_webui_static_dir(config) / "assets" / asset_path).resolve()
        root = (_webui_static_dir(config) / "assets").resolve()
        try:
            path.relative_to(root)
            data = path.read_bytes()
        except (OSError, ValueError):
            return Response("asset not found", status_code=404, media_type="text/plain")
        media_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        return Response(
            content=data,
            media_type=media_type,
            headers={"Cache-Control": "public, max-age=3600"},
        )

    @app.get("/api/dashboard")
    def dashboard(uri: str | None = Query(default=None)) -> dict[str, object]:
        return service.dashboard(uri=uri)

    @app.get("/api/table/{table_name:path}/data")
    def table_data(
        table_name: str,
        mode: str = Query(default="tail"),
        limit: int = Query(default=100),
        uri: str | None = Query(default=None),
    ) -> dict[str, object]:
        return _table_data_preview(
            table_name=table_name,
            uri=uri or config.uri,
            mode=mode,
            limit=limit,
        )

    @app.get("/api/table/{table_name:path}")
    def table_detail(
        table_name: str,
        uri: str | None = Query(default=None),
    ) -> dict[str, object]:
        return _table_detail(table_name, uri or config.uri)

    return app


def _webui_static_dir(config: WebuiConfig) -> Path:
    return config.static_dir or Path(__file__).with_name("webui_static")


def _read_webui_index(config: WebuiConfig) -> str:
    path = _webui_static_dir(config) / "index.html"
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return (
            "<!doctype html><html><head><title>Zippy Console</title></head>"
            "<body><main><h1>Zippy Console static assets are missing</h1>"
            "<p>Run npm run build in app/zippy-console and package the dist output.</p>"
            "</main></body></html>"
        )


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
        gateway = _gateway_unavailable("master connection not established")
        try:
            client = zippy.connect(uri=query_uri)
            gateway = _gateway_summary(client)
            tables = list(zippy.ops.list_tables(master=client))
            table_details = [_table_summary(table, client) for table in tables]
            alerts = [
                alert for table in table_details for alert in _dict_items(table.get("alerts"))
            ]
            overall_status = _overall_status(table_details, alerts)
            master.update(
                {
                    "status": "ok",
                    "table_count": len(table_details),
                    "error": None,
                }
            )
        except Exception as error:
            table_details = []
            alerts = []
            overall_status = "error"
            master.update(
                {
                    "status": "error",
                    "table_count": 0,
                    "error": str(error),
                }
            )

        pm = _pm_summary()
        tasks = _dict_items(pm.get("tasks"))
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
            "sealed_segments": sum(
                _int_value(table.get("sealed_count")) for table in table_details
            ),
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
            "overall_status": overall_status,
            "gateway": gateway,
            "gateway_status": gateway.get("status"),
            "pm": pm,
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
                "sources": _list_len(snapshot.get("sources")),
                "engines": _list_len(snapshot.get("engines")),
            },
        }


def _table_detail(table_name: str, uri: str) -> dict[str, object]:
    try:
        client = zippy.connect(uri=uri)
        info = zippy.ops.table_info(table_name, master=client)
        health = zippy.ops.table_health(table_name, master=client)
        return {"ok": True, "table": info, "health": health}
    except Exception as error:
        return {"ok": False, "error": str(error), "table_name": table_name}


def _table_data_preview(
    *,
    table_name: str,
    uri: str,
    mode: str,
    limit: int,
) -> dict[str, object]:
    normalized_mode = mode if mode in {"head", "tail"} else "tail"
    normalized_limit = min(max(int(limit), 1), 1000)
    try:
        client = zippy.connect(uri=uri)
        query = zippy.read_table(table_name, master=client)
        selected = (
            query.head(normalized_limit)
            if normalized_mode == "head"
            else query.tail(normalized_limit)
        )
        table = selected.collect()
        columns = list(table.column_names)
        rows = [
            {key: _json_safe_value(value) for key, value in row.items()}
            for row in table.to_pylist()
        ]
        return {
            "ok": True,
            "table_name": table_name,
            "mode": normalized_mode,
            "limit": normalized_limit,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": len(rows) >= normalized_limit,
            "error": None,
        }
    except Exception as error:
        return {
            "ok": False,
            "table_name": table_name,
            "mode": normalized_mode,
            "limit": normalized_limit,
            "columns": [],
            "rows": [],
            "row_count": 0,
            "truncated": False,
            "error": str(error),
        }


def _table_summary(table: dict[str, object], client: Any) -> dict[str, object]:
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
        "sealed_count": _list_len(info.get("sealed_segments")),
        "persisted_count": _list_len(info.get("persisted_files")),
        "persist_event_count": _list_len(info.get("persist_events")),
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


def _gateway_summary(client: object) -> dict[str, object]:
    get_config = getattr(client, "get_config", None)
    config: dict[str, object] = {}
    if get_config is not None:
        try:
            loaded_config = get_config()
        except Exception as error:
            return _gateway_unavailable(str(error))
        if isinstance(loaded_config, dict):
            config = loaded_config

    gateway = config.get("gateway", {})
    if not isinstance(gateway, dict):
        gateway = {}
    enabled = gateway.get("enabled", True)
    endpoint = gateway.get("endpoint")
    protocol_version = gateway.get("protocol_version")
    if enabled is False or not endpoint:
        return {
            "status": "ok",
            "healthy": True,
            "enabled": bool(enabled) if enabled is not None else True,
            "endpoint": endpoint,
            "protocol_version": protocol_version,
            "metrics": {},
            "error": None,
        }

    metrics: dict[str, object] = {}
    gateway_metrics = getattr(client, "gateway_metrics", None)
    if gateway_metrics is not None:
        try:
            loaded_metrics = gateway_metrics()
        except Exception as error:
            return {
                "status": "error",
                "healthy": False,
                "enabled": True,
                "endpoint": endpoint,
                "protocol_version": protocol_version,
                "metrics": {},
                "error": str(error),
            }
        if isinstance(loaded_metrics, dict):
            metrics = loaded_metrics
    return {
        "status": "ok",
        "healthy": True,
        "enabled": True,
        "endpoint": endpoint,
        "protocol_version": protocol_version,
        "metrics": metrics,
        "error": None,
    }


def _gateway_unavailable(error: str) -> dict[str, object]:
    return {
        "status": "error",
        "healthy": False,
        "enabled": None,
        "endpoint": None,
        "protocol_version": None,
        "metrics": {},
        "error": error,
    }


def _pm_summary() -> dict[str, object]:
    try:
        tasks = [_pm_task_summary(task) for task in PmCommandRunner.default().list_tasks()]
    except Exception as error:
        return {
            "status": "error",
            "error": str(error),
            "task_count": 0,
            "tasks": [],
        }
    return {
        "status": "ok",
        "error": None,
        "task_count": len(tasks),
        "tasks": tasks,
    }


def _pm_task_summary(task: PmTaskInfo) -> dict[str, object]:
    return {
        "kind": task.run_mode or "pm",
        "name": task.name,
        "status": task.status,
        "process_id": task.pid,
        "input_stream": None,
        "output_stream": None,
        "writer_epoch": None,
        "metrics": {
            "task_id": task.task_id,
            "health": task.health,
            "uptime_ms": task.uptime_ms,
            "memory_bytes": task.memory_bytes,
            "restart_count": task.restart_count,
            "last_exit_code": task.last_exit_code,
            "cpu_percent": task.cpu_percent,
            "schedule_state": task.schedule_state,
            "started_at": task.started_at,
            "stopped_at": task.stopped_at,
        },
        "cmd": task.cmd,
        "cwd": task.cwd,
        "dependencies": task.dependencies or [],
        "dependents": task.dependents or [],
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
    ]:
        for item in _dict_items(snapshot.get(key)):
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
    rows: list[tuple[tuple[float, str, int], dict[str, object]]] = []
    for path in files[:3]:
        for line_index, line in enumerate(_tail_lines(path, 80)):
            row = _parse_log_line(line, path)
            rows.append((_log_sort_key(row, path, line_index), row))
    rows.sort(key=lambda item: item[0])
    return [row for _, row in rows[-160:]]


def _parse_log_line(line: str, path: Path) -> dict[str, object]:
    try:
        payload = json.loads(line)
        if isinstance(payload, dict):
            payload.setdefault("log_file", str(path))
            redacted = _redact_log_payload(payload)
            return redacted if isinstance(redacted, dict) else {}
    except json.JSONDecodeError:
        pass
    return {"message": line, "log_file": str(path)}


def _redact_log_payload(value: object) -> object:
    if isinstance(value, dict):
        result: dict[str, object] = {}
        for key, item in value.items():
            key_text = str(key)
            if key_text.lower() in SENSITIVE_LOG_FIELD_NAMES:
                result[key_text] = "[redacted]"
            else:
                result[key_text] = _redact_log_payload(item)
        return result
    if isinstance(value, list):
        return [_redact_log_payload(item) for item in value]
    return value


def _tail_lines(path: Path, limit: int) -> list[str]:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            lines = handle.readlines()
    except OSError:
        return []
    return [line.rstrip("\n") for line in lines[-limit:] if line.strip()]


def _log_sort_key(row: dict[str, object], path: Path, line_index: int) -> tuple[float, str, int]:
    timestamp = _log_timestamp(row)
    return (timestamp if timestamp is not None else _mtime(path), str(path), line_index)


def _log_timestamp(row: dict[str, object]) -> float | None:
    for key in ("timestamp", "time", "ts"):
        timestamp = _parse_timestamp(row.get(key))
        if timestamp is not None:
            return timestamp
    return None


def _parse_timestamp(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return _normalize_numeric_timestamp(float(value))
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return _normalize_numeric_timestamp(float(text))
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _normalize_numeric_timestamp(value: float) -> float:
    if value > 1_000_000_000_000_000:
        return value / 1_000_000_000
    if value > 1_000_000_000_000:
        return value / 1_000
    return value


def _load_registry_snapshot(uri: str) -> dict[str, object]:
    path = _registry_snapshot_path(uri)
    result: dict[str, object] = {
        "path": str(path) if path is not None else None,
        "loaded": False,
        "streams": [],
        "sources": [],
        "engines": [],
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
    return _dict_items(schema.get("fields"))


def _table_row_count(info: dict[str, object]) -> int:
    total = _descriptor_rows(info.get("active_segment_descriptor"))
    for segment in _dict_items(info.get("sealed_segments")):
        total += _descriptor_rows(segment)
    return total


def _dict_items(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _list_len(value: object) -> int:
    return len(value) if isinstance(value, list) else 0


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


def _json_safe_value(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(item) for item in value]
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    return str(value)


def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def webui_url(host: str, port: int) -> str:
    display_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    return f"http://{display_host}:{port}/"
