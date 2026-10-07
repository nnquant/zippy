"""
Lightweight local Web UI for Zippy runtime observability.
"""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import mimetypes
import os
from pathlib import Path
import time
from typing import cast

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, Response
import uvicorn

import pyarrow as pa
from zippy_client import Client

from .native_inspection import inspect_table, query_snapshots, schema_info
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
        namespace: str = Query(default="default"),
        mode: str = Query(default="head", pattern="^(head|tail)$"),
        order_by: list[str] | None = Query(default=None),
        descending: bool = Query(default=False),
        limit: int = Query(default=100, ge=1, le=1000),
        uri: str | None = Query(default=None),
    ) -> dict[str, object]:
        return _table_data_preview(
            table_name=table_name,
            uri=uri or config.uri,
            namespace=namespace,
            mode=mode,
            limit=limit,
            order_by=order_by,
            descending=descending,
        )

    @app.get("/api/table/{table_name:path}")
    def table_detail(
        table_name: str,
        namespace: str = Query(default="default"),
        uri: str | None = Query(default=None),
    ) -> dict[str, object]:
        return _table_detail(table_name, uri or config.uri, namespace)

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
        logs = _read_recent_logs(self.config.log_dir)
        catalog: dict[str, object] = {"status": "ok", "error": None}
        try:
            with Client(query_uri) as client:
                tables = client.list_tables()
            tables.sort(key=lambda item: (str(item["namespace"]), str(item["name"])))
        except Exception as error:
            tables = []
            catalog = {"status": "error", "error": str(error)}
        pm = _pm_summary()
        tasks = cast(list[dict[str, object]], pm["tasks"])
        return {
            "generated_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "webui": {
                "pid": os.getpid(),
                "uptime_sec": int(now - self.started_at),
                "host": self.config.host,
                "port": self.config.port,
            },
            "catalog": catalog,
            "pm": pm,
            "totals": {
                "tables": len(tables),
                "namespaces": len({str(table["namespace"]) for table in tables}),
                "tasks": len(tasks),
            },
            "tasks": tasks,
            "tables": tables,
            "log_tail": logs[-80:],
        }


def _table_detail(table_name: str, uri: str, namespace: str) -> dict[str, object]:
    try:
        with Client(uri) as client:
            return {"ok": True, "table": inspect_table(client, table_name, namespace)}
    except Exception as error:
        return {"ok": False, "error": str(error), "table_name": table_name, "namespace": namespace}


def _table_data_preview(
    *,
    table_name: str,
    uri: str,
    namespace: str,
    mode: str,
    limit: int,
    order_by: list[str] | None,
    descending: bool,
) -> dict[str, object]:
    identity: dict[str, object] = {
        "table_name": table_name,
        "namespace": namespace,
        "mode": mode,
        "limit": limit,
        "order_by": order_by or [],
        "descending": descending,
    }
    try:
        if mode not in {"head", "tail"} or not 1 <= limit <= 1000:
            raise ValueError("preview requires head or tail mode and a limit between 1 and 1000")
        if mode == "tail" and not order_by:
            raise ValueError("tail preview requires explicit order_by columns")
        with ExitStack() as resources:
            client = resources.enter_context(Client(uri))
            query = resources.enter_context(
                client.read_table(table_name, namespace=namespace, snapshot=True)
            )
            if order_by:
                if any(name not in query.schema().names for name in order_by):
                    raise ValueError("order_by contains an unknown column")
                query = resources.enter_context(query.sort(order_by, descending=descending))
            selected = resources.enter_context(
                query.head(limit + 1) if mode == "head" else query.tail(limit + 1)
            )
            profile = selected.profile()
            table = profile["result"]
            if not isinstance(table, pa.Table):
                raise RuntimeError("native preview did not return an Arrow table")
            snapshots = query_snapshots(profile)
            truncated = table.num_rows > limit
            if truncated:
                table = table.slice(0 if mode == "head" else 1, limit)
            # Arrow string casts preserve nanoseconds without an optional pandas conversion.
            columns = [
                column.cast(pa.string()) if pa.types.is_timestamp(field.type) else column
                for field, column in zip(table.schema, table.columns, strict=True)
            ]
            display_table = pa.Table.from_arrays(columns, names=table.column_names)
            rows = [
                {key: _json_safe_value(value) for key, value in row.items()}
                for row in display_table.to_pylist()
            ]
            return {
                **identity,
                "ok": True,
                "columns": table.column_names,
                "schema": schema_info(table.schema),
                "snapshots": _json_safe_value(snapshots),
                "rows": rows,
                "row_count": len(rows),
                "truncated": truncated,
                "error": None,
            }
    except Exception as error:
        return {
            **identity,
            "ok": False,
            "columns": [],
            "rows": [],
            "row_count": 0,
            "truncated": False,
            "error": str(error),
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


def _read_recent_logs(log_dir: Path) -> list[dict[str, object]]:
    files = sorted(log_dir.glob("**/*.jsonl"), key=_mtime, reverse=True)[:3]
    rows: list[tuple[tuple[float, str, int], dict[str, object]]] = []
    for path in files:
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


def _json_safe_value(value: object) -> object:
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) > 2**53 - 1:
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
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
