"""
GatewayServer management commands for the zippy CLI.
"""

from __future__ import annotations

import json
import signal
import subprocess
import sys
import threading

import click

import zippy

from .cli_common import DEFAULT_CONTROL_ENDPOINT, cli_error, echo_json


@click.group("gateway")
def gateway_group() -> None:
    """
    Manage the cross-platform GatewayServer data plane.
    """


def _remote_uri_from_master_uri(master_uri: str) -> str:
    if master_uri.startswith("tcp://"):
        return f"zippy://{master_uri.removeprefix('tcp://')}/default"
    return master_uri


def run_gateway_smoke(
    *,
    master_uri: str,
    gateway_endpoint: str,
    token: str | None,
    stream_name: str,
    timeout_sec: float,
) -> dict[str, object]:
    """
    Run an end-to-end smoke with a child process acting as a remote client.

    :param master_uri: TCP master URI used by the local smoke master.
    :type master_uri: str
    :param gateway_endpoint: GatewayServer endpoint advertised by master config.
    :type gateway_endpoint: str
    :param token: Optional GatewayServer access token.
    :type token: str | None
    :param stream_name: Temporary stream name used by the probe.
    :type stream_name: str
    :param timeout_sec: Child process timeout in seconds.
    :type timeout_sec: float
    :returns: Probe result emitted by the remote child process.
    :rtype: dict[str, object]
    :raises RuntimeError: If the child process fails.
    """
    server = zippy.MasterServer(
        uri=master_uri,
        config={
            "remote_gateway": {
                "enabled": True,
                "endpoint": gateway_endpoint,
                "token": token,
                "protocol_version": 1,
            }
        },
    )
    server.start()
    gateway = zippy.GatewayServer(
        endpoint=gateway_endpoint,
        master=zippy.MasterClient(uri=master_uri),
        token=token,
    ).start()
    remote_uri = _remote_uri_from_master_uri(master_uri)
    child_code = f"""
import json
import pyarrow as pa
import zippy as zp

schema = pa.schema([
    ("instrument_id", pa.string()),
    ("last_price", pa.float64()),
])
zp.connect({remote_uri!r}, app="gateway_smoke_child")
writer = zp.get_writer({stream_name!r}, schema=schema, batch_size=1)
writer.write({{"instrument_id": "IF2606", "last_price": 4102.5}})
writer.close()
table = (
    zp.read_table({stream_name!r})
    .filter(zp.col("instrument_id") == "IF2606")
    .select("instrument_id", "last_price")
    .collect()
)
print(json.dumps({{"stream_name": {stream_name!r}, "rows": table.num_rows, "data": table.to_pydict()}}, sort_keys=True))
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", child_code],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        if result.returncode != 0:
            raise RuntimeError(
                "gateway smoke child failed "
                f"returncode=[{result.returncode}] stderr=[{result.stderr.strip()}]"
            )
        output = result.stdout.strip().splitlines()
        if not output:
            raise RuntimeError("gateway smoke child produced no output")
        payload = json.loads(output[-1])
        payload["gateway_metrics"] = gateway.metrics()
        return payload
    finally:
        gateway.stop()
        server.stop()
        server.join()


def run_gateway_smoke_client(
    *,
    uri: str,
    stream_name: str,
) -> dict[str, object]:
    """
    Run a client-only Gateway smoke against an existing remote master/Gateway.

    :param uri: Remote master URI, usually ``zippy://host:port/default``.
    :type uri: str
    :param stream_name: Temporary stream name used by the probe.
    :type stream_name: str
    :returns: Probe result with row count and data.
    :rtype: dict[str, object]
    """
    import pyarrow as pa

    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    zippy.connect(uri, app="gateway_smoke_client")
    writer = zippy.get_writer(stream_name, schema=schema, batch_size=1)
    writer.write({"instrument_id": "IF2606", "last_price": 4102.5})
    writer.close()
    table = (
        zippy.read_table(stream_name)
        .filter(zippy.col("instrument_id") == "IF2606")
        .select("instrument_id", "last_price")
        .collect()
    )
    return {
        "stream_name": stream_name,
        "rows": table.num_rows,
        "data": table.to_pydict(),
    }


@gateway_group.command("run")
@click.option(
    "--uri",
    default=DEFAULT_CONTROL_ENDPOINT,
    show_default=True,
    help="local zippy-master URI used by GatewayServer",
)
@click.option(
    "--endpoint",
    default="127.0.0.1:17666",
    show_default=True,
    help="GatewayServer listen endpoint",
)
@click.option("--token", default=None, help="remote client access token")
@click.option(
    "--max-write-rows",
    type=int,
    default=None,
    help="maximum rows accepted in one remote write_batch",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="emit JSON metrics on stop")
@click.option("--once", is_flag=True, hidden=True)
def run_gateway(
    uri: str,
    endpoint: str,
    token: str | None,
    max_write_rows: int | None,
    as_json: bool,
    once: bool,
) -> None:
    """
    Run a GatewayServer for remote writer/subscriber/query clients.
    """
    gateway = None
    try:
        master = zippy.connect(uri=uri, app="gateway_server")
        gateway = zippy.GatewayServer(
            endpoint=endpoint,
            master=master,
            token=token,
            max_write_rows=max_write_rows,
        ).start()
        click.echo(f"gateway started endpoint=[{gateway.endpoint}]")
        if once:
            return

        stop_event = threading.Event()

        def request_stop(signum, frame) -> None:
            del signum, frame
            stop_event.set()

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)
        stop_event.wait()
    except KeyboardInterrupt:
        return
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    finally:
        if gateway is not None:
            metrics = gateway.metrics()
            gateway.stop()
            if as_json:
                echo_json(metrics)


@gateway_group.command("smoke")
@click.option(
    "--master-uri",
    default="tcp://127.0.0.1:17690",
    show_default=True,
    help="temporary TCP master URI used by the smoke",
)
@click.option(
    "--gateway-endpoint",
    default="127.0.0.1:17666",
    show_default=True,
    help="temporary GatewayServer endpoint used by the smoke",
)
@click.option("--token", default=None, help="GatewayServer access token")
@click.option("--stream", "stream_name", default="gateway_smoke_ticks", show_default=True)
@click.option("--timeout-sec", type=float, default=15.0, show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, help="emit JSON output")
def smoke_gateway(
    master_uri: str,
    gateway_endpoint: str,
    token: str | None,
    stream_name: str,
    timeout_sec: float,
    as_json: bool,
) -> None:
    """
    Run a cross-process GatewayServer writer/query smoke.
    """
    try:
        result = run_gateway_smoke(
            master_uri=master_uri,
            gateway_endpoint=gateway_endpoint,
            token=token,
            stream_name=stream_name,
            timeout_sec=timeout_sec,
        )
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
        cli_error(str(error))
    if as_json:
        echo_json(result)
    else:
        click.echo(
            "gateway smoke ok "
            f"stream_name=[{result['stream_name']}] rows=[{result['rows']}]"
        )


@gateway_group.command("smoke-client")
@click.option(
    "--uri",
    required=True,
    help="existing remote master URI, for example zippy://wsl-host:17690/default",
)
@click.option("--stream", "stream_name", default="gateway_smoke_ticks", show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False, help="emit JSON output")
def smoke_gateway_client(uri: str, stream_name: str, as_json: bool) -> None:
    """
    Run a client-only Gateway smoke against an existing remote master.
    """
    try:
        result = run_gateway_smoke_client(uri=uri, stream_name=stream_name)
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    if as_json:
        echo_json(result)
    else:
        click.echo(
            "gateway smoke-client ok "
            f"stream_name=[{result['stream_name']}] rows=[{result['rows']}]"
        )
