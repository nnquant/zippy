"""Build and install both distributions from archives outside the Git checkout.

:param --work-dir: An empty directory for archives, build cache, wheels and the test environment.
:param --client-only: Build and test only the lightweight distribution, without Rust or zippy.
:param --arrow-version: Optional exact PyArrow version for the isolated client test matrix.
:param --offline: Require cached Python and Cargo dependencies.
:raises SystemExit: If a build, installation or isolated runtime check fails.
:example: uv run --no-sync scripts/check_distributions.py --work-dir /tmp/zippy-release-check
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import zipfile


def run(command: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    """Run one checked build command with an explicit directory and environment."""
    subprocess.run(command, cwd=cwd, env=env, check=True)


def main() -> None:
    """Verify archive builds and imports in a fresh virtual environment."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--client-only", action="store_true", help="verify without Rust or server")
    parser.add_argument("--arrow-version", help="explicit client test dependency matrix version")
    parser.add_argument(
        "--offline", action="store_true", help="use cached Python dependencies only"
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    work = args.work_dir.resolve()
    if work.exists() and any(work.iterdir()):
        raise SystemExit("distribution check work directory must be empty")
    work.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env["PYO3_PYTHON"] = sys.executable
    # Keep archive artifacts separate from the checkout's cached build metadata.
    env["CARGO_TARGET_DIR"] = str(work / "target")
    if args.offline:
        env["CARGO_NET_OFFLINE"] = "true"
    network = ["--offline"] if args.offline else []
    wheels = work / "wheels"
    projects = [("client", root / "clients/python")]
    if not args.client_only:
        projects.insert(0, ("server", root))
    for name, project in projects:
        archives = work / name
        archives.mkdir()
        run(
            ["uv", "build", *network, "--sdist", "--out-dir", str(archives), str(project)],
            cwd=project,
            env=env,
        )
        archive = next(archives.glob("*.tar.gz"))
        with tarfile.open(archive) as source:
            if name == "client" and any(
                member.name.endswith(("Cargo.toml", ".so", ".pyd", ".pyc")) or "_shm" in member.name
                for member in source.getmembers()
            ):
                raise SystemExit("client source archive contains retired native artifacts")
            source.extractall(archives, filter="data")
        extracted = next(path for path in archives.iterdir() if path.is_dir())
        if any(extracted.rglob(".git")):
            raise SystemExit("source distribution unexpectedly contains Git metadata")
        run(
            [
                "uv",
                "build",
                *network,
                "--wheel",
                "--python",
                sys.executable,
                "--out-dir",
                str(wheels),
                str(extracted),
            ],
            cwd=extracted,
            env=env,
        )
    client_wheel = next(wheels.glob("zippy_client-*.whl"))
    if not client_wheel.name.endswith("-py3-none-any.whl"):
        raise SystemExit("lightweight client wheel is not platform independent")
    with zipfile.ZipFile(client_wheel) as archive:
        names = set(archive.namelist())
        required = {
            "zippy_client/py.typed",
            "zippy_client/row.py",
            "zippy_client/flight_subscriber.py",
            "zippy_client/snapshot.py",
            "zippy_client/_generated/protocol_pb2.py",
            "zippy_client/_generated/protocol_pb2.pyi",
        }
        if not required <= names or any(
            name.endswith((".so", ".pyd", ".dll", ".pyc")) or "_shm" in name for name in names
        ):
            raise SystemExit("client wheel contains missing contracts or retired native artifacts")
        metadata = archive.read(next(name for name in names if name.endswith("/WHEEL"))).decode()
        if "Root-Is-Purelib: true" not in metadata:
            raise SystemExit("client wheel does not declare pure Python installation")
    client_venv = work / "client-venv"
    run(["uv", "venv", str(client_venv), "--python", sys.executable], cwd=work, env=env)
    client_python = client_venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    run(
        [
            "uv",
            "pip",
            "install",
            *network,
            "--python",
            str(client_python),
            str(client_wheel),
            "pytest>=8.3",
            "pytest-cov>=6",
            *([f"pyarrow=={args.arrow_version}"] if args.arrow_version else []),
        ],
        cwd=work,
        env=env,
    )
    run(
        [
            str(client_python),
            "-I",
            "-c",
            "import importlib.util as u; import zippy_client as c; "
            "assert u.find_spec('zippy') is None; assert u.find_spec('zippy_client._shm') is None; "
            "assert callable(c.Client) and callable(c.FlightSubscriber) and callable(c.FixedSnapshot); "
            "assert not hasattr(c, 'GatewaySubscriber') and not hasattr(c, 'ShmSubscriber'); "
            "assert u.find_spec('zippy_client.transports.gateway') is None; "
            "assert u.find_spec('zippy_client.protocol') is None; "
            "assert not hasattr(c, 'GatewayWriter'); "
            "print('isolated pure Python client verified', c.__file__)",
        ],
        cwd=work,
        env=env,
    )
    run(
        [
            str(client_python),
            "-I",
            "-m",
            "pytest",
            "--import-mode=importlib",
            "-c",
            str(root / "clients/python/pyproject.toml"),
            str(root / "clients/python/tests"),
            "-q",
        ],
        cwd=work,
        env=env,
    )
    if args.client_only:
        if (work / "target").exists():
            raise SystemExit(
                "pure Python client verification unexpectedly produced Cargo artifacts"
            )
        return
    venv = work / "venv"
    run(["uv", "venv", str(venv), "--python", sys.executable], cwd=work, env=env)
    python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    run(
        [
            "uv",
            "pip",
            "install",
            *network,
            "--python",
            str(python),
            *map(str, wheels.glob("*.whl")),
            *([f"pyarrow=={args.arrow_version}"] if args.arrow_version else []),
        ],
        cwd=work,
        env=env,
    )
    run(
        [
            str(python),
            "-I",
            "-c",
            "import importlib.metadata as m; import importlib.util as u; import zippy, zippy_client; "
            "assert zippy.__version__ == m.version('zippy'); "
            "assert zippy.__build_revision__ is None; "
            "assert not any('rspm' in item for item in m.requires('zippy')); "
            "assert callable(zippy_client.Client); "
            "assert zippy.Client is zippy_client.Client and not hasattr(zippy, 'read_sql'); "
            "assert callable(zippy.NativeProcessor) and callable(zippy.ComputePipeline); "
            "assert not hasattr(zippy, 'run_master_daemon'); "
            "assert not any(hasattr(zippy, name) for name in "
            "('connect', 'master', 'ops', 'Table', 'Session', 'Pipeline', 'read_table', "
            "'get_writer', 'subscribe', 'subscribe_table', 'GatewayServer', 'MasterServer')); "
            "assert zippy.TS_EMA.__module__ == 'zippy.factors'; "
            "assert zippy.compare_replay.__module__ == 'zippy.replay_compare'; "
            "assert u.find_spec('zippy.cli_gateway') is None; "
            "assert u.find_spec('zippy.remote_gateway_perf') is None; "
            "print('isolated distributions verified', zippy.__version__, zippy.__file__)",
        ],
        cwd=work,
        env=env,
    )
    run([str(python), "-I", "-m", "zippy", "server", "run", "--help"], cwd=work, env=env)
    help_result = subprocess.run(
        [str(python), "-I", "-m", "zippy", "--help"],
        cwd=work,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    if "gateway" in help_result.stdout.lower():
        raise SystemExit("installed CLI still advertises the retired gateway command")
    run(
        [str(python), "-I", str(root / "scripts/check_native_client.py"), "--installed-server"],
        cwd=work,
        env=env,
    )


if __name__ == "__main__":
    main()
