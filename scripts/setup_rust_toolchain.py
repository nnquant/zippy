"""Install the repository's pinned Rust toolchain for CI.

The toolchain file is the sole version source for local and CI builds.
:example: python scripts/setup_rust_toolchain.py
"""

from pathlib import Path
import subprocess
import tomllib


def main() -> None:
    """Install the channel and components declared in rust-toolchain.toml."""
    root = Path(__file__).resolve().parents[1]
    with (root / "rust-toolchain.toml").open("rb") as handle:
        toolchain = tomllib.load(handle)["toolchain"]
    command = ["rustup", "toolchain", "install", toolchain["channel"], "--profile", "minimal"]
    for component in toolchain.get("components", []):
        command.extend(["--component", component])
    subprocess.run(command, check=True, cwd=root)
    subprocess.run(["rustup", "show", "active-toolchain"], check=True, cwd=root)


if __name__ == "__main__":
    main()
