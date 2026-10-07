"""Verify that the Python distribution embeds the current console build.

:raises SystemExit: If a bundled file is missing, extra or stale.
:example: npm --prefix app/zippy-console run build && uv run scripts/check_frontend_bundle.py
"""

from pathlib import Path


def main() -> None:
    """Compare all built frontend paths and bytes with the packaged static assets."""
    root = Path(__file__).resolve().parents[1]
    built = root / "app/zippy-console/dist"
    packaged = root / "python/zippy/webui_static"
    if not (built / "index.html").is_file():
        raise SystemExit("console build is missing; run npm build first")
    expected = {
        path.relative_to(built): path.read_bytes() for path in built.rglob("*") if path.is_file()
    }
    actual = {
        path.relative_to(packaged): path.read_bytes()
        for path in packaged.rglob("*")
        if path.is_file()
    }
    drift = sorted(
        str(path)
        for path in expected.keys() | actual.keys()
        if expected.get(path) != actual.get(path)
    )
    if drift:
        raise SystemExit("packaged frontend differs from build: " + ", ".join(drift))
    print(f"frontend bundle verified files=[{len(expected)}]")


if __name__ == "__main__":
    main()
