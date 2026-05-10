#!/usr/bin/env bash
set -euo pipefail

python_bin="${PYO3_PYTHON:-.venv/bin/python}"
if [[ -x "${python_bin}" ]]; then
    python_libdir="$("${python_bin}" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
    if [[ -n "${python_libdir}" ]]; then
        export LD_LIBRARY_PATH="${python_libdir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
    fi
fi

exec "$@"
