#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$ROOT_DIR/build"
DEPS_DIR="$BUILD_DIR/test-deps"
PYCACHE_DIR="$BUILD_DIR/pycache"
PYTEST_CACHE_DIR="$BUILD_DIR/pytest-cache"
COVERAGE_DIR="$BUILD_DIR/coverage"
TMP_WORK_DIR="$BUILD_DIR/tmp"
PYTHON_BIN="${PYTHON:-python3}"

rm -rf "$DEPS_DIR" "$PYCACHE_DIR" "$PYTEST_CACHE_DIR" "$COVERAGE_DIR" "$TMP_WORK_DIR"
mkdir -p "$DEPS_DIR" "$PYCACHE_DIR" "$PYTEST_CACHE_DIR" "$COVERAGE_DIR" "$TMP_WORK_DIR"

export PYTHONPYCACHEPREFIX="$PYCACHE_DIR"
export PYTHONDONTWRITEBYTECODE=1
export PYTEST_ADDOPTS="${PYTEST_ADDOPTS:-} -o cache_dir=$PYTEST_CACHE_DIR"
export COVERAGE_FILE="$COVERAGE_DIR/.coverage"
export PIP_NO_CACHE_DIR=1
export PIP_NO_COMPILE=1
export PIP_DISABLE_PIP_VERSION_CHECK=1
export TMPDIR="$TMP_WORK_DIR"

cd "$ROOT_DIR"

"$PYTHON_BIN" - <<'PY'
import importlib.metadata
import importlib.util
import sys

if importlib.util.find_spec("openevent.sdk") is None:
    print("missing Python dependency in the current environment: openevent-sdk>=0.3.0", file=sys.stderr)
    sys.exit(2)
try:
    version = importlib.metadata.version("openevent-sdk")
except importlib.metadata.PackageNotFoundError:
    print("missing Python dependency in the current environment: openevent-sdk>=0.3.0", file=sys.stderr)
    sys.exit(2)
parts = tuple(int(part) for part in version.split(".")[:3] if part.isdigit())
if parts < (0, 3, 0):
    print(f"openevent-sdk>=0.3.0 is required, found {version}", file=sys.stderr)
    sys.exit(2)
PY

"$PYTHON_BIN" -m pip install -q --upgrade --no-compile --target "$DEPS_DIR" pytest PyYAML
export PYTHONPATH="$ROOT_DIR/src:$DEPS_DIR${PYTHONPATH:+:$PYTHONPATH}"
"$PYTHON_BIN" -m pytest tests "$@"

printf 'test artifacts: %s\n' "$BUILD_DIR"
