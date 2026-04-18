# Source this file: `source scripts/dev-env.sh`.
#
# Sets up a local Python/PyO3 dev environment without touching anything
# outside the repo:
#
#   1. creates `.venv/` if missing (python -m venv)
#   2. activates it
#   3. installs `maturin` + `pytest` + `pytest-asyncio` on first run
#   4. exports `PYO3_CONFIG_FILE` if `.cargo/pyo3-config.txt` exists
#
# After sourcing, run `maturin develop --release` to build the native
# extension into the virtualenv.
#
# Idempotent: safe to source multiple times.

_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ ! -d "$_repo_root/.venv" ]; then
    echo "→ creating virtualenv at $_repo_root/.venv"
    python3 -m venv "$_repo_root/.venv"
fi

# shellcheck disable=SC1091
. "$_repo_root/.venv/bin/activate"

if ! python -c 'import maturin' >/dev/null 2>&1; then
    echo "→ installing maturin + pytest + pytest-asyncio"
    python -m pip install --quiet --upgrade pip
    python -m pip install --quiet maturin pytest pytest-asyncio msgpack
fi

if [ -f "$_repo_root/.cargo/pyo3-config.txt" ]; then
    export PYO3_CONFIG_FILE="$_repo_root/.cargo/pyo3-config.txt"
    echo "→ PYO3_CONFIG_FILE=$PYO3_CONFIG_FILE"
fi

unset _repo_root
