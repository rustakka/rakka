#!/usr/bin/env bash
# Dashboard dev loop — runs the axum backend on :9100 (with cargo-watch
# if available) and the Vite dev server on :5173 concurrently. The Vite
# config proxies /api and /ws to 9100 so the browser hits a single
# origin at http://localhost:5173.
#
# Press Ctrl-C once to tear both down.

set -euo pipefail

BIND="${ATOMR_DASHBOARD_BIND:-127.0.0.1:9100}"
NODE="${ATOMR_DASHBOARD_NODE:-dev}"
FEATURES="${ATOMR_DASHBOARD_FEATURES:-bin,aggregator,metrics-prometheus}"

here="$(cd "$(dirname "$0")/.." && pwd)"
cd "$here"

backend_cmd=(cargo run -q -p atomr-dashboard --features "$FEATURES" -- \
    --bind "$BIND" --node "$NODE")

if command -v cargo-watch >/dev/null 2>&1; then
    watch_args=("${backend_cmd[@]:1}")
    backend_cmd=(cargo watch -q -c -w crates -w xtask -x "${watch_args[*]}")
fi

"${backend_cmd[@]}" &
BACKEND_PID=$!

pushd crates/atomr-dashboard/ui >/dev/null
if command -v pnpm >/dev/null 2>&1; then
    pnpm dev &
elif command -v npm >/dev/null 2>&1; then
    npm run dev &
else
    echo "atomr: neither pnpm nor npm found on PATH; install one to run the Vite dev server" >&2
    kill "$BACKEND_PID" 2>/dev/null || true
    exit 1
fi
FRONTEND_PID=$!
popd >/dev/null

trap 'kill $BACKEND_PID $FRONTEND_PID 2>/dev/null || true' INT TERM EXIT
wait
