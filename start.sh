#!/bin/sh
set -eu
node /opt/bgutil/server/build/main.js --host 127.0.0.1 --port 4416 &
POT_PID=$!
trap 'kill "$POT_PID" 2>/dev/null || true' EXIT INT TERM
exec uvicorn app:app --host 0.0.0.0 --port "${PORT:-9000}" --workers 1
