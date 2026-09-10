#!/bin/sh
set -eu
: "${LISTEN_PORT:?}"
: "${TARGET:?}"
if [ -n "${LOCK_FILE:-}" ] && [ -f "$LOCK_FILE" ]; then
  while true; do
    printf 'HTTP/1.1 423 Locked\r\nConnection: close\r\nContent-Length: 6\r\n\r\nlocked' | nc -l -p "$LISTEN_PORT" || true
  done
fi
exec socat TCP-LISTEN:"${LISTEN_PORT}",fork,reuseaddr TCP:"${TARGET}"
