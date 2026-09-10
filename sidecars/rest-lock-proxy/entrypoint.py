"""Container configuration boundary for the REST takeover lock proxy."""
from __future__ import annotations

import os
from pathlib import Path

from lock_proxy import RestLockProxy


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} must be set")
    return value


def main() -> None:
    port_text = required("LISTEN_PORT")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise SystemExit("LISTEN_PORT must be an integer") from exc
    if not 1 <= port <= 65535:
        raise SystemExit("LISTEN_PORT must be between 1 and 65535")
    RestLockProxy(
        listen=("0.0.0.0", port),
        target=required("TARGET"),
        lock_file=Path(required("LOCK_FILE")),
    ).serve_forever()


if __name__ == "__main__":
    main()
