#!/usr/bin/env python3
"""Fail unless the child image kept Agent entrypoint/cmd and UID 1000."""

from __future__ import annotations

import json
import os
import subprocess
import sys


def inspect(image: str, field: str):
    output = subprocess.check_output(
        ["docker", "image", "inspect", "--format", "{{json " + field + "}}", image],
        text=True,
    ).strip()
    return json.loads(output)


def main() -> int:
    agent = os.environ.get("AGENT_IMAGE", "")
    child = os.environ.get("TEST_IMAGE", "")
    if not agent or not child:
        raise SystemExit("AGENT_IMAGE and TEST_IMAGE are required")
    for field in (".Config.User", ".Config.Entrypoint", ".Config.Cmd"):
        parent = inspect(agent, field)
        current = inspect(child, field)
        print(f"{field}: parent={parent!r} child={current!r}", flush=True)
        if field == ".Config.User":
            if current != "1000:1000":
                raise SystemExit("child runtime user must be 1000:1000")
            continue
        if parent != current:
            raise SystemExit(f"inherited {field} mismatch")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
