#!/usr/bin/env python3
"""Extract the unique manifest digest from Docker's push summary on stdin."""

from __future__ import annotations

import re
import sys


DIGEST_PATTERN = re.compile(
    r"(?m)^[^\r\n]*:\s+digest:\s*(sha256:[0-9a-f]{64})\s+size:\s*\d+\s*$"
)


def main() -> int:
    matches = DIGEST_PATTERN.findall(sys.stdin.read())
    if len(matches) != 1:
        print(
            f"expected exactly one Docker push summary digest, found {len(matches)}",
            file=sys.stderr,
        )
        return 1
    print(matches[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
