#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_NAMES = {"auth.json", "credentials.json", "sessions", "memories", "logs"}
ALLOWED_BINARIES: dict[str, str] = {}
FORBIDDEN_PATTERNS = {
    "broker reference": re.compile("o" + "p://", re.IGNORECASE),
    "private key": re.compile("BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY"),
    "private topology": re.compile(r"(?:10\.\d+\.\d+\.\d+|192\.168\.\d+\.\d+|172\.(?:1[6-9]|2\d|3[01])\.\d+\.\d+)"),
}


def forbidden_part(part: str) -> bool:
    return part == ".env" or part.startswith(".env.") or part in FORBIDDEN_NAMES


def paths() -> list[Path]:
    visible = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    ignored = subprocess.run(
        ["git", "ls-files", "--others", "--ignored", "--exclude-standard"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if visible.returncode == 0 and ignored.returncode == 0:
        names = set(visible.stdout.splitlines())
        for line in ignored.stdout.splitlines():
            if any(forbidden_part(part) for part in Path(line).parts):
                names.add(line)
        if names:
            return [ROOT / line for line in sorted(names)]
    return [p for p in ROOT.rglob("*") if p.is_file() and ".git" not in p.parts]


def main() -> int:
    candidates = paths()
    errors: list[str] = []
    for path in candidates:
        relative = path.relative_to(ROOT)
        if any(forbidden_part(part) for part in relative.parts):
            errors.append(f"forbidden path: {relative}")
        if path.is_symlink():
            errors.append(f"symlink requires review: {relative}")
            continue
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            expected = ALLOWED_BINARIES.get(relative.as_posix())
            if expected is None:
                errors.append(f"binary file requires dedicated scanning: {relative}")
                continue
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != expected:
                errors.append(f"binary hash mismatch: {relative}")
            continue
        for label, pattern in FORBIDDEN_PATTERNS.items():
            if pattern.search(text):
                errors.append(f"{label}: {relative}")
    if errors:
        for error in sorted(errors):
            print(error)
        return 1
    print(f"public tree policy: OK ({len(candidates)} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
