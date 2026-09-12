"""Allowlisted read-only source snapshot tools."""
from __future__ import annotations

import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any


MAX_FILE_BYTES = 512 * 1024
MAX_LIST_FILES = 2_000
MAX_SEARCH_FILES = 5_000
MAX_SEARCH_MATCHES = 100
MAX_SEARCH_BYTES = 128 * 1024 * 1024
MAX_SEARCH_SECONDS = 5.0
MAX_READ_LINES = 500
MAX_QUERY_CHARS = 256
MAX_SNIPPET_CHARS = 300
_SNAPSHOT_NAME = ".hermes-snapshot.json"
_PUBLISHING_NAME = ".hermes-publishing"


class ReadonlySourceSnapshot:
    def __init__(self, roots: dict[str, str] | None) -> None:
        self.roots = {
            str(name): Path(path).expanduser()
            for name, path in (roots or {}).items()
            if isinstance(name, str) and name and isinstance(path, str) and path
        }

    def _root(self, repository: object) -> Path | None:
        if not isinstance(repository, str) or repository not in self.roots:
            return None
        root = self.roots[repository]
        try:
            resolved = root.resolve(strict=True)
        except (OSError, RuntimeError):
            return None
        if not resolved.is_dir():
            return None
        return resolved

    def _reject_path(self, root: Path, relative: object) -> tuple[Path, str] | str:
        if (
            not isinstance(relative, str)
            or not relative
            or relative.strip() != relative
            or "\\" in relative
            or "\x00" in relative
        ):
            return "path_not_allowed"
        candidate = Path(relative)
        if candidate.is_absolute() or ".." in candidate.parts or not candidate.parts:
            return "path_not_allowed"
        current = root
        for part in candidate.parts:
            current = current / part
            try:
                if current.is_symlink():
                    return "symlink_not_allowed"
            except OSError:
                return "path_not_allowed"
        try:
            resolved = current.resolve(strict=False)
            resolved.relative_to(root)
        except (OSError, RuntimeError, ValueError):
            return "path_not_allowed"
        return resolved, candidate.as_posix()

    @staticmethod
    def _is_updating(root: Path) -> bool:
        try:
            return (root / _PUBLISHING_NAME).exists()
        except OSError:
            return True

    @staticmethod
    def _bounded_positive_int(value: object, default: int, maximum: int) -> int | None:
        if value is None:
            return default
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        if value < 1 or value > maximum:
            return None
        return value

    @staticmethod
    def _iter_regular_files(root: Path) -> Iterator[Path]:
        for raw_directory, directory_names, file_names in os.walk(
            root,
            followlinks=False,
        ):
            directory = Path(raw_directory)
            retained_directories: list[str] = []
            for name in sorted(directory_names):
                path = directory / name
                try:
                    if not path.is_symlink():
                        retained_directories.append(name)
                except OSError:
                    continue
            directory_names[:] = retained_directories
            for name in sorted(file_names):
                path = directory / name
                try:
                    if path.is_file() and not path.is_symlink():
                        yield path
                except OSError:
                    continue

    @staticmethod
    def _read_text(path: Path) -> tuple[str | None, str | None]:
        try:
            if path.stat().st_size > MAX_FILE_BYTES:
                return None, "file_too_large"
            with path.open("rb") as handle:
                data = handle.read(MAX_FILE_BYTES + 1)
        except OSError:
            return None, "unreadable"
        if len(data) > MAX_FILE_BYTES:
            return None, "file_too_large"
        if b"\x00" in data:
            return None, "binary_file"
        try:
            return data.decode("utf-8"), None
        except UnicodeDecodeError:
            return None, "binary_file"

    def list_files(self, repository: object) -> dict[str, Any]:
        root = self._root(repository)
        if root is None:
            return {"ok": False, "error": "unknown_repository"}
        if self._is_updating(root):
            return {"ok": False, "error": "snapshot_updating"}
        files: list[str] = []
        truncated = False
        for path in self._iter_regular_files(root):
            if path.name in {_SNAPSHOT_NAME, _PUBLISHING_NAME}:
                continue
            if len(files) >= MAX_LIST_FILES:
                truncated = True
                break
            files.append(path.relative_to(root).as_posix())
        if self._is_updating(root):
            return {"ok": False, "error": "snapshot_updating"}
        return {
            "ok": True,
            "repository": repository,
            "files": files,
            "truncated": truncated,
        }

    def read_file(
        self,
        repository: object,
        relative: object,
        *,
        offset: object = None,
        limit: object = None,
    ) -> dict[str, Any]:
        root = self._root(repository)
        if root is None:
            return {"ok": False, "error": "unknown_repository"}
        if self._is_updating(root):
            return {"ok": False, "error": "snapshot_updating"}
        start = self._bounded_positive_int(offset, 1, 10_000_000)
        line_limit = self._bounded_positive_int(limit, 200, MAX_READ_LINES)
        if start is None or line_limit is None:
            return {"ok": False, "error": "invalid_line_range"}
        resolved = self._reject_path(root, relative)
        if isinstance(resolved, str):
            return {"ok": False, "error": resolved}
        path, display = resolved
        if not path.is_file():
            return {"ok": False, "error": "not_found"}
        text, error = self._read_text(path)
        if error is not None:
            return {"ok": False, "error": error}
        if self._is_updating(root):
            return {"ok": False, "error": "snapshot_updating"}
        assert text is not None
        lines = text.splitlines()
        selected = lines[start - 1 : start - 1 + line_limit]
        numbered = "\n".join(
            f"{line_number}|{line}"
            for line_number, line in enumerate(selected, start=start)
        )
        return {
            "ok": True,
            "repository": repository,
            "path": display,
            "start_line": start,
            "end_line": start + len(selected) - 1,
            "total_lines": len(lines),
            "content": numbered,
        }

    def search(self, query: object, repository: object = None) -> dict[str, Any]:
        if not isinstance(query, str) or not query.strip():
            return {"ok": False, "error": "empty_query"}
        if len(query) > MAX_QUERY_CHARS:
            return {"ok": False, "error": "query_too_long"}
        if repository is None:
            repositories = list(self.roots)
        elif isinstance(repository, str) and repository in self.roots:
            repositories = [repository]
        else:
            return {"ok": False, "error": "unknown_repository"}

        needle = query.casefold()
        matches: list[dict[str, Any]] = []
        scanned_files = 0
        scanned_bytes = 0
        truncated = False
        deadline = time.monotonic() + MAX_SEARCH_SECONDS
        stop = False
        for selected_repository in repositories:
            root = self._root(selected_repository)
            if root is None:
                continue
            if self._is_updating(root):
                return {"ok": False, "error": "snapshot_updating"}
            for path in self._iter_regular_files(root):
                if path.name in {_SNAPSHOT_NAME, _PUBLISHING_NAME}:
                    continue
                if time.monotonic() >= deadline or scanned_files >= MAX_SEARCH_FILES:
                    truncated = True
                    stop = True
                    break
                try:
                    size = path.stat().st_size
                except OSError:
                    continue
                if size > MAX_FILE_BYTES:
                    continue
                if scanned_bytes + size > MAX_SEARCH_BYTES:
                    truncated = True
                    stop = True
                    break
                scanned_files += 1
                scanned_bytes += size
                text, error = self._read_text(path)
                if error is not None or text is None:
                    continue
                for line_number, line in enumerate(text.splitlines(), start=1):
                    if needle not in line.casefold():
                        continue
                    matches.append(
                        {
                            "repository": selected_repository,
                            "path": path.relative_to(root).as_posix(),
                            "line": line_number,
                            "text": line[:MAX_SNIPPET_CHARS],
                        }
                    )
                    if len(matches) >= MAX_SEARCH_MATCHES:
                        truncated = True
                        stop = True
                        break
                if stop:
                    break
            if self._is_updating(root):
                return {"ok": False, "error": "snapshot_updating"}
            if stop:
                break
        return {
            "ok": True,
            "matches": matches,
            "scanned_files": scanned_files,
            "scanned_bytes": scanned_bytes,
            "truncated": truncated,
        }


def register(ctx: Any) -> None:
    snapshot = ReadonlySourceSnapshot(ctx.get_config("roots"))

    def source_list(args: dict[str, Any] | None = None, **_: Any) -> dict[str, Any]:
        args = args or {}
        return snapshot.list_files(args.get("repository"))

    def source_read(args: dict[str, Any] | None = None, **_: Any) -> dict[str, Any]:
        args = args or {}
        return snapshot.read_file(
            args.get("repository"),
            args.get("path"),
            offset=args.get("offset"),
            limit=args.get("limit"),
        )

    def source_search(args: dict[str, Any] | None = None, **_: Any) -> dict[str, Any]:
        args = args or {}
        return snapshot.search(args.get("query"), args.get("repository"))

    ctx.register_tool(
        name="source_list",
        toolset="source_snapshot",
        schema={
            "type": "object",
            "additionalProperties": False,
            "properties": {"repository": {"type": "string"}},
            "required": ["repository"],
        },
        handler=source_list,
    )
    ctx.register_tool(
        name="source_read",
        toolset="source_snapshot",
        schema={
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "repository": {"type": "string"},
                "path": {"type": "string"},
                "offset": {"type": "integer", "minimum": 1},
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_READ_LINES,
                },
            },
            "required": ["repository", "path"],
        },
        handler=source_read,
    )
    ctx.register_tool(
        name="source_search",
        toolset="source_snapshot",
        schema={
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "query": {"type": "string", "minLength": 1, "maxLength": MAX_QUERY_CHARS},
                "repository": {"type": "string"},
            },
            "required": ["query"],
        },
        handler=source_search,
    )
