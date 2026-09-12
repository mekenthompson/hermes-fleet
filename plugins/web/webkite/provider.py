"""Webkite search and extraction through the local webkite CLI."""
from __future__ import annotations

import json
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor
import shutil
import subprocess
from typing import Any, Dict, List

from agent.web_search_provider import WebSearchProvider

_TIMEOUT_SECS = 45
_EXTRACT_CONCURRENCY = 4
_CLEANUP_SECS = 1


class WebkiteWebSearchProvider(WebSearchProvider):
    @property
    def name(self) -> str:
        return "webkite"

    @property
    def display_name(self) -> str:
        return "Webkite"

    def is_available(self) -> bool:
        return bool(shutil.which("webkite"))

    def supports_search(self) -> bool:
        return True

    def supports_extract(self) -> bool:
        return True

    def _run(self, args: List[str], *, deadline: float | None = None) -> subprocess.CompletedProcess[str]:
        command = ["webkite", *args]
        if deadline is None:
            deadline = time.monotonic() + _TIMEOUT_SECS
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(command, _TIMEOUT_SECS)
        proc = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, start_new_session=True,
        )
        try:
            stdout, stderr = proc.communicate(timeout=max(0, deadline - time.monotonic()))
            return subprocess.CompletedProcess(command, proc.returncode, stdout, stderr)
        except BaseException:
            # Kill the process group: a renderer may inherit the output pipes.
            # A bounded drain also protects against a descendant escaping the group.
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                proc.communicate(timeout=_CLEANUP_SECS)
            except subprocess.TimeoutExpired:
                proc.wait(timeout=_CLEANUP_SECS)
            raise
        finally:
            proc.stdout.close()
            proc.stderr.close()

    def search(self, query: str, limit: int = 5) -> Dict[str, Any]:
        if not query or not query.strip():
            return {"success": False, "error": "Empty search query"}
        try:
            proc = self._run(
                [
                    "search",
                    "-f",
                    "json",
                    "--limit",
                    str(max(1, min(int(limit), 20))),
                    "-q",
                    "--",
                    query.strip(),
                ]
            )
        except subprocess.TimeoutExpired:
            return {"success": False, "error": "Webkite search timed out"}
        except FileNotFoundError:
            return {"success": False, "error": "webkite binary not found"}
        if proc.returncode != 0:
            return {"success": False, "error": "webkite search failed"}
        try:
            payload = json.loads(proc.stdout or "[]")
        except json.JSONDecodeError:
            return {"success": False, "error": "Webkite search returned non-JSON"}
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            rows = payload.get("results") or payload.get("data") or []
        else:
            return {"success": False, "error": "Webkite search returned unexpected JSON"}
        web = []
        for position, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                continue
            web.append(
                {
                    "title": row.get("title") or "",
                    "url": row.get("url") or "",
                    "description": row.get("snippet") or row.get("description") or "",
                    "position": position,
                }
            )
        return {"success": True, "data": {"web": web}}

    def extract(self, urls: List[str], **kwargs: Any) -> List[Dict[str, Any]]:
        if not urls:
            return []
        # All workers, including queued URLs, consume one batch budget. Joining
        # the bounded pool ensures no extraction threads survive the call.
        deadline = time.monotonic() + _TIMEOUT_SECS
        with ThreadPoolExecutor(max_workers=min(_EXTRACT_CONCURRENCY, len(urls))) as pool:
            return list(pool.map(lambda url: self._extract_one(url, deadline=deadline), urls))

    def _extract_one(self, url: str, *, deadline: float | None = None) -> Dict[str, Any]:
        try:
            proc = self._run(["read", "-f", "json", "-q", "--", url], deadline=deadline)
        except subprocess.TimeoutExpired:
            return {"url": url, "title": "", "content": "", "error": "Webkite read timed out"}
        except FileNotFoundError:
            return {"url": url, "title": "", "content": "", "error": "webkite binary not found"}
        except OSError:
            return {"url": url, "title": "", "content": "", "error": "webkite read failed"}
        if proc.returncode != 0:
            return {"url": url, "title": "", "content": "", "error": "webkite read failed"}
        try:
            payload = json.loads(proc.stdout or "{}")
        except json.JSONDecodeError:
            return {"url": url, "title": "", "content": "", "error": "Webkite read returned non-JSON"}
        if not isinstance(payload, dict):
            return {
                "url": url,
                "title": "",
                "content": "",
                "error": "Webkite read returned unexpected JSON",
            }
        metadata = payload.get("metadata") or {}
        body = payload.get("body") or {}
        content = body.get("markdown") or body.get("text") or ""
        title = metadata.get("title") or ""
        outcome = ((payload.get("outcome") or {}).get("outcome") or "").lower()
        if outcome and outcome not in {"ok", "success"} and not content:
            return {"url": url, "title": title, "content": "", "error": f"Webkite outcome: {outcome}"}
        return {
            "url": payload.get("url_final") or url,
            "title": title,
            "content": content,
            "raw_content": body.get("text") or content,
            "metadata": {
                "source": "webkite",
                "http_status": ((payload.get("diagnostics") or {}).get("network") or {}).get("http_status"),
                "render": ((payload.get("diagnostics") or {}).get("render") or {}).get("tier"),
            },
        }
