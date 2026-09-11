"""Env-driven public skin. No household names or baked origins."""
from __future__ import annotations

import os


class PublicUrlError(ValueError):
    """Canonical public URL is missing or inconsistent."""


def _strip(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def brand() -> str:
    return _strip("BRAND") or "Browser"


def local_tz() -> str:
    return _strip("LOCAL_TZ") or "UTC"


def canonical_public_url() -> str:
    origin = _strip("PUBLIC_ORIGIN")
    base = _strip("PUBLIC_BASE")
    if origin and base and origin != base:
        raise PublicUrlError("PUBLIC_ORIGIN and PUBLIC_BASE disagree")
    value = origin or base
    if not value:
        raise PublicUrlError("missing PUBLIC_ORIGIN or PUBLIC_BASE")
    return value
