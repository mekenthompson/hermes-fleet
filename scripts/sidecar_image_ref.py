"""Require digest-pinned public Fleet sidecar image names."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from image_ref import require_digest_reference

ALLOWED_REPOSITORIES = frozenset(
    {
        "ghcr.io/mekenthompson/hermes-fleet-rest-lock-proxy",
        "ghcr.io/mekenthompson/hermes-fleet-tcp-proxy",
        "ghcr.io/mekenthompson/hermes-fleet-browser-broker",
        "ghcr.io/mekenthompson/hermes-fleet-kokoro",
        "ghcr.io/mekenthompson/hermes-fleet-camofox",
    }
)


def require_sidecar_image(value: str) -> str:
    """Return *value* if it is an allowed sidecar repository at a digest pin."""
    if ":" in value.split("@", 1)[0]:
        raise ValueError("sidecar image must be repository@sha256, with no tag")
    pinned = require_digest_reference(value)
    repository = pinned.split("@", 1)[0]
    if repository not in ALLOWED_REPOSITORIES:
        raise ValueError(
            "sidecar image must be one of the public hermes-fleet-* packages at a digest"
        )
    return pinned
