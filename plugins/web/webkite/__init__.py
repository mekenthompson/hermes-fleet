"""Webkite web search and extraction plugin."""
from __future__ import annotations

from .provider import WebkiteWebSearchProvider


def register(ctx) -> None:
    ctx.register_web_search_provider(WebkiteWebSearchProvider())
