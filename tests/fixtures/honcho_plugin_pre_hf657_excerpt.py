"""Excerpt of the pinned plastic-labs/honcho hermes-plugin-honcho __init__.py (pre-HF-657)."""
from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from agent.memory_manager import sanitize_context
from agent.memory_provider import MemoryProvider, is_trivial_prompt
from agent.coding_context import INTERACTIVE_CODING_PLATFORMS as _LOCAL_PLATFORMS
from agent.turn_author import a2a_key
from .client import HonchoClientConfig, resolve_config_path
from .client import _host_block, _HostLookup
from .client import join_plugin_threads, spawn_context_thread
from .dialectic import DialecticMixin
from .session_peers import assistant_peer_id_for, sanitize_peer_id
from .session_context import usable_honcho_summary
from .tool_schemas import ALL_TOOL_SCHEMAS
from tools.registry import tool_error

logger = logging.getLogger(__name__)


# Gateway-internal notifications arrive through the same user-role channel as genuine
# user messages; they are execution metadata and must never become durable memory.
# Deliberately anchored: a human discussing one of these strings mid-message is valid input.
_INTERNAL_GATEWAY_TURN_RE = re.compile(
    r"^\s*(?:"
    r"\[ASYNC (?:DELEGATION )?(?:BATCH )?COMPLETE[^\]]*\]|"
    r"\[CONTEXT COMPACTION[^\]]*\]|"
    r"\[CONTEXT SUMMARY\]:?|"
    r"\[PRIOR CONTEXT[^\]]*\]|"
    r"\[Your active task list was preserved across context compression\]|"
    r"\[IMPORTANT: Background process \d+ matched watch pattern[^\n]*|"
    r"A background fan-out of \d+ subagent\(s\) you dispatched earlier has finished\.|"
    r"A background subagent you dispatched earlier has finished\."
    r")",
    re.IGNORECASE,
)


def _is_internal_gateway_turn(text: str) -> bool:
    """Return True for machine-generated gateway/delegation notifications."""
    return bool(_INTERNAL_GATEWAY_TURN_RE.match(text or ""))

