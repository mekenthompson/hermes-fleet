"""The public tree owns the whole generic Linear agent implementation.

A deployment overlay may supply read-only policy maps and its own unpublished
plugins. It must not need to carry a fork of this worker. These tests pin the
module set, the child-facing symbol surface, and the absence of deployment
identity, so a regression is a test failure rather than a silent re-fork.
"""
from __future__ import annotations

import importlib
import importlib.util
import json
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "plugins" / "linear-agent"
SCOPE = ROOT / "scripts" / "fleet-image-change-scope.py"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(PLUGIN))

# Every generic module the worker needs to run, provision, publish, track and
# be independently read. Deployment policy maps are deliberately absent.
GENERIC_MODULES = {
    "linear_activity",
    "linear_agent",
    "linear_attachments",
    "linear_budgets",
    "linear_chat_closeout",
    "linear_completion",
    "linear_connect",
    "linear_cos_dispatch",
    "linear_guard_health",
    "linear_handoff",
    "linear_limits",
    "linear_live_canary",
    "linear_oauth",
    "linear_ownership",
    "linear_parent_continuation",
    "linear_parent_followup",
    "linear_policy",
    "linear_project_updates",
    "linear_provision",
    "linear_quota",
    "linear_readiness",
    "linear_reconcile",
    "linear_resume",
    "linear_runtime",
    "linear_stop",
    "linear_tracking",
}

# The names an overlay's plugin entry point, operator CLIs and tests bind to.
CHILD_FACING_SYMBOLS = {
    "linear_activity": (
        "LinearActivityClient",
        "LinearGraphQLError",
        "DEFAULT_WAITING_STATE_NAME",
        "board_statuses",
        "waiting_unblock_comment_accepted",
    ),
    "linear_agent": ("LinearWorker", "unauthorized_response_body_from_entry"),
    "linear_budgets": ("IssueBudget", "IssueBudgetLedger"),
    "linear_chat_closeout": ("ChatCloseoutRegistry", "ChatCloseoutRetryService", "QUIET_SECONDS"),
    "linear_guard_health": ("WorkerGuardHealth", "SCHEMA_VERSION"),
    "linear_handoff": ("HandoffDenied", "require_fleet_authority_store"),
    "linear_live_canary": ("run_canary", "inspect_worker"),
    "linear_oauth": ("make_oauth", "validate_private_directory"),
    "linear_parent_followup": (
        "ParentFollowupQueue",
        "child_id_from_session_event",
        "lookup_parent_issue_id",
        "parent_id_from_session_event",
        "resolve_parent_issue_id",
    ),
    "linear_project_updates": (
        "LinearProjectUpdatePublisher",
        "NoProjectUpdate",
        "ProjectUpdateError",
        "publish_session_updates",
    ),
    "linear_provision": ("expected_container_hostname", "provision", "verify_managed_oauth"),
    "linear_quota": ("IDLE_POLL_SECONDS", "LinearQuotaGate"),
    "linear_readiness": ("LinearDependencyReadiness",),
    "linear_reconcile": ("InspectionError", "inspect_state"),
    "linear_runtime": (
        "LINEAR_WORKTREE",
        "ProfileLinearRuntime",
        "install_worktree_cwd_pin",
        "pin_issue_worktree_cwd",
    ),
    "linear_stop": (
        "FOLLOW_UP_ACTIVITY_SIGNALS",
        "LINEAR_ACTIVITY_SIGNALS",
        "STOP_ACTIVITY_SIGNALS",
    ),
    "linear_tracking": ("LinearTracking", "TrackingError"),
}

# Real identities, private issue keys and household wording never belong in the
# public image. ``KEN-`` matches the private Linear key prefix, not ``HF-``.
FORBIDDEN_IDENTITY = {
    "household name or real identity": re.compile(r"\bKens?\b|\bThompson\b", re.IGNORECASE),
    "private Linear issue key": re.compile(r"\bKEN-\d"),
    "deployment profile identity": re.compile(
        r"\b(?:" + "klank" + "er|ag" + "gie|over" + "lord|cl" + "erk" + r")\b",
        re.IGNORECASE,
    ),
}


def load(name: str):
    sys.modules.pop(name, None)
    return importlib.import_module(name)


def load_script(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PublicLinearImplementationParityTests(unittest.TestCase):
    def test_public_owns_every_generic_module(self) -> None:
        present = {path.stem for path in PLUGIN.glob("*.py") if path.name != "__init__.py"}
        self.assertEqual(present, GENERIC_MODULES)

    def test_generic_modules_export_the_child_facing_surface(self) -> None:
        for name, symbols in sorted(CHILD_FACING_SYMBOLS.items()):
            module = load(name)
            for symbol in symbols:
                with self.subTest(module=name, symbol=symbol):
                    self.assertTrue(hasattr(module, symbol), f"{name}.{symbol} is missing")

    def test_no_deployment_identity_or_household_wording(self) -> None:
        for path in sorted(PLUGIN.iterdir()):
            if not path.is_file():
                continue
            text = path.read_text(encoding="utf-8")
            for label, pattern in FORBIDDEN_IDENTITY.items():
                with self.subTest(file=path.name, label=label):
                    found = pattern.search(text)
                    self.assertIsNone(found, f"{label} in {path.name}: {found.group(0) if found else ''}")

    def test_waiting_state_label_is_generic_and_overridable(self) -> None:
        activity = load("linear_activity")
        default = activity.DEFAULT_WAITING_STATE_NAME
        self.assertIsInstance(default, str)
        self.assertTrue(default)
        self.assertEqual(activity.board_statuses()["waiting"][0], default)
        self.assertEqual(
            activity.board_statuses("Blocked on Reporter")["waiting"],
            ("Blocked on Reporter", None),
        )
        client = activity.LinearActivityClient("token", waiting_state_name="Blocked on Reporter")
        with self.assertRaisesRegex(ValueError, "Blocked on Reporter"):
            client.dispatch("issue-1", "issue_status", "waiting")

    def test_deployment_policy_maps_stay_out_of_the_public_tree(self) -> None:
        for name in ("linear-agents.json", "linear-publishers.json"):
            with self.subTest(name=name):
                self.assertEqual(list(ROOT.rglob(name)), [])

    def test_image_scope_and_build_cover_every_new_module(self) -> None:
        scope = load_script(SCOPE, "fleet_image_change_scope")
        for path in sorted(PLUGIN.glob("*.py")):
            relative = path.relative_to(ROOT).as_posix()
            with self.subTest(path=relative):
                self.assertTrue(scope.is_image_input(relative))
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("COPY plugins/linear-agent/ /opt/hermes/plugins/linear-agent/", dockerfile)
        self.assertIn("python3 -m py_compile /opt/hermes/plugins/linear-agent/*.py", dockerfile)

    def test_plugin_stays_default_disabled(self) -> None:
        contract = json.loads((ROOT / "contracts" / "plugins.json").read_text(encoding="utf-8"))
        entry = next(item for item in contract["components"] if item["id"] == "linear-agent")
        self.assertFalse(entry["default_enabled"])
        source = (PLUGIN / "__init__.py").read_text(encoding="utf-8")
        self.assertIn('_boolean_config(ctx, "enabled", False)', source)


if __name__ == "__main__":
    unittest.main()
