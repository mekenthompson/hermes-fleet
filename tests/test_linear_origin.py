"""The mounted evidence edge accepts no model-supplied identity or authority grant."""
import argparse
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from linear_fake_api import load_plugin
plugin = load_plugin()
from hermes_fleet_linear_plugin import origin, ops, transport


class OriginEdgeTests(unittest.TestCase):
    def test_reference_shape_cannot_select_a_home_database_or_identity(self):
        ref = dict(version=1, profile="default", session_id="session", admission_id="admission", input_id="input",
                   instance_id="owner", authority_epoch=1, execution_generation=1, scope_digest="digest")
        self.assertEqual(origin.reference(ref), ref)
        for changed in ({**ref, "home": "/foreign"}, {**ref, "principal": "human"},
                        {**ref, "authority_epoch": True}, {**ref, "session_id": ""}, {}):
            with self.subTest(changed=changed), self.assertRaises(transport.Unavailable):
                origin.reference(changed)

    def test_scope_projection_excludes_credentials_and_unrelated_settings(self):
        settings = {"identity": {"organization_id": "workspace", "teams": ["team"],
                                 "credentials": "SECRET_SENTINEL", "viewer_id": "app"},
                    "credentials": "SECRET_SENTINEL"}
        projected = origin.scope(settings)
        self.assertEqual(projected, {"linear": {"organization_id": "workspace", "teams": ["team"]},
                                     "specialist_scope": None})
        self.assertNotIn("SECRET_SENTINEL", json.dumps(projected))

    def test_registered_action_has_no_context_selectors_and_needs_no_linear_service(self):
        handlers = {}
        ctx = SimpleNamespace(get_config=lambda key, default=None: True if key == "enabled" else default,
                              register_tool=lambda **kwargs: handlers.update({kwargs["name"]: kwargs["handler"]}),
                              register_hook=lambda *args: None)
        with patch.dict("os.environ", {"_HERMES_GATEWAY": ""}):
            plugin.register(ctx)
        context = transport.NativeContext(home="/synthetic", profile="default", platform="local", source="gui",
                                           session_key="key", session_id="session")
        with patch.object(transport, "capture_context", return_value=context), patch.object(origin, "current", return_value={
                "reference": {"admission_id": "current"}, "permission_granted": False}) as current:
            result = json.loads(handlers["linear"]({"action": "origin_reference"}))
            self.assertTrue(result["ok"])
            self.assertFalse(result["permission_granted"])
            self.assertEqual(current.call_args.args[0], context)
            for selector in ("session_id", "profile", "home", "issue", "task_id", "body"):
                result = json.loads(handlers["linear"]({"action": "origin_reference", selector: "forged"}))
                self.assertFalse(result["ok"])
            self.assertEqual(current.call_count, 1)

    def test_child_is_refused_before_any_owner_or_state_access(self):
        context = transport.NativeContext(home="/synthetic", profile="default", platform="local", source="gui",
                                           session_key="key", session_id="session", delegated_child=True)
        with patch.object(origin, "_run") as run, self.assertRaises(transport.Unavailable):
            origin.current(context, {})
        run.assert_not_called()

    def test_owner_cli_only_accepts_one_complete_reference(self):
        parser = argparse.ArgumentParser()
        ops.setup(parser)
        args = parser.parse_args(["inspect-origin", "--reference-json", '{"admission_id":"one"}'])
        with (patch.object(origin, "inspect", return_value={"original_message": "one request", "permission_granted": False}) as inspect_origin,
              patch("builtins.print")):
            self.assertEqual(ops.run(args, Path("unused"), Path("unused"), "default"), 0)
        inspect_origin.assert_called_once_with({"admission_id": "one"}, {})
