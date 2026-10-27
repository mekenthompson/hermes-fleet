import json
import os
import unittest
import test_linear_origin_native as base
import test_linear_origin_provider_native as provider
from test_linear_specialist_authorization import SESSION

class CleanupReview(base.OriginScenarios):
    async def test_review_opaque_checkpoint(self):
        await self.stopped_issue()
        (self.home / "processes.json").write_text(json.dumps([{"pid": os.getpid(), "systemd_unit": "unattributable-live-scope"}]))
        evidence = await self.cleanup_inspected()
        self.assertFalse(evidence["quiescence_proven"], evidence)

    async def test_review_missing_owner_checkpoint(self):
        await self.stopped_issue()
        (self.home / "processes.json").write_text(json.dumps([{"session_key": self.authority.sessions[self.sid].route, "pid": os.getpid(), "systemd_unit": "own-live-scope"}]))
        evidence = await self.cleanup_inspected()
        self.assertFalse(evidence["quiescence_proven"], evidence)

class ProviderReview(provider.LinearProviderOriginScenarios):
    async def test_review_later_activity_agent_session_basis(self):
        await self.linear()
        self.settings["specialist_scope"]["requester_basis"] = "agent_session"
        self.api = self.provider.api(scope=self.settings["specialist_scope"])
        self.provider.sessions[SESSION]["creator"] = None
        evidence = await self.current()
        self.assertEqual(evidence["provider_verification"]["state"], "verified")

def load_tests(loader, tests, pattern):
    return unittest.TestSuite([CleanupReview("test_review_opaque_checkpoint"), CleanupReview("test_review_missing_owner_checkpoint"), ProviderReview("test_review_later_activity_agent_session_basis")])
