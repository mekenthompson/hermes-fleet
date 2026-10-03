"""Credential capture and refresh boundaries using only synthetic transports."""
import json
import tempfile
import threading
import unittest
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from linear_fake_api import Clock, load_plugin

load_plugin()
from hermes_fleet_linear_plugin import BoundLinearAPI
from hermes_fleet_linear_plugin.api import LinearAPI, LinearError, RateLimited
from hermes_fleet_linear_plugin.oauth import ConnectItem, ConnectOAuth, token_provider


IDENTITY = {"viewer_id": "app-a", "organization_id": "org-a"}


def response(data):
    return 200, {}, json.dumps({"data": data}).encode()


class RefreshProvider:
    def __init__(self, initial="synthetic-stale", refreshed="synthetic-fresh"):
        self.initial, self.refreshed = initial, refreshed
        self.invalidations = 0
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return self.refreshed if self.invalidations else self.initial

    def invalidate(self):
        self.invalidations += 1


class SyntheticLinear:
    def __init__(self, reply=None):
        self.reply = reply
        self.requests = []

    def __call__(self, url, body, headers):
        request = json.loads(body)
        query, variables = request["query"], request["variables"]
        credential = headers["Authorization"].removeprefix("Bearer ")
        kind = "identity" if "IdentityBinding" in query else "operation"
        self.requests.append((kind, credential, variables))
        if self.reply is not None:
            reply = self.reply(kind, credential, variables)
            if reply is not None:
                return reply
        if kind == "identity":
            return response({"viewer": {"id": "foreign" if credential == "synthetic-foreign" else "app-a"},
                             "organization": {"id": "foreign" if credential == "synthetic-foreign-org" else "org-a"}})
        for field in ("issueUpdate", "commentCreate", "agentActivityCreate", "projectUpdateCreate"):
            if field in query:
                return response({field: {"success": True}})
        return response({"issue": {"id": "issue-1", "team": {"key": "OPS"}, "project": {"id": "project-1"}}})

    def credentials(self):
        return [(kind, credential) for kind, credential, _ in self.requests]


class CredentialBindingTests(unittest.TestCase):
    def test_provider_error_cannot_print_credential_material(self):
        import traceback
        def unavailable():
            raise RuntimeError("synthetic-secret-in-provider-error")
        client = BoundLinearAPI(unavailable, identity=IDENTITY, transport=SyntheticLinear())
        with self.assertLogs("linear", level="ERROR") as captured:
            with self.assertRaises(LinearError) as refused:
                client.issue("issue-1")
        self.assertNotIn("synthetic-secret-in-provider-error", " ".join(captured.output))
        self.assertNotIn("synthetic-secret-in-provider-error", "".join(
            traceback.format_exception(refused.exception)))

    def test_plain_api_keeps_its_provider_replacement_retry_without_identity_checks(self):
        replacement = RefreshProvider(initial="synthetic-short-lived", refreshed="synthetic-fresh")

        def replace(kind, credential, _):
            if credential == "synthetic-stale":
                client.token = replacement
                return 401, {}, b"{}"
            return response({"viewer": {"id": "plain-viewer"}})

        transport = SyntheticLinear(replace)
        client = LinearAPI(lambda: "synthetic-stale", transport=transport)
        failure, viewer = None, None
        try:
            viewer = client.viewer_id()
        except LinearError as exc:
            failure = exc
        self.assertIsNone(failure, "plain LinearAPI must preserve its existing replacement-provider retry")
        self.assertEqual(viewer, "plain-viewer")
        self.assertEqual(client.viewer_id(), "plain-viewer")
        self.assertEqual(transport.credentials(), [("operation", "synthetic-stale"), ("operation", "synthetic-fresh")])
        self.assertEqual((replacement.calls, replacement.invalidations), (1, 1))

    def test_public_identity_verification_does_not_return_credentials(self):
        transport = SyntheticLinear()
        client = BoundLinearAPI(lambda: "synthetic-a", identity=IDENTITY, transport=transport)
        self.assertIsNone(client.verify_identity())
        self.assertEqual(transport.credentials(), [("identity", "synthetic-a")])

    def test_reads_and_all_named_mutations_capture_once_and_refuse_foreign_replays(self):
        operations = (
            lambda api: api.issue("issue-1"),
            lambda api: api.graphql("query Issue($id: String!) { issue(id: $id) { id } }", {"id": "issue-1"}),
            lambda api: api.update_issue("issue-1", {"stateId": "started"}),
            lambda api: api.create_comment("client-1", "issue-1", "body"),
            lambda api: api.create_activity("client-1", "session-1", {"type": "response"}),
            lambda api: api.create_project_update("client-1", "project-1", "body"),
        )
        for operation in operations:
            for foreign in ("synthetic-foreign", "synthetic-foreign-org"):
                with self.subTest(operation=operations.index(operation), foreign=foreign):
                    supplied = iter(("synthetic-a", foreign))
                    transport = SyntheticLinear()
                    client = BoundLinearAPI(lambda: next(supplied), identity=IDENTITY, transport=transport)
                    operation(client)
                    self.assertEqual(transport.credentials(), [("identity", "synthetic-a"),
                                                               ("operation", "synthetic-a")])
                    with self.assertRaises(LinearError) as refused:
                        operation(client)
                    self.assertFalse(refused.exception.retryable)
                    self.assertEqual(transport.credentials()[-1], ("identity", foreign))
                    self.assertEqual(len(transport.requests), 3)

    def test_authorized_operation_401_refresh_is_verified_before_retry(self):
        provider = RefreshProvider()
        transport = SyntheticLinear(lambda kind, credential, _: (401, {}, b"{}")
                                    if kind == "operation" and credential == provider.initial else None)
        client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)
        client.update_issue("issue-1", {"stateId": "started"})
        self.assertEqual(transport.credentials(), [("identity", provider.initial), ("operation", provider.initial),
                                                   ("identity", provider.refreshed), ("operation", provider.refreshed)])
        self.assertEqual((provider.calls, provider.invalidations), (2, 1))

    def test_mutation_guard_runs_after_each_verified_credential_and_does_not_leak(self):
        provider = RefreshProvider()
        transport = SyntheticLinear(lambda kind, credential, _: (401, {}, b"{}")
                                    if kind == "operation" and credential == provider.initial else None)
        client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)
        seen = []

        @contextmanager
        def guard():
            seen.append(transport.credentials())
            yield

        with client.guarded_mutation(guard):
            client.update_issue("issue-1", {"stateId": "started"})
        self.assertEqual(seen, [[("identity", provider.initial)],
                                [("identity", provider.initial), ("operation", provider.initial),
                                 ("identity", provider.refreshed)]])
        client.update_issue("issue-1", {"stateId": "started"})
        self.assertEqual(len(seen), 2)

    def test_foreign_identity_401_refresh_never_reaches_an_operation(self):
        for foreign in ("synthetic-foreign", "synthetic-foreign-org"):
            with self.subTest(foreign=foreign):
                provider = RefreshProvider(refreshed=foreign)
                transport = SyntheticLinear(lambda kind, credential, _: (401, {}, b"{}")
                                            if credential == provider.initial else None)
                client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)
                with self.assertRaises(LinearError) as refused:
                    client.issue("issue-1")
                self.assertFalse(refused.exception.retryable)
                self.assertEqual(transport.credentials(), [("identity", provider.initial), ("identity", foreign)])
                self.assertEqual((provider.calls, provider.invalidations), (2, 1))

    def test_identity_and_operation_each_keep_their_one_refresh_retry(self):
        credentials = iter(("synthetic-expired", "synthetic-short-lived", "synthetic-fresh"))

        class Provider:
            invalidations = 0
            def __call__(self):
                return next(credentials)
            def invalidate(self):
                self.invalidations += 1

        provider = Provider()
        transport = SyntheticLinear(lambda kind, credential, _: (401, {}, b"{}")
                                    if credential == "synthetic-expired" or
                                    (kind == "operation" and credential == "synthetic-short-lived") else None)
        client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)
        client.update_issue("issue-1", {"stateId": "started"})
        self.assertEqual(transport.credentials(), [("identity", "synthetic-expired"),
                                                   ("identity", "synthetic-short-lived"),
                                                   ("operation", "synthetic-short-lived"),
                                                   ("identity", "synthetic-fresh"), ("operation", "synthetic-fresh")])
        self.assertEqual(provider.invalidations, 2)

    def test_repeated_401_is_bounded_for_identity_and_operation(self):
        for failing_kind in ("identity", "operation"):
            with self.subTest(failing_kind=failing_kind):
                provider = RefreshProvider()
                transport = SyntheticLinear(lambda kind, credential, _: (401, {}, b"{}")
                                            if kind == failing_kind else None)
                client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)
                with self.assertRaises(LinearError) as failed:
                    client.update_issue("issue-1", {"stateId": "started"})
                self.assertTrue(failed.exception.retryable)
                self.assertEqual((provider.calls, provider.invalidations), (2, 1))
                self.assertEqual(len(transport.requests), 2 if failing_kind == "identity" else 4)

    def test_provider_without_invalidation_does_not_retry_401(self):
        for failing_kind in ("identity", "operation"):
            with self.subTest(failing_kind=failing_kind):
                transport = SyntheticLinear(lambda kind, credential, _: (401, {}, b"{}")
                                            if kind == failing_kind else None)
                client = BoundLinearAPI(lambda: "synthetic-a", identity=IDENTITY, transport=transport)
                with self.assertRaises(LinearError) as failed:
                    client.issue("issue-1")
                self.assertTrue(failed.exception.retryable)
                self.assertEqual(len(transport.requests), 1 if failing_kind == "identity" else 2)

    def test_provider_replacement_after_verification_keeps_capture_and_refuses_next_call(self):
        original = RefreshProvider(initial="synthetic-a")
        replacement = RefreshProvider(initial="synthetic-foreign")

        def replace(kind, credential, _):
            if kind == "identity" and credential == original.initial:
                client.token = replacement

        transport = SyntheticLinear(replace)
        client = BoundLinearAPI(original, identity=IDENTITY, transport=transport)
        client.update_issue("issue-1", {"stateId": "started"})
        self.assertIs(client.token, replacement)
        with self.assertRaises(LinearError) as refused:
            client.issue("issue-1")
        self.assertFalse(refused.exception.retryable)
        self.assertEqual(transport.credentials(), [("identity", original.initial), ("operation", original.initial),
                                                   ("identity", replacement.initial)])
        self.assertEqual((original.calls, replacement.calls), (1, 1))

    def test_401_invalidates_rejected_provider_and_verifies_the_replacement(self):
        for replacement_token in ("synthetic-fresh", "synthetic-foreign", "synthetic-foreign-org"):
            with self.subTest(replacement_token=replacement_token):
                original = RefreshProvider()
                replacement = RefreshProvider(initial=replacement_token)

                def replace(kind, credential, _):
                    if kind == "operation" and credential == original.initial:
                        client.token = replacement
                        return 401, {}, b"{}"

                transport = SyntheticLinear(replace)
                client = BoundLinearAPI(original, identity=IDENTITY, transport=transport)
                if replacement_token == "synthetic-fresh":
                    client.update_issue("issue-1", {"stateId": "started"})
                else:
                    with self.assertRaises(LinearError) as refused:
                        client.update_issue("issue-1", {"stateId": "started"})
                    self.assertFalse(refused.exception.retryable)
                self.assertEqual((original.invalidations, replacement.invalidations), (1, 0))
                self.assertIs(client.token, replacement)
                expected = [("identity", original.initial), ("operation", original.initial),
                            ("identity", replacement_token)]
                if replacement_token == "synthetic-fresh":
                    expected.append(("operation", replacement_token))
                self.assertEqual(transport.credentials(), expected)

    def test_identity_401_provider_replacement_retains_the_provider_used_by_the_operation(self):
        original = RefreshProvider(initial="synthetic-expired", refreshed="synthetic-expired")
        replacement = RefreshProvider(initial="synthetic-short-lived", refreshed="synthetic-fresh")

        def replace(kind, credential, _):
            if kind == "identity" and credential == original.initial:
                client.token = replacement
                return 401, {}, b"{}"
            if kind == "operation" and credential == replacement.initial:
                return 401, {}, b"{}"

        transport = SyntheticLinear(replace)
        client = BoundLinearAPI(original, identity=IDENTITY, transport=transport)
        failure = None
        try:
            client.update_issue("issue-1", {"stateId": "started"})
        except LinearError as exc:
            failure = exc
        self.assertIsNone(failure, "an identity retry must verify a replacement and retain its provider for invalidation")
        self.assertEqual(transport.credentials(), [("identity", original.initial),
                                                   ("identity", replacement.initial), ("operation", replacement.initial),
                                                   ("identity", replacement.refreshed), ("operation", replacement.refreshed)])
        self.assertEqual((original.invalidations, replacement.invalidations), (1, 1))
        self.assertIs(client.token, replacement)

    def test_concurrent_calls_keep_separate_captured_credentials(self):
        local = threading.local()
        barrier = threading.Barrier(2)
        requests, counts = {}, {}
        lock = threading.Lock()

        def provider():
            local.calls += 1
            return f"synthetic-{local.label}" if local.calls == 1 else "synthetic-foreign"

        def transport(url, body, headers):
            kind = "identity" if "IdentityBinding" in json.loads(body)["query"] else "operation"
            credential = headers["Authorization"].removeprefix("Bearer ")
            with lock:
                requests.setdefault(local.label, []).append((kind, credential))
            if kind == "identity":
                return response({"viewer": {"id": "app-a"}, "organization": {"id": "org-a"}})
            return response({"issueUpdate": {"success": True}})

        client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)

        def run(label):
            local.label, local.calls = label, 0
            barrier.wait(timeout=10)  # callers race admission; requests serialize for quota observation
            client.update_issue(label, {"stateId": "started"})
            with lock:
                counts[label] = local.calls

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(run, label) for label in ("left", "right")]
            for future in futures:
                future.result(timeout=15)
        self.assertEqual(counts, {"left": 1, "right": 1})
        for label in ("left", "right"):
            self.assertEqual(requests[label], [("identity", f"synthetic-{label}"),
                                               ("operation", f"synthetic-{label}")])
        self.assertIs(client.token, provider)

    def test_concurrent_401_refresh_does_not_change_another_verified_call(self):
        local = threading.local()
        barrier = threading.Barrier(2)
        requests, counts = {}, {}
        lock = threading.Lock()

        class Provider:
            def __call__(self):
                local.calls += 1
                return f"synthetic-{local.label}-{local.invalidations}"
            def invalidate(self):
                local.invalidations += 1

        provider = Provider()

        def transport(url, body, headers):
            kind = "identity" if "IdentityBinding" in json.loads(body)["query"] else "operation"
            credential = headers["Authorization"].removeprefix("Bearer ")
            with lock:
                requests.setdefault(local.label, []).append((kind, credential))
            if kind == "identity":
                return response({"viewer": {"id": "app-a"}, "organization": {"id": "org-a"}})
            if local.label == "left" and local.invalidations == 0:
                return 401, {}, b"{}"
            return response({"issueUpdate": {"success": True}})

        client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)

        def run(label):
            local.label, local.calls, local.invalidations = label, 0, 0
            barrier.wait(timeout=10)  # refresh and its verified retry complete before the next caller
            client.update_issue(label, {"stateId": "started"})
            with lock:
                counts[label] = (local.calls, local.invalidations)

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(run, label) for label in ("left", "right")]
            for future in futures:
                future.result(timeout=15)
        self.assertEqual(counts, {"left": (2, 1), "right": (1, 0)})
        self.assertEqual(requests["left"], [("identity", "synthetic-left-0"), ("operation", "synthetic-left-0"),
                                           ("identity", "synthetic-left-1"), ("operation", "synthetic-left-1")])
        self.assertEqual(requests["right"], [("identity", "synthetic-right-0"), ("operation", "synthetic-right-0")])
        self.assertIs(client.token, provider)

    def test_token_file_replacement_is_captured_then_reverified_on_next_read(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            path = home / "linear-token"
            path.write_text("synthetic-a\n")
            path.chmod(0o600)
            provider = token_provider({"credentials": {"mode": "token_file", "path": str(path)}}, home)

            def replace(kind, credential, _):
                if kind == "identity" and credential == "synthetic-a":
                    path.write_text("synthetic-foreign\n")

            transport = SyntheticLinear(replace)
            client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)
            self.assertEqual(client.issue("issue-1")["id"], "issue-1")
            with self.assertRaises(LinearError) as refused:
                client.issue("issue-1")
            self.assertFalse(refused.exception.retryable)
            self.assertEqual(transport.credentials(), [("identity", "synthetic-a"), ("operation", "synthetic-a"),
                                                       ("identity", "synthetic-foreign")])
            self.assertIs(client.token, provider)

    def test_connect_oauth_refresh_verifies_authorized_and_foreign_tokens(self):
        from test_linear_plugin import FakeConnect

        for refreshed in ("synthetic-fresh", "synthetic-foreign", "synthetic-foreign-org"):
            with self.subTest(refreshed=refreshed), tempfile.TemporaryDirectory() as directory:
                connect, clock, posts = FakeConnect(), Clock(), []
                item = ConnectItem("https://connect.example", "synthetic-connect", "vault-x", "item-x",
                                   transport=connect)

                def post(form):
                    posts.append(form["refresh_token"])
                    return {"access_token": "synthetic-stale" if len(posts) == 1 else refreshed,
                            "refresh_token": f"r{len(posts)}", "expires_in": 3600}

                cache = Path(directory) / "oauth.json"
                provider = ConnectOAuth(item, cache, clock=clock, post=post)
                transport = SyntheticLinear(lambda kind, credential, _: (401, {}, b"{}")
                                            if kind == "operation" and credential == "synthetic-stale" else None)
                client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport, clock=clock)
                if refreshed == "synthetic-fresh":
                    client.create_activity("client-1", "session-1", {"type": "response"})
                else:
                    with self.assertRaises(LinearError) as refused:
                        client.create_activity("client-1", "session-1", {"type": "response"})
                    self.assertFalse(refused.exception.retryable)
                expected = [("identity", "synthetic-stale"), ("operation", "synthetic-stale"),
                            ("identity", refreshed)]
                if refreshed == "synthetic-fresh":
                    expected.append(("operation", refreshed))
                self.assertEqual(transport.credentials(), expected)
                self.assertEqual(posts, ["r0", "r1"])
                self.assertEqual(cache.stat().st_mode & 0o777, 0o600)
                self.assertIs(client.token, provider)

    def test_rate_limit_blocks_credentials_until_reset_then_checks_replacement(self):
        for limited_kind in ("identity", "operation"):
            for status in (400, 429):
                with self.subTest(limited_kind=limited_kind, status=status):
                    clock = Clock()
                    provider = RefreshProvider(initial="synthetic-a")

                    def limit(kind, credential, _):
                        if kind == limited_kind and credential == "synthetic-a":
                            return status, {"X-RateLimit-Requests-Reset": str(int((clock() + 90) * 1000))}, json.dumps({
                                "errors": [{"extensions": {"code": "RATELIMITED"}}]}).encode()

                    transport = SyntheticLinear(limit)
                    client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport, clock=clock)
                    with self.assertRaises(RateLimited) as limited:
                        client.issue("issue-1")
                    self.assertEqual(limited.exception.until, clock() + 90)
                    before_pause = list(transport.requests)
                    provider.initial = "synthetic-foreign"
                    with self.assertRaises(RateLimited):
                        client.issue("issue-1")
                    self.assertEqual(transport.requests, before_pause)
                    self.assertEqual(provider.calls, 1)
                    clock.now += 90
                    with self.assertRaises(LinearError) as refused:
                        client.issue("issue-1")
                    self.assertFalse(refused.exception.retryable)
                    self.assertEqual(transport.credentials()[-1], ("identity", "synthetic-foreign"))
                    self.assertEqual(len(transport.requests), len(before_pause) + 1)

    def test_uncertain_create_is_not_automatically_retried_and_replay_rechecks_identity(self):
        provider = RefreshProvider(initial="synthetic-a")
        accepted, attempts = {}, []

        def create(kind, credential, variables):
            if kind == "operation":
                fields = variables["input"]
                client_id = fields["id"]
                attempts.append(dict(fields))
                if client_id not in accepted:
                    accepted[client_id] = dict(fields)
                    raise TimeoutError("synthetic lost response after acceptance")
                return 200, {}, json.dumps({"data": None, "errors": [{
                    "message": "conflict on insert of Comment", "extensions": {
                        "code": "INPUT_ERROR", "userPresentableMessage": f"Entity Comment with id {client_id} already exists."}
                }]}).encode()

        transport = SyntheticLinear(create)
        client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)
        with self.assertRaises(LinearError) as uncertain:
            client.create_comment("client-1", "issue-1", "body")
        self.assertTrue(uncertain.exception.retryable)
        self.assertEqual(len(attempts), 1)
        provider.initial = "synthetic-foreign"
        with self.assertRaises(LinearError) as refused:
            client.create_comment("client-1", "issue-1", "body")
        self.assertFalse(refused.exception.retryable)
        self.assertEqual(len(attempts), 1)
        provider.initial = "synthetic-a"
        client.create_comment("client-1", "issue-1", "body")
        self.assertEqual(len(accepted), 1)
        self.assertEqual(attempts, [accepted["client-1"], accepted["client-1"]])
        self.assertEqual(transport.credentials(), [("identity", "synthetic-a"), ("operation", "synthetic-a"),
                                                   ("identity", "synthetic-foreign"),
                                                   ("identity", "synthetic-a"), ("operation", "synthetic-a")])
        self.assertEqual(provider.invalidations, 0)

    def test_refresh_failure_is_retryable_without_another_operation(self):
        class Provider(RefreshProvider):
            def __call__(self):
                if self.invalidations:
                    raise RuntimeError("synthetic refresh unavailable")
                return super().__call__()

        provider = Provider()
        transport = SyntheticLinear(lambda kind, credential, _: (401, {}, b"{}") if kind == "operation" else None)
        client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)
        with self.assertLogs("linear", level="ERROR"), self.assertRaises(LinearError) as failed:
            client.update_issue("issue-1", {"stateId": "started"})
        self.assertTrue(failed.exception.retryable)
        self.assertEqual(transport.credentials(), [("identity", "synthetic-stale"), ("operation", "synthetic-stale")])
        self.assertEqual(provider.invalidations, 1)

    def test_optional_scope_lookups_and_writes_both_verify_their_credentials(self):
        credentials = iter(("synthetic-a", "synthetic-foreign"))
        transport = SyntheticLinear()
        identity = {**IDENTITY, "teams": ["OPS"], "projects": ["project-1"]}
        client = BoundLinearAPI(lambda: next(credentials), identity=identity, transport=transport)
        with self.assertRaises(LinearError) as refused:
            client.update_issue("issue-1", {"stateId": "started"})
        self.assertFalse(refused.exception.retryable)
        self.assertEqual(transport.credentials(), [("identity", "synthetic-a"), ("operation", "synthetic-a"),
                                                   ("identity", "synthetic-foreign")])


    def test_initial_operation_uses_the_credential_whose_identity_was_verified(self):
        supplied = []
        requests = []

        def provider():
            credential = "synthetic-a" if not supplied else "synthetic-foreign"
            supplied.append(credential)
            return credential

        def transport(url, body, headers):
            query = json.loads(body)["query"]
            credential = headers["Authorization"].removeprefix("Bearer ")
            requests.append(("identity" if "IdentityBinding" in query else "mutation", credential))
            data = ({"viewer": {"id": "app-a" if credential == "synthetic-a" else "foreign"},
                     "organization": {"id": "org-a"}} if "IdentityBinding" in query else
                    {"issueUpdate": {"success": True}})
            return 200, {}, json.dumps({"data": data}).encode()

        client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)
        client.update_issue("issue-1", {"stateId": "started"})
        self.assertEqual(requests, [("identity", "synthetic-a"), ("mutation", "synthetic-a")])
        self.assertEqual(supplied, ["synthetic-a"])

    def test_foreign_401_refresh_is_verified_and_refused_before_retrying_mutation(self):
        requests = []

        class Provider:
            invalidations = 0

            def __call__(self):
                return "synthetic-foreign" if self.invalidations else "synthetic-stale"

            def invalidate(self):
                self.invalidations += 1

        def transport(url, body, headers):
            query = json.loads(body)["query"]
            credential = headers["Authorization"].removeprefix("Bearer ")
            kind = "identity" if "IdentityBinding" in query else "mutation"
            requests.append((kind, credential))
            if kind == "identity":
                return 200, {}, json.dumps({"data": {
                    "viewer": {"id": "foreign" if credential == "synthetic-foreign" else "app-a"},
                    "organization": {"id": "org-a"}}}).encode()
            if credential == "synthetic-stale":
                return 401, {}, b"{}"
            return 200, {}, b'{"data": {"issueUpdate": {"success": true}}}'

        provider = Provider()
        client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)
        with self.assertRaises(LinearError) as refused:
            client.update_issue("issue-1", {"stateId": "started"})
        self.assertFalse(refused.exception.retryable)
        self.assertEqual(requests, [("identity", "synthetic-stale"), ("mutation", "synthetic-stale"),
                                    ("identity", "synthetic-foreign")])
        self.assertEqual(provider.invalidations, 1)

    def test_pause_raised_during_verification_prevents_the_operation(self):
        clock = Clock()
        requests = []

        def transport(url, body, headers):
            query = json.loads(body)["query"]
            requests.append(query)
            if "IdentityBinding" in query:
                # Another in-flight request can publish a pause while this identity reply arrives.
                client.paused_until = clock() + 90
                return 200, {}, b'{"data": {"viewer": {"id": "app-a"}, "organization": {"id": "org-a"}}}'
            return 200, {}, b'{"data": {"issueUpdate": {"success": true}}}'

        client = BoundLinearAPI(lambda: "synthetic-a", identity=IDENTITY, clock=clock, transport=transport)
        with self.assertRaises(RateLimited):
            client.update_issue("issue-1", {"stateId": "started"})
        self.assertEqual(len(requests), 1)

    def test_identity_401_refresh_keeps_the_authorized_refresh_for_the_operation(self):
        requests = []

        class Provider:
            invalidations = 0

            def __call__(self):
                return "synthetic-fresh" if self.invalidations else "synthetic-stale"

            def invalidate(self):
                self.invalidations += 1

        def transport(url, body, headers):
            query = json.loads(body)["query"]
            credential = headers["Authorization"].removeprefix("Bearer ")
            kind = "identity" if "IdentityBinding" in query else "mutation"
            requests.append((kind, credential))
            if kind == "identity" and credential == "synthetic-stale":
                return 401, {}, b"{}"
            data = ({"viewer": {"id": "app-a"}, "organization": {"id": "org-a"}}
                    if kind == "identity" else {"issueUpdate": {"success": True}})
            return 200, {}, json.dumps({"data": data}).encode()

        provider = Provider()
        client = BoundLinearAPI(provider, identity=IDENTITY, transport=transport)
        failure = None
        try:
            client.update_issue("issue-1", {"stateId": "started"})
        except LinearError as exc:
            failure = exc
        self.assertIsNone(failure, "an identity 401 must retain the existing one-refresh retry")
        self.assertEqual(requests, [("identity", "synthetic-stale"), ("identity", "synthetic-fresh"),
                                    ("mutation", "synthetic-fresh")])
        self.assertEqual(provider.invalidations, 1)
