"""Mid-turn Linear sends: interim notes, clarify questions and approval prompts.

Core sends these without ``reply_to``. The adapter used to refuse every one of them, so
a clarify was "[clarify prompt could not be delivered]", a text approval waited out
``approvals.timeout`` and denied, and heartbeats vanished. Here they post into the turn's
own Agent Session: interim notes as a ``thought``, questions as an ``elicitation`` (with
Linear's ``select`` signal for one pick), and the person's next prompt answers them
through Core's own clarify and approval resolution. The turn's reply is unchanged.

The Linear provider is the fixture; Core's adapter base, clarify and approval modules,
and the plugin's outbox are the real ones.
"""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_linear_native_bridge_scenarios as native

QUESTION = "Which environment should the migration run against?"
CHOICES = ["staging", "production"]


class Delivery:
    """One provider input as the reporting thread hands it to the adapter."""

    def __init__(self, event):
        self.profile, self.logical_agent = "default", "default"
        self.delivery_id = event["agentActivity"]["id"]
        self.payload = json.dumps(event)
        self.acknowledged = 0

    def event(self):
        return json.loads(self.payload)

    def acknowledge(self):
        self.acknowledged += 1


@unittest.skipIf(native.CORE is not None, f"native source unavailable ({native.CORE})")
class NativePromptScenarios(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = native.NativeBridgeScenarios()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.bridge, self.store, self.authority = self.fixture.bridge, self.fixture.store, self.fixture.authority
        self.posted = []
        original = self.bridge.api.transport
        def observe(url, body, headers):
            payload = json.loads(body)
            if "agentActivityCreate" in payload["query"]:
                self.posted.append(payload["variables"]["input"])
            return original(url, body, headers)
        self.bridge.api.transport = observe
        from gateway.config import Platform, PlatformConfig
        from gateway.platform_registry import platform_registry
        with patch.object(platform_registry, "is_registered", return_value=True):
            Platform("linear")
        from hermes_fleet_linear_plugin import native_input
        self.native_input = native_input
        # This finite prompt fixture projects receipts itself; guarded seam tests own the real Core ledger.
        committed = patch.object(native_input, "committed_conversation_state",
                                 return_value={"live": False, "latest_activity": None, "turn_live": True})
        committed.start()
        self.addCleanup(committed.stop)
        self.running = {"bridge": self.bridge}
        self.adapter = native_input.create_adapter(PlatformConfig(enabled=True), self.running)
        self.assertTrue(await self.adapter.connect())
        self.addAsyncCleanup(self.adapter.disconnect)
        # The runner's allowlist check: only the issue's person may answer.
        self.adapter.set_authorization_check(lambda user_id, *_args, **_kwargs: user_id == native.USER)
        self.messages = []
        async def admit(event):
            # Core commits the input, then calls the committed-input hook.
            self.messages.append(event)
            await self.adapter.on_message_admitted(event, SimpleNamespace(control=None,
                admission_id=event.message_id, sequence=len(self.messages), status="queued"))
        self.adapter.set_message_handler(lambda event: None)
        self.adapter.handle_message = admit
        self.loop = asyncio.get_running_loop()

    def delivery(self, identity, body, *, user=native.USER, delivery_id=None):
        from datetime import datetime, timezone
        self.authority.activities.setdefault(identity, {"id": identity, "user": {"id": user},
            "createdAt": datetime.now(timezone.utc).isoformat(), "agentSession": {"id": native.SESSION}})
        event = self.fixture.event("prompted", body=body)
        event["agentActivity"].update(id=identity, user={"id": user})
        delivery = Delivery(event)
        delivery.delivery_id = delivery_id or identity
        return delivery

    async def deliver(self, identity, body, *, user=native.USER, delivery_id=None):
        delivery = self.delivery(identity, body, user=user, delivery_id=delivery_id)
        await self.route(delivery)
        return delivery

    async def route(self, delivery):
        self.assertTrue(self.adapter.route(delivery))
        future = self.adapter._submitted.get(self.adapter._delivery_key(delivery))
        if future is not None:
            await asyncio.wait_for(asyncio.wrap_future(future), 10)

    async def start_turn(self):
        delivery = await self.deliver(native.ACTIVITY, "Run the schema migration for this issue.")
        self.assertEqual(delivery.acknowledged, 1)
        self.assertEqual(len(self.messages), 1)
        return self.messages[0]

    def session_key(self, event):
        return self.adapter._core_session_keys(event.source)[0] if hasattr(self.adapter, "_core_session_keys") \
            else self.adapter._source_session_key(event.source)

    async def until_posted(self, count):
        async with asyncio.timeout(10):
            while len(self.posted) < count:
                await asyncio.sleep(0.01)

    def ask(self, session_key, clarify_id, choices):
        """Core's clarify path: register, send the card, plain-text fallback, bounded wait."""
        from gateway.run_turn_runner_clarify_delivery import _clarify_send_then_wait, text_fallback_coro
        from tools import clarify_gateway as clarify
        send = dict(chat_id=native.ISSUE, question=QUESTION, choices=choices, clarify_id=clarify_id,
                    session_key=session_key, metadata={"thread_id": native.SESSION})
        clarify.register(clarify_id=clarify_id, session_key=session_key, question=QUESTION, choices=choices)
        future = asyncio.run_coroutine_threadsafe(self.adapter.send_clarify(**send), self.loop)
        def fallback():
            coro = text_fallback_coro(self.adapter, **send)
            return None if coro is None else asyncio.run_coroutine_threadsafe(coro, self.loop)
        return _clarify_send_then_wait(future, clarify_id=clarify_id, session_key=session_key,
                                       clarify_mod=clarify, fallback=fallback)

    def render_approval(self, session_key, data):
        """Exercise Core's real card routing and delivery-outcome fallback logic."""
        from gateway.run_turn_runner import TurnRunner
        runner = SimpleNamespace(
            _ctx=SimpleNamespace(_status_adapter=self.adapter, _status_chat_id=native.ISSUE,
                session_key=session_key, _status_thread_metadata={"thread_id": native.SESSION}),
            _schedule=lambda coro, _label: asyncio.run_coroutine_threadsafe(coro, self.loop),
            _close_native_stream_boundary=lambda _label: None)
        return TurnRunner._render_approval_sync(runner, data)

    def request_approval(self, session_key):
        from tools.approval_gateway_wait import _await_gateway_decision
        return _await_gateway_decision(session_key, lambda data: self.render_approval(session_key, data), {
            "command": "rm -rf build/", "description": "recursive delete", "pattern_key": "recursive delete"})

    async def display_approval(self, entry, **flags):
        key = self.session_key(self.messages[0])
        return await self.adapter.send_exec_approval(native.ISSUE, entry.data["command"], key,
            request_id=entry.data["request_id"], metadata={"thread_id": native.SESSION}, **flags)

    async def answer_later(self, key, waiter, cleanup):
        try:
            return await asyncio.wait_for(waiter, 10)
        finally:
            if not waiter.done():
                cleanup(key)

    async def test_interim_send_posts_a_thought_into_the_turns_session(self):
        from gateway.run import _interim_metadata
        await self.start_turn()
        result = await self.adapter.send(native.ISSUE, "Still checking the migration plan.",
                                         metadata=_interim_metadata({"thread_id": native.SESSION}))
        self.assertTrue(result.success, result.error)
        self.assertEqual(self.posted[-1]["agentSessionId"], native.SESSION)
        self.assertEqual(self.posted[-1]["content"], {"type": "thought", "body": "Still checking the migration plan."})
        self.assertNotIn("signal", self.posted[-1])

    async def test_clarify_with_choices_is_a_select_elicitation_and_the_reply_answers_it(self):
        from tools import clarify_gateway as clarify
        turn = await self.start_turn()
        key = self.session_key(turn)
        waiter = asyncio.create_task(asyncio.to_thread(self.ask, key, "clarify-env", CHOICES))
        await self.until_posted(1)
        question = self.posted[-1]
        self.assertEqual(question["agentSessionId"], native.SESSION)
        self.assertEqual(question["content"]["type"], "elicitation")
        self.assertIn(QUESTION, question["content"]["body"])
        self.assertEqual(question["signal"], "select")
        self.assertEqual([option["label"] for option in question["signalMetadata"]["options"]], CHOICES)
        self.assertTrue(all(option["value"].startswith(self.native_input.CHOICE_VALUE_PREFIX)
                            for option in question["signalMetadata"]["options"]))
        answer = await self.deliver("answer-activity", "production")
        response, answered = await self.answer_later(key, waiter, clarify.clear_session)
        self.assertEqual((response, answered), ("production", True))
        self.assertEqual(answer.acknowledged, 1)
        self.assertEqual(len(self.messages), 1)  # the answer went to the clarify, not a new turn

    async def test_open_clarify_takes_free_text_and_a_stranger_cannot_answer(self):
        from tools import clarify_gateway as clarify
        turn = await self.start_turn()
        key = self.session_key(turn)
        waiter = asyncio.create_task(asyncio.to_thread(self.ask, key, "clarify-open", None))
        await self.until_posted(1)
        self.assertEqual(self.posted[-1]["content"]["type"], "elicitation")
        self.assertNotIn("signal", self.posted[-1])
        await self.deliver("stranger-activity", "use production", user="someone-else")
        self.assertIsNotNone(clarify.get_pending("clarify-open"))
        await self.deliver("answer-activity", "Use staging first, please.")
        response, answered = await self.answer_later(key, waiter, clarify.clear_session)
        self.assertEqual((response, answered), ("Use staging first, please.", True))

    async def test_clarify_without_a_verified_session_is_a_definitive_failure(self):
        from gateway.run_turn_runner_clarify_delivery import UNDELIVERED
        response, answered = await asyncio.to_thread(self.ask, "agent:main:linear:dm:none", "clarify-none", CHOICES)
        self.assertEqual((response, answered), (UNDELIVERED, False))
        self.assertEqual(self.posted, [])

    async def test_text_approval_is_an_elicitation_and_approve_resolves_it(self):
        await self.check_approval("approve", "once")

    async def test_text_approval_deny_resolves_it(self):
        await self.check_approval("/deny not on the shared host", "deny")

    async def test_approval_confirmation_survives_a_previous_turns_final_response(self):
        await self.check_approval("approve", "once", reopened=True)

    async def check_approval(self, reply, choice, *, reopened=False):
        from tools import approval
        turn = await self.start_turn()
        if reopened:
            result = await self.adapter.send(native.ISSUE, "First check finished.", reply_to=native.ACTIVITY,
                                             metadata={"thread_id": native.SESSION})
            self.assertTrue(result.success, result.error)
            await self.deliver("next-activity", "Now run the migration.")
            turn = self.messages[-1]
        key = self.session_key(turn)
        expected = len(self.posted) + 1
        waiter = asyncio.create_task(asyncio.to_thread(self.request_approval, key))
        await self.until_posted(expected)
        prompt = self.posted[-1]
        self.assertEqual(prompt["agentSessionId"], native.SESSION)
        self.assertEqual(prompt["content"]["type"], "elicitation")
        self.assertIn("rm -rf build/", prompt["content"]["body"])
        answer = await self.deliver("answer-activity", reply)
        result = await self.answer_later(key, waiter, approval.unregister_gateway_notify)
        self.assertTrue(result["resolved"], result)
        self.assertEqual(result["choice"], choice)
        if choice == "deny":
            self.assertEqual(result["reason"], "not on the shared host")
        self.assertEqual(answer.acknowledged, 1)
        self.assertEqual(len(self.messages), 2 if reopened else 1)
        self.assertEqual(self.posted[-1]["content"]["type"], "thought")  # the confirmation

    def approval_value(self, entry, label="approve"):
        question = next(row for row in self.store.conversation_questions(native.ISSUE, native.SESSION)
                        if row["native_prompt"].get("request_id") == entry.data["request_id"])
        return next(option["value"] for option in question["signal_metadata"]["options"] if option["label"] == label)

    async def pending_approvals(self, *, display_sibling=True):
        """Real core FIFO entries, with no blocking command threads to leak on failure."""
        from tools import approval
        from tools.approval_gateway_wait import _ApprovalEntry
        turn = await self.start_turn()
        key = self.session_key(turn)
        entries = [_ApprovalEntry({"command": command}) for command in ("first", "second")]
        with approval._lock:
            approval._gateway_queues[key] = entries.copy()
        self.addCleanup(approval.unregister_gateway_notify, key)
        result = await self.display_approval(entries[0])
        self.assertTrue(result.success, result.error)
        if display_sibling:
            result = await self.display_approval(entries[1])
            self.assertTrue(result.success, result.error)
        return entries

    async def test_concurrent_deliveries_of_one_approval_activity_resolve_only_one_entry(self):
        first, second = await self.pending_approvals()
        first_value = self.approval_value(first)
        publishing, release = asyncio.Event(), asyncio.Event()
        original = self.adapter._post
        async def delayed(*args, **kwargs):
            publishing.set()
            await release.wait()
            return await original(*args, **kwargs)
        answer = self.delivery("answer-activity", first_value, delivery_id="first-delivery")
        duplicate = self.delivery("answer-activity", first_value, delivery_id="duplicate-delivery")
        with patch.object(self.adapter, "_post", side_effect=delayed):
            task = asyncio.create_task(self.route(answer))
            await asyncio.wait_for(publishing.wait(), 10)
            replay = asyncio.create_task(self.route(duplicate))
            try:
                async with asyncio.timeout(10):
                    while not replay.done() and second.result is None:
                        await asyncio.sleep(0.01)
                self.assertEqual([first.result, second.result], ["once", None])
                self.assertTrue(first.event.is_set())
                self.assertFalse(second.event.is_set())
            finally:
                release.set()
                await asyncio.gather(task, replay)
        self.assertEqual([answer.acknowledged, duplicate.acknowledged], [1, 1])
        self.assertEqual(len(self.messages), 1)
        self.assertEqual([p["content"]["type"] for p in self.posted],
                         ["elicitation", "elicitation", "thought", "elicitation"])
        restored = self.store.outbox_row(self.posted[-1]["id"])["payload"]
        self.assertEqual(restored["native_prompt"]["request_id"], second.data["request_id"])
        self.assertEqual(self.posted[-1]["content"], self.posted[1]["content"])
        self.assertEqual(self.posted[-1]["signalMetadata"], self.posted[1]["signalMetadata"])
        await self.deliver("new-answer-activity", "approve")
        self.assertEqual(second.result, "once")

    async def test_confirmation_failure_cannot_replay_an_approval_on_inbox_retry(self):
        first, second = await self.pending_approvals()
        first_value = self.approval_value(first)
        answer = self.delivery("answer-activity", first_value, delivery_id="failed-delivery")
        with patch.object(self.bridge, "flush", side_effect=RuntimeError("publication interrupted")):
            with self.assertRaisesRegex(RuntimeError, "publication interrupted"):
                await self.route(answer)
        self.assertEqual([first.result, second.result], ["once", None])
        self.assertEqual(answer.acknowledged, 0)
        retry = await self.deliver("answer-activity", first_value, delivery_id="retry-delivery")
        self.assertEqual(retry.acknowledged, 1)
        self.assertEqual([first.result, second.result], ["once", None])
        self.assertFalse(second.event.is_set())
        # The already-queued confirmation can still publish independently of inbox retry.
        await asyncio.to_thread(self.bridge.flush)
        self.assertEqual([p["content"]["type"] for p in self.posted],
                         ["elicitation", "elicitation", "thought", "elicitation"])
        restored = self.store.outbox_row(self.posted[-1]["id"])["payload"]
        self.assertEqual(restored["native_prompt"]["request_id"], second.data["request_id"])
        self.assertEqual(self.posted[-1]["content"], self.posted[1]["content"])
        self.assertEqual(self.posted[-1]["signalMetadata"], self.posted[1]["signalMetadata"])
        self.assertEqual(len(self.messages), 1)
        await self.deliver("new-answer-activity", "approve")
        self.assertEqual(second.result, "once")

    async def test_clarify_activity_replay_cannot_answer_the_next_question(self):
        from tools import clarify_gateway as clarify
        key = self.session_key(await self.start_turn())
        self.addCleanup(clarify.clear_session, key)
        for identity in ("first-question", "second-question"):
            clarify.register(clarify_id=identity, session_key=key, question=QUESTION, choices=CHOICES)
            result = await self.adapter.send_clarify(native.ISSUE, QUESTION, choices=CHOICES,
                clarify_id=identity, session_key=key, metadata={"thread_id": native.SESSION})
            self.assertTrue(result.success, result.error)
            if identity == "first-question":
                await self.deliver("answer-activity", "staging")
                self.assertEqual(clarify.wait_for_response(identity, timeout=0), "staging")
        duplicate = await self.deliver("answer-activity", "staging", delivery_id="duplicate-delivery")
        self.assertEqual(duplicate.acknowledged, 1)
        self.assertIsNotNone(clarify.get_pending("second-question"))
        await self.deliver("new-answer-activity", "production")
        self.assertEqual(clarify.wait_for_response("second-question", timeout=0), "production")
        self.assertEqual(len(self.messages), 1)

    async def replacement_adapter(self):
        """A separate adapter and Store connection, sharing only persisted provider state."""
        from copy import copy
        from gateway.config import PlatformConfig
        from hermes_fleet_linear_plugin.store import Store
        bridge = copy(self.bridge)
        bridge.store = Store(self.store.path)
        replacement = self.native_input.create_adapter(PlatformConfig(enabled=True), {"bridge": bridge})
        self.assertTrue(await replacement.connect())
        self.addAsyncCleanup(replacement.disconnect)
        replacement.set_message_handler(lambda event: None)
        replacement.set_authorization_check(lambda user_id, *_args, **_kwargs: user_id == native.USER)
        return replacement

    async def test_consumed_approval_activity_survives_adapter_replacement(self):
        first, second = await self.pending_approvals()
        first_value = self.approval_value(first)
        await self.deliver("answer-activity", first_value)
        self.assertEqual([first.result, second.result], ["once", None])
        self.adapter = await self.replacement_adapter()
        replay = await self.deliver("answer-activity", first_value, delivery_id="after-reconnect")
        self.assertEqual(replay.acknowledged, 1)
        self.assertEqual([first.result, second.result], ["once", None])
        await self.deliver("new-answer-activity", "approve")
        self.assertEqual(second.result, "once")

    async def test_two_adapters_cannot_consume_the_same_approval_activity(self):
        first, second = await self.pending_approvals()
        first_value = self.approval_value(first)
        other = await self.replacement_adapter()
        publishing, release = asyncio.Event(), asyncio.Event()
        original = self.adapter._post
        async def delayed(*args, **kwargs):
            publishing.set()
            await release.wait()
            return await original(*args, **kwargs)
        with patch.object(self.adapter, "_post", side_effect=delayed):
            task = asyncio.create_task(self.deliver("answer-activity", first_value))
            await asyncio.wait_for(publishing.wait(), 10)
            duplicate = self.delivery("answer-activity", first_value, delivery_id="other-adapter")
            try:
                self.assertTrue(other.route(duplicate))
                future = other._submitted.get(other._delivery_key(duplicate))
                if future is not None:
                    await asyncio.wait_for(asyncio.wrap_future(future), 10)
                self.assertEqual([first.result, second.result], ["once", None])
                self.assertEqual(duplicate.acknowledged, 1)
            finally:
                release.set()
                await task

    async def test_final_reply_is_unchanged_and_nothing_posts_after_it(self):
        from gateway.run import _interim_metadata
        from hermes_fleet_linear_plugin.bridge import stable_uuid4
        import hashlib
        await self.start_turn()
        body = "The migration ran on staging; production is next."
        result = await self.adapter.send(native.ISSUE, body, reply_to=native.ACTIVITY,
                                         metadata={"thread_id": native.SESSION})
        self.assertTrue(result.success, result.error)
        self.assertEqual(self.posted[-1]["content"], {"type": "response", "body": body})
        digest = hashlib.sha256(body.encode()).hexdigest()
        self.assertEqual(result.message_id, stable_uuid4("native-reply", self.bridge.profile, native.ISSUE,
                                                         native.SESSION, native.ACTIVITY, digest))
        late = await self.adapter.send(native.ISSUE, "Still working.",
                                       metadata=_interim_metadata({"thread_id": native.SESSION}))
        self.assertFalse(late.success)
        self.assertFalse(late.retryable)
        self.assertEqual(len(self.posted), 1)
        # A new prompt reopens the session: its interim notes post again.
        await self.deliver("next-activity", "Now production, please.")
        again = await self.adapter.send(native.ISSUE, "Starting production.",
                                        metadata=_interim_metadata({"thread_id": native.SESSION}))
        self.assertTrue(again.success, again.error)
        self.assertEqual(self.posted[-1]["content"]["type"], "thought")

    async def test_send_without_reply_to_and_no_verified_session_is_still_refused(self):
        result = await self.adapter.send(native.ISSUE, "Orphan note.", metadata={"thread_id": native.SESSION})
        self.assertFalse(result.success)
        self.assertEqual(self.posted, [])

    async def test_queued_interim_is_not_delivered_after_the_final_reply(self):
        from hermes_fleet_linear_plugin.native_input import queue_reply
        await self.start_turn()
        pending = queue_reply(self.bridge, native.ISSUE, native.SESSION, native.ACTIVITY,
                              "Late progress", kind="thought")
        result = await self.adapter.send(native.ISSUE, "Finished.", reply_to=native.ACTIVITY,
                                         metadata={"thread_id": native.SESSION})
        self.assertTrue(result.success, result.error)
        self.bridge.flush(only_id=pending)
        self.assertEqual([p["content"]["type"] for p in self.posted], ["response"])
        self.assertEqual(self.bridge.store.outbox_row(pending)["state"], "failed")

    async def test_native_done_also_closes_the_turns_progress_lane(self):
        await self.start_turn()
        self.bridge._decline(native.ISSUE, native.SESSION, "Done.", activity_id="native-done")
        self.bridge.flush()
        result = await self.adapter.send(native.ISSUE, "Late note.",
            metadata={"thread_id": native.SESSION, "_interim_send": True})
        self.assertFalse(result.success)
        self.assertEqual([p["content"]["type"] for p in self.posted], ["response"])
        await self.deliver("next-activity", "Start the next check.")
        result = await self.adapter.send(native.ISSUE, "Checking.",
            metadata={"thread_id": native.SESSION, "_interim_send": True})
        self.assertTrue(result.success, result.error)
        self.assertEqual(self.posted[-1]["content"]["type"], "thought")


    async def test_displayed_request_does_not_authorize_an_undisplayed_sibling_after_replacement(self):
        from tools import approval
        first, second = await self.pending_approvals(display_sibling=False)
        key = self.session_key(self.messages[0])
        self.assertEqual(approval.resolve_gateway_approval(key, "deny", request_id=first.data["request_id"]), 1)
        self.adapter = await self.replacement_adapter()
        # Ordinary fallback in this fixture must remain visible without a fake admission receipt.
        ordinary = []
        async def receive(event):
            ordinary.append(event)
        self.adapter.handle_message = receive
        await self.deliver("sibling-answer", "approve")
        self.assertIsNone(second.result)
        self.assertEqual([event.message_id for event in ordinary], ["sibling-answer"])

    async def test_exact_card_preserves_offered_approval_capabilities(self):
        from tools import approval
        from tools.approval_gateway_wait import _ApprovalEntry
        key = self.session_key(await self.start_turn())
        entry = _ApprovalEntry({"command": "bounded-command"})
        with approval._lock:
            approval._gateway_queues[key] = [entry]
        self.addCleanup(approval.unregister_gateway_notify, key)
        result = await self.display_approval(entry, allow_session=False, allow_permanent=False)
        self.assertTrue(result.success, result.error)
        await self.deliver("overbroad-answer", "/approve always")
        self.assertIsNone(entry.result)
        await self.deliver("exact-answer", "approve")
        self.assertEqual(entry.result, "once")

    async def test_core_renderer_declines_missing_identity_without_text_fallback(self):
        from gateway.run_turn_runner import _ExecApprovalDeclined
        key = self.session_key(await self.start_turn())
        with patch.object(self.adapter, "send", wraps=self.adapter.send) as fallback:
            with self.assertRaises(_ExecApprovalDeclined):
                await asyncio.to_thread(self.render_approval, key, {"command": "unbound-command"})
        fallback.assert_not_called()
        self.assertEqual(self.posted, [])

    async def test_core_renderer_preserves_ambiguous_exact_card_without_text_fallback(self):
        from tools import approval
        from tools.approval_gateway_wait import _ApprovalEntry
        key = self.session_key(await self.start_turn())
        entry = _ApprovalEntry({"command": "bounded-command"})
        with approval._lock:
            approval._gateway_queues[key] = [entry]
        self.addCleanup(approval.unregister_gateway_notify, key)
        with patch.object(self.bridge, "flush", return_value=0), \
                patch.object(self.adapter, "send", wraps=self.adapter.send) as fallback:
            await asyncio.to_thread(self.render_approval, key, entry.data)
        fallback.assert_not_called()
        await self.deliver("undelivered-answer", "approve")
        self.assertIsNone(entry.result)  # queued but unattempted is not a displayed card

    async def test_core_renderer_failed_card_cannot_fall_back_to_broad_consent(self):
        from gateway.platforms.base import SendResult
        from tools import approval
        from tools.approval_gateway_wait import _ApprovalEntry
        key = self.session_key(await self.start_turn())
        entry = _ApprovalEntry({"command": "bounded-command"})
        with approval._lock:
            approval._gateway_queues[key] = [entry]
        self.addCleanup(approval.unregister_gateway_notify, key)
        with patch.object(self.adapter, "_send_exec_approval_prompt", return_value=SendResult(False, error="failed")), \
                patch.object(self.adapter, "send", wraps=self.adapter.send) as fallback:
            await asyncio.to_thread(self.render_approval, key, entry.data)
        fallback.assert_called_once()
        self.assertEqual(self.posted, [])
        await self.deliver("unbound-answer", "approve")
        self.assertIsNone(entry.result)

    async def old_first_delivery(self, kind):
        from tools import approval, clarify_gateway as clarify
        from tools.approval_gateway_wait import _ApprovalEntry
        key = self.session_key(await self.start_turn())
        old = self.delivery("old-unseen-answer", "approve" if kind == "approval" else "staging")
        self.assertEqual(len(self.messages), 1)
        if kind == "approval":
            entry = _ApprovalEntry({"command": "new-command"})
            with approval._lock:
                approval._gateway_queues[key] = [entry]
            self.addCleanup(approval.unregister_gateway_notify, key)
            result = await self.display_approval(entry)
        else:
            clarify.register(clarify_id="new-question", session_key=key, question=QUESTION, choices=CHOICES)
            self.addCleanup(clarify.clear_session, key)
            result = await self.adapter.send_clarify(native.ISSUE, QUESTION, CHOICES, "new-question", key,
                metadata={"thread_id": native.SESSION})
        self.assertTrue(result.success, result.error)
        handle = self.adapter.handle_message
        self.adapter = await self.replacement_adapter()
        self.adapter.handle_message = handle
        await self.route(old)
        self.assertEqual(old.acknowledged, 1)
        self.assertEqual(len(self.messages), 2)  # first delivery remains ordinary input
        if kind == "approval":
            self.assertIsNone(entry.result)
        else:
            self.assertIsNotNone(clarify.get_pending("new-question"))
        await self.deliver("fresh-answer", "approve" if kind == "approval" else "production")
        if kind == "approval":
            self.assertEqual(entry.result, "once")
        else:
            self.assertEqual(clarify.wait_for_response("new-question", timeout=0.1), "production")

    async def test_first_delivery_created_before_approval_is_not_consent(self):
        await self.old_first_delivery("approval")

    async def test_first_delivery_created_before_clarify_is_not_an_answer(self):
        await self.old_first_delivery("clarify")

    async def test_missing_authoritative_question_time_cannot_grant_consent(self):
        from hermes_fleet_linear_plugin.api import LinearError
        first, second = await self.pending_approvals(display_sibling=False)
        question = self.authority.activities[self.posted[-1]["id"]]
        stamp = question.pop("createdAt")
        answer = self.delivery("answer-activity", "approve")
        with self.assertRaises(LinearError):
            await self.route(answer)
        self.assertIsNone(first.result)
        self.assertIsNone(second.result)
        self.assertEqual(answer.acknowledged, 0)
        question["createdAt"] = stamp
        await self.deliver("answer-activity", "approve", delivery_id="chronology-restored")
        self.assertEqual(first.result, "once")

    async def test_approval_keeps_the_operation_context_that_core_rendered(self):
        from tools import approval
        from tools.approval_gateway_wait import _ApprovalEntry
        key = self.session_key(await self.start_turn())
        description = "Cancel task t_12345678 only after its logs are saved."
        entry = _ApprovalEntry({"command": "rm -rf build/", "description": description})
        with approval._lock:
            approval._gateway_queues[key] = [entry]
        self.addCleanup(approval.unregister_gateway_notify, key)
        await asyncio.to_thread(self.render_approval, key, entry.data)
        displayed = self.posted[-1]["content"]["body"]
        await self.deliver("answer-activity", "approve")
        self.assertEqual(entry.result, "once")
        self.assertIn(description, displayed)
        self.assertIn(entry.data["command"], displayed)

    async def altered_question_cannot_answer(self, *, legacy=False):
        from tools import clarify_gateway as clarify
        from hermes_fleet_linear_plugin.bridge import INTERNAL_ONLY_REPLY
        key = self.session_key(await self.start_turn())
        question = "Do you want me to cancel task t_12345678?"
        pending = clarify.register("stored-question", key, question, ["Yes", "No"])
        self.addCleanup(clarify.clear_session, key)
        sent = await self.adapter.send_clarify(native.ISSUE, question, pending.choices, pending.clarify_id, key,
            metadata={"thread_id": native.SESSION})
        self.assertTrue(sent.success, sent.error)
        payload = self.store.outbox_row(sent.message_id)["payload"]
        if legacy:
            # Exact prior-version shape: a substituted notice retains the Core
            # identity and select options, but has no faithful-display evidence.
            payload["native_prompt"].pop("display", None)
            payload["content"]["body"] = INTERNAL_ONLY_REPLY
            self.authority.activities[sent.message_id]["content"]["body"] = INTERNAL_ONLY_REPLY
        else:
            payload["signal_metadata"] = {"options": [{"value": "No"}, {"value": "Yes"}]}
        with self.store._tx() as db:
            db.execute("UPDATE outbox SET payload=? WHERE id=?", (json.dumps(payload), sent.message_id))
        retry = await self.adapter.send_clarify(native.ISSUE, question, pending.choices, pending.clarify_id, key,
            metadata={"thread_id": native.SESSION})
        handle = self.adapter.handle_message
        self.adapter = await self.replacement_adapter()
        self.adapter.handle_message = handle
        await self.deliver("answer-activity", "Yes")
        self.assertIsNone(pending.response, "altered decision content retained consent routing")
        self.assertEqual(len(self.messages), 2)
        self.assertFalse(retry.success, "stored substituted content was reported as a faithful delivery")

    async def test_legacy_substituted_notice_cannot_answer_after_replacement(self):
        await self.altered_question_cannot_answer(legacy=True)

    async def test_changed_visible_options_cannot_answer_after_replacement(self):
        await self.altered_question_cannot_answer()

    async def test_empty_clarification_question_is_refused_without_consent_routing(self):
        from tools import clarify_gateway as clarify
        key = self.session_key(await self.start_turn())
        pending = clarify.register("empty-question", key, " ", ["Yes", "No"])
        self.addCleanup(clarify.clear_session, key)
        sent = await self.adapter.send_clarify(native.ISSUE, " ", pending.choices, pending.clarify_id, key,
            metadata={"thread_id": native.SESSION})
        self.assertFalse(sent.success)
        self.assertEqual(self.posted, [])
        await self.deliver("answer-activity", "Yes")
        self.assertIsNone(pending.response)
        self.assertEqual(len(self.messages), 2)


    async def test_legacy_approval_option_cannot_impersonate_new_request_text_consent(self):
        from tools import approval
        from tools.approval_gateway_wait import _ApprovalEntry
        key = self.session_key(await self.start_turn())
        first, second = [_ApprovalEntry({"command": command}) for command in ("old-command", "new-command")]
        with approval._lock:
            approval._gateway_queues[key] = [first, second]
        self.addCleanup(approval.unregister_gateway_notify, key)
        with patch.object(self.bridge, "flush", return_value=0):
            queued = await self.display_approval(first)
        payload = self.store.outbox_row(queued.message_id)["payload"]
        # Exact pre-upgrade select protocol: the emitted value contains no request identity.
        payload["signal_metadata"] = {"options": [{"value": word} for word in ("approve", "approve session", "approve always", "deny")]}
        payload["native_prompt"].pop("request_identity", None)
        payload["native_prompt"].pop("selection_answers", None)
        payload["native_prompt"]["display"]["signal_metadata"] = payload["signal_metadata"]
        self.store.rewrite(queued.message_id, payload, self.bridge.clock())
        await asyncio.to_thread(self.bridge.flush, only_id=queued.message_id)
        self.assertEqual(self.store.outbox_row(queued.message_id)["state"], "sent")
        await self.deliver("old-request-answer", "approve")
        self.assertEqual(first.result, "once")
        self.assertEqual(self.store.outbox_row(queued.message_id)["payload"]["answered_by"], "old-request-answer")
        self.assertTrue((await self.display_approval(second)).success)
        selected = self.approval_value(second)
        await self.deliver("old-card-click", "approve")
        self.assertIsNone(second.result, "An old unbound card selected the new command")
        await self.deliver("new-card-click", selected)
        self.assertEqual(second.result, "once")

    async def answer_marks_every_presentation(self, *, legacy=False):
        from tools import approval
        from tools.approval_gateway_wait import _ApprovalEntry
        from hermes_fleet_linear_plugin.bridge import stable_uuid4
        key = self.session_key(await self.start_turn())
        entry = _ApprovalEntry({"command": "bounded-command"})
        with approval._lock:
            approval._gateway_queues[key] = [entry]
        self.addCleanup(approval.unregister_gateway_notify, key)
        with patch.object(self.bridge, "flush", return_value=0):
            first = await self.display_approval(entry)
        payload = self.store.outbox_row(first.message_id)["payload"]
        if legacy:
            options = payload["signal_metadata"]["options"]
            payload["signal_metadata"] = {"options": [{"value": option["label"]} for option in options]}
            payload["native_prompt"].pop("request_identity", None)
            payload["native_prompt"].pop("selection_answers", None)
            payload["native_prompt"]["display"]["signal_metadata"] = payload["signal_metadata"]
            self.store.rewrite(first.message_id, payload, self.bridge.clock())
        await asyncio.to_thread(self.bridge.flush, only_id=first.message_id)
        second_id = stable_uuid4("fixture-question-redisplay", first.message_id)
        repeated = {**payload, "id": second_id, "write_started": False}
        repeated.pop("applied", None)
        repeated.pop("conversation_attempt_id", None)
        self.assertEqual(self.store.enqueue("activity", repeated, at=self.bridge.clock()), second_id)
        await asyncio.to_thread(self.bridge.flush, only_id=second_id)
        self.assertEqual(self.store.outbox_row(second_id)["state"], "sent")
        value = payload["signal_metadata"]["options"][0]["value"]
        await self.deliver("same-request-answer", value)
        self.assertEqual(entry.result, "once")
        for identity in (first.message_id, second_id):
            self.assertEqual(self.store.outbox_row(identity)["payload"]["answered_by"], "same-request-answer")
        # A later unrelated question still has the normal single-question text path.
        next_entry = _ApprovalEntry({"command": "next-command"})
        with approval._lock:
            approval._gateway_queues[key] = [next_entry]
        self.assertTrue((await self.display_approval(next_entry)).success)
        await self.deliver("next-request-answer", self.approval_value(next_entry) if legacy else "approve")
        self.assertEqual(next_entry.result, "once")

    async def test_one_answer_marks_every_presentation_of_its_exact_request(self):
        await self.answer_marks_every_presentation()

    async def test_one_answer_marks_legacy_redisplays_by_exact_core_identity(self):
        await self.answer_marks_every_presentation(legacy=True)

    async def test_tampered_native_answer_mapping_cannot_change_a_displayed_capability(self):
        first, _ = await self.pending_approvals(display_sibling=False)
        question = next(row for row in self.store.conversation_questions(native.ISSUE, native.SESSION)
                        if row["native_prompt"].get("request_id") == first.data["request_id"])
        selected = question["signal_metadata"]["options"][0]["value"]
        question["native_prompt"]["selection_answers"][selected] = "approve always"
        with self.store._tx() as db:
            db.execute("UPDATE outbox SET payload=? WHERE id=?", (json.dumps(question), question["id"]))
        await self.deliver("altered-native-value", selected)
        self.assertIsNone(first.result)

    async def test_late_consumed_redisplay_cannot_impersonate_text_consent_to_a_new_request(self):
        from tools import approval
        from tools.approval_gateway_wait import _ApprovalEntry
        from hermes_fleet_linear_plugin.bridge import stable_uuid4
        key = self.session_key(await self.start_turn())
        first, second = [_ApprovalEntry({"command": command}) for command in ("old-command", "new-command")]
        with approval._lock:
            approval._gateway_queues[key] = [first, second]
        self.addCleanup(approval.unregister_gateway_notify, key)
        original = await self.display_approval(first)
        payload = self.store.outbox_row(original.message_id)["payload"]
        repeated_id = stable_uuid4("fixture-late-answered-question", original.message_id)
        repeated = {**payload, "id": repeated_id, "write_started": False}
        repeated.pop("applied", None)
        repeated.pop("conversation_attempt_id", None)
        self.store.enqueue("activity", repeated, at=self.bridge.clock())
        transport, delayed = self.bridge.api.transport, []
        def hold_redisplay(url, body, headers):
            request = json.loads(body)
            if "agentActivityCreate" in request["query"] and request["variables"]["input"]["id"] == repeated_id:
                delayed.append((url, body, headers))
                raise TimeoutError("fixture redisplay remains remotely in flight")
            return transport(url, body, headers)
        self.bridge.api.transport = hold_redisplay
        await asyncio.to_thread(self.bridge.flush, only_id=repeated_id)
        self.assertTrue(self.store.outbox_row(repeated_id)["payload"]["write_started"])
        await self.deliver("old-request-answer", self.approval_value(first))
        self.assertEqual(first.result, "once")
        self.assertEqual(self.store.outbox_row(repeated_id)["payload"]["answered_by"], "old-request-answer")
        self.assertTrue((await self.display_approval(second)).success)
        selected = self.approval_value(second)
        transport(*delayed[0])  # A appears after both its own answer and B's display.
        await self.deliver("ambiguous-after-redisplay", "approve")
        self.assertIsNone(second.result, "An already-consumed but late-visible card was wrongly skipped")
        await self.deliver("exact-new-card-selection", selected)
        self.assertEqual(second.result, "once")

if __name__ == "__main__":
    unittest.main()
