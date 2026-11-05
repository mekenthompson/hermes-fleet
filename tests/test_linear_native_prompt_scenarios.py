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
        self.authority.activities[identity] = {"id": identity, "user": {"id": user},
                                               "agentSession": {"id": native.SESSION}}
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

    def request_approval(self, session_key):
        """Core's text approval path (no native buttons): the prompt, then the gateway wait."""
        from gateway.run import _format_exec_approval_fallback, _interim_metadata
        from tools.approval_gateway_wait import _await_gateway_decision
        def notify(data):
            text = _format_exec_approval_fallback(data["command"], data["description"], "/")
            metadata = {"thread_id": native.SESSION, "is_approval_prompt": True}
            asyncio.run_coroutine_threadsafe(self.adapter.send(
                native.ISSUE, text, metadata=_interim_metadata(metadata)), self.loop).result(timeout=15)
        return _await_gateway_decision(session_key, notify, {
            "command": "rm -rf build/", "description": "recursive delete", "pattern_key": "recursive delete"})

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
        self.assertEqual(question["signalMetadata"], {"options": [{"value": "staging"}, {"value": "production"}]})
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

    async def pending_approvals(self):
        """Real core FIFO entries, with no blocking command threads to leak on failure."""
        from tools import approval
        from tools.approval_gateway_wait import _ApprovalEntry
        turn = await self.start_turn()
        key = self.session_key(turn)
        entries = [_ApprovalEntry({"command": command}) for command in ("first", "second")]
        with approval._lock:
            approval._gateway_queues[key] = entries.copy()
        self.addCleanup(approval.unregister_gateway_notify, key)
        result = await self.adapter.send(native.ISSUE, "Approve the first command?", metadata={
            "thread_id": native.SESSION, "is_approval_prompt": True})
        self.assertTrue(result.success, result.error)
        return entries

    async def test_concurrent_deliveries_of_one_approval_activity_resolve_only_one_entry(self):
        first, second = await self.pending_approvals()
        publishing, release = asyncio.Event(), asyncio.Event()
        original = self.adapter._post
        async def delayed(*args, **kwargs):
            publishing.set()
            await release.wait()
            return await original(*args, **kwargs)
        answer = self.delivery("answer-activity", "approve", delivery_id="first-delivery")
        duplicate = self.delivery("answer-activity", "approve", delivery_id="duplicate-delivery")
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
        self.assertEqual([p["content"]["type"] for p in self.posted], ["elicitation", "thought"])
        await self.deliver("new-answer-activity", "approve")
        self.assertEqual(second.result, "once")

    async def test_confirmation_failure_cannot_replay_an_approval_on_inbox_retry(self):
        first, second = await self.pending_approvals()
        answer = self.delivery("answer-activity", "approve", delivery_id="failed-delivery")
        with patch.object(self.bridge, "flush", side_effect=RuntimeError("publication interrupted")):
            with self.assertRaisesRegex(RuntimeError, "publication interrupted"):
                await self.route(answer)
        self.assertEqual([first.result, second.result], ["once", None])
        self.assertEqual(answer.acknowledged, 0)
        retry = await self.deliver("answer-activity", "approve", delivery_id="retry-delivery")
        self.assertEqual(retry.acknowledged, 1)
        self.assertEqual([first.result, second.result], ["once", None])
        self.assertFalse(second.event.is_set())
        # The already-queued confirmation can still publish independently of inbox retry.
        await asyncio.to_thread(self.bridge.flush)
        self.assertEqual([p["content"]["type"] for p in self.posted], ["elicitation", "thought"])
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
        await self.deliver("answer-activity", "approve")
        self.assertEqual([first.result, second.result], ["once", None])
        self.adapter = await self.replacement_adapter()
        self.adapter._asked_in(native.ISSUE, native.SESSION)
        replay = await self.deliver("answer-activity", "approve", delivery_id="after-reconnect")
        self.assertEqual(replay.acknowledged, 1)
        self.assertEqual([first.result, second.result], ["once", None])
        await self.deliver("new-answer-activity", "approve")
        self.assertEqual(second.result, "once")

    async def test_two_adapters_cannot_consume_the_same_approval_activity(self):
        first, second = await self.pending_approvals()
        other = await self.replacement_adapter()
        other._asked_in(native.ISSUE, native.SESSION)
        publishing, release = asyncio.Event(), asyncio.Event()
        original = self.adapter._post
        async def delayed(*args, **kwargs):
            publishing.set()
            await release.wait()
            return await original(*args, **kwargs)
        with patch.object(self.adapter, "_post", side_effect=delayed):
            task = asyncio.create_task(self.deliver("answer-activity", "approve"))
            await asyncio.wait_for(publishing.wait(), 10)
            duplicate = self.delivery("answer-activity", "approve", delivery_id="other-adapter")
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


if __name__ == "__main__":
    unittest.main()
