"""No models: physical worker deadlines, bounded queues and process recovery."""
import os
import io
import json
from contextlib import ExitStack
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch
from concurrent.futures import TimeoutError

import server
from worker_lane import WorkLane, LaneBusy, DeadlineExpired


class WorkLaneTests(unittest.TestCase):
    def test_both_http_handlers_cancel_expired_queue_entries_and_reject_overload(self):
        class Request:
            def __init__(self):
                body = json.dumps({'text': 'hello'}).encode()
                self.headers = {'Content-Length': str(len(body))}
                self.rfile = io.BytesIO(body)
                self.response = None
            def _authed(self): return True
            def _send_json(self, status, body): self.response = (status, body)
        for name, pool_name, model, timeout, function, handler in [
            ('tts', '_tts_pool', '_tts', 'TTS_REQUEST_TIMEOUT_S', '_run_synthesis', server.Handler._handle_tts),
            ('stt', '_pool', '_model', 'REQUEST_TIMEOUT_S', '_run_inference', server.Handler._handle_stt),
        ]:
            entered, release = threading.Event(), threading.Event()
            lane = WorkLane(name, workers=1, pending=1, on_stalled=lambda *_: None)
            try:
                lane.submit(lambda: (entered.set(), release.wait(3)), timeout=2)
                self.assertTrue(entered.wait(1))
                with ExitStack() as stack:
                    for key, value in [(pool_name, lane), (model, object()), (timeout, .02), ('_wedged', False)]:
                        stack.enter_context(patch.object(server, key, value))
                    stack.enter_context(patch.object(server, '_parse_multipart', return_value=(b'audio', None)))
                    stack.enter_context(patch.object(server, '_transcode_to_wav', return_value=b'wav'))
                    compute = stack.enter_context(patch.object(server, function))
                    submitted = []
                    submit = lane.submit
                    def tracked_submit(*args, **kwargs):
                        future = submit(*args, **kwargs)
                        submitted.append(future)
                        return future
                    stack.enter_context(patch.object(lane, 'submit', side_effect=tracked_submit))
                    expired = Request()
                    handler(expired)
                    self.assertEqual(expired.response, (503, {'ok': False, 'reason': 'timeout'}))
                    self.assertTrue(submitted[0].cancelled(), 'HTTP deadline must cancel queued inference')
                    overloaded = Request()
                    handler(overloaded)
                    self.assertEqual(overloaded.response, (503, {'ok': False, 'reason': 'busy'}))
                    release.set()
                    self.assertTrue(lane.close())
                    compute.assert_not_called()
            finally:
                release.set()
                lane.close()

    def test_timeout_cancels_queued_work_without_freeing_queue_capacity_early(self):
        entered, release, unwanted = threading.Event(), threading.Event(), threading.Event()
        lane = WorkLane('test', workers=1, pending=1, on_stalled=lambda *_: None)
        def blocked():
            entered.set()
            release.wait(3)
        try:
            running = lane.submit(blocked, timeout=2)
            self.assertTrue(entered.wait(1))
            queued = lane.submit(unwanted.set, timeout=.02)
            with self.assertRaises(TimeoutError): queued.result(timeout=.03)
            self.assertTrue(queued.cancel())
            # A cancelled executor entry still occupies bounded physical space.
            for _ in range(100):
                with self.assertRaises(LaneBusy): lane.submit(unwanted.set, timeout=1)
            release.set()
            running.result(timeout=1)
        finally:
            release.set()
            lane.close()
        self.assertFalse(unwanted.is_set())

    def test_queued_deadline_is_checked_before_native_work_starts(self):
        entered, release, unwanted = threading.Event(), threading.Event(), threading.Event()
        lane = WorkLane('test', workers=1, pending=1, on_stalled=lambda *_: None)
        try:
            first = lane.submit(lambda: (entered.set(), release.wait(3)), timeout=2)
            self.assertTrue(entered.wait(1))
            second = lane.submit(unwanted.set, timeout=.01)
            time.sleep(.02)
            release.set()
            first.result(timeout=1)
            with self.assertRaises(DeadlineExpired): second.result(timeout=1)
        finally:
            release.set()
            lane.close()
        self.assertFalse(unwanted.is_set())

    def test_physical_stall_is_detected_without_any_more_requests(self):
        release, stalled = threading.Event(), threading.Event()
        lane = WorkLane('tts', workers=1, pending=1, on_stalled=lambda name: stalled.set(), stall_multiplier=1)
        try:
            work = lane.submit(lambda: release.wait(3), timeout=.03)
            self.assertTrue(stalled.wait(1))
            self.assertFalse(work.done(), 'watchdog must track physical execution')
        finally:
            release.set()
            lane.close()

    def test_normal_completion_cancels_watchdog_and_reuses_capacity(self):
        stalled = threading.Event()
        lane = WorkLane('test', workers=1, pending=1, on_stalled=lambda *_: stalled.set(), stall_multiplier=1)
        try:
            for _ in range(10): self.assertEqual(lane.submit(lambda: 'ok', timeout=.05).result(timeout=1), 'ok')
            self.assertFalse(stalled.wait(.08))
        finally: lane.close()

    def test_hung_process_exits_and_fresh_process_accepts_work(self):
        directory = str(Path(server.__file__).resolve().parent)
        env = {**os.environ, 'PYTHONPATH': directory, 'VOICE_WATCHDOG_RESTART_GRACE_S': '0.02'}
        code = '''
import threading
import server
from worker_lane import WorkLane
stuck = threading.Event()
lane = WorkLane('tts', workers=1, pending=1, on_stalled=server._request_recovery, stall_multiplier=1)
lane.submit(lambda: stuck.wait(60), timeout=.03)
# Healthy STT cannot mask a wedged TTS lane.
healthy = WorkLane('stt', workers=1, pending=1, on_stalled=server._request_recovery)
while True:
    healthy.submit(lambda: True, timeout=1).result(timeout=1)
'''
        result = subprocess.run([sys.executable, '-c', code], env=env, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 70, result.stderr.decode())
        recovered = subprocess.run([sys.executable, '-c',
            "from worker_lane import WorkLane; lane=WorkLane('tts',workers=1,pending=1,on_stalled=lambda *_: None); print(lane.submit(lambda: 'recovered', timeout=1).result(timeout=2)); lane.close()"],
            env=env, capture_output=True, timeout=5)
        self.assertEqual(recovered.returncode, 0, recovered.stderr.decode())
        self.assertEqual(recovered.stdout.strip(), b'recovered')
