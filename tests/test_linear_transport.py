"""Private RPC framing and process isolation; official producer coverage is in native_frontdoor."""
from __future__ import annotations
import contextvars
import json
import multiprocessing
import os
import tempfile
import unittest
from pathlib import Path
from linear_fake_api import load_plugin
load_plugin()
from hermes_fleet_linear_plugin import transport


def context(home):
    return transport.NativeContext(home=str(home), profile='alpha', platform='api_server', source='',
                                   session_key='host-key', session_id='host-id', user_id='human', scope_id='workspace')


def gateway_process(home, ready, stop):
    server = transport.Server(Path(home), 'alpha', lambda args, host:
        json.dumps({'ok': True, 'context': host.to_wire(), 'args': args, 'pid': os.getpid()}))
    ready.set()
    stop.wait(10)
    server.close()


class LinearTransportTests(unittest.TestCase):
    def test_separate_process_preserves_all_host_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            ctx = multiprocessing.get_context('spawn')
            ready, stop = ctx.Event(), ctx.Event()
            worker = ctx.Process(target=gateway_process, args=(str(home), ready, stop))
            worker.start()
            try:
                self.assertTrue(ready.wait(5))
                host = context(home)
                for action in ('status', 'track', 'followup', 'add_comment'):
                    result = json.loads(transport.chat_request(home, {'action': action, 'profile': 'forged'}, host))
                    self.assertNotEqual(result['pid'], os.getpid())
                    self.assertEqual(result['context'], host.to_wire())
                self.assertEqual(transport.request(home, {'op': 'status'})['pid'], worker.pid)
            finally:
                stop.set()
                worker.join(5)
                if worker.is_alive():
                    worker.terminate()
                    worker.join()
            self.assertEqual(worker.exitcode, 0)
            self.assertFalse(transport.endpoint(home).exists())

    def test_foreign_home_and_duplicate_listener_are_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            home, seen = Path(tmp), []
            server = transport.Server(home, 'alpha', lambda *args: seen.append(args))
            try:
                with self.assertRaises(transport.Unavailable):
                    transport.Server(home, 'alpha', lambda *args: None)
                with self.assertRaises(transport.Uncertain):
                    transport.request(home, {'op': 'chat', 'home': '/foreign', 'args': {}, 'context': context(home).to_wire()})
                self.assertEqual(seen, [])
            finally:
                server.close()

    def test_scoped_home_context_is_preserved_into_rpc_consumer(self):
        scope = contextvars.ContextVar('profile-scope', default='foreign')
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            token = scope.set('alpha')
            server = transport.Server(home, 'alpha', lambda args, host: scope.get())
            scope.reset(token)
            try:
                self.assertEqual(transport.chat_request(home, {}, context(home)), 'alpha')
            finally:
                server.close()

    def test_unsafe_paths_and_missing_service_do_not_create_an_executor(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            with self.assertRaises(transport.Unavailable):
                transport.request(home, {'op': 'status'})
            transport.endpoint(home).parent.mkdir(mode=0o755)
            transport.endpoint(home).parent.chmod(0o755)
            with self.assertRaises(transport.Unavailable):
                transport.Server(home, 'alpha', lambda *args: None)
            transport.endpoint(home).parent.chmod(0o700)
            transport.endpoint(home).write_text('not a socket')
            with self.assertRaises(transport.Unavailable):
                transport.Server(home, 'alpha', lambda *args: None)

    def test_wire_context_rejects_extra_or_missing_identity_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            for mutation in ({'profile': 'foreign'}, {'home': '/foreign'}, {'session_id': ''}, {'extra': 'model'}):
                with self.subTest(mutation=mutation), self.assertRaises(transport.Unavailable):
                    transport.NativeContext.from_wire({**context(home).to_wire(), **mutation}, home, 'alpha')
