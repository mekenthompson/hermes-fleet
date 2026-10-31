#!/usr/bin/env python3
"""Bounded functional acceptance for a credential-free disposable container, not a live profile.

Run with the installed Agent interpreter. No downloads, profile migrations, public ports,
or sandbox fallbacks. The caller must enforce Docker timeout and remove the test container.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import urllib.request

FORBIDDEN = {'--no-sandbox', '--disable-setuid-sandbox', '--disable-seccomp-filter-sandbox',
             '--disable-namespace-sandbox', '--single-process', '--no-zygote',
             '--disable-gpu-sandbox', '--in-process-gpu'}


def sandbox_proven(text: str, renderers: list[dict]) -> bool:
    required = ('Layer 1 Sandbox\tNamespace', 'PID namespaces\tYes', 'Network namespaces\tYes',
                'Seccomp-BPF sandbox\tYes', 'Seccomp-BPF sandbox supports TSYNC\tYes',
                'You are adequately sandboxed.')
    if not all(row in text for row in required) or not renderers:
        return False
    try:
        return all(r['uid'] == ['1000'] * 4
                   and len(r['nspid']) >= 3 and r['nnp'] == '1' and r['seccomp'] == '2'
                   and int(r['filters']) >= 2 and int(r['caps'], 16) == 0
                   and not any(arg.split('=', 1)[0] in FORBIDDEN for arg in r['argv'])
                   for r in renderers)
    except (KeyError, ValueError, TypeError):
        return False


def renderer_evidence(profile: str) -> list[dict]:
    result = []
    for process in Path('/proc').iterdir():
        if not process.name.isdigit():
            continue
        try:
            # Chromium rewrites renderer cmdline as one space-separated process title.
            # This verifier owns the temporary profile path (no user-supplied argv).
            argv = (process / 'cmdline').read_bytes().decode().replace('\0', ' ').split()
            if '--type=renderer' not in argv or '--user-data-dir=' + profile not in argv:
                continue
            status = dict(line.split(':', 1) for line in (process / 'status').read_text().splitlines())
            result.append({'pid': int(process.name), 'argv': argv, 'uid': status['Uid'].split(),
                           'nspid': status['NSpid'].split(), 'nnp': status['NoNewPrivs'].strip(),
                           'seccomp': status['Seccomp'].strip(), 'filters': status['Seccomp_filters'].strip(),
                           'caps': status['CapEff'].strip()})
        except (OSError, UnicodeError, KeyError, ValueError) as exc:
            print(f'renderer evidence unavailable: {process.name}: {exc}', file=sys.stderr)
            continue
    return result


class CDP:
    def __init__(self, endpoint: str):
        from websockets.sync.client import connect
        with urllib.request.urlopen(endpoint + '/json/list', timeout=5) as response:
            pages = json.load(response)
        self.socket = connect(next(p['webSocketDebuggerUrl'] for p in pages if p['type'] == 'page'),
                              open_timeout=5, close_timeout=2)
        self.sequence = 0

    def call(self, method: str, params: dict) -> dict:
        self.sequence += 1
        self.socket.send(json.dumps({'id': self.sequence, 'method': method, 'params': params}))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            reply = json.loads(self.socket.recv(timeout=max(.01, deadline - time.monotonic())))
            if reply.get('id') == self.sequence:
                if 'error' in reply:
                    raise RuntimeError(str(reply['error']))
                return reply.get('result', {})
        raise TimeoutError(method)

    def text(self) -> str:
        return self.call('Runtime.evaluate', {'expression': 'document.body.innerText',
                                              'returnByValue': True})['result'].get('value', '')


def check(headed: bool) -> dict:
    if os.getuid() == 0:
        raise RuntimeError('Run acceptance as the non-root image user.')
    sys.path.append('/opt/hermes')
    with tempfile.TemporaryDirectory(prefix='sandbox-accept-') as home:
        os.environ.update(HOME=home, HERMES_REAL_HOME=home, HERMES_HOME=home)
        from hermes_cli.config import atomic_config_write
        atomic_config_write(Path(home) / 'config.yaml', {
            'browser': {'sandbox_required': True}, 'terminal': {'backend': 'local'},
            'bot_desktop': {'placement': 'host'}})
        from tools.bot_desktop import runtime, browser
        from tools.browser_launcher import launch_chromium, stop_browser
        from hermes_cli.browser_runtime import chromium_sandbox_required
        if not chromium_sandbox_required():
            raise RuntimeError('Installed Agent does not honor browser.sandbox_required.')
        launched = None
        client = None
        try:
            if headed:
                runtime.start()
            env = dict(os.environ)
            if headed:
                env.update(runtime.published_env())
            profile = str(browser.profile_dir())
            launched, error = launch_chromium('sandbox-acceptance', executable=browser.executable(),
                                              user_data_dir=profile, ephemeral=False, headed=headed, env=env)
            if launched is None:
                raise RuntimeError(error)
            client = CDP(launched.http_url)
            client.call('Page.navigate', {'url': 'chrome://sandbox'})
            deadline = time.monotonic() + 5
            text = ''
            while time.monotonic() < deadline:
                text = client.text()
                if 'Seccomp-BPF' in text:
                    break
                time.sleep(.05)
            client.call('Page.navigate', {'url': 'data:text/html,<body><script>document.body.textContent=6*7</script>'})
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and client.text() != '42':
                time.sleep(.05)
            javascript = client.text() == '42'
            renderers = renderer_evidence(profile)
            apparmor = Path(f'/proc/{launched.pid}/attr/current').read_text().strip()
            ok = sandbox_proven(text, renderers) and javascript and apparmor == 'docker-default (enforce)'
            return {'ok': ok, 'headed': headed, 'diagnostics': text, 'javascript': javascript,
                    'apparmor': apparmor, 'renderers': renderers}
        finally:
            if client is not None:
                client.socket.close()
            if launched is not None:
                stop_browser('sandbox-acceptance', force=True)
            if headed:
                runtime.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--headed', action='store_true')
    args = parser.parse_args()
    try:
        result = check(args.headed)
    except Exception as exc:
        result = {'ok': False, 'error': str(exc)}
    print(json.dumps(result, sort_keys=True))
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
