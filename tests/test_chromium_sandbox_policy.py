"""Chromium policy changes only observed namespace operations from Docker's effective default."""
import hashlib
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class ChromiumSandboxPolicyTests(unittest.TestCase):
    def test_only_exact_observed_namespace_operations_are_added(self):
        policy = json.loads((ROOT / 'security/chromium-seccomp.json').read_text())
        provenance = json.loads((ROOT / 'security/chromium-seccomp-provenance.json').read_text())
        original = dict(policy, syscalls=policy['syscalls'][:-4])
        digest = hashlib.sha256(json.dumps(original, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.assertEqual(digest, provenance['docker_effective_default_sha256'])
        expected = [('clone', 0x10000011), ('unshare', 0x10000000),
                    ('clone', 0x70000011), ('clone', 0x20000011)]
        self.assertEqual(policy['syscalls'][-4:], [
            {'names': [name], 'action': 'SCMP_ACT_ALLOW',
             'args': [{'index': 0, 'value': flags, 'op': 'SCMP_CMP_EQ'}]}
            for name, flags in expected])
        self.assertEqual(policy['defaultAction'], 'SCMP_ACT_ERRNO')
        self.assertEqual(policy['defaultErrnoRet'], 1)
        for forbidden in ('mount', 'setns', 'bpf', 'keyctl'):
            self.assertFalse(any(forbidden in rule['names'] and rule['action'] == 'SCMP_ACT_ALLOW'
                                 for rule in policy['syscalls']), forbidden)
        self.assertEqual([r for r in policy['syscalls'] if 'clone3' in r['names']],
                         [{'names': ['clone3'], 'action': 'SCMP_ACT_ERRNO', 'errnoRet': 38}])


if __name__ == '__main__':
    unittest.main()
