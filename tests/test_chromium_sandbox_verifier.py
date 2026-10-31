import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class ChromiumVerifierTests(unittest.TestCase):
    def test_requires_diagnostics_and_renderer_proof(self):
        spec = importlib.util.spec_from_file_location('sandbox_check', ROOT / 'scripts/verify-chromium-sandbox.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        text = 'Layer 1 Sandbox\tNamespace\nPID namespaces\tYes\nNetwork namespaces\tYes\nSeccomp-BPF sandbox\tYes\nSeccomp-BPF sandbox supports TSYNC\tYes\nYou are adequately sandboxed.'
        renderer = {'argv': ['chromium', '--type=renderer'], 'uid': ['1000'] * 4,
                    'nspid': ['10', '2', '1'], 'nnp': '1', 'seccomp': '2', 'filters': '2', 'caps': '0000000000000000'}
        self.assertTrue(module.sandbox_proven(text, [renderer]))
        self.assertFalse(module.sandbox_proven(text, []))
        self.assertFalse(module.sandbox_proven(text.replace('Namespace', 'SUID'), [renderer]))
        for key, value in [('nnp', '0'), ('filters', '1'), ('nspid', ['10']),
                           ('caps', '0000000000000001'), ('argv', ['chromium', '--no-sandbox'])]:
            self.assertFalse(module.sandbox_proven(text, [dict(renderer, **{key: value})]), key)
