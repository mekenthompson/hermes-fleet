"""Non-regular policy inputs must fail promptly, not stall startup."""
import importlib.util
import os
import stat
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

PLUGIN = Path(__file__).resolve().parents[1] / "plugins" / "linear-agent"


class PolicyDescriptorTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("policy_descriptor_test", PLUGIN / "linear_policy.py")
        assert spec is not None and spec.loader is not None
        self.policy = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.policy)

    def test_rejected_leaf_does_not_leak_descriptor(self):
        root = SimpleNamespace(st_uid=0, st_mode=stat.S_IFDIR | 0o755)
        untrusted = SimpleNamespace(st_uid=1000, st_mode=stat.S_IFREG | 0o444)
        with patch.object(self.policy.os, "geteuid", return_value=1000), \
             patch.object(self.policy.os, "open", side_effect=[10, 11]), \
             patch.object(self.policy.os, "fstat", side_effect=[root, untrusted]), \
             patch.object(self.policy.os, "close") as close:
            with self.assertRaises(self.policy.ImmutablePolicyError):
                self.policy.read_immutable_json(Path("/policy.json"), description="test")
            self.assertCountEqual([call.args[0] for call in close.call_args_list], [10, 11])

    def test_leaf_stat_error_does_not_leak_descriptor(self):
        root = SimpleNamespace(st_uid=0, st_mode=stat.S_IFDIR | 0o755)
        with patch.object(self.policy.os, "geteuid", return_value=1000), \
             patch.object(self.policy.os, "open", side_effect=[10, 11]), \
             patch.object(self.policy.os, "fstat", side_effect=[root, OSError("stat failed")]), \
             patch.object(self.policy.os, "close") as close:
            with self.assertRaises(self.policy.ImmutablePolicyError):
                self.policy.read_immutable_json(Path("/policy.json"), description="test")
            self.assertCountEqual([call.args[0] for call in close.call_args_list], [10, 11])


class PolicySpecialFileTests(unittest.TestCase):
    def test_runtime_owned_read_only_policy_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "policy.json"
            path.write_text('{"agents": []}\n', encoding="utf-8")
            path.chmod(0o444)
            code = (
                "from pathlib import Path\n"
                "from linear_policy import read_immutable_json, ImmutablePolicyError\n"
                "import sys\n"
                "try:\n"
                "    read_immutable_json(Path(sys.argv[1]), description='test policy')\n"
                "except ImmutablePolicyError as exc:\n"
                "    assert str(exc), exc\n"
                "else:\n"
                "    raise AssertionError('runtime-owned policy accepted')\n"
            )
            result = subprocess.run(
                [sys.executable, "-c", code, str(path)], cwd=PLUGIN,
                capture_output=True, text=True, timeout=3,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    @unittest.skipUnless(hasattr(os, "mkfifo"), "requires POSIX FIFOs")
    def test_fifo_is_rejected_without_waiting_for_a_writer(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "policy.json"
            os.mkfifo(path, 0o444)
            code = (
                "from pathlib import Path\n"
                "from linear_policy import read_immutable_json, ImmutablePolicyError\n"
                "import sys\n"
                "try:\n"
                "    read_immutable_json(Path(sys.argv[1]), description='test policy')\n"
                "except ImmutablePolicyError as exc:\n"
                "    assert str(exc), exc\n"
                "else:\n"
                "    raise AssertionError('FIFO accepted')\n"
            )
            result = subprocess.run(
                [sys.executable, "-c", code, str(path)], cwd=PLUGIN,
                capture_output=True, text=True, timeout=3,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
