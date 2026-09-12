"""Non-regular policy inputs must fail promptly, not stall startup."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

PLUGIN = Path(__file__).resolve().parents[1] / "plugins" / "linear-agent"


class PolicySpecialFileTests(unittest.TestCase):
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
                "    assert 'regular file' in str(exc), str(exc)\n"
                "else:\n"
                "    raise AssertionError('FIFO accepted')\n"
            )
            result = subprocess.run(
                [sys.executable, "-c", code, str(path)], cwd=PLUGIN,
                capture_output=True, text=True, timeout=3,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
