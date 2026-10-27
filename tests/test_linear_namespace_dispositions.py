"""Real descriptor/file checks with explicitly synthetic root-custody metadata."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

from linear_fake_api import load_plugin
load_plugin()
from hermes_fleet_linear_plugin import cleanup


def receipt_fixture(home, epoch, *, ingress=None):
    ingress = ingress or home / "protected" / "default" / "ingress.db"
    directory = ingress.parent / cleanup.NAMESPACE_DISPOSITION_DIRECTORY
    directory.mkdir(parents=True, exist_ok=True)
    value = {"version": 1, "kind": "pid_namespace_disposed", "deployment_profile": ingress.parent.name,
        "native_profile": "default", "home": str(home), "home_dev": home.stat().st_dev,
        "home_ino": home.stat().st_ino, "epoch": epoch, "container_id": "a" * 64,
        "host_pid1": 23456, "pid1_start_ticks": int(epoch.rpartition(":")[2]),
        "pid_namespace_inode": 34567, "host_pid_namespace_inode": 45678, "pid_mode": "private",
        "old_instance_disposed": True, "pid1_gone": True}
    leaf = directory / (hashlib.sha256(epoch.encode()).hexdigest() + ".json")
    leaf.write_text(json.dumps(value))
    leaf.chmod(0o440)
    return ingress, leaf, value


@contextmanager
def root_metadata(change=None):
    """UID1000 CI cannot chown root: simulate custody, retaining actual FD I/O.

    This is a source fixture, never a host disposal observation or live proof.
    """
    actual = os.fstat
    def fixture(fd):
        info = actual(fd)
        fields = list(info)
        fields[4], fields[5] = 0, 1000
        if stat.S_ISDIR(info.st_mode):
            fields[0] = stat.S_IFDIR | 0o750
        if change:
            fields = change(info, fields)
        return os.stat_result(fields)
    with patch.object(os, "fstat", side_effect=fixture):
        yield


class NamespaceDispositionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.epoch = "00000000-0000-0000-0000-000000000001:12345"
        self.ingress, self.leaf, self.value = receipt_fixture(self.home, self.epoch)

    def read(self):
        return cleanup._namespace_disposition(self.ingress, self.home, "default", "default", self.epoch)

    def write(self, value):
        self.leaf.chmod(0o640)
        self.leaf.write_text(json.dumps(value))
        self.leaf.chmod(0o440)

    def test_descriptor_read_exact_schema_and_volume_scope(self):
        with root_metadata():
            observed = self.read()
        self.assertEqual(observed, {"epoch": self.epoch,
            "sha256": hashlib.sha256(self.leaf.read_bytes()).hexdigest()})
        self.assertEqual(set(self.value), cleanup.NAMESPACE_DISPOSITION_FIELDS)

    def test_untrusted_or_writable_transport_is_never_authority(self):
        with self.assertRaises(cleanup.Unknown):
            self.read()  # Real UID1000-created ancestry cannot publish a host fact.
        for field, value in ((4, 1000), (5, 0), (3, 2), (0, stat.S_IFREG | 0o660)):
            def change(info, fields):
                if stat.S_ISREG(info.st_mode):
                    fields[field] = value
                return fields
            with self.subTest(field=field, value=value), root_metadata(change), self.assertRaises(cleanup.Unknown):
                self.read()
        def writable_parent(info, fields):
            if stat.S_ISDIR(info.st_mode):
                fields[0] = stat.S_IFDIR | 0o770
            return fields
        with root_metadata(writable_parent), self.assertRaises(cleanup.Unknown):
            self.read()

    def test_unknown_schema_scope_and_disposal_facts_hold(self):
        cases = {"version": True, "kind": "unknown", "deployment_profile": "foreign", "native_profile": "foreign",
            "home": str(self.home / "foreign"), "home_dev": self.value["home_dev"] + 1,
            "home_ino": self.value["home_ino"] + 1, "epoch": "foreign:12345", "container_id": "not-an-id",
            "host_pid1": True, "pid1_start_ticks": 0, "pid_namespace_inode": self.value["host_pid_namespace_inode"],
            "host_pid_namespace_inode": False, "pid_mode": "host", "old_instance_disposed": False, "pid1_gone": 1}
        for field, bad in cases.items():
            with self.subTest(field=field):
                self.write({**self.value, field: bad})
                with root_metadata(), self.assertRaises((cleanup.Unknown, ValueError)):
                    self.read()
        for value in ({**self.value, "extra": True}, {k: v for k, v in self.value.items() if k != "epoch"}):
            self.write(value)
            with root_metadata(), self.assertRaises(cleanup.Unknown):
                self.read()

    def test_missing_symlink_hardlink_and_oversize_receipts_hold(self):
        raw = self.leaf.read_bytes()
        self.leaf.unlink()
        with root_metadata(), self.assertRaises(cleanup.Unknown):
            self.read()
        target = self.home / "untrusted.json"
        target.write_bytes(raw)
        self.leaf.symlink_to(target)
        with root_metadata(), self.assertRaises(cleanup.Unknown):
            self.read()
        self.leaf.unlink()
        os.link(target, self.leaf)
        with root_metadata(), self.assertRaises(cleanup.Unknown):
            self.read()
        self.leaf.unlink()
        self.leaf.write_bytes(b" " * (cleanup.NAMESPACE_DISPOSITION_MAX_BYTES + 1))
        self.leaf.chmod(0o440)
        with root_metadata(), self.assertRaises(cleanup.Unknown):
            self.read()

    def test_symlink_directory_and_duplicate_json_keys_hold(self):
        self.leaf.chmod(0o640)
        self.leaf.write_text(json.dumps(self.value)[:-1] + ',"pid1_gone":true}')
        self.leaf.chmod(0o440)
        with root_metadata(), self.assertRaises(cleanup.Unknown):
            self.read()
        directory = self.leaf.parent
        moved = directory.with_name("moved")
        directory.rename(moved)
        directory.symlink_to(moved, target_is_directory=True)
        with root_metadata(), self.assertRaises(cleanup.Unknown):
            self.read()
