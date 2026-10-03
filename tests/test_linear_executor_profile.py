"""Executor names must resolve to the same home as the Linear service."""
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch
from linear_fake_api import load_plugin

plugin = load_plugin()
from hermes_fleet_linear_plugin.bridge import Kanban


class ExecutorProfileTests(unittest.TestCase):
    def resolve(self, profile, home, homes, existing):
        profiles = types.ModuleType('hermes_cli.profiles')
        profiles.get_profile_dir = homes.__getitem__
        profiles.profile_exists = existing.__contains__
        with patch.dict(sys.modules, {'hermes_cli.profiles': profiles}):
            return Kanban.resolve_executor_profile(profile, home)

    def test_standalone_container_uses_default_executor_for_same_home(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            self.assertEqual(self.resolve('sample', home, {'default': home}, {'default'}), 'default')

    def test_named_profile_keeps_its_executor(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            named = home / 'profiles' / 'sample'
            named.mkdir(parents=True)
            self.assertEqual(self.resolve('sample', named, {'sample': named, 'default': home},
                                          {'sample', 'default'}), 'sample')

    def test_foreign_home_cannot_fall_back_to_default(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            other = home / 'other'
            other.mkdir()
            with self.assertRaises(ValueError):
                self.resolve('sample', other, {'default': home}, {'default'})

    def test_create_records_resolved_executor(self):
        from contextlib import nullcontext
        from unittest.mock import Mock
        adapter = object.__new__(Kanban)
        adapter.executor_profile = 'default'
        adapter.board = 'sample-board'
        adapter.kb = Mock()
        adapter.conn = lambda: nullcontext('connection')
        adapter.create(title='sample', assignee='sample')
        adapter.kb.create_task.assert_called_once_with('connection', board='sample-board', title='sample', assignee='default')
