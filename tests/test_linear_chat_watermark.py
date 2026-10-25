"""New native reporting uses the remote outcome watermark."""
from test_linear_task_tracking import TaskTrackingTests
from hermes_fleet_linear_plugin.bridge import iso_ms

class TrackingWatermarkTests(TaskTrackingTests):
    def test_binding_uses_remote_watermark(self):
        issue=self.remote.issues['issue-1']
        self.assertTrue(self.invoke('track',task_id='task-1')['ok'])
        self.assertEqual(self.bridge.store.get('issue-1')['last_updated_at'],iso_ms(issue['updatedAt']))

    def test_missing_remote_watermark_refuses_only_reporting(self):
        self.remote.issues['issue-1']['updatedAt']=None
        self.assertFalse(self.invoke('track',task_id='task-1')['ok'])
        self.assertIsNone(self.bridge.store.get('issue-1'))
        self.assertEqual(self.board.task.status,'running')
