import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.billing_event import event_id
from cdp.store import Store
from cdp.stream import observe
from cdp.stream_snapshot import export_stream_state, restore_stream_state

ORDER = "ORD_20260115_SH001_0001"
UPDATED = "2026-01-14T16:30:00.000000+00:00"


def write(store, version, ended):
    event = {
        "schema_version": "1.0", "event_id": event_id(ORDER, version, UPDATED),
        "order_id": ORDER, "version": version, "updated_at": UPDATED,
        "status": "COMPLETED", "order": {"station_id": "ST_SH_001", "end_time": ended},
    }
    observe(store, ORDER.encode(), json.dumps(event).encode())


class StreamSnapshotTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.source = Store(Path(directory.name) / "source")
        self.target = Store(Path(directory.name) / "target")
        for store in (self.source, self.target):
            store.initialize()
            write(store, 1, "2026-01-14T16:30:00Z")
        write(self.source, 2, "2099-01-15T16:30:00Z")

    def test_restores_quarantine_and_frontier_idempotently(self):
        with self.target.connect() as db:
            db.execute("UPDATE billing_stream_progress SET max_event_time_utc = '2026-01-14T16:29:00+00:00'")
        bundle = export_stream_state(self.source)
        raw = bundle.model_dump_json().encode()
        self.assertEqual(restore_stream_state(self.target, raw),
                         {"quarantine_restored": 1, "watermark_restored": True})
        self.assertEqual(export_stream_state(self.target), bundle)
        self.assertEqual(restore_stream_state(self.target, raw),
                         {"quarantine_restored": 0, "watermark_restored": False})

    def test_conflicting_target_quarantine_is_not_overwritten(self):
        write(self.target, 2, "2099-01-15T16:35:00Z")
        before = export_stream_state(self.target)
        with self.assertRaisesRegex(ValueError, "target quarantine differs"):
            restore_stream_state(self.target, export_stream_state(self.source).model_dump_json().encode())
        self.assertEqual(export_stream_state(self.target), before)

    def test_changed_event_set_and_corrupt_bundle_are_rejected(self):
        bundle = export_stream_state(self.source)
        tampered = bundle.model_dump()
        tampered["snapshot"]["quarantine"] = []
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            restore_stream_state(self.target, json.dumps(tampered).encode())
        write(self.target, 3, "2026-01-14T16:35:00Z")
        before = export_stream_state(self.target)
        with self.assertRaisesRegex(ValueError, "target stream events"):
            restore_stream_state(self.target, bundle.model_dump_json().encode())
        self.assertEqual(export_stream_state(self.target), before)


if __name__ == "__main__":
    unittest.main()
