import json
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.billing_event import event_id
from cdp.recovery_check import compare_windows
from cdp.store import Store
from cdp.stream import apply_late_events, observe

DAY = date(2026, 1, 14)
ORDER = "ORD_20260115_SH001_0001"
UPDATED = "2026-01-14T19:00:00.000000+00:00"


def record(store, version, ended="2026-01-14T16:30:00Z"):
    event = {
        "schema_version": "1.0", "event_id": event_id(ORDER, version, UPDATED),
        "order_id": ORDER, "version": version, "updated_at": UPDATED, "status": "COMPLETED",
        "order": {"station_id": "ST_SH_001", "end_time": ended},
    }
    observe(store, ORDER.encode(), json.dumps(event).encode())


class RecoveryCheckTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.source = Store(Path(directory.name) / "source")
        self.target = Store(Path(directory.name) / "target")
        self.source.initialize()
        self.target.initialize()

    def compare(self):
        return compare_windows(self.source, self.target, DAY, DAY)

    def test_equal_window_counts_do_not_hide_different_event_identity(self):
        record(self.source, 1)
        record(self.target, 2)
        report = self.compare()
        self.assertEqual(report["result"], "mismatch")
        self.assertEqual(report["window_differences"], 0)
        self.assertFalse(report["days"][0]["event_fingerprint_match"])

    def test_pending_late_blocks_acceptance_until_backfill(self):
        for store in (self.source, self.target):
            record(store, 1, "2026-01-14T19:00:00Z")
            record(store, 2)
        report = self.compare()
        self.assertEqual(report["result"], "mismatch")
        self.assertEqual(report["days"][0]["target_pending_late"], 1)
        self.assertTrue(report["days"][0]["event_fingerprint_match"])
        apply_late_events(self.source, DAY)
        report = self.compare()
        self.assertEqual(report["window_examples"][0]["target_count"], 0)
        apply_late_events(self.target, DAY)
        self.assertEqual(self.compare()["result"], "match")

    def test_stream_state_distinguishes_quarantine_from_visible_windows(self):
        for store in (self.source, self.target):
            record(store, 1)
        record(self.source, 2, "2099-01-14T16:30:00Z")
        self.assertEqual(self.compare()["result"], "match")
        report = compare_windows(self.source, self.target, DAY, DAY, with_stream_state=True)
        self.assertEqual(report["result"], "mismatch")
        self.assertEqual(report["window_differences"], 0)
        self.assertTrue(report["stream_state"]["watermark_match"])
        self.assertFalse(report["stream_state"]["quarantine_match"])
        self.assertEqual(report["stream_state"]["source"]["quarantined_events"], 1)
        record(self.target, 2, "2099-01-14T16:30:00Z")
        self.assertEqual(compare_windows(self.source, self.target, DAY, DAY, with_stream_state=True)["result"], "match")

    def test_watermark_comparison_normalizes_precision_and_flags_a_real_gap(self):
        for store in (self.source, self.target):
            record(store, 1)
        with self.target.connect() as connection:
            connection.execute("UPDATE billing_stream_progress SET max_event_time_utc = ?",
                               ("2026-01-14T16:30:00+00:00",))
        report = compare_windows(self.source, self.target, DAY, DAY, with_stream_state=True)
        self.assertTrue(report["stream_state"]["watermark_match"])
        with self.target.connect() as connection:
            connection.execute("UPDATE billing_stream_progress SET max_event_time_utc = ?",
                               ("2026-01-14T16:30:00.000001+00:00",))
        report = compare_windows(self.source, self.target, DAY, DAY, with_stream_state=True)
        self.assertEqual(report["result"], "mismatch")
        self.assertFalse(report["stream_state"]["watermark_match"])

    def test_empty_scope_and_reversed_range(self):
        self.assertEqual(self.compare()["result"], "no_data")
        with self.assertRaisesRegex(ValueError, "through-day"):
            compare_windows(self.source, self.target, DAY, date(2026, 1, 13))


if __name__ == "__main__":
    unittest.main()
