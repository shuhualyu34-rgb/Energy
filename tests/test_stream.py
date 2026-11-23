import json
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.billing import BillingContractError
from cdp.billing_event import event_id
from cdp.store import Store
from cdp.stream import Observation, apply_late_events, billing_windows, observe

ORDER_ID = "ORD_20260115_SH001_0001"
UPDATED = "2026-01-14T16:31:00.000000+00:00"


def envelope(version=1, status="COMPLETED", end="2026-01-14T16:34:59.999999+00:00"):
    return json.dumps({
        "schema_version": "1.0", "event_id": event_id(ORDER_ID, version, UPDATED),
        "order_id": ORDER_ID, "version": version, "updated_at": UPDATED, "status": status,
        "order": {"station_id": "ST_SH_001", "end_time": end},
    }).encode()


class StreamTest(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Store(Path(temporary.name))
        self.store.initialize()

    def test_replay_and_five_minute_window_boundary(self):
        key = ORDER_ID.encode()
        self.assertEqual(observe(self.store, key, envelope()), Observation.APPLIED)
        self.assertEqual(observe(Store(self.store.directory), key, envelope()), Observation.DUPLICATE)
        self.assertEqual(observe(self.store, key, envelope(2, "REFUNDED", "2026-01-14T16:35:00Z")), Observation.APPLIED)
        rows = billing_windows(self.store, date(2026, 1, 14))
        self.assertEqual([(r["window_start_utc"], r["status"], r["event_count"]) for r in rows], [
            ("2026-01-14T16:30:00+00:00", "COMPLETED", 1),
            ("2026-01-14T16:35:00+00:00", "REFUNDED", 1),
        ])
        self.assertEqual(billing_windows(self.store, date(2026, 1, 15)), [])

    def test_identity_collision_bad_key_and_future_time(self):
        self.assertEqual(observe(self.store, ORDER_ID.encode(), envelope()), Observation.APPLIED)
        with self.assertRaisesRegex(BillingContractError, "conflicting payload"):
            observe(self.store, ORDER_ID.encode(), envelope(status="REFUNDED"))
        with self.assertRaisesRegex(BillingContractError, "identity"):
            observe(self.store, b"other", envelope())
        tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
        future = envelope(2, end=tomorrow.isoformat())
        self.assertEqual(observe(self.store, ORDER_ID.encode(), future), Observation.QUARANTINED)
        self.assertEqual(observe(self.store, ORDER_ID.encode(), future), Observation.DUPLICATE)
        self.assertEqual(observe(self.store, ORDER_ID.encode(), envelope(3)), Observation.APPLIED)

    def test_two_hour_boundary_and_idempotent_backfill(self):
        key = ORDER_ID.encode()
        self.assertEqual(observe(self.store, key, envelope(1, end="2026-01-14T18:35:00Z")), Observation.APPLIED)
        restarted = Store(self.store.directory)
        self.assertEqual(observe(restarted, key, envelope(2, end="2026-01-14T16:35:00Z")), Observation.APPLIED)
        late = envelope(3, end="2026-01-14T16:34:59.999999Z")
        self.assertEqual(observe(restarted, key, late), Observation.LATE)
        self.assertEqual(observe(restarted, key, late), Observation.DUPLICATE)
        day = date(2026, 1, 14)
        self.assertEqual(len(billing_windows(restarted, day)), 2)
        self.assertEqual(apply_late_events(restarted, date(2026, 1, 15)), 0)
        self.assertEqual(apply_late_events(restarted, day), 1)
        self.assertEqual(apply_late_events(restarted, day), 0)
        self.assertEqual(len(billing_windows(restarted, day)), 3)
        self.assertEqual(observe(restarted, key, late), Observation.DUPLICATE)


if __name__ == "__main__":
    unittest.main()
