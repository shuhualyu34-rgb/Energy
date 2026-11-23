import hashlib
import json
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import ValidationError

from cdp.store import Store, VersionConflict

SAMPLE = Path("samples/order.json").read_bytes()
DAY = date(2026, 1, 15)


def change(**fields) -> bytes:
    return json.dumps(json.loads(SAMPLE) | fields).encode()


class OrdersTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = Store(Path(directory.name))
        self.store.initialize()

    def test_replay_and_shanghai_midnight(self):
        self.assertEqual(self.store.ingest(SAMPLE), "inserted")
        for _ in range(3):
            self.assertEqual(self.store.ingest(SAMPLE), "duplicate")
        self.assertEqual(self.store.daily(date(2026, 1, 14)), [])
        self.assertEqual(self.store.daily(DAY), [{
            "business_day": "2026-01-15",
            "station_id": "demo-station-001",
            "order_count": 1,
            "energy_wh": 10000,
            "electricity_fee_cents": 800,
            "service_fee_cents": 300,
        }])
        archived = list(self.store.directory.glob("ods/orders/*.json"))
        self.assertEqual(len(archived), 1)
        self.assertEqual(archived[0].read_bytes(), SAMPLE)
        with self.store.connect(readonly=True) as connection:
            row = connection.execute("SELECT raw_hash FROM dwd_order").fetchone()
            self.assertEqual(row["raw_hash"], hashlib.sha256(SAMPLE).hexdigest())
            self.assertEqual(row["raw_hash"], archived[0].stem)
        replay = Store(self.store.directory / "replay")
        self.assertEqual(replay.ingest(archived[0].read_bytes()), "inserted")
        self.assertEqual(replay.daily(DAY), self.store.daily(DAY))

    def test_correction_stale_replay_and_version_conflict(self):
        self.store.ingest(SAMPLE)
        correction = change(version=2, service_fee_cents=400)
        self.assertEqual(self.store.ingest(correction), "updated")
        self.assertEqual(self.store.ingest(SAMPLE), "stale")
        with self.assertRaises(VersionConflict):
            self.store.ingest(change(version=2, service_fee_cents=500))
        self.assertEqual(self.store.daily(DAY)[0]["service_fee_cents"], 400)

    def test_contract_rejects_invalid_input_before_archiving(self):
        future = datetime.now(timezone.utc) + timedelta(days=1)
        for fields in [
            {"phone": "synthetic-sensitive-field"},
            {"energy_wh": -1},
            {"version": True},
            {"station_id": ""},
            {"ended_at": "2026-01-14T23:00:00+08:00"},
            {"ended_at": "2026-01-15T00:30:00"},
            {"ended_at": future.isoformat()},
        ]:
            with self.subTest(fields=fields), self.assertRaises(ValidationError):
                self.store.ingest(change(**fields))
        self.assertEqual(self.store.daily(DAY), [])
        self.assertFalse(list(self.store.directory.glob("ods/orders/*.json")))


if __name__ == "__main__":
    unittest.main()
