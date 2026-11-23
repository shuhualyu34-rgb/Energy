import json
import sqlite3
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.collect import BILLING_V1, collect
from cdp.kafka_recovery import recovery_topic, replay_archives
from cdp.store import Store


class KafkaRecoveryTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        source = root / "source.sqlite3"
        with closing(sqlite3.connect(source)) as db, db:
            db.executescript(Path("samples/billing-v1-demo.sql").read_text())
        self.store = Store(root / "destination")
        collect(source, self.store, "restore", datetime(2026, 1, 15, tzinfo=timezone.utc),
                1, source_format=BILLING_V1)

    def test_recovery_progress_is_scoped_and_normal_markers_are_untouched(self):
        events = []

        def send(key, value):
            events.append((key, json.loads(value)))
            return 0, len(events) - 1

        first = replay_archives(self.store, "broker-1", "restore1", send, 1)
        self.assertEqual((first["replayed"], first["remaining"]), (1, 1))
        second = replay_archives(self.store, "broker-1", "restore1", send, 5)
        self.assertEqual((second["replayed"], second["remaining"]), (1, 0))
        self.assertEqual(replay_archives(self.store, "broker-1", "restore1", send, 5)["replayed"], 0)
        self.assertEqual(replay_archives(self.store, "broker-2", "restore1", send, 5)["replayed"], 2)
        self.assertEqual(events[:2], events[2:])
        with self.store.connect(readonly=True) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM billing_kafka_delivery").fetchone()[0], 0)

    def test_unarchived_versions_and_failed_delivery_are_not_acknowledged(self):
        with self.store.connect() as db:
            db.execute("DELETE FROM billing_order_archive WHERE version = 2")

        def fail(*_):
            raise TimeoutError("acknowledgement was lost")

        with self.assertRaises(TimeoutError):
            replay_archives(self.store, "broker-1", "restore", fail, 1)
        result = replay_archives(self.store, "broker-1", "restore", lambda *_: (0, 0), 5)
        self.assertEqual((result["replayed"], result["remaining"], result["legacy_without_archive"]), (1, 0, 1))
        with self.assertRaises(ValueError):
            recovery_topic("../billing.orders.v1")


if __name__ == "__main__":
    unittest.main()
