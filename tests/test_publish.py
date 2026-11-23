import json
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.billing_event import BILLING_ARCHIVE
from cdp.billing_ledger import billing_history, verify_archive
from cdp.collect import BILLING_V1, collect
from cdp.publish import publish_pending
from cdp.store import Store


class PublishTest(unittest.TestCase):
    def test_failed_send_preserves_pending_event_and_stable_id(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.db"
            connection = sqlite3.connect(source)
            connection.executescript(Path("samples/billing-v1-demo.sql").read_text())
            connection.close()
            store = Store(root / "dest")
            collect(source, store, "fixture", datetime(2026, 1, 15, tzinfo=timezone.utc), 1,
                    source_format=BILLING_V1)
            attempts = []

            def lost_ack(key, payload):
                attempts.append((key, json.loads(payload)))
                raise TimeoutError("ack lost")

            with self.assertRaises(TimeoutError):
                publish_pending(store, lost_ack, 2)

            delivered = []

            def send(key, payload):
                delivered.append((key, json.loads(payload)))
                return (2, len(delivered) - 1)

            self.assertEqual(publish_pending(store, send, 1), 1)
            self.assertEqual(publish_pending(store, send, 2), 1)
            self.assertEqual(publish_pending(store, send, 2), 0)
            self.assertEqual(delivered[0], attempts[0])
            self.assertEqual(delivered[0][0], delivered[1][0])
            self.assertNotEqual(delivered[0][1]["event_id"], delivered[1][1]["event_id"])
            self.assertEqual([event["status"] for _, event in delivered], ["COMPLETED", "REFUNDED"])
            self.assertNotIn("user_id_hmac", json.dumps(delivered[0][1]))


    def test_corrupt_archive_blocks_publishing_and_verifies_privacy(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.db"
            connection = sqlite3.connect(source)
            connection.executescript(Path("samples/billing-v1-demo.sql").read_text())
            connection.close()
            store = Store(root / "dest")
            collect(source, store, "fixture", datetime(2026, 1, 15, tzinfo=timezone.utc), 1,
                    source_format=BILLING_V1)
            order_id = "ORD_20260115_SH001_0001"
            history = billing_history(store, order_id)
            digest = history[0]["archive_sha256"]
            archive = store.archive_path(digest, BILLING_ARCHIVE)
            original = archive.read_bytes()
            self.assertNotIn(b"user_id_hmac", original)
            self.assertNotIn(b"plate_num_hmac", original)
            self.assertEqual([r["result"] for r in verify_archive(store, order_id)["versions"]], ["verified", "verified"])

            archive.write_bytes(b"{}")
            self.assertEqual(verify_archive(store, order_id)["versions"][0]["result"], "checksum_mismatch")
            with self.assertRaisesRegex(ValueError, "archive checksum mismatch"):
                publish_pending(store, lambda *_: self.fail("corrupt payload was sent"), 10)
            with store.connect(readonly=True) as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM billing_kafka_delivery").fetchone()[0], 0)

            archive.unlink()
            self.assertEqual(verify_archive(store, order_id)["versions"][0]["result"], "archive_missing")
            archive.write_bytes(original)
            self.assertEqual(publish_pending(store, lambda key, payload: (0, 0), 10), 2)


if __name__ == "__main__":
    unittest.main()
