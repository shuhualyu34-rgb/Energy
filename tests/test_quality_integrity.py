import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.quality_audit import quality_audit, rule_catalog
from cdp.quality_integrity import INTEGRITY_CHECKS
from cdp.store import Store


class QualityIntegrityTest(unittest.TestCase):
    def test_each_persisted_integrity_check_detects_its_failure(self):
        with TemporaryDirectory() as directory:
            store = Store(Path(directory))
            store.initialize()
            self.assertEqual(len(rule_catalog()), 40)
            integrity_ids = {check.rule.rule_id for check in INTEGRITY_CHECKS}
            self.assertEqual(len(integrity_ids), 20)

            def results():
                return {row["rule_id"]: row["count"] for row in quality_audit(store)["results"]
                        if row["rule_id"] in integrity_ids}

            self.assertEqual(results(), dict.fromkeys(integrity_ids, 0))
            stamp = "2026-01-15T12:00:00.000000+00:00"
            earlier = "2026-01-15T11:55:00.000000+00:00"
            with store.connect() as db:
                db.executemany("INSERT INTO dwd_billing_order_v1 VALUES (?, 1, ?, 'COMPLETED', '{}')",
                               [("missing-history", stamp), ("mismatch", stamp)])
                db.executemany("""
                    INSERT INTO dwd_billing_order_v1_history
                    VALUES (?, ?, ?, 'COMPLETED', ?, NULL)
                """, [("mismatch", 1, stamp, '{"changed": true}'),
                      ("mismatch", 2, stamp, '{}'), ("missing-current", 1, stamp, '{}')])
                db.execute("INSERT INTO billing_order_archive VALUES ('orphan', 1, ?, 'invalid')", (stamp,))
                db.execute("INSERT INTO billing_feature_availability VALUES ('orphan', 1, ?, ?)", (stamp, earlier))
                db.executemany("INSERT INTO billing_kafka_delivery VALUES (?, 1, ?, 0, 0)",
                               [("orphan", stamp), ("mismatch", stamp)])
                db.execute("INSERT INTO billing_stream_event VALUES ('event', 'hash', 'station', 'COMPLETED', ?)",
                           (stamp,))
                db.execute("INSERT INTO billing_stream_quarantine VALUES ('event', 'hash', ?, 'future')", (stamp,))
                db.execute("INSERT INTO billing_stream_late VALUES ('orphan', ?, ?, FALSE)", (stamp, earlier))
                db.execute("INSERT INTO billing_spark_sink_progress VALUES ('checkpoint', -1, ?, '{}')", (stamp,))
                db.execute("INSERT INTO billing_collection_quality VALUES ('source', 'blocked', ?, NULL)", (stamp,))
                db.execute("""
                    INSERT INTO billing_collection_quality_history
                    VALUES ('check', ?, 'source', 'ok', NULL, NULL)
                """, (stamp,))
                db.execute("INSERT INTO billing_collection_rejection VALUES ('check', 'order', 'record', 1)")

            expected = dict.fromkeys(integrity_ids, 1)
            expected.update(
                billing_history_without_archive=3,
                billing_history_without_feature_time=3,
                stream_window_ahead_of_progress=0,
            )
            self.assertEqual(results(), expected)

            with store.connect() as db:
                db.execute("INSERT INTO billing_stream_progress VALUES ('billing-orders', ?)", (earlier,))
            expected.update(stream_events_without_progress=0, stream_window_ahead_of_progress=1)
            self.assertEqual(results(), expected)


if __name__ == "__main__":
    unittest.main()
