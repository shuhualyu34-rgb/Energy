import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.billing_ledger import ingest_billing
from cdp.collection_quality import record_quality
from cdp.metrics import metrics_text, record_spark_sink
from cdp.quality_audit import quality_audit, rule_catalog
from cdp.quality_rules import QualityRule
from cdp.store import Store


class MetricsTest(unittest.TestCase):
    def test_empty_state_and_monotonic_sink_report(self):
        with TemporaryDirectory() as directory:
            store = Store(Path(directory))
            store.initialize()
            initial = metrics_text(store)
            self.assertIn("cdp_kafka_pending_events 0\n", initial)
            self.assertNotIn("cdp_stream_watermark_timestamp_seconds", initial)
            self.assertNotIn("cdp_spark_sink_last_batch_id", initial)
            record_spark_sink(store, "/dev/private-checkpoint-path", 3, {"late": 2, "duplicate": 1})
            record_spark_sink(store, "/dev/private-checkpoint-path", 2, {"applied": 99})
            result = metrics_text(store)
            self.assertIn("cdp_spark_sink_last_batch_id 3\n", result)
            self.assertIn('cdp_spark_sink_last_batch_events{outcome="late"} 2\n', result)
            self.assertIn('cdp_spark_sink_last_batch_events{outcome="applied"} 0\n', result)
            self.assertNotIn("private-checkpoint-path", result)

    def test_quality_catalog_and_live_audit_report_actionable_results(self):
        rules = {item["rule_id"]: item for item in rule_catalog()}
        self.assertEqual(len(rules), len(rule_catalog()))
        self.assertEqual({key for key, row in rules.items() if row["severity"] == "blocking"}, set(QualityRule))
        self.assertTrue(all(row["owner_role"] and row["action"] for row in rules.values()))
        with TemporaryDirectory() as directory:
            store = Store(Path(directory))
            store.initialize()
            self.assertTrue(all(row["status"] == "clear" for row in quality_audit(store)["results"]))
            record_quality(store, "legacy-source", "contract_validation")
            stamp = datetime(2026, 1, 15, tzinfo=timezone.utc).isoformat(timespec="microseconds")
            orders = []
            for number in (1, 2):
                order = {
                    "order_id": f"ORD_20260115_SH001_{number:04d}", "station_id": "ST_SH_001",
                    "gun_id": "GUN_SH001_01A", "user_id_hmac": "a" * 64, "status": "COMPLETED", "version": 1,
                    "start_time": "2026-01-14T23:50:00Z", "end_time": stamp, "updated_at": stamp,
                    "total_kwh": 10, "elec_fee_cents": 800, "service_fee_cents": 300, "total_amount_cents": 1100,
                }
                self.assertEqual(ingest_billing(store, json.dumps(order).encode()), "inserted")
                orders.append(order["order_id"])
            with store.connect() as db:
                db.execute("INSERT INTO billing_kafka_delivery VALUES (?, 1, ?, -1, 0)", (orders[0], stamp))
            results = {row["rule_id"]: row for row in quality_audit(store)["results"]}
            self.assertEqual(set(results), set(rules) - set(QualityRule))
            for rule, status in (("kafka_delivery_coordinates", "warning"),
                                 ("blocked_source_without_rule", "observation"),
                                 ("accepted_versions_pending_kafka", "observation")):
                self.assertEqual((results[rule]["count"], results[rule]["status"]), (1, status))
            self.assertIn("cdp_kafka_pending_events 1\n", metrics_text(store))


if __name__ == "__main__":
    unittest.main()
