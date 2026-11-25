import sqlite3
import unittest
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from cdp.billing_ledger import billing_history
from cdp.collect import BILLING_V1, backfill, collect
from cdp.collection_quality import (
    RejectedRecord, collection_quality, daily_quality, quality_history, record_quality,
)
from cdp.metrics import metrics_text
from cdp.quality_rules import QualityRule
from cdp.store import Store

DAY = date(2026, 1, 15)


class CollectorTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        self.source = root / "billing.sqlite3"
        self.store = Store(root / "destination")
        self.execute(Path("samples/billing-demo.sql").read_text(), script=True)

    def execute(self, sql, parameters=(), *, script=False):
        with closing(sqlite3.connect(self.source)) as connection, connection:
            if script:
                connection.executescript(sql)
            else:
                connection.execute(sql, parameters)

    def add_order(self, order_id, minute, *, energy=500):
        self.execute("""
            INSERT INTO billing_order_snapshot
            SELECT ?, ?, station_id, 1, started_at, ended_at, ?, 100, 50
            FROM billing_order_snapshot WHERE order_id = 'demo-order-001'
        """, (order_id, f"2026-01-14T16:{minute:02d}:00.000000+00:00", energy))

    def run_collection(self, hour, minute):
        return collect(
            self.source, self.store, "billing-demo",
            datetime(2026, 1, 14, hour, minute, tzinfo=timezone.utc), page_size=1,
        )["outcomes"]

    def test_keyset_paging_lookback_and_versions(self):
        self.add_order("same-timestamp", 31)
        self.add_order("past-cutoff", 51)
        self.assertEqual(self.run_collection(16, 50), {"inserted": 2})
        self.add_order("late-visible", 40)
        self.execute("""
            UPDATE billing_order_snapshot
            SET version = 2, service_fee_cents = 400, updated_at = ?
            WHERE order_id = 'demo-order-001'
        """, ("2026-01-14T16:52:00.000000+00:00",))
        self.assertEqual(self.run_collection(16, 55), {"inserted": 2, "updated": 1})
        self.assertEqual(self.run_collection(16, 55), {"duplicate": 3})
        self.assertEqual(self.store.daily(DAY)[0]["order_count"], 4)
        self.assertEqual(self.store.daily(DAY)[0]["service_fee_cents"], 550)

    def test_failed_page_retries_without_advancing_cutoff(self):
        self.assertEqual(self.run_collection(16, 40), {"inserted": 1})
        self.add_order("good-first", 44)
        self.add_order("invalid-next", 45, energy=-1)
        with self.assertRaisesRegex(ValueError, "cutoff unchanged"):
            self.run_collection(17, 15)
        self.assertEqual(self.store.daily(DAY)[0]["order_count"], 2)
        self.execute("UPDATE billing_order_snapshot SET energy_wh = 500 WHERE order_id = ?", ("invalid-next",))
        self.assertEqual(self.run_collection(17, 16), {"duplicate": 2, "inserted": 1})
        self.assertEqual(self.store.daily(DAY)[0]["order_count"], 3)

    def test_backfill_recovers_older_commit_without_moving_checkpoint(self):
        self.assertEqual(self.run_collection(17, 0), {"inserted": 1})
        self.add_order("very-late", 33)
        self.assertEqual(self.run_collection(17, 5), {})
        result = backfill(
            self.source, self.store,
            datetime(2026, 1, 14, 16, 33, tzinfo=timezone.utc),
            datetime(2026, 1, 14, 16, 33, tzinfo=timezone.utc), page_size=1,
        )
        self.assertEqual(result["outcomes"], {"inserted": 1})
        self.assertEqual(self.run_collection(17, 6), {})
        self.assertEqual(self.store.daily(DAY)[0]["order_count"], 2)

    def test_empty_success_can_be_queried(self):
        self.assertEqual(self.run_collection(16, 30), {})
        self.assertEqual(self.store.daily(DAY), [])

    def test_missing_source_is_not_created(self):
        missing = self.source.with_name("missing.sqlite3")
        with self.assertRaises(sqlite3.OperationalError):
            collect(missing, self.store, "missing", datetime.now(timezone.utc), 1)
        self.assertFalse(missing.exists())


    def test_billing_v1_cursor_ties_and_independent_checkpoints(self):
        self.execute(Path("samples/billing-v1-demo.sql").read_text(), script=True)
        self.assertEqual(self.run_collection(17, 0), {"inserted": 1})
        through = datetime(2026, 1, 14, 16, 40, tzinfo=timezone.utc)
        report = collect(self.source, self.store, "billing-demo", through, 1, source_format=BILLING_V1)
        self.assertEqual(report["outcomes"], {"inserted": 1, "updated": 1})
        history = billing_history(self.store, "ORD_20260115_SH001_0001")
        self.assertEqual([event["status"] for event in history], ["COMPLETED", "REFUNDED"])
        self.assertEqual(history[0]["tou_slices"][0]["energy_wh"], 10000)
        replay = collect(self.source, self.store, "billing-demo", through, 1, source_format=BILLING_V1)
        self.assertEqual(replay["outcomes"], {"stale": 1, "duplicate": 1})
        self.assertEqual(self.store.daily(DAY)[0]["order_count"], 1)

    def test_billing_v1_cursor_mismatch_keeps_checkpoint(self):
        self.execute(Path("samples/billing-v1-demo.sql").read_text(), script=True)
        self.execute("UPDATE billing_v1_event SET payload = json_set(payload, '$.version', 4) WHERE version = 2")
        first = datetime(2026, 1, 14, 17, 0, tzinfo=timezone.utc)
        with self.assertRaisesRegex(ValueError, "cutoff unchanged"):
            collect(self.source, self.store, "v1-test", first, 1, source_format=BILLING_V1)
        self.execute("UPDATE billing_v1_event SET payload = json_set(payload, '$.version', 2) WHERE version = 2")
        retry = datetime(2026, 1, 14, 17, 30, tzinfo=timezone.utc)
        report = collect(self.source, self.store, "v1-test", retry, 1, source_format=BILLING_V1)
        self.assertEqual(report["outcomes"], {"duplicate": 1, "updated": 1})



    def test_v1_field_rename_blocks_and_corrected_rerun_clears_status(self):
        self.execute(Path("samples/billing-v1-demo.sql").read_text(), script=True)
        self.execute("""
            UPDATE billing_v1_event
            SET updated_at = '2026-01-14T16:44:00.000000+00:00',
                payload = json_set(payload, '$.updated_at', '2026-01-14T16:44:00Z')
            WHERE version = 2
        """)
        self.execute("""
            UPDATE billing_v1_event SET payload = json_remove(
                json_set(payload, '$.pay_amount', json_extract(payload, '$.total_amount_cents')),
                '$.total_amount_cents'
            ) WHERE version = 2
        """)
        times = [datetime(2026, 1, 14, h, m, tzinfo=timezone.utc) for h, m in [(16, 40), (17, 15), (17, 16)]]
        source = "field-rename"
        first = collect(self.source, self.store, source, times[0], 1, source_format=BILLING_V1)
        self.assertEqual(first["outcomes"], {"inserted": 1})
        with self.assertRaisesRegex(ValueError, "cutoff unchanged") as rejected:
            collect(self.source, self.store, source, times[1], 1, source_format=BILLING_V1)
        self.assertIsNotNone(rejected.exception.record)
        self.assertIsNone(rejected.exception.record.declared_amount_cents)
        row, = collection_quality(self.store)
        self.assertEqual((row["status"], row["failure_code"], row["rule_id"]),
                         ("blocked", "contract_validation", "billing_schema"))
        self.assertIn("cdp_collection_quality_blocked_sources 1\n", metrics_text(self.store))
        self.assertEqual(len(billing_history(self.store, "ORD_20260115_SH001_0001")), 1)
        with closing(sqlite3.connect(self.store.directory / "collection.sqlite3")) as db:
            self.assertEqual(db.execute("SELECT through_utc FROM collection_checkpoint").fetchone(), (first["through_utc"],))

        self.execute("""
            UPDATE billing_v1_event SET payload = json_remove(
                json_set(payload, '$.total_amount_cents', json_extract(payload, '$.pay_amount')),
                '$.pay_amount'
            ) WHERE version = 2
        """)
        recovered = collect(self.source, self.store, source, times[2], 1, source_format=BILLING_V1)
        self.assertEqual(recovered["outcomes"], {"duplicate": 1, "updated": 1})
        row, = collection_quality(self.store)
        self.assertEqual((row["status"], row["failure_code"], row["rule_id"]), ("ok", None, None))
        self.assertIn("cdp_collection_quality_blocked_sources 0\n", metrics_text(self.store))

    def test_quality_history_and_daily_rollup_across_shanghai_midnight(self):
        day = date(2026, 1, 15)
        self.store.initialize()
        timestamps = (
            "2026-01-14T15:59:59+00:00",
            "2026-01-14T16:00:00+00:00",
            "2026-01-14T16:00:01+00:00",
            "2026-01-15T16:00:00+00:00",
        )
        with patch("cdp.collection_quality.datetime") as clock:
            clock.now.side_effect = [datetime.fromisoformat(value) for value in timestamps]
            record_quality(self.store, "source", None)
            record_quality(self.store, "source", "contract_validation", QualityRule.BILLING_SCHEMA)
            record_quality(self.store, "source", None)
            record_quality(self.store, "other", None)

        report = daily_quality(self.store, day)
        self.assertEqual((report["total_checks"], report["blocked_checks"]), (2, 1))
        self.assertEqual([(row["status"], row["rule_id"], row["check_count"]) for row in report["rows"]],
                         [("blocked", "billing_schema", 1), ("ok", None, 1)])
        first, = quality_history(self.store, day, limit=1, offset=0)
        second, = quality_history(self.store, day, limit=1, offset=1)
        self.assertEqual((first["status"], second["status"]), ("blocked", "ok"))
        self.assertNotEqual(first["check_id"], second["check_id"])
        self.assertEqual(quality_history(self.store, day, limit=1, offset=2), [])
        current = next(row for row in collection_quality(self.store) if row["source_key"] == "source")
        self.assertEqual((current["status"], current["rule_id"]), ("ok", None))


    def test_daily_quarantine_deduplicates_and_withholds_ambiguous_amounts(self):
        self.store.initialize()
        day = date(2026, 1, 15)
        observations = (
            RejectedRecord("order-a", "record-a", 1100),
            RejectedRecord("order-a", "record-a", 1100),
            RejectedRecord("order-a", "record-a-v2", 1100),
            RejectedRecord("order-b", "record-b", 2000),
            RejectedRecord("order-b", "record-b-v2", 2200),
            RejectedRecord("order-c", "record-c", None),
            None,
        )
        with patch("cdp.collection_quality.datetime") as clock:
            clock.now.return_value = datetime(2026, 1, 15, tzinfo=timezone.utc)
            for rejection in observations:
                record_quality(self.store, "source", "contract_validation", rejection=rejection)

        report = daily_quality(self.store, day)
        self.assertEqual(report["blocked_checks"], 7)
        self.assertEqual(report["quarantine"], {
            "distinct_orders": 3, "distinct_records": 5,
            "orders_with_unknown_amount": 2, "known_declared_amount_cents": 1100,
            "blocked_checks_without_evidence": 1,
        })
        history = quality_history(self.store, day, limit=10, offset=0)
        self.assertEqual(sum(row["has_rejection_evidence"] for row in history), 6)
        self.assertNotIn("order_ref", history[0])
        self.assertNotIn("record_ref", history[0])


if __name__ == "__main__":
    unittest.main()
