import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.billing import BillingContractError, map_billing_order
from cdp.billing_ledger import billing_history, ingest_billing, verify_archive
from cdp.store import Store

PAYLOAD = {
    "order_id": "ORD_20260115_SH001_0001",
    "station_id": "ST_SH_001",
    "gun_id": "GUN_SH001_01A",
    "user_id_hmac": "a" * 64,
    "start_time": "2026-01-14T15:50:00Z",
    "end_time": "2026-01-14T16:30:00Z",
    "updated_at": "2026-01-14T16:31:00Z",
    "version": 1,
    "status": "COMPLETED",
    "total_kwh": 10.25,
    "total_amount_cents": 1110,
    "elec_fee_cents": 800,
    "service_fee_cents": 300,
    "penalty_fee_cents": 20,
    "discount_amount_cents": 10,
    "tou_slices": [{
        "tou_type": "FLAT",
        "start_time": "2026-01-14T15:50:00Z",
        "end_time": "2026-01-14T16:30:00Z",
        "kwh": 10.25,
        "elec_fee_cents": 800,
        "service_fee_cents": 300,
    }],
}


def payload(**updates):
    return json.dumps(PAYLOAD | updates).encode()


class BillingTest(unittest.TestCase):
    def test_completed_mapping_omits_pseudonymous_identifiers(self):
        status, order = map_billing_order(payload())
        self.assertEqual(status, "COMPLETED")
        self.assertEqual(order.energy_wh, 10250)
        self.assertEqual(order.electricity_fee_cents, 800)
        self.assertNotIn("user_id_hmac", order.model_dump())

    def test_lifecycle_states_do_not_project_to_completed_order(self):
        for status in ("CHARGING", "REFUNDED", "ABNORMAL_CLOSED"):
            self.assertEqual(map_billing_order(payload(status=status)), (status, None))

    def test_rejects_unbalanced_invalid_and_inconsistent_payloads(self):
        for updates in (
            {"total_amount_cents": 1100},
            {"updated_at": "2026-01-15T00:31:00+08:00"},
            {"event_time": "2026-01-14T16:29:00Z"},
            {"total_kwh": 10.2511},
            {"tou_slices": [PAYLOAD["tou_slices"][0] | {"kwh": 10.26}]},
            {"user_id_hmac": "not-a-digest"},
        ):
            with self.subTest(updates=updates), self.assertRaises(BillingContractError):
                map_billing_order(payload(**updates))

    def test_ledger_orders_versions_and_then_update_time(self):
        with TemporaryDirectory() as directory:
            self.check_ledger(Store(Path(directory)))

    def check_ledger(self, store):
        original = payload()
        refund = payload(status="REFUNDED", version=2, updated_at="2026-01-15T16:31:00Z")
        correction = payload(status="REFUNDED", version=2, updated_at="2026-01-15T16:32:00Z")
        self.assertEqual(ingest_billing(store, original), "inserted")
        self.assertEqual(ingest_billing(store, refund), "updated")
        self.assertEqual(ingest_billing(store, correction), "updated")
        self.assertEqual(ingest_billing(store, correction), "duplicate")
        self.assertEqual(ingest_billing(store, refund), "stale")
        self.assertEqual(ingest_billing(store, payload(updated_at="2026-01-16T16:31:00Z")), "stale")
        with self.assertRaises(BillingContractError):
            ingest_billing(store, payload(status="COMPLETED", version=2, updated_at="2026-01-15T16:32:00Z"))
        with store.connect(readonly=True) as connection:
            row = connection.execute("SELECT status, snapshot FROM dwd_billing_order_v1").fetchone()
        self.assertEqual(row["status"], "REFUNDED")
        self.assertNotIn("user_id_hmac", json.loads(row["snapshot"]))
        history = billing_history(store, PAYLOAD["order_id"])
        self.assertEqual([row["status"] for row in history], ["COMPLETED", "REFUNDED", "REFUNDED"])
        self.assertEqual(history[0]["tou_slices"][0]["energy_wh"], 10250)
        self.assertEqual(history[-1]["version"], 2)

    def test_conflicting_slices_do_not_modify_history(self):
        with TemporaryDirectory() as directory:
            store = Store(Path(directory))
            self.assertEqual(ingest_billing(store, payload()), "inserted")
            slices = [PAYLOAD["tou_slices"][0] | {"tou_type": "PEAK"}]
            with self.assertRaisesRegex(BillingContractError, "conflicting TOU"):
                ingest_billing(store, payload(tou_slices=slices))
            history = billing_history(store, PAYLOAD["order_id"])
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]["tou_slices"][0]["tou_type"], "FLAT")

    def test_migration_preserves_existing_snapshot_without_inventing_slices(self):
        with TemporaryDirectory() as directory:
            store = Store(Path(directory))
            ingest_billing(store, payload())
            with store.connect() as connection:
                connection.execute("DROP TABLE billing_order_archive")
                connection.execute("DROP TABLE dwd_billing_order_v1_history")
            upgraded = Store(Path(directory))
            history = billing_history(upgraded, PAYLOAD["order_id"])
            self.assertEqual(len(history), 1)
            self.assertIsNone(history[0]["tou_slices"])
            self.assertEqual(verify_archive(upgraded, PAYLOAD["order_id"])["versions"][0]["result"], "legacy_no_archive")
            self.assertEqual(ingest_billing(upgraded, payload()), "duplicate")
            self.assertEqual(billing_history(upgraded, PAYLOAD["order_id"]), history)



if __name__ == "__main__":
    unittest.main()
