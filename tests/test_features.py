import json
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from cdp.billing_ledger import ingest_billing
from cdp.features import station_features
from cdp.store import Store
from cdp.weather import ingest_forecast

ORDER = {
    "order_id": "ORD_20260115_SH001_0001",
    "station_id": "ST_SH_001",
    "gun_id": "GUN_SH001_01A",
    "user_id_hmac": "a" * 64,
    "start_time": "2026-01-14T09:00:00Z",
    "end_time": "2026-01-14T10:00:00Z",
    "updated_at": "2026-01-14T10:01:00Z",
    "status": "COMPLETED",
    "version": 1,
    "total_kwh": 10.25,
    "total_amount_cents": 1100,
    "elec_fee_cents": 800,
    "service_fee_cents": 300,
}


def timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value)


class PointInTimeFeaturesTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = Store(Path(directory.name))

    def accept(self, available_at, **fields):
        with patch("cdp.billing_ledger.datetime") as clock:
            clock.now.return_value = timestamp(available_at)
            return ingest_billing(self.store, json.dumps(ORDER | fields).encode())

    def rows(self, as_of):
        return station_features(self.store, timestamp(as_of))["rows"]

    def test_late_ingestion_and_later_refund_do_not_change_past_features(self):
        self.accept("2026-01-14T11:00:00Z")
        self.assertEqual(self.rows("2026-01-14T10:30:00Z"), [])
        before_refund = self.rows("2026-01-14T11:30:00Z")
        self.assertEqual(before_refund, [{
            "station_id": "ST_SH_001",
            "completed_order_count_24h": 1,
            "completed_energy_wh_24h": 10250,
        }])
        self.accept("2026-01-14T12:30:00Z", status="REFUNDED", version=2, updated_at="2026-01-14T11:15:00Z")
        self.assertEqual(self.rows("2026-01-14T11:30:00Z"), before_refund)
        self.assertEqual(self.rows("2026-01-14T12:31:00Z"), [])

    def test_source_timestamp_and_half_open_lookback(self):
        self.accept("2026-01-14T11:00:00Z", updated_at="2026-01-14T12:00:00Z")
        self.assertEqual(self.rows("2026-01-14T11:59:59Z"), [])
        self.assertEqual(len(self.rows("2026-01-15T10:00:00Z")), 1)
        self.assertEqual(self.rows("2026-01-15T10:00:00.000001Z"), [])
        self.accept("2026-01-14T10:00:00Z", order_id="ORD_20260115_SH001_0002", updated_at="2026-01-14T10:00:00Z")
        self.assertEqual(self.rows("2026-01-14T10:00:00Z"), [])

    def test_missing_legacy_evidence_and_duplicate_preserve_cutoff(self):
        self.accept("2026-01-14T11:00:00Z")
        with self.store.connect() as connection:
            connection.execute("DELETE FROM billing_feature_availability")
        self.assertEqual(self.rows("2026-01-14T12:00:00Z"), [])
        self.assertEqual(self.accept("2026-01-14T11:15:00Z"), "duplicate")
        self.assertEqual(self.accept("2026-01-14T11:30:00Z"), "duplicate")
        self.assertEqual(self.rows("2026-01-14T11:14:59Z"), [])
        self.assertEqual(len(self.rows("2026-01-14T11:15:00Z")), 1)


    def forecast(self, received, **changes):
        row = {
            "kind": "forecast", "station_id": "ST_SH_001",
            "issued_at": "2026-01-14T10:00:00Z", "valid_at": "2026-01-14T12:00:00Z",
            "temperature_centi_c": 1200, "precipitation_micrometers": 0,
        }
        with patch("cdp.weather.datetime") as clock:
            clock.now.return_value = timestamp(received)
            return ingest_forecast(self.store, json.dumps(row | changes).encode())

    def test_weather_respects_target_publication_and_platform_availability(self):
        self.accept("2026-01-14T11:00:00Z")
        self.forecast("2026-01-14T11:05:00Z")
        self.forecast("2026-01-14T11:35:00Z", issued_at="2026-01-14T11:15:00Z", temperature_centi_c=1500)
        self.forecast("2026-01-14T11:10:00Z", issued_at="2026-01-14T11:40:00Z", temperature_centi_c=1600)

        def at(cutoff):
            return station_features(self.store, timestamp(cutoff), with_weather=True)

        before_receipt = at("2026-01-14T11:04:59Z")
        self.assertIsNone(before_receipt["rows"][0]["weather"])
        self.assertEqual(before_receipt["forecast_target_utc"], "2026-01-14T12:00:00.000000+00:00")
        self.assertEqual(at("2026-01-14T11:30:00Z")["rows"][0]["weather"]["temperature_centi_c"], 1200)
        self.assertEqual(at("2026-01-14T11:39:00Z")["rows"][0]["weather"]["temperature_centi_c"], 1500)
        self.assertEqual(at("2026-01-14T11:40:00Z")["rows"][0]["weather"]["temperature_centi_c"], 1600)
        self.assertIsNone(at("2026-01-14T12:00:00Z")["rows"][0]["weather"])

    def test_rejects_observations_and_preserves_first_available_time(self):
        self.accept("2026-01-14T11:00:00Z")
        with self.assertRaisesRegex(ValueError, "invalid weather forecast"):
            self.forecast("2026-01-14T11:05:00Z", kind="observation")
        with self.assertRaisesRegex(ValueError, "UTC hour"):
            self.forecast("2026-01-14T11:05:00Z", valid_at="2026-01-14T12:03:00Z")
        self.assertEqual(self.forecast("2026-01-14T11:05:00Z"), "inserted")
        self.assertEqual(self.forecast("2026-01-14T11:30:00Z"), "duplicate")
        with self.assertRaisesRegex(ValueError, "conflicting values"):
            self.forecast("2026-01-14T11:35:00Z", temperature_centi_c=1700)
        rows = station_features(self.store, timestamp("2026-01-14T11:06:00Z"), with_weather=True)["rows"]
        self.assertEqual(rows[0]["weather"]["temperature_centi_c"], 1200)


if __name__ == "__main__":
    unittest.main()
