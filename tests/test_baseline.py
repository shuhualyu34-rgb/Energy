import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from cdp.baseline import backtest, forecast
from cdp.billing_ledger import ingest_billing
from cdp.store import Store

CUTOFF = datetime(2026, 1, 15, 0, tzinfo=timezone.utc)


class BaselineTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = Store(Path(directory.name))

    def order(self, station, reference, ended, kwh, *, received=None):
        station_code = f"{station:03}"
        payload = {
            "order_id": f"ORD_20260115_SH{station_code}_{reference:04}",
            "station_id": f"ST_SH_{station_code}",
            "gun_id": f"GUN_SH{station_code}_01A",
            "user_id_hmac": "a" * 64,
            "start_time": (ended - timedelta(minutes=30)).isoformat(),
            "end_time": ended.isoformat(),
            "updated_at": ended.isoformat(),
            "status": "COMPLETED",
            "version": 1,
            "total_kwh": kwh,
            "total_amount_cents": 0,
            "elec_fee_cents": 0,
            "service_fee_cents": 0,
        }
        with patch("cdp.billing_ledger.datetime") as clock:
            clock.now.return_value = received if received is not None else ended
            ingest_billing(self.store, json.dumps(payload).encode())

    def test_inference_excludes_unavailable_orders_and_normalizes_cutoff(self):
        self.order(1, 1, CUTOFF - timedelta(hours=2), 10)
        self.order(2, 2, CUTOFF - timedelta(hours=1), 20, received=CUTOFF + timedelta(minutes=1))
        result = forecast(self.store, datetime.fromisoformat("2026-01-15T08:00:00+08:00"))
        self.assertEqual(result["target_start_utc"], "2026-01-15T00:00:00.000000+00:00")
        self.assertEqual(result["target_end_utc"], "2026-01-16T00:00:00.000000+00:00")
        self.assertEqual(result["predictions"], [{"station_id": "ST_SH_001", "predicted_energy_wh": 10000}])

    def test_backtest_reports_error_only_for_paired_stations(self):
        self.order(1, 1, CUTOFF - timedelta(hours=3), 10)
        self.order(2, 2, CUTOFF - timedelta(hours=2), 5)
        self.order(1, 3, CUTOFF + timedelta(hours=5), 8)
        self.order(3, 4, CUTOFF + timedelta(hours=6), 20)
        self.order(1, 5, CUTOFF + timedelta(hours=7), 100, received=CUTOFF + timedelta(hours=25))
        result = backtest(self.store, CUTOFF)
        self.assertEqual(result["missing_actuals"], ["ST_SH_002"])
        self.assertEqual(result["missing_predictions"], ["ST_SH_003"])
        self.assertEqual(result["evaluated"], [{
            "station_id": "ST_SH_001",
            "predicted_energy_wh": 10000,
            "actual_energy_wh": 8000,
            "absolute_error_wh": 2000,
        }])
        self.assertEqual(result["metrics"], {
            "evaluated_stations": 1, "forecast_station_count": 2, "actual_station_count": 2,
            "mae_wh": 2000.0, "wape": 0.25,
        })

    def test_rejects_unfinished_backtest_target(self):
        with self.assertRaisesRegex(ValueError, "target period has not ended"):
            backtest(self.store, datetime.now(timezone.utc) - timedelta(hours=1))

    def test_confirmed_zero_actual_remains_a_pair_and_has_no_wape(self):
        self.order(1, 1, CUTOFF - timedelta(hours=2), 1)
        self.order(1, 2, CUTOFF + timedelta(hours=2), 0)
        metrics = backtest(self.store, CUTOFF)["metrics"]
        self.assertEqual(metrics["evaluated_stations"], 1)
        self.assertEqual(metrics["mae_wh"], 1000)
        self.assertIsNone(metrics["wape"])


if __name__ == "__main__":
    unittest.main()
