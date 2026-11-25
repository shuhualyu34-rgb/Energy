import hashlib
import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import ValidationError

from cdp.reconcile_three_way import SOURCES, FrozenStationDay, compare_station_days
from cdp.store import Store


class ThreeWayReconciliationTest(unittest.TestCase):
    def snapshot(self, source, **changes):
        sample = json.loads(Path("samples/reference-day.json").read_bytes())
        values = sample | {
            "source": source,
            "timezone": "Asia/Shanghai",
            "metric_version": "demo-0.1",
            "frozen_at": "2026-01-16T08:00:00+08:00",
        }
        return FrozenStationDay.model_validate(values | changes)

    def test_offsets_normalize_and_missing_stations_are_reported_separately(self):
        snapshots = {source: self.snapshot(source) for source in SOURCES}
        snapshots["finance"] = self.snapshot("finance", frozen_at="2026-01-16T00:00:00Z")
        report = compare_station_days(snapshots)
        self.assertTrue(report["matched"])
        self.assertEqual(report["frozen_at_utc"], "2026-01-16T00:00:00+00:00")

        station, = snapshots["finance"].rows
        changes = [
            station.model_dump() | {"service_fee_cents": 299},
            station.model_dump() | {"station_id": "demo-station-002"},
        ]
        snapshots["finance"] = self.snapshot("finance", rows=changes)
        report = compare_station_days(snapshots)
        self.assertFalse(report["matched"])
        self.assertEqual(report["differences"], [
            {"station_id": "demo-station-001", "missing_from": [],
             "metrics": {"service_fee_cents": {"finance": 299, "operations": 300, "platform": 300}}},
            {"station_id": "demo-station-002", "missing_from": ["operations", "platform"], "metrics": {}},
        ])

    def test_invalid_scopes_duplicates_and_empty_inputs_are_rejected(self):
        original = {source: self.snapshot(source) for source in SOURCES}
        station, = original["finance"].rows
        for change in (
            {"business_day": "2026-01-14"},
            {"metric_version": "demo-0.2"},
            {"frozen_at": "2026-01-16T08:01:00+08:00"},
            {"rows": [station, station]},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                compare_station_days(original | {"finance": self.snapshot("finance", **change)})
        with self.assertRaisesRegex(ValueError, "all three snapshots are empty"):
            compare_station_days({source: self.snapshot(source, rows=[]) for source in SOURCES})
        with self.assertRaisesRegex(ValueError, "end of the Shanghai business day"):
            compare_station_days({source: self.snapshot(source, frozen_at="2026-01-15T20:00:00+08:00")
                                  for source in SOURCES})

    def test_freeze_time_requires_an_explicit_timezone(self):
        for value in ("2026-01-16T08:00:00", 1768521600, "1768521600"):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                self.snapshot("finance", frozen_at=value)

    def test_cli_statuses_and_input_checksums(self):
        with TemporaryDirectory() as directory:
            command = [sys.executable, "-m", "cdp.reconcile_three_way"]
            root = Path(directory)
            platform = Store(root / "platform")
            self.assertEqual(platform.ingest(Path("samples/order.json").read_bytes()), "inserted")
            platform_rows = platform.daily(self.snapshot("platform").business_day)
            for row in platform_rows:
                row.pop("business_day")
            files = {source: root / f"{source}.json" for source in SOURCES}
            for source, path in files.items():
                snapshot = self.snapshot(source, rows=platform_rows) if source == "platform" else self.snapshot(source)
                path.write_text(snapshot.model_dump_json())
                command.extend([f"--{source}", str(path)])
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)["snapshot_sha256"], {
                source: hashlib.sha256(path.read_bytes()).hexdigest() for source, path in files.items()
            })
            files["finance"].write_text(self.snapshot("finance", rows=[]).model_dump_json())
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 2, result.stderr)
            files["finance"].write_text(self.snapshot("operations").model_dump_json())
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 1, result.stderr)


if __name__ == "__main__":
    unittest.main()
