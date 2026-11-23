#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python -E - <<'PY'
import io
import json
from contextlib import redirect_stdout
from email.message import Message
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.error import HTTPError

from cdp import weather
from cdp.store import Store

url = "https://forecast.example.invalid/snapshot"
snapshot = json.dumps({
    "kind": "forecast", "station_id": "ST_SH_001",
    "issued_at": "2026-01-15T10:00:00Z", "valid_at": "2026-01-15T12:00:00Z",
    "temperature_centi_c": 1200, "precipitation_micrometers": 0,
}).encode()
elapsed = 0
waits = []


def simulated_sleep(seconds):
    global elapsed
    waits.append(seconds)
    elapsed += seconds


def response(*args, **kwargs):
    if elapsed < 600:
        headers = Message()
        headers["Retry-After"] = "20"
        raise HTTPError(url, 429, "rate limited", headers, io.BytesIO())
    return io.BytesIO(snapshot)


with TemporaryDirectory() as directory, patch.multiple(
    weather, monotonic=lambda: elapsed, sleep=simulated_sleep, urlopen=response,
):
    store = Store(Path(directory))
    output = io.StringIO()
    with patch("sys.argv", ["weather", "--url", url]), patch.object(
        weather, "configured_store", return_value=store,
    ), redirect_stdout(output):
        weather.main()
    assert output.getvalue().strip() == "inserted"
    assert 600 <= elapsed <= 600 + 15 * 60
    assert waits[:3] == [20, 20, 20] and waits[-1] == 60, waits
    assert weather.ingest_forecast(store, weather.fetch_forecast(url)) == "duplicate"
    with store.connect(readonly=True) as db:
        assert db.execute("SELECT COUNT(*) FROM station_weather_forecast").fetchone()[0] == 1
    print(f"S6: persisted one forecast after {elapsed}s simulated time and {len(waits)} retries.")

    elapsed = 0
    try:
        weather.fetch_forecast(url, retry_budget=10)
    except ValueError as error:
        assert "retry budget" in str(error)
    else:
        raise AssertionError("expected a bounded failure")
    assert elapsed == 0
PY
