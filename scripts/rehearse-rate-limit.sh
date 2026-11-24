#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python -E - <<'PY'
import io
import json
from contextlib import redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from unittest.mock import patch

from cdp import weather
from cdp.store import Store

snapshot = {
    "kind": "forecast", "station_id": "ST_SH_001",
    "issued_at": "2026-01-15T10:00:00Z", "valid_at": "2026-01-15T12:00:00Z",
    "temperature_centi_c": 1200, "precipitation_micrometers": 0,
}
response_body = json.dumps(snapshot).encode()
outage_seconds = 10 * 60
recovery_limit = 15 * 60
retry_after = 20
elapsed = 0
waits = []
requests = []
always_limited = False


def simulated_sleep(seconds):
    global elapsed
    waits.append(seconds)
    elapsed += seconds


class ForecastHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/snapshot":
            self.send_error(404)
            return
        limited = always_limited or elapsed < outage_seconds
        status = 429 if limited else 200
        requests.append((elapsed, status))
        self.send_response(status)
        if limited:
            self.send_header("Retry-After", str(retry_after))
        else:
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        if not limited:
            self.wfile.write(response_body)

    def log_message(self, format, *args):
        pass


def run_import(store, url):
    output = io.StringIO()
    with patch("sys.argv", ["weather", "--url", url]), patch.object(
        weather, "configured_store", return_value=store,
    ), redirect_stdout(output):
        weather.main()
    return output.getvalue().strip()


with HTTPServer(("127.0.0.1", 0), ForecastHandler) as server, TemporaryDirectory() as directory:
    url = f"http://127.0.0.1:{server.server_port}/snapshot"
    worker = Thread(target=server.serve_forever)
    worker.start()
    try:
        store = Store(Path(directory) / "recovered")
        with patch.multiple(weather, monotonic=lambda: elapsed, sleep=simulated_sleep):
            assert run_import(store, url) == "inserted"
            recovered_at = elapsed
            assert requests[0] == (0, 429)
            assert requests[-1] == (recovered_at, 200)
            assert all(status == 429 and at < outage_seconds for at, status in requests[:-1]), requests
            assert len(requests) == len(waits) + 1, requests
            assert waits[:3] == [20, 20, 20] and waits[-1] == 60, waits
            assert all(wait >= retry_after for wait in waits), waits
            recovery_delay = recovered_at - outage_seconds
            assert 0 <= recovery_delay <= recovery_limit, recovery_delay
            retry_count = len(waits)
            assert run_import(store, url) == "duplicate"
            with store.connect(readonly=True) as db:
                row, = db.execute("""
                    SELECT station_id, temperature_centi_c, precipitation_micrometers, available_at_utc
                    FROM station_weather_forecast
                """).fetchall()
            assert tuple(row)[:3] == (
                snapshot["station_id"], snapshot["temperature_centi_c"], snapshot["precipitation_micrometers"],
            ), dict(row)
            assert row["available_at_utc"], dict(row)
            print(f"S6: HTTP 429 for {outage_seconds}s simulated time. "
                  f"Recovered and persisted one forecast within {recovery_delay:.0f}s after service restoration "
                  f"({retry_count} retries); replay was a duplicate.")

            always_limited = True
            elapsed = 0
            requests.clear()
            empty_store = Store(Path(directory) / "exhausted")
            errors = io.StringIO()
            with redirect_stderr(errors):
                try:
                    run_import(empty_store, url)
                except SystemExit as error:
                    assert error.code == 1, error.code
                else:
                    raise AssertionError("persistent throttling should exhaust the retry budget")
            assert "retry budget" in errors.getvalue(), errors.getvalue()
            assert all(status == 429 for _, status in requests), requests
            assert not empty_store.database.exists()
            print(f"S6: persistent HTTP 429 exhausted the retry budget after {elapsed:.0f}s "
                  "simulated time without writing a forecast.")
    finally:
        server.shutdown()
        worker.join()
PY
