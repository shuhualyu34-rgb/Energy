"""Load station forecast snapshots for point-in-time feature experiments."""

import argparse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from time import monotonic, sleep
from typing import Annotated, Literal
from urllib.error import HTTPError
from urllib.request import urlopen

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .billing import utc
from .config import configured_store
from .contract import Key
from .store import Store


class Forecast(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: Literal["forecast"]
    station_id: Key
    issued_at: str
    valid_at: str
    temperature_centi_c: Annotated[int, Field(ge=-10000, le=10000)]
    precipitation_micrometers: Annotated[int, Field(ge=0, le=2**31 - 1)]


def ingest_forecast(store: Store, raw: bytes) -> str:
    try:
        snapshot = Forecast.model_validate_json(raw)
    except ValidationError:
        raise ValueError("invalid weather forecast snapshot") from None
    issued, valid = utc(snapshot.issued_at), utc(snapshot.valid_at)
    if issued >= valid:
        raise ValueError("weather forecast must be issued before its target hour")
    if valid.minute or valid.second or valid.microsecond:
        raise ValueError("weather target must start on a UTC hour")

    position = (snapshot.station_id, valid.isoformat(timespec="microseconds"), issued.isoformat(timespec="microseconds"))
    reading = (snapshot.temperature_centi_c, snapshot.precipitation_micrometers)
    store.initialize()
    b = store.bind
    with store.write_order(f"forecast:{snapshot.station_id}") as connection:
        existing = connection.execute(f"""
            SELECT temperature_centi_c, precipitation_micrometers
            FROM station_weather_forecast
            WHERE (station_id, valid_at_utc, issued_at_utc) = ({b}, {b}, {b})
        """, position).fetchone()
        if existing:
            if (existing["temperature_centi_c"], existing["precipitation_micrometers"]) != reading:
                raise ValueError("conflicting values for the same forecast snapshot")
        else:
            connection.execute(f"""
                INSERT INTO station_weather_forecast (
                    station_id, valid_at_utc, issued_at_utc, temperature_centi_c, precipitation_micrometers
                ) VALUES ({b}, {b}, {b}, {b}, {b})
            """, (*position, *reading))

    # The snapshot is durable before it can become eligible for feature generation.
    available = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    with store.connect() as connection:
        connection.execute(f"""
            UPDATE station_weather_forecast SET available_at_utc = {b}
            WHERE (station_id, valid_at_utc, issued_at_utc) = ({b}, {b}, {b})
              AND available_at_utc IS NULL
        """, (available, *position))
    return "duplicate" if existing else "inserted"


def forecasts_as_of(store: Store, cutoff: str, target: str) -> dict[str, dict]:
    b = store.bind
    with store.connect(readonly=True) as connection:
        rows = connection.execute(f"""
            WITH ranked AS (
                SELECT station_id, issued_at_utc, temperature_centi_c, precipitation_micrometers,
                       ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY issued_at_utc DESC) AS position
                FROM station_weather_forecast
                WHERE valid_at_utc = {b} AND issued_at_utc <= {b} AND available_at_utc <= {b}
            )
            SELECT station_id, issued_at_utc, temperature_centi_c, precipitation_micrometers
            FROM ranked WHERE position = 1
        """, (target, cutoff, cutoff)).fetchall()
    return {row["station_id"]: {
        "issued_at_utc": row["issued_at_utc"],
        "temperature_centi_c": row["temperature_centi_c"],
        "precipitation_micrometers": row["precipitation_micrometers"],
    } for row in rows}


def fetch_forecast(url: str, *, retry_budget: int = 900) -> bytes:
    """Fetch a forecast, backing off on HTTP 429 within the retry window."""
    deadline = monotonic() + retry_budget
    delay = 5
    while True:
        try:
            with urlopen(url, timeout=10) as response:
                raw = response.read(65537)
                if len(raw) > 65536:
                    raise ValueError("weather forecast response exceeds 64 KiB")
                return raw
        except HTTPError as error:
            with error:
                if error.code != 429:
                    raise
                retry_after = error.headers.get("Retry-After", "").strip()
            wait = max(delay, retry_after_seconds(retry_after))
            if wait > deadline - monotonic():
                raise ValueError("weather forecast rate limit exceeded retry budget") from None
            sleep(wait)
            delay = min(delay * 2, 60)


def retry_after_seconds(value: str) -> float:
    if value.isascii() and value.isdecimal():
        return float(value)
    if value:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is not None:
                return max(0, (retry_at - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, OverflowError):
            pass
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="*", type=Path)
    parser.add_argument("--url", help="HTTP(S) URL serving one forecast snapshot")
    args = parser.parse_args()
    if bool(args.files) == bool(args.url):
        parser.error("provide forecast files or --url")
    if args.url and not args.url.startswith(("http://", "https://")):
        parser.error("--url must use HTTP or HTTPS")
    store = configured_store()
    if args.url:
        try:
            print(ingest_forecast(store, fetch_forecast(args.url)))
        except (OSError, ValueError) as error:
            parser.exit(1, f"{error}\n")
        return
    for file in args.files:
        try:
            print(ingest_forecast(store, file.read_bytes()))
        except (OSError, ValueError) as error:
            parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
