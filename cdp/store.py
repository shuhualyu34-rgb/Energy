import hashlib
import json
import os
import re
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import date, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from zoneinfo import ZoneInfo

from .contract import Order

BUSINESS_ZONE = ZoneInfo("Asia/Shanghai")
SCHEMA = """
CREATE TABLE IF NOT EXISTS dwd_order (
    order_id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    started_at_utc TEXT NOT NULL,
    ended_at_utc TEXT NOT NULL,
    business_day TEXT NOT NULL,
    energy_wh INTEGER NOT NULL,
    electricity_fee_cents INTEGER NOT NULL,
    service_fee_cents INTEGER NOT NULL,
    snapshot_hash TEXT NOT NULL,
    raw_hash TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS dwd_order_day_station ON dwd_order (business_day, station_id);
CREATE TABLE IF NOT EXISTS dwd_billing_order_v1 (
    order_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL CHECK (version > 0),
    updated_at_utc TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('CHARGING','COMPLETED','ABNORMAL_CLOSED','REFUNDED')),
    snapshot TEXT NOT NULL
);
CREATE VIEW IF NOT EXISTS ads_station_daily AS
SELECT station_id, business_day, COUNT(*) AS order_count,
       SUM(energy_wh) AS energy_wh,
       SUM(electricity_fee_cents) AS electricity_fee_cents,
       SUM(service_fee_cents) AS service_fee_cents
FROM dwd_order
GROUP BY station_id, business_day;
"""


class VersionConflict(ValueError):
    """The same order version has different business data."""


class Store:
    bind = "?"

    def __init__(self, directory: Path):
        self.directory = directory.resolve()
        self.database = self.directory / "cdp.sqlite3"
        self.initialized = False

    @contextmanager
    def connect(self, *, readonly: bool = False) -> Iterator[sqlite3.Connection]:
        if readonly:
            connection = sqlite3.connect(self.database.as_uri() + "?mode=ro", uri=True)
        else:
            connection = sqlite3.connect(self.database)
        with closing(connection), connection:
            connection.row_factory = sqlite3.Row
            yield connection

    def initialize(self) -> None:
        if self.initialized:
            return
        self.directory.mkdir(parents=True, exist_ok=True)
        migrations = Path(__file__).with_name("migrations")
        upgrade = "".join(
            (migrations / f"postgres_{version:03}.sql").read_text()
            for version in (3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
        )
        with self.connect() as connection:
            connection.executescript("BEGIN IMMEDIATE;\n" + SCHEMA + upgrade + "\nCOMMIT;")
        self.initialized = True

    def ingest(self, raw: bytes) -> str:
        order = Order.model_validate_json(raw)
        started = order.started_at.astimezone(timezone.utc).isoformat()
        ended = order.ended_at.astimezone(timezone.utc).isoformat()
        snapshot = order.model_dump(mode="json")
        snapshot.update(started_at=started, ended_at=ended)
        canonical = json.dumps(snapshot, sort_keys=True).encode()
        raw_hash = hashlib.sha256(raw).hexdigest()
        snapshot_hash = hashlib.sha256(canonical).hexdigest()
        business_day = order.ended_at.astimezone(BUSINESS_ZONE).date().isoformat()

        self.initialize()
        with self.write_order(order.order_id) as connection:
            existing = connection.execute(
                f"SELECT version, snapshot_hash FROM dwd_order WHERE order_id = {self.bind}",
                (order.order_id,),
            ).fetchone()
            if existing and existing["version"] > order.version:
                return "stale"
            if existing and existing["version"] == order.version:
                if existing["snapshot_hash"] != snapshot_hash:
                    raise VersionConflict("order version has conflicting business data")
                return "duplicate"

            self.archive(raw_hash, raw)
            placeholders = ", ".join([self.bind] * 11)
            connection.execute(
                f"""INSERT INTO dwd_order (
                    order_id, station_id, version, started_at_utc, ended_at_utc,
                    business_day, energy_wh, electricity_fee_cents,
                    service_fee_cents, snapshot_hash, raw_hash
                ) VALUES ({placeholders})
                ON CONFLICT(order_id) DO UPDATE SET
                    station_id = excluded.station_id,
                    version = excluded.version,
                    started_at_utc = excluded.started_at_utc,
                    ended_at_utc = excluded.ended_at_utc,
                    business_day = excluded.business_day,
                    energy_wh = excluded.energy_wh,
                    electricity_fee_cents = excluded.electricity_fee_cents,
                    service_fee_cents = excluded.service_fee_cents,
                    snapshot_hash = excluded.snapshot_hash,
                    raw_hash = excluded.raw_hash""",
                (
                    order.order_id, order.station_id, order.version, started, ended,
                    business_day, order.energy_wh, order.electricity_fee_cents,
                    order.service_fee_cents, snapshot_hash, raw_hash,
                ),
            )
        return "updated" if existing else "inserted"

    @contextmanager
    def write_order(self, order_id: str):
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            yield connection

    def ready(self) -> None:
        with self.connect(readonly=True) as connection:
            connection.execute("SELECT 1 FROM ads_station_daily LIMIT 0")

    def archived_orders(self) -> Iterator[bytes]:
        with self.connect(readonly=True) as connection:
            references = connection.execute(
                "SELECT raw_hash FROM dwd_order ORDER BY order_id"
            ).fetchall()
        for reference in references:
            digest = reference["raw_hash"]
            yield self.read_archive(digest)

    def archive_path(self, digest: str, namespace: str) -> Path:
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("invalid archive checksum")
        return self.directory / "ods" / namespace / f"{digest}.json"

    def read_archive(self, digest: str, *, namespace: str = "orders") -> bytes:
        raw = self.archive_path(digest, namespace).read_bytes()
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError(f"archive checksum mismatch: {digest}")
        return raw

    def archive(self, digest: str, raw: bytes, *, namespace: str = "orders") -> None:
        target = self.archive_path(digest, namespace)
        directory = target.parent
        directory.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(dir=directory) as temporary:
            temporary.write(raw)
            temporary.flush()
            os.fsync(temporary.fileno())
            try:
                os.link(temporary.name, target)
            except FileExistsError:
                if target.read_bytes() != raw:
                    raise ValueError(f"archive checksum mismatch: {digest}")
            for parent in (directory, directory.parent, self.directory):
                directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)

    def daily(self, day: date) -> list[dict]:
        with self.connect(readonly=True) as connection:
            rows = connection.execute(
                f"SELECT * FROM ads_station_daily WHERE business_day = {self.bind} ORDER BY station_id",
                (day.isoformat(),),
            )
            return [dict(row) for row in rows]
