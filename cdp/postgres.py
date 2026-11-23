from contextlib import contextmanager
from datetime import date
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .store import Store


READ_RELATIONS = (
    "ads_station_daily",
    "billing_collection_quality",
    "billing_feature_availability",
    "billing_kafka_delivery",
    "billing_spark_sink_progress",
    "billing_stream_event",
    "billing_stream_late",
    "billing_stream_progress",
    "billing_stream_quarantine",
    "dwd_billing_order_v1_history",
)


class PostgresStore(Store):
    bind = "%s"

    def __init__(self, directory: Path, dsn: str, *, api_reader: bool = False):
        super().__init__(directory)
        self.dsn = dsn
        self.api_reader = api_reader

    @contextmanager
    def connect(self, *, readonly: bool = False):
        with psycopg.connect(self.dsn, row_factory=dict_row, connect_timeout=5) as connection:
            if readonly or self.api_reader:
                connection.read_only = True
            yield connection

    def initialize(self) -> None:
        if self.initialized:
            return
        with self.connect() as connection:
            connection.execute("SELECT pg_advisory_xact_lock(387214, 1)")
            connection.execute("""
                CREATE TABLE IF NOT EXISTS cdp_migration (version INTEGER PRIMARY KEY)
            """)
            directory = Path(__file__).with_name("migrations")
            for version in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12):
                applied = connection.execute(
                    "SELECT 1 FROM cdp_migration WHERE version = %s", (version,)
                )
                if applied.fetchone() is None:
                    migration = directory / f"postgres_{version:03}.sql"
                    connection.execute(migration.read_text())
                    connection.execute("INSERT INTO cdp_migration VALUES (%s)", (version,))
        self.initialized = True

    def ready(self) -> None:
        if not self.api_reader:
            super().ready()
            return
        checks = sql.SQL(" UNION ALL ").join(
            sql.SQL("SELECT 1 FROM {}").format(sql.Identifier(name)) for name in READ_RELATIONS
        )
        with self.connect(readonly=True) as connection:
            connection.execute(checks + sql.SQL(" LIMIT 0"))

    def verify_api_reader(self) -> None:
        with self.connect(readonly=True) as connection:
            privileges = connection.execute("""
                SELECT
                    rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls AS elevated,
                    has_schema_privilege(current_user, 'public', 'CREATE') AS creates_in_public,
                    EXISTS (
                        SELECT 1 FROM pg_class AS c
                        JOIN pg_namespace AS n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v')
                            AND has_table_privilege(current_user, c.oid, 'INSERT,UPDATE,DELETE,TRUNCATE')
                    ) AS writes_business_data
                FROM pg_roles WHERE rolname = current_user
            """).fetchone()
            if not privileges or any(privileges.values()):
                raise ValueError("API PostgreSQL connection must use a non-writable reader role")
        self.ready()
        # API startup relies on migrations run by the writer, including for nested query helpers.
        self.initialized = True

    @contextmanager
    def write_order(self, order_id: str):
        # Lock by business key, including when the first version has no row yet.
        with self.connect() as connection:
            connection.execute(
                "SELECT pg_advisory_xact_lock(387215, hashtext(%s))", (order_id,)
            )
            yield connection

    def daily(self, day: date) -> list[dict]:
        rows = super().daily(day)
        for row in rows:
            row["business_day"] = row["business_day"].isoformat()
            for key in ("energy_wh", "electricity_fee_cents", "service_fee_cents"):
                row[key] = int(row[key])
        return rows
