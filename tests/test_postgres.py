import json
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from cdp.checkpoint import CheckpointInUse, checkpoint_lease
from cdp.postgres import PostgresStore
from cdp.reconcile import ReferenceDay, reconcile
from cdp.store import VersionConflict

DSN = os.environ.get("CDP_POSTGRES_DSN")
SAMPLE = Path("samples/order.json").read_bytes()
DAY = date(2026, 1, 15)


@unittest.skipUnless(DSN, "requires PostgreSQL sandbox")
class PostgresTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        schema = "test_" + uuid4().hex
        self.schema = sql.Identifier(schema)
        with psycopg.connect(DSN) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(self.schema))
        self.addCleanup(self.drop_schema)
        dsn = make_conninfo(DSN, options=f"-c search_path={schema}")
        self.store = PostgresStore(Path(directory.name), dsn)
        self.store.initialize()

    def drop_schema(self):
        with psycopg.connect(DSN) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(self.schema))

    def test_checkpoint_lease_conflicts_and_releases_on_exit(self):
        checkpoint = f"/tmp/cdp-checkpoint-{uuid4().hex}"
        with checkpoint_lease(DSN, checkpoint):
            with self.assertRaises(CheckpointInUse), checkpoint_lease(DSN, checkpoint):
                self.fail("duplicate lease was acquired")
            with checkpoint_lease(DSN, checkpoint + "-other") as connection:
                self.assertEqual(connection.execute("SELECT 1").fetchone(), (1,))
        with checkpoint_lease(DSN, checkpoint) as connection:
            self.assertEqual(connection.execute("SELECT 1").fetchone(), (1,))

    def test_migration_owner_cannot_serve_as_an_explicit_api_reader(self):
        with self.assertRaisesRegex(ValueError, "must use a non-writable reader role"):
            self.store.verify_api_reader()

    def test_parallel_writes_replay_and_reconciliation(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            outcomes = list(pool.map(self.store.ingest, [SAMPLE] * 12))
        self.assertEqual(outcomes.count("inserted"), 1)
        self.assertEqual(outcomes.count("duplicate"), 11)
        reference = ReferenceDay.model_validate_json(Path("samples/reference-day.json").read_bytes())
        self.assertTrue(reconcile(self.store, reference)["matched"])
        self.assertEqual(self.store.daily(date(2026, 1, 14)), [])

        correction = json.loads(SAMPLE)
        correction.update(version=2, service_fee_cents=400)
        raw = json.dumps(correction).encode()
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(self.store.ingest, [SAMPLE, raw]))
        self.assertEqual(self.store.ingest(SAMPLE), "stale")

        correction["service_fee_cents"] = 500
        with self.assertRaises(VersionConflict):
            self.store.ingest(json.dumps(correction).encode())
        self.assertEqual(list(self.store.archived_orders()), [raw])

        report = reconcile(self.store, reference)
        self.assertFalse(report["matched"])
        self.assertEqual(report["differences"][0]["source"]["service_fee_cents"], 300)
        self.assertEqual(report["differences"][0]["platform"]["service_fee_cents"], 400)


if __name__ == "__main__":
    unittest.main()
