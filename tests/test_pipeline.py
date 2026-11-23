import sqlite3
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from cdp.billing_ledger import billing_history
from cdp.collection_quality import CollectionRejected
from cdp.pipeline import run_once
from cdp.store import Store


class PipelineTest(unittest.TestCase):
    def test_only_publishes_after_successful_collection(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.db"
            store = Store(root / "target")
            with closing(sqlite3.connect(source)) as db, db:
                db.executescript(Path("samples/billing-v1-demo.sql").read_text())
                db.execute("""
                    UPDATE billing_v1_event
                    SET payload = json_remove(payload, '$.total_amount_cents')
                    WHERE version = 2
                """)
            with patch("cdp.pipeline.publish_to_kafka", return_value=2) as publish:
                with self.assertRaises(CollectionRejected):
                    run_once(store, source, "pipeline-test", "broker:9092", 1, 10)
                publish.assert_not_called()

                with closing(sqlite3.connect(source)) as db, db:
                    db.execute("""
                        UPDATE billing_v1_event
                        SET payload = json_set(payload, '$.total_amount_cents', 1100)
                        WHERE version = 2
                    """)
                result = run_once(store, source, "pipeline-test", "broker:9092", 1, 10)
                publish.assert_called_once_with(store, "broker:9092", 10)
            self.assertEqual(result["collection"]["outcomes"], {"duplicate": 1, "updated": 1})
            self.assertEqual(result["published"], 2)
            self.assertEqual(len(billing_history(store, "ORD_20260115_SH001_0001")), 2)


if __name__ == "__main__":
    unittest.main()
