import json
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.collection_state import (
    CHECKPOINT_FILE, CHECKPOINT_SCHEMA, CheckpointBundle, Snapshot, SourceCheckpoint,
    checksum, export_state, restore_state,
)


class CollectionStateTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def make_bundle(self, hour=16):
        snapshot = Snapshot(format_version=1, checkpoints=[
            SourceCheckpoint(source_id="billing-v1:validated", through_utc=f"2026-01-14T{hour}:31:00.000000+00:00"),
        ])
        return CheckpointBundle(snapshot=snapshot, sha256=checksum(snapshot))

    def test_round_trip_and_idempotent_restore(self):
        original = self.root / "source"
        original.mkdir()
        with closing(sqlite3.connect(original / CHECKPOINT_FILE)) as db, db:
            db.execute(CHECKPOINT_SCHEMA)
            db.execute("INSERT INTO collection_checkpoint VALUES (?, ?)",
                       ("billing-v1:validated", "2026-01-14T16:31:00.000000+00:00"))
        bundle = export_state(original)
        self.assertEqual(bundle, self.make_bundle())
        target = self.root / "target"
        raw = bundle.model_dump_json().encode()
        self.assertEqual(restore_state(target, raw), {"status": "restored", "source_count": 1})
        self.assertEqual(restore_state(target, raw), {"status": "already_current", "source_count": 1})
        self.assertEqual(export_state(target), bundle)

    def test_rejects_modified_or_different_progress(self):
        original = self.make_bundle()
        tampered = original.model_dump()
        tampered["snapshot"]["checkpoints"][0]["through_utc"] = "2026-01-14T19:31:00.000000+00:00"
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            restore_state(self.root, json.dumps(tampered).encode())
        self.assertFalse((self.root / CHECKPOINT_FILE).exists())

        restore_state(self.root, original.model_dump_json().encode())
        with self.assertRaisesRegex(ValueError, "existing collection checkpoints differ"):
            restore_state(self.root, self.make_bundle(hour=17).model_dump_json().encode())
        self.assertEqual(export_state(self.root), original)

    def test_duplicate_source_entries_are_rejected(self):
        snapshot = self.make_bundle().snapshot
        snapshot.checkpoints *= 2
        bundle = CheckpointBundle(snapshot=snapshot, sha256=checksum(snapshot))
        with self.assertRaisesRegex(ValueError, "duplicate source IDs"):
            restore_state(self.root, bundle.model_dump_json().encode())
        self.assertFalse((self.root / CHECKPOINT_FILE).exists())


if __name__ == "__main__":
    unittest.main()
