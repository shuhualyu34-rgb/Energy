import hashlib
import io
import sqlite3
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import boto3
from botocore.response import StreamingBody
from botocore.stub import Stubber

from cdp.archive_s3 import RemoteChecksumMismatch, S3Archive, transfer_all, transfer_version
from cdp.billing_event import BILLING_ARCHIVE
from cdp.collect import BILLING_V1, collect
from cdp.store import Store


class S3ArchiveTest(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = Store(Path(directory.name))
        self.body = b'{"schema_version":"1.0"}'
        self.digest = hashlib.sha256(self.body).hexdigest()
        client = boto3.client("s3", aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1")
        self.stub = Stubber(client)
        self.stub.activate()
        self.addCleanup(self.stub.deactivate)
        self.remote = S3Archive(client, "cdp-test-bucket", "test")
        self.locator = {"Bucket": "cdp-test-bucket", "Key": f"test/ods/billing-v1/{self.digest}.json"}

    def stub_read(self, content):
        self.stub.add_response("get_object", {"Body": StreamingBody(io.BytesIO(content), len(content))},
                               self.locator)

    def test_conditional_upload_and_repeated_backup(self):
        self.store.archive(self.digest, self.body, namespace=BILLING_ARCHIVE)
        request = self.locator | {"IfNoneMatch": "*", "Body": self.body, "ContentType": "application/json"}
        self.stub.add_response("put_object", {}, request)
        self.stub_read(self.body)
        self.assertEqual(transfer_version(self.store, self.remote, self.digest, "backup"), "uploaded")
        self.stub.add_client_error("put_object", "PreconditionFailed", http_status_code=412,
                                   expected_params=request)
        self.stub_read(self.body)
        self.assertEqual(transfer_version(self.store, self.remote, self.digest, "backup"), "already_remote")
        self.stub.assert_no_pending_responses()

    def test_corrupt_remote_does_not_restore_and_verified_retry_succeeds(self):
        self.stub_read(b"corrupt")
        with self.assertRaisesRegex(ValueError, "remote archive checksum mismatch"):
            transfer_version(self.store, self.remote, self.digest, "restore")
        self.assertFalse(self.store.archive_path(self.digest, BILLING_ARCHIVE).exists())
        self.stub_read(self.body)
        self.assertEqual(transfer_version(self.store, self.remote, self.digest, "restore"), "restored")
        self.assertEqual(self.store.read_archive(self.digest, namespace=BILLING_ARCHIVE), self.body)
        self.assertEqual(transfer_version(self.store, self.remote, self.digest, "restore"), "already_local")
        path = self.store.archive_path(self.digest, BILLING_ARCHIVE)
        path.write_bytes(b"damaged local copy")
        self.assertEqual(transfer_version(self.store, self.remote, self.digest, "restore"), "local_checksum_mismatch")
        self.assertEqual(path.read_bytes(), b"damaged local copy")
        self.stub.assert_no_pending_responses()


    def test_bulk_pages_preserve_partial_failure_report(self):
        source = self.store.directory / "fixture.sqlite3"
        with closing(sqlite3.connect(source)) as db, db:
            db.executescript(Path("samples/billing-v1-demo.sql").read_text())
        collect(source, self.store, "archive-suite", datetime(2026, 1, 15, tzinfo=timezone.utc), 1,
                source_format=BILLING_V1)
        with patch("cdp.archive_s3.BATCH_SIZE", 1), patch(
            "cdp.archive_s3.transfer_version", side_effect=[RemoteChecksumMismatch(), "restored"],
        ) as transfer:
            result = transfer_all(self.store, self.remote, "restore")

        self.assertEqual(transfer.call_count, 2)
        self.assertEqual(result["version_count"], 2)
        self.assertEqual(result["results"], {"remote_checksum_mismatch": 1, "restored": 1})
        failed, = result["failure_examples"]
        self.assertEqual(failed["order_id"], "ORD_20260115_SH001_0001")
        self.assertEqual(failed["version"], 1)


if __name__ == "__main__":
    unittest.main()
