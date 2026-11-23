"""Back up and restore checksummed V1 billing archives using an S3 bucket."""

import argparse
import hashlib
import json
import os
from collections import Counter

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from .billing_event import BILLING_ARCHIVE
from .billing_ledger import billing_history
from .config import configured_store
from .store import Store

BATCH_SIZE = 500
SUCCESS = {"uploaded", "already_remote", "restored", "already_local"}


class RemoteChecksumMismatch(ValueError):
    pass


class S3Archive:
    def __init__(self, client, bucket: str, prefix: str):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        if not self.prefix or any(part in {"", ".", ".."} for part in self.prefix.split("/")):
            raise ValueError("provide a nonempty archive prefix")

    def key(self, digest: str) -> str:
        return f"{self.prefix}/ods/{BILLING_ARCHIVE}/{digest}.json"

    def read(self, digest: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=self.key(digest))
        body = response["Body"]
        try:
            data = body.read()
        finally:
            body.close()
        if hashlib.sha256(data).hexdigest() != digest:
            raise RemoteChecksumMismatch("remote archive checksum mismatch")
        return data

    def backup(self, digest: str, data: bytes) -> str:
        try:
            self.client.put_object(
                Bucket=self.bucket, Key=self.key(digest), Body=data,
                ContentType="application/json", IfNoneMatch="*",
            )
        except ClientError as error:
            if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 412:
                raise
            outcome = "already_remote"
        else:
            outcome = "uploaded"
        self.read(digest)
        return outcome


def transfer_version(store: Store, remote: S3Archive, digest: str | None, action: str) -> str:
    if not digest:
        return "legacy_no_archive"
    try:
        local = store.read_archive(digest, namespace=BILLING_ARCHIVE)
    except ValueError:
        return "local_checksum_mismatch"
    except FileNotFoundError:
        if action == "backup":
            return "local_archive_missing"
        data = remote.read(digest)
        store.archive(digest, data, namespace=BILLING_ARCHIVE)
        return "restored"
    return remote.backup(digest, local) if action == "backup" else "already_local"


def transfer_order(store: Store, remote: S3Archive, order_id: str, action: str) -> dict:
    if action not in {"backup", "restore"}:
        raise ValueError("unknown archive action")
    results = []
    for version in billing_history(store, order_id):
        digest = version["archive_sha256"]
        results.append({
            "version": version["version"], "updated_at_utc": version["updated_at_utc"],
            "archive_sha256": digest, "result": transfer_version(store, remote, digest, action),
        })
    return {"order_id": order_id, "action": action, "versions": results}


def transfer_all(store: Store, remote: S3Archive, action: str) -> dict:
    store.initialize()
    outcomes = Counter()
    failure_examples = []
    b = store.bind
    cursor = ("", 0, "")
    while True:
        with store.connect(readonly=True) as connection:
            rows = connection.execute(f"""
                SELECT h.order_id, h.version, h.updated_at_utc, a.archive_sha256
                FROM dwd_billing_order_v1_history AS h
                LEFT JOIN billing_order_archive AS a
                    ON (a.order_id, a.version, a.updated_at_utc) =
                       (h.order_id, h.version, h.updated_at_utc)
                WHERE (h.order_id, h.version, h.updated_at_utc) > ({b}, {b}, {b})
                ORDER BY h.order_id, h.version, h.updated_at_utc
                LIMIT {b}
            """, (*cursor, BATCH_SIZE)).fetchall()
        if not rows:
            break

        for row in rows:
            try:
                result = transfer_version(store, remote, row["archive_sha256"], action)
            except RemoteChecksumMismatch:
                result = "remote_checksum_mismatch"
            except ClientError as error:
                status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                result = "remote_archive_missing" if status == 404 else "remote_transfer_failed"
            except BotoCoreError:
                result = "remote_transfer_failed"
            except OSError:
                result = "local_archive_unreadable"
            outcomes[result] += 1
            if result not in SUCCESS and len(failure_examples) < 20:
                failure_examples.append(dict(row) | {"result": result})
        last = rows[-1]
        cursor = (last["order_id"], last["version"], last["updated_at_utc"])

    return {
        "action": action, "version_count": sum(outcomes.values()), "results": dict(outcomes),
        "failure_examples": failure_examples,
    }



def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["backup", "restore"])
    parser.add_argument("order_id", nargs="?")
    parser.add_argument("--all", action="store_true", help="Transfer every V1 history version")
    parser.add_argument("--bucket", default=os.getenv("CDP_S3_BUCKET"))
    parser.add_argument("--prefix", default=os.getenv("CDP_S3_ARCHIVE_PREFIX", "cdp1"))
    args = parser.parse_args()
    if bool(args.order_id) == args.all:
        parser.error("provide an order ID or --all")
    if not args.bucket:
        parser.error("provide --bucket or CDP_S3_BUCKET")
    try:
        endpoint = os.getenv("CDP_S3_ENDPOINT_URL")
        client = boto3.client(
            "s3", endpoint_url=endpoint,
            config=Config(connect_timeout=5, read_timeout=20,
                          s3={"addressing_style": "path"} if endpoint else None),
        )
        store = configured_store()
        remote = S3Archive(client, args.bucket, args.prefix)
        if args.all:
            report = transfer_all(store, remote, args.action)
        else:
            report = transfer_order(store, remote, args.order_id, args.action)
    except (BotoCoreError, ClientError) as error:
        parser.exit(1, f"archive transfer failed ({type(error).__name__})\n")
    except (OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(report, indent=2))
    if args.all:
        complete = report["version_count"] > 0 and all(result in SUCCESS for result in report["results"])
    else:
        complete = bool(report["versions"]) and all(row["result"] in SUCCESS for row in report["versions"])
    return 0 if complete else 2


if __name__ == "__main__":
    raise SystemExit(main())
