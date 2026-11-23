"""Export or restore the incremental collection checkpoint."""

import argparse
import hashlib
import hmac
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .config import configured_store

CHECKPOINT_FILE = "collection.sqlite3"
CHECKPOINT_SCHEMA = """
CREATE TABLE IF NOT EXISTS collection_checkpoint (
    source_id TEXT PRIMARY KEY, through_utc TEXT NOT NULL
)
"""
SELECT_CHECKPOINTS = "SELECT source_id, through_utc FROM collection_checkpoint ORDER BY source_id"


class SourceCheckpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    source_id: Annotated[str, Field(pattern=r"^(billing-v1:)?[A-Za-z0-9_-]{1,64}$")]
    through_utc: str

    @field_validator("through_utc")
    @classmethod
    def validate_cutoff(cls, value: str) -> str:
        at = datetime.fromisoformat(value)
        if at.utcoffset() is None or at.astimezone(timezone.utc).isoformat(timespec="microseconds") != value:
            raise ValueError("checkpoint cutoff must use canonical UTC")
        return value


class Snapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    format_version: Literal[1]
    checkpoints: list[SourceCheckpoint]


class CheckpointBundle(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    snapshot: Snapshot
    sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


def checksum(snapshot: Snapshot) -> str:
    body = json.dumps(snapshot.model_dump(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


def export_state(directory: Path) -> CheckpointBundle:
    uri = (directory / CHECKPOINT_FILE).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        rows = connection.execute(SELECT_CHECKPOINTS).fetchall()
    snapshot = Snapshot(format_version=1, checkpoints=[
        SourceCheckpoint(source_id=key, through_utc=cutoff) for key, cutoff in rows
    ])
    return CheckpointBundle(snapshot=snapshot, sha256=checksum(snapshot))


def restore_state(directory: Path, raw: bytes) -> dict:
    bundle = CheckpointBundle.model_validate_json(raw)
    if not hmac.compare_digest(checksum(bundle.snapshot), bundle.sha256):
        raise ValueError("checkpoint checksum mismatch")
    checkpoints = sorted((r.source_id, r.through_utc) for r in bundle.snapshot.checkpoints)
    if len({key for key, _ in checkpoints}) != len(checkpoints):
        raise ValueError("checkpoint snapshot contains duplicate source IDs")

    directory.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(directory / CHECKPOINT_FILE, timeout=1)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(CHECKPOINT_SCHEMA)
        current = connection.execute(SELECT_CHECKPOINTS).fetchall()
        if current and current != checkpoints:
            raise ValueError("existing collection checkpoints differ; restore into an empty target")
        if not current:
            connection.executemany("INSERT INTO collection_checkpoint VALUES (?, ?)", checkpoints)
    return {"status": "already_current" if current else "restored", "source_count": len(checkpoints)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("export", help="Write a checksummed JSON snapshot to stdout")
    restore = commands.add_parser("restore", help="Restore a verified snapshot into the configured data directory")
    restore.add_argument("file", type=Path)
    args = parser.parse_args()
    directory = configured_store().directory
    try:
        if args.command == "export":
            report = export_state(directory).model_dump()
        else:
            report = restore_state(directory, args.file.read_bytes())
    except ValidationError:
        parser.exit(1, "invalid collection checkpoint format\n")
    except (ValueError, OSError, sqlite3.Error) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
