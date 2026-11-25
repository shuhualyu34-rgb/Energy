"""Import validated lifecycle snapshots into the separate V1 development ledger."""

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from .billing import BillingContractError, energy_wh, utc, validate_billing_order
from .billing_event import BILLING_ARCHIVE, billing_payload
from .config import configured_store
from .quality_rules import QualityRule
from .store import Store


def normalized_slices(row: dict) -> str:
    structured = []
    for part in sorted(row.get("tou_slices") or [], key=lambda item: utc(item["start_time"])):
        fields = {
            key: part[key] for key in (
                "tou_type", "elec_fee_cents", "service_fee_cents",
                "elec_rate_cents", "service_rate_cents",
            ) if key in part
        }
        if any(type(value) is not int or value > 2**63 - 1 for key, value in fields.items() if key != "tou_type"):
            raise BillingContractError(
                "TOU fees and rates must be expressed as BIGINT-compatible integers", QualityRule.BILLING_TOU_INTEGER_RANGE,
            )
        structured.append({
            **fields,
            "start_time": utc(part["start_time"]).isoformat(),
            "end_time": utc(part["end_time"]).isoformat(),
            "energy_wh": energy_wh(part["kwh"]),
        })
    return json.dumps(structured, sort_keys=True)


def ingest_billing(
    store: Store, raw: bytes, *, source_position: tuple[str, str, int] | None = None,
) -> str:
    row = validate_billing_order(raw)
    order_id, version, status = row["order_id"], row["version"], row["status"]
    integers = (
        "total_amount_cents", "elec_fee_cents", "service_fee_cents",
        "penalty_fee_cents", "discount_amount_cents",
    )
    amounts = {key: row.get(key, 0) for key in integers}
    if any(type(value) is not int or value > 2**63 - 1 for value in (*amounts.values(), version)):
        raise BillingContractError(
            "version and money must be expressed as BIGINT-compatible integers", QualityRule.BILLING_INTEGER_RANGE,
        )

    updated = utc(row["updated_at"]).isoformat(timespec="microseconds")
    if source_position is not None and source_position != (updated, order_id, version):
        raise BillingContractError(
            "source cursor does not match the billing payload", QualityRule.BILLING_SOURCE_POSITION,
        )
    snapshot = json.dumps({
        "station_id": row["station_id"],
        "gun_id": row["gun_id"],
        "start_time": utc(row["start_time"]).isoformat(),
        "end_time": utc(row["end_time"]).isoformat(),
        "energy_wh": energy_wh(row["total_kwh"]),
        "currency": "CNY",
        **amounts,
    }, sort_keys=True)

    slices = normalized_slices(row)
    store.initialize()
    position = (order_id, version, updated)
    outcome = write_snapshot(store, position, status, snapshot, slices)
    if outcome != "stale":
        # Only record availability after the history write has committed.
        mark_available(store, position)
    return outcome


def mark_available(store: Store, position: tuple[str, int, str]) -> None:
    bind = store.bind
    available = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    with store.connect() as connection:
        connection.execute(f"""
            INSERT INTO billing_feature_availability
                (order_id, version, updated_at_utc, available_at_utc)
            VALUES ({bind}, {bind}, {bind}, {bind})
            ON CONFLICT (order_id, version, updated_at_utc) DO NOTHING
        """, (*position, available))


def write_snapshot(
    store: Store, position: tuple[str, int, str], status: str, snapshot: str, slices: str,
) -> str:
    order_id, version, updated = position
    bind = store.bind
    with store.write_order(order_id) as connection:
        existing = connection.execute(f"""
            SELECT version, updated_at_utc
            FROM dwd_billing_order_v1 WHERE order_id = {bind}
        """, (order_id,)).fetchone()
        if existing:
            previous = (existing["version"], existing["updated_at_utc"])
            incoming = (version, updated)
            if incoming < previous:
                return "stale"
            if incoming == previous:
                accepted = connection.execute(f"""
                    SELECT status, snapshot, tou_slices FROM dwd_billing_order_v1_history
                    WHERE order_id = {bind} AND version = {bind} AND updated_at_utc = {bind}
                """, (order_id, version, updated)).fetchone()
                if (status, snapshot) != (accepted["status"], accepted["snapshot"]):
                    raise BillingContractError(
                        "conflicting snapshots at the same version and update time", QualityRule.ORDER_VERSION_CONFLICT,
                    )
                if accepted["tou_slices"] is not None and slices != accepted["tou_slices"]:
                    raise BillingContractError(
                        "conflicting TOU slices at the same version and update time", QualityRule.ORDER_VERSION_CONFLICT,
                    )
                return "duplicate"

        archived = billing_payload(position, status, snapshot, slices)
        digest = hashlib.sha256(archived).hexdigest()
        store.archive(digest, archived, namespace=BILLING_ARCHIVE)
        event_values = ", ".join([bind] * 6)
        connection.execute(f"""
            INSERT INTO dwd_billing_order_v1_history
                (order_id, version, updated_at_utc, status, snapshot, tou_slices)
            VALUES ({event_values})
        """, (order_id, version, updated, status, snapshot, slices))
        connection.execute(f"""
            INSERT INTO billing_order_archive (order_id, version, updated_at_utc, archive_sha256)
            VALUES ({bind}, {bind}, {bind}, {bind})
        """, (*position, digest))
        values = ", ".join([bind] * 5)
        connection.execute(f"""
            INSERT INTO dwd_billing_order_v1 (order_id, version, updated_at_utc, status, snapshot)
            VALUES ({values})
            ON CONFLICT(order_id) DO UPDATE SET version = excluded.version,
                updated_at_utc = excluded.updated_at_utc, status = excluded.status,
                snapshot = excluded.snapshot
        """, (order_id, version, updated, status, snapshot))
    return "updated" if existing else "inserted"


def billing_history(store: Store, order_id: str) -> list[dict]:
    store.initialize()
    with store.connect(readonly=True) as connection:
        rows = connection.execute(f"""
            SELECT h.version, h.updated_at_utc, h.status, h.snapshot, h.tou_slices, a.archive_sha256
            FROM dwd_billing_order_v1_history AS h
            LEFT JOIN billing_order_archive AS a
                ON (a.order_id, a.version, a.updated_at_utc) = (h.order_id, h.version, h.updated_at_utc)
            WHERE h.order_id = {store.bind}
            ORDER BY h.version, h.updated_at_utc
        """, (order_id,)).fetchall()
    return [
        dict(row) | {
            "snapshot": json.loads(row["snapshot"]),
            "tou_slices": json.loads(row["tou_slices"]) if row["tou_slices"] is not None else None,
        }
        for row in rows
    ]


def verify_archive(store: Store, order_id: str) -> dict:
    versions = []
    for row in billing_history(store, order_id):
        digest = row["archive_sha256"]
        outcome = "legacy_no_archive"
        if digest is not None:
            try:
                store.read_archive(digest, namespace=BILLING_ARCHIVE)
            except FileNotFoundError:
                outcome = "archive_missing"
            except ValueError:
                outcome = "checksum_mismatch"
            except OSError:
                outcome = "archive_unreadable"
            else:
                outcome = "verified"
        versions.append({
            "version": row["version"],
            "updated_at_utc": row["updated_at_utc"],
            "archive_sha256": digest,
            "result": outcome,
        })
    return {"order_id": order_id, "versions": versions}



def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", type=Path, nargs="*")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--history", metavar="ORDER_ID")
    mode.add_argument("--verify-archive", metavar="ORDER_ID")
    args = parser.parse_args()
    if bool(args.files) == bool(args.history or args.verify_archive):
        parser.error("provide input files, --history or --verify-archive")
    store = configured_store()
    if args.history:
        print(json.dumps(billing_history(store, args.history), indent=2))
        return
    if args.verify_archive:
        result = verify_archive(store, args.verify_archive)
        print(json.dumps(result, indent=2))
        if not result["versions"] or any(row["result"] != "verified" for row in result["versions"]):
            raise SystemExit(2)
        return
    for path in args.files:
        try:
            print(ingest_billing(store, path.read_bytes()))
        except (OSError, ValueError) as error:
            parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
