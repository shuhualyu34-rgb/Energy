"""Validate and map an approved billing V1 payload without writing to DWD."""

import argparse
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from jsonschema import Draft7Validator
from pydantic import ValidationError

from .contract import Order
from .quality_rules import QualityRule

SCHEMA = json.loads(Path(__file__).with_name("billing_order_schema.json").read_text())
VALIDATOR = Draft7Validator(SCHEMA, format_checker=Draft7Validator.FORMAT_CHECKER)


class BillingContractError(ValueError):
    def __init__(self, message: str, rule_id: QualityRule | None = None):
        super().__init__(message)
        self.rule_id = rule_id


def utc(value: str) -> datetime:
    try:
        timestamp = datetime.fromisoformat(value)
    except ValueError:
        raise BillingContractError(
            "invalid billing timestamp", QualityRule.BILLING_TIMESTAMP_UTC,
        ) from None
    if timestamp.utcoffset() != timedelta(0):
        raise BillingContractError(
            "billing timestamps must be UTC", QualityRule.BILLING_TIMESTAMP_UTC,
        )
    return timestamp.astimezone(timezone.utc)


def energy_wh(value: Decimal | int) -> int:
    kwh = Decimal(value)
    if kwh >= Decimal("100000000") or kwh != kwh.quantize(Decimal("0.001")):
        raise BillingContractError(
            "energy is outside the supported Wh precision or range", QualityRule.BILLING_ENERGY_WH,
        )
    return int(kwh * 1000)


def validate_billing_order(raw: bytes) -> dict:
    try:
        row = json.loads(raw, parse_float=Decimal, parse_constant=reject_constant)
    except (ValueError, UnicodeError):
        raise BillingContractError(
            "invalid billing JSON", QualityRule.BILLING_JSON,
        ) from None
    if not VALIDATOR.is_valid(row):
        raise BillingContractError(
            "payload failed billing V1 schema validation", QualityRule.BILLING_SCHEMA,
        )

    start, end = utc(row["start_time"]), utc(row["end_time"])
    utc(row["updated_at"])
    if start >= end or utc(row.get("event_time", row["end_time"])) != end:
        raise BillingContractError(
            "inconsistent order or event timestamps", QualityRule.BILLING_TIME_ORDER,
        )
    if row.get("currency", "CNY") != "CNY":
        raise BillingContractError(
            "only CNY is supported", QualityRule.BILLING_CURRENCY,
        )
    expected = (
        row["elec_fee_cents"] + row["service_fee_cents"]
        + row.get("penalty_fee_cents", 0) - row.get("discount_amount_cents", 0)
    )
    if row["total_amount_cents"] != expected:
        raise BillingContractError(
            "payment components do not balance", QualityRule.BILLING_AMOUNT_BALANCE,
        )
    wh = energy_wh(row["total_kwh"])

    slices = row.get("tou_slices")
    if slices:
        previous_end = start
        for part in sorted(slices, key=lambda item: utc(item["start_time"])):
            begin, finish = utc(part["start_time"]), utc(part["end_time"])
            if begin < previous_end or finish <= begin or finish > end:
                raise BillingContractError(
                    "invalid or overlapping TOU slices", QualityRule.BILLING_TOU_INTERVAL,
                )
            previous_end = finish
        totals = (
            sum(energy_wh(part["kwh"]) for part in slices),
            sum(part["elec_fee_cents"] for part in slices),
            sum(part["service_fee_cents"] for part in slices),
        )
        if totals != (wh, row["elec_fee_cents"], row["service_fee_cents"]):
            raise BillingContractError(
                "TOU totals do not reconcile with the order", QualityRule.BILLING_TOU_TOTALS,
            )

    return row


def map_billing_order(raw: bytes) -> tuple[str, Order | None]:
    row = validate_billing_order(raw)
    status = row["status"]
    if status != "COMPLETED":
        return status, None
    try:
        order = Order(
            schema_version="0.1", order_id=row["order_id"], station_id=row["station_id"],
            version=row["version"], started_at=row["start_time"], ended_at=row["end_time"],
            energy_wh=energy_wh(row["total_kwh"]), electricity_fee_cents=row["elec_fee_cents"],
            service_fee_cents=row["service_fee_cents"],
        )
    except ValidationError:
        raise BillingContractError("order exceeds the target snapshot constraints") from None
    return status, order


def reject_constant(_value: str) -> None:
    raise ValueError("nonfinite JSON number")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path)
    args = parser.parse_args()
    try:
        status, order = map_billing_order(args.file.read_bytes())
    except (OSError, BillingContractError) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps({"status": status, "projection_available": order is not None}))


if __name__ == "__main__":
    main()
