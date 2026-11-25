"""Stable identifiers for checks that can block a collection batch."""

from enum import StrEnum


class QualityRule(StrEnum):
    BILLING_JSON = "billing_json"
    BILLING_SCHEMA = "billing_schema"
    BILLING_TIMESTAMP_UTC = "billing_timestamp_utc"
    BILLING_TIME_ORDER = "billing_time_order"
    BILLING_CURRENCY = "billing_currency"
    BILLING_AMOUNT_BALANCE = "billing_amount_balance"
    BILLING_ENERGY_WH = "billing_energy_wh"
    BILLING_TOU_INTERVAL = "billing_tou_interval"
    BILLING_TOU_TOTALS = "billing_tou_totals"
    BILLING_TOU_INTEGER_RANGE = "billing_tou_integer_range"
    BILLING_INTEGER_RANGE = "billing_integer_range"
    BILLING_SOURCE_POSITION = "billing_source_position"
    ORDER_VERSION_CONFLICT = "order_version_conflict"
    DEMO_ORDER_CONTRACT = "demo_order_contract"
