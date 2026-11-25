"""Stable identifiers and metadata for collection quality rules."""

from dataclasses import dataclass
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


@dataclass(frozen=True)
class RuleDefinition:
    rule_id: str
    dimension: str
    severity: str
    checks: str
    owner_role: str
    action: str


def collection_rule(rule: QualityRule, dimension: str, checks: str, action: str) -> RuleDefinition:
    return RuleDefinition(rule, dimension, "blocking", checks, "数据工程", action)


COLLECTION_RULES = (
    collection_rule(QualityRule.BILLING_JSON, "有效性", "V1 载荷可解析为 JSON", "修复源端序列化后重采"),
    collection_rule(QualityRule.BILLING_SCHEMA, "完整性", "V1 字段符合契约及必填项要求", "核对上游字段变化与契约版本后重采"),
    collection_rule(QualityRule.BILLING_TIMESTAMP_UTC, "有效性", "V1 时间戳有效且为 UTC", "修正源端时间格式或时区后重采"),
    collection_rule(QualityRule.BILLING_TIME_ORDER, "一致性", "结束晚于开始且事件时间等于结束时间", "核对订单生命周期时间后重采"),
    collection_rule(QualityRule.BILLING_CURRENCY, "有效性", "订单使用人民币", "核实源币种配置后重采"),
    collection_rule(QualityRule.BILLING_AMOUNT_BALANCE, "准确性", "订单总额与费用组件勾稽", "核对源端费用及抵扣明细后重采"),
    collection_rule(QualityRule.BILLING_ENERGY_WH, "有效性", "电量符合整数 Wh 精度和支持范围", "核对计量单位和精度后重采"),
    collection_rule(QualityRule.BILLING_TOU_INTERVAL, "一致性", "分时区间有效、位于订单内且无重叠", "核对各计费时段边界后重采"),
    collection_rule(QualityRule.BILLING_TOU_TOTALS, "准确性", "分时电量和两项费用与订单汇总一致", "核对分时计费明细后重采"),
    collection_rule(QualityRule.BILLING_TOU_INTEGER_RANGE, "有效性", "分时费率和金额符合整数分范围", "核对分时费率和金额单位后重采"),
    collection_rule(QualityRule.BILLING_INTEGER_RANGE, "有效性", "订单版本和金额符合整数存储范围", "核对源端版本号和金额单位后重采"),
    collection_rule(QualityRule.BILLING_SOURCE_POSITION, "一致性", "V1 源游标与订单键、版本及更新时间相符", "修复索引列与载荷的映射后重采"),
    collection_rule(QualityRule.ORDER_VERSION_CONFLICT, "唯一性", "相同订单版本的业务内容一致", "核对同版本差异并修正来源后重采"),
    collection_rule(QualityRule.DEMO_ORDER_CONTRACT, "完整性", "演示订单符合 0.1 契约", "修复演示样本后重采"),
)
