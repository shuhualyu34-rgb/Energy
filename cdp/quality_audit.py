"""Read-only checks of billing publication and streaming quality indicators."""

from dataclasses import asdict
from datetime import datetime, timezone

from .metrics import operational_counts
from .quality_integrity import INTEGRITY_CHECKS, integrity_counts
from .quality_rules import COLLECTION_RULES, RuleDefinition
from .store import Store

AUDIT_RULES = (
    RuleDefinition("accepted_versions_pending_kafka", "及时性", "observation", "待 Kafka 确认的计费版本",
                   "数据工程", "结合待发布持续时间检查发布任务和 Kafka"),
    RuleDefinition("stream_late_pending", "及时性", "observation", "待回填的迟到事件",
                   "数据工程", "检查迟到日期及回填任务进度"),
    RuleDefinition("stream_late_applied", "及时性", "observation", "已回填的迟到事件",
                   "数据工程", "抽查已回填窗口及对账结果"),
    RuleDefinition("stream_time_quarantine", "有效性", "observation", "流时间隔离事件",
                   "数据工程", "核查设备时间和事件时间来源"),
    RuleDefinition("kafka_delivery_coordinates", "有效性", "warning", "Kafka 确认记录出现负分区或偏移量",
                   "数据工程", "暂停使用异常确认记录并核查 Kafka 投递凭证"),
    RuleDefinition("blocked_source_without_rule", "完整性", "observation", "受阻来源缺少细分规则编号",
                   "数据工程", "核查历史记录或用当前版本重采以获得具体规则"),
)


ALL_AUDIT_RULES = (*AUDIT_RULES, *(item.rule for item in INTEGRITY_CHECKS))


def rule_catalog() -> list[dict]:
    return [asdict(rule) for rule in (*COLLECTION_RULES, *ALL_AUDIT_RULES)]


def quality_audit(store: Store) -> dict:
    with store.connect(readonly=True) as connection:
        state = operational_counts(connection, store.bind)
        additional = connection.execute("""
            SELECT
                (SELECT COUNT(*) FROM billing_kafka_delivery
                 WHERE kafka_partition < 0 OR kafka_offset < 0) AS invalid_coordinates,
                (SELECT COUNT(*) FROM billing_collection_quality AS quality
                 LEFT JOIN billing_collection_quality_rule AS rule ON rule.source_key = quality.source_key
                 WHERE quality.status = 'blocked' AND rule.rule_id IS NULL) AS missing_rule
        """).fetchone()
        integrity = integrity_counts(connection, store.bind)

    counts = {
        "accepted_versions_pending_kafka": state["accepted_versions"] - state["acknowledged_versions"],
        "stream_late_pending": state["pending_late"],
        "stream_late_applied": state["applied_late"],
        "stream_time_quarantine": state["quarantined"],
        "kafka_delivery_coordinates": additional["invalid_coordinates"],
        "blocked_source_without_rule": additional["missing_rule"],
        **integrity,
    }
    return {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
        "results": [
            {"rule_id": rule.rule_id, "count": counts[rule.rule_id],
             "status": rule.severity if counts[rule.rule_id] else "clear"}
            for rule in ALL_AUDIT_RULES
        ],
    }
