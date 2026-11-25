"""Additional D10 checks over persisted development data."""

from dataclasses import dataclass

from .quality_rules import RuleDefinition


@dataclass(frozen=True)
class IntegrityCheck:
    rule: RuleDefinition
    query: str


def check(rule_id: str, dimension: str, condition: str, action: str, query: str,
          *, severity: str = "warning") -> IntegrityCheck:
    return IntegrityCheck(RuleDefinition(rule_id, dimension, severity, condition, "数据工程", action), query)


INTEGRITY_CHECKS = (
    check("billing_current_without_history", "完整性", "当前订单缺少相同版本历史",
          "核查入账事务和历史迁移", """
        SELECT COUNT(*) FROM dwd_billing_order_v1 AS c
        WHERE NOT EXISTS (SELECT 1 FROM dwd_billing_order_v1_history AS h
                          WHERE h.order_id = c.order_id AND h.version = c.version
                            AND h.updated_at_utc = c.updated_at_utc)
    """),
    check("billing_current_history_mismatch", "一致性", "当前订单和对应历史的状态或快照不同",
          "比对源版本与历史后修复当前快照", """
        SELECT COUNT(*) FROM dwd_billing_order_v1 AS c
        JOIN dwd_billing_order_v1_history AS h
          ON h.order_id = c.order_id AND h.version = c.version AND h.updated_at_utc = c.updated_at_utc
        WHERE c.status <> h.status OR c.snapshot <> h.snapshot
    """),
    check("billing_history_ahead_of_current", "及时性", "历史包含比当前订单更新的版本",
          "核查订单更新是否完整提交", """
        SELECT COUNT(*) FROM dwd_billing_order_v1 AS c
        WHERE EXISTS (SELECT 1 FROM dwd_billing_order_v1_history AS h
                      WHERE h.order_id = c.order_id AND (h.version > c.version
                        OR (h.version = c.version AND h.updated_at_utc > c.updated_at_utc)))
    """),
    check("billing_history_without_current", "完整性", "历史订单缺少当前记录",
          "核查当前明细是否漏迁移", """
        SELECT COUNT(DISTINCT h.order_id) FROM dwd_billing_order_v1_history AS h
        WHERE NOT EXISTS (SELECT 1 FROM dwd_billing_order_v1 AS c WHERE c.order_id = h.order_id)
    """),
    check("billing_archive_without_history", "一致性", "归档索引缺少对应版本历史",
          "比对归档和数据库恢复批次", """
        SELECT COUNT(*) FROM billing_order_archive AS a
        WHERE NOT EXISTS (SELECT 1 FROM dwd_billing_order_v1_history AS h
                          WHERE (h.order_id, h.version, h.updated_at_utc) =
                                (a.order_id, a.version, a.updated_at_utc))
    """),
    check("billing_archive_digest_length", "有效性", "归档 SHA-256 摘要长度异常",
          "用归档核验工具核查索引与文件", """
        SELECT COUNT(*) FROM billing_order_archive WHERE LENGTH(archive_sha256) <> 64
    """),
    check("billing_history_without_archive", "完整性", "历史版本缺少归档索引",
          "区分旧库迁移记录与新版本漏归档", """
        SELECT COUNT(*) FROM dwd_billing_order_v1_history AS h
        WHERE NOT EXISTS (SELECT 1 FROM billing_order_archive AS a
                          WHERE (a.order_id, a.version, a.updated_at_utc) =
                                (h.order_id, h.version, h.updated_at_utc))
    """, severity="observation"),
    check("billing_feature_without_history", "一致性", "特征可用时间缺少对应版本历史",
          "检查特征证据与历史迁移是否同步", """
        SELECT COUNT(*) FROM billing_feature_availability AS f
        WHERE NOT EXISTS (SELECT 1 FROM dwd_billing_order_v1_history AS h
                          WHERE (h.order_id, h.version, h.updated_at_utc) =
                                (f.order_id, f.version, f.updated_at_utc))
    """),
    check("billing_feature_before_source_update", "及时性", "特征采集时间早于源更新时间",
          "检查源系统与采集系统的时钟偏差", """
        SELECT COUNT(*) FROM billing_feature_availability WHERE available_at_utc < updated_at_utc
    """, severity="observation"),
    check("billing_history_without_feature_time", "完整性", "历史版本缺少特征可用时间",
          "区分旧库证据缺失和新版本采集未完成", """
        SELECT COUNT(*) FROM dwd_billing_order_v1_history AS h
        WHERE NOT EXISTS (SELECT 1 FROM billing_feature_availability AS f
                          WHERE (f.order_id, f.version, f.updated_at_utc) =
                                (h.order_id, h.version, h.updated_at_utc))
    """, severity="observation"),
    check("kafka_delivery_without_history", "一致性", "Kafka 确认记录缺少对应版本历史",
          "比对投递记录和恢复批次", """
        SELECT COUNT(*) FROM billing_kafka_delivery AS k
        WHERE NOT EXISTS (SELECT 1 FROM dwd_billing_order_v1_history AS h
                          WHERE (h.order_id, h.version, h.updated_at_utc) =
                                (k.order_id, k.version, k.updated_at_utc))
    """),
    check("kafka_delivery_duplicate_coordinates", "唯一性", "多条确认记录共用 Kafka 分区和偏移量",
          "核对是否复用了不同 Kafka 数据集的确认记录", """
        SELECT COUNT(*) FROM (
            SELECT kafka_partition, kafka_offset FROM billing_kafka_delivery
            GROUP BY kafka_partition, kafka_offset HAVING COUNT(*) > 1
        ) AS duplicate_coordinates
    """),
    check("stream_event_also_quarantined", "唯一性", "同一流事件同时出现在正常表和隔离表",
          "核对去重记录与隔离恢复顺序", """
        SELECT COUNT(*) FROM billing_stream_event AS e
        JOIN billing_stream_quarantine AS q ON q.event_id = e.event_id
    """),
    check("stream_late_without_event", "一致性", "迟到记录缺少对应流事件",
          "检查流状态恢复和补数批次", """
        SELECT COUNT(*) FROM billing_stream_late AS l
        WHERE NOT EXISTS (SELECT 1 FROM billing_stream_event AS e WHERE e.event_id = l.event_id)
    """),
    check("stream_late_not_before_watermark", "准确性", "迟到记录的事件时间未早于水位",
          "核对事件时间与判迟边界", """
        SELECT COUNT(*) FROM billing_stream_late WHERE event_time_utc >= watermark_utc
    """),
    check("stream_events_without_progress", "完整性", "已有流事件但缺少账单流进度",
          "核查水位状态是否遗漏恢复", """
        SELECT CASE WHEN EXISTS (SELECT 1 FROM billing_stream_event)
             AND NOT EXISTS (SELECT 1 FROM billing_stream_progress WHERE stream = 'billing-orders')
             THEN 1 ELSE 0 END
    """),
    check("stream_window_ahead_of_progress", "一致性", "账单事件窗口起点晚于流进度",
          "比对窗口状态与恢复水位", """
        SELECT COUNT(*) FROM billing_stream_event AS e
        JOIN billing_stream_progress AS p ON p.stream = 'billing-orders'
        WHERE e.window_start_utc > p.max_event_time_utc
    """),
    check("spark_negative_batch_id", "有效性", "Spark 已完成批次编号为负",
          "检查 checkpoint 数据及落库记录", """
        SELECT COUNT(*) FROM billing_spark_sink_progress WHERE batch_id < 0
    """),
    check("collection_status_reason_mismatch", "一致性", "采集状态与失败原因不匹配",
          "检查最近一次采集及质量状态是否一致", """
        SELECT COUNT(*) FROM billing_collection_quality
        WHERE (status = 'blocked' AND failure_code IS NULL)
           OR (status = 'ok' AND failure_code IS NOT NULL)
    """),
    check("collection_rejection_on_success", "一致性", "成功的采集检查关联了隔离证据",
          "核查检查记录和隔离证据的关联", """
        SELECT COUNT(*) FROM billing_collection_rejection AS r
        JOIN billing_collection_quality_history AS h ON h.check_id = r.check_id
        WHERE h.status = 'ok'
    """),
)


def integrity_counts(connection, bind: str) -> dict[str, int]:
    selects = [f"SELECT CAST({bind} AS TEXT) AS rule_id, ({item.query}) AS count" for item in INTEGRITY_CHECKS]
    rows = connection.execute(" UNION ALL ".join(selects), tuple(
        item.rule.rule_id for item in INTEGRITY_CHECKS
    )).fetchall()
    return {row["rule_id"]: row["count"] for row in rows}
