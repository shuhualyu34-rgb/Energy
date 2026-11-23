import unittest

from cdp.billing_event import TOPIC
from cdp.checkpoint import recovery_checkpoint
from cdp.consume import NORMAL_GROUP, consumer_route


class ConsumerRouteTest(unittest.TestCase):
    def test_normal_group_preserves_existing_behavior(self):
        self.assertEqual(consumer_route(None, None, None), (TOPIC, NORMAL_GROUP))
        self.assertEqual(consumer_route("manual-demo", None, None), (TOPIC, "manual-demo"))

    def test_recovery_groups_are_isolated_by_run_and_target(self):
        topic, group = consumer_route(None, "drill-01", "shadow-db")
        self.assertEqual(topic, "billing.orders.v1.recovery.drill-01")
        self.assertEqual(group, "cdp-billing-recovery.drill-01.shadow-db")
        self.assertNotEqual(consumer_route(None, "a-b", "c")[1], consumer_route(None, "a", "b-c")[1])

    def test_spark_checkpoints_are_scoped_and_do_not_reuse_the_normal_path(self):
        normal = "/data/billing-stream-v1"
        path = recovery_checkpoint(normal, "cluster-a", "drill", "restored-db")
        self.assertEqual(path, "/data/billing-stream-v1.recovery/cluster-a.drill.restored-db")
        self.assertEqual(recovery_checkpoint(normal + "/", "cluster-a", "drill", "restored-db"), path)
        self.assertNotEqual(recovery_checkpoint(normal, "cluster-b", "drill", "restored-db"), path)
        self.assertNotEqual(recovery_checkpoint(normal, "cluster-a", "drill", "other-db"), path)
        with self.assertRaises(ValueError):
            recovery_checkpoint(normal, "../cluster", "drill", "restored-db")
        with self.assertRaises(ValueError):
            recovery_checkpoint("/", "cluster-a", "drill", "restored-db")

    def test_rejects_partial_and_conflicting_recovery_routes(self):
        for params in [
            (None, "drill", None),
            (None, None, "target"),
            ("manual", "drill", "target"),
            ("cdp-billing-recovery.drill.target", None, None),
            (None, "drill", "bad.target"),
        ]:
            with self.subTest(params=params), self.assertRaises(ValueError):
                consumer_route(*params)


if __name__ == "__main__":
    unittest.main()
