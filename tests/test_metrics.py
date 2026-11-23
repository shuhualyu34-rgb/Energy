import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.metrics import metrics_text, record_spark_sink
from cdp.store import Store


class MetricsTest(unittest.TestCase):
    def test_empty_state_and_monotonic_sink_report(self):
        with TemporaryDirectory() as directory:
            store = Store(Path(directory))
            store.initialize()
            initial = metrics_text(store)
            self.assertIn("cdp_kafka_pending_events 0\n", initial)
            self.assertNotIn("cdp_stream_watermark_timestamp_seconds", initial)
            self.assertNotIn("cdp_spark_sink_last_batch_id", initial)
            record_spark_sink(store, "/dev/private-checkpoint-path", 3, {"late": 2, "duplicate": 1})
            record_spark_sink(store, "/dev/private-checkpoint-path", 2, {"applied": 99})
            result = metrics_text(store)
            self.assertIn("cdp_spark_sink_last_batch_id 3\n", result)
            self.assertIn('cdp_spark_sink_last_batch_events{outcome="late"} 2\n', result)
            self.assertIn('cdp_spark_sink_last_batch_events{outcome="applied"} 0\n', result)
            self.assertNotIn("private-checkpoint-path", result)


if __name__ == "__main__":
    unittest.main()
