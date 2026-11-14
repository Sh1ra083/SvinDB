#!/usr/bin/env python3
"""Benchmark control-plane regression tests; no server processes are started."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import bench_vs_es as bench


class RefreshPolicyTests(unittest.TestCase):
    def test_both_ingest_policies_are_explicit(self):
        self.assertEqual(bench.refresh_settings("es", "disabled"), {"refresh_interval": "-1"})
        self.assertEqual(bench.refresh_settings("es", "default"), {"refresh_interval": "1s"})
        self.assertEqual(bench.refresh_settings("svindb", "disabled"),
                         {"refresh_interval_ms": 0, "live_max_docs": 0})
        self.assertEqual(bench.refresh_settings("svindb", "default"),
                         {"refresh_interval_ms": 1000, "live_max_docs": 10000})
        with self.assertRaises(ValueError):
            bench.refresh_settings("es", "typo")

    def test_restores_and_verifies_before_readiness(self):
        for engine in ("es", "svindb"):
            effective = bench.refresh_settings(engine, "default")
            readback = {"bench": {"settings": {"index": effective}}} if engine == "es" else effective
            actions = ["_flush?wait_if_ongoing=true", "_refresh"] if engine == "es" else ["_flush"]
            http = Mock()
            http.call.side_effect = [(200, {}), (200, readback)] + [(200, {})] * len(actions)
            self.assertEqual(bench.finish_ingest(engine, http, "bench"), effective)
            self.assertEqual(http.call.call_args_list[0].args,
                             ("PUT", "/bench/_settings", effective))
            self.assertEqual(http.call.call_args_list[1].args, ("GET", "/bench/_settings"))
            self.assertEqual([c.args for c in http.call.call_args_list[2:]],
                             [("POST", "/bench/" + a) for a in actions])

    def test_pending_svindb_flush_is_waited_and_has_a_deadline(self):
        effective = bench.refresh_settings("svindb", "default")
        http = Mock()
        http.call.side_effect = [(200, {}), (200, effective), (202, {}), (200, {})]
        with patch.object(bench.time, "sleep") as sleep:
            bench.finish_ingest("svindb", http, "bench")
            sleep.assert_called_once_with(0.1)
        http.call.side_effect = [(200, {}), (200, effective), (202, {})]
        with self.assertRaisesRegex(RuntimeError, "readiness failed"):
            bench.finish_ingest("svindb", http, "bench", timeout=0)

    def test_inaccurate_readback_and_failed_readiness_fail_closed(self):
        http = Mock()
        http.call.side_effect = [(200, {}), (200, {"refresh_interval_ms": 1000, "live_max_docs": 10000})]
        with self.assertRaisesRegex(RuntimeError, "expected 0"):
            bench.configure_refresh("svindb", http, "bench", "disabled")
        http.call.side_effect = [(200, {}), (200, bench.refresh_settings("svindb", "default")),
                                 (500, {"error": "flush failed"})]
        with self.assertRaisesRegex(RuntimeError, "readiness failed"):
            bench.finish_ingest("svindb", http, "bench")


class CorpusTests(unittest.TestCase):
    def test_auto_id_changes_only_action_lines(self):
        with tempfile.TemporaryDirectory() as directory:
            explicit, automatic = [Path(directory) / name for name in ("explicit", "automatic")]
            bench.generate_corpus(explicit, 3, 42)
            bench.generate_corpus(automatic, 3, 42, auto_id=True)
            left, right = explicit.read_text().splitlines(), automatic.read_text().splitlines()
            self.assertEqual(left[1::2], right[1::2])
            self.assertTrue(all(json.loads(row) == {"index": {}} for row in right[::2]))
            self.assertEqual(json.loads(left[0]), {"index": {"_id": "d0"}})


if __name__ == "__main__":
    unittest.main()
