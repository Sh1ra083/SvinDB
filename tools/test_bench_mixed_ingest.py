#!/usr/bin/env python3
"""Observable bulk/query/settlement oracles for mixed ingest benchmark."""
import json
import unittest
import time
from unittest.mock import Mock

import bench_mixed_ingest as bench
from bench_startup import BenchmarkError


class MixedOracleTests(unittest.TestCase):
    def test_whole_request_429_is_retryable_but_item_429_is_failure(self):
        self.assertFalse(bench.validate_bulk(429, {"reason": "busy"}, 1))
        with self.assertRaises(BenchmarkError):
            bench.validate_bulk(200, {"errors": True, "items": [{"index": {"status": 429}}]}, 1)

    def test_permanent_status_missing_items_and_silent_partial_fail(self):
        for status, reply in [(400, {"error": "mapping"}), (200, {"errors": False, "items": []}),
                              (200, {"errors": False, "items": [{"index": {"status": 400}}]})]:
            with self.subTest(status=status, reply=reply), self.assertRaises(BenchmarkError):
                bench.validate_bulk(status, reply, 1)
        self.assertTrue(bench.validate_bulk(200, {"errors": False, "items": [{"index": {"status": 201}}]}, 1))

    def test_corpus_body_is_deterministic_across_batch_boundaries(self):
        self.assertEqual(bench.body_for(0, 3)+bench.body_for(3, 8), bench.body_for(0, 8))
        lines = bench.body_for(3, 4, payload_bytes=2048).splitlines()
        self.assertEqual(json.loads(lines[0]), {"index": {"_id": "d3"}})
        self.assertEqual(len(json.loads(lines[1])["message"].encode()), 2048)

    def test_partial_or_inexact_search_never_counts_as_success(self):
        good = {"partial": False, "complete": True, "total": {"value": 1, "relation": "eq"}, "hits": []}
        bench.validate_query(good, 10)
        for change in ({"partial": True}, {"complete": False}, {"total": {"value": 1, "relation": "gte"}},
                       {"total": {"value": 11, "relation": "eq"}}):
            with self.subTest(change=change), self.assertRaises(BenchmarkError):
                bench.validate_query({**good, **change}, 10)

    def test_source_oracle_accepts_known_revisions_and_rejects_corruption(self):
        reply = {"partial": False, "complete": True, "total": {"value": 1, "relation": "eq"},
                 "hits": [{"_id": "d3", "_source": bench.source(3)}]}
        bench.validate_query(reply, 10)
        reply["hits"][0]["_source"]["tag"] = "wrong"
        with self.assertRaises(BenchmarkError):
            bench.validate_query(reply, 10)

    def test_dashboard_uses_aggregation_contract_without_hit_total(self):
        reply = {"partial": False, "complete": True, "hits": [], "aggs": {"tags": {
            "doc_count_error_upper_bound": 0, "sum_other_doc_count": 2,
            "buckets": [{"key": "group1", "doc_count": 3}]}}}
        bench.validate_query(reply, 10, aggregation=True)
        reply["aggs"]["tags"]["buckets"].append({"key": "group1", "doc_count": 1})
        with self.assertRaises(BenchmarkError):
            bench.validate_query(reply, 10, aggregation=True)

    def test_settlement_requires_empty_buffers_no_active_job_and_ready_ack(self):
        health = {"ok": True, "buffered": 0, "flush_in_progress": False,
                  "compaction": {"pending_schedulable": 0, "running_segments": 0},
                  "watermarks": {"ack": 4, "ready": 4}}
        self.assertTrue(bench.settled(health))
        for change in ({"buffered": 1}, {"flush_in_progress": True},
                       {"compaction": {"pending_schedulable": 1, "running_segments": 0}},
                       {"watermarks": {"ack": 4, "ready": 3}},
                       {"watermarks": None}, {"watermarks": {}},
                       {"watermarks": {"ack": 4}}, {"watermarks": {"ready": 4}},
                       {"watermarks": {"ack": None, "ready": None}},
                       {"watermarks": {"ack": "4", "ready": "4"}},
                       {"watermarks": {"ack": 4.0, "ready": 4.0}},
                       {"watermarks": {"ack": True, "ready": True}},
                       {"watermarks": {"ack": -1, "ready": -1}},
                       {"watermarks": {"ack": 2**64, "ready": 2**64}}):
            with self.subTest(change=change):
                self.assertFalse(bench.settled({**health, **change}))
        self.assertFalse(bench.settled({"ok": True}))

    def test_readiness_request_respects_remaining_budget_and_expiry(self):
        node = Mock()
        bench.call_before(node, time.monotonic()+.1, "GET", "/logs/_health")
        timeout = node.request.call_args.kwargs["timeout"]
        self.assertGreater(timeout, 0)
        self.assertLessEqual(timeout, .1)
        node.request.reset_mock()
        with self.assertRaises(BenchmarkError):
            bench.call_before(node, time.monotonic()-1, "GET", "/logs/_health")
        node.request.assert_not_called()

    def test_metrics_filter_keeps_ingest_wait_histogram_and_server_rss(self):
        lines = ['# TYPE ignored gauge', 'svindb_write_lock_wait_ms_sum{lock="ingest"} 4',
                 'svindb_write_lock_wait_ms_sum{lock="layout"} 9', 'svindb_process_resident_bytes 123']
        self.assertEqual(bench.metrics_selected('\n'.join(lines)), [lines[1], lines[3]])


if __name__ == "__main__":
    unittest.main()
