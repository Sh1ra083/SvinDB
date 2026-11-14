#!/usr/bin/env python3
"""Readiness-oracle regressions; no server processes or external dependencies."""
import time
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock

import bench_startup as bench


class StartupReadinessTests(unittest.TestCase):
    def node(self, replies):
        node = Mock()
        node.started = time.monotonic()
        node.request.side_effect = replies
        return node

    def test_point_get_retries_503_and_stale_source_until_latest(self):
        latest = bench.source(0, True)
        node = self.node([(503, {"error": "warming"}),
                          (200, {"found": True, "_source": bench.source(0)}),
                          (200, {"found": True, "_source": latest})])
        result = bench.probe(node, "point_get", {"a": {"latest_source": latest}}, time.monotonic() + 1, 0)
        self.assertTrue(result["passed"])
        self.assertEqual(result["attempts"], 3)
        self.assertEqual(result["http_status_counts"], {"503": 1, "200": 2})

    def test_search_needs_both_exact_count_and_latest_source(self):
        latest = bench.source(0, True)
        total = lambda n: {"partial": False, "complete": True,
                           "total": {"value": n, "relation": "eq"}}
        marker = {**total(1), "hits": [{"_id": "0", "_source": latest}]}
        stale = {**total(1), "hits": [{"_id": "0", "_source": bench.source(0)}]}
        node = self.node([(200, total(9)), (200, total(10)), (200, stale),
                          (200, total(10)), (200, marker)])
        result = bench.probe(node, "search", {"a": {"count": 10, "latest_source": latest}}, time.monotonic() + 1, 0)
        self.assertTrue(result["passed"])
        self.assertEqual(result["attempts"], 3)
        self.assertEqual(node.request.call_count, 5)

    def test_search_rejects_lower_bound_count(self):
        self.assertFalse(bench.exact_total({"partial": False, "complete": True,
                                            "total": {"value": 10, "relation": "gte"}}, 10))

    def test_search_and_marker_require_explicit_complete_response(self):
        latest = bench.source(0, True)
        complete = {"partial": False, "complete": True,
                    "total": {"value": 1, "relation": "eq"},
                    "hits": [{"_id": "0", "_source": latest}]}
        self.assertTrue(bench.exact_total(complete, 1))
        self.assertTrue(bench.exact_marker(complete, latest))
        for change in ({"partial": True}, {"partial": None}, {"partial": "false"},
                       {"partial": "missing"}, {"complete": False}, {"complete": None},
                       {"complete": "true"}, {"complete": "missing"}):
            incomplete = {**complete, **change}
            with self.subTest(change=change):
                self.assertFalse(bench.exact_total(incomplete, 1))
                self.assertFalse(bench.exact_marker(incomplete, latest))
        for field in ("partial", "complete"):
            incomplete = {key: value for key, value in complete.items() if key != field}
            with self.subTest(missing=field):
                self.assertFalse(bench.exact_total(incomplete, 1))
                self.assertFalse(bench.exact_marker(incomplete, latest))

    def test_search_retries_incomplete_reply_until_deadline(self):
        latest = bench.source(0, True)
        incomplete = {"total": {"value": 1, "relation": "eq"},
                      "hits": [{"_id": "0", "_source": latest}]}
        complete = {**incomplete, "partial": False, "complete": True}
        expected = {"a": {"count": 1, "latest_source": latest}}
        node = self.node([(200, incomplete), (200, incomplete), (200, complete), (200, complete)])
        result = bench.probe(node, "search", expected, time.monotonic() + 1, 0)
        self.assertTrue(result["passed"])
        self.assertEqual(result["attempts"], 3)

        node = self.node([(200, incomplete)] * 100)
        result = bench.probe(node, "search", expected, time.monotonic() + .01, .002)
        self.assertFalse(result["passed"])
        self.assertGreater(result["attempts"], 0)
        self.assertLess(node.request.call_count, 100)

    def test_marker_rejects_duplicate_hits_and_wrong_id(self):
        latest = bench.source(0, True)
        reply = {"partial": False, "complete": True,
                 "total": {"value": 1, "relation": "eq"}, "hits": [{"_id": "other", "_source": latest}]}
        self.assertFalse(bench.exact_marker(reply, latest))
        reply["hits"] = [{"_id": "0", "_source": latest}] * 2
        self.assertFalse(bench.exact_marker(reply, latest))

    def test_point_read_requires_every_index(self):
        latest = bench.source(0, True)
        node = self.node([(200, {"found": True, "_source": latest}),
                          (200, {"found": False}),
                          (200, {"found": True, "_source": latest}),
                          (200, {"found": True, "_source": latest})])
        expected = {name: {"latest_source": latest} for name in ("a", "b")}
        result = bench.probe(node, "point_get", expected, time.monotonic() + 1, 0)
        self.assertTrue(result["passed"])
        self.assertEqual(result["attempts"], 2)

    def test_fixture_clone_preserves_header_identity_and_isolates_mutable_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            seed = root / "seed"
            seed.mkdir()
            for name in ("head.tsv", "deleted.bin", "000001.log", "catalog.bin"):
                (seed / name).write_bytes(b"original")
            destination = root / "clone"
            bench.clone_fixture(seed, destination)
            self.assertEqual((seed / "head.tsv").stat().st_ino,
                             (destination / "head.tsv").stat().st_ino)
            for name in ("deleted.bin", "000001.log", "catalog.bin"):
                self.assertNotEqual((seed / name).stat().st_ino,
                                    (destination / name).stat().st_ino)
                (destination / name).write_bytes(b"changed")
                self.assertEqual((seed / name).read_bytes(), b"original")

    def test_expired_deadline_does_not_call_server_or_claim_readiness(self):
        node = self.node([])
        result = bench.probe(node, "health", {}, time.monotonic() - 1, 0)
        self.assertFalse(result["passed"])
        self.assertIsNone(result["seconds"])
        node.request.assert_not_called()

    def test_health_http_200_requires_ok_true(self):
        node = self.node([(200, {"ok": False}), (200, {"ok": True})])
        result = bench.probe(node, "health", {}, time.monotonic() + 1, 0)
        self.assertTrue(result["passed"])
        self.assertEqual(result["attempts"], 2)


if __name__ == "__main__":
    unittest.main()
