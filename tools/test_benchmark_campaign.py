#!/usr/bin/env python3
"""Регрессии измерительного контура, не требующие работающего узла."""

from __future__ import annotations

import argparse
import json
import sys
import unittest
from pathlib import Path
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchmark_campaign as campaign  # noqa: E402


class QueryReferenceTests(unittest.TestCase):
    def test_a_real_term_cannot_be_benchmarked_as_an_empty_answer(self) -> None:
        spec = {"name": "term/frequent", "builder": "frequency", "level": "frequent"}
        with self.assertRaises(campaign.CampaignError):
            campaign.validate_query_reference(spec, {"hits": []})
        campaign.validate_query_reference(spec, {"hits": [{"_id": "a"}]})

    def test_the_miss_family_requires_an_empty_answer(self) -> None:
        spec = {"name": "term/miss", "builder": "frequency", "level": "miss"}
        campaign.validate_query_reference(spec, {"hits": []})
        with self.assertRaises(campaign.CampaignError):
            campaign.validate_query_reference(spec, {"hits": [{"_id": "wrong"}]})


class StressCorpusContract(unittest.TestCase):
    def test_es_batch_canonicalizes_duplicate_json_keys(self) -> None:
        payload = campaign.es_batch(
            [b'{"id":"doc-1","embedding_sparse":{"term":1,"term":2}}']
        )
        rows = payload.splitlines()
        self.assertEqual(json.loads(rows[0]), {"index": {"_id": "doc-1"}})
        self.assertEqual(json.loads(rows[1])["embedding_sparse"], {"term": 2})

    def test_flat_delete_lane_uses_the_flat_corpus_ids(self) -> None:
        self.assertEqual(campaign.corpus_identifier("flat", 7), "flat-0000000007")
        self.assertEqual(campaign.corpus_identifier("mixed", 7), "mixed-0000000007")

    def test_flat_stress_uses_only_flat_10m_fields(self) -> None:
        document = campaign.stress_document("flat", "doc", 7, "marker")
        self.assertEqual(
            set(document),
            {"id", "message", "views", "ts", "channel"},
        )
        self.assertEqual(document["channel"], "marker")
        self.assertEqual(document["views"], 7)

    def test_mixed_stress_keeps_the_mixed_oracle_fields(self) -> None:
        document = campaign.stress_document("mixed", "doc", 7, "marker")
        self.assertEqual(document["status"], "marker")
        self.assertEqual(document["sequence"], 7)
        self.assertIn("text", document)

    def test_backpressure_is_retried_but_another_error_is_not_hidden(self) -> None:
        replies = iter(
            [
                campaign.Reply(429, b"", 1.0, {}),
                campaign.Reply(429, b"", 1.0, {}),
                campaign.Reply(200, b"{}", 1.0, {}),
            ]
        )
        with mock.patch.object(campaign.time, "sleep"):
            reply, retries = campaign.stress_retry_429(lambda: next(replies))
        self.assertEqual(reply.status, 200)
        self.assertEqual(retries, 2)

        reply, retries = campaign.stress_retry_429(
            lambda: campaign.Reply(400, b"bad", 1.0, {})
        )
        self.assertEqual(reply.status, 400)
        self.assertEqual(retries, 0)


class ResourceLimitParsing(unittest.TestCase):
    def test_memory_units_and_unrestricted(self) -> None:
        self.assertEqual(campaign.parse_byte_limit("1 GiB"), 1 << 30)
        self.assertEqual(campaign.parse_byte_limit("1.5GiB"), 3 << 29)
        self.assertIsNone(campaign.parse_byte_limit("unrestricted"))

    def test_invalid_limits_are_rejected(self) -> None:
        for value in ("0", "-1GiB", "one-gib", "1XB"):
            with self.subTest(value=value), self.assertRaises(campaign.CampaignError):
                campaign.parse_byte_limit(value)
        for value in ("0", "-2", "many"):
            with self.subTest(value=value), self.assertRaises(campaign.CampaignError):
                campaign.parse_cpu_limit(value)


class ResourceContract(unittest.TestCase):
    @staticmethod
    def args(**overrides: object) -> argparse.Namespace:
        values = {
            "memory_limit": "unrestricted",
            "cpu_limit": "unrestricted",
            "pid": None,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_unrestricted_contract_does_not_invent_enforcement(self) -> None:
        got = campaign.validate_resource_contract(self.args())
        self.assertTrue(got["enforced"])
        self.assertIsNone(got["memory_requested"])
        self.assertIsNone(got["cpu_cores_requested"])

    def test_label_without_cgroup_process_is_rejected(self) -> None:
        with mock.patch.object(campaign.sys, "platform", "linux"):
            with self.assertRaisesRegex(campaign.CampaignError, "--pid"):
                campaign.validate_resource_contract(
                    self.args(memory_limit="1GiB")
                )

    def test_observed_exact_limits_are_accepted(self) -> None:
        observed = {
            "available": True,
            "path": "/sys/fs/cgroup/test.scope",
            "memory_max_effective": 1 << 30,
            "cpu_cores_effective": 2.0,
        }
        with (
            mock.patch.object(campaign.sys, "platform", "linux"),
            mock.patch.object(campaign, "cgroup_sample", return_value=observed),
            mock.patch.object(
                campaign.os,
                "sched_getaffinity",
                return_value={0, 1},
                create=True,
            ),
        ):
            got = campaign.validate_resource_contract(
                self.args(memory_limit="1GiB", cpu_limit="2", pid=123)
            )
        self.assertTrue(got["enforced"])
        self.assertEqual(got["observed"], observed)
        self.assertEqual(got["affinity_cpus"], 2)

    def test_declared_and_observed_limits_must_match(self) -> None:
        observed = {
            "available": True,
            "path": "/sys/fs/cgroup/test.scope",
            "memory_max_effective": 2 << 30,
            "cpu_cores_effective": None,
        }
        with (
            mock.patch.object(campaign.sys, "platform", "linux"),
            mock.patch.object(campaign, "cgroup_sample", return_value=observed),
            self.assertRaisesRegex(campaign.CampaignError, "фактически действует"),
        ):
            campaign.validate_resource_contract(
                self.args(memory_limit="1GiB", pid=123)
            )


class ParserContract(unittest.TestCase):
    def test_mixed_es_mapping_preserves_documents_with_invalid_geo_fixture(self) -> None:
        mapping = campaign.es_mapping("mixed")
        self.assertTrue(
            mapping["mappings"]["properties"]["place"]["ignore_malformed"]
        )

    def test_load_exposes_cpu_limit(self) -> None:
        args = campaign.build_parser().parse_args(
            [
                "load",
                "--candidate",
                "candidate",
                "--corpus",
                "flat",
                "--output",
                "result.json",
                "--port",
                "9000",
                "--engine",
                "svindb",
                "--corpus-path",
                "corpus",
                "--cpu-limit",
                "8",
            ]
        )
        self.assertEqual(args.cpu_limit, "8")


class ResourceValidity(unittest.TestCase):
    def test_clean_cgroup_is_valid(self) -> None:
        self.assertEqual(
            campaign.resource_validity_violations(
                {
                    "cgroup": {
                        "memory_swap_peak": 0,
                        "memory_events_delta": {"oom": 0, "oom_kill": 0},
                    },
                    "host_vm_delta": {"pswpin": 0, "pswpout": 0, "oom_kill": 0},
                }
            ),
            [],
        )

    def test_swap_and_oom_make_run_invalid(self) -> None:
        got = campaign.resource_validity_violations(
            {
                "cgroup": {
                    "memory_swap_peak": 4096,
                    "memory_events_delta": {"oom": 2, "oom_kill": 1},
                },
                "host_vm_delta": {"pswpout": 3},
            }
        )
        self.assertEqual(len(got), 4)
        self.assertTrue(any("swap" in value for value in got))
        self.assertTrue(any("oom_kill=1" in value for value in got))


class BoundaryResourceValidity(unittest.TestCase):
    def test_successful_large_bodies_are_not_called_a_malformed_leak(self) -> None:
        got = campaign.boundary_resource_contract(
            {"status.VmHWM": 100, "status.VmRSS": 100, "fds": 5},
            {
                "status.VmHWM": (1500 << 20) + 100,
                "status.VmRSS": (500 << 20) + 100,
                "fds": 5,
            },
            {
                "status.VmHWM": (1500 << 20) + 100,
                "status.VmRSS": (520 << 20) + 100,
                "fds": 6,
            },
        )
        self.assertTrue(got["valid"])
        self.assertEqual(got["malformed_rss_growth_bytes"], 20 << 20)

    def test_malformed_growth_and_transient_explosion_are_rejected(self) -> None:
        malformed = campaign.boundary_resource_contract(
            {"status.VmHWM": 0, "status.VmRSS": 0, "fds": 5},
            {"status.VmHWM": 100 << 20, "status.VmRSS": 100 << 20, "fds": 5},
            {"status.VmHWM": 200 << 20, "status.VmRSS": 200 << 20, "fds": 5},
        )
        self.assertFalse(malformed["valid"])
        transient = campaign.boundary_resource_contract(
            {"status.VmHWM": 0, "status.VmRSS": 0, "fds": 5},
            {"status.VmHWM": (2 << 30) + 1, "status.VmRSS": 100, "fds": 5},
            {"status.VmHWM": (2 << 30) + 1, "status.VmRSS": 100, "fds": 5},
        )
        self.assertFalse(transient["valid"])


if __name__ == "__main__":
    unittest.main()
