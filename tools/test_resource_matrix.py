#!/usr/bin/env python3
"""Автономные проверки resource-matrix runner."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchmark_campaign as campaign  # noqa: E402
import resource_matrix  # noqa: E402


class ServerEnvironment(unittest.TestCase):
    def test_environment_is_parsed_without_losing_empty_values(self) -> None:
        self.assertEqual(
            resource_matrix.server_environment(["A=one=two", "EMPTY="]),
            {"A": "one=two", "EMPTY": ""},
        )

    def test_invalid_or_duplicate_names_are_rejected(self) -> None:
        for values in (["NO_EQUALS"], ["1BAD=x"], ["A=1", "A=2"]):
            with self.subTest(values=values), self.assertRaises(campaign.CampaignError):
                resource_matrix.server_environment(values)


if __name__ == "__main__":
    unittest.main()
