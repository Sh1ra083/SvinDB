#!/usr/bin/env python3
"""Регрессионные тесты HTTP-фаззера."""

import json
import unittest

import fuzz
import fuzz_ingest


class DeepQueryTests(unittest.TestCase):
    def test_small_deep_query_is_valid_json(self):
        depth = 3
        decoded = json.loads(fuzz.deep_query_bytes(depth))

        query = decoded["query"]
        for _ in range(depth):
            query = query["should"][0]["bool"]

        self.assertEqual(query["should"][0]["field"], 0)
        self.assertEqual(query["should"][0]["terms"], ["общее"])

    def test_very_deep_query_does_not_use_python_recursion(self):
        depth = 5000
        encoded = fuzz.deep_query_bytes(depth)

        self.assertIsInstance(encoded, bytes)
        self.assertTrue(encoded.startswith(b'{"k":10,"query":'))
        self.assertTrue(encoded.endswith(b'}'))
        self.assertEqual(encoded.count(b'{"should":[{"bool":'), depth)

    def test_preencoded_body_is_not_serialized_again(self):
        encoded = fuzz.deep_query_bytes(5000)
        self.assertIs(fuzz.encode_body(encoded), encoded)

    def test_negative_depth_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "глубина"):
            fuzz.deep_query_bytes(-1)


class ProcessStateTests(unittest.TestCase):
    def test_ingest_fuzzer_checks_the_process_it_started(self):
        class Process:
            def __init__(self, result):
                self.result = result

            def poll(self):
                return self.result

        self.assertTrue(fuzz_ingest.alive(Process(None)))
        self.assertFalse(fuzz_ingest.alive(Process(1)))


if __name__ == "__main__":
    unittest.main()
