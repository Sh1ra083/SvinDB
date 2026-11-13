#!/usr/bin/env python3
"""Дешёвые проверки формата и семантического oracle segment-scale runner."""

from __future__ import annotations

import tempfile
import unittest
import zlib
from pathlib import Path

import segment_scale


class SegmentScaleTest(unittest.TestCase):
    def test_manifest_exactly_matches_rust_format(self) -> None:
        names = ["seg-scale-00000000000000000000", "seg-scale-00000000000000000001"]
        names_body = "".join(f"{name}\n" for name in names).encode()
        expected = (
            f"#svindb-manifest\t7\t2\t{zlib.crc32(names_body) & 0xFFFF_FFFF:08x}\n".encode()
            + names_body
        )
        self.assertEqual(segment_scale.manifest_bytes(names, 7), expected)

    def test_manifest_rejects_format_boundaries(self) -> None:
        with self.assertRaises(segment_scale.ScaleError):
            segment_scale.manifest_bytes(["wrong0001"], 1)
        with self.assertRaises(segment_scale.ScaleError):
            segment_scale.manifest_bytes(["seg/0001"], 1)
        with self.assertRaises(segment_scale.ScaleError):
            segment_scale.manifest_bytes(["seg" + "0" * 30], 1)
        with self.assertRaises(segment_scale.ScaleError):
            segment_scale.manifest_bytes([f"seg{i}" for i in range(30_001)], 1)

    def test_hardlink_tree_does_not_copy_data(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "source"
            source.mkdir()
            (source / "head.tsv").write_bytes(b"head")
            (source / "nested").mkdir()
            (source / "nested" / "seg.dat").write_bytes(b"body")
            destination = root / "destination"
            segment_scale.hardlink_tree(source, destination)
            self.assertEqual((destination / "head.tsv").read_bytes(), b"head")
            self.assertEqual((destination / "nested" / "seg.dat").read_bytes(), b"body")
            self.assertEqual(
                (source / "head.tsv").stat().st_ino,
                (destination / "head.tsv").stat().st_ino,
            )

    def test_oracle_checks_count_cardinality_and_mget(self) -> None:
        query_by_name = {query.name: query for query in segment_scale.queries(100)}
        segment_scale.validate_reply(
            query_by_name["agg/count"], {"aggs": {"all": {"count": 1000}}}, 1000
        )
        segment_scale.validate_reply(
            query_by_name["agg/cardinality"],
            {"aggs": {"channels": {"value": 17}}},
            1000,
        )
        segment_scale.validate_reply(
            query_by_name["mget/100"],
            {"docs": [{"found": True} for _ in range(100)]},
            1000,
        )
        with self.assertRaises(segment_scale.ScaleError):
            segment_scale.validate_reply(
                query_by_name["agg/count"], {"aggs": {"all": {"count": 999}}}, 1000
            )


if __name__ == "__main__":
    unittest.main()
