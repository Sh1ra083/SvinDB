#!/usr/bin/env python3
"""Проверки достоверности crash matrix без запуска базы данных."""

import argparse
import copy
import os
from pathlib import Path
import sys
import socket
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import failure_matrix as matrix


class FailureMatrixTests(unittest.TestCase):
    def test_short_release_revision_is_kept_in_the_manifest(self):
        self.assertEqual(
            matrix.runtime_revision(
                "svindb 3.0.0-alpha.1 revision=d4b0716 target=linux-x86_64"
            ),
            "d4b0716",
        )
        self.assertEqual(
            matrix.runtime_revision(
                "svindb 3.0.0-alpha.1 revision=0123456789abcdef0123456789abcdef01234567 target=linux-x86_64"
            ),
            "0123456789abcdef0123456789abcdef01234567",
        )

    @unittest.skipUnless(sys.platform == "linux", "проверка Linux socket ownership")
    def test_listener_is_owned_by_the_exact_process_and_never_guessed(self):
        with socket.socket() as first:
            first.bind(("127.0.0.1", 0))
            first.listen()
            self.assertEqual(matrix.owned_listener_port(os.getpid()), first.getsockname()[1])
            with socket.socket() as second:
                second.bind(("127.0.0.1", 0))
                second.listen()
                with self.assertRaises(matrix.MatrixError):
                    matrix.owned_listener_port(os.getpid())

    def test_quiet_health_requires_exact_live_count_and_all_watermarks(self):
        good = {"ok": True, "docs": 2, "dead": 1, "buffered": 0,
                "flush_in_progress": False, "write_queue": {"pending": 0},
                "wal": {"seq": 3, "checkpoint": 3, "applied": 3},
                "watermarks": {"ack": 3, "visible": 3, "ready": 3, "settled": 3},
                "compaction": {"pending_segments": 0, "running_segments": 0}}
        variants = [(good, True)]
        for group, key, value in [("watermarks", "settled", 2), ("wal", "checkpoint", 2),
                                  ("compaction", "running_segments", 1), ("write_queue", "pending", 1)]:
            changed = copy.deepcopy(good)
            changed[group][key] = value
            variants.append((changed, False))
        wrong_count = copy.deepcopy(good)
        wrong_count["docs"] = 3
        variants.append((wrong_count, False))
        for health, valid in variants:
            with self.subTest(health=health):
                clock = iter(100 + n / 10 for n in range(100))
                reply = mock.Mock(status=200)
                reply.json.return_value = health
                with mock.patch.object(matrix, "one_request", return_value=reply), \
                     mock.patch.object(matrix.time, "monotonic", side_effect=lambda: next(clock)), \
                     mock.patch.object(matrix.time, "sleep"):
                    if valid:
                        self.assertEqual(matrix.stable_health(1, 1, 1), good)
                    else:
                        with self.assertRaises(matrix.MatrixError):
                            matrix.stable_health(1, 1, 1)

    def test_only_the_requested_failpoint_and_its_exit_code_prove_the_crash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            node = matrix.Node(root / "binary", root / "index", 1, root / "node.log", "wal.after_fsync")
            marker = "SVINDB_FAILPOINTS: точка «wal.after_fsync» снимает процесс\n"
            for code, log, valid in [(137, marker, True), (137, "OOM", False),
                                     (-9, marker, False), (0, marker, False),
                                     (137, marker.replace("wal.after_fsync", "other"), False)]:
                with self.subTest(code=code, log=log):
                    node.log.write_text(log)
                    node.process = mock.Mock()
                    node.process.wait.return_value = code
                    if valid:
                        self.assertEqual(node.expect_crash(1), 137)
                    else:
                        with self.assertRaises(matrix.MatrixError):
                            node.expect_crash(1)

    def test_failed_start_and_missing_failpoint_always_stop_the_created_node(self):
        for stage in ["start", "expect_crash"]:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                args = argparse.Namespace(root=root, binary=root / "binary", timeout=0.1,
                                          start_seed=1, keep_success=True)
                node = mock.Mock()
                getattr(node, stage).side_effect = matrix.MatrixError("заданная ошибка")
                node.stop.return_value = 0
                with mock.patch.object(matrix, "Node", return_value=node), \
                     mock.patch.object(matrix, "clone_template"), \
                     mock.patch.object(matrix, "trigger_operation", return_value={"topk_before": []}):
                    result = matrix.run_case(args, matrix.CASES[0], root / "template", [], 0)
                self.assertFalse(result["valid"])
                node.stop.assert_called_once_with(0.1)

    def test_a_forced_shutdown_is_not_a_successful_graceful_shutdown(self):
        node = matrix.Node(Path("unused"), Path("unused"), 1, Path("unused"))
        with mock.patch.object(node, "stop", return_value=-9):
            with self.assertRaises(matrix.MatrixError):
                node.stop_clean(1)

    def test_cleanup_failure_is_recorded_and_never_removes_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = argparse.Namespace(root=root, binary=root / "binary", timeout=0.1,
                                      start_seed=1, keep_success=False)
            node = mock.Mock()
            node.start.side_effect = matrix.MatrixError("startup")
            node.stop.side_effect = matrix.MatrixError("cleanup")
            with mock.patch.object(matrix, "Node", return_value=node), \
                 mock.patch.object(matrix, "clone_template"):
                result = matrix.run_case(args, matrix.CASES[0], root / "template", [], 0)
            self.assertFalse(result["valid"])
            self.assertTrue(result["cleanup_errors"])
            self.assertTrue((root / "cases" / result["case"] / "result.json").is_file())


if __name__ == "__main__":
    unittest.main()
