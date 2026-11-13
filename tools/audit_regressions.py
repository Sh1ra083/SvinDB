#!/usr/bin/env python3
"""Run isolated HTTP audit regressions against a local SvinDB binary.

Usage: python3 tools/audit_regressions.py [--binary target/debug/svindb]
Build first with: cargo build -p svindb-server
Only temporary data and a loopback subprocess are used; the node is always stopped.
"""
import argparse
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest


class Node:
    def __init__(self, binary):
        self.binary = binary
        self.temp = tempfile.TemporaryDirectory(prefix="svindb-audit-regressions-")
        self.log_path = Path(self.temp.name) / "node.log"
        self.log = self.log_path.open("wb")
        self.child = None
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        try:
            self.child = subprocess.Popen(
                [str(binary), "serve", str(Path(self.temp.name) / "data"), str(self.port),
                 "--bind=127.0.0.1", "--no-maintain", "--sync-flush", "--threads=2"],
                env={**os.environ, "SVINDB_DISK_FLOOR_MB": "1"},
                stdout=self.log, stderr=subprocess.STDOUT,
            )
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if self.child.poll() is not None:
                    raise RuntimeError(f"node exited: {self.child.returncode}")
                try:
                    if self.request("GET", "/_health")[0] == 200:
                        return
                except (OSError, http.client.HTTPException):
                    pass
                time.sleep(.025)
            raise RuntimeError("node readiness exceeded 20 seconds")
        except BaseException:
            self.close()
            raise

    def request(self, method, path, body=None, ndjson=False):
        raw = body if ndjson else (None if body is None else json.dumps(body))
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        try:
            connection.request(method, path, raw,
                               {"Content-Type": "application/x-ndjson" if ndjson else "application/json"})
            response = connection.getresponse()
            content = response.read()
            headers = {key.lower(): value for key, value in response.getheaders()}
            try:
                payload = json.loads(content) if content else {}
            except ValueError:
                payload = {"raw": content.decode(errors="replace")}
            return response.status, payload, headers
        finally:
            connection.close()

    def close(self):
        if self.child is not None:
            self.child.terminate()
            try:
                self.child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.child.wait(timeout=5)
        self.log.close()
        if self.log_path.exists():
            self.last_log = self.log_path.read_text(errors="replace")
        self.temp.cleanup()


class AuditRegressions(unittest.TestCase):
    node = None

    def call(self, method, path, body=None, status=200, ndjson=False):
        actual, payload, headers = self.node.request(method, path, body, ndjson)
        self.assertEqual(actual, status, f"{method} {path}: HTTP {actual}: {payload}")
        return payload, headers

    def create(self, name, properties=None):
        self.call("PUT", f"/{name}", {"mappings": {"dynamic": "strict", "properties": properties or {
            "message": {"type": "text"}, "id": {"type": "long"}}}})
        return f"/{name}"

    def flush(self, index):
        self.call("POST", index + "/_flush", {})

    def search(self, index, query=None, **extra):
        payload, _ = self.call("POST", index + "/_search",
                               {"query": query or {"match_all": {}}, "size": 100, **extra})
        hits = payload["hits"]
        return payload, hits["hits"] if isinstance(hits, dict) else hits

    def bulk(self, index, entries):
        text = "".join(json.dumps(part) + "\n" for part in entries)
        return self.call("POST", index + "/_bulk", text, ndjson=True)[0]

    def test_keepalive_waits_for_the_next_request(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.node.port, timeout=5)
        try:
            requests = [
                ("PUT", "/keepalive-reuse", {"mappings": {"dynamic": "strict", "properties": {
                    "ordinal": {"type": "long"}, "message": {"type": "text"}}}}),
                ("PUT", "/keepalive-reuse/_settings",
                 {"refresh_interval_ms": 0, "live_max_docs": 0, "flush_mb": 32}),
                ("GET", "/_health", None),
            ]
            original_socket = None
            for method, path, body in requests:
                connection.request(method, path, None if body is None else json.dumps(body),
                                   {"Content-Type": "application/json"})
                response = connection.getresponse()
                content = response.read()
                self.assertEqual(response.status, 200, f"{method} {path}: {content!r}")
                self.assertEqual(response.getheader("Connection"), "keep-alive")
                if original_socket is None:
                    original_socket = connection.sock
                self.assertIs(connection.sock, original_socket, "client reconnected instead of reusing TCP")
                # Make the next request arrive after the worker returns to reading.
                # Accepted sockets must block here even when the listener does not.
                time.sleep(.025)
        finally:
            connection.close()

    def test_routes_404_and_405_allow(self):
        self.call("GET", "/_definitely_unknown_audit_route", status=404)
        _, headers = self.call("DELETE", "/_health", status=405)
        self.assertIn("GET", [method.strip() for method in headers.get("allow", "").split(",")])
        self.assertNotIn("DELETE", headers["allow"])

    def test_strict_query_and_aggregation_fail_closed(self):
        index = self.create("strict")
        for body in [
            {"query": {"unsupported_query": {}}},
            {"query": {"bool": {"must": [{"match_all": {}}], "unsupported_clause": []}}},
            {"aggs": {"bad": {"unsupported_aggregation": {"field": "id"}}}},
            {"aggs": {"bad": {"terms": {"field": "message", "unsupported_option": True}}}},
        ]:
            with self.subTest(body=body):
                self.call("POST", index + "/_search", body, status=400)

    def test_put_overwrites_a_flushed_document(self):
        index = self.create("put-overwrite")
        self.call("PUT", index + "/_doc/same", {"message": "before", "id": 1})
        self.flush(index)
        self.call("PUT", index + "/_doc/same", {"message": "after", "id": 2})
        self.flush(index)
        _, hits = self.search(index)
        self.assertEqual([hit["_id"] for hit in hits], ["same"])
        self.assertEqual(hits[0]["_source"], {"message": "after", "id": 2})
        _, old = self.search(index, {"term": {"message": "before"}})
        self.assertEqual(old, [])

    def test_es_bulk_index_upserts_repeated_id(self):
        index = self.create("bulk-upsert")
        for number, message in [(1, "before"), (2, "after")]:
            result = self.bulk(index, [{"index": {"_id": "same"}}, {"message": message, "id": number}])
            self.assertFalse(result.get("errors", False), result)
            self.flush(index)
        _, hits = self.search(index)
        self.assertEqual([hit["_id"] for hit in hits], ["same"])
        self.assertEqual(hits[0]["_source"]["message"], "after")
        result = self.bulk(index, [
            {"index": {"_id": "same"}}, {"message": "third", "id": 3},
            {"index": {"_id": "same"}}, {"message": "last", "id": 4},
        ])
        self.assertFalse(result.get("errors", False), result)
        self.flush(index)
        _, hits = self.search(index)
        self.assertEqual([hit["_id"] for hit in hits], ["same"])
        self.assertEqual(hits[0]["_source"]["message"], "last")

    def test_generated_ids_do_not_reuse_source_id(self):
        index = self.create("generated")
        result = self.bulk(index, [{"index": {}}, {"message": "first", "id": 42},
                                   {"index": {}}, {"message": "second", "id": 42}])
        self.assertFalse(result.get("errors", False), result)
        self.flush(index)
        _, hits = self.search(index)
        self.assertEqual(len(hits), 2)
        identifiers = [hit["_id"] for hit in hits]
        self.assertEqual(len(set(identifiers)), 2)
        self.assertNotIn("42", identifiers)
        self.assertEqual([hit["_source"]["id"] for hit in hits], [42, 42])

    def test_timestamp_and_dotted_field_mapping_and_queries(self):
        index = self.create("field-names", {"@timestamp": {"type": "date"},
                                           "host.name": {"type": "keyword"}})
        self.call("PUT", index + "/_doc/one", {"@timestamp": "2026-11-13T10:00:00Z", "host.name": "alpha"})
        self.flush(index)
        _, hits = self.search(index, {"bool": {"filter": [
            {"term": {"host.name": "alpha"}},
            {"range": {"@timestamp": {"gte": "2026-11-13T00:00:00Z", "lt": "2026-11-14T00:00:00Z"}}},
        ]}})
        self.assertEqual([hit["_id"] for hit in hits], ["one"])

    def test_size_zero_returns_no_hits_without_aggregations(self):
        index = self.create("size-zero")
        self.call("PUT", index + "/_doc/one", {"message": "present", "id": 1})
        self.flush(index)
        _, visible = self.search(index)
        self.assertEqual(len(visible), 1)
        _, hits = self.search(index, size=0)
        self.assertEqual(hits, [])

    def test_bulk_item_errors_do_not_discard_valid_items(self):
        index = self.create("bulk-mixed")
        self.call("PUT", index + "/_doc/remove", {"message": "remove", "id": 0})
        self.flush(index)
        result = self.bulk(index, [
            {"index": {"_id": "good"}}, {"message": "accepted", "id": 1},
            {"index": {"_id": "bad"}}, {"message": "invalid", "undeclared": "field"},
            {"delete": {"_id": "remove"}},
        ])
        self.assertTrue(result["errors"], result)
        self.assertEqual(len(result["items"]), 3, result)
        items = [next(iter(item.values())) for item in result["items"]]
        self.assertIn(items[0]["status"], [200, 201])
        self.assertEqual(items[1]["status"], 400)
        self.assertIn(items[2]["status"], [200, 202])
        self.flush(index)
        _, hits = self.search(index)
        self.assertEqual([hit["_id"] for hit in hits], ["good"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path(__file__).resolve().parents[1] / "target/debug/svindb")
    args = parser.parse_args()
    node = Node(args.binary.resolve())
    AuditRegressions.node = node
    try:
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(AuditRegressions))
    finally:
        node.close()
        print(f"Node stopped: pid={node.child.pid}, returncode={node.child.returncode}; temporary data removed.")
    if result.wasSuccessful():
        print(f"PASS: {result.testsRun} isolated HTTP audit regressions; binary={args.binary.resolve()}")
        return 0
    print("Node diagnostics:\n" + node.last_log[-12000:])
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
