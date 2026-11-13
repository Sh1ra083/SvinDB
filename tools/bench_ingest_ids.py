#!/usr/bin/env python3
"""Measure isolated ES NDJSON HTTP ingestion without supplied document IDs.

Run: cargo build -p svindb-server
     python3 tools/bench_ingest_ids.py --output ingest-no-id.json
This is a local debug-build measurement, not a production throughput claim.
Generated IDs are checked exactly using a temporary SQLite UNIQUE index.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import http.client
import json
from pathlib import Path
import sqlite3
import time

from audit_regressions import Node


def positive(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docs", type=positive, default=200_000)
    parser.add_argument("--batch", type=positive, default=1_000)
    parser.add_argument("--binary", type=Path, default=Path(__file__).resolve().parents[1] / "target/debug/svindb")
    parser.add_argument("--output", type=Path, default=Path("ingest-no-id.json"))
    args = parser.parse_args()
    binary = args.binary.resolve()
    digest = hashlib.sha256()
    with binary.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "binary": str(binary), "binary_sha256": digest.hexdigest(),
        "build_profile": "debug" if binary.parent.name == "debug" else "unverified",
        "scope": "local HTTP correctness and timing; no production throughput or before/after claim",
        "docs": args.docs, "batch_size": args.batch,
        "format": "ES NDJSON: index action without _id; source has ordinal and message, no id",
        "client_concurrency": 1, "http_connection_reuse": False, "id_uniqueness_method": "temporary SQLite PRIMARY KEY, exact",
        "settings": {"refresh_interval_ms": 0, "live_max_docs": 0, "flush_mb": 32},
        "passed": False,
    }
    node = None
    ledger = None
    try:
        node = Node(binary)
        report["node_pid"] = node.child.pid
        def request(method, path, body=None, ndjson=False):
            encoded = body if ndjson else (None if body is None else json.dumps(body))
            connection = http.client.HTTPConnection("127.0.0.1", node.port, timeout=180)
            try:
                connection.request(method, path, encoded,
                                   {"Content-Type": "application/x-ndjson" if ndjson else "application/json",
                                    "Connection": "close"})
                reply = connection.getresponse()
                payload = json.loads(reply.read())
                if reply.status != 200:
                    raise RuntimeError(f"{method} {path}: HTTP {reply.status}: {payload}")
                return payload
            finally:
                connection.close()

        request("PUT", "/without-id", {
            "mappings": {"dynamic": "strict", "properties": {
                "ordinal": {"type": "long"}, "message": {"type": "text"},
            }},
        })
        request("PUT", "/without-id/_settings", report["settings"])
        ledger = sqlite3.connect(str(Path(node.temp.name) / "ids.sqlite"))
        ledger.execute("PRAGMA cache_size=-4096")
        ledger.execute("CREATE TABLE generated_ids (id TEXT PRIMARY KEY) WITHOUT ROWID")
        started = time.perf_counter()
        request_seconds = 0.0
        batches = 0
        acknowledged = 0
        for start in range(0, args.docs, args.batch):
            stop = min(args.docs, start + args.batch)
            body = "".join('{"index":{}}\n' + json.dumps({
                "ordinal": ordinal, "message": "reproducible generated document identifier benchmark",
            }, separators=(",", ":")) + "\n" for ordinal in range(start, stop))
            began_request = time.perf_counter()
            result = request("POST", "/without-id/_bulk", body, ndjson=True)
            request_seconds += time.perf_counter() - began_request
            if result.get("errors") is not False:
                raise RuntimeError(f"bulk errors in batch {batches}: {result}")
            items = result.get("items", [])
            if len(items) != stop - start:
                raise RuntimeError(f"batch {batches}: expected {stop - start} item responses, got {len(items)}")
            generated = []
            for item in items:
                detail = item.get("index", {})
                identifier = detail.get("_id")
                if detail.get("status") not in (200, 201) or not isinstance(identifier, str) or not identifier:
                    raise RuntimeError(f"invalid index response: {item}")
                generated.append((identifier,))
            try:
                with ledger:
                    ledger.executemany("INSERT INTO generated_ids(id) VALUES (?)", generated)
            except sqlite3.IntegrityError as error:
                raise RuntimeError(f"duplicate generated _id in batch {batches}") from error
            batches += 1
            acknowledged += len(items)
            if batches % 20 == 0 or acknowledged == args.docs:
                print(f"Acknowledged {acknowledged}/{args.docs}; batches={batches}; elapsed={time.perf_counter() - started:.3f}s", flush=True)
        ingestion_seconds = time.perf_counter() - started
        unique = ledger.execute("SELECT count(*) FROM generated_ids").fetchone()[0]
        before_flush = time.perf_counter()
        request("POST", "/without-id/_flush", {})
        flush_seconds = time.perf_counter() - before_flush
        search = request("POST", "/without-id/_search", {
            "query": {"match_all": {}}, "size": 0, "track_total_hits": True, "_source": False,
        })
        total = search.get("total")
        if total != {"value": args.docs, "relation": "eq"}:
            raise RuntimeError(f"final search exact count mismatch: expected {args.docs}, response={search}")
        hits = search.get("hits")
        if hits != [] or unique != args.docs or acknowledged != args.docs:
            raise RuntimeError(f"verification mismatch: hits={hits}, unique={unique}, acknowledged={acknowledged}")
        report.update({
            "passed": True, "batches": batches, "acknowledged_docs": acknowledged,
            "unique_generated_ids": unique, "final_search_total": total,
            "ingestion_elapsed_seconds": ingestion_seconds,
            "ingestion_docs_per_second": args.docs / ingestion_seconds,
            "http_ingestion_request_seconds": request_seconds,
            "final_flush_seconds": flush_seconds,
            "ingestion_plus_flush_seconds": ingestion_seconds + flush_seconds,
            "ingestion_plus_flush_docs_per_second": args.docs / (ingestion_seconds + flush_seconds),
            "timing_includes": "batch generation, HTTP request/response, response validation, exact SQLite ID uniqueness checks; final flush reported separately",
        })
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        if ledger is not None:
            ledger.close()
        if node is not None:
            node.close()
            report.update({"node_stopped": node.child.poll() is not None,
                           "node_returncode": node.child.returncode, "temporary_data_removed": True})
            if not report["passed"]:
                report["node_log_tail"] = node.last_log[-8000:]
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
        print(json.dumps(report, indent=2, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
