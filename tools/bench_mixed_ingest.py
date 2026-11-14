#!/usr/bin/env python3
"""Bounded continuous ingest + dashboards + real maintenance, stdlib only.

python3 tools/bench_mixed_ingest.py --binary /path/to/svindb --output /outside/repo/mixed.json
Default: 2/4 writers x batch 500/1000, four readers, up to 100k deterministic
operations/case in a 15s admission window. No build, cache purge or production data.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import http.client
import json
from pathlib import Path
import statistics
import tempfile
import threading
import time

from bench_startup import Node, BenchmarkError, binary_sha256, positive

SETTINGS = {"refresh_interval_ms": 200, "live_max_docs": 10000,
            "flush_docs": 1000, "flush_mb": 32, "wal_group_commit_ms": 2,
            "target_docs": 8000, "merge_at_once": 4, "per_tier": 4,
            "floor_mb": 1, "merge_threads": 1, "l0_soft_segments": 12,
            "l0_hard_segments": 32}
QUERIES = [
    ("recent", {"query": {"match_all": {}}, "size": 10, "track_total_hits": True}),
    ("service", {"query": {"term": {"tag": "group3"}}, "size": 10, "track_total_hits": True}),
    ("range", {"query": {"range": {"ordinal": {"gte": 0, "lt": 50000}}}, "size": 0, "track_total_hits": True}),
    ("dashboard", {"query": {"match_all": {}}, "size": 0, "track_total_hits": True,
                   "aggs": {"tags": {"terms": {"field": "tag", "size": 8}}}}),
]


def source(identifier, revision=2, payload_bytes=1024):
    return {"ordinal": identifier, "revision": revision, "tag": f"group{identifier % 16}",
            "message": ("common searchable dashboard service request completed " + "payload " * ((payload_bytes+7)//8))[:payload_bytes]}


def body_for(start, stop, revision=2, payload_bytes=1024):
    return "".join(json.dumps({"index": {"_id": f"d{i}"}}, separators=(",", ":")) + "\n"
                   + json.dumps(source(i, revision, payload_bytes), separators=(",", ":")) + "\n" for i in range(start, stop))


def validate_bulk(status, reply, count):
    if status == 429:
        return False
    if status != 200 or not isinstance(reply, dict):
        raise BenchmarkError(f"permanent bulk HTTP {status}: {reply}")
    items = reply.get("items", [])
    if (reply.get("errors") is not False or len(items) != count
            or any(item.get("index", {}).get("status") not in (200, 201) for item in items)):
        raise BenchmarkError(f"bulk partial/item failure: {reply}")
    return True


def validate_query(reply, maximum, aggregation=False, payload_bytes=1024):
    if not isinstance(reply, dict) or reply.get("partial") is not False or reply.get("complete") is not True:
        raise BenchmarkError(f"incomplete search: {reply}")
    total = reply.get("total", {})
    if not aggregation and (total.get("relation") != "eq" or not isinstance(total.get("value"), int) or not 0 <= total["value"] <= maximum):
        raise BenchmarkError(f"invalid total: {reply}")
    hits = reply.get("hits", [])
    if isinstance(hits, dict):
        hits = hits.get("hits", [])
    for hit in hits:
        doc = hit.get("_source", {})
        ordinal, revision = doc.get("ordinal"), doc.get("revision")
        if not isinstance(ordinal, int) or revision not in (1, 2) or doc != source(ordinal, revision, payload_bytes) or hit.get("_id") != f"d{ordinal}":
            raise BenchmarkError(f"invalid source: {hit}")
    if aggregation:
        tags = reply.get("aggs", {}).get("tags", {})
        buckets = tags.get("buckets")
        if (not isinstance(buckets, list) or len(buckets) > 8
                or len({b.get("key") for b in buckets}) != len(buckets)
                or any(b.get("key") not in {f"group{i}" for i in range(16)} or not isinstance(b.get("doc_count"), int) or b["doc_count"] < 0 for b in buckets)):
            raise BenchmarkError(f"invalid low-cardinality dashboard: {reply}")
        other = tags.get("sum_other_doc_count")
        if (tags.get("doc_count_error_upper_bound") != 0 or not isinstance(other, int) or other < 0
                or sum(b["doc_count"] for b in buckets)+other > maximum):
            raise BenchmarkError(f"inexact aggregation: {reply}")


def summary(values):
    ordered = sorted(values)
    return {"count": len(ordered), "p50_ms": statistics.median(ordered) if ordered else None,
            "p99_ms": ordered[min(len(ordered)-1, int(.99 * (len(ordered)-1)))] if ordered else None,
            "max_ms": max(ordered) if ordered else None}


class Client:
    def __init__(self, port):
        self.connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)

    def call(self, method, path, body=None, text=False):
        raw = body if isinstance(body, str) else json.dumps(body) if body is not None else None
        self.connection.request(method, path, raw, {"Content-Type": "application/x-ndjson" if isinstance(body, str) else "application/json"})
        response = self.connection.getresponse()
        data = response.read().decode()
        return response.status, data if text else json.loads(data)

    def close(self):
        self.connection.close()


def metrics_selected(raw):
    return [line for line in raw.splitlines() if not line.startswith("#") and (
        ('svindb_write_lock_wait_ms' in line and 'lock="ingest"' in line)
        or 'svindb_process_resident_bytes ' in line)]


def call_before(node, deadline, method, path, body=None):
    remaining = deadline-time.monotonic()
    if remaining <= 0:
        raise BenchmarkError("HTTP readiness deadline exceeded")
    return node.request(method, path, body, timeout=min(3, remaining))


def settled(health):
    compact = health.get("compaction", {})
    watermarks = health.get("watermarks", {})
    return (health.get("ok") is True and health.get("buffered") == 0
            and health.get("flush_in_progress") is False
            and compact.get("pending_schedulable") == 0 and compact.get("running_segments") == 0
            and bool(watermarks) and watermarks.get("ready") == watermarks.get("ack"))


def one(binary, root, writers, batch_size, args):
    report = {"writers": writers, "readers": 4, "batch_size": batch_size, "settings": SETTINGS,
              "passed": False, "errors": [], "stats_trend": [], "queries": [], "batches": []}
    # Identical bodies/IDs are prepared before any node or timed workload.
    batches = [(at, min(at+batch_size, args.docs), body_for(at, min(at+batch_size, args.docs), payload_bytes=args.payload_bytes))
               for at in range(0, args.docs, batch_size)]
    report["client_precomputed_corpus_bytes"] = sum(len(body.encode()) for _, _, body in batches)
    report["corpus_sha256"] = hashlib.sha256("".join(body for _, _, body in batches).encode()).hexdigest()
    node = Node(binary, root / "data", root / "node.log", maintain=True, threads=10, sync_flush=False)
    stop = threading.Event()
    guard = threading.Lock()
    try:
        node.health(time.monotonic() + 20)
        node.checked("PUT", "/logs", {"mappings": {"dynamic": "strict", "properties": {
            "ordinal": {"type": "long"}, "revision": {"type": "long"},
            "tag": {"type": "keyword"}, "message": {"type": "text"}}}})
        node.checked("PUT", "/logs/_settings", SETTINGS)
        seed_count = min(1000, args.docs)
        validate_bulk(*node.request("POST", "/logs/_bulk", body_for(0, seed_count, 1, args.payload_bytes), raw=True), seed_count)
        flush_deadline = time.monotonic() + args.settle_seconds
        while time.monotonic() < flush_deadline:
            code, payload = call_before(node, flush_deadline, "POST", "/logs/_flush", {})
            if code == 200:
                break
            if code not in (202, 429):
                raise BenchmarkError(f"seed flush: {code} {payload}")
            time.sleep(.05)
        else:
            raise BenchmarkError("seed flush deadline")
        # Seed correctness is required before simultaneous workload starts.
        code, payload = node.request("POST", "/logs/_search", {"query": {"match_all": {}}, "size": 0, "track_total_hits": True})
        if code != 200 or payload.get("total") != {"value": seed_count, "relation": "eq"}:
            raise BenchmarkError(f"seed count: {code} {payload}")
        work_index = 0
        active_writers = writers
        ready = threading.Barrier(writers + 4 + 2)
        started = [None]
        acknowledged = set()

        def error(message):
            with guard:
                report["errors"].append(str(message))
            stop.set()

        def writer(worker):
            nonlocal work_index, active_writers
            client = Client(node.port)
            ready.wait()
            try:
                while not stop.is_set() and time.monotonic() < started[0] + args.seconds:
                    with guard:
                        if work_index >= len(batches):
                            break
                        selected = batches[work_index]
                        work_index += 1
                    at, end, body = selected
                    record = {"worker": worker, "start_id": at, "end_id": end, "attempts": [], "acknowledged": False,
                              "start_seconds": time.monotonic()-started[0]}
                    begin = time.monotonic()
                    retry_deadline = min(started[0]+args.seconds+args.retry_seconds, begin+args.retry_seconds)
                    try:
                        while time.monotonic() < retry_deadline and not stop.is_set():
                            call_started = time.monotonic()
                            code, reply = client.call("POST", "/logs/_bulk", body)
                            record["attempts"].append({"status": code, "latency_ms": (time.monotonic()-call_started)*1000})
                            if validate_bulk(code, reply, end-at):
                                record["acknowledged"] = True
                                with guard:
                                    acknowledged.update(range(at, end))
                                break
                            record.setdefault("retry_bodies", []).append(reply)
                            time.sleep(min(.1, max(0, retry_deadline-time.monotonic())))
                        if not record["acknowledged"]:
                            raise BenchmarkError(f"bulk retry deadline IDs {at}:{end}")
                    finally:
                        record["end_seconds"] = time.monotonic()-started[0]
                        record["end_to_end_ms"] = (time.monotonic()-begin)*1000
                        with guard:
                            report["batches"].append(record)
            except Exception as exc:
                error(exc)
            finally:
                client.close()
                with guard:
                    active_writers -= 1

        def reader(worker):
            client = Client(node.port)
            ready.wait()
            ordinal = worker
            try:
                while not stop.is_set():
                    with guard:
                        running = active_writers > 0
                    if not running:
                        break
                    name, query = QUERIES[ordinal % len(QUERIES)]
                    begin = time.monotonic()
                    code, reply = client.call("POST", "/logs/_search", query)
                    record = {"worker": worker, "name": name, "status": code,
                              "start_seconds": begin-started[0], "latency_ms": (time.monotonic()-begin)*1000}
                    with guard:
                        report["queries"].append(record)
                    if code == 200:
                        validate_query(reply, max(seed_count, args.docs), name == "dashboard", args.payload_bytes)
                    elif code == 429:
                        time.sleep(.05)
                    else:
                        raise BenchmarkError(f"query {name}: HTTP {code}: {reply}")
                    ordinal += 1
            except Exception as exc:
                error(exc)
            finally:
                client.close()

        def sampler():
            client = Client(node.port)
            ready.wait()
            try:
                while not stop.is_set():
                    with guard:
                        running = active_writers
                    code, health = client.call("GET", "/logs/_health")
                    if code != 200:
                        raise BenchmarkError(f"sampler health {code}: {health}")
                    code, metrics = client.call("GET", "/logs/_metrics", text=True)
                    report["stats_trend"].append({"seconds": time.monotonic()-started[0],
                                                  "active_writers": running, "health": health,
                                                  "metrics_status": code,
                                                  "ingest_lock_wait_and_rss": metrics_selected(metrics) if code == 200 else []})
                    if not running:
                        break
                    stop.wait(.25)
            except Exception as exc:
                error(exc)
            finally:
                client.close()

        with ThreadPoolExecutor(max_workers=writers+5) as pool:
            futures = [pool.submit(writer, i) for i in range(writers)] + [pool.submit(reader, i) for i in range(4)] + [pool.submit(sampler)]
            started[0] = time.monotonic()
            ready.wait()
            for future in futures:
                future.result()
        report["mixed_elapsed_seconds"] = time.monotonic()-started[0]
        ack_elapsed = max((b["end_seconds"] for b in report["batches"] if b["acknowledged"]), default=0)
        report["acknowledged_document_operations"] = len(acknowledged)
        report["ack_elapsed_seconds"] = ack_elapsed
        report["workload_shape"] = "burst-cap-exhausted" if work_index >= len(batches) and ack_elapsed < args.seconds else "bounded-admission-window"
        merged_samples = [s["seconds"] for s in report["stats_trend"] if s["active_writers"] > 0 and s["health"].get("compaction_progress", {}).get("completed_sources", 0) > 0]
        report["observed_merge_ingest_overlap_seconds"] = max(0, ack_elapsed-min(merged_samples)) if merged_samples else 0
        report["minimum_overlap_seconds"] = args.min_overlap_seconds
        report["minimum_overlap_observed"] = report["observed_merge_ingest_overlap_seconds"] >= args.min_overlap_seconds
        report["ack_docs_per_second"] = len(acknowledged)/ack_elapsed if ack_elapsed else 0
        report["query_latency_all_attempts"] = summary([q["latency_ms"] for q in report["queries"]])
        report["query_latency_success"] = summary([q["latency_ms"] for q in report["queries"] if q["status"] == 200])
        report["query_latency_by_shape"] = {name: summary([q["latency_ms"] for q in report["queries"] if q["name"] == name and q["status"] == 200]) for name, _ in QUERIES}
        metric_lines = [line for sample in report["stats_trend"] for line in sample["ingest_lock_wait_and_rss"]]
        report["ingest_lock_wait_metrics_available"] = any("svindb_write_lock_wait_ms" in line for line in metric_lines)
        rss = [float(line.rsplit(" ", 1)[1]) for line in metric_lines if line.startswith("svindb_process_resident_bytes ")]
        report["sampled_server_rss_peak_bytes"] = max(rss) if rss else None
        report["rss_scope"] = "server process gauge sampled every ~250ms; sampled peak, excludes Python precomputed corpus; not lifetime peak RSS"
        report["query_429"] = sum(q["status"] == 429 for q in report["queries"])
        report["write_429"] = sum(a["status"] == 429 for b in report["batches"] for a in b["attempts"])
        report["write_attempt_latency"] = summary([a["latency_ms"] for b in report["batches"] for a in b["attempts"]])
        report["batch_end_to_end_latency"] = summary([b["end_to_end_ms"] for b in report["batches"]])
        if report["errors"]:
            raise BenchmarkError("mixed workload failed; inspect errors and raw attempts")
        settle_started = time.monotonic()
        deadline = settle_started+args.settle_seconds
        report["flush_attempts"] = []
        while time.monotonic() < deadline:
            code, reply = call_before(node, deadline, "POST", "/logs/_flush", {})
            report["flush_attempts"].append({"status": code, "reply": reply})
            if code == 200:
                break
            if code not in (202, 429):
                raise BenchmarkError(f"final flush {code}: {reply}")
            time.sleep(.1)
        else:
            raise BenchmarkError("final flush deadline")
        expected_ids = set(range(seed_count)) | acknowledged
        sample_ids = sorted({0, seed_count-1, *sorted(acknowledged)[::max(1, len(acknowledged)//20)], max(expected_ids)})
        report["expected_final_count"] = len(expected_ids)
        stable = 0
        while time.monotonic() < deadline:
            code, count = call_before(node, deadline, "POST", "/logs/_search", {"query": {"match_all": {}}, "size": 0, "track_total_hits": True})
            code_h, health = call_before(node, deadline, "GET", "/logs/_health")
            if code == 200:
                validate_query(count, len(expected_ids), payload_bytes=args.payload_bytes)
            report["settlement_trend"] = report.get("settlement_trend", []) + [{"seconds": time.monotonic()-started[0], "health": health}]
            report["final_health"] = health
            correct = code == 200 and count.get("total") == {"value": len(expected_ids), "relation": "eq"} and code_h == 200 and settled(health)
            if correct:
                samples = []
                for identifier in sample_ids:
                    status, reply = call_before(node, deadline, "GET", f"/logs/_doc/d{identifier}")
                    expected = source(identifier, 2 if identifier in acknowledged else 1, args.payload_bytes)
                    samples.append({"id": identifier, "status": status, "reply": reply})
                    correct = correct and status == 200 and reply.get("found") is True and reply.get("_source") == expected
                report["final_point_samples"] = samples
            stable = stable + 1 if correct else 0
            if stable >= 2:
                report["final_count"] = count["total"]
                break
            time.sleep(.1)
        else:
            raise BenchmarkError("exact count/source + quiescent maintenance readiness deadline")
        report["settle_elapsed_seconds"] = time.monotonic()-settle_started
        report["write_start_to_correct_settled_seconds"] = time.monotonic()-started[0]
        report["merge_running_samples_during_ingest"] = sum(sample["active_writers"] > 0 and sample["health"].get("compaction", {}).get("running_segments", 0) > 0 for sample in report["stats_trend"])
        progress = report["final_health"].get("compaction_progress", {})
        report["completed_merge_input_bytes"] = progress.get("completed_input_bytes")
        report["completed_merge_sources"] = progress.get("completed_sources")
        # jobs includes catalog-only work, so require completed input sources too.
        report["completed_maintenance_jobs"] = progress.get("jobs")
        report["merge_overlap_observed"] = any(s["active_writers"] > 0 and s["health"].get("compaction_progress", {}).get("completed_sources", 0) >= 4 for s in report["stats_trend"])
        if progress.get("completed_sources", 0) < 8 or progress.get("completed_input_bytes", 0) <= 0 or not report["merge_overlap_observed"]:
            raise BenchmarkError("fixture did not prove repeated real merges overlapping active ingest")
        if not any(q["status"] == 200 for q in report["queries"]):
            raise BenchmarkError("no successful dashboards during ingest")
        report["passed"] = True
    except Exception as exc:
        report["errors"].append(str(exc))
    finally:
        stop.set()
        report["process_resources"] = node.stop()
        if not report["passed"]:
            report["log_tail"] = node.tail()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--label", default="exploratory")
    parser.add_argument("--writers", nargs="+", type=positive, default=[2, 4])
    parser.add_argument("--batch", nargs="+", type=positive, default=[500, 1000])
    parser.add_argument("--docs", type=positive, default=100000)
    parser.add_argument("--payload-bytes", type=positive, default=1024, help="ASCII message bytes/document; client corpus generated before timer")
    parser.add_argument("--min-overlap-seconds", type=positive, default=3, help="reported classification threshold; short cap bursts are labelled, never called steady capacity")
    parser.add_argument("--seconds", type=positive, default=15)
    parser.add_argument("--retry-seconds", type=positive, default=8)
    parser.add_argument("--settle-seconds", type=positive, default=20)
    args = parser.parse_args()
    if args.payload_bytes < 64:
        parser.error("--payload-bytes must be at least 64")
    output = args.output.resolve()
    if output.is_relative_to(Path(__file__).resolve().parents[1]):
        parser.error("reports must be outside checkout")
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "binary": str(args.binary.resolve()),
              "binary_sha256": binary_sha256(args.binary.resolve()),
              "script_sha256": binary_sha256(Path(__file__)),
              "parameters": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
              "scope": "warm/uncontrolled OS cache; isolated local maintenance-enabled node,10 server threads; four readers plus one stats/metrics sampler; no build; provisional until quiet paired comparison",
              "latency": "client wall clock; successful queries and all attempts separately; bulk attempt and end-to-end including retries separately",
              "metrics": "server Prometheus ingest-lock histogram and process RSS where available; getrusage child CPU through shutdown; block counters OS-specific, zero is not zero physical IO",
              "passed": False, "cases": []}
    try:
        with tempfile.TemporaryDirectory(prefix="svindb-mixed-ingest-") as temporary:
            for writers in args.writers:
                for batch in args.batch:
                    root = Path(temporary)/f"w{writers}-b{batch}"
                    root.mkdir()
                    case = one(args.binary.resolve(), root, writers, batch, args)
                    report["cases"].append(case)
                    print(json.dumps({k: case.get(k) for k in ("writers", "batch_size", "passed", "ack_docs_per_second", "write_429", "completed_merge_sources", "errors")}), flush=True)
            report["passed"] = all(case["passed"] for case in report["cases"])
    except Exception as exc:
        report["error"] = str(exc)
    finally:
        report["temporary_data_removed"] = True
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2)+"\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
