#!/usr/bin/env python3
"""Benchmark process start to health, exact search and exact point-read readiness.

Example (no build is performed):
  python3 tools/bench_startup.py --binary /path/to/svindb --output /outside/repo/startup.json
Every repetition copies an isolated fixture before timing. OS caches are warm/uncontrolled;
this is not a cold-cache or production benchmark. Uses only Python's standard library.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import subprocess
import tempfile
import time


class BenchmarkError(RuntimeError):
    pass


def binary_sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def exact_total(reply, count):
    return isinstance(reply, dict) and reply.get("total") == {"value": count, "relation": "eq"}


def exact_get(reply, source):
    return isinstance(reply, dict) and reply.get("found") is True and reply.get("_source") == source


def exact_marker(reply, source):
    if not exact_total(reply, 1):
        return False
    hits = reply.get("hits")
    if isinstance(hits, dict):
        hits = hits.get("hits")
    return (isinstance(hits, list) and len(hits) == 1
            and hits[0].get("_id") == "0" and hits[0].get("_source") == source)


class Node:
    def __init__(self, binary, data, log_path, maintain=False, threads=2, sync_flush=True):
        self.port = None
        self.log_path = log_path
        self.log = log_path.open("wb")
        self.usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        self.started = time.monotonic()
        try:
            self.child = subprocess.Popen(
                [str(binary), "serve", str(data), "0", "--bind=127.0.0.1",
                 *(["--no-maintain"] if not maintain else []),
                 *(["--sync-flush"] if sync_flush else []), f"--threads={threads}"],
                env={**os.environ, "SVINDB_DISK_FLOOR_MB": "1"},
                stdout=self.log, stderr=subprocess.STDOUT,
            )
        except BaseException:
            self.log.close()
            raise

    def discover_port(self, deadline):
        while time.monotonic() < deadline:
            if self.child.poll() is not None:
                raise BenchmarkError(f"server exited {self.child.returncode}: {self.tail()}")
            for line in self.log_path.read_text(errors="replace").splitlines():
                if line.startswith("SVINDB_LISTEN "):
                    self.port = int(line.rsplit(":", 1)[1])
                    return
            time.sleep(.005)
        raise BenchmarkError(f"no SVINDB_LISTEN before deadline: {self.tail()}")

    def request(self, method, path, body=None, *, raw=False, timeout=10):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        try:
            connection.request(method, path, body if raw else (json.dumps(body) if body is not None else None),
                               {"Content-Type": "application/x-ndjson" if raw else "application/json",
                                "Connection": "close"})
            response = connection.getresponse()
            content = response.read()
            return response.status, json.loads(content) if content else {}
        finally:
            connection.close()

    def checked(self, method, path, body=None, **kwargs):
        status, reply = self.request(method, path, body, **kwargs)
        if status not in (200, 201):
            raise BenchmarkError(f"{method} {path}: HTTP {status}: {reply}")
        return reply

    def health(self, deadline):
        self.discover_port(deadline)
        while time.monotonic() < deadline:
            try:
                status, reply = self.request("GET", "/_health", timeout=min(1, max(.001, deadline-time.monotonic())))
                if status == 200 and reply.get("ok") is True:
                    return
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(.01)
        raise BenchmarkError("seed health deadline exceeded")

    def tail(self):
        return self.log_path.read_text(errors="replace")[-4000:]

    def stop(self, crash=False):
        if self.child.poll() is None:
            self.child.kill() if crash else self.child.terminate()
        try:
            self.child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.child.kill()
            self.child.wait(timeout=5)
        self.log.close()
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        return {
            "returncode": self.child.returncode,
            "user_cpu_seconds": usage.ru_utime - self.usage_before.ru_utime,
            "system_cpu_seconds": usage.ru_stime - self.usage_before.ru_stime,
            "input_blocks": usage.ru_inblock - self.usage_before.ru_inblock,
            "output_blocks": usage.ru_oublock - self.usage_before.ru_oublock,
            "resource_scope": "child lifetime through shutdown; includes readiness requests and shutdown, excludes fixture copying",
            "io_note": "getrusage block operations, OS-specific; zeros do not prove zero physical disk IO",
        }


def bulk(node, index, documents):
    body = "".join(json.dumps({"index": {"_id": str(identifier)}}) + "\n"
                   + json.dumps(source) + "\n" for identifier, source in documents)
    reply = node.checked("POST", f"/{index}/_bulk", body, raw=True)
    items = reply.get("items", [])
    if (reply.get("errors") is not False or len(items) != len(documents)
            or any(item.get("index", {}).get("status") not in (200, 201) for item in items)):
        raise BenchmarkError(f"bulk oracle failed: {reply}")


def source(ordinal, latest=False):
    return {"ordinal": ordinal, "message": "latesttoken" if latest else "basetoken", "revision": 2 if latest else 1}


def clone_fixture(seed, destination):
    # ID catalog stamps immutable head.tsv inode+mtime. Copying that file would
    # invalidate the catalog; linking mutable WAL/deletion files would contaminate
    # the seed. Preserve only immutable segment headers and copy everything else.
    def copy_file(source_path, target_path):
        if Path(source_path).name == "head.tsv":
            os.link(source_path, target_path)
            return target_path
        return shutil.copy2(source_path, target_path)
    shutil.copytree(seed, destination, copy_function=copy_file)


def disk_state(data):
    manifests = {}
    wal_bytes = 0
    catalogs = {}
    files = list(data.rglob("*"))
    for path in files:
        if path.is_file() and path.name == "catalog.bin":
            catalogs[str(path.relative_to(data))] = path.stat().st_size
        if path.is_file() and path.name == "segments.tsv":
            lines = path.read_text().splitlines()
            manifests[str(path.relative_to(data))] = len([line for line in lines if line and not line.startswith("#")])
        if path.is_file() and "wal" in path.relative_to(data).parts and path.suffix == ".log":
            wal_bytes += path.stat().st_size
    return {"segments_by_manifest": manifests, "catalog_file_bytes": catalogs, "wal_log_bytes": wal_bytes,
            "logical_file_bytes": sum(path.stat().st_size for path in files if path.is_file())}


def prepare(binary, root, docs, indexes, segments, pending, batch, deadline_seconds):
    data = root / "seed"
    node = Node(binary, data, root / "seed.log", maintain=True)
    expected = {}
    try:
        node.health(time.monotonic() + deadline_seconds)
        for number in range(indexes):
            name = f"startup-{number}"
            count = docs // indexes + (number < docs % indexes)
            expected[name] = {"count": count, "latest_source": source(0, True)}
            node.checked("PUT", f"/{name}", {"mappings": {"dynamic": "strict", "properties": {
                "ordinal": {"type": "long"}, "message": {"type": "text"}, "revision": {"type": "long"}}}})
            node.checked("PUT", f"/{name}/_settings", {
                "refresh_interval_ms": 0, "live_max_docs": 0, "flush_docs": max(1_000_000, docs + pending + 1),
                "flush_mb": 32, "l0_soft_segments": 512, "l0_hard_segments": 1024,
                "per_tier": 256, "merge_at_once": 256, "target_docs": 1_000_000})
            for segment in range(segments):
                begin, end = count * segment // segments, count * (segment + 1) // segments
                for start in range(begin, end, batch):
                    bulk(node, name, [(ordinal, source(ordinal, ordinal == 0 and not pending))
                                      for ordinal in range(start, min(end, start + batch))])
                node.checked("POST", f"/{name}/_flush", {})
            # Maintenance creates the supported ID catalog while high tier limits
            # preserve this fixture's exact segment count. Verify covered and tail IDs.
            disk_oracle = {"0": source(0, not pending), str(count - 1): source(count - 1, count == 1 and not pending)}
            verified = probe(node, "point_get", {name: {"point_documents": disk_oracle}},
                             time.monotonic() + deadline_seconds, .01)
            if not verified["passed"]:
                raise BenchmarkError(f"seed catalog/point readiness failed for {name}: {verified}; {node.tail()}")
            expected[name]["point_documents"] = {"0": source(0, True), str(count - 1): source(count - 1, count == 1)}
            if pending:
                # ACKs are durable by the server contract; abrupt kill avoids a shutdown flush.
                bulk(node, name, [(0, source(0, True))] + [(count + n, source(count + n)) for n in range(pending)])
                expected[name]["count"] += pending
        before = disk_state(data)
        actual = sorted(before["segments_by_manifest"].values())
        if actual != [segments] * indexes:
            raise BenchmarkError(f"fixture segment count mismatch: {before}")
        if pending and before["wal_log_bytes"] == 0:
            raise BenchmarkError("pending WAL fixture has no WAL file bytes")
    finally:
        node.stop(crash=True)
    return data, expected, before


def probe(node, kind, expected, deadline, interval):
    attempts = 0
    statuses = {}
    last = None
    while time.monotonic() < deadline:
        attempts += 1
        valid = True
        try:
            requests = [("GET", "/_health", None, lambda r: r.get("ok") is True)] if kind == "health" else []
            for name, oracle in expected.items():
                if kind == "point_get":
                    point_documents = oracle.get("point_documents", {"0": oracle.get("latest_source")})
                    for identifier, expected_source in point_documents.items():
                        requests.append(("GET", f"/{name}/_doc/{identifier}", None,
                                         lambda r, s=expected_source: exact_get(r, s)))
                elif kind == "search":
                    requests.extend([
                        ("POST", f"/{name}/_search", {"query": {"match_all": {}}, "size": 0, "track_total_hits": True},
                         lambda r, n=oracle["count"]: exact_total(r, n)),
                        ("POST", f"/{name}/_search", {"query": {"term": {"message": "latesttoken"}}, "size": 2, "track_total_hits": True},
                         lambda r, s=oracle["latest_source"]: exact_marker(r, s)),
                    ])
            for method, path, body, oracle in requests:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    valid = False
                    break
                status, reply = node.request(method, path, body, timeout=min(1, remaining))
                statuses[str(status)] = statuses.get(str(status), 0) + 1
                last = {"path": path, "status": status, "reply": reply}
                if status != 200 or not oracle(reply):
                    valid = False
                    # Retry 503 and stale/wrong successful responses, recording them as unready.
                    break
            if valid and time.monotonic() <= deadline:
                return {"passed": True, "seconds": time.monotonic() - node.started,
                        "attempts": attempts, "http_status_counts": statuses, "last_observation": last}
        except (OSError, http.client.HTTPException, ValueError) as error:
            last = {"transport_error": str(error)}
        time.sleep(min(interval, max(0, deadline - time.monotonic())))
    return {"passed": False, "seconds": None, "attempts": attempts,
            "http_status_counts": statuses, "last_observation": last}


def measure(binary, data, log, expected, deadline_seconds, interval, maintain=False):
    node = Node(binary, data, log, maintain=maintain)
    result = {"passed": False}
    try:
        deadline = node.started + deadline_seconds
        node.discover_port(deadline)
        result["listen_observed_seconds"] = time.monotonic() - node.started
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {kind: pool.submit(probe, node, kind, expected, deadline, interval)
                       for kind in ("health", "search", "point_get")}
            result["readiness"] = {kind: future.result() for kind, future in futures.items()}
        result["passed"] = all(stage["passed"] for stage in result["readiness"].values())
        result["all_correct_ready_seconds"] = max((stage["seconds"] or 0 for stage in result["readiness"].values())) if result["passed"] else None
    except Exception as error:
        result["error"] = str(error)
    finally:
        result["process_resources"] = node.stop()
        if not result["passed"]:
            result["log_tail"] = node.tail()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", default="exploratory", help="operator-supplied context, e.g. provisional-concurrent-builds or quiet-paired")
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--docs", type=positive, default=20_000, help="total base documents across indexes")
    parser.add_argument("--indexes", type=positive, nargs="+", default=[1, 4])
    parser.add_argument("--segments", type=positive, nargs="+", default=[1, 20], help="segments per index; base doc count held constant")
    parser.add_argument("--pending-docs", type=positive, default=1000, help="new unflushed docs per index, plus one replacement")
    parser.add_argument("--repeats", type=positive, default=3)
    parser.add_argument("--maintenance", choices=("disabled", "enabled"), default="disabled",
                        help="restart maintenance; seed enables maintenance with high merge limits to prepare valid ID catalog")
    parser.add_argument("--deadline", type=positive, default=10, help="seconds per restart, all probes share deadline")
    parser.add_argument("--batch", type=positive, default=1000)
    parser.add_argument("--poll-ms", type=positive, default=10)
    args = parser.parse_args()
    binary = args.binary.resolve()
    output = args.output.resolve()
    checkout = Path(__file__).resolve().parents[1]
    if output.is_relative_to(checkout):
        parser.error("--output must be outside checkout")
    if args.docs // max(args.indexes) < max(args.segments):
        parser.error("each index needs at least one document per segment")
    report = {"schema_version": 1, "started_at": datetime.now(timezone.utc).isoformat(),
              "binary": str(binary), "binary_sha256": binary_sha256(binary),
              "platform": platform.platform(), "python": platform.python_version(), "logical_cpus": os.cpu_count(),
              "benchmark_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "parameters": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
              "cache_state": "warm/uncontrolled OS cache; process restarts after fixture creation/copy; no cache purge",
              "timing": "monotonic from before Popen through first fully correct probe pass; separate concurrent health/search/GET probes; includes probe overhead and scheduling; not cold-cache latency",
              "fixture_clone": "hardlink immutable head.tsv to preserve inode-stamped ID catalog; copy every other file including mutable WAL/deletion state; outside startup timing",
              "fixture_catalog": "normal maintenance during seed; per_tier=256/merge_at_once=256/target_docs=1000000; exact first+last disk GET before pending WAL; manifest segment counts checked",
              "fixture_stop": "SIGKILL after acknowledged writes, including flushed fixtures; no graceful shutdown flush",
              "oracle": "every index exact match_all count; unique latest marker hit with exact _id/_source; GET id=0 latest and last base disk ID with exact _source; health alone never ready",
              "scope": "isolated loopback local subprocess; maintenance mode explicit in parameters; 2 server threads; no Rust build; no power-loss durability proof",
              "cases": [], "passed": False}
    try:
        with tempfile.TemporaryDirectory(prefix="svindb-startup-bench-") as temporary:
            root = Path(temporary)
            for indexes in args.indexes:
                for segments in args.segments:
                    for pending in (0, args.pending_docs):
                        case_root = root / f"i{indexes}-s{segments}-w{pending}"
                        case_root.mkdir()
                        print(f"Preparing indexes={indexes} segments/index={segments} pending/index={pending}", flush=True)
                        seed, expected, state = prepare(binary, case_root, args.docs, indexes, segments, pending, args.batch, args.deadline)
                        case = {"indexes": indexes, "segments_per_index": segments,
                                "pending_new_docs_per_index": pending, "pending_replacements_per_index": int(bool(pending)),
                                "expected": expected, "seed_disk_state": state, "runs": []}
                        report["cases"].append(case)
                        for repeat in range(args.repeats):
                            data = case_root / f"run-{repeat}"
                            clone_fixture(seed, data)
                            run = measure(binary, data, case_root / f"run-{repeat}.log", expected, args.deadline, args.poll_ms / 1000, args.maintenance == "enabled")
                            run["repeat"] = repeat + 1
                            case["runs"].append(run)
                            shutil.rmtree(data)
                            print(json.dumps({"indexes": indexes, "segments": segments, "pending": pending,
                                              "repeat": repeat + 1, "passed": run["passed"],
                                              "ready_seconds": run.get("all_correct_ready_seconds")}), flush=True)
            report["passed"] = all(run["passed"] for case in report["cases"] for run in case["runs"])
    except KeyboardInterrupt:
        report["error"] = "interrupted"
    except Exception as error:
        report["error"] = str(error)
    finally:
        report["temporary_data_removed"] = True
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
