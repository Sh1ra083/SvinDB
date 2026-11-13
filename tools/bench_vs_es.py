#!/usr/bin/env python3
"""SvinDB vs Elasticsearch on one machine: speed, size and resource use.

Only the Python standard library is needed. macOS and Linux are supported; on
macOS the resource counters come from ``proc_pid_rusage`` (exact CPU time, peak
physical footprint, bytes read and written to disk), on Linux from ``/proc``.

What it does
------------
1. Generates a deterministic synthetic corpus (no real data) as Elasticsearch
   ``_bulk`` NDJSON. The same file is sent to both engines.
2. Starts both servers itself, so it can read their resource counters.
3. Ingests the corpus, then measures: time to searchable, disk size, CPU time,
   peak memory, bytes written.
4. Runs a fixed set of query families one request at a time (single
   connection, query variants rotated, Elasticsearch request cache disabled)
   and records latency percentiles and server CPU time per request.
5. Checks that both engines return the same answer (hit counts, sums,
   buckets). A speed number is only reported next to its correctness check.
6. Runs a mixed multi-client load, then restarts both servers and measures the
   time to the first answered query.

Nothing is tuned for either side beyond what is printed in the report header.
"""
import argparse
import ctypes
import ctypes.util
import http.client
import json
import math
import os
import platform
import queue
import random
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

# --------------------------------------------------------------------------
# Corpus
# --------------------------------------------------------------------------

CONSONANTS = "bcdfghjklmnprstvz"
VOWELS = "aeiou"
SYLLABLES = [c + v for c in CONSONANTS for v in VOWELS]  # 85 syllables
VOCAB_SIZE = 30000


def make_vocab(size=VOCAB_SIZE):
    base = len(SYLLABLES)
    words = []
    for i in range(size):
        n = i + base  # always at least two syllables
        parts = []
        while n:
            parts.append(SYLLABLES[n % base])
            n //= base
        words.append("".join(reversed(parts)))
    return words


def zipf_cumulative(size, s=1.0):
    total, out = 0.0, []
    weights = [1.0 / ((r + 1) ** s) for r in range(size)]
    for w in weights:
        total += w
        out.append(total)
    return out, weights


def pick_ranks_for_df(weights, mean_len, fractions):
    """Rank whose analytic document frequency is closest to each fraction."""
    total = sum(weights)
    df = [1.0 - (1.0 - w / total) ** mean_len for w in weights]
    return {name: min(range(len(df)), key=lambda r: abs(df[r] - f)) for name, f in fractions.items()}


def generate_corpus(path, docs, seed):
    """Writes ES _bulk NDJSON; returns the metadata the query set needs."""
    rng = random.Random(seed)
    vocab = make_vocab()
    cum, weights = zipf_cumulative(len(vocab))
    categories = [f"cat{n:03d}" for n in range(100)]
    cat_cum, _ = zipf_cumulative(len(categories), 0.8)
    authors = 50_000
    days = [time.strftime("%Y-%m-%dT", time.gmtime(1_640_995_200 + d * 86400)) for d in range(730)]
    rank_of = {w: r for r, w in enumerate(vocab)}
    bigrams = []
    mean_len = 42
    with open(path, "w", encoding="utf-8") as out:
        for i in range(docs):
            title = rng.choices(vocab, cum_weights=cum, k=rng.randint(3, 6))
            body = rng.choices(vocab, cum_weights=cum, k=rng.randint(25, 60))
            doc = {
                "title": " ".join(title),
                "body": " ".join(body),
                "cat": rng.choices(categories, cum_weights=cat_cum)[0],
                "author": f"user{rng.randrange(authors):05d}",
                "views": int(rng.lognormvariate(4.0, 1.6)),
                "price": round(rng.uniform(1, 500), 2),
                "ts": f"{days[rng.randrange(730)]}{rng.randrange(24):02d}:{rng.randrange(60):02d}:{rng.randrange(60):02d}Z",
            }
            if i < 4000:
                for a, b in zip(body, body[1:]):
                    if 300 <= rank_of[a] <= 4000 and 300 <= rank_of[b] <= 4000 and a != b:
                        bigrams.append((a, b))
            out.write('{"index":{"_id":"d%d"}}\n' % i)
            out.write(json.dumps(doc, separators=(",", ":")))
            out.write("\n")
    picks = pick_ranks_for_df(
        weights, mean_len, {"frequent": 0.30, "medium": 0.01, "rare": 0.0002}
    )
    return vocab, picks, bigrams


def term_variants(vocab, rank, n=5):
    """Neighbouring ranks have nearly the same frequency; they defeat caching."""
    lo = max(0, rank - 2)
    return [vocab[r] for r in range(lo, lo + n)]


# --------------------------------------------------------------------------
# Resource counters
# --------------------------------------------------------------------------


class Counters:
    """Cumulative CPU / disk counters and current memory of one process."""

    def __init__(self):
        self.mac = sys.platform == "darwin"
        if self.mac:
            libc = ctypes.CDLL(ctypes.util.find_library("c"))
            self.libc = libc

            class TB(ctypes.Structure):
                _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]

            tb = TB()
            libc.mach_timebase_info(ctypes.byref(tb))
            self.scale = tb.numer / tb.denom
            names = (
                "user_time system_time pkg_idle_wkups interrupt_wkups pageins wired_size "
                "resident_size phys_footprint proc_start_abstime proc_exit_abstime "
                "child_user_time child_system_time child_pkg_idle_wkups child_interrupt_wkups "
                "child_pageins child_elapsed_abstime diskio_bytesread diskio_byteswritten "
                "cpu_qos_default cpu_qos_maintenance cpu_qos_background cpu_qos_utility "
                "cpu_qos_legacy cpu_qos_user_initiated cpu_qos_user_interactive "
                "billed_system_time serviced_system_time logical_writes "
                "lifetime_max_phys_footprint instructions cycles billed_energy "
                "serviced_energy interval_max_phys_footprint runnable_time"
            ).split()
            self.RU = type(
                "RU4",
                (ctypes.Structure,),
                {"_fields_": [("uuid", ctypes.c_uint8 * 16)] + [(n, ctypes.c_uint64) for n in names]},
            )

    def read(self, pid):
        if self.mac:
            r = self.RU()
            if self.libc.proc_pid_rusage(pid, 4, ctypes.byref(r)) != 0:
                return None
            return {
                "cpu_s": (r.user_time + r.system_time) * self.scale / 1e9,
                "rss": r.resident_size,
                "footprint": r.phys_footprint,
                "peak_footprint": r.lifetime_max_phys_footprint,
                "disk_read": r.diskio_bytesread,
                "disk_write": r.diskio_byteswritten,
            }
        try:
            stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            hz = os.sysconf("SC_CLK_TCK")
            cpu = (int(stat[11]) + int(stat[12])) / hz
            rss = int(stat[21]) * os.sysconf("SC_PAGE_SIZE")
            io = dict(l.split(": ") for l in Path(f"/proc/{pid}/io").read_text().splitlines())
            status = Path(f"/proc/{pid}/status").read_text()
            hwm = next((int(l.split()[1]) * 1024 for l in status.splitlines() if l.startswith("VmHWM")), rss)
            return {
                "cpu_s": cpu, "rss": rss, "footprint": rss, "peak_footprint": hwm,
                "disk_read": int(io["read_bytes"]), "disk_write": int(io["write_bytes"]),
            }
        except OSError:
            return None


def thread_count(pid):
    try:
        if sys.platform == "darwin":
            out = subprocess.check_output(["ps", "-M", "-p", str(pid)], text=True)
            return max(0, len(out.strip().splitlines()) - 1)
        return len(os.listdir(f"/proc/{pid}/task"))
    except (OSError, subprocess.CalledProcessError):
        return None


class Phase:
    """Measures one engine process across a block of work."""

    def __init__(self, counters, pid):
        self.c, self.pid = counters, pid

    def __enter__(self):
        self.t0 = time.perf_counter()
        self.a = self.c.read(self.pid)
        self.peak_rss = self.a["rss"]
        self.peak_fp = self.a["footprint"]
        self.samples = []
        self.stop = threading.Event()
        self.th = threading.Thread(target=self._sample, daemon=True)
        self.th.start()
        return self

    def _sample(self):
        while not self.stop.wait(0.2):
            s = self.c.read(self.pid)
            if s:
                self.peak_rss = max(self.peak_rss, s["rss"])
                self.peak_fp = max(self.peak_fp, s["footprint"])
                self.samples.append(s["footprint"])

    def __exit__(self, *exc):
        self.stop.set()
        self.th.join()
        b = self.c.read(self.pid)
        self.wall_s = time.perf_counter() - self.t0
        self.result = {
            "wall_s": round(self.wall_s, 3),
            "cpu_s": round(b["cpu_s"] - self.a["cpu_s"], 3),
            "cpu_cores_avg": round((b["cpu_s"] - self.a["cpu_s"]) / self.wall_s, 2) if self.wall_s else 0,
            "disk_write_bytes": b["disk_write"] - self.a["disk_write"],
            "disk_read_bytes": b["disk_read"] - self.a["disk_read"],
            "peak_rss_bytes": max(self.peak_rss, b["rss"]),
            "peak_footprint_bytes": max(self.peak_fp, b["footprint"]),
            "end_footprint_bytes": b["footprint"],
            "threads": thread_count(self.pid),
        }


def dir_size(path):
    logical = allocated = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                st = os.lstat(os.path.join(root, f))
            except OSError:
                continue
            logical += st.st_size
            allocated += st.st_blocks * 512
    return {"logical_bytes": logical, "allocated_bytes": allocated}


# --------------------------------------------------------------------------
# Servers
# --------------------------------------------------------------------------


class Http:
    def __init__(self, port):
        self.port = port
        self.local = threading.local()

    def conn(self):
        c = getattr(self.local, "c", None)
        if c is None:
            c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=1800)
            self.local.c = c
        return c

    def call(self, method, path, body=None, ctype="application/json", raw=False):
        data = body if isinstance(body, (bytes, type(None))) else json.dumps(body).encode()
        for attempt in (0, 1):
            c = self.conn()
            try:
                c.request(method, path, data, {"Content-Type": ctype})
                r = c.getresponse()
                payload = r.read()
                if raw:
                    return r.status, payload
                try:
                    return r.status, json.loads(payload) if payload else None
                except ValueError:
                    return r.status, payload.decode("utf-8", "replace")
            except (http.client.HTTPException, OSError):
                c.close()
                self.local.c = None
                if attempt:
                    raise


def listening_pid(port):
    try:
        out = subprocess.check_output(["lsof", "-ti", f"tcp:{port}", "-sTCP:LISTEN"], text=True)
        return int(out.split()[0])
    except (subprocess.CalledProcessError, IndexError, OSError):
        return None


class Server:
    def __init__(self, name, cmd, env, port, workdir):
        self.name, self.cmd, self.env, self.port, self.workdir = name, cmd, env, port, workdir
        self.proc = None
        self.pid = None
        self.http = Http(port)

    def start(self, health):
        log = open(self.workdir / f"{self.name}.log", "ab")
        t0 = time.perf_counter()
        self.proc = subprocess.Popen(
            self.cmd, env={**os.environ, **self.env}, stdout=log, stderr=log, start_new_session=True
        )
        deadline = time.time() + 300
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"{self.name} exited with {self.proc.returncode}")
            try:
                if health(self.http):
                    break
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(0.1)
        else:
            raise RuntimeError(f"{self.name} did not become ready")
        self.pid = listening_pid(self.port) or self.proc.pid
        return time.perf_counter() - t0

    def stop(self):
        if self.pid is None:
            return
        try:
            os.kill(self.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        if self.proc:
            try:
                self.proc.wait(timeout=120)
            except subprocess.TimeoutExpired:
                os.kill(self.pid, signal.SIGKILL)
        end = time.time() + 120
        while time.time() < end and listening_pid(self.port):
            time.sleep(0.2)
        self.http.local = threading.local()
        self.pid = None


# --------------------------------------------------------------------------
# Query set
# --------------------------------------------------------------------------


def match(field, text):
    return {"match": {field: text}}


def build_queries(vocab, picks, bigrams):
    fr = term_variants(vocab, picks["frequent"])
    md = term_variants(vocab, picks["medium"])
    rr = term_variants(vocab, picks["rare"])
    qs = []

    def add(name, kind, es, sv=None, note=""):
        qs.append({"name": name, "kind": kind, "es": es, "sv": sv if sv is not None else es, "note": note})

    add("term / rare", "search", [{"query": match("body", t), "size": 10} for t in rr])
    add("term / medium", "search", [{"query": match("body", t), "size": 10} for t in md])
    add("term / frequent", "search", [{"query": match("body", t), "size": 10} for t in fr])
    add("OR of two terms", "search",
        [{"query": {"bool": {"should": [match("body", md[i]), match("body", rr[i])]}}, "size": 10} for i in range(5)])
    add("AND frequent + medium", "search",
        [{"query": {"bool": {"must": [match("body", fr[i]), match("body", md[i])]}}, "size": 10} for i in range(5)])
    add("AND frequent + frequent", "search",
        [{"query": {"bool": {"must": [match("body", fr[i]), match("body", fr[(i + 1) % 5])]}}, "size": 10} for i in range(5)])
    add("phrase (two words)", "search",
        [{"query": {"match_phrase": {"body": f"{a} {b}"}}, "size": 10} for a, b in bigrams[:5]],
        [{"phrases": [{"field": "body", "terms": [a, b]}], "k": 10} for a, b in bigrams[:5]],
        "ES match_phrase; SvinDB native phrases (the ES dialect has no match_phrase)")
    add("term + range filter", "search",
        [{"query": {"bool": {"must": [match("body", md[i])], "filter": [{"range": {"views": {"gte": 50 + 10 * i}}}]}}, "size": 10}
         for i in range(5)])
    add("term, sort by views", "search",
        [{"query": match("body", md[i]), "size": 10, "sort": [{"views": "desc"}]} for i in range(5)])
    add("range count (size 0)", "search",
        [{"query": {"bool": {"filter": [{"range": {"views": {"gte": 100 + 25 * i, "lte": 400 + 25 * i}}}]}}, "size": 0, "track_total_hits": True}
         for i in range(5)])
    add("terms agg on category", "agg",
        [{"query": match("body", md[i]), "size": 0, "aggs": {"a": {"terms": {"field": "cat", "size": 10}}}} for i in range(5)])
    add("stats agg, all docs", "agg",
        [{"query": {"match_all": {}}, "size": 0, "aggs": {"a": {"stats": {"field": "views"}}}}])
    add("terms agg, high cardinality", "agg",
        [{"query": {"match_all": {}}, "size": 0, "aggs": {"a": {"terms": {"field": "author", "size": 10}}}}])
    add("date histogram (month)", "agg",
        [{"query": match("body", md[i]), "size": 0, "aggs": {"a": {"date_histogram": {"field": "ts", "calendar_interval": "1M"}}}}
         for i in range(5)])
    return qs


def search_path(engine, index):
    if engine == "es":
        return f"/{index}/_search?request_cache=false"
    return f"/{index}/_search"


def with_count(engine, body):
    """Variant used only for verification: exact hit count, no hits returned."""
    b = json.loads(json.dumps(body))
    if engine == "es":
        b["size"] = 0
        b["track_total_hits"] = True
    else:
        b["size"] = 0
        b.pop("k", None)
        if "phrases" in b:
            b["k"] = 1
            b.pop("size", None)
        b.setdefault("aggs", {})
        b["aggs"]["__n"] = {"count": {}}
    return b


def total_of(engine, resp):
    if not isinstance(resp, dict):
        return None
    if engine == "es":
        return resp.get("hits", {}).get("total", {}).get("value")
    agg = resp.get("aggs", {}).get("__n")
    return agg.get("count") if agg else None


def summary(engine, kind, resp):
    """Canonical comparable summary of one aggregation response."""
    a = (resp or {}).get("aggregations" if engine == "es" else "aggs", {}).get("a")
    if a is None:
        return None
    if "buckets" in a:
        rows = [(b.get("key_as_string", b["key"]) if False else b["key"], b["doc_count"]) for b in a["buckets"]]
        return {"counts": sorted((c for _, c in rows if c), reverse=True), "keys": sorted(str(k) for k, c in rows if c)}
    if "sum" in a:
        return {"count": a["count"], "min": a["min"], "max": a["max"], "sum": a["sum"]}
    return a


def same_summary(x, y):
    if x is None or y is None:
        return False
    if "sum" in x:
        return (x["count"] == y["count"] and x["min"] == y["min"] and x["max"] == y["max"]
                and math.isclose(x["sum"], y["sum"], rel_tol=1e-9, abs_tol=1e-6))
    if x.get("counts") == y.get("counts"):
        # terms: ties at the cut-off may choose different keys; counts must match
        return True
    return False


# --------------------------------------------------------------------------
# Measurement
# --------------------------------------------------------------------------


def pct(values, p):
    s = sorted(values)
    return s[max(0, math.ceil(p * len(s)) - 1)] if s else None


def ingest(engine, srv, index, corpus, docs_per_batch, threads, counters):
    q = queue.Queue(maxsize=threads * 2)
    errors, sent = [], [0]
    lock = threading.Lock()

    def worker():
        while True:
            item = q.get()
            if item is None:
                return
            body, n = item
            st, resp = srv.http.call("POST", f"/{index}/_bulk", body, ctype="application/x-ndjson")
            bad = st != 200 or (isinstance(resp, dict) and resp.get("errors"))
            with lock:
                if bad:
                    errors.append((st, str(resp)[:200]))
                sent[0] += n

    with Phase(counters, srv.pid) as ph:
        ws = [threading.Thread(target=worker) for _ in range(threads)]
        for w in ws:
            w.start()
        with open(corpus, "rb") as f:
            while True:
                lines = []
                for _ in range(docs_per_batch * 2):
                    line = f.readline()
                    if not line:
                        break
                    lines.append(line)
                if not lines:
                    break
                q.put((b"".join(lines), len(lines) // 2))
        for _ in ws:
            q.put(None)
        for w in ws:
            w.join()
        acked = time.perf_counter() - ph.t0
    return {"acked_s": round(acked, 3), "sent": sent[0], "errors": errors[:3], "error_count": len(errors), "phase": ph.result}


def run_family(engine, srv, index, fam, reps, warm, counters):
    bodies = fam["es" if engine == "es" else "sv"]
    path = search_path(engine, index)
    lat, errs, last_err = [], 0, None
    n_total = warm + reps
    for i in range(warm):  # warm-up outside the measured block
        srv.http.call("POST", path, bodies[i % len(bodies)])
    with Phase(counters, srv.pid) as ph:
        for i in range(reps):
            body = bodies[i % len(bodies)]
            t0 = time.perf_counter()
            st, resp = srv.http.call("POST", path, body)
            lat.append((time.perf_counter() - t0) * 1000)
            if st != 200:
                errs += 1
                last_err = f"{st} {str(resp)[:160]}"
    out = {
        "requests": reps, "errors": errs, "last_error": last_err,
        "p50_ms": round(pct(lat, 0.50), 3), "p95_ms": round(pct(lat, 0.95), 3),
        "p99_ms": round(pct(lat, 0.99), 3), "mean_ms": round(statistics.fmean(lat), 3),
        "server_cpu_ms_per_request": round(ph.result["cpu_s"] * 1000 / reps, 3),
        "peak_footprint_bytes": ph.result["peak_footprint_bytes"],
    }
    return out


def verify_family(engines, index, fam):
    res = {"checked": 0, "count_mismatch": [], "agg_mismatch": [], "unsupported": {}, "matches": []}
    for v in range(min(3, len(fam["es"]))):
        totals, sums = {}, {}
        for name, srv in engines.items():
            body = with_count(name, fam["es" if name == "es" else "sv"][v])
            st, resp = srv.http.call("POST", search_path(name, index), body)
            if st != 200:
                res["unsupported"][name] = f"{st} {str(resp)[:160]}"
                continue
            totals[name] = total_of(name, resp)
            if fam["kind"] == "agg":
                body2 = json.loads(json.dumps(fam["es" if name == "es" else "sv"][v]))
                st2, resp2 = srv.http.call("POST", search_path(name, index), body2)
                sums[name] = summary(name, fam["kind"], resp2) if st2 == 200 else None
        if len(totals) == 2:
            res["checked"] += 1
            res["matches"].append(totals["es"])
            if totals["es"] != totals["svindb"]:
                res["count_mismatch"].append({"variant": v, "es": totals["es"], "svindb": totals["svindb"]})
        if fam["kind"] == "agg" and len(sums) == 2 and not same_summary(sums["es"], sums["svindb"]):
            res["agg_mismatch"].append({"variant": v, "es": sums["es"], "svindb": sums["svindb"]})
    # A comparison where both sides return zero proves nothing.
    res["nonzero"] = all(m for m in res["matches"]) and bool(res["matches"])
    res["ok"] = (res["checked"] > 0 and res["nonzero"] and not res["count_mismatch"]
                 and not res["agg_mismatch"] and not res["unsupported"])
    return res


def verify_sort(engines, index, fam):
    out = {"mismatch": []}
    for v in range(len(fam["es"])):
        vals = {}
        for name, srv in engines.items():
            st, resp = srv.http.call("POST", search_path(name, index), fam["es" if name == "es" else "sv"][v])
            if st != 200:
                return {"ok": False, "error": f"{name}: {st}"}
            hits = resp["hits"]["hits"] if name == "es" else resp["hits"]
            vals[name] = [h["sort"][0] if "sort" in h else h["_source"]["views"] for h in hits]
        if vals["es"] != vals["svindb"]:
            out["mismatch"].append({"variant": v, "es": vals["es"], "svindb": vals["svindb"]})
    out["ok"] = not out["mismatch"]
    return out


def overlap_at_10(engines, index, fam):
    """Informational only: BM25 variants differ, so top-10 need not be equal."""
    ratios = []
    for v in range(len(fam["es"])):
        ids = {}
        for name, srv in engines.items():
            st, resp = srv.http.call("POST", search_path(name, index), fam["es" if name == "es" else "sv"][v])
            if st != 200:
                return None
            hits = resp["hits"]["hits"] if name == "es" else resp["hits"]
            ids[name] = {h["_id"] for h in hits}
        if ids["es"]:
            ratios.append(len(ids["es"] & ids["svindb"]) / len(ids["es"]))
    return round(statistics.fmean(ratios), 3) if ratios else None


def point_reads(engine, srv, index, ids, counters, reps=2000):
    lat, miss = [], 0
    for i in range(50):
        srv.http.call("GET", f"/{index}/_doc/{ids[i % len(ids)]}")
    with Phase(counters, srv.pid) as ph:
        for i in range(reps):
            t0 = time.perf_counter()
            st, resp = srv.http.call("GET", f"/{index}/_doc/{ids[i % len(ids)]}")
            lat.append((time.perf_counter() - t0) * 1000)
            if st != 200 or not (resp or {}).get("found"):
                miss += 1
    return {
        "requests": reps, "not_found_or_error": miss,
        "p50_ms": round(pct(lat, 0.5), 3), "p95_ms": round(pct(lat, 0.95), 3),
        "p99_ms": round(pct(lat, 0.99), 3),
        "server_cpu_ms_per_request": round(ph.result["cpu_s"] * 1000 / reps, 3),
    }


def mixed_load(engine, srv, index, families, ids, clients, seconds, counters, seed):
    lat, errs = [], [0]
    lock = threading.Lock()
    stop = time.time() + seconds
    path = search_path(engine, index)

    def client(n):
        rng = random.Random(seed + n)
        mine = []
        while time.time() < stop:
            if rng.random() < 0.15:
                t0 = time.perf_counter()
                st, _ = srv.http.call("GET", f"/{index}/_doc/{rng.choice(ids)}")
            else:
                fam = rng.choice(families)
                bodies = fam["es" if engine == "es" else "sv"]
                t0 = time.perf_counter()
                st, _ = srv.http.call("POST", path, rng.choice(bodies))
            mine.append((time.perf_counter() - t0) * 1000)
            if st != 200:
                with lock:
                    errs[0] += 1
        with lock:
            lat.extend(mine)

    with Phase(counters, srv.pid) as ph:
        ts = [threading.Thread(target=client, args=(n,)) for n in range(clients)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
    return {
        "clients": clients, "seconds": seconds, "requests": len(lat), "errors": errs[0],
        "qps": round(len(lat) / ph.wall_s, 1),
        "p50_ms": round(pct(lat, 0.5), 3), "p95_ms": round(pct(lat, 0.95), 3),
        "p99_ms": round(pct(lat, 0.99), 3),
        "phase": ph.result,
    }


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

ES_MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"},
    "mappings": {"dynamic": "strict", "properties": {
        "title": {"type": "text"}, "body": {"type": "text"}, "cat": {"type": "keyword"},
        "author": {"type": "keyword"}, "views": {"type": "long"}, "price": {"type": "double"},
        "ts": {"type": "date"}}},
}
SV_MAPPING = {"mappings": {k: v for k, v in ES_MAPPING["mappings"].items()}}


def es_health(h):
    st, r = h.call("GET", "/_cluster/health")
    return st == 200 and isinstance(r, dict) and r.get("status") in ("green", "yellow")


def sv_health(h):
    st, _ = h.call("GET", "/_health")
    return st == 200


def wait_settled(srv, index, quiet_s=8, timeout=900):
    """SvinDB: wait until the segment count stops changing (background merge idle)."""
    end, last, since = time.time() + timeout, None, time.time()
    while time.time() < end:
        st, rows = srv.http.call("GET", "/_cat/indices")
        count = next((r.get("segments.count") for r in rows if r.get("index") == index), None) if st == 200 else None
        if count != last:
            last, since = count, time.time()
        elif time.time() - since >= quiet_s:
            return last
        time.sleep(1)
    return last


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--docs", type=int, default=1_000_000)
    ap.add_argument("--seed", type=int, default=20261114)
    ap.add_argument("--svindb", required=True, help="path to the svindb binary")
    ap.add_argument("--es-home", required=True, help="Elasticsearch distribution directory")
    ap.add_argument("--es-heap", default="4g")
    ap.add_argument("--workdir", required=True, help="scratch directory (needs ~5x the corpus size free)")
    ap.add_argument("--output", required=True)
    ap.add_argument("--es-port", type=int, default=19200)
    ap.add_argument("--svindb-port", type=int, default=19300)
    ap.add_argument("--batch-docs", type=int, default=5000)
    ap.add_argument("--ingest-threads", type=int, default=4)
    ap.add_argument("--reps", type=int, default=100)
    ap.add_argument("--warm", type=int, default=15)
    ap.add_argument("--mixed-clients", type=int, default=8)
    ap.add_argument("--mixed-seconds", type=int, default=30)
    ap.add_argument("--keep", action="store_true", help="keep data directories afterwards")
    args = ap.parse_args()

    work = Path(args.workdir).resolve()
    work.mkdir(parents=True, exist_ok=True)
    corpus = work / "corpus.ndjson"
    es_conf, es_data, es_logs = work / "es-config", work / "es-data", work / "es-logs"
    sv_root = work / "svindb-data"
    for d in (es_data, es_logs, sv_root):
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)
    es_conf.mkdir(exist_ok=True)

    print(f"generating {args.docs} documents ...", flush=True)
    t0 = time.perf_counter()
    vocab, picks, bigrams = generate_corpus(corpus, args.docs, args.seed)
    gen_s = time.perf_counter() - t0
    corpus_bytes = corpus.stat().st_size
    print(f"  corpus {corpus_bytes / 1e6:.0f} MB in {gen_s:.0f}s", flush=True)
    if len(bigrams) < 5:
        sys.exit("not enough phrase candidates; use more documents")

    es_home = Path(args.es_home)
    for name in ("jvm.options", "log4j2.properties"):
        text = (es_home / "config" / name).read_text()
        if name == "jvm.options":
            text = "\n".join(l for l in text.splitlines() if not l.startswith(("-Xms", "-Xmx")))
        (es_conf / name).write_text(text)
    (es_conf / "elasticsearch.yml").write_text(
        "cluster.name: bench\nnode.name: bench-1\ndiscovery.type: single-node\n"
        f"network.host: 127.0.0.1\nhttp.port: {args.es_port}\n"
        f"path.data: {es_data}\npath.logs: {es_logs}\n"
        "xpack.security.enabled: false\nxpack.ml.enabled: false\n"
        "cluster.routing.allocation.disk.threshold_enabled: false\n")

    es = Server("es", [str(es_home / "bin" / "elasticsearch")],
                {"ES_PATH_CONF": str(es_conf), "ES_JAVA_OPTS": f"-Xms{args.es_heap} -Xmx{args.es_heap}"},
                args.es_port, work)
    sv = Server("svindb", [args.svindb, "serve", str(sv_root), str(args.svindb_port)],
                {"SVINDB_DISK_FLOOR_MB": "1"}, args.svindb_port, work)
    engines = {"es": es, "svindb": sv}
    counters = Counters()
    index = "bench"

    result = {
        "machine": {
            "platform": platform.platform(), "machine": platform.machine(),
            "cpus": os.cpu_count(),
            "memory_bytes": int(subprocess.check_output(["sysctl", "-n", "hw.memsize"]).strip())
            if sys.platform == "darwin" else os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
            "load_avg_at_start": os.getloadavg(),
        },
        "setup": {
            "documents": args.docs, "seed": args.seed, "corpus_bytes": corpus_bytes,
            "es_heap": args.es_heap, "ingest_threads": args.ingest_threads, "batch_docs": args.batch_docs,
            "es_settings": ES_MAPPING["settings"], "reps": args.reps, "warm": args.warm,
            "term_ranks": picks,
            "svindb_env": {"SVINDB_DISK_FLOOR_MB": "1"},
            "es_overrides": {"cluster.routing.allocation.disk.threshold_enabled": False},
        },
        "start": {}, "ingest": {}, "size": {}, "queries": {}, "verify": {}, "point": {},
        "mixed": {}, "restart": {}, "idle": {},
    }

    try:
        result["start"]["es_s"] = round(es.start(es_health), 2)
        result["start"]["svindb_s"] = round(sv.start(sv_health), 2)
        st, ver = es.http.call("GET", "/")
        result["setup"]["es_version"] = ver["version"]["number"]
        st, ver = sv.http.call("GET", "/_stats")
        result["setup"]["svindb_version"] = ver.get("version") if isinstance(ver, dict) else None
        time.sleep(3)
        result["idle"]["before_ingest"] = {
            n: {k: v for k, v in counters.read(s.pid).items() if k in ("rss", "footprint")}
            for n, s in engines.items()}

        # ---- create indices
        st, r = es.http.call("PUT", f"/{index}", ES_MAPPING)
        assert st == 200, r
        st, r = es.http.call("GET", f"/_cluster/health/{index}?wait_for_status=yellow&timeout=120s")
        assert st == 200 and r.get("status") in ("green", "yellow"), r
        st, r = sv.http.call("PUT", f"/{index}", SV_MAPPING)
        assert st == 200, r

        # ---- ingest
        for name, srv in engines.items():
            print(f"ingest {name} ...", flush=True)
            res = ingest(name, srv, index, corpus, args.batch_docs, args.ingest_threads, counters)
            if res["error_count"]:
                raise RuntimeError(f"{name} ingest errors: {res['errors']}")
            t_ready = time.perf_counter()
            with Phase(counters, srv.pid) as ph:
                if name == "es":
                    srv.http.call("POST", f"/{index}/_flush?wait_if_ongoing=true")
                    srv.http.call("POST", f"/{index}/_refresh")
                else:
                    srv.http.call("POST", f"/{index}/_flush")
            res["ready_extra_s"] = round(time.perf_counter() - t_ready, 3)
            res["ready_s"] = round(res["acked_s"] + res["ready_extra_s"], 3)
            res["docs_per_s_to_ready"] = round(args.docs / res["ready_s"])
            res["ready_phase"] = ph.result
            result["ingest"][name] = res
            # document count must match before any number is believed
            st, cnt = srv.http.call("POST", f"/{index}/_search" + ("?request_cache=false" if name == "es" else ""),
                                    with_count(name, {"query": {"match_all": {}}, "size": 0}))
            got = total_of(name, cnt)
            result["ingest"][name]["indexed_count"] = got
            if got != args.docs:
                raise RuntimeError(f"{name} holds {got} documents, expected {args.docs}")

        # ---- sizes: as ingested, then settled
        def sizes(tag):
            es_dir = dir_size(es_data)
            sv_dir = dir_size(sv_root)
            st, cat = es.http.call("GET", "/_cat/segments/bench?format=json")
            es_segments = len(cat) if st == 200 and isinstance(cat, list) else None
            st, rows = sv.http.call("GET", "/_cat/indices")
            sv_segments = next((r.get("segments.count") for r in rows if r.get("index") == index), None)
            result["size"][tag] = {"es": {**es_dir, "segments": es_segments},
                                   "svindb": {**sv_dir, "segments": sv_segments}}

        sizes("after_ingest")
        print("settling background merges ...", flush=True)
        with Phase(counters, sv.pid) as ph:
            result["size"]["svindb_settle_segments"] = wait_settled(sv, index)
        result["size"]["svindb_settle_phase"] = ph.result
        with Phase(counters, es.pid) as ph:
            es.http.call("POST", f"/{index}/_forcemerge?max_num_segments=1")
            es.http.call("POST", f"/{index}/_flush")
            es.http.call("POST", f"/{index}/_refresh")
        result["size"]["es_forcemerge_phase"] = ph.result
        sizes("settled")
        result["idle"]["after_ingest"] = {
            n: {k: v for k, v in counters.read(s.pid).items() if k in ("rss", "footprint")}
            for n, s in engines.items()}
        st, jvm = es.http.call("GET", "/_nodes/stats/jvm")
        if st == 200:
            node = next(iter(jvm["nodes"].values()))
            result["idle"]["es_jvm_heap_used_bytes"] = node["jvm"]["mem"]["heap_used_in_bytes"]
            result["idle"]["es_jvm_heap_max_bytes"] = node["jvm"]["mem"]["heap_max_in_bytes"]
        st, text = sv.http.call("GET", f"/{index}/_metrics", raw=True)
        if st == 200:
            keep = [l for l in text.decode().splitlines()
                    if not l.startswith("#") and any(k in l for k in ("alloc", "heap", "resident"))]
            result["idle"]["svindb_memory_metrics"] = keep[:20]

        # ---- queries: correctness first, then speed
        families = build_queries(vocab, picks, bigrams)
        for fam in families:
            if fam["name"] == "term, sort by views":
                result["verify"][fam["name"]] = verify_sort(engines, index, fam)
            else:
                v = verify_family(engines, index, fam)
                if fam["kind"] == "search" and "count" not in fam["name"]:
                    v["top10_overlap"] = overlap_at_10(engines, index, fam)
                result["verify"][fam["name"]] = v
        for fam in families:
            row = {"note": fam["note"], "kind": fam["kind"]}
            for name, srv in engines.items():
                print(f"query {fam['name']} [{name}]", flush=True)
                row[name] = run_family(name, srv, index, fam, args.reps, args.warm, counters)
            result["queries"][fam["name"]] = row

        ids = [f"d{i}" for i in random.Random(args.seed).sample(range(args.docs), 2000)]
        for name, srv in engines.items():
            print(f"point reads [{name}]", flush=True)
            result["point"][name] = point_reads(name, srv, index, ids, counters)

        # ---- mixed concurrent load
        mix = [f for f in families if f["name"] not in ("terms agg, high cardinality",)]
        for name, srv in engines.items():
            print(f"mixed load [{name}]", flush=True)
            result["mixed"][name] = mixed_load(name, srv, index, mix, ids, args.mixed_clients,
                                               args.mixed_seconds, counters, args.seed)

        # ---- restart: time until the first query is answered
        result["size"]["final"] = {"es": dir_size(es_data), "svindb": dir_size(sv_root)}
        for name, srv in engines.items():
            srv.stop()
        for name, srv in engines.items():
            health = es_health if name == "es" else sv_health
            t0 = time.perf_counter()
            up = srv.start(health)
            body = with_count(name, {"query": match("body", vocab[picks["medium"]]), "size": 0})
            for _ in range(600):
                st, resp = srv.http.call("POST", search_path(name, index), body)
                if st == 200 and total_of(name, resp):
                    break
                time.sleep(0.1)
            result["restart"][name] = {
                "process_ready_s": round(up, 2),
                "first_query_answered_s": round(time.perf_counter() - t0, 2),
                "footprint_bytes_after_first_query": counters.read(srv.pid)["footprint"],
            }
        result["idle"]["after_restart_first_query"] = {
            n: {k: v for k, v in counters.read(s.pid).items() if k in ("rss", "footprint")}
            for n, s in engines.items()}
    finally:
        for srv in engines.values():
            srv.stop()
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(result, indent=2, ensure_ascii=False))
        if not args.keep:
            shutil.rmtree(es_data, ignore_errors=True)
            shutil.rmtree(sv_root, ignore_errors=True)
            corpus.unlink(missing_ok=True)
    print(f"results written to {args.output}")


if __name__ == "__main__":
    main()
