#!/usr/bin/env python3
"""Воспроизводимый release-gate aliases/rollover на нескольких поколениях.

Управление окружением:

    SVINDB_BIN=target/release/svindb ALIAS_GATE_DOCS=1000000 \
      ALIAS_GATE_GENERATIONS=10 python3 tools/alias_release_gate.py

`ALIAS_GATE_KEEP=1` оставляет root и печатает его путь. По умолчанию временный
каталог удаляется после полного check/restart oracle.
"""

from __future__ import annotations

import http.client
import json
import os
from pathlib import Path
import shutil
import socket
import statistics
import subprocess
import tempfile
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
BIN = Path(os.environ.get("SVINDB_BIN", ROOT / "target/release/svindb")).resolve()
DOCS = int(os.environ.get("ALIAS_GATE_DOCS", "1000000"))
GENERATIONS = int(os.environ.get("ALIAS_GATE_GENERATIONS", "10"))
KEEP = os.environ.get("ALIAS_GATE_KEEP") == "1"
BATCH = int(os.environ.get("ALIAS_GATE_BATCH", "1000"))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request(port: int, method: str, path: str, body: Any | None = None) -> tuple[int, Any]:
    raw = b"" if body is None else json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
    headers = {"Content-Type": "application/json"} if raw else {}
    conn.request(method, path, body=raw, headers=headers)
    reply = conn.getresponse()
    payload = reply.read()
    conn.close()
    try:
        value: Any = json.loads(payload) if payload else None
    except json.JSONDecodeError:
        value = payload.decode("utf-8", "replace")
    return reply.status, value


class Node:
    def __init__(self, root: Path):
        self.root = root
        self.port = free_port()
        env = os.environ.copy()
        env.setdefault("SVINDB_DISK_FLOOR_MB", "1")
        self.child = subprocess.Popen(
            [str(BIN), "serve", str(root), str(self.port), "--sync-flush"],
            cwd=ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        until = time.monotonic() + 60
        while time.monotonic() < until:
            if self.child.poll() is not None:
                raise RuntimeError(f"SvinDB завершился при старте, код {self.child.returncode}")
            try:
                code, _ = request(self.port, "GET", "/_health")
                if code == 200:
                    return
            except OSError:
                pass
            time.sleep(0.025)
        raise RuntimeError("SvinDB не открыл порт за 60 секунд")

    def stop(self) -> None:
        if self.child.poll() is not None:
            return
        self.child.terminate()
        try:
            self.child.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.child.kill()
            self.child.wait(timeout=10)


def must(port: int, method: str, path: str, body: Any | None = None, code: int = 200) -> Any:
    status, value = request(port, method, path, body)
    if status != code:
        raise AssertionError(f"{method} {path}: HTTP {status}, ожидался {code}: {value}")
    return value


def generation_name(number: int) -> str:
    return f"messages-{number:06d}"


def count(port: int) -> int:
    value = must(
        port,
        "POST",
        "/all_messages/_search",
        {"match_all": True, "k": 1, "aggs": {"count": {"stats": {}}}},
    )
    return int(value["aggs"]["count"]["count"])


def search_latency(port: int, path: str, repeats: int = 100) -> float:
    body = {"words": [{"field": "body", "terms": ["общий"]}], "k": 10, "ids": True}
    for _ in range(5):
        must(port, "POST", path, body)
    samples = []
    for _ in range(repeats):
        began = time.perf_counter_ns()
        must(port, "POST", path, body)
        samples.append((time.perf_counter_ns() - began) / 1_000_000)
    return statistics.median(samples)


def full_check(root: Path) -> None:
    indices = root / "indices"
    for index in sorted(path for path in indices.iterdir() if path.is_dir()):
        subprocess.run(
            [str(BIN), "check", str(index), "--full"],
            cwd=ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
        )


def run() -> dict[str, Any]:
    if not BIN.is_file():
        raise SystemExit(f"release-бинарник не найден: {BIN}")
    if DOCS <= 0 or GENERATIONS <= 0 or DOCS < GENERATIONS:
        raise SystemExit("ALIAS_GATE_DOCS и ALIAS_GATE_GENERATIONS должны быть положительными")
    work = Path(tempfile.mkdtemp(prefix="svindb-alias-release-gate-"))
    node: Node | None = None
    started = time.monotonic()
    try:
        node = Node(work)
        first = generation_name(1)
        must(
            node.port,
            "PUT",
            f"/{first}",
            {
                "fields": "body:text,generation:int64,ordinal:int64",
                "metadata": {
                    "description": "release-gate сообщений",
                    "tags": ["messages", "release-gate", "hot"],
                },
                "aliases": {"all_messages": {"is_write_index": True}},
            },
        )
        accepted = 0
        per_generation = DOCS // GENERATIONS
        remainder = DOCS % GENERATIONS
        single_concrete_ms = 0.0
        single_alias_ms = 0.0
        for generation in range(1, GENERATIONS + 1):
            amount = per_generation + (1 if generation <= remainder else 0)
            left = amount
            while left:
                take = min(left, BATCH)
                docs = [
                    {
                        "id": f"g{generation}-{accepted + at}",
                        "body": f"общий generationmarker{generation} уникальный-{accepted + at}",
                        "generation": generation,
                        "ordinal": accepted + at,
                    }
                    for at in range(take)
                ]
                answer = must(node.port, "POST", "/all_messages/_bulk", {"documents": docs})
                if int(answer.get("added", -1)) != take:
                    raise AssertionError(f"bulk принял не всю пачку: {answer}")
                accepted += take
                left -= take
            must(node.port, "POST", "/all_messages/_flush", {})
            if generation == 1:
                single_concrete_ms = search_latency(node.port, f"/{first}/_search")
                single_alias_ms = search_latency(node.port, "/all_messages/_search")
            if generation < GENERATIONS:
                rolled = must(node.port, "POST", "/all_messages/_rollover", {})
                expected = generation_name(generation + 1)
                if not rolled.get("rolled_over") or rolled.get("new_index") != expected:
                    raise AssertionError(f"rollover не создал {expected}: {rolled}")

        if accepted != DOCS or count(node.port) != DOCS:
            raise AssertionError(f"счёт до restart: accepted={accepted}, search={count(node.port)}")
        rows = must(node.port, "GET", "/_cat/indices")
        if len(rows) != GENERATIONS:
            raise AssertionError(f"поколений {len(rows)}, ожидалось {GENERATIONS}")
        writers = [row for row in rows if row.get("write_aliases") == ["all_messages"]]
        if len(writers) != 1 or writers[0]["index"] != generation_name(GENERATIONS):
            raise AssertionError(f"write index неоднозначен: {writers}")
        for row in rows[:-1]:
            if "write" not in row.get("blocks", []):
                raise AssertionError(f"архивное поколение без write block: {row}")
        if GENERATIONS > 1:
            old_status, _ = request(
                node.port,
                "POST",
                f"/{first}/_bulk",
                {
                    "documents": [
                        {
                            "id": "forbidden",
                            "body": "не пройти",
                            "generation": 0,
                            "ordinal": 0,
                        }
                    ]
                },
            )
            if old_status != 403:
                raise AssertionError(
                    f"прямая запись в архив ответила {old_status}, ожидался 403"
                )
        sample = must(
            node.port,
            "POST",
            "/all_messages/_search",
            {"words": [{"field": "body", "terms": [f"generationmarker{GENERATIONS}"]}], "k": 10, "ids": True},
        )
        if not sample["hits"] or sample["hits"][0].get("_index") != generation_name(GENERATIONS):
            raise AssertionError(f"маркер последнего поколения не найден: {sample}")

        node.stop()
        node = Node(work)
        after_restart = count(node.port)
        if after_restart != DOCS:
            raise AssertionError(f"после restart документов {after_restart}, ожидалось {DOCS}")
        aliases = must(node.port, "GET", "/_alias/all_messages")
        if len(aliases) != GENERATIONS:
            raise AssertionError(f"после restart alias targets {len(aliases)}")
        node.stop()
        node = None
        full_check(work)
        return {
            "ok": True,
            "documents": DOCS,
            "generations": GENERATIONS,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "single_concrete_median_ms": round(single_concrete_ms, 4),
            "single_alias_median_ms": round(single_alias_ms, 4),
            "single_alias_overhead_percent": round(
                (single_alias_ms / single_concrete_ms - 1) * 100 if single_concrete_ms else 0.0,
                2,
            ),
            "root": str(work) if KEEP else None,
        }
    finally:
        if node is not None:
            node.stop()
        if not KEEP:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2, sort_keys=True))
