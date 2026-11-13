#!/usr/bin/env python3
"""Полный предрелизный E2E-прогон SvinDB на миллионе документов.

Стенд проверяет обе дороги приёма (CLI и HTTP), заранее рассчитанные ответы
поиска, вложенные массивы, новые типы, агрегации, замену и удаление, повторное
открытие, обслуживание, перепаковку и снимок с восстановлением. Он не снимает
ожидания с самого движка: ожидаемые числа выводятся из генератора корпуса, а не
из предыдущего ответа SvinDB.

По умолчанию временный каталог удаляется после успеха и сохраняется после
ошибки. Настройки:

    SVINDB_BIN=target/release/svindb python3 tools/release_gate_million.py
    GATE_DOCS=1000000 GATE_KEEP=1 python3 tools/release_gate_million.py
"""

from __future__ import annotations

import http.client
import json
import os
import queue
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BIN = Path(os.environ.get("SVINDB_BIN", ROOT / "target/release/svindb")).resolve()
N = int(os.environ.get("GATE_DOCS", "1000000"))
KEEP = os.environ.get("GATE_KEEP") == "1"
BATCH = int(os.environ.get("GATE_BATCH", "2000"))
THREADS = int(os.environ.get("GATE_THREADS", "4"))
SCHEMA = (
    "message:2,author.name:keyword,"
    "reactions.emoji:keyword:slots,reactions.count:int32:slots,"
    "place:geo_point,payload:binary,published:date:iso,"
    "score:double,channel:keyword"
)


class GateError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise GateError(message)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def document(i: int) -> dict:
    cities = (
        {"lat": 55.7558, "lon": 37.6173},
        {"lat": 59.9343, "lon": 30.3351},
        {"lat": 55.7963, "lon": 49.1088},
    )
    payloads = ("AQAAAB8ABbBc", "aqaaab8abbbc", "AAEC")
    reactions = [
        {"emoji": "up" if slot == 0 else "fire", "count": (i + slot) % 100}
        for slot in range(i % 4)
    ]
    return {
        "id": f"d{i}",
        "message": f"общий миллион bucket{i % 10} marker{i % 1000}",
        "author": {"name": f"author-{i % 1000}"},
        "reactions": reactions,
        "place": cities[i % 3],
        "payload": payloads[i % 3],
        "published": f"2024-01-{i % 31 + 1:02d}T12:34:56Z",
        "score": (i % 1000) / 10.0,
        "channel": f"channel-{i % 17}",
    }


def make_corpus(path: Path) -> None:
    started = time.perf_counter()
    with path.open("w", encoding="utf-8", buffering=1 << 20) as out:
        for i in range(N):
            out.write(json.dumps(document(i), ensure_ascii=False, separators=(",", ":")))
            out.write("\n")
    print(f"  корпус: {N:,} строк, {path.stat().st_size / (1 << 20):.1f} МиБ, "
          f"{time.perf_counter() - started:.1f} с")


def command(*args: str, timeout: int = 1800) -> subprocess.CompletedProcess[str]:
    run = subprocess.run(
        [str(BIN), *map(str, args)], capture_output=True, text=True, timeout=timeout
    )
    if run.returncode != 0:
        tail = (run.stdout + "\n" + run.stderr)[-2000:]
        raise GateError(f"команда {' '.join(args)} завершилась кодом {run.returncode}:\n{tail}")
    return run


def request(port: int, method: str, path: str, body=None, timeout: int = 900):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    raw = None if body is None else json.dumps(body, ensure_ascii=False).encode()
    headers = {"Content-Type": "application/json"} if raw is not None else {}
    conn.request(method, path, raw, headers)
    reply = conn.getresponse()
    data = reply.read()
    conn.close()
    try:
        decoded = json.loads(data)
    except Exception:
        decoded = data.decode("utf-8", "replace")
    return reply.status, decoded


class Node:
    def __init__(self, index: Path, fields: str | None = None):
        self.index = index
        self.port = free_port()
        self.log = index.parent / f"node-{self.port}.log"
        args = [
            str(BIN),
            "serve",
            str(index),
            str(self.port),
            "--single-index",
            "--no-maintain",
            "--flush=100000",
        ]
        if fields:
            args.append(f"--fields={fields}")
        self.stream = self.log.open("w", encoding="utf-8")
        self.process = subprocess.Popen(args, stdout=self.stream, stderr=subprocess.STDOUT)
        for _ in range(240):
            if self.process.poll() is not None:
                break
            try:
                if request(self.port, "GET", "/_health", timeout=3)[0] == 200:
                    return
            except Exception:
                pass
            time.sleep(0.25)
        self.stop()
        tail = self.log.read_text(encoding="utf-8", errors="replace")[-2000:]
        raise GateError(f"узел не поднялся:\n{tail}")

    def stop(self) -> None:
        if getattr(self, "process", None) is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        if getattr(self, "stream", None) is not None and not self.stream.closed:
            self.stream.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stop()


def load_http(port: int, corpus: Path) -> None:
    jobs: queue.Queue[list[bytes] | None] = queue.Queue(maxsize=THREADS * 3)
    failures: list[str] = []
    accepted = 0
    lock = threading.Lock()

    def worker() -> None:
        nonlocal accepted
        while True:
            lines = jobs.get()
            try:
                if lines is None:
                    return
                body = b'{"documents":[' + b",".join(lines) + b']}'
                last = ""
                for attempt in range(6):
                    try:
                        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=900)
                        conn.request("POST", "/_bulk", body, {"Content-Type": "application/json"})
                        reply = conn.getresponse()
                        data = reply.read()
                        conn.close()
                        decoded = json.loads(data)
                        if reply.status < 300 and decoded.get("added") == len(lines):
                            with lock:
                                accepted += len(lines)
                            break
                        last = f"HTTP {reply.status}: {data[:300]!r}"
                    except Exception as error:
                        last = repr(error)
                    time.sleep(0.2 * (attempt + 1))
                else:
                    with lock:
                        failures.append(last)
            finally:
                jobs.task_done()

    workers = [threading.Thread(target=worker, daemon=True) for _ in range(THREADS)]
    for item in workers:
        item.start()

    started = time.perf_counter()
    batch: list[bytes] = []
    with corpus.open("rb") as source:
        for line in source:
            if line.strip():
                batch.append(line.rstrip(b"\n"))
            if len(batch) == BATCH:
                jobs.put(batch)
                batch = []
        if batch:
            jobs.put(batch)
    for _ in workers:
        jobs.put(None)
    jobs.join()
    require(not failures, f"HTTP-загрузка потеряла пачки: {failures[:3]}")
    require(accepted == N, f"HTTP-загрузка приняла {accepted}, ожидалось {N}")
    status, reply = request(port, "POST", "/_flush", {})
    require(status == 200 and reply.get("flushed") is True, f"сброс не прошёл: {status} {reply}")
    for _ in range(240):
        status, health = request(port, "GET", "/_health")
        if status == 200 and health.get("docs") == N and health.get("buffered") == 0:
            break
        time.sleep(0.5)
    else:
        raise GateError(f"после загрузки health не сошёлся: {health}")
    elapsed = time.perf_counter() - started
    print(f"  HTTP-приём: {accepted:,} документов, {elapsed:.1f} с, {accepted / elapsed:,.0f} док/с")


def count(port: int, selection: dict) -> int:
    body = dict(selection)
    if not any(key in body for key in ("words", "must", "query", "filter", "phrases")):
        body["filter"] = [{"field": "score"}]
    body["k"] = 0
    body["aggs"] = {"gate_count": {"count": {}}}
    status, reply = request(port, "POST", "/", body)
    require(status == 200, f"запрос счёта отвергнут: {status} {reply}")
    try:
        return int(reply["aggs"]["gate_count"]["count"])
    except Exception as error:
        raise GateError(f"в ответе нет счёта: {reply}") from error


def exact(name: str, got: int, expected: int) -> None:
    require(got == expected, f"{name}: получено {got:,}, ожидалось {expected:,}")
    print(f"    ✓ {name}: {got:,}")


def verify_baseline(port: int, total: int, deleted: bool = False) -> None:
    removed = 2 if deleted else 0
    exact("весь корпус", count(port, {}), total - removed)
    exact(
        "полнотекстовый bucket7",
        count(port, {"words": [{"field": "message", "terms": ["bucket7"]}]}),
        total // 10 - (2 if deleted else 0),
    )
    exact(
        "вложенный author.name",
        count(port, {"words": [{"field": "author.name", "terms": ["author-7"]}]}),
        total // 1000,
    )
    exact(
        "рамка вокруг Москвы",
        count(port, {"filter": [
            {"field": "place.lat", "from": 55.0, "to": 56.0},
            {"field": "place.lon", "from": 37.0, "to": 38.0},
        ]}),
        (total + 2) // 3 - (1 if deleted else 0),
    )
    exact(
        "binary с учётом регистра",
        count(port, {"words": [{"field": "payload", "terms": ["AQAAAB8ABbBc"]}]}),
        (total + 2) // 3 - (1 if deleted else 0),
    )

    same_slot = {"query": {"must": [{"slots": {
        "group": "reactions",
        "all": [
            {"field": "reactions.emoji", "terms": ["up"]},
            {"range": {"field": "reactions.count", "from": 4, "to": 4}},
        ],
    }}]}}
    different_slots = {"query": {"must": [
        {"slots": {"group": "reactions", "all": [
            {"field": "reactions.emoji", "terms": ["up"]}
        ]}},
        {"slots": {"group": "reactions", "all": [
            {"range": {"field": "reactions.count", "from": 4, "to": 4}}
        ]}},
    ]}}
    exact("up и count=4 в одном элементе", count(port, same_slot), 0)
    exact("up и count=4 в разных элементах", count(port, different_slots), total // 100)

    status, reply = request(port, "POST", "/", {
        "k": 0,
        "filter": [{"field": "score"}],
        "aggs": {
            "элементы": {"over": "reactions", "count": {"field": "reactions.count"}},
            "дни": {"date_histogram": {
                "field": "published", "calendar_interval": "1d", "time_zone": "+03:00"
            }},
            "оценка": {"stats": {"field": "score"}},
        },
    })
    require(status == 200, f"агрегации отвергнуты: {status} {reply}")
    reaction_total = sum(i % 4 for i in range(total)) - (1 if deleted else 0)
    exact("число элементов массива", reply["aggs"]["элементы"]["count"], reaction_total)
    exact(
        "сумма корзин date_histogram",
        sum(item["doc_count"] for item in reply["aggs"]["дни"]["buckets"]),
        total - removed,
    )
    exact("число double в stats", reply["aggs"]["оценка"]["count"], total - removed)

    status, reply = request(
        port,
        "POST",
        "/gate/_search",
        {"query": {"match": {"message": "bucket7"}}, "size": 5, "_source": True},
    )
    hits = reply.get("hits", []) if isinstance(reply, dict) else []
    require(
        status == 200
        and len(hits) == 5
        and all(hit.get("_id") and hit.get("_source", {}).get("message") for hit in hits),
        f"Elasticsearch-совместимый поиск не вернул ID/source: {status} {reply}",
    )
    print("    ✓ Elasticsearch-совместимый маршрут поиска")


def verify_tools(index: Path) -> None:
    command("check", str(index), "--full")
    command("inspect", str(index), "--deep", "--json")
    command("inspect", str(index), "--plan", "--json")
    command("wal", str(index))
    print("  ✓ check, inspect, inspect --plan и wal")


def main() -> int:
    require(N >= 2000 and N % 1000 == 0,
            "GATE_DOCS должен быть не меньше 2000 и кратен 1000 для точных ожиданий")
    require(BIN.is_file(), f"бинарный файл не найден: {BIN}")
    work = Path(tempfile.mkdtemp(prefix="svindb-release-gate-"))
    failed = True
    try:
        corpus = work / "million.ndjson"
        print("== генерация детерминированного корпуса")
        make_corpus(corpus)

        print("== офлайн-индексация CLI")
        offline = work / "offline"
        started = time.perf_counter()
        command("index", str(corpus), str(offline), "100000", SCHEMA)
        print(f"  индекс построен за {time.perf_counter() - started:.1f} с")
        verify_tools(offline)
        with Node(offline) as node:
            verify_baseline(node.port, N)

        print("== HTTP-приём")
        online = work / "online"
        online.mkdir()
        with Node(online, SCHEMA) as node:
            load_http(node.port, corpus)
            verify_baseline(node.port, N)

            replacements = []
            for i in (7, 1007):
                item = document(i)
                item["message"] = "общий миллион releasemarker"
                replacements.append(item)
            status, reply = request(node.port, "POST", "/_put", {"documents": replacements})
            # В 0.3.0 прежние дисковые версии гасятся вместе с публикацией
            # новых, а не до неё. Поэтому ACK сообщает две принятые версии и
            # ноль уже погашенных: `replaced` считает только замены внутри
            # текущего буфера. После flush точные запросы ниже доказывают, что
            # обе новые версии опубликованы, а прежние действительно погашены.
            require(
                status == 200
                and reply.get("added") == 2
                and reply.get("replaced") == 0,
                f"замена двух документов не прошла: {status} {reply}",
            )
            status, reply = request(node.port, "POST", "/_delete", {"ids": ["d8", "d9"]})
            require(status == 200 and reply.get("deleted") == 2,
                    f"удаление двух документов не прошло: {status} {reply}")
            request(node.port, "POST", "/_flush", {})
            exact(
                "заменённые документы видны",
                count(node.port, {"words": [{"field": "message", "terms": ["releasemarker"]}]}),
                2,
            )
            verify_baseline(node.port, N, deleted=True)

        print("== повторное открытие и обслуживание")
        verify_tools(online)
        command("merge", str(online), "64", str(N * 2))
        command("bloom", str(online), "10")
        command("unpack", str(online))
        command("check", str(online), "--full")
        command("pack", str(online))
        verify_tools(online)
        with Node(online) as node:
            verify_baseline(node.port, N, deleted=True)

        print("== снимок и восстановление")
        snapshots = work / "snapshots"
        restored = work / "restored"
        snapshots.mkdir()
        command("snapshot", str(online), str(snapshots), "release-gate", "--fast")
        command("restore", str(snapshots / "release-gate"), str(restored))
        verify_tools(restored)
        with Node(restored) as node:
            verify_baseline(node.port, N, deleted=True)

        failed = False
        scale = "МИЛЛИОННЫЙ" if N >= 1_000_000 else "МАСШТАБНЫЙ"
        print(f"\nПОЛНЫЙ {scale} GATE ПРОШЁЛ: {N:,} документов")
        return 0
    except Exception as error:
        print(f"\nОТКАЗ: {error}", file=sys.stderr)
        return 1
    finally:
        if KEEP or failed:
            print(f"временные данные сохранены: {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
