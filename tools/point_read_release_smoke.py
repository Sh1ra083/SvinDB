#!/usr/bin/env python3
"""Read-only GET/MGET smoke кандидата на loopback по независимому ID/source ledger.

Не заменяет performance campaign: проверка JSON на клиенте входит в wall time.
После отдельного ожидания readiness нет скрытых повторов неуспешных запросов.
"""

import argparse
import concurrent.futures
from contextlib import closing
import hashlib
import http.client
import json
import random
import sqlite3
import time
import urllib.parse
from pathlib import Path


def digest(source):
    raw = json.dumps(source, sort_keys=True, separators=(",", ":"),
                     ensure_ascii=False, allow_nan=False).encode()
    return hashlib.sha256(raw).digest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    db = sqlite3.connect(f"file:{args.ledger.resolve()}?mode=ro", uri=True)
    count = db.execute("SELECT count(*) FROM docs").fetchone()[0]
    rowids = random.Random(20260917).sample(range(1, count + 1), min(count, 4096))
    expected = {}
    for rowid in rowids:
        row = db.execute("SELECT id,hash FROM docs WHERE rowid=?", (rowid,)).fetchone()
        if row is None:
            raise ValueError("ledger должен иметь непрерывные rowid")
        expected[row[0]] = row[1]
    missing = "__point_release_smoke_missing__"
    if db.execute("SELECT 1 FROM docs WHERE id=?", (missing,)).fetchone():
        raise ValueError("missing fixture оказался существующим ID")
    db.close()
    ids = list(expected)
    if not ids:
        raise ValueError("пустой ledger")

    def request(connection, method, path, body=None):
        connection.request(method, path, None if body is None else json.dumps(body),
                           {"Content-Type": "application/json"})
        response = connection.getresponse()
        return response.status, json.loads(response.read())

    def verify(item, identifier):
        if item.get("id") != identifier:
            raise ValueError("ответ относится к другому ID")
        if identifier == missing:
            if item.get("found") is not False:
                raise ValueError("несуществующий ID воскрес")
        elif item.get("found") is not True or digest(item.get("_source")) != expected[identifier]:
            raise ValueError("ID/source не совпал с независимым ledger")

    started = time.monotonic()
    with closing(http.client.HTTPConnection("127.0.0.1", args.port, timeout=10)) as connection:
        while True:
            status, item = request(connection, "GET", "/_doc/" + urllib.parse.quote(ids[0], safe="") + "?source_only=true")
            if status == 200:
                verify(item, ids[0])
                break
            if status != 503 or item.get("reason") != "id_directory_not_ready" or time.monotonic() - started > 120:
                raise ValueError(f"point readiness не достигнута: HTTP {status}")
            time.sleep(.05)
        _, health = request(connection, "GET", "/_health")
        _, stats = request(connection, "GET", "/_stats")
        status, item = request(connection, "GET", "/_doc/" + missing)
        if status != 404:
            raise ValueError(f"missing GET: HTTP {status}")
        verify(item, missing)

    def worker(worker_id):
        checked = 0
        with closing(http.client.HTTPConnection("127.0.0.1", args.port, timeout=30)) as connection:
            for offset in range(32):
                identifier = ids[(worker_id * 131 + offset) % len(ids)]
                status, item = request(connection, "GET", "/_doc/" + urllib.parse.quote(identifier, safe="") + "?source_only=true")
                if status != 200:
                    raise ValueError(f"GET: HTTP {status}")
                verify(item, identifier)
                checked += 1
            for batch in range(2):
                selected = [ids[(worker_id * 131 + batch * 99 + i) % len(ids)] for i in range(99)] + [missing]
                status, body = request(connection, "POST", "/_mget", {"ids": selected})
                if status != 200 or len(body.get("docs", [])) != len(selected):
                    raise ValueError(f"MGET100: HTTP {status}, неполный ответ")
                for identifier, item in zip(selected, body["docs"]):
                    verify(item, identifier)
                    checked += 1
        return checked

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        checked = sum(pool.map(worker, range(32)))
    result = {"valid": True, "concurrency": 32, "get_requests": 1024,
              "mget100_requests": 64, "checked_items": checked,
              "sampled_ids": len(ids), "errors": 0, "health": health,
              "point_read": stats.get("point_read"), "wall_seconds": time.monotonic() - started,
              "contract": "canonical source hash; ordered MGET100 including missing; not a performance SLO"}
    with args.output.open("x") as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
