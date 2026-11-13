#!/usr/bin/env python3
"""Смешанный release-gate write alias под rollover и аварийным restart.

Проверка намеренно отделена от длинного 10M ingest-gate: здесь важна не
пропускная способность, а пересечение операций. Несколько писателей параллельно
делают insert/update/delete через alias, читатели непрерывно ищут по всем
поколениям, а управляющий поток выполняет rollover. В конце процесс убивается
во время активной нагрузки; после restart durable oracle сверяет каждый
подтверждённый документ по его физическому поколению.

Настройки окружения:

    SVINDB_BIN=target/release/svindb ALIAS_STRESS_SECONDS=30 \
      ALIAS_STRESS_WRITERS=8 ALIAS_STRESS_READERS=4 \
      python3 tools/alias_stress_gate.py
"""

from __future__ import annotations

from collections import Counter, defaultdict
import http.client
import json
import os
from pathlib import Path
import shutil
import threading
import time
from typing import Any
import tempfile

from alias_release_gate import BIN, Node, full_check, generation_name, must


DURATION = float(os.environ.get("ALIAS_STRESS_SECONDS", "30"))
WRITERS = int(os.environ.get("ALIAS_STRESS_WRITERS", "8"))
READERS = int(os.environ.get("ALIAS_STRESS_READERS", "4"))
ROLLOVERS = int(os.environ.get("ALIAS_STRESS_ROLLOVERS", "3"))
KEEP = os.environ.get("ALIAS_STRESS_KEEP") == "1"
ALIAS = "all_messages"


class Phase:
    """Короткий reader/writer barrier между документами и rollover.

    Писатели работают параллельно. Rollover лишь запрещает начать следующую
    логическую цепочку и ждёт завершения уже начатых, поэтому oracle точно
    знает физическое поколение каждой подтверждённой операции. Поиск barrier
    не берёт и действительно пересекается с переключением alias.
    """

    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.paused = False
        self.active = 0
        self.generation = 1

    def enter(self) -> int:
        with self.condition:
            while self.paused:
                self.condition.wait()
            self.active += 1
            return self.generation

    def leave(self) -> None:
        with self.condition:
            self.active -= 1
            if self.active == 0:
                self.condition.notify_all()

    def pause(self) -> None:
        with self.condition:
            self.paused = True
            while self.active:
                self.condition.wait()

    def advanced(self) -> None:
        with self.condition:
            self.generation += 1
            self.paused = False
            self.condition.notify_all()

    def resume_without_change(self) -> None:
        with self.condition:
            self.paused = False
            self.condition.notify_all()


class JsonClient:
    """Одно keep-alive соединение на worker вместо исчерпания ephemeral ports."""

    def __init__(self, port: int) -> None:
        self.port = port
        self.connection: http.client.HTTPConnection | None = None

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def request(self, method: str, path: str, body: Any) -> tuple[int, Any]:
        raw = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
        if self.connection is None:
            self.connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=120)
        try:
            self.connection.request(
                method,
                path,
                body=raw,
                headers={"Content-Type": "application/json"},
            )
            reply = self.connection.getresponse()
            payload = reply.read()
            status = reply.status
            if reply.will_close:
                self.close()
        except BaseException:
            self.close()
            raise
        try:
            value: Any = json.loads(payload) if payload else None
        except json.JSONDecodeError:
            value = payload.decode("utf-8", "replace")
        return status, value


def checked_request(
    client: JsonClient,
    method: str,
    path: str,
    body: Any,
    crashing: threading.Event,
) -> tuple[int, Any] | None:
    retries = 0
    while True:
        try:
            status, value = client.request(method, path, body)
        except OSError:
            if crashing.is_set():
                return None
            raise
        if status == 429 and retries < 8:
            retries += 1
            time.sleep(min(0.5, 0.005 * 2**retries))
            continue
        if 200 <= status < 300:
            return status, value
        if crashing.is_set() and status == 0:
            return None
        raise RuntimeError(f"{method} {path}: HTTP {status}: {value}")


def validate_oracle(
    port: int,
    oracle: dict[tuple[str, str], tuple[bool, str, int]],
    uncertain: dict[tuple[str, str], set[tuple[bool, str, int]]],
) -> int:
    by_index: dict[str, list[tuple[str, set[tuple[bool, str, int]]]]] = defaultdict(list)
    for index, identifier in sorted(set(oracle) | set(uncertain)):
        key = (index, identifier)
        options = uncertain[key] if key in uncertain else {oracle[key]}
        by_index[index].append((identifier, options))
    failures = []
    actual_live = 0
    for index, rows in by_index.items():
        for offset in range(0, len(rows), 1000):
            part = rows[offset : offset + 1000]
            answer = must(
                port,
                "POST",
                f"/{index}/_mget",
                {"ids": [identifier for identifier, _ in part]},
            )
            docs = answer.get("docs", [])
            if len(docs) != len(part):
                failures.append(f"{index}: mget вернул {len(docs)} из {len(part)}")
                continue
            for expected, actual in zip(part, docs):
                identifier, options = expected
                actual_found = actual.get("found") is True
                fields = actual.get("fields") or {}
                actual_state = (
                    (True, str(fields.get("marker")), int(fields.get("revision", -1)))
                    if actual_found
                    else (False, "", 0)
                )
                actual_live += int(actual_found)
                if actual.get("id") != identifier or actual_state not in options:
                    failures.append(
                        f"{index}/{identifier}: состояние={actual_state}, "
                        f"допустимы {sorted(options)}"
                    )
                if len(failures) >= 20:
                    raise AssertionError("oracle разошёлся:\n" + "\n".join(failures))
    if failures:
        raise AssertionError("oracle разошёлся:\n" + "\n".join(failures))
    return actual_live


def run() -> dict[str, Any]:
    if not BIN.is_file():
        raise SystemExit(f"release-бинарник не найден: {BIN}")
    if DURATION < 3 or WRITERS < 1 or READERS < 1 or ROLLOVERS < 1:
        raise SystemExit("нужны >=3 секунд, >=1 writer/reader/rollover")

    root = Path(tempfile.mkdtemp(prefix="svindb-alias-stress-gate-"))
    node: Node | None = None
    stop = threading.Event()
    crashing = threading.Event()
    phase = Phase()
    oracle: dict[tuple[str, str], tuple[bool, str, int]] = {}
    # Для запроса, оборванного самим kill, ACK неизвестен. Oracle хранит оба
    # законных исхода, но только для конкретной текущей операции; все ранее
    # подтверждённые состояния остаются обязательными.
    uncertain: dict[tuple[str, str], set[tuple[bool, str, int]]] = {}
    oracle_lock = threading.Lock()
    errors: list[str] = []
    errors_lock = threading.Lock()
    counters: Counter[str] = Counter()
    counter_lock = threading.Lock()

    def failed(error: BaseException) -> None:
        with errors_lock:
            errors.append(repr(error))
        stop.set()

    def writer(number: int) -> None:
        client = JsonClient(node.port)
        sequence = 0
        try:
            while not stop.is_set():
                generation = phase.enter()
                try:
                    identifier = f"stress-{number:02d}-{sequence:09d}"
                    index = generation_name(generation)
                    key = (index, identifier)
                    marker_one = f"w{number}-s{sequence}-r1"
                    with oracle_lock:
                        uncertain[key] = {(False, "", 0), (True, marker_one, 1)}
                    inserted = checked_request(
                        client,
                        "POST",
                        f"/{ALIAS}/_bulk",
                        {
                            "documents": [
                                {
                                    "id": identifier,
                                    "body": "stress_marker insert общий",
                                    "marker": marker_one,
                                    "revision": 1,
                                    "worker": number,
                                }
                            ]
                        },
                        crashing,
                    )
                    if inserted is None:
                        return
                    with oracle_lock:
                        oracle[key] = (True, marker_one, 1)
                        uncertain.pop(key, None)
                    with counter_lock:
                        counters["insert"] += 1

                    marker_two = f"w{number}-s{sequence}-r2"
                    with oracle_lock:
                        uncertain[key] = {(True, marker_one, 1), (True, marker_two, 2)}
                    updated = checked_request(
                        client,
                        "POST",
                        f"/{ALIAS}/_put",
                        {
                            "documents": [
                                {
                                    "id": identifier,
                                    "body": "stress_marker update общий",
                                    "marker": marker_two,
                                    "revision": 2,
                                    "worker": number,
                                }
                            ]
                        },
                        crashing,
                    )
                    if updated is None:
                        return
                    with oracle_lock:
                        oracle[key] = (True, marker_two, 2)
                        uncertain.pop(key, None)
                    with counter_lock:
                        counters["update"] += 1

                    if sequence % 4 == 0:
                        with oracle_lock:
                            uncertain[key] = {(True, marker_two, 2), (False, "", 0)}
                        deleted = checked_request(
                            client,
                            "POST",
                            f"/{ALIAS}/_delete",
                            {"ids": [identifier]},
                            crashing,
                        )
                        if deleted is None:
                            return
                        with oracle_lock:
                            oracle[key] = (False, "", 0)
                            uncertain.pop(key, None)
                        with counter_lock:
                            counters["delete"] += 1
                    sequence += 1
                except BaseException as error:  # noqa: BLE001 - поток обязан передать любой отказ
                    if not crashing.is_set():
                        failed(error)
                    return
                finally:
                    phase.leave()
        finally:
            client.close()

    def reader() -> None:
        client = JsonClient(node.port)
        try:
            while not stop.is_set():
                try:
                    answer = checked_request(
                        client,
                        "POST",
                        f"/{ALIAS}/_search",
                        {
                            "words": [{"field": "body", "terms": ["stress_marker"]}],
                            "k": 25,
                            "ids": True,
                        },
                        crashing,
                    )
                    if answer is None:
                        return
                    with counter_lock:
                        counters["search"] += 1
                except BaseException as error:  # noqa: BLE001
                    if not crashing.is_set():
                        failed(error)
                    return
        finally:
            client.close()

    started = time.monotonic()
    try:
        node = Node(root)
        must(
            node.port,
            "PUT",
            f"/{generation_name(1)}",
            {
                "fields": "body:text,marker:keyword,revision:int64,worker:int64",
                "metadata": {
                    "description": "смешанный alias stress",
                    "tags": ["release-gate", "stress"],
                },
                "aliases": {ALIAS: {"is_write_index": True}},
            },
        )
        workers = [threading.Thread(target=writer, args=(number,), daemon=True) for number in range(WRITERS)]
        readers = [threading.Thread(target=reader, daemon=True) for _ in range(READERS)]
        for thread in workers + readers:
            thread.start()

        rollover_at = [DURATION * (number + 1) / (ROLLOVERS + 1) for number in range(ROLLOVERS)]
        completed_rollovers = 0
        while time.monotonic() - started < DURATION and not stop.is_set():
            elapsed = time.monotonic() - started
            if completed_rollovers < len(rollover_at) and elapsed >= rollover_at[completed_rollovers]:
                phase.pause()
                try:
                    answer = must(node.port, "POST", f"/{ALIAS}/_rollover", {})
                    if answer.get("rolled_over") is not True:
                        raise AssertionError(f"rollover не выполнен: {answer}")
                    phase.advanced()
                    completed_rollovers += 1
                    with counter_lock:
                        counters["rollover"] += 1
                except BaseException:
                    phase.resume_without_change()
                    raise
            time.sleep(0.01)

        if errors:
            raise AssertionError("нагрузка отказала до аварии: " + "; ".join(errors[:20]))
        # Убийство происходит без pause: в этот момент есть активные HTTP
        # записи и поиски. В oracle попали только ответы, для которых клиент
        # уже увидел 2xx; оборванный ответ намеренно считается неопределённым.
        crashing.set()
        node.child.kill()
        node.child.wait(timeout=30)
        stop.set()
        for thread in workers + readers:
            thread.join(timeout=30)

        node = Node(root)
        actual_oracle_live = validate_oracle(node.port, oracle, uncertain)
        counted = must(
            node.port,
            "POST",
            f"/{ALIAS}/_search",
            {"match_all": True, "k": 1, "aggs": {"count": {"stats": {}}}},
        )
        actual_live = int(counted["aggs"]["count"]["count"])
        if actual_live != actual_oracle_live:
            raise AssertionError(
                f"после crash/restart live={actual_live}, oracle={actual_oracle_live}"
            )
        aliases = must(node.port, "GET", f"/_alias/{ALIAS}")
        if len(aliases) != ROLLOVERS + 1:
            raise AssertionError(f"alias targets={len(aliases)}, ожидалось {ROLLOVERS + 1}")
        rows = must(node.port, "GET", "/_cat/indices")
        writers_after = [row for row in rows if row.get("write_aliases") == [ALIAS]]
        if len(writers_after) != 1:
            raise AssertionError(f"после crash write index неоднозначен: {writers_after}")
        for row in rows:
            if row not in writers_after and "write" not in row.get("blocks", []):
                raise AssertionError(f"старое поколение потеряло write block: {row}")
        node.stop()
        full_check(root)
        result = {
            "documents_checked": len(set(oracle) | set(uncertain)),
            "elapsed_seconds": round(time.monotonic() - started, 2),
            "expected_live": actual_oracle_live,
            "generations": ROLLOVERS + 1,
            "operations": dict(sorted(counters.items())),
            "ok": True,
            "root": str(root) if KEEP else None,
            "transport_errors_before_crash": 0,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return result
    finally:
        stop.set()
        if node is not None:
            node.stop()
        if KEEP:
            print(f"root сохранён: {root}")
        else:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    run()
