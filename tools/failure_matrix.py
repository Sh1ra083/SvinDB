#!/usr/bin/env python3
"""Матрица аварийных окон SvinDB с перезапуском и независимыми проверками.

Выпускной прогон использует бинарник, собранный с feature ``failpoints``.
Каждая точка срабатывает в отдельном процессе и отдельной копии индекса.
Зелёным считается не сам подъём, а точное прикладное состояние, один живой
marker, неизменный top-K, порядок WAL и успешный ``check --full`` остановленного
индекса. Успешные каталоги удаляются; каталог любого отказа сохраняется.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benchmark_campaign import append_json, atomic_json, json_request, one_request, sha256_file


FIELDS = "--fields=kind:keyword,revision:int64,state:keyword,body:1"


@dataclass(frozen=True)
class Case:
    point: str
    operation: str
    trigger_flush: bool = False


CASES = (
    Case("wal.after_fsync_before_ack", "bulk"),
    Case("add.after_apply_before_ack", "bulk"),
    Case("put.after_apply_before_ack", "put"),
    Case("delete.after_apply_before_seq", "delete"),
    Case("mutation.after_replace_before_delete", "mutation"),
    Case("mutation.after_delete_before_ack", "mutation"),
    Case("manifest.before_publish", "bulk", True),
    Case("manifest.after_publish", "bulk", True),
    Case("checkpoint.after_write", "bulk", True),
    Case("checkpoint.after_trim", "bulk", True),
    Case("replication.after_apply_before_done", "replicate"),
)


def runtime_revision(version: str) -> str:
    """Извлечь короткий или полный revision из `--version` без потери provenance."""
    match = re.search(r"\brevision=([0-9a-f]{7,40})\b", version)
    return match.group(1) if match else "unknown"


class MatrixError(RuntimeError):
    """Нарушение, которое делает отдельную ячейку красной."""


def owned_listener_port(pid: int) -> int | None:
    """Порт берётся у открытого socket дочернего процесса, не резервируется пробой."""
    owned = set()
    try:
        for descriptor in (Path("/proc") / str(pid) / "fd").iterdir():
            with contextlib.suppress(OSError):
                target = os.readlink(descriptor)
                if target.startswith("socket:[") and target.endswith("]"):
                    owned.add(target[8:-1])
        ports = set()
        if not owned:
            return None
        for protocol in ("tcp", "tcp6"):
            table = Path("/proc") / str(pid) / "net" / protocol
            try:
                lines = table.read_text().splitlines()[1:]
            except FileNotFoundError:
                continue
            for line in lines:
                fields = line.split()
                if len(fields) > 9 and fields[3] == "0A" and fields[9] in owned:
                    ports.add(int(fields[1].rsplit(":", 1)[1], 16))
        if len(ports) > 1:
            raise MatrixError(f"у тестового процесса {pid} несколько listeners: {sorted(ports)}")
        return next(iter(ports), None)
    except (FileNotFoundError, ProcessLookupError):
        return None


def splitmix(value: int) -> int:
    value = (value + 0x9E3779B97F4A7C15) & ((1 << 64) - 1)
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & ((1 << 64) - 1)
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & ((1 << 64) - 1)
    return value ^ (value >> 31)


def document(identifier: str, marker: str, revision: int, kind: str = "crash_target") -> dict[str, Any]:
    return {
        "id": identifier,
        "kind": kind,
        "revision": revision,
        "state": marker,
        "body": f"аварийная матрица {marker} версия {revision}",
    }


class Node:
    def __init__(
        self,
        binary: Path,
        index: Path,
        port: int,
        log: Path,
        failpoint: str | None = None,
    ) -> None:
        self.binary = binary
        self.index = index
        self.port = port
        self.log = log
        self.failpoint = failpoint
        self.process: subprocess.Popen[bytes] | None = None
        self._stream: Any = None

    def start(self, timeout: float) -> None:
        self.log.parent.mkdir(parents=True, exist_ok=True)
        self._stream = self.log.open("ab", buffering=0)
        environment = os.environ.copy()
        environment["SVINDB_DISK_FLOOR_MB"] = "1"
        if self.failpoint:
            environment["SVINDB_FAILPOINTS"] = f"{self.failpoint}=kill,count=1"
        else:
            environment.pop("SVINDB_FAILPOINTS", None)
        command = [
            str(self.binary),
            "serve",
            str(self.index),
            str(self.port),
            "--single-index",
            FIELDS,
            "--flush=1000000",
            "--target=1000000",
            "--refresh-ms=0",
            "--no-maintain",
        ]
        self.process = subprocess.Popen(
            command,
            stdout=self._stream,
            stderr=subprocess.STDOUT,
            env=environment,
            start_new_session=True,
        )
        deadline = time.monotonic() + timeout
        last = "порт не открылся"
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise MatrixError(
                    f"узел завершился до старта кодом {self.process.returncode}; журнал {self.log}"
                )
            owned_port = owned_listener_port(self.process.pid)
            if owned_port is None:
                time.sleep(0.025)
                continue
            self.port = owned_port
            try:
                reply = one_request("127.0.0.1", self.port, "GET", "/_health", timeout=1)
                last = reply.data[:500].decode("utf-8", "replace")
                if reply.status == 200 and reply.json().get("ok"):
                    return
            except Exception as error:
                last = repr(error)
            time.sleep(0.025)
        raise MatrixError(f"узел не поднялся за {timeout:g} с: {last}; журнал {self.log}")

    def expect_crash(self, timeout: float) -> int:
        if self.process is None:
            raise MatrixError("процесс не был запущен")
        try:
            code = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            raise MatrixError(f"точка {self.failpoint} не сняла процесс") from error
        # Failpoint использует _exit(137), не произвольную ошибку процесса.
        # Без marker OOM/другая авария ошибочно считались нужным crash window.
        marker = f"SVINDB_FAILPOINTS: точка «{self.failpoint}» снимает процесс"
        with self.log.open("rb") as log:
            size = log.seek(0, os.SEEK_END)
            log.seek(max(0, size - 65536))
            tail = log.read().decode("utf-8", "replace")
        if code != 137 or marker not in tail:
            raise MatrixError(f"точка {self.failpoint} не подтверждена: exit={code}, marker={marker in tail}")
        return code

    def stop(self, timeout: float) -> int | None:
        if self.process is None:
            if self._stream is not None:
                self._stream.close()
                self._stream = None
            return None
        if self.process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=timeout)
        code = self.process.returncode
        self.process = None
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        return code

    def stop_clean(self, timeout: float) -> None:
        code = self.stop(timeout)
        if code != 0:
            raise MatrixError(f"штатная остановка завершилась кодом {code}; журнал {self.log}")

    def close_after_crash(self) -> None:
        self.process = None
        if self._stream is not None:
            self._stream.close()
            self._stream = None


def request(port: int, method: str, path: str, body: Any | None, timeout: float) -> tuple[int, Any]:
    reply = json_request("127.0.0.1", port, method, path, body, timeout)
    decoded: Any = None
    with contextlib.suppress(json.JSONDecodeError):
        decoded = reply.json()
    return reply.status, decoded


def accepted(port: int, path: str, body: Any, timeout: float) -> dict[str, Any]:
    status, decoded = request(port, "POST", path, body, timeout)
    if not 200 <= status < 300:
        raise MatrixError(f"{path}: HTTP {status}: {decoded}")
    if not isinstance(decoded, dict):
        raise MatrixError(f"{path}: ответ не JSON object: {decoded!r}")
    return decoded


def wait_flush(port: int, timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        status, last = request(port, "POST", "/_flush", {}, timeout)
        if status == 200:
            return last if isinstance(last, dict) else {}
        if status != 202:
            raise MatrixError(f"/_flush: HTTP {status}: {last}")
        time.sleep(0.05)
    raise MatrixError(f"flush не завершился за {timeout:g} с: {last}")


def topk(port: int, timeout: float) -> list[str]:
    decoded = accepted(
        port,
        "/",
        {"k": 10, "ids": True, "words": [{"field": "kind", "terms": ["stable_rank"]}]},
        timeout,
    )
    return [str(hit.get("id")) for hit in decoded.get("hits", [])]


def marker_hits(port: int, marker: str, timeout: float) -> list[str]:
    decoded = accepted(
        port,
        "/",
        {"k": 10, "ids": True, "words": [{"field": "state", "terms": [marker]}]},
        timeout,
    )
    return [str(hit.get("id")) for hit in decoded.get("hits", [])]


def get_document(port: int, identifier: str, timeout: float) -> tuple[bool, dict[str, Any]]:
    path = "/_doc/" + urllib.parse.quote(identifier, safe="")
    reply = one_request("127.0.0.1", port, "GET", path, timeout=timeout)
    if reply.status == 404:
        return False, {}
    if reply.status != 200:
        raise MatrixError(f"GET {identifier}: HTTP {reply.status}: {reply.data[:500]!r}")
    decoded = reply.json()
    return True, decoded.get("fields", {})


def stable_health(port: int, timeout: float, expected_live: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    stable_since: float | None = None
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            reply = one_request("127.0.0.1", port, "GET", "/_health", timeout=2)
            if reply.status != 200:
                time.sleep(0.05)
                continue
            last = reply.json()
            wal = last.get("wal", {})
            ordered = (
                0
                <= int(wal.get("checkpoint", -1))
                <= int(wal.get("applied", -1))
                <= int(wal.get("seq", -1))
            )
            quiet = (
                last.get("ok") is True
                and int(last.get("buffered", -1)) == 0
                and not last.get("flush_in_progress", False)
                and int(last.get("docs", -1)) - int(last.get("dead", -1)) == expected_live
                and int(last.get("write_queue", {}).get("pending", -1)) == 0
            )
            marks = last.get("watermarks", {})
            seq = int(wal.get("seq", -1))
            settled = (
                int(wal.get("checkpoint", -1)) == seq
                and int(wal.get("applied", -1)) == seq
                and all(int(marks.get(name, -1)) == seq for name in ("ack", "visible", "ready", "settled"))
                and int(last.get("compaction", {}).get("pending_segments", -1)) == 0
                and int(last.get("compaction", {}).get("running_segments", -1)) == 0
            )
            if ordered and quiet and settled:
                stable_since = stable_since or time.monotonic()
                if time.monotonic() - stable_since >= 0.25:
                    return last
            else:
                stable_since = None
        except Exception:
            stable_since = None
        time.sleep(0.05)
    raise MatrixError(f"после восстановления нет тихого ordered-состояния: {last}")


def full_check(binary: Path, index: Path, timeout: float) -> dict[str, Any]:
    completed = subprocess.run(
        [str(binary), "check", str(index), "--full", "--json"],
        capture_output=True,
        text=True,
        timeout=max(120.0, timeout),
        check=False,
    )
    try:
        decoded = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise MatrixError(
            f"check --full вернул не JSON: code={completed.returncode}, "
            f"stdout={completed.stdout[-1000:]!r}, stderr={completed.stderr[-1000:]!r}"
        ) from error
    if not (
        completed.returncode == 0
        and decoded.get("ok") is True
        and decoded.get("broken") == 0
        and decoded.get("depth") == "full"
    ):
        raise MatrixError(f"check --full не прошёл: code={completed.returncode}: {decoded}")
    return decoded


def clone_template(template: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(template, destination, copy_function=shutil.copy2)


def make_template(binary: Path, root: Path, timeout: float) -> tuple[Path, list[str]]:
    template = root / "template"
    if template.exists():
        shutil.rmtree(template)
    port = 0
    node = Node(binary, template, port, root / "template.log")
    try:
        node.start(timeout)
        port = node.port
        docs = [
            document(f"stable-{number:03}", f"stable-marker-{number:03}", 1, "stable_rank")
            for number in range(20)
        ]
        accepted(port, "/_bulk", {"documents": docs}, timeout)
        wait_flush(port, timeout)
        expected = topk(port, timeout)
        if len(expected) != 10 or len(set(expected)) != 10:
            raise MatrixError(f"template top-K негоден: {expected}")
        stable_health(port, timeout, 20)
    finally:
        node.stop_clean(timeout)
    full_check(binary, template, timeout)
    return template, expected


def trigger_operation(case: Case, port: int, seed: int, timeout: float) -> dict[str, Any]:
    identifier = f"failure-{case.operation}-{seed:016x}"
    marker = f"marker-{case.point.replace('.', '-')}-{seed:016x}"
    old_marker = marker + "-old"
    deleted_id = identifier + "-old"

    if case.operation == "put":
        accepted(port, "/_bulk", {"documents": [document(identifier, old_marker, 1)]}, timeout)
    elif case.operation == "delete":
        accepted(port, "/_bulk", {"documents": [document(identifier, old_marker, 1)]}, timeout)
    elif case.operation == "mutation":
        accepted(port, "/_bulk", {"documents": [document(deleted_id, old_marker, 1)]}, timeout)

    before = topk(port, timeout)
    path = "/_bulk"
    body: dict[str, Any] = {"documents": [document(identifier, marker, 2)]}
    expected_present = True
    if case.operation == "bulk":
        body = {"documents": [document(identifier, marker, 1)]}
    elif case.operation == "put":
        path = "/_put"
    elif case.operation == "delete":
        path = "/_delete"
        body = {"ids": [identifier]}
        expected_present = False
    elif case.operation == "mutation":
        path = "/_mutate"
        body = {"documents": [document(identifier, marker, 2)], "deletes": [deleted_id]}

    ack: dict[str, Any] = {"observed": False}
    try:
        status, decoded = request(port, "POST", path, body, timeout)
        ack = {"observed": True, "status": status, "body": decoded}
    except Exception as error:
        ack = {"observed": False, "transport": repr(error)}
    if case.trigger_flush:
        if not ack.get("observed") or not 200 <= int(ack.get("status", 0)) < 300:
            raise MatrixError(f"операция до аварийного flush не подтверждена: {ack}")
        try:
            status, decoded = request(port, "POST", "/_flush", {}, timeout)
            ack["flush"] = {"observed": True, "status": status, "body": decoded}
        except Exception as error:
            ack["flush"] = {"observed": False, "transport": repr(error)}

    return {
        "id": identifier,
        "deleted_id": deleted_id if case.operation == "mutation" else None,
        "marker": marker,
        "old_marker": old_marker,
        "revision": 1 if case.operation == "bulk" else 2,
        "expected_present": expected_present,
        "topk_before": before,
        "ack": ack,
    }


def verify_operation(port: int, operation: str, expected: dict[str, Any], timeout: float) -> dict[str, Any]:
    found, fields = get_document(port, expected["id"], timeout)
    if expected["expected_present"]:
        if not found:
            raise MatrixError(f"durable {operation} потерял {expected['id']}")
        if fields.get("state") != expected["marker"] or int(fields.get("revision", -1)) != expected["revision"]:
            raise MatrixError(f"{expected['id']}: восстановлена не та версия: {fields}")
        hits = marker_hits(port, expected["marker"], timeout)
        if hits != [expected["id"]]:
            raise MatrixError(f"marker новой версии найден не один раз: {hits}")
    else:
        if found:
            raise MatrixError(f"durable delete воскресил {expected['id']}: {fields}")
        hits = marker_hits(port, expected["old_marker"], timeout)
        if hits:
            raise MatrixError(f"marker удалённой версии остался в поиске: {hits}")
    if operation == "put":
        old = marker_hits(port, expected["old_marker"], timeout)
        if old:
            raise MatrixError(f"старая put-версия осталась в поиске: {old}")
    if operation == "mutation":
        deleted, deleted_fields = get_document(port, expected["deleted_id"], timeout)
        if deleted:
            raise MatrixError(f"delete-половина mutation потеряна: {deleted_fields}")
        if marker_hits(port, expected["old_marker"], timeout):
            raise MatrixError("старый marker mutation остался в поиске")
    after = topk(port, timeout)
    if after != expected["topk_before"]:
        raise MatrixError(
            f"top-K изменился после crash: before={expected['topk_before']}, after={after}"
        )
    return {"found": found, "fields": fields, "marker_hits": marker_hits(port, expected["marker"], timeout), "topk_after": after}


def replication_record(seed: int) -> tuple[str, str, str]:
    identifier = f"failure-replicate-{seed:016x}"
    marker = f"marker-replication-{seed:016x}"
    body = json.dumps(
        {"documents": [document(identifier, marker, 1)]},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    record = {"term": 1, "at": 1, "op": "add", "body": body}
    return identifier, marker, json.dumps(record, ensure_ascii=False, separators=(",", ":"))


def run_replication_case(
    args: argparse.Namespace,
    case: Case,
    case_root: Path,
    seed: int,
    nodes: list[Node],
) -> dict[str, Any]:
    index = case_root / "index"
    port = 0
    crashed = Node(args.binary, index, port, case_root / "crashed.log", case.point)
    nodes.append(crashed)
    crashed.start(args.timeout)
    port = crashed.port
    identifier, marker, raw = replication_record(seed)
    try:
        one_request(
            "127.0.0.1",
            port,
            "POST",
            "/_replicate",
            raw.encode(),
            {"Content-Type": "application/json"},
            args.timeout,
        )
    except Exception:
        pass
    crash_code = crashed.expect_crash(args.timeout)
    crashed.close_after_crash()

    recovered = Node(args.binary, index, 0, case_root / "recovered.log")
    nodes.append(recovered)
    recovered.start(args.timeout)
    port = recovered.port
    try:
        reply = one_request(
            "127.0.0.1",
            port,
            "POST",
            "/_replicate",
            raw.encode(),
            {"Content-Type": "application/json"},
            args.timeout,
        )
        if reply.status != 200 or reply.json().get("applied") is not True:
            raise MatrixError(f"повтор репликации не завершил отметку: {reply.status} {reply.data!r}")
        wait_flush(port, args.timeout)
        found, fields = get_document(port, identifier, args.timeout)
        hits = marker_hits(port, marker, args.timeout)
        if not found or fields.get("state") != marker or hits != [identifier]:
            raise MatrixError(f"повтор репликации не идемпотентен: found={found}, fields={fields}, hits={hits}")
        health = stable_health(port, args.timeout, 1)
    finally:
        recovered.stop_clean(args.timeout)
    checked = full_check(args.binary, index, args.timeout)
    return {"crash_code": crash_code, "health": health, "check": checked, "hits": hits}


def run_case(
    args: argparse.Namespace,
    case: Case,
    template: Path,
    expected_topk: list[str],
    ordinal: int,
) -> dict[str, Any]:
    seed = splitmix(args.start_seed + ordinal)
    case_name = f"{case.point.replace('.', '_')}-{ordinal:04}-{seed:016x}"
    case_root = args.root / "cases" / case_name
    case_root.mkdir(parents=True, exist_ok=False)
    began = time.perf_counter()
    result: dict[str, Any] = {
        "case": case_name,
        "point": case.point,
        "operation": case.operation,
        "seed": seed,
        "valid": False,
    }
    nodes: list[Node] = []
    try:
        if case.operation == "replicate":
            result.update(run_replication_case(args, case, case_root, seed, nodes))
        else:
            index = case_root / "index"
            clone_template(template, index)
            port = 0
            crashed = Node(args.binary, index, port, case_root / "crashed.log", case.point)
            nodes.append(crashed)
            crashed.start(args.timeout)
            port = crashed.port
            expected = trigger_operation(case, port, seed, args.timeout)
            if expected["topk_before"] != expected_topk:
                raise MatrixError(
                    f"исходный top-K разошёлся с template: {expected['topk_before']} != {expected_topk}"
                )
            crash_code = crashed.expect_crash(args.timeout)
            crashed.close_after_crash()

            recovered = Node(args.binary, index, 0, case_root / "recovered.log")
            nodes.append(recovered)
            recovered.start(args.timeout)
            port = recovered.port
            try:
                wait_flush(port, args.timeout)
                verification = verify_operation(port, case.operation, expected, args.timeout)
                health = stable_health(port, args.timeout, 20 + int(expected["expected_present"]))
            finally:
                recovered.stop_clean(args.timeout)
            checked = full_check(args.binary, index, args.timeout)
            result.update(
                {
                    "crash_code": crash_code,
                    "expected": expected,
                    "verification": verification,
                    "health": health,
                    "check": checked,
                }
            )
        result["valid"] = True
    except Exception as error:
        result["error"] = repr(error)
    finally:
        # Не сработавшая точка или ошибка startup не оставляют живые процессы
        # перед следующей ячейкой. Удалять её каталог до cleanup нельзя.
        for node in reversed(nodes):
            try:
                node.stop(args.timeout)
            except Exception as error:
                result["valid"] = False
                result.setdefault("cleanup_errors", []).append(repr(error))
    result["seconds"] = time.perf_counter() - began
    atomic_json(case_root / "result.json", result)
    if result["valid"] and not args.keep_success:
        shutil.rmtree(case_root)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seeds", type=int, default=100, help="повторов на каждую точку")
    parser.add_argument("--start-seed", type=int, default=0x5A17C0DE20260815)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--keep-success", action="store_true")
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        help="точное имя точки; без ключа прогоняются все",
    )
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("crash matrix выполняется только на Linux build host")
    args.binary = args.binary.resolve()
    args.root = args.root.resolve()
    args.output = args.output.resolve()
    if args.seeds < 1:
        parser.error("--seeds должен быть положительным")
    if args.timeout <= 0:
        parser.error("--timeout должен быть положительным")
    if args.output.exists():
        parser.error(f"output уже существует: {args.output}")
    if not args.binary.is_file():
        parser.error(f"бинарник не найден: {args.binary}")
    known = {case.point for case in CASES}
    unknown = set(args.case) - known
    if unknown:
        parser.error(f"неизвестные точки: {sorted(unknown)}")
    return args


def main() -> int:
    args = parse_args()
    args.root.mkdir(parents=True, exist_ok=False)
    selected = [case for case in CASES if not args.case or case.point in args.case]
    manifest = {
        "format": 1,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "binary": str(args.binary),
        "binary_sha256": sha256_file(args.binary),
        "python": sys.version,
        "platform": platform.platform(),
        "seeds_per_point": args.seeds,
        "start_seed": args.start_seed,
        "points": [case.point for case in selected],
        "pid": os.getpid(),
        "harness_sha256": sha256_file(Path(__file__).resolve()),
    }
    version = subprocess.check_output([str(args.binary), "--version"], text=True).strip()
    manifest["binary_version"] = version
    # Release-сборки стенда могут использовать короткий git revision (7–40
    # символов). Отбрасывать его как `unknown` значит терять provenance ровно
    # у тех матриц, которые должны быть привязаны к конкретному кандидату.
    manifest["runtime_revision"] = runtime_revision(version)
    atomic_json(args.root / "manifest.json", manifest)

    template, expected_topk = make_template(args.binary, args.root, args.timeout)
    results = []
    failures = 0
    ordinal = 0
    for case in selected:
        for _ in range(args.seeds):
            result = run_case(args, case, template, expected_topk, ordinal)
            results.append(result)
            append_json(args.root / "results.jsonl", result, sync=True)
            ordinal += 1
            if not result["valid"]:
                failures += 1
                print(
                    f"FAIL {result['point']} seed={result['seed']:016x}: {result.get('error')}",
                    file=sys.stderr,
                    flush=True,
                )
                if result.get("cleanup_errors"):
                    raise MatrixError(f"кампания остановлена: процессы {result['case']} не очищены")
            elif ordinal % 25 == 0:
                print(f"готово {ordinal}/{len(selected) * args.seeds}", flush=True)

    by_point: dict[str, dict[str, Any]] = {}
    for case in selected:
        rows = [row for row in results if row["point"] == case.point]
        times = sorted(float(row["seconds"]) for row in rows)
        by_point[case.point] = {
            "operation": case.operation,
            "runs": len(rows),
            "passed": sum(bool(row["valid"]) for row in rows),
            "failed": sum(not row["valid"] for row in rows),
            "seconds_total": sum(times),
            "seconds_p50": times[len(times) // 2] if times else None,
            "failed_seeds": [row["seed"] for row in rows if not row["valid"]],
        }
    summary = {
        "format": 1,
        "valid": failures == 0,
        "runs": len(results),
        "passed": len(results) - failures,
        "failed": failures,
        "seeds_per_point": args.seeds,
        "by_point": by_point,
        "failed_cases": [row for row in results if not row["valid"]],
        "manifest": manifest,
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    atomic_json(args.output, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
