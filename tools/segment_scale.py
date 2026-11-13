#!/usr/bin/env python3
"""Измеряет практический предел числа сегментов без синтетики внутри движка.

Сценарий сначала штатно строит один настоящий сегмент, а затем создаёт
read-only индекс из множества каталогов, чьи неизменяемые файлы являются
жёсткими ссылками на этот сегмент. Это намеренно не модель распределения ID и
не замена корпусному тесту. Это изолированный худший случай топологии: реальный
парсер manifest, реальные mmap, реальные курсоры, агрегации и HTTP-путь платят
за 1/10/100/1 000/10 000 сегментов, не требуя записать миллиарды документов.

Жёсткие ссылки безопасны только потому, что измеряемый узел запускается с
``--read-only``. Скрипт проверяет этот контракт до каждого запуска и никогда не
переиспользует каталог результата, чтобы не перезаписать доказательства.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import statistics
import subprocess
import sys
import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchmark_campaign as campaign  # noqa: E402


class ScaleError(RuntimeError):
    """Отказ сценария, после которого результат нельзя считать доказательством."""


FIELDS = "message:2,channel:keyword,views:int64"


def atomic_bytes(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}")
    with temporary.open("xb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def atomic_json(path: Path, value: Any) -> None:
    atomic_bytes(
        path,
        (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )


def manifest_bytes(names: list[str], generation: int) -> bytes:
    if len(names) > 30_000:
        raise ScaleError(f"запрошено {len(names)} сегментов при формате не более 30000")
    for name in names:
        if len(name) > 32 or not name.startswith(("seg", "mrg", "cln")):
            raise ScaleError(f"негодное имя сегмента {name!r}")
        if not all(character.isalnum() or character in "_-" for character in name):
            raise ScaleError(f"негодное имя сегмента {name!r}")
    names_body = "".join(f"{name}\n" for name in names).encode("utf-8")
    crc = zlib.crc32(names_body) & 0xFFFF_FFFF
    head = f"#svindb-manifest\t{generation}\t{len(names)}\t{crc:08x}\n".encode()
    body = head + names_body
    if len(body) > 1 << 20:
        raise ScaleError(f"manifest весит {len(body)} Б при формате не более 1048576 Б")
    return body


def request(
    port: int,
    method: str,
    path: str,
    body: Any | None = None,
    timeout: float = 120,
) -> tuple[int, Any]:
    reply = campaign.json_request("127.0.0.1", port, method, path, body, timeout)
    try:
        decoded = reply.json()
    except Exception:
        decoded = reply.data.decode("utf-8", "replace")
    return reply.status, decoded


def wait_health(port: int, process: subprocess.Popen[bytes], timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise ScaleError(f"узел завершился до health с кодом {process.returncode}: {last}")
        try:
            status, decoded = request(port, "GET", "/_health", timeout=2)
            last = f"HTTP {status}: {decoded!r}"
            if status == 200 and isinstance(decoded, dict) and decoded.get("ok") is True:
                return decoded
        except Exception as error:
            last = repr(error)
        time.sleep(0.05)
    raise ScaleError(f"узел не стал healthy за {timeout} с: {last}")


def stop(process: subprocess.Popen[bytes], timeout: float = 30) -> None:
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def start_node(
    binary: Path,
    index: Path,
    port: int,
    log: Path,
    extra: list[str],
) -> tuple[subprocess.Popen[bytes], Any]:
    log.parent.mkdir(parents=True, exist_ok=True)
    stream = log.open("wb")
    try:
        process = subprocess.Popen(
            [str(binary), "serve", str(index), str(port), "--single-index", *extra],
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except Exception:
        stream.close()
        raise
    # Держим stream рядом с процессом: закрытие до завершения допустимо на Unix,
    # но усложняет диагностику на других системах.
    process._svindb_log_stream = stream  # type: ignore[attr-defined]
    return process, stream


def close_node(process: subprocess.Popen[bytes]) -> None:
    stop(process)
    stream = getattr(process, "_svindb_log_stream", None)
    if stream is not None:
        stream.close()


def template_documents(amount: int) -> list[dict[str, Any]]:
    return [
        {
            "id": f"scale-{number:06}",
            "message": f"common bucket{number % 10} phraseone phrasetwo unique{number}",
            "channel": f"channel-{number % 17}",
            "views": number,
        }
        for number in range(amount)
    ]


def build_template(args: argparse.Namespace, root: Path) -> Path:
    index = root / "template-index"
    index.mkdir()
    process, _ = start_node(
        args.binary,
        index,
        args.port,
        root / "logs" / "template.log",
        [f"--fields={FIELDS}", "--no-maintain", f"--flush={args.template_documents}"],
    )
    try:
        wait_health(args.port, process, args.start_timeout)
        status, decoded = request(
            args.port,
            "POST",
            "/_bulk",
            {"documents": template_documents(args.template_documents)},
            args.timeout,
        )
        if status != 200:
            raise ScaleError(f"приём шаблона: HTTP {status}: {decoded}")
        status, decoded = request(args.port, "POST", "/_flush", {}, args.timeout)
        if status != 200:
            raise ScaleError(f"flush шаблона: HTTP {status}: {decoded}")
        deadline = time.monotonic() + args.start_timeout
        stats: dict[str, Any] = {}
        while time.monotonic() < deadline:
            status, decoded = request(args.port, "GET", "/_stats", timeout=5)
            if status == 200 and isinstance(decoded, dict):
                stats = decoded
                if decoded.get("docs") == args.template_documents and decoded.get("segments") == 1:
                    break
            time.sleep(0.05)
        else:
            raise ScaleError(f"шаблон не сошёлся к одному сегменту: {stats}")
    finally:
        close_node(process)
    segments = sorted(
        path for path in index.iterdir() if path.is_dir() and (path / "head.tsv").is_file()
    )
    if len(segments) != 1:
        raise ScaleError(f"у шаблона найдено {len(segments)} каталогов сегментов вместо одного")
    return segments[0]


def hardlink_tree(source: Path, destination: Path) -> None:
    destination.mkdir()
    for path in source.rglob("*"):
        relative = path.relative_to(source)
        target = destination / relative
        if path.is_symlink():
            raise ScaleError(f"символическая ссылка в шаблоне запрещена: {path}")
        if path.is_dir():
            target.mkdir()
        elif path.is_file():
            os.link(path, target)
        else:
            raise ScaleError(f"необычный член шаблона запрещён: {path}")


def prepare_scale_index(root: Path, template: Path, maximum: int) -> tuple[Path, list[str]]:
    index = root / "scale-index"
    index.mkdir()
    schema = template.parent / "schema.json"
    if not schema.is_file():
        raise ScaleError(f"у шаблона нет схемы {schema}")
    shutil.copy2(schema, index / schema.name)
    names = [f"seg-scale-{number:020}" for number in range(maximum)]
    for ordinal, name in enumerate(names, 1):
        hardlink_tree(template, index / name)
        if ordinal % 1000 == 0:
            print(f"  подготовлено {ordinal:,}/{maximum:,} каталогов", flush=True)
    return index, names


def proc_maps(pid: int) -> int | None:
    try:
        with Path(f"/proc/{pid}/maps").open(encoding="utf-8", errors="replace") as stream:
            return sum(1 for _ in stream)
    except OSError:
        return None


def latency(values: list[float]) -> dict[str, Any]:
    result = campaign.latency_summary(values)
    result["mean_ms"] = statistics.fmean(values) if values else None
    return result


@dataclass(frozen=True)
class Query:
    name: str
    method: str
    path: str
    body: Any


def queries(template_documents_count: int) -> list[Query]:
    return [
        Query(
            "search/miss",
            "POST",
            "/",
            {"k": 10, "words": [{"field": "message", "terms": ["never-present"]}], "ids": True},
        ),
        Query(
            "search/frequent",
            "POST",
            "/",
            {"k": 10, "words": [{"field": "message", "terms": ["common"]}], "ids": True},
        ),
        Query(
            "search/must",
            "POST",
            "/",
            {
                "k": 10,
                "must": [
                    {"field": "message", "terms": ["common"]},
                    {"field": "message", "terms": ["bucket7"]},
                ],
                "ids": True,
            },
        ),
        Query(
            "search/phrase",
            "POST",
            "/",
            {
                "k": 10,
                "phrases": [{"field": "message", "terms": ["phraseone", "phrasetwo"], "slop": 0}],
                "ids": True,
            },
        ),
        Query(
            "agg/count",
            "POST",
            "/",
            {"k": 0, "filter": [{"field": "views"}], "aggs": {"all": {"count": {}}}},
        ),
        Query(
            "agg/cardinality",
            "POST",
            "/",
            {
                "k": 0,
                "filter": [{"field": "views"}],
                "aggs": {"channels": {"cardinality": {"field": "channel"}}},
            },
        ),
        Query(
            "mget/100",
            "POST",
            "/_mget",
            {"ids": [f"scale-{number % template_documents_count:06}" for number in range(100)]},
        ),
    ]


def validate_reply(query: Query, decoded: Any, total: int) -> None:
    if not isinstance(decoded, dict):
        raise ScaleError(f"{query.name}: ответ не JSON-объект: {decoded!r}")
    if query.name == "search/miss":
        if decoded.get("hits") != []:
            raise ScaleError(f"{query.name}: ожидался пустой ответ: {decoded!r}")
    elif query.name.startswith("search/"):
        if len(decoded.get("hits", [])) != 10:
            raise ScaleError(f"{query.name}: ожидалось 10 результатов: {decoded!r}")
    elif query.name == "agg/count":
        got = decoded.get("aggs", {}).get("all", {}).get("count")
        if got != total:
            raise ScaleError(f"{query.name}: count={got}, ожидалось {total}")
    elif query.name == "agg/cardinality":
        got = decoded.get("aggs", {}).get("channels", {}).get("value")
        if got != 17:
            raise ScaleError(f"{query.name}: cardinality={got}, ожидалось 17")
    elif query.name == "mget/100":
        docs = decoded.get("docs", [])
        if len(docs) != 100 or not all(item.get("found") is True for item in docs):
            raise ScaleError(f"{query.name}: не все 100 документов найдены: {decoded!r}")


def measure_queries(args: argparse.Namespace, count: int) -> tuple[dict[str, Any], int]:
    total = count * args.template_documents
    results: dict[str, Any] = {}
    errors = 0
    for query in queries(args.template_documents):
        timings: list[float] = []
        last: Any = None
        for ordinal in range(args.warmup + args.repetitions):
            began = time.perf_counter_ns()
            status, decoded = request(args.port, query.method, query.path, query.body, args.timeout)
            elapsed_ms = (time.perf_counter_ns() - began) / 1_000_000
            if status != 200:
                errors += 1
                raise ScaleError(f"{query.name}: HTTP {status}: {decoded}")
            validate_reply(query, decoded, total)
            last = decoded
            if ordinal >= args.warmup:
                timings.append(elapsed_ms)
        results[query.name] = {
            "latency": latency(timings),
            "semantic_digest": campaign.semantic_digest(last),
        }
    return results, errors


def read_metrics(port: int) -> dict[str, float] | None:
    try:
        reply = campaign.one_request("127.0.0.1", port, "GET", "/_metrics", timeout=10)
        if reply.status == 200:
            return campaign.parse_prometheus(reply.data.decode("utf-8", "replace"))
    except Exception:
        pass
    return None


def run_cell(
    args: argparse.Namespace,
    root: Path,
    index: Path,
    names: list[str],
    count: int,
    generation: int,
) -> dict[str, Any]:
    atomic_bytes(index / "segments.tsv", manifest_bytes(names[:count], generation))
    log = root / "logs" / f"segments-{count}.log"
    started_wall = time.time()
    started = time.perf_counter()
    process, _ = start_node(
        args.binary,
        index,
        args.port,
        log,
        ["--read-only", f"--threads={args.threads}"],
    )
    try:
        health = wait_health(args.port, process, args.start_timeout)
        startup = time.perf_counter() - started
        status, stats = request(args.port, "GET", "/_stats", timeout=args.timeout)
        if status != 200 or not isinstance(stats, dict):
            raise ScaleError(f"stats на {count} сегментах: HTTP {status}: {stats}")
        expected_docs = count * args.template_documents
        if stats.get("segments") != count or stats.get("docs") != expected_docs:
            raise ScaleError(
                f"stats на {count}: segments={stats.get('segments')}, docs={stats.get('docs')}, "
                f"ожидались {count} и {expected_docs}"
            )
        before = campaign.process_sample(process.pid)
        maps = proc_maps(process.pid)
        metrics_before = read_metrics(args.port)
        measured, errors = measure_queries(args, count)
        after = campaign.process_sample(process.pid)
        metrics_after = read_metrics(args.port)
        return {
            "segments": count,
            "synthetic_physical_docs": expected_docs,
            "started_at": started_wall,
            "startup_seconds": startup,
            "health": health,
            "stats": stats,
            "process_before": before,
            "process_after": after,
            "proc_maps": maps,
            "metrics_before": metrics_before,
            "metrics_after": metrics_after,
            "queries": measured,
            "http_errors": errors,
            "valid": True,
        }
    finally:
        close_node(process)


def full_check(args: argparse.Namespace, root: Path, index: Path) -> dict[str, Any]:
    output = root / "check-full.log"
    began = time.perf_counter()
    completed = subprocess.run(
        [str(args.binary), "check", str(index), "--full", "--json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=args.check_timeout,
    )
    output.write_text(completed.stdout + completed.stderr, encoding="utf-8")
    return {
        "command": [str(args.binary), "check", str(index), "--full", "--json"],
        "returncode": completed.returncode,
        "elapsed_seconds": time.perf_counter() - began,
        "output": str(output),
        "tail": (completed.stdout + completed.stderr)[-4000:],
        "valid": completed.returncode == 0,
    }


def parser() -> argparse.ArgumentParser:
    out = argparse.ArgumentParser(description=__doc__)
    out.add_argument("--binary", type=Path, required=True)
    out.add_argument("--output-root", type=Path, required=True)
    out.add_argument("--count", type=int, action="append", default=[])
    out.add_argument("--template-documents", type=int, default=100)
    out.add_argument("--warmup", type=int, default=10)
    out.add_argument("--repetitions", type=int, default=100)
    out.add_argument("--threads", type=int, default=max(1, min(16, os.cpu_count() or 1)))
    out.add_argument("--port", type=int, default=19412)
    out.add_argument("--timeout", type=float, default=120)
    out.add_argument("--start-timeout", type=float, default=300)
    out.add_argument("--check-timeout", type=float, default=3600)
    out.add_argument("--skip-full-check", action="store_true")
    return out


def main() -> int:
    args = parser().parse_args()
    args.binary = args.binary.resolve()
    args.output_root = args.output_root.resolve()
    counts = args.count or [1, 10, 100, 1_000, 10_000]
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        raise ScaleError(f"бинарник не исполняемый файл: {args.binary}")
    if args.output_root.exists():
        raise ScaleError(f"каталог результата уже существует: {args.output_root}")
    if counts != sorted(set(counts)) or not counts or counts[0] < 1 or counts[-1] > 30_000:
        raise ScaleError("--count должны быть уникальными возрастающими числами от 1 до 30000")
    if args.template_documents < 100:
        raise ScaleError("в шаблоне нужно не менее 100 документов для всех проверок")
    if args.warmup < 0 or args.repetitions < 1:
        raise ScaleError("warmup неотрицателен, repetitions положителен")
    args.output_root.mkdir(parents=True)

    template = build_template(args, args.output_root)
    index, names = prepare_scale_index(args.output_root, template, counts[-1])
    manifest = {
        "format": 1,
        "kind": "segment-topology-scale",
        "created_at": campaign.now_iso(),
        "binary": str(args.binary),
        "binary_sha256": campaign.sha256_file(args.binary),
        "source": campaign.git_manifest(Path(__file__).resolve().parents[1]),
        "counts": counts,
        "template_documents": args.template_documents,
        "warning": (
            "Жёстко связанные копии одного сегмента измеряют топологический overhead; "
            "это не корпусный benchmark и не доказательство ёмкости данных."
        ),
        "arguments": vars(args) | {"binary": str(args.binary), "output_root": str(args.output_root)},
    }
    atomic_json(args.output_root / "manifest.json", manifest)

    results: list[dict[str, Any]] = []
    try:
        for generation, count in enumerate(counts, 1):
            print(f"== {count:,} сегментов", flush=True)
            result = run_cell(args, args.output_root, index, names, count, generation)
            results.append(result)
            atomic_json(args.output_root / "results.json", results)
            print(
                f"  startup={result['startup_seconds']:.3f} с, maps={result['proc_maps']}, "
                f"miss p50={result['queries']['search/miss']['latency']['p50_ms']:.3f} мс",
                flush=True,
            )
        check = None
        if not args.skip_full_check:
            check = full_check(args, args.output_root, index)
            if not check["valid"]:
                raise ScaleError(f"check --full не прошёл: {check['tail']}")
        summary = {
            "valid": all(result["valid"] for result in results) and (check is None or check["valid"]),
            "cells": len(results),
            "maximum_segments": counts[-1],
            "maximum_synthetic_physical_docs": counts[-1] * args.template_documents,
            "check_full": check,
            "results": str(args.output_root / "results.json"),
            "manifest": str(args.output_root / "manifest.json"),
        }
        atomic_json(args.output_root / "summary.json", summary)
        print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
        return 0 if summary["valid"] else 1
    except Exception as error:
        atomic_json(
            args.output_root / "failure.json",
            {"valid": False, "error": repr(error), "completed_cells": len(results)},
        )
        raise


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ScaleError, campaign.CampaignError) as error:
        print(f"ОШИБКА: {error}", file=sys.stderr)
        raise SystemExit(1)
