#!/usr/bin/env python3
"""Воспроизводимая кампания SvinDB: manifest, ingest, query и stress.

Скрипт намеренно использует только стандартную библиотеку Python. На стенде
нужны лишь Python 3, продуктовый бинарник и, для профилей, обычные Linux
интерфейсы ``/proc``. Каждый режим пишет самостоятельный JSON; потерянная или
неопределённая запись всегда делает прогон неуспешным, а не исчезает из средней.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import hashlib
import http.client
import json
import math
import os
import platform
import queue
import random
import re
import select
import shutil
import shlex
import signal
import socket
import sqlite3
import stat
import statistics
import subprocess
import sys
import threading
import time
import urllib.parse
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKLOAD = ROOT / "tools/campaign_workloads.json"
JSON_HEADERS = {"Content-Type": "application/json"}
NDJSON_HEADERS = {"Content-Type": "application/x-ndjson"}


class CampaignError(RuntimeError):
    """Нарушение измерительного контракта, при котором цифру принимать нельзя."""


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def append_json(path: Path, value: Any, *, sync: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n")
        stream.flush()
        if sync:
            os.fsync(stream.fileno())


def sha256_file(path: Path, block: int = 8 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(block):
            digest.update(chunk)
    return digest.hexdigest()


def run_text(command: list[str], *, cwd: Path | None = None) -> str:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return f"недоступно: {error}"
    text = (result.stdout or result.stderr).strip()
    return text if result.returncode == 0 else f"код {result.returncode}: {text[-2000:]}"


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    at = (len(ordered) - 1) * q
    lo = math.floor(at)
    hi = math.ceil(at)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - at) + ordered[hi] * (at - lo)


def latency_summary(values: list[float]) -> dict[str, Any]:
    summary = {
        "count": len(values),
        "min_ms": min(values) if values else None,
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "p99_9_ms": percentile(values, 0.999),
        "max_ms": max(values) if values else None,
        "mean_ms": statistics.fmean(values) if values else None,
    }
    if values:
        ordered = sorted(values)
        intervals = {}
        for name, q in (("p50_ms", 0.50), ("p95_ms", 0.95), ("p99_ms", 0.99), ("p99_9_ms", 0.999)):
            center = q * (len(ordered) - 1)
            radius = 1.96 * math.sqrt(max(0.0, len(ordered) * q * (1.0 - q)))
            lo = max(0, min(len(ordered) - 1, math.floor(center - radius)))
            hi = max(0, min(len(ordered) - 1, math.ceil(center + radius)))
            intervals[name] = [ordered[lo], ordered[hi]]
        if len(values) > 1:
            mean = summary["mean_ms"]
            half = 1.96 * statistics.stdev(values) / math.sqrt(len(values))
            intervals["mean_ms"] = [max(0.0, mean - half), mean + half]
        summary["confidence_95"] = {
            "method": "normal order-statistic interval for quantiles; normal mean interval",
            "intervals": intervals,
        }
    return summary


def parse_prometheus(raw: str) -> dict[str, float]:
    values: dict[str, float] = {}
    for line in raw.splitlines():
        if not line or line.startswith("#"):
            continue
        name, _, value = line.rpartition(" ")
        try:
            values[name] = float(value)
        except ValueError:
            continue
    return values


@dataclass
class Reply:
    status: int
    data: bytes
    elapsed_ms: float
    headers: dict[str, str]

    def json(self) -> Any:
        return json.loads(self.data)


_HTTP_CONNECTIONS = threading.local()


def _persistent_connection(host: str, port: int, timeout: float) -> http.client.HTTPConnection:
    """Возвращает keep-alive соединение текущего worker-потока.

    Один TCP handshake на каждый запрос искажает p99 именно в сторону, которую
    кампания пытается измерить. Соединение принадлежит потоку клиента, поэтому
    в нём нет общей блокировки и один worker не может случайно использовать
    сокет другого. При изменении endpoint/timeout соединение заменяется.
    """
    key = (host, port)
    held = getattr(_HTTP_CONNECTIONS, "held", None)
    if held is None or held[0] != key:
        if held is not None:
            with contextlib.suppress(Exception):
                held[1].close()
        connection = http.client.HTTPConnection(host, port, timeout=timeout)
        _HTTP_CONNECTIONS.held = (key, connection)
        return connection
    held[1].timeout = timeout
    return held[1]


def one_request(
    host: str,
    port: int,
    method: str,
    path: str,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 900,
) -> Reply:
    connection = _persistent_connection(host, port, timeout)
    began = time.perf_counter()
    try:
        connection.request(method, path, body, headers or {})
        response = connection.getresponse()
        data = response.read()
        elapsed = (time.perf_counter() - began) * 1000.0
        return Reply(response.status, data, elapsed, dict(response.getheaders()))
    except Exception:
        # После частичного ответа состояние HTTP/1.1 неизвестно. Следующий
        # запрос получает новый сокет, иначе ошибка одного запроса протечёт в
        # соседнюю latency-наблюдение.
        with contextlib.suppress(Exception):
            connection.close()
        held = getattr(_HTTP_CONNECTIONS, "held", None)
        if held is not None and held[1] is connection:
            _HTTP_CONNECTIONS.held = None
        raise


def close_persistent_connection() -> None:
    """Закрывает keep-alive сокет текущего потока при явном завершении worker."""
    held = getattr(_HTTP_CONNECTIONS, "held", None)
    if held is not None:
        with contextlib.suppress(Exception):
            held[1].close()
        _HTTP_CONNECTIONS.held = None


def json_request(
    host: str,
    port: int,
    method: str,
    path: str,
    value: Any | None = None,
    timeout: float = 900,
) -> Reply:
    body = None
    headers: dict[str, str] = {}
    if value is not None:
        body = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        headers = JSON_HEADERS
    return one_request(host, port, method, path, body, headers, timeout)


def process_sample(pid: int) -> dict[str, Any]:
    root = Path(f"/proc/{pid}")
    sample: dict[str, Any] = {"at": time.time(), "pid": pid}
    if not root.exists():
        sample["missing"] = True
        return sample
    for name in ("status", "smaps_rollup", "io"):
        path = root / name
        try:
            for line in path.read_text(errors="replace").splitlines():
                key, _, raw = line.partition(":")
                raw = raw.strip().split()[0] if raw.strip() else ""
                if raw.isdigit():
                    sample[f"{name}.{key}"] = int(raw) * (1024 if "kB" in line else 1)
        except OSError:
            pass
    try:
        stat = (root / "stat").read_text().split()
        ticks = os.sysconf("SC_CLK_TCK")
        sample["cpu_seconds"] = (int(stat[13]) + int(stat[14])) / ticks
        sample["minor_faults"] = int(stat[9])
        sample["major_faults"] = int(stat[11])
    except (OSError, ValueError, IndexError):
        pass
    with contextlib.suppress(OSError):
        sample["fds"] = len(list((root / "fd").iterdir()))
    return sample


def boundary_resource_contract(
    before: dict[str, Any],
    after_limits: dict[str, Any],
    after_malformed: dict[str, Any],
) -> dict[str, Any]:
    """Разделяет память полезной большой записи и утечку ошибочного пути.

    Boundary сначала намеренно принимает до 128 МиБ тел одновременно. Считать
    всю оставшуюся после этого память «утечкой malformed JSON» неверно: в ней
    находятся опубликованный сегмент, mmap-страницы и удержанные allocator-ом
    области успешной индексации. Поэтому transient peak ограничивается отдельно,
    а длинная серия негодных запросов сравнивается со снимком уже после flush.
    """
    peak_growth = max(
        0,
        after_limits.get("status.VmHWM", 0) - before.get("status.VmHWM", 0),
    )
    malformed_rss_growth = max(
        0,
        after_malformed.get("status.VmRSS", 0)
        - after_limits.get("status.VmRSS", 0),
    )
    malformed_fd_growth = max(
        0,
        after_malformed.get("fds", 0) - after_limits.get("fds", 0),
    )
    # 128 МиБ сырых тел получают не больше 16-кратного transient-конверта.
    # Само ограничение cgroup проверяется отдельным 1 GiB boundary-запуском:
    # там admission обязан сузить одновременность и не дать дойти до этого
    # абсолютного потолка.
    peak_limit = 2 << 30
    malformed_rss_limit = 64 << 20
    fd_limit = 16
    return {
        "peak_rss_growth_bytes": peak_growth,
        "peak_rss_growth_limit_bytes": peak_limit,
        "malformed_rss_growth_bytes": malformed_rss_growth,
        "malformed_rss_growth_limit_bytes": malformed_rss_limit,
        "malformed_fd_growth": malformed_fd_growth,
        "malformed_fd_growth_limit": fd_limit,
        "valid": (
            peak_growth <= peak_limit
            and malformed_rss_growth <= malformed_rss_limit
            and malformed_fd_growth <= fd_limit
        ),
    }


_SIZE_UNITS = {
    "": 1,
    "b": 1,
    "k": 1 << 10,
    "kb": 1 << 10,
    "kib": 1 << 10,
    "m": 1 << 20,
    "mb": 1 << 20,
    "mib": 1 << 20,
    "g": 1 << 30,
    "gb": 1 << 30,
    "gib": 1 << 30,
    "t": 1 << 40,
    "tb": 1 << 40,
    "tib": 1 << 40,
}


def parse_byte_limit(raw: str) -> int | None:
    """Разбирает объявленный лимит; ``unrestricted`` означает отсутствие клетки."""
    text = raw.strip().lower().replace("_", "").replace(" ", "")
    if text in {"", "max", "none", "unlimited", "unrestricted"}:
        return None
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([a-z]*)", text)
    if not match or match.group(2) not in _SIZE_UNITS:
        raise CampaignError(
            f"непонятный memory limit {raw!r}; нужен размер вроде 1GiB либо unrestricted"
        )
    value = float(match.group(1)) * _SIZE_UNITS[match.group(2)]
    if not math.isfinite(value) or value < 1:
        raise CampaignError(f"memory limit должен быть положительным, получено {raw!r}")
    return int(value)


def parse_cpu_limit(raw: str) -> float | None:
    """Число логических CPU в quota; не путать с concurrency нагрузки."""
    text = raw.strip().lower()
    if text in {"", "max", "none", "unlimited", "unrestricted"}:
        return None
    try:
        value = float(text)
    except ValueError as error:
        raise CampaignError(f"непонятный CPU limit {raw!r}; нужно число ядер") from error
    if not math.isfinite(value) or value <= 0:
        raise CampaignError(f"CPU limit должен быть положительным, получено {raw!r}")
    return value


def process_cgroup(pid: int) -> Path | None:
    """Возвращает cgroup v2 процесса; v1 намеренно не изображается как v2."""
    try:
        lines = Path(f"/proc/{pid}/cgroup").read_text().splitlines()
    except OSError:
        return None
    for line in lines:
        hierarchy, controllers, relative = line.split(":", 2)
        if hierarchy == "0" and controllers == "":
            return Path("/sys/fs/cgroup") / relative.lstrip("/")
    return None


def _cgroup_ancestors(path: Path) -> Iterator[Path]:
    root = Path("/sys/fs/cgroup")
    current = path
    while current == root or root in current.parents:
        yield current
        if current == root:
            break
        current = current.parent


def _effective_memory_max(path: Path) -> int | None:
    limits = []
    for current in _cgroup_ancestors(path):
        try:
            raw = (current / "memory.max").read_text().strip()
        except OSError:
            continue
        if raw != "max":
            with contextlib.suppress(ValueError):
                limits.append(int(raw))
    return min(limits) if limits else None


def _effective_cpu_cores(path: Path) -> float | None:
    limits = []
    for current in _cgroup_ancestors(path):
        try:
            quota, period = (current / "cpu.max").read_text().split()
        except (OSError, ValueError):
            continue
        if quota == "max":
            continue
        with contextlib.suppress(ValueError, ZeroDivisionError):
            limits.append(int(quota) / int(period))
    return min(limits) if limits else None


def _key_value_file(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return values
    for line in lines:
        parts = line.split()
        if len(parts) == 2:
            with contextlib.suppress(ValueError):
                values[parts[0]] = int(parts[1])
    return values


def cgroup_sample(pid: int) -> dict[str, Any]:
    path = process_cgroup(pid)
    if path is None:
        return {"available": False}
    sample: dict[str, Any] = {
        "available": True,
        "path": str(path),
        "memory_max_effective": _effective_memory_max(path),
        "cpu_cores_effective": _effective_cpu_cores(path),
    }
    for name in ("memory.current", "memory.peak", "memory.swap.current", "memory.swap.peak"):
        with contextlib.suppress(OSError, ValueError):
            sample[name.replace(".", "_")] = int((path / name).read_text())
    sample["memory_events"] = _key_value_file(path / "memory.events")
    sample["memory_events_local"] = _key_value_file(path / "memory.events.local")
    return sample


def validate_resource_contract(args: argparse.Namespace) -> dict[str, Any]:
    """Не даёт manifest назвать лимитом строку, которая ничего не ограничила."""
    memory_requested = parse_byte_limit(getattr(args, "memory_limit", "unrestricted"))
    cpu_requested = parse_cpu_limit(getattr(args, "cpu_limit", "unrestricted"))
    pid = getattr(args, "pid", None)
    contract: dict[str, Any] = {
        "memory_requested": memory_requested,
        "cpu_cores_requested": cpu_requested,
        "enforced": memory_requested is None and cpu_requested is None,
    }
    if memory_requested is None and cpu_requested is None:
        if pid and sys.platform.startswith("linux"):
            contract["observed"] = cgroup_sample(pid)
        return contract
    if not sys.platform.startswith("linux"):
        raise CampaignError("resource-limited клетка требует Linux cgroup v2")
    if not pid:
        raise CampaignError("resource-limited клетка требует --pid ограниченного процесса")
    observed = cgroup_sample(pid)
    if not observed.get("available"):
        raise CampaignError(f"PID {pid} не находится в доступном cgroup v2")
    contract["observed"] = observed
    if memory_requested is not None:
        actual = observed.get("memory_max_effective")
        tolerance = 4096
        if actual is None:
            raise CampaignError(
                f"объявлен memory limit {memory_requested} Б, но memory.max процесса равен max"
            )
        if abs(actual - memory_requested) > tolerance:
            raise CampaignError(
                f"объявлен memory limit {memory_requested} Б, фактически действует {actual} Б"
            )
    if cpu_requested is not None:
        actual = observed.get("cpu_cores_effective")
        if actual is None or abs(actual - cpu_requested) > 0.01:
            raise CampaignError(
                f"объявлен CPU limit {cpu_requested:g}, фактически действует {actual}"
            )
        with contextlib.suppress(OSError):
            affinity = len(os.sched_getaffinity(pid))
            contract["affinity_cpus"] = affinity
            if affinity + 1e-9 < cpu_requested:
                raise CampaignError(
                    f"CPU quota просит {cpu_requested:g}, но affinity процесса содержит {affinity} CPU"
                )
    contract["enforced"] = True
    return contract


def host_sample(*, include_processes: bool) -> dict[str, Any]:
    """Снимает шум машины, swap/throttle и счётчики блочных устройств."""
    sample: dict[str, Any] = {}
    with contextlib.suppress(OSError):
        load = Path("/proc/loadavg").read_text().split()
        sample["load_average"] = [float(value) for value in load[:3]]
        sample["runnable"] = load[3]
    with contextlib.suppress(OSError):
        first = Path("/proc/stat").read_text().splitlines()[0].split()
        names = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")
        sample["cpu_ticks"] = {
            name: int(value) for name, value in zip(names, first[1:])
        }
    with contextlib.suppress(OSError):
        wanted = {"MemAvailable", "SwapTotal", "SwapFree", "Dirty", "Writeback"}
        memory: dict[str, int] = {}
        for line in Path("/proc/meminfo").read_text().splitlines():
            key, _, raw = line.partition(":")
            if key in wanted:
                memory[key] = int(raw.strip().split()[0]) * 1024
        sample["memory"] = memory
    with contextlib.suppress(OSError):
        wanted = {"pswpin", "pswpout", "oom_kill"}
        vm: dict[str, int] = {}
        for line in Path("/proc/vmstat").read_text().splitlines():
            key, raw = line.split()
            if key in wanted:
                vm[key] = int(raw)
        sample["vm"] = vm
    with contextlib.suppress(OSError):
        disks = {}
        for line in Path("/proc/diskstats").read_text().splitlines():
            fields = line.split()
            if len(fields) < 14 or fields[2].startswith(("loop", "ram")):
                continue
            disks[fields[2]] = {
                "reads": int(fields[3]),
                "sectors_read": int(fields[5]),
                "writes": int(fields[7]),
                "sectors_written": int(fields[9]),
                "io_ms": int(fields[12]),
            }
        sample["diskstats"] = disks
    with contextlib.suppress(OSError, ValueError):
        sample["cpu0_frequency_khz"] = int(
            Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq").read_text()
        )
    temperatures = []
    for path in Path("/sys/class/thermal").glob("thermal_zone*/temp"):
        with contextlib.suppress(OSError, ValueError):
            temperatures.append(int(path.read_text()))
    if temperatures:
        sample["thermal_millicelsius"] = temperatures
    if include_processes:
        sample["top_processes"] = run_text(
            ["ps", "-eo", "pid=,pcpu=,comm=,args=", "--sort=-pcpu"]
        ).splitlines()[:12]
    return sample


class Sampler:
    def __init__(self, args: argparse.Namespace, output: Path):
        self.host = args.host
        self.port = args.port
        self.pid = args.pid
        self.interval = args.sample_interval
        self.output = output
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        if not self.pid:
            return
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        ordinal = 0
        while not self.stop_event.is_set():
            sample = process_sample(self.pid)
            if sys.platform.startswith("linux"):
                sample["cgroup"] = cgroup_sample(self.pid)
            sample["host"] = host_sample(include_processes=ordinal % 20 == 0)
            try:
                response = one_request(self.host, self.port, "GET", "/_metrics", timeout=10)
                if response.status == 200:
                    sample["metrics"] = parse_prometheus(response.data.decode("utf-8", "replace"))
            except Exception as error:  # сам sampler не вправе оборвать нагрузку
                sample["metrics_error"] = repr(error)
            append_json(self.output, sample)
            ordinal += 1
            self.stop_event.wait(self.interval)

    def close(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=self.interval + 5)

    def __enter__(self) -> "Sampler":
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def git_manifest(root: Path) -> dict[str, Any]:
    head = run_text(["git", "rev-parse", "HEAD"], cwd=root)
    status = run_text(["git", "status", "--porcelain=v1"], cwd=root)
    return {
        "head": head,
        "clean": status == "",
        "status": status.splitlines(),
        "diff_sha256": hashlib.sha256(
            run_text(["git", "diff", "--binary"], cwd=root).encode()
        ).hexdigest(),
    }


def source_manifest(root: Path) -> dict[str, Any]:
    """Хеширует исходники, не захватывая сборки, индексы и служебный Git."""
    accepted_names = {"Cargo.lock", "Cargo.toml", "Makefile", "Dockerfile"}
    accepted_suffixes = {
        ".rs", ".py", ".sh", ".toml", ".json", ".md", ".yml", ".yaml"
    }
    files = []
    # `Path.rglob` всё равно заходит в target, даже если потом отфильтровать
    # результат. На campaign-host это означало обход 4,7 ГиБ артефактов после
    # каждого прогона. `os.walk` позволяет отсечь деревья до спуска в них.
    for current, directories, names in os.walk(root):
        directories[:] = sorted(
            name
            for name in directories
            if name not in (".git", ".claude", ".codex", "__pycache__")
            and not name.startswith("target")
        )
        for name in sorted(names):
            path = Path(current) / name
            if name in accepted_names or path.suffix in accepted_suffixes:
                files.append(path)
    files.sort()
    checksums: dict[str, str] = {}
    digest = hashlib.sha256()
    total_bytes = 0
    for path in files:
        relative = path.relative_to(root).as_posix()
        checksum = sha256_file(path)
        checksums[relative] = checksum
        digest.update(relative.encode() + b"\0" + checksum.encode() + b"\n")
        total_bytes += path.stat().st_size
    return {
        "files": len(files),
        "bytes": total_bytes,
        "tree_sha256": digest.hexdigest(),
        "sha256": checksums,
    }


def optional_file_manifest(raw: str | None) -> dict[str, Any] | None:
    """Возвращает provenance небольшого входа без неявного хеширования корпуса."""
    if not raw:
        return None
    path = Path(raw).resolve()
    if not path.is_file():
        return {"path": str(path), "missing": True}
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


_SVINDB_AUXILIARY_TOP_LEVEL = frozenset({".pins", ".trash", ".creating"})


def storage_accounting(root: Path) -> dict[str, Any]:
    """Считает физический состав индекса, не переходя по symlink.

    ``root_bytes`` — все обычные файлы непосредственно в каталоге индекса и
    его подкаталогах. ``wal_bytes`` — файлы под единственным top-level
    каталогом ``wal/``; ``aux_bytes`` — служебные top-level каталоги
    ``.pins/``, ``.trash/`` и ``.creating/``. Остаток — ``active_index_bytes``
    (сегменты и метаданные индекса). Symlink намеренно не является физическим
    содержимым данного каталога и не разрешается при обходе.
    """
    root = Path(root)
    totals = {"root_bytes": 0, "wal_bytes": 0, "active_index_bytes": 0, "aux_bytes": 0}
    try:
        root_metadata = root.lstat()
    except OSError:
        root_metadata = None
    if root_metadata is None or not stat.S_ISDIR(root_metadata.st_mode):
        raise CampaignError(f"каталог индекса не существует или не является каталогом: {root}")
    def failed_walk(error: OSError) -> None:
        raise CampaignError(f"неполный обход storage: {error}") from error

    for current, directories, names in os.walk(root, followlinks=False, onerror=failed_walk):
        directories[:] = [name for name in directories if not (Path(current) / name).is_symlink()]
        relative = Path(current).relative_to(root)
        top_level = relative.parts[0] if relative.parts else ""
        bucket = "wal_bytes" if top_level == "wal" else (
            "aux_bytes" if top_level in _SVINDB_AUXILIARY_TOP_LEVEL else "active_index_bytes"
        )
        for name in names:
            path = Path(current) / name
            try:
                metadata = path.lstat()
            except OSError as error:
                raise CampaignError(f"storage изменился или недоступен: {path}: {error}") from error
            if not stat.S_ISREG(metadata.st_mode):
                continue
            size = metadata.st_size
            totals["root_bytes"] += size
            totals[bucket] += size
    return {
        **totals,
        "definition": "root_bytes=все обычные файлы; wal_bytes=top-level wal/; aux_bytes=top-level .pins/.trash/.creating; active_index_bytes=остаток; symlink исключены",
    }


def automatic_run_manifest(
    args: argparse.Namespace,
    *,
    started_at: str,
    started_wall: float,
    status: str,
    error: str | None,
) -> None:
    """Пишет обязательный sidecar каждого нагрузочного прогона.

    Большой корпус повторно не читается: его полное доказательство лежит в
    результате ``validate-corpus``, а здесь фиксируются путь и хеш metadata.
    Исходники, workload, schema/settings и фактически запущенный ELF при этом
    хешируются для каждого прогона.
    """
    if args.command not in {"load", "query", "stress", "boundary", "prepare-es"}:
        return
    output = Path(args.output).resolve()
    manifest_path = output.with_name(output.name + ".manifest.json")
    source = ROOT
    binary: dict[str, Any] | None = None
    pid = getattr(args, "pid", None)
    if pid:
        try:
            executable_link = Path(f"/proc/{pid}/exe")
            if executable_link.exists():
                executable = executable_link.resolve()
                try:
                    binary = {
                        "path": str(executable),
                        "bytes": executable.stat().st_size,
                        "sha256": sha256_file(executable),
                        "version": run_text([str(executable), "--version"]),
                    }
                except OSError as executable_error:
                    binary = {
                        "path": str(executable),
                        "unavailable": repr(executable_error),
                        "declared_path": os.environ.get("SVINDB_CAMPAIGN_BINARY"),
                    }
        except OSError as executable_error:
            # У процесса ES /proc/<pid>/exe может быть закрыт правами
            # контейнера. Это ограничивает только host-side binary
            # attribution; версия ES зафиксирована digest в campaign manifest.
            binary = {
                "path": f"/proc/{pid}/exe",
                "unavailable": repr(executable_error),
                "declared_path": os.environ.get("SVINDB_CAMPAIGN_BINARY"),
            }
    process_identity: dict[str, Any] | None = None
    if pid and Path(f"/proc/{pid}").exists():
        process_root = Path(f"/proc/{pid}")
        process_identity = {"pid": pid}
        with contextlib.suppress(OSError):
            process_identity["cmdline"] = (
                process_root.joinpath("cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace").strip()
            )
        with contextlib.suppress(OSError):
            process_identity["cwd"] = str(process_root.joinpath("cwd").resolve())
        with contextlib.suppress(OSError):
            process_identity["cgroup"] = process_root.joinpath("cgroup").read_text().splitlines()
    corpus_metadata = None
    corpus_path = getattr(args, "corpus_path", None)
    if corpus_path:
        raw_corpus = Path(corpus_path).resolve()
        corpus_metadata = optional_file_manifest(
            str(raw_corpus / "corpus.json") if raw_corpus.is_dir() else str(raw_corpus)
        )
    index_files: dict[str, Any] = {}
    index_path = getattr(args, "index_path", None)
    if index_path:
        root = Path(index_path).resolve()
        for name in ("schema.json", "settings.json"):
            value = optional_file_manifest(str(root / name))
            if value:
                index_files[name] = value
    workload = optional_file_manifest(getattr(args, "workload_file", None))
    result = optional_file_manifest(str(output))
    endpoints: dict[str, Any] = {}
    host = getattr(args, "host", None)
    port = getattr(args, "port", None)
    engine = getattr(args, "engine", None)
    if host and port:
        paths = (
            ("root", "/"),
            ("index_settings", f"/{args.index}/_settings"),
            ("index_mapping", f"/{args.index}/_mapping"),
        ) if engine == "es" or args.command == "prepare-es" else (
            ("health", "/_health"),
            ("settings", "/_settings"),
        )
        for label, path in paths:
            try:
                reply = one_request(host, port, "GET", path, timeout=10)
                endpoints[label] = {
                    "path": path,
                    "status": reply.status,
                    "sha256": hashlib.sha256(reply.data).hexdigest(),
                    "body": reply.json() if len(reply.data) <= 1 << 20 else None,
                }
            except Exception as endpoint_error:
                endpoints[label] = {"path": path, "error": repr(endpoint_error)}
    arguments = {
        key: value
        for key, value in vars(args).items()
        if key != "func" and isinstance(value, (str, int, float, bool, list, type(None)))
    }
    value = {
        "format": 1,
        "command": args.command,
        "status": status,
        "error": error,
        "started_at": started_at,
        "finished_at": now_iso(),
        "elapsed_seconds": time.time() - started_wall,
        "arguments": arguments,
        "declared_provenance": {
            "revision": os.environ.get("SVINDB_CAMPAIGN_REVISION"),
            "source_state": os.environ.get("SVINDB_CAMPAIGN_SOURCE_STATE"),
            "feature_flags": os.environ.get("SVINDB_CAMPAIGN_FEATURES"),
            "corpus_tree_sha256": os.environ.get("SVINDB_CAMPAIGN_CORPUS_SHA256"),
        },
        "git": git_manifest(source),
        "source_tree": source_manifest(source),
        "binary": binary,
        "process": process_identity,
        "resource_contract": getattr(args, "resource_contract", None),
        "engine_endpoints": endpoints,
        "inputs": {
            "corpus_metadata": corpus_metadata,
            "workload": workload,
            "index": index_files,
        },
        "result": result,
        "host": {
            "hostname": platform.node(),
            "platform": platform.platform(),
            "uname": run_text(["uname", "-a"]),
            "cpu": run_text(["lscpu"]),
            "memory_at_finish": run_text(["free", "-b"]),
            "block": run_text(["lsblk", "-b", "-o", "NAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL"]),
            "mount": run_text(["findmnt", "-T", str(output.parent)]),
            "compiler": run_text(["rustc", "-vV"]),
        },
    }
    atomic_json(manifest_path, value)


def command_manifest(args: argparse.Namespace) -> None:
    output = Path(args.output)
    inputs: dict[str, Any] = {}
    for raw in args.input:
        path = Path(raw).resolve()
        if path.is_file():
            inputs[str(path)] = {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
        elif path.is_dir():
            files = sorted(p for p in path.rglob("*") if p.is_file())
            digest = hashlib.sha256()
            size = 0
            for file in files:
                relative = str(file.relative_to(path)).encode()
                checksum = sha256_file(file)
                digest.update(relative + b"\0" + checksum.encode() + b"\n")
                size += file.stat().st_size
            inputs[str(path)] = {
                "files": len(files),
                "bytes": size,
                "tree_sha256": digest.hexdigest(),
            }
        else:
            inputs[str(path)] = {"missing": True}
    manifest = {
        "format": 1,
        "created_at": now_iso(),
        "run_id": args.run_id,
        "candidate": args.candidate,
        "declared_revision": args.revision,
        "source_state": args.source_state,
        "feature_flags": args.feature_flags,
        "corpus": args.corpus,
        "workload": args.workload,
        "seed": args.seed,
        "duration_seconds": args.duration,
        "concurrency": args.concurrency,
        "durability": args.durability,
        "memory_limit": args.memory_limit,
        "cpu_limit": args.cpu_limit,
        "resource_contract": getattr(args, "resource_contract", None),
        "git": git_manifest(Path(args.source).resolve()),
        "source_tree": source_manifest(Path(args.source).resolve()),
        "inputs": inputs,
        "binary": None,
        "host": {
            "hostname": platform.node(),
            "platform": platform.platform(),
            "python": sys.version,
            "uname": run_text(["uname", "-a"]),
            "cpu": run_text(["lscpu"]),
            "memory": run_text(["free", "-b"]),
            "block": run_text(["lsblk", "-b", "-o", "NAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL"]),
            "mount": run_text(["findmnt", "-T", str(output.parent)]),
            "compiler": run_text(["rustc", "-vV"]),
        },
    }
    if args.binary:
        binary = Path(args.binary).resolve()
        manifest["binary"] = {
            "path": str(binary),
            "bytes": binary.stat().st_size,
            "sha256": sha256_file(binary),
            "version": run_text([str(binary), "--version"]),
        }
    atomic_json(output, manifest)
    print(json.dumps({"manifest": str(output), "inputs": len(inputs)}, ensure_ascii=False))


def corpus_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    files = sorted(path.glob("part-*.ndjson"))
    if not files:
        raise CampaignError(f"в {path} нет part-*.ndjson")
    return files


def command_validate_corpus(args: argparse.Namespace) -> None:
    root = Path(args.corpus_path).resolve()
    files = corpus_files(root)
    began = time.perf_counter()
    total = 0
    raw_bytes = 0
    body_bytes = 0
    field_counts: Counter[str] = Counter()
    classes = [0] * 5
    seen_bodies: set[bytes] | None = set() if args.hash_body_uniqueness else None
    digest = hashlib.sha256()
    prefix = "flat" if args.kind == "flat" else "mixed"
    text_field = "message" if args.kind == "flat" else "text"
    for path in files:
        file_digest = hashlib.sha256()
        with path.open("rb", buffering=8 << 20) as stream:
            for line in stream:
                raw_bytes += len(line)
                file_digest.update(line)
                try:
                    doc = json.loads(line)
                except Exception as error:
                    raise CampaignError(f"{path}:{total + 1}: негодный JSON: {error}") from error
                expected = (
                    args.id_template.format(i=total)
                    if args.id_template
                    else f"{prefix}-{total:010}"
                )
                if doc.get("id") != expected:
                    raise CampaignError(
                        f"нарушена уникальная последовательность id: {doc.get('id')!r}, ожидался {expected!r}"
                    )
                body = doc.get(text_field)
                if not isinstance(body, str):
                    raise CampaignError(f"{expected}: нет строкового {text_field}")
                if not args.skip_marker_proof and total and f"unique_{total:x}" not in body:
                    raise CampaignError(f"{expected}: тело не содержит уникальный маркер")
                if seen_bodies is not None:
                    body_hash = hashlib.blake2b(body.encode(), digest_size=16).digest()
                    if body_hash in seen_bodies:
                        raise CampaignError(f"{expected}: повтор тела (BLAKE2b-128 {body_hash.hex()})")
                    seen_bodies.add(body_hash)
                n = len(body.encode())
                body_bytes += n
                if args.kind == "mixed":
                    classes[0 if n <= 160 else 1 if n <= 2 << 10 else 2 if n <= 16 << 10 else 3 if n <= 64 << 10 else 4] += 1
                field_counts.update(doc.keys())
                total += 1
                if args.documents and total >= args.documents:
                    break
        digest.update(path.name.encode() + b"\0" + file_digest.hexdigest().encode() + b"\n")
        if args.documents and total >= args.documents:
            break
    result = {
        "format": 1,
        "kind": args.kind,
        "documents": total,
        "raw_bytes": raw_bytes,
        "body_bytes": body_bytes,
        "tree_sha256": digest.hexdigest(),
        "fields": dict(field_counts),
        "classes": classes,
        "seconds": time.perf_counter() - began,
        "valid": not args.documents or total == args.documents,
        "uniqueness_proof": (
            "последовательный id и полный набор BLAKE2b-128 тел"
            if seen_bodies is not None
            else "последовательный id и уникальный маркер внутри каждого непустого тела"
        ),
    }
    if args.documents and total != args.documents:
        raise CampaignError(f"прочитано {total}, ожидалось {args.documents}")
    atomic_json(Path(args.output), result)
    print(json.dumps(result, ensure_ascii=False))


def nonce_marker(value: int, width: int = 5) -> str:
    # Только символы, которые и SvinDB, и standard analyzer ES считают
    # разделителями. Маркер делает тело побайтово уникальным, не добавляя ни
    # одного токена и не меняя Zipf/Heaps-распределение исходного корпуса.
    alphabet = "!#$%&()*+,-./:;<=>?@[]^{|}~"
    digits = []
    for _ in range(width):
        value, digit = divmod(value, len(alphabet))
        digits.append(alphabet[digit])
    if value:
        raise CampaignError("ширины nonce недостаточно для номера документа")
    return "".join(reversed(digits))


def command_normalize_flat(args: argparse.Namespace) -> None:
    """Делает первые N документов старого Flat-корпуса побайтово различными."""
    source = Path(args.source).resolve()
    destination = Path(args.destination).resolve()
    if destination.exists():
        raise CampaignError(f"{destination} уже существует; преобразователь не перезаписывает данные")
    destination.mkdir(parents=True)
    files = corpus_files(source)
    began = time.perf_counter()
    total_bytes = 0
    written = 0
    current_part = -1
    output_stream = None
    digest = hashlib.sha256()
    try:
        for line in iter_lines(files, args.documents):
            part = min(args.parts - 1, written * args.parts // args.documents)
            if part != current_part:
                if output_stream is not None:
                    output_stream.close()
                current_part = part
                output_stream = (destination / f"part-{part:03}.ndjson").open(
                    "wb", buffering=8 << 20
                )
            doc = json.loads(line)
            expected = args.id_template.format(i=written)
            if doc.get("id") != expected:
                raise CampaignError(f"исходный id {doc.get('id')!r}, ожидался {expected!r}")
            message = doc.get("message")
            if not isinstance(message, str):
                raise CampaignError(f"{expected}: нет строкового message")
            doc["message"] = f"{message} {nonce_marker(written)}"
            encoded = json.dumps(
                doc, ensure_ascii=False, separators=(",", ":")
            ).encode() + b"\n"
            output_stream.write(encoded)
            digest.update(encoded)
            total_bytes += len(encoded)
            written += 1
    finally:
        if output_stream is not None:
            output_stream.close()
    if written != args.documents:
        raise CampaignError(f"преобразовано {written}, ожидалось {args.documents}")
    metadata = {
        "format": 1,
        "source": str(source),
        "documents": written,
        "parts": args.parts,
        "bytes": total_bytes,
        "content_sha256": digest.hexdigest(),
        "id_template": args.id_template,
        "uniqueness": (
            "пятисимвольный base27 marker только из разделителей; "
            "побайтовая уникальность без изменения токенов"
        ),
        "seconds": time.perf_counter() - began,
    }
    atomic_json(destination / "corpus.json", metadata)
    atomic_json(Path(args.output), metadata)
    print(json.dumps(metadata, ensure_ascii=False))


def iter_lines(files: list[Path], limit: int) -> Iterator[bytes]:
    seen = 0
    for path in files:
        with path.open("rb", buffering=8 << 20) as stream:
            for line in stream:
                line = line.rstrip(b"\n")
                if not line:
                    continue
                yield line
                seen += 1
                if seen >= limit:
                    return


def batch_lines(lines: Iterable[bytes], max_docs: int, max_bytes: int) -> Iterator[list[bytes]]:
    batch: list[bytes] = []
    size = 16
    for line in lines:
        addition = len(line) + 1
        if batch and (len(batch) >= max_docs or size + addition > max_bytes):
            yield batch
            batch = []
            size = 16
        if addition + 16 > max_bytes:
            raise CampaignError(
                f"один документ занимает {addition} байт и не помещается в лимит пачки {max_bytes}"
            )
        batch.append(line)
        size += addition
    if batch:
        yield batch


ID_RE = re.compile(br'^\s*\{\s*"id"\s*:\s*"([^"\\]+)"')


def es_batch(lines: list[bytes]) -> bytes:
    chunks: list[bytes] = []
    for line in lines:
        # ES включает strict duplicate detection JSON-полей. Старые mixed
        # fixtures могли повторить hash-ключ внутри sparse_vector, тогда как
        # SvinDB и corpus validator применяли обычную семантику last-value-wins.
        # Канонизация делает логическую запись валидной для обеих сторон и не
        # позволяет незаметно потерять документ из ES checkpoint.
        doc = json.loads(line)
        identifier = str(doc["id"]).encode()
        canonical = json.dumps(
            doc, ensure_ascii=False, separators=(",", ":")
        ).encode()
        chunks.extend((b'{"index":{"_id":"' + identifier + b'"}}\n', canonical + b"\n"))
    return b"".join(chunks)


@dataclass
class LoadTally:
    expected: int
    acked: int = 0
    bytes: int = 0
    tokens: int = 0
    batches: int = 0
    retries_429: int = 0
    statuses: Counter[int] = field(default_factory=Counter)
    errors: list[dict[str, Any]] = field(default_factory=list)
    latencies: list[float] = field(default_factory=list)
    first_ack: float | None = None
    middle_ack: float | None = None
    last_ack: float | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)

    def accepted(self, docs: int, body_bytes: int, latency: float, began: float) -> None:
        with self.lock:
            self.acked += docs
            self.bytes += body_bytes
            self.batches += 1
            self.latencies.append(latency)
            elapsed = time.perf_counter() - began
            if self.first_ack is None:
                self.first_ack = elapsed
            if self.middle_ack is None and self.acked >= self.expected // 2:
                self.middle_ack = elapsed
            if self.acked >= self.expected:
                self.last_ack = elapsed

    def failure(self, kind: str, detail: str, docs: int) -> None:
        with self.lock:
            self.errors.append({"kind": kind, "detail": detail[:1000], "documents": docs})


def worker_load(
    args: argparse.Namespace,
    jobs: queue.Queue[list[bytes] | None],
    tally: LoadTally,
    began: float,
) -> None:
    path = f"/{args.index}/_bulk" if args.engine == "es" else "/_bulk"
    protocol = args.protocol
    if protocol == "auto":
        protocol = "ndjson" if args.engine == "es" else "native"
    headers = NDJSON_HEADERS if protocol == "ndjson" else JSON_HEADERS
    while True:
        lines = jobs.get()
        try:
            if lines is None:
                close_persistent_connection()
                return
            body = (
                es_batch(lines)
                if protocol == "ndjson"
                else b'{"documents":[' + b",".join(lines) + b"]}"
            )
            attempts = 0
            while True:
                attempts += 1
                try:
                    reply = one_request(args.host, args.port, "POST", path, body, headers, args.timeout)
                except Exception as error:
                    # После transport error сервер мог уже сделать durable ACK.
                    # Повтор native /_bulk создал бы дубль; такой прогон обязан
                    # остановиться и пройти oracle, а не выдавать красивую цифру.
                    tally.failure("transport_ambiguous", repr(error), len(lines))
                    break
                with tally.lock:
                    tally.statuses[reply.status] += 1
                if reply.status == 429 and attempts <= args.retry_429:
                    with tally.lock:
                        tally.retries_429 += 1
                    time.sleep(min(2.0, 0.05 * 2 ** (attempts - 1)))
                    continue
                if 200 <= reply.status < 300:
                    try:
                        decoded = reply.json()
                        if args.engine == "es" and decoded.get("errors"):
                            raise CampaignError("Elasticsearch bulk содержит item errors")
                        if args.engine == "svindb" and decoded.get("added") != len(lines):
                            raise CampaignError(
                                f"SvinDB подтвердил added={decoded.get('added')} вместо {len(lines)}"
                            )
                    except Exception as error:
                        tally.failure("bad_ack", f"{error}: {reply.data[:500]!r}", len(lines))
                        break
                    tally.accepted(len(lines), len(body), reply.elapsed_ms, began)
                    break
                tally.failure("http", f"HTTP {reply.status}: {reply.data[:500]!r}", len(lines))
                break
        finally:
            jobs.task_done()


def get_json(args: argparse.Namespace, path: str) -> dict[str, Any]:
    reply = one_request(args.host, args.port, "GET", path, timeout=args.timeout)
    if reply.status >= 300:
        raise CampaignError(f"GET {path}: HTTP {reply.status}: {reply.data[:500]!r}")
    return reply.json()


def wait_ready_and_settled(args: argparse.Namespace, expected: int, began: float) -> dict[str, Any]:
    if args.engine == "es":
        flush = one_request(args.host, args.port, "POST", f"/{args.index}/_refresh", b"", timeout=args.timeout)
        if flush.status >= 300:
            raise CampaignError(f"refresh ES: HTTP {flush.status}: {flush.data[:500]!r}")
    else:
        flush = json_request(args.host, args.port, "POST", "/_flush", {}, args.timeout)
        if flush.status not in (200, 202):
            raise CampaignError(f"flush SvinDB: HTTP {flush.status}: {flush.data[:500]!r}")
    deadline = time.monotonic() + args.settle_timeout
    ready_at = None
    stable_since = None
    stable_signature = None
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        if args.engine == "es":
            count = get_json(args, f"/{args.index}/_count").get("count", -1)
            stats = get_json(args, f"/{args.index}/_stats?filter_path=_all.total.merges.current,_all.total.segments.count")
            total = stats.get("_all", {}).get("total", {})
            merges = total.get("merges", {}).get("current", 0)
            segments = total.get("segments", {}).get("count", -1)
            last = {"docs": count, "merges": merges, "segments": segments}
            is_ready = count == expected
            is_quiescent = is_ready and merges == 0
            settle_complete = merges == 0
            signature = (segments, merges)
        else:
            last = get_json(args, "/_health")
            count = last.get("docs", -1)
            compaction = last.get("compaction", {})
            is_ready = count == expected and last.get("buffered") == 0
            pending = compaction.get("pending_segments", 0)
            running = compaction.get("running_segments", 0)
            # `pending` учитывается в результате, но не заставляет runner
            # ждать два часа неизменного долга. Это важное различие:
            # физически тихий состав может быть не fully settled из-за
            # рассогласования backlog/choose или budget. Такое измерение
            # завершается, но получает complete=false и красный gate.
            is_quiescent = is_ready and not last.get("flush_in_progress") and not running
            # Тишина планировщика — необходимое условие settled, но не
            # достаточное, и это доказано разбором кода, а не догадкой.
            #
            # `backlog` считает превышение над `allowed_seg_count`, а тот даёт
            # каждому **замороженному** сегменту +1 к допустимому составу.
            # Двенадцать сегментов по 2.5 ГиБ, каждый наполовину мёртвый, дают
            # ровно `pending == 0` — и прежний предикат называл это settled.
            # Именно такое состояние объясняет измеренную потерю 31.7% QPS на
            # частом слове, пережившую «полное» сведение: индекс тих, но
            # половина постингов в нём мертва, и отсечение по потолкам блоков
            # разбирает мёртвое наравне с живым.
            #
            # Поэтому к тишине добавляется доля мёртвого. `docs` в `/_health` —
            # физический счёт (сумма `s.docs`, включая погашенные), `dead` —
            # погашенные, значит доля считается прямо их отношением.
            dead = last.get("dead", 0) or 0
            dead_share = dead / max(1, count)
            last["dead_share"] = round(dead_share, 6)
            # Судить надо по **достижимому**, а не по долгу, и это выяснилось
            # измерением. Пятнадцать сегментов по 666 тысяч документов при
            # потолке окна в миллион дают вечный долг в три сегмента: слить их
            # нельзя ни в какой паре. Прежний предикат называл такой узел не
            # досведённым, хотя делать ему было нечего, — отсюда и «settled
            # достигнут в 1 прогоне из 6» у кампании.
            #
            # Долг остаётся в результате: он мера того, насколько состав дальше
            # желаемого, и по нему заводят предупреждение. Но признаком
            # завершения он быть не может.
            schedulable = compaction.get("pending_schedulable")
            if schedulable is None:
                # Сборка без этого поля: судим по долгу, как раньше, и говорим
                # об этом в результате — иначе разные прогоны сравнивались бы по
                # разным правилам молча.
                schedulable = pending
                last["settle_predicate"] = "pending_segments"
            else:
                last["settle_predicate"] = "pending_schedulable"
            settle_complete = (
                schedulable == 0 and running == 0 and dead_share <= args.settle_max_dead
            )
            signature = (last.get("segments"), pending, running)
        if is_ready and ready_at is None:
            ready_at = time.perf_counter() - began
        if is_quiescent:
            if signature != stable_signature:
                stable_signature = signature
                stable_since = time.monotonic()
            if stable_since is not None and time.monotonic() - stable_since >= args.settle_stable:
                blocked_reason = ""
                if args.engine == "svindb":
                    blocked_reason = str(
                        last.get("compaction", {}).get("blocked", "") or ""
                    ).strip()
                    if not settle_complete and not blocked_reason:
                        compaction = last.get("compaction", {})
                        schedulable = compaction.get("pending_schedulable")
                        pending = compaction.get("pending_segments", 0)
                        running = compaction.get("running_segments", 0)
                        dead_share = last.get("dead_share", 0)
                        reasons = []
                        if isinstance(schedulable, int) and schedulable > 0:
                            reasons.append(
                                f"доступно для сведения сегментов: {schedulable}"
                            )
                        elif schedulable is None and pending:
                            reasons.append(f"ожидают сведения сегментов: {pending}")
                        if running:
                            reasons.append(f"сейчас сводятся сегменты: {running}")
                        if dead_share > args.settle_max_dead:
                            reasons.append(
                                "доля погашенных документов "
                                f"{dead_share:.6f} выше предела {args.settle_max_dead:.6f}"
                            )
                        blocked_reason = "; ".join(reasons) or (
                            "состав не выполнил settled-предикат; "
                            "состояние сохранено в settled.state"
                        )
                return {
                    "ready_seconds": ready_at,
                    "settled_seconds": time.perf_counter() - began,
                    "settled_complete": settle_complete,
                    "blocked_reason": blocked_reason,
                    "state": last,
                }
        else:
            stable_since = None
            stable_signature = None
        time.sleep(0.5)
    raise CampaignError(f"индекс не достиг settled за {args.settle_timeout} с: {last}")


def summarize_samples(path: Path) -> dict[str, Any]:
    rows = []
    if path.exists():
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                with contextlib.suppress(json.JSONDecodeError):
                    rows.append(json.loads(line))
    if not rows:
        return {}
    keys = {
        "rss": "status.VmRSS",
        "pss": "smaps_rollup.Pss",
        "anonymous_rss": "smaps_rollup.Anonymous",
        "read_bytes": "io.read_bytes",
        "write_bytes": "io.write_bytes",
        "cpu_seconds": "cpu_seconds",
        "minor_faults": "minor_faults",
        "major_faults": "major_faults",
        "voluntary_context_switches": "status.voluntary_ctxt_switches",
        "involuntary_context_switches": "status.nonvoluntary_ctxt_switches",
        "fds": "fds",
    }
    out: dict[str, Any] = {"samples": len(rows)}
    cumulative = {
        "read_bytes",
        "write_bytes",
        "cpu_seconds",
        "minor_faults",
        "major_faults",
        "voluntary_context_switches",
        "involuntary_context_switches",
    }
    for label, key in keys.items():
        values = [row[key] for row in rows if key in row]
        if not values:
            continue
        out[label + "_peak"] = max(values)
        if label in cumulative:
            by_pid: dict[int, list[float]] = {}
            for row in rows:
                if key in row and isinstance(row.get("pid"), int):
                    by_pid.setdefault(row["pid"], []).append(row[key])
            out[label + "_delta"] = sum(
                process_values[-1] - process_values[0]
                for process_values in by_pid.values()
            )
            out[label + "_pids"] = len(by_pid)
        else:
            out[label + "_delta"] = values[-1] - values[0]
        out[label + "_last"] = values[-1]
    first_metrics = next((row.get("metrics") for row in rows if row.get("metrics")), {})
    last_metrics = next((row.get("metrics") for row in reversed(rows) if row.get("metrics")), {})
    out["metrics_first"] = first_metrics
    out["metrics_last"] = last_metrics
    host_rows = [row["host"] for row in rows if isinstance(row.get("host"), dict)]
    loads = [host.get("load_average", [None])[0] for host in host_rows]
    loads = [value for value in loads if isinstance(value, (int, float))]
    if loads:
        out["host_load1_peak"] = max(loads)
    available = [
        host.get("memory", {}).get("MemAvailable")
        for host in host_rows
        if isinstance(host.get("memory"), dict)
    ]
    available = [value for value in available if isinstance(value, int)]
    if available:
        out["host_mem_available_min"] = min(available)
    if len(host_rows) >= 2:
        first_cpu = host_rows[0].get("cpu_ticks", {})
        last_cpu = host_rows[-1].get("cpu_ticks", {})
        if first_cpu and last_cpu:
            deltas = {
                key: last_cpu.get(key, 0) - value for key, value in first_cpu.items()
            }
            total = sum(deltas.values())
            out["host_cpu_tick_delta"] = deltas
            out["host_cpu_steal_fraction"] = deltas.get("steal", 0) / max(1, total)
        first_vm = host_rows[0].get("vm", {})
        last_vm = host_rows[-1].get("vm", {})
        if first_vm and last_vm:
            out["host_vm_delta"] = {
                key: last_vm.get(key, 0) - value for key, value in first_vm.items()
            }
        first_disks = host_rows[0].get("diskstats", {})
        last_disks = host_rows[-1].get("diskstats", {})
        disk_delta = {}
        for disk, before in first_disks.items():
            after = last_disks.get(disk)
            if not isinstance(after, dict):
                continue
            disk_delta[disk] = {
                key: after.get(key, 0) - value for key, value in before.items()
            }
        if disk_delta:
            out["host_disk_delta"] = disk_delta
    top_snapshots = [
        host["top_processes"]
        for host in host_rows
        if isinstance(host.get("top_processes"), list)
    ]
    if top_snapshots:
        out["host_top_process_snapshots"] = top_snapshots
    cgroups = [row.get("cgroup") for row in rows if isinstance(row.get("cgroup"), dict)]
    if cgroups:
        out["cgroup"] = {
            "path": cgroups[-1].get("path"),
            "memory_max_effective": cgroups[-1].get("memory_max_effective"),
            "cpu_cores_effective": cgroups[-1].get("cpu_cores_effective"),
            "memory_current_peak": max(
                (row.get("memory_current", 0) for row in cgroups), default=0
            ),
            "memory_peak": max((row.get("memory_peak", 0) for row in cgroups), default=0),
            "memory_swap_peak": max(
                (row.get("memory_swap_peak", 0) for row in cgroups), default=0
            ),
        }
        first_events = cgroups[0].get("memory_events", {})
        last_events = cgroups[-1].get("memory_events", {})
        out["cgroup"]["memory_events_delta"] = {
            key: last_events.get(key, 0) - first_events.get(key, 0)
            for key in set(first_events) | set(last_events)
        }
    return out


def resource_validity_violations(samples: dict[str, Any]) -> list[str]:
    """Возвращает ресурсные события, несовместимые с каноническим прогоном."""
    violations = []
    cgroup = samples.get("cgroup", {})
    if isinstance(cgroup, dict):
        swap_peak = cgroup.get("memory_swap_peak", 0) or 0
        if swap_peak > 0:
            violations.append(f"cgroup использовал swap: peak={swap_peak} Б")
        events = cgroup.get("memory_events_delta", {})
        if isinstance(events, dict):
            for name in ("oom", "oom_kill", "oom_group_kill"):
                value = events.get(name, 0) or 0
                if value > 0:
                    violations.append(f"cgroup memory.events {name}={value}")
    host_vm = samples.get("host_vm_delta", {})
    if isinstance(host_vm, dict):
        for name in ("pswpin", "pswpout", "oom_kill"):
            value = host_vm.get(name, 0) or 0
            if value > 0:
                violations.append(f"host vmstat {name}={value}")
    return violations


def command_load(args: argparse.Namespace) -> None:
    if args.engine == "es" and args.protocol == "native":
        raise CampaignError("Elasticsearch принимает этот корпус только через NDJSON bulk")
    output = Path(args.output)
    started_at = now_iso()
    samples = output.with_suffix(".samples.jsonl")
    files = corpus_files(Path(args.corpus_path))
    if args.engine == "svindb":
        initial = get_json(args, "/_health")
        if initial.get("docs") != 0:
            raise CampaignError(f"канонический ingest требует пустой индекс, сейчас {initial}")
    else:
        initial_count = get_json(args, f"/{args.index}/_count").get("count")
        if initial_count != 0:
            raise CampaignError(f"канонический ingest требует пустой ES index, сейчас {initial_count}")
    metrics_before: dict[str, float] = {}
    if args.engine == "svindb":
        before_reply = one_request(args.host, args.port, "GET", "/_metrics", timeout=10)
        if before_reply.status == 200:
            metrics_before = parse_prometheus(before_reply.data.decode("utf-8", "replace"))
    jobs: queue.Queue[list[bytes] | None] = queue.Queue(maxsize=args.concurrency * 3)
    tally = LoadTally(args.documents)
    began = time.perf_counter()
    if args.corpus == "flat":
        visibility_ids = {
            "first": "flat-0000000000",
            "middle": f"flat-{max(0, args.documents // 2):010}",
            "last": f"flat-{max(0, args.documents - 1):010}",
        }
    elif args.corpus == "mixed":
        visibility_ids = {
            "first": "mixed-0000000000",
            "middle": f"mixed-{max(0, args.documents // 2):010}",
            "last": f"mixed-{max(0, args.documents - 1):010}",
        }
    else:
        visibility_ids = {}
    point_visibility: dict[str, float] = {}
    visibility_stop = threading.Event()

    def observe_visibility() -> None:
        pending = dict(visibility_ids)
        while pending and not visibility_stop.is_set():
            for label, identifier in list(pending.items()):
                path = (
                    f"/{args.index}/_doc/{urllib.parse.quote(identifier)}"
                    if args.engine == "es"
                    else f"/_doc/{urllib.parse.quote(identifier)}"
                )
                try:
                    reply = one_request(args.host, args.port, "GET", path, timeout=5)
                    if reply.status == 200:
                        point_visibility[label] = time.perf_counter() - began
                        pending.pop(label, None)
                except Exception:
                    pass
            visibility_stop.wait(0.05)

    visibility_thread = threading.Thread(target=observe_visibility, daemon=True)
    threads = [
        threading.Thread(target=worker_load, args=(args, jobs, tally, began), daemon=True)
        for _ in range(args.concurrency)
    ]
    with Sampler(args, samples):
        visibility_thread.start()
        for thread in threads:
            thread.start()
        for batch in batch_lines(
            iter_lines(files, args.documents), args.batch_documents, args.batch_bytes
        ):
            jobs.put(batch)
        for _ in threads:
            jobs.put(None)
        jobs.join()
        ack_seconds = time.perf_counter() - began
        if tally.errors:
            result = {
                "format": 1,
                "valid": False,
                "phase": "ack",
                "candidate": args.candidate,
                "engine": args.engine,
                "documents": args.documents,
                "acked": tally.acked,
                "errors": tally.errors,
                "statuses": dict(tally.statuses),
                "ack_seconds": ack_seconds,
            }
            atomic_json(output, result)
            raise CampaignError(f"приём нарушен: {tally.errors[:3]}")
        if tally.acked != args.documents:
            raise CampaignError(f"ACK для {tally.acked}, ожидалось {args.documents}")
        settled = wait_ready_and_settled(args, args.documents, began)
    visibility_stop.set()
    visibility_thread.join(timeout=10)
    if set(point_visibility) != set(visibility_ids):
        raise CampaignError(
            f"не все контрольные id стали видимы точечному чтению: {point_visibility}"
        )
    samples_summary = summarize_samples(samples)
    metrics_after: dict[str, float] = {}
    if args.engine == "svindb":
        after_reply = one_request(args.host, args.port, "GET", "/_metrics", timeout=10)
        if after_reply.status == 200:
            metrics_after = parse_prometheus(after_reply.data.decode("utf-8", "replace"))
    physical = None
    engine_store: dict[str, Any] = {}
    if args.index_path:
        storage = storage_accounting(Path(args.index_path))
        physical = storage["root_bytes"]
    elif args.engine == "es":
        store_reply = get_json(args, f"/{args.index}/_stats/store,translog")
        total = store_reply.get("_all", {}).get("total", {})
        store_bytes = total.get("store", {}).get("size_in_bytes", 0)
        translog_bytes = total.get("translog", {}).get("size_in_bytes", 0)
        engine_store = {
            "store_bytes": store_bytes,
            "translog_bytes": translog_bytes,
            "total_bytes": store_bytes + translog_bytes,
            "definition": "store_bytes=ES _stats store.size_in_bytes; translog_bytes=ES _stats translog.size_in_bytes; total_bytes=сумма",
        }
        storage = {
            "store_bytes": store_bytes,
            "translog_bytes": translog_bytes,
            "total_bytes": store_bytes + translog_bytes,
            "definition": engine_store["definition"],
        }
        physical = store_bytes + translog_bytes
    else:
        storage = None
    resource_violations = resource_validity_violations(samples_summary)
    result = {
        "format": 1,
        "valid": bool(settled["settled_complete"] and not resource_violations),
        "candidate": args.candidate,
        "engine": args.engine,
        "protocol": (
            args.protocol
            if args.protocol != "auto"
            else ("ndjson" if args.engine == "es" else "native")
        ),
        "durability": args.durability,
        "memory_limit": args.memory_limit,
        "cpu_limit": args.cpu_limit,
        "resource_contract": getattr(args, "resource_contract", None),
        "corpus": args.corpus,
        "documents": args.documents,
        "request_bytes": tally.bytes,
        "ack": {
            "seconds": ack_seconds,
            "docs_per_second": args.documents / ack_seconds,
            "mib_per_second": tally.bytes / (1 << 20) / ack_seconds,
            "first_seconds": tally.first_ack,
            "middle_seconds": tally.middle_ack,
            "last_seconds": tally.last_ack,
            "bulk_latency": latency_summary(tally.latencies),
        },
        "ready": {
            "seconds": settled["ready_seconds"],
            "docs_per_second": args.documents / settled["ready_seconds"],
        },
        "point_visibility_seconds": point_visibility,
        "settled": {
            "seconds": settled["settled_seconds"],
            "docs_per_second": args.documents / settled["settled_seconds"],
            "complete": settled["settled_complete"],
            "blocked_reason": settled["blocked_reason"],
            "state": settled["state"],
        },
        "retries_429": tally.retries_429,
        "statuses": dict(tally.statuses),
        "errors": [],
        "physical_index_bytes": physical,
        "engine_store": engine_store,
        "storage_accounting": storage,
        "bytes_per_doc": physical / args.documents if physical is not None else None,
        "resources": samples_summary,
        "resource_validity_violations": resource_violations,
        "metrics_boundaries": {"before": metrics_before, "after": metrics_after},
        "started_at": started_at,
        "finished_at": now_iso(),
    }
    cpu = samples_summary.get("cpu_seconds_delta")
    if cpu is not None:
        result["cpu_seconds_per_million"] = cpu * 1_000_000 / args.documents
    write_bytes = samples_summary.get("write_bytes_delta")
    if write_bytes is not None:
        result["write_amplification"] = write_bytes / max(1, tally.bytes)
    metrics_last = metrics_after or samples_summary.get("metrics_last", {})
    tokens = metrics_last.get("svindb_ingest_tokens_profiled_total")
    if tokens is not None:
        result["tokens"] = int(tokens)
        result["tokens_per_second_ready"] = tokens / settled["ready_seconds"]
    postings = metrics_last.get("svindb_ingest_postings_profiled_total")
    if postings is not None:
        result["postings"] = int(postings)
        result["postings_per_second_ready"] = postings / settled["ready_seconds"]
    metrics_first = metrics_before or samples_summary.get("metrics_first", {})
    allocations = metrics_last.get("svindb_allocations_total")
    allocated_bytes = metrics_last.get("svindb_allocated_bytes_total")
    if allocations is not None and allocated_bytes is not None:
        allocation_delta = allocations - metrics_first.get("svindb_allocations_total", 0)
        allocated_delta = allocated_bytes - metrics_first.get("svindb_allocated_bytes_total", 0)
        result["allocation_profile"] = {
            "allocations": int(allocation_delta),
            "allocated_bytes": int(allocated_delta),
            "allocations_per_document": allocation_delta / args.documents,
            "allocated_bytes_per_document": allocated_delta / args.documents,
        }
    atomic_json(output, result)
    print(json.dumps(result, ensure_ascii=False))


def splitmix(seed: int, value: int) -> int:
    z = (value + seed + 0x9E3779B97F4A7C15) & ((1 << 64) - 1)
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & ((1 << 64) - 1)
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & ((1 << 64) - 1)
    return z ^ (z >> 31)


def dense_query(identifier: int) -> list[float]:
    return [((splitmix(identifier, d) % 2001) - 1000) / 1000.0 for d in range(384)]


def corpus_identifier(corpus: str, number: int) -> str:
    """Вернуть настоящий ID из соответствующего воспроизводимого корпуса."""
    if corpus == "flat":
        # Генератор flat использует тот же десятизначный суффикс, что и
        # mixed-корпус. Старый msg-N принадлежал историческому fixture и не
        # должен превращать корректный GET/MGET в ложный 404.
        return f"flat-{number:010}"
    return f"{corpus}-{number:010}"


def native_query(spec: dict[str, Any], corpus: str, config: dict[str, Any], documents: int) -> tuple[str, str, Any, int]:
    meta = config["corpora"][corpus]
    field = meta["text_field"]
    freq = meta["frequencies"]
    builder = spec["builder"]
    common = {"field": field, "terms": [freq["frequent"]]}
    medium = {"field": field, "terms": [freq["medium"]]}
    rare = {"field": field, "terms": [freq["rare"]]}
    body: dict[str, Any] = {"k": 10}
    expected = 200
    if builder == "frequency":
        body["words"] = [{"field": field, "terms": [freq[spec["level"]]]}]
    elif builder == "words":
        terms = [common, medium, rare, common, medium, rare, common, medium]
        body["words"] = terms[: spec["count"]]
    elif builder == "bool":
        mode = spec["mode"]
        if mode == "should":
            body["words"] = [common, medium]
        elif mode == "must":
            body["must"] = [common, medium]
        elif mode == "must_not":
            body["must"] = [common]
            body["must_not"] = [rare]
        elif mode == "minimum_should_match":
            body["words"] = [common, medium, rare]
            body["minimum_should_match"] = 2
        else:
            body["query"] = {
                "must": [
                    {"term": common},
                    {
                        "bool": {
                            "should": [{"term": medium}, {"term": rare}],
                            "minimum_should_match": 1,
                        }
                    },
                ]
            }
    elif builder == "prefix" and corpus == "mixed":
        n = spec["expansions"]
        prefix = {1: "prefx00000*", 10: "prefx0000*", 100: "prefx000*", 1000: "prefx00*", 10000: "prefx*"}[n]
        body.update(words=[{"field": field, "terms": [prefix]}], max_expansions=n)
    elif builder == "phrase" and corpus == "mixed":
        words = [
            "phraseone", "phrasetwo", "phrasethree", "phrasefour",
            "phrasefive", "phrasesix", "phraseseven", "phraseeight",
        ][: spec["words"]]
        body["phrases"] = [{"field": field, "terms": words, "slop": spec["slop"]}]
    elif builder == "transliteration" and corpus == "mixed":
        body.update(words=[{"field": field, "terms": ["poisk"]}], translit=True)
    elif builder == "filter" and corpus == "mixed":
        upper = max(0, int(documents * spec["selectivity"]) - 1)
        body["filter"] = [{"field": "sequence", "from": 0, "to": upper}]
    elif builder == "sort" and corpus == "mixed":
        keys = [
            {"field": "sequence", "order": "desc"},
            {"field": "views", "order": "asc"},
            {"field": "score", "order": "desc"},
            {"field": "ratio", "order": "asc"},
        ][: spec["keys"]]
        body.update(words=[common], sort=keys)
    elif builder == "topk":
        body.update(words=[common], k=spec["k"])
    elif builder == "paging":
        body.update(words=[common], k=10000, size=100, ids=True)
        if spec["mode"] == "from":
            body["from"] = 9900
        else:
            # Нулевая страница поставляет настоящий cursor во время прогона;
            # здесь отмечается двухшаговый сценарий.
            body["_campaign_search_after"] = True
    elif builder == "hydrate":
        body["words"] = [common]
        mode = spec["mode"]
        if mode == "id":
            body["ids"] = True
        elif mode == "small":
            body["fields"] = ["channel"]
        elif mode == "long":
            body["fields"] = [field]
    elif builder == "highlight" and corpus == "mixed":
        body.update(
            words=[common],
            fields=[field],
            highlight={"fields": [field], "size": spec["size"]},
        )
    elif builder == "aggregation" and corpus == "mixed":
        kind = spec["kind"]
        body["filter"] = [{"field": "views"}]
        definitions: dict[str, Any] = {
            "count": {"count": {}},
            "stats": {"stats": {"field": "views"}},
            "histogram": {"histogram": {"field": "views", "interval": 10000}},
            "range": {"range": {"field": "views", "ranges": [{"to": 100}, {"from": 100, "to": 10000}, {"from": 10000}]}},
            "terms": {"terms": {"field": "channel", "size": 100}},
            "cardinality": {"cardinality": {"field": "channel"}},
        }
        body["aggs"] = {kind: definitions[kind]}
    elif builder == "slots" and corpus == "mixed":
        if spec["mode"] == "same":
            body["query"] = {"must": [{"slots": {"group": "reactions", "all": [
                {"term": {"field": "reactions.emoji", "value": "up"}},
                {"range": {"field": "reactions.count", "from": 100}},
            ]}}]}
        else:
            # Два независимых slot-узла требуют совпадений где угодно в
            # массиве; в отличие от одного узла выше, это могут быть разные
            # элементы.
            body["query"] = {"must": [
                {"slots": {"group": "reactions", "all": [
                    {"term": {"field": "reactions.emoji", "value": "up"}},
                ]}},
                {"slots": {"group": "reactions", "all": [
                    {"range": {"field": "reactions.count", "from": 100}},
                ]}},
            ]}
    elif builder == "dense_vector" and corpus == "mixed":
        body = {"k": 10, "vector": {"field": "embedding", "query": dense_query(0)}, "ids": True}
    elif builder in ("sparse_vector", "hybrid") and corpus == "mixed":
        sparse = {f"sparse_{splitmix(1, d) % 65536:x}": 0.1 + d / 16 for d in range(16)}
        body = {"k": 10, "sparse": {"field": "embedding_sparse", "query": sparse}, "ids": True}
        if builder == "hybrid":
            body["words"] = [common]
    elif builder == "get":
        ids = [corpus_identifier(corpus, i) for i in range(spec["count"])]
        if len(ids) == 1:
            return "GET", f"/_doc/{urllib.parse.quote(ids[0], safe='')}", None, 200
        return "POST", "/_mget", {"ids": ids}, 200
    elif builder == "breaker":
        terms = [{"field": field, "terms": [f"missing-{i}"]} for i in range(256)]
        body = {
            "k": 10000,
            "words": terms,
            "aggs": {f"a{i}": {"terms": {"field": "channel", "size": 10000}} for i in range(32)},
        }
        expected = 429
    else:
        raise KeyError("not_applicable")
    return "POST", "/", body, expected


def es_query(spec: dict[str, Any], corpus: str, config: dict[str, Any], documents: int, index: str) -> tuple[str, str, Any, int]:
    # Семантически совпадающее подмножество переводится явно. SvinDB-only
    # slots/breaker не выдаются за сравнение с другим контрактом.
    method, _, native, expected = native_query(spec, corpus, config, documents)
    if method == "GET" or spec["builder"] == "get":
        if spec["count"] == 1:
            identifier = corpus_identifier(corpus, 0)
            return "GET", f"/{index}/_doc/{identifier}", None, 200
        ids = [corpus_identifier(corpus, i) for i in range(spec["count"])]
        return "POST", f"/{index}/_mget", {"ids": ids}, 200
    builder = spec["builder"]
    meta = config["corpora"][corpus]
    field = meta["text_field"]
    freq = meta["frequencies"]
    if builder == "frequency":
        body = {"size": 10, "query": {"match": {field: freq[spec["level"]]}}}
    elif builder == "bool":
        mode = spec["mode"]
        if mode == "should":
            query = {"bool": {"should": [
                {"match": {field: freq["frequent"]}},
                {"match": {field: freq["medium"]}},
            ]}}
        elif mode == "must":
            query = {"bool": {"must": [
                {"match": {field: freq["frequent"]}},
                {"match": {field: freq["medium"]}},
            ]}}
        elif mode == "must_not":
            query = {"bool": {
                "must": [{"match": {field: freq["frequent"]}}],
                "must_not": [{"match": {field: freq["rare"]}}],
            }}
        elif mode == "minimum_should_match":
            query = {"bool": {
                "should": [
                    {"match": {field: freq["frequent"]}},
                    {"match": {field: freq["medium"]}},
                    {"match": {field: freq["rare"]}},
                ],
                "minimum_should_match": 2,
            }}
        else:
            raise KeyError("not_applicable")
        body = {"size": 10, "query": query}
    elif builder == "topk":
        body = {"size": spec["k"], "query": {"match": {field: freq["frequent"]}}}
    elif builder == "hydrate":
        body = {"size": 10, "query": {"match": {field: freq["frequent"]}}}
        mode = spec["mode"]
        body["_source"] = False if mode in ("none", "id") else ["channel" if mode == "small" else field]
    elif builder == "prefix" and corpus == "mixed":
        n = spec["expansions"]
        prefix = {1: "prefx00000", 10: "prefx0000", 100: "prefx000", 1000: "prefx00", 10000: "prefx"}[n]
        body = {"size": 10, "query": {"prefix": {field: {"value": prefix, "rewrite": "scoring_boolean"}}}}
    elif builder == "phrase" and corpus == "mixed":
        words = ["phraseone", "phrasetwo", "phrasethree", "phrasefour", "phrasefive", "phrasesix", "phraseseven", "phraseeight"][: spec["words"]]
        body = {"size": 10, "query": {"match_phrase": {field: {"query": " ".join(words), "slop": spec["slop"]}}}}
    elif builder == "filter" and corpus == "mixed":
        upper = max(0, int(documents * spec["selectivity"]) - 1)
        body = {"size": 10, "query": {"range": {"sequence": {"gte": 0, "lte": upper}}}}
    elif builder == "sort" and corpus == "mixed":
        keys = ["sequence", "views", "score", "ratio"][: spec["keys"]]
        body = {"size": 10, "query": {"match": {field: freq["frequent"]}}, "sort": [{key: "desc"} for key in keys]}
    elif builder == "highlight" and corpus == "mixed":
        body = {"size": 10, "query": {"match": {field: freq["frequent"]}}, "_source": [field], "highlight": {"fields": {field: {"fragment_size": spec["size"]}}}}
    elif builder == "dense_vector" and corpus == "mixed":
        body = {"size": 10, "knn": {"field": "embedding", "query_vector": dense_query(0), "k": 10, "num_candidates": 100}}
    elif builder == "aggregation" and corpus == "mixed":
        kind = spec["kind"]
        agg = {
            "count": {"value_count": {"field": "views"}},
            "stats": {"stats": {"field": "views"}},
            "histogram": {"histogram": {"field": "views", "interval": 10000}},
            "range": {"range": {"field": "views", "ranges": [{"to": 100}, {"from": 100, "to": 10000}, {"from": 10000}]}},
            "terms": {"terms": {"field": "channel", "size": 100}},
            "cardinality": {"cardinality": {"field": "channel"}},
        }[kind]
        body = {"size": 0, "query": {"exists": {"field": "views"}}, "aggs": {kind: agg}}
    else:
        raise KeyError("not_applicable")
    return "POST", f"/{index}/_search", body, expected


def execute_query_once(args: argparse.Namespace, method: str, path: str, body: Any, expected: int) -> tuple[float, Any]:
    reply = json_request(args.host, args.port, method, path, body, args.timeout)
    if reply.status != expected:
        raise CampaignError(f"{method} {path}: HTTP {reply.status}, ожидался {expected}: {reply.data[:500]!r}")
    decoded = reply.json() if reply.data else {}
    return reply.elapsed_ms, decoded


def validate_query_reference(spec: dict[str, Any], decoded: Any) -> None:
    """Не позволяет принять быстрый benchmark пустого ответа за поиск.

    Семантический hash доказывает повторяемость ответа, но пустой ответ тоже
    повторяем. Для term/top-k/words/bool матрица называет реальные термы
    корпуса; если workload с корпусом разошлись, прогон обязан остановиться, а
    не публиковать тысячи QPS обхода, который ничего не нашёл.
    """
    builder = spec.get("builder")
    level = spec.get("level")
    if builder not in {"frequency", "words", "bool", "topk", "paging", "hydrate"}:
        return
    hits = decoded.get("hits") if isinstance(decoded, dict) else None
    if isinstance(hits, dict):
        hits = hits.get("hits")
    if not isinstance(hits, list):
        raise CampaignError(f"{spec.get('name')}: ответ не содержит массив hits")
    if builder == "frequency" and level == "miss":
        if hits:
            raise CampaignError(f"{spec.get('name')}: заведомо отсутствующий term дал hits")
        return
    if not hits:
        raise CampaignError(
            f"{spec.get('name')}: контрольный ответ пуст; workload не соответствует корпусу"
        )


def semantic_view(value: Any) -> Any:
    """Убирает только runtime/physical-поля, сохраняя логический ответ."""
    if isinstance(value, dict):
        return {
            key: semantic_view(item)
            for key, item in value.items()
            # `visited` — диагностическое число кандидатов до конкурентного
            # pruning. При том же top-K оно законно меняется от того, какой
            # сегмент первым поднял общий floor, поэтому это не часть ответа.
            if key
            not in ("took", "took_ms", "stats", "_shards", "_index", "doc", "visited")
        }
    if isinstance(value, list):
        return [semantic_view(item) for item in value]
    return value


def semantic_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            semantic_view(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def identity_view(value: Any) -> Any:
    """Логический состав ответа без score и физических tie-breaker-ключей."""
    if isinstance(value, dict):
        return {
            key: identity_view(item)
            for key, item in value.items()
            if key
            not in (
                "took",
                "took_ms",
                "stats",
                "_shards",
                "_index",
                "doc",
                "visited",
                "score",
                "sort",
            )
        }
    if isinstance(value, list):
        return [identity_view(item) for item in value]
    return value


def identity_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            identity_view(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def command_query_mixed(
    args: argparse.Namespace,
    selected: list[dict[str, Any]],
    config: dict[str, Any],
    output: Path,
) -> None:
    """Смешанный saturation-профиль с раздельными хвостами каждой семьи."""
    prepared = []
    skipped = []
    for spec in selected:
        # Breaker меняет настройки узла, а search_after зависит от предыдущего
        # ответа. Оба сценария измеряются полной матрицей, но не смешиваются с
        # независимыми stateless-запросами saturation-профиля.
        if spec["builder"] == "breaker" or spec.get("mode") == "search_after":
            skipped.append({"family": spec["name"], "status": "not_applicable"})
            continue
        try:
            method, path, body, expected = (
                native_query(spec, args.corpus, config, args.documents)
                if args.engine == "svindb"
                else es_query(spec, args.corpus, config, args.documents, args.index)
            )
        except KeyError:
            skipped.append({"family": spec["name"], "status": "not_applicable"})
            continue
        for _ in range(args.warmup):
            execute_query_once(args, method, path, body, expected)
        _, reference = execute_query_once(args, method, path, body, expected)
        validate_query_reference(spec, reference)
        response_sha256 = semantic_digest(reference)
        oracle_body = body
        if (
            args.engine == "svindb"
            and method == "POST"
            and path == "/"
            and expected == 200
            and isinstance(body, dict)
        ):
            oracle_body = json.loads(json.dumps(body))
            oracle_body["ids"] = True
        _, oracle_response = execute_query_once(args, method, path, oracle_body, expected)
        prepared.append(
            {
                "spec": spec,
                "method": method,
                "path": path,
                "body": body,
                "expected": expected,
                "response_sha256": response_sha256,
                "semantic_sha256": semantic_digest(oracle_response),
                "identity_sha256": identity_digest(oracle_response),
            }
        )
    if not prepared:
        raise CampaignError("для mixed query-профиля не осталось применимых семей")

    per_family = {
        item["spec"]["name"]: {
            "latencies": [],
            "took": [],
            "stats": Counter(),
            "errors": [],
        }
        for item in prepared
    }
    lock = threading.Lock()
    issued = 0
    began = time.perf_counter()

    def task() -> None:
        nonlocal issued
        try:
            while True:
                with lock:
                    if issued >= args.mixed_requests:
                        return
                    ordinal = issued
                    issued += 1
                item = prepared[splitmix(args.seed, ordinal) % len(prepared)]
                name = item["spec"]["name"]
                try:
                    elapsed, decoded = execute_query_once(
                        args,
                        item["method"],
                        item["path"],
                        item["body"],
                        item["expected"],
                    )
                    digest = semantic_digest(decoded)
                    if digest != item["response_sha256"]:
                        raise CampaignError(
                            f"{name}: ответ внутри mixed-профиля разошёлся: "
                            f"{digest} != {item['response_sha256']}"
                        )
                    with lock:
                        row = per_family[name]
                        row["latencies"].append(elapsed)
                        if isinstance(decoded, dict) and isinstance(decoded.get("took_ms"), (int, float)):
                            row["took"].append(float(decoded["took_ms"]))
                        for key, value in decoded.get("stats", {}).items() if isinstance(decoded, dict) else []:
                            if isinstance(value, (int, float)):
                                row["stats"][key] += value
                except Exception as error:
                    with lock:
                        per_family[name]["errors"].append(repr(error))
        finally:
            close_persistent_connection()

    workers = [threading.Thread(target=task, daemon=True) for _ in range(args.concurrency)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    seconds = time.perf_counter() - began
    families = []
    for item in prepared:
        name = item["spec"]["name"]
        row = per_family[name]
        families.append(
            {
                "family": name,
                "status": "passed" if not row["errors"] else "failed",
                "requests": len(row["latencies"]),
                "seconds": seconds,
                "qps": len(row["latencies"]) / seconds,
                "latency": latency_summary(row["latencies"]),
                "server_took": latency_summary(row["took"]),
                "semantic_sha256": item["semantic_sha256"],
                "identity_sha256": item["identity_sha256"],
                "response_sha256": item["response_sha256"],
                "stats_total": dict(row["stats"]),
                "errors": row["errors"][:20],
            }
        )
    families.extend(skipped)
    result = {
        "format": 1,
        "candidate": args.candidate,
        "engine": args.engine,
        "corpus": args.corpus,
        "concurrency": args.concurrency,
        "mixed": True,
        "mixed_requests": args.mixed_requests,
        "seconds": seconds,
        "qps": sum(row["requests"] for row in families if "requests" in row) / seconds,
        "families": families,
        "valid": all(row["status"] in ("passed", "not_applicable") for row in families),
    }
    atomic_json(output, result)
    print(
        json.dumps(
            {"output": str(output), "valid": result["valid"], "requests": args.mixed_requests},
            ensure_ascii=False,
        )
    )
    if not result["valid"]:
        raise CampaignError("mixed query-профиль завершён с ошибками")


def command_query_unmeasured(args: argparse.Namespace) -> None:
    output = Path(args.output)
    config = json.loads(Path(args.workload_file).read_text(encoding="utf-8"))

    def matches_family(name: str, pattern: str) -> bool:
        """Поддерживает прежний поиск подстроки и точный шаблон ``=имя``."""
        return name == pattern[1:] if pattern.startswith("=") else pattern in name

    selected = [
        spec for spec in config["query_families"]
        if not args.family
        or any(matches_family(spec["name"], pattern) for pattern in args.family)
    ]
    if args.mixed_requests:
        command_query_mixed(args, selected, config, output)
        return
    results = []
    for spec in selected:
        try:
            built = (
                native_query(spec, args.corpus, config, args.documents)
                if args.engine == "svindb"
                else es_query(spec, args.corpus, config, args.documents, args.index)
            )
        except KeyError:
            results.append({"family": spec["name"], "status": "not_applicable"})
            continue
        method, path, body, expected = built
        search_after = isinstance(body, dict) and body.pop("_campaign_search_after", False)
        original_budget = None
        first_request: dict[str, Any] | None = None
        try:
            if args.engine == "svindb" and spec["builder"] == "breaker":
                settings = json_request(args.host, args.port, "GET", "/_settings", timeout=args.timeout)
                if settings.status != 200:
                    raise CampaignError(f"не прочитаны настройки breaker: HTTP {settings.status}")
                original_budget = settings.json().get("memory_budget_mb", 256)
                changed = json_request(
                    args.host,
                    args.port,
                    "PUT",
                    "/_settings",
                    {"memory_budget_mb": 32},
                    args.timeout,
                )
                if changed.status != 200:
                    raise CampaignError(f"не выставлен диагностический budget: HTTP {changed.status}")
            if args.capture_first:
                first_elapsed, first_response = execute_query_once(
                    args, method, path, body, expected
                )
                first_request = {
                    "elapsed_ms": first_elapsed,
                    "semantic_sha256": semantic_digest(first_response),
                }
            for _ in range(args.warmup):
                _, first = execute_query_once(args, method, path, body, expected)
                if search_after:
                    hits = first.get("hits", [])
                    if hits:
                        body["search_after"] = hits[-1].get("sort", [hits[-1].get("score"), hits[-1].get("doc")])
            # Производственная длительность и счётчики работы меняются от
            # запуска к запуску, но сама выдача одного и того же запроса — нет.
            # Отдельный контрольный ответ не входит в latency-выборку.
            _, semantic_reference = execute_query_once(args, method, path, body, expected)
            validate_query_reference(spec, semantic_reference)
            response_sha256 = semantic_digest(semantic_reference)
            # Латентность поиска без hydration не должна незаметно превратиться
            # в latency поиска с ID. Поэтому ID запрашиваются отдельным
            # контрольным вызовом вне выборки; его hash можно честно сравнить
            # между HEAD/worktree даже при разных физических doc ordinals.
            oracle_body = body
            if (
                args.engine == "svindb"
                and method == "POST"
                and path == "/"
                and expected == 200
                and isinstance(body, dict)
            ):
                oracle_body = json.loads(json.dumps(body))
                oracle_body["ids"] = True
            _, oracle_response = execute_query_once(args, method, path, oracle_body, expected)
            semantic_sha256 = semantic_digest(oracle_response)
            identity_sha256 = identity_digest(oracle_response)
            latencies: list[float] = []
            took: list[float] = []
            stats_sum: Counter[str] = Counter()
            errors: list[str] = []
            began = time.perf_counter()
            lock = threading.Lock()
            issued = 0

            def task() -> None:
                nonlocal issued
                try:
                    while True:
                        with lock:
                            if issued >= args.repetitions:
                                return
                            issued += 1
                        try:
                            elapsed, decoded = execute_query_once(args, method, path, body, expected)
                            digest = semantic_digest(decoded)
                            if digest != response_sha256:
                                raise CampaignError(
                                    f"ответ внутри прогона разошёлся: {digest} != {response_sha256}"
                                )
                            with lock:
                                latencies.append(elapsed)
                                if isinstance(decoded, dict) and isinstance(decoded.get("took_ms"), (int, float)):
                                    took.append(float(decoded["took_ms"]))
                                for key, value in decoded.get("stats", {}).items() if isinstance(decoded, dict) else []:
                                    if isinstance(value, (int, float)):
                                        stats_sum[key] += value
                        except Exception as error:
                            with lock:
                                errors.append(repr(error))
                finally:
                    close_persistent_connection()

            workers = [threading.Thread(target=task, daemon=True) for _ in range(args.concurrency)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
            seconds = time.perf_counter() - began
            results.append(
                {
                    "family": spec["name"],
                    "status": "passed" if not errors and len(latencies) == args.repetitions else "failed",
                    "requests": len(latencies),
                    "seconds": seconds,
                    "qps": len(latencies) / seconds,
                    "latency": latency_summary(latencies),
                    "server_took": latency_summary(took),
                    "semantic_sha256": semantic_sha256,
                    "identity_sha256": identity_sha256,
                    "response_sha256": response_sha256,
                    "first_request": first_request,
                    "stats_total": dict(stats_sum),
                    "errors": errors[:20],
                }
            )
        except Exception as error:
            results.append({"family": spec["name"], "status": "failed", "errors": [repr(error)]})
        finally:
            if original_budget is not None:
                restored = json_request(
                    args.host,
                    args.port,
                    "PUT",
                    "/_settings",
                    {"memory_budget_mb": original_budget},
                    args.timeout,
                )
                if restored.status != 200:
                    results.append(
                        {
                            "family": "breaker/restore-settings",
                            "status": "failed",
                            "errors": [f"HTTP {restored.status}: {restored.data[:300]!r}"],
                        }
                    )
        completed = next(
            row for row in reversed(results) if row.get("family") == spec["name"]
        )
        print(
            json.dumps(
                {
                    "family": spec["name"],
                    "status": completed["status"],
                    "qps": completed.get("qps"),
                    "p99_ms": completed.get("latency", {}).get("p99_ms"),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
    result = {
        "format": 1,
        "candidate": args.candidate,
        "engine": args.engine,
        "corpus": args.corpus,
        "concurrency": args.concurrency,
        "repetitions": args.repetitions,
        "families": results,
        "valid": all(row["status"] in ("passed", "not_applicable") for row in results),
    }
    atomic_json(output, result)
    print(json.dumps({"output": str(output), "valid": result["valid"], "families": len(results)}, ensure_ascii=False))
    if not result["valid"]:
        raise CampaignError("одна или несколько query families не прошли")


def command_query(args: argparse.Namespace) -> None:
    """Измеряет запросы вместе с CPU, памятью, faults, I/O и /_metrics."""
    output = Path(args.output)
    samples = output.with_suffix(".samples.jsonl")
    try:
        with Sampler(args, samples):
            command_query_unmeasured(args)
    finally:
        # Внутренний runner сначала сохраняет логический результат. Даже если
        # oracle обнаружил ошибку и поднял CampaignError, диагностические
        # ресурсы должны остаться рядом с невалидным прогоном.
        if output.exists():
            result = json.loads(output.read_text(encoding="utf-8"))
            result["resources"] = summarize_samples(samples)
            atomic_json(output, result)


class Oracle:
    def __init__(self, path: Path):
        self.path = path
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS latest(id TEXT PRIMARY KEY, revision INTEGER NOT NULL, deleted INTEGER NOT NULL, marker TEXT NOT NULL)"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS operations(seq INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL, op TEXT NOT NULL, id TEXT NOT NULL, revision INTEGER NOT NULL, marker TEXT NOT NULL)"
        )
        self.connection.commit()
        self.lock = threading.Lock()
        self.pending: queue.Queue[tuple[str, str, int, str] | None] = queue.Queue(maxsize=1_000_000)
        self.writer = threading.Thread(target=self._write, daemon=True)
        self.writer.start()

    def _write(self) -> None:
        while True:
            first = self.pending.get()
            if first is None:
                self.pending.task_done()
                return
            batch = [first]
            deadline = time.monotonic() + 0.01
            while len(batch) < 1024 and time.monotonic() < deadline:
                try:
                    item = self.pending.get_nowait()
                except queue.Empty:
                    break
                if item is None:
                    # Sentinel допустим только после `flush`, то есть за ним
                    # уже нет операций. Возвращаем его очереди после batch.
                    self.pending.task_done()
                    break
                batch.append(item)
            with self.lock:
                for operation, identifier, revision, marker in batch:
                    deleted = int(operation == "delete")
                    self.connection.execute(
                        "INSERT INTO operations(at,op,id,revision,marker) VALUES(?,?,?,?,?)",
                        (time.time(), operation, identifier, revision, marker),
                    )
                    self.connection.execute(
                        "INSERT INTO latest(id,revision,deleted,marker) VALUES(?,?,?,?) "
                        "ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,deleted=excluded.deleted,marker=excluded.marker "
                        "WHERE excluded.revision >= latest.revision",
                        (identifier, revision, deleted, marker),
                    )
                self.connection.commit()
            for _ in batch:
                self.pending.task_done()

    def record(self, operation: str, identifier: str, revision: int, marker: str) -> None:
        self.pending.put((operation, identifier, revision, marker))

    def flush(self) -> None:
        self.pending.join()

    def rows(self) -> list[tuple[str, int, int, str]]:
        self.flush()
        with self.lock:
            return list(self.connection.execute("SELECT id,revision,deleted,marker FROM latest ORDER BY id"))

    def superseded_markers(self, limit: int = 10_000) -> list[tuple[str, str]]:
        """Берёт доказательную выборку старых маркеров, которых уже не должно быть."""
        self.flush()
        with self.lock:
            return list(
                self.connection.execute(
                    "SELECT o.id,o.marker FROM operations o JOIN latest l ON l.id=o.id "
                    "WHERE o.op<>'delete' AND o.marker<>l.marker LIMIT ?",
                    (limit,),
                )
            )

    def close(self) -> None:
        self.flush()
        self.pending.put(None)
        self.pending.join()
        self.writer.join(timeout=30)
        self.connection.close()


def stress_field_contract(corpus: str) -> tuple[str, str, str]:
    """Возвращает text/marker/revision поля фактической схемы корпуса.

    Stress запускается поверх уже построенного 10M-индекса. Flat и Mixed
    намеренно имеют разные production-схемы, поэтому подмена Flat-полей
    ``message/views/channel`` на Mixed-поля ``text/sequence/status`` должна
    останавливаться ещё в runner, а не превращаться в миллионы ожидаемых 400.
    """
    if corpus == "flat":
        return "message", "channel", "views"
    if corpus == "mixed":
        return "text", "status", "sequence"
    raise CampaignError(f"для stress неизвестен контракт корпуса: {corpus!r}")


def stress_document(
    corpus: str,
    identifier: str,
    revision: int,
    marker: str,
    large: bool = False,
) -> dict[str, Any]:
    text = f"stress_marker {marker} revision_{revision} frequent_term"
    if large:
        text += " large_payload" * 100_000
    text_field, marker_field, revision_field = stress_field_contract(corpus)
    document: dict[str, Any] = {
        "id": identifier,
        text_field: text,
        # Отдельный keyword даёт oracle точный version marker. Искать marker в
        # анализируемом тексте нельзя: общий префикс разложится на токены и
        # прежние версии станут неотличимы от новых совпадений.
        marker_field: marker,
        revision_field: revision,
    }
    if corpus == "flat":
        # Поле обязательно для честного соответствия Flat-10M схеме; значение
        # детерминировано и остаётся внутри допустимого epoch_second.
        document["ts"] = 1_700_000_000 + revision
    else:
        document["title"] = marker
        document["channel"] = "stress"
        document["views"] = revision
    return document


def stress_mutation(args: argparse.Namespace, operation: str, identifier: str, revision: int, marker: str, large: bool) -> Reply:
    if args.engine == "svindb":
        if operation == "delete":
            return json_request(args.host, args.port, "POST", "/_delete", {"ids": [identifier]}, args.timeout)
        endpoint = "/_bulk" if operation == "insert" else "/_put"
        return json_request(
            args.host,
            args.port,
            "POST",
            endpoint,
            {
                "documents": [
                    stress_document(args.corpus, identifier, revision, marker, large)
                ]
            },
            args.timeout,
        )
    path = f"/{args.index}/_doc/{urllib.parse.quote(identifier, safe='')}"
    if operation == "delete":
        return one_request(args.host, args.port, "DELETE", path, timeout=args.timeout)
    if operation == "insert":
        path += "?op_type=create"
    return json_request(
        args.host,
        args.port,
        "PUT",
        path,
        stress_document(args.corpus, identifier, revision, marker, large),
        args.timeout,
    )


def stress_retry_429(request: Any, attempts: int = 8) -> tuple[Reply, int]:
    """Повторяет только явный backpressure, который ничего не применил.

    HTTP 429 входит в контракт насыщения SvinDB и Elasticsearch. Считать его
    потерянной операцией нельзя, но и объявлять успешным до фактического 2xx
    нельзя. Другие HTTP-ответы и transport errors здесь не скрываются.
    """
    retries = 0
    while True:
        reply = request()
        if reply.status != 429 or retries >= attempts:
            return reply, retries
        retries += 1
        time.sleep(min(1.0, 0.01 * 2**retries))


def stress_seed_updates(args: argparse.Namespace, oracle: Oracle) -> dict[str, Any]:
    """Создаёт до измерения фиксированный пул настоящих целей для update."""
    batch_size = min(5000, max(1, args.oracle_ids))
    written = 0
    retries = 0
    errors = []
    for offset in range(0, args.oracle_ids, batch_size):
        documents = []
        for ordinal in range(offset, min(args.oracle_ids, offset + batch_size)):
            worker = ordinal % args.concurrency
            slot = ordinal // args.concurrency
            identifier = f"stress-update-{args.profile}-{worker:03}-{slot:06}"
            marker = f"seed_{args.seed:x}_{args.profile}_{worker}_{slot}"
            documents.append(
                (
                    identifier,
                    marker,
                    stress_document(args.corpus, identifier, 0, marker),
                )
            )
        if args.engine == "svindb":
            body = json.dumps(
                {"documents": [document for _, _, document in documents]},
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
            path = "/_bulk"
            headers = JSON_HEADERS
        else:
            chunks = []
            for identifier, _, document in documents:
                chunks.append(json.dumps({"create": {"_id": identifier}}, separators=(",", ":")))
                chunks.append(json.dumps(document, ensure_ascii=False, separators=(",", ":")))
            body = ("\n".join(chunks) + "\n").encode()
            path = f"/{args.index}/_bulk"
            headers = NDJSON_HEADERS
        attempts = 0
        while True:
            attempts += 1
            try:
                reply = one_request(
                    args.host, args.port, "POST", path, body, headers, args.timeout
                )
            except Exception as error:
                errors.append({"offset": offset, "transport": repr(error)})
                break
            if reply.status == 429 and attempts <= 8:
                retries += 1
                time.sleep(min(1.0, 0.01 * 2**attempts))
                continue
            valid = 200 <= reply.status < 300
            if valid:
                decoded = reply.json()
                valid = not (args.engine == "es" and decoded.get("errors", False))
                valid = valid and not (
                    args.engine == "svindb" and decoded.get("added") != len(documents)
                )
            if not valid:
                errors.append(
                    {
                        "offset": offset,
                        "status": reply.status,
                        "body": reply.data[:500].decode("utf-8", "replace"),
                    }
                )
                break
            for identifier, marker, _ in documents:
                oracle.record("insert", identifier, 0, marker)
            written += len(documents)
            break
        if errors:
            break
    oracle.flush()
    return {
        "documents": written,
        "expected": args.oracle_ids,
        "retries_429": retries,
        "errors": errors,
        "valid": written == args.oracle_ids and not errors,
    }


def stress_cas_probe(args: argparse.Namespace, oracle: Oracle) -> dict[str, Any]:
    """Доказывает атомарность CAS настоящей одновременной гонкой."""
    if args.engine != "svindb":
        return {"status": "not_applicable", "reason": "контракт ES использует другой CAS"}
    identifier = f"stress-cas-{args.profile}-{args.seed:x}"
    _, _, revision_field = stress_field_contract(args.corpus)
    competitors = max(2, min(16, args.concurrency))

    def compete(number: int) -> tuple[int, str, bytes]:
        marker = f"cas_{args.seed:x}_{args.profile}_{number}"
        body = {
            "documents": [stress_document(args.corpus, identifier, 1, marker)],
            "conditions": [
                {"id": identifier, "field": revision_field, "equals": None}
            ],
        }
        reply = json_request(args.host, args.port, "POST", "/_put", body, args.timeout)
        return reply.status, marker, reply.data

    with concurrent.futures.ThreadPoolExecutor(max_workers=competitors) as pool:
        replies = list(pool.map(compete, range(competitors)))
    winners = [(marker, body) for status, marker, body in replies if 200 <= status < 300]
    conflicts = sum(status == 409 for status, _, _ in replies)
    valid = len(winners) == 1 and conflicts == competitors - 1
    if valid:
        oracle.record("insert", identifier, 1, winners[0][0])
    return {
        "status": "passed" if valid else "failed",
        "competitors": competitors,
        "http_statuses": dict(Counter(status for status, _, _ in replies)),
        "winner_marker": winners[0][0] if len(winners) == 1 else None,
        "responses": [body[:300].decode("utf-8", "replace") for _, _, body in replies],
        "valid": valid,
    }


def stress_disconnect_probe(args: argparse.Namespace, oracle: Oracle) -> dict[str, Any]:
    """Отправляет полное тело, теряет ACK и разрешает неопределённость чтением."""
    identifier = f"stress-disconnect-{args.profile}-{args.seed:x}"
    marker = f"disconnect_{args.seed:x}_{args.profile}"
    document = stress_document(args.corpus, identifier, 1, marker)
    if args.engine == "svindb":
        path = "/_bulk"
        body = json.dumps({"documents": [document]}, separators=(",", ":")).encode()
    else:
        path = f"/{args.index}/_doc/{urllib.parse.quote(identifier, safe='')}"
        body = json.dumps(document, separators=(",", ":")).encode()
    method = "POST" if args.engine == "svindb" else "PUT"
    request = (
        f"{method} {path} HTTP/1.1\r\nHost: {args.host}\r\nContent-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
    ).encode() + body
    try:
        sock = socket.create_connection((args.host, args.port), timeout=args.timeout)
        sock.sendall(request)
        # Ждём только готовности ответа на сокете, не читая ни его статус, ни
        # тело. Это доказывает, что сервер обработал полное тело, но оставляет
        # ACK ненаблюдавшимся на уровне приложения клиента.
        readable, _, _ = select.select([sock], [], [], min(args.timeout, 30))
        if not readable:
            sock.close()
            return {
                "status": "failed",
                "resolution": "response_not_ready",
                "ack_observed": False,
                "valid": False,
            }
        sock.close()
    except Exception as error:
        return {"status": "failed", "transport": repr(error), "valid": False}
    # ACK намеренно не читался. Факт применения берётся только из GET.
    deadline = time.monotonic() + min(args.timeout, 30)
    last = None
    while time.monotonic() < deadline:
        try:
            path = (
                f"/{args.index}/_doc/{urllib.parse.quote(identifier, safe='')}"
                if args.engine == "es"
                else f"/_doc/{urllib.parse.quote(identifier, safe='')}"
            )
            reply = one_request(args.host, args.port, "GET", path, timeout=5)
            last = reply.status
            if reply.status == 200:
                value = reply.json()
                fields = value.get("_source", {}) if args.engine == "es" else value.get("fields", {})
                _, marker_field, revision_field = stress_field_contract(args.corpus)
                if (
                    fields.get(marker_field) == marker
                    and int(fields.get(revision_field, -1)) == 1
                ):
                    oracle.record("insert", identifier, 1, marker)
                    return {
                        "status": "passed",
                        "resolution": "applied",
                        "ack_observed": False,
                        "valid": True,
                    }
                return {
                    "status": "failed",
                    "resolution": "different_value",
                    "fields": fields,
                    "valid": False,
                }
            if reply.status == 404:
                time.sleep(0.05)
                continue
            return {"status": "failed", "resolution_http": reply.status, "valid": False}
        except Exception:
            time.sleep(0.05)
    # Не применённая операция при потерянном ACK тоже корректно разрешена:
    # oracle её не подтверждает и не требует воскресить.
    return {
        "status": "passed" if last == 404 else "failed",
        "resolution": "not_applied" if last == 404 else "unresolved",
        "last_http": last,
        "ack_observed": False,
        "valid": last == 404,
    }


def stress_burst(args: argparse.Namespace, oracle: Oracle, ordinal: int) -> dict[str, Any]:
    """Посылает настоящий bulk burst и повторяет документированный 429."""
    documents = []
    for number in range(args.burst_documents):
        identifier = f"burst-{args.profile}-{ordinal:04}-{number:06}"
        marker = f"burst_{args.seed:x}_{ordinal}_{number}"
        documents.append(
            (
                identifier,
                marker,
                stress_document(args.corpus, identifier, 1, marker),
            )
        )
    if args.engine == "svindb":
        path = "/_bulk"
        body = json.dumps(
            {"documents": [document for _, _, document in documents]},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        headers = JSON_HEADERS
    else:
        path = f"/{args.index}/_bulk"
        chunks = []
        for identifier, _, document in documents:
            chunks.append(json.dumps({"index": {"_id": identifier}}, separators=(",", ":")))
            chunks.append(json.dumps(document, ensure_ascii=False, separators=(",", ":")))
        body = ("\n".join(chunks) + "\n").encode()
        headers = NDJSON_HEADERS
    retries = 0
    while True:
        reply = one_request(args.host, args.port, "POST", path, body, headers, args.timeout)
        if reply.status != 429 or retries >= 8:
            break
        retries += 1
        time.sleep(min(1.0, 0.01 * 2**retries))
    valid = 200 <= reply.status < 300
    if valid and args.engine == "es":
        decoded = reply.json()
        valid = not decoded.get("errors", False)
    if valid:
        for identifier, marker, _ in documents:
            oracle.record("insert", identifier, 1, marker)
    return {
        "ordinal": ordinal,
        "documents": len(documents),
        "bytes": len(body),
        "http_status": reply.status,
        "retries_429": retries,
        "valid": valid,
    }


def verify_oracle(args: argparse.Namespace, oracle: Oracle) -> dict[str, Any]:
    rows = oracle.rows()
    failures = []
    _, marker_field, revision_field = stress_field_contract(args.corpus)
    if args.engine == "svindb":
        for offset in range(0, len(rows), 1000):
            batch = rows[offset : offset + 1000]
            reply = json_request(args.host, args.port, "POST", "/_mget", {"ids": [row[0] for row in batch]}, args.timeout)
            if reply.status != 200:
                failures.append(f"mget HTTP {reply.status}: {reply.data[:300]!r}")
                continue
            docs = reply.json().get("docs", [])
            for expected, got in zip(batch, docs):
                identifier, revision, deleted, marker = expected
                if deleted:
                    if got.get("found"):
                        failures.append(f"{identifier}: удалённый документ найден")
                elif not got.get("found"):
                    failures.append(f"{identifier}: подтверждённая версия не найдена")
                else:
                    fields = got.get("fields", {})
                    if (
                        int(fields.get(revision_field, -1)) != revision
                        or fields.get(marker_field) != marker
                    ):
                        failures.append(f"{identifier}: {fields}, ожидались revision={revision}, marker={marker}")
                if len(failures) >= 100:
                    break
    else:
        for offset in range(0, len(rows), 1000):
            batch = rows[offset : offset + 1000]
            reply = json_request(args.host, args.port, "POST", f"/{args.index}/_mget", {"ids": [row[0] for row in batch]}, args.timeout)
            if reply.status != 200:
                failures.append(f"mget HTTP {reply.status}: {reply.data[:300]!r}")
                continue
            for expected, got in zip(batch, reply.json().get("docs", [])):
                identifier, revision, deleted, marker = expected
                if deleted != int(not got.get("found")):
                    failures.append(f"{identifier}: found={got.get('found')}, deleted={deleted}")
                elif not deleted:
                    source = got.get("_source", {})
                    if (
                        source.get(revision_field) != revision
                        or source.get(marker_field) != marker
                    ):
                        failures.append(f"{identifier}: неверная версия {source}")
                if len(failures) >= 100:
                    break
    def marker_hits(marker: str) -> tuple[int, list[str | None]]:
        # Один уникальный keyword-term позволяет требовать ровно один ID без
        # неоднозначности should/top-K и отдельно доказывает, что прежняя
        # версия с другим marker больше не находится.
        if args.engine == "svindb":
            reply = json_request(
                args.host,
                args.port,
                "POST",
                "/",
                {
                    "k": 10,
                    "words": [{"field": marker_field, "terms": [marker]}],
                    "ids": True,
                },
                args.timeout,
            )
            hits = reply.json().get("hits", []) if reply.status == 200 else []
            return reply.status, [hit.get("id") for hit in hits]
        reply = json_request(
            args.host,
            args.port,
            "POST",
            f"/{args.index}/_search",
            {
                "size": 10,
                "_source": False,
                "query": {"term": {marker_field: marker}},
            },
            args.timeout,
        )
        hits = reply.json().get("hits", {}).get("hits", []) if reply.status == 200 else []
        return reply.status, [hit.get("_id") for hit in hits]

    live = [(identifier, marker) for identifier, _, deleted, marker in rows if not deleted]
    if len(live) > 10_000:
        step = len(live) / 10_000
        live = [live[min(len(live) - 1, int(index * step))] for index in range(10_000)]
    marker_checked = 0
    for identifier, marker in live:
        status, got = marker_hits(marker)
        if status != 200 or got != [identifier]:
            failures.append(
                f"latest marker {marker!r}: HTTP {status}, ids={got[:10]}, ожидался {identifier!r}"
            )
        marker_checked += 1
        if len(failures) >= 100:
            break

    superseded = oracle.superseded_markers()
    superseded_found = []
    for identifier, marker in superseded:
        status, got = marker_hits(marker)
        if status != 200:
            failures.append(f"superseded marker {marker!r}: HTTP {status}")
        if got:
            superseded_found.extend(got)
            failures.append(
                f"погашенный marker {marker!r} id={identifier!r} снова находит {got[:10]}"
            )
        if len(failures) >= 100:
            break
    if superseded_found:
        failures.append(
            f"погашенные version markers снова находятся: {superseded_found[:20]}"
        )
    return {
        "checked": len(rows),
        "marker_search_checked": marker_checked,
        "superseded_markers_checked": len(superseded),
        "superseded_found": superseded_found[:100],
        "failures": failures,
        "valid": not failures,
    }


def stress_topk(args: argparse.Namespace, *, seed: bool) -> dict[str, Any]:
    """Фиксирует неизменяемую top-K семью до нагрузки и после crash/recovery."""
    term = f"stresstopk{args.seed:x}"
    text_field, _, _ = stress_field_contract(args.corpus)
    documents = []
    for number in range(32):
        identifier = f"stress-topk-{args.profile}-{args.seed:x}-{number:02}"
        marker = f"topk_{args.seed:x}_{number:02}"
        document = stress_document(args.corpus, identifier, number + 1, marker)
        document[text_field] = " ".join([term] * (number + 1))
        documents.append(document)
    if seed:
        if args.engine == "svindb":
            reply = json_request(
                args.host,
                args.port,
                "POST",
                "/_put",
                {"documents": documents},
                args.timeout,
            )
            if reply.status != 200:
                return {
                    "valid": False,
                    "phase": "seed",
                    "http_status": reply.status,
                    "body": reply.data[:500].decode("utf-8", "replace"),
                }
            flush = json_request(args.host, args.port, "POST", "/_flush", {}, args.timeout)
            if flush.status not in (200, 202):
                return {"valid": False, "phase": "flush", "http_status": flush.status}
        else:
            chunks = []
            for document in documents:
                chunks.append(
                    json.dumps({"index": {"_id": document["id"]}}, separators=(",", ":"))
                )
                chunks.append(json.dumps(document, ensure_ascii=False, separators=(",", ":")))
            reply = one_request(
                args.host,
                args.port,
                "POST",
                f"/{args.index}/_bulk?refresh=wait_for",
                ("\n".join(chunks) + "\n").encode(),
                NDJSON_HEADERS,
                args.timeout,
            )
            if reply.status != 200 or reply.json().get("errors", True):
                return {
                    "valid": False,
                    "phase": "seed",
                    "http_status": reply.status,
                    "body": reply.data[:500].decode("utf-8", "replace"),
                }
    if args.engine == "svindb":
        reply = json_request(
            args.host,
            args.port,
            "POST",
            "/",
            {
                "k": 20,
                "words": [{"field": text_field, "terms": [term]}],
                "ids": True,
            },
            args.timeout,
        )
    else:
        reply = json_request(
            args.host,
            args.port,
            "POST",
            f"/{args.index}/_search",
            {
                "size": 20,
                "_source": False,
                "query": {"match": {text_field: term}},
            },
            args.timeout,
        )
    if reply.status != 200:
        return {"valid": False, "phase": "query", "http_status": reply.status}
    response = reply.json()
    hits = (
        response.get("hits", [])
        if args.engine == "svindb"
        else response.get("hits", {}).get("hits", [])
    )
    identifiers = [
        hit.get("id") if args.engine == "svindb" else hit.get("_id") for hit in hits
    ]
    return {
        "valid": len(hits) == 20 and len(set(identifiers)) == 20,
        "hits": len(hits),
        "ids": identifiers,
        "identity_sha256": identity_digest(response),
        "semantic_sha256": semantic_digest(response),
    }


def storage_health(args: argparse.Namespace) -> dict[str, Any]:
    """Проверяет порядок WAL и дожидается физически тихого состава."""
    if args.engine != "svindb":
        return {"valid": True, "status": "not_applicable"}
    deadline = time.monotonic() + args.restart_grace
    stable_since: float | None = None
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = get_json(args, "/_health")
        wal = last.get("wal", {})
        seq = int(wal.get("seq", 0))
        checkpoint = int(wal.get("checkpoint", 0))
        applied = int(wal.get("applied", 0))
        ordered = 0 <= checkpoint <= applied <= seq
        quiet = (
            int(last.get("buffered", 0)) == 0
            and not last.get("flush_in_progress", False)
            and int(last.get("compaction", {}).get("running_segments", 0)) == 0
        )
        if ordered and quiet:
            stable_since = stable_since or time.monotonic()
            if time.monotonic() - stable_since >= 2.0:
                return {
                    "valid": True,
                    "seq": seq,
                    "checkpoint": checkpoint,
                    "applied": applied,
                    "state": last,
                }
        else:
            stable_since = None
        time.sleep(0.1)
    wal = last.get("wal", {})
    return {
        "valid": False,
        "reason": "WAL нарушил порядок либо индекс не стал тихим",
        "seq": wal.get("seq"),
        "checkpoint": wal.get("checkpoint"),
        "applied": wal.get("applied"),
        "state": last,
    }


def run_full_check(args: argparse.Namespace) -> dict[str, Any]:
    """Запускает операторскую полную проверку того же каталога индекса."""
    if args.engine != "svindb":
        return {"valid": True, "status": "not_applicable"}
    if not args.binary or not args.index_path:
        return {
            "valid": False,
            "reason": "для SvinDB stress обязательны --binary и --index-path",
        }
    command = [str(Path(args.binary).resolve()), "check", str(Path(args.index_path).resolve()), "--full", "--json"]
    began = time.perf_counter()
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(120.0, args.timeout),
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"valid": False, "command": command, "error": repr(error)}
    try:
        decoded = json.loads(completed.stdout)
    except json.JSONDecodeError:
        decoded = None
    valid = (
        completed.returncode == 0
        and isinstance(decoded, dict)
        and decoded.get("ok") is True
        and decoded.get("broken") == 0
        and decoded.get("depth") == "full"
    )
    return {
        "valid": valid,
        "command": command,
        "returncode": completed.returncode,
        "seconds": time.perf_counter() - began,
        "result": decoded,
        "stderr": completed.stderr[-2000:],
    }


def resolve_uncertain(
    args: argparse.Namespace,
    oracle: Oracle,
    operations: list[tuple[str, str, int, str]],
) -> dict[str, Any]:
    """Разрешает потерянные ACK по фактической последней версии после restart."""
    resolved = 0
    not_applied = 0
    failures = []
    _, marker_field, revision_field = stress_field_contract(args.corpus)
    # Последняя попытка по marker достаточна: более поздняя подтверждённая
    # операция уже лежит в oracle с большей revision и не должна быть затёрта.
    for operation, identifier, revision, marker in operations:
        path = (
            f"/{args.index}/_doc/{urllib.parse.quote(identifier, safe='')}"
            if args.engine == "es"
            else f"/_doc/{urllib.parse.quote(identifier, safe='')}"
        )
        try:
            reply = one_request(args.host, args.port, "GET", path, timeout=args.timeout)
            if reply.status not in (200, 404):
                failures.append(f"{identifier}: HTTP {reply.status}")
                continue
            value = reply.json()
            found = reply.status == 200 and value.get("found", True)
            fields = value.get("_source", {}) if args.engine == "es" else value.get("fields", {})
            if operation == "delete" and not found:
                oracle.record(operation, identifier, revision, marker)
                resolved += 1
            elif (
                found
                and fields.get(marker_field) == marker
                and int(fields.get(revision_field, -1)) == revision
            ):
                oracle.record(operation, identifier, revision, marker)
                resolved += 1
            else:
                not_applied += 1
        except Exception as error:
            failures.append(f"{identifier}: {error!r}")
    oracle.flush()
    return {
        "ambiguous": len(operations),
        "resolved_as_applied": resolved,
        "resolved_as_not_applied_or_superseded": not_applied,
        "failures": failures,
    }


def restart_node(args: argparse.Namespace, sampler: Sampler) -> dict[str, Any]:
    if not args.restart_command or not args.pid:
        raise CampaignError("для restart нужны --pid и --restart-command")
    old_pid = args.pid
    used_signal = signal.SIGKILL if args.restart_mode == "kill-9" else signal.SIGTERM
    began = time.perf_counter()
    os.kill(old_pid, used_signal)
    deadline = time.monotonic() + args.restart_grace
    while time.monotonic() < deadline:
        try:
            os.kill(old_pid, 0)
        except ProcessLookupError:
            break
        # В campaign-shell исходный сервер — соседний child, поэтому только
        # shell может сделать wait(2). После SIGKILL он некоторое время виден
        # как zombie и `kill(pid, 0)` продолжает отвечать успехом, хотя порт и
        # ресурсы уже освобождены. Считать zombie живым означало бы ложный
        # 120-секундный timeout каждого контролируемого crash-test.
        try:
            stat = Path(f"/proc/{old_pid}/stat").read_text()
            state = stat[stat.rfind(")") + 2 :].split()[0]
            if state == "Z":
                break
        except (OSError, IndexError):
            break
        time.sleep(0.05)
    else:
        raise CampaignError(f"PID {old_pid} не завершился после {args.restart_mode}")
    log = Path(args.output).with_suffix(".restart.log").open("a", encoding="utf-8")
    process = subprocess.Popen(
        shlex.split(args.restart_command),
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    log.close()
    health_deadline = time.monotonic() + args.restart_grace
    last = ""
    while time.monotonic() < health_deadline:
        if process.poll() is not None:
            raise CampaignError(f"перезапущенный узел завершился кодом {process.returncode}")
        try:
            reply = one_request(args.host, args.port, "GET", "/_health", timeout=2)
            last = reply.data[:500].decode("utf-8", "replace")
            if reply.status == 200 and reply.json().get("ok"):
                args.pid = process.pid
                sampler.pid = process.pid
                return {
                    "mode": args.restart_mode,
                    "old_pid": old_pid,
                    "new_pid": process.pid,
                    "seconds": time.perf_counter() - began,
                    "health": reply.json(),
                }
        except Exception as error:
            last = repr(error)
        time.sleep(0.1)
    raise CampaignError(f"узел не восстановился за {args.restart_grace} с: {last}")


def command_stress(args: argparse.Namespace) -> None:
    output = Path(args.output)
    samples = output.with_suffix(".samples.jsonl")
    config = json.loads(Path(args.workload_file).read_text(encoding="utf-8"))
    profile = config["stress_profiles"][args.profile]
    raw_phases = profile.get("phases") if isinstance(profile, dict) else None
    phases = raw_phases or [{"name": args.profile, "weights": profile}]
    selectors = []
    for phase in phases:
        weights = phase["weights"]
        choices = list(weights)
        cumulative = []
        total = 0
        for operation in choices:
            total += weights[operation]
            cumulative.append(total)
        if total != 100:
            raise CampaignError(
                f"профиль {args.profile}/{phase['name']} задаёт {total}% вместо 100%"
            )
        selectors.append((phase["name"], choices, cumulative, total))
    oracle = Oracle(output.with_suffix(".oracle.sqlite3"))
    stop = threading.Event()
    restarting = threading.Event()
    tally = Counter()
    latencies: dict[str, list[float]] = {key: [] for key in ("read", "insert", "update", "delete")}
    latency_seen: Counter[str] = Counter()
    errors: list[dict[str, Any]] = []
    uncertain: list[tuple[str, str, int, str]] = []
    restarts: list[dict[str, Any]] = []
    lock = threading.Lock()
    text_field, _, _ = stress_field_contract(args.corpus)
    seed_probe = stress_seed_updates(args, oracle)
    if not seed_probe["valid"]:
        errors.append({"operation": "seed_update_pool", "detail": seed_probe})
    cas_probe = stress_cas_probe(args, oracle)
    if not cas_probe.get("valid", True):
        errors.append({"operation": "cas_probe", "detail": cas_probe})
    topk_before = stress_topk(args, seed=True)
    if not topk_before.get("valid"):
        errors.append({"operation": "topk_seed", "detail": topk_before})
    began = time.perf_counter()
    deadline = began + args.duration

    def record_latency(operation: str, elapsed_ms: float) -> None:
        """Держит ограниченную равномерную выборку без роста на длинном soak."""
        latency_seen[operation] += 1
        seen = latency_seen[operation]
        values = latencies[operation]
        capacity = 200_000
        if len(values) < capacity:
            values.append(elapsed_ms)
            return
        position = splitmix(args.seed, seen) % seen
        if position < capacity:
            values[position] = elapsed_ms

    def worker(number: int) -> None:
        rng = random.Random(args.seed + number)
        revisions: dict[int, int] = {}
        slots = max(1, args.oracle_ids // args.concurrency)
        insert_ordinal = 0
        delete_ordinal = 0
        last_insert: str | None = None
        while not stop.is_set() and time.perf_counter() < deadline:
            if restarting.is_set():
                # Уже отправленные запросы остаются честно неоднозначными, но
                # новые не должны превращать контролируемый restart в tight
                # loop из тысяч ConnectionRefused.
                time.sleep(0.01)
                continue
            elapsed = max(0.0, time.perf_counter() - began)
            phase_index = min(
                len(selectors) - 1,
                int(elapsed * len(selectors) / max(0.001, args.duration)),
            )
            phase_name, choices, cumulative, total = selectors[phase_index]
            roll = rng.randrange(total)
            operation = next(name for name, ceiling in zip(choices, cumulative) if roll < ceiling)
            slot = rng.randrange(slots)
            identifier = f"stress-{number:03}-{slot:06}"
            try:
                if operation == "read":
                    if rng.random() < 0.35:
                        if last_insert is not None and rng.random() < 0.5:
                            identifier = last_insert
                        else:
                            slot = rng.randrange(slots)
                            identifier = (
                                f"stress-update-{args.profile}-{number:03}-{slot:06}"
                            )
                        path = f"/{args.index}/_doc/{identifier}" if args.engine == "es" else f"/_doc/{identifier}"
                        reply, retries_429 = stress_retry_429(
                            lambda: one_request(
                                args.host,
                                args.port,
                                "GET",
                                path,
                                timeout=args.timeout,
                            )
                        )
                        accepted = reply.status in (200, 404)
                    else:
                        path = f"/{args.index}/_search" if args.engine == "es" else "/"
                        body = (
                            {
                                "size": 10,
                                "query": {"match": {text_field: "frequent_term"}},
                            }
                            if args.engine == "es"
                            else {
                                "k": 10,
                                "words": [
                                    {
                                        "field": text_field,
                                        "terms": ["frequent_term"],
                                    }
                                ],
                                "ids": True,
                            }
                        )
                        reply, retries_429 = stress_retry_429(
                            lambda: json_request(
                                args.host,
                                args.port,
                                "POST",
                                path,
                                body,
                                args.timeout,
                            )
                        )
                        accepted = reply.status == 200
                else:
                    if operation == "insert":
                        identifier = (
                            f"stress-insert-{args.profile}-{number:03}-{insert_ordinal:012}"
                        )
                        insert_ordinal += 1
                        revision = 1
                        marker = (
                            f"run_{args.seed:x}_insert_{number}_{insert_ordinal - 1}"
                        )
                    elif operation == "update":
                        slot = rng.randrange(slots)
                        identifier = (
                            f"stress-update-{args.profile}-{number:03}-{slot:06}"
                        )
                        revision = revisions.get(slot, 0) + 1
                        marker = f"run_{args.seed:x}_update_{number}_{slot}_{revision}"
                    else:
                        base = (number + delete_ordinal * args.concurrency) % args.base_documents
                        delete_ordinal += 1
                        identifier = (
                            f"mixed-{base:010}"
                            if args.corpus == "mixed"
                            else corpus_identifier("flat", base)
                        )
                        revision = 0
                        marker = (
                            f"run_{args.seed:x}_delete_{number}_{delete_ordinal - 1}"
                        )
                    with lock:
                        mutation_ordinal = tally["mutation_attempts"]
                        tally["mutation_attempts"] += 1
                    reply, retries_429 = stress_retry_429(
                        lambda: stress_mutation(
                            args,
                            operation,
                            identifier,
                            revision,
                            marker,
                            args.large_every > 0
                            and mutation_ordinal % args.large_every == 0,
                        )
                    )
                    accepted = 200 <= reply.status < 300 or (operation == "delete" and reply.status == 404)
                    if accepted:
                        if operation == "update":
                            revisions[slot] = revision
                        elif operation == "insert":
                            last_insert = identifier
                        oracle.record(operation, identifier, revision, marker)
                        with lock:
                            tally["mutations"] += 1
                with lock:
                    tally[operation] += 1
                    tally[f"phase/{phase_name}/{operation}"] += 1
                    tally[f"http_{reply.status}"] += 1
                    tally["retries_429"] += retries_429
                    record_latency(operation, reply.elapsed_ms)
                    if not accepted:
                        if restarting.is_set():
                            tally["intentional_restart_http"] += 1
                        else:
                            errors.append({"operation": operation, "status": reply.status, "body": reply.data[:500].decode("utf-8", "replace")})
            except Exception as error:
                with lock:
                    if restarting.is_set():
                        tally["intentional_restart_transport"] += 1
                        if operation != "read":
                            uncertain.append((operation, identifier, revision, marker))
                    else:
                        tally[f"{operation}_transport"] += 1
                        errors.append({"operation": operation, "transport": repr(error), "ambiguous": operation != "read"})
        close_persistent_connection()

    singleton_id = f"stress-singleton-{args.profile}-{args.seed:x}"

    def singleton_worker() -> None:
        revision = 0
        while not stop.is_set() and time.perf_counter() < deadline:
            if restarting.is_set():
                time.sleep(0.01)
                continue
            revision += 1
            marker = f"singleton_{args.seed:x}_{args.profile}_{revision}"
            try:
                reply, retries_429 = stress_retry_429(
                    lambda: stress_mutation(
                        args, "update", singleton_id, revision, marker, False
                    )
                )
                accepted = 200 <= reply.status < 300
                with lock:
                    tally["singleton_updates"] += int(accepted)
                    tally[f"singleton_http_{reply.status}"] += 1
                    tally["singleton_retries_429"] += retries_429
                    record_latency("update", reply.elapsed_ms)
                    if accepted:
                        oracle.record("update", singleton_id, revision, marker)
                    elif not restarting.is_set():
                        errors.append({"operation": "singleton", "status": reply.status})
            except Exception as error:
                with lock:
                    if restarting.is_set():
                        tally["singleton_restart_transport"] += 1
                        uncertain.append(("update", singleton_id, revision, marker))
                    else:
                        errors.append({"operation": "singleton", "transport": repr(error)})
            stop.wait(0.01)
        close_persistent_connection()

    workers = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(args.concurrency)]
    singleton_thread = threading.Thread(target=singleton_worker, daemon=True)
    bursts: list[dict[str, Any]] = []
    disconnect_probe: dict[str, Any] | None = None
    try:
        with Sampler(args, samples) as sampler:
            for worker_thread in workers:
                worker_thread.start()
            singleton_thread.start()
            next_burst = began + args.burst_interval if args.burst_interval else float("inf")
            restart_at = began + args.restart_at if args.restart_at else float("inf")
            disconnect_at = began + max(1.0, args.duration / 3)
            restarted = False
            disconnected = False
            burst_ordinal = 0
            while time.perf_counter() < deadline:
                if not restarted and time.perf_counter() >= restart_at:
                    oracle.flush()
                    restarting.set()
                    restarts.append(restart_node(args, sampler))
                    restarting.clear()
                    restarted = True
                if not disconnected and time.perf_counter() >= disconnect_at:
                    disconnect_probe = stress_disconnect_probe(args, oracle)
                    if not disconnect_probe.get("valid"):
                        errors.append({"operation": "disconnect_probe", "detail": disconnect_probe})
                    disconnected = True
                if time.perf_counter() >= next_burst:
                    burst = stress_burst(args, oracle, burst_ordinal)
                    bursts.append(burst)
                    tally["burst_documents"] += burst["documents"] if burst["valid"] else 0
                    tally["burst_retries_429"] += burst["retries_429"]
                    if not burst["valid"]:
                        errors.append({"operation": "burst", "detail": burst})
                    burst_ordinal += 1
                    next_burst += args.burst_interval
                time.sleep(0.25)
            stop.set()
            for worker_thread in workers:
                worker_thread.join(timeout=args.timeout)
            singleton_thread.join(timeout=args.timeout)
        if args.engine == "svindb":
            reply = json_request(args.host, args.port, "POST", "/_flush", {}, args.timeout)
            if reply.status not in (200, 202):
                errors.append({"operation": "final_flush", "status": reply.status})
        else:
            one_request(args.host, args.port, "POST", f"/{args.index}/_refresh", b"", timeout=args.timeout)
        ambiguity = resolve_uncertain(args, oracle, uncertain)
        if ambiguity["failures"]:
            errors.extend({"operation": "resolve_restart", "detail": failure} for failure in ambiguity["failures"])
        verification = verify_oracle(args, oracle)
        health = storage_health(args)
        if not health["valid"]:
            errors.append({"operation": "storage_health", "detail": health})
        topk_after = stress_topk(args, seed=False)
        topk_same = (
            topk_before.get("valid")
            and topk_after.get("valid")
            # Общий N корпуса меняется из-за законных insert/delete, поэтому
            # BM25 score может измениться без ошибки. Состав и порядок top-K —
            # контракт; именно их и сравниваем после аварийного подъёма.
            and topk_before.get("ids") == topk_after.get("ids")
        )
        topk_verification = {
            "valid": bool(topk_same),
            "compared_after_restart": bool(restarts),
            "before": topk_before,
            "after": topk_after,
        }
        if not topk_same:
            errors.append({"operation": "topk_after_restart", "detail": topk_verification})
        full_check = run_full_check(args)
        if not full_check["valid"]:
            errors.append({"operation": "check_full", "detail": full_check})
    finally:
        stop.set()
        oracle.close()
    seconds = time.perf_counter() - began
    result = {
        "format": 1,
        "candidate": args.candidate,
        "engine": args.engine,
        "profile": args.profile,
        "phase_plan": [
            {
                "name": phase["name"],
                "weights": phase["weights"],
                "planned_seconds": args.duration / len(phases),
            }
            for phase in phases
        ],
        "duration_seconds": seconds,
        "concurrency": args.concurrency,
        "operations": dict(tally),
        "ops_per_second": sum(tally[k] for k in ("read", "insert", "update", "delete")) / seconds,
        "latency": {key: latency_summary(values) for key, values in latencies.items()},
        "latency_observations": dict(latency_seen),
        "latency_sample_sizes": {key: len(values) for key, values in latencies.items()},
        "errors": errors[:100],
        "error_count": len(errors),
        "oracle": verification,
        "storage_health": health,
        "topk_after_restart": topk_verification,
        "full_check": full_check,
        "restart_ambiguity": ambiguity,
        "restarts": restarts,
        "cas_probe": cas_probe,
        "seed_probe": seed_probe,
        "disconnect_probe": disconnect_probe,
        "bursts": bursts,
        "resources": summarize_samples(samples),
        "valid": (
            not errors
            and verification["valid"]
            and health["valid"]
            and topk_verification["valid"]
            and full_check["valid"]
        ),
    }
    atomic_json(output, result)
    print(json.dumps({"output": str(output), "valid": result["valid"], "operations": dict(tally)}, ensure_ascii=False))
    if not result["valid"]:
        raise CampaignError("stress завершён с transport/correctness нарушениями")


def command_boundary(args: argparse.Namespace) -> None:
    output = Path(args.output)
    results = []
    before = process_sample(args.pid) if args.pid else {}

    def case(name: str, method: str, path: str, body: bytes, headers: dict[str, str], allowed: set[int]) -> None:
        try:
            reply = one_request(args.host, args.port, method, path, body, headers, args.timeout)
            results.append({"name": name, "status": reply.status, "allowed": sorted(allowed), "bounded": reply.status in allowed, "bytes": len(body), "reply": reply.data[:300].decode("utf-8", "replace")})
        except Exception as error:
            results.append({"name": name, "transport": repr(error), "bounded": False, "bytes": len(body)})

    cases = [
        (
            "wrong-type",
            {"documents": [{"id": "boundary-wrong", "views": "NaN", "text": "x"}]},
            {400},
        ),
        (
            "empty-values",
            {"documents": [{"id": "boundary-empty", "text": "", "channel": None}]},
            {200, 400},
        ),
        (
            "huge-slots",
            {
                "documents": [
                    {
                        "id": "boundary-slots",
                        "text": "x",
                        "reactions": [
                            {"emoji": "up", "count": i} for i in range(4096)
                        ],
                    }
                ]
            },
            {200, 400, 413, 429},
        ),
    ]
    for name, value, allowed in cases:
        case(
            name,
            "POST",
            "/_bulk",
            json.dumps(value, separators=(",", ":")).encode(),
            JSON_HEADERS,
            allowed,
        )
    case("invalid-json", "POST", "/_bulk", b'{"documents":[', JSON_HEADERS, {400})
    case("invalid-utf8", "POST", "/_bulk", b'{"documents":[{"id":"x","text":"\xff"}]}', JSON_HEADERS, {400})
    deep = (
        b'{"documents":[{"id":"boundary-deep","text":"x","deep":'
        + b"[" * 256
        + b"0"
        + b"]" * 256
        + b"}]}"
    )
    case("deep-json", "POST", "/_bulk", deep, JSON_HEADERS, {400, 413})
    for mib in (1, 4, 8, 16, 31, 33):
        body = b'{"documents":[{"id":"boundary-size-' + str(mib).encode() + b'","text":"' + b"x" * (mib << 20) + b'"}]}'
        case(f"body-{mib}MiB", "POST", "/_bulk", body, JSON_HEADERS, {200} if mib <= 31 else {400, 413})
    # Если много query-клауз ссылаются только на уже погашенные
    # keyword-версии, поиск не вправе возвращать соседние живые документы. Это
    # отдельная correctness-проверка oracle, а не часть performance latency.
    old_documents = [
        {
            "id": f"boundary-dead-clause-{number:02}",
            "text": "control",
            "status": f"boundary_old_{number:02}",
        }
        for number in range(22)
    ]
    new_documents = [
        {
            "id": f"boundary-dead-clause-{number:02}",
            "text": "control",
            "status": f"boundary_new_{number:02}",
        }
        for number in range(22)
    ]
    setup = json_request(
        args.host, args.port, "POST", "/_bulk", {"documents": old_documents}, args.timeout
    )
    replace = json_request(
        args.host, args.port, "POST", "/_put", {"documents": new_documents}, args.timeout
    )
    json_request(args.host, args.port, "POST", "/_flush", {}, args.timeout)
    dead_query = json_request(
        args.host,
        args.port,
        "POST",
        "/",
        {
            "k": 100,
            "words": [
                {"field": "status", "terms": [f"boundary_old_{number:02}"]}
                for number in range(22)
            ],
            "ids": True,
        },
        args.timeout,
    )
    dead_hits = dead_query.json().get("hits", []) if dead_query.status == 200 else []
    results.append(
        {
            "name": "many-dead-keyword-clauses",
            "setup_status": setup.status,
            "replace_status": replace.status,
            "status": dead_query.status,
            "hits": dead_hits[:100],
            "bounded": (
                setup.status == 200
                and replace.status == 200
                and dead_query.status == 200
                and not dead_hits
            ),
        }
    )
    # Четыре почти предельных тела одновременно дают около 124 МиБ inflight.
    # Каждое имеет свой id, поэтому успешные ответы тоже однозначны.
    def inflight(number: int) -> dict[str, Any]:
        body = (
            b'{"documents":[{"id":"boundary-inflight-'
            + str(number).encode()
            + b'","text":"'
            + b"y" * (31 << 20)
            + b'"}]}'
        )
        try:
            reply = one_request(args.host, args.port, "POST", "/_bulk", body, JSON_HEADERS, args.timeout)
            return {
                "name": f"inflight-31MiB-{number}",
                "status": reply.status,
                "bounded": reply.status in (200, 400, 413, 429),
                "bytes": len(body),
            }
        except Exception as error:
            return {
                "name": f"inflight-31MiB-{number}",
                "transport": repr(error),
                "bounded": False,
                "bytes": len(body),
            }

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results.extend(pool.map(inflight, range(4)))
    # Оборванное тело проверяется сырым сокетом: клиент не ждёт ответа и
    # отдельно убеждается, что после него узел обслуживает честный запрос.
    try:
        sock = socket.create_connection((args.host, args.port), timeout=args.timeout)
        sock.sendall(b"POST /_bulk HTTP/1.1\r\nHost: x\r\nContent-Length: 1000000\r\n\r\n{\"documents\":[")
        sock.close()
        healthy = one_request(args.host, args.port, "GET", "/_health", timeout=10)
        results.append({"name": "truncated-request", "bounded": healthy.status == 200, "health_status": healthy.status})
    except Exception as error:
        results.append({"name": "truncated-request", "bounded": False, "transport": repr(error)})
    # Большие успешные документы должны покинуть mutable buffer до снимка:
    # иначе их законная память была бы приписана последующим 100k malformed
    # requests. Одновременно это проверяет, что near-limit body не оставляет
    # узел навсегда в flush/compaction debt.
    settled = False
    settle_status = None
    settle_health: dict[str, Any] = {}
    try:
        flushed = json_request(args.host, args.port, "POST", "/_flush", {}, args.timeout)
        settle_status = flushed.status
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            reply = json_request(args.host, args.port, "GET", "/_health", timeout=10)
            settle_health = reply.json() if reply.status == 200 else {}
            compaction = settle_health.get("compaction", {})
            settled = (
                reply.status == 200
                and settle_health.get("buffered") == 0
                and not settle_health.get("flush_in_progress")
                and compaction.get("pending_segments", 0) == 0
                and compaction.get("running_segments", 0) == 0
            )
            if settled:
                break
            time.sleep(0.1)
    except Exception as error:
        settle_health = {"transport": repr(error)}
    results.append(
        {
            "name": "large-bodies-settled",
            "status": settle_status,
            "health": settle_health,
            "bounded": settled and settle_status in (200, 202),
        }
    )
    after_limits = process_sample(args.pid) if args.pid else {}
    # Остаток 100k-набора — дешёвые оборванные JSON requests; они не пишут
    # данные и дают проверить утечку памяти/FD на длинной серии. HTTP 200 здесь
    # был бы именно correctness-дефектом, а не допустимым bounded-ответом.
    repeated = Counter()
    repeated_transport = []
    for i in range(max(0, args.requests - len(results))):
        try:
            reply = one_request(
                args.host,
                args.port,
                "POST",
                "/_bulk",
                b'{"documents":[',
                JSON_HEADERS,
                args.timeout,
            )
            repeated[reply.status] += 1
            if reply.status not in (400, 429):
                results.append({"name": f"repeat-{i}", "bounded": False, "status": reply.status})
        except Exception as error:
            repeated_transport.append(repr(error))
            results.append({"name": f"repeat-{i}", "bounded": False, "transport": repr(error)})
            if len(repeated_transport) >= 20:
                break
    after = process_sample(args.pid) if args.pid else {}
    rss_growth = after.get("status.VmRSS", 0) - before.get("status.VmRSS", 0)
    fd_growth = after.get("fds", 0) - before.get("fds", 0)
    resource_contract = boundary_resource_contract(before, after_limits, after)
    bounded_resources = resource_contract["valid"]
    result = {
        "format": 1,
        "requests": args.requests,
        "cases": results,
        "before": before,
        "after_limits": after_limits,
        "after": after,
        "repeated_statuses": dict(repeated),
        "repeated_transport_errors": repeated_transport,
        "rss_growth_bytes": rss_growth,
        "fd_growth": fd_growth,
        "resource_contract": resource_contract,
        "bounded_resources": bounded_resources,
        "valid": (
            all(case.get("bounded", False) for case in results)
            and not repeated_transport
            and bounded_resources
        ),
    }
    atomic_json(output, result)
    print(json.dumps({"output": str(output), "valid": result["valid"], "recorded_cases": len(results)}, ensure_ascii=False))
    if not result["valid"]:
        raise CampaignError("boundary-набор выявил неограниченный или неверный отказ")


def es_mapping(corpus: str) -> dict[str, Any]:
    # Основная кампания сравнивает не дефолтную компрессию ES с нашим
    # форматом, а явно зафиксированный production-профиль. `best_compression`
    # обязан быть задан при создании индекса: смена настройки после загрузки
    # не перекодирует существующие сегменты и дала бы неоднородный baseline.
    base = {"settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "-1",
        "index.codec": "best_compression",
        # ES 9.5.3 по умолчанию исключает dense_vector из _source. Для
        # честного round-trip с lossless source SvinDB это должно быть явно
        # отключено, а источник должен храниться на диске.
        "index.mapping.source.mode": "stored",
        "index.mapping.exclude_source_vectors": False,
    }}
    if corpus == "flat":
        base["mappings"] = {"properties": {"message": {"type": "text", "index_options": "positions"}, "views": {"type": "long"}, "ts": {"type": "date", "format": "epoch_second"}, "channel": {"type": "keyword"}}}
    else:
        base["mappings"] = {"properties": {
            "text": {"type": "text", "index_options": "positions"}, "title": {"type": "text", "index_options": "positions"},
            "channel": {"type": "keyword"}, "status": {"type": "keyword"}, "views": {"type": "long"}, "sequence": {"type": "integer"},
            "published": {"type": "date"}, "flag": {"type": "boolean"}, "score": {"type": "double"}, "ratio": {"type": "float"},
            # Mixed-корпус намеренно содержит значения вне географического
            # диапазона для проверки строгого SvinDB round-trip. Эти точки не
            # участвуют в A/B-запросах; ES должен принять документ целиком,
            # иначе corpus checkpoint незаметно теряет записи.
            "place": {"type": "geo_point", "ignore_malformed": True}, "author": {"properties": {"name": {"type": "keyword"}}},
            "reactions": {"type": "nested", "properties": {"emoji": {"type": "keyword"}, "count": {"type": "integer"}}},
            "payload": {"type": "binary", "doc_values": False}, "raw": {"type": "keyword", "index": False},
            "embedding": {"type": "dense_vector", "dims": 384, "index": True, "similarity": "l2_norm"},
            "embedding_sparse": {"type": "sparse_vector"},
        }}
    return base


def command_prepare_es(args: argparse.Namespace) -> None:
    # Имя должно быть уникальным для run_id; удалять существующее runner не
    # имеет права, поэтому коллизия — явный отказ.
    exists = one_request(args.host, args.port, "HEAD", f"/{args.index}", timeout=args.timeout)
    if exists.status == 200:
        raise CampaignError(f"ES index {args.index!r} уже существует; выберите уникальное имя")
    body = es_mapping(args.corpus)
    body["settings"]["index.translog.durability"] = args.durability
    reply = json_request(args.host, args.port, "PUT", f"/{args.index}", body, args.timeout)
    if reply.status >= 300 or not reply.json().get("acknowledged"):
        raise CampaignError(f"создание ES index: HTTP {reply.status}: {reply.data[:1000]!r}")
    settings_reply = one_request(args.host, args.port, "GET", f"/{args.index}/_settings", timeout=args.timeout)
    if settings_reply.status >= 300:
        raise CampaignError(
            f"проверка настроек ES {args.index}: HTTP {settings_reply.status}: {settings_reply.data[:1000]!r}"
        )
    settings_value = settings_reply.json().get(args.index, {}).get("settings", {}).get("index", {})
    codec_value = settings_value.get("codec")
    codec = codec_value.get("name") if isinstance(codec_value, dict) else codec_value
    durability = settings_value.get("translog", {}).get("durability")
    mapping_value = settings_value.get("mapping", {})
    source_value = mapping_value.get("source", {}) if isinstance(mapping_value, dict) else {}
    source_mode = source_value.get("mode") if isinstance(source_value, dict) else None
    exclude_source_vectors = mapping_value.get("exclude_source_vectors") if isinstance(mapping_value, dict) else None
    if isinstance(exclude_source_vectors, str):
        exclude_source_vectors = {"true": True, "false": False}.get(exclude_source_vectors.lower())
    if (
        codec != "best_compression"
        or durability != args.durability
        or source_mode != "stored"
        or exclude_source_vectors is not False
    ):
        raise CampaignError(
            f"ES {args.index}: readback codec={codec!r}, durability={durability!r}, "
            f"source.mode={source_mode!r}, exclude_source_vectors={exclude_source_vectors!r}; "
            f"ожидались best_compression, {args.durability}, stored и false"
        )
    atomic_json(Path(args.output), {
        "index": args.index,
        "corpus": args.corpus,
        "durability": args.durability,
        "codec": "best_compression",
        "source_mode": "stored",
        "exclude_source_vectors": False,
        "request": body,
        "response": reply.json(),
        "settings_readback": settings_reply.json(),
    })


def command_report(args: argparse.Namespace) -> None:
    root = Path(args.results).resolve()
    files = sorted(root.rglob("*.json"))
    rows = []
    query_rows = []
    stress_rows = []
    boundary_rows = []
    violations = []
    for path in files:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        relative = str(path.relative_to(root))
        if "ready" in value and "documents" in value:
            rows.append({
                "file": relative,
                "candidate": value.get("candidate"),
                "engine": value.get("engine"),
                "corpus": value.get("corpus"),
                "durability": value.get("durability"),
                "protocol": value.get("protocol"),
                "ack_dps": value.get("ack", {}).get("docs_per_second"),
                "ready_dps": value.get("ready", {}).get("docs_per_second"),
                "settled_dps": value.get("settled", {}).get("docs_per_second"),
                "p99_ms": value.get("ack", {}).get("bulk_latency", {}).get("p99_ms"),
                "p999_ms": value.get("ack", {}).get("bulk_latency", {}).get("p99_9_ms"),
                "tokens_per_second": value.get("tokens_per_second_ready"),
                "rss": value.get("resources", {}).get("rss_peak"),
                "pss": value.get("resources", {}).get("pss_peak"),
                "bytes_per_doc": value.get("bytes_per_doc"),
                "write_amplification": value.get("write_amplification"),
                "valid": value.get("valid"),
            })
        if isinstance(value.get("families"), list):
            for family in value["families"]:
                latency = family.get("latency", {})
                query_rows.append({
                    "file": relative,
                    "candidate": value.get("candidate"),
                    "engine": value.get("engine"),
                    "corpus": value.get("corpus"),
                    "concurrency": value.get("concurrency"),
                    "family": family.get("family"),
                    "status": family.get("status"),
                    "qps": family.get("qps"),
                    "p50_ms": latency.get("p50_ms"),
                    "p95_ms": latency.get("p95_ms"),
                    "p99_ms": latency.get("p99_ms"),
                    "p999_ms": latency.get("p99_9_ms"),
                    "max_ms": latency.get("max_ms"),
                    "semantic_sha256": family.get("semantic_sha256"),
                    "identity_sha256": family.get("identity_sha256"),
                })
        if "profile" in value and "oracle" in value:
            stress_rows.append({
                "file": relative,
                "candidate": value.get("candidate"),
                "engine": value.get("engine"),
                "profile": value.get("profile"),
                "concurrency": value.get("concurrency"),
                "ops_per_second": value.get("ops_per_second"),
                "errors": value.get("error_count"),
                "oracle_valid": value.get("oracle", {}).get("valid"),
                "restarts": len(value.get("restarts", [])),
                "valid": value.get("valid"),
            })
        if "bounded_resources" in value and "cases" in value:
            boundary_rows.append({
                "file": relative,
                "requests": value.get("requests"),
                "rss_growth": value.get("rss_growth_bytes"),
                "fd_growth": value.get("fd_growth"),
                "valid": value.get("valid"),
            })
        if value.get("valid") is False:
            violations.append(relative)
    lines = [
        "# Измерительная кампания SvinDB",
        "",
        f"Сформировано: `{now_iso()}`. Источник результатов: `{root}`.",
        "",
        "## Приём",
        "",
        "| Кандидат | Движок | Протокол | Корпус | Durability | ACK док/с | Ready док/с | Settled док/с | токенов/с | bulk p99 | bulk p99.9 | Peak PSS | Б/док | WA |",
        "|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        number = lambda key: "—" if row[key] is None else f"{row[key]:,.2f}"
        lines.append(
            f"| {row['candidate']} | {row['engine']} | {row['protocol']} | {row['corpus']} | {row['durability']} | "
            f"{number('ack_dps')} | {number('ready_dps')} | {number('settled_dps')} | "
            f"{number('tokens_per_second')} | {number('p99_ms')} | {number('p999_ms')} | "
            f"{number('pss')} | {number('bytes_per_doc')} | {number('write_amplification')} |"
        )
    lines.extend([
        "",
        "## Поиск и чтение",
        "",
        "| Кандидат | Движок | Корпус | C | Семья | Статус | QPS | p50 | p95 | p99 | p99.9 | max |",
        "|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|",
    ])
    for row in query_rows:
        def qnumber(key: str) -> str:
            return "—" if row[key] is None else f"{row[key]:,.3f}"
        lines.append(
            f"| {row['candidate']} | {row['engine']} | {row['corpus']} | {row['concurrency']} | "
            f"{row['family']} | {row['status']} | {qnumber('qps')} | {qnumber('p50_ms')} | "
            f"{qnumber('p95_ms')} | {qnumber('p99_ms')} | {qnumber('p999_ms')} | {qnumber('max_ms')} |"
        )
    lines.extend([
        "",
        "## Смешанная нагрузка",
        "",
        "| Кандидат | Движок | Профиль | C | ops/s | Ошибки | Oracle | Restarts | Валиден |",
        "|---|---|---|---:|---:|---:|---|---:|---|",
    ])
    for row in stress_rows:
        ops = "—" if row["ops_per_second"] is None else f"{row['ops_per_second']:,.2f}"
        lines.append(
            f"| {row['candidate']} | {row['engine']} | {row['profile']} | {row['concurrency']} | "
            f"{ops} | {row['errors']} | {row['oracle_valid']} | {row['restarts']} | {row['valid']} |"
        )
    lines.extend(["", "## Boundary-набор", ""])
    if boundary_rows:
        lines.extend([
            "| Файл | Запросов | Рост RSS | Рост FD | Валиден |",
            "|---|---:|---:|---:|---|",
        ])
        for row in boundary_rows:
            lines.append(
                f"| {row['file']} | {row['requests']} | {row['rss_growth']} | "
                f"{row['fd_growth']} | {row['valid']} |"
            )
    else:
        lines.append("Boundary-результатов пока нет.")
    lines.extend(["", "## Нарушения", ""])
    if violations:
        lines.extend(f"- `{item}` помечен как невалидный." for item in violations)
    else:
        lines.append("Невалидных результатов в каталоге не обнаружено.")
    lines.extend(
        [
            "",
            "## Правило интерпретации",
            "",
            "Строка включается в итоговое сравнение только вместе с manifest, пройденным correctness gate и нулём transport/oracle нарушений. ACK, ready и settled не взаимозаменяемы; порог 100/120 тыс. док/с применяется к median `ready` трёх Flat-10M прогонов.",
            "",
        ]
    )
    Path(args.output).write_text("\n".join(lines), encoding="utf-8")
    atomic_json(Path(args.output).with_suffix(".json"), {
        "ingest": rows,
        "query": query_rows,
        "stress": stress_rows,
        "boundary": boundary_rows,
        "violations": violations,
        "source_files": len(files),
    })
    print(json.dumps({"report": args.output, "rows": len(rows), "violations": len(violations)}, ensure_ascii=False))


def add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--corpus", required=True, choices=("flat", "mixed", "boundary"))
    parser.add_argument("--seed", type=int, default=6497668530671045374)
    parser.add_argument("--duration", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--durability", default="request")
    parser.add_argument("--memory-limit", default="unrestricted")
    parser.add_argument(
        "--cpu-limit",
        default="unrestricted",
        help="фактическая cgroup v2 quota в логических CPU; строка без cgroup не принимается",
    )
    parser.add_argument("--output", required=True)


def add_http(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--pid", type=int)
    parser.add_argument("--sample-interval", type=float, default=0.5)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    manifest = commands.add_parser("manifest", help="зафиксировать бинарник, входы и машину")
    add_common(manifest)
    manifest.add_argument("--run-id", required=True)
    manifest.add_argument("--revision", default=os.environ.get("SVINDB_CAMPAIGN_REVISION"))
    manifest.add_argument("--source-state", default=os.environ.get("SVINDB_CAMPAIGN_SOURCE_STATE"))
    manifest.add_argument("--feature-flags", default=os.environ.get("SVINDB_CAMPAIGN_FEATURES"))
    manifest.add_argument("--source", default=str(ROOT))
    manifest.add_argument("--binary")
    manifest.add_argument("--input", action="append", default=[])
    manifest.add_argument("--workload", default=str(DEFAULT_WORKLOAD))
    manifest.set_defaults(func=command_manifest)

    validate = commands.add_parser("validate-corpus", help="проверить JSON, состав и уникальность корпуса")
    validate.add_argument("--corpus-path", required=True)
    validate.add_argument("--kind", required=True, choices=("flat", "mixed"))
    validate.add_argument("--documents", type=int, default=10_000_000)
    validate.add_argument("--id-template", help="например msg-{i}; по умолчанию <kind>-<i:010>")
    validate.add_argument("--skip-marker-proof", action="store_true")
    validate.add_argument("--hash-body-uniqueness", action="store_true")
    validate.add_argument("--output", required=True)
    validate.set_defaults(func=command_validate_corpus)

    normalize = commands.add_parser(
        "normalize-flat", help="сохранить старый Zipf/Heaps Flat, устранив повторы тел"
    )
    normalize.add_argument("--source", required=True)
    normalize.add_argument("--destination", required=True)
    normalize.add_argument("--documents", type=int, default=10_000_000)
    normalize.add_argument("--parts", type=int, default=30)
    normalize.add_argument("--id-template", default="msg-{i}")
    normalize.add_argument("--output", required=True)
    normalize.set_defaults(func=command_normalize_flat)

    load = commands.add_parser("load", help="измерить ACK, ready и settled")
    add_common(load)
    add_http(load)
    load.add_argument("--engine", required=True, choices=("svindb", "es"))
    load.add_argument(
        "--protocol",
        choices=("auto", "native", "ndjson"),
        default="auto",
        help="формат bulk; NDJSON поддерживается как ES, так и SvinDB",
    )
    load.add_argument("--corpus-path", required=True)
    load.add_argument("--documents", type=int, default=10_000_000)
    load.add_argument("--batch-documents", type=int, default=5000)
    load.add_argument("--batch-bytes", type=int, default=16 << 20)
    load.add_argument("--retry-429", type=int, default=8)
    load.add_argument("--index", default="bench")
    load.add_argument("--index-path")
    load.add_argument("--settle-timeout", type=int, default=7200)
    load.add_argument("--settle-stable", type=int, default=60)
    # Умолчание равно `dead_fraction` узла: сведённым считается состав, который
    # сам узел не считает требующим уборки. Другое значение здесь означало бы,
    # что стенд и узел спорят о том, что такое «прибрано».
    load.add_argument(
        "--settle-max-dead",
        type=float,
        default=0.20,
        help="предельная доля погашенных документов для settled; тишины планировщика мало",
    )
    load.set_defaults(func=command_load)

    query = commands.add_parser("query", help="прогнать матрицу чтения и поиска")
    add_common(query)
    add_http(query)
    query.add_argument("--engine", required=True, choices=("svindb", "es"))
    query.add_argument("--index", default="bench")
    query.add_argument("--documents", type=int, default=10_000_000)
    query.add_argument("--workload-file", default=str(DEFAULT_WORKLOAD))
    query.add_argument("--repetitions", type=int, default=1000)
    query.add_argument("--warmup", type=int, default=10)
    query.add_argument(
        "--capture-first",
        action="store_true",
        help="сохранить самый первый запрос до warmup/reference; для process-cold запускайте отдельный процесс на семью",
    )
    query.add_argument(
        "--family",
        action="append",
        default=[],
        help="подстрока имени семейства; =имя выбирает только точное имя",
    )
    query.add_argument(
        "--mixed-requests",
        type=int,
        default=0,
        help="перемешать выбранные stateless-семьи в одном профиле этого размера",
    )
    query.set_defaults(func=command_query)

    stress = commands.add_parser("stress", help="смешанная нагрузка с независимым durable oracle")
    add_common(stress)
    add_http(stress)
    stress.add_argument("--engine", required=True, choices=("svindb", "es"))
    stress.add_argument("--index", default="bench")
    stress.add_argument(
        "--profile",
        required=True,
        choices=("read-heavy", "balanced", "update-heavy", "delete-churn", "combined", "soak"),
    )
    stress.add_argument("--workload-file", default=str(DEFAULT_WORKLOAD))
    stress.add_argument("--oracle-ids", type=int, default=100_000)
    stress.add_argument(
        "--base-documents",
        type=int,
        default=10_000_000,
        help="размер исходного корпуса, из которого delete lane берёт неповторяющиеся ID",
    )
    stress.add_argument("--large-every", type=int, default=10_000)
    stress.add_argument("--burst-interval", type=int, default=900)
    stress.add_argument("--burst-documents", type=int, default=5000)
    stress.add_argument("--restart-at", type=int, default=0, help="секунда контролируемого restart; 0 выключает")
    stress.add_argument("--restart-mode", choices=("graceful", "kill-9"), default="kill-9")
    stress.add_argument("--restart-command", help="команда нового процесса без shell-операторов")
    stress.add_argument("--restart-grace", type=int, default=120)
    stress.add_argument(
        "--binary",
        help="тот же svindb ELF для обязательного check --full --json",
    )
    stress.add_argument(
        "--index-path",
        help="каталог текущего индекса для обязательного check --full",
    )
    stress.set_defaults(func=command_stress)

    boundary = commands.add_parser("boundary", help="100k boundary и malformed запросов")
    boundary.add_argument("--host", default="127.0.0.1")
    boundary.add_argument("--port", type=int, required=True)
    boundary.add_argument("--pid", type=int)
    boundary.add_argument("--timeout", type=float, default=120)
    boundary.add_argument("--requests", type=int, default=100_000)
    boundary.add_argument("--output", required=True)
    boundary.set_defaults(func=command_boundary)

    prepare = commands.add_parser("prepare-es", help="создать новый ES index без удаления старых")
    prepare.add_argument("--host", default="127.0.0.1")
    prepare.add_argument("--port", type=int, required=True)
    prepare.add_argument("--timeout", type=float, default=120)
    prepare.add_argument("--index", required=True)
    prepare.add_argument("--corpus", required=True, choices=("flat", "mixed"))
    prepare.add_argument("--durability", required=True, choices=("request", "async"))
    prepare.add_argument("--output", required=True)
    prepare.set_defaults(func=command_prepare_es)

    report = commands.add_parser("report", help="собрать компактный Markdown и JSON")
    report.add_argument("--results", required=True)
    report.add_argument("--output", required=True)
    report.set_defaults(func=command_report)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    started_at = now_iso()
    started_wall = time.time()
    status = "passed"
    failure: str | None = None
    manifest_failure: Exception | None = None
    try:
        if args.command in {"load", "query", "stress"}:
            args.resource_contract = validate_resource_contract(args)
        args.func(args)
    except CampaignError as error:
        status = "failed"
        failure = str(error)
        print(f"ОШИБКА КАМПАНИИ: {error}", file=sys.stderr)
    except Exception as error:
        status = "crashed"
        failure = repr(error)
        raise
    finally:
        try:
            automatic_run_manifest(
                args,
                started_at=started_at,
                started_wall=started_wall,
                status=status,
                error=failure,
            )
        except Exception as manifest_error:
            # Потеря provenance делает прогон неприемлемым, даже если сама
            # нагрузка закончилась. При уже диагностированном отказе сохраняем
            # исходную причину, но обязательно печатаем второй дефект.
            print(
                f"ОШИБКА MANIFEST: {manifest_error!r}",
                file=sys.stderr,
            )
            manifest_failure = manifest_error
    if manifest_failure is not None and status == "passed":
        return 3
    return 0 if status == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
