#!/usr/bin/env python3
"""Последовательно прогоняет ingest-матрицу под настоящими cgroup v2 лимитами."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parent))
import benchmark_campaign as campaign  # noqa: E402


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def safe_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-.") or "cell"


def server_environment(values: list[str]) -> dict[str, str]:
    """Разбирает воспроизводимое окружение узла и запрещает неоднозначные имена."""
    result: dict[str, str] = {}
    for raw in values:
        name, separator, value = raw.partition("=")
        if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise campaign.CampaignError(
                f"--server-env требует NAME=VALUE с допустимым именем, получено {raw!r}"
            )
        if name in result:
            raise campaign.CampaignError(f"--server-env повторяет переменную {name}")
        result[name] = value
    return result


def run(command: list[str], *, timeout: float = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def require_host() -> None:
    if not sys.platform.startswith("linux"):
        raise campaign.CampaignError("resource matrix требует Linux")
    if not Path("/sys/fs/cgroup/cgroup.controllers").exists():
        raise campaign.CampaignError("resource matrix требует cgroup v2")
    probe = run(["systemd-run", "--user", "--wait", "--collect", "--quiet", "/usr/bin/true"])
    if probe.returncode != 0:
        raise campaign.CampaignError(
            f"user systemd-run недоступен: {(probe.stderr or probe.stdout).strip()}"
        )


def wait_main_pid(unit: str, timeout: float) -> int:
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        shown = run(
            ["systemctl", "--user", "show", unit, "--property=MainPID", "--value"],
            timeout=10,
        )
        last = (shown.stdout or shown.stderr).strip()
        if shown.returncode == 0:
            try:
                pid = int(last)
            except ValueError:
                pid = 0
            if pid > 0 and Path(f"/proc/{pid}").exists():
                return pid
        time.sleep(0.05)
    raise campaign.CampaignError(f"unit {unit} не получил MainPID за {timeout} с: {last}")


def wait_health(host: str, port: int, timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        try:
            reply = campaign.one_request(host, port, "GET", "/_health", timeout=2)
            last = reply.data[:500].decode("utf-8", "replace")
            if reply.status == 200 and reply.json().get("ok") is True:
                return reply.json()
        except Exception as error:
            last = repr(error)
        time.sleep(0.1)
    raise campaign.CampaignError(f"узел не стал healthy за {timeout} с: {last}")


def stop_unit(unit: str, log: Path) -> None:
    run(["systemctl", "--user", "stop", unit], timeout=120)
    journal = run(
        ["journalctl", "--user", "--unit", unit, "--no-pager", "--output=short-iso"],
        timeout=120,
    )
    log.write_text(journal.stdout + journal.stderr, encoding="utf-8")


def cell(
    args: argparse.Namespace,
    memory: str,
    cpu: str,
    ordinal: int,
) -> dict[str, Any]:
    label = safe_name(f"m-{memory}-c-{cpu}")
    unit = safe_name(f"svindb-matrix-{os.getpid()}-{ordinal}-{label}") + ".service"
    root = Path(args.output_root).resolve()
    index = root / "indexes" / label
    result = root / "results" / f"{label}.json"
    log = root / "logs" / f"{label}.log"
    if index.exists() or result.exists():
        raise campaign.CampaignError(
            f"клетка {label} уже существует; матрица не перезаписывает доказательства"
        )
    index.mkdir(parents=True)
    result.parent.mkdir(parents=True, exist_ok=True)
    log.parent.mkdir(parents=True, exist_ok=True)

    memory_bytes = campaign.parse_byte_limit(memory)
    cpu_cores = campaign.parse_cpu_limit(cpu)
    properties = ["MemoryAccounting=yes", "CPUAccounting=yes", "TasksMax=4096"]
    if memory_bytes is not None:
        properties.append(f"MemoryMax={memory_bytes}")
        # Swap подменяет лимит памяти диском и делает клетки несопоставимыми.
        properties.append("MemorySwapMax=0")
    if cpu_cores is not None:
        properties.append(f"CPUQuota={cpu_cores * 100:g}%")
    start = ["systemd-run", "--user", "--quiet", "--collect", f"--unit={unit}"]
    for prop in properties:
        start.extend(["--property", prop])
    environment = server_environment(args.server_env)
    environment.setdefault("SVINDB_DISK_FLOOR_MB", "1")
    for name, value in sorted(environment.items()):
        start.append(f"--setenv={name}={value}")
    start.extend(
        [
            str(Path(args.binary).resolve()),
            "serve",
            str(index),
            str(args.port),
            "--single-index",
            f"--fields={args.fields}",
            *args.server_arg,
        ]
    )
    began = time.time()
    launched = run(start)
    if launched.returncode != 0:
        raise campaign.CampaignError(
            f"не поднялся {unit}: {(launched.stderr or launched.stdout).strip()}"
        )
    try:
        pid = wait_main_pid(unit, args.start_timeout)
        initial = wait_health(args.host, args.port, args.start_timeout)
        command = [
            sys.executable,
            str(Path(__file__).resolve().with_name("benchmark_campaign.py")),
            "load",
            "--candidate",
            f"{args.candidate}-{label}",
            "--corpus",
            args.corpus,
            "--seed",
            str(args.seed),
            "--concurrency",
            str(args.concurrency),
            "--durability",
            args.durability,
            "--memory-limit",
            memory,
            "--cpu-limit",
            cpu,
            "--output",
            str(result),
            "--host",
            args.host,
            "--port",
            str(args.port),
            "--pid",
            str(pid),
            "--engine",
            "svindb",
            "--protocol",
            args.protocol,
            "--corpus-path",
            str(Path(args.corpus_path).resolve()),
            "--documents",
            str(args.documents),
            "--batch-documents",
            str(args.batch_documents),
            "--batch-bytes",
            str(args.batch_bytes),
            "--index-path",
            str(index),
            "--settle-timeout",
            str(args.settle_timeout),
            "--settle-stable",
            str(args.settle_stable),
        ]
        completed = run(command, timeout=args.cell_timeout)
        decoded = None
        if result.is_file():
            with result.open(encoding="utf-8") as stream:
                decoded = json.load(stream)
        return {
            "cell": label,
            "memory_limit": memory,
            "cpu_limit": cpu,
            "unit": unit,
            "pid": pid,
            "initial_health": initial,
            "start_command": start,
            "server_environment": environment,
            "command": command,
            "returncode": completed.returncode,
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-4000:],
            "result": str(result),
            "valid": completed.returncode == 0 and bool(decoded and decoded.get("valid")),
            "elapsed_seconds": time.time() - began,
        }
    finally:
        stop_unit(unit, log)


def parser() -> argparse.ArgumentParser:
    out = argparse.ArgumentParser(description=__doc__)
    out.add_argument("--binary", required=True)
    out.add_argument("--candidate", required=True)
    out.add_argument("--corpus", choices=("flat", "mixed"), required=True)
    out.add_argument("--corpus-path", required=True)
    out.add_argument("--documents", type=int, default=10_000_000)
    out.add_argument("--fields", required=True)
    out.add_argument("--output-root", required=True)
    out.add_argument("--memory", action="append", default=[])
    out.add_argument("--cpu", action="append", default=[])
    out.add_argument("--host", default="127.0.0.1")
    out.add_argument("--port", type=int, default=19410)
    out.add_argument("--seed", type=int, default=6497668530671045374)
    out.add_argument("--concurrency", type=int, default=4)
    out.add_argument("--durability", default="request")
    out.add_argument("--protocol", choices=("native", "ndjson"), default="native")
    out.add_argument("--batch-documents", type=int, default=5000)
    out.add_argument("--batch-bytes", type=int, default=16 << 20)
    out.add_argument("--settle-timeout", type=int, default=7200)
    out.add_argument("--settle-stable", type=int, default=60)
    out.add_argument("--start-timeout", type=float, default=120)
    out.add_argument("--cell-timeout", type=float, default=10_800)
    out.add_argument("--server-arg", action="append", default=[])
    out.add_argument(
        "--server-env",
        action="append",
        default=[],
        help="переменная окружения узла NAME=VALUE; каждое имя задаётся один раз",
    )
    return out


def main() -> int:
    args = parser().parse_args()
    require_host()
    memories = args.memory or ["1GiB", "4GiB", "16GiB", "unrestricted"]
    cpus = args.cpu or ["2", "8", "unrestricted"]
    # Разбор до первого запуска: опечатка не должна оставить половину матрицы.
    for value in memories:
        campaign.parse_byte_limit(value)
    for value in cpus:
        requested = campaign.parse_cpu_limit(value)
        if requested is not None and requested > (os.cpu_count() or 1):
            raise campaign.CampaignError(
                f"CPU cell {value} требует больше доступных процессоров, чем {(os.cpu_count() or 1)}"
            )
    server_environment(args.server_env)
    root = Path(args.output_root).resolve()
    if root.exists() and any(root.iterdir()):
        raise campaign.CampaignError(
            f"output root {root} не пуст; доказательства не перезаписываются"
        )
    root.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] = {
        "format": 1,
        "started_at": campaign.now_iso(),
        "candidate": args.candidate,
        "memory_limits": memories,
        "cpu_limits": cpus,
        "cells": [],
    }
    summary_path = root / "resource-matrix.json"
    for ordinal, (memory, cpu) in enumerate(
        (pair for memory in memories for pair in ((memory, cpu) for cpu in cpus)),
        start=1,
    ):
        try:
            value = cell(args, memory, cpu, ordinal)
        except Exception as error:
            value = {
                "cell": safe_name(f"m-{memory}-c-{cpu}"),
                "memory_limit": memory,
                "cpu_limit": cpu,
                "valid": False,
                "error": repr(error),
            }
        summary["cells"].append(value)
        atomic_json(summary_path, summary)
    summary["finished_at"] = campaign.now_iso()
    summary["valid"] = all(value.get("valid") for value in summary["cells"])
    atomic_json(summary_path, summary)
    print(json.dumps({"output": str(summary_path), "valid": summary["valid"]}, ensure_ascii=False))
    return 0 if summary["valid"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
