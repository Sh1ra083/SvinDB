#!/usr/bin/env python3
"""Turns one or more bench_vs_es.py result files into a Markdown report.

With several files the median of every number is shown and the largest spread
between runs is reported, so a single noisy run cannot pass as a result.

  bench_vs_es_report.py --lang en run1.json run2.json run3.json > BENCHMARK.md
"""
import argparse
import json
import statistics
import sys

T = {
    "en": {
        "title": "SvinDB vs Elasticsearch: measured comparison",
        "status": "**Status of these numbers.** SvinDB is an early prototype. This is one machine, one synthetic corpus and {runs} run(s) of the same test; it shows where the prototype stands today and is not a general verdict on either engine.",
        "h_setup": "Setup",
        "setup_rows": [
            ("Machine", "{platform}, {cpus} CPUs, {mem_gib:.0f} GiB RAM"),
            ("SvinDB", "{svindb_version}, release build, default settings, `SVINDB_DISK_FLOOR_MB=1` (see notes)"),
            ("Elasticsearch", "{es_version}, native macOS build, 1 node, heap {es_heap} (fixed), 1 shard, 0 replicas, `refresh_interval=-1` during load, disk watermarks off (see notes)"),
            ("Corpus", "{documents} synthetic documents, {corpus_mb:.0f} MB of NDJSON (title, body, keyword category and author, long, double, date); the same bytes are sent to both engines through the Elasticsearch `_bulk` API"),
            ("Client", "Python standard library, one persistent connection per client thread, ingest with {threads} threads and batches of {batch} documents"),
            ("Runs", "{runs} (median shown; largest spread of any p50 between runs: {spread:.0%}; a difference inside the spread is reported as “about the same”)"),
            ("Background load", "1-minute load average at the start of each run: {loads}. The machine also ran unrelated work, roughly one busy core"),
        ],
        "h_correct": "Correctness check",
        "correct_text": "A speed number is only worth reading next to proof that both engines answer the same question. For every query family the exact hit count, and for aggregations the sums, minimum, maximum and buckets, were compared on several query variants. A comparison where both sides return zero is rejected.",
        "correct_summary": "**{ok} of {total} query families returned identical results** ({ids}).",
        "correct_overlap": "Top-10 relevance lists are not expected to be identical because the BM25 variants differ; the average overlap of the first ten ids was {ov:.0%}.",
        "correct_fail": "Families that did NOT match: {names}.",
        "h_ingest": "Ingest and resource use while loading",
        "ingest_cols": ["", "SvinDB", "Elasticsearch", "SvinDB vs Elasticsearch"],
        "ingest_rows": {
            "ready": "Time until searchable (s)",
            "rate": "Documents per second until searchable",
            "cpu": "CPU time used (CPU-seconds)",
            "cores": "Average CPU cores busy",
            "written": "Bytes written to disk (MiB)",
            "peak_fp": "Peak physical footprint (MiB)",
            "peak_rss": "Peak resident memory (MiB)",
            "threads": "Threads",
        },
        "h_size": "Size on disk",
        "size_cols": ["", "SvinDB", "Elasticsearch", "SvinDB vs Elasticsearch"],
        "size_rows": {
            "after": "Right after loading, allocated (MiB)",
            "settled": "Settled, allocated (MiB)",
            "settled_logical": "Settled, logical file sizes (MiB)",
            "segments": "Segments after settling",
            "ratio": "Index size ÷ raw corpus (settled, allocated)",
        },
        "size_note": "“Settled” means: SvinDB after its background merging went idle and the write-ahead log was trimmed; Elasticsearch after `_forcemerge` to one segment, which is its best case and costs {fm_cpu:.0f} CPU-seconds and {fm_gib:.2f} GiB of extra writes.",
        "h_query": "Query latency and server CPU per request",
        "query_text": "One client, one request at a time, query variants rotated, Elasticsearch request cache disabled, {reps} measured requests per family after warm-up. Latency includes the HTTP round trip and the client; values near 0.1–0.2 ms are mostly that overhead. Lower is better.",
        "query_cols": ["Query", "SvinDB p50", "ES p50", "SvinDB p99", "ES p99", "SvinDB CPU ms/req", "ES CPU ms/req", "p50: SvinDB vs ES"],
        "h_point": "Point reads (GET by id)",
        "point_cols": ["", "SvinDB", "Elasticsearch", "SvinDB vs Elasticsearch"],
        "point_rows": {"p50": "p50 (ms)", "p99": "p99 (ms)", "cpu": "Server CPU (ms/request)"},
        "h_mixed": "Mixed concurrent load",
        "mixed_text": "{clients} clients for {seconds} s: 85% searches picked at random from the families above (high-cardinality terms aggregation excluded), 15% point reads. Higher throughput is better.",
        "mixed_cols": ["", "SvinDB", "Elasticsearch", "SvinDB vs Elasticsearch"],
        "mixed_rows": {"qps": "Requests per second", "p50": "p50 (ms)", "p99": "p99 (ms)", "cores": "Average CPU cores busy", "cpu_per": "CPU per request (ms)", "peak_rss": "Peak resident memory (MiB)", "peak_fp": "Peak physical footprint (MiB)", "errors": "Errors"},
        "h_restart": "Start-up and idle memory",
        "restart_cols": ["", "SvinDB", "Elasticsearch", "SvinDB vs Elasticsearch"],
        "restart_rows": {"first": "Restart until the first query is answered (s)", "fp_idle": "Footprint right after restart and one query (MiB)", "fp_loaded": "Footprint idle after loading (MiB)", "rss_loaded": "Resident memory idle after loading (MiB)"},
        "h_notes": "How to read this, and what it does not show",
        "notes": [
            "**Memory is counted two ways on purpose.** Physical footprint is memory owned by the process (heap and anonymous pages). Resident memory also includes mapped index files. SvinDB reads its index through `mmap`, so its working set sits in the operating system page cache and is largely missing from its footprint; resident memory is the fairer figure for it. Elasticsearch has a fixed {es_heap} heap that I chose, and a smaller heap would lower its footprint.",
            "**Warm cache only.** macOS cannot drop the page cache without administrator rights, so these are warm-cache numbers. The restart row restarts the process, not the disk cache.",
            "**Both safety valves were switched off.** The machine has little free disk, and both engines refuse writes in that state by default (`SVINDB_DISK_FLOOR_MB` for SvinDB, `cluster.routing.allocation.disk.threshold_enabled` for Elasticsearch). Both were relaxed for the test only.",
            "**Not production settings for either engine.** Elasticsearch is a single shard without replicas and with refresh paused during the load; SvinDB runs with defaults. Durability defaults were kept on both sides (acknowledged writes are flushed to disk).",
            "**Synthetic corpus, one machine, one background process.** Text is generated from a Zipf-distributed vocabulary of {vocab} words, so the term frequencies are realistic in shape but the language is not. The machine also ran unrelated background work, roughly one busy core.",
            "**Ingest used the Elasticsearch-compatible `_bulk` route on SvinDB**, which converts every document. This is the path migrating clients would use, but SvinDB's own bulk format was not measured.",
        ],
        "h_repro": "Reproduce",
        "repro": "```bash\ncargo build --release --locked -p svindb-server\npython3 -I tools/bench_vs_es.py --docs 1000000 \\\n  --svindb target/release/svindb --es-home /path/to/elasticsearch-{es_version} \\\n  --workdir /path/to/scratch --output run1.json\npython3 tools/bench_vs_es_report.py --lang en run1.json run2.json run3.json > docs/BENCHMARK.md\n```\n\nThe script starts both servers itself, needs roughly {need_gib:.0f} GiB of scratch space, and writes every raw number to the JSON file.",
        "faster": "{x:.1f}× faster",
        "slower": "{x:.1f}× slower",
        "less": "{x:.1f}× less",
        "more": "{x:.1f}× more",
        "same": "about the same",
        "better_qps": "{x:.1f}× higher",
        "worse_qps": "{x:.1f}× lower",
    },
    "ru": {
        "title": "SvinDB и Elasticsearch: измеренное сравнение",
        "status": "**Статус этих чисел.** SvinDB — ранний прототип. Это одна машина, один синтетический корпус и {runs} прогон(ов) одного и того же теста; цифры показывают, где прототип находится сегодня, и не являются общим приговором ни одному из движков.",
        "h_setup": "Условия",
        "setup_rows": [
            ("Машина", "{platform}, {cpus} CPU, {mem_gib:.0f} ГиБ ОЗУ"),
            ("SvinDB", "{svindb_version}, релизная сборка, настройки по умолчанию, `SVINDB_DISK_FLOOR_MB=1` (см. примечания)"),
            ("Elasticsearch", "{es_version}, нативная сборка для macOS, 1 узел, куча {es_heap} (фиксированная), 1 шард, 0 реплик, `refresh_interval=-1` на время загрузки, дисковые пороги выключены (см. примечания)"),
            ("Корпус", "{documents} синтетических документов, {corpus_mb:.0f} МБ NDJSON (title, body, keyword-категория и автор, long, double, date); одни и те же байты отправляются обоим движкам через `_bulk` Elasticsearch"),
            ("Клиент", "стандартная библиотека Python, одно постоянное соединение на поток клиента, загрузка в {threads} потока(ов) пачками по {batch} документов"),
            ("Прогоны", "{runs} (показана медиана; наибольший разброс любого p50 между прогонами: {spread:.0%}; разница внутри разброса названа «примерно одинаково»)"),
            ("Фоновая нагрузка", "средняя загрузка за минуту в начале каждого прогона: {loads}. На машине также работала посторонняя нагрузка, примерно одно занятое ядро"),
        ],
        "h_correct": "Проверка корректности",
        "correct_text": "Число скорости имеет смысл читать только рядом с доказательством, что оба движка отвечают на один и тот же вопрос. Для каждого семейства запросов сравнивалось точное число совпадений, а для агрегаций — суммы, минимум, максимум и корзины на нескольких вариантах запроса. Сравнение, в котором обе стороны вернули ноль, отвергается.",
        "correct_summary": "**{ok} из {total} семейств запросов вернули одинаковый результат** ({ids}).",
        "correct_overlap": "Списки топ-10 по релевантности не обязаны совпадать, потому что варианты BM25 различаются; среднее пересечение первых десяти id — {ov:.0%}.",
        "correct_fail": "Семейства, которые НЕ совпали: {names}.",
        "h_ingest": "Загрузка и расход ресурсов при ней",
        "ingest_cols": ["", "SvinDB", "Elasticsearch", "SvinDB относительно Elasticsearch"],
        "ingest_rows": {
            "ready": "Время до готовности к поиску (с)",
            "rate": "Документов в секунду до готовности",
            "cpu": "Затрачено процессорного времени (CPU-секунд)",
            "cores": "В среднем занято ядер",
            "written": "Записано на диск (МиБ)",
            "peak_fp": "Пик физического объёма (МиБ)",
            "peak_rss": "Пик резидентной памяти (МиБ)",
            "threads": "Потоков",
        },
        "h_size": "Размер на диске",
        "size_cols": ["", "SvinDB", "Elasticsearch", "SvinDB относительно Elasticsearch"],
        "size_rows": {
            "after": "Сразу после загрузки, выделено (МиБ)",
            "settled": "После стабилизации, выделено (МиБ)",
            "settled_logical": "После стабилизации, логический размер файлов (МиБ)",
            "segments": "Сегментов после стабилизации",
            "ratio": "Размер индекса ÷ исходный корпус (стабилизированный, выделено)",
        },
        "size_note": "«Стабилизированный» означает: SvinDB — после того как фоновые слияния затихли и журнал записи усечён; Elasticsearch — после `_forcemerge` до одного сегмента, это его лучший случай, и он стоит {fm_cpu:.0f} CPU-секунд и {fm_gib:.2f} ГиБ дополнительной записи.",
        "h_query": "Задержка запросов и процессорное время сервера на запрос",
        "query_text": "Один клиент, по одному запросу, варианты запроса чередуются, кеш запросов Elasticsearch выключен, {reps} измеряемых запросов на семейство после прогрева. Задержка включает сетевой круг HTTP и клиента; значения около 0,1–0,2 мс — в основном эти накладные расходы. Меньше — лучше.",
        "query_cols": ["Запрос", "SvinDB p50", "ES p50", "SvinDB p99", "ES p99", "SvinDB CPU мс/запр.", "ES CPU мс/запр.", "p50: SvinDB относительно ES"],
        "h_point": "Точечные чтения (GET по id)",
        "point_cols": ["", "SvinDB", "Elasticsearch", "SvinDB относительно Elasticsearch"],
        "point_rows": {"p50": "p50 (мс)", "p99": "p99 (мс)", "cpu": "CPU сервера (мс/запрос)"},
        "h_mixed": "Смешанная параллельная нагрузка",
        "mixed_text": "{clients} клиентов в течение {seconds} с: 85% поисков, выбранных случайно из семейств выше (агрегация по ключу с высокой кардинальностью исключена), 15% точечных чтений. Больше пропускная способность — лучше.",
        "mixed_cols": ["", "SvinDB", "Elasticsearch", "SvinDB относительно Elasticsearch"],
        "mixed_rows": {"qps": "Запросов в секунду", "p50": "p50 (мс)", "p99": "p99 (мс)", "cores": "В среднем занято ядер", "cpu_per": "CPU на запрос (мс)", "peak_rss": "Пик резидентной памяти (МиБ)", "peak_fp": "Пик физического объёма (МиБ)", "errors": "Ошибок"},
        "h_restart": "Запуск и память в покое",
        "restart_cols": ["", "SvinDB", "Elasticsearch", "SvinDB относительно Elasticsearch"],
        "restart_rows": {"first": "Перезапуск до ответа на первый запрос (с)", "fp_idle": "Физический объём сразу после перезапуска и одного запроса (МиБ)", "fp_loaded": "Физический объём в покое после загрузки (МиБ)", "rss_loaded": "Резидентная память в покое после загрузки (МиБ)"},
        "h_notes": "Как это читать и чего это не показывает",
        "notes": [
            "**Память посчитана двумя способами намеренно.** Физический объём — память, принадлежащая процессу (куча и анонимные страницы). Резидентная память включает ещё и отображённые файлы индекса. SvinDB читает индекс через `mmap`, поэтому его рабочий набор лежит в страничном кеше операционной системы и в его физическом объёме почти не виден; для него честнее резидентная память. У Elasticsearch фиксированная куча {es_heap}, которую выбрал я, и меньшая куча уменьшила бы его объём.",
            "**Только прогретый кеш.** macOS не умеет сбрасывать страничный кеш без прав администратора, поэтому числа получены на тёплом кеше. Строка про перезапуск перезапускает процесс, а не кеш диска.",
            "**Оба предохранителя отключены.** На машине мало свободного диска, и оба движка по умолчанию отказываются писать в таком состоянии (`SVINDB_DISK_FLOOR_MB` у SvinDB, `cluster.routing.allocation.disk.threshold_enabled` у Elasticsearch). Для теста оба ослаблены.",
            "**Это не боевые настройки ни одного из движков.** Elasticsearch — один шард без реплик и с приостановленным refresh на время загрузки; SvinDB — настройки по умолчанию. Умолчания долговечности сохранены с обеих сторон (подтверждённые записи сброшены на диск).",
            "**Синтетический корпус, одна машина, один фоновый процесс.** Текст порождён из словаря на {vocab} слов с распределением Ципфа: частоты терминов правдоподобны по форме, но язык — нет. На машине также работала посторонняя фоновая нагрузка, примерно одно занятое ядро.",
            "**Загрузка у SvinDB шла через совместимый с Elasticsearch маршрут `_bulk`**, который преобразует каждый документ. Это путь, которым пойдут переносимые клиенты, но собственный формат пачек SvinDB не измерялся.",
        ],
        "h_repro": "Как повторить",
        "repro": "```bash\ncargo build --release --locked -p svindb-server\npython3 -I tools/bench_vs_es.py --docs 1000000 \\\n  --svindb target/release/svindb --es-home /path/to/elasticsearch-{es_version} \\\n  --workdir /path/to/scratch --output run1.json\npython3 tools/bench_vs_es_report.py --lang ru run1.json run2.json run3.json > docs/BENCHMARK.ru.md\n```\n\nСкрипт сам запускает оба сервера, ему нужно примерно {need_gib:.0f} ГиБ свободного места, и он пишет все сырые числа в JSON-файл.",
        "faster": "быстрее в {x:.1f} раза",
        "slower": "медленнее в {x:.1f} раза",
        "less": "меньше в {x:.1f} раза",
        "more": "больше в {x:.1f} раза",
        "same": "примерно одинаково",
        "better_qps": "выше в {x:.1f} раза",
        "worse_qps": "ниже в {x:.1f} раза",
    },
}

MIB = 2 ** 20


def med(runs, path):
    vals = []
    for r in runs:
        v = r
        for p in path:
            v = v[p]
        vals.append(v)
    return statistics.median(vals)


def verdict(t, sv, es, lower_is_better=True, kind="time", noise=0.0):
    """kind: time -> faster/slower, mem -> less/more, rate -> higher/lower.

    A difference smaller than 10% or smaller than the spread seen between
    repeated runs is reported as "about the same", never as a win."""
    if not sv or not es:
        return "—"
    if abs(sv - es) / max(sv, es) < max(0.10, noise):
        return t["same"]
    sv_better = sv < es if lower_is_better else sv > es
    x = max(sv, es) / min(sv, es)
    if kind == "time":
        return loc(t["faster" if sv_better else "slower"].format(x=x))
    if kind == "mem":
        return loc(t["less" if sv_better else "more"].format(x=x))
    return loc(t["better_qps"if sv_better else "worse_qps"].format(x=x))


def table(cols, rows):
    out = ["| " + " | ".join(cols) + " |", "|" + "|".join(["---"] + ["---:"] * (len(cols) - 2) + ["---"]) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(out)


LANG = "en"


def loc(text):
    """Russian typography: thin grouping by space, decimal comma."""
    if LANG != "ru":
        return text
    return text.replace(",", "\u00a0").replace(".", ",")


def num(x, d=1):
    return loc(f"{x:,.{d}f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lang", choices=("en", "ru"), default="en")
    ap.add_argument("files", nargs="+")
    a = ap.parse_args()
    global LANG
    LANG = a.lang
    t = T[a.lang]
    runs = [json.load(open(f)) for f in a.files]
    r0 = runs[0]
    setup = r0["setup"]
    spread = 0.0
    for fam in r0["queries"]:
        for eng in ("es", "svindb"):
            vals = [r["queries"][fam][eng]["p50_ms"] for r in runs]
            if len(vals) > 1 and statistics.median(vals):
                spread = max(spread, (max(vals) - min(vals)) / statistics.median(vals))
    ctx = {
        "platform": r0["machine"]["platform"].replace("-Mach-O", ""), "cpus": r0["machine"]["cpus"],
        "mem_gib": r0["machine"]["memory_bytes"] / 2 ** 30, "svindb_version": setup["svindb_version"],
        "es_version": setup["es_version"],
        "es_heap": setup["es_heap"].upper().replace("G", " ГиБ" if a.lang == "ru" else " GiB"),
        "documents": loc(f"{setup['documents']:,}"), "corpus_mb": setup["corpus_bytes"] / 1e6,
        "threads": setup["ingest_threads"], "batch": setup["batch_docs"], "runs": len(runs), "spread": spread,
        "loads": ", ".join(loc(f"{r['machine']['load_avg_at_start'][0]:.1f}") for r in runs),
        "reps": setup["reps"], "vocab": loc(f"{30000:,}"), "need_gib": max(3, setup["corpus_bytes"] * 8 / 2 ** 30),
    }
    L = [f"# {t['title']}", "", t["status"].format(**ctx), "", f"## {t['h_setup']}", ""]
    L.append(table(["", ""], [(k, v.format(**ctx)) for k, v in t["setup_rows"]]).replace("|---|---:|", "|---|---|"))

    # correctness
    ver = r0["verify"]
    ok = [n for n, v in ver.items() if v.get("ok")]
    bad = [n for n, v in ver.items() if not v.get("ok")]
    ov = [v["top10_overlap"] for v in ver.values() if v.get("top10_overlap") is not None]
    L += ["", f"## {t['h_correct']}", "", t["correct_text"], "",
          t["correct_summary"].format(ok=len(ok), total=len(ver), ids=", ".join(f"`{n}`" for n in ok))]
    if ov:
        L += ["", t["correct_overlap"].format(ov=statistics.fmean(ov))]
    if bad:
        L += ["", t["correct_fail"].format(names=", ".join(f"`{n}`" for n in bad))]

    # ingest
    g = lambda eng, *p: med(runs, ("ingest", eng) + p)
    rows = []
    R = t["ingest_rows"]
    def pair(label, sv, es, d=1, lower=True, kind="time", scale=1):
        return (label, num(sv / scale, d), num(es / scale, d), verdict(t, sv, es, lower, kind))
    rows.append(pair(R["ready"], g("svindb", "ready_s"), g("es", "ready_s"), 1))
    rows.append(pair(R["rate"], g("svindb", "docs_per_s_to_ready"), g("es", "docs_per_s_to_ready"), 0, False, "rate"))
    rows.append(pair(R["cpu"], g("svindb", "phase", "cpu_s") + g("svindb", "ready_phase", "cpu_s"),
                     g("es", "phase", "cpu_s") + g("es", "ready_phase", "cpu_s"), 0, True, "mem"))
    rows.append(pair(R["cores"], g("svindb", "phase", "cpu_cores_avg"), g("es", "phase", "cpu_cores_avg"), 1, True, "mem"))
    rows.append(pair(R["written"], g("svindb", "phase", "disk_write_bytes") + g("svindb", "ready_phase", "disk_write_bytes"),
                     g("es", "phase", "disk_write_bytes") + g("es", "ready_phase", "disk_write_bytes"), 0, True, "mem", MIB))
    rows.append(pair(R["peak_fp"], g("svindb", "phase", "peak_footprint_bytes"), g("es", "phase", "peak_footprint_bytes"), 0, True, "mem", MIB))
    rows.append(pair(R["peak_rss"], g("svindb", "phase", "peak_rss_bytes"), g("es", "phase", "peak_rss_bytes"), 0, True, "mem", MIB))
    rows.append((R["threads"], num(g("svindb", "phase", "threads"), 0), num(g("es", "phase", "threads"), 0), ""))
    L += ["", f"## {t['h_ingest']}", "", table(t["ingest_cols"], rows)]

    # size
    s = lambda tag, eng, key: med(runs, ("size", tag, eng, key))
    S = t["size_rows"]
    corpus = setup["corpus_bytes"]
    rows = [
        pair(S["after"], s("after_ingest", "svindb", "allocated_bytes"), s("after_ingest", "es", "allocated_bytes"), 0, True, "mem", MIB),
        pair(S["settled"], s("settled", "svindb", "allocated_bytes"), s("settled", "es", "allocated_bytes"), 0, True, "mem", MIB),
        pair(S["settled_logical"], s("settled", "svindb", "logical_bytes"), s("settled", "es", "logical_bytes"), 0, True, "mem", MIB),
        (S["segments"], num(s("settled", "svindb", "segments"), 0), num(s("settled", "es", "segments"), 0), ""),
        (S["ratio"], f"{s('settled', 'svindb', 'allocated_bytes') / corpus:.2f}", f"{s('settled', 'es', 'allocated_bytes') / corpus:.2f}", ""),
    ]
    fm = med(runs, ("size", "es_forcemerge_phase", "cpu_s")), med(runs, ("size", "es_forcemerge_phase", "disk_write_bytes")) / 2 ** 30
    L += ["", f"## {t['h_size']}", "", table(t["size_cols"], rows), "", t["size_note"].format(fm_cpu=fm[0], fm_gib=fm[1])]

    # queries
    rows = []
    q = lambda fam, eng, key: med(runs, ("queries", fam, eng, key))
    for fam in r0["queries"]:
        noise = 0.0
        for eng in ("es", "svindb"):
            vals = [r["queries"][fam][eng]["p50_ms"] for r in runs]
            if len(vals) > 1 and statistics.median(vals):
                noise = max(noise, (max(vals) - min(vals)) / statistics.median(vals))
        rows.append((fam,
                     num(q(fam, "svindb", "p50_ms"), 2), num(q(fam, "es", "p50_ms"), 2),
                     num(q(fam, "svindb", "p99_ms"), 2), num(q(fam, "es", "p99_ms"), 2),
                     num(q(fam, "svindb", "server_cpu_ms_per_request"), 2), num(q(fam, "es", "server_cpu_ms_per_request"), 2),
                     verdict(t, q(fam, "svindb", "p50_ms"), q(fam, "es", "p50_ms"), True, "time", noise)))
    L += ["", f"## {t['h_query']}", "", t["query_text"].format(**ctx), "", table(t["query_cols"], rows)]
    notes = {f: v["note"] for f, v in r0["queries"].items() if v.get("note")}
    for f, n in notes.items():
        L += ["", f"*{f}:* {n}."]

    # point
    P = t["point_rows"]
    p = lambda eng, key: med(runs, ("point", eng, key))
    rows = [pair(P["p50"], p("svindb", "p50_ms"), p("es", "p50_ms"), 3),
            pair(P["p99"], p("svindb", "p99_ms"), p("es", "p99_ms"), 3),
            pair(P["cpu"], p("svindb", "server_cpu_ms_per_request"), p("es", "server_cpu_ms_per_request"), 3, True, "mem")]
    L += ["", f"## {t['h_point']}", "", table(t["point_cols"], rows)]

    # mixed
    M = t["mixed_rows"]
    m = lambda eng, *k: med(runs, ("mixed", eng) + k)
    rows = [pair(M["qps"], m("svindb", "qps"), m("es", "qps"), 0, False, "rate"),
            pair(M["p50"], m("svindb", "p50_ms"), m("es", "p50_ms"), 2),
            pair(M["p99"], m("svindb", "p99_ms"), m("es", "p99_ms"), 2),
            pair(M["cores"], m("svindb", "phase", "cpu_cores_avg"), m("es", "phase", "cpu_cores_avg"), 1, True, "mem"),
            pair(M["cpu_per"], m("svindb", "phase", "cpu_s") * 1000 / m("svindb", "requests"),
                 m("es", "phase", "cpu_s") * 1000 / m("es", "requests"), 2, True, "mem"),
            pair(M["peak_rss"], m("svindb", "phase", "peak_rss_bytes"), m("es", "phase", "peak_rss_bytes"), 0, True, "mem", MIB),
            pair(M["peak_fp"], m("svindb", "phase", "peak_footprint_bytes"), m("es", "phase", "peak_footprint_bytes"), 0, True, "mem", MIB),
            (M["errors"], num(m("svindb", "errors"), 0), num(m("es", "errors"), 0), "")]
    mm = r0["mixed"]["svindb"]
    L += ["", f"## {t['h_mixed']}", "", t["mixed_text"].format(clients=mm["clients"], seconds=mm["seconds"]), "", table(t["mixed_cols"], rows)]

    # restart
    X = t["restart_rows"]
    x = lambda eng, *k: med(runs, k[:1] + (eng,) + k[1:])
    rows = [pair(X["first"], x("svindb", "restart", "first_query_answered_s"), x("es", "restart", "first_query_answered_s"), 2),
            pair(X["fp_idle"], x("svindb", "restart", "footprint_bytes_after_first_query"), x("es", "restart", "footprint_bytes_after_first_query"), 0, True, "mem", MIB),
            pair(X["fp_loaded"], med(runs, ("idle", "after_ingest", "svindb", "footprint")), med(runs, ("idle", "after_ingest", "es", "footprint")), 0, True, "mem", MIB),
            pair(X["rss_loaded"], med(runs, ("idle", "after_ingest", "svindb", "rss")), med(runs, ("idle", "after_ingest", "es", "rss")), 0, True, "mem", MIB)]
    L += ["", f"## {t['h_restart']}", "", table(t["restart_cols"], rows)]

    L += ["", f"## {t['h_notes']}", ""] + [f"- {n.format(**ctx)}" for n in t["notes"]]
    L += ["", f"## {t['h_repro']}", "", t["repro"].format(**ctx), ""]
    sys.stdout.write("\n".join(L))


if __name__ == "__main__":
    main()
