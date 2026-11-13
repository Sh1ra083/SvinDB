#!/usr/bin/env python3
"""Фаззер приёма: узел обязан отказывать негодному документу, а не умирать."""
import http.client, json, os, shutil, subprocess, sys, time

PORT = int(os.environ.get("ZPORT", "19491"))
DIR = f'{os.environ.get("ZDIR", "/tmp/sv-fuzz")}-ingest'
BIN = os.environ.get("ZBIN", "/tmp/svindb-h1")
FIELDS = "title:2,body:1,цена:int64,v:dense_vector:4"
BIG = 9223372036854775807
NUL = chr(0)


def boot():
    """Поднимает собственный узел, не полагаясь на предыдущий fuzz-тест."""
    subprocess.run(["pkill", "-f", f"serve {DIR} "], capture_output=True)
    time.sleep(1)
    shutil.rmtree(DIR, ignore_errors=True)
    os.makedirs(DIR, exist_ok=True)
    log = open(f"{DIR}.log", "w")
    try:
        p = subprocess.Popen([BIN, "serve", DIR, str(PORT), "--single-index", f"--fields={FIELDS}"],
                             stdout=log, stderr=subprocess.STDOUT,
                             start_new_session=True)
    finally:
        log.close()
    for _ in range(80):
        if not alive(p):
            return None
        try:
            c = http.client.HTTPConnection("localhost", PORT, timeout=5)
            c.request("GET", "/_health"); c.getresponse().read(); c.close()
            return p
        except Exception:
            time.sleep(0.25)
    stop(p)
    return None


def call(m, path, body=None, to=120):
    c = http.client.HTTPConnection("localhost", PORT, timeout=to)
    d = json.dumps(body).encode() if body is not None else None
    c.request(m, path, d, {"Content-Type": "application/json"})
    r = c.getresponse(); out = r.read(); c.close()
    return r.status, out[:90]


def alive(process):
    return process.poll() is None


CASES = [
    ("id пустой", {"documents": [{"id": "", "title": "a", "body": "b"}]}),
    ("id отсутствует", {"documents": [{"title": "a", "body": "b"}]}),
    ("id не строка", {"documents": [{"id": 123, "title": "a"}]}),
    ("id длиной 1 МБ", {"documents": [{"id": "x" * 1000000, "title": "a"}]}),
    ("поле — массив", {"documents": [{"id": "m1", "title": ["a", "b"]}]}),
    ("поле — объект", {"documents": [{"id": "m2", "title": {"a": 1}}]}),
    ("поле — null", {"documents": [{"id": "m3", "title": None}]}),
    ("число вместо текста", {"documents": [{"id": "m4", "title": 42}]}),
    ("текст вместо числа", {"documents": [{"id": "m5", "цена": "не число"}]}),
    ("число за i64", {"documents": [{"id": "m6", "цена": BIG}]}),
    ("число дробное в int64", {"documents": [{"id": "m7", "цена": 1.5}]}),
    ("вектор не той длины", {"documents": [{"id": "m8", "v": "1 2 3 4 5 6 7"}]}),
    ("вектор мусором", {"documents": [{"id": "m9", "v": "не вектор вовсе"}]}),
    ("вектор с nan/inf", {"documents": [{"id": "m10", "v": "nan inf 1 2"}]}),
    ("нулевые байты в тексте", {"documents": [{"id": "m11", "body": f"а{NUL}б{NUL}в"}]}),
    ("один документ на 8 МБ", {"documents": [{"id": "m12", "body": "я" * 4000000}]}),
    ("десять тысяч документов", {"documents": [{"id": f"z{i}", "body": "общее"} for i in range(10000)]}),
    ("documents не массив", {"documents": {"id": "a"}}),
    ("повтор одного id", {"documents": [{"id": "dup", "body": "a"} for _ in range(500)]}),
]

DEL = [
    ("ids не массив", {"ids": "a"}),
    ("ids пустой", {"ids": []}),
    ("сто тысяч ids", {"ids": [f"d{i}" for i in range(100000)]}),
    ("id длиной 1 МБ", {"ids": ["x" * 1000000]}),
]


def exercise(process):
    """Обстреливает уже запущенный узел и возвращает код ворот."""
    bad = []
    for name, b in CASES:
        t0 = time.perf_counter()
        try:
            st, out = call("POST", "/_bulk", b)
        except Exception as e:
            st, out = -1, repr(e)[:70].encode()
        dt = time.perf_counter() - t0
        if not alive(process):
            print(f"  ✗✗ УЗЕЛ УМЕР на «{name}»"); bad.append(name); break
        w = " ⚠медленно" if dt > 5 else ""
        print(f"  {'⚠' if w else '·'} {name:26} {st:>4} {dt:6.3f} с "
              f"{out.decode(errors='replace')[:50]}{w}")
    for name, b in DEL:
        t0 = time.perf_counter()
        try:
            st, out = call("POST", "/_delete", b)
        except Exception as e:
            st, out = -1, repr(e)[:70].encode()
        dt = time.perf_counter() - t0
        if not alive(process):
            print(f"  ✗✗ УЗЕЛ УМЕР на удалении «{name}»"); bad.append(name); break
        print(f"  · удаление: {name:20} {st:>4} {dt:6.3f} с {out.decode(errors='replace')[:40]}")

    print("\nузел", "жив" if alive(process) else "УМЕР")
    if alive(process):
        call("POST", "/_flush", {})
        time.sleep(1.5)
        st, h = call("GET", "/_health")
        print("health:", h.decode()[:130])
        # и поиск после всего этого обязан работать
        st, r = call("POST", "/", {"k": 3, "words": [{"field": 1, "terms": ["общее"]}], "ids": True})
        print("поиск после фаззинга:", st, r.decode(errors="replace")[:80])
    return 1 if bad else 0


def stop(process):
    """Завершает только тот узел, который запустил этот фаззер."""
    if not alive(process):
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def main():
    process = boot()
    if not process:
        print("узел не поднялся")
        return 1
    try:
        return exercise(process)
    finally:
        stop(process)


if __name__ == "__main__":
    sys.exit(main())
