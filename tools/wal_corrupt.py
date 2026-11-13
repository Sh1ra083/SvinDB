#!/usr/bin/env python3
"""Порча журнала приёма: узел обязан подняться и честно сказать, что уцелело.

Журнал — единственное, что стоит между «ответили принято» и диском. Его портят
не злоумышленники, а обрыв питания посреди записи, и обращаться с ним надо как с
недоверенным входом: оборванный хвост отбрасывается, а испорченная середина
обязана быть замечена, а не проиграна как есть.

Проверяется: узел поднялся (не убит сигналом), сказал в `/_health`, сколько
переиграл и сколько байт отбросил, и после этого работает.
"""
import http.client, json, os, shutil, signal, subprocess, sys, time

BIN = os.environ.get("WBIN", "/tmp/svindb-h4")
BASE = "/private/tmp/sv-wal"
PORT = 19496
FIELDS = "title:2,body:1"
N = 400

crashes = []


def call(m, p, b=None, to=30):
    c = http.client.HTTPConnection("localhost", PORT, timeout=to)
    c.request(m, p, json.dumps(b).encode() if b is not None else None,
              {"Content-Type": "application/json"})
    r = c.getresponse(); d = r.read(); c.close()
    try:
        return r.status, json.loads(d)
    except Exception:
        return r.status, d


def start(d):
    p = subprocess.Popen(
        [BIN, "serve", d, str(PORT), "--single-index", f"--fields={FIELDS}", "--no-maintain"],
                         stdout=open("/tmp/wal-node.log", "w"), stderr=subprocess.STDOUT,
                         start_new_session=True)
    for _ in range(60):
        if p.poll() is not None:
            return p, False
        try:
            call("GET", "/_health", to=3)
            return p, True
        except Exception:
            time.sleep(0.25)
    return p, False


def build_gold():
    shutil.rmtree(BASE, ignore_errors=True)
    gold = BASE + "/gold"
    os.makedirs(gold)
    p, up = start(gold)
    if not up:
        print("узел не поднялся:", open("/tmp/wal-node.log").read()[-300:]); sys.exit(1)
    # Кладём без сброса: всё живёт только в журнале и буфере.
    call("POST", "/_bulk", {"documents": [
        {"id": f"d{i}", "title": f"док{i}", "body": "общее тело"} for i in range(N)]})
    time.sleep(0.3)
    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
    time.sleep(0.6)
    files = []
    wal = os.path.join(gold, "wal")
    if os.path.isdir(wal):
        files = sorted(os.listdir(wal))
    print(f"эталон: журнал из {len(files)} файлов: {files}\n")
    return gold, files


def probe(tag, work):
    p, up = start(work)
    if not up:
        code = p.poll()
        tail = open("/tmp/wal-node.log").read()[-200:].strip().replace("\n", " ")
        if code is not None and code < 0:
            crashes.append((tag, f"УБИТ СИГНАЛОМ {-code}"))
            return f"УБИТ СИГНАЛОМ {-code}"
        return f"отказал: {tail[:80]}"
    st, h = call("GET", "/_health")
    w = h.get("wal", {}) if isinstance(h, dict) else {}
    docs = h.get("docs", -1) if isinstance(h, dict) else -1
    buffered = h.get("buffered", -1) if isinstance(h, dict) else -1
    call("POST", "/_flush", {})
    time.sleep(1.0)
    st, h2 = call("GET", "/_health")
    total = h2.get("docs", -1) if isinstance(h2, dict) else -1
    st, r = call("POST", "/", {"k": 3, "words": [{"field": 1, "terms": ["общее"]}]})
    works = st == 200
    if p.poll() is not None:
        crashes.append((tag, "УПАЛ ПОСЛЕ ПОДЪЁМА"))
        return "УПАЛ ПОСЛЕ ПОДЪЁМА"
    subprocess.run(["pkill", "-f", f"serve {work} "], capture_output=True)
    return (f"поднялся: вернул {total}/{N}, переиграно {w.get('replayed_adds', '?')}, "
            f"отброшено {w.get('torn_bytes', '?')} б, поиск {'ок' if works else 'НЕТ'}")


def main():
    gold, files = build_gold()
    if not files:
        print("журнала нет — нечего портить"); return 1
    work = BASE + "/work"
    cases = []
    for f in files:
        for kind in ("хвост срезан на 7 байт", "срезан наполовину", "обнулён",
                     "байт в середине испорчен", "дописан мусор"):
            cases.append((f, kind))
    for f, kind in cases:
        shutil.rmtree(work, ignore_errors=True)
        shutil.copytree(gold, work)
        path = os.path.join(work, "wal", f)
        data = bytearray(open(path, "rb").read())
        if not data:
            continue
        if kind == "хвост срезан на 7 байт":
            data = data[:-7] if len(data) > 7 else data[:0]
        elif kind == "срезан наполовину":
            data = data[: len(data) // 2]
        elif kind == "обнулён":
            data = bytearray(len(data))
        elif kind == "байт в середине испорчен":
            data[len(data) // 2] ^= 0xFF
        else:
            data += b"\xde\xad\xbe\xef" * 64
        open(path, "wb").write(bytes(data))
        tag = f"{f} — {kind}"
        v = probe(tag, work)
        mark = "✗" if ("УБИТ" in v or "УПАЛ" in v) else "·"
        print(f"  {mark} {tag:38} {v}")
    print(f"\nпадений: {len(crashes)}")
    for t, v in crashes:
        print(f"  ✗ {t}: {v}")
    return 1 if crashes else 0


if __name__ == "__main__":
    sys.exit(main())
