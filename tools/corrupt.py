#!/usr/bin/env python3
"""Порча файлов сегмента: узел обязан отказаться, а не упасть.

Сборка идёт с `panic = "abort"`, поэтому паника на чтении повреждённого файла
убивает процесс целиком. Для узла, который держит сотню соединений, это разница
между «этот индекс не открылся» и «упал сервер».

Порча трёх видов, и они ловят разное:
  * **обрезание** — срез фиксированной длины за концом отображения;
  * **обнуление** — длины и смещения, прочтённые как ноль, дают бесконечные
    циклы и деления на ноль;
  * **порча байтов** — значения вне допустимого: номера полей, коды Хаффмана,
    длины блоков.

Порча идёт по двум мишеням, потому что сегменты бывают двух видов.

Сегмент, собранный начиная с контейнера, — это один `seg.dat`, внутри которого
куски (`postings.bin`, `terms.idx`, `<поле>.sbin` и прочие) лежат подряд, а где
они лежат, говорит оглавление в хвосте файла. Порча целым файлом такому сегменту
достаёт лишь до обрамления: обрезание и обнуление сносят концевик, отказ
приходит с первых тридцати двух байт, и ни одного куска узел даже не касается.
Проверять этим можно только само обрамление — и оно проверяется, порядок ниже
сохранён. Но куски надо портить **поимённо**: читается оглавление, ищется кусок,
переворачивается бит по смещению внутри него. Заголовок, концевик и оглавление
при этом целы, то есть спрашивается ровно то, что важно, — доберётся ли отказ до
самого куска, или узел прочтёт испорченное как годное.

Сегмент, собранный до контейнера, лежит россыпью файлов, и порча целым файлом
для него единственная. Она же — единственный способ проверить `head.tsv`,
`sums.tsv` и `live.bin`: эти трое в контейнер не кладутся никогда.

Каждый случай проверяется двумя способами: `svindb check` (он для того и есть) и
настоящий узел, который пытается открыть каталог и выполнить запрос.
"""
import json, os, random, shutil, struct, subprocess, sys, time, http.client

BIN = os.environ.get("CBIN", "/tmp/svindb-h2")
BASE = "/private/tmp/sv-corrupt"
GOLD = BASE + "/gold"
WORK = BASE + "/work"
PORT = 19495
FIELDS = "title:2,body:1,цена:int64,метка:keyword"

# Раскладка контейнера сегмента — та же, что у `container.rs`; изменится там,
# изменится и здесь, и разойтись они не смогут молча: опознание не сойдётся.
CONTAINER = "seg.dat"
C_HEAD, C_ENTRY, C_NAME, C_TAIL = 64, 96, 72, 32
C_MAGIC = b"\x8fSVIN\r\n\x1a"
C_TAIL_MAGIC = b"\x8fSVN"

ok = 0
crashes = []
checks = []
notes = []


def sh(*a, **kw):
    return subprocess.run(a, capture_output=True, text=True, timeout=120, **kw)


def build_gold():
    shutil.rmtree(BASE, ignore_errors=True)
    os.makedirs(GOLD)
    docs = "\n".join(json.dumps({
        "id": f"d{i}", "title": f"док{i} общее", "body": f"тело{i} слово{i % 7} общее весьма длинное",
        "цена": i % 500, "метка": f"м{i % 4}"}, ensure_ascii=False) for i in range(4000))
    src = BASE + "/corpus.ndjson"
    open(src, "w").write(docs + "\n")
    r = sh(BIN, "index", src, GOLD, "2000", FIELDS)
    if r.returncode != 0:
        print("эталон не собрался:", r.stdout[-400:], r.stderr[-400:]); sys.exit(1)
    segs = [d for d in os.listdir(GOLD) if os.path.isdir(os.path.join(GOLD, d))]
    print(f"эталон: {len(segs)} сегмента, файлов в первом: "
          f"{len(os.listdir(os.path.join(GOLD, sorted(segs)[0])))}")


def probe(tag):
    """Открыть испорченный каталог узлом и постучаться. Возвращает исход."""
    global ok
    # 1. check — он для того и есть, падать ему нельзя тем более
    r = sh(BIN, "check", WORK, "--full")
    if r.returncode not in (0, 1):
        crashes.append((tag, f"check упал: код {r.returncode} {r.stderr[-200:]}"))
        return "check упал"
    seen = "поймал" if r.returncode == 1 else "НЕ ЗАМЕТИЛ"
    checks.append((tag, seen))
    # 2. настоящий узел
    subprocess.run(["pkill", "-f", f"serve {WORK} "], capture_output=True)
    time.sleep(0.4)
    log = open("/tmp/corrupt-node.log", "w")
    p = subprocess.Popen([BIN, "serve", WORK, str(PORT), "--read-only"],
                         stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    up = False
    for _ in range(40):
        if p.poll() is not None:
            break
        try:
            c = http.client.HTTPConnection("localhost", PORT, timeout=3)
            c.request("GET", "/_health"); c.getresponse().read(); c.close()
            up = True; break
        except Exception:
            time.sleep(0.2)
    verdict = "не открылся"
    if up:
        try:
            for q in ({"k": 10, "words": [{"field": 1, "terms": ["общее"]}], "ids": True,
                       "fields": ["body"]},
                      {"k": 10, "words": [{"field": 1, "terms": ["сл*"]}], "max_expansions": 500},
                      {"k": 10, "must": [{"field": 1, "terms": ["общее"]}],
                       "filter": [{"field": "цена", "from": 1, "to": 100}]},
                      {"k": 5, "words": [{"field": 1, "terms": ["общее"]}],
                       "sort": [{"field": "цена", "order": "desc"}]},
                      {"k": 0, "words": [{"field": 1, "terms": ["общее"]}],
                       "aggs": {"s": {"stats": {"field": "цена"}}}}):
                c = http.client.HTTPConnection("localhost", PORT, timeout=20)
                c.request("POST", "/", json.dumps(q).encode(), {"Content-Type": "application/json"})
                c.getresponse().read(); c.close()
            verdict = "открылся и отвечает"
        except Exception as e:
            verdict = f"открылся, но запрос: {repr(e)[:50]}"
        if p.poll() is not None:
            verdict = "УПАЛ НА ЗАПРОСЕ"
    else:
        # не поднялся — это законно, если он сказал почему
        tail = open("/tmp/corrupt-node.log").read()[-300:].strip().replace("\n", " ")
        code = p.poll()
        # смерть по сигналу (< 0) — это паника/abort, а не отказ
        if code is not None and code < 0:
            verdict = f"УБИТ СИГНАЛОМ {-code}: {tail[:120]}"
        else:
            verdict = f"отказал: {tail[:90]}"
    subprocess.run(["pkill", "-f", f"serve {WORK} "], capture_output=True)
    if "УПАЛ" in verdict or "УБИТ" in verdict:
        crashes.append((tag, verdict))
    else:
        ok += 1
    return verdict


def with_copy():
    shutil.rmtree(WORK, ignore_errors=True)
    shutil.copytree(GOLD, WORK)


def seg_files():
    segs = sorted(d for d in os.listdir(GOLD) if os.path.isdir(os.path.join(GOLD, d)))
    first = os.path.join(GOLD, segs[0])
    return segs[0], sorted(os.listdir(first))


def pieces(path):
    """Оглавление контейнера: [(имя, смещение, длина)]. Пусто — не контейнер.

    Три чтения — заголовок, концевик, оглавление, — а не файл целиком: `seg.dat`
    бывает на гигабайты, и класть его в память ради имён кусков незачем. Смысл
    полей — в шапке `container.rs`; здесь только смещения.
    """
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        total = f.tell()
        if total < C_HEAD + C_TAIL:
            return []
        f.seek(0)
        head = f.read(C_HEAD)
        f.seek(total - C_TAIL)
        tail = f.read(C_TAIL)
        if head[:8] != C_MAGIC or tail[28:32] != C_TAIL_MAGIC:
            return []
        count, = struct.unpack_from("<I", head, 16)
        dir_off, = struct.unpack_from("<Q", head, 24)
        dir_len, = struct.unpack_from("<Q", head, 32)
        if dir_off + dir_len > total or dir_len != count * C_ENTRY:
            return []
        f.seek(dir_off)
        table = f.read(dir_len)
    out = []
    for i in range(count):
        e = table[i * C_ENTRY:(i + 1) * C_ENTRY]
        name = e[:C_NAME].split(b"\0", 1)[0].decode("utf-8", "replace")
        off, length = struct.unpack_from("<QQ", e, C_NAME)
        if off + length > dir_off:
            notes.append(f"кусок {name} обещан за пределами кусков — оглавление уже испорчено")
            continue
        out.append((name, off, length))
    return out


def poke_piece(path, off, length, kind):
    """Портит кусок контейнера на месте, не читая файл целиком.

    Портится только тело куска: заголовок, оглавление и концевик остаются
    целыми — иначе отказ пришёл бы с обрамления и до самого куска не дошёл, а
    проверяется ровно обратное.

    Ложь — портить нечем: кусок нулевой длины это законное содержимое.
    """
    if length == 0:
        return False
    at = {"бит в начале": off,
          "бит в середине": off + length // 2,
          "бит в конце": off + length - 1}.get(kind)
    with open(path, "r+b") as f:
        if at is None:
            # Обнуление: длины и смещения, прочтённые как ноль, дают
            # бесконечные циклы и деления на ноль — их ловит именно оно.
            f.seek(off)
            f.write(bytes(length))
            return True
        f.seek(at)
        was = f.read(1)
        if not was:
            return False
        f.seek(at)
        # Переворачивается старший бит: он меняет значение сильнее всего и не
        # превращает одну годную длину в другую годную.
        f.write(bytes([was[0] ^ 0x80]))
    return True


def corrupt_pieces(seg):
    """Прогоняет порчу по каждому куску контейнера."""
    src = os.path.join(GOLD, seg, CONTAINER)
    table = pieces(src)
    if not table:
        print(f"{seg}/{CONTAINER}: оглавление не прочлось — сегмент рассыпан либо уже испорчен\n")
        return
    print(f"порчу куски контейнера {seg}/{CONTAINER}: {len(table)} штук\n")
    for name, off, length in table:
        for kind in ("бит в начале", "бит в середине", "бит в конце", "обнулён"):
            with_copy()
            if not poke_piece(os.path.join(WORK, seg, CONTAINER), off, length, kind):
                notes.append(f"{name} — {kind}: кусок пуст, портить нечем")
                continue
            tag = f"{CONTAINER}[{name}] — {kind}"
            v = probe(tag)
            mark = "✗" if ("УПАЛ" in v or "УБИТ" in v) else "·"
            print(f"  {mark} {tag:44} {v[:80]}")


def main():
    build_gold()
    seg, files = seg_files()
    if CONTAINER in files:
        corrupt_pieces(seg)
    print(f"порчу файлы сегмента {seg}: {len(files)} штук\n")
    random.seed(7)
    for name in files:
        for kind in ("обрезан вдвое", "обрезан до 3 байт", "обнулён", "байты испорчены"):
            with_copy()
            path = os.path.join(WORK, seg, name)
            try:
                data = bytearray(open(path, "rb").read())
            except Exception:
                continue
            if kind == "обрезан вдвое":
                data = data[: len(data) // 2]
            elif kind == "обрезан до 3 байт":
                data = data[:3]
            elif kind == "обнулён":
                data = bytearray(len(data))
            else:
                for _ in range(max(1, len(data) // 50)):
                    if data:
                        data[random.randrange(len(data))] = random.randrange(256)
            open(path, "wb").write(bytes(data))
            tag = f"{name} — {kind}"
            v = probe(tag)
            mark = "✗" if ("УПАЛ" in v or "УБИТ" in v) else "·"
            print(f"  {mark} {tag:34} {v[:90]}")
    missed = [t for t, v in checks if v == "НЕ ЗАМЕТИЛ"]
    print(f"\nслучаев без падения: {ok}, падений: {len(crashes)}")
    print(f"check --full поймал: {len(checks) - len(missed)} из {len(checks)}")
    if missed:
        print("не замечено проверкой:")
        for t in missed:
            print(f"  ~ {t}")
    # Пропущенное называется вслух: молча пропустить кусок значит соврать в
    # итоге, будто проверено больше, чем проверено.
    for n in notes:
        print(f"  – пропущено: {n}")
    for t, v in crashes:
        print(f"  ✗ {t}: {v}")
    return 1 if crashes else 0


if __name__ == "__main__":
    sys.exit(main())
