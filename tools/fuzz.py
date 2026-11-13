#!/usr/bin/env python3
"""Фаззер поверхности HTTP: узел обязан отказывать, а не умирать.

Проверяется три вещи, и все три — свойства узла, а не ответа:
  * процесс жив после каждой посылки (сборка идёт с panic = "abort",
    поэтому любая паника убивает узел вместе со всеми соединениями);
  * память не уходит в гигабайты от одного маленького запроса;
  * ответ приходит за разумное время.

Содержание ответа тут не проверяется: негодный запрос вправе получить и 400,
и пустую выдачу — лишь бы узел остался жив и в себе.
"""
import http.client, json, os, random, subprocess, sys, time

PORT = int(os.environ.get("ZPORT", "19491"))
DIR = os.environ.get("ZDIR", "/tmp/sv-fuzz")
BIN = os.environ.get("ZBIN", "/tmp/svindb-h1")
FIELDS = "title:2,body:1,цена:int64,дата:int64,метка:keyword,v:dense_vector:4"

BIG = 9223372036854775807
LONG = "я" * 5000
DEEP = 200
LEAF_QUERY = b'{"should":[{"field":0,"terms":["\\u043e\\u0431\\u0449\\u0435\\u0435"]}]}'


def boot():
    subprocess.run(["pkill", "-f", f"serve {DIR} "], capture_output=True)
    time.sleep(1)
    subprocess.run(["rm", "-rf", DIR], capture_output=True)
    os.makedirs(DIR, exist_ok=True)
    p = subprocess.Popen([BIN, "serve", DIR, str(PORT), "--single-index", f"--fields={FIELDS}"],
                         stdout=open("/tmp/fuzz-node.log", "w"),
                         stderr=subprocess.STDOUT, start_new_session=True)
    for _ in range(80):
        try:
            c = http.client.HTTPConnection("localhost", PORT, timeout=5)
            c.request("GET", "/_health"); c.getresponse().read(); c.close()
            return p
        except Exception:
            time.sleep(0.25)
    return None


def seed():
    docs = [{"id": f"d{i}", "title": f"док{i} общее", "body": f"тело{i} слово{i % 9} общее",
             "цена": i * 7 % 1000, "дата": 1700000000 + i, "метка": f"м{i % 5}",
             "v": f"{i % 3} {i % 5} 1 0"} for i in range(3000)]
    post("/_bulk", {"documents": docs})
    post("/_flush", {})
    time.sleep(1.5)


def encode_body(body, raw=None):
    """Кодирует обычный объект или принимает заранее собранный JSON."""
    if raw is not None:
        return raw
    if isinstance(body, (bytes, bytearray)):
        return bytes(body)
    return json.dumps(body).encode()


def post(path, body, raw=None, timeout=25):
    c = http.client.HTTPConnection("localhost", PORT, timeout=timeout)
    data = encode_body(body, raw)
    c.request("POST", path, data, {"Content-Type": "application/json"})
    r = c.getresponse(); out = r.read(); c.close()
    return r.status, out


def nest(depth):
    """Дерево ветвей глубиной `depth` — проверка на переполнение стека."""
    q = {"should": [{"field": 0, "terms": ["общее"]}]}
    for _ in range(depth):
        q = {"should": [{"bool": q}]}
    return q


def deep_query_bytes(depth):
    """Собирает глубокий JSON итеративно, не расходуя стек сериализатора."""
    if depth < 0:
        raise ValueError("глубина не может быть отрицательной")
    layer = b'{"should":[{"bool":'
    return b'{"k":10,"query":' + layer * depth + LEAF_QUERY + b'}]}' * depth + b'}'


def cases():
    W = lambda f=0, *t: {"field": f, "terms": list(t) or ["общее"]}
    yield "k огромный", {"k": BIG, "words": [W()]}
    yield "k отрицательный", {"k": -5, "words": [W()]}
    yield "k дробный", {"k": 1.5, "words": [W()]}
    yield "from огромный", {"k": 10, "from": BIG, "words": [W()]}
    yield "size огромный", {"k": 10, "size": BIG, "words": [W()]}
    yield "max_expansions огромный", {"k": 10, "max_expansions": BIG, "words": [W(0, "об*")]}
    yield "маска из одной звезды", {"k": 10, "words": [W(0, "*")], "max_expansions": 10000}
    yield "маска пустая", {"k": 10, "words": [W(0, "")]}
    yield "слово длиной 5000", {"k": 10, "words": [W(0, LONG)]}
    yield "слово 5000 с транслитом", {"k": 10, "translit": True, "max_expansions": 10000,
                                      "words": [W(0, "ye" * 2500)]}
    yield "тысяча слов", {"k": 10, "words": [W(0, f"с{i}") for i in range(1000)]}
    yield "тысяча must", {"k": 10, "must": [W(0, f"с{i}") for i in range(1000)]}
    yield "тысяча must_not", {"k": 10, "words": [W()], "must_not": [W(0, f"с{i}") for i in range(1000)]}
    yield "глубокое дерево", {"k": 10, "query": nest(DEEP)}
    yield "очень глубокое дерево", deep_query_bytes(5000)
    yield "поле отрицательное", {"k": 10, "words": [{"field": -1, "terms": ["a"]}]}
    yield "поле огромное", {"k": 10, "words": [{"field": BIG, "terms": ["a"]}]}
    yield "поле дробное", {"k": 10, "words": [{"field": 1.5, "terms": ["a"]}]}
    yield "terms не массив", {"k": 10, "words": [{"field": 0, "terms": "a"}]}
    yield "terms из чисел", {"k": 10, "words": [{"field": 0, "terms": [1, 2, 3]}]}
    yield "terms пустой", {"k": 10, "words": [{"field": 0, "terms": []}]}
    yield "words не массив", {"k": 10, "words": {"field": 0}}
    yield "boost бесконечный", {"k": 10, "words": [dict(W(), boost=1e308 * 10)]}
    yield "boost NaN строкой", {"k": 10, "words": [dict(W(), boost="nan")]}
    yield "boost отрицательный", {"k": 10, "words": [dict(W(), boost=-1e9)]}
    yield "sort по тексту", {"k": 10, "words": [W()], "sort": [{"field": "body", "order": "desc"}]}
    yield "sort по несуществующему", {"k": 10, "words": [W()], "sort": [{"field": "нет", "order": "desc"}]}
    yield "sort из 50 полей", {"k": 10, "words": [W()],
                               "sort": [{"field": "цена", "order": "asc"} for _ in range(50)]}
    yield "filter from>to", {"k": 10, "words": [W()], "filter": [{"field": "цена", "from": 900, "to": 1}]}
    yield "filter пределы", {"k": 10, "words": [W()],
                             "filter": [{"field": "цена", "from": -BIG - 1, "to": BIG}]}
    yield "filter по тексту", {"k": 10, "words": [W()], "filter": [{"field": "body", "from": 1, "to": 2}]}
    yield "фраза из 500 слов", {"k": 10, "phrases": [{"field": 1, "terms": [f"с{i}" for i in range(500)], "slop": 0}]}
    yield "фраза slop огромный", {"k": 10, "phrases": [{"field": 1, "terms": ["тело1", "общее"], "slop": BIG}]}
    yield "фраза пустая", {"k": 10, "phrases": [{"field": 1, "terms": [], "slop": 0}]}
    yield "search_after мусор", {"k": 10, "words": [W()], "search_after": ["a", "b"]}
    yield "search_after короткий", {"k": 10, "words": [W()], "search_after": [1.5]}
    yield "search_after длинный", {"k": 10, "words": [W()], "search_after": [1.5] * 100}
    yield "highlight по числу", {"k": 10, "words": [W()], "highlight": {"fields": ["цена"], "size": 100}}
    yield "highlight size огромный", {"k": 10, "words": [W()], "highlight": {"fields": ["body"], "size": BIG}}
    yield "aggs 100 штук", {"k": 0, "words": [W()],
                            "aggs": {f"a{i}": {"stats": {"field": "цена"}} for i in range(100)}}
    yield "aggs histogram interval 0", {"k": 0, "words": [W()],
                                        "aggs": {"h": {"histogram": {"field": "цена", "interval": 0}}}}
    yield "aggs terms size огромный", {"k": 0, "words": [W()],
                                       "aggs": {"t": {"terms": {"field": "цена", "size": BIG}}}}
    yield "aggs range 10000 диапазонов", {"k": 0, "words": [W()], "aggs": {"r": {"range": {
        "field": "цена", "ranges": [{"from": i, "to": i + 1} for i in range(10000)]}}}}
    yield "вектор не той длины", {"k": 5, "vector": {"field": "v", "query": [1, 2]}}
    yield "вектор огромный", {"k": 5, "vector": {"field": "v", "query": [1.0] * 100000}}
    yield "вектор с NaN", {"k": 5, "vector": {"field": "v", "query": ["nan", 1, 2, 3]}}
    yield "вектор по тексту", {"k": 5, "vector": {"field": "body", "query": [1, 2, 3, 4]}}
    yield "fields несуществующее", {"k": 5, "words": [W()], "fields": ["нет"]}
    yield "fields 1000 раз", {"k": 5, "words": [W()], "fields": ["body"] * 1000}
    yield "min_should больше числа", {"k": 5, "query": {"should": [W()], "minimum_should_match": 99}}
    yield "min_should отрицательный", {"k": 5, "query": {"should": [W()], "minimum_should_match": -1}}
    yield "пустой объект", {}
    yield "только k", {"k": 10}
    yield "group несуществующий", {"k": 5, "words": [W()], "group": {"field": "нет"}}


BAD_BODIES = [
    (b"", "пустое тело"),
    (b"null", "null"),
    (b"[]", "массив вместо объекта"),
    (b'"stroka"', "строка вместо объекта"),
    (b"{", "оборванный объект"),
    (b'{"k":}', "битый JSON"),
    (b'{"k":1e999}', "число вне double"),
    (b'{"k":1,"words":[{"field":0,"terms":["\xff\xfe"]}]}', "негодный UTF-8"),
    (b'{"k":1,"words":[{"field":0,"terms":["\x00"]}]}', "нулевой байт"),
    (b"[" * 10000, "десять тысяч скобок"),
    (b'{"a":' * 5000 + b"1" + b"}" * 5000, "глубокая вложенность JSON"),
]


def rss(pid):
    try:
        return int(subprocess.run(["ps", "-o", "rss=", "-p", str(pid)],
                                  capture_output=True, text=True).stdout.strip() or 0) // 1024
    except Exception:
        return -1


def alive(p):
    return p.poll() is None


def main():
    p = boot()
    if not p:
        print("узел не поднялся"); return 1
    seed()
    base = rss(p.pid)
    print(f"узел поднят, RSS {base} МБ\n")
    bad = []
    for name, body in list(cases()) + [(n, None) for _, n in []]:
        t0 = time.perf_counter()
        try:
            st, out = post("/", body)
            err = ""
        except Exception as e:
            st, out, err = -1, b"", repr(e)[:80]
        dt = time.perf_counter() - t0
        if not alive(p):
            print(f"  ✗✗ УЗЕЛ УМЕР на «{name}»")
            bad.append((name, "узел умер")); break
        m = rss(p.pid)
        flag = ""
        if m > base + 512: flag = f"  ⚠ RSS {m} МБ (+{m - base})"
        if dt > 5: flag += f"  ⚠ {dt:.1f} с"
        if err: flag += f"  ⚠ {err}"
        if flag: bad.append((name, flag.strip()))
        print(f"  {'⚠' if flag else '·'} {name:34} {st:>4} {dt:7.3f} с {flag}")
    for raw, name in BAD_BODIES:
        t0 = time.perf_counter()
        try:
            st, out = post("/", None, raw=raw); err = ""
        except Exception as e:
            st, out, err = -1, b"", repr(e)[:80]
        dt = time.perf_counter() - t0
        if not alive(p):
            print(f"  ✗✗ УЗЕЛ УМЕР на «{name}»"); bad.append((name, "узел умер")); break
        m = rss(p.pid)
        flag = ""
        if m > base + 512: flag = f"  ⚠ RSS {m} МБ"
        if dt > 5: flag += f"  ⚠ {dt:.1f} с"
        if err: flag += f"  ⚠ {err}"
        if flag: bad.append((name, flag.strip()))
        print(f"  {'⚠' if flag else '·'} {name:34} {st:>4} {dt:7.3f} с {flag}")

    print(f"\nузел {'жив' if alive(p) else 'УМЕР'}, RSS {rss(p.pid)} МБ (было {base})")
    if bad:
        print("\nтребует внимания:")
        for n, f in bad: print(f"  ! {n}: {f}")
    subprocess.run(["pkill", "-f", f"serve {DIR} "], capture_output=True)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
