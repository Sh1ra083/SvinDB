#!/usr/bin/env python3
"""Функциональный прогон по всей поверхности SvinDB через HTTP.

Не микротесты ядра — их 412 и они про внутренности. Здесь проверяется то, что
видит клиент: каждая возможность вызывается так, как её вызвал бы человек, и
ответ сверяется с ожидаемым по существу, а не по форме.

Прогон идёт на **отдельном** индексе со своей схемой, чтобы не портить корпус
замеров и чтобы каждая проверка знала, что именно лежит внутри.
"""
import http.client, json, subprocess, sys, time, os, shutil

PORT = int(os.environ.get("FPORT", "19420"))
DIR = os.environ.get("FDIR", "/tmp/sv-func")
BIN = os.environ.get("FBIN", "/tmp/svindb-b2")

ok = fail = 0
failures = []

def call(method, path, body=None, port=None, raw=False):
    c = http.client.HTTPConnection("localhost", port or PORT, timeout=120)
    if body is None:
        b = None
    elif raw:
        b = body
    else:
        b = json.dumps(body).encode()
    c.request(method, path, b, {"Content-Type": "application/json"})
    r = c.getresponse(); data = r.read(); c.close()
    try:
        return r.status, json.loads(data)
    except Exception:
        return r.status, data

def check(name, cond, detail=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"  ✓ {name}")
    else:
        fail += 1
        failures.append(f"{name}: {detail}")
        print(f"  ✗ {name}  {detail}")

def hits(r):
    return r[1].get("hits", []) if isinstance(r[1], dict) else []

def ids(r, field="id"):
    return [h.get("id") for h in hits(r)]

# ---------------------------------------------------------------- подготовка
def boot():
    subprocess.run(["pkill", "-f", f"serve {DIR}"], capture_output=True)
    time.sleep(1)
    shutil.rmtree(DIR, ignore_errors=True)
    os.makedirs(DIR, exist_ok=True)
    subprocess.Popen(
        [BIN, "serve", DIR, str(PORT), "--single-index", "--flush=100000",
         "--fields=title:2,body:1,цена:int64,дата:int64,метка:keyword"],
        stdout=open(f"/tmp/func-{PORT}.log", "w"), stderr=subprocess.STDOUT,
        start_new_session=True)
    for _ in range(60):
        try:
            st, _ = call("GET", "/_health")
            if st == 200: return True
        except Exception: pass
        time.sleep(0.5)
    return False

DOCS = [
    {"id": "d1", "title": "красный квадрат", "body": "первое тело общее слово",
     "цена": 100, "дата": 1700000000, "метка": "Alpha Beta"},
    {"id": "d2", "title": "синий круг", "body": "второе тело общее",
     "цена": 250, "дата": 1700000100, "метка": "gamma"},
    {"id": "d3", "title": "зелёный квадрат", "body": "третье тело общее слово редкоеслово",
     "цена": 250, "дата": 1700000200, "метка": "Alpha"},
    {"id": "d4", "title": "красный круг", "body": "четвёртое тело",
     "цена": 900, "дата": 1700000300, "метка": "delta"},
]

def main():
    if not boot():
        print("узел не поднялся"); sys.exit(1)

    print("\n== приём и схема")
    st, r = call("POST", "/_bulk", {"documents": DOCS})
    check("bulk принял четыре", st == 200 and r.get("added") == 4, str(r))
    call("POST", "/_flush", {})
    time.sleep(1)
    st, h = call("GET", "/_health")
    check("health видит четыре", h.get("docs") == 4, str(h))

    print("\n== поиск по словам")
    r = call("POST", "/", {"k": 10, "words": [{"field": "body", "terms": ["общее"]}],
                           "fields": [], "ids": True})
    check("одно слово находит три", len(hits(r)) == 3, str(r[1])[:200])

    r = call("POST", "/", {"k": 10, "words": [{"field": "title", "terms": ["красный"]},
                                              {"field": "title", "terms": ["квадрат"]}],
                           "fields": [], "ids": True})
    check("два слова: d1 первым", ids(r)[:1] == ["d1"], str(ids(r)))

    r = call("POST", "/", {"k": 10, "words": [{"field": "body", "terms": ["редкоеслово"]}],
                           "fields": [], "ids": True})
    check("редкое слово находит один", ids(r) == ["d3"], str(ids(r)))

    print("\n== маска и раскрытие")
    r = call("POST", "/", {"k": 10, "words": [{"field": "body", "terms": ["ред*"]}],
                           "fields": [], "ids": True})
    check("маска находит d3", ids(r) == ["d3"], str(ids(r)))

    print("\n== булево дерево")
    r = call("POST", "/", {"k": 10, "must": [{"field": "body", "terms": ["общее"]}],
                           "must_not": [{"field": "body", "terms": ["слово"]}],
                           "fields": [], "ids": True})
    check("must + must_not", sorted(ids(r)) == ["d2"], str(ids(r)))

    r = call("POST", "/", {"k": 10, "query": {"should": [
        {"bool": {"must": [{"field": "title", "terms": ["квадрат"]},
                           {"field": "title", "terms": ["зелёный"]}]}}]},
        "fields": [], "ids": True})
    check("вложенная ветвь", ids(r) == ["d3"], str(ids(r)))

    r = call("POST", "/", {"k": 10, "query": {
        "should": [{"field": "title", "terms": ["красный"]},
                   {"field": "title", "terms": ["круг"]},
                   {"field": "title", "terms": ["квадрат"]}],
        "minimum_should_match": 2}, "fields": [], "ids": True})
    check("minimum_should_match=2", sorted(ids(r)) == ["d1", "d4"], str(ids(r)))

    print("\n== фразы")
    r = call("POST", "/", {"k": 10, "phrases": [
        {"field": "title", "terms": ["красный", "квадрат"], "slop": 0}], "fields": [], "ids": True})
    check("фраза точная", ids(r) == ["d1"], str(ids(r)))
    r = call("POST", "/", {"k": 10, "phrases": [
        {"field": "title", "terms": ["квадрат", "красный"], "slop": 0}], "fields": [], "ids": True})
    check("фраза обратного порядка не совпала", ids(r) == [], str(ids(r)))

    print("\n== числовые поля")
    r = call("POST", "/", {"k": 10, "words": [{"field": "body", "terms": ["тело"]}],
                           "filter": [{"field": "цена", "from": 200, "to": 300}],
                           "fields": [], "ids": True})
    check("отбор по диапазону", sorted(ids(r)) == ["d2", "d3"], str(ids(r)))

    r = call("POST", "/", {"k": 10, "words": [{"field": "body", "terms": ["тело"]}],
                           "sort": [{"field": "цена", "order": "desc"}], "fields": [], "ids": True})
    check("сортировка по убыванию цены", ids(r)[0] == "d4", str(ids(r)))

    r = call("POST", "/", {"k": 10, "filter": [{"field": "цена", "from": 900, "to": 900}],
                           "fields": [], "ids": True})
    check("запрос без слов, только отбор", ids(r) == ["d4"], str(ids(r)))

    print("\n== ключевое слово")
    r = call("POST", "/", {"k": 10, "words": [{"field": "метка", "terms": ["alpha beta"]}],
                           "fields": [], "ids": True})
    check("keyword целиком, без разбора", ids(r) == ["d1"], str(ids(r)))

    print("\n== страницы")
    r = call("POST", "/", {"k": 2, "words": [{"field": "body", "terms": ["тело"]}],
                           "fields": [], "ids": True})
    first = ids(r)
    last = hits(r)[-1]
    r2 = call("POST", "/", {"k": 2, "words": [{"field": "body", "terms": ["тело"]}],
                            "search_after": [last["score"], last["doc"]], "fields": [], "ids": True})
    check("вторая страница не повторяет первую",
          len(set(first) & set(ids(r2))) == 0 and len(ids(r2)) > 0,
          f"{first} затем {ids(r2)}")

    r = call("POST", "/", {"k": 2, "from": 2, "words": [{"field": "body", "terms": ["тело"]}],
                           "fields": [], "ids": True})
    check("from сдвигает выдачу", len(set(first) & set(ids(r))) == 0, f"{first} / {ids(r)}")

    print("\n== выдача содержимого и подсветка")
    r = call("POST", "/", {"k": 1, "words": [{"field": "body", "terms": ["редкоеслово"]}],
                           "ids": True, "fields": ["title", "body", "цена"]})
    f = hits(r)[0].get("fields", {}) if hits(r) else {}
    # Число отдаётся числом, а не строкой: клиенту не нужно знать типы,
    # которые движок и так знает. Прежде здесь ждали "250" строкой.
    check("поля отдаются", f.get("title") == "зелёный квадрат" and f.get("цена") == 250, str(f))
    r = call("POST", "/", {"k": 1, "words": [{"field": "body", "terms": ["редкоеслово"]}],
                           "fields": ["body"], "highlight": {"fields": ["body"], "size": 80}})
    hl = hits(r)[0].get("highlight", {}) if hits(r) else {}
    check("подсветка нашла слово", "редкоеслово" in str(hl) and "<" in str(hl), str(hl))

    print("\n== замена и удаление")
    st, r = call("POST", "/_put", {"documents": [
        {"id": "d2", "title": "синий круг", "body": "второе тело изменённое",
         "цена": 250, "дата": 1700000100, "метка": "gamma"}]})
    # Дисковая версия гасится только вместе с публикацией новой. В момент ACK
    # новая версия ещё числится добавленной, а `replaced` считает лишь версии,
    # уже погашенные прямо в буфере приёма.
    check("put принял замену без преждевременного гашения",
          r.get("added") == 1 and r.get("replaced") == 0, str(r))
    call("POST", "/_flush", {}); time.sleep(1)
    r = call("POST", "/", {"k": 10, "words": [{"field": "body", "terms": ["изменённое"]}],
                           "fields": [], "ids": True})
    check("замена видна поиском", ids(r) == ["d2"], str(ids(r)))
    r = call("POST", "/", {"k": 10, "words": [{"field": "body", "terms": ["общее"]}],
                           "fields": [], "ids": True})
    check("старая копия d2 не находится", "d2" not in ids(r), str(ids(r)))

    st, r = call("POST", "/_delete", {"ids": ["d4"]})
    check("delete погасил один", r.get("deleted") == 1, str(r))
    call("POST", "/_flush", {}); time.sleep(1)
    r = call("POST", "/", {"k": 10, "words": [{"field": "title", "terms": ["круг"]}],
                           "fields": [], "ids": True})
    check("удалённый не находится", "d4" not in ids(r), str(ids(r)))

    print("\n== агрегации")
    st, r = call("POST", "/", {"k": 0, "words": [{"field": "body", "terms": ["тело"]}],
                               "aggs": {"по_цене": {"stats": {"field": "цена"}},
                                        "сколько": {"count": {}}}})
    a = r.get("aggs", {}) if isinstance(r, dict) else {}
    # После удаления d4 живы d1(100), d2(250), d3(250).
    st_ = a.get("по_цене", {})
    check("агрегация stats по цене",
          (st_.get("count"), st_.get("min"), st_.get("max"), st_.get("sum")) == (3, 100, 250, 600),
          str(a)[:200])
    check("агрегация count", a.get("сколько", {}).get("count") == 3, str(a)[:200])

    print("\n== настройки на ходу")
    st, r = call("GET", "/_settings")
    check("настройки читаются", st == 200 and "target_docs" in str(r), str(r)[:150])

    print("\n== отказы вместо молчания")
    st, r = call("POST", "/", {"k": 1, "words": [{"field": 99, "terms": ["a"]}]})
    check("несуществующее поле — отказ", st >= 400 and "error" in str(r), f"{st} {r}")
    st, r = call("POST", "/_bulk", b"not json", raw=True)
    check("негодное тело — отказ", st >= 400, f"{st} {r}")
    st, r = call("POST", "/", {"k": 1, "words": [{"field": 0, "terms": ["a"]}],
                               "search_after": [1.5, 99999999999999999]})
    check("курсор за потолком — отказ", st >= 400, f"{st} {r}")

    print("\n== целость")
    p = subprocess.run([BIN, "check", DIR, "--full"], capture_output=True, text=True)
    check("check --full доволен", p.returncode == 0, p.stdout[-300:] + p.stderr[-300:])

    print(f"\n=== ИТОГО: {ok} прошло, {fail} упало")
    for f in failures:
        print("  ! " + f)
    return 0 if fail == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
