#!/usr/bin/env python3
"""Заливка NDJSON в SvinDB или Elasticsearch одним и тем же способом.

Меряется **до готовности к поиску**: от первого байта до момента, когда поиск
видит все документы. Поэтому в конце — сброс и ожидание, пока счётчик сойдётся.

  load.py svindb <файл> <порт> <сколько> [потоков] [пачка]
  load.py es     <файл> <порт> <сколько> [потоков] [пачка] [индекс]
"""
import http.client, json, sys, threading, time, queue

def batches(path, limit, size):
    """Пачки строк из файла. Кончился файл — проходим его заново с приставкой.

    Приставка меняет **только** `id`, и это несущее. Содержимое остаётся тем
    же, значит частоты слов растут ровно во столько же раз, во сколько растёт
    корпус, — а обратная частота есть отношение, и она не меняется. Вес слова
    остаётся тем же, что на исходном корпусе; иначе замер мерил бы другую
    задачу.

    Без приставки было бы хуже, чем «неточно»: одинаковые `id` у SvinDB — это
    замена, а не второй документ, и корпус на сто миллионов дал бы индекс на
    двадцать пять. Проверено: прогон без неё залил ровно исходные 25 млн и
    отчитался об успехе.

    Чего приём **не** даёт: словарь не растёт. У настоящих ста миллионов
    уникальных слов было бы больше, и словарные структуры весили бы больше.
    Место на диске отсюда надо читать как нижнюю оценку — одинаково для обеих
    сторон.
    """
    n = 0
    buf = []
    pass_no = 0
    while n < limit:
        empty = True
        with open(path, "rb") as f:
            for line in f:
                if not line.strip():
                    continue
                empty = False
                if pass_no:
                    line = salt(line, pass_no)
                buf.append(line.rstrip(b"\n"))
                n += 1
                if len(buf) == size:
                    yield buf; buf = []
                if n >= limit:
                    break
        if empty:
            # Пустой файл — иначе цикл крутился бы вечно, «заливая» ничего.
            break
        pass_no += 1
    if buf:
        yield buf


def salt(line, pass_no):
    """Приписывает номер прохода к `id`, не трогая остального.

    Правкой байтов, а не разбором JSON: разбор с обратной сборкой на ста
    миллионах строк стоит больше, чем сама заливка, и замер мерил бы Python.
    """
    at = line.find(b'"id"')
    if at < 0:
        return line
    q = line.find(b'"', line.find(b":", at) + 1)
    if q < 0:
        return line
    return line[: q + 1] + f"p{pass_no}".encode() + line[q + 1 :]

def post(host, port, path, body, hdr):
    c = http.client.HTTPConnection(host, port, timeout=900)
    c.request("POST", path, body, hdr)
    r = c.getresponse(); r.read(); c.close()
    return r.status

def run(kind, path, port, limit, threads, size, index="bench"):
    q = queue.Queue(maxsize=threads * 3)
    err = []

    def worker():
        c = http.client.HTTPConnection("localhost", port, timeout=900)
        hdr = {"Content-Type": "application/x-ndjson" if kind == "es" else "application/json"}
        url = f"/{index}/_bulk" if kind == "es" else "/_bulk"
        while True:
            b = q.get()
            if b is None:
                q.task_done(); break
            # Повтор с отступом: потерянная пачка — это молча недосчитанные
            # документы, а замер «до готовности» тогда меряет не то.
            for attempt in range(6):
                try:
                    c.request("POST", url, b, hdr)
                    r = c.getresponse(); data = r.read()
                    if r.status < 300:
                        break
                    err.append((r.status, data[:200]))
                except Exception as e:
                    err.append((0, str(e)[:200]))
                    try: c.close()
                    except Exception: pass
                    c = http.client.HTTPConnection("localhost", port, timeout=900)
                time.sleep(0.2 * (attempt + 1))
            else:
                err.append((-1, "пачка потеряна после шести попыток"))
            q.task_done()
        c.close()

    ws = [threading.Thread(target=worker, daemon=True) for _ in range(threads)]
    for w in ws: w.start()

    t0 = time.perf_counter()
    sent = 0
    for b in batches(path, limit, size):
        if kind == "es":
            body = b"".join(b'{"index":{}}\n' + line + b"\n" for line in b)
        else:
            body = b'{"documents":[' + b",".join(b) + b"]}"
        q.put(body)
        sent += len(b)
    for _ in ws: q.put(None)
    q.join()
    t_sent = time.perf_counter() - t0

    # Готовность к поиску: сброс и ожидание, пока счётчик сойдётся.
    if kind == "es":
        post("localhost", port, f"/{index}/_refresh", b"", {})
        def count():
            c = http.client.HTTPConnection("localhost", port, timeout=900)
            c.request("GET", f"/{index}/_count"); r = c.getresponse()
            j = json.loads(r.read()); c.close(); return j.get("count", 0)
    else:
        post("localhost", port, "/_flush", b"{}", {"Content-Type": "application/json"})
        def count():
            c = http.client.HTTPConnection("localhost", port, timeout=900)
            c.request("GET", "/_health"); r = c.getresponse()
            j = json.loads(r.read()); c.close(); return j.get("docs", 0)

    while True:
        n = count()
        if n >= sent: break
        time.sleep(0.5)
    total = time.perf_counter() - t0
    print(json.dumps({
        "kind": kind, "docs": sent, "sent_s": round(t_sent, 1),
        "ready_s": round(total, 1), "dps": int(sent / total), "errors": err[:3],
    }, ensure_ascii=False))

if __name__ == "__main__":
    kind, path, port, limit = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
    th = int(sys.argv[5]) if len(sys.argv) > 5 else 4
    sz = int(sys.argv[6]) if len(sys.argv) > 6 else 5000
    ix = sys.argv[7] if len(sys.argv) > 7 else "bench"
    run(kind, path, port, limit, th, sz, ix)
