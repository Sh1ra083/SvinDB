#!/usr/bin/env python3
"""Стенд запросов: одно постоянное соединение, время меряется у клиента.

Меряется лучшее из N повторов после прогрева. Лучшее, а не среднее, — потому
что нас интересует стоимость самой работы, а не шум планировщика поверх неё.
Одинаково для SvinDB и Elasticsearch, иначе сравнивать нечего.
"""
import http.client, json, sys, time, statistics

def bench(host, port, queries, reps=20, warm=3, path="/", hdr=None, es=False):
    out = []
    for name, body in queries:
        raw = json.dumps(body).encode()
        c = http.client.HTTPConnection(host, port, timeout=600)
        p = path(body) if callable(path) else path
        h = {"Content-Type": "application/json"}
        if hdr: h.update(hdr)
        times, last = [], None
        for i in range(warm + reps):
            t0 = time.perf_counter()
            c.request("POST", p, raw, h)
            r = c.getresponse()
            data = r.read()
            t1 = time.perf_counter()
            if i >= warm: times.append((t1 - t0) * 1000)
            last = data
        c.close()
        j = json.loads(last)
        if es:
            total = j.get("hits", {}).get("total", {}).get("value", 0)
            got = len(j.get("hits", {}).get("hits", []))
            extra = f"total={total}"
        else:
            got = len(j.get("hits", []))
            extra = f"visited={j.get('visited','-')}"
        out.append((name, min(times), statistics.median(times), got, extra))
    return out

def show(title, rows):
    print(f"\n=== {title}")
    for name, best, med, got, extra in rows:
        print(f"  {name:<30} {best:8.3f} мс (med {med:7.3f})  hits={got:<6} {extra}")

if __name__ == "__main__":
    pass
