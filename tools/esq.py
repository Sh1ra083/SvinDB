#!/usr/bin/env python3
"""Тот же набор запросов к Elasticsearch. Формы подобраны так, чтобы ES решал
ту же задачу, а не более простую.

`track_total_hits: false` — потому что SvinDB тоже не считает все совпадения:
он отдаёт верхушку. Оставить полный подсчёт значило бы заставить ES делать
работу, которой у SvinDB нет, и выигрыш был бы измерением этой разницы.
"""
import sys
from bench import bench, show

F = "message"
def T(w, boost=None):
    d = {"term": {F: {"value": w}}}
    return d

def Q(body, k=10):
    return {"size": k, "track_total_hits": False, "query": body, "_source": False}

QUERIES = [
    ("два слова",         Q({"bool": {"should": [T("кўрсатилган"), T("ҳодиса")]}})),
    ("три слова",         Q({"bool": {"should": [T("кўрсатилган"), T("ҳодиса"), T("эмасми")]}})),
    ("редкое слово",      Q(T("грустная"))),
    ("маска",             Q({"prefix": {F: "резул"}})),
    ("частое слово",      Q(T("https"))),
    ("must + must_not",   Q({"bool": {"must": [T("https")], "must_not": [T("me")]}})),
    ("глубокая k=1000",   Q(T("https"), k=1000)),
    ("частое, k=100",     Q(T("https"), k=100)),
    ("два частых",        Q({"bool": {"should": [T("https"), T("не")]}})),
    ("must два частых",   Q({"bool": {"must": [T("https"), T("не")]}})),
    ("must три частых",   Q({"bool": {"must": [T("https"), T("не"), T("в")]}})),
    ("отбор по числу",    Q({"bool": {"must": [T("https")],
                                      "filter": [{"range": {"views": {"gte": 1000}}}]}})),
    ("сортировка по ts",  dict(Q(T("https")), sort=[{"ts": "desc"}])),
]

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 19200
    index = sys.argv[2] if len(sys.argv) > 2 else "bench"
    reps = int(sys.argv[3]) if len(sys.argv) > 3 else 15
    show(f"Elasticsearch /{index}",
         bench("localhost", port, QUERIES, reps=reps, warm=5,
               path=f"/{index}/_search", es=True))
