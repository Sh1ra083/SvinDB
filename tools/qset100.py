#!/usr/bin/env python3
"""Набор запросов к SvinDB — тот же по составу, что у esq.py."""
import sys
from bench import bench, show

W = lambda *t: {"field": "message", "terms": list(t)}
BIG = 9223372036854775807

QUERIES = [
    ("два слова",        {"k": 10, "words": [W("кўрсатилган"), W("ҳодиса")]}),
    ("три слова",        {"k": 10, "words": [W("кўрсатилган"), W("ҳодиса"), W("эмасми")]}),
    ("редкое слово",     {"k": 10, "words": [W("грустная")]}),
    ("маска",            {"k": 10, "words": [W("резул*")], "max_expansions": 50}),
    ("частое слово",     {"k": 10, "words": [W("https")]}),
    ("must + must_not",  {"k": 10, "must": [W("https")], "must_not": [W("me")]}),
    ("глубокая k=1000",  {"k": 1000, "words": [W("https")]}),
    ("частое, k=100",    {"k": 100, "words": [W("https")]}),
    ("два частых",       {"k": 10, "words": [W("https"), W("не")]}),
    ("must два частых",  {"k": 10, "must": [W("https"), W("не")]}),
    ("must три частых",  {"k": 10, "must": [W("https"), W("не"), W("в")]}),
    ("отбор по числу",   {"k": 10, "must": [W("https")],
                          "filter": [{"field": "views", "from": 1000, "to": BIG}]}),
    ("сортировка по ts", {"k": 10, "words": [W("https")],
                          "sort": [{"field": "ts", "order": "desc"}]}),
]

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 19410
    reps = int(sys.argv[2]) if len(sys.argv) > 2 else 15
    show(f"SvinDB :{port}", bench("localhost", port, QUERIES, reps=reps, warm=5))
