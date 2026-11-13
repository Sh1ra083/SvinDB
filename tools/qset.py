#!/usr/bin/env python3
"""Набор запросов и прогон по SvinDB. Поле 0 — message."""
import sys, json
from bench import bench, show

F = 0
W = lambda *t: {"field": F, "terms": list(t)}

QUERIES = [
    ("два слова",            {"k": 10, "words": [W("кўрсатилган"), W("ҳодиса")]}),
    ("три слова",            {"k": 10, "words": [W("кўрсатилган"), W("ҳодиса"), W("эмасми")]}),
    ("редкое слово",         {"k": 10, "words": [W("грустная")]}),
    ("маска",                {"k": 10, "words": [W("резул*")], "max_expansions": 50}),
    ("частое слово",         {"k": 10, "words": [W("https")]}),
    ("must + must_not",      {"k": 10, "must": [W("https")], "must_not": [W("me")]}),
    ("глубокая верхушка",    {"k": 1000, "words": [W("https")]}),
    ("частое, k=100",        {"k": 100, "words": [W("https")]}),
    ("два частых",           {"k": 10, "words": [W("https"), W("не")]}),
    ("must два частых",      {"k": 10, "must": [W("https"), W("не")]}),
]

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 19400
    reps = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    only = sys.argv[3] if len(sys.argv) > 3 else None
    qs = [q for q in QUERIES if not only or only in q[0]]
    show(f"SvinDB :{port}", bench("localhost", port, qs, reps=reps))
