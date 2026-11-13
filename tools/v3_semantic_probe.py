#!/usr/bin/env python3
"""Независимый oracle literal boolean: синтетический корпус, ES и SvinDB.

Пишет только новые индексы с уникальным именем на loopback стендах. Данные
production не нужны. Все ожидаемые ID вычисляются из исходных Python-объектов.
"""
import argparse
import http.client
import json
import math
import random
import time
import uuid
from pathlib import Path


def canonical_json(value):
    """Сравнение source с сохранением массивов и Int/Float формы JSON numbers."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def request(port, method, path, body=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
    try:
        conn.request(method, path, json.dumps(body) if body is not None else None,
                     {"Content-Type": "application/json"})
        response = conn.getresponse()
        payload = json.loads(response.read())
        if response.status not in (200, 201):
            raise RuntimeError(f"{port} {method} {path}: {response.status} {payload}")
        return payload
    finally:
        conn.close()


def matches(query, terms):
    """Независимая рекурсивная семантика bool, без обращения к движкам."""
    if "term" in query:
        return query["term"]["body"] in terms
    boolean = query["bool"]
    must = boolean.get("must", [])
    should = boolean.get("should", [])
    minimum = max(boolean.get("minimum_should_match", 0), int(bool(should) and not must))
    return (all(matches(clause, terms) for clause in must)
            and not any(matches(clause, terms) for clause in boolean.get("must_not", []))
            and sum(matches(clause, terms) for clause in should) >= minimum)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--sparse", action="store_true", help="добавить missing/null/empty text для проверки field statistics")
    args = parser.parse_args()
    index = "svindb-v3-semantic-" + uuid.uuid4().hex
    docs = [
        {"id": "a", "body": "alpha beta"},
        {"id": "b", "body": "alpha alpha beta"},
        {"id": "c", "body": "alpha"},
        {"id": "d", "body": "alpha gamma"},
        {"id": "e", "body": "beta gamma"},
        {"id": "f", "body": "gamma"},
    ]
    if args.sparse:
        docs.extend([{"id": "g"}, {"id": "h", "body": None}, {"id": "i", "body": ""}])
    mapping = {"mappings": {"dynamic": "strict", "properties": {
        "id": {"type": "keyword"}, "body": {"type": "text"}}}}
    es_mapping = {**mapping, "settings": {"number_of_shards": 1,
        "number_of_replicas": 0, "index.codec": "best_compression",
        "index.translog.durability": "request"}}
    request(19225, "PUT", "/" + index, es_mapping)
    request(19325, "PUT", "/" + index, mapping)
    for offset in range(0, len(docs), 2):
        for doc in docs[offset:offset + 2]:
            request(19225, "PUT", f"/{index}/_doc/{doc['id']}", doc)
        request(19325, "POST", f"/{index}/_bulk", {"documents": docs[offset:offset + 2]})
        request(19325, "POST", f"/{index}/_flush")
    request(19225, "POST", f"/{index}/_refresh")
    rows = []
    scenarios = [
        (["alpha", "beta"], [], [], 0),
        (["alpha"], [], ["gamma"], 0),
        ([], ["alpha", "beta"], [], 1),
        ([], ["alpha", "beta"], [], 2),
        (["missing"], ["alpha"], [], 0),
        (["alpha"], ["beta"], [], 0),
        ([], [], ["gamma"], 0),
    ]
    rng = random.Random(30002)
    vocabulary = ["alpha", "beta", "gamma", "missing"]
    for _ in range(96):
        must = rng.sample(vocabulary, rng.randrange(3))
        should = rng.sample(vocabulary, rng.randrange(4))
        excluded = rng.sample(vocabulary, rng.randrange(3))
        if not must and not should and not excluded:
            excluded = ["missing"]
        scenarios.append((must, should, excluded, rng.randrange(len(should) + 1)))
    booleans = [{"must": [{"term": {"body": t}} for t in must],
                   "should": [{"term": {"body": t}} for t in should],
                   "must_not": [{"term": {"body": t}} for t in excluded],
                   "minimum_should_match": minimum}
                for must, should, excluded, minimum in scenarios]
    # Отрицательная ветвь OR может совпасть без единого positive posting.
    # Обязательное положительное условие снаружи меняет допустимый lead,
    # но не право внутренней отрицательной ветви вернуть совпадение.
    for positive in vocabulary:
        for negative in vocabulary:
            alternatives = [{"term": {"body": positive}},
                            {"bool": {"must_not": [{"term": {"body": negative}}]}}]
            booleans.append({"should": alternatives, "minimum_should_match": 1})
            booleans.append({"must": [{"term": {"body": "alpha"}},
                                       {"bool": {"should": alternatives}}]})
    for boolean in booleans:
        expected = sorted(doc["id"] for doc in docs
                          if matches({"bool": boolean}, set((doc.get("body") or "").split())))
        query = {"size": 20, "_source": True, "query": {"bool": boolean}}
        try:
            es = request(19225, "POST", f"/{index}/_search", {**query, "track_total_hits": True})
            sv = request(19325, "POST", f"/{index}/_search", {**query,
                "svindb": {"partial": False}, "aggs": {"matches": {"count": {}}}})
        except (RuntimeError, OSError, ValueError, http.client.HTTPException) as error:
            rows.append({"query": boolean, "expected": expected, "passed": False,
                         "error": str(error)})
            continue
        es_hits = es["hits"]["hits"]
        sv_hits = sv["hits"]
        es_ids = sorted(str(h["_id"]) for h in es_hits)
        sv_ids = sorted(str(h.get("_id", h.get("id"))) for h in sv_hits)
        counts = [es["hits"]["total"]["value"], sv["aggs"]["matches"]["count"]]
        es_scores = {str(h["_id"]): h["_score"] for h in es_hits}
        sv_scores = {str(h.get("_id", h.get("id"))): h["score"] for h in sv_hits}
        scores_match = es_scores.keys() == sv_scores.keys() and all(
            math.isclose(score, sv_scores[key], rel_tol=1e-5, abs_tol=1e-6)
            for key, score in es_scores.items())
        sources = {doc["id"]: doc for doc in docs}
        sources_match = all(canonical_json(h.get("_source")) == canonical_json(sources[str(h.get("_id", h.get("id")))])
                            for h in es_hits + sv_hits)
        passed = (es_ids == sv_ids == expected and counts == [len(expected)] * 2
                  and not es.get("timed_out") and not es.get("_shards", {}).get("failed")
                  and sv.get("complete") is True and not sv.get("partial")
                  and sources_match and scores_match)
        rows.append({"query": boolean, "expected": expected, "es_ids": es_ids,
                     "sv_ids": sv_ids, "counts": counts, "passed": passed,
                     "scores_match": scores_match, "sources_match": sources_match,
                     "es_scores": es_scores, "sv_scores": sv_scores})
    result = {"synthetic": True, "sparse": args.sparse, "index": index, "time": time.time(), "checks": rows,
              "versions": {"es": request(19225, "GET", "/"),
                           "svindb": request(19325, "GET", "/_stats")}}
    with args.out.open("x") as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
    print(json.dumps({"passed": sum(row["passed"] for row in rows), "total": len(rows),
                      "artifact": str(args.out)}))
    raise SystemExit(0 if all(row["passed"] for row in rows) else 1)


if __name__ == "__main__":
    main()
