#!/usr/bin/env python3
"""Синтетический oracle typed filters/source и document/value count.

Только уникальные индексы loopback-стендов GPU; не нагрузочный клиент.
Ожидаемые ответы вычисляются из исходных объектов, не из другого движка.
"""
import argparse
import json
import time
import uuid
from pathlib import Path

from v3_semantic_probe import canonical_json, request


def present(doc, field):
    value = doc.get(field)
    return value is not None and value != []


def values(doc, field):
    value = doc.get(field)
    return [] if value is None else value if isinstance(value, list) else [value]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    index = "svindb-v3-typed-" + uuid.uuid4().hex
    docs = [
        {"id": "a", "body": "alpha", "rank": -9, "tags": ["red", "blue"], "nums": [1, 1, 2]},
        {"id": "b", "body": "alpha beta", "rank": 0, "tags": ["blue"], "nums": [3]},
        {"id": "c", "body": "beta", "rank": 9007199254740992, "tags": [], "nums": []},
        {"id": "d", "body": "gamma", "rank": 9007199254740993, "tags": None, "nums": None},
        {"id": "e", "body": "alpha", "rank": None, "tags": ["red"]},
        {"id": "f", "body": "gamma", "tags": [""], "nums": [4, 5]},
        {"id": "g", "body": "alpha", "rank": -9223372036854775808, "tags": ["end"]},
        {"id": "h", "body": "gamma", "rank": 9223372036854775807},
    ]
    mapping = {"mappings": {"dynamic": "strict", "properties": {
        "id": {"type": "keyword"}, "body": {"type": "text"},
        "rank": {"type": "long"}, "tags": {"type": "keyword"},
        "nums": {"type": "long"},
    }}}
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
    scenarios = [
        ("all", {"match_all": {}}, lambda d: True),
        ("rank_gt_2pow53", {"range": {"rank": {"gt": 9007199254740992}}},
         lambda d: present(d, "rank") and d["rank"] > 9007199254740992),
        ("rank_negative", {"range": {"rank": {"lt": 0}}},
         lambda d: present(d, "rank") and d["rank"] < 0),
        ("rank_exists", {"exists": {"field": "rank"}}, lambda d: present(d, "rank")),
        ("tags_exists", {"exists": {"field": "tags"}}, lambda d: present(d, "tags")),
        ("tags_any", {"terms": {"tags": ["red", "blue"]}},
         lambda d: any(t in values(d, "tags") for t in ["red", "blue"])),
        ("tags_both", {"bool": {"must": [{"term": {"tags": "red"}}, {"term": {"tags": "blue"}}]}},
         lambda d: all(t in values(d, "tags") for t in ["red", "blue"])),
        ("rank_missing_or_null", {"bool": {"must_not": [{"exists": {"field": "rank"}}]}},
         lambda d: not present(d, "rank")),
        ("numeric_multivalue", {"range": {"nums": {"gte": 2, "lte": 4}}},
         lambda d: any(2 <= n <= 4 for n in values(d, "nums"))),
    ]
    rows = []
    sources = {doc["id"]: doc for doc in docs}
    for name, query, predicate in scenarios:
        expected = sorted(d["id"] for d in docs if predicate(d))
        body = {"size": 20, "query": query, "_source": True}
        try:
            es = request(19225, "POST", f"/{index}/_search", {**body, "track_total_hits": True})
            sv = request(19325, "POST", f"/{index}/_search", {**body,
                "svindb": {"partial": False}, "aggs": {"matches": {"count": {}}}})
            es_hits, sv_hits = es["hits"]["hits"], sv["hits"]
            es_ids = sorted(h["_id"] for h in es_hits)
            sv_ids = sorted(h.get("_id", h.get("id")) for h in sv_hits)
            counts = [es["hits"]["total"]["value"], sv["aggs"]["matches"]["count"]]
            source_ok = all(canonical_json(h["_source"]) == canonical_json(sources[h.get("_id", h.get("id"))])
                            for h in es_hits + sv_hits)
            complete = (sv.get("complete") is True and not sv.get("partial")
                        and not es.get("timed_out") and not es["_shards"]["failed"])
            rows.append({"name": name, "expected": expected, "es_ids": es_ids,
                         "sv_ids": sv_ids, "counts": counts, "source_ok": source_ok,
                         "passed": es_ids == sv_ids == expected
                         and counts == [len(expected)] * 2 and source_ok and complete})
        except (RuntimeError, OSError, ValueError, KeyError) as error:
            rows.append({"name": name, "passed": False, "error": str(error)})
    for field in ["rank", "nums"]:
        expected_values = [v for d in docs for v in values(d, field)]
        query = {"size": 0, "query": {"match_all": {}},
                 "aggs": {"values": {"stats": {"field": field}}}}
        try:
            es = request(19225, "POST", f"/{index}/_search", {**query, "track_total_hits": True})
            sv = request(19325, "POST", f"/{index}/_search", {**query, "svindb": {"partial": False}})
            expected_stats = {"count": len(expected_values), "min": min(expected_values),
                              "max": max(expected_values), "sum": sum(expected_values)}
            # ES суммирует через double; сравнение точных Int64 sum выше 2^53
            # требует отдельного договора. Здесь проверяем count/min/max,
            # а sum сравниваем только на заведомо точно представимом nums.
            keys = ["count", "min", "max", "sum"] if field == "nums" else ["count"]
            es_stats = {k: es["aggregations"]["values"][k] for k in keys}
            sv_stats = {k: sv["aggs"]["values"][k] for k in keys}
            expected_stats = {k: expected_stats[k] for k in keys}
            rows.append({"name": "value_stats_" + field, "expected": expected_stats,
                         "es": es_stats, "sv": sv_stats,
                         "passed": expected_stats == es_stats == sv_stats
                         and sv.get("complete") is True and not sv.get("partial")
                         and not es.get("timed_out") and not es["_shards"]["failed"]})
        except (RuntimeError, OSError, ValueError, KeyError) as error:
            rows.append({"name": "value_stats_" + field, "passed": False, "error": str(error)})
    for field in ["tags", "nums"]:
        keys = sorted({v for doc in docs for v in values(doc, field)})
        expected = [{"key": key, "doc_count": sum(key in values(doc, field) for doc in docs)} for key in keys]
        body = {"size": 0, "query": {"match_all": {}},
                "aggs": {"values": {"terms": {"field": field, "size": 100}}}}
        try:
            es = request(19225, "POST", f"/{index}/_search", body)
            sv = request(19325, "POST", f"/{index}/_search", {**body, "svindb": {"partial": False}})
            def buckets(aggregation):
                return sorted(({"key": b["key"], "doc_count": b["doc_count"]}
                               for b in aggregation["buckets"]), key=lambda b: b["key"])
            es_terms, sv_terms = es["aggregations"]["values"], sv["aggs"]["values"]
            es_values, sv_values = buckets(es_terms), buckets(sv_terms)
            rows.append({"name": "terms_" + field, "expected": expected, "es": es_values, "sv": sv_values,
                         "passed": expected == es_values == sv_values
                         and es_terms["doc_count_error_upper_bound"] == sv_terms["doc_count_error_upper_bound"] == 0
                         and sv.get("complete") is True and not sv.get("partial")
                         and not es.get("timed_out") and not es["_shards"]["failed"]})
        except (RuntimeError, OSError, ValueError, KeyError) as error:
            rows.append({"name": "terms_" + field, "passed": False, "error": str(error)})
    for field in ["rank", "nums"]:
        for direction in ["asc", "desc"]:
            desc = direction == "desc"
            def key(doc):
                vs = values(doc, field)
                value = (max(vs) if desc else min(vs)) if vs else None
                return (value is None, 0 if value is None else -value if desc else value, doc["id"])
            expected = [d["id"] for d in sorted(docs, key=key)]
            body = {"size": 20, "query": {"match_all": {}}, "_source": True,
                    "sort": [{field: {"order": direction}}]}
            try:
                es = request(19225, "POST", f"/{index}/_search", {**body, "track_total_hits": True})
                sv = request(19325, "POST", f"/{index}/_search", {**body, "svindb": {"partial": False}})
                es_ids = [h["_id"] for h in es["hits"]["hits"]]
                sv_ids = [h.get("_id", h.get("id")) for h in sv["hits"]]
                rows.append({"name": f"sort_{field}_{direction}", "expected": expected,
                             "es_ids": es_ids, "sv_ids": sv_ids,
                             "es_oracle_match": es_ids == expected, "sv_oracle_match": sv_ids == expected,
                             "comparable": es_ids == expected,
                             "passed": es_ids == sv_ids == expected and sv.get("complete") is True
                             and not sv.get("partial") and not es.get("timed_out") and not es["_shards"]["failed"]})
            except (RuntimeError, OSError, ValueError, KeyError) as error:
                rows.append({"name": f"sort_{field}_{direction}", "passed": False, "error": str(error)})
    result = {"synthetic": True, "index": index, "time": time.time(), "checks": rows,
              "versions": {"es": request(19225, "GET", "/"),
                           "svindb": request(19325, "GET", "/_stats")}}
    with args.out.open("x") as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
    print(json.dumps({"passed": sum(r["passed"] for r in rows), "total": len(rows),
                      "artifact": str(args.out)}))
    raise SystemExit(0 if all(r["passed"] for r in rows) else 1)


if __name__ == "__main__":
    main()
