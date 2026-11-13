#!/usr/bin/env python3
"""Ограниченный smoke поиска: сохраняет задержки и хеши, но не документы."""
import argparse
import concurrent.futures
import hashlib
import http.client
import json
import time
import threading

LOCAL = threading.local()


def request(port, body):
    started = time.perf_counter()
    if not hasattr(LOCAL, "connection"):
        LOCAL.connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    connection = LOCAL.connection
    try:
        connection.request("POST", "/", json.dumps(body), {"Content-Type": "application/json"})
        response = connection.getresponse()
        raw = response.read(64 * 1024 * 1024 + 1)
        value = json.loads(raw)
        stable = {key: value.get(key) for key in ("hits", "aggs", "aggregations", "total", "complete", "partial", "terminated")}
        return {"status": response.status, "ms": (time.perf_counter()-started)*1000,
                "bytes": len(raw), "hits": len(value.get("hits", [])),
                "digest": hashlib.sha256(json.dumps(stable, sort_keys=True).encode()).hexdigest(),
                "complete": value.get("complete"), "partial": value.get("partial"),
                "terminated": value.get("terminated"), "error": value.get("error"),
                "keys": sorted(value), "aggs": value.get("aggs", value.get("aggregations"))}
    except Exception as error:
        connection.close()
        del LOCAL.connection
        return {"status": "transport_error", "ms": (time.perf_counter()-started)*1000, "error": str(error)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=19301)
    parser.add_argument("--peers", action="store_true")
    parser.add_argument("--repetitions", type=int, default=1000)
    args = parser.parse_args()
    cases = {
        "literal_news": {"k": 10, "words": [{"field": "text", "terms": ["новости"]}], "ids": True},
        "boolean_two_terms": {"k": 10, "must": [{"field": "text", "terms": ["новости"]}, {"field": "text", "terms": ["москва"]}], "ids": True},
        "prefix_exact_count": {"k": 10, "words": [{"field": "text", "terms": ["новост*"]}], "max_expansions": 200, "ids": True, "aggs": {"matches": {"count": {}}}},
        "numeric_sorted": {"k": 50, "filter": [{"field": "counters.views", "from": 1000}], "sort": [{"field": "date", "order": "desc"}], "ids": True},
        "count_stats": {"k": 0, "words": [{"field": "text", "terms": ["новости"]}], "aggs": {"matches": {"count": {}}, "views": {"stats": {"field": "counters.views"}}}},
    }
    if args.peers:
        cases = {
            "title_literal": {"k": 10, "words": [{"field": "title", "terms": ["новости"]}], "ids": True},
            "title_prefix_count": {"k": 10, "words": [{"field": "title", "terms": ["новост*"]}], "max_expansions": 200, "ids": True, "aggs": {"matches": {"count": {}}}},
            "title_boolean": {"k": 10, "must": [{"field": "title", "terms": ["новости"]}, {"field": "title", "terms": ["москва"]}], "ids": True},
            "title_count_stats": {"k": 0, "words": [{"field": "title", "terms": ["новости"]}], "aggs": {"matches": {"count": {}}, "members": {"stats": {"field": "participants_count"}}}},
        }
    for name, body in cases.items():
        body["fields"] = ["title"] if args.peers else ["text"]
        first = request(args.port, body)
        print(json.dumps({"case": name, "phase": "first", "body": body, "result": first}, ensure_ascii=False), flush=True)
        if first["status"] != 200:
            continue
        for concurrency in (1, 4):
            started = time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                rows = list(pool.map(lambda _: request(args.port, body), range(args.repetitions)))
            print(json.dumps({"case": name, "phase": "repeat", "concurrency": concurrency,
                              "elapsed_s": time.perf_counter()-started, "results": rows}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
