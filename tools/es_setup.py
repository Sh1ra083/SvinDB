#!/usr/bin/env python3
"""Индекс Elasticsearch, сопоставимый с тем, что SvinDB делает всегда.

Сопоставимость — это позиции, частоты терминов и включённый `_source`.
Конфиг без них (`index_options: docs`) честен для сравнения по **месту**, но не
по запросам: без частот BM25 вырождается в постоянный вес, и верхушка из равных
весов берётся первыми k без работы, то есть ES отвечал бы на более простой
вопрос.

`refresh_interval: -1` на время заливки и один явный `_refresh` в конце — это то
же, что `--flush=200000` плюс `/_flush` у SvinDB: обе стороны платят за
готовность к поиску один раз и в одном месте.
"""
import http.client, json, sys

def call(port, method, path, body=None):
    c = http.client.HTTPConnection("localhost", port, timeout=1800)
    b = json.dumps(body).encode() if body is not None else None
    c.request(method, path, b, {"Content-Type": "application/json"})
    r = c.getresponse(); data = r.read(); c.close()
    return r.status, data

def create(port, index, shards=1):
    call(port, "DELETE", f"/{index}")
    body = {
        "settings": {
            "number_of_shards": shards,
            "number_of_replicas": 0,
            "refresh_interval": "-1",
            "index": {"translog": {"durability": "async"}},
        },
        "mappings": {
            "properties": {
                "id":      {"type": "keyword"},
                "message": {"type": "text", "index_options": "positions"},
                "views":   {"type": "long"},
                "ts":      {"type": "long"},
                "channel": {"type": "long"},
            }
        },
    }
    st, data = call(port, "PUT", f"/{index}", body)
    print("create", st, data[:200].decode())

if __name__ == "__main__":
    port = int(sys.argv[1]); index = sys.argv[2]
    shards = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    create(port, index, shards)
