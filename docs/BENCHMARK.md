# SvinDB vs Elasticsearch: measured comparison

**Status of these numbers.** SvinDB is an early prototype. This is one machine, one synthetic corpus and 3 run(s) of the same test; it shows where the prototype stands today and is not a general verdict on either engine.

## Setup

|  |  |
|---|---|
| Machine | macOS-26.3.1-arm64-arm-64bit, 10 CPUs, 16 GiB RAM |
| SvinDB | 3.0.0-alpha.3, release build, default settings, `SVINDB_DISK_FLOOR_MB=1` (see notes) |
| Elasticsearch | 9.2.3, native macOS build, 1 node, heap 4 GiB (fixed), 1 shard, 0 replicas, `refresh_interval=-1` during load, disk watermarks off (see notes) |
| Corpus | 1,000,000 synthetic documents, 386 MB of NDJSON (title, body, keyword category and author, long, double, date); the same bytes are sent to both engines through the Elasticsearch `_bulk` API |
| Client | Python standard library, one persistent connection per client thread, ingest with 4 threads and batches of 5000 documents |
| Runs | 3 (median shown; largest spread of any p50 between runs: 41%; a difference inside the spread is reported as “about the same”) |
| Background load | 1-minute load average at the start of each run: 3.2, 5.9, 6.4. The machine also ran unrelated work, roughly one busy core |

## Correctness check

A speed number is only worth reading next to proof that both engines answer the same question. For every query family the exact hit count, and for aggregations the sums, minimum, maximum and buckets, were compared on several query variants. A comparison where both sides return zero is rejected.

**14 of 14 query families returned identical results** (`term / rare`, `term / medium`, `term / frequent`, `OR of two terms`, `AND frequent + medium`, `AND frequent + frequent`, `phrase (two words)`, `term + range filter`, `term, sort by views`, `range count (size 0)`, `terms agg on category`, `stats agg, all docs`, `terms agg, high cardinality`, `date histogram (month)`).

Top-10 relevance lists are not expected to be identical because the BM25 variants differ; the average overlap of the first ten ids was 96%.

## Ingest and resource use while loading

|  | SvinDB | Elasticsearch | SvinDB vs Elasticsearch |
|---|---:|---:|---|
| Time until searchable (s) | 45.4 | 13.4 | 3.4× slower |
| Documents per second until searchable | 22,018 | 74,710 | 3.4× lower |
| CPU time used (CPU-seconds) | 79 | 42 | 1.9× more |
| Average CPU cores busy | 1.7 | 3.9 | 2.2× less |
| Bytes written to disk (MiB) | 1,383 | 1,076 | 1.3× more |
| Peak physical footprint (MiB) | 720 | 4,512 | 6.3× less |
| Peak resident memory (MiB) | 724 | 3,018 | 4.2× less |
| Threads | 98 | 110 |  |

## Size on disk

|  | SvinDB | Elasticsearch | SvinDB vs Elasticsearch |
|---|---:|---:|---|
| Right after loading, allocated (MiB) | 408 | 349 | 1.2× more |
| Settled, allocated (MiB) | 335 | 346 | about the same |
| Settled, logical file sizes (MiB) | 293 | 346 | 1.2× less |
| Segments after settling | 13 | 1 |  |
| Index size ÷ raw corpus (settled, allocated) | 0.91 | 0.94 |  |

“Settled” means: SvinDB after its background merging went idle and the write-ahead log was trimmed; Elasticsearch after `_forcemerge` to one segment, which is its best case and costs 7 CPU-seconds and 0.68 GiB of extra writes.

## Query latency and server CPU per request

One client, one request at a time, query variants rotated, Elasticsearch request cache disabled, 100 measured requests per family after warm-up. Latency includes the HTTP round trip and the client; values near 0.1–0.2 ms are mostly that overhead. Lower is better.

| Query | SvinDB p50 | ES p50 | SvinDB p99 | ES p99 | SvinDB CPU ms/req | ES CPU ms/req | p50: SvinDB vs ES |
|---|---:|---:|---:|---:|---:|---:|---|
| term / rare | 0.18 | 0.97 | 0.23 | 1.67 | 0.10 | 2.69 | 5.4× faster |
| term / medium | 0.22 | 1.01 | 0.28 | 1.72 | 0.14 | 3.78 | 4.5× faster |
| term / frequent | 0.30 | 1.02 | 0.52 | 2.10 | 1.10 | 3.50 | 3.4× faster |
| OR of two terms | 0.32 | 1.37 | 1.07 | 5.13 | 0.22 | 14.28 | 4.3× faster |
| AND frequent + medium | 0.67 | 1.09 | 0.89 | 1.79 | 2.40 | 3.08 | 1.6× faster |
| AND frequent + frequent | 3.75 | 3.71 | 4.51 | 4.37 | 13.59 | 5.29 | about the same |
| phrase (two words) | 0.39 | 0.47 | 0.56 | 0.62 | 0.32 | 0.85 | about the same |
| term + range filter | 0.82 | 0.92 | 0.96 | 1.35 | 0.73 | 1.96 | about the same |
| term, sort by views | 0.96 | 0.76 | 1.57 | 0.92 | 0.90 | 1.50 | 1.3× slower |
| range count (size 0) | 1.44 | 0.28 | 2.20 | 0.37 | 6.30 | 0.53 | 5.1× slower |
| terms agg on category | 0.64 | 0.66 | 1.01 | 0.87 | 1.68 | 1.40 | about the same |
| stats agg, all docs | 5.55 | 9.14 | 6.26 | 10.46 | 17.58 | 10.89 | 1.6× faster |
| terms agg, high cardinality | 83.65 | 6.30 | 94.89 | 7.80 | 155.01 | 8.15 | 13.3× slower |
| date histogram (month) | 0.42 | 0.91 | 0.55 | 1.10 | 1.62 | 1.81 | 2.1× faster |

*phrase (two words):* ES match_phrase; SvinDB native phrases (the ES dialect has no match_phrase).

## Point reads (GET by id)

|  | SvinDB | Elasticsearch | SvinDB vs Elasticsearch |
|---|---:|---:|---|
| p50 (ms) | 0.168 | 0.323 | 1.9× faster |
| p99 (ms) | 0.731 | 0.680 | about the same |
| Server CPU (ms/request) | 0.197 | 0.428 | 2.2× less |

## Mixed concurrent load

8 clients for 30 s: 85% searches picked at random from the families above (high-cardinality terms aggregation excluded), 15% point reads. Higher throughput is better.

|  | SvinDB | Elasticsearch | SvinDB vs Elasticsearch |
|---|---:|---:|---|
| Requests per second | 1,406 | 2,899 | 2.1× lower |
| p50 (ms) | 2.25 | 1.14 | 2.0× slower |
| p99 (ms) | 42.28 | 21.86 | 1.9× slower |
| Average CPU cores busy | 5.5 | 7.3 | 1.3× less |
| CPU per request (ms) | 3.90 | 2.52 | 1.6× more |
| Peak resident memory (MiB) | 129 | 2,958 | 23.0× less |
| Peak physical footprint (MiB) | 69 | 4,581 | 66.7× less |
| Errors | 0 | 0 |  |

## Start-up and idle memory

|  | SvinDB | Elasticsearch | SvinDB vs Elasticsearch |
|---|---:|---:|---|
| Restart until the first query is answered (s) | 0.36 | 13.67 | 38.0× faster |
| Footprint right after restart and one query (MiB) | 25 | 4,468 | 180.6× less |
| Footprint idle after loading (MiB) | 261 | 4,530 | 17.4× less |
| Resident memory idle after loading (MiB) | 103 | 572 | 5.5× less |

## How to read this, and what it does not show

- **Memory is counted two ways on purpose.** Physical footprint is memory owned by the process (heap and anonymous pages). Resident memory also includes mapped index files. SvinDB reads its index through `mmap`, so its working set sits in the operating system page cache and is largely missing from its footprint; resident memory is the fairer figure for it. Elasticsearch has a fixed 4 GiB heap that I chose, and a smaller heap would lower its footprint.
- **Warm cache only.** macOS cannot drop the page cache without administrator rights, so these are warm-cache numbers. The restart row restarts the process, not the disk cache.
- **Both safety valves were switched off.** The machine has little free disk, and both engines refuse writes in that state by default (`SVINDB_DISK_FLOOR_MB` for SvinDB, `cluster.routing.allocation.disk.threshold_enabled` for Elasticsearch). Both were relaxed for the test only.
- **Not production settings for either engine.** Elasticsearch is a single shard without replicas and with refresh paused during the load; SvinDB runs with defaults. Durability defaults were kept on both sides (acknowledged writes are flushed to disk).
- **Synthetic corpus, one machine, one background process.** Text is generated from a Zipf-distributed vocabulary of 30,000 words, so the term frequencies are realistic in shape but the language is not. The machine also ran unrelated background work, roughly one busy core.
- **Ingest used the Elasticsearch-compatible `_bulk` route on SvinDB**, which converts every document. This is the path migrating clients would use, but SvinDB's own bulk format was not measured.

## Reproduce

```bash
cargo build --release --locked -p svindb-server
python3 -I tools/bench_vs_es.py --docs 1000000 \
  --svindb target/release/svindb --es-home /path/to/elasticsearch-9.2.3 \
  --workdir /path/to/scratch --output run1.json
python3 tools/bench_vs_es_report.py --lang en run1.json run2.json run3.json > docs/BENCHMARK.md
```

The script starts both servers itself, needs roughly 3 GiB of scratch space, and writes every raw number to the JSON file.
