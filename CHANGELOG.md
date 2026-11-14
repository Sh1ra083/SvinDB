# Changelog

## 3.0.0-alpha.4 — 2026-11-14

This prerelease improves ingest and concurrent search on a single node. It does
not declare the remaining schema, client-compatibility or cluster roadmap complete.

### Ingest

- Prepare explicit-ID replacements in detached fragments before acquiring the
  shared ingest lock; preserve repeated IDs within each batch and WAL apply order.
- Allow unconditional writers to prepare and group durable commits concurrently.
  Conditional updates and mixed mutations exclude intervening writes while their
  predicates are checked and applied. CAS remains atomic.
- Avoid waiting for a background flush when its retained ID hashes prove the
  replacement ID absent. A hash collision only adds a conservative wait.
- Use slicing-by-8 IEEE CRC32 with the same incremental checksums and WAL bytes.
- Avoid constructing and discarding flat document copies for lossless WAL input.
- Apply deferred supersedes when an atomic WAL batch or mixed mutation publishes
  a segment during recovery, preventing duplicate live versions after a crash
  between segment publication and checkpoint.

### Search and aggregation

- Charge one CPU unit per interactive search by default; keep explicit
  configuration limits and the independent analytics/control/point-read lanes.
- Share a 25 ms pressure snapshot and skip worker I/O accounting when statistics
  are disabled. Read published ingest-memory accounting without its mutex.
- Count eligible scalar numeric ranges using existing block bounds and bucket summaries;
  tombstones, multivalued columns and incompatible snapshots retain exact scans.
- Let `size:0` avoid the top-K heap while preserving exact/threshold total-hit
  requests and aggregation results, including searches without collectors.
- Visit selected keyword ordinals sequentially and merge exact byte keys;
  materialize strings only for the final terms buckets. Lexical ties, multivalue
  deduplication, missing values and the existing memory-safety limit are preserved.

### Validation and delivery

- Start the short incomplete-header deadline after the first HTTP bytes arrive,
  including delayed and already-buffered input; retain the idle keep-alive
  timeout and avoid treating TLS handshake records as header progress.
- Added regression coverage for shared write commits, nonblocking memory
  accounting, delayed flush replacement, detached replacement equivalence,
  range summaries, dictionary traversal and exact cross-segment terms.
- Make concurrency fixtures wait for verified reader progress under ongoing
  writes; bound replication load independently of admission-queue saturation.
- Added Criterion benchmarks and explicit matching refresh modes for comparison
  tools, plus Linux profiling instructions.
- Package product documentation from an allowlist. Generated benchmark reports,
  raw measurement JSON, baseline snapshots and local working notes are excluded
  from source archives and release packages.
- Build Linux x86_64 (Ubuntu 22.04 / glibc 2.35) and macOS arm64 archives from the
  same release reference, with checksums and extracted-binary functional smoke.

### Measured local changes

Three paired native release runs on one macOS arm64 host, 100,000 documents,
1,000 documents per batch, reused HTTP connections, refresh and live refresh
disabled during loading, and automatic maintenance disabled in both versions.
Values below are medians of three runs; mixed search uses eight clients and
three query shapes. All runs verified the final exact count and had no search
errors. These numbers compare the source base with this release candidate,
not Elasticsearch or another platform.

| Workload | Before | After |
|---|---:|---:|
| Explicit-ID ingest, 1 client | 54,624 docs/s | 64,322 docs/s |
| Explicit-ID ingest, 4 clients | 61,193 docs/s | 127,246 docs/s |
| Explicit-ID ingest, 8 clients | 60,777 docs/s | 172,370 docs/s |
| Load to searchable, 8 ingest clients | 1.714 s | 0.683 s |
| Mixed search throughput, 8 clients | 1,957 requests/s | 2,674 requests/s |
| Mixed search p99 | 15.04 ms | 9.89 ms |
| Exact numeric range count p50 | 0.574 ms | 0.270 ms |
| Keyword terms p50, 40,000 distinct values | 15.71 ms | 6.97 ms |

Reproduce with `tools/compare_versions.py` using two release binaries and an
external `--output` path. Raw reports and working measurements are not included
in the repository or release assets. Parity targets relative to Elasticsearch
are not established by these runs.

### Compatibility

Existing WAL and segment formats are unchanged. Existing indexes do not require
reindexing for these improvements. This is still an alpha release: the public API,
schema evolution and distributed-operation restrictions in the README remain.
Exact terms/cardinality still reject requests exceeding their documented safety
limit; no silent approximation or truncation was introduced.

Performance results describe only their stated workload and platform. No new
claim of parity with Elasticsearch, production capacity, Linux/macOS equivalence
or completion of the production roadmap is made by this release.
