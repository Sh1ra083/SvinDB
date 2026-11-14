# Changelog

## 3.0.0-alpha.5 — 2026-11-15

This prerelease continues single-node ingest, recovery and compaction work.

### Ingest and recovery

- Stream flat and cell-based document encodings into the WAL batch buffer. Avoid
  per-document body buffers, copied cell strings and temporary decimal strings;
  preserve byte-for-byte ADD/PUT/BATCH/MUTATION framing and checksums.
- Reuse checked immutable segment handles when replay publishes a new segment.
  Newly discovered or replaced segments still undergo full validation.
- Start the ID-directory rescan immediately, after WAL recovery has established
  the final startup view. Remove the initial 500 ms rescan delay.

- Mark visibility as changing only after acquiring ingest. A waiting refresh or
  writer leaves the previous immutable snapshot available; mixed mutation still
  covers both replacement and delete, then releases visibility before checkpoint.
  The 100 ms snapshot deadline and consistency checks remain unchanged.
- Replace active snapshot retry loops before search CPU admission with bounded
  cooperative waits (50 to 500 microseconds), on both odd generation and changed
  generation. Retain the 100 ms budget and request cancellation/deadline checks;
  stable first attempts do not wait. Linux release CI repeats the unchanged
  multi-index contention regression ten times.

### Compaction and durability

- Open and deeply validate compaction outputs outside the ingest/layout locks.
  Commit checks source/output identities and constructs the next view from the
  latest disk and memory generations without reopening those outputs.
- Reconcile flush completions before commit so a memory generation and its disk
  replacement never survive together. Preserve late replacements and deletions.
- Retain an already published active-buffer snapshot when live refresh exceeds
  its limit, using runtime buffer identity rather than document count or WAL seq.
  A real buffer handoff invalidates that identity and removes the old twin.
- Serialize live-map synchronization and restore the dirty flag after failure.
  Install an already committed manifest view even on a later sync error, report
  failure and poison the writer; checkpoint/trim remains blocked until restart.

### Measured local changes

Three paired native release runs on one macOS arm64 host. Startup uses identical
copies of alpha.4-created fixtures: 20,000 base documents total, 20 segments per
index, one or four indexes, and either no pending WAL documents or 1,000 new
unflushed documents plus one replacement per index. Both versions use two server
threads and disable maintenance during restart. OS caches are warm/uncontrolled.
Readiness is measured from process creation through health, complete exact search
and latest-source point GET; it is not merely an open port. Values are medians.

| Restart workload | alpha.4 | alpha.5 candidate |
|---|---:|---:|
| One index, flushed | 655 ms | 165 ms |
| One index, pending WAL | 707 ms | 215 ms |
| Four indexes, flushed | 812 ms | 367 ms |
| Four indexes, pending WAL | 1,001 ms | 567 ms |

All 24 final paired restarts passed their correctness checks. The one-index,
flushed candidate varied from 151 to 500 ms; that outlier is retained. These small
local samples do not establish cold-cache latency or production startup bounds.

A separate maintenance-enabled workload uses 1 KiB message payloads, batches of
1,000, four dashboard readers, a 15-second admission window with bounded retry
drain, `flush_docs=1000` and `target_docs=8000`. Each row is the median of three
runs. The 200,000-operation cap was not exhausted.

| Writers | alpha.4 ACK docs/s | alpha.5 ACK docs/s | alpha.4 query p99 | alpha.5 query p99 |
|---:|---:|---:|---:|---:|
| 2 | 6,549 | 6,169 | 13.13 ms | 13.92 ms |
| 4 | 6,700 | 7,341 | 17.03 ms | 18.64 ms |

All six final cases per version verified exact counts, latest-source samples and
settled readiness after repeated real merges. They had no failed or partial
search responses; write 429 retries remain necessary and use the same IDs/body.
Throughput ranges overlap: this release does not claim a sustained-ingest speedup.
An earlier alpha.4 run returned `read_view_raced`; its failed result was retained,
and a separate regression covers waiting refresh without an actual publication.
Actual publication may still exhaust the bounded snapshot deadline.

Reproduction tools: `tools/bench_startup.py`, `tools/bench_mixed_ingest.py` and
`tools/compare_versions.py`, with reports written outside the checkout. These
measurements compare local release candidates with the published alpha.4 binary;
they do not compare Elasticsearch, Linux performance or indefinite ingest capacity.

### Validation and compatibility

- Added deterministic late-flush, memory-limit and sync-failure regressions,
  including preservation of the WAL for a replaced document in a merge survivor.
- Make the HTTP write-queue saturation fixture deterministic: the all-feature
  gate holds a durable pre-ACK write at an existing failpoint, checks strict 429,
  then restarts unarmed and verifies recovery and a successful retry. It no
  longer assumes a group-commit time window remains occupied between requests.
- Added isolated startup and maintenance-enabled ingest tools. Readiness requires
  complete exact searches, latest source GETs and explicit valid ACK/ready
  watermarks; health alone does not establish readiness.
- Existing WAL and segment formats remain unchanged. These changes do not remove
  duplicated source/value bytes from WAL, add independent ingest builders or
  enable concurrent compactions across the process. Remaining schema, aggregation,
  client-compatibility and distributed-operation limitations still apply.
- Generated benchmark reports, raw results and working notes remain outside the
  repository and release assets.

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
