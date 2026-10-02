# Performance

## Weekly benchmark

{{#include ../generated/bench.md}}

Passes: **cold** is the first pass after a PostgreSQL restart with new DuckDB
processes; **warm** repeats the same queries; **new params** runs fresh
random queries on warm processes.

## Sizing

**PostgreSQL:** about one core per four reader cores. Per query, readers
spend about 4× the CPU PostgreSQL does (decoding Parquet, building results).
With DuckDB's cache warm, PostgreSQL mostly answers DuckLake's catalog
queries. In every run here, 3 PostgreSQL cores were 3–50% busy serving 13
reader cores.

**Readers:** DuckDB gives a query at most one thread per row group it reads,
and each thread fetches its row group's columns one after another. So N reader
cores can be N single-threaded readers (the most throughput) or fewer readers
with more threads (lower latency). pgvfs sizes its I/O threads and connection
pool from each DuckDB's `threads` setting.

| Readers × threads (13 cores) | Houston warm q/s | p50 | p95 | 100M `hits` warm q/s | p50 | p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 13 × 1 | 371 | 21 ms | 88 ms | 76 | 46 ms | 440 ms |
| 6 × 2 | 346 | 11 ms | 40 ms | 49 | 36 ms | 202 ms |
| 4 × 3 | 302 | 9 ms | 28 ms | | | |
| 1 × 13 | 137 | 6 ms | 11 ms | | | |

For many small lookups, 2 threads per reader halves latency for about 7% less
throughput. For wide scans it costs about 35%.

## Reference results

All on one 16-core laptop, DuckDB 1.5.6, PostgreSQL 18 in a container.
[Loading data](loading.md#measurements) has the layout measurements.
Absolute numbers moved by up to 1.7× with the laptop's other load (warm
Houston ran at 390–660 queries/s on the same day), so compare rows within a
table: those runs alternated under the same conditions.

**Compression and the footer cache.** LZ4 instead of snappy, plus the
Parquet footer cache pgvfs now turns on, took 13 Houston readers from 292 to
415 queries/s (p50 28.5 → 18.4 ms). The benchmarks now load with
[zstd and Parquet V2](loading.md#1-set-the-lake-options-before-the-first-insert),
which read 36% fewer bytes but served 25–35% fewer queries/s than LZ4 on
Houston. The weekly history records each run's layout; the other results on
this page used LZ4.

**Against an S3 gateway.** Full ClickBench (100M rows) through DuckDB's httpfs
and [pgvs3](https://github.com/adonm/pgvs3), a PostgreSQL-backed S3 gateway,
against pgvfs, with a fresh DuckDB per query: geomean 430 ms vs **371 ms**,
the same when warm (229 ms). Short queries gain most, because the HTTP hop and
revalidation are gone. [Records](https://github.com/adonm/pgvfs/blob/main/docs/results/vs-s3-gateway.jsonl).

**Where pgvfs's time goes.** On a cold pass pgvfs is about 30% of query time
(Houston) and most of it on a 100M-row wide table; on warm passes it's 0%. A
read costs mostly its bytes: alone, 8 KB takes 0.11 ms and 115 KB 0.2 ms, of
which PostgreSQL executes 0.01–0.03 ms. Under load the rest is queueing for
CPU.

**Tried, and not worth it:**

- **Block read-ahead** (fetch 254 KB blocks, cache them): 30–38% slower cold.
  DuckDB already merges nearby reads, so only half the extra bytes were used.
- **Index scans for small reads:** PostgreSQL's execution was already
  0.01–0.03 ms.
- **Pipelining a read's pieces:** every read measured was a single piece.

## Running the benchmarks

```sh
just bench city                         # Overture Houston, 1M rows: about a minute
just bench hits                         # ClickBench, 100M rows: about 4 minutes (14 GB download)
MODE=profile just bench city            # where one warm query of each kind spends its time
READERS=6 just bench city               # 6 readers with 2 threads each
LOAD_ARGS="--variant trickle" just bench city   # layout variants: see bench/city.py
```

Each run loads the data into a fresh PostgreSQL container, pins PostgreSQL to
about a fifth of the cores, restarts it, then runs lockstep readers: one cold
pass, warm passes and a new-parameter pass. Every pass reports queries/s,
latency, PostgreSQL's CPU and `pgvfs_stats()`. Memory stays bounded (see
`scripts/bench.sh`).
