# Performance

## Weekly benchmark

{{#include ../generated/bench.md}}

Passes: **cold** is the first pass after PostgreSQL restarts with its files
evicted from the operating system's page cache, in new DuckDB processes;
**warm** repeats the same queries; **new params** runs fresh random queries on
warm processes. `ROUNDS` repeats the restart, eviction and passes; the lines of
each round carry its `round`.

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

Fewer threads also lower peak memory on an aggregation that would not fit:
each thread aggregates into its own hash table, so a query grouping on millions
of distinct values can approach `threads × groups` of memory
([DuckDB on dimension tables](https://duckdb.org/2026/10/02/dimension-tables)).
Narrower group keys shrink each entry but not the count, and data clustered by
the group key helps too.

## Reference results

All on one 16-core laptop, DuckDB 1.5.6, PostgreSQL 18 in a container.
[Loading data](loading.md#measurements) has the layout measurements. These
results predate the page-cache eviction in the benchmark's cold passes, so their
cold numbers had the operating system's cache warm; the weekly history is
measured that way.
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

**Schema shape.** The layout decides which bytes a query fetches; column types
decide what it does with them. DuckDB measures the same timestamp column at
0.9 s aggregated as `TIMESTAMP` against 3.9 s as `VARCHAR`
([its schema guide](https://duckdb.org/docs/current/guides/performance/schema)),
and DuckLake takes only `NOT NULL`, so unlike a plain DuckDB table there are no
keys or indexes to declare. Replacing long repeated strings with narrow integer
keys is the change that shrinks a lake's bytes rather than just its read set
([dimension tables](https://duckdb.org/2026/10/02/dimension-tables));
[Loading data](loading.md#4-keep-columns-typed-and-narrow) has the pattern.
Not measured here.

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
ROUNDS=3 just bench city                # three restarts, each with its files evicted
PG_DEVICE=/dev/nvme0n1 PG_READ_IOPS=3000 PG_READ_BPS=125mb just bench city  # gp3's baseline disk
LOAD_ARGS="--variant trickle" just bench city   # layout variants: see bench/city.py
```

Each run loads the data into a fresh PostgreSQL container, on a data volume
that the rounds keep. Each round then starts PostgreSQL in a new container
pinned to about a fifth of the cores, evicts its files from the operating
system's page cache, and runs lockstep readers: one cold pass, warm passes and a
new-parameter pass. Every pass reports queries/s, latency, PostgreSQL's CPU and
`pgvfs_stats()`. Memory stays bounded: `LOAD_MEMORY` (default 4 GiB, sorts spill
to disk), `READER_MEMORY` (2 GiB per reader) and `PG_MEMORY` (6 GB for
PostgreSQL). `PG_DEVICE` with `PG_READ_IOPS` and `PG_READ_BPS` throttles
PostgreSQL's disk reads, a container setting that needs the device that holds
Docker's volumes. The bench recipe is in the [justfile](https://github.com/adonm/pgvfs/blob/main/justfile).
