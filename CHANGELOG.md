# Changelog

pgvfs is in beta: the SQL interface and configuration are expected to stay,
but storage layout changes are still possible (see "Storage layout" in the
docs). Each release lists its layout version.

## 0.2.0-beta.6

Storage layout: **v2** (unchanged). The functions moved to `functions.sql`, which
writers keep current; the tables did not change.

- **Any process whose role can write may write.** The writer lease, its
  dedicated connection and the "another writer holds the lock" error are gone.
  Concurrent writers publish independently. Two writers publishing one path: the
  second fails with a clear error and rolls back. A `SELECT`-only role's write,
  drop or reap fails with PostgreSQL's permission error.
- **Reaping runs in every writer,** under a try-lock, so concurrent reaps skip.
  Each reap deletes a bounded range of rows per file. `PGVFS_REAP_GRACE_S` sets
  the grace (600 seconds by default); a read that outlives it names the setting
  in its error.
- **Credentials per volume.** A `postgres` secret named `pgvfs_<volume>` gives
  that volume its own database; each database has its own connection pool. DuckDB
  keeps no `SCOPE` on postgres secrets, so the name is what routes a path.
- **Build memory is bounded.** `tantivy_index` reserves each open build's
  `memory_budget` (plus about 10 MB) against `memory_limit` and fails before any
  build starts that would exceed it. Indexing threads are DuckDB's `threads`
  shared among the open builds (at most 8 each), with one merge thread. Earlier
  versions started tantivy's default of about 21 threads per build.
- **Listing streams one primary-key range query,** and `glob` lists only the
  literal prefix before its first `*`, `?` or `[`.
- `pgvfs_drop_volume` works from any writer.
- Build: `just ext` builds the stable extension locally from the DuckDB 1.5.6
  submodule. The container build is for DuckDB 2.0 dev builds. Every script is a
  justfile recipe; `scripts/` is gone, and `site.py` moved to `tools/`.
- Benchmarks: cold passes evict PostgreSQL's files from the OS page cache;
  `ROUNDS` repeats restart, eviction and passes; `PG_DEVICE` with
  `PG_READ_IOPS` and `PG_READ_BPS` throttles PostgreSQL's disk reads.
- PostgreSQL 11 to 13 are end-of-life upstream. The contract still passes on
  them; use 14 or later.

Upgrading from beta.5:

- The first write by the database owner (or a superuser) replaces `pgvfs.reap`
  with the new one. Until then, writers that do not own the functions keep the
  beta.5 reap, which has no try-lock and deletes whole files at once.
- Builds that would exceed `memory_limit` now fail. Each open build reserves
  `memory_budget` plus about 10 MB, so at the default 256 MB budget a query
  can have at most `memory_limit` / 266 MB groups. Lower `memory_budget` or
  build fewer groups per query.
- A `postgres` secret named `pgvfs_<volume>` now takes precedence over
  `pgvfs_secret` for that volume. Rename any secret that already has that name.
- A SELECT-only role's `pgvfs_drop_volume` fails with PostgreSQL's permission
  error, not the old "writer" error.

## 0.2.0-beta.5

Storage layout: **v2** (unchanged).

- Exclude dead IDs before tantivy top-k, exact counts and aggregations, using
  portable 32/64-bit roaring BLOBs or `BIGINT[]` on a fast integer key. Bitmap
  paths use DuckDB's `read_blob`, so every filesystem works.
- Fast-field-only hit projection, `tantivy_count`, and `tantivy_aggregate`
  with tantivy's Elasticsearch-shaped requests, including terms and cardinality.
- OpenSearch query-DSL subset: fuzzy, prefix, exists, boolean clauses,
  minimum-should-match and non-scoring filters; unsupported parameters fail.
- List-of-splits search, counts and aggregation merging; optional combined
  BM25 statistics. Hits now also include their split `path`.
- `tantivy_merge` compacts segments natively, optionally removing excluded
  documents. `tantivy_index` accepts rows/structs directly.
- Prepared index plans own their execution resources correctly: repeated
  execution and retries after errors neither crash nor keep partial documents.
  A failed build open under a parallel `GROUP BY` (a bad schema) is a clean
  error in every thread; before, other threads reused the failed entry and
  crashed.
  Rust panics at the tantivy C boundary become errors; oversized top-k is clamped.
- Exclusion is now an alive bitset per segment, built once per split and set
  and kept (the last four), as tantivy's own deletes are, instead of a lookup
  per candidate: with 60% of 4 million documents excluded, a count of 2.8
  million matches takes 9 ms (was 61), a top 10 takes 6 ms (was 64).
- A list of splits is searched in parallel, up to DuckDB's `threads`, and the
  hits' documents are read in parallel too. Equal scores order the same at any
  thread count.
- Search options `offset` (a page reads documents only for its own hits),
  `collapse` (the best hit of each value of a fast field) and `ignore_unmapped`;
  count options `limit` (stop after `limit + 1`) and `distinct` (the exact
  number of distinct values). `limit` was an unknown option before.
- Query DSL: paths inside JSON fields in `term`, `terms`, `match`,
  `match_phrase`, `range`, `exists` and `multi_match`; `exists` on an indexed
  text field without a fast column falls back to its terms.
- Query DSL: `prefix`, `wildcard`, `regexp` and `fuzzy` on paths inside JSON
  fields (the path's strings only; the path-skipping automaton is adapted from
  Quickwit, Apache-2.0), and `case_insensitive` on `term`, `prefix`, `wildcard`
  and `regexp`.
- `tantivy_search`: `sort` by a fast field (numbers, dates, text; missing values
  last; `score` is `NULL`), and `highlight` (a fourth column: HTML snippets of
  stored text fields). Query DSL: `more_like_this` for a text.
- `bench/search.py` and `bench/index_memory.py`: timings, compaction cost and
  build memory (documented with `memory_budget`; a `TMPDIR` on a tmpfs counts
  as memory).
- Every function registers a description and an example, so DuckDB's
  `duckdb_functions()` and the community docs page list them (they showed
  `NULL` for all but `pgvfs_stats`).
- `pgvfs_drop_volume` removes one volume through the writer and invalidates
  caches. Directory removal unpublishes a key prefix.
- DuckDB 2.0 scalar error declarations and filesystem access checks, including
  cached splits. SQL and PostgreSQL tests cover snapshot liveness and native
  compaction; changes inserted then deleted within one range are not indexed.

## 0.2.0-beta.4

Storage layout: **v2** (unchanged).

- Full-text search with tantivy, in the same extension. `tantivy_index` (an
  aggregate) builds an immutable split, one file, from any query's rows, one
  per group; `tantivy_search` searches a split (laterally, many);
  `tantivy_drop` removes one. They use only DuckDB's filesystem, so splits
  can live on pgvfs, local disk or object storage. Schemas, documents,
  options and queries are tantivy's own. The `tantivy_create_index` and
  `tantivy_match_bm25` macros work like DuckDB's fts. Which splits make up an
  index is up to SQL: the docs show a split per DuckLake commit, using
  `table_changes`, `rowid` and `snapshot_id`, and range compaction.
- Docs: vector search as plain SQL over a lake clustered by nearest centroid
  (IVF), with a synthetic check (`bench/ivf.py`).
- The extension grows by about 5 MB (tantivy, without its zstd feature).
  Built against DuckDB 2.0 (nightly), which embeds much less of DuckDB, it
  is about 21 MB instead of 39 MB.

## 0.2.0-beta.3

Storage layout: **v2** (unchanged).

- Restore Intel and Apple Silicon macOS targets in the distribution workflow
  and community submission.
- Document signed community installation and a self-contained DuckLake quick
  start, with separate instructions for unsigned 2.0 dev builds.
- Docs: tested on Amazon Aurora PostgreSQL; a "Related projects" section
  (Aurora direct querying, pg_duckdb, pg_lake) and how pgvfs's millisecond
  lookups compare.
- Docs: the recommended layout is now zstd and Parquet V2. LZ4 stores 55–58%
  more but serves 25–35% more queries/s to CPU-bound readers; the guide
  covers when to pick it. Loading data adds registering pre-written
  files for big loads, commit sizing, JSON over VARIANT payloads, snapshot
  retention, why to keep positional delete files, and how to check pruning.
- `pgvfs_stats` has a function description, so `duckdb_functions()` and the
  community extension page describe it.
- Benchmarks load with zstd and Parquet V2 (was LZ4, V1). Weekly results
  drop accordingly (Houston warm throughput about 30% lower on the same
  machine) and are not comparable with earlier runs. Each benchmark record
  now includes the lake's layout.
- The end-to-end test covers copying finished Parquet into pgvfs with
  `FORMAT blob` and registering it with `ducklake_add_data_files`.

## 0.2.0-beta.2

Storage layout: **v2** (unchanged).

- Builds for Linux musl (Alpine), amd64 and arm64.
- macOS left out of the multi-platform build for now.

## 0.2.0-beta.1

Storage layout: **v2**.

First beta. Highlights since the project split out of pgvs3:

- **Install** from <https://pgvfs.adonm.dev> for DuckDB 1.5.6 and recent 2.0
  dev builds (linux_amd64); DuckDB's extension pipeline also builds it for
  Linux arm64 and Windows. macOS builds but is not published yet.
- **One writer, many readers:** an advisory-lock writer lease; readers need
  only `SELECT` and work on streaming standbys (a writer there is refused).
- **Credentials** from a `postgres` secret shared with DuckLake's catalog.
- **Performance defaults:** Parquet footer cache on load, a 10 s open cache,
  I/O threads and pool sized from DuckDB's `threads`; documented lake layout
  (8K-row groups, LZ4, 64 MB files, a declared sort order).
- **Reliability:** reads retry once on a lost connection; interrupted writes
  publish nothing; a lost writer lock stops writes; the writer reaps garbage
  when it takes the lock.
- **Compatibility:** PostgreSQL 11–18, no extensions or superuser; TLS with
  certificate verification.
- `pgvfs_stats()` for monitoring.
