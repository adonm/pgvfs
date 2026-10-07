# Changelog

pgvfs is in beta: the SQL interface and configuration are expected to stay,
but storage layout changes are still possible (see "Storage layout" in the
docs). Each release lists its layout version.

## Unreleased

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
  Rust panics at the tantivy C boundary become errors; oversized top-k is clamped.
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
