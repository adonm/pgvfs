# Changelog

pgvfs is in beta: the SQL interface and configuration are expected to stay,
but storage layout changes are still possible (see "Storage layout" in the
docs). Each release lists its layout version.

## 0.2.0-beta.1

Storage layout: **v2**.

First beta. Highlights since the project split out of pgvs3:

- **Install** from <https://pgvfs.adonm.dev> for DuckDB 1.5.6 and recent 2.0
  dev builds (linux_amd64); DuckDB's extension pipeline also builds it for
  Linux arm64, macOS (Intel and Apple Silicon) and Windows.
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
