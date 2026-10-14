# Releases

Every version is a pre-release on GitHub
([all releases](https://github.com/adonm/pgvfs/releases)), and every DuckDB
build is published separately as `duckdb-<duckdb version>`, so a build can be
updated without a new pgvfs version. Three ways to install:

- **`INSTALL pgvfs FROM community`** — DuckDB's own signed build of the current
  release. Available for stable DuckDB once the update below is merged;
  [Install](install.md) has the details.
- **`INSTALL pgvfs FROM 'https://pgvfs.adonm.dev'`** — this site's build for
  your DuckDB version, including 2.0 dev wheels
  ([builds](install.md#builds)).
- **A file** — download `pgvfs.duckdb_extension` from a release or from the
  build table, and `LOAD` it.

Release notes for each version are on its release page; the changelog below is
the same text, newest first.

## Changelog

{{#include ../generated/changelog.md}}
