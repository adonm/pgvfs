# Development

```sh
mise install                 # rust, just, uv, python, mdbook
just check                   # fmt, clippy, unit tests
just ext                     # extension for DuckDB 1.5.6 -> target/ext/release/
just ext nightly [WHEEL]     # for a DuckDB 2.0 dev wheel (default: newest) -> target/ext/nightly/
just contract                # storage contract on a disposable PostgreSQL (PG_IMAGE=postgres:11..18)
just compat                  # the contract on every supported PostgreSQL
just e2e [release|nightly]   # build, then contract + DuckDB end-to-end
just bench [city|hits]       # benchmarks (see Performance)
just site                    # this site from local builds -> .tmp/site/
```

Docker is needed for builds and tests.

## Layout

| Path | |
| --- | --- |
| `schema.sql` | the storage layout |
| `src/store.rs` | reads, the `COPY` writer, the writer lock, reaping |
| `src/pg.rs` | connection pool and TLS |
| `src/lib.rs` | the C interface used by the extension, the open cache, `pgvfs_stats()` |
| `extension/` | the C++ DuckDB `FileSystem` adapter, its build and end-to-end test |
| `tests/` | the storage contract, against real PostgreSQL |
| `bench/` | benchmark harness (`lake.py`) and datasets (`city.py`, `hits.py`) |
| `scripts/` | build inputs, test and benchmark runners, release and site tools |
| `docs/` | this site (mdbook) |

The storage is Rust. The extension is a thin C++ layer because DuckDB's stable
C API can use filesystems but cannot register one.

## Building the extension

A C++ DuckDB extension must be statically linked against the exact DuckDB
build that loads it, as DuckDB's own extensions are (Python loads DuckDB with
`RTLD_LOCAL`, so the host's symbols are out of reach). DuckDB is never
compiled here: `scripts/duckdb.sh` fetches headers and prebuilt static
libraries, and the container only links (`extension/build.sh`, a few seconds).

- **release:** the release's `static-libs-linux-amd64.zip` and source tarball,
  pinned by SHA-256 in `scripts/duckdb.sh`. To move to a new release, update
  the version and both digests.
- **nightly:** each 2.0 dev wheel is cut by a run of DuckDB's `Main` CI on its
  exact commit, which keeps the static libraries for 90 days. The download is
  checked against GitHub's recorded digest and needs a GitHub token
  (`gh auth login` or `GH_TOKEN`).

`target/ext/<target>/DUCKDB_PY` records the wheel a build loads into.

## CI and releases

- **Each push** (`ci.yml`): `just check`, then the contract on PostgreSQL 18
  and end-to-end tests for the stable build.
- **Weekly, Monday** (`weekly.yml`, or by hand): the contract on PostgreSQL
  11; end-to-end tests for the stable and newest 2.0 dev builds; each build
  published as the pre-release `duckdb-<version>` (every stable build and the
  last 4 dev builds are kept); `just bench city` appended to the `bench`
  pre-release's `history.jsonl`.
- **Pages** (`pages.yml`, weekly and on docs changes): this site, assembled
  from the releases by `scripts/site.py`, is also the `INSTALL ... FROM`
  repository.
