# Development

```sh
mise install                              # rust, just, uv, python, mdbook, cmake, ninja
git submodule update --init --recursive   # DuckDB's source, for `just ext`
just check                    # fmt, clippy, unit and property tests, and that the C headers match the FFI
just headers                  # regenerate the C headers from the Rust FFI
just ext                      # the extension, built here from DuckDB v1.5.6 -> target/ext/release/
just ext-nightly [WHEEL]      # a DuckDB 2.0 dev wheel (default: newest), built in a container -> target/ext/nightly/
just contract [EXT]          # storage contract on disposable PostgreSQL (PG_IMAGE=postgres:11..18); with EXT, the end-to-end test too
just compat                   # the contract on every supported PostgreSQL
just e2e [release|nightly]    # build, then contract + DuckDB end-to-end
just bench [city|hits]       # benchmarks (see Performance)
just site                     # this site from local builds -> .tmp/site/
```

Every build, test, benchmark and release command is a recipe in the
[justfile](https://github.com/adonm/pgvfs/blob/main/justfile); `just --list`
shows them. Docker is needed for the contract, the nightly build and the
benchmark. `just contract` starts a primary, a streaming standby and a TLS-only
server with a throwaway CA, and tests against all three: exact reads, concurrent
writers, same-path publish conflicts, cooperative reaps, function refresh, listing
ranges, readers with `SELECT` only and on the standby, TLS verification, lost
connections, interrupted writes, readers during writes, and offsets past 4 GiB.
With an extension, it also runs the DuckDB end-to-end test through it: a second
process's writes, a `SELECT`-only role's refused writes, and volumes routed to
two databases by their secrets.

## Layout

| Path | |
| --- | --- |
| `schema.sql`, `functions.sql` | the storage layout, and the functions writers keep current |
| `src/store.rs` | reads, the `COPY` writer, the advisory-locked install, reaping |
| `src/pg.rs` | connection pool and TLS |
| `src/lib.rs` | the C interface used by the extension, the open cache, `pgvfs_stats()` |
| `tantivy/` | tantivy splits (a workspace crate, linked into the same staticlib): build, bundle, search |
| `extension/` | the C++ DuckDB `FileSystem` adapter, the tantivy SQL functions, the container build, the end-to-end test |
| `tests/` | the storage contract, against real PostgreSQL; `headers.rs` checks `pgvfs.h` against the FFI |
| `cbindgen.toml`, `tantivy/cbindgen.toml` | how the two C headers in `extension/src/include/` are generated from the Rust FFI |
| `deny.toml` | supply-chain policy for `cargo deny`: permissive licences, crates.io only, advisories |
| `bench/` | benchmark harness (`lake.py`), datasets (`city.py`, `hits.py`), the IVF check (`ivf.py`) |
| `tools/` | the Pages site generator (`site.py`) |
| `justfile` | every build, test, benchmark and release command |
| `docs/` | this site (mdbook) |

The storage is Rust. The extension is a thin C++ layer because DuckDB's stable
C API can use filesystems but cannot register one.

**The C ABI.** `extension/src/include/pgvfs.h` and `tantivy.h` are generated
from the `extern "C"` functions in Rust (cbindgen) and checked in. A signature
that differs between Rust and C++ still compiles on both sides and crashes at
run time, so `just check` fails when a header is out of date: run `just headers`
and commit the result. Document an FFI function in Rust; its comment becomes the
header's.

**Properties.** `tantivy/src/search/properties.rs` runs proptest over random
queries, options and exclusions on four splits: the answer is the same on one
thread and on many; a page is a slice of the ranking; excluded documents are the
ranking without them (and never crowd out `top_k`); collapse keeps the best hit
of each group; bytes and JSON that are not a bitmap or a query are refused, not
a panic. `PROPTEST_CASES=3000 cargo test -p duckdb-tantivy properties` runs them
longer. A failure prints a minimal case and saves its seed in
`tantivy/proptest-regressions/`: commit that file with the fix.

**Supply chain.** CI runs `cargo deny check licenses bans sources` on every
change and the RustSec advisories weekly. Dependencies must be permissively
licensed and come from crates.io: no git dependencies, so nothing unreleased is
linked into the extension. An accepted advisory goes in `deny.toml` with its
reason.

## Building the extension

A C++ DuckDB extension must be statically linked against the exact DuckDB
build that loads it, as DuckDB's own extensions are (Python loads DuckDB with
`RTLD_LOCAL`, so the host's symbols are out of reach).

- **Stable (`just ext`):** DuckDB's extension template compiles the pinned
  `duckdb` submodule (v1.5.6) and links the extension, with the Rust crate built
  by cargo through Corrosion (`CMakeLists.txt`, `extension_config.cmake`). The
  first build compiles DuckDB once, about 7 minutes; later builds are
  incremental. `just ext` fails if the submodule is any other release, and
  writes `target/ext/release/` with the wheel (`DUCKDB_PY`) and the version
  (`DUCKDB_VERSION`) that `bench` and the release recipes read. `make test` runs
  `test/sql/`. The `Distribution` workflow runs this build for Linux (amd64,
  arm64, glibc and musl), macOS (Intel and Apple Silicon) and Windows on tags,
  weekly and by hand.
- **Nightly (`just ext-nightly`):** each 2.0 dev wheel is cut by a run of
  DuckDB's `Main` CI on its exact commit, which keeps the static libraries for
  90 days. The recipe downloads them and the source tree, checks the digest, and
  links the extension in a container (`extension/Containerfile`,
  `extension/build.sh`) without compiling DuckDB, in seconds. The download needs
  a GitHub token (`gh auth login` or `GH_TOKEN`).

## CI and releases

- **Community releases:** DuckDB builds and signs pgvfs for `INSTALL pgvfs
  FROM community`. Updates are submitted to
  [duckdb/community-extensions](https://github.com/duckdb/community-extensions/tree/main/extensions/pgvfs),
  pinning the release commit in `description.yml`. Our weekly unsigned
  builds remain available for 2.0 dev versions.
- **Each push** (`ci.yml`): `just check`, then the stable build (DuckDB compiles
  once per commit; the cache falls back to an older commit's), the contract on
  PostgreSQL 18 and the end-to-end test.
- **Tags, weekly and by hand** (`distribution.yml`): the multi-platform build
  with DuckDB's extension pipeline (Linux amd64 and arm64, glibc and musl;
  macOS Intel and Apple Silicon; Windows).
- **Weekly, Monday** (`weekly.yml`, or by hand): the contract on PostgreSQL
  11; end-to-end tests for the stable and newest 2.0 dev builds; each build
  published as the pre-release `duckdb-<version>` (every stable build and the
  last 4 dev builds are kept); `just bench city` appended to the `bench`
  pre-release's `history.jsonl`.
- **Pages** (`pages.yml`, weekly and on docs changes): this site, assembled
  from the releases by `tools/site.py`, is also the `INSTALL ... FROM`
  repository.
