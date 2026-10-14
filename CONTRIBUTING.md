# Contributing

`docs/src/development.md` has the build, test and release recipes; this page is
the short version of how the code is written.

## Style

Code carries the design; comments do not.

- **No comments that restate the code.** A name, a small function or a plain
  struct is the fix for most of what a comment would say. A block that needs a
  paragraph of explanation is usually the wrong shape: split it, or make the
  data do the talking.
- **A comment earns its place** only when it states something the code cannot:
  an invariant another module depends on, behaviour of DuckDB or PostgreSQL
  that is not derivable here (and the version it was true for), or the reason a
  simple-looking alternative is wrong. One line. A file header may carry a few.
- **Doc comments on FFI functions are the exception:** cbindgen turns them into
  `pgvfs.h` and `tantivy.h`, so they are the C ABI's documentation. Write them
  once, where the function is declared (`src/lib.rs`), and run `just headers`.
- Tests are read as a specification: one short line naming what a block proves
  is welcome, as in `tests/store_contract.rs` and `extension/test/e2e.py`.

Code is data-oriented and functional.

- Plain structs, free functions, and pure helpers (`plan`, `runs`, `merge`)
  over stateful objects. Keep state in the one struct that owns it, behind one
  mutex, rather than spread across methods that mutate as they go.
- No globals beyond the documented ones: `store::STATS` and the once-init
  environment knobs. Prefer passing what a function needs to reaching for it.
- Keep the C ABI thin: the extension adapts DuckDB's interfaces and owns no
  logic that can live in Rust.

Formatting and lints are enforced: `cargo fmt`, `clippy -D warnings` (in
`just check`), and the checked-in headers from cbindgen.

## Tests

| Change | Run |
| --- | --- |
| anything | `just check` |
| storage, FFI, or the Rust crate | `just contract` (needs Docker) |
| `pgdb://` | `just contract`, and `just sqllogic` for DuckDB's own `test/sql` on it |
| the extension end to end | `just e2e` |
| performance-sensitive | `just bench-db`, `just bench city` (see `docs/src/performance.md`) |

A new skip in `test/configs/pgvfs.json` needs a reason that says why the test
cannot hold for `pgdb://`, not just that it fails.

## Commits

One logical change per commit, an imperative subject line ("Read one tantivy
split through up to four handles"), and a body that says why, not what. Say
when a change alters storage, the FFI, or a documented default.
