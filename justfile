set shell := ["bash", "-euo", "pipefail", "-c"]


default:
    @just --list

# Regenerate the C headers from the Rust FFI (`just check` fails when they drift).
headers:
    UPDATE_HEADERS=1 cargo test --locked --test headers
    UPDATE_HEADERS=1 cargo test --locked -p duckdb-tantivy --test headers

# fmt, clippy and unit tests (no database), and that the C headers match the FFI.
check:
    cargo fmt --all --check
    cargo clippy --locked --workspace --all-targets -- -D warnings
    cargo test --locked --workspace

# The DuckDB extension, built in a container -> target/ext/<TARGET>/:
# `release` (pinned stable) or `nightly [WHEEL]` (a 2.0 dev wheel, default newest).
ext TARGET="release" WHEEL="":
    @dir=$(bash scripts/duckdb.sh {{ TARGET }} {{ WHEEL }}) \
    && docker buildx build -q -f extension/Containerfile --build-context duckdb="$dir" \
         --build-arg PGVFS_VERSION="$(git describe --tags --always --dirty)" \
         --output type=local,dest=target/ext/{{ TARGET }} . >/dev/null \
    && cp "$dir/wheel" target/ext/{{ TARGET }}/DUCKDB_PY \
    && cp "$dir/version" target/ext/{{ TARGET }}/DUCKDB_VERSION \
    && echo "target/ext/{{ TARGET }}/pgvfs.duckdb_extension for duckdb==$(cat "$dir/wheel") ($(cat "$dir/version"))"

# Storage contract on a disposable PostgreSQL (PG_IMAGE=postgres:11 ... :18).
contract:
    bash scripts/contract.sh

# Contract plus the DuckDB end-to-end test through the built extension.
e2e TARGET="release" WHEEL="": (ext TARGET WHEEL)
    DUCKDB_PY=$(cat target/ext/{{ TARGET }}/DUCKDB_PY) \
      bash scripts/contract.sh target/ext/{{ TARGET }}/pgvfs.duckdb_extension

# The contract on every supported PostgreSQL major.
compat:
    for v in 11 13 15 17 18; do PG_IMAGE=postgres:$v bash scripts/contract.sh; done

# The Pages site from local builds (laid out like release assets) -> .tmp/site/.
site:
    rm -rf .tmp/site .tmp/releases
    for t in release nightly; do \
      [ -f target/ext/$t/pgvfs.duckdb_extension ] || continue; \
      v=$(cat target/ext/$t/DUCKDB_VERSION); mkdir -p .tmp/releases/$v; \
      cp target/ext/$t/pgvfs.duckdb_extension .tmp/releases/$v/; \
      printf '{"duckdb_version": "%s", "wheel": "%s", "commit": "local", "date": "%s"}\n' \
        "$v" "$(cat target/ext/$t/DUCKDB_PY)" "$(date +%F)" >.tmp/releases/$v/build.json; \
    done
    mise exec -- python3 scripts/site.py .tmp/site --releases .tmp/releases
    @echo "preview: python3 -m http.server -d .tmp/site"

# Benchmark DuckLake on pgvfs: `city` (1M rows, ~1 min) or `hits` (100M rows, ~4 min).
bench DATASET="city" *ARGS: ext
    bash scripts/bench.sh {{ DATASET }} {{ ARGS }}
