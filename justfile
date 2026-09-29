set shell := ["bash", "-euo", "pipefail", "-c"]


default:
    @just --list

# fmt, clippy and unit tests (no database).
check:
    cargo fmt --check
    cargo clippy --locked --all-targets -- -D warnings
    cargo test --locked

# The DuckDB extension, built in a container -> target/ext/<TARGET>/:
# `release` (pinned stable) or `nightly [WHEEL]` (a 2.0 dev wheel, default newest).
ext TARGET="release" WHEEL="":
    @dir=$(bash scripts/duckdb.sh {{ TARGET }} {{ WHEEL }}) \
    && docker buildx build -q -f extension/Containerfile --build-context duckdb="$dir" \
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

# The Pages site (docs + extension repository) from local builds -> .tmp/site/.
site:
    mise exec -- python3 scripts/site.py .tmp/site --ext target/ext/release target/ext/nightly
    @echo "preview: python3 -m http.server -d .tmp/site"

# Concurrent DuckLake readers on a disposable PostgreSQL (bench/readers.py).
bench *ARGS: ext
    DUCKDB_PY=$(cat target/ext/release/DUCKDB_PY) EXT=target/ext/release/pgvfs.duckdb_extension \
      bash scripts/bench.sh {{ ARGS }}
