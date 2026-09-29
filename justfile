set shell := ["bash", "-euo", "pipefail", "-c"]

EXT := "target/ext/pgvfs.duckdb_extension"

default:
    @just --list

# fmt, clippy and unit tests (no database).
check:
    cargo fmt --check
    cargo clippy --locked --all-targets -- -D warnings
    cargo test --locked

# The DuckDB extension, built in a container -> target/ext/.
ext:
    docker buildx build -f extension/Containerfile --output type=local,dest=target/ext .

# Storage contract on a disposable PostgreSQL (PG_IMAGE=postgres:11 ... :18).
contract:
    bash scripts/contract.sh

# Contract plus the DuckDB end-to-end test through the built extension.
e2e: ext
    bash scripts/contract.sh {{ EXT }}

# The contract on every supported PostgreSQL major.
compat:
    for v in 11 13 15 17 18; do PG_IMAGE=postgres:$v bash scripts/contract.sh; done

# Concurrent DuckLake readers on a disposable PostgreSQL (see bench/README.md).
bench *ARGS: ext
    bash scripts/bench.sh {{ ARGS }}
