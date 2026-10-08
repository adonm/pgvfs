set shell := ["bash", "-euo", "pipefail", "-c"]

# The DuckDB release `ext` builds against: the duckdb submodule must be this tag.
duckdb := "v1.5.6"

default:
    @just --list

# Regenerate the C headers from the Rust FFI (`just check` fails when they drift).
headers:
    UPDATE_HEADERS=1 cargo test --locked --test headers
    UPDATE_HEADERS=1 cargo test --locked -p duckdb-tantivy --test headers

# fmt, clippy, unit tests, and that the C headers match the FFI (no database needed).
check:
    cargo fmt --all --check
    cargo clippy --locked --workspace --all-targets -- -D warnings
    cargo test --locked --workspace

# The first build compiles DuckDB (about 7 minutes); later builds are incremental.

# The stable extension, built here from the pinned duckdb submodule -> target/ext/release/.
ext:
    #!/usr/bin/env bash
    set -euo pipefail
    # CI checks submodules out shallowly, without tags, and DuckDB's build reads its version from the
    # tag (without one it falls back to a dummy version). Fetch this release's tag if it is missing.
    if ! git -C duckdb rev-parse -q --verify "refs/tags/{{duckdb}}" >/dev/null; then
      git -C duckdb fetch -q --depth=1 origin "refs/tags/{{duckdb}}:refs/tags/{{duckdb}}"
    fi
    tag=$(git -C duckdb describe --tags --exact-match 2>/dev/null || echo none)
    [ "$tag" = "{{duckdb}}" ] || {
      echo "duckdb/ is at $tag, not {{duckdb}}: git submodule update --init --recursive" >&2
      exit 1
    }
    # A build directory from another generator, or with a compiler that is gone, cannot be reused.
    cache=build/release/CMakeCache.txt
    if [ -f "$cache" ]; then
      stale=0
      grep -q '^CMAKE_GENERATOR:INTERNAL=Ninja$' "$cache" || stale=1
      for var in CMAKE_C_COMPILER CMAKE_CXX_COMPILER; do
        [ -x "$(sed -n "s/^$var:FILEPATH=//p" "$cache")" ] || stale=1
      done
      if [ "$stale" = 1 ]; then
        echo "ext: discarding build/release (another generator, or a compiler that is gone)" >&2
        rm -rf build/release
      fi
    fi
    GEN=ninja make release
    out=target/ext/release
    mkdir -p "$out"
    cp build/release/extension/pgvfs/pgvfs.duckdb_extension "$out/"
    printf '%s\n' "{{duckdb}}" >"$out/DUCKDB_VERSION"
    printf '%s\n' "${tag#v}" >"$out/DUCKDB_PY"
    echo "$out/pgvfs.duckdb_extension for duckdb==${tag#v} ({{duckdb}})"

# The static libs come from DuckDB's own CI run for that commit (needs gh).
# WHEEL: a PyPI version (default: the newest 2.0 dev wheel).

# A DuckDB 2.0 dev build, linked in a container -> target/ext/nightly/.
ext-nightly WHEEL="":
    #!/usr/bin/env bash
    set -euo pipefail
    export GH_HOST=github.com
    wheel="{{WHEEL}}"
    if [ -z "$wheel" ]; then
      wheel=$(curl -fsSL https://pypi.org/pypi/duckdb/json | python3 -c 'import json, sys; dev = [v for v in json.load(sys.stdin)["releases"] if v.startswith("2.") and ".dev" in v]; print(max(dev, key=lambda v: int(v.rsplit("dev", 1)[1])))')
    fi
    read -r library sid < <(uv run --quiet --no-project --with "duckdb==$wheel" python -c 'import duckdb; print(*duckdb.connect().sql("PRAGMA version").fetchone()[:2])')
    # The extension footer carries DuckDB's version directory name: the tag, unless it is a -dev build
    # (ExtensionHelper::GetVersionDirectoryName).
    if [[ $library == *-dev* ]]; then version=$sid; else version=$library; fi
    dir=target/duckdb/$sid
    if [ ! -f "$dir/version" ]; then
      echo "ext-nightly: wheel $wheel is DuckDB $library ($sid)" >&2
      sha=$(gh api "repos/duckdb/duckdb/commits/$sid" -q .sha)
      run=$(gh api "repos/duckdb/duckdb/actions/workflows/Main.yml/runs?head_sha=$sha&status=success" \
        -q '[.workflow_runs[] | select(.head_repository.id == .repository.id)][0].id // empty')
      [ -n "$run" ] || { echo "ext-nightly: no successful DuckDB Main run on $sha for static libs" >&2; exit 1; }
      read -r artifact digest < <(gh api \
        "repos/duckdb/duckdb/actions/runs/$run/artifacts?name=duckdb-static-libs-linux-amd64.tar.gz" \
        -q '.artifacts[0] | "\(.id) \(.digest)"')
      [ -n "$artifact" ] || { echo "ext-nightly: run $run kept no linux-amd64 static libs (expired?)" >&2; exit 1; }
      rm -rf "$dir" && mkdir -p "$dir/libs" "$dir/src"
      echo "ext-nightly: static libs from run $run, artifact $artifact" >&2
      gh api "repos/duckdb/duckdb/actions/artifacts/$artifact/zip" >"$dir/libs.tar.gz"
      echo "${digest#sha256:}  $dir/libs.tar.gz" | sha256sum -c --quiet -
      tar xzf "$dir/libs.tar.gz" -C "$dir/libs" && rm "$dir/libs.tar.gz"
      curl -fsSL "https://github.com/duckdb/duckdb/archive/$sha.tar.gz" | tar xz -C "$dir/src" --strip-components 1
      echo "$wheel" >"$dir/wheel"
      echo "$version" >"$dir/version"
    fi
    docker buildx build -q -f extension/Containerfile --build-context duckdb="$dir" \
      --build-arg PGVFS_VERSION="$(git describe --tags --always --dirty)" \
      --output type=local,dest=target/ext/nightly . >/dev/null
    cp "$dir/wheel" target/ext/nightly/DUCKDB_PY
    cp "$dir/version" target/ext/nightly/DUCKDB_VERSION
    echo "target/ext/nightly/pgvfs.duckdb_extension for duckdb==$(cat "$dir/wheel") ($(cat "$dir/version"))"

# A primary, a streaming standby of it and a TLS-only server, all disposable. With EXT (a built
# extension), also the DuckDB end-to-end test through it. PG_IMAGE picks the version (default 18).

# Storage contract on disposable PostgreSQL servers (and the end-to-end test with EXT).
contract EXT="":
    #!/usr/bin/env bash
    set -euo pipefail
    ext="{{EXT}}"
    image=${PG_IMAGE:-postgres:18}
    wheel=1.5.6
    if [ -n "$ext" ]; then wheel=$(cat "$(dirname "$ext")/DUCKDB_PY"); fi
    name="pgvfs-contract-$$"
    certs=$(mktemp -d)
    cleanup() {
      docker rm -f "$name" "$name-standby" "$name-tls" >/dev/null 2>&1 || true
      docker network rm "$name" >/dev/null 2>&1 || true
      rm -rf "$certs"
    }
    trap cleanup EXIT
    ready() {
      for i in $(seq 1 120); do
        docker exec "$1" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && return
        [ "$i" = 120 ] && { docker logs "$1" >&2; exit 1; }
        sleep 0.5
      done
    }
    psql() { docker exec -i "$name" psql -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

    docker network create "$name" >/dev/null
    docker run -d --name "$name" --network "$name" --network-alias primary \
      -e POSTGRES_PASSWORD=postgres -p 127.0.0.1::5432 "$image" >/dev/null
    ready "$name"
    psql -c 'CREATE DATABASE pgvfs_t' -c 'CREATE DATABASE empty_t' -c 'CREATE DATABASE s3_t' \
      -c 'CREATE DATABASE second_t' -c "CREATE ROLE reader LOGIN PASSWORD 'reader'"
    psql -d s3_t -c 'CREATE SCHEMA s3p' -c 'CREATE TABLE s3p.chunks (x int)'
    # SELECT-only, granted ahead of the schema the writer will create.
    psql -d pgvfs_t -c 'ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT USAGE ON SCHEMAS TO reader' \
      -c 'ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT SELECT ON TABLES TO reader'

    # A streaming standby of the primary.
    docker exec "$name" bash -c 'echo "host replication all all md5" >> "$PGDATA/pg_hba.conf"'
    psql -c 'SELECT pg_reload_conf()' >/dev/null
    docker run -d --name "$name-standby" --network "$name" -e PGPASSWORD=postgres \
      -p 127.0.0.1::5432 --entrypoint bash "$image" -c '
        set -e; d=/var/lib/postgresql/standby
        pg_basebackup -h primary -U postgres -D "$d" -R -X stream --checkpoint=fast
        chown -R postgres:postgres "$d"; chmod 700 "$d"
        exec gosu postgres postgres -D "$d"' >/dev/null
    ready "$name-standby"

    # A TLS-only server, its certificate signed by a throwaway CA.
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=pgvfs-test-ca \
      -keyout "$certs/ca.key" -out "$certs/ca.crt" 2>/dev/null
    openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
      -keyout "$certs/server.key" -out "$certs/server.csr" 2>/dev/null
    printf 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n' >"$certs/ext"
    openssl x509 -req -in "$certs/server.csr" -CA "$certs/ca.crt" -CAkey "$certs/ca.key" \
      -CAcreateserial -days 1 -extfile "$certs/ext" -out "$certs/server.crt" 2>/dev/null
    chmod 644 "$certs"/*
    docker run -d --name "$name-tls" -e POSTGRES_PASSWORD=postgres -v "$certs:/certs:ro,z" \
      -p 127.0.0.1::5432 --entrypoint bash "$image" -c '
        cp /certs/server.crt /certs/server.key /tmp/ && chown postgres /tmp/server.*
        chmod 600 /tmp/server.key
        exec docker-entrypoint.sh postgres -c ssl=on \
          -c ssl_cert_file=/tmp/server.crt -c ssl_key_file=/tmp/server.key' >/dev/null
    ready "$name-tls"

    addr=$(docker port "$name" 5432/tcp)
    base="postgres://postgres:postgres@$addr"
    echo "== $(psql -tAc 'SHOW server_version') =="
    PGVFS_TEST_DB_URL="$base/pgvfs_t" PGVFS_TEST_EMPTY_DB_URL="$base/empty_t" \
      PGVFS_TEST_S3_DB_URL="$base/s3_t" PGVFS_TEST_READER_URL="postgres://reader:reader@$addr/pgvfs_t" \
      PGVFS_TEST_STANDBY_URL="postgres://postgres:postgres@$(docker port "$name-standby" 5432/tcp)/pgvfs_t" \
      PGVFS_TEST_TLS_URL="postgres://postgres:postgres@localhost:$(docker port "$name-tls" 5432/tcp | cut -d: -f2)/postgres?sslmode=require" \
      PGVFS_TEST_TLS_CA="$certs/ca.crt" PGVFS_POOL_MIN=1 PGVFS_POOL_MAX=8 \
      cargo test --locked --test store_contract -- --include-ignored --test-threads=1

    if [ -n "$ext" ]; then
      PGVFS_TEST_URL="$base/pgvfs_t" PGVFS_TEST_READER_URL="postgres://reader:reader@$addr/pgvfs_t" \
        PGVFS_TEST_SECOND_URL="$base/second_t" \
        PGVFS_TEST_STANDBY_URL="postgres://postgres:postgres@$(docker port "$name-standby" 5432/tcp)/pgvfs_t" \
        uv run --quiet --no-project --with "duckdb==$wheel" python extension/test/e2e.py "$ext"
    fi

# Builds the extension (release: here; nightly: in a container), then runs the contract and the
# DuckDB end-to-end test through it. TARGET: release | nightly.

# Build, then the contract and the DuckDB end-to-end test through the extension.
e2e TARGET="release" WHEEL="":
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{TARGET}}" in
      release) just ext ;;
      nightly) just ext-nightly "{{WHEEL}}" ;;
      *) echo "e2e: TARGET is release or nightly, not {{TARGET}}" >&2; exit 2 ;;
    esac
    just contract "target/ext/{{TARGET}}/pgvfs.duckdb_extension"

# The contract on every supported PostgreSQL major.
compat:
    for v in 11 13 15 17 18; do PG_IMAGE=postgres:$v just contract; done

# The Pages site from local builds, laid out like the release assets -> .tmp/site/.
site:
    rm -rf .tmp/site .tmp/releases
    for t in release nightly; do \
      [ -f target/ext/$t/pgvfs.duckdb_extension ] || continue; \
      v=$(cat target/ext/$t/DUCKDB_VERSION); mkdir -p .tmp/releases/$v; \
      cp target/ext/$t/pgvfs.duckdb_extension .tmp/releases/$v/; \
      printf '{"duckdb_version": "%s", "wheel": "%s", "commit": "local", "date": "%s"}\n' \
        "$v" "$(cat target/ext/$t/DUCKDB_PY)" "$(date +%F)" >.tmp/releases/$v/build.json; \
    done
    mise exec -- python3 tools/site.py .tmp/site --releases .tmp/releases
    @echo "preview: python3 -m http.server -d .tmp/site"

# `city` (1M rows, about a minute) or `hits` (100M rows, about 4 minutes). Each of ROUNDS rounds
# (default 1) starts PostgreSQL fresh on the loaded data, evicts its files from the OS page cache,
# and runs the passes. PG_DEVICE with PG_READ_IOPS and PG_READ_BPS throttles its disk reads (gp3:
# 3000 and 125mb). Extra args go to `lake.py run`; LOAD_ARGS to `lake.py load`; MODE=profile times
# one warm query of each kind.

# DuckLake on pgvfs against a disposable PostgreSQL: city or hits.
bench DATASET="city" *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    dataset="{{DATASET}}"
    args="{{ARGS}}"
    cores=$(nproc)
    pg=$(( cores / 5 > 0 ? cores / 5 : 1 ))
    pg_cpus=${PG_CPUS:-0-$((pg - 1))}
    reader_cpus=${READER_CPUS:-$pg-$((cores - 1))}
    first=${reader_cpus%-*}
    last=${reader_cpus#*-}
    readers=${READERS:-$((last - first + 1))}
    ext=${EXT:-target/ext/release/pgvfs.duckdb_extension}
    if [ -z "${EXT:-}" ] && [ ! -f "$ext" ]; then just ext; fi
    wheel=${DUCKDB_PY:-$(cat "$(dirname "$ext")/DUCKDB_PY")}
    image=${PG_IMAGE:-postgres:18}
    rounds=${ROUNDS:-1}
    container="pgvfs-bench-$$"
    data="$container-data"
    trap 'docker rm -f "$container" >/dev/null 2>&1 || true; docker volume rm "$data" >/dev/null 2>&1 || true' EXIT
    lake() { uv run --quiet --no-project --with "duckdb==$wheel" --with pyarrow python bench/lake.py "$dataset" "$@"; }
    url() { echo "postgres://postgres:postgres@$(docker port "$container" 5432/tcp)/lake"; }
    ready() {
      for _ in $(seq 1 120); do
        docker exec "$container" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && return
        sleep 0.5
      done
      docker logs "$container" >&2
      exit 1
    }
    # PostgreSQL in a fresh container on the data volume, pinned to the CPUs in $1 and throttled
    # when PG_DEVICE is set (a throttle can only be set when a container is created).
    start() {
      local throttle=()
      if [ -n "${PG_DEVICE:-}" ]; then
        throttle=(--device-read-iops "$PG_DEVICE:${PG_READ_IOPS:?set PG_READ_IOPS}" \
          --device-read-bps "$PG_DEVICE:${PG_READ_BPS:?set PG_READ_BPS}")
      fi
      docker run -d --name "$container" -e POSTGRES_PASSWORD=postgres --shm-size=2g \
        --memory "${PG_MEMORY:-6g}" --cpuset-cpus "$1" -v "$data:/var/lib/postgresql" \
        -p 127.0.0.1::5432 "${throttle[@]}" "$image" -c shared_buffers=2GB -c max_connections=500 >/dev/null
      ready
    }
    # A clean shutdown (a checkpoint); the data stays on the volume.
    stop() {
      docker stop -t 120 "$container" >/dev/null 2>&1 || true
      docker rm -f "$container" >/dev/null 2>&1 || true
    }
    # Evict PostgreSQL's files from the OS page cache, so the next reads really are cold.
    evict() {
      docker exec "$container" bash -c 'find /var/lib/postgresql -type f -exec dd if={} iflag=nocache count=0 status=none \;'
    }

    lake download
    start "0-$((cores - 1))"
    docker exec "$container" psql -U postgres -qc 'CREATE DATABASE lake'
    # shellcheck disable=SC2086  # LOAD_ARGS is a list of flags
    lake load --ext "$ext" --url "$(url)" --load-memory "${LOAD_MEMORY:-4GiB}" ${LOAD_ARGS:-}
    stop
    if [ "${MODE:-run}" = profile ]; then
      start "$pg_cpus"
      echo "== profile: PostgreSQL on cores $pg_cpus, one reader on $reader_cpus =="
      taskset -c "$reader_cpus" \
        uv run --quiet --no-project --with "duckdb==$wheel" --with pyarrow python bench/lake.py "$dataset" \
        profile --ext "$ext" --url "$(url)" --memory-limit "${READER_MEMORY:-2GiB}" $args
      exit 0
    fi
    for round in $(seq 1 "$rounds"); do
      stop
      start "$pg_cpus"
      evict
      echo "== $dataset round $round of $rounds: PostgreSQL on cores $pg_cpus, $readers readers on $reader_cpus =="
      lake run --ext "$ext" --url "$(url)" --readers "$readers" --reader-cpus "$reader_cpus" \
        --pg-cpus "$pg_cpus" --pg-container "$container" --memory-limit "${READER_MEMORY:-2GiB}" \
        --round "$round" $args
    done
    stop

# Replaces the pre-release duckdb-<version> with the build in EXT_DIR (needs gh with write access).
release-publish EXT_DIR:
    #!/usr/bin/env bash
    set -euo pipefail
    export GH_HOST=github.com
    ext="{{EXT_DIR}}"
    [ -f "$ext/pgvfs.duckdb_extension" ] || { echo "no build in $ext" >&2; exit 0; }
    commit=$(git rev-parse --short=12 HEAD)
    today=$(date -u +%F)
    version=$(cat "$ext/DUCKDB_VERSION")
    wheel=$(cat "$ext/DUCKDB_PY")
    tag="duckdb-$version"
    work=$(mktemp -d)
    cp "$ext/pgvfs.duckdb_extension" "$work/"
    printf '{"duckdb_version": "%s", "wheel": "%s", "commit": "%s", "date": "%s"}\n' \
      "$version" "$wheel" "$commit" "$today" >"$work/build.json"
    gh release delete "$tag" --cleanup-tag --yes 2>/dev/null || true
    gh release create "$tag" --prerelease --target "$(git rev-parse HEAD)" \
      --title "pgvfs for DuckDB $version" \
      --notes "Built from $commit on $today for \`duckdb==$wheel\` (linux_amd64).

    \`\`\`sql
    INSTALL pgvfs FROM 'https://pgvfs.adonm.dev';   -- picks this build for DuckDB $version
    \`\`\`" \
      "$work/pgvfs.duckdb_extension" "$work/build.json"
    echo "published $tag"

# BENCH_JSONL holds lake.py's pass lines. They are appended to the bench pre-release's history.
release-bench BENCH_JSONL:
    #!/usr/bin/env bash
    set -euo pipefail
    export GH_HOST=github.com
    bench="{{BENCH_JSONL}}"
    grep -q '^{"pass' "$bench" || { echo "no benchmark results in $bench" >&2; exit 1; }
    commit=$(git rev-parse --short=12 HEAD)
    today=$(date -u +%F)
    work=$(mktemp -d)
    if ! gh release view bench >/dev/null 2>&1; then
      gh release create bench --prerelease --title "Weekly benchmark history" \
        --notes "Weekly benchmark (just bench city): one line per pass per run."
    fi
    gh release download bench --pattern history.jsonl --dir "$work" 2>/dev/null || true
    grep '^{"pass' "$bench" | python3 -c 'import json, os, sys; [print(json.dumps({**json.loads(line), "date": sys.argv[1], "commit": sys.argv[2], "cores": os.cpu_count()})) for line in sys.stdin]' "$today" "$commit" >>"$work/history.jsonl"
    gh release upload bench "$work/history.jsonl" --clobber
    echo "bench history: $(wc -l <"$work/history.jsonl") lines"

# Keeps every stable build and the newest 4 dev builds; deletes the older duckdb-* pre-releases.
release-prune:
    #!/usr/bin/env bash
    set -euo pipefail
    export GH_HOST=github.com
    keep=4
    versions=$(gh release list --limit 200 --json tagName -q '.[].tagName' | sed -n 's/^duckdb-//p' \
      | grep -Ev '^v[0-9]+\.[0-9]+\.[0-9]+$' || true)
    printf '%s\n' "$versions" | sort -V | head -n -"$keep" | while read -r version; do
      [ -n "$version" ] || continue
      gh release delete "duckdb-$version" --cleanup-tag --yes
      echo "pruned duckdb-$version"
    done

# Downloads every build (DIR/<version>/) and the benchmark history (DIR/bench/) from the releases.
release-fetch DIR:
    #!/usr/bin/env bash
    set -euo pipefail
    export GH_HOST=github.com
    dir="{{DIR}}"
    mkdir -p "$dir"
    for tag in $(gh release list --limit 200 --json tagName -q '.[].tagName' | grep '^duckdb-' || true); do
      gh release download "$tag" --dir "$dir/${tag#duckdb-}" --clobber
    done
    gh release download bench --pattern history.jsonl --dir "$dir/bench" --clobber 2>/dev/null || true
