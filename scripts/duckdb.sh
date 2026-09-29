#!/usr/bin/env bash
# Fetch what the extension links against: DuckDB's source tree (headers,
# footer script) and its prebuilt static libraries, for exactly the DuckDB
# build that will load it. Prints the prepared directory:
#
#   target/duckdb/<id>/{src/, libs/, version, wheel}
#
#   scripts/duckdb.sh release           pinned stable release (below)
#   scripts/duckdb.sh nightly [WHEEL]   a DuckDB 2.0 dev wheel (default: newest on PyPI)
#
# Release inputs are pinned by SHA-256. Nightly inputs are the static libs
# kept by DuckDB's own CI run on the wheel's exact commit; the download is
# checked against the digest GitHub records for the artifact. Downloading
# Actions artifacts needs a GitHub token (gh auth, or GH_TOKEN).
set -euo pipefail
cd "$(dirname "$0")/.."
log() { echo "duckdb.sh: $*" >&2; }

RELEASE=v1.5.6
RELEASE_SRC_SHA256=1fadcbe9e69e1470f9093b6bcde08daf477d729c449e59a807f45c346622099b
RELEASE_LIBS_SHA256=ab3d1d33951b8cb8bdcae740d8e06adf66bfb7ee880a3f7052d7a35088e16d2d
export GH_HOST=github.com

fetch_src() { # <git ref> <dir>
  mkdir -p "$2/src"
  curl -fsSL "https://github.com/duckdb/duckdb/archive/$1.tar.gz" | tar xz -C "$2/src" --strip-components 1
}

case "${1:?release | nightly [WHEEL]}" in
release)
  dir=target/duckdb/$RELEASE
  if [ ! -f "$dir/version" ]; then
    rm -rf "$dir" && mkdir -p "$dir"
    curl -fsSL -o "$dir/src.tar.gz" "https://github.com/duckdb/duckdb/archive/refs/tags/$RELEASE.tar.gz"
    curl -fsSL -o "$dir/libs.zip" \
      "https://github.com/duckdb/duckdb/releases/download/$RELEASE/static-libs-linux-amd64.zip"
    printf '%s  %s\n%s  %s\n' "$RELEASE_SRC_SHA256" "$dir/src.tar.gz" \
      "$RELEASE_LIBS_SHA256" "$dir/libs.zip" | sha256sum -c --quiet -
    mkdir -p "$dir/src" "$dir/libs"
    tar xzf "$dir/src.tar.gz" -C "$dir/src" --strip-components 1
    unzip -q "$dir/libs.zip" -d "$dir/libs"
    rm "$dir/src.tar.gz" "$dir/libs.zip"
    echo "${RELEASE#v}" >"$dir/wheel"
    echo "$RELEASE" >"$dir/version"
  fi
  ;;
nightly)
  wheel=${2:-$(curl -fsSL https://pypi.org/pypi/duckdb/json | python3 -c '
import json, sys
dev = [v for v in json.load(sys.stdin)["releases"] if v.startswith("2.") and ".dev" in v]
print(max(dev, key=lambda v: int(v.rsplit("dev", 1)[1])))')}
  read -r library sid < <(uv run --quiet --no-project --with "duckdb==$wheel" python -c '
import duckdb
print(*duckdb.connect().sql("PRAGMA version").fetchone()[:2])')
  # The extension footer carries DuckDB's version directory name: the tag,
  # unless it is a -dev build (ExtensionHelper::GetVersionDirectoryName).
  if [[ $library == *-dev* ]]; then version=$sid; else version=$library; fi
  dir=target/duckdb/$sid
  if [ ! -f "$dir/version" ]; then
    log "wheel $wheel is DuckDB $library ($sid)"
    sha=$(gh api "repos/duckdb/duckdb/commits/$sid" -q .sha)
    run=$(gh api "repos/duckdb/duckdb/actions/workflows/Main.yml/runs?head_sha=$sha&status=success" \
      -q '[.workflow_runs[] | select(.head_repository.id == .repository.id)][0].id // empty')
    [ -n "$run" ] || { log "no successful DuckDB Main run on $sha to take static libs from"; exit 1; }
    read -r artifact digest < <(gh api \
      "repos/duckdb/duckdb/actions/runs/$run/artifacts?name=duckdb-static-libs-linux-amd64.tar.gz" \
      -q '.artifacts[0] | "\(.id) \(.digest)"')
    [ -n "$artifact" ] || { log "run $run kept no linux-amd64 static libs (expired?)"; exit 1; }
    rm -rf "$dir" && mkdir -p "$dir/libs"
    log "static libs: run $run, artifact $artifact"
    gh api "repos/duckdb/duckdb/actions/artifacts/$artifact/zip" >"$dir/libs.tar.gz"
    echo "${digest#sha256:}  $dir/libs.tar.gz" | sha256sum -c --quiet -
    tar xzf "$dir/libs.tar.gz" -C "$dir/libs" && rm "$dir/libs.tar.gz"
    fetch_src "$sha" "$dir"
    echo "$wheel" >"$dir/wheel"
    echo "$version" >"$dir/version"
  fi
  ;;
*) log "unknown target: $1"; exit 2 ;;
esac
echo "$dir"
