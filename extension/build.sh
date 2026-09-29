#!/usr/bin/env bash
# Link the pgvfs loadable extension from DuckDB's released static libraries,
# as DuckDB's own extensions are linked, with no DuckDB compile.
#
#   build.sh DUCKDB_SRC STATIC_LIBS LIBPGVFS VERSION OUT
#
# DUCKDB_SRC:  source tree of the release (headers, dummy loader, footer script)
# STATIC_LIBS: unpacked static-libs-linux-amd64.zip of the same release
# LIBPGVFS:    cargo build --release -p pgvfs (not stripped)
# VERSION:     the loading DuckDB's version, e.g. v1.5.6
set -euo pipefail
src=$1 libs=$2 rust=$3 version=$4 out=$5
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Hidden visibility plus --exclude-libs: only pgvfs_duckdb_cpp_init is
# exported, so this copy of DuckDB never interposes on the host's.
g++ -std=c++17 -O2 -fPIC -DNDEBUG -DDUCKDB_BUILD_LOADABLE_EXTENSION -fvisibility=hidden -shared \
  -I"$src/src/include" -I"$src/third_party/utf8proc/include" -I"$here/src/include" \
  "$here/src/pgvfs_extension.cpp" "$src/extension/loader/dummy_static_extension_loader.cpp" \
  -o "$work/pgvfs.duckdb_extension" \
  -Wl,--start-group "$libs"/libduckdb_*.a -Wl,--end-group "$rust" \
  -Wl,--gc-sections -Wl,--exclude-libs,ALL -lgcc_s -lutil -lrt -lpthread -lm -ldl
strip --strip-unneeded "$work/pgvfs.duckdb_extension"

printf linux_amd64 >"$work/platform"
cmake -DABI_TYPE=CPP -DEXTENSION="$work/pgvfs.duckdb_extension" -DPLATFORM_FILE="$work/platform" \
  -DVERSION_FIELD="$version" -DEXTENSION_VERSION=v0.1.0 -DNULL_FILE="$src/scripts/null.txt" \
  -P "$src/scripts/append_metadata.cmake"
mv "$work/pgvfs.duckdb_extension" "$out"
