# DuckDB extension build (extension-ci-tools), as DuckDB community extensions
# are built: `make release`, `make test`. Also see justfile.
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=pgvfs
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

include extension-ci-tools/makefiles/duckdb_extension.Makefile
