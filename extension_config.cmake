# Included by DuckDB's build: the extension in this repository.
duckdb_extension_load(pgvfs
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/extension/src/include
    LOAD_TESTS
)
