#pragma once

#include "duckdb/common/file_opener.hpp"
#include "pgvfs.h"

namespace duckdb {

class ExtensionLoader;

// One PostgreSQL database as pgvfs sees it: the connection behind it and an id
// that names it in DuckDB's cache keys.
struct PgvfsTarget {
	PgvfsTarget() = default;
	PgvfsTarget(PgvfsConn *conn_, idx_t id_) : conn(conn_), id(id_) {
	}
	PgvfsConn *conn = nullptr;
	idx_t id = 0;
};

// The databases pgvfs:// is connected to, by volume (credentials as for
// pgvfs://<volume>/), shared with pgdb://.
class PgvfsConnector {
public:
	virtual ~PgvfsConnector() = default;
	virtual PgvfsTarget ConnectVolume(optional_ptr<FileOpener> opener, const string &volume) = 0;
	// A database connected earlier, by its id.
	virtual PgvfsConn *ConnById(idx_t id) = 0;
};

// Throws an IOException naming the operation and path, freeing `err`.
[[noreturn]] void PgvfsFail(const string &what, const string &path, char *err);

// pgdb:// (pgdb_filesystem.cpp).
void RegisterPgdb(ExtensionLoader &loader, PgvfsConnector &connector);

} // namespace duckdb
