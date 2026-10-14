// pgdb:// for DuckDB: a native DuckDB database stored in PostgreSQL (src/db.rs).
//
//   ATTACH 'pgdb://<volume>/<name>' AS db;               -- the one writer
//   ATTACH 'pgdb://<volume>/<name>' AS db (READ_ONLY);   -- any number of readers
//
// Attaching resolves the plain path to its newest gen, as pgdb://v/name@<gen>
// with a process-unique token. A refresher re-attaches read-only databases
// when a newer gen is published (ATTACH OR REPLACE), so a running transaction
// keeps the gen it started with; an explicit @<gen> stays there.
//
// DuckDB refuses a read-write and a read-only attach of one path, and only
// the writer's own attach may read its unpublished files (pgdb_open).
//
// Readers cache through DuckDB's external file cache, under the gen that last
// wrote each range (pgdb_runs): a new gen leaves every unchanged range.
#include "pgvfs_connector.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database_file_opener.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
// DuckDB 2.0 moved the external file cache's headers, and its reads return a
// handle group instead of a pointer into one pinned buffer.
#if __has_include("duckdb/storage/external_file_cache/caching_file_system.hpp")
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"
#define PGDB_CACHE_V2 1
#else
#include "duckdb/storage/caching_file_system.hpp"
#endif

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <set>
#include <thread>
#include <tuple>

namespace duckdb {

namespace {

constexpr const char *SCHEME = "pgdb://";
constexpr const char *CACHE_SCHEME = "pgdbc://";
constexpr const char *EXPIRED_MARK = "[pgdb gen reaped]";
// Attach tokens: process-wide, as DuckDB's attached paths are.
std::atomic<int64_t> next_token {0};
// Longest first, for parsing.
const char *const WAL_SUFFIXES[] = {".wal.checkpoint", ".wal.recovery", ".wal"};

struct PgdbPath {
	string volume;
	string name;
	int64_t gen = -1;   // -1: none given
	int64_t token = -1; // the attach (CanonicalizePath), -1: none
	string suffix;      // "" (the database) or a WAL suffix

	string Base() const {
		return string(SCHEME) + volume + "/" + name;
	}
	string Label() const {
		return Base() + (gen >= 0 ? "@" + std::to_string(gen) : "") +
		       (gen >= 0 && token >= 0 ? "~" + std::to_string(token) : "") + suffix;
	}
};

bool AllDigits(const string &s, idx_t from) {
	return from < s.size() && s.find_first_not_of("0123456789", from) == string::npos;
}

// pgdb://<volume>/<name>[@<gen>[~<token>]][<WAL suffix>]
PgdbPath Parse(const string &full) {
	if (!StringUtil::StartsWith(full, SCHEME)) {
		throw IOException("not a pgdb:// path: %s", full);
	}
	auto rest = full.substr(strlen(SCHEME));
	auto slash = rest.find('/');
	PgdbPath out;
	if (slash == string::npos || slash == 0 || slash + 1 == rest.size()) {
		throw IOException("pgdb path names no database: pgdb://<volume>/<name>, got %s", full);
	}
	out.volume = rest.substr(0, slash);
	auto tail = rest.substr(slash + 1);
	for (auto suffix : WAL_SUFFIXES) {
		if (StringUtil::EndsWith(tail, suffix) && tail.size() > strlen(suffix)) {
			out.suffix = suffix;
			tail = tail.substr(0, tail.size() - strlen(suffix));
			break;
		}
	}
	auto at = tail.rfind('@');
	if (at != string::npos) {
		auto tilde = tail.find('~', at);
		auto gen_end = tilde == string::npos ? tail.size() : tilde;
		if (gen_end > at + 1 && AllDigits(tail.substr(0, gen_end), at + 1) &&
		    (tilde == string::npos || AllDigits(tail, tilde + 1))) {
			out.gen = std::stoll(tail.substr(at + 1, gen_end - at - 1));
			if (tilde != string::npos) {
				out.token = std::stoll(tail.substr(tilde + 1));
			}
			tail = tail.substr(0, at);
		}
	}
	out.name = tail;
	return out;
}

class PgdbFileHandle : public FileHandle {
public:
	PgdbFileHandle(FileSystem &fs, const string &path, FileOpenFlags flags) : FileHandle(fs, path, flags) {
	}
	~PgdbFileHandle() override {
		if (file) {
			char *err = nullptr;
			if (pgdb_close(file, &err) != 0) {
				fprintf(stderr, "pgvfs: closing %s: %s\n", path.c_str(), err ? err : "?");
				pgvfs_free_str(err);
			}
		}
	}
	void Close() override {
		if (!file) {
			return;
		}
		auto *f = file;
		file = nullptr;
		char *err = nullptr;
		if (pgdb_close(f, &err) != 0) {
			PgvfsFail("close", path, err);
		}
	}

	PgdbFile *file = nullptr;
	PgdbInfo info {};
	PgvfsTarget target;
	optional_ptr<DatabaseInstance> db;
	idx_t position = 0;
};

// What the cache's inner reads see on a miss (bytes that never change):
// pgdbc://<target>/<db_id>/<file_id>@<gen>. Not registered with DuckDB.
class PgdbCacheFileSystem : public FileSystem {
public:
	explicit PgdbCacheFileSystem(PgvfsConnector &connector) : connector(connector) {
	}

	struct Handle : public FileHandle {
		Handle(FileSystem &fs, const string &path, FileOpenFlags flags) : FileHandle(fs, path, flags) {
		}
		void Close() override {
		}
		PgvfsConn *conn = nullptr;
		int32_t db_id = 0;
		int64_t file_id = 0;
		int64_t gen = 0;
	};

	std::string GetName() const override {
		return "PgdbCacheFileSystem";
	}
	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, CACHE_SCHEME);
	}
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener>) override {
		unsigned long long target = 0;
		int db_id = 0;
		long long file_id = 0, gen = 0;
		if (sscanf(path.c_str(), "pgdbc://%llu/%d/%lld@%lld", &target, &db_id, &file_id, &gen) != 4) {
			throw InternalException("bad pgdb cache path %s", path);
		}
		auto handle = make_uniq<Handle>(*this, path, flags);
		handle->conn = connector.ConnById(idx_t(target));
		handle->db_id = db_id;
		handle->file_id = file_id;
		handle->gen = gen;
		return std::move(handle);
	}
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		auto &h = handle.Cast<Handle>();
		char *err = nullptr;
		auto rc = pgdb_read_gen(h.conn, h.db_id, h.file_id, h.gen, static_cast<uint8_t *>(buffer), nr_bytes,
		                        int64_t(location), &err);
		if (rc == 2) {
			throw IOException("%s %s", EXPIRED_MARK, h.path);
		}
		if (rc != 0) {
			PgvfsFail("read", h.path, err);
		}
	}
	// Reads past the real end are zeros: pgdb_runs keeps them inside it.
	int64_t GetFileSize(FileHandle &) override {
		return int64_t(1) << 62;
	}
	string GetVersionTag(FileHandle &handle) override {
		return std::to_string(handle.Cast<Handle>().gen);
	}
	timestamp_t GetLastModifiedTime(FileHandle &) override {
		return timestamp_t(0);
	}
	bool OnDiskFile(FileHandle &) override {
		return false;
	}
	bool CanSeek() override {
		return true;
	}

private:
	PgvfsConnector &connector;
};

class PgdbFileSystem;

// Re-attaches this DuckDB's read-only pgdb:// databases when a newer gen is
// published. Holds only a weak reference: a Connection would keep the database
// open, and the thread stops when the filesystem goes.
struct PgdbRefresher : public enable_shared_from_this<PgdbRefresher> {
	std::mutex lock;
	std::condition_variable wake;
	bool started = false;
	bool stop = false;
	weak_ptr<DatabaseInstance> db;
	PgdbFileSystem *fs = nullptr;
	// Follows new gens: a plain attach, or a refresh. @<gen> does not.
	std::set<string> following;

	void Follow(const string &path, optional_ptr<FileOpener> opener) {
		std::lock_guard<std::mutex> guard(lock);
		following.insert(path);
		if (started || stop) {
			return;
		}
		auto instance = FileOpener::TryGetDatabase(opener);
		if (!instance) {
			return;
		}
		db = instance->shared_from_this();
		started = true;
		auto self = shared_from_this();
		std::thread([self] { self->Run(); }).detach();
	}

	bool Following(const string &path) {
		std::lock_guard<std::mutex> guard(lock);
		return following.count(path) > 0;
	}

	void Stop() {
		std::lock_guard<std::mutex> guard(lock);
		stop = true;
		wake.notify_all();
	}

	void Run();
	void Tick(DatabaseInstance &instance);
};

class PgdbFileSystem : public FileSystem {
public:
	explicit PgdbFileSystem(PgvfsConnector &connector)
	    : connector(connector), cache_fs(connector), refresher(make_shared_ptr<PgdbRefresher>()) {
		refresher->fs = this;
	}
	~PgdbFileSystem() override {
		refresher->Stop();
	}

	std::string GetName() const override {
		return "PgdbFileSystem";
	}

	bool CanHandleFile(const string &fpath) override {
		return StringUtil::StartsWith(fpath, SCHEME);
	}

	string CanonicalizePath(const string &path, optional_ptr<FileOpener> opener) override {
		auto p = Parse(path);
		if (p.gen >= 0 || !p.suffix.empty()) {
			return path;
		}
		int64_t gen = 0;
		if (!Latest(opener, p, gen)) {
			return path; // a new database
		}
		p.gen = gen;
		p.token = next_token++;
		auto canonical = p.Label();
		refresher->Follow(canonical, opener);
		return canonical;
	}

	bool Latest(optional_ptr<FileOpener> opener, const PgdbPath &p, int64_t &gen) {
		auto target = connector.ConnectVolume(opener, p.volume);
		char *err = nullptr;
		auto rc = pgdb_latest(target.conn, p.volume.c_str(), p.name.c_str(), &gen, &err);
		if (rc < 0) {
			PgvfsFail("open", p.Base(), err);
		}
		return rc == 0;
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener) override {
		auto p = Parse(path);
		auto target = connector.ConnectVolume(opener, p.volume);
		int mode = 0;
		if (flags.OpenForWriting()) {
			mode |= PGDB_WRITE;
			if (flags.CreateFileIfNotExists() || flags.OverwriteExistingFile()) {
				mode |= PGDB_CREATE;
			}
			if (flags.OverwriteExistingFile()) {
				mode |= PGDB_OVERWRITE;
			}
		}
		auto handle = make_uniq<PgdbFileHandle>(*this, path, flags);
		char *err = nullptr;
		auto rc = pgdb_open(target.conn, p.volume.c_str(), p.name.c_str(), p.gen, p.token, p.suffix.c_str(), mode,
		                    &handle->file, &handle->info, &err);
		if (rc < 0) {
			PgvfsFail("open", path, err);
		}
		if (rc == 1) {
			if (flags.ReturnNullIfNotExists()) {
				return nullptr;
			}
			throw IOException("pgdb file not found: %s", path);
		}
		handle->target = target;
		handle->db = FileOpener::TryGetDatabase(opener);
		if (flags.OpenForAppending()) {
			handle->position = idx_t(pgdb_size(handle->file));
		}
		return std::move(handle);
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		auto &h = handle.Cast<PgdbFileHandle>();
		auto size = pgdb_size(h.file);
		if (nr_bytes < 0 || int64_t(location) + nr_bytes > size) {
			throw IOException("pgdb read past end of %s (%lld bytes at %llu, size %lld)", h.path, (long long)nr_bytes,
			                  (unsigned long long)location, (long long)size);
		}
		if (nr_bytes == 0) {
			return;
		}
		auto out = static_cast<data_ptr_t>(buffer);
		if (h.info.writer || !h.db || !ReadCached(h, out, nr_bytes, int64_t(location))) {
			ReadDirect(h, out, nr_bytes, int64_t(location));
		}
	}

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		auto &h = handle.Cast<PgdbFileHandle>();
		auto left = pgdb_size(h.file) - int64_t(h.position);
		auto n = MinValue<int64_t>(nr_bytes, MaxValue<int64_t>(left, 0));
		Read(handle, buffer, n, h.position);
		h.position += idx_t(n);
		return n;
	}

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		auto &h = handle.Cast<PgdbFileHandle>();
		char *err = nullptr;
		if (pgdb_write(h.file, static_cast<const uint8_t *>(buffer), nr_bytes, int64_t(location), &err) != 0) {
			PgvfsFail("write", h.path, err);
		}
	}

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		auto &h = handle.Cast<PgdbFileHandle>();
		Write(handle, buffer, nr_bytes, h.position);
		h.position += idx_t(nr_bytes);
		return nr_bytes;
	}

	void Truncate(FileHandle &handle, int64_t new_size) override {
		auto &h = handle.Cast<PgdbFileHandle>();
		char *err = nullptr;
		if (pgdb_truncate(h.file, new_size, &err) != 0) {
			PgvfsFail("truncate", h.path, err);
		}
		h.position = MinValue<idx_t>(h.position, idx_t(new_size));
	}

	// Publishes everything the database's writer has written: a new gen.
	void FileSync(FileHandle &handle) override {
		auto &h = handle.Cast<PgdbFileHandle>();
		char *err = nullptr;
		if (pgdb_sync(h.file, &err) != 0) {
			PgvfsFail("sync", h.path, err);
		}
	}

	int64_t GetFileSize(FileHandle &handle) override {
		return pgdb_size(handle.Cast<PgdbFileHandle>().file);
	}

	timestamp_t GetLastModifiedTime(FileHandle &) override {
		return timestamp_t(0);
	}

	FileType GetFileType(FileHandle &) override {
		return FileType::FILE_TYPE_REGULAR;
	}

	FileMetadata Stats(FileHandle &handle) override {
		FileMetadata meta;
		meta.file_size = GetFileSize(handle);
		meta.last_modification_time = GetLastModifiedTime(handle);
		meta.file_type = FileType::FILE_TYPE_REGULAR;
		return meta;
	}

	void Seek(FileHandle &handle, idx_t location) override {
		handle.Cast<PgdbFileHandle>().position = location;
	}

	void Reset(FileHandle &handle) override {
		handle.Cast<PgdbFileHandle>().position = 0;
	}

	idx_t SeekPosition(FileHandle &handle) override {
		return handle.Cast<PgdbFileHandle>().position;
	}

	bool CanSeek() override {
		return true;
	}

	// Remote: DuckDB then batches contiguous block reads (storage_block_prefetch).
	bool OnDiskFile(FileHandle &) override {
		return false;
	}

	bool IsPipe(const string &, optional_ptr<FileOpener>) override {
		return false;
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override {
		auto p = Parse(filename);
		auto target = connector.ConnectVolume(opener, p.volume);
		char *err = nullptr;
		auto rc =
		    pgdb_exists(target.conn, p.volume.c_str(), p.name.c_str(), p.gen, p.token, p.suffix.c_str(), &err);
		if (rc < 0) {
			PgvfsFail("stat", filename, err);
		}
		return rc == 0;
	}

	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		if (!TryRemoveFile(filename, opener)) {
			throw IOException("pgdb file not found: %s", filename);
		}
	}

	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		auto p = Parse(filename);
		auto target = connector.ConnectVolume(opener, p.volume);
		char *err = nullptr;
		auto rc = pgdb_remove(target.conn, p.volume.c_str(), p.name.c_str(), p.suffix.c_str(), &err);
		if (rc < 0) {
			PgvfsFail("remove", filename, err);
		}
		return rc == 0;
	}

	// DuckDB moves .wal.checkpoint over .wal at the end of a checkpoint.
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override {
		auto from = Parse(source);
		auto to = Parse(target);
		if (from.volume != to.volume || from.name != to.name) {
			throw NotImplementedException("pgdb moves files only within one database: %s -> %s", source, target);
		}
		auto conn = connector.ConnectVolume(opener, from.volume);
		char *err = nullptr;
		if (pgdb_rename(conn.conn, from.volume.c_str(), from.name.c_str(), from.suffix.c_str(), to.suffix.c_str(),
		                &err) != 0) {
			PgvfsFail("move", source, err);
		}
	}

	bool DirectoryExists(const string &, optional_ptr<FileOpener>) override {
		return false;
	}

	void CreateDirectory(const string &directory, optional_ptr<FileOpener>) override {
		throw NotImplementedException("pgdb:// holds databases, not directories: %s (spill to a local "
		                              "temp_directory)",
		                              directory);
	}

	void CreateDirectoriesRecursive(const string &directory, optional_ptr<FileOpener> opener) override {
		CreateDirectory(directory, opener);
	}

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener) override {
		if (HasGlob(path)) {
			throw NotImplementedException("pgdb:// cannot list files: %s", path);
		}
		vector<OpenFileInfo> out;
		if (FileExists(path, opener)) {
			out.emplace_back(path);
		}
		return out;
	}

	PgvfsConnector &connector;
	PgdbCacheFileSystem cache_fs;
	shared_ptr<PgdbRefresher> refresher;

private:
	void ReadDirect(PgdbFileHandle &h, data_ptr_t out, int64_t len, int64_t pos) {
		char *err = nullptr;
		if (pgdb_read(h.file, out, len, pos, &err) != 0) {
			PgvfsFail("read", h.path, err);
		}
	}

	static void CollectRun(void *ctx, int64_t offset, int64_t len, int64_t gen) {
		static_cast<vector<std::tuple<int64_t, int64_t, int64_t>> *>(ctx)->emplace_back(offset, len, gen);
	}

	// A range per key gen through DuckDB's external file cache; false if the read
	// cannot be keyed (read it directly).
	bool ReadCached(PgdbFileHandle &h, data_ptr_t out, int64_t len, int64_t pos) {
		vector<std::tuple<int64_t, int64_t, int64_t>> runs;
		if (pgdb_runs(h.file, pos, len, CollectRun, &runs) != 0) {
			return false;
		}
		CachingFileSystem cache(cache_fs, *h.db);
		for (auto &run : runs) {
			int64_t offset, n, gen;
			std::tie(offset, n, gen) = run;
			OpenFileInfo info(StringUtil::Format("%s%llu/%d/%lld@%lld", CACHE_SCHEME, (unsigned long long)h.target.id,
			                                     h.info.db_id, (long long)h.info.file_id, (long long)gen));
			info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
			// The bytes under a key never change.
			info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
			try {
				auto file = cache.OpenFile(info, FileFlags::FILE_FLAGS_READ);
#ifdef PGDB_CACHE_V2
				file->Read(idx_t(n), idx_t(offset)).CopyTo(out + (offset - pos), idx_t(n));
#else
				data_ptr_t data = nullptr;
				auto pin = file->Read(data, idx_t(n), idx_t(offset));
				memcpy(out + (offset - pos), data, size_t(n));
#endif
			} catch (std::exception &e) {
				if (string(e.what()).find(EXPIRED_MARK) == string::npos) {
					throw;
				}
				ReadDirect(h, out + (offset - pos), n, offset); // fails if this gen is reaped too
			}
		}
		return true;
	}
};

void PgdbRefresher::Run() {
	auto last = std::chrono::steady_clock::now();
	while (true) {
		double interval = 0;
		if (auto instance = db.lock()) {
			Value value;
			if (instance->TryGetCurrentSetting("pgdb_refresh_interval", value) && !value.IsNull()) {
				interval = value.GetValue<double>();
			}
		} else {
			return;
		}
		{
			// Short waits, so a changed interval applies within a second.
			std::unique_lock<std::mutex> guard(lock);
			auto wait = interval > 0 ? MinValue<double>(interval, 1.0) : 1.0;
			wake.wait_for(guard, std::chrono::duration<double>(wait), [&] { return stop; });
			if (stop) {
				return;
			}
		}
		auto now = std::chrono::steady_clock::now();
		if (interval <= 0 || std::chrono::duration<double>(now - last).count() < interval) {
			continue;
		}
		last = now;
		auto instance = db.lock();
		if (!instance) {
			return;
		}
		try {
			Tick(*instance);
		} catch (std::exception &e) {
			fprintf(stderr, "pgvfs: pgdb refresh: %s\n", e.what());
		}
	}
}

// One round: re-attach each followed read-only database whose gen is behind.
void PgdbRefresher::Tick(DatabaseInstance &instance) {
	Connection con(instance);
	auto attached = con.Query("SELECT database_name, path FROM duckdb_databases() "
	                          "WHERE readonly AND starts_with(path, 'pgdb://')");
	if (attached->HasError()) {
		return;
	}
	// Database-level, as DuckDB's own WAL I/O.
	DatabaseFileOpener opener(instance);
	for (auto &row : *attached) {
		auto name = row.GetValue<string>(0);
		auto path = row.GetValue<string>(1);
		if (!Following(path)) {
			continue;
		}
		auto p = Parse(path);
		int64_t gen = 0;
		if (!fs->Latest(&opener, p, gen) || gen <= p.gen) {
			continue;
		}
		p.gen = gen;
		p.token = -1; // read-only: no token needed
		auto next = p.Label();
		{
			std::lock_guard<std::mutex> guard(lock);
			following.insert(next);
		}
		auto result = con.Query("ATTACH OR REPLACE '" + StringUtil::Replace(next, "'", "''") + "' AS " +
		                        SQLIdentifier::ToString(name) + " (READ_ONLY)");
		std::lock_guard<std::mutex> guard(lock);
		following.erase(path);
		if (result->HasError()) {
			// Stop following it (no retry loop): it stays at its gen.
			fprintf(stderr, "pgvfs: not refreshing %s: %s\n", path.c_str(), result->GetError().c_str());
			following.erase(next);
		}
	}
}

} // namespace

void RegisterPgdb(ExtensionLoader &loader, PgvfsConnector &connector) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("pgdb_refresh_interval",
	                          "Seconds between checks for a newer published version of each pgdb:// database "
	                          "attached READ_ONLY by its plain path; a newer one is attached in its place (running "
	                          "transactions keep theirs). 0 turns refreshing off",
	                          LogicalType::DOUBLE, Value::DOUBLE(10), nullptr, SetScope::GLOBAL);
	db.GetFileSystem().RegisterSubSystem(make_uniq<PgdbFileSystem>(connector));
}

} // namespace duckdb
