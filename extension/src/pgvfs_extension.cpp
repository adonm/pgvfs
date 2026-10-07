// pgvfs:// for DuckDB: files stored as PostgreSQL rows (src/).
//
// This file only adapts DuckDB's C++ FileSystem interface, which the stable C
// API cannot register, to the Rust storage layer's C ABI (pgvfs.h), and adds
// SQL functions for tantivy full-text indexes stored the same way. Paths are
// pgvfs://<volume>/<path>. Files are immutable once written: a write streams
// into a new file_id and publishes on Close(); the file_id is the cache
// version tag, so DuckDB's external file cache never serves stale bytes.
#include "pgvfs_extension.hpp"
#include "pgvfs.h"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <cstdlib>
#include <mutex>

namespace duckdb {

namespace {

constexpr const char *SCHEME = "pgvfs://";
constexpr idx_t SCHEME_LEN = 8;

struct PgvfsPath {
	string volume;
	string path; // may be empty or end in '/' for directory operations
};

PgvfsPath Parse(const string &full) {
	if (full.rfind(SCHEME, 0) != 0) {
		throw IOException("not a pgvfs:// path: %s", full);
	}
	auto rest = full.substr(SCHEME_LEN);
	auto slash = rest.find('/');
	PgvfsPath out;
	out.volume = rest.substr(0, slash);
	out.path = slash == string::npos ? "" : rest.substr(slash + 1);
	if (out.volume.empty()) {
		throw IOException("pgvfs path needs a volume: pgvfs://<volume>/<path>, got %s", full);
	}
	return out;
}

PgvfsPath ParseFile(const string &full) {
	auto p = Parse(full);
	if (p.path.empty() || p.path.back() == '/') {
		throw IOException("pgvfs path names no file: %s", full);
	}
	return p;
}

string DirPrefix(const string &path) {
	return path.empty() || path.back() == '/' ? path : path + "/";
}

[[noreturn]] void Fail(const string &what, const string &path, char *err) {
	string msg = err ? string(err) : "unknown error";
	pgvfs_free_str(err);
	throw IOException("pgvfs %s %s: %s", what, path, msg);
}

void CollectPath(void *ctx, const char *path, size_t len) {
	static_cast<vector<string> *>(ctx)->emplace_back(path, len);
}

// Glob by path segment: '*' and '?' stay within a segment, '**' spans any.
bool MatchSegments(const vector<string> &key, idx_t k, const vector<string> &pat, idx_t p) {
	if (p == pat.size()) {
		return k == key.size();
	}
	if (pat[p] == "**") {
		for (idx_t i = k; i <= key.size(); i++) {
			if (MatchSegments(key, i, pat, p + 1)) {
				return true;
			}
		}
		return false;
	}
	return k < key.size() && Glob(key[k].c_str(), key[k].size(), pat[p].c_str(), pat[p].size()) &&
	       MatchSegments(key, k + 1, pat, p + 1);
}

class PgvfsFileSystem;

class PgvfsFileHandle : public FileHandle {
public:
	PgvfsFileHandle(FileSystem &fs, const string &path, FileOpenFlags flags, PgvfsConn *conn)
	    : FileHandle(fs, path, flags), conn(conn) {
	}
	~PgvfsFileHandle() override {
		if (writer) {
			pgvfs_writer_abort(writer); // never published without Close()
		}
	}
	void Close() override {
		if (!writer) {
			return;
		}
		auto *w = writer;
		writer = nullptr;
		char *err = nullptr;
		if (pgvfs_writer_publish(w, &err) != 0) {
			Fail("publish", path, err);
		}
		file.size = int64_t(written);
	}

	PgvfsConn *conn;
	PgvfsFile file {};
	PgvfsWriter *writer = nullptr;
	idx_t written = 0;
	idx_t position = 0;
};

class PgvfsFileSystem : public FileSystem {
public:
	~PgvfsFileSystem() override {
		if (conn) {
			pgvfs_disconnect(conn);
		}
	}

	std::string GetName() const override {
		return "PgvfsFileSystem";
	}

	bool CanHandleFile(const string &fpath) override {
		return fpath.rfind(SCHEME, 0) == 0;
	}

	string CanonicalizePath(const string &path, optional_ptr<FileOpener>) override {
		return path;
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener) override {
		auto p = ParseFile(path);
		auto *c = Conn(opener);
		if (flags.OpenForAppending() || (flags.OpenForReading() && flags.OpenForWriting())) {
			throw NotImplementedException("pgvfs files are written once, sequentially: %s", path);
		}
		auto handle = make_uniq<PgvfsFileHandle>(*this, path, flags, c);
		if (flags.OpenForWriting()) {
			// Every write makes a new file, replacing any at this path on Close().
			if (!flags.CreateFileIfNotExists() && !flags.OverwriteExistingFile()) {
				throw NotImplementedException("pgvfs cannot rewrite a file in place: %s", path);
			}
			if (flags.ExclusiveCreate() || flags.ReturnNullIfExists()) {
				PgvfsFile existing;
				char *err = nullptr;
				auto rc = pgvfs_open(c, p.volume.c_str(), p.path.c_str(), &existing, &err);
				if (rc < 0) {
					Fail("open", path, err);
				}
				if (rc == 0) {
					if (flags.ReturnNullIfExists()) {
						return nullptr;
					}
					throw IOException("pgvfs file already exists: %s", path);
				}
			}
			char *err = nullptr;
			handle->writer = pgvfs_writer_open(c, p.volume.c_str(), p.path.c_str(), &err);
			if (!handle->writer) {
				Fail("create", path, err);
			}
			return std::move(handle);
		}
		char *err = nullptr;
		auto rc = pgvfs_open(c, p.volume.c_str(), p.path.c_str(), &handle->file, &err);
		if (rc < 0) {
			Fail("open", path, err);
		}
		if (rc == 1) {
			if (flags.ReturnNullIfNotExists()) {
				return nullptr;
			}
			throw IOException("pgvfs file not found: %s", path);
		}
		return std::move(handle);
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		auto &h = Reader(handle);
		if (nr_bytes < 0 || int64_t(location) + nr_bytes > h.file.size) {
			throw IOException("pgvfs read past end of %s (%lld bytes at %llu, size %lld)", h.path,
			                  (long long)nr_bytes, (unsigned long long)location, (long long)h.file.size);
		}
		char *err = nullptr;
		if (pgvfs_read(h.conn, &h.file, static_cast<uint8_t *>(buffer), nr_bytes, int64_t(location), &err) != 0) {
			Fail("read", h.path, err);
		}
	}

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		auto &h = Reader(handle);
		auto left = h.file.size - int64_t(h.position);
		auto n = MinValue<int64_t>(nr_bytes, MaxValue<int64_t>(left, 0));
		Read(handle, buffer, n, h.position);
		h.position += idx_t(n);
		return n;
	}

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		auto &h = handle.Cast<PgvfsFileHandle>();
		if (location != h.written) {
			throw NotImplementedException("pgvfs writes are sequential: %s (write at %llu, size %llu)", h.path,
			                              (unsigned long long)location, (unsigned long long)h.written);
		}
		Write(handle, buffer, nr_bytes);
	}

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		auto &h = handle.Cast<PgvfsFileHandle>();
		if (!h.writer) {
			throw IOException("pgvfs file is not open for writing: %s", h.path);
		}
		char *err = nullptr;
		if (pgvfs_writer_write(h.writer, static_cast<const uint8_t *>(buffer), nr_bytes, &err) != 0) {
			Fail("write", h.path, err);
		}
		h.written += idx_t(nr_bytes);
		h.position = h.written;
		return nr_bytes;
	}

	void FileSync(FileHandle &) override {
		// Durability is the publishing commit in Close().
	}

	int64_t GetFileSize(FileHandle &handle) override {
		auto &h = handle.Cast<PgvfsFileHandle>();
		return h.writer ? int64_t(h.written) : h.file.size;
	}

	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		return timestamp_t(handle.Cast<PgvfsFileHandle>().file.created_us);
	}

	string GetVersionTag(FileHandle &handle) override {
		return std::to_string(handle.Cast<PgvfsFileHandle>().file.file_id);
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
		handle.Cast<PgvfsFileHandle>().position = location;
	}

	void Reset(FileHandle &handle) override {
		handle.Cast<PgvfsFileHandle>().position = 0;
	}

	idx_t SeekPosition(FileHandle &handle) override {
		return handle.Cast<PgvfsFileHandle>().position;
	}

	bool CanSeek() override {
		return true;
	}

	// Remote: the Parquet reader then prefetches whole column-chunk ranges.
	bool OnDiskFile(FileHandle &) override {
		return false;
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override {
		auto p = Parse(filename);
		if (p.path.empty() || p.path.back() == '/') {
			return false;
		}
		PgvfsFile f;
		char *err = nullptr;
		auto rc = pgvfs_open(Conn(opener), p.volume.c_str(), p.path.c_str(), &f, &err);
		if (rc < 0) {
			Fail("stat", filename, err);
		}
		return rc == 0;
	}

	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		if (!TryRemoveFile(filename, opener)) {
			throw IOException("pgvfs file not found: %s", filename);
		}
	}

	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		auto p = ParseFile(filename);
		char *err = nullptr;
		auto rc = pgvfs_remove(Conn(opener), p.volume.c_str(), p.path.c_str(), &err);
		if (rc < 0) {
			Fail("remove", filename, err);
		}
		return rc == 0;
	}

	// DuckDB's COPY overwrites an existing non-remote path by writing a temp
	// file and moving it into place.
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override {
		auto from = ParseFile(source);
		auto to = ParseFile(target);
		if (from.volume != to.volume) {
			throw NotImplementedException("pgvfs cannot move files between volumes: %s -> %s", source, target);
		}
		char *err = nullptr;
		if (pgvfs_rename(Conn(opener), from.volume.c_str(), from.path.c_str(), to.path.c_str(), &err) != 0) {
			Fail("move", source, err);
		}
	}

	// Directories are key prefixes: they exist while they hold a file.
	// DuckLake never removes directories, so RemoveDirectory stays
	// unimplemented (DuckDB's base class throws).
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override {
		auto p = Parse(directory);
		return !List(Conn(opener), p.volume, DirPrefix(p.path), 1).empty();
	}

	void CreateDirectory(const string &, optional_ptr<FileOpener>) override {
	}

	void CreateDirectoriesRecursive(const string &, optional_ptr<FileOpener>) override {
	}

	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener) override {
		auto p = Parse(directory);
		auto prefix = DirPrefix(p.path);
		string last_dir;
		auto keys = List(Conn(opener), p.volume, prefix, -1);
		for (auto &key : keys) {
			auto rest = key.substr(prefix.size());
			auto slash = rest.find('/');
			if (slash == string::npos) {
				callback(rest, false);
			} else if (rest.substr(0, slash) != last_dir) {
				last_dir = rest.substr(0, slash);
				callback(last_dir, true);
			}
		}
		return !keys.empty();
	}

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener) override {
		auto p = Parse(path);
		vector<OpenFileInfo> out;
		if (!HasGlob(p.path)) {
			if (FileExists(path, opener)) {
				out.emplace_back(path);
			}
			return out;
		}
		auto first = p.path.find_first_of("*?[");
		auto cut = p.path.rfind('/', first);
		auto prefix = cut == string::npos ? "" : p.path.substr(0, cut + 1);
		auto pattern = StringUtil::Split(p.path, '/');
		auto base = string(SCHEME) + p.volume + "/";
		for (auto &key : List(Conn(opener), p.volume, prefix, -1)) {
			if (MatchSegments(StringUtil::Split(key, '/'), 0, pattern, 0)) {
				out.emplace_back(base + key);
			}
		}
		return out;
	}

	// For SQL functions: this database's connection.
	PgvfsConn *Connect(ClientContext &context) {
		ClientContextFileOpener opener(context);
		return Conn(&opener);
	}

private:
	PgvfsFileHandle &Reader(FileHandle &handle) {
		auto &h = handle.Cast<PgvfsFileHandle>();
		if (h.writer) {
			throw IOException("pgvfs file is open for writing: %s", h.path);
		}
		return h;
	}

	vector<string> List(PgvfsConn *c, const string &volume, const string &prefix, int64_t limit) {
		vector<string> keys;
		char *err = nullptr;
		if (pgvfs_list(c, volume.c_str(), prefix.c_str(), limit, CollectPath, &keys, &err) != 0) {
			Fail("list", string(SCHEME) + volume + "/" + prefix, err);
		}
		return keys;
	}

	// One connection pool per database, opened on first use. Credentials
	// resolve like the postgres extension's, so one secret can serve both
	// DuckLake's catalog and pgvfs: the postgres secret named by pgvfs_secret,
	// else $PGVFS_URL, else the unnamed default postgres secret.
	PgvfsConn *Conn(optional_ptr<FileOpener> opener) {
		auto target = ConnectionString(opener);
		std::lock_guard<std::mutex> guard(lock);
		if (conn) {
			if (!target.empty() && target != conn_target) {
				throw InvalidInputException("pgvfs is already connected to another database in this process");
			}
			return conn;
		}
		if (target.empty()) {
			throw InvalidInputException(
			    "pgvfs needs PostgreSQL credentials: CREATE SECRET (TYPE postgres, ...), SET pgvfs_secret, "
			    "or PGVFS_URL");
		}
		// Size pgvfs's I/O threads and pool to this database's DuckDB threads.
		int64_t threads = 0;
		auto db = FileOpener::TryGetDatabase(opener);
		if (db) {
			threads = int64_t(TaskScheduler::GetScheduler(*db).NumberOfThreads());
		}
		char *err = nullptr;
		conn = pgvfs_connect(target.c_str(), threads, &err);
		if (!conn) {
			Fail("connect to", "PostgreSQL", err);
		}
		conn_target = target;
		return conn;
	}

	static string ConnectionString(optional_ptr<FileOpener> opener) {
		string name;
		Value setting;
		if (FileOpener::TryGetCurrentSetting(opener, "pgvfs_secret", setting) && !setting.IsNull()) {
			name = setting.ToString();
		}
		bool explicit_secret = !name.empty();
		if (!explicit_secret) {
			auto env = std::getenv("PGVFS_URL");
			if (env && *env) {
				return env;
			}
			name = "__default_postgres";
		}
		auto context = FileOpener::TryGetClientContext(opener);
		if (!context) {
			return "";
		}
		auto &secrets = SecretManager::Get(*context);
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(*context);
		auto entry = secrets.GetSecretByName(transaction, name);
		if (!entry) {
			entry = secrets.GetSecretByName(transaction, name, "local_file");
		}
		if (!entry) {
			if (explicit_secret) {
				throw InvalidInputException("pgvfs_secret: no secret named \"%s\"", name);
			}
			return "";
		}
		if (entry->secret->GetType() != "postgres") {
			throw InvalidInputException("pgvfs_secret \"%s\" is a %s secret, not postgres", name,
			                            entry->secret->GetType());
		}
		return SecretToConnectionString(dynamic_cast<const KeyValueSecret &>(*entry->secret));
	}

	// A postgres secret as a key='value' connection string (tokio-postgres
	// rejects options it does not support, e.g. passfile or sslrootcert).
	static string SecretToConnectionString(const KeyValueSecret &secret) {
		auto uri = secret.TryGetValue("uri");
		if (!uri.IsNull()) {
			return uri.ToString();
		}
		if (!secret.TryGetValue("aws_rds_secret").IsNull()) {
			throw NotImplementedException("pgvfs does not support RDS IAM (aws_rds_secret) postgres secrets");
		}
		string out;
		for (auto &entry : secret.secret_map) {
			if (entry.second.IsNull()) {
				continue;
			}
			string value;
			for (char c : entry.second.ToString()) {
				if (c == '\\' || c == '\'') {
					value += '\\';
				}
				value += c;
			}
			out += (out.empty() ? "" : " ") + entry.first + "='" + value + "'";
		}
		return out;
	}

	std::mutex lock;
	PgvfsConn *conn = nullptr;
	string conn_target;
};

// pgvfs_stats(): JSON of process-wide counters (opens, reads, bytes, time
// spent in pgvfs, range queries). Cumulative; diff two samples.
void StatsFunction(DataChunk &, ExpressionState &, Vector &result) {
	char *json = pgvfs_stats();
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<string_t>(result)[0] = StringVector::AddString(result, json);
	pgvfs_free_str(json);
}

// Tantivy full-text indexes stored in pgvfs (src/index.rs). Three thin
// functions pass tantivy's own JSON through; SQL macros (below) compose them.

// How SQL functions find this database's PgvfsFileSystem.
template <class INFO>
struct FsInfo : public INFO {
	explicit FsInfo(PgvfsFileSystem &fs) : fs(fs) {
	}
	PgvfsFileSystem &fs;
};

// A split is a directory: pgvfs://<volume>/<path>.
PgvfsPath ParseIndex(const Value &url) {
	if (url.IsNull()) {
		throw InvalidInputException("a tantivy index path cannot be NULL");
	}
	auto p = Parse(url.ToString());
	if (p.path.empty()) {
		throw InvalidInputException("a tantivy index needs a directory: pgvfs://<volume>/<path>, got %s",
		                            url.ToString());
	}
	return p;
}

// tantivy_index(index, schema, doc [, options]): an aggregate that builds a
// split, an immutable tantivy index, at `index` from the documents (JSON
// objects) of its rows, committed as the aggregate finishes. Arguments are
// per row, so GROUP BY builds one split per group. The writer only.
struct Split {
	PgvfsIndexBuild *build = nullptr;
	string url;
	PgvfsPath where;
};

// One query's splits, by path.
struct IndexJob {
	~IndexJob() {
		for (auto &split : splits) {
			if (split.second->build) {
				pgvfs_index_abort(split.second->build);
			}
		}
	}

	Split &Get(const string &url, const string &schema, const string &options) {
		std::lock_guard<std::mutex> guard(lock);
		auto &split = splits[url];
		if (!split) {
			split = make_uniq<Split>();
			split->url = url;
			split->where = ParseIndex(Value(url));
			char *err = nullptr;
			split->build = pgvfs_index_open(conn, split->where.volume.c_str(), split->where.path.c_str(),
			                                schema.c_str(), options.empty() ? nullptr : options.c_str(), &err);
			if (!split->build) {
				Fail("index", url, err);
			}
		}
		return *split;
	}

	int64_t Commit(Split &split) {
		PgvfsIndexBuild *build;
		{
			std::lock_guard<std::mutex> guard(lock);
			build = split.build;
			split.build = nullptr;
		}
		if (!build) {
			throw InvalidInputException("tantivy_index: %s is committed twice", split.url);
		}
		char *err = nullptr;
		auto docs = pgvfs_index_commit(build, &err);
		if (docs < 0) {
			Fail("index", split.url, err);
		}
		return docs;
	}

	PgvfsConn *conn = nullptr;
	std::mutex lock;
	unordered_map<string, unique_ptr<Split>> splits;
};

struct IndexBindData : public FunctionData {
	explicit IndexBindData(shared_ptr<IndexJob> job) : job(std::move(job)) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<IndexBindData>(job);
	}
	bool Equals(const FunctionData &other) const override {
		return job == other.Cast<IndexBindData>().job;
	}
	shared_ptr<IndexJob> job;
};

struct IndexState {
	Split *split;
	int64_t docs;
};

unique_ptr<FunctionData> IndexBind(ClientContext &context, AggregateFunction &function,
                                   vector<unique_ptr<Expression>> &) {
	auto job = make_shared_ptr<IndexJob>();
	job->conn = function.function_info->Cast<FsInfo<AggregateFunctionInfo>>().fs.Connect(context);
	return make_uniq<IndexBindData>(std::move(job));
}

void IndexInitialize(const AggregateFunction &, data_ptr_t state) {
	*reinterpret_cast<IndexState *>(state) = {nullptr, 0};
}

void IndexUpdate(Vector inputs[], AggregateInputData &input, idx_t input_count, Vector &states, idx_t count) {
	auto &job = *input.bind_data->Cast<IndexBindData>().job;
	UnifiedVectorFormat args[4], state_format;
	for (idx_t a = 0; a < input_count; a++) {
		inputs[a].ToUnifiedFormat(count, args[a]);
	}
	states.ToUnifiedFormat(count, state_format);
	auto state_ptrs = UnifiedVectorFormat::GetData<IndexState *>(state_format);
	auto text = [&](idx_t a, idx_t i, const char *what) -> string {
		auto row = args[a].sel->get_index(i);
		if (!args[a].validity.RowIsValid(row)) {
			if (a == 3) {
				return "";
			}
			throw InvalidInputException("tantivy_index: the %s cannot be NULL", what);
		}
		return UnifiedVectorFormat::GetData<string_t>(args[a])[row].GetString();
	};
	for (idx_t i = 0; i < count; i++) {
		auto doc_row = args[2].sel->get_index(i);
		if (!args[2].validity.RowIsValid(doc_row)) {
			continue;
		}
		auto &state = *state_ptrs[state_format.sel->get_index(i)];
		auto url = text(0, i, "index");
		if (!state.split) {
			state.split = &job.Get(url, text(1, i, "schema"), input_count > 3 ? text(3, i, "options") : "");
		} else if (state.split->url != url) {
			throw InvalidInputException("tantivy_index builds one split per group: %s and %s", state.split->url,
			                            url);
		}
		auto doc = UnifiedVectorFormat::GetData<string_t>(args[2])[doc_row];
		char *err = nullptr;
		if (pgvfs_index_add(state.split->build, doc.GetData(), doc.GetSize(), &err) != 0) {
			Fail("index", url, err);
		}
		state.docs++;
	}
}

void IndexSimpleUpdate(Vector inputs[], AggregateInputData &input, idx_t input_count, data_ptr_t state, idx_t count) {
	Vector states(Value::POINTER(CastPointerToValue(state)));
	IndexUpdate(inputs, input, input_count, states, count);
}

void IndexCombine(Vector &source, Vector &target, AggregateInputData &, idx_t count) {
	auto from = FlatVector::GetData<IndexState *>(source);
	auto to = FlatVector::GetData<IndexState *>(target);
	for (idx_t i = 0; i < count; i++) {
		if (!to[i]->split) {
			to[i]->split = from[i]->split;
		} else if (from[i]->split && from[i]->split != to[i]->split) {
			throw InvalidInputException("tantivy_index builds one split per group: %s and %s", to[i]->split->url,
			                            from[i]->split->url);
		}
		to[i]->docs += from[i]->docs;
	}
}

void IndexFinalize(Vector &states, AggregateInputData &input, Vector &result, idx_t count, idx_t offset) {
	auto &job = *input.bind_data->Cast<IndexBindData>().job;
	UnifiedVectorFormat state_format;
	states.ToUnifiedFormat(count, state_format);
	auto state_ptrs = UnifiedVectorFormat::GetData<IndexState *>(state_format);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto docs = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *state_ptrs[state_format.sel->get_index(i)];
		// No documents, no split.
		docs[offset + i] = state.split ? job.Commit(*state.split) : 0;
	}
}

// tantivy_search(index, query [, options]): tantivy's query language over a
// split; (score, doc) per hit, best first, doc holding the stored fields. An
// in-out function, so arguments may be columns: FROM splits s CROSS JOIN
// tantivy_search(s.path, 'cats') searches every split listed. (In-out
// functions take no named parameters, hence options as JSON.)
struct SearchBindData : public TableFunctionData {
	PgvfsConn *conn = nullptr;
};

struct SearchState : public LocalTableFunctionState {
	idx_t row = 0;
	bool searched = false;
	vector<std::pair<double, string>> hits;
	idx_t next = 0;
};

void CollectHit(void *ctx, double score, const char *doc, size_t len) {
	static_cast<SearchState *>(ctx)->hits.emplace_back(score, string(doc, len));
}

unique_ptr<FunctionData> SearchBind(ClientContext &context, TableFunctionBindInput &input,
                                    vector<LogicalType> &types, vector<string> &names) {
	auto data = make_uniq<SearchBindData>();
	data->conn = input.info->Cast<FsInfo<TableFunctionInfo>>().fs.Connect(context);
	types = {LogicalType::DOUBLE, LogicalType::JSON()};
	names = {"score", "doc"};
	return std::move(data);
}

unique_ptr<LocalTableFunctionState> SearchInit(ExecutionContext &, TableFunctionInitInput &,
                                               GlobalTableFunctionState *) {
	return make_uniq<SearchState>();
}

OperatorResultType SearchInOut(ExecutionContext &, TableFunctionInput &data, DataChunk &input, DataChunk &output) {
	auto &bind = data.bind_data->Cast<SearchBindData>();
	auto &state = data.local_state->Cast<SearchState>();
	while (true) {
		if (!state.searched) {
			if (state.row >= input.size()) {
				state.row = 0;
				return OperatorResultType::NEED_MORE_INPUT;
			}
			state.searched = true;
			state.hits.clear();
			state.next = 0;
			auto index = input.GetValue(0, state.row);
			auto query = input.GetValue(1, state.row);
			auto options = input.ColumnCount() > 2 ? input.GetValue(2, state.row) : Value();
			if (!index.IsNull() && !query.IsNull()) { // NULL finds nothing
				auto split = ParseIndex(index);
				auto text = query.ToString();
				auto json = options.IsNull() ? string() : options.ToString();
				char *err = nullptr;
				if (pgvfs_index_search(bind.conn, split.volume.c_str(), split.path.c_str(), text.c_str(),
				                       json.c_str(), CollectHit, &state, &err) != 0) {
					Fail("search", index.ToString(), err);
				}
			}
		}
		if (state.next < state.hits.size()) {
			auto scores = FlatVector::GetData<double>(output.data[0]);
			auto docs = FlatVector::GetData<string_t>(output.data[1]);
			idx_t n = 0;
			for (; n < STANDARD_VECTOR_SIZE && state.next < state.hits.size(); n++, state.next++) {
				scores[n] = state.hits[state.next].first;
				docs[n] = StringVector::AddString(output.data[1], state.hits[state.next].second);
			}
			output.SetCardinality(n);
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		state.searched = false;
		state.row++;
	}
}

// tantivy_drop(index): remove a split; whether there was one. A scalar, so
// SQL can drop many: SELECT tantivy_drop(path) FROM ... The writer only.
struct ConnData : public FunctionData {
	explicit ConnData(PgvfsConn *conn) : conn(conn) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ConnData>(conn);
	}
	bool Equals(const FunctionData &other) const override {
		return conn == other.Cast<ConnData>().conn;
	}
	PgvfsConn *conn;
};

unique_ptr<FunctionData> DropBind(ClientContext &context, ScalarFunction &function, vector<unique_ptr<Expression>> &) {
	return make_uniq<ConnData>(function.function_info->Cast<FsInfo<ScalarFunctionInfo>>().fs.Connect(context));
}

void DropFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto *conn = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<ConnData>().conn;
	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(), [&](string_t url) {
		auto split = ParseIndex(Value(url.GetString()));
		char *err = nullptr;
		auto rc = pgvfs_index_drop(conn, split.volume.c_str(), split.path.c_str(), &err);
		if (rc < 0) {
			Fail("drop", url.GetString(), err);
		}
		return rc == 0;
	});
}

// DuckDB fts-style use, composed in SQL: index columns of a table under a
// key, then score rows by the key. Copy and adapt them for other schemas.
const DefaultTableMacro CREATE_INDEX_MACRO = {
    DEFAULT_SCHEMA,
    "tantivy_create_index",
    {"index", "input_table", "input_id", "input_values", nullptr},
    {{"stemmer", "'porter'"},
     {"stopwords", "'english'"},
     {"strip_accents", "true"},
     {"lower", "true"},
     {nullptr, nullptr}},
    R"(
SELECT tantivy_index(
    index,
    to_json(list_prepend(
        json_object('name', '_key', 'type', 'text', 'options', json_object('stored', true, 'coerce', true)),
        list_transform(input_values, lambda f: json_object('name', f, 'type', 'text', 'options', json_object(
            'coerce', true,
            'indexing', json_object('record', 'position', 'fieldnorms', true, 'tokenizer', 'fts'))))))::VARCHAR,
    to_json(t)::VARCHAR,
    json_object('tokenizers', json_object('fts', json_object('tokenizer', 'simple', 'filters', to_json(list_filter([
        json_object('remove_long', 40),
        CASE WHEN lower THEN to_json('lowercase') END,
        CASE WHEN strip_accents THEN to_json('ascii_folding') END,
        CASE WHEN stopwords <> 'none' THEN json_object('stop_words', stopwords) END,
        CASE WHEN stemmer <> 'none' THEN json_object('stemmer', stemmer) END
    ], lambda x: x IS NOT NULL)))))::VARCHAR
) AS docs
FROM (
    SELECT COLUMNS(lambda c: c = input_id) AS _key, COLUMNS(lambda c: list_contains(input_values, c))
    FROM query_table(input_table)
) t
)"};

const DefaultMacro MATCH_BM25_MACRO = {
    DEFAULT_SCHEMA,
    "tantivy_match_bm25",
    {"index", "input_id", "query_string", nullptr},
    {{"fields", "NULL"}, {"conjunctive", "false"}, {nullptr, nullptr}},
    R"((
SELECT max(__tantivy_score)
FROM (
    SELECT score AS __tantivy_score, doc->>'_key' AS __tantivy_key
    FROM tantivy_search(index, query_string, json_object(
        'fields', regexp_split_to_array(trim(fields), '\s*,\s*'),
        'conjunctive', conjunctive::BOOLEAN))
)
WHERE __tantivy_key = input_id::VARCHAR
))"};

void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("pgvfs_secret",
	                          "Name of the postgres secret pgvfs:// connects with. Unset: $PGVFS_URL, else the "
	                          "unnamed default postgres secret (the one DuckLake's catalog uses)",
	                          LogicalType::VARCHAR);
	// Cache Parquet footers across queries (off by default in DuckDB). Safe:
	// the cache is keyed by path and last-modified time, and a pgvfs path's
	// bytes change only with a new file_id and created_at. With small row
	// groups the footer is large; re-parsing it was half of a small query.
	if (config.HasExtensionOption("parquet_metadata_cache")) {
		config.SetOption("parquet_metadata_cache", Value::BOOLEAN(true));
	}
	auto fs = make_uniq<PgvfsFileSystem>();
	auto &pgvfs = *fs;
	db.GetFileSystem().RegisterSubSystem(std::move(fs));

	AggregateFunctionSet index("tantivy_index");
	AggregateFunction index_fn({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT,
	                           AggregateFunction::StateSize<IndexState>, IndexInitialize, IndexUpdate, IndexCombine,
	                           IndexFinalize, FunctionNullHandling::SPECIAL_HANDLING, IndexSimpleUpdate, IndexBind);
	index_fn.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	index_fn.function_info = make_shared_ptr<FsInfo<AggregateFunctionInfo>>(pgvfs);
	index_fn.SetVolatile();
	index.AddFunction(index_fn);
	index_fn.arguments.push_back(LogicalType::VARCHAR);
	index.AddFunction(index_fn);
	loader.RegisterFunction(std::move(index));

	TableFunctionSet search("tantivy_search");
	TableFunction search_fn({LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr, SearchBind, nullptr, SearchInit);
	search_fn.in_out_function = SearchInOut;
	search_fn.function_info = make_shared_ptr<FsInfo<TableFunctionInfo>>(pgvfs);
	search.AddFunction(search_fn);
	search_fn.arguments.push_back(LogicalType::VARCHAR);
	search.AddFunction(search_fn);
	loader.RegisterFunction(std::move(search));

	ScalarFunction drop("tantivy_drop", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, DropFunction, DropBind);
	drop.function_info = make_shared_ptr<FsInfo<ScalarFunctionInfo>>(pgvfs);
	drop.SetVolatile();
	loader.RegisterFunction(std::move(drop));

	loader.RegisterFunction(*DefaultTableFunctionGenerator::CreateTableMacroInfo(CREATE_INDEX_MACRO));
	loader.RegisterFunction(*DefaultFunctionGenerator::CreateInternalMacroInfo(MATCH_BM25_MACRO));

	ScalarFunction stats("pgvfs_stats", vector<LogicalType> {}, LogicalType::VARCHAR, StatsFunction);
	stats.SetVolatile();
	CreateScalarFunctionInfo stats_info(stats);
	FunctionDescription stats_doc;
	stats_doc.description = "This process's pgvfs counters as JSON: file opens and open-cache hits, reads, bytes, "
	                        "time spent in pgvfs and range queries sent to PostgreSQL. Cumulative; diff two samples.";
	stats_doc.examples = {"pgvfs_stats()::JSON"};
	stats_info.descriptions.push_back(std::move(stats_doc));
	loader.RegisterFunction(std::move(stats_info));
}

} // namespace

void PgvfsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string PgvfsExtension::Name() {
	return "pgvfs";
}

std::string PgvfsExtension::Version() const {
#ifdef EXT_VERSION_PGVFS
	return EXT_VERSION_PGVFS; // git describe, set by DuckDB's extension build
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(pgvfs, loader) {
	duckdb::LoadInternal(loader);
}
}
