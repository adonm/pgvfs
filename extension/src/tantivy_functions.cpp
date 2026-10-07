// Tantivy full-text search splits (tantivy/) as SQL. Storage is DuckDB's
// filesystem, so a split lives wherever DuckDB reads and writes: pgvfs://,
// local files, object stores. A split is one immutable file; which splits
// make up an index is for SQL to decide. Three thin functions pass tantivy's
// own JSON through, and two macros compose them like DuckDB's fts.
#include "pgvfs_extension.hpp"
#include "tantivy.h"

#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <chrono>
#include <cstring>
#include <mutex>

// DuckDB 2.0 reworked its C++ function API; the few differences are marked.
#if __has_include("duckdb/function/aggregate_state_layout.hpp")
#define PGVFS_DUCKDB_V2 1
#endif

namespace duckdb {

namespace {

template <class T>
T *MutableData(Vector &vector) {
#ifdef PGVFS_DUCKDB_V2
	return FlatVector::GetDataMutable<T>(vector);
#else
	return FlatVector::GetData<T>(vector);
#endif
}

void Unified(Vector &vector, idx_t count, UnifiedVectorFormat &format) {
#ifdef PGVFS_DUCKDB_V2
	vector.ToUnifiedFormat(format);
#else
	vector.ToUnifiedFormat(count, format);
#endif
}

#ifdef PGVFS_DUCKDB_V2
using ColumnNames = vector<Identifier>;
#else
using ColumnNames = vector<string>;
#endif

void SetRows(DataChunk &chunk, idx_t count) {
#ifdef PGVFS_DUCKDB_V2
	chunk.SetCardinalityUnsafe(count); // child vectors were written in place
#else
	chunk.SetCardinality(count);
#endif
}

[[noreturn]] void Fail(const string &what, const string &path, char *err) {
	string msg = err ? string(err) : "unknown error";
	tantivy_free_str(err);
	throw IOException("tantivy %s %s: %s", what, path, msg);
}

void SetMessage(char *msg, size_t cap, const char *what) {
	strncpy(msg, what, cap - 1);
	msg[cap - 1] = '\0';
}

// A split open for search. Its file is read through one handle under a lock:
// not every filesystem reads one handle from several threads.
struct OpenSplit {
	~OpenSplit() {
		if (split) {
			tantivy_split_close(split); // before the handle it reads
		}
	}
	unique_ptr<FileHandle> handle;
	std::mutex lock;
	TantivySplit *split = nullptr;
	string version;
	std::chrono::steady_clock::time_point checked;
};

int ReadSplit(void *ctx, uint8_t *buf, uint64_t len, uint64_t offset, char *msg, size_t cap) {
	auto &open = *static_cast<OpenSplit *>(ctx);
	try {
		std::lock_guard<std::mutex> guard(open.lock);
		open.handle->Read(buf, len, offset);
		return 0;
	} catch (std::exception &e) {
		SetMessage(msg, cap, e.what());
		return -1;
	}
}

// This database's splits open for search, by path, kept in its object cache.
// A split is checked for a change (size, modification time, version tag) at
// most every 10 s; this database's own builds and drops forget it at once.
struct Splits : public ObjectCacheEntry {
	static shared_ptr<Splits> Of(ClientContext &context) {
		return ObjectCache::GetObjectCache(context).GetOrCreate<Splits>(ObjectType());
	}
	static string ObjectType() {
		return "tantivy_splits";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx(); // never evicted
	}

	shared_ptr<OpenSplit> Get(ClientContext &context, const string &path) {
		auto now = std::chrono::steady_clock::now();
		{
			std::lock_guard<std::mutex> guard(lock);
			auto it = open.find(path);
			if (it != open.end() && now - it->second->checked < std::chrono::seconds(10)) {
				return it->second;
			}
		}
		// The client's filesystem: it brings the client's credentials.
		auto &fs = FileSystem::GetFileSystem(context);
		auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
		if (!handle) {
			throw IOException("no tantivy split at %s", path);
		}
		auto version = fs.GetVersionTag(*handle) + "/" + std::to_string(handle->GetFileSize()) + "/" +
		               std::to_string(fs.GetLastModifiedTime(*handle).value);
		{
			std::lock_guard<std::mutex> guard(lock);
			auto it = open.find(path);
			if (it != open.end() && it->second->version == version) {
				it->second->checked = now;
				return it->second;
			}
		}
		auto split = make_shared_ptr<OpenSplit>();
		split->handle = std::move(handle);
		split->version = version;
		split->checked = now;
		char *err = nullptr;
		split->split = tantivy_split_open(split->handle->GetFileSize(), ReadSplit, split.get(), &err);
		if (!split->split) {
			Fail("open", path, err);
		}
		std::lock_guard<std::mutex> guard(lock);
		open[path] = split;
		return split;
	}

	void Forget(const string &path) {
		std::lock_guard<std::mutex> guard(lock);
		open.erase(path);
	}

	std::mutex lock;
	unordered_map<string, shared_ptr<OpenSplit>> open;
};

// tantivy_index(index, schema, doc [, options]): an aggregate that builds a
// split at `index` from the documents (JSON objects) of its rows, written as
// the aggregate finishes. Arguments are per row, so GROUP BY builds one split
// per group.
struct Build {
	TantivyBuild *build = nullptr;
	string url;
};

int WriteSplit(void *ctx, const uint8_t *buf, uint64_t len, char *msg, size_t cap) {
	try {
		static_cast<FileHandle *>(ctx)->Write(const_cast<uint8_t *>(buf), len);
		return 0;
	} catch (std::exception &e) {
		SetMessage(msg, cap, e.what());
		return -1;
	}
}

// One query's builds, by path.
struct IndexJob {
	IndexJob(ClientContext &context, shared_ptr<Splits> splits) : context(context), splits(std::move(splits)) {
	}
	~IndexJob() {
		for (auto &build : builds) {
			if (build.second->build) {
				tantivy_build_abort(build.second->build);
			}
		}
	}

	Build &Get(const string &url, const string &schema, const string &options) {
		std::lock_guard<std::mutex> guard(lock);
		auto &build = builds[url];
		if (!build) {
			build = make_uniq<Build>();
			build->url = url;
			char *err = nullptr;
			build->build =
			    tantivy_build_open(schema.c_str(), options.empty() ? nullptr : options.c_str(), &err);
			if (!build->build) {
				Fail("index", url, err);
			}
		}
		return *build;
	}

	// Write the split to its path, which must be free: splits are immutable.
	int64_t Commit(Build &b) {
		TantivyBuild *build;
		{
			std::lock_guard<std::mutex> guard(lock);
			build = b.build;
			b.build = nullptr;
		}
		if (!build) {
			throw InvalidInputException("tantivy_index: %s is written twice", b.url);
		}
		auto &fs = FileSystem::GetFileSystem(context);
		unique_ptr<FileHandle> handle;
		try {
			if (fs.FileExists(b.url)) {
				throw IOException("tantivy_index: %s already exists (splits are immutable: drop it or use another "
				                  "path)",
				                  b.url);
			}
			handle = fs.OpenFile(b.url, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		} catch (...) {
			tantivy_build_abort(build);
			throw;
		}
		try {
			char *err = nullptr;
			auto docs = tantivy_build_finish(build, WriteSplit, handle.get(), &err);
			if (docs < 0) {
				Fail("index", b.url, err);
			}
			handle->Close();
			splits->Forget(b.url);
			return docs;
		} catch (...) {
			handle.reset();
			fs.TryRemoveFile(b.url);
			throw;
		}
	}

	ClientContext &context;
	shared_ptr<Splits> splits;
	std::mutex lock;
	unordered_map<string, unique_ptr<Build>> builds;
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
	Build *build;
	int64_t docs;
};

#ifdef PGVFS_DUCKDB_V2
unique_ptr<FunctionData> IndexBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
#else
unique_ptr<FunctionData> IndexBind(ClientContext &context, AggregateFunction &, vector<unique_ptr<Expression>> &) {
#endif
	return make_uniq<IndexBindData>(make_shared_ptr<IndexJob>(context, Splits::Of(context)));
}

// For DuckDB's aggregate templates.
struct IndexOp {
	template <class STATE>
	static void Initialize(STATE &state) {
		state = {nullptr, 0};
	}
	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!target.build) {
			target.build = source.build;
		} else if (source.build && source.build != target.build) {
			throw InvalidInputException("tantivy_index builds one split per group: %s and %s", target.build->url,
			                            source.build->url);
		}
		target.docs += source.docs;
	}
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		// No documents, no split.
		auto &job = *finalize_data.input.bind_data->template Cast<IndexBindData>().job;
		target = state.build ? job.Commit(*state.build) : 0;
	}
	static bool IgnoreNull() {
		return false;
	}
};

// Add each row's document to its state's build; `state_of(i)` is row i's state.
template <class STATE_OF>
void AddRows(Vector inputs[], AggregateInputData &input, idx_t input_count, idx_t count, STATE_OF &&state_of) {
	auto &job = *input.bind_data->Cast<IndexBindData>().job;
	UnifiedVectorFormat args[4];
	for (idx_t a = 0; a < input_count; a++) {
		Unified(inputs[a], count, args[a]);
	}
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
		IndexState &state = state_of(i);
		auto url = text(0, i, "index");
		if (!state.build) {
			state.build = &job.Get(url, text(1, i, "schema"), input_count > 3 ? text(3, i, "options") : "");
		} else if (state.build->url != url) {
			throw InvalidInputException("tantivy_index builds one split per group: %s and %s", state.build->url,
			                            url);
		}
		auto doc = UnifiedVectorFormat::GetData<string_t>(args[2])[doc_row];
		char *err = nullptr;
		if (tantivy_build_add(state.build->build, doc.GetData(), doc.GetSize(), &err) != 0) {
			Fail("index", url, err);
		}
		state.docs++;
	}
}

void IndexUpdate(Vector inputs[], AggregateInputData &input, idx_t input_count, Vector &states, idx_t count) {
	UnifiedVectorFormat format;
	Unified(states, count, format);
	auto ptrs = UnifiedVectorFormat::GetData<IndexState *>(format);
	AddRows(inputs, input, input_count, count, [&](idx_t i) -> IndexState & { return *ptrs[format.sel->get_index(i)]; });
}

#ifndef PGVFS_DUCKDB_V2
// DuckDB 1.x updates ungrouped aggregates through this.
void IndexSimpleUpdate(Vector inputs[], AggregateInputData &input, idx_t input_count, data_ptr_t state, idx_t count) {
	AddRows(inputs, input, input_count, count,
	        [&](idx_t) -> IndexState & { return *reinterpret_cast<IndexState *>(state); });
}
#endif

// tantivy_search(index, query [, options]): tantivy's query language over a
// split; (score, doc) per hit, best first, doc holding the stored fields. An
// in-out function, so arguments may be columns: FROM splits s CROSS JOIN
// tantivy_search(s.path, 'cats') searches every split listed. (In-out
// functions take no named parameters, hence options as JSON.)
struct SearchBindData : public TableFunctionData {};

struct SearchState : public LocalTableFunctionState {
	idx_t row = 0;
	bool searched = false;
	vector<std::pair<double, string>> hits;
	idx_t next = 0;
};

void CollectHit(void *ctx, double score, const char *doc, size_t len) {
	static_cast<SearchState *>(ctx)->hits.emplace_back(score, string(doc, len));
}

unique_ptr<FunctionData> SearchBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
                                    ColumnNames &names) {
	auto data = make_uniq<SearchBindData>();
	types = {LogicalType::DOUBLE, LogicalType::JSON()};
	names = {"score", "doc"};
	return std::move(data);
}

unique_ptr<LocalTableFunctionState> SearchInit(ExecutionContext &, TableFunctionInitInput &,
                                               GlobalTableFunctionState *) {
	return make_uniq<SearchState>();
}

OperatorResultType SearchInOut(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
                               DataChunk &output) {
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
				auto path = index.ToString();
				auto split = Splits::Of(context.client)->Get(context.client, path);
				auto text = query.ToString();
				auto json = options.IsNull() ? string() : options.ToString();
				char *err = nullptr;
				if (tantivy_split_search(split->split, text.c_str(), json.c_str(), CollectHit, &state, &err) != 0) {
					Fail("search", path, err);
				}
			}
		}
		if (state.next < state.hits.size()) {
			auto scores = MutableData<double>(output.data[0]);
			auto docs = MutableData<string_t>(output.data[1]);
			idx_t n = 0;
			for (; n < STANDARD_VECTOR_SIZE && state.next < state.hits.size(); n++, state.next++) {
				scores[n] = state.hits[state.next].first;
				docs[n] = StringVector::AddString(output.data[1], state.hits[state.next].second);
			}
			SetRows(output, n);
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		state.searched = false;
		state.row++;
	}
}

// tantivy_drop(index): remove a split; whether there was one. A scalar, so
// SQL can drop many: SELECT tantivy_drop(path) FROM ...
void DropFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &splits = *Splits::Of(context);
	auto &fs = FileSystem::GetFileSystem(context);
	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(), [&](string_t url) {
		auto path = url.GetString();
		splits.Forget(path);
		return fs.TryRemoveFile(path);
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
#ifdef PGVFS_DUCKDB_V2
    "(index, input_id, query_string, fields := NULL, conjunctive := false) AS "
#else
    {"index", "input_id", "query_string", nullptr},
    {{"fields", "NULL"}, {"conjunctive", "false"}, {nullptr, nullptr}},
#endif
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

} // namespace

void RegisterTantivy(ExtensionLoader &loader) {
	AggregateFunctionSet index("tantivy_index");
	for (idx_t args : {3, 4}) {
		AggregateFunction fn(vector<LogicalType>(args, LogicalType::VARCHAR), LogicalType::BIGINT,
		                     AggregateFunction::StateSize<IndexState>,
		                     AggregateFunction::StateInitialize<IndexState, IndexOp>, IndexUpdate,
		                     AggregateFunction::StateCombine<IndexState, IndexOp>,
		                     AggregateFunction::StateFinalize<IndexState, int64_t, IndexOp>,
		                     FunctionNullHandling::SPECIAL_HANDLING,
#ifdef PGVFS_DUCKDB_V2
		                     nullptr,
#else
		                     IndexSimpleUpdate,
#endif
		                     IndexBind);
		fn.SetVolatile();
		index.AddFunction(fn);
	}
	loader.RegisterFunction(std::move(index));

	TableFunctionSet search("tantivy_search");
	for (idx_t args : {2, 3}) {
		TableFunction fn(vector<LogicalType>(args, LogicalType::VARCHAR), nullptr, SearchBind, nullptr, SearchInit);
		fn.in_out_function = SearchInOut;
		search.AddFunction(fn);
	}
	loader.RegisterFunction(std::move(search));

	ScalarFunction drop("tantivy_drop", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, DropFunction);
	drop.SetVolatile();
	loader.RegisterFunction(std::move(drop));

	loader.RegisterFunction(*DefaultTableFunctionGenerator::CreateTableMacroInfo(CREATE_INDEX_MACRO));
	loader.RegisterFunction(*DefaultFunctionGenerator::CreateInternalMacroInfo(MATCH_BM25_MACRO));
}

} // namespace duckdb
