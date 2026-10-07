// Tantivy full-text search splits (tantivy/) as SQL. Storage is DuckDB's
// filesystem, so a split lives wherever DuckDB reads and writes: pgvfs://,
// local files, object stores. A split is one immutable file; which splits
// make up an index is for SQL to decide. Thin functions pass tantivy's own
// JSON through (schemas, documents, queries or OpenSearch query DSL,
// aggregations), and two macros compose them like DuckDB's fts.
#include "pgvfs_extension.hpp"
#include "tantivy.h"

#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
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

void CheckText(const string &text) {
	if (text.find('\0') != string::npos) {
		throw InvalidInputException("tantivy text arguments cannot contain NUL");
	}
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
		CheckText(path);
		if (!DBConfig::GetConfig(context).CanAccessFile(path, FileType::FILE_TYPE_REGULAR) ||
		    FileSystem::GetFileSystem(context).IsDisabledForPath(path)) {
			throw PermissionException("Cannot access tantivy split %s - file system operations are disabled", path);
		}
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

	void ForgetPrefix(const string &prefix) {
		std::lock_guard<std::mutex> guard(lock);
		for (auto it = open.begin(); it != open.end();) {
			if (it->first.compare(0, prefix.size(), prefix) == 0) {
				it = open.erase(it);
			} else {
				++it;
			}
		}
	}

	std::mutex lock;
	unordered_map<string, shared_ptr<OpenSplit>> open;
};

// Splits opened for one call: a path, or a list of them.
struct Opened {
	Opened(ClientContext &context, const Value &index) {
		auto splits = Splits::Of(context);
		auto add = [&](const Value &path) {
			if (path.IsNull()) {
				throw InvalidInputException("a tantivy split path cannot be NULL");
			}
			paths.push_back(path.ToString());
			open.push_back(splits->Get(context, paths.back()));
			handles.push_back(open.back()->split);
		};
		if (index.type().id() == LogicalTypeId::LIST) {
			for (auto &path : ListValue::GetChildren(index)) {
				add(path);
			}
		} else {
			add(index);
		}
	}
	string Name() const {
		return paths.size() == 1 ? paths[0] : std::to_string(paths.size()) + " splits";
	}
	vector<string> paths;
	vector<shared_ptr<OpenSplit>> open;
	vector<const TantivySplit *> handles;
};

// Documents to leave out, by a fast field's value (options.exclude_field): a
// serialized roaring bitmap (BLOB), or ids (BIGINT[]).
struct Exclude {
	explicit Exclude(const Value &v) {
		if (v.IsNull()) {
			return;
		}
		if (v.type().id() == LogicalTypeId::BLOB) {
			kind = 1;
			bytes = StringValue::Get(v);
		} else {
			kind = 2;
			for (auto &id : ListValue::GetChildren(v)) {
				if (!id.IsNull()) {
					ids.push_back(id.GetValue<int64_t>());
				}
			}
		}
	}
	const void *Data() const {
		return kind == 1 ? static_cast<const void *>(bytes.data()) : static_cast<const void *>(ids.data());
	}
	size_t Len() const {
		return kind == 1 ? bytes.size() : ids.size();
	}
	int kind = 0;
	string bytes;
	vector<int64_t> ids;
};

string Text(const Value &v) {
	auto text = v.IsNull() ? string() : v.ToString();
	CheckText(text);
	return text;
}

int WriteSplit(void *ctx, const uint8_t *buf, uint64_t len, char *msg, size_t cap) {
	try {
		static_cast<FileHandle *>(ctx)->Write(const_cast<uint8_t *>(buf), len);
		return 0;
	} catch (std::exception &e) {
		SetMessage(msg, cap, e.what());
		return -1;
	}
}

// Write a new split at `url`, which must not exist (splits are immutable):
// `write(handle, err)` streams it and returns its documents, or -1. Nothing
// is left behind if anything fails.
template <class WRITE>
int64_t WriteNewSplit(ClientContext &context, const string &url, const char *what, WRITE &&write) {
	CheckText(url);
	auto &fs = FileSystem::GetFileSystem(context);
	if (fs.FileExists(url)) {
		throw IOException("%s: %s already exists (splits are immutable: drop it or use another path)", what, url);
	}
	auto handle = fs.OpenFile(url, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	try {
		char *err = nullptr;
		auto docs = write(*handle, &err);
		if (docs < 0) {
			Fail(what, url, err);
		}
		handle->Close();
		Splits::Of(context)->Forget(url);
		return docs;
	} catch (...) {
		handle.reset();
		fs.TryRemoveFile(url);
		throw;
	}
}

// tantivy_index(index, schema, doc [, options]): an aggregate that builds a
// split at `index` from the documents (JSON objects, or rows) of its rows,
// written as the aggregate finishes. Arguments are per row, so GROUP BY builds
// one split per group.
struct Build {
	~Build() {
		if (build) {
			tantivy_build_abort(build);
		}
	}
	TantivyBuild *build = nullptr;
	string url;
};

// One query's builds, by path.
struct IndexJob {
	explicit IndexJob(ClientContext &context) : context(context) {
	}
	shared_ptr<Build> Get(const string &url, const string &schema, const string &options) {
		std::lock_guard<std::mutex> guard(lock);
		auto build = builds[url].lock();
		if (!build || !build->build) {
			build = make_shared_ptr<Build>();
			build->url = url;
			char *err = nullptr;
			build->build = tantivy_build_open(schema.c_str(), options.empty() ? nullptr : options.c_str(), &err);
			if (!build->build) {
				Fail("index", url, err);
			}
			builds[url] = build;
		}
		return build;
	}

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
		bool finished = false;
		try {
			return WriteNewSplit(context, b.url, "tantivy_index", [&](FileHandle &handle, char **err) {
				finished = true; // tantivy_build_finish frees the build, whatever happens
				return tantivy_build_finish(build, WriteSplit, &handle, err);
			});
		} catch (...) {
			if (!finished) {
				tantivy_build_abort(build);
			}
			throw;
		}
	}

	ClientContext &context;
	std::mutex lock;
	unordered_map<string, weak_ptr<Build>> builds;
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
	// States own builds; bind data only finds them. Destruction can be deferred
	// until after the plan's bind data is gone.
	shared_ptr<Build> *build;
};

#ifdef PGVFS_DUCKDB_V2
unique_ptr<FunctionData> IndexBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
#else
unique_ptr<FunctionData> IndexBind(ClientContext &context, AggregateFunction &,
                                   vector<unique_ptr<Expression>> &arguments) {
#endif
	// A row (any value but text) is indexed as its JSON.
#ifdef PGVFS_DUCKDB_V2
	auto doc_type = arguments[2]->GetReturnType().id();
#else
	auto doc_type = arguments[2]->return_type.id();
#endif
	if (doc_type != LogicalTypeId::VARCHAR) {
		if (!ExtensionHelper::TryAutoLoadExtension(context, "json")) {
			throw InvalidInputException(
			    "tantivy_index: rows as documents need the json extension (LOAD json), or pass JSON text");
		}
		arguments[2] = BoundCastExpression::AddCastToType(context, std::move(arguments[2]), LogicalType::JSON());
	}
	return make_uniq<IndexBindData>(make_shared_ptr<IndexJob>(context));
}

// For DuckDB's aggregate templates.
struct IndexOp {
	template <class STATE>
	static void Initialize(STATE &state) {
		state = {nullptr};
	}
	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!target.build) {
			if (source.build) {
				target.build = make_uniq<shared_ptr<Build>>(*source.build).release();
			}
		} else if (source.build && source.build->get() != target.build->get()) {
			throw InvalidInputException("tantivy_index builds one split per group: %s and %s", (*target.build)->url,
			                            (*source.build)->url);
		}
	}
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		// No documents, no split.
		auto &job = *finalize_data.input.bind_data->template Cast<IndexBindData>().job;
		target = state.build ? job.Commit(**state.build) : 0;
	}
	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		delete state.build;
		state.build = nullptr;
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
		auto value = UnifiedVectorFormat::GetData<string_t>(args[a])[row].GetString();
		CheckText(value);
		return value;
	};
	for (idx_t i = 0; i < count; i++) {
		auto doc_row = args[2].sel->get_index(i);
		if (!args[2].validity.RowIsValid(doc_row)) {
			continue;
		}
		IndexState &state = state_of(i);
		auto url = text(0, i, "index");
		if (!state.build) {
			state.build = make_uniq<shared_ptr<Build>>(
			                  job.Get(url, text(1, i, "schema"), input_count > 3 ? text(3, i, "options") : ""))
			                  .release();
		} else if ((*state.build)->url != url) {
			throw InvalidInputException("tantivy_index builds one split per group: %s and %s", (*state.build)->url,
			                            url);
		}
		auto doc = UnifiedVectorFormat::GetData<string_t>(args[2])[doc_row];
		char *err = nullptr;
		if (tantivy_build_add((*state.build)->build, doc.GetData(), doc.GetSize(), &err) != 0) {
			Fail("index", url, err);
		}
	}
}

void IndexUpdate(Vector inputs[], AggregateInputData &input, idx_t input_count, Vector &states, idx_t count) {
	UnifiedVectorFormat format;
	Unified(states, count, format);
	auto ptrs = UnifiedVectorFormat::GetData<IndexState *>(format);
	AddRows(inputs, input, input_count, count,
	        [&](idx_t i) -> IndexState & { return *ptrs[format.sel->get_index(i)]; });
}

#ifndef PGVFS_DUCKDB_V2
// DuckDB 1.x updates ungrouped aggregates through this.
void IndexSimpleUpdate(Vector inputs[], AggregateInputData &input, idx_t input_count, data_ptr_t state, idx_t count) {
	AddRows(inputs, input, input_count, count,
	        [&](idx_t) -> IndexState & { return *reinterpret_cast<IndexState *>(state); });
}
#endif

// tantivy_search(index, query [, options [, exclude]]): a query in tantivy's
// syntax, or OpenSearch query DSL (JSON), over a split or a list of them;
// per hit, best first across them, (score, doc, path). An in-out function,
// so arguments may be columns: FROM splits s CROSS JOIN tantivy_search(s.path,
// 'cats') searches each split listed. (In-out functions take no named
// parameters, hence options as JSON.)
struct SearchBindData : public TableFunctionData {};

struct SearchState : public LocalTableFunctionState {
	struct Hit {
		idx_t split;
		double score;
		string doc;
	};
	idx_t row = 0;
	bool searched = false;
	vector<string> paths;
	vector<Hit> hits;
	idx_t next = 0;
};

void CollectHit(void *ctx, size_t split, double score, const char *doc, size_t len) {
	static_cast<SearchState *>(ctx)->hits.push_back({split, score, string(doc, len)});
}

unique_ptr<FunctionData> SearchBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
                                    ColumnNames &names) {
	types = {LogicalType::DOUBLE, LogicalType::JSON(), LogicalType::VARCHAR};
	names = {"score", "doc", "path"};
	return make_uniq<SearchBindData>();
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
			if (!index.IsNull() && !query.IsNull()) { // NULL finds nothing
				Opened splits(context.client, index);
				state.paths = splits.paths;
				auto text = Text(query);
				auto options = input.ColumnCount() > 2 ? Text(input.GetValue(2, state.row)) : string();
				Exclude exclude(input.ColumnCount() > 3 ? input.GetValue(3, state.row) : Value());
				char *err = nullptr;
				if (tantivy_search(splits.handles.data(), splits.handles.size(), text.c_str(), options.c_str(),
				                   exclude.kind, exclude.Data(), exclude.Len(), CollectHit, &state, &err) != 0) {
					Fail("search", splits.Name(), err);
				}
			}
		}
		if (state.next < state.hits.size()) {
			auto scores = MutableData<double>(output.data[0]);
			auto docs = MutableData<string_t>(output.data[1]);
			auto paths = MutableData<string_t>(output.data[2]);
			idx_t n = 0;
			for (; n < STANDARD_VECTOR_SIZE && state.next < state.hits.size(); n++, state.next++) {
				auto &hit = state.hits[state.next];
				scores[n] = hit.score;
				docs[n] = StringVector::AddString(output.data[1], hit.doc);
				paths[n] = StringVector::AddString(output.data[2], state.paths[hit.split]);
			}
			SetRows(output, n);
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		state.searched = false;
		state.row++;
	}
}

// tantivy_count(index, query [, options [, exclude]]): the number of matches
// over a split or a list of them.
void CountFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	for (idx_t i = 0; i < args.size(); i++) {
		auto index = args.data[0].GetValue(i);
		auto query = args.data[1].GetValue(i);
		if (index.IsNull() || query.IsNull()) {
			result.SetValue(i, Value());
			continue;
		}
		Opened splits(context, index);
		auto text = Text(query);
		auto options = args.ColumnCount() > 2 ? Text(args.data[2].GetValue(i)) : string();
		Exclude exclude(args.ColumnCount() > 3 ? args.data[3].GetValue(i) : Value());
		char *err = nullptr;
		auto n = tantivy_count(splits.handles.data(), splits.handles.size(), text.c_str(), options.c_str(),
		                       exclude.kind, exclude.Data(), exclude.Len(), &err);
		if (n < 0) {
			Fail("count", splits.Name(), err);
		}
		result.SetValue(i, Value::BIGINT(n));
	}
}

// tantivy_aggregate(index, query, aggs [, options [, exclude]]): tantivy's
// aggregations (Elasticsearch's JSON: terms, cardinality, stats, histogram,
// ...) over the matches, merged across splits.
void AggregateJsonFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = MutableData<string_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		auto index = args.data[0].GetValue(i);
		auto query = args.data[1].GetValue(i);
		auto aggs = args.data[2].GetValue(i);
		if (index.IsNull() || query.IsNull() || aggs.IsNull()) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		Opened splits(context, index);
		auto text = Text(query);
		auto aggs_json = Text(aggs);
		auto options = args.ColumnCount() > 3 ? Text(args.data[3].GetValue(i)) : string();
		Exclude exclude(args.ColumnCount() > 4 ? args.data[4].GetValue(i) : Value());
		char *json = nullptr;
		char *err = nullptr;
		if (tantivy_aggregate(splits.handles.data(), splits.handles.size(), text.c_str(), aggs_json.c_str(),
		                      options.c_str(), exclude.kind, exclude.Data(), exclude.Len(), &json, &err) != 0) {
			Fail("aggregate", splits.Name(), err);
		}
		out[i] = StringVector::AddString(result, json);
		tantivy_free_str(json);
	}
	if (args.size() == 1 && args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

// tantivy_merge(splits, target [, options [, exclude]]): merge splits of one
// schema into a new split at `target`, leaving out excluded documents; the
// documents kept. Their segments are copied, not re-indexed.
void MergeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	for (idx_t i = 0; i < args.size(); i++) {
		auto sources = args.data[0].GetValue(i);
		auto target = args.data[1].GetValue(i);
		if (sources.IsNull() || target.IsNull()) {
			result.SetValue(i, Value());
			continue;
		}
		Opened splits(context, sources);
		auto options = args.ColumnCount() > 2 ? Text(args.data[2].GetValue(i)) : string();
		Exclude exclude(args.ColumnCount() > 3 ? args.data[3].GetValue(i) : Value());
		auto docs = WriteNewSplit(context, target.ToString(), "tantivy_merge", [&](FileHandle &handle, char **err) {
			return tantivy_merge(splits.handles.data(), splits.handles.size(), options.c_str(), exclude.kind,
			                     exclude.Data(), exclude.Len(), WriteSplit, &handle, err);
		});
		result.SetValue(i, Value::BIGINT(docs));
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
		CheckText(path);
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

void ForgetTantivyPrefix(ClientContext &context, const string &prefix) {
	auto splits = ObjectCache::GetObjectCache(context).Get<Splits>(Splits::ObjectType());
	if (splits) {
		splits->ForgetPrefix(prefix);
	}
}

void RegisterTantivy(ExtensionLoader &loader) {
	AggregateFunctionSet index("tantivy_index");
	for (idx_t args : {3, 4}) {
		vector<LogicalType> types(args, LogicalType::VARCHAR);
		types[2] = LogicalType::ANY; // the document: JSON text, or a row
		AggregateFunction fn(types, LogicalType::BIGINT, AggregateFunction::StateSize<IndexState>,
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
		fn.SetStateDestructorCallback(AggregateFunction::StateDestroy<IndexState, IndexOp>);
		fn.SetVolatile();
#ifdef PGVFS_DUCKDB_V2
		fn.SetFallible();
#endif
		index.AddFunction(fn);
	}
	loader.RegisterFunction(std::move(index));

	// Every search-like function: (index, query, [options, [exclude]]), the
	// index a path or a list of them, the exclude set a roaring BLOB or ids.
	auto signatures = [](vector<LogicalType> after_query) {
		vector<vector<LogicalType>> out;
		for (auto &index : vector<LogicalType> {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)}) {
			vector<LogicalType> base {index, LogicalType::VARCHAR};
			base.insert(base.end(), after_query.begin(), after_query.end());
			out.push_back(base);
			base.push_back(LogicalType::VARCHAR);
			out.push_back(base);
			for (auto &exclude : vector<LogicalType> {LogicalType::BLOB, LogicalType::LIST(LogicalType::BIGINT)}) {
				auto with_exclude = base;
				with_exclude.push_back(exclude);
				out.push_back(with_exclude);
			}
		}
		return out;
	};

	TableFunctionSet search("tantivy_search");
	for (auto &types : signatures({})) {
		TableFunction fn(types, nullptr, SearchBind, nullptr, SearchInit);
		fn.in_out_function = SearchInOut;
		search.AddFunction(fn);
	}
	loader.RegisterFunction(std::move(search));

	ScalarFunctionSet count("tantivy_count");
	for (auto &types : signatures({})) {
		ScalarFunction fn(types, LogicalType::BIGINT, CountFunction);
		fn.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		fn.SetVolatile(); // filesystem contents can change between executions
#ifdef PGVFS_DUCKDB_V2
		fn.SetFallible();
#endif
		count.AddFunction(fn);
	}
	loader.RegisterFunction(std::move(count));

	ScalarFunctionSet aggregate("tantivy_aggregate");
	for (auto &types : signatures({LogicalType::VARCHAR})) {
		ScalarFunction fn(types, LogicalType::JSON(), AggregateJsonFunction);
		fn.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
		fn.SetVolatile();
#ifdef PGVFS_DUCKDB_V2
		fn.SetFallible();
#endif
		aggregate.AddFunction(fn);
	}
	loader.RegisterFunction(std::move(aggregate));

	ScalarFunctionSet merge("tantivy_merge");
	for (auto &types : signatures({})) {
		if (types[0].id() == LogicalTypeId::LIST) { // (splits, target, ...)
			ScalarFunction fn(types, LogicalType::BIGINT, MergeFunction);
			fn.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
			fn.SetVolatile();
#ifdef PGVFS_DUCKDB_V2
			fn.SetFallible();
#endif
			merge.AddFunction(fn);
		}
	}
	loader.RegisterFunction(std::move(merge));

	ScalarFunction drop("tantivy_drop", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, DropFunction);
	drop.SetVolatile();
#ifdef PGVFS_DUCKDB_V2
	drop.SetFallible();
#endif
	loader.RegisterFunction(std::move(drop));

	loader.RegisterFunction(*DefaultTableFunctionGenerator::CreateTableMacroInfo(CREATE_INDEX_MACRO));
	loader.RegisterFunction(*DefaultFunctionGenerator::CreateInternalMacroInfo(MATCH_BM25_MACRO));
}

} // namespace duckdb
