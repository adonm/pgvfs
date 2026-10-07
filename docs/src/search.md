# Full-text search

pgvfs stores [tantivy](https://github.com/quickwit-oss/tantivy) search
indexes in PostgreSQL alongside the lake. The writer builds them from any
query; every reader searches them with BM25 ranking. There is no search
cluster, and indexes share the lake's secret, backup and roles.

As in [Quickwit](https://quickwit.io/docs/overview/concepts/querying), an
index is made of **splits**. A split is an immutable tantivy index under one
`pgvfs://<volume>/<path>`. The extension builds, searches and drops splits.
Everything else is SQL: which splits make up an index, when to add one
(say, after each load), and when to merge them. A DuckLake table makes a good
list of splits, versioned with the data it indexes.

| Function | |
| --- | --- |
| `tantivy_index(index, schema, doc [, options])` | aggregate: builds a split from its rows |
| `tantivy_search(index, query, ...)` | table: searches a split; with a lateral join, many |
| `tantivy_drop(index)` | scalar: removes a split |
| `tantivy_create_index`, `tantivy_match_bm25` | macros that work like DuckDB's [fts](https://duckdb.org/docs/current/core_extensions/full_text_search) |

## Quick start

On the writer, index the `title` and `body` columns of `lake.docs` under its
key `id`:

```sql
FROM tantivy_create_index('pgvfs://lake-fts/docs', 'lake.docs', 'id', ['title', 'body']);
```

On any reader, score rows (`NULL` when they don't match):

```sql
SELECT id, title, score
FROM (
    SELECT *, tantivy_match_bm25('pgvfs://lake-fts/docs', id, 'small cats') AS score
    FROM lake.docs
)
WHERE score IS NOT NULL
ORDER BY score DESC;
```

or fetch only the top hits and join them to the table, which avoids scanning
it:

```sql
SELECT d.*, s.score
FROM tantivy_search('pgvfs://lake-fts/docs', 'small cats', '{"top_k": 10}') s
JOIN lake.docs d ON d.id = (s.doc->>'_key')::INTEGER
ORDER BY s.score DESC;
```

Keep splits outside the lake's `DATA_PATH`, for example in their own volume.
DuckLake's `ducklake_delete_orphaned_files` deletes files under `DATA_PATH`
that its catalog doesn't track.

A split never changes, so to refresh this one, drop it and build it again:
`SELECT tantivy_drop('pgvfs://lake-fts/docs')`. Readers see the new split
within `PGVFS_OPEN_CACHE_S` (10 s). To keep searching without a gap, build
new splits instead, as below.

## Keeping an index current with DuckLake

Index each load's changes as a new split. DuckLake's
[`table_changes`](https://ducklake.select/docs/stable/duckdb/advanced_features/data_change_feed)
gives the rows inserted or updated between two snapshots. Each row's `rowid`
stays the same across updates and compaction, and its `snapshot_id` is the
snapshot that wrote the current version. Together they let a search keep
only hits on current rows.

```sql
-- once: the index's splits, and the first one, of the whole table
CREATE TABLE lake.docs_splits (path VARCHAR, snapshot BIGINT, docs BIGINT);
SET VARIABLE s1 = (SELECT id::BIGINT FROM lake.current_snapshot());
INSERT INTO lake.docs_splits
SELECT path, getvariable('s1'), tantivy_index(path,
    '[{"name": "rowid", "type": "i64", "options": {"stored": true, "indexed": true}},
      {"name": "body", "type": "text", "options": {"indexing": {"record": "position", "tokenizer": "en_stem"}}}]',
    to_json(c))
FROM (SELECT 'pgvfs://lake-fts/docs/' || getvariable('s1') AS path, rowid, * FROM lake.docs) c
GROUP BY path;

-- after each load, on the writer: a split of the rows changed since the last
-- one, at their latest version (rows deleted since drop out at search time)
SET VARIABLE s0 = (SELECT max(snapshot) FROM lake.docs_splits);
SET VARIABLE s1 = (SELECT id::BIGINT FROM lake.current_snapshot());
INSERT INTO lake.docs_splits
SELECT path, getvariable('s1'), tantivy_index(path, '<the same schema>', to_json(c))
FROM (
    SELECT 'pgvfs://lake-fts/docs/' || getvariable('s1') AS path, *
    FROM lake.table_changes('docs', getvariable('s0') + 1, getvariable('s1'))
    WHERE change_type IN ('insert', 'update_postimage')
    QUALIFY row_number() OVER (PARTITION BY rowid ORDER BY snapshot_id DESC) = 1
) c
GROUP BY path;
```

`table_changes` reads only the files that changed, so a load's split costs
in proportion to the load, not the table. A split's row in `docs_splits` is written only once the split is complete.
If the statement fails after that, the split is left unlisted; drop it. A
load with no changes adds no split. To search, search every split and keep
only hits on rows that still exist and whose current version that split
indexed:

```sql
SELECT d.*, h.score
FROM lake.docs_splits s
CROSS JOIN tantivy_search(s.path, 'small cats', '{"top_k": 10}') h
JOIN lake.docs d ON d.rowid = (h.doc->>'rowid')::BIGINT AND d.snapshot_id <= s.snapshot
ORDER BY h.score DESC
LIMIT 10;
```

`top_k` applies per split, so it bounds each split's work while `LIMIT`
picks the overall best. Each split scores with its own term statistics, as
Elasticsearch shards and Quickwit splits do.

**Compaction.** Every split is a search, so merge them as they pile up. The
same statement over a longer range, `table_changes('docs', a + 1, b)`, builds
one split that replaces the splits covering snapshots `a + 1` to `b`. Swap it
in and drop them:

```sql
CREATE TEMP TABLE merged AS
SELECT path, 1234 AS snapshot, tantivy_index(path, '<schema>', to_json(c)) AS docs
FROM (<the query above over table_changes('docs', 1001, 1234), path 'pgvfs://lake-fts/docs/1001-1234'>) c
GROUP BY path;
CREATE TEMP TABLE replaced AS FROM lake.docs_splits WHERE snapshot BETWEEN 1001 AND 1234;
BEGIN;
DELETE FROM lake.docs_splits WHERE snapshot BETWEEN 1001 AND 1234;
INSERT INTO lake.docs_splits FROM merged;
COMMIT;
SELECT tantivy_drop(path) FROM replaced;
```

This needs the change feed for the range, so compact before expiring those
snapshots. After that, `FROM lake.docs WHERE snapshot_id BETWEEN 1001 AND 1234`
selects the same rows by scanning the table.

**Large tables.** A tantivy split holds at most 2³¹ documents, and a smaller
one builds, merges and opens faster. Partition big builds: `GROUP BY` builds
one split per group, so a path such as
`'pgvfs://lake-fts/docs/' || s1 || '/' || rowid % 16` gives 16 splits per
build. Merge the per-load splits into larger ranges over time, keeping a few
dozen in all, since a query searches each one.

## The functions

### `tantivy_index(index, schema, doc [, options])`

An aggregate that builds a split at `index` from one JSON document per row
and returns the number of documents. It commits when the aggregate finishes.
The arguments are per row: with `GROUP BY`, each group builds its own split,
and every row of a group must name the same `index`. No rows, no split. Only
the writer can build, and `index` must not hold a split already. A failed or
cancelled build leaves nothing behind.

- `schema` is a tantivy schema as JSON: an array of
  [field entries](https://docs.rs/tantivy/latest/tantivy/schema/index.html).
  Each has a `name`, a `type` (`text`, `i64`, `u64`, `f64`, `bool`, `date`,
  `facet`, `bytes`, `json_object` or `ip_addr`) and `options` (indexing,
  tokenizer, `stored`, `fast`, `coerce` and so on).
- `doc` is a JSON object, usually `to_json(t)` for row `t`. Fields not in the
  schema and `null`s are skipped; an array gives a field several values.
  `coerce` on a text field indexes numbers as text.
- `options`, as JSON:
  - `tokenizers`: named analyzers to use in the schema, besides tantivy's
    built-in `default`, `raw`, `en_stem` and `whitespace`. Each is a
    `tokenizer` (`"simple"`, `"whitespace"`, `"raw"`,
    `{"ngram": {"min": 2, "max": 3, "prefix_only": false}}`, or
    `{"regex": "..."}`) and a list of `filters`. A filter is `"lowercase"`,
    `"ascii_folding"`, `"alpha_num_only"`, `{"remove_long": 40}`,
    `{"stop_words": "english"}` (or a list of words), or
    `{"stemmer": "english"}`. They are saved with the split, so readers use
    the same ones.
  - `settings`: tantivy's index settings: `docstore_compression` (`"lz4"` or
    `"none"`) and `docstore_blocksize`.
  - `memory_budget`: the indexing memory in bytes (default 256 MB).
  - `merge`: merge into one segment (default `true`), so a search reads each
    term once.

### `tantivy_search(index, query [, options])`

Searches the split at `index`. Returns `score DOUBLE` and `doc JSON` per hit,
best first; `doc` holds the hit's stored fields, a value each or an array for
several. All arguments may be columns, so a lateral join searches every split
in a table (`FROM splits s CROSS JOIN tantivy_search(s.path, ...)`), or runs a
query per row. A `NULL` index or query finds nothing. The query uses
tantivy's
[query language](https://docs.rs/tantivy/latest/tantivy/query/struct.QueryParser.html):
words, `"phrases"`, `+required` and `-excluded` terms, `field:term` and
ranges such as `id:[10 TO 20]`. `options`, as JSON:

| Option | Default | |
| --- | --- | --- |
| `top_k` | every hit | the number of top hits |
| `fields` | every indexed text field | the fields to search for terms that name none |
| `conjunctive` | `false` | every term must match |
| `strict` | `false` | fail on query syntax errors instead of dropping what tantivy cannot parse |

### `tantivy_drop(index)`

Removes the split at `index`, and returns whether there was one. On the
writer. A scalar function, so `SELECT tantivy_drop(path) FROM ...` drops
many.

### The fts-style macros

`tantivy_create_index(index, input_table, input_id, input_values, ...)`
builds a split of the `input_values` columns of `input_table`, keyed by
`input_id` (stored as `_key`). It returns the number of rows indexed. Named
parameters:

| Parameter | Default | |
| --- | --- | --- |
| `stemmer` | `'porter'` | `'porter'` (English), another of tantivy's 18 languages (`'french'`, `'german'`, ...), or `'none'` |
| `stopwords` | `'english'` | a language, or `'none'`. Tantivy's English list is Lucene's 33 words, not fts's 571 |
| `strip_accents` | `true` | fold accents (`café` matches `cafe`) |
| `lower` | `true` | lowercase |

`tantivy_match_bm25(index, input_id, query_string, fields := NULL, conjunctive := false)`
returns a row's score, or `NULL`. `fields` restricts the search to some
columns (`'title, body'`), and `conjunctive` requires every term. The query
is parsed leniently, so any user input works. BM25 uses tantivy's
`k1 = 1.2` and `b = 0.75`.

Both macros are short SQL over the functions above (`CREATE_INDEX_MACRO`
and `MATCH_BM25_MACRO` in `extension/src/pgvfs_extension.cpp`). Copy and adapt
them for other schemas. DuckLake can store your own
[macros](https://ducklake.select/docs/stable/duckdb/advanced_features/macros)
with the lake.

## How it works

- **Storage.** A split's files are ordinary pgvfs files, directly under its
  path; the storage layout is unchanged. A split is one tantivy index with
  one segment, written once by one tantivy `IndexWriter`. Dropping it
  unpublishes its files, whose rows stay for the usual 10-minute grace for
  searches already running.
- **Builds** run on the pgvfs writer, using tantivy's indexing threads.
  Each file is written to a local temporary file (`TMPDIR`) and uploaded when
  complete, so a build needs free local disk about the size of the split.
- **Searches** open each split once per DuckDB database and keep its term
  dictionaries in memory. A search then reads each query term's postings,
  plus the stored fields of the hits. A split dropped and rebuilt at the same
  path by another process is picked up within `PGVFS_OPEN_CACHE_S`.
