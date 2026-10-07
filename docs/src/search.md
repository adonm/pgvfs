# Full-text search

The pgvfs extension also carries [tantivy](https://github.com/quickwit-oss/tantivy)
search indexes: build them from any query, and search them with BM25 ranking.
Stored in pgvfs, they live in PostgreSQL alongside the lake, with its secret,
backup and roles, and no search cluster. They use only DuckDB's filesystem,
though, so they work as well on local files or object storage.

As in [Quickwit](https://quickwit.io/docs/overview/concepts/querying), an
index is made of **splits**. A split is an immutable tantivy index in a single
file, at any path DuckDB can write: `pgvfs://lake-fts/docs.tantivy`,
`/data/docs.tantivy`, `s3://bucket/docs.tantivy`. The extension builds,
searches and drops splits. Everything else is SQL: which splits make up an
index, when to add one (say, after each load), and when to merge them. A
DuckLake table makes a good list of splits, versioned with the data it
indexes.

| Function | |
| --- | --- |
| `tantivy_index(index, schema, doc [, options])` | aggregate: builds a split from its rows |
| `tantivy_search(index, query, ...)` | table: ranked hits from one split or a list of them, searched in parallel |
| `tantivy_count(index, query, ...)` | scalar: exact total (or capped, or distinct values), without loading stored documents |
| `tantivy_aggregate(index, query, aggs, ...)` | scalar: tantivy's aggregations as JSON, merged across splits |
| `tantivy_merge(splits, target, ...)` | scalar: native compaction, without re-indexing documents |
| `tantivy_drop(index)` | scalar: removes a split |
| `tantivy_create_index`, `tantivy_match_bm25` | macros that work like DuckDB's [fts](https://duckdb.org/docs/current/core_extensions/full_text_search) |

## Quick start

On the writer, index the `title` and `body` columns of `lake.docs` under its
key `id`:

```sql
FROM tantivy_create_index('pgvfs://lake-fts/docs.tantivy', 'lake.docs', 'id', ['title', 'body']);
```

On any reader, score rows (`NULL` when they don't match):

```sql
SELECT id, title, score
FROM (
    SELECT *, tantivy_match_bm25('pgvfs://lake-fts/docs.tantivy', id, 'small cats') AS score
    FROM lake.docs
)
WHERE score IS NOT NULL
ORDER BY score DESC;
```

or fetch only the top hits and join them to the table, which avoids scanning
it:

```sql
SELECT d.*, s.score
FROM tantivy_search('pgvfs://lake-fts/docs.tantivy', 'small cats', '{"top_k": 10}') s
JOIN lake.docs d ON d.id = (s.doc->>'_key')::INTEGER
ORDER BY s.score DESC;
```

Keep splits outside the lake's `DATA_PATH`, for example in their own volume.
DuckLake's `ducklake_delete_orphaned_files` deletes files under `DATA_PATH`
that its catalog doesn't track.

A split never changes, so to refresh this one, drop it and build it again:
`SELECT tantivy_drop('pgvfs://lake-fts/docs.tantivy')`. Other readers see the
new split within 10 s. To keep searching without a gap, build new splits
instead, as below.

## Keeping an index current with DuckLake

Index each load's changes as a new split. DuckLake's
[`table_changes`](https://ducklake.select/docs/stable/duckdb/advanced_features/data_change_feed)
gives the rows inserted or updated between two snapshots. Each row's `rowid`
stays the same across updates and compaction, and its `snapshot_id` is the
snapshot that wrote the current version. Together they let a search keep
only hits on current rows. To exclude stale versions **before** top-k, counting
and aggregation, record deletes and updates as invalidations too.

```sql
-- once: splits, invalidations, and a durable indexing watermark
CREATE TABLE lake.docs_splits (path VARCHAR, snapshot BIGINT, docs BIGINT);
CREATE TABLE lake.docs_fts_dead (rowid BIGINT, snapshot BIGINT);
CREATE TABLE lake.docs_fts_state (snapshot BIGINT);
BEGIN;
SET VARIABLE s1 = (SELECT id::BIGINT FROM lake.current_snapshot());
INSERT INTO lake.docs_splits
SELECT path, getvariable('s1'), tantivy_index(path,
    '[{"name": "rowid", "type": "i64", "options": {"stored": true, "indexed": true, "fast": true}},
      {"name": "body", "type": "text", "options": {"indexing": {"record": "position", "tokenizer": "en_stem"}}}]',
    c)
FROM (SELECT 'pgvfs://lake-fts/docs/' || getvariable('s1') || '.tantivy' AS path, rowid, * FROM lake.docs) c
GROUP BY path;
INSERT INTO lake.docs_fts_state VALUES (getvariable('s1'));
COMMIT;

-- after each load, on the writer
BEGIN;
SET VARIABLE s0 = (SELECT snapshot FROM lake.docs_fts_state);
SET VARIABLE s1 = (SELECT id::BIGINT FROM lake.current_snapshot());
CREATE OR REPLACE TEMP TABLE docs_changes AS
FROM lake.table_changes('docs', getvariable('s0') + 1, getvariable('s1'));

INSERT INTO lake.docs_fts_dead
SELECT DISTINCT rowid, snapshot_id FROM docs_changes
WHERE change_type IN ('delete', 'update_preimage');

INSERT INTO lake.docs_splits
SELECT path, getvariable('s1'), tantivy_index(path, '<the same schema>', c)
FROM (
    SELECT 'pgvfs://lake-fts/docs/' || getvariable('s1') || '.tantivy' AS path, *
    FROM (
        SELECT * FROM docs_changes
        WHERE change_type IN ('insert', 'update_postimage', 'delete')
        QUALIFY row_number() OVER (PARTITION BY rowid ORDER BY snapshot_id DESC) = 1
    ) latest
    WHERE change_type IN ('insert', 'update_postimage')
) c
GROUP BY path;
UPDATE lake.docs_fts_state SET snapshot = getvariable('s1');
COMMIT;
```

`table_changes` reads only the files that changed, so a load's split costs
in proportion to the load, not the table. Choose the latest event **including
deletes**, then discard deleted rows: otherwise a row inserted and deleted
inside the range would incorrectly enter the split. The watermark advances
even on delete-only loads. Retain the change feed until each range is processed.

Publish the splits, invalidations and watermark together. Files themselves are
not transactional: if the SQL transaction fails after writing a split, drop
the unlisted file before retrying.

Readers can search the latest **indexed** snapshot coherently, even while the
next load is being indexed. Each split gets the IDs invalidated after its
snapshot and up to the indexing watermark:

```sql
BEGIN;
SET VARIABLE r = (SELECT snapshot FROM lake.docs_fts_state);
SELECT d.*, h.score
FROM lake.docs_splits s
CROSS JOIN tantivy_search(s.path, 'small cats',
    '{"top_k": 10, "fast": ["rowid"], "exclude_field": "rowid"}',
    (SELECT list(rowid) FROM lake.docs_fts_dead x
     WHERE x.snapshot > s.snapshot AND x.snapshot <= getvariable('r'))) h
JOIN lake.docs d AT (VERSION => getvariable('r')) ON d.rowid = (h.doc->>'rowid')::BIGINT
ORDER BY h.score DESC
LIMIT 10;
COMMIT;
```

`top_k` applies per split, so it bounds each split's work while `LIMIT`
picks the overall best. Exclusion happens before that bound: a dead hit cannot
crowd out a live one. Sum `tantivy_count(s.path, query, options, dead_ids)` with
the same exclusions for the exact total. Without exclusions, joining live rows
**after** top-k can underfill a page and cannot give correct totals.

Keep the indexed data snapshot available if joining its rows as above. Each
split scores with its own term statistics by default. A list argument searches
several splits in one call; `global_stats` uses their combined statistics.

**Native compaction.** Merge splits as they pile up. When rowids occur in old
and new versions, first prune each source with its **own** dead set, then merge
the live segments. Neither step rescans the table or re-indexes documents:

```sql
BEGIN;
SET VARIABLE r = (SELECT snapshot FROM lake.docs_fts_state);
CREATE TEMP TABLE replaced AS SELECT *, uuid()::VARCHAR AS token FROM lake.docs_splits;
CREATE TEMP TABLE cleaned AS
SELECT 'pgvfs://lake-fts/staging/' || s.token || '.tantivy' AS path,
       tantivy_merge([s.path], 'pgvfs://lake-fts/staging/' || s.token || '.tantivy',
           '{"exclude_field": "rowid"}',
           (SELECT list(rowid) FROM lake.docs_fts_dead x
            WHERE x.snapshot > s.snapshot AND x.snapshot <= getvariable('r'))) AS docs
FROM replaced s;
CREATE TEMP TABLE merged AS
SELECT 'pgvfs://lake-fts/docs/compact-' || getvariable('r') || '.tantivy' AS path,
       getvariable('r') AS snapshot,
       tantivy_merge((SELECT list(path) FROM cleaned),
           'pgvfs://lake-fts/docs/compact-' || getvariable('r') || '.tantivy') AS docs;
DELETE FROM lake.docs_splits;
INSERT INTO lake.docs_splits FROM merged;
COMMIT;
SELECT tantivy_drop(path) FROM (SELECT path FROM replaced UNION ALL SELECT path FROM cleaned);
```

The output must have a new path; use a unique one for another compaction at the
same watermark. On object storage, delay dropping replaced files until old
readers have finished. Native merging does not deduplicate keys automatically.
Retain invalidations while an active split still needs them. You can instead
rebuild from a longer change-feed range, using the same latest-event rule.

**What a merge costs.** `tantivy_merge` copies each source into a local
temporary index (`TMPDIR`), merges all its segments on one thread, and writes
the result. Four splits of 460 MB (4 million documents, 60% excluded), with
`TMPDIR` on disk:

| | copy in | merge | write out | total | re-index the 1.6M live documents from source |
| --- | ---: | ---: | ---: | ---: | ---: |
| local files | 0.6 s | 11.5 s | 0.3 s | 12.7 s | 21.6 s |
| pgvfs, PostgreSQL on the same host | 4.4 s | 10.9 s | 1.7 s | 17.2 s | 20.1 s |

The copy is a quarter of the time through pgvfs at most, so reading the
sources in place would not remove the cost: the merge is. It is one thread,
so it is the part a busy host slows most, while indexing from source uses
every core. When most of the documents are excluded, compare the two on your
data (`bench/search.py --merge` does).

**Large tables.** A tantivy split holds at most 2³¹ documents, and a smaller
one builds, merges and opens faster. Partition big builds: `GROUP BY` builds
one split per group, so a path such as
`'pgvfs://lake-fts/docs/' || s1 || '-' || rowid % 16 || '.tantivy'` gives 16 splits per
build. Merge the per-load splits into larger ranges over time, keeping a few
dozen in all, since a query searches each one.

## The functions

### `tantivy_index(index, schema, doc [, options])`

An aggregate that builds a split at `index`, a path DuckDB can write, from one
document per row, and returns the number of documents. The split is
written as the aggregate finishes. The arguments are per row: with
`GROUP BY`, each group builds its own split, and every row of a group must
name the same `index`. No rows, no split. `index` must not exist yet (on
pgvfs, only the writer can write). A failed split write cleans up its target;
already completed splits are not rolled back with the SQL statement.

- `schema` is a tantivy schema as JSON: an array of
  [field entries](https://docs.rs/tantivy/latest/tantivy/schema/index.html).
  Each has a `name`, a `type` (`text`, `i64`, `u64`, `f64`, `bool`, `date`,
  `facet`, `bytes`, `json_object` or `ip_addr`) and `options` (indexing,
  tokenizer, `stored`, `fast`, `coerce` and so on).
- `doc` is a row/`STRUCT` (just pass `t`; it is cast to JSON, which needs DuckDB's
  `json` extension, autoloaded in standard builds), or a JSON object as text.
  Fields not in the schema and `null`s are skipped; an array gives a field
  several values.
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

**Memory and disk of a build.** Every group of a query is built at the same
time, and each takes about `memory_budget` of heap (256 MB by default, plus
about 10 MB; less for a group with less data than that) on top of DuckDB's own
memory. `memory_limit` does not bound it, and a materialised source table is
DuckDB memory as well. So

> peak ≈ DuckDB's memory + groups × (`memory_budget` + 10 MB)

for groups bigger than the budget. Measured on 2 million documents of about
440 bytes of split each, streamed from a query (DuckDB alone: 40 MB):

| groups | 256 MB (default) | 64 MB | 16 MB |
| ---: | ---: | ---: | ---: |
| 1 | 300 MB | 120 MB | |
| 4 | 780 MB | 320 MB | |
| 16 | 1,680 MB | 1,140 MB | 390 MB |

The default stays below `groups × 266 MB` when groups hold less than the budget
(16 groups of 125,000 documents used 100 MB each). A smaller budget only makes
more, smaller segments to merge at the end: slower for one big group (12 s to
18 s here), not for many (14 s to 13 s). To build a large table in less memory,
lower `memory_budget` or build fewer groups per query.

Each group also keeps its index in a local temporary directory (`TMPDIR`)
until its split is written: about the size of the split, for all groups at
once. On a tmpfs (often `/tmp`) that is memory too, so point `TMPDIR` at a disk.

### `tantivy_search(index, query [, options [, exclude]])`

Searches a path (`VARCHAR`) or a list of paths (`VARCHAR[]`). Returns
`score DOUBLE`, `doc JSON`, and the hit's split `path VARCHAR` per hit,
best first; `doc` holds the hit's stored fields, a value each or an array for
several. All arguments may be columns, so a lateral join searches every split
in a table (`FROM splits s CROSS JOIN tantivy_search(s.path, ...)`), or runs a
query per row. A `NULL` index or query finds nothing. The query uses
tantivy's
[query language](https://docs.rs/tantivy/latest/tantivy/query/struct.QueryParser.html):
words, `"phrases"`, `+required` and `-excluded` terms, `field:term` and
ranges such as `id:[10 TO 20]`. A query starting with `{` is instead
[OpenSearch query DSL](#query-dsl). `options`, as JSON:

| Option | Default | |
| --- | --- | --- |
| `top_k` | every hit | hits to return across the whole list of splits; use a bound on large indexes |
| `offset` | `0` | hits to skip before `top_k`, across all the splits. Documents are read only for the hits returned, so a deep page costs its scoring, not its position |
| `collapse` | none | return only the best hit of each distinct value of this fast field (text, number, boolean or date; one value per document), best first. `top_k` and `offset` then count values. Documents without a value form one group |
| `fields` | every indexed text field | the fields to search for terms that name none |
| `conjunctive` | `false` | every term must match |
| `strict` | `false` | fail on query syntax errors instead of dropping what tantivy cannot parse |
| `fast` | stored fields | build `doc` only from these fast fields, without reading stored documents; `[]` returns `{}` |
| `exclude_field` | none | an `i64` or `u64` fast field whose values are in `exclude`; exactly one value on every document |
| `global_stats` | `false` | use combined BM25 statistics; needs the same schema across splits. As in tantivy, statistics include excluded (deleted) documents until the splits are merged |
| `ignore_unmapped` | `false` | in [query DSL](#query-dsl), a field a split lacks matches nothing instead of failing the call (the other half of searching splits built with different schemas) |

`exclude` is a portable roaring bitmap `BLOB` (32-bit bitmap or 64-bit treemap),
or a `BIGINT[]` of dead IDs. `NULL` means no exclusions. Signed IDs use their
unsigned 64-bit bit pattern in a treemap. Read a bitmap path with DuckDB itself:

```sql
FROM tantivy_search('pgvfs://lake-fts/docs.tantivy', 'roof',
    '{"top_k": 10, "exclude_field": "rowid", "fast": ["rowid"]}',
    (SELECT content FROM read_blob('pgvfs://lake-fts/dead.roaring')));
```

Exclusion works like tantivy's own deletes. The first call with an exclusion
reads the key column of each split once and builds an alive bitset for its
segments; the split keeps the last four (by `exclude_field` and the set), so
later calls with the same set skip dead documents as tantivy's deletes are
skipped: they never crowd out `top_k`, and never reach a count or an
aggregation. A set that changes on every call pays that first read each time:
about 20 ms per million documents of a split (splits build in parallel).

One exclusion set applies to every split in a list. If a rowid is dead in an
old split but live in a new one, use per-split exclusions with lateral calls
as above, or index a unique **document-version** key. Do not globally exclude
that rowid from both versions.

Fast projection keeps grouping and typed output in SQL. For example, with
`roof_id` and `building_id` declared fast in your schema:

```sql
SELECT doc->>'building_id' AS building,
       arg_max((doc->>'roof_id')::BIGINT, score) AS best_roof
FROM tantivy_search(['s3://bucket/part-1.tantivy', 's3://bucket/part-2.tantivy'],
    'roof', '{"fast": ["roof_id", "building_id"]}')
GROUP BY building;
```

A list of splits is searched in parallel, up to DuckDB's `threads` at a time,
and merged: hits by score (equal scores in list order, then document order),
so a page does not depend on how many threads ran. One call over the list also
gives `global_stats`, a merged `top_k` and, for deep pages, `offset`: calling
once per split in a lateral join gives all of these up.

### `tantivy_count(index, query [, options [, exclude]])`

The exact number of matches, as `BIGINT`, across one split or a list of them.
Uses the same query, options and exclusions as search, but ignores `top_k`
and reads no stored documents. A `NULL` index or query returns `NULL`; an
empty list returns zero. Two options change what it counts:

| Option | |
| --- | --- |
| `limit` | stop counting after `limit + 1` matches and return that, so "more than 100" costs a hundred matches, not millions: `{"limit": 100}` returns 101 when there are more |
| `distinct` | the **exact** number of distinct values of a fast field among the matches (text, number, boolean or date; one value per document; documents without one are not counted), merged across splits. Memory is proportional to the distinct values. The `cardinality` aggregation is approximate but much faster and smaller; use it when an estimate will do |

`offset`, `collapse` and `distinct`/`limit` belong to one call each: a search
rejects `limit` and `distinct`, a count rejects `offset` and `collapse`, and
`limit` cannot be combined with `distinct`.

### `tantivy_aggregate(index, query, aggs [, options [, exclude]])`

Tantivy's [aggregation module](https://docs.rs/tantivy/latest/tantivy/aggregation/index.html),
directly: Elasticsearch-shaped aggregation requests and results as JSON.
Includes terms, cardinality, stats, ranges and histograms; fields must be fast.
Intermediate results merge natively across splits. Exclusions apply first;
`top_k` does not limit aggregations. Cardinality is approximate. Tantivy's
default memory and bucket limits apply across the request.

```sql
SELECT tantivy_aggregate(['part-1.tantivy', 'part-2.tantivy'], 'roof',
    '{"buildings": {"terms": {"field": "building_id", "size": 20}},
      "unique_roofs": {"cardinality": {"field": "roof_id"}}}');
```

### `tantivy_merge(splits, target [, options [, exclude]])`

Copies the source segments into a local temporary index and merges them into
one split at `target`, returning the documents kept. `splits` is a nonempty
`VARCHAR[]`; schemas and custom tokenizer definitions must agree. The first
source's index settings are used. Sources are unchanged, and `target` must
not exist. The temporary index needs disk for the sources and merged output.

Options: `memory_budget` (default 256 MB) and `exclude_field`. Exclusions have
the same formats as search, and delete documents during compaction. They need
only a fast integer key, not an indexed one. A merge does not deduplicate IDs;
the caller decides which versions are live.

### Query DSL

Pass the query object directly, or wrap it as `{"query": ...}` (not a whole
OpenSearch search request with `size`, `_source`, etc.):

```sql
FROM tantivy_search('docs.tantivy',
    '{"bool": {
        "must": {"match": {"body": {"query": "roofs", "fuzziness": "AUTO"}}},
        "filter": {"term": {"building_id": "b17"}},
        "must_not": {"term": {"status": "demolished"}}
    }}', '{"top_k": 10, "fast": ["roof_id", "building_id"]}');
```

This is a **subset**, compiled to tantivy queries, not an OpenSearch server.
Unsupported query types and parameters fail rather than being silently ignored:

| Query | Supported parameters |
| --- | --- |
| `bool` | `must`, `should`, `must_not`, `filter`, `minimum_should_match` |
| `match` | `query`, `operator` (`and`/`or`), `minimum_should_match`, `fuzziness`, `fuzzy_transpositions` |
| `match_phrase`, `match_phrase_prefix` | `query`; `slop` for phrases, `max_expansions` for phrase prefixes |
| `multi_match` | named `fields` (with `^boost`), `best_fields`, `most_fields`, `phrase`, `phrase_prefix`; applicable match parameters, `tie_breaker` |
| `term`, `terms` | exact typed values, without analysis; `term` takes `case_insensitive` for text |
| `prefix`, `wildcard`, `regexp` | text `value`, `case_insensitive`; wildcard `*`, `?`, `\` escapes; a regular expression matches a whole term |
| `fuzzy` | text `value`, `fuzziness`, `transpositions` |
| `exists` | `field`: a fast field, or an indexed text field (slower: any term in its dictionary), or a JSON field (any path in it) |
| `range` | `gt`, `gte`, `lt`, `lte` |
| `constant_score`, `dis_max` | `filter`; `queries` and `tie_breaker`, respectively |
| `match_all`, `match_none` | |
| `query_string`, `simple_query_string` | `query`, named `fields`, `default_field`, `default_operator`; tantivy syntax, lenient for `simple_query_string` |

`boost` is supported in query parameter objects. `minimum_should_match` accepts
integers, negative integers, percentages and negative percentages, not
conditional expressions. Fuzziness is `0`, `1`, `2` or `AUTO[:low,high]`.
`bool.filter` and `must_not` do not contribute scores. Numeric values, booleans,
RFC 3339 dates (or epoch milliseconds), facets and IP addresses are typed by
the schema. Regular expressions and scoring follow tantivy's behaviour.

**JSON fields.** A path inside a `json_object` field is a field name:
`{"term": {"attributes.contractor": "acme"}}`. `term`, `terms`, `match`
(analyzed by the JSON field's tokenizer), `match_phrase`, `range` (fast, numeric
bounds typed by their JSON literal) and `exists` accept paths, as do
`multi_match` fields. So do `prefix`, `wildcard`, `regexp` and `fuzzy` (and
`match`'s `fuzziness`), which see only the strings at their own path. Free-form
attributes then need no field per attribute in the schema, and splits with
different attribute sets share one schema. `match_phrase_prefix` on a path
fails.

**Case.** Term-level queries (`term`, `terms`, `prefix`, `wildcard`, `regexp`)
are not analyzed, as in OpenSearch: on a field whose tokenizer lowercases,
`{"prefix": {"title": "Big"}}` finds nothing. `case_insensitive: true` matches
the indexed terms in any case. The value of a `prefix` or a case-insensitive
`term` is literal; in `regexp` it is an expression.

**Missing fields.** A field the schema lacks is an error, so a typo cannot
quietly match nothing. `ignore_unmapped` (a search option) makes it match
nothing instead, as in OpenSearch, for splits built with different schemas.

### `tantivy_drop(index)`

Removes the split at `index`, and returns whether there was one (on pgvfs,
on the writer). A scalar function, so `SELECT tantivy_drop(path) FROM ...`
drops many.

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
uses tantivy's lenient query-string parser, or the DSL above. BM25 uses tantivy's
`k1 = 1.2` and `b = 0.75`.

Both macros are short SQL over the functions above (`CREATE_INDEX_MACRO`
and `MATCH_BM25_MACRO` in `extension/src/tantivy_functions.cpp`). Copy and adapt
them for other schemas. DuckLake can store your own
[macros](https://ducklake.select/docs/stable/duckdb/advanced_features/macros)
with the lake.

## How it works

- **Builds** use tantivy's own index directory in a local temporary one
  (`TMPDIR`), with tantivy's indexing threads, so a build needs free local
  disk about the size of the split. The finished index (merged to one
  segment) is written to `index` as a single file: its files back to back
  and a small footer naming them.
- **Searches** read a split through DuckDB's filesystem, opening it once per
  DuckDB database and keeping its term dictionaries in memory. A search then
  reads query postings and the requested stored or fast fields. Counts and
  aggregations skip the document store. An open split is
  checked for a change (size, modification time, version tag) at most every
  10 s, so another process's rebuild at the same path shows up within that.
- **Parallel searches.** A call over a list of splits searches them on up to
  `threads` threads (each split reads its file through its own handle), then
  merges. Documents of the hits are read in parallel too, one thread a split.
- **Exclusions** are alive bitsets built once per split and set, as above. On
  4 million documents in 4 splits, 60% of them excluded, a count of 2.8 million
  matches takes 9 ms (1.4 ms with nothing excluded), a top 10 takes 6 ms (5 ms),
  and a new set costs 25 ms the first time (`bench/search.py` measures these).
- **Drops** remove the file. On pgvfs its rows stay for the usual 10-minute
  grace, so searches already running finish; object stores delete at once,
  so there drop replaced splits a while after swapping them out.
- **Storage.** On pgvfs a split is an ordinary file; the storage layout is
  unchanged.

## Roadmap

What a real workload (a 12.5M-document index in 13 splits, read through pgvfs,
replaced documents excluded by key) still lacks.

**A faster merge.** `tantivy_merge` is one thread (see "What a merge costs").
Merging halves of the list at once and then their results would use more cores
at the price of a second pass; measure it on the real splits before building it.

**A community release that includes tantivy.** The signed `INSTALL pgvfs FROM
community` build predates the search functions, so projects that need them
load a local build with `allow_unsigned_extensions`. The next release, and its
submission to the community repository, fixes that.
