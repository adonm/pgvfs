/* C ABI of tantivy splits (tantivy/src/lib.rs), linked into the pgvfs
 * staticlib. Storage is the caller's: a build writes its split to a
 * callback, a split reads itself through one. Errors are returned in *err as
 * heap strings, freed with tantivy_free_str; callbacks report theirs as a
 * message of at most cap bytes in msg and a nonzero return. */
#ifndef TANTIVY_H
#define TANTIVY_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TantivyBuild TantivyBuild;
typedef struct TantivySplit TantivySplit;

typedef int (*tantivy_read_cb)(void *ctx, uint8_t *buf, uint64_t len, uint64_t offset, char *msg, size_t cap);
typedef int (*tantivy_write_cb)(void *ctx, const uint8_t *buf, uint64_t len, char *msg, size_t cap);
typedef void (*tantivy_hit_cb)(void *ctx, size_t split, double score, const char *doc, size_t len);

void tantivy_free_str(char *s);

/* schema and options (may be NULL) are JSON */
TantivyBuild *tantivy_build_open(const char *schema, const char *options, char **err);
/* one JSON object; any thread; 0 ok, -1 error */
int tantivy_build_add(const TantivyBuild *b, const char *doc, size_t len, char **err);
/* commits, writes the split to cb and frees b; documents indexed, or -1 */
int64_t tantivy_build_finish(TantivyBuild *b, tantivy_write_cb cb, void *ctx, char **err);
/* discards and frees b */
void tantivy_build_abort(TantivyBuild *b);

/* cb reads the split's size bytes, from any thread; ctx outlives the split */
TantivySplit *tantivy_split_open(uint64_t size, tantivy_read_cb cb, void *ctx, char **err);
void tantivy_split_close(TantivySplit *s);

/* Over n splits: a query in tantivy's syntax or OpenSearch query DSL (JSON),
 * options (may be NULL) as JSON, and an exclude set: kind 0 none, 1 a roaring
 * bitmap of len bytes, 2 len int64 values. */
/* cb per hit, best first: split position, score, doc as JSON; 0 ok, -1 error */
int tantivy_search(const TantivySplit *const *splits, size_t n, const char *query, const char *options,
                   int exclude_kind, const void *exclude, size_t exclude_len, tantivy_hit_cb cb, void *ctx, char **err);
/* the number of matches, or -1 */
int64_t tantivy_count(const TantivySplit *const *splits, size_t n, const char *query, const char *options,
                      int exclude_kind, const void *exclude, size_t exclude_len, char **err);
/* tantivy aggregations (Elasticsearch JSON) over the matches; *out freed with
 * tantivy_free_str; 0 ok, -1 error */
int tantivy_aggregate(const TantivySplit *const *splits, size_t n, const char *query, const char *aggs,
                      const char *options, int exclude_kind, const void *exclude, size_t exclude_len, char **out,
                      char **err);
/* merges the splits into one written to cb, without excluded documents;
 * documents kept, or -1 */
int64_t tantivy_merge(const TantivySplit *const *splits, size_t n, const char *options, int exclude_kind,
                      const void *exclude, size_t exclude_len, tantivy_write_cb cb, void *ctx, char **err);

#ifdef __cplusplus
}
#endif

#endif
