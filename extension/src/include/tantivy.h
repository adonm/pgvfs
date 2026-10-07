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
typedef void (*tantivy_hit_cb)(void *ctx, double score, const char *doc, size_t len);

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
/* options (may be NULL) is JSON; cb per hit, best first: score and stored
 * fields as JSON; 0 ok, -1 error */
int tantivy_split_search(const TantivySplit *s, const char *query, const char *options, tantivy_hit_cb cb, void *ctx,
                         char **err);

#ifdef __cplusplus
}
#endif

#endif
