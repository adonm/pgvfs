/* C ABI of the pgvfs Rust staticlib (src/lib.rs). Errors are
 * returned in *err as heap strings, freed with pgvfs_free_str. */
#ifndef PGVFS_H
#define PGVFS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct PgvfsConn PgvfsConn;
typedef struct PgvfsWriter PgvfsWriter;

typedef struct {
	int64_t file_id;
	int64_t size;
	int64_t created_us; /* microseconds since the Unix epoch */
} PgvfsFile;

typedef void (*pgvfs_list_cb)(void *ctx, const char *path, size_t len);

/* process-wide counters as JSON, cumulative; free with pgvfs_free_str */
char *pgvfs_stats(void);

/* sized for a DuckDB with `threads` threads (<= 0: one per core) */
PgvfsConn *pgvfs_connect(const char *url, int64_t threads, char **err);
void pgvfs_disconnect(PgvfsConn *conn);
void pgvfs_free_str(char *s);

/* 0 found, 1 not found, -1 error */
int pgvfs_open(const PgvfsConn *conn, const char *volume, const char *path, PgvfsFile *out, char **err);
/* exactly len bytes at pos, inside the file; 0 ok, -1 error */
int pgvfs_read(const PgvfsConn *conn, const PgvfsFile *file, uint8_t *buf, int64_t len, int64_t pos, char **err);
/* limit < 0: all; 0 ok, -1 error */
int pgvfs_list(const PgvfsConn *conn, const char *volume, const char *prefix, int64_t limit, pgvfs_list_cb cb,
               void *ctx, char **err);
/* 0 removed, 1 not found, -1 error */
int pgvfs_remove(const PgvfsConn *conn, const char *volume, const char *path, char **err);
/* replaces any file at `to`; 0 ok, -1 error */
int pgvfs_rename(const PgvfsConn *conn, const char *volume, const char *from, const char *to, char **err);

PgvfsWriter *pgvfs_writer_open(const PgvfsConn *conn, const char *volume, const char *path, char **err);
int pgvfs_writer_write(PgvfsWriter *w, const uint8_t *buf, int64_t len, char **err);
/* publishes and frees w; 0 ok, -1 error (nothing published) */
int pgvfs_writer_publish(PgvfsWriter *w, char **err);
/* discards and frees w */
void pgvfs_writer_abort(PgvfsWriter *w);

/* Tantivy splits, each an immutable index at pgvfs://<volume>/<path>
 * (src/index.rs). Schema, documents and options are JSON; options may be NULL. */
typedef struct PgvfsIndexBuild PgvfsIndexBuild;
typedef void (*pgvfs_hit_cb)(void *ctx, double score, const char *doc, size_t len);

PgvfsIndexBuild *pgvfs_index_open(const PgvfsConn *conn, const char *volume, const char *path, const char *schema,
                                  const char *options, char **err);
/* one JSON object; any thread; 0 ok, -1 error */
int pgvfs_index_add(const PgvfsIndexBuild *b, const char *doc, size_t len, char **err);
/* commits and frees b; documents indexed, or -1 (its files removed) */
int64_t pgvfs_index_commit(PgvfsIndexBuild *b, char **err);
/* discards and frees b, removing its files */
void pgvfs_index_abort(PgvfsIndexBuild *b);
/* cb per hit, best first: score and stored fields as JSON; 0 ok, -1 error */
int pgvfs_index_search(const PgvfsConn *conn, const char *volume, const char *path, const char *query,
                       const char *options, pgvfs_hit_cb cb, void *ctx, char **err);
/* 0 dropped, 1 not found, -1 error */
int pgvfs_index_drop(const PgvfsConn *conn, const char *volume, const char *path, char **err);

#ifdef __cplusplus
}
#endif

#endif
