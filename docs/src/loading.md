# Loading data

How a lake is laid out decides most of its read speed, so spend the effort
at load time. A query's cost is the files and row groups it cannot rule out
from their min/max statistics, times what it must decode in the rest. The
steps below are in order of impact; the measurements are at the end.

## 1. Set the lake options before the first insert

```sql
CALL lake.set_option('parquet_compression', 'zstd');
CALL lake.set_option('parquet_version', 2);
CALL lake.set_option('parquet_row_group_size', 8192);
CALL lake.set_option('target_file_size', '64MB');
```

They are stored in the catalog and apply to files written afterwards.
DuckLake takes no defaults from extensions, so set them on every lake.

- **8,192-row row groups.** A query reads whole row groups, so their size
  sets lookup latency: a 1,000-row lookup took 41 ms with DuckDB's default
  of 122,880 rows, 10 ms with 8,192, and 12.5 ms with 2,048. For large tables
  mostly scanned by long key ranges, 32K–64K rows gives more throughput (the
  option can be set per table).
- **zstd.** Every byte of a pgvfs lake is PostgreSQL storage, WAL and backup,
  and cold reads fetch it from PostgreSQL. LZ4 stored 55–58% more than zstd,
  and snappy (DuckDB's default) 70% more with no faster reads. LZ4 decodes
  faster, though: when readers are CPU-bound it served 25–35% more queries
  per second (13 readers on Houston), while one client's sequential lookups
  were only 4% faster. Choose LZ4 for a read-heavy lake whose storage is
  cheap; the option can be set per table.
- **Parquet V2** encodings made files about 4% smaller than V1, with about
  the same read speed. Writing them was about 15% slower.
- **64 MB files** instead of the 512 MB default. DuckLake skips whole files
  using statistics in its catalog before opening any, so smaller files leave
  less to read in the ones a query does open.

Set these, and each table's sort order (below), once when the lake is
created, not in every load. One 200 GB load that re-declared its sort
orders inside each batch's transaction slowed from 34 s to 102 s per batch
over its first 84 batches.

## 2. Declare a sort order that matches your filters

```sql
ALTER TABLE lake.events SET SORTED BY (site_id, day);
```

DuckLake then sorts every insert, merge and rewrite by it. Put the column
queries filter on most first. This was the biggest single win measured: 2×
read throughput on 100M rows.

For geometry, sort by a space-filling curve so an area of interest touches
few row groups:

```sql
ALTER TABLE lake.buildings SET SORTED BY (ST_Hilbert(geometry, {'min_x': -95.65, 'min_y': 29.60, 'max_x': -95.15, 'max_y': 29.95}::BOX_2D));
```

Sorting on one coordinate was about 15% slower. Data that already arrives
spatially clustered, like Overture's, needs no re-sort.

## 3. Load big tables in key-range batches

One `INSERT` of 100M wide rows sorts them all at once, which can exhaust
memory. Instead, insert disjoint ranges of the leading sort key, in order,
about 5M rows at a time:

```sql
SET memory_limit = '4GiB';            -- bigger sorts spill to temp_directory
SET temp_directory = '/fast/disk/duckdb-temp';
SET preserve_insertion_order = false;
INSERT INTO lake.events FROM 'events.parquet' WHERE site_id >= 0 AND site_id < 1000;
INSERT INTO lake.events FROM 'events.parquet' WHERE site_id >= 1000 AND site_id < 2000;
-- ...
```

Each sort stays small, and ranges don't overlap across files, so a filtered
query opens one or two files instead of all of them. `bench/hits.py` shows
the pattern.

## 4. Keep columns typed

Convert dates, timestamps and numbers from strings or epochs at load time
(`make_date`, `epoch_ms`, casts). Statistics on typed columns prune; on
strings they mostly don't.

Store nested payloads as `JSON` (text in Parquet), with the fields queries
filter on as typed columns beside them. On DuckDB 1.5.6, shredded `VARIANT`
was 14–19× slower than JSON both for reading payloads whole and for
filtering or aggregating on one field inside them, at the same storage, and
JSON loaded about 35% faster. A 2.0 nightly through DuckLake was still 5–7×
slower.

## 5. For big or continuous loads, write files elsewhere and register them

When the writer itself is the bottleneck, move sorting and encoding out of
it. Any number of DuckDB processes, anywhere, write finished Parquet files in
the lake's layout. The one writer only copies their bytes into pgvfs and
registers them in the catalog, without decoding them:

```sql
-- each producer: one sorted, contiguous key range per file
CREATE TEMP TABLE batch AS
    SELECT *, ntile(8) OVER (ORDER BY site_id, day) - 1 AS bucket FROM 'new_events.parquet';
COPY (SELECT * EXCLUDE (bucket) FROM batch WHERE bucket = 0 ORDER BY site_id, day)
TO 'bucket-0000.parquet' (FORMAT parquet, COMPRESSION zstd, PARQUET_VERSION V2, ROW_GROUP_SIZE 8192);
-- ... one COPY per bucket

-- the writer: copy the bytes, then register the files in one transaction
COPY (SELECT content FROM read_blob('bucket-0000.parquet'))
TO 'pgvfs://lake/main/events/bucket-0000.parquet' (FORMAT blob);
BEGIN;
CALL ducklake_add_data_files('lake', 'events', ['pgvfs://lake/main/events/bucket-0000.parquet']);
COMMIT;
```

- **The files must already be in the lake's layout.** DuckLake neither sorts
  nor rewrites registered files, and their schema must match the table's.
  Pick the bucket count so each file is about 64 MB. Don't rely on
  `FILE_SIZE_BYTES` rotation instead: it interleaves key ranges across
  files, so none of them prune.
- **Copy from the writer process.** The writer lock belongs to the first
  process that writes, so a separate copier would be refused. Several
  connections of the writer's DuckDB share the lock and copy in parallel:
  one streamed about 300 MiB/s, four 536 MiB/s; six and eight were slower
  (PostgreSQL on 8 cores).
- **Commit about 2 GB of files at a time.** Each commit has fixed catalog
  work and makes a snapshot. A 3.2 GiB load took 40.6 s in 256 MB commits,
  35.9 s in 2 GB commits and 36.5 s in 8 GB commits.
- **Give the writer threads; cap its memory with `memory_limit`.** The same
  load took 77 s on 1 thread and 33 s on 6, with the same 9.5 GiB peak, so
  fewer threads only made it slower.

Registering new rows this way ran at 160–190 MiB/s of Parquet, copy and
commit included. Updates and deletes still cost per changed row, because
they must find the rows they replace.

## 6. Keep it compact

Every insert writes its own files and a catalog snapshot, and files from
separate inserts overlap, so statistics prune less. A lake fed by 100 small
inserts was 5× slower than one sorted load. After trickle inserts:

```sql
CALL ducklake_merge_adjacent_files('lake');
CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL 1 HOUR);
CALL ducklake_cleanup_old_files('lake', older_than => now() - INTERVAL 1 HOUR);
```

Merging recovered most of the loss, but merged files keep each insert's rows
together rather than globally sorted. For all of it back, reload into a fresh
sorted table.

Expiring snapshots ends time travel to them, and cleanup then deletes their
files. If anything reads old versions (`AT (VERSION => ...)`, change feeds,
history built on snapshot ids), merge and rewrite but don't expire those
snapshots.

## 7. Rewrite after deletes and updates

Deletes go into delete files that every later scan merges in; 10% of rows
deleted cost 15–23%. Rewrite, then expire and clean up as above:

```sql
CALL ducklake_rewrite_data_files('lake', delete_threshold => 0.05);
```

Without `delete_threshold`, a file is only rewritten once 95% of its rows are
deleted.

Keep DuckLake's default delete files (Parquet row positions). Its
experimental `write_deletion_vectors` option writes Puffin bitmaps instead:
84% smaller, but point and bounding-box reads through pgvfs were about 2×
slower.

## Check what a query reads

`EXPLAIN ANALYZE` shows each `DUCKLAKE_SCAN`'s `Total Files Read`, so you can
see whether a filter prunes before measuring anything. For bytes and round
trips, diff `SELECT pgvfs_stats()` before and after a query.

## Not worth it

- **Splitting hot and cold columns** into separate tables: Parquet is
  columnar, so a query decodes only the columns it reads anyway. No change
  measured.
- **Partitioning** except on low-cardinality columns (a month, a region) that
  nearly every query filters on. Each partition value gets its own files; a
  sort does the same job without many small files.
- **Turning off sorting** to load faster: ingest went from 7.4 s to 7.3 s,
  and reads got both faster and slower. When registering pre-sorted files,
  sorting doesn't run anyway.
- **`skip_stats_columns`** (drop statistics on wide unfiltered columns)
  could shrink the catalog, but it isn't in the DuckLake builds for DuckDB 1.5
  or the current 2.0 dev. Untested.

## Measurements

13 readers, PostgreSQL on 3 cores, 16-core laptop; `just bench city` and
`just bench hits`. The tables below were measured with LZ4, before the
benchmarks moved to zstd and Parquet V2.

**Compression** (Houston, 2 alternating rounds each; `LOAD_ARGS="--compression
lz4 --parquet-version 1" just bench city`; warm is the median warm pass):

| Layout | Read on the cold pass | Cold q/s | Warm q/s | Warm p50 |
| --- | ---: | ---: | ---: | ---: |
| LZ4, V1 | 681 MiB | 193 | 307–318 | 25–27 ms |
| zstd, V2 | 434 MiB | 157–162 | 205–243 | 33–37 ms |
| zstd, V1 | 446 MiB | 138–175 | 186–228 | 33–44 ms |

**ClickBench `hits`** (100M rows, 105 columns, per-site dashboard queries):

| Load | Load time | Cold | Warm | New parameters |
| --- | ---: | ---: | ---: | ---: |
| Source order | 62 s | 16 q/s, p50 337 ms | 40 q/s, p50 140 ms | 25 q/s |
| Sorted, key-range batches | 139 s | 30 q/s, p50 163 ms | 76 q/s, p50 46 ms | 36 q/s |
| … with 64K-row groups | 213 s | 40 q/s, p50 134 ms | 89 q/s, p50 47 ms | 47 q/s |

**File size** (the sorted table, 3 alternating runs each, same load time):

| Files | Cold p50 | Warm p50 | New parameters p50 |
| --- | ---: | ---: | ---: |
| 512 MB (40 files) | 229–250 ms | 62–76 ms | 149–169 ms |
| 64 MB (223 files) | 176–192 ms | 52–65 ms | 121–142 ms |

**Maintenance** (Overture Houston, 1M rows, the recommended setup, 2 runs
each; `LOAD_ARGS="--variant ..." just bench city`):

| Layout | Files | Snapshots | Warm q/s | Warm p50 | Cold p50 |
| --- | ---: | ---: | ---: | ---: | ---: |
| One sorted load | 4 | 7 | 389–398 | 20 ms | 29–36 ms |
| 100 small inserts | 200 | 205 | 77–82 | 144–153 ms | 133–153 ms |
| … then merged and cleaned up | 4 | 1 | 284–288 | 30–31 ms | 61–66 ms |
| 10% of rows deleted¹ | 4 + 4 delete files | 9 | 299–308 | 30 ms | 44–50 ms |
| … then rewritten and cleaned up | 2 | 1 | 319–348 | 24–27 ms | 43–48 ms |
| Hot and cold columns split | 3 + cold tables | 9 | 380–399 | 19–20 ms | 35–38 ms |

¹ Measured before pgvfs cached file opens. DuckDB re-opens delete files on
every query to check them, and the cache now answers those checks from
memory; that took about 10% off this row's warm latency.

### A change-history lake

From an API built on pgvfs: parcels, buildings and roof parts with every
captured version, synthetic data seeded from Overture. DuckDB 1.5.6; for the
pgvfs runs, one writer process with 6 threads and 8 GB and PostgreSQL 18 on
8 cores and 8 GB. Single runs, so treat small differences as noise.

**Compression and row groups** (local files, 3 batches of 100K parcels, 100
sequential ID lookups):

| Layout | Data | Lookup p50 / p99 |
| --- | ---: | ---: |
| zstd, 122,880-row groups | 281 MB | 122 / 142 ms |
| zstd, 8,192-row groups | 298 MB | 57 / 70 ms |
| LZ4, 8,192-row groups | 470 MB | 55 / 70 ms |
| LZ4, 32,768-row groups | 450 MB | 50 / 59 ms |

**Storage format** (in pgvfs; one 500K-parcel stream registered as finished
files; reads are 1-client p50 with positional deletes):

| Compression, Parquet version | Files | Load | Building lookup | Bounding-box page |
| --- | ---: | ---: | ---: | ---: |
| zstd, V1 | 819 MiB | 7.5 s | 56 ms | 403 ms |
| zstd, V2 | 785 MiB | 8.7 s | 56 ms | 398 ms |
| snappy, V1 | 1,400 MiB | 8.8 s | 58 ms | 414 ms |
| snappy, V2 | 1,361 MiB | 10.3 s | 59 ms | 405 ms |

**Delete files** (the same 2.9M deleted rows in 26 files):

| Format | Size | Building lookup | Bounding-box page |
| --- | ---: | ---: | ---: |
| Parquet positions (default) | 11.9 MB | 56 ms | 398 ms |
| Puffin deletion vectors | 1.9 MB | 106 ms | 907 ms |
