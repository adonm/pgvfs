# Loading data

How a lake is laid out decides most of its read speed, so spend the effort
at load time. A query's cost is the files and row groups it cannot rule out
from their min/max statistics, times what it must decode in the rest. The
steps below are in order of impact; the measurements are at the end.

## 1. Set the lake options before the first insert

```sql
CALL lake.set_option('parquet_row_group_size', 8192);
CALL lake.set_option('parquet_compression', 'lz4');
CALL lake.set_option('target_file_size', '64MB');
```

They are stored in the catalog and apply to files written afterwards.
DuckLake takes no defaults from extensions, so set them on every lake.

- **8,192-row row groups.** A query reads whole row groups, so their size
  sets lookup latency: a 1,000-row lookup took 41 ms with DuckDB's default
  of 122,880 rows, 10 ms with 8,192, and 12.5 ms with 2,048. For large tables
  mostly scanned by long key ranges, 32K–64K rows gives more throughput (the
  option can be set per table).
- **LZ4** decompresses faster than the default snappy.
- **64 MB files** instead of the 512 MB default. DuckLake skips whole files
  using statistics in its catalog before opening any, so smaller files leave
  less to read in the ones a query does open.

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

## 5. Keep it compact

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

## 6. Rewrite after deletes and updates

Deletes go into delete files that every later scan merges in; 10% of rows
deleted cost 15–23%. Rewrite, then expire and clean up as above:

```sql
CALL ducklake_rewrite_data_files('lake', delete_threshold => 0.05);
```

Without `delete_threshold`, a file is only rewritten once 95% of its rows are
deleted.

## Not worth it

- **Splitting hot and cold columns** into separate tables: Parquet is
  columnar, so a query decodes only the columns it reads anyway. No change
  measured.
- **Partitioning** except on low-cardinality columns (a month, a region) that
  nearly every query filters on. Each partition value gets its own files; a
  sort does the same job without many small files.
- **`skip_stats_columns`** (drop statistics on wide unfiltered columns)
  could shrink the catalog, but it isn't in the DuckLake builds for DuckDB 1.5
  or the current 2.0 dev. Untested.

## Measurements

13 readers, PostgreSQL on 3 cores, 16-core laptop; `just bench city` and
`just bench hits`.

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
