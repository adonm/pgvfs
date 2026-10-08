# Vector search

Nearest-neighbour search over a large lake needs no extension: an IVF
(inverted file) index is a column. Cluster the vectors once, store each row's
nearest centroid as `cluster`, and sort the table by it. A query then probes
its few nearest centroids, and DuckLake reads only those clusters' row
groups, from their min/max statistics, before ranking them exactly.

```sql
-- centroids: k-means on a sample (bench/ivf.py does this in SQL), then
CREATE TABLE lake.centroids (c INTEGER, v FLOAT[]);

-- every row's nearest centroid, the table sorted by it
CREATE TABLE lake.vecs AS
SELECT d.id, arg_min(c.c, list_distance(d.v, c.v)) AS cluster, d.v
FROM data d, lake.centroids c
GROUP BY d.id, d.v
ORDER BY cluster;
ALTER TABLE lake.vecs SET SORTED BY (cluster);

-- the 10 nearest rows, probing the 4 nearest clusters
SELECT id, list_distance(v, $q) AS distance
FROM lake.vecs
WHERE cluster IN (SELECT c FROM lake.centroids ORDER BY list_distance(v, $q) LIMIT 4)
ORDER BY distance
LIMIT 10;
```

DuckLake has no fixed-size `ARRAY` type, so vectors are `FLOAT[]` lists and
distances `list_distance` (or `list_cosine_distance`). New rows take their
nearest centroid as they load, the same `arg_min` against `lake.centroids`.
Retrain and rewrite when the data drifts far from the centroids.

## How well it works

`bench/ivf.py` checks this on synthetic data: 200,000 vectors of 64
dimensions in 256 blobs, 64 clusters (k-means on a 20,000-row sample),
2,048-row row groups, against exact search over the same lake. Recall@10 is
over 50 queries; bytes are those a cold DuckDB read from PostgreSQL.

| Probes | Recall@10, separate blobs | Recall@10, overlapping blobs | Bytes read, share of exact |
| ---: | ---: | ---: | ---: |
| 1 | 0.998 | 0.51 | 2.5–2.7% |
| 2 | 1.0 | 0.59 | 5–7% |
| 4 | 1.0 | 0.69 | 10–14% |
| 8 | 1.0 | 0.79 | 20–28% |
| 16 | | 0.88 | 39% |
| 32 | | 0.98 | 69% |

Reads track the probes: the subquery prunes as well as a literal `IN` list.
Recall depends on how clustered the data is. Real embeddings are clustered,
nearer the first column than the second, but measure on your own, and pick
the probes for the recall you need.

Recall and bytes trade off directly: on overlapping blobs, 4 probes give 0.69
recall for 10–14% of the bytes, and 16 give 0.88 for 39%. Clusters are the
unit of I/O here. A graph index (HNSW, say) visits rows one at a time, and in
pgvfs every read fetches at least one 8 KB chunk, so a search that visits
hundreds of rows reads far more than a few probed clusters. That is an argument
from the storage design, not a measurement: we have not benchmarked a graph
index. Consider one only if the probes your recall needs read more bytes than a
graph search would.

## At scale

Pick the cluster count so a cluster spans a row group or two: for 10 billion
rows and 8,192-row groups, about a million clusters of 10,000 rows. A query
then reads tens of row groups whatever the table's size. Finding the nearest
of a million centroids is itself a scan of a million vectors per query; when
that matters, two levels of centroids (coarse clusters of fine ones) keep it
small, still in SQL.
