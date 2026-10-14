//! pgdb:// databases: DuckDB's database and WAL files as versioned 8112-byte
//! chunks (dbschema.sql). One writer publishes a gen per fsync in one
//! statement; readers open a gen and read the newest version of each chunk at
//! or below it. A read checks the reap horizon in its own statement, so an
//! expired gen fails rather than reading wrong bytes. DuckDB's external file
//! cache keys on the last gen that wrote a chunk (`ChangeMap::runs`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Result};
use bytes::{BufMut, Bytes, BytesMut};
use futures::{SinkExt, TryStreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{ToSql, Type};

use crate::pg::{Dedicated, Pool};
use crate::store;

pub const CHUNK: i64 = 8112;
const CH: usize = CHUNK as usize;
pub const DBSCHEMA: &str = include_str!("../dbschema.sql");
/// In the error of a read whose gen was reaped: callers fall back or re-attach.
pub const EXPIRED: &str = "pgdb version expired";
/// The files of a database `name`: `name` itself and DuckDB's WAL files.
pub const SUFFIXES: [&str; 4] = ["", ".wal", ".wal.checkpoint", ".wal.recovery"];

/// Rows per parallel read piece, as for pgvfs:// files (store.rs).
const PIECE_ROWS: i64 = 1032;
/// The writer sends whole staged chunks in batches of about this much
/// (PGVFS_DB_FLUSH_KB, for tests that want many batches).
fn flush_bytes() -> usize {
    static BYTES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *BYTES.get_or_init(|| {
        std::env::var("PGVFS_DB_FLUSH_KB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&kb| kb > 0)
            .map_or(8 << 20, |kb| kb << 10)
    })
}
/// ... and everything, partial chunks too, once this much is staged.
const MAX_STAGED: usize = 64 << 20;
/// Change-map segments before a reader rebases (forgets them; its cache
/// keys restart from the newest gen).
const MAX_SEGMENTS: usize = 200_000;

const READ_SQL: &str =
    "SELECT -1::int4, reap_horizon, NULL::bytea FROM pgvfs.dbs WHERE db_id = $4 \
     UNION ALL SELECT no, gen, data FROM pgvfs.dbchunks \
     WHERE file_id = $1 AND no >= $2 AND no <= $3 AND gen <= $5";
const FENCE: &str =
    "WITH ok AS (SELECT 1 FROM pgvfs.dbs WHERE db_id = $1 AND epoch = $2 FOR SHARE) ";
const UPSERT: &str = " ON CONFLICT (file_id, no, gen) DO UPDATE SET data = EXCLUDED.data";
const COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\x00\x00\x00\x00\x00\x00\x00\x00";

pub fn check_name(name: &str) -> Result<()> {
    anyhow::ensure!(
        (1..=1024).contains(&name.len())
            && !name.contains('@')
            && !SUFFIXES[1..].iter().any(|s| name.ends_with(s)),
        "invalid pgdb database name {name:?} (1-1024 bytes, no '@', not ending in a WAL suffix)"
    );
    Ok(())
}

fn check_suffix(path: &str) -> Result<()> {
    anyhow::ensure!(
        SUFFIXES.contains(&path),
        "pgdb:// holds only databases and their WAL files, not {path:?}"
    );
    Ok(())
}

fn no_schema(e: &tokio_postgres::Error) -> bool {
    e.code() == Some(&SqlState::UNDEFINED_TABLE)
}

/// Makes the database writable (store::ensure_writable) and installs the
/// pgdb tables if they are missing.
pub async fn ensure_schema(pool: &Pool) -> Result<()> {
    store::ensure_writable(pool).await?;
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    tx.batch_execute("SELECT pg_advisory_xact_lock(hashtext('pgvfs.install')::bigint)")
        .await?;
    let missing: bool = tx
        .query_one("SELECT to_regclass('pgvfs.dbchunks') IS NULL", &[])
        .await?
        .try_get(0)?;
    if missing {
        tx.batch_execute(DBSCHEMA).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// The newest published gen of a database, or None if there is no such database.
pub async fn latest(pool: &Pool, volume: &str, name: &str) -> Result<Option<i64>> {
    let conn = pool.get().await?;
    match conn
        .query_typed_opt(
            "SELECT committed_gen FROM pgvfs.dbs WHERE volume = $1 AND name = $2",
            &[(&volume, Type::TEXT), (&name, Type::TEXT)],
        )
        .await
    {
        Err(e) if no_schema(&e) => Ok(None),
        r => Ok(r?.map(|row| row.get(0))),
    }
}

/// This process's databases, and the writer of each that it has open.
#[derive(Default)]
pub struct Dbs {
    map: Mutex<HashMap<(String, String), Arc<Db>>>,
    by_id: Mutex<HashMap<i32, Weak<Db>>>,
}

impl Dbs {
    pub fn by_id(&self, db_id: i32) -> Option<Arc<Db>> {
        self.by_id.lock().unwrap().get(&db_id)?.upgrade()
    }
}

pub struct Db {
    pub db_id: i32,
    pub label: String,
    writer: Mutex<Option<(Arc<Writer>, usize)>>,
    changes: Mutex<ChangeMap>,
    readahead: Mutex<Readahead>,
}

/// A read continuing the previous one also fetches the blocks after it, so a
/// scan's round trips drop from one per block to one per window. A read after
/// a gap fetches exactly its own bytes.
#[derive(Default)]
struct Readahead {
    last: HashMap<(i64, i64), (i64, u64)>,
    extents: HashMap<(i64, i64), Vec<Extent>>,
    held: usize,
    next: u64,
}

struct Extent {
    start: i64,
    stamp: u64,
    data: Box<[u8]>,
}

enum Plan {
    Hit { skip: usize, index: usize },
    Fetch { len: i64, widen: bool },
}

impl Readahead {
    fn bytes(&self) -> i64 {
        static BYTES: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
        *BYTES.get_or_init(|| {
            std::env::var("PGVFS_DB_READAHEAD_KB")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .map_or(1 << 20, |kb| (kb << 10) as i64)
        })
    }

    fn slots(&self) -> usize {
        static SLOTS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *SLOTS.get_or_init(|| {
            std::env::var("PGVFS_DB_READAHEAD_SLOTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8)
        })
    }

    fn stamp(&mut self) -> u64 {
        self.next += 1;
        self.next
    }

    fn plan(&mut self, key: (i64, i64), pos: i64, len: i64) -> Plan {
        let stamp = self.stamp();
        if let Some(extents) = self.extents.get_mut(&key) {
            for (index, e) in extents.iter_mut().enumerate() {
                if pos >= e.start && pos + len <= e.start + e.data.len() as i64 {
                    e.stamp = stamp;
                    return Plan::Hit {
                        skip: (pos - e.start) as usize,
                        index,
                    };
                }
            }
        }
        let ra = self.bytes();
        let widen = ra > 0 && len <= ra && self.last.get(&key).map(|&(end, _)| end) == Some(pos);
        Plan::Fetch {
            len: if widen { len + ra } else { len },
            widen,
        }
    }

    fn put(&mut self, key: (i64, i64), start: i64, data: Box<[u8]>) {
        let stamp = self.stamp();
        while self.held >= self.slots() {
            let oldest = self
                .extents
                .iter()
                .flat_map(|(k, e)| e.iter().enumerate().map(move |(i, x)| (*k, i, x.stamp)))
                .min_by_key(|&(_, _, stamp)| stamp);
            let Some((key, index, _)) = oldest else { break };
            let extents = self.extents.get_mut(&key).unwrap();
            extents.swap_remove(index);
            self.held -= 1;
            if extents.is_empty() {
                self.extents.remove(&key);
            }
        }
        self.extents
            .entry(key)
            .or_default()
            .push(Extent { start, stamp, data });
        self.held += 1;
    }

    fn done(&mut self, key: (i64, i64), pos: i64, len: i64) {
        let stamp = self.stamp();
        self.last.insert(key, (pos + len, stamp));
        if self.last.len() > self.slots() * 8 {
            if let Some(old) = self
                .last
                .iter()
                .min_by_key(|(_, &(_, stamp))| stamp)
                .map(|(k, _)| *k)
            {
                self.last.remove(&old);
            }
        }
    }
}

impl Dbs {
    pub async fn find(
        &self,
        pool: &Pool,
        volume: &str,
        name: &str,
        create: bool,
    ) -> Result<Option<Arc<Db>>> {
        store::check_volume(volume)?;
        check_name(name)?;
        let key = (volume.to_owned(), name.to_owned());
        if let Some(db) = self.map.lock().unwrap().get(&key) {
            return Ok(Some(db.clone()));
        }
        let conn = pool.get().await?;
        if create {
            conn.execute_typed(
                "INSERT INTO pgvfs.dbs (volume, name) VALUES ($1, $2) ON CONFLICT (volume, name) DO NOTHING",
                &[(&volume, Type::TEXT), (&name, Type::TEXT)],
            )
            .await?;
        }
        let row = match conn
            .query_typed_opt(
                "SELECT db_id FROM pgvfs.dbs WHERE volume = $1 AND name = $2",
                &[(&volume, Type::TEXT), (&name, Type::TEXT)],
            )
            .await
        {
            Err(e) if no_schema(&e) => None,
            r => r?,
        };
        let Some(row) = row else { return Ok(None) };
        let db = Arc::new(Db {
            db_id: row.get(0),
            label: format!("pgdb://{volume}/{name}"),
            writer: Mutex::new(None),
            changes: Mutex::new(ChangeMap::new()),
            readahead: Mutex::new(Readahead::default()),
        });
        let mut map = self.map.lock().unwrap();
        let db = map.entry(key).or_insert(db).clone();
        self.by_id
            .lock()
            .unwrap()
            .insert(db.db_id, Arc::downgrade(&db));
        Ok(Some(db))
    }
}

impl Db {
    /// This process's writer, if it has the database open for writing.
    pub fn writer(&self) -> Option<Arc<Writer>> {
        self.writer.lock().unwrap().as_ref().map(|(w, _)| w.clone())
    }

    /// A handle on the writer, opening it if this process has none.
    pub fn acquire_writer(
        self: &Arc<Self>,
        rt: &tokio::runtime::Handle,
        pool: &Pool,
    ) -> Result<Arc<Writer>> {
        let mut slot = self.writer.lock().unwrap();
        if let Some((w, n)) = slot.as_mut() {
            w.check()?;
            *n += 1;
            return Ok(w.clone());
        }
        let w = rt.block_on(Writer::open(rt.clone(), pool.clone(), self.clone()))?;
        *slot = Some((w.clone(), 1));
        Ok(w)
    }

    pub fn join_writer(&self) -> Option<Arc<Writer>> {
        let mut slot = self.writer.lock().unwrap();
        slot.as_mut().map(|(w, n)| {
            *n += 1;
            w.clone()
        })
    }

    /// The last handle closes: publish and release the database.
    pub fn release_writer(&self) -> Result<()> {
        let mut slot = self.writer.lock().unwrap();
        let Some((w, n)) = slot.as_mut() else {
            return Ok(());
        };
        *n -= 1;
        if *n > 0 {
            return Ok(());
        }
        let w = w.clone();
        *slot = None;
        w.finish() // under the slot lock: the next writer waits for this one
    }

    /// Bring the change map up to `gen`.
    async fn advance(&self, pool: &Pool, gen: i64, horizon: i64) -> Result<()> {
        let from = {
            let mut m = self.changes.lock().unwrap();
            if m.base < 0 || horizon > m.base {
                // First gen, or the base was reaped: start over.
                let at = gen.max(m.upto);
                m.reset(at);
            }
            if gen <= m.upto {
                return Ok(());
            }
            m.upto
        };
        let conn = pool.get().await?;
        let rows = conn
            .query_typed(
                "SELECT gen, changes FROM pgvfs.dbgens WHERE db_id = $1 AND gen > $2 AND gen <= $3 ORDER BY gen",
                &[
                    (&self.db_id, Type::INT4),
                    (&from, Type::INT8),
                    (&gen, Type::INT8),
                ],
            )
            .await?;
        let mut m = self.changes.lock().unwrap();
        if m.upto != from {
            if m.upto < gen {
                m.reset(gen); // raced with another advance; restart rather than merge
            }
            return Ok(());
        }
        let complete = rows.len() as i64 == gen - from
            && rows
                .iter()
                .enumerate()
                .all(|(i, r)| r.get::<_, i64>(0) == from + 1 + i as i64);
        if !complete {
            m.reset(gen); // some were pruned: their keys are unknown
            return Ok(());
        }
        for row in &rows {
            let g: i64 = row.get(0);
            let changes: Vec<i64> = row.get(1);
            for t in changes.as_chunks::<3>().0 {
                m.insert(t[0], t[1], t[2], g);
            }
        }
        m.upto = gen;
        if m.segments > MAX_SEGMENTS {
            let at = m.upto;
            m.reset(at);
        }
        Ok(())
    }

    /// (first chunk, end chunk, key gen) runs for a read, None if unkeyable.
    pub fn runs(&self, file_id: i64, k0: i64, k1: i64, gen: i64) -> Option<Vec<(i64, i64, i64)>> {
        self.changes.lock().unwrap().runs(file_id, k0, k1, gen)
    }
}

/// Which gen last wrote each chunk, for gens in (base, upto]: segments of
/// chunks per file, each with the gens that wrote it, ascending. A chunk no
/// segment covers is unchanged since base. Valid for reads at gens in
/// [base, upto].
pub struct ChangeMap {
    base: i64,
    upto: i64,
    files: HashMap<i64, BTreeMap<i64, Seg>>,
    segments: usize,
}

struct Seg {
    end: i64,
    gens: Vec<i64>,
}

impl ChangeMap {
    pub fn new() -> ChangeMap {
        ChangeMap {
            base: -1,
            upto: -1,
            files: HashMap::new(),
            segments: 0,
        }
    }

    pub fn reset(&mut self, at: i64) {
        self.base = at;
        self.upto = at;
        self.files.clear();
        self.segments = 0;
    }

    /// Chunks [lo, hi) of `file` were written at `gen`.
    pub fn insert(&mut self, file: i64, lo: i64, hi: i64, gen: i64) {
        if lo >= hi {
            return;
        }
        let map = self.files.entry(file).or_default();
        for at in [lo, hi] {
            let split = map
                .range_mut(..at)
                .next_back()
                .filter(|(_, s)| s.end > at)
                .map(|(_, s)| {
                    let tail = Seg {
                        end: s.end,
                        gens: s.gens.clone(),
                    };
                    s.end = at;
                    tail
                });
            if let Some(tail) = split {
                map.insert(at, tail);
                self.segments += 1;
            }
        }
        let mut cur = lo;
        let starts: Vec<i64> = map.range(lo..hi).map(|(&s, _)| s).collect();
        let mut fresh = Vec::new();
        for s in starts {
            if s > cur {
                fresh.push((cur, s));
            }
            let seg = map.get_mut(&s).unwrap();
            if seg.gens.last() != Some(&gen) {
                seg.gens.push(gen);
            }
            cur = seg.end;
        }
        if cur < hi {
            fresh.push((cur, hi));
        }
        for (a, b) in fresh {
            map.insert(
                a,
                Seg {
                    end: b,
                    gens: vec![gen],
                },
            );
            self.segments += 1;
        }
    }

    /// The map's runs for [k0, k1], merged where key gens are equal.
    pub fn runs(&self, file: i64, k0: i64, k1: i64, gen: i64) -> Option<Vec<(i64, i64, i64)>> {
        if self.base < 0 || gen < self.base || gen > self.upto {
            return None;
        }
        let mut out: Vec<(i64, i64, i64)> = Vec::new();
        let mut push = |a: i64, b: i64, key: i64| {
            if let Some(last) = out.last_mut() {
                if last.1 == a && last.2 == key {
                    last.1 = b;
                    return;
                }
            }
            out.push((a, b, key));
        };
        let end = k1 + 1;
        let mut cur = k0;
        if let Some(map) = self.files.get(&file) {
            let first = map
                .range(..=k0)
                .next_back()
                .filter(|(_, s)| s.end > k0)
                .map(|(&s, _)| s)
                .unwrap_or(k0);
            for (&s, seg) in map.range(first..end) {
                let a = s.max(k0);
                if a > cur {
                    push(cur, a, self.base);
                }
                let b = seg.end.min(end);
                let n = seg.gens.partition_point(|&g| g <= gen);
                let key = if n == 0 { self.base } else { seg.gens[n - 1] };
                push(a, b, key);
                cur = b;
            }
        }
        if cur < end {
            push(cur, end, self.base);
        }
        Some(out)
    }
}

impl Default for ChangeMap {
    fn default() -> Self {
        ChangeMap::new()
    }
}

/// A published file as a reader opened it.
pub struct Version {
    pub db: Arc<Db>,
    pub gen: i64,
    pub file_id: i64,
    pub size: i64,
}

/// `path` of the database at `gen` (None: the newest). Fails if that gen
/// has been reaped.
pub async fn open_version(
    pool: &Pool,
    db: &Arc<Db>,
    gen: Option<i64>,
    path: &str,
) -> Result<Option<Version>> {
    check_suffix(path)?;
    // One implicit transaction; simple-query text: every value is an integer
    // or one of SUFFIXES.
    let gen_sql = gen.map_or("d.committed_gen".to_owned(), |g| g.to_string());
    let sql = format!(
        "SET LOCAL enable_indexscan = on; \
         SELECT d.committed_gen, d.reap_horizon, f.file_id, f.size FROM pgvfs.dbs d \
         LEFT JOIN LATERAL (SELECT file_id, size FROM pgvfs.dbfiles \
           WHERE db_id = d.db_id AND path = '{path}' AND gen <= {gen_sql} \
           ORDER BY gen DESC LIMIT 1) f ON true \
         WHERE d.db_id = {}",
        db.db_id
    );
    let conn = pool.get().await?;
    let messages = conn.simple_query(&sql).await?;
    drop(conn);
    let row = messages
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .ok_or_else(|| anyhow!("{} is gone", db.label))?;
    let int = |i: usize| -> Result<Option<i64>> {
        row.get(i)
            .map(str::parse::<i64>)
            .transpose()
            .map_err(Into::into)
    };
    let committed = int(0)?.unwrap_or(0);
    let horizon = int(1)?.unwrap_or(0);
    let gen = gen.unwrap_or(committed);
    anyhow::ensure!(
        gen <= committed,
        "{}@{gen}: no such version (the newest is {committed})",
        db.label
    );
    anyhow::ensure!(
        gen >= horizon,
        "{}@{gen}: {EXPIRED} (superseded for longer than the reap grace); attach the database again",
        db.label
    );
    let (Some(file_id), Some(size)) = (int(2)?, int(3)?) else {
        return Ok(None);
    };
    db.advance(pool, gen, horizon).await?;
    Ok(Some(Version {
        db: db.clone(),
        gen,
        file_id,
        size,
    }))
}

/// Fill `buf` with the file's bytes from `pos` as of `gen`: the newest
/// version of each chunk at or below it, zeros where none is. Fails (with
/// EXPIRED) unless the reap horizon is at most `check`: versions superseded
/// above `check` may be gone.
/// A read with readahead (see `Readahead`).
pub async fn read_with_readahead(
    db: &Db,
    pool: &Pool,
    file_id: i64,
    gen: i64,
    pos: i64,
    buf: &mut [u8],
) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let key = (file_id, gen);
    let plan = db
        .readahead
        .lock()
        .unwrap()
        .plan(key, pos, buf.len() as i64);
    match plan {
        Plan::Hit { skip, index } => {
            let cache = db.readahead.lock().unwrap();
            let e = &cache.extents[&key][index];
            buf.copy_from_slice(&e.data[skip..skip + buf.len()]);
            store::STATS
                .db_readahead_hits
                .fetch_add(1, Ordering::Relaxed);
        }
        Plan::Fetch { len, widen } => {
            let mut fetched = vec![0u8; len as usize];
            read_versions(pool, db.db_id, file_id, gen, gen, pos, &mut fetched).await?;
            buf.copy_from_slice(&fetched[..buf.len()]);
            let mut cache = db.readahead.lock().unwrap();
            if widen {
                let extra = fetched.len() as u64 - buf.len() as u64;
                cache.put(key, pos, fetched.into());
                store::STATS
                    .db_readahead_bytes
                    .fetch_add(extra, Ordering::Relaxed);
            }
            cache.done(key, pos, buf.len() as i64);
        }
    }
    Ok(())
}

pub async fn read_versions(
    pool: &Pool,
    db_id: i32,
    file_id: i64,
    gen: i64,
    check: i64,
    pos: i64,
    buf: &mut [u8],
) -> Result<()> {
    let len = buf.len() as i64;
    if len == 0 {
        return Ok(());
    }
    anyhow::ensure!(pos >= 0, "negative read position");
    let first = pos / CHUNK;
    let last = (pos + len - 1) / CHUNK;
    let mut pieces = Vec::new();
    let mut rest = buf;
    let mut lo = first;
    while lo <= last {
        let hi = (lo + PIECE_ROWS - 1).min(last);
        let start = (lo * CHUNK).max(pos);
        let end = ((hi + 1) * CHUNK).min(pos + len);
        let (piece, tail) = rest.split_at_mut((end - start) as usize);
        rest = tail;
        pieces.push(read_piece(
            pool, db_id, file_id, gen, check, lo, hi, start, piece,
        ));
        lo = hi + 1;
    }
    store::STATS
        .db_pieces
        .fetch_add(pieces.len() as u64, Ordering::Relaxed);
    futures::future::try_join_all(pieces).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn read_piece(
    pool: &Pool,
    db_id: i32,
    file_id: i64,
    gen: i64,
    check: i64,
    lo: i64,
    hi: i64,
    start: i64,
    out: &mut [u8],
) -> Result<()> {
    let mut tries = 0;
    loop {
        match read_piece_once(pool, db_id, file_id, gen, check, lo, hi, start, out).await {
            Err(e) if tries == 0 && store::lost_connection(&e) => tries += 1,
            r => return r,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_piece_once(
    pool: &Pool,
    db_id: i32,
    file_id: i64,
    gen: i64,
    check: i64,
    lo: i64,
    hi: i64,
    start: i64,
    out: &mut [u8],
) -> Result<()> {
    out.fill(0);
    let conn = pool.get().await?;
    let stmt = conn
        .prepare_typed_cached(
            READ_SQL,
            &[Type::INT8, Type::INT4, Type::INT4, Type::INT4, Type::INT8],
        )
        .await?;
    let (lo32, hi32) = (lo as i32, hi as i32);
    let params: [&(dyn ToSql + Sync); 5] = [&file_id, &lo32, &hi32, &db_id, &gen];
    let rows = conn.query_raw(&stmt, params).await?;
    futures::pin_mut!(rows);
    let end = start + out.len() as i64;
    let mut best = vec![-1i64; (hi - lo + 1) as usize];
    let mut horizon = None;
    while let Some(row) = rows.try_next().await? {
        let no: i32 = row.try_get(0)?;
        let g: i64 = row.try_get(1)?;
        if no < 0 {
            horizon = Some(g);
            continue;
        }
        let slot = &mut best[(no as i64 - lo) as usize];
        if g <= *slot {
            continue;
        }
        *slot = g;
        let data: &[u8] = row.try_get(2)?;
        let row_start = no as i64 * CHUNK;
        let a = start.max(row_start);
        let b = end.min(row_start + CHUNK);
        if a >= b {
            continue;
        }
        let dst = &mut out[(a - start) as usize..(b - start) as usize];
        dst.fill(0);
        let have = (row_start + data.len() as i64).min(b);
        if have > a {
            dst[..(have - a) as usize]
                .copy_from_slice(&data[(a - row_start) as usize..(have - row_start) as usize]);
        }
    }
    match horizon {
        None => bail!("pgdb database {db_id} is gone"),
        Some(h) if h > check => bail!(
            "{EXPIRED}: gen {check} of database {db_id} was superseded for longer than the reap grace \
             (PGVFS_REAP_GRACE_S: {} seconds here); attach the database again",
            store::reap_grace_seconds()
        ),
        _ => Ok(()),
    }
}

/// A chunk staged by the writer: bytes [a, b) of `data` are written.
struct Staged {
    data: Vec<u8>,
    cov: Vec<(u32, u32)>,
    /// 0: not sent yet; else the batch that carries it.
    seq: u64,
}

impl Staged {
    fn new() -> Staged {
        Staged {
            data: vec![0; CH],
            cov: Vec::new(),
            seq: 0,
        }
    }

    /// Whether the first `need` bytes are all written.
    fn covers(&self, need: usize) -> bool {
        need == 0
            || self
                .cov
                .first()
                .is_some_and(|&(a, b)| a == 0 && b as usize >= need)
    }

    fn add(&mut self, a: u32, b: u32) {
        let (mut a, mut b) = (a, b);
        self.cov.retain(|&(x, y)| {
            if y < a || x > b {
                return true;
            }
            a = a.min(x);
            b = b.max(y);
            false
        });
        let at = self.cov.partition_point(|&(x, _)| x < a);
        self.cov.insert(at, (a, b));
    }
}

/// A chunk for the writer task: `data` holds the chunk's bytes; `cov` (if
/// not complete) the ranges of it written, merged into the newest version.
struct Out {
    file_id: i64,
    no: i32,
    data: Vec<u8>,
    cov: Option<Vec<(u32, u32)>>,
    /// Nothing for this chunk in this gen yet: plain COPY, no upsert.
    fresh: bool,
}

struct PublishMsg {
    gen: i64,
    chunks: Vec<Out>,
    paths: Vec<String>,
    file_ids: Vec<Option<i64>>,
    sizes: Vec<Option<i64>>,
    retired: Vec<i64>,
    changes: Vec<i64>,
}

type Reply<T> = oneshot::Sender<Result<T>>;

enum Msg {
    Batch {
        gen: i64,
        seq: u64,
        chunks: Vec<Out>,
    },
    Create(Reply<i64>),
    Truncate {
        gen: i64,
        file_id: i64,
        size: i64,
        reply: Reply<()>,
    },
    Publish {
        msg: Box<PublishMsg>,
        seq: u64,
        reply: Reply<()>,
    },
    Barrier(Reply<()>),
    Close {
        clean: bool,
        reply: Reply<()>,
    },
}

#[derive(Clone, Copy)]
struct FileState {
    file_id: i64,
    size: i64,
}

#[derive(Default)]
struct WState {
    /// The working gen: published gen + 1.
    gen: i64,
    files: HashMap<String, FileState>,
    dirty: bool,
    dirty_paths: BTreeSet<String>,
    retired: Vec<i64>,
    changed: HashMap<i64, BTreeSet<i64>>,
    /// (file_id, first chunk) of truncations in this gen.
    truncs: Vec<(i64, i64)>,
    staged: HashMap<(i64, i64), Staged>,
    /// Staged chunks not sent yet.
    unsent: usize,
    /// The batch `prune` last pruned up to.
    pruned: u64,
    sent: std::collections::HashSet<(i64, i64)>,
    /// Recently sent chunks' content (file tails): appending needs no read.
    clean: HashMap<(i64, i64), Vec<u8>>,
    next_seq: u64,
}

/// The one writer of a database in this process. Writes are staged, sent in
/// order to a task on one dedicated connection (which holds the database's
/// advisory lock), and published by `sync`.
pub struct Writer {
    db: Arc<Db>,
    epoch: i64,
    pool: Pool,
    rt: tokio::runtime::Handle,
    tx: mpsc::Sender<Msg>,
    done: Arc<AtomicU64>,
    failed: Arc<Mutex<Option<String>>>,
    state: Mutex<WState>,
    last_reap: AtomicU64,
    /// The token of the attach whose main file this writer has open.
    main: Mutex<Option<i64>>,
}

impl Writer {
    async fn open(rt: tokio::runtime::Handle, pool: Pool, db: Arc<Db>) -> Result<Arc<Writer>> {
        let client = pool.dedicated().await?;
        // Point lookups: the pool's sessions scan chunks by range.
        client.batch_execute("SET enable_indexscan = on").await?;
        let locked: bool = client
            .query_typed_one(
                "SELECT pg_try_advisory_lock(hashtext('pgvfs.db'), $1)",
                &[(&db.db_id, Type::INT4)],
            )
            .await?
            .get(0);
        anyhow::ensure!(
            locked,
            "{} is attached for writing by another process",
            db.label
        );
        let row = client
            .query_typed_one(
                "WITH old AS (SELECT clean FROM pgvfs.dbs WHERE db_id = $1 FOR UPDATE) \
                 UPDATE pgvfs.dbs d SET epoch = d.epoch + 1, clean = false FROM old \
                 WHERE d.db_id = $1 RETURNING d.epoch, d.committed_gen, old.clean",
                &[(&db.db_id, Type::INT4)],
            )
            .await?;
        let (epoch, committed, was_clean): (i64, i64, bool) = (row.get(0), row.get(1), row.get(2));
        if !was_clean {
            // A dead writer's unpublished writes.
            client
                .execute_typed(
                    "DELETE FROM pgvfs.dbchunks c USING pgvfs.dbfileids i \
                     WHERE i.db_id = $1 AND c.file_id = i.file_id AND c.gen > $2",
                    &[(&db.db_id, Type::INT4), (&committed, Type::INT8)],
                )
                .await?;
            client
                .execute_typed(
                    "UPDATE pgvfs.dbfileids SET retired_gen = $2 WHERE db_id = $1 AND retired_gen IS NULL \
                     AND file_id NOT IN (SELECT file_id FROM (SELECT DISTINCT ON (path) file_id \
                       FROM pgvfs.dbfiles WHERE db_id = $1 AND gen <= $2 ORDER BY path, gen DESC) v \
                       WHERE file_id IS NOT NULL)",
                    &[(&db.db_id, Type::INT4), (&committed, Type::INT8)],
                )
                .await?;
        }
        let rows = client
            .query_typed(
                "SELECT path, file_id, size FROM (SELECT DISTINCT ON (path) path, file_id, size \
                 FROM pgvfs.dbfiles WHERE db_id = $1 AND gen <= $2 ORDER BY path, gen DESC) v \
                 WHERE file_id IS NOT NULL",
                &[(&db.db_id, Type::INT4), (&committed, Type::INT8)],
            )
            .await?;
        let files = rows
            .iter()
            .map(|r| {
                (
                    r.get::<_, String>(0),
                    FileState {
                        file_id: r.get(1),
                        size: r.get(2),
                    },
                )
            })
            .collect();
        let (tx, rx) = mpsc::channel(4);
        let done = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(Mutex::new(None));
        rt.spawn(run_writer(
            client,
            db.db_id,
            epoch,
            rx,
            done.clone(),
            failed.clone(),
        ));
        Ok(Arc::new(Writer {
            db,
            epoch,
            pool,
            rt,
            tx,
            done,
            failed,
            state: Mutex::new(WState {
                gen: committed + 1,
                files,
                ..WState::default()
            }),
            last_reap: AtomicU64::new(0),
            main: Mutex::new(None),
        }))
    }

    /// The attach `token`'s main file: one writer open per process.
    pub fn claim_main(&self, token: i64) -> Result<()> {
        let mut main = self.main.lock().unwrap();
        anyhow::ensure!(
            main.is_none(),
            "{} is already attached for writing in this process",
            self.db.label
        );
        *main = Some(token);
        Ok(())
    }

    pub fn release_main(&self) {
        *self.main.lock().unwrap() = None;
    }

    /// The writer's own open (a plain path or its attach), not a published version.
    pub fn owns(&self, gen: i64, token: i64) -> bool {
        gen < 0 || (token >= 0 && *self.main.lock().unwrap() == Some(token))
    }

    fn check(&self) -> Result<()> {
        match self.failed.lock().unwrap().as_deref() {
            Some(e) => bail!("{} writer failed: {e}", self.db.label),
            None => Ok(()),
        }
    }

    fn send(&self, msg: Msg) -> Result<()> {
        if self.tx.blocking_send(msg).is_err() {
            self.check()?;
            bail!("{} writer stopped", self.db.label);
        }
        Ok(())
    }

    fn call<T>(&self, make: impl FnOnce(Reply<T>) -> Msg) -> Result<T> {
        let (reply, rx) = oneshot::channel();
        self.send(make(reply))?;
        rx.blocking_recv()
            .map_err(|_| anyhow!("{} writer stopped", self.db.label))?
    }

    /// Forget staged chunks whose batch committed.
    fn prune(&self, st: &mut WState) {
        let done = self.done.load(Ordering::Acquire);
        if done != st.pruned {
            st.pruned = done;
            st.staged.retain(|_, s| s.seq == 0 || s.seq > done);
        }
    }

    pub fn exists(&self, path: &str) -> bool {
        self.state.lock().unwrap().files.contains_key(path)
    }

    pub fn size(&self, path: &str) -> Option<i64> {
        self.state.lock().unwrap().files.get(path).map(|f| f.size)
    }

    pub fn file_id(&self, path: &str) -> Option<i64> {
        self.state
            .lock()
            .unwrap()
            .files
            .get(path)
            .map(|f| f.file_id)
    }

    /// Create `path` if missing; `overwrite` replaces it.
    pub fn create(&self, path: &str, overwrite: bool) -> Result<()> {
        check_suffix(path)?;
        self.check()?;
        let mut st = self.state.lock().unwrap();
        if let Some(f) = st.files.get(path).copied() {
            if !overwrite {
                return Ok(());
            }
            retire(&mut st, f.file_id);
        }
        let file_id = self.call(Msg::Create)?;
        st.files.insert(path.into(), FileState { file_id, size: 0 });
        st.dirty_paths.insert(path.into());
        st.dirty = true;
        Ok(())
    }

    pub fn remove(&self, path: &str) -> Result<bool> {
        self.check()?;
        let mut st = self.state.lock().unwrap();
        let Some(f) = st.files.remove(path) else {
            return Ok(false);
        };
        retire(&mut st, f.file_id);
        st.dirty_paths.insert(path.into());
        st.dirty = true;
        Ok(true)
    }

    pub fn rename(&self, from: &str, to: &str) -> Result<()> {
        check_suffix(to)?;
        self.check()?;
        let mut st = self.state.lock().unwrap();
        let f = st
            .files
            .remove(from)
            .ok_or_else(|| anyhow!("{}{from}: no such file", self.db.label))?;
        if let Some(old) = st.files.insert(to.into(), f) {
            retire(&mut st, old.file_id);
        }
        st.dirty_paths.insert(from.into());
        st.dirty_paths.insert(to.into());
        st.dirty = true;
        Ok(())
    }

    pub fn write(&self, path: &str, pos: i64, data: &[u8]) -> Result<()> {
        self.check()?;
        if data.is_empty() {
            return Ok(());
        }
        anyhow::ensure!(pos >= 0, "negative write position");
        let mut st = self.state.lock().unwrap();
        let st = &mut *st;
        let f = *st
            .files
            .get(path)
            .ok_or_else(|| anyhow!("{}{path}: not open for writing", self.db.label))?;
        let end = pos + data.len() as i64;
        anyhow::ensure!(end / CHUNK < i32::MAX as i64, "pgdb file too large");
        self.prune(st);
        for k in pos / CHUNK..=(end - 1) / CHUNK {
            let key = (f.file_id, k);
            let a = pos.max(k * CHUNK);
            let b = end.min((k + 1) * CHUNK);
            if !matches!(st.staged.get(&key), Some(s) if s.seq == 0) {
                // Sent or never staged: stage a copy to write over.
                let mut s = Staged::new();
                if let Some(old) = st.staged.get(&key) {
                    s.data.copy_from_slice(&old.data);
                    s.cov = old.cov.clone();
                } else if let Some(c) = st.clean.get(&key) {
                    s.data[..c.len()].copy_from_slice(c);
                    s.cov.push((0, c.len() as u32));
                }
                st.staged.insert(key, s);
                st.unsent += 1;
            }
            let s = st.staged.get_mut(&key).unwrap();
            let (ca, cb) = ((a - k * CHUNK) as usize, (b - k * CHUNK) as usize);
            s.data[ca..cb].copy_from_slice(&data[(a - pos) as usize..(b - pos) as usize]);
            s.add(ca as u32, cb as u32);
            st.changed.entry(f.file_id).or_default().insert(k);
        }
        let file = st.files.get_mut(path).unwrap();
        file.size = file.size.max(end);
        st.dirty_paths.insert(path.into());
        st.dirty = true;
        debug_assert_eq!(st.unsent, st.staged.values().filter(|s| s.seq == 0).count());
        let unsent = st.unsent * CH;
        if unsent >= flush_bytes() {
            self.flush(st, unsent >= MAX_STAGED)?;
        }
        Ok(())
    }

    /// Send staged chunks: complete ones, or all with `partial`.
    fn flush(&self, st: &mut WState, partial: bool) -> Result<()> {
        let outs = take_outs(st, partial);
        if outs.is_empty() {
            return Ok(());
        }
        st.next_seq += 1;
        let seq = st.next_seq;
        mark_sent(st, &outs, seq);
        self.send(Msg::Batch {
            gen: st.gen,
            seq,
            chunks: outs,
        })
    }

    /// Send everything staged and wait for the commit.
    fn drain(&self, st: &mut WState) -> Result<()> {
        self.flush(st, true)?;
        self.call(Msg::Barrier)?;
        self.prune(st);
        Ok(())
    }

    pub fn read(&self, path: &str, pos: i64, buf: &mut [u8]) -> Result<()> {
        self.check()?;
        if buf.is_empty() {
            return Ok(());
        }
        let mut guard = self.state.lock().unwrap();
        let st = &mut *guard;
        let f = *st
            .files
            .get(path)
            .ok_or_else(|| anyhow!("{}{path}: no such file", self.db.label))?;
        let end = pos + buf.len() as i64;
        anyhow::ensure!(
            pos >= 0 && end <= f.size,
            "read [{pos}, {end}) outside {}{path} of {} bytes",
            self.db.label,
            f.size
        );
        self.prune(st);
        let (k0, k1) = (pos / CHUNK, (end - 1) / CHUNK);
        let mut all_staged = true;
        let mut partial = false;
        for k in k0..=k1 {
            match st.staged.get(&(f.file_id, k)) {
                Some(s) if s.covers(need(f.size, k)) => {}
                Some(_) => partial = true,
                None => all_staged = false,
            }
        }
        if partial {
            self.drain(st)?;
            all_staged = false;
        }
        // Copy staged chunks under the lock, read the rest without it.
        let mut overlay = Vec::new();
        for k in k0..=k1 {
            if let Some(s) = st.staged.get(&(f.file_id, k)) {
                let a = pos.max(k * CHUNK);
                let b = end.min((k + 1) * CHUNK);
                overlay.push((
                    (a - pos) as usize,
                    s.data[(a - k * CHUNK) as usize..(b - k * CHUNK) as usize].to_vec(),
                ));
            }
        }
        let gen = st.gen;
        drop(guard);
        if !all_staged {
            self.rt.block_on(read_versions(
                &self.pool,
                self.db.db_id,
                f.file_id,
                gen,
                gen,
                pos,
                buf,
            ))?;
        }
        for (at, bytes) in overlay {
            buf[at..at + bytes.len()].copy_from_slice(&bytes);
        }
        Ok(())
    }

    pub fn truncate(&self, path: &str, size: i64) -> Result<()> {
        self.check()?;
        anyhow::ensure!(size >= 0, "negative size");
        let mut st = self.state.lock().unwrap();
        let st = &mut *st;
        let f = *st
            .files
            .get(path)
            .ok_or_else(|| anyhow!("{}{path}: no such file", self.db.label))?;
        self.drain(st)?;
        if size < f.size {
            let gen = st.gen;
            self.call(|reply| Msg::Truncate {
                gen,
                file_id: f.file_id,
                size,
                reply,
            })?;
            st.clean.retain(|k, _| k.0 != f.file_id);
            st.staged.retain(|k, _| k.0 != f.file_id);
            recount(st);
            st.truncs.push((f.file_id, size / CHUNK));
        }
        st.files.get_mut(path).unwrap().size = size;
        st.dirty_paths.insert(path.into());
        st.dirty = true;
        Ok(())
    }

    /// Publish everything written since the last publish as the next gen.
    pub fn sync(&self) -> Result<()> {
        self.check()?;
        let mut st = self.state.lock().unwrap();
        self.publish(&mut st)
    }

    fn publish(&self, st: &mut WState) -> Result<()> {
        if !st.dirty {
            return Ok(());
        }
        self.prune(st);
        // Partials are merged by the task; whole ones ride in the publish.
        let any_partial = st
            .staged
            .iter()
            .any(|(k, s)| s.seq == 0 && !s.covers(need(size_of(st, k.0), k.1)));
        if any_partial {
            self.flush(st, true)?;
        }
        let chunks = take_outs(st, false);
        st.next_seq += 1;
        let seq = st.next_seq;
        mark_sent(st, &chunks, seq);
        let mut msg = PublishMsg {
            gen: st.gen,
            chunks,
            paths: Vec::new(),
            file_ids: Vec::new(),
            sizes: Vec::new(),
            retired: std::mem::take(&mut st.retired),
            changes: Vec::new(),
        };
        for p in &st.dirty_paths {
            let f = st.files.get(p);
            msg.paths.push(p.clone());
            msg.file_ids.push(f.map(|f| f.file_id));
            msg.sizes.push(f.map(|f| f.size));
        }
        for (&file_id, set) in &st.changed {
            let mut it = set.iter().copied();
            let Some(mut lo) = it.next() else { continue };
            let mut hi = lo + 1;
            for k in it {
                if k != hi {
                    msg.changes.extend([file_id, lo, hi]);
                    lo = k;
                }
                hi = k + 1;
            }
            msg.changes.extend([file_id, lo, hi]);
        }
        for &(file_id, first) in &st.truncs {
            msg.changes.extend([file_id, first, i32::MAX as i64]);
        }
        let result = self.call(|reply| Msg::Publish {
            msg: Box::new(msg),
            seq,
            reply,
        });
        if let Err(e) = result {
            *self.failed.lock().unwrap() = Some(format!("{e:#}"));
            return Err(e);
        }
        st.gen += 1;
        st.dirty = false;
        st.dirty_paths.clear();
        st.changed.clear();
        st.truncs.clear();
        st.sent.clear();
        self.prune(st);
        store::STATS.db_publishes.fetch_add(1, Ordering::Relaxed);
        self.maybe_reap();
        Ok(())
    }

    /// Publish what is left, mark the database clean, close the connection.
    fn finish(&self) -> Result<()> {
        let result = self
            .check()
            .and_then(|_| self.publish(&mut self.state.lock().unwrap()));
        let clean = result.is_ok();
        let closed = self.call(|reply| Msg::Close { clean, reply });
        result.and(closed)
    }

    /// Reap superseded versions in the background, at most once a minute.
    fn maybe_reap(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let last = self.last_reap.load(Ordering::Relaxed);
        if now < last + 60
            || self
                .last_reap
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        let (pool, db_id) = (self.pool.clone(), self.db.db_id);
        self.rt.spawn(async move {
            if let Err(e) = reap(&pool, db_id, store::reap_grace_seconds()).await {
                eprintln!("pgvfs: reaping pgdb database {db_id} failed: {e:#}");
            }
        });
    }

    pub fn epoch(&self) -> i64 {
        self.epoch
    }
}

fn need(size: i64, k: i64) -> usize {
    (size - k * CHUNK).clamp(0, CHUNK) as usize
}

fn size_of(st: &WState, file_id: i64) -> i64 {
    st.files
        .values()
        .find(|f| f.file_id == file_id)
        .map_or(0, |f| f.size)
}

fn recount(st: &mut WState) {
    st.unsent = st.staged.values().filter(|s| s.seq == 0).count();
}

/// Drop a file from the writer's state; its rows are reaped after the gen
/// that retires it.
fn retire(st: &mut WState, file_id: i64) {
    st.retired.push(file_id);
    st.staged.retain(|k, s| k.0 != file_id || s.seq != 0);
    recount(st);
    st.clean.retain(|k, _| k.0 != file_id);
    st.changed.remove(&file_id);
}

/// The unsent staged chunks to send: complete ones, or all with `partial`.
fn take_outs(st: &mut WState, partial: bool) -> Vec<Out> {
    let sizes: HashMap<i64, i64> = st.files.values().map(|f| (f.file_id, f.size)).collect();
    let mut keys: Vec<(i64, i64)> = Vec::new();
    let mut gone: Vec<(i64, i64)> = Vec::new();
    for (k, s) in &st.staged {
        if s.seq != 0 {
            continue;
        }
        let n = sizes.get(&k.0).map_or(0, |&size| need(size, k.1));
        if n == 0 {
            gone.push(*k); // beyond the end, or the file is gone
        } else if partial || s.covers(n) {
            keys.push(*k);
        }
    }
    st.unsent -= gone.len();
    for k in gone {
        st.staged.remove(&k);
    }
    keys.sort_unstable();
    keys.into_iter()
        .map(|k| {
            let s = &st.staged[&k];
            let n = need(sizes[&k.0], k.1);
            Out {
                file_id: k.0,
                no: k.1 as i32,
                data: s.data[..n].to_vec(),
                cov: (!s.covers(n)).then(|| s.cov.clone()),
                // A truncation tombstoned chunks past its end.
                fresh: !st.sent.contains(&k)
                    && !st.truncs.iter().any(|&(f, first)| f == k.0 && k.1 >= first),
            }
        })
        .collect()
}

/// Mark `outs` as sent in batch `seq`, and remember each file's last whole
/// chunk (its tail, which the next append writes into).
fn mark_sent(st: &mut WState, outs: &[Out], seq: u64) {
    let mut tails: HashMap<i64, usize> = HashMap::new();
    st.unsent -= outs.len();
    for (i, o) in outs.iter().enumerate() {
        st.staged.get_mut(&(o.file_id, o.no as i64)).unwrap().seq = seq;
        st.sent.insert((o.file_id, o.no as i64));
        st.clean.remove(&(o.file_id, o.no as i64));
        if o.cov.is_none() {
            tails.insert(o.file_id, i); // outs are sorted: the last wins
        }
    }
    if st.clean.len() > 1024 {
        st.clean.clear();
    }
    for (_, i) in tails {
        let o = &outs[i];
        st.clean.insert((o.file_id, o.no as i64), o.data.clone());
    }
}

/// The writer's connection: runs messages in order. After a failure every
/// later message fails (and nothing more is written).
async fn run_writer(
    client: Dedicated,
    db_id: i32,
    epoch: i64,
    mut rx: mpsc::Receiver<Msg>,
    done: Arc<AtomicU64>,
    failed: Arc<Mutex<Option<String>>>,
) {
    let w = Task {
        client: &client,
        db_id,
        epoch,
    };
    while let Some(msg) = rx.recv().await {
        let broken = failed.lock().unwrap().clone();
        let fail = |e: &anyhow::Error| {
            let mut f = failed.lock().unwrap();
            if f.is_none() {
                *f = Some(format!("{e:#}"));
            }
        };
        macro_rules! run {
            ($reply:expr, $op:expr) => {{
                let result = match &broken {
                    Some(e) => Err(anyhow!("writer failed earlier: {e}")),
                    None => $op.await,
                };
                if let Err(e) = &result {
                    fail(e);
                }
                let _ = $reply.send(result);
            }};
        }
        match msg {
            Msg::Batch { gen, seq, chunks } => {
                if broken.is_none() {
                    match w.insert(gen, chunks).await {
                        Ok(()) => done.store(seq, Ordering::Release),
                        Err(e) => fail(&e),
                    }
                }
            }
            Msg::Create(reply) => run!(reply, w.create()),
            Msg::Truncate {
                gen,
                file_id,
                size,
                reply,
            } => run!(reply, w.truncate(gen, file_id, size)),
            Msg::Publish { msg, seq, reply } => {
                let result = match &broken {
                    Some(e) => Err(anyhow!("writer failed earlier: {e}")),
                    None => w.publish(*msg).await,
                };
                match &result {
                    Ok(()) => done.store(seq, Ordering::Release),
                    Err(e) => fail(e),
                }
                let _ = reply.send(result);
            }
            Msg::Barrier(reply) => run!(reply, async { Ok::<(), anyhow::Error>(()) }),
            Msg::Close { clean, reply } => {
                let result = if clean && broken.is_none() {
                    w.client
                        .execute_typed(
                            "UPDATE pgvfs.dbs SET clean = true WHERE db_id = $1 AND epoch = $2",
                            &[(&db_id, Type::INT4), (&epoch, Type::INT8)],
                        )
                        .await
                        .map(|_| ())
                        .map_err(Into::into)
                } else {
                    Ok(())
                };
                let _ = reply.send(result);
                break; // dropping the client closes the connection and its lock
            }
        }
    }
}

struct Task<'a> {
    client: &'a Dedicated,
    db_id: i32,
    epoch: i64,
}

impl Task<'_> {
    fn fenced(&self) -> anyhow::Error {
        anyhow!(
            "pgdb database {} was attached for writing by another process; this writer is fenced off",
            self.db_id
        )
    }

    /// Merge partial chunks into their newest versions.
    async fn complete(&self, gen: i64, chunks: &mut [Out]) -> Result<()> {
        let (ids, nos): (Vec<i64>, Vec<i32>) = chunks
            .iter()
            .filter(|c| c.cov.is_some())
            .map(|c| (c.file_id, c.no))
            .unzip();
        if ids.is_empty() {
            return Ok(());
        }
        let rows = self
            .client
            .query_typed(
                "SELECT DISTINCT ON (c.file_id, c.no) c.file_id, c.no, c.data FROM pgvfs.dbchunks c \
                 JOIN unnest($1::int8[], $2::int4[]) AS u(f, n) ON c.file_id = u.f AND c.no = u.n \
                 WHERE c.gen <= $3 ORDER BY c.file_id, c.no, c.gen DESC",
                &[
                    (&ids, Type::INT8_ARRAY),
                    (&nos, Type::INT4_ARRAY),
                    (&gen, Type::INT8),
                ],
            )
            .await?;
        let base: HashMap<(i64, i32), Vec<u8>> = rows
            .into_iter()
            .map(|r| ((r.get(0), r.get(1)), r.get(2)))
            .collect();
        for c in chunks.iter_mut() {
            let Some(cov) = c.cov.take() else { continue };
            let mut merged = base.get(&(c.file_id, c.no)).cloned().unwrap_or_default();
            merged.resize(c.data.len(), 0);
            for (a, b) in cov {
                let b = (b as usize).min(merged.len());
                let a = (a as usize).min(b);
                merged[a..b].copy_from_slice(&c.data[a..b]);
            }
            c.data = merged;
        }
        Ok(())
    }

    async fn insert(&self, gen: i64, mut chunks: Vec<Out>) -> Result<()> {
        self.complete(gen, &mut chunks).await?;
        let (fresh, again): (Vec<Out>, Vec<Out>) = chunks.into_iter().partition(|c| c.fresh);
        if !again.is_empty() {
            self.upsert(gen, &again).await?;
        }
        if !fresh.is_empty() {
            self.copy(gen, &fresh).await?;
        }
        Ok(())
    }

    /// Chunks that already have a row in this gen.
    async fn upsert(&self, gen: i64, chunks: &[Out]) -> Result<()> {
        let ids: Vec<i64> = chunks.iter().map(|c| c.file_id).collect();
        let nos: Vec<i32> = chunks.iter().map(|c| c.no).collect();
        let data: Vec<&[u8]> = chunks.iter().map(|c| c.data.as_slice()).collect();
        let n = self
            .client
            .execute_typed(
                &format!(
                    "{FENCE}INSERT INTO pgvfs.dbchunks (file_id, gen, no, data) \
                     SELECT u.f, $3, u.n, u.d FROM ok, unnest($4::int8[], $5::int4[], $6::bytea[]) AS u(f, n, d)\
                     {UPSERT}"
                ),
                &[
                    (&self.db_id, Type::INT4),
                    (&self.epoch, Type::INT8),
                    (&gen, Type::INT8),
                    (&ids, Type::INT8_ARRAY),
                    (&nos, Type::INT4_ARRAY),
                    (&data, Type::BYTEA_ARRAY),
                ],
            )
            .await?;
        if n != chunks.len() as u64 {
            return Err(self.fenced());
        }
        Ok(())
    }

    /// Chunks new to this gen: COPY under a FOR SHARE fence (see publish).
    async fn copy(&self, gen: i64, chunks: &[Out]) -> Result<()> {
        let result = async {
            let fence = self
                .client
                .simple_query(&format!(
                    "BEGIN; SELECT 1 FROM pgvfs.dbs WHERE db_id = {} AND epoch = {} FOR SHARE",
                    self.db_id, self.epoch
                ))
                .await?;
            if !fence
                .iter()
                .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            {
                return Err(self.fenced());
            }
            let sink = self
                .client
                .copy_in::<_, Bytes>(
                    "COPY pgvfs.dbchunks (file_id, gen, no, data) FROM STDIN WITH (FORMAT binary)",
                )
                .await?;
            futures::pin_mut!(sink);
            let mut frame = BytesMut::with_capacity(
                COPY_HEADER.len() + chunks.iter().map(|c| c.data.len() + 40).sum::<usize>(),
            );
            frame.put_slice(COPY_HEADER);
            for c in chunks {
                frame.put_i16(4);
                frame.put_i32(8);
                frame.put_i64(c.file_id);
                frame.put_i32(8);
                frame.put_i64(gen);
                frame.put_i32(4);
                frame.put_i32(c.no);
                frame.put_i32(c.data.len() as i32);
                frame.put_slice(&c.data);
            }
            frame.put_slice(&[0xFF, 0xFF]);
            sink.send(frame.freeze()).await?;
            sink.as_mut().finish().await?;
            self.client.batch_execute("COMMIT").await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = self.client.batch_execute("ROLLBACK").await;
        }
        result
    }

    async fn create(&self) -> Result<i64> {
        let row = self
            .client
            .query_typed_opt(
                &format!(
                    "{FENCE}INSERT INTO pgvfs.dbfileids (file_id, db_id) \
                     SELECT nextval('pgvfs.file_ids'), $1 FROM ok RETURNING file_id"
                ),
                &[(&self.db_id, Type::INT4), (&self.epoch, Type::INT8)],
            )
            .await?;
        row.map(|r| r.get(0)).ok_or_else(|| self.fenced())
    }

    /// Tombstone every chunk past `size` and cut the one it ends in.
    async fn truncate(&self, gen: i64, file_id: i64, size: i64) -> Result<()> {
        let keep = (size + CHUNK - 1) / CHUNK;
        let params = |extra: &'static str| {
            format!("{FENCE}INSERT INTO pgvfs.dbchunks (file_id, gen, no, data) {extra}{UPSERT}")
        };
        let fence: Option<tokio_postgres::Row> = self
            .client
            .query_typed_opt(
                "SELECT 1 FROM pgvfs.dbs WHERE db_id = $1 AND epoch = $2",
                &[(&self.db_id, Type::INT4), (&self.epoch, Type::INT8)],
            )
            .await?;
        if fence.is_none() {
            return Err(self.fenced());
        }
        self.client
            .execute_typed(
                &params(
                    "SELECT $3, $4, e.no, ''::bytea FROM ok, \
                     (SELECT DISTINCT no FROM pgvfs.dbchunks WHERE file_id = $3 AND no >= $5) e",
                ),
                &[
                    (&self.db_id, Type::INT4),
                    (&self.epoch, Type::INT8),
                    (&file_id, Type::INT8),
                    (&gen, Type::INT8),
                    (&(keep as i32), Type::INT4),
                ],
            )
            .await?;
        if size % CHUNK != 0 {
            let (no, len) = ((size / CHUNK) as i32, (size % CHUNK) as i32);
            self.client
                .execute_typed(
                    &params(
                        "SELECT $3, $4, $5, substring(s.data from 1 for $6) FROM ok, \
                         (SELECT data FROM pgvfs.dbchunks WHERE file_id = $3 AND no = $5 AND gen <= $4 \
                          ORDER BY gen DESC LIMIT 1) s",
                    ),
                    &[
                        (&self.db_id, Type::INT4),
                        (&self.epoch, Type::INT8),
                        (&file_id, Type::INT8),
                        (&gen, Type::INT8),
                        (&no, Type::INT4),
                        (&len, Type::INT4),
                    ],
                )
                .await?;
        }
        Ok(())
    }

    /// The gen, its last chunks, files, retirements and change list: one
    /// statement, so all of it or none.
    async fn publish(&self, mut m: PublishMsg) -> Result<()> {
        self.complete(m.gen, &mut m.chunks).await?;
        let ids: Vec<i64> = m.chunks.iter().map(|c| c.file_id).collect();
        let nos: Vec<i32> = m.chunks.iter().map(|c| c.no).collect();
        let data: Vec<&[u8]> = m.chunks.iter().map(|c| c.data.as_slice()).collect();
        let row = self
            .client
            .query_typed_one(
                &format!(
                    "WITH d AS (UPDATE pgvfs.dbs SET committed_gen = $2 \
                       WHERE db_id = $1 AND epoch = $3 RETURNING db_id), \
                     c AS (INSERT INTO pgvfs.dbchunks (file_id, gen, no, data) \
                       SELECT u.f, $2, u.n, u.x FROM d, unnest($4::int8[], $5::int4[], $6::bytea[]) AS u(f, n, x)\
                       {UPSERT}), \
                     f AS (INSERT INTO pgvfs.dbfiles (db_id, path, gen, file_id, size) \
                       SELECT d.db_id, u.p, $2, u.i, u.s FROM d, unnest($7::text[], $8::int8[], $9::int8[]) AS u(p, i, s)), \
                     r AS (UPDATE pgvfs.dbfileids SET retired_gen = $2 \
                       WHERE file_id = ANY($10::int8[]) AND EXISTS (SELECT 1 FROM d)), \
                     g AS (INSERT INTO pgvfs.dbgens (db_id, gen, changes) SELECT d.db_id, $2, $11 FROM d) \
                     SELECT count(*) FROM d"
                ),
                &[
                    (&self.db_id, Type::INT4),
                    (&m.gen, Type::INT8),
                    (&self.epoch, Type::INT8),
                    (&ids, Type::INT8_ARRAY),
                    (&nos, Type::INT4_ARRAY),
                    (&data, Type::BYTEA_ARRAY),
                    (&m.paths, Type::TEXT_ARRAY),
                    (&m.file_ids, Type::INT8_ARRAY),
                    (&m.sizes, Type::INT8_ARRAY),
                    (&m.retired, Type::INT8_ARRAY),
                    (&m.changes, Type::INT8_ARRAY),
                ],
            )
            .await?;
        if row.get::<_, i64>(0) != 1 {
            return Err(self.fenced());
        }
        Ok(())
    }
}

/// Reap a database's versions superseded at or below its horizon: the newest
/// gen published more than `grace` seconds ago. The horizon is published
/// before any row goes, and every read checks it in the same statement.
/// Bounded work per call. Returns the chunk rows deleted.
pub async fn reap(pool: &Pool, db_id: i32, grace: f64) -> Result<u64> {
    let mut conn = pool.get().await?;
    let horizon: Option<i64> = conn
        .query_typed_one(
            "SELECT max(gen) FROM pgvfs.dbgens WHERE db_id = $1 \
             AND committed_at < now() - make_interval(secs => $2)",
            &[(&db_id, Type::INT4), (&grace, Type::FLOAT8)],
        )
        .await?
        .get(0);
    let Some(h) = horizon else { return Ok(0) };
    conn.execute_typed(
        "UPDATE pgvfs.dbs SET reap_horizon = $2 WHERE db_id = $1 AND reap_horizon < $2",
        &[(&db_id, Type::INT4), (&h, Type::INT8)],
    )
    .await?;
    let mut through: i64 = conn
        .query_typed_one(
            "SELECT reaped_through FROM pgvfs.dbs WHERE db_id = $1",
            &[(&db_id, Type::INT4)],
        )
        .await?
        .get(0);
    let mut deleted = 0;
    // Each gen's change list names the chunks it superseded: one delete per
    // batch of gens. Bounded per call; a writer reaps once a minute.
    for _ in 0..50 {
        let gens = conn
            .query_typed(
                "SELECT gen, changes FROM pgvfs.dbgens WHERE db_id = $1 AND gen > $2 AND gen <= $3 \
                 ORDER BY gen LIMIT 2000",
                &[(&db_id, Type::INT4), (&through, Type::INT8), (&h, Type::INT8)],
            )
            .await?;
        let Some(last) = gens.last().map(|r| r.get::<_, i64>(0)) else {
            break;
        };
        let (mut fs, mut los, mut his, mut gs) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for row in &gens {
            let g: i64 = row.get(0);
            let changes: Vec<i64> = row.get(1);
            for t in changes.as_chunks::<3>().0 {
                fs.push(t[0]);
                los.push(t[1]);
                his.push(t[2]);
                gs.push(g);
            }
        }
        let tx = conn.transaction().await?;
        // Index range scans per change (the pool's sessions turn them off).
        tx.batch_execute("SET LOCAL enable_indexscan = on").await?;
        // Versions older than a change, and a tombstone once nothing older is left.
        deleted += tx
            .execute(
                "DELETE FROM pgvfs.dbchunks c \
                 USING unnest($1::int8[], $2::int8[], $3::int8[], $4::int8[]) AS u(f, lo, hi, g) \
                 WHERE c.file_id = u.f AND c.no >= u.lo AND c.no < u.hi \
                 AND (c.gen < u.g OR (c.gen = u.g AND octet_length(c.data) = 0))",
                &[&fs, &los, &his, &gs],
            )
            .await?;
        tx.execute(
            "UPDATE pgvfs.dbs SET reaped_through = $2 WHERE db_id = $1",
            &[&db_id, &last],
        )
        .await?;
        tx.execute(
            "DELETE FROM pgvfs.dbgens WHERE db_id = $1 AND gen <= $2",
            &[&db_id, &last],
        )
        .await?;
        tx.commit().await?;
        through = last;
        if gens.len() < 2000 {
            break;
        }
    }
    let tx = conn.transaction().await?;
    let retired = tx
        .query(
            "SELECT file_id FROM pgvfs.dbfileids WHERE db_id = $1 AND retired_gen <= $2 LIMIT 100",
            &[&db_id, &h],
        )
        .await?;
    for row in &retired {
        let file_id: i64 = row.get(0);
        deleted += tx
            .execute("DELETE FROM pgvfs.dbchunks WHERE file_id = $1", &[&file_id])
            .await?;
        tx.execute(
            "DELETE FROM pgvfs.dbfileids WHERE file_id = $1",
            &[&file_id],
        )
        .await?;
    }
    tx.execute(
        "DELETE FROM pgvfs.dbfiles f WHERE f.db_id = $1 AND f.gen < $2 AND EXISTS \
         (SELECT 1 FROM pgvfs.dbfiles n WHERE n.db_id = f.db_id AND n.path = f.path \
          AND n.gen > f.gen AND n.gen <= $2)",
        &[&db_id, &h],
    )
    .await?;
    tx.execute(
        "DELETE FROM pgvfs.dbfiles WHERE db_id = $1 AND gen <= $2 AND file_id IS NULL",
        &[&db_id, &h],
    )
    .await?;
    tx.commit().await?;
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readahead_widens_only_a_continuation() {
        let key = (1, 7);
        let mut ra = Readahead::default();
        let bytes = ra.bytes();
        assert!(matches!(
            ra.plan(key, 0, 100),
            Plan::Fetch {
                len: 100,
                widen: false
            }
        ));
        ra.done(key, 0, 100);
        assert!(matches!(
            ra.plan(key, 100, 100),
            Plan::Fetch { len, widen: true } if len == 100 + bytes
        ));
        ra.put(key, 100, vec![7u8; (100 + bytes) as usize].into());
        assert!(ra.held == 1);
        ra.done(key, 100, 100);
        assert!(matches!(
            ra.plan(key, 150, 100),
            Plan::Hit { skip: 50, index: 0 }
        ));
        assert!(matches!(
            ra.plan(key, 100 + bytes + 5000, 100),
            Plan::Fetch {
                len: 100,
                widen: false
            }
        ));
    }

    /// The key gen of chunk `k` at `gen`, by brute force over the history.
    fn model(history: &[(i64, i64, i64, i64)], base: i64, k: i64, gen: i64) -> i64 {
        history
            .iter()
            .filter(|&&(_, lo, hi, g)| lo <= k && k < hi && g <= gen)
            .map(|&(_, _, _, g)| g)
            .max()
            .unwrap_or(base)
    }

    #[test]
    fn change_map_keys_each_chunk_by_its_last_write() {
        let mut seed = 7u64;
        let mut rand = |n: i64| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((seed >> 33) % n as u64) as i64
        };
        let base = 10;
        let mut m = ChangeMap::new();
        m.reset(base);
        let mut history = Vec::new();
        for g in base + 1..base + 200 {
            for _ in 0..rand(4) {
                let lo = rand(300);
                let hi = lo + 1 + rand(40);
                m.insert(1, lo, hi, g);
                history.push((1, lo, hi, g));
            }
            m.upto = g;
        }
        for _ in 0..500 {
            let gen = base + rand(200);
            let k0 = rand(350);
            let k1 = k0 + rand(60);
            let runs = m.runs(1, k0, k1, gen).unwrap();
            let mut k = k0;
            for &(a, b, key) in &runs {
                assert_eq!(a, k, "runs are contiguous");
                assert!(b > a);
                for c in a..b {
                    assert_eq!(key, model(&history, base, c, gen), "chunk {c} at gen {gen}");
                }
                k = b;
            }
            assert_eq!(k, k1 + 1);
            for w in runs.windows(2) {
                assert_ne!(w[0].2, w[1].2, "equal neighbours merge");
            }
        }
        assert!(m.runs(1, 0, 1, base - 1).is_none());
        assert!(m.runs(1, 0, 1, m.upto + 1).is_none());
        assert_eq!(m.runs(2, 0, 9, base + 5).unwrap(), vec![(0, 10, base)]);
    }

    #[test]
    fn staged_coverage_merges_ranges() {
        let mut s = Staged::new();
        s.add(10, 20);
        s.add(30, 40);
        assert!(!s.covers(40));
        s.add(0, 10);
        s.add(20, 30);
        assert_eq!(s.cov, vec![(0, 40)]);
        assert!(s.covers(40) && !s.covers(41) && s.covers(0));
        s.add(35, 50);
        assert_eq!(s.cov, vec![(0, 50)]);
    }
}
