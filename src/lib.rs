//! C ABI over the pgvfs storage layer, for the DuckDB `pgvfs://` filesystem
//! (extension/). The C++ side only registers the scheme with DuckDB, whose
//! FileSystem registration is not in the stable C API: storage is all here.
//!
//! Connecting needs only read access. The first write operation makes the
//! database writable for this process (`store::ensure_writable` installs or
//! refreshes the layout and functions under a transaction lock); writes then
//! need only the privileges PostgreSQL grants.
//!
//! Calls block the calling DuckDB thread on this connection's tokio runtime.
//!
//! Strings in are UTF-8 (rejected otherwise); errors come back as `*err`,
//! freed with `pgvfs_free_str`.
//!
//! Safety (every function): handles are those this library returned and not
//! yet freed; strings are NUL-terminated; buffers are valid for `len` bytes. A
//! connection outlives its writers and is used from any thread.
#![allow(clippy::missing_safety_doc)]

pub mod db;
pub mod pg;
pub mod store;

/// Search splits: their C ABI (`tantivy_*`) ships in this staticlib.
pub use duckdb_tantivy;

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use bytes::BytesMut;
use pg::Pool;
use tokio::sync::mpsc;

use store::{FileInfo, WriteMsg, WRITE_BATCH};

pub struct PgvfsConn {
    rt: tokio::runtime::Runtime,
    pool: Pool,
    /// Whether this process has made the database writable (see `writable`).
    writable: Mutex<bool>,
    /// Whether this process has installed the pgdb tables (see `db_writable`).
    db_writable: Mutex<bool>,
    last_reap: AtomicU64,
    files: FileCache,
    dbs: db::Dbs,
}

/// (volume, path) -> file, for `open`. DuckDB re-opens files to check them,
/// e.g. DuckLake delete files on every query, each a round trip. A path's
/// file changes only when it is rewritten: this process's own writes, renames
/// and removes drop the entry; another process's show up within the TTL
/// (PGVFS_OPEN_CACHE_S, default 10, 0 off). DuckLake never rewrites a path.
struct FileCache {
    ttl: std::time::Duration,
    map: Mutex<std::collections::HashMap<(String, String), (FileInfo, std::time::Instant)>>,
}

impl FileCache {
    fn get(&self, volume: &str, path: &str) -> Option<FileInfo> {
        let map = self.map.lock().unwrap();
        map.get(&(volume.to_owned(), path.to_owned()))
            .filter(|(_, at)| at.elapsed() < self.ttl)
            .map(|(f, _)| *f)
    }

    fn put(&self, volume: &str, path: &str, file: FileInfo) {
        if self.ttl.is_zero() {
            return;
        }
        let mut map = self.map.lock().unwrap();
        if map.len() >= 100_000 {
            let ttl = self.ttl;
            map.retain(|_, (_, at)| at.elapsed() < ttl);
        }
        map.insert(
            (volume.to_owned(), path.to_owned()),
            (file, std::time::Instant::now()),
        );
    }

    fn forget(&self, volume: &str, path: &str) {
        self.map
            .lock()
            .unwrap()
            .remove(&(volume.to_owned(), path.to_owned()));
    }
}

impl PgvfsConn {
    /// Makes the database writable, once per connection, before any write.
    fn writable(&self) -> Result<()> {
        let mut done = self.writable.lock().unwrap();
        if !*done {
            self.rt.block_on(store::ensure_writable(&self.pool))?;
            *done = true;
            // A writer catches up on garbage left while none was writing.
            self.maybe_reap();
        }
        Ok(())
    }

    /// Installs the pgdb tables, once per connection, before a database writer.
    fn db_writable(&self) -> Result<()> {
        let mut done = self.db_writable.lock().unwrap();
        if !*done {
            self.rt.block_on(db::ensure_schema(&self.pool))?;
            *done = true;
        }
        Ok(())
    }

    /// Reap past-grace garbage in the background, at most once a minute.
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
        let pool = self.pool.clone();
        self.rt.spawn(async move {
            if let Err(e) = store::reap(&pool).await {
                eprintln!("pgvfs: garbage reap failed: {e:#}");
            }
        });
    }
}

pub struct PgvfsWriter {
    conn: *const PgvfsConn,
    volume: String,
    path: String,
    tx: Option<mpsc::Sender<WriteMsg>>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
    buf: BytesMut,
}

#[repr(C)]
pub struct PgvfsFile {
    pub file_id: i64,
    pub size: i64,
    /// Microseconds since the Unix epoch.
    pub created_us: i64,
}

fn set_err(err: *mut *mut c_char, e: &anyhow::Error) {
    if err.is_null() {
        return;
    }
    let msg = format!("{e:#}").replace('\0', " ");
    unsafe { *err = CString::new(msg).unwrap_or_default().into_raw() };
}

fn text<'a>(p: *const c_char) -> Result<&'a str> {
    if p.is_null() {
        return Err(anyhow!("null string"));
    }
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map_err(|_| anyhow!("pgvfs paths must be UTF-8"))
}

fn conn<'a>(c: *const PgvfsConn) -> &'a PgvfsConn {
    unsafe { &*c }
}

/// Tokio workers drive every pooled connection's protocol I/O and the COPY
/// writers: one per DuckDB thread (PGVFS_IO_THREADS overrides). Not one per
/// core: hosts running several DuckDB readers would oversubscribe.
fn env_num(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n: &i64| n >= 0)
        .unwrap_or(default)
}

fn io_threads(threads: usize) -> usize {
    std::env::var("PGVFS_IO_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(threads)
}

/// Connect and verify the layout (read access suffices), sized for a DuckDB
/// with `threads` threads (<= 0: one per core). NULL + `*err` on failure.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_connect(
    url: *const c_char,
    threads: i64,
    err: *mut *mut c_char,
) -> *mut PgvfsConn {
    let run = || -> Result<PgvfsConn> {
        let url = text(url)?;
        let threads = if threads > 0 {
            threads as usize
        } else {
            store::default_threads()
        };
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(io_threads(threads))
            .thread_name("pgvfs")
            .enable_all()
            .build()?;
        let pool = rt.block_on(async {
            let pool = store::connect(url, threads).await?;
            store::verify(&pool).await?;
            Ok::<_, anyhow::Error>(pool)
        })?;
        Ok(PgvfsConn {
            rt,
            pool,
            writable: Mutex::new(false),
            db_writable: Mutex::new(false),
            last_reap: AtomicU64::new(0),
            files: FileCache {
                ttl: std::time::Duration::from_secs(env_num("PGVFS_OPEN_CACHE_S", 10) as u64),
                map: Mutex::default(),
            },
            dbs: db::Dbs::default(),
        })
    };
    match run() {
        Ok(c) => Box::into_raw(Box::new(c)),
        Err(e) => {
            set_err(err, &e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn pgvfs_disconnect(c: *mut PgvfsConn) {
    if !c.is_null() {
        let c = unsafe { Box::from_raw(c) };
        c.rt.shutdown_background();
    }
}

#[no_mangle]
pub unsafe extern "C" fn pgvfs_free_str(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

/// 0 found (fills `out`), 1 not found, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_open(
    c: *const PgvfsConn,
    volume: *const c_char,
    path: *const c_char,
    out: *mut PgvfsFile,
    err: *mut *mut c_char,
) -> c_int {
    let c = conn(c);
    let run = || -> Result<Option<FileInfo>> {
        let (volume, path) = (text(volume)?, text(path)?);
        store::check_volume(volume)?;
        if let Some(f) = c.files.get(volume, path) {
            store::STATS.open_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(f));
        }
        let found = c.rt.block_on(store::open(&c.pool, volume, path))?;
        if let Some(f) = found {
            c.files.put(volume, path, f);
        }
        Ok(found)
    };
    let t0 = std::time::Instant::now();
    let result = run();
    store::STATS.opens.fetch_add(1, Ordering::Relaxed);
    store::STATS
        .open_ns
        .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    match result {
        Ok(Some(f)) => {
            unsafe {
                *out = PgvfsFile {
                    file_id: f.file_id,
                    size: f.size,
                    created_us: store::micros(f.created_at),
                }
            };
            0
        }
        Ok(None) => 1,
        Err(e) => {
            set_err(err, &e);
            -1
        }
    }
}

/// Fill exactly `len` bytes from `pos`; the range must lie inside the file.
/// 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_read(
    c: *const PgvfsConn,
    file: *const PgvfsFile,
    buf: *mut u8,
    len: i64,
    pos: i64,
    err: *mut *mut c_char,
) -> c_int {
    if len <= 0 {
        return 0;
    }
    let c = conn(c);
    let f = unsafe { &*file };
    let info = FileInfo {
        file_id: f.file_id,
        size: f.size,
        created_at: UNIX_EPOCH,
    };
    let out = unsafe { std::slice::from_raw_parts_mut(buf, len as usize) };
    let t0 = std::time::Instant::now();
    let result = c.rt.block_on(store::read_at(&c.pool, &info, pos, out));
    let stats = &store::STATS;
    stats.reads.fetch_add(1, Ordering::Relaxed);
    stats.read_bytes.fetch_add(len as u64, Ordering::Relaxed);
    stats
        .read_ns
        .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    match result {
        Ok(()) => 0,
        Err(e) => {
            set_err(err, &e);
            -1
        }
    }
}

/// Process-wide counters as JSON (free with `pgvfs_free_str`). Cumulative:
/// callers diff two samples.
#[no_mangle]
pub extern "C" fn pgvfs_stats() -> *mut c_char {
    let s = &store::STATS;
    let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
    let json = format!(
        "{{\"opens\": {}, \"open_hits\": {}, \"open_ms\": {:.1}, \"reads\": {}, \"read_bytes\": {}, \"read_ms\": {:.1}, \"pieces\": {}, \"db_reads\": {}, \"db_read_bytes\": {}, \"db_pieces\": {}, \"db_cache_bytes\": {}, \"db_publishes\": {}, \"db_readahead_hits\": {}, \"db_readahead_bytes\": {}}}",
        get(&s.opens),
        get(&s.open_hits),
        get(&s.open_ns) as f64 / 1e6,
        get(&s.reads),
        get(&s.read_bytes),
        get(&s.read_ns) as f64 / 1e6,
        get(&s.pieces),
        get(&s.db_reads),
        get(&s.db_read_bytes),
        get(&s.db_pieces),
        get(&s.db_cache_bytes),
        get(&s.db_publishes),
        get(&s.db_readahead_hits),
        get(&s.db_readahead_bytes)
    );
    CString::new(json).unwrap_or_default().into_raw()
}

pub type ListCb = extern "C" fn(ctx: *mut c_void, path: *const c_char, len: usize);

/// Call `cb` for each path under `prefix`, in byte order, as the rows stream
/// in (`limit` paths at most; a negative limit means all). 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_list(
    c: *const PgvfsConn,
    volume: *const c_char,
    prefix: *const c_char,
    limit: i64,
    cb: ListCb,
    ctx: *mut c_void,
    err: *mut *mut c_char,
) -> c_int {
    let c = conn(c);
    let run = || -> Result<()> {
        let (volume, prefix) = (text(volume)?, text(prefix)?);
        store::check_volume(volume)?;
        let limit = (limit >= 0).then_some(limit);
        let mut emit = |path: &str| cb(ctx, path.as_ptr().cast(), path.len());
        c.rt.block_on(store::list(&c.pool, volume, prefix, limit, &mut emit))
    };
    match run() {
        Ok(()) => 0,
        Err(e) => {
            set_err(err, &e);
            -1
        }
    }
}

/// 0 removed, 1 not found, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_remove(
    c: *const PgvfsConn,
    volume: *const c_char,
    path: *const c_char,
    err: *mut *mut c_char,
) -> c_int {
    let c = conn(c);
    let run = || -> Result<bool> {
        let (volume, path) = (text(volume)?, text(path)?);
        store::check_volume(volume)?;
        c.writable()?;
        c.files.forget(volume, path);
        c.rt.block_on(store::remove(&c.pool, volume, path))
    };
    match run() {
        Ok(found) => {
            c.maybe_reap();
            if found {
                0
            } else {
                1
            }
        }
        Err(e) => {
            set_err(err, &e);
            -1
        }
    }
}

/// Remove every file under `prefix` ("" for the whole volume). Returns how
/// many, or -1.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_remove_prefix(
    c: *const PgvfsConn,
    volume: *const c_char,
    prefix: *const c_char,
    err: *mut *mut c_char,
) -> i64 {
    let c = conn(c);
    let run = || -> Result<i64> {
        let (volume, prefix) = (text(volume)?, text(prefix)?);
        store::check_volume(volume)?;
        c.writable()?;
        c.files
            .map
            .lock()
            .unwrap()
            .retain(|(v, p), _| v != volume || !p.starts_with(prefix));
        c.rt.block_on(store::remove_prefix(&c.pool, volume, prefix))
    };
    match run() {
        Ok(n) => {
            c.maybe_reap();
            n
        }
        Err(e) => {
            set_err(err, &e);
            -1
        }
    }
}

/// Rename within a volume, replacing the target. 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_rename(
    c: *const PgvfsConn,
    volume: *const c_char,
    from: *const c_char,
    to: *const c_char,
    err: *mut *mut c_char,
) -> c_int {
    let c = conn(c);
    let run = || -> Result<()> {
        let (volume, from, to) = (text(volume)?, text(from)?, text(to)?);
        store::check_volume(volume)?;
        c.writable()?;
        c.files.forget(volume, from);
        c.files.forget(volume, to);
        c.rt.block_on(store::rename(&c.pool, volume, from, to))
    };
    match run() {
        Ok(()) => 0,
        Err(e) => {
            set_err(err, &e);
            -1
        }
    }
}

/// Start writing a new file at `path`. It is published (replacing any file
/// there) only by `pgvfs_writer_publish`; `pgvfs_writer_abort` discards it.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_writer_open(
    c: *const PgvfsConn,
    volume: *const c_char,
    path: *const c_char,
    err: *mut *mut c_char,
) -> *mut PgvfsWriter {
    let cr = conn(c);
    let run = || -> Result<PgvfsWriter> {
        let (volume, path) = (text(volume)?, text(path)?);
        cr.writable()?;
        let (tx, task) =
            store::spawn_writer(cr.rt.handle(), cr.pool.clone(), volume.into(), path.into())?;
        Ok(PgvfsWriter {
            conn: c,
            volume: volume.into(),
            path: path.into(),
            tx: Some(tx),
            task: Some(task),
            buf: BytesMut::with_capacity(WRITE_BATCH),
        })
    };
    match run() {
        Ok(w) => Box::into_raw(Box::new(w)),
        Err(e) => {
            set_err(err, &e);
            std::ptr::null_mut()
        }
    }
}

impl PgvfsWriter {
    fn send(&mut self, msg: WriteMsg) -> Result<()> {
        let sent = self
            .tx
            .as_ref()
            .ok_or_else(|| anyhow!("pgvfs writer already failed"))?
            .blocking_send(msg);
        if sent.is_err() {
            // The COPY task stopped: report why.
            return Err(self
                .join()
                .err()
                .unwrap_or_else(|| anyhow!("pgvfs writer stopped")));
        }
        Ok(())
    }

    fn join(&mut self) -> Result<()> {
        self.tx = None;
        let task = self
            .task
            .take()
            .ok_or_else(|| anyhow!("pgvfs writer already finished"))?;
        conn(self.conn).rt.block_on(task)?
    }
}

/// Append `len` bytes. 0 ok, -1 error (the file is then unpublishable).
#[no_mangle]
pub unsafe extern "C" fn pgvfs_writer_write(
    w: *mut PgvfsWriter,
    buf: *const u8,
    len: i64,
    err: *mut *mut c_char,
) -> c_int {
    let w = unsafe { &mut *w };
    if len > 0 {
        w.buf
            .extend_from_slice(unsafe { std::slice::from_raw_parts(buf, len as usize) });
    }
    while w.buf.len() >= WRITE_BATCH {
        let batch = w.buf.split_to(WRITE_BATCH).freeze();
        if let Err(e) = w.send(WriteMsg::Data(batch)) {
            set_err(err, &e);
            return -1;
        }
    }
    0
}

/// Publish and free the writer. 0 ok, -1 error (nothing published).
#[no_mangle]
pub unsafe extern "C" fn pgvfs_writer_publish(w: *mut PgvfsWriter, err: *mut *mut c_char) -> c_int {
    let mut w = unsafe { Box::from_raw(w) };
    let run = |w: &mut PgvfsWriter| -> Result<()> {
        conn(w.conn).writable()?;
        if !w.buf.is_empty() {
            let rest = w.buf.split().freeze();
            w.send(WriteMsg::Data(rest))?;
        }
        w.send(WriteMsg::Publish)?;
        w.join()
    };
    let result = run(&mut w);
    conn(w.conn).files.forget(&w.volume, &w.path);
    conn(w.conn).maybe_reap();
    match result {
        Ok(()) => 0,
        Err(e) => {
            set_err(err, &e);
            -1
        }
    }
}

/// Discard and free the writer; nothing is published.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_writer_abort(w: *mut PgvfsWriter) {
    if !w.is_null() {
        // Dropping the sender ends the COPY task, which rolls back.
        drop(unsafe { Box::from_raw(w) });
    }
}

// pgdb:// databases (db.rs). A database is (volume, name); `path` names one of
// its files: "" (the database) or a WAL suffix such as ".wal".

/// `pgdb_open` flags.
pub const PGDB_WRITE: c_int = 1;
pub const PGDB_CREATE: c_int = 2;
pub const PGDB_OVERWRITE: c_int = 4;

pub struct PgdbFile {
    conn: *const PgvfsConn,
    kind: DbKind,
}

enum DbKind {
    /// A published file at the gen a reader opened.
    Read(db::Version),
    /// A file of the database this process writes: reads see its writes.
    Write {
        db: Arc<db::Db>,
        writer: Arc<db::Writer>,
        path: String,
        /// The database's main file opened for writing (`Writer::claim_main`).
        main: bool,
    },
}

/// What `pgdb_open` opened: `gen` is -1 (and `writer` 1) for the writer's files.
#[repr(C)]
pub struct PgdbInfo {
    pub db_id: i32,
    pub file_id: i64,
    pub gen: i64,
    pub writer: c_int,
}

pub type RunCb = extern "C" fn(ctx: *mut c_void, offset: i64, len: i64, gen: i64);

fn ok_or<T>(result: Result<T>, err: *mut *mut c_char, ok: impl FnOnce(T) -> c_int) -> c_int {
    match result {
        Ok(v) => ok(v),
        Err(e) => {
            set_err(err, &e);
            -1
        }
    }
}

fn find_db(c: &PgvfsConn, volume: &str, name: &str, create: bool) -> Result<Option<Arc<db::Db>>> {
    c.rt.block_on(c.dbs.find(&c.pool, volume, name, create))
}

/// The newest published gen of a database: 0 found (`*gen`), 1 no such database, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgdb_latest(
    c: *const PgvfsConn,
    volume: *const c_char,
    name: *const c_char,
    gen: *mut i64,
    err: *mut *mut c_char,
) -> c_int {
    let c = conn(c);
    let run = || -> Result<Option<i64>> {
        let (volume, name) = (text(volume)?, text(name)?);
        c.rt.block_on(db::latest(&c.pool, volume, name))
    };
    ok_or(run(), err, |found| match found {
        Some(g) => {
            unsafe { *gen = g };
            0
        }
        None => 1,
    })
}

/// Open `path` of a database: for writing (PGDB_WRITE, taking the database's
/// write lock unless this process holds it; PGDB_CREATE makes the database
/// and file if missing, PGDB_OVERWRITE empties the file), else for reading
/// at `gen` (< 0: the newest). `token` (< 0: none) names the attach the path
/// came from: the writer's own attach (or a plain path) reads the writer's
/// files, any other a published version. 0 ok (`*out`, `*info`), 1 not
/// found, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgdb_open(
    c: *const PgvfsConn,
    volume: *const c_char,
    name: *const c_char,
    gen: i64,
    token: i64,
    path: *const c_char,
    flags: c_int,
    out: *mut *mut PgdbFile,
    info: *mut PgdbInfo,
    err: *mut *mut c_char,
) -> c_int {
    let cp = c;
    let c = conn(c);
    let run = || -> Result<Option<(DbKind, PgdbInfo)>> {
        let (volume, name, path) = (text(volume)?, text(name)?, text(path)?);
        let create = flags & PGDB_CREATE != 0;
        if flags & PGDB_WRITE != 0 {
            anyhow::ensure!(
                gen < 0 || token >= 0,
                "pgdb://{volume}/{name}@{gen}: a pinned version is read-only"
            );
            c.db_writable()?;
            let Some(db) = find_db(c, volume, name, create)? else {
                return Ok(None);
            };
            let writer = db.acquire_writer(c.rt.handle(), &c.pool)?;
            let main = path.is_empty();
            if main {
                if let Err(e) = writer.claim_main(token) {
                    db.release_writer()?;
                    return Err(e);
                }
            }
            let made = (|| -> Result<bool> {
                if !writer.exists(path) && !create {
                    return Ok(false);
                }
                writer.create(path, flags & PGDB_OVERWRITE != 0)?;
                Ok(true)
            })();
            if !matches!(made, Ok(true)) {
                if main {
                    writer.release_main();
                }
                db.release_writer()?;
                made?;
                return Ok(None);
            }
            let info = PgdbInfo {
                db_id: db.db_id,
                file_id: writer.file_id(path).unwrap_or(-1),
                gen: -1,
                writer: 1,
            };
            let path = path.to_owned();
            return Ok(Some((
                DbKind::Write {
                    db,
                    writer,
                    path,
                    main,
                },
                info,
            )));
        }
        let Some(db) = find_db(c, volume, name, false)? else {
            return Ok(None);
        };
        if let Some(writer) = db.join_writer() {
            if writer.owns(gen, token) {
                if !writer.exists(path) {
                    db.release_writer()?;
                    return Ok(None);
                }
                let info = PgdbInfo {
                    db_id: db.db_id,
                    file_id: writer.file_id(path).unwrap_or(-1),
                    gen: -1,
                    writer: 1,
                };
                let path = path.to_owned();
                let main = false;
                return Ok(Some((
                    DbKind::Write {
                        db,
                        writer,
                        path,
                        main,
                    },
                    info,
                )));
            }
            db.release_writer()?; // another attach: a published version
        }
        let gen = (gen >= 0).then_some(gen);
        let Some(v) = c.rt.block_on(db::open_version(&c.pool, &db, gen, path))? else {
            return Ok(None);
        };
        let info = PgdbInfo {
            db_id: db.db_id,
            file_id: v.file_id,
            gen: v.gen,
            writer: 0,
        };
        Ok(Some((DbKind::Read(v), info)))
    };
    ok_or(run(), err, |found| match found {
        Some((kind, i)) => {
            unsafe {
                *info = i;
                *out = Box::into_raw(Box::new(PgdbFile { conn: cp, kind }));
            }
            0
        }
        None => 1,
    })
}

/// Close and free a file. Closing the last of a writer's files publishes what
/// is left and releases the database. 0 ok, -1 error (the file is freed anyway).
#[no_mangle]
pub unsafe extern "C" fn pgdb_close(f: *mut PgdbFile, err: *mut *mut c_char) -> c_int {
    if f.is_null() {
        return 0;
    }
    let f = unsafe { Box::from_raw(f) };
    match f.kind {
        DbKind::Read(_) => 0,
        DbKind::Write {
            db, writer, main, ..
        } => {
            if main {
                writer.release_main();
            }
            ok_or(db.release_writer(), err, |_| 0)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn pgdb_size(f: *const PgdbFile) -> i64 {
    match &unsafe { &*f }.kind {
        DbKind::Read(v) => v.size,
        DbKind::Write { writer, path, .. } => writer.size(path).unwrap_or(0),
    }
}

/// Fill exactly `len` bytes from `pos`, straight from PostgreSQL (and, for
/// the writer, its staged writes). 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgdb_read(
    f: *const PgdbFile,
    buf: *mut u8,
    len: i64,
    pos: i64,
    err: *mut *mut c_char,
) -> c_int {
    if len <= 0 {
        return 0;
    }
    let f = unsafe { &*f };
    let c = conn(f.conn);
    let out = unsafe { std::slice::from_raw_parts_mut(buf, len as usize) };
    let result = match &f.kind {
        DbKind::Read(v) => {
            if pos < 0 || pos + len > v.size {
                Err(anyhow!(
                    "read [{pos}, {}) outside {}@{} of {} bytes",
                    pos + len,
                    v.db.label,
                    v.gen,
                    v.size
                ))
            } else {
                store::STATS.db_reads.fetch_add(1, Ordering::Relaxed);
                store::STATS
                    .db_read_bytes
                    .fetch_add(len as u64, Ordering::Relaxed);
                c.rt.block_on(db::read_with_readahead(
                    &v.db, &c.pool, v.file_id, v.gen, pos, out,
                ))
            }
        }
        DbKind::Write { writer, path, .. } => writer.read(path, pos, out),
    };
    ok_or(result, err, |_| 0)
}

/// For a reader: call `cb` with runs of [pos, pos + len) and the gen whose
/// bytes each run can be read (and cached) as (`pgdb_read_gen`). 0 ok, 1 not
/// cacheable (read with `pgdb_read`).
#[no_mangle]
pub unsafe extern "C" fn pgdb_runs(
    f: *const PgdbFile,
    pos: i64,
    len: i64,
    cb: RunCb,
    ctx: *mut c_void,
) -> c_int {
    let DbKind::Read(v) = &unsafe { &*f }.kind else {
        return 1;
    };
    if len <= 0 {
        return 0;
    }
    if pos < 0 || pos + len > v.size {
        return 1; // pgdb_read reports it
    }
    let end = pos + len;
    let Some(runs) =
        v.db.runs(v.file_id, pos / db::CHUNK, (end - 1) / db::CHUNK, v.gen)
    else {
        return 1;
    };
    for (a, b, gen) in runs {
        let s = (a * db::CHUNK).max(pos);
        let e = (b * db::CHUNK).min(end);
        cb(ctx, s, e - s, gen);
    }
    store::STATS
        .db_cache_bytes
        .fetch_add(len as u64, Ordering::Relaxed);
    0
}

/// Fill `len` bytes from `pos` of a file as of `gen` (zeros past its end),
/// for DuckDB's external file cache. 0 ok, 2 that gen's versions may be
/// reaped (read with `pgdb_read` instead), -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgdb_read_gen(
    c: *const PgvfsConn,
    db_id: i32,
    file_id: i64,
    gen: i64,
    buf: *mut u8,
    len: i64,
    pos: i64,
    err: *mut *mut c_char,
) -> c_int {
    if len <= 0 {
        return 0;
    }
    let c = conn(c);
    let out = unsafe { std::slice::from_raw_parts_mut(buf, len as usize) };
    store::STATS.db_reads.fetch_add(1, Ordering::Relaxed);
    store::STATS
        .db_read_bytes
        .fetch_add(len as u64, Ordering::Relaxed);
    let result = match c.dbs.by_id(db_id) {
        Some(db) => c.rt.block_on(db::read_with_readahead(
            &db, &c.pool, file_id, gen, pos, out,
        )),
        None => c.rt.block_on(db::read_versions(
            &c.pool, db_id, file_id, gen, gen, pos, out,
        )),
    };
    match result {
        Ok(()) => 0,
        Err(e) if format!("{e:#}").contains(db::EXPIRED) => 2,
        Err(e) => {
            set_err(err, &e);
            -1
        }
    }
}

fn writer_of<'a>(f: *mut PgdbFile) -> Result<(&'a db::Writer, &'a str)> {
    match &unsafe { &*f }.kind {
        DbKind::Write { writer, path, .. } => Ok((writer, path)),
        DbKind::Read(v) => Err(anyhow!("{} is open read-only", v.db.label)),
    }
}

/// Write `len` bytes at `pos`. Staged: published by `pgdb_sync`. 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgdb_write(
    f: *mut PgdbFile,
    buf: *const u8,
    len: i64,
    pos: i64,
    err: *mut *mut c_char,
) -> c_int {
    let run = || -> Result<()> {
        let (w, path) = writer_of(f)?;
        if len <= 0 {
            return Ok(());
        }
        w.write(path, pos, unsafe {
            std::slice::from_raw_parts(buf, len as usize)
        })
    };
    ok_or(run(), err, |_| 0)
}

#[no_mangle]
pub unsafe extern "C" fn pgdb_truncate(
    f: *mut PgdbFile,
    size: i64,
    err: *mut *mut c_char,
) -> c_int {
    let run = || -> Result<()> {
        let (w, path) = writer_of(f)?;
        w.truncate(path, size)
    };
    ok_or(run(), err, |_| 0)
}

/// Publish everything this database's writer has written as the next gen.
#[no_mangle]
pub unsafe extern "C" fn pgdb_sync(f: *mut PgdbFile, err: *mut *mut c_char) -> c_int {
    let run = || -> Result<()> {
        match &unsafe { &*f }.kind {
            DbKind::Write { writer, .. } => writer.sync(),
            DbKind::Read(_) => Ok(()),
        }
    };
    ok_or(run(), err, |_| 0)
}

/// 0 exists, 1 not, -1 error. For the writer's own attach (`token`, see
/// `pgdb_open`) or a plain path its files; otherwise as of `gen` (< 0: the newest).
#[no_mangle]
pub unsafe extern "C" fn pgdb_exists(
    c: *const PgvfsConn,
    volume: *const c_char,
    name: *const c_char,
    gen: i64,
    token: i64,
    path: *const c_char,
    err: *mut *mut c_char,
) -> c_int {
    let c = conn(c);
    let run = || -> Result<bool> {
        let (volume, name, path) = (text(volume)?, text(name)?, text(path)?);
        let Some(db) = find_db(c, volume, name, false)? else {
            return Ok(false);
        };
        if let Some(w) = db.writer().filter(|w| w.owns(gen, token)) {
            return Ok(w.exists(path));
        }
        let gen = (gen >= 0).then_some(gen);
        Ok(c.rt
            .block_on(db::open_version(&c.pool, &db, gen, path))?
            .is_some())
    };
    ok_or(run(), err, |found| if found { 0 } else { 1 })
}

fn db_writer(c: &PgvfsConn, volume: &str, name: &str) -> Result<Arc<db::Writer>> {
    find_db(c, volume, name, false)?
        .and_then(|db| db.writer())
        .ok_or_else(|| anyhow!("pgdb://{volume}/{name} is not attached for writing here"))
}

/// Remove a file of a database this process writes. 0 removed, 1 not found, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgdb_remove(
    c: *const PgvfsConn,
    volume: *const c_char,
    name: *const c_char,
    path: *const c_char,
    err: *mut *mut c_char,
) -> c_int {
    let c = conn(c);
    let run = || -> Result<bool> {
        let (volume, name, path) = (text(volume)?, text(name)?, text(path)?);
        let Some(db) = find_db(c, volume, name, false)? else {
            return Ok(false);
        };
        if let Some(w) = db.writer() {
            return w.remove(path);
        }
        // DuckDB clears a stale WAL before it opens a new database: absent
        // is fine; anything else needs the writer.
        match c.rt.block_on(db::open_version(&c.pool, &db, None, path))? {
            None => Ok(false),
            Some(_) => Err(anyhow!(
                "pgdb://{volume}/{name}{path}: the database is not attached for writing here"
            )),
        }
    };
    ok_or(run(), err, |found| if found { 0 } else { 1 })
}

/// Rename a file of a database this process writes, replacing `to`. 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgdb_rename(
    c: *const PgvfsConn,
    volume: *const c_char,
    name: *const c_char,
    from: *const c_char,
    to: *const c_char,
    err: *mut *mut c_char,
) -> c_int {
    let c = conn(c);
    let run = || -> Result<()> {
        let (volume, name, from, to) = (text(volume)?, text(name)?, text(from)?, text(to)?);
        db_writer(c, volume, name)?.rename(from, to)
    };
    ok_or(run(), err, |_| 0)
}
