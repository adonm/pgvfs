//! C ABI over the pgvfs storage layer, for the DuckDB `pgvfs://` filesystem
//! (extension/). The C++ side only registers the scheme with DuckDB, whose
//! FileSystem registration is not in the stable C API; storage is all here.
//!
//! Connecting needs only read access. The first write operation takes the
//! database's writer lease (see `store::WriterLease`) and keeps it for the
//! life of the connection.
//!
//! Calls block the calling DuckDB thread on this connection's tokio runtime.
//! Strings in are UTF-8 (rejected otherwise); errors come back as `*err`,
//! freed with `pgvfs_free_str`.
//!
//! Safety (every function): handles are those this library returned and not
//! yet freed; strings are NUL-terminated; buffers are valid for `len` bytes. A
//! connection outlives its writers and is used from any thread.
#![allow(clippy::missing_safety_doc)]

pub mod pg;
pub mod store;

/// Search splits: their C ABI (`tantivy_*`) ships in this staticlib.
pub use duckdb_tantivy;

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use bytes::BytesMut;
use pg::Pool;
use tokio::sync::mpsc;

use store::{FileInfo, WriteMsg, WriterLease, WRITE_BATCH};

pub struct PgvfsConn {
    rt: tokio::runtime::Runtime,
    pool: Pool,
    writer: Mutex<Option<WriterLease>>,
    last_reap: AtomicU64,
    files: FileCache,
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
    /// Take (once) and check the writer lease before any write.
    fn writer(&self) -> Result<()> {
        let mut lease = self.writer.lock().unwrap();
        if lease.is_none() {
            *lease = Some(self.rt.block_on(store::acquire_writer(&self.pool))?);
            // A new writer catches up on garbage left while none was writing.
            self.maybe_reap();
        }
        lease.as_ref().unwrap().check()
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
            writer: Mutex::new(None),
            last_reap: AtomicU64::new(0),
            files: FileCache {
                ttl: std::time::Duration::from_secs(env_num("PGVFS_OPEN_CACHE_S", 10) as u64),
                map: Mutex::default(),
            },
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
        "{{\"opens\": {}, \"open_hits\": {}, \"open_ms\": {:.1}, \"reads\": {}, \"read_bytes\": {}, \"read_ms\": {:.1}, \"pieces\": {}}}",
        get(&s.opens),
        get(&s.open_hits),
        get(&s.open_ns) as f64 / 1e6,
        get(&s.reads),
        get(&s.read_bytes),
        get(&s.read_ns) as f64 / 1e6,
        get(&s.pieces)
    );
    CString::new(json).unwrap_or_default().into_raw()
}

pub type ListCb = extern "C" fn(ctx: *mut c_void, path: *const c_char, len: usize);

/// Call `cb` for up to `limit` paths under `prefix` (limit < 0: all), in byte
/// order. 0 ok, -1 error.
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
        let mut left = if limit < 0 { i64::MAX } else { limit };
        let mut after = String::new();
        while left > 0 {
            let page = left.min(1000);
            let batch =
                c.rt.block_on(store::list(&c.pool, volume, prefix, &after, page))?;
            for p in &batch {
                cb(ctx, p.as_ptr().cast(), p.len());
            }
            left -= batch.len() as i64;
            if (batch.len() as i64) < page {
                break;
            }
            after = batch.last().cloned().unwrap_or_default();
        }
        Ok(())
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
        c.writer()?;
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
        c.writer()?;
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
        cr.writer()?;
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
        conn(w.conn).writer()?;
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
