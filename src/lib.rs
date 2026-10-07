//! C ABI over the pgvfs storage layer, for the DuckDB `pgvfs://` filesystem
//! (extension/). The C++ side only registers the scheme and SQL functions
//! with DuckDB, whose FileSystem registration is not in the stable C API;
//! storage and tantivy indexes (`index`) are all here.
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
//! connection outlives its writers and index builds and is used from any
//! thread.
#![allow(clippy::missing_safety_doc)]

pub mod index;
pub mod pg;
pub mod store;

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
    indexes: index::Cache,
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
    /// Connect and verify the layout (read access suffices), sized for a
    /// DuckDB with `threads` threads (0: one per core).
    pub fn connect(url: &str, threads: usize) -> Result<PgvfsConn> {
        let threads = if threads > 0 {
            threads
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
            indexes: index::Cache::default(),
        })
    }

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

    /// The file published at (volume, path), through the open cache.
    pub fn open(&self, volume: &str, path: &str) -> Result<Option<FileInfo>> {
        let run = || -> Result<Option<FileInfo>> {
            store::check_volume(volume)?;
            if let Some(f) = self.files.get(volume, path) {
                store::STATS.open_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(f));
            }
            let found = self.rt.block_on(store::open(&self.pool, volume, path))?;
            if let Some(f) = found {
                self.files.put(volume, path, f);
            }
            Ok(found)
        };
        let t0 = Instant::now();
        let result = run();
        store::STATS.opens.fetch_add(1, Ordering::Relaxed);
        store::STATS
            .open_ns
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }

    /// Fill `buf` with the file's bytes from `pos`; the range must lie inside
    /// the file.
    pub fn read(&self, file: &FileInfo, pos: i64, buf: &mut [u8]) -> Result<()> {
        let t0 = Instant::now();
        let result = self.rt.block_on(store::read_at(&self.pool, file, pos, buf));
        let stats = &store::STATS;
        stats.reads.fetch_add(1, Ordering::Relaxed);
        stats
            .read_bytes
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        stats
            .read_ns
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }

    /// Call `f` for up to `limit` paths under `prefix` (limit < 0: all), in
    /// byte order.
    pub fn list(
        &self,
        volume: &str,
        prefix: &str,
        limit: i64,
        mut f: impl FnMut(&str),
    ) -> Result<()> {
        store::check_volume(volume)?;
        let mut left = if limit < 0 { i64::MAX } else { limit };
        let mut after = String::new();
        while left > 0 {
            let page = left.min(1000);
            let batch = self
                .rt
                .block_on(store::list(&self.pool, volume, prefix, &after, page))?;
            for p in &batch {
                f(p);
            }
            left -= batch.len() as i64;
            if (batch.len() as i64) < page {
                break;
            }
            after = batch.last().cloned().unwrap_or_default();
        }
        Ok(())
    }

    /// Unpublish a file. Returns false if nothing was there.
    pub fn remove(&self, volume: &str, path: &str) -> Result<bool> {
        store::check_volume(volume)?;
        self.writer()?;
        self.files.forget(volume, path);
        let found = self.rt.block_on(store::remove(&self.pool, volume, path))?;
        self.maybe_reap();
        Ok(found)
    }

    /// Rename within a volume, replacing the target.
    pub fn rename(&self, volume: &str, from: &str, to: &str) -> Result<()> {
        store::check_volume(volume)?;
        self.writer()?;
        self.files.forget(volume, from);
        self.files.forget(volume, to);
        self.rt
            .block_on(store::rename(&self.pool, volume, from, to))
    }
}

/// A new file at `path`, published (replacing any file there) only by
/// `publish`; dropping it discards it.
pub struct PgvfsWriter {
    conn: &'static PgvfsConn,
    volume: String,
    path: String,
    tx: Option<mpsc::Sender<WriteMsg>>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
    buf: BytesMut,
}

impl PgvfsWriter {
    pub fn open(conn: &'static PgvfsConn, volume: &str, path: &str) -> Result<PgvfsWriter> {
        conn.writer()?;
        let (tx, task) = store::spawn_writer(
            conn.rt.handle(),
            conn.pool.clone(),
            volume.into(),
            path.into(),
        )?;
        Ok(PgvfsWriter {
            conn,
            volume: volume.into(),
            path: path.into(),
            tx: Some(tx),
            task: Some(task),
            buf: BytesMut::with_capacity(WRITE_BATCH),
        })
    }

    /// Append bytes. After an error the file is unpublishable.
    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        self.buf.extend_from_slice(data);
        while self.buf.len() >= WRITE_BATCH {
            let batch = self.buf.split_to(WRITE_BATCH).freeze();
            self.send(WriteMsg::Data(batch))?;
        }
        Ok(())
    }

    /// Publish the file at its path.
    pub fn publish(mut self) -> Result<()> {
        let run = |w: &mut PgvfsWriter| -> Result<()> {
            w.conn.writer()?;
            if !w.buf.is_empty() {
                let rest = w.buf.split().freeze();
                w.send(WriteMsg::Data(rest))?;
            }
            w.send(WriteMsg::Publish)?;
            w.join()
        };
        let result = run(&mut self);
        self.conn.files.forget(&self.volume, &self.path);
        self.conn.maybe_reap();
        result
    }

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
        self.conn.rt.block_on(task)?
    }
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

fn bytes_text<'a>(p: *const c_char, len: usize) -> Result<&'a str> {
    if p.is_null() {
        return Err(anyhow!("null string"));
    }
    std::str::from_utf8(unsafe { std::slice::from_raw_parts(p.cast(), len) })
        .map_err(|_| anyhow!("strings must be UTF-8"))
}

fn conn<'a>(c: *const PgvfsConn) -> &'a PgvfsConn {
    unsafe { &*c }
}

/// 0 ok, -1 error (in `*err`).
fn status(result: Result<()>, err: *mut *mut c_char) -> c_int {
    match result {
        Ok(()) => 0,
        Err(e) => {
            set_err(err, &e);
            -1
        }
    }
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
    match text(url).and_then(|url| PgvfsConn::connect(url, threads.max(0) as usize)) {
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
    match text(volume).and_then(|v| Ok((v, text(path)?))) {
        Ok((volume, path)) => match conn(c).open(volume, path) {
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
            Err(e) => status(Err(e), err),
        },
        Err(e) => status(Err(e), err),
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
    let f = unsafe { &*file };
    let info = FileInfo {
        file_id: f.file_id,
        size: f.size,
        created_at: UNIX_EPOCH,
    };
    let out = unsafe { std::slice::from_raw_parts_mut(buf, len as usize) };
    status(conn(c).read(&info, pos, out), err)
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
    let run = || -> Result<()> {
        conn(c).list(text(volume)?, text(prefix)?, limit, |p| {
            cb(ctx, p.as_ptr().cast(), p.len())
        })
    };
    status(run(), err)
}

/// 0 removed, 1 not found, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_remove(
    c: *const PgvfsConn,
    volume: *const c_char,
    path: *const c_char,
    err: *mut *mut c_char,
) -> c_int {
    match text(volume).and_then(|v| conn(c).remove(v, text(path)?)) {
        Ok(true) => 0,
        Ok(false) => 1,
        Err(e) => status(Err(e), err),
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
    let run = || conn(c).rename(text(volume)?, text(from)?, text(to)?);
    status(run(), err)
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
    match text(volume).and_then(|v| PgvfsWriter::open(conn(c), v, text(path)?)) {
        Ok(w) => Box::into_raw(Box::new(w)),
        Err(e) => {
            set_err(err, &e);
            std::ptr::null_mut()
        }
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
    if len <= 0 {
        return 0;
    }
    let data = unsafe { std::slice::from_raw_parts(buf, len as usize) };
    status(w.write(data), err)
}

/// Publish and free the writer. 0 ok, -1 error (nothing published).
#[no_mangle]
pub unsafe extern "C" fn pgvfs_writer_publish(w: *mut PgvfsWriter, err: *mut *mut c_char) -> c_int {
    status(unsafe { Box::from_raw(w) }.publish(), err)
}

/// Discard and free the writer; nothing is published.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_writer_abort(w: *mut PgvfsWriter) {
    if !w.is_null() {
        // Dropping the sender ends the COPY task, which rolls back.
        drop(unsafe { Box::from_raw(w) });
    }
}

/// Start building a tantivy split at pgvfs://`volume`/`path` (see `index`):
/// `schema` is a tantivy schema as JSON, `options` (may be NULL) build options
/// as JSON. Add documents with `pgvfs_index_add`, from any thread; finish with
/// `pgvfs_index_commit` or `pgvfs_index_abort`.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_index_open(
    c: *const PgvfsConn,
    volume: *const c_char,
    path: *const c_char,
    schema: *const c_char,
    options: *const c_char,
    err: *mut *mut c_char,
) -> *mut index::Build {
    let run = || -> Result<index::Build> {
        let options = if options.is_null() {
            ""
        } else {
            text(options)?
        };
        index::Build::open(conn(c), text(volume)?, text(path)?, text(schema)?, options)
    };
    match run() {
        Ok(b) => Box::into_raw(Box::new(b)),
        Err(e) => {
            set_err(err, &e);
            std::ptr::null_mut()
        }
    }
}

/// Add one document (a JSON object). 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_index_add(
    b: *const index::Build,
    doc: *const c_char,
    len: usize,
    err: *mut *mut c_char,
) -> c_int {
    let b = unsafe { &*b };
    status(bytes_text(doc, len).and_then(|doc| b.add(doc)), err)
}

/// Commit the split and free the build. Returns the documents indexed, or -1
/// on error (the build's files are removed).
#[no_mangle]
pub unsafe extern "C" fn pgvfs_index_commit(b: *mut index::Build, err: *mut *mut c_char) -> i64 {
    match unsafe { Box::from_raw(b) }.commit() {
        Ok(n) => n as i64,
        Err(e) => {
            set_err(err, &e);
            -1
        }
    }
}

/// Discard and free the build, removing its files.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_index_abort(b: *mut index::Build) {
    if !b.is_null() {
        drop(unsafe { Box::from_raw(b) });
    }
}

pub type HitCb = extern "C" fn(ctx: *mut c_void, score: f64, doc: *const c_char, len: usize);

/// Search the split at pgvfs://`volume`/`path` with a tantivy query;
/// `options` (may be NULL) is search options as JSON. Calls `cb` per hit, best
/// first, with its score and stored fields as a JSON object. 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_index_search(
    c: *const PgvfsConn,
    volume: *const c_char,
    path: *const c_char,
    query: *const c_char,
    options: *const c_char,
    cb: HitCb,
    ctx: *mut c_void,
    err: *mut *mut c_char,
) -> c_int {
    let run = || -> Result<()> {
        let options = if options.is_null() {
            ""
        } else {
            text(options)?
        };
        index::search(
            conn(c),
            text(volume)?,
            text(path)?,
            text(query)?,
            options,
            |score, doc| cb(ctx, score as f64, doc.as_ptr().cast(), doc.len()),
        )
    };
    status(run(), err)
}

/// Remove the split at pgvfs://`volume`/`path`. 0 removed, 1 none there, -1
/// error.
#[no_mangle]
pub unsafe extern "C" fn pgvfs_index_drop(
    c: *const PgvfsConn,
    volume: *const c_char,
    path: *const c_char,
    err: *mut *mut c_char,
) -> c_int {
    match text(volume).and_then(|v| index::drop_index(conn(c), v, text(path)?)) {
        Ok(true) => 0,
        Ok(false) => 1,
        Err(e) => status(Err(e), err),
    }
}
