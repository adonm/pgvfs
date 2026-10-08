//! C ABI over tantivy splits (`split`, `search`, `dsl`), for the DuckDB
//! extension (extension/src/tantivy_functions.cpp). The C++ side does all
//! storage, through DuckDB's filesystem, so splits live wherever DuckDB can
//! read and write: local files, object stores, pgvfs.
//!
//! Errors come back as `*err`, freed with `tantivy_free_str`; a panic is an
//! error too, never an abort of the host. Callbacks report theirs by writing
//! a message of at most `cap` bytes (NUL included) to `msg` and returning
//! nonzero.
//!
//! Exclude sets (`kind`, `data`, `len`): 0 none; 1 a serialized roaring
//! bitmap of `len` bytes; 2 `len` int64 values. `threads` is how many splits
//! a call may search at once.
//!
//! Safety (every function): handles are those this library returned and not
//! yet freed; strings are NUL-terminated or come with their length; a split's
//! `ctx` outlives it.
#![allow(clippy::missing_safety_doc)]

mod dsl;
mod pattern;
pub mod search;
pub mod split;

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use search::{Exclude, Request};
use split::{Build, Split};

pub type ReadCb = extern "C" fn(
    ctx: *mut c_void,
    buf: *mut u8,
    len: u64,
    offset: u64,
    msg: *mut c_char,
    cap: usize,
) -> c_int;
pub type WriteCb = extern "C" fn(
    ctx: *mut c_void,
    buf: *const u8,
    len: u64,
    msg: *mut c_char,
    cap: usize,
) -> c_int;
pub type HitCb = extern "C" fn(
    ctx: *mut c_void,
    split: usize,
    score: f64,
    doc: *const c_char,
    len: usize,
    highlight: *const c_char,
    highlight_len: usize,
);

const MSG_CAP: usize = 1024;

fn set_err(err: *mut *mut c_char, e: &anyhow::Error) {
    if !err.is_null() {
        let msg = format!("{e:#}").replace('\0', " ");
        unsafe { *err = CString::new(msg).unwrap_or_default().into_raw() };
    }
}

/// Run `f`, reporting its error or panic in `*err` and returning `failed`.
fn guard<T>(err: *mut *mut c_char, failed: T, f: impl FnOnce() -> Result<T>) -> T {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            set_err(err, &e);
            failed
        }
        Err(panic) => {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown".into());
            set_err(err, &anyhow!("tantivy panicked: {msg}"));
            failed
        }
    }
}

fn text<'a>(p: *const c_char) -> Result<&'a str> {
    if p.is_null() {
        return Ok("");
    }
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map_err(|_| anyhow!("strings must be UTF-8"))
}

fn handle<'a, T>(p: *const T) -> Result<&'a T> {
    unsafe { p.as_ref() }.ok_or_else(|| anyhow!("null tantivy handle"))
}

fn data<'a, T>(p: *const T, len: usize) -> Result<&'a [T]> {
    if len == 0 {
        return Ok(&[]);
    }
    anyhow::ensure!(
        !p.is_null() && p.is_aligned(),
        "invalid tantivy data pointer"
    );
    anyhow::ensure!(
        len <= isize::MAX as usize / std::mem::size_of::<T>(),
        "tantivy data is too long"
    );
    Ok(unsafe { std::slice::from_raw_parts(p, len) })
}

/// A callback's message, from its NUL-terminated buffer.
fn message(buf: &[u8]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

fn splits<'a>(p: *const *const Split, n: usize) -> Result<Vec<&'a Split>> {
    data(p, n)?.iter().map(|&s| handle(s)).collect()
}

fn exclude(kind: c_int, data: *const c_void, len: usize) -> Result<Option<Exclude>> {
    Ok(match kind {
        0 => None,
        1 => Some(Exclude::from_roaring(self::data(data.cast::<u8>(), len)?)?),
        2 => Some(Exclude::from_id_slice(self::data(data.cast::<i64>(), len)?)),
        _ => return Err(anyhow!("unknown exclude kind {kind}")),
    })
}

fn writer(cb: WriteCb, ctx: *mut c_void) -> impl FnMut(&[u8]) -> Result<()> {
    move |bytes| {
        let mut msg = [0u8; MSG_CAP];
        match cb(
            ctx,
            bytes.as_ptr(),
            bytes.len() as u64,
            msg.as_mut_ptr().cast(),
            MSG_CAP,
        ) {
            0 => Ok(()),
            _ => Err(anyhow!("{}", message(&msg))),
        }
    }
}

/// The C++ context of a split's file: thread-safe, outlives the split.
struct Ctx(*mut c_void);
unsafe impl Send for Ctx {}
unsafe impl Sync for Ctx {}

#[no_mangle]
pub unsafe extern "C" fn tantivy_free_str(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

/// Start a split: `schema` is a tantivy schema as JSON, `options` (may be
/// NULL) build options as JSON. `threads` indexing threads (at least 1) share
/// the build's memory budget; `live` builds of the same query are already open,
/// and `max_memory` is DuckDB's memory limit in bytes (0: none). NULL + `*err`
/// on failure.
#[no_mangle]
pub unsafe extern "C" fn tantivy_build_open(
    schema: *const c_char,
    options: *const c_char,
    threads: usize,
    live: usize,
    max_memory: u64,
    err: *mut *mut c_char,
) -> *mut Build {
    guard(err, std::ptr::null_mut(), || {
        Ok(Box::into_raw(Box::new(Build::new(
            text(schema)?,
            text(options)?,
            threads,
            live,
            max_memory,
        )?)))
    })
}

/// Add one document (a JSON object), from any thread. 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn tantivy_build_add(
    b: *const Build,
    doc: *const c_char,
    len: usize,
    err: *mut *mut c_char,
) -> c_int {
    guard(err, -1, || {
        let build = handle(b)?;
        let doc = data(doc.cast::<u8>(), len)?;
        build.add(std::str::from_utf8(doc).map_err(|_| anyhow!("documents must be UTF-8"))?)?;
        Ok(0)
    })
}

/// Commit, write the split to `cb`, and free the build. Returns the documents
/// indexed, or -1.
#[no_mangle]
pub unsafe extern "C" fn tantivy_build_finish(
    b: *mut Build,
    cb: WriteCb,
    ctx: *mut c_void,
    err: *mut *mut c_char,
) -> i64 {
    guard(err, -1, || {
        handle(b)?;
        Ok(unsafe { Box::from_raw(b) }.finish(writer(cb, ctx))? as i64)
    })
}

/// Discard and free the build.
#[no_mangle]
pub unsafe extern "C" fn tantivy_build_abort(b: *mut Build) {
    if !b.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| drop(unsafe { Box::from_raw(b) })));
    }
}

/// Open the split of `size` bytes that `cb` reads, from any thread, with
/// `ctx`. NULL + `*err` on failure.
#[no_mangle]
pub unsafe extern "C" fn tantivy_split_open(
    size: u64,
    cb: ReadCb,
    ctx: *mut c_void,
    err: *mut *mut c_char,
) -> *mut Split {
    let ctx = Ctx(ctx);
    let read = move |offset: u64, buf: &mut [u8]| {
        let ctx = &ctx;
        let mut msg = [0u8; MSG_CAP];
        match cb(
            ctx.0,
            buf.as_mut_ptr(),
            buf.len() as u64,
            offset,
            msg.as_mut_ptr().cast(),
            MSG_CAP,
        ) {
            0 => Ok(()),
            _ => Err(std::io::Error::other(message(&msg))),
        }
    };
    guard(err, std::ptr::null_mut(), || {
        Ok(Box::into_raw(Box::new(Split::open(size, Arc::new(read))?)))
    })
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_split_close(s: *mut Split) {
    if !s.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| drop(unsafe { Box::from_raw(s) })));
    }
}

/// Search `n` splits with a query (tantivy's syntax, or OpenSearch query DSL
/// as JSON); `options` (may be NULL) is search options as JSON. Calls `cb` per
/// hit, best first: its split's position, score (NaN when the hits are ordered
/// by a field), doc as a JSON object and, if asked for, its snippets as a JSON
/// object (NULL otherwise). 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn tantivy_search(
    s: *const *const Split,
    n: usize,
    query: *const c_char,
    options: *const c_char,
    exclude_kind: c_int,
    exclude_data: *const c_void,
    exclude_len: usize,
    threads: usize,
    cb: HitCb,
    ctx: *mut c_void,
    err: *mut *mut c_char,
) -> c_int {
    guard(err, -1, || {
        let splits = splits(s, n)?;
        let exclude = exclude(exclude_kind, exclude_data, exclude_len)?;
        let request = Request::new(&splits, text(query)?, text(options)?, exclude.as_ref())?
            .with_threads(threads);
        for hit in request.hits()? {
            let (highlight, highlight_len) = match &hit.highlight {
                Some(snippets) => (snippets.as_ptr().cast(), snippets.len()),
                None => (std::ptr::null(), 0),
            };
            cb(
                ctx,
                hit.split,
                hit.score as f64,
                hit.doc.as_ptr().cast(),
                hit.doc.len(),
                highlight,
                highlight_len,
            );
        }
        Ok(0)
    })
}

/// The number of matches over `n` splits, or -1.
#[no_mangle]
pub unsafe extern "C" fn tantivy_count(
    s: *const *const Split,
    n: usize,
    query: *const c_char,
    options: *const c_char,
    exclude_kind: c_int,
    exclude_data: *const c_void,
    exclude_len: usize,
    threads: usize,
    err: *mut *mut c_char,
) -> i64 {
    guard(err, -1, || {
        let splits = splits(s, n)?;
        let exclude = exclude(exclude_kind, exclude_data, exclude_len)?;
        let request = Request::new(&splits, text(query)?, text(options)?, exclude.as_ref())?
            .with_threads(threads);
        Ok(request.count()? as i64)
    })
}

/// Tantivy aggregations (`aggs`, Elasticsearch's JSON) over the matches in
/// `n` splits, merged; the result in `*out` (free with `tantivy_free_str`).
/// 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn tantivy_aggregate(
    s: *const *const Split,
    n: usize,
    query: *const c_char,
    aggs: *const c_char,
    options: *const c_char,
    exclude_kind: c_int,
    exclude_data: *const c_void,
    exclude_len: usize,
    threads: usize,
    out: *mut *mut c_char,
    err: *mut *mut c_char,
) -> c_int {
    guard(err, -1, || {
        anyhow::ensure!(!out.is_null(), "null tantivy result pointer");
        let splits = splits(s, n)?;
        let exclude = exclude(exclude_kind, exclude_data, exclude_len)?;
        let request = Request::new(&splits, text(query)?, text(options)?, exclude.as_ref())?
            .with_threads(threads);
        let json = request.aggregate(text(aggs)?)?;
        unsafe { *out = CString::new(json)?.into_raw() };
        Ok(0)
    })
}

/// Merge `n` splits into one without the excluded documents, written to
/// `cb`. Returns the documents kept, or -1.
#[no_mangle]
pub unsafe extern "C" fn tantivy_merge(
    s: *const *const Split,
    n: usize,
    options: *const c_char,
    exclude_kind: c_int,
    exclude_data: *const c_void,
    exclude_len: usize,
    cb: WriteCb,
    ctx: *mut c_void,
    err: *mut *mut c_char,
) -> i64 {
    guard(err, -1, || {
        let splits = splits(s, n)?;
        let exclude = exclude(exclude_kind, exclude_data, exclude_len)?;
        Ok(split::merge(&splits, text(options)?, exclude.as_ref(), writer(cb, ctx))? as i64)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_panics_and_null_handles_without_aborting() {
        let mut err = std::ptr::null_mut();
        assert_eq!(
            guard(&mut err, -1, || -> Result<i32> { panic!("test panic") }),
            -1
        );
        assert!(unsafe { CStr::from_ptr(err) }
            .to_str()
            .unwrap()
            .contains("test panic"));
        unsafe { tantivy_free_str(err) };
        err = std::ptr::null_mut();
        assert_eq!(
            unsafe { tantivy_build_add(std::ptr::null(), std::ptr::null(), 0, &mut err) },
            -1
        );
        assert!(unsafe { CStr::from_ptr(err) }
            .to_str()
            .unwrap()
            .contains("null tantivy handle"));
        unsafe { tantivy_free_str(err) };
        assert!(splits(std::ptr::null(), 1).is_err());
        assert!(exclude(1, std::ptr::null(), 1).is_err());
        assert!(exclude(2, std::ptr::null(), 1).is_err());
        assert!(splits(std::ptr::null(), 0).unwrap().is_empty());
    }
}
