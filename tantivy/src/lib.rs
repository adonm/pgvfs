//! C ABI over tantivy splits (`split`), for the DuckDB tantivy extension
//! (extension/). The C++ side does all storage, through DuckDB's
//! filesystem, so splits live wherever DuckDB can read and write: local
//! files, object stores, pgvfs.
//!
//! Errors come back as `*err`, freed with `tantivy_free_str`. Callbacks
//! report theirs by writing a message of at most `cap` bytes (NUL included)
//! to `msg` and returning nonzero.
//!
//! Safety (every function): handles are those this library returned and not
//! yet freed; strings are NUL-terminated or come with their length; a split's
//! `ctx` outlives it.
#![allow(clippy::missing_safety_doc)]

pub mod split;

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::sync::Arc;

use anyhow::{anyhow, Result};
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
pub type HitCb = extern "C" fn(ctx: *mut c_void, score: f64, doc: *const c_char, len: usize);

const MSG_CAP: usize = 1024;

fn set_err(err: *mut *mut c_char, e: &anyhow::Error) {
    if !err.is_null() {
        let msg = format!("{e:#}").replace('\0', " ");
        unsafe { *err = CString::new(msg).unwrap_or_default().into_raw() };
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

/// A callback's message, from its NUL-terminated buffer.
fn message(buf: &[u8]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

fn status(result: Result<()>, err: *mut *mut c_char) -> c_int {
    match result {
        Ok(()) => 0,
        Err(e) => {
            set_err(err, &e);
            -1
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
/// NULL) build options as JSON. NULL + `*err` on failure.
#[no_mangle]
pub unsafe extern "C" fn tantivy_build_open(
    schema: *const c_char,
    options: *const c_char,
    err: *mut *mut c_char,
) -> *mut Build {
    match text(schema).and_then(|s| Build::new(s, text(options)?)) {
        Ok(b) => Box::into_raw(Box::new(b)),
        Err(e) => {
            set_err(err, &e);
            std::ptr::null_mut()
        }
    }
}

/// Add one document (a JSON object), from any thread. 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn tantivy_build_add(
    b: *const Build,
    doc: *const c_char,
    len: usize,
    err: *mut *mut c_char,
) -> c_int {
    let run = || {
        let doc = unsafe { std::slice::from_raw_parts(doc.cast::<u8>(), len) };
        let doc = std::str::from_utf8(doc).map_err(|_| anyhow!("documents must be UTF-8"))?;
        unsafe { &*b }.add(doc)
    };
    status(run(), err)
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
    let build = unsafe { Box::from_raw(b) };
    let result = build.finish(|bytes| {
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
    });
    match result {
        Ok(n) => n as i64,
        Err(e) => {
            set_err(err, &e);
            -1
        }
    }
}

/// Discard and free the build.
#[no_mangle]
pub unsafe extern "C" fn tantivy_build_abort(b: *mut Build) {
    if !b.is_null() {
        drop(unsafe { Box::from_raw(b) });
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
    match Split::open(size, Arc::new(read)) {
        Ok(s) => Box::into_raw(Box::new(s)),
        Err(e) => {
            set_err(err, &e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tantivy_split_close(s: *mut Split) {
    if !s.is_null() {
        drop(unsafe { Box::from_raw(s) });
    }
}

/// Search with a tantivy query; `options` (may be NULL) is search options as
/// JSON. Calls `cb` per hit, best first, with its score and stored fields as a
/// JSON object. 0 ok, -1 error.
#[no_mangle]
pub unsafe extern "C" fn tantivy_split_search(
    s: *const Split,
    query: *const c_char,
    options: *const c_char,
    cb: HitCb,
    ctx: *mut c_void,
    err: *mut *mut c_char,
) -> c_int {
    let run = || -> Result<()> {
        for (score, doc) in unsafe { &*s }.search(text(query)?, text(options)?)? {
            cb(ctx, score as f64, doc.as_ptr().cast(), doc.len());
        }
        Ok(())
    };
    status(run(), err)
}
