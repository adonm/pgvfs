//! Tantivy splits on pgvfs against a real PostgreSQL. Run by `just contract`,
//! serially: one writer connection serves every test (one writer lease per
//! database).
//!
//! PGVFS_TEST_DB_URL       the database under test
//! PGVFS_TEST_READER_URL   PGVFS_TEST_DB_URL as a SELECT-only role (optional)

use std::sync::OnceLock;

use anyhow::Result;
use pgvfs::index::{self, Build};
use pgvfs::PgvfsConn;

const SCHEMA: &str = r#"[
    {"name": "id", "type": "i64", "options": {"stored": true, "indexed": true}},
    {"name": "body", "type": "text", "options": {"indexing":
        {"record": "position", "fieldnorms": true, "tokenizer": "en_stem"}}}
]"#;

fn writer() -> &'static PgvfsConn {
    static CONN: OnceLock<&'static PgvfsConn> = OnceLock::new();
    CONN.get_or_init(|| {
        let url = std::env::var("PGVFS_TEST_DB_URL").expect("PGVFS_TEST_DB_URL");
        Box::leak(Box::new(PgvfsConn::connect(&url, 4).expect("connect")))
    })
}

fn volume(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("t-{tag}-{}-{nanos}", std::process::id())
}

fn build(conn: &'static PgvfsConn, vol: &str, path: &str, docs: &[(i64, &str)]) -> Result<u64> {
    let b = Build::open(conn, vol, path, SCHEMA, "")?;
    for (id, body) in docs {
        b.add(&serde_json::json!({"id": id, "body": body}).to_string())?;
    }
    b.commit()
}

/// (score, id) per hit, best first.
fn hits(
    conn: &'static PgvfsConn,
    vol: &str,
    path: &str,
    query: &str,
    options: &str,
) -> Result<Vec<(f32, i64)>> {
    let mut out = Vec::new();
    index::search(conn, vol, path, query, options, |score, doc| {
        let doc: serde_json::Value = serde_json::from_str(doc).unwrap();
        out.push((score, doc["id"].as_i64().unwrap()));
    })?;
    Ok(out)
}

fn ids(conn: &'static PgvfsConn, vol: &str, path: &str, query: &str) -> Result<Vec<i64>> {
    let mut ids: Vec<i64> = hits(conn, vol, path, query, "")?
        .into_iter()
        .map(|h| h.1)
        .collect();
    ids.sort();
    Ok(ids)
}

fn files(conn: &PgvfsConn, vol: &str, prefix: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    conn.list(vol, prefix, -1, |p| out.push(p.to_owned()))?;
    Ok(out)
}

#[test]
#[ignore]
fn builds_searches_and_drops_splits() -> Result<()> {
    let w = writer();
    let vol = volume("ix");
    let many: Vec<(i64, String)> = (0..5000)
        .map(|i| {
            (
                i,
                format!("doc {i} {}", if i % 10 == 0 { "foxes" } else { "cats" }),
            )
        })
        .collect();
    let many: Vec<(i64, &str)> = many.iter().map(|(i, s)| (*i, s.as_str())).collect();
    assert_eq!(build(w, &vol, "idx/a", &many)?, 5000);
    assert_eq!(ids(w, &vol, "idx/a", "fox")?.len(), 500);
    let top = hits(w, &vol, "idx/a", "fox", r#"{"top_k": 3}"#)?;
    assert_eq!(top.len(), 3);
    assert!(top.windows(2).all(|w| w[0].0 >= w[1].0), "{top:?}");
    assert_eq!(ids(w, &vol, "idx/a", "id:42")?, [42]);
    // Flat, merged to one segment.
    let a = files(w, &vol, "idx/a/")?;
    assert!(a.contains(&"idx/a/meta.json".to_owned()), "{a:?}");
    let mut segments: Vec<&str> = a
        .iter()
        .filter_map(|f| f.strip_prefix("idx/a/")?.split_once('.'))
        .map(|(s, _)| s)
        .filter(|s| !s.is_empty() && *s != "meta")
        .collect();
    segments.dedup();
    assert_eq!(segments.len(), 1, "{a:?}");
    // Splits are immutable.
    assert!(Build::open(w, &vol, "idx/a", SCHEMA, "").is_err());

    build(w, &vol, "idx/b", &[(9000, "a fox, a fox")])?;
    assert_eq!(ids(w, &vol, "idx/b", "fox")?, [9000]);

    // A split's files are flat: dropping a parent leaves nested splits.
    build(w, &vol, "idx", &[(1, "fox")])?;
    assert!(index::drop_index(w, &vol, "idx")?);
    assert_eq!(files(w, &vol, "idx/a/")?, a);
    assert_eq!(ids(w, &vol, "idx/b", "fox")?, [9000]);

    // One build per path at a time; an abandoned one leaves nothing.
    let c = Build::open(w, &vol, "idx/c", SCHEMA, "")?;
    assert!(Build::open(w, &vol, "idx/c", SCHEMA, "").is_err());
    c.add(r#"{"id": 9, "body": "fox"}"#)?;
    drop(c);
    assert!(files(w, &vol, "idx/c/")?.is_empty());

    assert!(index::drop_index(w, &vol, "idx/a")?);
    assert!(files(w, &vol, "idx/a/")?.is_empty());
    assert!(!index::drop_index(w, &vol, "idx/a")?);
    let err = hits(w, &vol, "idx/a", "fox", "").unwrap_err();
    assert!(format!("{err:#}").contains("no tantivy index"), "{err:#}");
    // Dropped, the path takes a new split.
    build(w, &vol, "idx/a", &[(7, "fox")])?;
    assert_eq!(ids(w, &vol, "idx/a", "fox")?, [7]);
    Ok(())
}

#[test]
#[ignore]
fn bad_input_fails_cleanly() -> Result<()> {
    let w = writer();
    let vol = volume("bad");
    assert!(Build::open(w, &vol, "s", "not json", "").is_err());
    assert!(Build::open(w, &vol, "s", SCHEMA, r#"{"nope": 1}"#).is_err());
    assert!(Build::open(w, &vol, "/", SCHEMA, "").is_err());
    assert!(files(w, &vol, "")?.is_empty());
    let b = Build::open(w, &vol, "s", SCHEMA, "")?;
    assert!(b.add("[1]").is_err());
    assert!(b.add(r#"{"id": "x"}"#).is_err());
    b.add(r#"{"id": 1, "body": null, "unknown": 2}"#)?;
    assert_eq!(b.commit()?, 1);
    assert_eq!(ids(w, &vol, "s", "id:1")?, [1]);
    assert!(hits(w, &vol, "s", "body:(", r#"{"strict": true}"#).is_err());
    assert!(hits(w, &vol, "s", "x", r#"{"top": 1}"#).is_err());
    Ok(())
}

#[test]
#[ignore]
fn readers_search_without_write_access() -> Result<()> {
    let Ok(url) = std::env::var("PGVFS_TEST_READER_URL") else {
        return Ok(());
    };
    let w = writer();
    let vol = volume("rd");
    build(w, &vol, "s", &[(1, "fox"), (2, "cat")])?;
    // See the writer's changes at once rather than after the open cache TTL.
    std::env::set_var("PGVFS_OPEN_CACHE_S", "0");
    let r: &'static PgvfsConn = Box::leak(Box::new(PgvfsConn::connect(&url, 2)?));
    assert_eq!(ids(r, &vol, "s", "fox")?, [1]);
    // A split dropped and rebuilt at the same path is reopened.
    assert!(index::drop_index(w, &vol, "s")?);
    build(w, &vol, "s", &[(3, "fox")])?;
    assert_eq!(ids(r, &vol, "s", "fox")?, [3]);
    assert!(Build::open(r, &vol, "other", SCHEMA, "").is_err());
    assert!(index::drop_index(r, &vol, "s").is_err());
    Ok(())
}
