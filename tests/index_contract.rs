//! Tantivy indexes on pgvfs against a real PostgreSQL. Run by `just contract`,
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
const PATH: &str = "idx/docs";

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

fn build(conn: &'static PgvfsConn, vol: &str, docs: &[(i64, String)]) -> Result<u64> {
    let b = Build::open(conn, vol, PATH, SCHEMA, "")?;
    for (id, body) in docs {
        b.add(&serde_json::json!({"id": id, "body": body}).to_string())?;
    }
    b.commit()
}

fn ids(conn: &'static PgvfsConn, vol: &str, query: &str, options: &str) -> Result<Vec<i64>> {
    let mut out = Vec::new();
    index::search(conn, vol, PATH, query, options, |_, doc| {
        let doc: serde_json::Value = serde_json::from_str(doc).unwrap();
        out.push(doc["id"].as_i64().unwrap());
    })?;
    Ok(out)
}

fn files(conn: &PgvfsConn, vol: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    conn.list(vol, "", -1, |p| out.push(p.to_owned()))?;
    Ok(out)
}

fn generations(files: &[String]) -> Vec<String> {
    let mut gens: Vec<String> = files
        .iter()
        .filter_map(|f| f.strip_prefix("idx/docs/")?.split_once('/'))
        .map(|(g, _)| g.to_owned())
        .collect();
    gens.dedup();
    gens
}

fn docs(n: i64) -> Vec<(i64, String)> {
    (0..n)
        .map(|i| {
            (
                i,
                format!("doc {i} {}", if i % 10 == 0 { "foxes" } else { "cats" }),
            )
        })
        .collect()
}

#[test]
#[ignore]
fn builds_searches_rebuilds_and_drops() -> Result<()> {
    let w = writer();
    let vol = volume("ix");
    assert_eq!(build(w, &vol, &docs(5000))?, 5000);
    assert_eq!(ids(w, &vol, "fox", "")?.len(), 500);
    assert_eq!(ids(w, &vol, "fox", r#"{"limit": 3}"#)?.len(), 3);
    assert_eq!(ids(w, &vol, "id:42", "")?, [42]);
    let first = files(w, &vol)?;
    assert!(first.contains(&"idx/docs/current".to_owned()), "{first:?}");
    assert_eq!(generations(&first).len(), 1, "{first:?}");

    // A rebuild replaces the index at once and removes the old generation.
    build(w, &vol, &[(1, "a fox".into())])?;
    assert_eq!(ids(w, &vol, "fox", "")?, [1]);
    let second = files(w, &vol)?;
    assert_eq!(generations(&second).len(), 1, "{second:?}");
    assert_ne!(generations(&first), generations(&second));

    // One build per index at a time; an abandoned one leaves nothing.
    let b = Build::open(w, &vol, PATH, SCHEMA, "")?;
    assert!(Build::open(w, &vol, PATH, SCHEMA, "").is_err());
    b.add(r#"{"id": 9, "body": "fox"}"#)?;
    drop(b);
    assert_eq!(files(w, &vol)?, second);
    assert_eq!(ids(w, &vol, "fox", "")?, [1]);

    assert!(index::drop_index(w, &vol, PATH)?);
    assert!(files(w, &vol)?.is_empty());
    assert!(!index::drop_index(w, &vol, PATH)?);
    let err = index::search(w, &vol, PATH, "fox", "", |_, _| {}).unwrap_err();
    assert!(format!("{err:#}").contains("no tantivy index"), "{err:#}");
    Ok(())
}

#[test]
#[ignore]
fn bad_input_fails_cleanly() -> Result<()> {
    let w = writer();
    let vol = volume("bad");
    assert!(Build::open(w, &vol, PATH, "not json", "").is_err());
    assert!(Build::open(w, &vol, PATH, SCHEMA, r#"{"nope": 1}"#).is_err());
    assert!(Build::open(w, &vol, "/", SCHEMA, "").is_err());
    let b = Build::open(w, &vol, PATH, SCHEMA, "")?;
    assert!(b.add("[1]").is_err());
    assert!(b.add(r#"{"id": "x"}"#).is_err());
    b.add(r#"{"id": 1, "body": null, "unknown": 2}"#)?;
    assert_eq!(b.commit()?, 1);
    assert_eq!(ids(w, &vol, "id:1", "")?, [1]);
    let strict = index::search(w, &vol, PATH, "body:(", r#"{"strict": true}"#, |_, _| {});
    assert!(strict.is_err());
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
    build(w, &vol, &[(1, "fox".into()), (2, "cat".into())])?;
    // See the writer's rebuilds at once rather than after the open cache TTL.
    std::env::set_var("PGVFS_OPEN_CACHE_S", "0");
    let r: &'static PgvfsConn = Box::leak(Box::new(PgvfsConn::connect(&url, 2)?));
    assert_eq!(ids(r, &vol, "fox", "")?, [1]);
    build(w, &vol, &[(3, "fox".into())])?;
    assert_eq!(ids(r, &vol, "fox", "")?, [3]);
    assert!(Build::open(r, &vol, "idx/other", SCHEMA, "").is_err());
    assert!(index::drop_index(r, &vol, PATH).is_err());
    Ok(())
}
