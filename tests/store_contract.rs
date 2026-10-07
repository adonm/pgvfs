//! pgvfs storage contract against a real PostgreSQL. Run by `just contract`,
//! serially (`--test-threads=1`): there is one writer lease per database.
//!
//! PGVFS_TEST_DB_URL       the database under test
//! PGVFS_TEST_EMPTY_DB_URL a database with no pgvfs layout (optional)
//! PGVFS_TEST_READER_URL   PGVFS_TEST_DB_URL as a SELECT-only role (optional)
//! PGVFS_TEST_S3_DB_URL    a database holding an `s3p` schema (optional)
//! PGVFS_TEST_STANDBY_URL  PGVFS_TEST_DB_URL's database on a streaming standby (optional)
//! PGVFS_TEST_TLS_URL      a TLS-only server (sslmode=require), and
//! PGVFS_TEST_TLS_CA       the CA that signed its certificate (optional)

use anyhow::Result;
use pgvfs::pg::Pool;
use pgvfs::store::WriterLease;
use pgvfs::store::{self, WriteMsg, ROW_BYTES, WRITE_BATCH};
use tokio_postgres::types::Type;

/// A pool plus this test's writer lease (which installs the layout).
async fn writer() -> Result<(Pool, WriterLease)> {
    let pool = store::connect(
        &std::env::var("PGVFS_TEST_DB_URL")?,
        store::default_threads(),
    )
    .await?;
    store::verify(&pool).await?;
    let lease = store::acquire_writer(&pool).await?;
    Ok((pool, lease))
}

fn volume(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("t-{tag}-{}-{nanos}", std::process::id())
}

fn bytes(n: usize, seed: u8) -> Vec<u8> {
    (0..n)
        .map(|i| {
            (i as u64)
                .wrapping_mul(2654435761)
                .wrapping_add(seed as u64) as u8
        })
        .collect()
}

async fn write(pool: &Pool, volume: &str, path: &str, data: &[u8]) -> Result<()> {
    let (tx, task) = store::spawn_writer(
        &tokio::runtime::Handle::current(),
        pool.clone(),
        volume.into(),
        path.into(),
    )?;
    for batch in data.chunks(WRITE_BATCH) {
        tx.send(WriteMsg::Data(batch.to_vec().into())).await?;
    }
    tx.send(WriteMsg::Publish).await?;
    task.await?
}

async fn count(pool: &Pool, sql: &str, id: i64) -> Result<i64> {
    let conn = pool.get().await?;
    Ok(conn
        .query_typed_one(sql, &[(&id, Type::INT8)])
        .await?
        .try_get(0)?)
}

#[tokio::test]
#[ignore]
async fn round_trips_exact_ranges() -> Result<()> {
    let (pool, _lease) = writer().await?;
    let vol = volume("rt");
    let row = ROW_BYTES as usize;
    for (i, n) in [
        0,
        1,
        row - 1,
        row,
        row + 1,
        WRITE_BATCH + 5,
        3 * 1032 * row + 17,
    ]
    .into_iter()
    .enumerate()
    {
        let data = bytes(n, i as u8);
        let path = format!("d/f{i}.bin");
        write(&pool, &vol, &path, &data).await?;
        let f = store::open(&pool, &vol, &path).await?.expect("published");
        assert_eq!(f.size, n as i64);
        assert_eq!(store::read_all(&pool, &f).await?, data, "size {n}");
        for (pos, len) in [
            (0, 1),
            (row - 3, 7),
            (n / 3, n / 2),
            (n.saturating_sub(5), 5),
        ] {
            if pos + len > n || len == 0 {
                continue;
            }
            let mut buf = vec![0; len];
            store::read_at(&pool, &f, pos as i64, &mut buf).await?;
            assert_eq!(buf, data[pos..pos + len], "size {n} at {pos}+{len}");
        }
        let mut past = vec![0; 1];
        assert!(store::read_at(&pool, &f, n as i64, &mut past)
            .await
            .is_err());
    }
    Ok(())
}

#[tokio::test]
#[ignore]
async fn abandoned_write_leaves_nothing() -> Result<()> {
    let (pool, _lease) = writer().await?;
    let vol = volume("ab");
    let (tx, task) = store::spawn_writer(
        &tokio::runtime::Handle::current(),
        pool.clone(),
        vol.clone(),
        "x".into(),
    )?;
    tx.send(WriteMsg::Data(bytes(WRITE_BATCH, 1).into()))
        .await?;
    drop(tx);
    assert!(task.await?.is_err());
    assert!(store::open(&pool, &vol, "x").await?.is_none());
    let conn = pool.get().await?;
    let orphans: i64 = conn
        .query_typed_one(
            "SELECT count(*) FROM pgvfs.chunks c WHERE NOT EXISTS \
             (SELECT 1 FROM pgvfs.files f WHERE f.file_id = c.file_id) \
             AND NOT EXISTS (SELECT 1 FROM pgvfs.garbage g WHERE g.file_id = c.file_id)",
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(orphans, 0);
    Ok(())
}

#[tokio::test]
#[ignore]
async fn overwrite_queues_old_file_and_reap_honours_grace() -> Result<()> {
    let (pool, _lease) = writer().await?;
    let vol = volume("ow");
    write(&pool, &vol, "a", &bytes(3 * ROW_BYTES as usize, 1)).await?;
    let old = store::open(&pool, &vol, "a").await?.unwrap();
    write(&pool, &vol, "a", &bytes(10, 2)).await?;
    let new = store::open(&pool, &vol, "a").await?.unwrap();
    assert_ne!(old.file_id, new.file_id);
    assert_eq!(store::read_all(&pool, &new).await?, bytes(10, 2));
    let garbage = "SELECT count(*) FROM pgvfs.garbage WHERE file_id = $1";
    let chunks = "SELECT count(*) FROM pgvfs.chunks WHERE file_id = $1";
    assert_eq!(count(&pool, garbage, old.file_id).await?, 1);
    // Inside the grace period the old bytes stay readable.
    store::reap(&pool).await?;
    assert_eq!(store::read_all(&pool, &old).await?.len(), old.size as usize);
    let conn = pool.get().await?;
    conn.query_typed_one("SELECT pgvfs.reap(interval '0 seconds')", &[])
        .await?;
    assert_eq!(count(&pool, chunks, old.file_id).await?, 0);
    assert_eq!(count(&pool, garbage, old.file_id).await?, 0);
    let err = store::read_all(&pool, &old).await.unwrap_err();
    assert!(format!("{err:#}").contains("missing"), "{err:#}");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn list_remove_rename() -> Result<()> {
    let (pool, _lease) = writer().await?;
    let vol = volume("ls");
    for p in ["a/1", "a/2", "a/b/3", "ab", "c"] {
        write(&pool, &vol, p, p.as_bytes()).await?;
    }
    assert_eq!(
        store::list(&pool, &vol, "a/", "", 100).await?,
        ["a/1", "a/2", "a/b/3"]
    );
    assert_eq!(
        store::list(&pool, &vol, "a", "a/1", 2).await?,
        ["a/2", "a/b/3"]
    );
    assert_eq!(
        store::list(&pool, &vol, "a_", "", 100).await?,
        Vec::<String>::new()
    );

    assert!(store::remove(&pool, &vol, "c").await?);
    assert!(!store::remove(&pool, &vol, "c").await?);

    store::rename(&pool, &vol, "ab", "a/1").await?;
    let f = store::open(&pool, &vol, "a/1").await?.unwrap();
    assert_eq!(store::read_all(&pool, &f).await?, b"ab");
    assert!(store::open(&pool, &vol, "ab").await?.is_none());
    assert!(store::rename(&pool, &vol, "missing", "x").await.is_err());

    for p in ["a/1", "a/2", "a/b/3"] {
        assert!(store::remove(&pool, &vol, p).await?);
    }
    assert!(store::list(&pool, &vol, "", "", 100).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore]
async fn removes_prefixes_and_volumes() -> Result<()> {
    let (pool, _lease) = writer().await?;
    let vol = volume("rp");
    let other = volume("rq");
    for p in ["a/1", "a/2", "a/b/3", "ab", "b/4"] {
        write(&pool, &vol, p, b"x").await?;
    }
    write(&pool, &other, "a/1", b"y").await?;
    let queued = |id| {
        count(
            &pool,
            "SELECT count(*) FROM pgvfs.garbage WHERE file_id = $1",
            id,
        )
    };
    let a1 = store::open(&pool, &vol, "a/1").await?.unwrap();
    assert_eq!(store::remove_prefix(&pool, &vol, "a/").await?, 3);
    assert_eq!(
        queued(a1.file_id).await?,
        1,
        "rows wait out the grace period"
    );
    assert_eq!(store::list(&pool, &vol, "", "", 100).await?, ["ab", "b/4"]);
    assert_eq!(store::remove_prefix(&pool, &vol, "").await?, 2);
    assert_eq!(store::remove_prefix(&pool, &vol, "").await?, 0);
    assert!(
        store::open(&pool, &other, "a/1").await?.is_some(),
        "other volumes stay"
    );
    Ok(())
}

#[tokio::test]
#[ignore]
async fn one_writer_at_a_time() -> Result<()> {
    let (pool, lease) = writer().await?;
    let err = store::acquire_writer(&pool)
        .await
        .err()
        .expect("second writer refused");
    assert!(
        format!("{err:#}").contains("another pgvfs writer"),
        "{err:#}"
    );
    lease.check()?;
    drop(lease);
    // The lock goes with the connection; the server notices it close.
    for _ in 0..50 {
        if store::acquire_writer(&pool).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    anyhow::bail!("lease not released after its connection closed")
}

#[tokio::test]
#[ignore]
async fn readers_need_no_layout_and_no_write_access() -> Result<()> {
    if let Ok(url) = std::env::var("PGVFS_TEST_EMPTY_DB_URL") {
        let pool = store::connect(&url, store::default_threads()).await?;
        store::verify(&pool).await?;
        assert!(store::open(&pool, "lake", "x").await?.is_none());
        assert!(store::list(&pool, "lake", "", "", 10).await?.is_empty());
    }
    if let Ok(url) = std::env::var("PGVFS_TEST_READER_URL") {
        let vol = volume("ro");
        {
            let (pool, _lease) = writer().await?;
            write(&pool, &vol, "f", &bytes(3 * ROW_BYTES as usize, 7)).await?;
        }
        let pool = store::connect(&url, store::default_threads()).await?;
        store::verify(&pool).await?;
        assert_eq!(store::list(&pool, &vol, "", "", 10).await?, ["f"]);
        let f = store::open(&pool, &vol, "f").await?.unwrap();
        assert_eq!(
            store::read_all(&pool, &f).await?,
            bytes(3 * ROW_BYTES as usize, 7)
        );
        assert!(
            store::remove(&pool, &vol, "f").await.is_err(),
            "reader must not delete"
        );
    }
    Ok(())
}

/// Terminate other backends of the pool's database matching `query_like`.
async fn kill_backends(pool: &Pool, query_like: &str) -> Result<i64> {
    let conn = pool.get().await?;
    Ok(conn
        .query_typed_one(
            "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
             WHERE datname = current_database() AND pid <> pg_backend_pid() AND query LIKE $1",
            &[(&query_like, Type::TEXT)],
        )
        .await?
        .try_get(0)?)
}

#[tokio::test]
#[ignore]
async fn reads_survive_lost_connections() -> Result<()> {
    let (pool, _lease) = writer().await?;
    let vol = volume("lost");
    let data = bytes(3 * ROW_BYTES as usize, 3);
    write(&pool, &vol, "f", &data).await?;
    let f = store::open(&pool, &vol, "f").await?.unwrap();
    for _ in 0..3 {
        assert_eq!(store::read_all(&pool, &f).await?, data);
        // Every pooled reader connection dies (as in a PostgreSQL restart
        // or a network blip); the next open, list and read must still work.
        kill_backends(&pool, "SELECT%").await?;
        assert!(store::open(&pool, &vol, "f").await?.is_some());
        assert_eq!(store::list(&pool, &vol, "", "", 10).await?, ["f"]);
    }
    Ok(())
}

#[tokio::test]
#[ignore]
async fn interrupted_write_publishes_nothing() -> Result<()> {
    let (pool, _lease) = writer().await?;
    let vol = volume("cut");
    let (tx, task) = store::spawn_writer(
        &tokio::runtime::Handle::current(),
        pool.clone(),
        vol.clone(),
        "x".into(),
    )?;
    tx.send(WriteMsg::Data(bytes(WRITE_BATCH, 1).into()))
        .await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(kill_backends(&pool, "COPY pgvfs.chunks%").await?, 1);
    let _ = tx.send(WriteMsg::Data(bytes(WRITE_BATCH, 2).into())).await;
    let _ = tx.send(WriteMsg::Publish).await;
    assert!(
        task.await?.is_err(),
        "a write whose connection died must fail"
    );
    assert!(store::open(&pool, &vol, "x").await?.is_none());
    let conn = pool.get().await?;
    let orphans: i64 = conn
        .query_typed_one(
            "SELECT count(*) FROM pgvfs.chunks c WHERE NOT EXISTS \
             (SELECT 1 FROM pgvfs.files f WHERE f.file_id = c.file_id) \
             AND NOT EXISTS (SELECT 1 FROM pgvfs.garbage g WHERE g.file_id = c.file_id)",
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(orphans, 0);
    Ok(())
}

#[tokio::test]
#[ignore]
async fn lost_writer_lock_stops_writes() -> Result<()> {
    let (pool, lease) = writer().await?;
    lease.check()?;
    let conn = pool.get().await?;
    let killed: i64 = conn
        .query_typed_one(
            "SELECT count(pg_terminate_backend(pid)) FROM pg_locks \
             WHERE locktype = 'advisory' AND granted AND pid <> pg_backend_pid()",
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(killed, 1, "the writer holds exactly one advisory lock");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(lease.check().is_err(), "a lost lock must be noticed");
    drop(lease);
    // Once lost, another writer can take over.
    let again = store::acquire_writer(&pool).await?;
    again.check()?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn readers_see_consistent_files_while_writing() -> Result<()> {
    let (pool, _lease) = writer().await?;
    let vol = volume("mix");
    let len = |i: usize| (i * 7919) % (5 * ROW_BYTES as usize) + 1;
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut readers = Vec::new();
    for r in 0..4 {
        let (pool, vol, done) = (pool.clone(), vol.clone(), done.clone());
        readers.push(tokio::spawn(async move {
            let mut checked = 0;
            while !done.load(std::sync::atomic::Ordering::Relaxed) || checked == 0 {
                for path in store::list(&pool, &vol, "", "", 1000).await? {
                    let i: usize = path[1..].parse()?;
                    // Removed between list and open: fine, as long as what
                    // is there is exactly what was written.
                    let Some(f) = store::open(&pool, &vol, &path).await? else {
                        continue;
                    };
                    let got = store::read_all(&pool, &f).await?;
                    anyhow::ensure!(got == bytes(len(i), i as u8), "reader {r}: {path} corrupt");
                    checked += 1;
                }
            }
            Ok::<_, anyhow::Error>(checked)
        }));
    }
    for i in 0..40 {
        write(&pool, &vol, &format!("f{i}"), &bytes(len(i), i as u8)).await?;
        if i % 5 == 4 {
            assert!(store::remove(&pool, &vol, &format!("f{}", i - 2)).await?);
        }
    }
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    for r in readers {
        assert!(r.await?? > 0);
    }
    Ok(())
}

#[tokio::test]
#[ignore]
async fn offsets_beyond_4_gib() -> Result<()> {
    // Rows around byte 4.5 GiB of a 5 GiB file, without writing 5 GiB: the
    // offset arithmetic must not wrap anywhere in 32 bits.
    let (pool, _lease) = writer().await?;
    let vol = volume("big");
    let row = ROW_BYTES as usize;
    let first = (4_500_000_000i64 / ROW_BYTES) as i32;
    let size = 5_000_000_000i64;
    let conn = pool.get().await?;
    let id: i64 = conn
        .query_typed_one("SELECT nextval('pgvfs.file_ids')", &[])
        .await?
        .try_get(0)?;
    let data = bytes(3 * row, 7);
    for k in 0..3i32 {
        let chunk = &data[k as usize * row..(k as usize + 1) * row];
        let no = first + k;
        conn.execute_typed(
            "INSERT INTO pgvfs.chunks (file_id, no, data) VALUES ($1, $2, $3)",
            &[(&id, Type::INT8), (&no, Type::INT4), (&chunk, Type::BYTEA)],
        )
        .await?;
    }
    conn.execute_typed(
        "INSERT INTO pgvfs.files (volume, path, file_id, size) VALUES ($1, 'big', $2, $3)",
        &[(&vol, Type::TEXT), (&id, Type::INT8), (&size, Type::INT8)],
    )
    .await?;
    let f = store::open(&pool, &vol, "big").await?.unwrap();
    assert_eq!(f.size, size);
    let at = first as i64 * ROW_BYTES + ROW_BYTES / 2;
    let mut buf = vec![0; row];
    store::read_at(&pool, &f, at, &mut buf).await?;
    assert_eq!(buf, data[row / 2..row / 2 + row]);
    Ok(())
}

#[tokio::test]
#[ignore]
async fn readers_work_on_a_standby() -> Result<()> {
    let Ok(url) = std::env::var("PGVFS_TEST_STANDBY_URL") else {
        return Ok(());
    };
    let vol = volume("sb");
    let data = bytes(4 * ROW_BYTES as usize + 9, 5);
    {
        let (pool, _lease) = writer().await?;
        write(&pool, &vol, "f", &data).await?;
    }
    let standby = store::connect(&url, 2).await?;
    store::verify(&standby).await?;
    let mut found = None;
    for _ in 0..100 {
        found = store::open(&standby, &vol, "f").await?;
        if found.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let f = found.expect("the file replicated to the standby");
    assert_eq!(store::read_all(&standby, &f).await?, data);
    assert_eq!(store::list(&standby, &vol, "", "", 10).await?, ["f"]);
    let err = store::acquire_writer(&standby)
        .await
        .err()
        .expect("no writer on a standby");
    assert!(format!("{err:#}").contains("standby"), "{err:#}");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn tls_verifies_the_server() -> Result<()> {
    let (Ok(url), Ok(ca)) = (
        std::env::var("PGVFS_TEST_TLS_URL"),
        std::env::var("PGVFS_TEST_TLS_CA"),
    ) else {
        return Ok(());
    };
    // Tests run serially, so the process environment is ours.
    std::env::set_var("PGVFS_DB_CA_FILE", &ca);
    let pool = store::connect(&url, 2).await?;
    let conn = pool.get().await?;
    let ssl: bool = conn
        .query_typed_one(
            "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .await?
        .try_get(0)?;
    assert!(ssl, "sslmode=require must use TLS");
    drop(conn);
    std::env::remove_var("PGVFS_DB_CA_FILE");
    let err = store::connect(&url, 2)
        .await
        .err()
        .expect("an unknown CA must be refused");
    assert!(
        format!("{err:#}").to_lowercase().contains("certificate"),
        "{err:#}"
    );
    Ok(())
}

#[tokio::test]
#[ignore]
async fn refuses_an_s3_gateway_database() -> Result<()> {
    let Ok(url) = std::env::var("PGVFS_TEST_S3_DB_URL") else {
        return Ok(());
    };
    let pool = store::connect(&url, store::default_threads()).await?;
    let err = store::verify(&pool).await.unwrap_err();
    assert!(format!("{err:#}").contains("S3 gateway"), "{err:#}");
    Ok(())
}

#[test]
fn volume_names() {
    for ok in ["lake", "a", "l-1.x_y"] {
        store::check_volume(ok).unwrap();
    }
    for bad in ["", "Lake", "-a", "a/b", &"x".repeat(64)] {
        assert!(store::check_volume(bad).is_err(), "{bad}");
    }
}
