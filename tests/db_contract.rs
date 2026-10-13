//! pgdb:// storage contract against a real PostgreSQL (db.rs). Run by `just
//! contract`, serially. PGVFS_TEST_DB_URL names the database under test; each
//! test uses its own database name.
//!
//! The writer's API blocks (it is called from DuckDB threads), so these tests
//! drive it from the test thread with a runtime of their own.

use std::sync::Arc;

use anyhow::Result;
use pgvfs::db::{self, Dbs, CHUNK};
use pgvfs::pg::Pool;
use pgvfs::store;
use tokio::runtime::Runtime;

struct Env {
    rt: Runtime,
    pool: Pool,
    dbs: Dbs,
}

fn env() -> Result<Env> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let pool = rt.block_on(async {
        let pool = store::connect(&std::env::var("PGVFS_TEST_DB_URL")?, 4).await?;
        db::ensure_schema(&pool).await?;
        Ok::<_, anyhow::Error>(pool)
    })?;
    Ok(Env {
        rt,
        pool,
        dbs: Dbs::default(),
    })
}

fn name(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{tag}-{}-{nanos}.duckdb", std::process::id())
}

impl Env {
    fn db(&self, name: &str, create: bool) -> Result<Arc<db::Db>> {
        Ok(self
            .rt
            .block_on(self.dbs.find(&self.pool, "t", name, create))?
            .expect("database"))
    }

    fn read(&self, db: &Arc<db::Db>, gen: Option<i64>, path: &str) -> Result<Option<Vec<u8>>> {
        let Some(v) = self
            .rt
            .block_on(db::open_version(&self.pool, db, gen, path))?
        else {
            return Ok(None);
        };
        let mut out = vec![0xAA; v.size as usize];
        self.rt.block_on(db::read_versions(
            &self.pool, db.db_id, v.file_id, v.gen, v.gen, 0, &mut out,
        ))?;
        Ok(Some(out))
    }

    fn latest(&self, name: &str) -> Result<i64> {
        Ok(self
            .rt
            .block_on(db::latest(&self.pool, "t", name))?
            .unwrap())
    }

    fn sql(&self, sql: &str) -> Result<()> {
        self.rt.block_on(async {
            self.pool.get().await?.batch_execute(sql).await?;
            Ok(())
        })
    }
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % n.max(1)
    }
}

/// Random writes (aligned blocks, appends and unaligned spans), truncations,
/// reads and syncs against an in-memory model of each file; after every sync
/// a reader at that gen sees exactly the model as it was.
#[test]
#[ignore]
fn writer_matches_a_model_at_every_gen() -> Result<()> {
    let e = env()?;
    for seed in [42, 7, 1234, 99] {
        model_run(&e, seed)?;
    }
    Ok(())
}

fn model_run(e: &Env, seed: u64) -> Result<()> {
    let name = name("model");
    let db = e.db(&name, true)?;
    let w = db.acquire_writer(e.rt.handle(), &e.pool)?;
    let paths = ["", ".wal"];
    let mut model: Vec<Vec<u8>> = vec![Vec::new(), Vec::new()];
    for p in paths {
        w.create(p, false)?;
    }
    let mut rng = Rng(seed);
    let mut snapshots: Vec<(i64, Vec<Vec<u8>>)> = Vec::new();
    let block = 262_144u64;
    for step in 0..400 {
        let i = rng.below(2) as usize;
        let len_now = model[i].len() as u64;
        match rng.below(10) {
            0..=4 => {
                let (pos, len) = match rng.below(3) {
                    // A DuckDB block at its place after the 12 KiB header.
                    0 => (12_288 + rng.below(12) * block, block),
                    // An append of a WAL record.
                    1 => (len_now, 1 + rng.below(20_000)),
                    _ => (rng.below(len_now + 30_000), 1 + rng.below(3 * CHUNK as u64)),
                };
                let data: Vec<u8> = (0..len).map(|_| rng.below(256) as u8).collect();
                w.write(paths[i], pos as i64, &data)?;
                let end = (pos + len) as usize;
                if model[i].len() < end {
                    model[i].resize(end, 0);
                }
                model[i][pos as usize..end].copy_from_slice(&data);
            }
            5 => {
                let size = rng.below(len_now + 1);
                w.truncate(paths[i], size as i64)?;
                model[i].truncate(size as usize);
            }
            6 | 7 if len_now > 0 => {
                let pos = rng.below(len_now);
                let len = 1 + rng.below((len_now - pos).min(4 * CHUNK as u64));
                let mut out = vec![0x55; len as usize];
                w.read(paths[i], pos as i64, &mut out)?;
                assert_eq!(
                    out,
                    model[i][pos as usize..(pos + len) as usize],
                    "step {step}: writer reads its writes"
                );
            }
            _ => {
                w.sync()?;
                snapshots.push((e.latest(&name)?, model.clone()));
            }
        }
        assert_eq!(w.size(paths[i]), Some(model[i].len() as i64));
    }
    w.sync()?;
    snapshots.push((e.latest(&name)?, model.clone()));
    for (gen, files) in &snapshots {
        for (i, p) in paths.iter().enumerate() {
            let got = e.read(&db, Some(*gen), p)?.expect("file");
            assert!(got == files[i], "seed {seed} gen {gen} file {p:?} differs");
        }
    }
    assert!(snapshots.len() > 20, "{} gens", snapshots.len());
    db.release_writer()?;
    Ok(())
}

/// A reader's gen never changes under it; a new gen shows the new bytes;
/// removed and renamed files follow the gen.
#[test]
#[ignore]
fn readers_keep_their_gen() -> Result<()> {
    let e = env()?;
    let name = name("gens");
    let db = e.db(&name, true)?;
    let w = db.acquire_writer(e.rt.handle(), &e.pool)?;
    w.create("", false)?;
    w.write("", 0, &[1; 20_000])?;
    w.sync()?;
    let g1 = e.latest(&name)?;
    w.write("", 5_000, &[2; 100])?;
    w.create(".wal.checkpoint", false)?;
    w.write(".wal.checkpoint", 0, b"wal")?;
    w.sync()?;
    let g2 = e.latest(&name)?;
    assert_eq!(g2, g1 + 1);
    assert_eq!(e.read(&db, Some(g1), "")?.unwrap(), vec![1; 20_000]);
    let mut want = vec![1; 20_000];
    want[5_000..5_100].fill(2);
    assert_eq!(e.read(&db, Some(g2), "")?.unwrap(), want);
    assert_eq!(e.read(&db, Some(g1), ".wal.checkpoint")?, None);
    w.rename(".wal.checkpoint", ".wal")?;
    w.sync()?;
    let g3 = e.latest(&name)?;
    assert_eq!(e.read(&db, Some(g3), ".wal")?.unwrap(), b"wal");
    assert_eq!(e.read(&db, Some(g3), ".wal.checkpoint")?, None);
    assert!(w.remove(".wal")?);
    w.sync()?;
    assert_eq!(e.read(&db, None, ".wal")?, None);
    assert_eq!(e.read(&db, Some(g3), ".wal")?.unwrap(), b"wal");
    // A sync with nothing written publishes nothing.
    let g = e.latest(&name)?;
    w.sync()?;
    assert_eq!(e.latest(&name)?, g);
    db.release_writer()?;
    Ok(())
}

/// One writer at a time; a writer that dies leaves its published gens and
/// nothing else; the next writer cleans up after it.
#[test]
#[ignore]
fn one_writer_and_recovery_from_a_dead_one() -> Result<()> {
    let e = env()?;
    let name = name("lock");
    let db = e.db(&name, true)?;
    let w = db.acquire_writer(e.rt.handle(), &e.pool)?;
    w.create("", false)?;
    w.write("", 0, &[7; 10_000])?;
    w.sync()?;
    let published = e.latest(&name)?;

    // Another process (its own Dbs) cannot write.
    let other = Dbs::default();
    let db2 =
        e.rt.block_on(other.find(&e.pool, "t", &name, false))?
            .unwrap();
    let refused = db2.acquire_writer(e.rt.handle(), &e.pool).err().unwrap();
    assert!(
        format!("{refused:#}").contains("attached for writing by another process"),
        "{refused:#}"
    );

    // Unpublished writes, then the writer's connection dies.
    w.write("", 0, &[9; 200_000])?;
    w.read("", 0, &mut [0; 10])?; // drains: the rows are in PostgreSQL
    e.sql(&format!(
        "SELECT pg_terminate_backend(pid) FROM pg_locks \
         WHERE locktype = 'advisory' AND objid = {} AND objsubid = 2",
        db.db_id
    ))?;
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert!(w.sync().is_err(), "a writer without its connection fails");

    let w2 = db2.acquire_writer(e.rt.handle(), &e.pool)?;
    assert_eq!(w2.size(""), Some(10_000));
    let mut out = vec![0; 10_000];
    w2.read("", 0, &mut out)?;
    assert_eq!(
        out,
        vec![7; 10_000],
        "the dead writer's unpublished rows are gone"
    );
    assert_eq!(e.latest(&name)?, published);
    w2.write("", 0, &[8; 5])?;
    w2.sync()?;
    db2.release_writer()?;
    let _ = db.release_writer();
    assert_eq!(&e.read(&db, None, "")?.unwrap()[..6], &[8, 8, 8, 8, 8, 7]);
    Ok(())
}

/// Reaping keeps the newest version at or below the horizon, removes the
/// rest, and a reader of an older gen gets EXPIRED instead of wrong bytes.
#[test]
#[ignore]
fn reaping_expires_old_gens_but_keeps_new_ones() -> Result<()> {
    let e = env()?;
    let name = name("reap");
    let db = e.db(&name, true)?;
    let w = db.acquire_writer(e.rt.handle(), &e.pool)?;
    w.create("", false)?;
    w.create(".wal", false)?;
    w.write("", 0, &[1; 3 * CHUNK as usize])?;
    w.write(".wal", 0, &[5; 100])?;
    w.sync()?;
    let g1 = e.latest(&name)?;
    w.write("", CHUNK, &[2; CHUNK as usize])?;
    w.truncate(".wal", 0)?;
    w.sync()?;
    let g2 = e.latest(&name)?;
    assert!(w.remove(".wal")?);
    w.sync()?;
    let g3 = e.latest(&name)?;

    let v1 =
        e.rt.block_on(db::open_version(&e.pool, &db, Some(g1), ""))?
            .unwrap();
    let rows = |e: &Env| -> Result<i64> {
        e.rt.block_on(async {
            Ok(e.pool
                .get()
                .await?
                .query_one(
                    "SELECT count(*) FROM pgvfs.dbchunks c JOIN pgvfs.dbfileids i USING (file_id) \
                     WHERE i.db_id = $1",
                    &[&db.db_id],
                )
                .await?
                .get(0))
        })
    };
    let before = rows(&e)?;
    let deleted = e.rt.block_on(db::reap(&e.pool, db.db_id, 0.0))?;
    assert!(deleted >= 2, "deleted {deleted}");
    assert_eq!(rows(&e)?, before - deleted as i64);
    // The newest gen reads as before; g1's superseded chunk is gone.
    let mut want = vec![1; 3 * CHUNK as usize];
    want[CHUNK as usize..2 * CHUNK as usize].fill(2);
    assert_eq!(e.read(&db, Some(g3), "")?.unwrap(), want);
    let mut out = vec![0; v1.size as usize];
    let err =
        e.rt.block_on(db::read_versions(
            &e.pool, db.db_id, v1.file_id, g1, g1, 0, &mut out,
        ))
        .unwrap_err();
    assert!(format!("{err:#}").contains(db::EXPIRED), "{err:#}");
    assert!(e
        .rt
        .block_on(db::open_version(&e.pool, &db, Some(g2), ""))
        .is_err());
    // Reading g1's bytes as of the horizon still works for unchanged chunks.
    let mut first = vec![0; CHUNK as usize];
    e.rt.block_on(db::read_versions(
        &e.pool, db.db_id, v1.file_id, g3, g3, 0, &mut first,
    ))?;
    assert_eq!(first, vec![1; CHUNK as usize]);
    db.release_writer()?;
    Ok(())
}

/// Many small commits (a version each, more than one reap batch): reaping
/// catches up and leaves only the newest version of each chunk.
#[test]
#[ignore]
fn reaping_catches_up_on_many_gens() -> Result<()> {
    let e = env()?;
    let name = name("many");
    let db = e.db(&name, true)?;
    let w = db.acquire_writer(e.rt.handle(), &e.pool)?;
    w.create(".wal", false)?;
    for i in 0..2500u32 {
        w.write(".wal", (i % 300) as i64 * 40, &i.to_le_bytes())?;
        w.sync()?;
    }
    let reaped = e.rt.block_on(db::reap(&e.pool, db.db_id, 0.0))?;
    assert!(reaped >= 2400, "reaped {reaped}");
    let (rows, versions): (i64, i64) = e.rt.block_on(async {
        let row = e
            .pool
            .get()
            .await?
            .query_one(
                "SELECT count(*), count(DISTINCT gen) FROM pgvfs.dbchunks c \
                 JOIN pgvfs.dbfileids i USING (file_id) WHERE i.db_id = $1",
                &[&db.db_id],
            )
            .await?;
        Ok::<_, anyhow::Error>((row.get(0), row.get(1)))
    })?;
    assert_eq!((rows, versions), (2, 2), "one version per chunk is left");
    let latest = e.read(&db, None, ".wal")?.unwrap();
    let mut want = vec![0u8; 300 * 40];
    for i in 0..2500u32 {
        let at = (i % 300) as usize * 40;
        want[at..at + 4].copy_from_slice(&i.to_le_bytes());
    }
    assert_eq!(&latest[..], &want[..latest.len()]);
    db.release_writer()?;
    Ok(())
}
