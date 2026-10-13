"""pgdb:// end to end: a native DuckDB database in PostgreSQL, one writer and
readers in other DuckDB instances (each its own pgvfs connection, as another
process would have). Run by e2e.py, or alone:

    PGVFS_TEST_URL=postgres://... python pgdb.py path/to/pgvfs.duckdb_extension
"""

import json
import os
import subprocess
import sys
import time
from urllib.parse import unquote, urlsplit

import duckdb


def check_pgdb(ext, url, standby_url=None, reader_url=None):
    vol = f"pgdb-{os.getpid()}-{int(time.time())}"
    path = f"pgdb://{vol}/sales.duckdb"

    def connect(u=url):
        con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        con.execute(f"LOAD '{ext}'")
        con.execute(
            f"CREATE SECRET (TYPE postgres, HOST '{u.hostname}', PORT {u.port or 5432}, "
            f"USER '{unquote(u.username)}', PASSWORD '{unquote(u.password)}', "
            f"DATABASE '{u.path.lstrip('/')}')"
        )
        return con

    def one(con, sql, *args):
        return con.execute(sql, args).fetchone()

    def stats(con):
        return json.loads(one(con, "SELECT pgvfs_stats()")[0])

    def raises(fn, text):
        try:
            fn()
        except duckdb.Error as e:
            assert text in str(e), str(e)
            return
        raise AssertionError(f"expected an error with {text!r}")

    # The writer: a new database, with everything a table format cannot keep.
    w = connect()
    w.execute(f"ATTACH '{path}' AS w")
    w.execute("USE w")
    w.execute("""
        CREATE TABLE customers (id BIGINT PRIMARY KEY, email VARCHAR UNIQUE NOT NULL);
        CREATE TABLE orders (
            id BIGINT PRIMARY KEY,
            customer_id BIGINT REFERENCES customers(id),
            amount DECIMAL(12, 2) CHECK (amount >= 0));
        CREATE INDEX orders_by_customer ON orders(customer_id);
        CREATE SEQUENCE seq;
        CREATE VIEW big_orders AS SELECT * FROM orders WHERE amount > 500;
    """)
    w.execute("USE memory")
    w.execute("INSERT INTO w.customers SELECT i, 'c' || i || '@x' FROM range(1000) t(i)")
    w.execute("INSERT INTO w.orders SELECT i, i % 1000, (i % 1000)::DECIMAL(12,2) FROM range(2000000) t(i)")
    w.execute("CHECKPOINT w")
    raises(lambda: w.execute("INSERT INTO w.orders VALUES (-1, 99999, 1)"), "foreign key")
    raises(lambda: w.execute("INSERT INTO w.customers VALUES (5000, 'c1@x')"), "unique")
    raises(lambda: w.execute("INSERT INTO w.orders VALUES (-2, 1, -5)"), "CHECK")
    # Committed but not checkpointed: in the WAL.
    w.execute("INSERT INTO w.orders VALUES (2000000, 7, 42)")
    expected = one(w, "SELECT count(*), sum(amount) FROM w.orders")
    assert expected[0] == 2000001, expected

    # A second writer elsewhere is refused at once.
    raises(lambda: connect().execute(f"ATTACH '{path}' AS w2"), "attached for writing by another process")
    # In the writer's DuckDB: a second read-write attach is refused, a
    # read-only one reads the published version.
    raises(lambda: w.execute(f"ATTACH '{path}' AS w2"), "already attached for writing")
    w.execute(f"ATTACH '{path}' AS wr (READ_ONLY)")
    assert one(w, "SELECT count(*), sum(amount) FROM wr.orders") == expected
    w.execute("DETACH wr")

    # A reader elsewhere: the same database, WAL replayed, constraints and all.
    r = connect()
    r.execute("SET pgdb_refresh_interval = 0.5")
    r.execute(f"ATTACH '{path}' AS r (READ_ONLY)")
    attached = one(r, "SELECT path FROM duckdb_databases() WHERE database_name = 'r'")[0]
    assert attached.startswith(path + "@"), attached
    assert one(r, "SELECT count(*), sum(amount) FROM r.orders") == expected
    assert one(r, "SELECT count(*) FROM r.big_orders") == (one(w, "SELECT count(*) FROM w.big_orders")[0],)
    assert one(r, "SELECT count(*) FROM duckdb_constraints() WHERE database_name = 'r'")[0] >= 5
    assert one(r, "SELECT count(*) FROM duckdb_indexes() WHERE database_name = 'r'") == (1,)
    assert one(r, "SELECT amount FROM r.orders WHERE id = 2000000") == (42,)
    raises(lambda: r.execute("INSERT INTO r.orders VALUES (-3, 1, 1)"), "read-only")

    # Warm reads come from DuckDB's external file cache, not PostgreSQL.
    r.execute("SELECT sum(amount) FROM r.orders").fetchall()
    s0 = stats(r)
    r.execute("SELECT sum(amount) FROM r.orders").fetchall()
    s1 = stats(r)
    assert s1["db_cache_bytes"] >= s0["db_cache_bytes"]

    # New commits reach the reader by itself; a running transaction keeps its version.
    r.execute("PREPARE counted AS SELECT count(*) FROM r.orders")
    r.execute("BEGIN")
    before = one(r, "SELECT count(*) FROM r.orders")[0]
    w.execute("INSERT INTO w.orders SELECT i, 3, 1 FROM range(2000001, 2000101) t(i)")
    deadline = time.time() + 15
    while time.time() < deadline:
        time.sleep(0.5)
        if one(r, "SELECT path FROM duckdb_databases() WHERE database_name = 'r'")[0] != attached:
            break
    assert one(r, "SELECT path FROM duckdb_databases() WHERE database_name = 'r'")[0] != attached, "no refresh"
    assert one(r, "SELECT count(*) FROM r.orders")[0] == before, "a transaction keeps its version"
    r.execute("COMMIT")
    assert one(r, "SELECT count(*) FROM r.orders")[0] == before + 100
    assert one(r, "EXECUTE counted")[0] == before + 100, "a prepared statement rebinds"
    # Re-reading after a refresh: unchanged ranges are still cached.
    s2 = stats(r)
    r.execute("SELECT sum(amount) FROM r.orders").fetchall()
    s3 = stats(r)
    fetched = s3["db_read_bytes"] - s2["db_read_bytes"]
    through_cache = s3["db_cache_bytes"] - s2["db_cache_bytes"]
    assert through_cache > 0 and fetched < through_cache / 2, (fetched, through_cache)

    # An explicit version stays put.
    gen = int(attached.rsplit("@", 1)[1].split("~")[0])
    pinned = connect()
    pinned.execute("SET pgdb_refresh_interval = 0.5")
    pinned.execute(f"ATTACH '{path}@{gen}' AS p (READ_ONLY)")
    assert one(pinned, "SELECT count(*) FROM p.orders") == (before,)

    # A checkpoint after a large rewrite, then readers again.
    w.execute("UPDATE w.orders SET amount = amount + 1 WHERE id % 2 = 0")
    w.execute("CHECKPOINT w")
    expected = one(w, "SELECT count(*), sum(amount) FROM w.orders")
    deadline = time.time() + 15
    while time.time() < deadline and one(r, "SELECT count(*), sum(amount) FROM r.orders") != expected:
        time.sleep(0.5)
    assert one(r, "SELECT count(*), sum(amount) FROM r.orders") == expected
    assert one(pinned, "SELECT count(*) FROM p.orders") == (before,)

    # Detach and attach again: the writer's database is all there.
    w.execute("DETACH w")
    w.execute(f"ATTACH '{path}' AS w")
    assert one(w, "SELECT count(*), sum(amount) FROM w.orders") == expected
    assert one(w, "SELECT nextval('w.seq')") == (1,)
    w.execute("DETACH w")

    # A writer process that dies with commits only in its WAL: the next writer
    # replays them (and drops what the dead one wrote but never published).
    crash = f"""
import duckdb, os
con = duckdb.connect(config={{"allow_unsigned_extensions": "true"}})
con.execute("LOAD '{ext}'")
con.execute("CREATE SECRET (TYPE postgres, HOST '{url.hostname}', PORT {url.port or 5432}, "
            "USER '{unquote(url.username)}', PASSWORD '{unquote(url.password)}', DATABASE '{url.path.lstrip('/')}')")
con.execute("ATTACH '{path}' AS w")
con.execute("INSERT INTO w.orders SELECT i, 5, 2 FROM range(3000000, 3000500) t(i)")
con.execute("BEGIN")
con.execute("INSERT INTO w.orders SELECT i, 5, 2 FROM range(4000000, 4100000) t(i)")
os._exit(9)  # no rollback, no checkpoint, no detach
"""
    assert subprocess.run([sys.executable, "-c", crash]).returncode == 9
    w.execute(f"ATTACH '{path}' AS w")
    assert one(w, "SELECT count(*) FROM w.orders WHERE id >= 3000000") == (500,)
    expected = one(w, "SELECT count(*), sum(amount) FROM w.orders")
    w.execute("DETACH w")

    # Readers on a streaming standby.
    if standby_url:
        sb = connect(standby_url)
        deadline = time.time() + 30
        last = None
        while True:
            try:
                sb.execute(f"ATTACH '{path}' AS s (READ_ONLY)")
                last = one(sb, "SELECT count(*), sum(amount) FROM s.orders")
                if last == expected:
                    break
            except duckdb.Error as e:
                last = e
            sb.execute("DETACH DATABASE IF EXISTS s")
            if time.time() > deadline:
                raise AssertionError(f"standby never caught up: {last!r}, want {expected}")
            time.sleep(0.5)
        raises(lambda: sb.execute(f"ATTACH 'pgdb://{vol}/other.duckdb' AS o"), "standby")

    # A SELECT-only role reads it; PostgreSQL refuses it the writer.
    if reader_url:
        rd = connect(reader_url)
        rd.execute(f"ATTACH '{path}' AS r (READ_ONLY)")
        assert one(rd, "SELECT count(*), sum(amount) FROM r.orders") == expected
        raises(lambda: rd.execute(f"ATTACH 'pgdb://{vol}/mine.duckdb' AS m"), "permission denied")
    print("pgdb ok")


if __name__ == "__main__":
    standby = os.environ.get("PGVFS_TEST_STANDBY_URL")
    reader = os.environ.get("PGVFS_TEST_READER_URL")
    check_pgdb(
        sys.argv[1],
        urlsplit(os.environ["PGVFS_TEST_URL"]),
        standby_url=urlsplit(standby) if standby else None,
        reader_url=urlsplit(reader) if reader else None,
    )
