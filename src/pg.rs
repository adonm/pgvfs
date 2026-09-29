//! PostgreSQL connections: tokio-postgres over rustls, pooled most recently
//! used first.
//!
//! LIFO, because a TCP sender restarts slow start on a connection idle longer
//! than its RTO (Linux: >= 200 ms): reusing the most recent connection keeps a
//! hot working set the size of the real concurrency (on Aurora a 64 KiB fetch
//! took 1.18 ms on a hot connection, 2.17 ms after 300 ms idle). No ping on
//! checkout: a broken connection fails its query and is discarded.

use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect, TlsStream};
use tokio_postgres::types::Type;
use tokio_postgres::{config::SslMode, Client, Socket, Statement};

const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
/// Idle connections beyond `min` close after this long unused.
const IDLE_MAX: Duration = Duration::from_secs(600);

pub struct Options {
    /// Connections opened at start and kept however idle.
    pub min: usize,
    /// Connections open at once, idle or checked out.
    pub max: usize,
    /// Run on every new connection (one simple-query batch).
    pub session: String,
    /// The hot statement, prepared once per connection (`Pooled::range`).
    pub range_sql: &'static str,
    pub range_types: &'static [Type],
}

#[derive(Clone)]
pub struct Pool(Arc<Inner>);

struct Inner {
    config: tokio_postgres::Config,
    tls: MakeRustls,
    opts: Options,
    /// Idle connections, most recently used last.
    idle: Mutex<Vec<Conn>>,
    slots: Arc<Semaphore>,
}

struct Conn {
    client: Client,
    /// Prepared on first use: the layout may not exist when a pool starts.
    range: OnceCell<Statement>,
    idle_since: Instant,
}

impl Pool {
    pub async fn connect(url: &str, opts: Options) -> Result<Pool> {
        let mut config: tokio_postgres::Config = url.parse()?;
        anyhow::ensure!(
            config.get_ssl_mode() == SslMode::Require
                || local_db(&config)
                || std::env::var("PGVFS_DB_ALLOW_PLAINTEXT").as_deref() == Ok("true"),
            "remote PostgreSQL requires sslmode=require (or PGVFS_DB_ALLOW_PLAINTEXT=true on an isolated network)"
        );
        if config.get_application_name().is_none() {
            config.application_name("pgvfs");
        }
        if config.get_connect_timeout().is_none() {
            config.connect_timeout(Duration::from_secs(10));
        }
        let pool = Pool(Arc::new(Inner {
            config,
            tls: MakeRustls::new()?,
            slots: Arc::new(Semaphore::new(opts.max)),
            opts,
            idle: Mutex::new(Vec::new()),
        }));
        let warm =
            futures::future::try_join_all((0..pool.0.opts.min).map(|_| pool.0.open())).await?;
        pool.0.idle.lock().unwrap().extend(warm);
        Ok(pool)
    }

    /// The most recently used idle connection, else a new one.
    pub async fn get(&self) -> Result<Pooled> {
        let slot = tokio::time::timeout(ACQUIRE_TIMEOUT, self.0.slots.clone().acquire_owned())
            .await
            .map_err(|_| anyhow!("no PostgreSQL connection free within {ACQUIRE_TIMEOUT:?}"))??;
        let conn = loop {
            let top = self.0.idle.lock().unwrap().pop();
            match top {
                Some(c) if c.client.is_closed() => continue,
                Some(c) => break c,
                None => break self.0.open().await?,
            }
        };
        Ok(Pooled {
            conn: Some(conn),
            pool: self.clone(),
            _slot: slot,
        })
    }

    /// A connection outside the pool, for state that lives as long as the
    /// connection (the writer's advisory lock).
    pub async fn dedicated(&self) -> Result<Client> {
        Ok(self.0.open().await?.client)
    }
}

impl Inner {
    async fn open(&self) -> Result<Conn> {
        let (client, connection) = self.config.connect(self.tls.clone()).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("pgvfs: postgres connection closed: {e}");
            }
        });
        client.batch_execute(&self.opts.session).await?;
        Ok(Conn {
            client,
            range: OnceCell::new(),
            idle_since: Instant::now(),
        })
    }
}

/// A checked-out connection. Dropping it returns it to the top of the idle
/// stack: tokio-postgres keeps a client usable after a cancelled query, a
/// dropped transaction has already queued its ROLLBACK, and a dropped COPY
/// sink sends CopyFail, all ahead of the next request.
pub struct Pooled {
    conn: Option<Conn>,
    pool: Pool,
    _slot: OwnedSemaphorePermit,
}

impl Pooled {
    /// This connection's prepared range statement.
    pub async fn range(&mut self) -> Result<Statement> {
        let conn = self.conn.as_mut().expect("live until drop");
        let opts = &self.pool.0.opts;
        Ok(conn
            .range
            .get_or_try_init(|| conn.client.prepare_typed(opts.range_sql, opts.range_types))
            .await?
            .clone())
    }

    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.conn.as_mut().expect("live until drop").client
    }
}

impl Deref for Pooled {
    type Target = Client;

    fn deref(&self) -> &Client {
        &self.conn.as_ref().expect("live until drop").client
    }
}

impl Drop for Pooled {
    fn drop(&mut self) {
        let Some(mut conn) = self.conn.take() else {
            return;
        };
        if conn.client.is_closed() {
            return;
        }
        conn.idle_since = Instant::now();
        let stale = {
            let mut idle = self.pool.0.idle.lock().unwrap();
            idle.push(conn);
            // The bottom of the stack is the least recently used.
            (idle.len() > self.pool.0.opts.min && idle[0].idle_since.elapsed() > IDLE_MAX)
                .then(|| idle.remove(0))
        };
        drop(stale); // closes it outside the lock
    }
}

fn local_db(config: &tokio_postgres::Config) -> bool {
    !config.get_hosts().is_empty()
        && config.get_hosts().iter().all(|host| match host {
            tokio_postgres::config::Host::Tcp(name) => {
                name == "localhost" || name.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
            }
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(_) => true,
        })
        && config.get_hostaddrs().iter().all(IpAddr::is_loopback)
}

// ---------------------------------------------------------------------------
// TLS: verify the server's certificate chain and hostname against the system
// CAs (plus PGVFS_DB_CA_FILE for a private CA).
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MakeRustls(Arc<rustls::ClientConfig>);

impl MakeRustls {
    fn new() -> Result<Self> {
        let native = rustls_native_certs::load_native_certs();
        anyhow::ensure!(
            native.errors.is_empty(),
            "could not load system CAs: {:?}",
            native.errors
        );
        let mut roots = rustls::RootCertStore::empty();
        for cert in native.certs {
            roots.add(cert)?;
        }
        if let Ok(path) = std::env::var("PGVFS_DB_CA_FILE") {
            let pem = std::fs::read(path)?;
            for cert in rustls_pemfile::certs(&mut std::io::Cursor::new(pem)) {
                roots.add(cert?)?;
            }
        }
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
        Ok(Self(Arc::new(config)))
    }
}

impl MakeTlsConnect<Socket> for MakeRustls {
    type Stream = RustlsStream;
    type TlsConnect = RustlsConnect;
    type Error = rustls::pki_types::InvalidDnsNameError;

    fn make_tls_connect(&mut self, domain: &str) -> Result<RustlsConnect, Self::Error> {
        Ok(RustlsConnect {
            name: ServerName::try_from(domain)?.to_owned(),
            config: self.0.clone(),
        })
    }
}

struct RustlsConnect {
    name: ServerName<'static>,
    config: Arc<rustls::ClientConfig>,
}

impl TlsConnect<Socket> for RustlsConnect {
    type Stream = RustlsStream;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<RustlsStream>> + Send>>;

    fn connect(self, stream: Socket) -> Self::Future {
        let connector = tokio_rustls::TlsConnector::from(self.config);
        Box::pin(async move { connector.connect(self.name, stream).await.map(RustlsStream) })
    }
}

struct RustlsStream(tokio_rustls::client::TlsStream<Socket>);

impl TlsStream for RustlsStream {
    fn channel_binding(&self) -> ChannelBinding {
        ChannelBinding::none()
    }
}

impl AsyncRead for RustlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for RustlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_loopback_addresses_allow_plaintext_by_default() {
        for url in [
            "postgres://localhost/db?sslmode=disable",
            "postgres://127.0.0.1/db?sslmode=prefer",
            "postgres://[::1]/db?sslmode=disable",
        ] {
            assert!(local_db(&url.parse().unwrap()), "{url}");
        }
        for url in [
            "postgres://db.example/db?sslmode=prefer",
            "postgres://localhost/db?hostaddr=192.0.2.1&sslmode=disable",
        ] {
            assert!(!local_db(&url.parse().unwrap()), "{url}");
        }
    }
}
