//! PostgreSQL connections: deadpool-postgres over rustls.
//!
//! LIFO, because a TCP sender restarts slow start on a connection idle longer
//! than its RTO (Linux: >= 200 ms): reusing the most recent connection keeps a
//! hot working set the size of the real concurrency (on Aurora a 64 KiB fetch
//! took 1.18 ms on a hot connection, 2.17 ms after 300 ms idle). Recycling
//! only checks that a connection is open: a broken one fails its query.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use deadpool_postgres::{
    Hook, HookError, Manager, ManagerConfig, Object, QueueMode, RecyclingMethod, Runtime,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use tokio_postgres::config::SslMode;
use tokio_postgres::Client;
use tokio_postgres_rustls::MakeRustlsConnect;

pub type Pooled = Object;

pub struct Options {
    /// Connections opened at start.
    pub min: usize,
    /// Connections open at once, idle or checked out.
    pub max: usize,
    /// Run on every new connection (one simple-query batch).
    pub session: String,
}

#[derive(Clone)]
pub struct Pool {
    pool: deadpool_postgres::Pool,
    config: tokio_postgres::Config,
    tls: MakeRustlsConnect,
    session: Arc<str>,
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
        let tls = MakeRustlsConnect::new(tls_config()?);
        let session: Arc<str> = opts.session.into();
        let manager = Manager::from_config(
            config.clone(),
            tls.clone(),
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let hook_session = session.clone();
        let pool = deadpool_postgres::Pool::builder(manager)
            .max_size(opts.max)
            .queue_mode(QueueMode::Lifo)
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(Duration::from_secs(30)))
            .post_create(Hook::async_fn(move |client, _| {
                let session = hook_session.clone();
                Box::pin(async move {
                    client
                        .batch_execute(&session)
                        .await
                        .map_err(HookError::Backend)
                })
            }))
            .build()?;
        // Open the warm set now rather than on the first queries.
        let warm = futures::future::try_join_all((0..opts.min).map(|_| pool.get())).await?;
        drop(warm);
        Ok(Pool {
            pool,
            config,
            tls,
            session,
        })
    }

    /// The most recently used idle connection, else a new one.
    pub async fn get(&self) -> Result<Pooled> {
        Ok(self.pool.get().await?)
    }

    /// A connection outside the pool, for state that lives as long as the
    /// connection (the writer's advisory lock).
    pub async fn dedicated(&self) -> Result<Client> {
        let (client, connection) = self.config.connect(self.tls.clone()).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("pgvfs: postgres connection closed: {e}");
            }
        });
        client.batch_execute(&self.session).await?;
        Ok(client)
    }
}

/// Server certificates checked against the system CAs, plus
/// PGVFS_DB_CA_FILE for a private CA.
fn tls_config() -> Result<rustls::ClientConfig> {
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
        for cert in CertificateDer::pem_slice_iter(&pem) {
            roots.add(cert?)?;
        }
    }
    Ok(rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth())
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
