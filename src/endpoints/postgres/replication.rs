//! Logical-replication transport: wraps `pgwire-replication` for the streaming
//! wire protocol and uses `sqlx` for the slot-lifecycle control plane
//! (`CREATE`/`ADVANCE`/`DROP` replication slot), which requires an ordinary SQL
//! connection rather than a replication one.
//!
//! The URL parsing, slot lifecycle, and TLS mapping follow the approach in
//! faucet-stream (crates/source/postgres-cdc/src/replication.rs, dual
//! Apache-2.0 OR MIT), adapted to mq-bridge's config and error types.

use crate::endpoints::postgres::state::format_lsn;
use crate::models::PostgresCdcConfig;
use anyhow::{anyhow, Context};
use percent_encoding::percent_decode_str;
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, TlsConfig as PgTlsConfig};
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, PgConnection};
use std::path::PathBuf;
use std::time::Duration;
use tracing::{debug, warn};

pub use pgwire_replication::ReplicationEvent;

/// Postgres connection coordinates parsed from the configured URL.
struct PgCoords {
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: String,
}

fn parse_url(url: &str) -> anyhow::Result<PgCoords> {
    let parsed = url::Url::parse(url).context("postgres-cdc: invalid connection URL")?;
    let host = parsed
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "postgres-cdc: connection URL is missing a host \
                 (expected postgres://user@host[:port]/dbname)"
            )
        })?
        .to_owned();
    let port = parsed.port().unwrap_or(5432);
    // The `url` crate leaves userinfo percent-encoded; decode it so credentials
    // with reserved characters (e.g. `@`, `:`, `/`) reach the server intact.
    let user = percent_decode_str(parsed.username())
        .decode_utf8_lossy()
        .into_owned();
    if user.is_empty() {
        return Err(anyhow!(
            "postgres-cdc: connection URL is missing a user \
             (expected postgres://user@host[:port]/dbname)"
        ));
    }
    let password = percent_decode_str(parsed.password().unwrap_or(""))
        .decode_utf8_lossy()
        .into_owned();
    let dbname = parsed.path().trim_start_matches('/').to_owned();
    let dbname = if dbname.is_empty() {
        "postgres".to_owned()
    } else {
        dbname
    };
    Ok(PgCoords {
        host,
        port,
        user,
        password,
        dbname,
    })
}

/// Map mq-bridge's shared [`TlsConfig`](crate::models::TlsConfig) onto
/// pgwire-replication's TLS model. Client-certificate mTLS is not exposed by
/// the replication transport in this version — warn if it was requested.
fn map_tls(tls: &crate::models::TlsConfig) -> PgTlsConfig {
    if tls.cert_file.is_some() || tls.key_file.is_some() {
        warn!(
            "postgres-cdc: client-certificate mTLS is not supported by the replication \
             transport yet; ignoring cert_file/key_file"
        );
    }
    if !tls.required {
        return PgTlsConfig::disabled();
    }
    if tls.accept_invalid_certs {
        // Encrypt but do not verify the server certificate.
        return PgTlsConfig::require();
    }
    // Verify the server certificate (and hostname), optionally against a
    // supplied CA bundle; falls back to the system roots when `ca_file` is None.
    PgTlsConfig::verify_full(tls.ca_file.clone().map(PathBuf::from))
}

/// Open a plain SQL connection for control-plane slot operations, applying the
/// same TLS enforcement as the replication stream so the slot lifecycle can't
/// silently fall back to an unverified `sslmode=prefer` connection.
async fn control_conn(url: &str, tls: &crate::models::TlsConfig) -> anyhow::Result<PgConnection> {
    let mut opts: PgConnectOptions = url
        .parse()
        .context("postgres-cdc: invalid connection URL")?;
    if tls.required {
        opts = opts.ssl_mode(if tls.accept_invalid_certs {
            sqlx::postgres::PgSslMode::Require
        } else {
            sqlx::postgres::PgSslMode::VerifyFull
        });
        if let Some(ca_file) = &tls.ca_file {
            opts = opts.ssl_root_cert(ca_file);
        }
    }
    opts.connect()
        .await
        .map_err(|e| anyhow!("postgres-cdc: control-plane connect failed: {e}"))
}

/// Ensure the logical replication slot exists, creating it with the `pgoutput`
/// plugin if missing and `create_if_missing` is true.
pub async fn ensure_slot(
    url: &str,
    slot_name: &str,
    create_if_missing: bool,
    temporary: bool,
    tls: &crate::models::TlsConfig,
) -> anyhow::Result<()> {
    let mut conn = control_conn(url, tls).await?;

    let row: Option<(String,)> =
        sqlx::query_as("SELECT slot_name::text FROM pg_replication_slots WHERE slot_name = $1")
            .bind(slot_name)
            .fetch_optional(&mut conn)
            .await
            .map_err(|e| anyhow!("postgres-cdc: slot lookup failed: {e}"))?;

    if row.is_some() {
        debug!("postgres-cdc: replication slot '{slot_name}' already exists");
        return Ok(());
    }
    if !create_if_missing {
        return Err(anyhow!(
            "postgres-cdc: replication slot '{slot_name}' does not exist and create_slot = false"
        ));
    }

    sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput', $2)")
        .bind(slot_name)
        .bind(temporary)
        .execute(&mut conn)
        .await
        .map_err(|e| anyhow!("postgres-cdc: create slot failed: {e}"))?;

    if temporary {
        debug!("postgres-cdc: created temporary replication slot '{slot_name}'");
    } else {
        warn!(
            "postgres-cdc: created PERMANENT replication slot '{slot_name}' — it retains WAL on \
             the server until consumed or explicitly dropped. Use temporary_slot: true for \
             ephemeral runs."
        );
    }
    Ok(())
}

/// Advance the slot's `confirmed_flush_lsn` to `lsn` while the slot is inactive
/// (before the stream is opened), so a resumed run skips already-persisted
/// changes. `pg_replication_slot_advance` never moves a slot backwards or past
/// the server's insert pointer, so a stale `lsn` is a safe no-op.
pub async fn advance_slot(
    url: &str,
    slot_name: &str,
    lsn: u64,
    tls: &crate::models::TlsConfig,
) -> anyhow::Result<()> {
    if lsn == 0 {
        return Ok(());
    }
    let mut conn = control_conn(url, tls).await?;
    sqlx::query("SELECT pg_replication_slot_advance($1, $2::pg_lsn)")
        .bind(slot_name)
        .bind(format_lsn(lsn))
        .execute(&mut conn)
        .await
        .map_err(|e| anyhow!("postgres-cdc: advance slot failed: {e}"))?;
    debug!("postgres-cdc: advanced slot '{slot_name}' confirmed_flush_lsn to {lsn:#x}");
    Ok(())
}

/// Wait for the slot to become inactive, then advance its `confirmed_flush_lsn`
/// to `lsn`. Used on graceful teardown: the streaming connection must be stopped
/// first, because `pg_replication_slot_advance` refuses to run on an active slot.
/// The server releases the slot shortly after the walsender's socket closes, so
/// we poll `pg_replication_slots.active` briefly before advancing.
pub async fn advance_slot_when_inactive(
    url: &str,
    slot_name: &str,
    lsn: u64,
    tls: &crate::models::TlsConfig,
) -> anyhow::Result<()> {
    if lsn == 0 {
        return Ok(());
    }
    let mut conn = control_conn(url, tls).await?;
    for _ in 0..40 {
        let active: Option<(Option<bool>,)> =
            sqlx::query_as("SELECT active FROM pg_replication_slots WHERE slot_name = $1")
                .bind(slot_name)
                .fetch_optional(&mut conn)
                .await
                .map_err(|e| anyhow!("postgres-cdc: slot status check failed: {e}"))?;
        match active {
            // Slot no longer exists — nothing to advance.
            None => return Ok(()),
            // Inactive (or NULL) — safe to advance.
            Some((Some(false) | None,)) => break,
            // Still held by a walsender — wait for release.
            Some((Some(true),)) => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    sqlx::query("SELECT pg_replication_slot_advance($1, $2::pg_lsn)")
        .bind(slot_name)
        .bind(format_lsn(lsn))
        .execute(&mut conn)
        .await
        .map_err(|e| anyhow!("postgres-cdc: advance slot failed: {e}"))?;
    debug!("postgres-cdc: advanced slot '{slot_name}' confirmed_flush_lsn to {lsn:#x} on shutdown");
    Ok(())
}

/// Open the logical replication stream. `pgwire-replication` handles TCP, TLS,
/// auth, `START_REPLICATION`, keepalives, and standby-status feedback.
pub async fn start_replication(
    config: &PostgresCdcConfig,
    start_lsn: Option<u64>,
) -> anyhow::Result<ReplicationClient> {
    let coords = parse_url(&config.url)?;
    let status_interval = Duration::from_millis(config.status_interval_ms.max(1));

    let cfg = ReplicationConfig {
        host: coords.host,
        port: coords.port,
        user: coords.user,
        password: coords.password,
        database: coords.dbname,
        tls: map_tls(&config.tls),
        slot: config.slot_name.clone(),
        publication: config.publication.clone(),
        start_lsn: Lsn::from_u64(start_lsn.unwrap_or(0)),
        stop_at_lsn: None,
        status_interval,
        idle_wakeup_interval: status_interval,
        buffer_events: 8192,
    };

    ReplicationClient::connect(cfg)
        .await
        .map_err(|e| anyhow!("postgres-cdc: start_replication failed: {e}"))
}
