//! Logical-replication transport: wraps `pgwire-replication` for the streaming
//! wire protocol and uses `sqlx` for the slot-lifecycle control plane
//! (`CREATE`/`ADVANCE`/`DROP` replication slot), which requires an ordinary SQL
//! connection rather than a replication one.
//!
//! The URL parsing, slot lifecycle, and TLS mapping follow the approach in
//! faucet-stream (crates/source/postgres-cdc/src/replication.rs, dual
//! Apache-2.0 OR MIT), adapted to mq-bridge's config and error types.
//! Pinned upstream commit and per-file changes: `./VENDORED.md`.

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

/// A publication table reference is a plain identifier or a single `schema.table`,
/// each part `[A-Za-z0-9_]`. Both are interpolated into DDL, so they are validated
/// (not bindable as parameters).
fn is_valid_pg_table_ref(s: &str) -> bool {
    match s.split_once('.') {
        Some((schema, table)) => {
            super::is_valid_pg_ident(schema) && super::is_valid_pg_ident(table)
        }
        None => super::is_valid_pg_ident(s),
    }
}

/// Split a table reference into `(schema, table)`; an unqualified name has no schema.
fn split_table_ref(s: &str) -> (Option<&str>, &str) {
    match s.split_once('.') {
        Some((schema, table)) => (Some(schema), table),
        None => (None, s),
    }
}

/// True for a Postgres `duplicate_object` (42710) error — e.g. a concurrent
/// `CREATE PUBLICATION` won the race between our existence check and create.
fn is_duplicate_object(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|c| c == "42710")
}

/// Ensure the publication exists, creating it if missing. With `tables` empty the
/// publication is `FOR ALL TABLES` (needs superuser); otherwise
/// `FOR TABLE <t1, t2, ...>` (needs ownership of each).
///
/// When the publication already exists and named `tables` are given, missing ones
/// are **added** (`ALTER PUBLICATION ... ADD TABLE`) so the config stays the source
/// of truth; tables are never dropped, so an operator's extra tables are left intact.
/// Publication and table names are identifiers (not bindable as parameters), so
/// they are validated as `[A-Za-z0-9_]` (optionally `schema.table`) before interpolation.
pub async fn ensure_publication(
    url: &str,
    publication: &str,
    tables: &[String],
    tls: &crate::models::TlsConfig,
) -> anyhow::Result<()> {
    if !super::is_valid_pg_ident(publication) {
        return Err(anyhow!(
            "postgres-cdc: `publication` must be a non-empty [A-Za-z0-9_] identifier"
        ));
    }
    for t in tables {
        if !is_valid_pg_table_ref(t) {
            return Err(anyhow!(
                "postgres-cdc: publication table '{t}' must be a [A-Za-z0-9_] identifier \
                 (optionally schema-qualified as schema.table)"
            ));
        }
    }
    let mut conn = control_conn(url, tls).await?;

    let exists = sqlx::query_as::<_, (String,)>(
        "SELECT pubname::text FROM pg_publication WHERE pubname = $1",
    )
    .bind(publication)
    .fetch_optional(&mut conn)
    .await
    .map_err(|e| anyhow!("postgres-cdc: publication lookup failed: {e}"))?
    .is_some();

    if !exists {
        let sql = if tables.is_empty() {
            format!("CREATE PUBLICATION {publication} FOR ALL TABLES")
        } else {
            format!(
                "CREATE PUBLICATION {publication} FOR TABLE {}",
                tables.join(", ")
            )
        };
        // Identifiers were validated above, so the interpolated DDL is safe.
        match sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&mut conn)
            .await
        {
            Ok(_) => warn!(
                "postgres-cdc: created publication '{publication}' — it defines which tables are \
                 captured; drop it with DROP PUBLICATION when no longer needed."
            ),
            // Lost the create race to a concurrent starter; the publication now exists.
            Err(e) if is_duplicate_object(&e) => {
                debug!("postgres-cdc: publication '{publication}' created concurrently")
            }
            Err(e) => return Err(anyhow!("postgres-cdc: create publication failed: {e}")),
        }
    }

    // Reconcile membership (add-only) when we manage a specific table list.
    if !tables.is_empty() {
        add_missing_publication_tables(&mut conn, publication, tables).await?;
    }
    Ok(())
}

/// Add any `tables` not already in the publication via `ALTER PUBLICATION ... ADD TABLE`.
/// An unqualified name is matched by table name in any schema (it resolves via `search_path`);
/// a `schema.table` reference is matched exactly. Never removes tables.
async fn add_missing_publication_tables(
    conn: &mut PgConnection,
    publication: &str,
    tables: &[String],
) -> anyhow::Result<()> {
    let current: Vec<(String, String)> = sqlx::query_as(
        "SELECT schemaname::text, tablename::text FROM pg_publication_tables WHERE pubname = $1",
    )
    .bind(publication)
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| anyhow!("postgres-cdc: publication membership lookup failed: {e}"))?;

    for t in tables {
        let (schema, table) = split_table_ref(t);
        let present = current
            .iter()
            .any(|(s, tab)| tab == table && schema.map(|want| want == s).unwrap_or(true));
        if present {
            continue;
        }
        let sql = format!("ALTER PUBLICATION {publication} ADD TABLE {t}");
        match sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&mut *conn)
            .await
        {
            Ok(_) => warn!("postgres-cdc: added table '{t}' to publication '{publication}'"),
            // Lost the race to a concurrent reconciler; the table is now a member.
            Err(e) if is_duplicate_object(&e) => {
                debug!("postgres-cdc: table '{t}' already in publication '{publication}'")
            }
            Err(e) => {
                return Err(anyhow!(
                    "postgres-cdc: add table '{t}' to publication failed: {e}"
                ))
            }
        }
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
