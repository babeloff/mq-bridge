//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use crate::canonical_message::tracing_support::LazyMessageIds;
use crate::models::SqlxConfig;
use crate::traits::{
    BoxFuture, ConsumerError, EndpointStatus, MessageConsumer, MessageDisposition,
    MessagePublisher, PublisherError, ReceivedBatch, Sent, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use sqlx::any::AnyPoolOptions;
use sqlx::{AnyPool, AssertSqlSafe, Column, Row};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, trace, warn};

fn is_deadlock_error(e: &sqlx::Error) -> bool {
    if let Some(db_err) = e.as_database_error() {
        match db_err.code() {
            Some(code) => {
                let c = code.as_ref();
                c == "1213" || c == "40001" || c == "40P01" || c == "1205"
            }
            None => false,
        }
    } else {
        false
    }
}

fn is_valid_table_name(name: &str) -> bool {
    if name.is_empty() || name.starts_with('.') || name.ends_with('.') || name.contains("..") {
        return false;
    }
    name.split('.')
        .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
}

/// Checks if a SQL query string contains a `(payload)` clause, ignoring case and whitespace.
fn contains_payload_clause(query: &str) -> bool {
    let lower_query = query.to_lowercase();
    let mut search_start = 0;
    while let Some(open_paren_idx) = lower_query[search_start..].find('(') {
        let absolute_open_idx = search_start + open_paren_idx;
        // Find the matching closing parenthesis
        if let Some(close_paren_idx) = lower_query[absolute_open_idx..].find(')') {
            let absolute_close_idx = absolute_open_idx + close_paren_idx;
            // Extract content between parentheses
            let content = &lower_query[absolute_open_idx + 1..absolute_close_idx];
            // Trim whitespace and check if it's "payload"
            if content.trim() == "payload" {
                return true;
            }
            // Continue searching after the found closing parenthesis
            search_start = absolute_close_idx + 1;
        } else {
            // No closing parenthesis found, stop searching
            break;
        }
    }
    false
}

fn audited_sql(sql: &str) -> AssertSqlSafe<&str> {
    AssertSqlSafe(sql)
}

/// Where a single bound value in a token-based `insert_query` comes from.
/// Resolved per-row; never falls back between the two sources.
#[derive(Debug, Clone, PartialEq)]
enum ColumnSource {
    /// `${metadata:<key>}` — `message.metadata.get(key)`, else NULL.
    Metadata(String),
    /// `${payload:<field>}` — top-level JSON field of the payload, else NULL.
    Payload(String),
}

/// A value ready to be bound, preserving JSON scalar type so strict dialects
/// (Postgres/MSSQL) accept numeric/bool columns instead of erroring on text.
#[derive(Debug, Clone, PartialEq)]
enum BindValue {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
}

/// Driver-specific positional placeholder for the given 1-based index.
fn positional_placeholder(driver_name: &str, index: usize) -> String {
    match driver_name {
        "PostgreSQL" => format!("${}", index),
        "Microsoft SQL Server" => format!("@p{}", index),
        _ => "?".to_string(),
    }
}

/// Parse `${metadata:<key>}` / `${payload:<field>}` tokens out of an `insert_query`,
/// rewriting each into a driver-appropriate positional placeholder assigned a running
/// 1-based index in encounter order. Returns the rewritten query and the ordered
/// sources (`sources[i]` resolves the value for the i-th placeholder). An empty
/// `Vec` means the query had no tokens → legacy single-payload-bind mode.
fn parse_insert_template(
    query: &str,
    driver_name: &str,
) -> anyhow::Result<(String, Vec<ColumnSource>)> {
    let mut out = String::with_capacity(query.len());
    let mut sources: Vec<ColumnSource> = Vec::new();
    let bytes = query.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            // Find the closing brace.
            let close = query[i + 2..].find('}').map(|off| i + 2 + off);
            let close = close.ok_or_else(|| {
                anyhow!(
                    "Malformed token in insert_query: unclosed '${{' near '{}'",
                    &query[i..]
                )
            })?;
            let inner = &query[i + 2..close];
            let (prefix, name) = inner.split_once(':').ok_or_else(|| {
                anyhow!("Malformed token in insert_query: '${{{}}}' is missing a ':' separator (expected ${{metadata:key}} or ${{payload:field}})", inner)
            })?;
            let name = name.trim();
            if name.is_empty() {
                return Err(anyhow!(
                    "Malformed token in insert_query: '${{{}}}' has an empty key/field name",
                    inner
                ));
            }
            let source = match prefix.trim() {
                "metadata" => ColumnSource::Metadata(name.to_string()),
                "payload" => ColumnSource::Payload(name.to_string()),
                other => {
                    return Err(anyhow!(
                        "Malformed token in insert_query: unknown prefix '{}' in '${{{}}}' (expected 'metadata' or 'payload')",
                        other,
                        inner
                    ))
                }
            };
            sources.push(source);
            out.push_str(&positional_placeholder(driver_name, sources.len()));
            i = close + 1;
        } else {
            // Copy this UTF-8 char verbatim.
            let ch = query[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    Ok((out, sources))
}

/// Resolve a `ColumnSource` for one message into a typed `BindValue`. `payload_json`
/// is the payload parsed once as JSON (`None` if not valid JSON). No fallback: a
/// `Payload` source never consults metadata and vice versa; an unresolvable source
/// yields `Null`.
fn resolve_source(
    msg: &CanonicalMessage,
    source: &ColumnSource,
    payload_json: &Option<serde_json::Value>,
) -> BindValue {
    match source {
        ColumnSource::Metadata(key) => match msg.metadata.get(key) {
            Some(v) => BindValue::Text(v.clone()),
            None => BindValue::Null,
        },
        ColumnSource::Payload(field) => match payload_json.as_ref().and_then(|v| v.get(field)) {
            Some(serde_json::Value::String(s)) => BindValue::Text(s.clone()),
            Some(serde_json::Value::Bool(b)) => BindValue::Bool(*b),
            Some(serde_json::Value::Number(n)) => {
                if let Some(i) = n.as_i64() {
                    BindValue::Int(i)
                } else if let Some(f) = n.as_f64() {
                    BindValue::Float(f)
                } else {
                    BindValue::Null
                }
            }
            _ => BindValue::Null,
        },
    }
}

type AnyQuery<'q> = sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments>;

/// Bind one typed value, mapping `Null` to a typed SQL NULL.
fn bind_value(query: AnyQuery<'_>, value: BindValue) -> AnyQuery<'_> {
    match value {
        BindValue::Null => query.bind(None::<String>),
        BindValue::Int(i) => query.bind(i),
        BindValue::Float(f) => query.bind(f),
        BindValue::Bool(b) => query.bind(b),
        BindValue::Text(s) => query.bind(s),
    }
}

/// Parse the payload as JSON once, then resolve+bind every column source for one row.
fn bind_message_sources<'q>(
    mut query: AnyQuery<'q>,
    msg: &CanonicalMessage,
    sources: &[ColumnSource],
) -> AnyQuery<'q> {
    let payload_json: Option<serde_json::Value> = serde_json::from_slice(&msg.payload).ok();
    for source in sources {
        query = bind_value(query, resolve_source(msg, source, &payload_json));
    }
    query
}

fn build_sqlx_url_with_tls(config: &SqlxConfig) -> anyhow::Result<String> {
    let mut url = url::Url::parse(&config.url)?;

    if let Some(username) = &config.username {
        url.set_username(username)
            .map_err(|_| anyhow!("Cannot set username on sqlx URL"))?;
    }
    if let Some(password) = &config.password {
        url.set_password(Some(password))
            .map_err(|_| anyhow!("Cannot set password on sqlx URL"))?;
    }

    if config.tls.required {
        let scheme = url.scheme().to_string();
        match scheme.as_str() {
            "postgres" | "postgresql" => {
                let mut query_pairs = url.query_pairs_mut();
                if config.tls.accept_invalid_certs {
                    query_pairs.append_pair("sslmode", "require");
                } else if config.tls.ca_file.is_some() {
                    query_pairs.append_pair("sslmode", "verify-ca");
                } else {
                    query_pairs.append_pair("sslmode", "require");
                }

                if let Some(ca) = &config.tls.ca_file {
                    query_pairs.append_pair("sslrootcert", ca);
                }
                if let Some(cert) = &config.tls.cert_file {
                    query_pairs.append_pair("sslcert", cert);
                }
                if let Some(key) = &config.tls.key_file {
                    query_pairs.append_pair("sslkey", key);
                }
                if let Some(pass) = &config.tls.cert_password {
                    query_pairs.append_pair("sslpassword", pass);
                }
            }
            "mysql" | "mariadb" => {
                // MySQL/MariaDB support for TLS options in URL is more limited.
                // It's generally better to use a client-side configuration file (`my.cnf`)
                // for complex TLS setups. We'll add what we can.
                warn!("For complex MySQL/MariaDB TLS setups, using a client configuration file (my.cnf) is recommended over URL parameters.");
                let mut query_pairs = url.query_pairs_mut();
                query_pairs.append_pair("ssl-mode", "REQUIRED");
            }
            "mssql" | "sqlserver" => {
                let mut query_pairs = url.query_pairs_mut();
                if config.tls.accept_invalid_certs {
                    query_pairs.append_pair("encrypt", "true");
                    query_pairs.append_pair("trust-server-certificate", "true");
                } else {
                    query_pairs.append_pair("encrypt", "strict");
                }
            }
            _ => {}
        }
    }

    Ok(url.to_string())
}

async fn create_sqlx_pool(config: &SqlxConfig) -> anyhow::Result<AnyPool> {
    let url = build_sqlx_url_with_tls(config)?;
    let mut pool_options = AnyPoolOptions::new();

    if let Some(max_conn) = config.max_connections {
        pool_options = pool_options.max_connections(max_conn);
    }
    if let Some(min_conn) = config.min_connections {
        pool_options = pool_options.min_connections(min_conn);
    }
    if let Some(timeout) = config.acquire_timeout_ms {
        pool_options = pool_options.acquire_timeout(Duration::from_millis(timeout));
    }
    if let Some(timeout) = config.idle_timeout_ms {
        pool_options = pool_options.idle_timeout(Duration::from_millis(timeout));
    }
    if let Some(lifetime) = config.max_lifetime_ms {
        pool_options = pool_options.max_lifetime(Duration::from_millis(lifetime));
    }

    Ok(pool_options.connect(&url).await?)
}

/// Returns a shared connection pool for this database, building one on first use.
async fn create_shared_sqlx_pool(config: &SqlxConfig) -> anyhow::Result<std::sync::Arc<AnyPool>> {
    let identity = crate::connection_registry::connection_identity((
        &config.url,
        &config.username,
        &config.password,
        config.tls.required,
        &config.tls.ca_file,
        &config.tls.cert_file,
        &config.tls.key_file,
        &config.tls.cert_password,
        config.tls.accept_invalid_certs,
        (
            config.max_connections,
            config.min_connections,
            config.acquire_timeout_ms,
            config.idle_timeout_ms,
            config.max_lifetime_ms,
        ),
    ));
    let config_clone = config.clone();
    crate::connection_registry::get_or_create(
        "sqlx-pool",
        identity,
        config.shared.unwrap_or(true),
        move || async move { create_sqlx_pool(&config_clone).await },
    )
    .await
}

pub struct SqlxPublisher {
    pool: AnyPool,
    // Retains the shared registry entry so concurrent publishers reuse this pool.
    _shared_pool: std::sync::Arc<AnyPool>,
    insert_query: String,
    /// Ordered value sources for token-based multi-column inserts. Empty = legacy
    /// single-payload-bind mode (query has no `${...}` tokens).
    column_sources: Vec<ColumnSource>,
    driver_name: String,
    table: String,
}

impl SqlxPublisher {
    pub async fn new(config: &SqlxConfig) -> anyhow::Result<Self> {
        sqlx::any::install_default_drivers();
        if !is_valid_table_name(&config.table) {
            return Err(anyhow!(
                "Invalid table name: '{}'. Only alphanumeric characters and underscores are allowed.",
                config.table
            ));
        }
        let shared_pool = create_shared_sqlx_pool(config).await?;
        let pool = (*shared_pool).clone();
        let table = config.table.clone();

        // Acquire a connection to determine the driver so we can use the correct SQL syntax.
        let conn = pool.acquire().await?;
        let driver_name = conn.backend_name().to_string();
        drop(conn);

        info!(table = %config.table, driver = %driver_name, "SQLx publisher connected");

        // Resolve the insert query and parse any `${metadata:...}`/`${payload:...}`
        // tokens into ordered value sources, rewriting them to positional placeholders.
        let raw_insert_query =
            config
                .insert_query
                .clone()
                .unwrap_or_else(|| match driver_name.as_str() {
                    "PostgreSQL" => format!("INSERT INTO {} (payload) VALUES ($1)", config.table),
                    "Microsoft SQL Server" => {
                        format!("INSERT INTO {} (payload) VALUES (@p1)", config.table)
                    }
                    _ => format!("INSERT INTO {} (payload) VALUES (?)", config.table),
                });
        let (insert_query, column_sources) =
            parse_insert_template(&raw_insert_query, &driver_name)?;

        if config.auto_create_table && !column_sources.is_empty() {
            return Err(anyhow!(
                "auto_create_table is not supported with a multi-column insert_query; create the table manually."
            ));
        }

        if config.auto_create_table {
            // --- Auto-create table and index ---
            let create_table_query = match driver_name.as_str() {
                "PostgreSQL" => format!(
                    "CREATE TABLE IF NOT EXISTS {} (id BIGSERIAL PRIMARY KEY, payload BYTEA NOT NULL, locked_until TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT NOW())",
                    config.table
                ),
                "MySQL" | "MariaDB" => format!(
                    "CREATE TABLE IF NOT EXISTS {} (id BIGINT AUTO_INCREMENT PRIMARY KEY, payload BLOB NOT NULL, locked_until DATETIME, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)",
                    config.table
                ),
                "SQLite" => format!(
                    "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY AUTOINCREMENT, payload BLOB NOT NULL, locked_until DATETIME, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)",
                    config.table
                ),
                "Microsoft SQL Server" => format!(
                    "IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{0}') AND type in (N'U'))
                CREATE TABLE {0} (id BIGINT IDENTITY(1,1) PRIMARY KEY, payload VARBINARY(MAX) NOT NULL, locked_until DATETIME2, created_at DATETIME2 DEFAULT GETUTCDATE())",
                    config.table
                ),
                _ => "".to_string(), // Don't attempt for unknown drivers
            };

            if !create_table_query.is_empty() {
                if let Err(e) = sqlx::query(audited_sql(&create_table_query))
                    .execute(&pool)
                    .await
                {
                    warn!(
                        "Failed to auto-create table '{}': {}. Please ensure it exists.",
                        config.table, e
                    );
                } else {
                    let table_name_for_index =
                        config.table.split('.').next_back().unwrap_or(&config.table);
                    let index_name = format!("idx_{}_locked_until", table_name_for_index);

                    let create_index_query = match driver_name.as_str() {
                        "PostgreSQL" | "SQLite" | "MariaDB" => {
                            format!(
                                "CREATE INDEX IF NOT EXISTS {} ON {} (locked_until)",
                                index_name, config.table
                            )
                        }
                        "MySQL" => {
                            format!(
                                "CREATE INDEX {} ON {} (locked_until)",
                                index_name, config.table
                            )
                        }
                        "Microsoft SQL Server" => {
                            format!(
                                "IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = N'{}' AND object_id = OBJECT_ID(N'{}'))
                                CREATE INDEX {} ON {} (locked_until)",
                                index_name, config.table, index_name, config.table
                            )
                        }
                        _ => "".to_string(),
                    };

                    if !create_index_query.is_empty() {
                        if let Err(e) = sqlx::query(audited_sql(&create_index_query))
                            .execute(&pool)
                            .await
                        {
                            let driver_lc = driver_name.to_lowercase();
                            if (driver_lc.contains("mysql") || driver_lc.contains("mariadb"))
                                && e.as_database_error()
                                    .is_some_and(|db_err| db_err.code().as_deref() == Some("1061"))
                            {
                                trace!("Index {} on {} already exists.", index_name, config.table);
                            } else {
                                warn!("Failed to create index on '{}': {}", config.table, e);
                            }
                        }
                    }
                }
            }
        }

        Ok(Self {
            pool,
            _shared_pool: shared_pool,
            insert_query,
            column_sources,
            driver_name,
            table,
        })
    }
}

#[async_trait]
impl MessagePublisher for SqlxPublisher {
    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        trace!(message_id = %format!("{:032x}", message.message_id), table = %self.table, "Publishing to SQL");
        let query = sqlx::query(audited_sql(&self.insert_query));
        let query = if self.column_sources.is_empty() {
            query.bind(message.payload.to_vec())
        } else {
            bind_message_sources(query, &message, &self.column_sources)
        };
        query
            .execute(&self.pool)
            .await
            .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
        Ok(Sent::Ack)
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }

        trace!(count = messages.len(), message_ids = ?LazyMessageIds(&messages), "Publishing batch to SQLx");

        // Manually construct the query with appropriate placeholders because
        // sqlx::QueryBuilder with the `Any` driver does not correctly rewrite `?` to `$N`.
        let base_query = match self.insert_query.to_uppercase().rfind("VALUES") {
            Some(pos) => &self.insert_query[..pos],
            None => {
                warn!("Could not optimize batch insert due to custom query format. Falling back to iterative inserts.");
                return self.send_batch_iterative(messages).await;
            }
        };

        // The `(payload)` single-column guard only applies to legacy mode; a
        // token-based query is already known-correct from `parse_insert_template`.
        if self.column_sources.is_empty() && !contains_payload_clause(base_query) {
            warn!("Could not optimize batch insert due to custom query format. Falling back to iterative inserts.");
            return self.send_batch_iterative(messages).await;
        }

        // Placeholders per row: N tokens in token mode, 1 (the payload) in legacy mode.
        // A running global 1-based index spans the whole batch.
        let per_row = self.column_sources.len().max(1);
        let mut placeholders = String::new();
        let mut param_idx = 1;
        for i in 0..messages.len() {
            if i > 0 {
                placeholders.push_str(", ");
            }
            placeholders.push('(');
            for j in 0..per_row {
                if j > 0 {
                    placeholders.push_str(", ");
                }
                placeholders.push_str(&positional_placeholder(&self.driver_name, param_idx));
                param_idx += 1;
            }
            placeholders.push(')');
        }

        let sql = format!("{} VALUES {}", base_query, placeholders);

        let mut query = sqlx::query(audited_sql(&sql));
        for msg in &messages {
            if self.column_sources.is_empty() {
                query = query.bind(msg.payload.to_vec());
            } else {
                query = bind_message_sources(query, msg, &self.column_sources);
            }
        }

        query
            .execute(&self.pool)
            .await
            .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
        Ok(SentBatch::Ack)
    }

    async fn status(&self) -> EndpointStatus {
        let (healthy, error) = match self.pool.acquire().await {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };

        EndpointStatus {
            healthy,
            target: self.table.clone(),
            error,
            details: serde_json::json!({ "driver": self.driver_name, "pool_size": self.pool.size(), "pool_idle": self.pool.num_idle() }),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl SqlxPublisher {
    /// Fallback implementation that inserts messages one by one within a transaction.
    /// This is less performant than a single multi-row insert statement.
    async fn send_batch_iterative(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
        for msg in &messages {
            let query = sqlx::query(audited_sql(&self.insert_query));
            let query = if self.column_sources.is_empty() {
                query.bind(msg.payload.to_vec())
            } else {
                bind_message_sources(query, msg, &self.column_sources)
            };
            query
                .execute(&mut *tx)
                .await
                .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
        }
        tx.commit()
            .await
            .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
        Ok(SentBatch::Ack)
    }
}

pub struct SqlxConsumer {
    pool: AnyPool,
    select_query: String,
    delete_after_read: bool,
    table: String,
    polling_interval: Duration,
    driver_name: String,
}

impl SqlxConsumer {
    pub async fn new(config: &SqlxConfig) -> anyhow::Result<Self> {
        sqlx::any::install_default_drivers();
        if !is_valid_table_name(&config.table) {
            return Err(anyhow!(
                "Invalid table name: '{}'. Only alphanumeric characters and underscores are allowed.",
                config.table
            ));
        }
        let pool = create_sqlx_pool(config).await?;

        // Acquire a connection to determine the driver so we can use the correct SQL syntax later.
        let conn = pool.acquire().await?;
        let driver_name = conn.backend_name().to_string();
        // Immediately return the connection to the pool.
        drop(conn);
        info!(table = %config.table, driver = %driver_name, "SQLx consumer connected");

        let select_query = if let Some(query) = &config.select_query {
            match driver_name.as_str() {
                "PostgreSQL" => {
                    if !query.contains("$1") {
                        return Err(anyhow!("Custom select_query for PostgreSQL must contain a '$1' placeholder for the batch size limit."));
                    }
                    query.clone()
                }
                "Microsoft SQL Server" => {
                    if !query.contains("@p1") {
                        return Err(anyhow!("Custom select_query for SQL Server must contain a '@p1' placeholder for the batch size limit."));
                    }
                    query.clone()
                }
                _ => {
                    return Err(anyhow!("Custom select_query is not supported for the '{}' driver. It is only supported for PostgreSQL and Microsoft SQL Server.", driver_name));
                }
            }
        } else {
            match driver_name.as_str() {
                "PostgreSQL" => {
                    // This CTE-based query atomically finds available rows, locks them,
                    // updates their `locked_until` timestamp, and returns them.
                    // This is a robust pattern for a work queue with multiple consumers.
                    format!(
                        r#"
WITH available AS (
    SELECT id FROM {0}
    WHERE locked_until IS NULL OR locked_until < NOW()
    ORDER BY id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
),
updated AS (
    UPDATE {0}
    SET locked_until = NOW() + interval '60 seconds'
    WHERE id IN (SELECT id FROM available)
    RETURNING id, payload
)
SELECT id, payload FROM updated"#,
                        config.table,
                    )
                }
                "Microsoft SQL Server" => {
                    // This query atomically finds available rows, locks them,
                    // updates their `locked_until` timestamp, and returns them.
                    format!(
                        r#"
UPDATE {0}
SET locked_until = DATEADD(second, 60, GETUTCDATE())
OUTPUT INSERTED.id, INSERTED.payload
WHERE id IN (SELECT TOP (@p1) id FROM {0} WITH (UPDLOCK, READPAST) WHERE locked_until IS NULL OR locked_until < GETUTCDATE() ORDER BY id)"#,
                        config.table
                    )
                }
                _ => format!("SELECT id, payload FROM {}", config.table),
            }
        };
        Ok(Self {
            pool,
            select_query,
            delete_after_read: config.delete_after_read,
            table: config.table.clone(),
            polling_interval: Duration::from_millis(config.polling_interval_ms.unwrap_or(100)),
            driver_name,
        })
    }
}

impl SqlxConsumer {
    async fn fetch_and_lock_mysql(
        &self,
        limit: usize,
    ) -> Result<Vec<sqlx::any::AnyRow>, ConsumerError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| ConsumerError::Connection(e.into()))?;

        // 1. Find and lock rows
        let lock_query = format!(
            "SELECT id FROM {} WHERE locked_until IS NULL OR locked_until < NOW() ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED",
            self.table
        );

        let locked_ids: Vec<i64> = sqlx::query(audited_sql(&lock_query))
            .bind(limit as i64)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| ConsumerError::Connection(e.into()))?
            .into_iter()
            .map(|row| row.get("id"))
            .collect();

        if locked_ids.is_empty() {
            tx.commit().await.ok(); // Nothing to do, commit and return
            return Ok(vec![]);
        }

        // 2. Update the `locked_until` for the locked rows
        let placeholders = locked_ids
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");
        let update_query = format!(
            "UPDATE {} SET locked_until = NOW() + INTERVAL 60 SECOND WHERE id IN ({})",
            self.table, placeholders
        );

        let mut query = sqlx::query(audited_sql(&update_query));
        for id in &locked_ids {
            query = query.bind(*id);
        }

        query
            .execute(&mut *tx)
            .await
            .map_err(|e| ConsumerError::Connection(e.into()))?;

        // 3. Select the full rows that we just locked
        let select_query = format!(
            "SELECT id, payload FROM {} WHERE id IN ({})",
            self.table, placeholders
        );

        let mut query = sqlx::query(audited_sql(&select_query));
        for id in &locked_ids {
            query = query.bind(*id);
        }

        let rows = query
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| ConsumerError::Connection(e.into()))?;

        // 4. Commit the transaction
        tx.commit()
            .await
            .map_err(|e| ConsumerError::Connection(e.into()))?;

        Ok(rows)
    }

    async fn fetch_and_lock_sqlite(
        &self,
        limit: usize,
    ) -> Result<Vec<sqlx::any::AnyRow>, ConsumerError> {
        // Use `BEGIN IMMEDIATE` to acquire a RESERVED lock on the database file,
        // preventing other connections from reading until this transaction is complete.
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| ConsumerError::Connection(e.into()))?;

        let select_query = format!(
            "SELECT id FROM {} WHERE locked_until IS NULL OR locked_until < datetime('now') ORDER BY id LIMIT ?",
            self.table
        );

        let locked_ids: Vec<i64> = sqlx::query(audited_sql(&select_query))
            .bind(limit as i64)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| ConsumerError::Connection(e.into()))?
            .into_iter()
            .map(|row| row.get("id"))
            .collect();

        if locked_ids.is_empty() {
            tx.commit().await.ok();
            return Ok(vec![]);
        }

        let placeholders = locked_ids
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");
        let update_query = format!(
            "UPDATE {} SET locked_until = datetime('now', '+60 seconds') WHERE id IN ({})",
            self.table, placeholders
        );

        let mut query = sqlx::query(audited_sql(&update_query));
        for id in &locked_ids {
            query = query.bind(*id);
        }
        query
            .execute(&mut *tx)
            .await
            .map_err(|e| ConsumerError::Connection(e.into()))?;

        let select_payload_query = format!(
            "SELECT id, payload FROM {} WHERE id IN ({})",
            self.table, placeholders
        );
        let mut query = sqlx::query(audited_sql(&select_payload_query));
        for id in &locked_ids {
            query = query.bind(*id);
        }
        let rows = query
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| ConsumerError::Connection(e.into()))?;

        tx.commit()
            .await
            .map_err(|e| ConsumerError::Connection(e.into()))?;

        Ok(rows)
    }
    async fn get_pending_count(&self) -> anyhow::Result<usize> {
        let query = match self.driver_name.as_str() {
            "PostgreSQL" | "MySQL" | "MariaDB" => format!(
                "SELECT COUNT(*) FROM {} WHERE locked_until IS NULL OR locked_until < NOW()",
                self.table
            ),
            "SQLite" => format!(
                "SELECT COUNT(*) FROM {} WHERE locked_until IS NULL OR locked_until < datetime('now')",
                self.table
            ),
            "Microsoft SQL Server" => format!(
                "SELECT COUNT(*) FROM {} WHERE locked_until IS NULL OR locked_until < GETUTCDATE()",
                self.table
            ),
            _ => anyhow::bail!("Unsupported driver for pending count: {}", self.driver_name),
        };

        let row: sqlx::any::AnyRow = sqlx::query(audited_sql(&query))
            .fetch_one(&self.pool)
            .await?;
        if let Ok(c) = row.try_get::<i64, _>(0) {
            usize::try_from(c).map_err(|e| anyhow!("i64 to usize conversion failed: {}", e))
        } else {
            let c: i32 = row.try_get(0)?;
            usize::try_from(c).map_err(|e| anyhow!("i32 to usize conversion failed: {}", e))
        }
    }
}
#[async_trait]
impl MessageConsumer for SqlxConsumer {
    // Acking deletes rows by id (`DELETE ... WHERE id IN (...)`), so each batch's
    // commit is independent; out-of-order concurrent commits cannot lose other
    // batches' rows.
    fn commit_requires_order(&self) -> bool {
        false
    }
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch {
                messages: Vec::new(),
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            });
        }
        let rows = match self.driver_name.as_str() {
            "PostgreSQL" | "Microsoft SQL Server" => sqlx::query(audited_sql(&self.select_query))
                .bind(max_messages as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| ConsumerError::Connection(anyhow!(e)))?,
            "MySQL" | "MariaDB" => self.fetch_and_lock_mysql(max_messages).await?,
            "SQLite" => self.fetch_and_lock_sqlite(max_messages).await?,
            _ => {
                // Fallback for unknown drivers with a simple, non-locking read.
                warn!("SQLx consumer for driver '{}' is using a non-locking read strategy. This is not safe for concurrent consumers.", self.driver_name);
                let final_query = format!("{} LIMIT ?", self.select_query);
                sqlx::query(audited_sql(&final_query))
                    .bind(max_messages as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| ConsumerError::Connection(anyhow!(e)))?
            }
        };

        if rows.is_empty() {
            // Source is drained: sleep to preserve the DB polling cadence, then
            // surface an empty batch so the route can pause (empty_batch_delay_ms)
            // or, when exit_on_empty is set, terminate gracefully.
            tokio::time::sleep(self.polling_interval).await;
            return Ok(ReceivedBatch {
                messages: Vec::new(),
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            });
        }

        let mut messages = Vec::new();
        let mut ids_to_delete = Vec::new();

        for row in rows.into_iter().take(max_messages) {
            let payload: Vec<u8> = row
                .try_get("payload")
                .context("Failed to get 'payload' column")?;
            let id: i64 = row.try_get("id").context("Failed to get 'id' column")?;
            messages.push(CanonicalMessage::new(payload, None));
            ids_to_delete.push(id);
        }
        trace!(count = messages.len(), "Received batch of SQLx messages");

        let pool = self.pool.clone();
        let table = self.table.clone();
        let delete = self.delete_after_read;
        let driver_name = self.driver_name.clone();

        let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
            let pool = pool.clone();
            let table = table.clone();
            let ids = ids_to_delete.clone();
            let driver_name = driver_name.clone();
            Box::pin(async move {
                if !delete {
                    return Ok(());
                }
                let mut ids_to_ack = Vec::new();
                for (i, disp) in dispositions.iter().enumerate() {
                    let should_ack = match disp {
                        MessageDisposition::Ack => true,
                        MessageDisposition::Reply(_) => {
                            tracing::warn!("SQLx consumer received a Reply/StreamReply, but replying is not supported by this endpoint. The reply payload is dropped, and the original message is acknowledged.");
                            true
                        }
                        MessageDisposition::Nack => false,
                    };

                    if should_ack {
                        if let Some(id) = ids.get(i) {
                            ids_to_ack.push(*id);
                        }
                    }
                }

                if !ids_to_ack.is_empty() {
                    // Manually construct the query with appropriate placeholders
                    // because sqlx::QueryBuilder with the `Any` driver does not
                    // correctly rewrite `?` to `$N` for PostgreSQL in this context.
                    let mut placeholders = String::new();
                    for i in 0..ids_to_ack.len() {
                        if i > 0 {
                            placeholders.push_str(", ");
                        }
                        match driver_name.as_str() {
                            "PostgreSQL" => placeholders.push_str(&format!("${}", i + 1)),
                            "Microsoft SQL Server" => {
                                placeholders.push_str(&format!("@p{}", i + 1))
                            }
                            _ => placeholders.push('?'),
                        }
                    }

                    let sql = format!("DELETE FROM {} WHERE id IN ({})", table, placeholders);

                    let mut attempts = 0;
                    loop {
                        let mut query = sqlx::query(audited_sql(&sql));
                        for id in &ids_to_ack {
                            query = query.bind(*id);
                        }

                        match query.execute(&pool).await {
                            Ok(_) => break,
                            Err(e) => {
                                if is_deadlock_error(&e) && attempts < 5 {
                                    attempts += 1;
                                    warn!(
                                        attempts,
                                        error = %e,
                                        "Deadlock detected during SQLx commit, retrying..."
                                    );
                                    tokio::time::sleep(Duration::from_millis(attempts * 50)).await;
                                    continue;
                                }
                                return Err(anyhow!("Failed to delete acked messages: {}", e));
                            }
                        }
                    }
                }
                Ok(())
            }) as BoxFuture<'static, anyhow::Result<()>>
        });

        Ok(ReceivedBatch { messages, commit })
    }

    async fn status(&self) -> EndpointStatus {
        let (mut healthy, mut error) = match self.pool.acquire().await {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };

        let mut pending = None;
        if healthy {
            match self.get_pending_count().await {
                Ok(c) => pending = Some(c),
                Err(e) => {
                    healthy = false;
                    error = Some(e.to_string());
                }
            }
        };

        EndpointStatus {
            healthy,
            target: self.table.clone(),
            pending,
            error,
            details: serde_json::json!({ "driver": self.driver_name, "pool_size": self.pool.size(), "pool_idle": self.pool.num_idle() }),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// --- Non-destructive `cursor_column` reader (arbitrary tables) ---

/// A cursor value, tracked per column and persisted as a tagged string.
#[derive(Clone, Debug, PartialEq)]
enum SqlCursor {
    Int(i64),
    Text(String),
}

impl SqlCursor {
    fn encode(&self) -> String {
        match self {
            SqlCursor::Int(n) => format!("int:{}", n),
            SqlCursor::Text(s) => format!("str:{}", s),
        }
    }

    fn decode(s: &str) -> Option<SqlCursor> {
        let (tag, val) = s.split_once(':')?;
        match tag {
            "int" => val.parse::<i64>().ok().map(SqlCursor::Int),
            "str" => Some(SqlCursor::Text(val.to_string())),
            _ => None,
        }
    }
}

/// Serialize a full row into a JSON object payload (`{column: value, ...}`), trying the
/// value types the `Any` driver supports. Unknown/unsupported types bind to JSON null.
fn row_to_json(row: &sqlx::any::AnyRow) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for col in row.columns() {
        map.insert(
            col.name().to_string(),
            extract_json_value(row, col.ordinal()),
        );
    }
    serde_json::Value::Object(map)
}

fn extract_json_value(row: &sqlx::any::AnyRow, idx: usize) -> serde_json::Value {
    use serde_json::Value;
    if let Ok(v) = row.try_get::<Option<i64>, _>(idx) {
        return v.map(Value::from).unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<Option<f64>, _>(idx) {
        return v.map(Value::from).unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<Option<bool>, _>(idx) {
        return v.map(Value::from).unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
        return v.map(Value::from).unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(idx) {
        // Bytes have no JSON scalar; expose as a base16 string so the copy is lossless-ish.
        return v
            .map(|b| Value::from(b.iter().map(|x| format!("{:02x}", x)).collect::<String>()))
            .unwrap_or(Value::Null);
    }
    Value::Null
}

fn extract_cursor(row: &sqlx::any::AnyRow, column: &str) -> Option<SqlCursor> {
    if let Ok(Some(n)) = row.try_get::<Option<i64>, _>(column) {
        return Some(SqlCursor::Int(n));
    }
    if let Ok(Some(s)) = row.try_get::<Option<String>, _>(column) {
        return Some(SqlCursor::Text(s));
    }
    None
}

/// Checkpoint store backed by a `mqb_cursors` table in the source database.
struct SqlTableCheckpointStore {
    pool: AnyPool,
    driver_name: String,
    meta_table: String,
    cursor_id: String,
}

impl SqlTableCheckpointStore {
    async fn ensure_table(&self) -> anyhow::Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (cursor_id VARCHAR(255) PRIMARY KEY, last_value TEXT)",
            self.meta_table
        );
        sqlx::query(audited_sql(&sql))
            .execute(&self.pool)
            .await
            .with_context(|| format!("Failed to create meta table '{}'", self.meta_table))?;
        Ok(())
    }
}

#[async_trait]
impl crate::checkpoint::CheckpointStore for SqlTableCheckpointStore {
    async fn load(&self) -> anyhow::Result<Option<String>> {
        let sql = format!(
            "SELECT last_value FROM {} WHERE cursor_id = {}",
            self.meta_table,
            positional_placeholder(&self.driver_name, 1)
        );
        let row = sqlx::query(audited_sql(&sql))
            .bind(self.cursor_id.clone())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.and_then(|r| r.try_get::<Option<String>, _>("last_value").ok().flatten()))
    }

    async fn save(&self, value: &str) -> anyhow::Result<()> {
        let p1 = positional_placeholder(&self.driver_name, 1);
        let p2 = positional_placeholder(&self.driver_name, 2);
        let sql = match self.driver_name.as_str() {
            "MySQL" | "MariaDB" => format!(
                "INSERT INTO {0} (cursor_id, last_value) VALUES ({1}, {2}) \
                 ON DUPLICATE KEY UPDATE last_value = VALUES(last_value)",
                self.meta_table, p1, p2
            ),
            _ => format!(
                "INSERT INTO {0} (cursor_id, last_value) VALUES ({1}, {2}) \
                 ON CONFLICT (cursor_id) DO UPDATE SET last_value = excluded.last_value",
                self.meta_table, p1, p2
            ),
        };
        sqlx::query(audited_sql(&sql))
            .bind(self.cursor_id.clone())
            .bind(value.to_string())
            .execute(&self.pool)
            .await
            .with_context(|| format!("Failed to persist cursor to '{}'", self.meta_table))?;
        Ok(())
    }
}

/// Build a checkpoint store on an **external** SQL database (its own pool), creating the meta
/// table if needed. Used when `checkpoint_store` is a `postgres|mysql|sqlite://…` URL.
pub(crate) async fn build_sql_checkpoint_store(
    url: &str,
    table: Option<String>,
    source_name: &str,
    cursor_id: &str,
) -> anyhow::Result<Arc<dyn crate::checkpoint::CheckpointStore>> {
    sqlx::any::install_default_drivers();
    let pool = AnyPool::connect(url)
        .await
        .with_context(|| format!("Failed to connect checkpoint store at '{}'", url))?;
    let driver_name = {
        let conn = pool.acquire().await?;
        let name = conn.backend_name().to_string();
        drop(conn);
        name
    };
    let meta_table = table.unwrap_or_else(|| crate::checkpoint::default_meta_name(source_name));
    source_sql_checkpoint_store(pool, driver_name, meta_table, source_name, cursor_id).await
}

/// Build a checkpoint store on an already-connected pool (typically the source's own datastore),
/// creating the meta table if needed.
async fn source_sql_checkpoint_store(
    pool: AnyPool,
    driver_name: String,
    meta_table: String,
    source_name: &str,
    cursor_id: &str,
) -> anyhow::Result<Arc<dyn crate::checkpoint::CheckpointStore>> {
    if !is_valid_table_name(&meta_table) {
        return Err(anyhow!("Invalid checkpoint table name: '{}'.", meta_table));
    }
    let store = SqlTableCheckpointStore {
        pool,
        driver_name,
        meta_table,
        cursor_id: crate::checkpoint::checkpoint_key(source_name, cursor_id),
    };
    store.ensure_table().await?;
    Ok(Arc::new(store))
}

/// A non-destructive, resumable reader over an **arbitrary** SQL table. Pages by a
/// monotonic `cursor_column` (`SELECT * ... WHERE col > $last ORDER BY col ASC LIMIT n`),
/// never deletes/locks source rows, and persists the last successfully-sunk value (keyed
/// by `cursor_id`) to a pluggable checkpoint store (a `mqb_cursors` table by default, or a
/// local file). At-least-once. Supported drivers: PostgreSQL, MySQL/MariaDB, SQLite.
pub struct SqlxCursorReader {
    pool: AnyPool,
    table: String,
    cursor_column: String,
    driver_name: String,
    polling_interval: Duration,
    checkpoint: Option<Arc<dyn crate::checkpoint::CheckpointStore>>,
    last_value: Arc<Mutex<Option<SqlCursor>>>,
}

impl SqlxCursorReader {
    pub async fn new(config: &SqlxConfig) -> anyhow::Result<Self> {
        sqlx::any::install_default_drivers();
        if config.delete_after_read {
            return Err(anyhow!(
                "SQLx `cursor_column` (non-destructive) and `delete_after_read` are mutually exclusive"
            ));
        }
        if !is_valid_table_name(&config.table) {
            return Err(anyhow!("Invalid table name: '{}'.", config.table));
        }
        let cursor_column = config
            .cursor_column
            .clone()
            .ok_or_else(|| anyhow!("cursor_column is required for the SQLx cursor reader"))?;
        if !is_valid_table_name(&cursor_column) {
            return Err(anyhow!("Invalid cursor_column name: '{}'.", cursor_column));
        }

        let pool = create_sqlx_pool(config).await?;
        let conn = pool.acquire().await?;
        let driver_name = conn.backend_name().to_string();
        drop(conn);

        if driver_name == "Microsoft SQL Server" {
            return Err(anyhow!(
                "cursor_column mode is not supported for Microsoft SQL Server"
            ));
        }
        info!(table = %config.table, column = %cursor_column, driver = %driver_name, "SQLx cursor reader connected");

        let checkpoint: Option<Arc<dyn crate::checkpoint::CheckpointStore>> = if let Some(cid) =
            &config.cursor_id
        {
            use crate::checkpoint::CheckpointBackend;
            let backend = match &config.checkpoint_store {
                // Absent: source datastore with an auto-unique meta table.
                None => CheckpointBackend::Source {
                    name: crate::checkpoint::default_meta_name(&config.table),
                },
                Some(spec) => crate::checkpoint::parse_checkpoint_store(spec)?,
            };
            let store = match backend {
                CheckpointBackend::Source { name } => {
                    source_sql_checkpoint_store(
                        pool.clone(),
                        driver_name.clone(),
                        name,
                        &config.table,
                        cid,
                    )
                    .await?
                }
                external => {
                    crate::checkpoint::build_external_store(external, &config.table, cid).await?
                }
            };
            Some(store)
        } else {
            warn!(
                table = %config.table,
                "SQLx cursor reader has no cursor_id; resume is disabled and every restart re-copies from the beginning. Set cursor_id to persist progress."
            );
            None
        };

        let last_value = match &checkpoint {
            Some(cp) => cp.load().await?.and_then(|s| {
                let decoded = SqlCursor::decode(&s);
                if decoded.is_none() {
                    warn!(value = %s, "Ignoring unparseable sql cursor; starting from beginning");
                }
                decoded
            }),
            None => None,
        };
        info!(table = %config.table, cursor_id = ?config.cursor_id, has_checkpoint = %last_value.is_some(), "SQLx cursor reader initialized");

        Ok(Self {
            pool,
            table: config.table.clone(),
            cursor_column,
            driver_name,
            polling_interval: Duration::from_millis(config.polling_interval_ms.unwrap_or(100)),
            checkpoint,
            last_value: Arc::new(Mutex::new(last_value)),
        })
    }
}

#[async_trait]
impl MessageConsumer for SqlxCursorReader {
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch {
                messages: Vec::new(),
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            });
        }

        let last = self.last_value.lock().unwrap().clone();
        let sql = match &last {
            Some(_) => format!(
                "SELECT * FROM {0} WHERE {1} > {2} ORDER BY {1} ASC LIMIT {3}",
                self.table,
                self.cursor_column,
                positional_placeholder(&self.driver_name, 1),
                positional_placeholder(&self.driver_name, 2),
            ),
            None => format!(
                "SELECT * FROM {0} ORDER BY {1} ASC LIMIT {2}",
                self.table,
                self.cursor_column,
                positional_placeholder(&self.driver_name, 1),
            ),
        };

        let mut query = sqlx::query(audited_sql(&sql));
        if let Some(c) = &last {
            query = match c {
                SqlCursor::Int(n) => query.bind(*n),
                SqlCursor::Text(s) => query.bind(s.clone()),
            };
        }
        // Peek one extra row beyond the batch so we can detect a run of equal cursor
        // values split across the LIMIT boundary; `col > last` would otherwise skip the
        // remainder of that run (silent row loss for a non-unique cursor_column).
        let fetch_limit = (max_messages as i64).saturating_add(1);
        query = query.bind(fetch_limit);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| ConsumerError::Connection(anyhow!(e)))?;

        if rows.is_empty() {
            // Drained: preserve polling cadence, then surface an empty batch so the route
            // can pause or terminate.
            tokio::time::sleep(self.polling_interval).await;
            return Ok(ReceivedBatch {
                messages: Vec::new(),
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            });
        }

        // Extract (cursor, message) for every fetched row.
        let mut fetched: Vec<(SqlCursor, CanonicalMessage)> = Vec::with_capacity(rows.len());
        for row in &rows {
            let cursor = extract_cursor(row, &self.cursor_column).ok_or_else(|| {
                ConsumerError::Connection(anyhow!(
                    "cursor_column '{}' missing or of unsupported type in result row",
                    self.cursor_column
                ))
            })?;
            let payload = serde_json::to_vec(&row_to_json(row)).unwrap_or_default();
            fetched.push((cursor, CanonicalMessage::new(payload, None)));
        }

        // If we fetched the peek row, more rows exist beyond this page. Drop the trailing
        // run whose value equals the peek row's value so a group of equal cursor values is
        // never split across pages; the trimmed rows are re-read next poll via `col > last`.
        let had_more = fetched.len() > max_messages;
        let mut emit_len = fetched.len().min(max_messages);
        if had_more {
            let peek_val = fetched[max_messages].0.clone();
            while emit_len > 0 && fetched[emit_len - 1].0 == peek_val {
                emit_len -= 1;
            }
            if emit_len == 0 {
                // A single cursor value fills the whole batch: it cannot be split safely.
                // Emit the batch and advance past the value, warning that any further rows
                // sharing this exact value may be skipped.
                warn!(
                    column = %self.cursor_column,
                    "cursor_column has a group of equal values larger than the batch size; increase batch_size to avoid skipping rows at this value"
                );
                emit_len = max_messages;
            }
        }
        fetched.truncate(emit_len);

        let mut messages = Vec::with_capacity(fetched.len());
        let mut cursors: Vec<SqlCursor> = Vec::with_capacity(fetched.len());
        for (cursor, msg) in fetched {
            cursors.push(cursor.clone());
            messages.push(msg);
            // Advance optimistically so the next page continues past this row; rolled back
            // in commit if a row is not acked.
            *self.last_value.lock().unwrap() = Some(cursor);
        }
        trace!(count = messages.len(), "Received batch of SQLx cursor rows");

        let checkpoint = self.checkpoint.clone();
        let last_value = self.last_value.clone();
        let resume_from = last; // cursor value before this batch (for rollback on nack)
        let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
            Box::pin(async move {
                // Count the contiguous run of Acks from the front (stop at first Nack).
                let mut acked = 0usize;
                for disp in dispositions.iter().take(cursors.len()) {
                    if matches!(disp, MessageDisposition::Ack | MessageDisposition::Reply(_)) {
                        acked += 1;
                    } else {
                        break;
                    }
                }
                let boundary = if acked == 0 {
                    resume_from
                } else {
                    Some(cursors[acked - 1].clone())
                };
                // If any row was not acked, roll the in-memory read cursor back to the
                // committed boundary so nacked/unprocessed rows are re-read next poll
                // (at-least-once) instead of being skipped until a restart.
                if acked < cursors.len() {
                    *last_value.lock().unwrap() = boundary.clone();
                }
                if let (Some(cur), Some(cp)) = (boundary, checkpoint) {
                    if let Err(e) = cp.save(&cur.encode()).await {
                        tracing::warn!(error = %e, "Failed to persist sql cursor. Rows may be reprocessed on restart.");
                    }
                }
                Ok(())
            }) as BoxFuture<'static, anyhow::Result<()>>
        });

        Ok(ReceivedBatch { messages, commit })
    }

    async fn status(&self) -> EndpointStatus {
        let (healthy, error) = match self.pool.acquire().await {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };
        EndpointStatus {
            healthy,
            target: self.table.clone(),
            error,
            details: serde_json::json!({ "driver": self.driver_name, "mode": "cursor_column", "cursor_column": self.cursor_column }),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{MessageConsumer, MessagePublisher};
    use tempfile::tempdir;

    async fn setup_db_file() -> (tempfile::TempDir, String) {
        use sqlx::Connection;
        sqlx::any::install_default_drivers();
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        #[cfg(windows)]
        let url = format!("sqlite:///{}", path.to_string_lossy().replace('\\', "/"));
        #[cfg(not(windows))]
        let url = format!("sqlite://{}", path.to_str().unwrap());

        // Explicitly create the file first and drop the handle to avoid locking issues on Windows.
        // The `connect` call will create the file if it doesn't exist, but this can be racy in tests.
        drop(tokio::fs::File::create(&path).await.unwrap());

        let mut conn = sqlx::AnyConnection::connect(&url).await.unwrap();
        sqlx::query(
            "CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload BLOB NOT NULL,
                locked_until DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        conn.close().await.unwrap();
        (dir, url)
    }

    /// Creates an arbitrary (non mq-bridge) table `orders(id, sku, qty)` seeded with `n` rows.
    async fn setup_arbitrary_table(n: i64) -> (tempfile::TempDir, String, AnyPool) {
        sqlx::any::install_default_drivers();
        let dir = tempdir().unwrap();
        let path = dir.path().join("arb.db");
        let url = format!("sqlite://{}", path.to_str().unwrap());
        drop(tokio::fs::File::create(&path).await.unwrap());
        let pool = AnyPool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE orders (id INTEGER PRIMARY KEY, sku TEXT, qty INTEGER)")
            .execute(&pool)
            .await
            .unwrap();
        for i in 1..=n {
            sqlx::query("INSERT INTO orders (id, sku, qty) VALUES (?, ?, ?)")
                .bind(i)
                .bind(format!("sku{}", i))
                .bind(i * 10)
                .execute(&pool)
                .await
                .unwrap();
        }
        (dir, url, pool)
    }

    #[test]
    fn test_sql_cursor_encode_decode_roundtrip() {
        for c in [SqlCursor::Int(42), SqlCursor::Text("abc:def".into())] {
            assert_eq!(SqlCursor::decode(&c.encode()), Some(c));
        }
        assert_eq!(SqlCursor::decode("garbage"), None);
    }

    // Regression: a non-unique cursor_column must not lose rows that share the value at a
    // page boundary. `ts` has a duplicate (20) straddling a batch of 2; all 5 rows must be
    // emitted exactly once. The naive `col > last` + LIMIT would drop the second ts=20.
    #[tokio::test]
    async fn test_sqlx_cursor_reader_non_unique_column_no_loss() {
        sqlx::any::install_default_drivers();
        let dir = tempdir().unwrap();
        let path = dir.path().join("dup.db");
        let url = format!("sqlite://{}", path.to_str().unwrap());
        drop(tokio::fs::File::create(&path).await.unwrap());
        let pool = AnyPool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER)")
            .execute(&pool)
            .await
            .unwrap();
        for (id, ts) in [(1, 10), (2, 20), (3, 20), (4, 30), (5, 40)] {
            sqlx::query("INSERT INTO events (id, ts) VALUES (?, ?)")
                .bind(id)
                .bind(ts)
                .execute(&pool)
                .await
                .unwrap();
        }

        let config = SqlxConfig {
            url: url.clone(),
            table: "events".to_string(),
            cursor_column: Some("ts".to_string()),
            ..Default::default()
        };
        let mut reader = SqlxCursorReader::new(&config).await.unwrap();

        let mut ids = Vec::new();
        loop {
            let b = reader.receive_batch(2).await.unwrap();
            if b.messages.is_empty() {
                break;
            }
            for m in &b.messages {
                let v: serde_json::Value = serde_json::from_slice(&m.payload).unwrap();
                ids.push(v["id"].as_i64().unwrap());
            }
            let n = b.messages.len();
            (b.commit)(vec![MessageDisposition::Ack; n]).await.unwrap();
        }
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![1, 2, 3, 4, 5],
            "no row lost at the duplicate boundary"
        );
    }

    // Regression: a mid-batch Nack must make the nacked (and following) rows re-read by the
    // same running reader, not skipped until a restart. Rows 1..4 are read; row 3 is nacked,
    // so the next receive_batch on the same reader resumes at row 3.
    #[tokio::test]
    async fn test_sqlx_cursor_reader_nack_redelivers_in_process() {
        let (_dir, url, _pool) = setup_arbitrary_table(5).await;
        let config = SqlxConfig {
            url: url.clone(),
            table: "orders".to_string(),
            cursor_column: Some("id".to_string()),
            ..Default::default()
        };
        let mut reader = SqlxCursorReader::new(&config).await.unwrap();

        let b = reader.receive_batch(4).await.unwrap();
        assert_eq!(b.messages.len(), 4);
        (b.commit)(vec![
            MessageDisposition::Ack,
            MessageDisposition::Ack,
            MessageDisposition::Nack,
            MessageDisposition::Ack,
        ])
        .await
        .unwrap();

        // Same reader (no restart): must re-read from row 3 (the first nacked row).
        let b2 = reader.receive_batch(4).await.unwrap();
        let ids: Vec<i64> = b2
            .messages
            .iter()
            .map(|m| {
                serde_json::from_slice::<serde_json::Value>(&m.payload).unwrap()["id"]
                    .as_i64()
                    .unwrap()
            })
            .collect();
        assert_eq!(
            ids,
            vec![3, 4, 5],
            "nacked rows must be redelivered in-process"
        );
    }

    #[tokio::test]
    async fn test_sqlx_cursor_reader_resumes_and_is_nondestructive() {
        let (_dir, url, pool) = setup_arbitrary_table(5).await;
        let config = SqlxConfig {
            url: url.clone(),
            table: "orders".to_string(),
            cursor_column: Some("id".to_string()),
            cursor_id: Some("copy-1".to_string()),
            ..Default::default()
        };

        let mut reader = SqlxCursorReader::new(&config).await.unwrap();
        let b1 = reader.receive_batch(3).await.unwrap();
        assert_eq!(b1.messages.len(), 3);
        // Payload is the full row serialized to JSON.
        let v: serde_json::Value = serde_json::from_slice(&b1.messages[0].payload).unwrap();
        assert_eq!(v["id"], 1);
        assert_eq!(v["sku"], "sku1");
        assert_eq!(v["qty"], 10);
        (b1.commit)(vec![MessageDisposition::Ack; 3]).await.unwrap();

        let b2 = reader.receive_batch(3).await.unwrap();
        assert_eq!(b2.messages.len(), 2);
        (b2.commit)(vec![MessageDisposition::Ack; 2]).await.unwrap();

        // Drained -> empty batch, independent of how the route handles empty batches.
        let b3 = reader.receive_batch(3).await.unwrap();
        assert!(b3.messages.is_empty());

        // Source table is untouched.
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM orders")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 5);

        // Checkpoint persisted in the auto-unique default meta table, keyed by <source>:<id>.
        let last: String = sqlx::query_scalar(
            "SELECT last_value FROM mqb_cursors_orders WHERE cursor_id = 'orders:copy-1'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(last, "int:5");

        // Restart from a fresh reader: resumes past the checkpoint -> nothing re-emitted.
        let mut reader2 = SqlxCursorReader::new(&config).await.unwrap();
        let again = reader2.receive_batch(10).await.unwrap();
        assert!(again.messages.is_empty());
    }

    #[tokio::test]
    async fn test_sqlx_cursor_reader_partial_ack_resumes_at_boundary() {
        let (_dir, url, _pool) = setup_arbitrary_table(5).await;
        let config = SqlxConfig {
            url: url.clone(),
            table: "orders".to_string(),
            cursor_column: Some("id".to_string()),
            cursor_id: Some("copy-1".to_string()),
            ..Default::default()
        };

        let mut reader = SqlxCursorReader::new(&config).await.unwrap();
        let b = reader.receive_batch(4).await.unwrap();
        assert_eq!(b.messages.len(), 4);
        // Ack the first two, nack the rest: checkpoint must stop at the contiguous boundary.
        (b.commit)(vec![
            MessageDisposition::Ack,
            MessageDisposition::Ack,
            MessageDisposition::Nack,
            MessageDisposition::Nack,
        ])
        .await
        .unwrap();

        // A restart resumes at row 3 (ids 3,4,5), never skipping the nacked rows.
        let mut reader2 = SqlxCursorReader::new(&config).await.unwrap();
        let b2 = reader2.receive_batch(10).await.unwrap();
        assert_eq!(b2.messages.len(), 3);
        let first: serde_json::Value = serde_json::from_slice(&b2.messages[0].payload).unwrap();
        assert_eq!(first["id"], 3);
    }

    #[tokio::test]
    async fn test_sqlx_cursor_reader_text_column_with_file_checkpoint() {
        sqlx::any::install_default_drivers();
        let dir = tempdir().unwrap();
        let path = dir.path().join("ev.db");
        let url = format!("sqlite://{}", path.to_str().unwrap());
        drop(tokio::fs::File::create(&path).await.unwrap());
        let pool = AnyPool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE events (k TEXT PRIMARY KEY, data TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        for k in ["a", "b", "c"] {
            sqlx::query("INSERT INTO events (k, data) VALUES (?, ?)")
                .bind(k)
                .bind(format!("data-{}", k))
                .execute(&pool)
                .await
                .unwrap();
        }

        let ckpt = dir.path().join("cursors.json");
        // Absolute tempdir path -> `file:///abs/path` (three-slash form).
        let config = SqlxConfig {
            url: url.clone(),
            table: "events".to_string(),
            cursor_column: Some("k".to_string()),
            cursor_id: Some("c1".to_string()),
            checkpoint_store: Some(format!("file://{}", ckpt.to_str().unwrap())),
            ..Default::default()
        };

        let mut reader = SqlxCursorReader::new(&config).await.unwrap();
        let b = reader.receive_batch(10).await.unwrap();
        assert_eq!(b.messages.len(), 3);
        (b.commit)(vec![MessageDisposition::Ack; 3]).await.unwrap();

        // File checkpoint written; source DB has NO meta table (read-only-source path).
        assert!(ckpt.exists());
        let meta_tables: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'mqb_cursors%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(meta_tables, 0);

        // Restart resumes from the file checkpoint -> nothing re-emitted.
        let mut reader2 = SqlxCursorReader::new(&config).await.unwrap();
        let again = reader2.receive_batch(10).await.unwrap();
        assert!(again.messages.is_empty());
    }

    // checkpoint_store pointing at a *different* database persists the cursor there, leaves the
    // source DB untouched, and still resumes across a restart.
    #[tokio::test]
    async fn test_sqlx_cursor_reader_external_db_checkpoint() {
        let (_dir_a, url_a, pool_a) = setup_arbitrary_table(3).await;

        // A separate SQLite database used only for checkpoints.
        let dir_b = tempdir().unwrap();
        let path_b = dir_b.path().join("ckpt.db");
        let url_b = format!("sqlite://{}", path_b.to_str().unwrap());
        drop(tokio::fs::File::create(&path_b).await.unwrap());
        let pool_b = AnyPool::connect(&url_b).await.unwrap();

        let config = SqlxConfig {
            url: url_a.clone(),
            table: "orders".to_string(),
            cursor_column: Some("id".to_string()),
            cursor_id: Some("copy-1".to_string()),
            checkpoint_store: Some(url_b.clone()),
            ..Default::default()
        };

        let mut reader = SqlxCursorReader::new(&config).await.unwrap();
        let b = reader.receive_batch(10).await.unwrap();
        assert_eq!(b.messages.len(), 3);
        (b.commit)(vec![MessageDisposition::Ack; 3]).await.unwrap();

        // Cursor landed in the external DB, in the auto-unique table keyed by <source>:<id>.
        let last: String = sqlx::query_scalar(
            "SELECT last_value FROM mqb_cursors_orders WHERE cursor_id = 'orders:copy-1'",
        )
        .fetch_one(&pool_b)
        .await
        .unwrap();
        assert_eq!(last, "int:3");

        // The source DB was never written to (no meta table).
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'mqb_cursors%'",
        )
        .fetch_one(&pool_a)
        .await
        .unwrap();
        assert_eq!(n, 0);

        // Restart resumes from the external checkpoint -> nothing re-emitted.
        let mut reader2 = SqlxCursorReader::new(&config).await.unwrap();
        assert!(reader2.receive_batch(10).await.unwrap().messages.is_empty());
    }

    #[tokio::test]
    async fn test_sqlx_roundtrip_delete() {
        let (_dir, url) = setup_db_file().await;

        let config = SqlxConfig {
            url: url.clone(),
            table: "messages".to_string(),
            delete_after_read: true,
            ..Default::default()
        };

        let publisher = SqlxPublisher::new(&config).await.unwrap();
        let msg_payload = b"hello sqlx".to_vec();
        let msg = CanonicalMessage::new(msg_payload.clone(), None);
        publisher.send(msg).await.unwrap();

        let mut consumer = SqlxConsumer::new(&config).await.unwrap();
        let received_batch = consumer.receive_batch(1).await.unwrap();
        assert_eq!(received_batch.messages.len(), 1);
        assert_eq!(received_batch.messages[0].payload.as_ref(), &msg_payload);

        (received_batch.commit)(vec![MessageDisposition::Ack])
            .await
            .unwrap();

        let pool = AnyPool::connect(&url).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_sqlx_roundtrip_no_delete() {
        let (_dir, url) = setup_db_file().await;

        let config = SqlxConfig {
            url: url.clone(),
            table: "messages".to_string(),
            delete_after_read: false,
            ..Default::default()
        };

        let publisher = SqlxPublisher::new(&config).await.unwrap();
        let msg_payload = b"hello sqlx no delete".to_vec();
        let msg = CanonicalMessage::new(msg_payload.clone(), None);
        publisher.send(msg).await.unwrap();

        let mut consumer = SqlxConsumer::new(&config).await.unwrap();
        let received_batch = consumer.receive_batch(1).await.unwrap();
        assert_eq!(received_batch.messages.len(), 1);

        (received_batch.commit)(vec![MessageDisposition::Ack])
            .await
            .unwrap();

        let pool = AnyPool::connect(&url).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_parse_insert_template_no_tokens() {
        let (q, sources) =
            parse_insert_template("INSERT INTO t (payload) VALUES (?)", "SQLite").unwrap();
        assert_eq!(q, "INSERT INTO t (payload) VALUES (?)");
        assert!(sources.is_empty());
    }

    #[test]
    fn test_parse_insert_template_single_metadata() {
        let (q, sources) =
            parse_insert_template("INSERT INTO t (a) VALUES (${metadata:x})", "PostgreSQL")
                .unwrap();
        assert_eq!(q, "INSERT INTO t (a) VALUES ($1)");
        assert_eq!(sources, vec![ColumnSource::Metadata("x".to_string())]);
    }

    #[test]
    fn test_parse_insert_template_mixed_dialects() {
        let tpl = "INSERT INTO t (a, b) VALUES (${metadata:a}, ${payload:b})";
        let expected = vec![
            ColumnSource::Metadata("a".to_string()),
            ColumnSource::Payload("b".to_string()),
        ];

        let (q, s) = parse_insert_template(tpl, "PostgreSQL").unwrap();
        assert_eq!(q, "INSERT INTO t (a, b) VALUES ($1, $2)");
        assert_eq!(s, expected);

        let (q, s) = parse_insert_template(tpl, "Microsoft SQL Server").unwrap();
        assert_eq!(q, "INSERT INTO t (a, b) VALUES (@p1, @p2)");
        assert_eq!(s, expected);

        let (q, s) = parse_insert_template(tpl, "MySQL").unwrap();
        assert_eq!(q, "INSERT INTO t (a, b) VALUES (?, ?)");
        assert_eq!(s, expected);
    }

    #[test]
    fn test_parse_insert_template_malformed() {
        assert!(parse_insert_template("VALUES (${metadata:x)", "SQLite").is_err()); // unclosed
        assert!(parse_insert_template("VALUES (${bogus:x})", "SQLite").is_err()); // bad prefix
        assert!(parse_insert_template("VALUES (${metadata})", "SQLite").is_err()); // no ':'
        assert!(parse_insert_template("VALUES (${payload:})", "SQLite").is_err());
        // empty field
    }

    #[test]
    fn test_resolve_source_metadata() {
        let mut msg = CanonicalMessage::new(b"{}".to_vec(), None);
        msg.metadata.insert("k".to_string(), "v".to_string());
        let json = serde_json::from_slice(&msg.payload).ok();
        assert_eq!(
            resolve_source(&msg, &ColumnSource::Metadata("k".to_string()), &json),
            BindValue::Text("v".to_string())
        );
        assert_eq!(
            resolve_source(&msg, &ColumnSource::Metadata("nope".to_string()), &json),
            BindValue::Null
        );
    }

    #[test]
    fn test_resolve_source_payload_types() {
        let msg = CanonicalMessage::new(
            br#"{"s":"x","i":5,"f":1.5,"b":true,"arr":[1],"n":null}"#.to_vec(),
            None,
        );
        let json = serde_json::from_slice(&msg.payload).ok();
        let p = |f: &str| resolve_source(&msg, &ColumnSource::Payload(f.to_string()), &json);
        assert_eq!(p("s"), BindValue::Text("x".to_string()));
        assert_eq!(p("i"), BindValue::Int(5));
        assert_eq!(p("f"), BindValue::Float(1.5));
        assert_eq!(p("b"), BindValue::Bool(true));
        assert_eq!(p("arr"), BindValue::Null);
        assert_eq!(p("n"), BindValue::Null);
        assert_eq!(p("missing"), BindValue::Null);
    }

    #[test]
    fn test_resolve_source_no_fallback() {
        // Payload source must NOT fall back to metadata and vice versa.
        let mut msg = CanonicalMessage::new(b"not json".to_vec(), None);
        msg.metadata.insert("k".to_string(), "meta".to_string());
        let json: Option<serde_json::Value> = serde_json::from_slice(&msg.payload).ok();
        assert!(json.is_none());
        // payload:k -> Null even though metadata has "k"
        assert_eq!(
            resolve_source(&msg, &ColumnSource::Payload("k".to_string()), &json),
            BindValue::Null
        );
    }

    #[tokio::test]
    async fn test_sqlx_multicolumn_insert() {
        let (_dir, url) = setup_db_file().await;
        let pool = AnyPool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE orders (sku TEXT, qty INTEGER, cust TEXT)")
            .execute(&pool)
            .await
            .unwrap();

        let config = SqlxConfig {
            url: url.clone(),
            table: "orders".to_string(),
            insert_query: Some(
                "INSERT INTO orders (sku, qty, cust) VALUES (${payload:sku}, ${payload:qty}, ${metadata:cust})"
                    .to_string(),
            ),
            ..Default::default()
        };
        let publisher = SqlxPublisher::new(&config).await.unwrap();

        let mut msg = CanonicalMessage::new(br#"{"sku":"abc","qty":7}"#.to_vec(), None);
        msg.metadata.insert("cust".to_string(), "c1".to_string());
        publisher.send(msg).await.unwrap();

        let row = sqlx::query("SELECT sku, qty, cust FROM orders")
            .fetch_one(&pool)
            .await
            .unwrap();
        let sku: String = row.get("sku");
        let qty: i64 = row.get("qty");
        let cust: String = row.get("cust");
        assert_eq!(sku, "abc");
        assert_eq!(qty, 7);
        assert_eq!(cust, "c1");
    }

    #[tokio::test]
    async fn test_sqlx_multicolumn_non_json_payload_nulls() {
        let (_dir, url) = setup_db_file().await;
        let pool = AnyPool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE t (a TEXT, b TEXT)")
            .execute(&pool)
            .await
            .unwrap();

        let config = SqlxConfig {
            url: url.clone(),
            table: "t".to_string(),
            insert_query: Some(
                "INSERT INTO t (a, b) VALUES (${metadata:a}, ${payload:b})".to_string(),
            ),
            ..Default::default()
        };
        let publisher = SqlxPublisher::new(&config).await.unwrap();

        let mut msg = CanonicalMessage::new(b"raw non-json".to_vec(), None);
        msg.metadata.insert("a".to_string(), "meta_a".to_string());
        publisher.send(msg).await.unwrap();

        let row = sqlx::query("SELECT a, b FROM t")
            .fetch_one(&pool)
            .await
            .unwrap();
        let a: String = row.get("a");
        let b: Option<String> = row.get("b");
        assert_eq!(a, "meta_a");
        assert_eq!(b, None); // payload not JSON -> NULL, no fallback to metadata
    }

    #[tokio::test]
    async fn test_sqlx_multicolumn_batch() {
        let (_dir, url) = setup_db_file().await;
        let pool = AnyPool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE t (a TEXT, b INTEGER)")
            .execute(&pool)
            .await
            .unwrap();

        let config = SqlxConfig {
            url: url.clone(),
            table: "t".to_string(),
            insert_query: Some(
                "INSERT INTO t (a, b) VALUES (${metadata:a}, ${payload:b})".to_string(),
            ),
            ..Default::default()
        };
        let publisher = SqlxPublisher::new(&config).await.unwrap();

        let mut msgs = Vec::new();
        for i in 0..3 {
            let mut m = CanonicalMessage::new(format!("{{\"b\":{}}}", i * 10).into_bytes(), None);
            m.metadata.insert("a".to_string(), format!("row{}", i));
            msgs.push(m);
        }
        publisher.send_batch(msgs).await.unwrap();

        let rows = sqlx::query("SELECT a, b FROM t ORDER BY b")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        for (i, row) in rows.iter().enumerate() {
            let a: String = row.get("a");
            let b: i64 = row.get("b");
            assert_eq!(a, format!("row{}", i));
            assert_eq!(b, (i as i64) * 10);
        }
    }

    #[tokio::test]
    async fn test_sqlx_auto_create_rejects_tokens() {
        let (_dir, url) = setup_db_file().await;
        let config = SqlxConfig {
            url,
            table: "t".to_string(),
            auto_create_table: true,
            insert_query: Some("INSERT INTO t (a) VALUES (${payload:a})".to_string()),
            ..Default::default()
        };
        assert!(SqlxPublisher::new(&config).await.is_err());
    }

    #[tokio::test]
    async fn test_sqlx_status() {
        let (_dir, url) = setup_db_file().await;
        let config = SqlxConfig {
            url: url.clone(),
            table: "messages".to_string(),
            ..Default::default()
        };

        let publisher = SqlxPublisher::new(&config).await.unwrap();
        let status = publisher.status().await;
        assert!(status.healthy);
        assert_eq!(status.target, "messages");
        assert!(status.details.get("driver").is_some());
    }
}
