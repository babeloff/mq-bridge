//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! ClickHouse endpoint over the ClickHouse **HTTP interface**.
//!
//! ClickHouse is an OLAP columnar store, not a message queue, so the two roles are asymmetric:
//! - **Publisher (sink):** batch-inserts messages with `FORMAT JSONEachRow`. This is where ClickHouse
//!   shines and matches the bridge's `send_batch`.
//! - **Consumer (source):** reads an existing table **non-destructively** by paging over a monotonic
//!   `cursor_column` (there is no native pub/sub), serializing each row to a JSON payload. Mirrors the
//!   SQLx cursor reader and reuses the shared [`crate::checkpoint`] store for durable resume.
//!
//! We talk raw HTTP (via `reqwest`) rather than the typed `clickhouse` crate because that crate is
//! RowBinary/`Row`-derive only and cannot round-trip arbitrary dynamic JSON. Raw HTTP also avoids the
//! crate's `?`-as-bind-placeholder quirk that would corrupt JSON payloads containing `?`.

use crate::checkpoint::{self, CheckpointBackend, CheckpointStore};
use crate::models::ClickHouseConfig;
use crate::traits::{
    BoxFuture, ConsumerError, EndpointStatus, MessageConsumer, MessageDisposition,
    MessagePublisher, PublisherError, ReceivedBatch, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, trace, warn};

/// Validate a ClickHouse identifier. Table names may be schema-qualified (`db.table`); column names
/// may not. Only ASCII alphanumerics and `_` are allowed, keeping interpolation into SQL injection-safe.
fn is_valid_ident(name: &str, allow_dot: bool) -> bool {
    if name.is_empty() || name.starts_with('.') || name.ends_with('.') || name.contains("..") {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || (allow_dot && c == '.'))
}

/// Resolve a publisher column-mapping token for one message into a JSON value.
/// `${payload:<field>}` → top-level payload field (JSON type preserved); `${metadata:<key>}` →
/// metadata string; anything else is a literal string. Unresolvable tokens yield JSON null.
fn resolve_token(
    token: &str,
    msg: &CanonicalMessage,
    payload_json: &Option<serde_json::Value>,
) -> serde_json::Value {
    use serde_json::Value;
    if let Some(inner) = token.strip_prefix("${").and_then(|t| t.strip_suffix('}')) {
        if let Some((prefix, name)) = inner.split_once(':') {
            return match prefix.trim() {
                "payload" => payload_json
                    .as_ref()
                    .and_then(|v| v.get(name.trim()))
                    .cloned()
                    .unwrap_or(Value::Null),
                "metadata" => msg
                    .metadata
                    .get(name.trim())
                    .map(|s| Value::String(s.clone()))
                    .unwrap_or(Value::Null),
                _ => Value::String(token.to_string()),
            };
        }
    }
    Value::String(token.to_string())
}

/// Build the JSON object (one JSONEachRow row) for a single message.
/// With a `columns` mapping, each column is resolved from its token; otherwise the whole payload is
/// used and must itself be a JSON object.
fn build_row(
    msg: &CanonicalMessage,
    columns: &Option<std::collections::BTreeMap<String, String>>,
) -> anyhow::Result<serde_json::Value> {
    let payload_json: Option<serde_json::Value> = serde_json::from_slice(&msg.payload).ok();
    match columns {
        Some(map) => {
            let mut obj = serde_json::Map::with_capacity(map.len());
            for (col, token) in map {
                obj.insert(col.clone(), resolve_token(token, msg, &payload_json));
            }
            Ok(serde_json::Value::Object(obj))
        }
        None => match payload_json {
            Some(v @ serde_json::Value::Object(_)) => Ok(v),
            _ => Err(anyhow!(
                "ClickHouse default insert requires a JSON object payload; set `columns` to map fields for non-object payloads"
            )),
        },
    }
}

/// A minimal ClickHouse HTTP client: every statement is a POST whose body is the SQL (plus inline
/// data for inserts). Auth and target database travel as headers/params so payload `?` chars are never
/// treated as bind placeholders.
struct ChClient {
    http: reqwest::Client,
    url: String,
    database: String,
    user: String,
    password: String,
}

impl ChClient {
    fn from_config(config: &ClickHouseConfig) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder();
        if config.tls.accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }
        if let Some(ca) = &config.tls.ca_file {
            let pem = std::fs::read(ca)
                .with_context(|| format!("Failed to read ClickHouse CA file '{}'", ca))?;
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(&pem)
                    .with_context(|| format!("Invalid ClickHouse CA certificate '{}'", ca))?,
            );
        }
        let http = builder
            .build()
            .context("Failed to build ClickHouse HTTP client")?;
        Ok(Self {
            http,
            url: config.url.trim_end_matches('/').to_string(),
            database: config.database.clone().unwrap_or_else(|| "default".into()),
            user: config.username.clone().unwrap_or_else(|| "default".into()),
            password: config.password.clone().unwrap_or_default(),
        })
    }

    /// POST `sql` (which may include trailing JSONEachRow data) and return the response body.
    /// `extra` adds request query params (e.g. `async_insert`, `param_*` typed query parameters).
    /// When `gzip_body` is set the request body is gzip-compressed (`Content-Encoding: gzip`), worth
    /// it for large insert bodies. Response bodies are transparently gunzipped by reqwest's `gzip`
    /// feature whenever the server compresses them (enabled per-request via `enable_http_compression`).
    async fn run(
        &self,
        sql: &str,
        extra: &[(&str, &str)],
        gzip_body: bool,
    ) -> anyhow::Result<String> {
        let mut params: Vec<(&str, &str)> = vec![("database", self.database.as_str())];
        params.extend_from_slice(extra);
        let mut req = self
            .http
            .post(&self.url)
            .query(&params)
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password);
        if gzip_body {
            use std::io::Write;
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(sql.as_bytes())
                .context("Failed to gzip ClickHouse request body")?;
            let compressed = enc.finish().context("Failed to finish gzip encoding")?;
            req = req.header("Content-Encoding", "gzip").body(compressed);
        } else {
            req = req.body(sql.to_string());
        }
        let resp = req
            .send()
            .await
            .with_context(|| format!("ClickHouse request to '{}' failed", self.url))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(anyhow!("ClickHouse returned {}: {}", status, text.trim()));
        }
        Ok(text)
    }
}

// --- Publisher (sink) ---

pub struct ClickHousePublisher {
    client: ChClient,
    table: String,
    columns: Option<std::collections::BTreeMap<String, String>>,
    async_insert: bool,
}

impl ClickHousePublisher {
    pub async fn new(config: &ClickHouseConfig) -> anyhow::Result<Self> {
        if !is_valid_ident(&config.table, true) {
            return Err(anyhow!(
                "Invalid ClickHouse table name: '{}'.",
                config.table
            ));
        }
        if let Some(map) = &config.columns {
            for col in map.keys() {
                if !is_valid_ident(col, false) {
                    return Err(anyhow!("Invalid ClickHouse column name: '{}'.", col));
                }
            }
        }
        let client = ChClient::from_config(config)?;
        client
            .run("SELECT 1", &[], false)
            .await
            .context("ClickHouse publisher connection check failed")?;
        info!(table = %config.table, "ClickHouse publisher connected");
        Ok(Self {
            client,
            table: config.table.clone(),
            columns: config.columns.clone(),
            async_insert: config.async_insert,
        })
    }
}

#[async_trait]
impl MessagePublisher for ClickHousePublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }
        let mut body = format!("INSERT INTO {} FORMAT JSONEachRow\n", self.table);
        for msg in &messages {
            let row = build_row(msg, &self.columns).map_err(PublisherError::NonRetryable)?;
            let line = serde_json::to_string(&row).map_err(|e| {
                PublisherError::NonRetryable(anyhow!("Failed to serialize row: {}", e))
            })?;
            body.push_str(&line);
            body.push('\n');
        }
        let extra: &[(&str, &str)] = if self.async_insert {
            &[("async_insert", "1"), ("wait_for_async_insert", "1")]
        } else {
            &[]
        };
        self.client
            .run(&body, extra, true)
            .await
            .map_err(PublisherError::Retryable)?;
        trace!(count = messages.len(), table = %self.table, "Published batch to ClickHouse");
        Ok(SentBatch::Ack)
    }

    async fn status(&self) -> EndpointStatus {
        let (healthy, error) = match self.client.run("SELECT 1", &[], false).await {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };
        EndpointStatus {
            healthy,
            target: self.table.clone(),
            error,
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// --- Consumer (cursor source) ---

/// A resume cursor value; encodes losslessly to/from the checkpoint store's opaque string.
#[derive(Debug, Clone, PartialEq)]
enum ChCursor {
    Int(i64),
    Text(String),
}

impl ChCursor {
    fn encode(&self) -> String {
        match self {
            ChCursor::Int(n) => format!("int:{}", n),
            ChCursor::Text(s) => format!("str:{}", s),
        }
    }

    fn decode(s: &str) -> Option<ChCursor> {
        let (tag, val) = s.split_once(':')?;
        match tag {
            "int" => val.parse::<i64>().ok().map(ChCursor::Int),
            "str" => Some(ChCursor::Text(val.to_string())),
            _ => None,
        }
    }

    /// ClickHouse typed-parameter type and value for `{last:<ty>}` substitution.
    fn param(&self) -> (&'static str, String) {
        match self {
            ChCursor::Int(n) => ("Int64", n.to_string()),
            ChCursor::Text(s) => ("String", s.clone()),
        }
    }
}

/// Extract the cursor value from a JSONEachRow row object.
fn extract_cursor(row: &serde_json::Value, column: &str) -> Option<ChCursor> {
    match row.get(column) {
        Some(serde_json::Value::Number(n)) => n.as_i64().map(ChCursor::Int),
        Some(serde_json::Value::String(s)) => Some(ChCursor::Text(s.clone())),
        _ => None,
    }
}

pub struct ClickHouseCursorReader {
    client: ChClient,
    table: String,
    cursor_column: String,
    select_columns: String,
    polling_interval: Duration,
    checkpoint: Option<Arc<dyn CheckpointStore>>,
    last_value: Arc<Mutex<Option<ChCursor>>>,
}

impl ClickHouseCursorReader {
    pub async fn new(config: &ClickHouseConfig) -> anyhow::Result<Self> {
        if !is_valid_ident(&config.table, true) {
            return Err(anyhow!(
                "Invalid ClickHouse table name: '{}'.",
                config.table
            ));
        }
        let cursor_column = config
            .cursor_column
            .clone()
            .ok_or_else(|| anyhow!("cursor_column is required for the ClickHouse cursor reader"))?;
        if !is_valid_ident(&cursor_column, false) {
            return Err(anyhow!("Invalid cursor_column name: '{}'.", cursor_column));
        }

        let client = ChClient::from_config(config)?;
        client
            .run("SELECT 1", &[], false)
            .await
            .context("ClickHouse cursor reader connection check failed")?;

        // Durable resume needs an external checkpoint store: ClickHouse is unsuited to per-row cursor
        // upserts, so the source-datastore backend is rejected here.
        let checkpoint: Option<Arc<dyn CheckpointStore>> = if let Some(cid) = &config.cursor_id {
            match &config.checkpoint_store {
                None => {
                    warn!(
                        table = %config.table,
                        "ClickHouse cursor reader has cursor_id but no checkpoint_store; resume is disabled. Set an external checkpoint_store (file://, postgres://, mongodb://) to persist progress."
                    );
                    None
                }
                Some(spec) => match checkpoint::parse_checkpoint_store(spec)? {
                    CheckpointBackend::Source { .. } => {
                        return Err(anyhow!(
                            "ClickHouse cursor reader requires an external checkpoint_store (file://, postgres://, or mongodb://); a source-datastore checkpoint is not supported because ClickHouse cannot cheaply upsert cursor rows."
                        ));
                    }
                    external => {
                        Some(checkpoint::build_external_store(external, &config.table, cid).await?)
                    }
                },
            }
        } else {
            warn!(
                table = %config.table,
                "ClickHouse cursor reader has no cursor_id; resume is disabled and every restart re-copies from the beginning."
            );
            None
        };

        let last_value = match &checkpoint {
            Some(cp) => cp.load().await?.and_then(|s| {
                let decoded = ChCursor::decode(&s);
                if decoded.is_none() {
                    warn!(value = %s, "Ignoring unparseable ClickHouse cursor; starting from beginning");
                }
                decoded
            }),
            None => None,
        };
        info!(table = %config.table, column = %cursor_column, has_checkpoint = %last_value.is_some(), "ClickHouse cursor reader connected");

        Ok(Self {
            client,
            table: config.table.clone(),
            cursor_column,
            select_columns: config
                .select_columns
                .clone()
                .unwrap_or_else(|| "*".to_string()),
            polling_interval: Duration::from_millis(config.polling_interval_ms.unwrap_or(100)),
            checkpoint,
            last_value: Arc::new(Mutex::new(last_value)),
        })
    }
}

#[async_trait]
impl MessageConsumer for ClickHouseCursorReader {
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch {
                messages: Vec::new(),
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            });
        }

        let last = self.last_value.lock().unwrap().clone();
        // Peek one extra row so a run of equal cursor values split across the LIMIT boundary is
        // detected (a `> last` bound would otherwise silently skip the remainder of that run).
        let fetch_limit = max_messages.saturating_add(1);

        let (sql, extra): (String, Vec<(&str, String)>) = match &last {
            Some(cur) => {
                let (ty, val) = cur.param();
                let sql = format!(
                    "SELECT {cols} FROM {table} WHERE {col} > {{last:{ty}}} ORDER BY {col} ASC LIMIT {lim} FORMAT JSONEachRow",
                    cols = self.select_columns,
                    table = self.table,
                    col = self.cursor_column,
                    ty = ty,
                    lim = fetch_limit,
                );
                (sql, vec![("param_last", val)])
            }
            None => {
                let sql = format!(
                    "SELECT {cols} FROM {table} ORDER BY {col} ASC LIMIT {lim} FORMAT JSONEachRow",
                    cols = self.select_columns,
                    table = self.table,
                    col = self.cursor_column,
                    lim = fetch_limit,
                );
                (sql, Vec::new())
            }
        };
        // Ask the server to gzip the (potentially large) result set; reqwest gunzips it transparently.
        // `output_format_json_quote_64bit_integers=0` keeps Int64/UInt64 as JSON numbers (ClickHouse
        // quotes them as strings by default), so numeric cursor values and payload ids stay numeric —
        // otherwise a UInt64 cursor would page by lexicographic string order.
        let mut extra_refs: Vec<(&str, &str)> = vec![
            ("enable_http_compression", "1"),
            ("output_format_json_quote_64bit_integers", "0"),
        ];
        extra_refs.extend(extra.iter().map(|(k, v)| (*k, v.as_str())));

        let body = self
            .client
            .run(&sql, &extra_refs, false)
            .await
            .map_err(ConsumerError::Connection)?;

        // Parse JSONEachRow: one JSON object per non-empty line.
        let mut fetched: Vec<(ChCursor, CanonicalMessage)> = Vec::new();
        for line in body.lines().filter(|l| !l.trim().is_empty()) {
            let row: serde_json::Value = serde_json::from_str(line).map_err(|e| {
                ConsumerError::Connection(anyhow!("Invalid JSONEachRow row: {}", e))
            })?;
            let cursor = extract_cursor(&row, &self.cursor_column).ok_or_else(|| {
                ConsumerError::Connection(anyhow!(
                    "cursor_column '{}' missing or of unsupported type in result row",
                    self.cursor_column
                ))
            })?;
            let payload = serde_json::to_vec(&row).unwrap_or_default();
            fetched.push((cursor, CanonicalMessage::new(payload, None)));
        }

        if fetched.is_empty() {
            // Drained: keep polling cadence, then surface an empty batch.
            tokio::time::sleep(self.polling_interval).await;
            return Ok(ReceivedBatch {
                messages: Vec::new(),
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            });
        }

        // Drop the trailing run equal to the peek row's value so a group of equal cursor values is
        // never split across pages; trimmed rows are re-read next poll via `col > last`.
        let had_more = fetched.len() > max_messages;
        let mut emit_len = fetched.len().min(max_messages);
        if had_more {
            let peek_val = fetched[max_messages].0.clone();
            while emit_len > 0 && fetched[emit_len - 1].0 == peek_val {
                emit_len -= 1;
            }
            if emit_len == 0 {
                warn!(
                    column = %self.cursor_column,
                    "cursor_column has a group of equal values larger than the batch size; increase batch_size to avoid skipping rows at this value"
                );
                emit_len = max_messages;
            }
        }
        fetched.truncate(emit_len);

        let mut messages = Vec::with_capacity(fetched.len());
        let mut cursors: Vec<ChCursor> = Vec::with_capacity(fetched.len());
        for (cursor, msg) in fetched {
            cursors.push(cursor.clone());
            messages.push(msg);
            // Advance optimistically; rolled back in commit if a row is not acked.
            *self.last_value.lock().unwrap() = Some(cursor);
        }
        trace!(
            count = messages.len(),
            "Received batch of ClickHouse cursor rows"
        );

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
                // Roll the in-memory read cursor back to the committed boundary so nacked rows are
                // re-read next poll (at-least-once) instead of skipped until a restart.
                if acked < cursors.len() {
                    *last_value.lock().unwrap() = boundary.clone();
                }
                if let (Some(cur), Some(cp)) = (boundary, checkpoint) {
                    if let Err(e) = cp.save(&cur.encode()).await {
                        warn!(error = %e, "Failed to persist ClickHouse cursor. Rows may be reprocessed on restart.");
                    }
                }
                Ok(())
            }) as BoxFuture<'static, anyhow::Result<()>>
        });

        Ok(ReceivedBatch { messages, commit })
    }

    async fn status(&self) -> EndpointStatus {
        let (healthy, error) = match self.client.run("SELECT 1", &[], false).await {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };
        EndpointStatus {
            healthy,
            target: self.table.clone(),
            error,
            details: serde_json::json!({ "mode": "cursor_column", "cursor_column": self.cursor_column }),
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
    use std::collections::BTreeMap;

    fn msg(payload: &serde_json::Value, meta: &[(&str, &str)]) -> CanonicalMessage {
        let mut m = CanonicalMessage::new(serde_json::to_vec(payload).unwrap(), None);
        for (k, v) in meta {
            m.metadata.insert(k.to_string(), v.to_string());
        }
        m
    }

    #[test]
    fn ident_validation() {
        assert!(is_valid_ident("orders", false));
        assert!(is_valid_ident("db.orders", true));
        assert!(!is_valid_ident("db.orders", false));
        assert!(!is_valid_ident("drop table", false));
        assert!(!is_valid_ident("", false));
        assert!(!is_valid_ident(".x", true));
        assert!(!is_valid_ident("a..b", true));
    }

    #[test]
    fn default_row_requires_object_payload() {
        let m = msg(&serde_json::json!({"id": 1, "sku": "a"}), &[]);
        let row = build_row(&m, &None).unwrap();
        assert_eq!(row, serde_json::json!({"id": 1, "sku": "a"}));

        // Non-object payloads are rejected in default mode.
        let scalar =
            CanonicalMessage::new(serde_json::to_vec(&serde_json::json!(42)).unwrap(), None);
        assert!(build_row(&scalar, &None).is_err());
    }

    #[test]
    fn mapped_row_resolves_tokens() {
        let m = msg(
            &serde_json::json!({"sku": "widget", "qty": 3}),
            &[("customer_id", "c-99")],
        );
        let mut cols = BTreeMap::new();
        cols.insert(
            "customer".to_string(),
            "${metadata:customer_id}".to_string(),
        );
        cols.insert("sku".to_string(), "${payload:sku}".to_string());
        cols.insert("qty".to_string(), "${payload:qty}".to_string());
        cols.insert("source".to_string(), "clickhouse".to_string()); // literal
        cols.insert("missing".to_string(), "${payload:nope}".to_string());

        let row = build_row(&m, &Some(cols)).unwrap();
        assert_eq!(
            row,
            serde_json::json!({
                "customer": "c-99",
                "sku": "widget",
                "qty": 3,            // numeric type preserved
                "source": "clickhouse",
                "missing": null,
            })
        );
    }

    #[test]
    fn cursor_encode_decode_roundtrip() {
        assert_eq!(
            ChCursor::decode(&ChCursor::Int(42).encode()),
            Some(ChCursor::Int(42))
        );
        assert_eq!(
            ChCursor::decode(&ChCursor::Text("2026-01-01".into()).encode()),
            Some(ChCursor::Text("2026-01-01".into()))
        );
        assert_eq!(ChCursor::decode("garbage"), None);
        assert_eq!(ChCursor::Int(7).param(), ("Int64", "7".to_string()));
    }

    #[test]
    fn extract_cursor_from_row() {
        let row = serde_json::json!({"id": 5, "name": "x"});
        assert_eq!(extract_cursor(&row, "id"), Some(ChCursor::Int(5)));
        assert_eq!(
            extract_cursor(&row, "name"),
            Some(ChCursor::Text("x".into()))
        );
        assert_eq!(extract_cursor(&row, "absent"), None);
    }
}
