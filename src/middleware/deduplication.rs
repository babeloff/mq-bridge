//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge
use crate::models::DeduplicationMiddleware;
use crate::support::interpolation::CompiledTemplate;
use crate::traits::{
    BoxFuture, ConsumerError, MessageConsumer, MessageDisposition, Received, ReceivedBatch,
};
use crate::CanonicalMessage;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use sled::Db;
use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, instrument, trace, warn};

/// Short TTL for a reservation held while a message is in flight. Kept small so a crash between
/// reserve and commit frees the key quickly for at-least-once redelivery.
pub(crate) const PENDING_TTL_SECS: u64 = 5;

/// A pluggable deduplication backend: a keyed store with an atomic reserve and a TTL.
///
/// `reserve` is the ordering point — it atomically claims a key, returning whether a *live*
/// entry already existed. `mark_processed` promotes a claimed key to the full TTL once the
/// message is committed. Local (`sled`) and shared (`mongodb`) backends implement this.
#[async_trait]
pub(crate) trait DedupStore: Send + Sync {
    /// Atomically reserve `key`. `Ok(true)` = a live entry already exists (duplicate);
    /// `Ok(false)` = freshly reserved (caller should process the message).
    async fn reserve(&self, key: &[u8], now: u64) -> Result<bool, ConsumerError>;

    /// Promote a reserved key to the processed state with the full TTL. Best-effort: a failure
    /// here only risks a later redelivery being reprocessed (at-least-once), so it logs rather
    /// than propagating.
    async fn mark_processed(&self, key: &[u8], now: u64);

    /// Best-effort periodic GC of expired keys. Default no-op for backends with native TTL.
    fn maybe_cleanup(&self, _now: u64) {}

    /// Flush buffered writes on disconnect. Default no-op.
    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// A parsed deduplication `store:` destination.
pub(crate) enum DedupBackend {
    /// Local single-instance Sled directory.
    Sled { path: String },
    /// Shared MongoDB collection (`mongodb://host/db[/collection]`).
    #[cfg(feature = "mongodb")]
    Mongo {
        url: String,
        database: String,
        collection: Option<String>,
    },
    /// Shared SQL table (`postgres|mysql|mariadb|sqlite://…[/table]`).
    #[cfg(feature = "sqlx")]
    Sqlx { url: String, table: Option<String> },
}

/// Parse a `store:` value. `sled:`/bare paths select the local store; other schemes are handed to
/// the checkpoint URL parser and only MongoDB is accepted (SQL/Redis land in later slices).
pub(crate) fn parse_dedup_store(spec: &str) -> anyhow::Result<DedupBackend> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err(anyhow!("deduplication store is empty"));
    }
    let scheme = spec
        .split_once(':')
        .map(|(s, _)| s.to_ascii_lowercase())
        .unwrap_or_default();
    // `C:\path` / `C:/path` is a Windows drive letter, not a URL scheme.
    if scheme.len() == 1 && scheme.chars().all(|c| c.is_ascii_alphabetic()) {
        return Ok(DedupBackend::Sled {
            path: spec.to_string(),
        });
    }
    match scheme.as_str() {
        "sled" => {
            let path = spec
                .strip_prefix("sled://")
                .or_else(|| spec.strip_prefix("sled:"))
                .unwrap_or(spec);
            Ok(DedupBackend::Sled {
                path: path.to_string(),
            })
        }
        // Schemeless (a bare filesystem path) -> local sled.
        "" => Ok(DedupBackend::Sled {
            path: spec.to_string(),
        }),
        _ => parse_networked_dedup_store(spec),
    }
}

/// Parse a non-sled `store:` scheme by delegating to the checkpoint URL parser, then accepting
/// only the backends deduplication supports in this build.
#[cfg(any(feature = "mongodb", feature = "sqlx"))]
fn parse_networked_dedup_store(spec: &str) -> anyhow::Result<DedupBackend> {
    match crate::checkpoint::parse_checkpoint_store(spec)? {
        #[cfg(feature = "mongodb")]
        crate::checkpoint::CheckpointBackend::Mongo {
            url,
            database,
            collection,
        } => Ok(DedupBackend::Mongo {
            url,
            database,
            collection,
        }),
        #[cfg(feature = "sqlx")]
        crate::checkpoint::CheckpointBackend::Sqlx { url, table } => {
            Ok(DedupBackend::Sqlx { url, table })
        }
        other => Err(anyhow!(
            "deduplication store '{spec}' is not supported (only sled, mongodb, and SQL); got {other:?}"
        )),
    }
}

#[cfg(not(any(feature = "mongodb", feature = "sqlx")))]
fn parse_networked_dedup_store(spec: &str) -> anyhow::Result<DedupBackend> {
    Err(anyhow!(
        "deduplication store '{spec}' requires a networked backend feature (mongodb or sqlx); this build supports only local sled paths"
    ))
}

async fn build_store(
    backend: DedupBackend,
    ttl_seconds: u64,
    route_name: &str,
) -> anyhow::Result<Arc<dyn DedupStore>> {
    #[cfg(not(any(feature = "mongodb", feature = "sqlx")))]
    let _ = route_name;
    match backend {
        DedupBackend::Sled { path } => Ok(Arc::new(SledDedupStore::new(&path, ttl_seconds)?)),
        #[cfg(feature = "mongodb")]
        DedupBackend::Mongo {
            url,
            database,
            collection,
        } => {
            crate::endpoints::mongodb::build_mongo_dedup_store(
                &url,
                &database,
                collection,
                ttl_seconds,
                route_name,
            )
            .await
        }
        #[cfg(feature = "sqlx")]
        DedupBackend::Sqlx { url, table } => {
            crate::endpoints::sqlx::build_sql_dedup_store(&url, table, ttl_seconds, route_name)
                .await
        }
    }
}

/// Local, single-instance deduplication store backed by a Sled database. Reserve/processed states
/// are a 9-byte value `[state, be_u64_timestamp]`; expiry is enforced on read (a stale entry is
/// reclaimable) and swept lazily by `maybe_cleanup`.
struct SledDedupStore {
    db: Arc<Db>,
    ttl_seconds: u64,
    last_cleanup: Arc<AtomicU64>,
}

impl SledDedupStore {
    fn new(path: &str, ttl_seconds: u64) -> anyhow::Result<Self> {
        let db = sled::open(path)?;
        Ok(Self {
            db: Arc::new(db),
            ttl_seconds,
            last_cleanup: Arc::new(AtomicU64::new(0)),
        })
    }
}

#[async_trait]
impl DedupStore for SledDedupStore {
    async fn reserve(&self, key: &[u8], now: u64) -> Result<bool, ConsumerError> {
        let now_bytes = now.to_be_bytes();

        const STATE_PENDING: u8 = 0;

        let mut pending_val = [0u8; 9];
        pending_val[0] = STATE_PENDING;
        pending_val[1..9].copy_from_slice(&now_bytes);

        let mut yield_counter = 0;
        let mut total_attempts = 0;
        const MAX_TOTAL_ATTEMPTS: usize = 1000;

        loop {
            if total_attempts >= MAX_TOTAL_ATTEMPTS {
                return Err(ConsumerError::Connection(anyhow::anyhow!(
                    "Deduplication CAS exceeded max attempts for key {}",
                    hex_key(key)
                )));
            }
            if yield_counter > 10 {
                tokio::task::yield_now().await;
                yield_counter = 0;
            }
            yield_counter += 1;
            total_attempts += 1;

            match self
                .db
                .compare_and_swap(key, None::<&[u8]>, Some(&pending_val[..]))
            {
                Ok(Ok(())) => return Ok(false),
                Ok(Err(cas_error)) => {
                    if let Some(current_bytes) = cas_error.current.as_deref() {
                        let (ts, ttl) = if current_bytes.len() == 9 {
                            let state = current_bytes[0];
                            let ts_bytes: [u8; 8] = current_bytes[1..9].try_into().unwrap();
                            (
                                u64::from_be_bytes(ts_bytes),
                                if state == STATE_PENDING {
                                    PENDING_TTL_SECS
                                } else {
                                    self.ttl_seconds
                                },
                            )
                        } else if current_bytes.len() == 8 {
                            let ts_bytes: [u8; 8] = current_bytes.try_into().unwrap();
                            (u64::from_be_bytes(ts_bytes), self.ttl_seconds)
                        } else {
                            (0, 0)
                        };

                        if now.saturating_sub(ts) < ttl {
                            return Ok(true);
                        }
                        match self.db.compare_and_swap(
                            key,
                            Some(current_bytes),
                            Some(&pending_val[..]),
                        ) {
                            Ok(Ok(())) => return Ok(false),
                            Ok(Err(_)) => continue,
                            Err(e) => {
                                return Err(ConsumerError::Connection(anyhow::anyhow!(
                                    "Deduplication DB error: {}",
                                    e
                                )))
                            }
                        }
                    } else {
                        continue;
                    }
                }
                Err(e) => {
                    return Err(ConsumerError::Connection(anyhow::anyhow!(
                        "Deduplication DB error: {}",
                        e
                    )))
                }
            }
        }
    }

    async fn mark_processed(&self, key: &[u8], now: u64) {
        let mut processed_val = [0u8; 9];
        processed_val[0] = 1; // STATE_PROCESSED
        processed_val[1..9].copy_from_slice(&now.to_be_bytes());
        if let Err(e) = self.db.insert(key, &processed_val[..]) {
            error!(
                "Failed to update key {} as processed in deduplication DB: {}",
                hex_key(key),
                e
            );
        } else {
            trace!("Updated message as processed in deduplication DB");
        }
    }

    fn maybe_cleanup(&self, now: u64) {
        let last = self.last_cleanup.load(Ordering::Acquire);
        if now.saturating_sub(last) > 30
            && self
                .last_cleanup
                .compare_exchange(last, now, Ordering::SeqCst, Ordering::Acquire)
                .is_ok()
        {
            let db = self.db.clone();
            let ttl = self.ttl_seconds;
            tokio::spawn(async move {
                let cutoff = now.saturating_sub(ttl);
                for (key, val) in db.iter().flatten() {
                    let len = val.len();
                    let ts_offset = if len == 9 {
                        1
                    } else if len == 8 {
                        0
                    } else {
                        continue;
                    };
                    if let Ok(ts_bytes) = val[ts_offset..ts_offset + 8].try_into() {
                        if u64::from_be_bytes(ts_bytes) < cutoff {
                            let _ = db.compare_and_swap(&key, Some(val), None::<&[u8]>);
                        }
                    }
                }
            });
        }
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.db.flush_async().await?;
        Ok(())
    }
}

/// Hex-encode a dedup key for logging and for string-keyed backends.
pub(crate) fn hex_key(key: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(key.len() * 2);
    for b in key {
        let _ = write!(s, "{:02x}", b);
    }
    s
}

pub struct DeduplicationConsumer {
    inner: Box<dyn MessageConsumer>,
    store: Arc<dyn DedupStore>,
    /// Compiled `key` template. `None` keys on `message_id`, which most sources
    /// regenerate per read — so a re-read of the same source dedupes nothing.
    key_template: Option<CompiledTemplate>,
}

/// The dedup key for a message: the rendered `key` template, or the raw `message_id`.
///
/// An unresolved selector renders empty, and an empty key would be shared by every message
/// missing that field — silently dropping all but the first. Such a message falls back to
/// `message_id` instead, so it is passed through rather than swallowed.
fn dedup_key(template: Option<&CompiledTemplate>, msg: &CanonicalMessage) -> Vec<u8> {
    match template {
        Some(t) => match t.render(Some(msg)) {
            key if key.is_empty() => {
                warn!(
                    message_id = %msg.message_id,
                    "Deduplication `key` template resolved to nothing; keying on message_id for this message"
                );
                msg.message_id.to_be_bytes().to_vec()
            }
            key => key,
        },
        None => msg.message_id.to_be_bytes().to_vec(),
    }
}

impl DeduplicationConsumer {
    pub async fn new(
        inner: Box<dyn MessageConsumer>,
        config: &DeduplicationMiddleware,
        route_name: &str,
    ) -> anyhow::Result<Self> {
        info!(
            "Deduplication Middleware enabled for route '{}' with TTL {}s",
            route_name, config.ttl_seconds
        );
        let backend = match (&config.store, &config.sled_path) {
            (Some(store), _) => parse_dedup_store(store)?,
            // Legacy `sled_path` is always a local path (never scheme-parsed).
            (None, Some(path)) => DedupBackend::Sled { path: path.clone() },
            (None, None) => {
                return Err(anyhow!(
                    "deduplication requires either `store` or `sled_path`"
                ))
            }
        };
        let store = build_store(backend, config.ttl_seconds, route_name).await?;
        let key_template = config
            .key
            .as_deref()
            .map(|k| CompiledTemplate::compile(k, None))
            .transpose()
            .context("invalid deduplication `key` template")?;
        Ok(Self {
            inner,
            store,
            key_template,
        })
    }
}

#[async_trait]
impl MessageConsumer for DeduplicationConsumer {
    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.inner.set_exit_on_empty(exit_on_empty);
    }

    fn commit_requires_order(&self) -> bool {
        self.inner.commit_requires_order()
    }
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        let inner_hook = self.inner.on_disconnect_hook();
        let store = self.store.clone();

        Some(Box::pin(async move {
            let mut first_error = None;
            if let Some(hook) = inner_hook {
                if let Err(err) = hook.await {
                    first_error = Some(err);
                }
            }
            if let Err(err) = store.flush().await {
                first_error.get_or_insert(err);
            }
            match first_error {
                Some(err) => Err(err),
                None => Ok(()),
            }
        }))
    }

    #[instrument(skip_all)]
    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        loop {
            let received = self.inner.receive().await?;

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("System time is before UNIX EPOCH")?
                .as_secs();

            self.store.maybe_cleanup(now);

            let key = dedup_key(self.key_template.as_ref(), &received.message);
            if self.store.reserve(&key, now).await? {
                info!(message_id = %format!("{:032x}", received.message.message_id), "Duplicate message detected and skipped");
                if let Err(e) = (received.commit)(MessageDisposition::Ack).await {
                    warn!("Failed to commit skipped duplicate message: {}", e);
                }
                continue;
            }

            let store = self.store.clone();
            let original_commit = received.commit;

            // Wrap commit to promote the reservation to "processed" state.
            let commit = Box::new(move |disposition: MessageDisposition| {
                Box::pin(async move {
                    let is_ack = matches!(
                        disposition,
                        MessageDisposition::Ack | MessageDisposition::Reply(_)
                    );
                    original_commit(disposition).await?;
                    if is_ack {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        store.mark_processed(&key, now).await;
                    }
                    Ok(())
                }) as crate::traits::BoxFuture<'static, anyhow::Result<()>>
            });

            return Ok(Received {
                message: received.message,
                commit,
            });
        }
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        loop {
            let ReceivedBatch {
                messages,
                commit: inner_commit,
            } = self.inner.receive_batch(max_messages).await?;

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| ConsumerError::Connection(anyhow::anyhow!(e)))?
                .as_secs();

            self.store.maybe_cleanup(now);

            // An empty inner batch is how a drained source signals `exit_on_empty`
            // (see `traits::drain_gated`). Looping on it here would spin forever on
            // an already-drained source, so pass it through untouched.
            if messages.is_empty() {
                return Ok(ReceivedBatch {
                    messages,
                    commit: inner_commit,
                });
            }

            let mut filtered_messages = Vec::with_capacity(messages.len());
            let mut kept_indices = Vec::with_capacity(messages.len());

            let mut kept_keys: Vec<Vec<u8>> = Vec::with_capacity(messages.len());

            for (idx, msg) in messages.iter().enumerate() {
                let key = dedup_key(self.key_template.as_ref(), msg);
                if self.store.reserve(&key, now).await? {
                    info!(message_id = %format!("{:032x}", msg.message_id), "Duplicate message detected and skipped");
                } else {
                    filtered_messages.push(msg.clone());
                    kept_indices.push(idx);
                    kept_keys.push(key);
                }
            }

            if filtered_messages.is_empty() {
                let _ = inner_commit(vec![MessageDisposition::Ack; messages.len()]).await;
                continue;
            }

            let store = self.store.clone();
            let total_len = messages.len();

            let commit: crate::traits::BatchCommitFunc = Box::new(move |dispositions| {
                let store = store.clone();
                let inner_commit = inner_commit;
                let kept_indices = kept_indices;
                let kept_keys = kept_keys;

                Box::pin(async move {
                    let mut full_dispositions = vec![MessageDisposition::Ack; total_len];
                    let mut acks = Vec::new();
                    for (i, disp) in dispositions.into_iter().enumerate() {
                        if matches!(disp, MessageDisposition::Ack | MessageDisposition::Reply(_)) {
                            acks.push(kept_keys[i].clone());
                        }
                        full_dispositions[kept_indices[i]] = disp;
                    }

                    inner_commit(full_dispositions).await?;

                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    for key in acks {
                        store.mark_processed(&key, now).await;
                    }
                    Ok(())
                }) as crate::traits::BoxFuture<'static, anyhow::Result<()>>
            });

            return Ok(ReceivedBatch {
                messages: filtered_messages,
                commit,
            });
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::memory::MemoryConsumer;
    use crate::models::DeduplicationMiddleware;
    use crate::CanonicalMessage;
    use std::time::Duration;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_deduplication_logic() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("dedup_test").to_str().unwrap().to_string();

        let config = DeduplicationMiddleware {
            store: None,
            sled_path: Some(db_path),
            ttl_seconds: 60,
            key: None,
        };

        let mem_consumer = MemoryConsumer::new_local("dedup_topic", 10);
        let channel = mem_consumer.channel();

        let msg1 = CanonicalMessage::new(b"data1".to_vec(), Some(100));
        channel.send_message(msg1).await.unwrap();

        let msg2 = CanonicalMessage::new(b"data1_dup".to_vec(), Some(100));
        channel.send_message(msg2).await.unwrap();

        let msg3 = CanonicalMessage::new(b"data2".to_vec(), Some(101));
        channel.send_message(msg3).await.unwrap();

        let mut dedup_consumer =
            DeduplicationConsumer::new(Box::new(mem_consumer), &config, "test_route")
                .await
                .unwrap();

        // First receive: Should be msg1 (ID 100)
        let rec1 = dedup_consumer.receive().await.unwrap();
        assert_eq!(rec1.message.message_id, 100);
        let _ = (rec1.commit)(crate::traits::MessageDisposition::Ack).await;

        // Second receive: Should be msg3 (ID 101). msg2 (ID 100) is skipped internally.
        let rec2 = dedup_consumer.receive().await.unwrap();
        assert_eq!(rec2.message.message_id, 101);
        let _ = (rec2.commit)(crate::traits::MessageDisposition::Ack).await;
    }

    /// Regression: an empty inner batch is how a drained source signals `exit_on_empty`.
    /// The wrapper used to loop on it, so a route with `deduplication` on the input moved
    /// everything and then hung `healthy: true` forever instead of completing.
    #[tokio::test]
    async fn empty_inner_batch_is_passed_through_for_drain() {
        let dir = tempdir().unwrap();
        let config = DeduplicationMiddleware {
            store: None,
            sled_path: Some(dir.path().join("dedup_drain").to_str().unwrap().to_string()),
            ttl_seconds: 60,
            key: None,
        };

        let mut mem_consumer = MemoryConsumer::new_local("dedup_drain_topic", 10);
        mem_consumer.set_exit_on_empty(true);

        let mut dedup_consumer =
            DeduplicationConsumer::new(Box::new(mem_consumer), &config, "test_route")
                .await
                .unwrap();

        let batch = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            dedup_consumer.receive_batch(16),
        )
        .await
        .expect("receive_batch must return on a drained source, not loop forever")
        .unwrap();

        assert!(batch.messages.is_empty());
    }

    /// With a `key` template, dedup follows the payload, not `message_id` — which most
    /// sources regenerate per read, so a re-read of the same data would dedupe nothing.
    #[tokio::test]
    async fn key_template_dedupes_on_payload_not_message_id() {
        let dir = tempdir().unwrap();
        let config = DeduplicationMiddleware {
            store: None,
            sled_path: Some(dir.path().join("dedup_key").to_str().unwrap().to_string()),
            ttl_seconds: 60,
            key: Some("${payload:order_id}".to_string()),
        };

        let mem_consumer = MemoryConsumer::new_local("dedup_key_topic", 10);
        let channel = mem_consumer.channel();

        // Same order_id, different message_ids: the second must be suppressed.
        for id in [1u128, 2, 3] {
            let body = if id == 3 {
                br#"{"order_id":"B"}"#
            } else {
                br#"{"order_id":"A"}"#
            };
            channel
                .send_message(CanonicalMessage::new(body.to_vec(), Some(id)))
                .await
                .unwrap();
        }

        let mut dedup_consumer =
            DeduplicationConsumer::new(Box::new(mem_consumer), &config, "test_route")
                .await
                .unwrap();

        // Timed out rather than awaited bare: a key that fails to resolve makes every
        // message share one key, so the second `receive` would block forever.
        macro_rules! recv {
            () => {
                tokio::time::timeout(Duration::from_secs(10), dedup_consumer.receive())
                    .await
                    .expect("receive timed out — the key template resolved to a constant")
                    .unwrap()
            };
        }

        let first = recv!();
        assert_eq!(first.message.message_id, 1);
        let _ = (first.commit)(crate::traits::MessageDisposition::Ack).await;

        // message_id 2 carries order_id "A" again and is skipped internally.
        let second = recv!();
        assert_eq!(
            second.message.message_id, 3,
            "the second copy of order_id A must be suppressed by the key template"
        );
    }

    /// A message missing the keyed field renders an empty key. Keying on that would make
    /// every such message collide, so all but the first would vanish; they fall back to
    /// `message_id` and are passed through instead.
    #[tokio::test]
    async fn messages_missing_the_keyed_field_are_not_collapsed() {
        let dir = tempdir().unwrap();
        let config = DeduplicationMiddleware {
            store: None,
            sled_path: Some(dir.path().join("dedup_miss").to_str().unwrap().to_string()),
            ttl_seconds: 60,
            key: Some("${payload:order_id}".to_string()),
        };

        let mem_consumer = MemoryConsumer::new_local("dedup_miss_topic", 10);
        let channel = mem_consumer.channel();
        for id in [1u128, 2, 3] {
            channel
                .send_message(CanonicalMessage::new(br#"{"other":1}"#.to_vec(), Some(id)))
                .await
                .unwrap();
        }

        let mut dedup_consumer =
            DeduplicationConsumer::new(Box::new(mem_consumer), &config, "test_route")
                .await
                .unwrap();
        dedup_consumer.set_exit_on_empty(true);

        let batch = tokio::time::timeout(Duration::from_secs(10), dedup_consumer.receive_batch(16))
            .await
            .expect("receive_batch timed out")
            .unwrap();
        assert_eq!(
            batch.messages.len(),
            3,
            "an unresolvable key must not collapse distinct messages into one"
        );
    }

    #[test]
    fn parse_sled_and_bare_paths() {
        assert!(matches!(
            parse_dedup_store("sled:///var/lib/dedup").unwrap(),
            DedupBackend::Sled { path } if path == "/var/lib/dedup"
        ));
        assert!(matches!(
            parse_dedup_store("/var/lib/dedup").unwrap(),
            DedupBackend::Sled { path } if path == "/var/lib/dedup"
        ));
    }
}
