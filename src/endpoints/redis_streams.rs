//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Redis Streams endpoint.
//!
//! Publishers `XADD` each message to a stream key. Consumers read through a
//! consumer group (`XREADGROUP` + per-message `XACK`) by default, or ephemerally
//! via `XREAD` from new messages when `subscriber_mode` is set.
//!
//! The payload is stored under the `payload` field and the mq-bridge message id
//! under `mqb_message_id`; every remaining metadata entry becomes its own field,
//! so messages stay readable by any other Redis client.

use crate::canonical_message::tracing_support::LazyMessageIds;
use crate::models::RedisStreamsConfig;
use crate::traits::{
    BoxFuture, ConsumerError, EndpointStatus, MessageConsumer, MessageDisposition, MessagePublisher,
    PublisherError, ReceivedBatch, SentBatch,
};
use crate::CanonicalMessage;
use crate::APP_NAME;
use anyhow::anyhow;
use async_channel::{bounded, Receiver};
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamAutoClaimOptions, StreamAutoClaimReply, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, IntoConnectionInfo};
use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tracing::trace;

/// The stream field used to carry the raw message payload.
const PAYLOAD_FIELD: &str = "payload";
/// The stream field used to carry the mq-bridge message id (hex).
const MESSAGE_ID_FIELD: &str = "mqb_message_id";
const DEFAULT_BLOCK_MS: u64 = 5000;
const DEFAULT_BUFFER: usize = 128;
/// Default idle time before a pending entry is reclaimed and redelivered.
const DEFAULT_REDELIVERY_MS: u64 = 60_000;

fn open_client(config: &RedisStreamsConfig) -> anyhow::Result<redis::Client> {
    // redis 1.3's ConnectionInfo has no public auth setter, so credentials are
    // injected into the URL's userinfo when provided as separate config fields.
    let url = url_with_credentials(config);
    let info = url
        .as_str()
        .into_connection_info()
        .map_err(|e| anyhow!("Invalid Redis URL: {}", e))?;
    redis::Client::open(info).map_err(|e| anyhow!("Failed to open Redis client: {}", e))
}

/// Injects `config.username`/`config.password` into the URL's userinfo. If the
/// URL already carries userinfo, or no credentials are configured, it is
/// returned unchanged. Credentials with URL-reserved characters (`@`, `:`, `/`)
/// should be percent-encoded in the URL directly rather than set as fields.
fn url_with_credentials(config: &RedisStreamsConfig) -> String {
    if config.username.is_none() && config.password.is_none() {
        return config.url.clone();
    }
    let Some(scheme_end) = config.url.find("://") else {
        return config.url.clone();
    };
    let (scheme, rest) = config.url.split_at(scheme_end + 3);
    let authority_end = rest.find('/').unwrap_or(rest.len());
    if rest[..authority_end].contains('@') {
        // Userinfo already present in the URL; leave it as the source of truth.
        return config.url.clone();
    }
    let user = config.username.as_deref().unwrap_or("");
    let userinfo = match &config.password {
        Some(password) => format!("{}:{}@", user, password),
        None => format!("{}@", user),
    };
    format!("{}{}{}", scheme, userinfo, rest)
}

// --- Publisher ---

pub struct RedisStreamsPublisher {
    conn: MultiplexedConnection,
    stream: String,
    maxlen: Option<usize>,
    approx_trim: bool,
}

impl RedisStreamsPublisher {
    pub async fn new(config: &RedisStreamsConfig) -> anyhow::Result<Self> {
        let stream = config
            .stream
            .clone()
            .ok_or_else(|| anyhow!("Stream key is required for Redis Streams publisher"))?;
        let client = open_client(config)?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| anyhow!("Failed to connect to Redis: {}", e))?;
        Ok(Self {
            conn,
            stream,
            maxlen: config.maxlen,
            approx_trim: config.approx_trim.unwrap_or(true),
        })
    }
}

#[async_trait]
impl MessagePublisher for RedisStreamsPublisher {
    async fn send_batch(
        &self,
        mut messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        trace!(stream = %self.stream, count = messages.len(), message_ids = ?LazyMessageIds(&messages), "Publishing batch of Redis Streams messages");
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }

        // XADD each message in one pipeline round trip.
        let mut pipe = redis::pipe();
        for message in &mut messages {
            // Source/provenance keys are per-hop context and must not be forwarded.
            message.strip_source_metadata();
            pipe.cmd("XADD").arg(&self.stream);
            if let Some(maxlen) = self.maxlen {
                pipe.arg("MAXLEN");
                if self.approx_trim {
                    pipe.arg("~");
                }
                pipe.arg(maxlen);
            }
            pipe.arg("*")
                .arg(PAYLOAD_FIELD)
                .arg(message.payload.as_ref())
                .arg(MESSAGE_ID_FIELD)
                .arg(format!("{:032x}", message.message_id));
            for (key, value) in &message.metadata {
                // Never let a metadata key shadow the reserved fields; a duplicate
                // XADD field would overwrite the payload/id on read (last wins).
                if key == PAYLOAD_FIELD || key == MESSAGE_ID_FIELD {
                    continue;
                }
                pipe.arg(key).arg(value);
            }
        }

        let mut conn = self.conn.clone();
        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|e| PublisherError::Retryable(anyhow!("Redis XADD failed: {}", e)))?;
        Ok(SentBatch::Ack)
    }

    async fn status(&self) -> EndpointStatus {
        let mut conn = self.conn.clone();
        let healthy = redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .is_ok();
        EndpointStatus {
            healthy,
            target: self.stream.clone(),
            error: if healthy {
                None
            } else {
                Some("Redis PING failed".to_string())
            },
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// --- Consumer ---

struct StreamEntry {
    /// The native Redis stream entry id, used for `XACK` in group mode.
    id: String,
    msg: CanonicalMessage,
}

pub struct RedisStreamsConsumer {
    rx: Receiver<Result<StreamEntry, ConsumerError>>,
    ack_conn: MultiplexedConnection,
    stream: String,
    /// `Some` in consumer-group mode (entries are acked); `None` in subscriber mode.
    group: Option<String>,
    buffer: VecDeque<StreamEntry>,
}

impl RedisStreamsConsumer {
    pub async fn new(config: &RedisStreamsConfig) -> anyhow::Result<Self> {
        let stream = config
            .stream
            .clone()
            .ok_or_else(|| anyhow!("Stream key is required for Redis Streams consumer"))?;
        let client = open_client(config)?;

        // A dedicated connection for the (blocking) reads, plus a separate one for
        // acks so an in-flight XREAD BLOCK never stalls XACK.
        let mut read_conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| anyhow!("Failed to connect to Redis: {}", e))?;
        let ack_conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| anyhow!("Failed to connect to Redis: {}", e))?;

        let block_ms = config.block_ms.unwrap_or(DEFAULT_BLOCK_MS) as usize;
        let count = config.internal_buffer_size.unwrap_or(DEFAULT_BUFFER).max(1);
        // 0 disables reclaiming; otherwise redeliver entries idle past this long.
        let redelivery_ms = config
            .redelivery_timeout_ms
            .unwrap_or(DEFAULT_REDELIVERY_MS);

        let group = if config.subscriber_mode {
            None
        } else {
            let group = config
                .group
                .clone()
                .unwrap_or_else(|| format!("{}-{}", APP_NAME, stream));
            // "$" reads only new messages; "0" replays the whole stream. Only takes
            // effect when the group is first created (BUSYGROUP is ignored below).
            let start_id = if config.read_from_start { "0" } else { "$" };
            let created: redis::RedisResult<()> = read_conn
                .xgroup_create_mkstream(&stream, &group, start_id)
                .await;
            if let Err(e) = created {
                // BUSYGROUP: the group already exists, which is expected on restart.
                if e.code() != Some("BUSYGROUP") {
                    return Err(anyhow!("Failed to create Redis consumer group: {}", e));
                }
            }
            Some(group)
        };

        // A unique per-instance name by default. Entries stranded under an old
        // name (e.g. after a restart) are recovered by the XAUTOCLAIM pass below,
        // which reclaims idle pending entries across the whole group.
        let consumer_name = config
            .consumer_name
            .clone()
            .unwrap_or_else(|| {
                format!(
                    "{}-{:032x}",
                    APP_NAME,
                    fast_uuid_v7::gen_id_with_sub_ms_4()
                )
            });

        let (tx, rx) = bounded::<Result<StreamEntry, ConsumerError>>(count);
        let task_stream = stream.clone();
        let task_group = group.clone();
        tokio::spawn(async move {
            // In subscriber mode we track the last delivered id ("$" = new only).
            let mut last_id = String::from("$");
            let reclaim_enabled = task_group.is_some() && redelivery_ms > 0;
            // Check for reclaimable entries on the read cadence; eligibility is
            // still gated by redelivery_ms of idleness on the server side.
            let reclaim_interval =
                Duration::from_millis(redelivery_ms.min(block_ms as u64).max(1000));
            let mut last_reclaim: Option<Instant> = None;
            loop {
                // Redeliver entries left pending past redelivery_ms (Nacked, or
                // orphaned by a crashed/renamed consumer) via XAUTOCLAIM.
                if reclaim_enabled
                    && last_reclaim.map_or(true, |t| t.elapsed() >= reclaim_interval)
                {
                    last_reclaim = Some(Instant::now());
                    if let Some(group) = &task_group {
                        let claim_opts = StreamAutoClaimOptions::default().count(count);
                        let claimed: redis::RedisResult<StreamAutoClaimReply> = read_conn
                            .xautoclaim_options(
                                &task_stream,
                                group,
                                &consumer_name,
                                redelivery_ms as usize,
                                "0-0",
                                claim_opts,
                            )
                            .await;
                        match claimed {
                            Ok(reply) => {
                                for entry in reply.claimed {
                                    let stream_entry = parse_entry(entry.id, entry.map);
                                    if tx.send(Ok(stream_entry)).await.is_err() {
                                        return; // consumer dropped
                                    }
                                }
                            }
                            // Transient; a hard error surfaces on the read below.
                            Err(e) => trace!(stream = %task_stream, error = %e, "Redis XAUTOCLAIM failed"),
                        }
                    }
                }

                let mut opts = StreamReadOptions::default().count(count).block(block_ms);
                let read_ids: Vec<&str> = match &task_group {
                    Some(group) => {
                        opts = opts.group(group, &consumer_name);
                        vec![">"]
                    }
                    None => vec![last_id.as_str()],
                };

                let reply: redis::RedisResult<Option<StreamReadReply>> = read_conn
                    .xread_options(&[&task_stream], read_ids.as_slice(), &opts)
                    .await;

                match reply {
                    Ok(Some(reply)) => {
                        for key in reply.keys {
                            for entry in key.ids {
                                if task_group.is_none() {
                                    last_id = entry.id.clone();
                                }
                                let stream_entry = parse_entry(entry.id, entry.map);
                                if tx.send(Ok(stream_entry)).await.is_err() {
                                    return; // consumer dropped
                                }
                            }
                        }
                    }
                    // BLOCK timeout with no data — just poll again.
                    Ok(None) => {}
                    Err(e) => {
                        if tx
                            .send(Err(ConsumerError::Connection(anyhow!(
                                "Redis XREAD failed: {}",
                                e
                            ))))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        // Avoid a hot error loop while disconnected.
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                }
            }
        });

        Ok(Self {
            rx,
            ack_conn,
            stream,
            group,
            buffer: VecDeque::new(),
        })
    }
}

/// Reconstructs a [`CanonicalMessage`] from a Redis stream entry's field map.
fn parse_entry(id: String, map: HashMap<String, redis::Value>) -> StreamEntry {
    let mut payload = Vec::new();
    let mut message_id = None;
    let mut metadata = HashMap::new();

    for (key, value) in map {
        if key == PAYLOAD_FIELD {
            payload = redis::from_redis_value::<Vec<u8>>(value).unwrap_or_default();
        } else if key == MESSAGE_ID_FIELD {
            if let Ok(s) = redis::from_redis_value::<String>(value) {
                if let Ok(n) = u128::from_str_radix(&s, 16) {
                    message_id = Some(n);
                }
            }
        } else {
            // A stored field must never spoof a reserved `mqb.src.*` cursor key;
            // the authoritative cursor is injected below.
            if crate::canonical_message::is_source_metadata_key(&key) {
                continue;
            }
            if let Ok(s) = redis::from_redis_value::<String>(value) {
                metadata.insert(key, s);
            }
        }
    }

    let mut msg = CanonicalMessage::new(payload, message_id);
    msg.metadata = metadata;
    // Opt-in via the MQB_SOURCE_METADATA env var; off by default.
    if crate::canonical_message::source_metadata_enabled() {
        msg.metadata
            .insert("mqb.src.redis_stream_id".to_string(), id.clone());
    }
    StreamEntry { id, msg }
}

#[async_trait]
impl MessageConsumer for RedisStreamsConsumer {
    // Redis acks each entry individually via XACK, so commits are order-independent.
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

        if self.buffer.is_empty() {
            // Block for the first entry.
            let entry = self
                .rx
                .recv()
                .await
                .map_err(|_| ConsumerError::EndOfStream)??;
            self.buffer.push_back(entry);
        }

        // Greedily drain whatever else is already buffered, up to max_messages.
        while self.buffer.len() < max_messages {
            match self.rx.try_recv() {
                Ok(Ok(entry)) => self.buffer.push_back(entry),
                // A connection error surfaces on the next blocking recv; stop here.
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }

        let mut messages = Vec::with_capacity(max_messages);
        let mut ids = Vec::with_capacity(max_messages);
        while messages.len() < max_messages {
            if let Some(entry) = self.buffer.pop_front() {
                ids.push(entry.id);
                messages.push(entry.msg);
            } else {
                break;
            }
        }

        trace!(stream = %self.stream, count = messages.len(), message_ids = ?LazyMessageIds(&messages), "Received batch of Redis Streams messages");

        let group = self.group.clone();
        let stream = self.stream.clone();
        let mut ack_conn = self.ack_conn.clone();
        let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
            Box::pin(async move {
                // Subscriber mode has no group and therefore nothing to ack.
                let Some(group) = group else {
                    return Ok(());
                };
                // Ack successfully handled entries; Nacked ones stay pending so the
                // group can redeliver them (e.g. via XAUTOCLAIM) later.
                let ack_ids: Vec<String> = ids
                    .into_iter()
                    .zip(dispositions)
                    .filter_map(|(id, disposition)| match disposition {
                        MessageDisposition::Ack | MessageDisposition::Reply(_) => Some(id),
                        MessageDisposition::Nack => None,
                    })
                    .collect();
                if !ack_ids.is_empty() {
                    ack_conn
                        .xack::<_, _, _, ()>(&stream, &group, &ack_ids)
                        .await
                        .map_err(|e| anyhow!("Redis XACK failed: {}", e))?;
                }
                Ok(())
            }) as BoxFuture<'static, anyhow::Result<()>>
        });

        Ok(ReceivedBatch { messages, commit })
    }

    async fn status(&self) -> EndpointStatus {
        let mut conn = self.ack_conn.clone();
        let healthy = redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .is_ok();
        EndpointStatus {
            healthy,
            target: self.stream.clone(),
            pending: Some(self.buffer.len() + self.rx.len()),
            error: if healthy {
                None
            } else {
                Some("Redis PING failed".to_string())
            },
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> redis::Value {
        redis::Value::BulkString(s.as_bytes().to_vec())
    }

    #[test]
    fn parse_entry_reads_payload_and_message_id() {
        let mut map = HashMap::new();
        map.insert(PAYLOAD_FIELD.to_string(), bulk("hello"));
        map.insert(
            MESSAGE_ID_FIELD.to_string(),
            bulk("0000000000000000000000000000002a"),
        );
        map.insert("user_key".to_string(), bulk("kept"));

        let entry = parse_entry("1-0".to_string(), map);
        assert_eq!(entry.msg.payload.as_ref(), b"hello");
        assert_eq!(entry.msg.message_id, 0x2a);
        assert_eq!(
            entry.msg.metadata.get("user_key").map(String::as_str),
            Some("kept")
        );
        // Reserved fields never leak into user metadata.
        assert!(!entry.msg.metadata.contains_key(PAYLOAD_FIELD));
        assert!(!entry.msg.metadata.contains_key(MESSAGE_ID_FIELD));
    }

    #[test]
    fn parse_entry_strips_spoofed_source_metadata() {
        let mut map = HashMap::new();
        map.insert(PAYLOAD_FIELD.to_string(), bulk("body"));
        map.insert("mqb.src.kafka_offset".to_string(), bulk("999"));
        map.insert("user_key".to_string(), bulk("kept"));

        let entry = parse_entry("2-0".to_string(), map);
        assert!(!entry.msg.metadata.contains_key("mqb.src.kafka_offset"));
        assert_eq!(
            entry.msg.metadata.get("user_key").map(String::as_str),
            Some("kept")
        );
    }

    #[test]
    fn parse_entry_without_message_id_gets_generated_id() {
        let mut map = HashMap::new();
        map.insert(PAYLOAD_FIELD.to_string(), bulk("body"));
        let entry = parse_entry("3-0".to_string(), map);
        // CanonicalMessage::new assigns a uuid v7 when none is provided.
        assert_ne!(entry.msg.message_id, 0);
    }
}
