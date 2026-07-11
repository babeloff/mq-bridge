//! Postgres logical-replication CDC source endpoint (pgoutput).
//!
//! Source-only. Streams row-level changes from a Postgres logical replication
//! slot, decodes the pgoutput protocol into flat JSON rows tagged with a
//! `postgres.operation` marker, and maps mq-bridge's ack/nack model onto the
//! slot's confirmed-LSN feedback:
//!
//! * **Ack** a batch → advance the confirmed LSN and feed it back to the server
//!   (`standby_status_update`), so the slot's `confirmed_flush_lsn` moves forward
//!   and WAL up to that point may be recycled.
//! * **Nack** / no commit → the confirmed LSN is **not** advanced, so on
//!   reconnect replication resumes from the last durably-acknowledged position —
//!   at-least-once, no data loss across an in-flight restart.
//!
//! The confirmed LSN is cumulative (like Kafka offsets), so
//! [`commit_requires_order`](MessageConsumer::commit_requires_order) is `true`.

mod pgoutput;
mod replication;
mod state;

use crate::canonical_message::CanonicalMessage;
use crate::checkpoint::{checkpoint_key, CheckpointStore, FileCheckpointStore};
use crate::errors::ConsumerError;
use crate::models::PostgresCdcConfig;
use crate::traits::{BatchCommitFunc, MessageConsumer, MessageDisposition};
use crate::ReceivedBatch;
use anyhow::anyhow;
use async_trait::async_trait;
use pgwire_replication::{Lsn, ReplicationClient};
use replication::ReplicationEvent;
use std::any::Any;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

use pgoutput::decoder::decode_message;
use pgoutput::messages::{Delete, Insert, Message, Relation, Truncate, Update};
use pgoutput::registry::RelationRegistry;
use pgoutput::tuple_to_json_object;
use state::{format_lsn, parse_lsn};

/// A logical-replication CDC consumer for a single Postgres publication + slot.
pub struct PostgresCdcConsumer {
    client: ReplicationClient,
    registry: RelationRegistry,
    /// Rows decoded within the currently-open transaction (flushed to `ready`
    /// on COMMIT, once their durable resume position — the commit `end_lsn` — is known).
    tx_buffer: Vec<CanonicalMessage>,
    /// Committed rows awaiting delivery, each paired with the LSN to confirm on ack.
    ready: VecDeque<(CanonicalMessage, u64)>,
    /// Highest durably-acknowledged LSN; fed back to the server as the slot's confirmed position.
    confirmed: Arc<AtomicU64>,
    checkpoint: Option<Arc<dyn CheckpointStore>>,
    ended: bool,
}

impl PostgresCdcConsumer {
    pub async fn new(config: &PostgresCdcConfig) -> anyhow::Result<Self> {
        if config.url.trim().is_empty() {
            return Err(anyhow!("postgres_cdc: `url` is required"));
        }
        // `publication` and `slot_name` flow into replication commands
        // (START_REPLICATION ... SLOT ... PUBLICATION ...). The slot-lifecycle
        // SQL binds them as parameters, but restrict them to a safe identifier
        // charset as defence-in-depth against injection into the replication path.
        if !is_valid_pg_ident(&config.publication) {
            return Err(anyhow!(
                "postgres_cdc: `publication` must be a non-empty [A-Za-z0-9_] identifier"
            ));
        }
        if !is_valid_pg_ident(&config.slot_name) {
            return Err(anyhow!(
                "postgres_cdc: `slot_name` must be a non-empty [A-Za-z0-9_] identifier"
            ));
        }

        replication::ensure_slot(
            &config.url,
            &config.slot_name,
            config.create_slot,
            config.temporary_slot,
        )
        .await?;

        // Optional secondary checkpoint (the slot's confirmed_flush_lsn is the
        // authoritative durable position; this file mirror seeds a slot-advance
        // on reconnect and aids observability).
        let checkpoint: Option<Arc<dyn CheckpointStore>> = match &config.checkpoint_store {
            Some(spec) => {
                let cid = config
                    .cursor_id
                    .clone()
                    .unwrap_or_else(|| config.slot_name.clone());
                // Only a local file path or a `file://` URL is a valid checkpoint
                // store; reject any other URL scheme rather than treating it as a
                // literal path.
                let path = if let Some(rest) = spec.strip_prefix("file://") {
                    rest.to_string()
                } else if let Some((scheme, _)) = spec.split_once("://") {
                    return Err(anyhow!(
                        "postgres_cdc: checkpoint_store scheme `{scheme}://` is not supported; \
                         use a local file path or a file:// URL"
                    ));
                } else {
                    spec.to_string()
                };
                Some(Arc::new(FileCheckpointStore::new(
                    path,
                    checkpoint_key("postgres_cdc", &cid),
                )))
            }
            None => None,
        };

        let start_lsn: Option<u64> = match &checkpoint {
            Some(cp) => cp.load().await?.and_then(|s| match parse_lsn(&s) {
                Ok(v) => Some(v),
                Err(e) => {
                    warn!(value = %s, error = %e, "postgres_cdc: ignoring unparseable checkpoint LSN");
                    None
                }
            }),
            None => None,
        };

        if let Some(lsn) = start_lsn {
            replication::advance_slot(&config.url, &config.slot_name, lsn).await?;
        }

        let client = replication::start_replication(config, start_lsn).await?;
        info!(
            slot = %config.slot_name,
            publication = %config.publication,
            resume_lsn = ?start_lsn.map(format_lsn),
            "postgres_cdc: replication stream started"
        );

        Ok(Self {
            client,
            registry: RelationRegistry::new(),
            tx_buffer: Vec::new(),
            ready: VecDeque::new(),
            confirmed: Arc::new(AtomicU64::new(start_lsn.unwrap_or(0))),
            checkpoint,
            ended: false,
        })
    }

    /// Push the current confirmed LSN to the server as the slot's applied position.
    fn feedback(&mut self) {
        let lsn = self.confirmed.load(Ordering::Acquire);
        self.client.update_applied_lsn(Lsn::from_u64(lsn));
    }

    /// Handle one replication event, staging rows and flushing committed
    /// transactions into the ready queue.
    fn handle_event(&mut self, ev: ReplicationEvent) -> anyhow::Result<()> {
        match ev {
            ReplicationEvent::Begin { .. } => {
                self.tx_buffer.clear();
            }
            ReplicationEvent::Commit { end_lsn, .. } => {
                let lsn = end_lsn.as_u64();
                for mut msg in self.tx_buffer.drain(..) {
                    msg.metadata
                        .insert("postgres.lsn".to_string(), format_lsn(lsn));
                    self.ready.push_back((msg, lsn));
                }
            }
            ReplicationEvent::XLogData { data, .. } => {
                let msg = decode_message(&data)?;
                self.stage_message(msg)?;
            }
            ReplicationEvent::KeepAlive { .. } => {
                // The library replies to keepalives itself; nothing to stage.
            }
            ReplicationEvent::Message { prefix, .. } => {
                // Logical decoding messages (pg_logical_emit_message) carry no
                // row-change data; skip them rather than terminating the stream.
                debug!(%prefix, "postgres_cdc: ignoring logical decoding message");
            }
            other => {
                // An unrecognized variant may carry change data — fail fast
                // rather than silently drop it.
                return Err(anyhow!(
                    "postgres_cdc: unhandled replication event {other:?}"
                ));
            }
        }
        Ok(())
    }

    fn stage_message(&mut self, msg: Message) -> anyhow::Result<()> {
        match msg {
            Message::Relation(r) => self.registry.insert(r),
            Message::Insert(i) => {
                let m = stage_insert(self.registry.get(i.relation_oid)?, &i);
                self.tx_buffer.push(m);
            }
            Message::Update(u) => {
                let m = stage_update(self.registry.get(u.relation_oid)?, &u);
                self.tx_buffer.push(m);
            }
            Message::Delete(d) => {
                let m = stage_delete(self.registry.get(d.relation_oid)?, &d);
                self.tx_buffer.push(m);
            }
            Message::Truncate(t) => {
                let mut staged = Vec::new();
                for oid in &t.relation_oids {
                    if let Ok(rel) = self.registry.get(*oid) {
                        staged.push(stage_truncate(rel, &t));
                    }
                }
                self.tx_buffer.extend(staged);
            }
            // Begin/Commit arrive as protocol-level ReplicationEvent variants,
            // handled in handle_event; Origin/Type are accepted and ignored.
            Message::Begin(_) | Message::Commit(_) | Message::Origin | Message::Type => {}
        }
        Ok(())
    }
}

/// Build a change-event message: JSON body + `postgres.operation`/schema/table metadata.
fn cdc_message(rel: &Relation, operation: &str, body: serde_json::Value) -> CanonicalMessage {
    let payload = serde_json::to_vec(&body).unwrap_or_default();
    let mut msg = CanonicalMessage::new_bytes(payload.into(), None);
    msg.metadata
        .insert("postgres.operation".to_string(), operation.to_string());
    msg.metadata
        .insert("postgres.schema".to_string(), rel.namespace.clone());
    msg.metadata
        .insert("postgres.table".to_string(), rel.name.clone());
    msg
}

/// A safe Postgres identifier for a publication / replication slot: non-empty
/// and `[A-Za-z0-9_]` only. Slot names are additionally lowercased by Postgres,
/// but rejecting anything outside this set is enough to keep the value from
/// escaping into the replication command it is interpolated into.
fn is_valid_pg_ident(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn stage_insert(rel: &Relation, i: &Insert) -> CanonicalMessage {
    let body = serde_json::Value::Object(tuple_to_json_object(rel, &i.new));
    cdc_message(rel, "insert", body)
}

fn stage_update(rel: &Relation, u: &Update) -> CanonicalMessage {
    let body = serde_json::Value::Object(tuple_to_json_object(rel, &u.new));
    cdc_message(rel, "update", body)
}

fn stage_delete(rel: &Relation, d: &Delete) -> CanonicalMessage {
    // For a delete the only data available is the old tuple (key columns, or the
    // full row under REPLICA IDENTITY FULL).
    let body = serde_json::Value::Object(tuple_to_json_object(rel, &d.old));
    cdc_message(rel, "delete", body)
}

fn stage_truncate(rel: &Relation, _t: &Truncate) -> CanonicalMessage {
    cdc_message(rel, "truncate", serde_json::json!({}))
}

#[async_trait]
impl MessageConsumer for PostgresCdcConsumer {
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch {
                messages: Vec::new(),
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            });
        }

        // Report the latest durably-acknowledged position before blocking.
        self.feedback();

        // Pump the replication stream until a committed transaction is ready.
        while self.ready.is_empty() {
            if self.ended {
                return Err(ConsumerError::EndOfStream);
            }
            match self.client.recv().await {
                Ok(None) | Ok(Some(ReplicationEvent::StoppedAt { .. })) => {
                    self.ended = true;
                    return Err(ConsumerError::EndOfStream);
                }
                Ok(Some(ev)) => {
                    self.handle_event(ev).map_err(ConsumerError::Connection)?;
                }
                Err(e) => {
                    return Err(ConsumerError::Connection(anyhow!(
                        "postgres_cdc: recv failed: {e}"
                    )));
                }
            }
        }

        let n = self.ready.len().min(max_messages);
        let mut messages = Vec::with_capacity(n);
        let mut lsns = Vec::with_capacity(n);
        for _ in 0..n {
            let (msg, lsn) = self.ready.pop_front().expect("ready is non-empty");
            messages.push(msg);
            lsns.push(lsn);
        }

        let confirmed = self.confirmed.clone();
        let checkpoint = self.checkpoint.clone();
        let commit: BatchCommitFunc = Box::new(move |dispositions: Vec<MessageDisposition>| {
            Box::pin(async move {
                // Advance to the LSN of the last consecutively-acked message.
                // A Nack (or an absent disposition) stops the advance there so
                // the un-acked change is redelivered on reconnect.
                let mut target = 0u64;
                for (disp, lsn) in dispositions.iter().zip(lsns.iter()) {
                    match disp {
                        MessageDisposition::Nack => break,
                        MessageDisposition::Ack | MessageDisposition::Reply(_) => target = *lsn,
                    }
                }
                if target > 0 {
                    let mut cur = confirmed.load(Ordering::Acquire);
                    while target > cur {
                        match confirmed.compare_exchange(
                            cur,
                            target,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => break,
                            Err(actual) => cur = actual,
                        }
                    }
                    if let Some(cp) = &checkpoint {
                        if let Err(e) = cp.save(&format_lsn(target)).await {
                            warn!(error = %e, "postgres_cdc: checkpoint save failed");
                        }
                    }
                }
                Ok(())
            })
        });

        Ok(ReceivedBatch { messages, commit })
    }

    /// The confirmed LSN is cumulative — acking a later LSN implicitly confirms
    /// everything before it — so commits must be applied in receive order.
    fn commit_requires_order(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
