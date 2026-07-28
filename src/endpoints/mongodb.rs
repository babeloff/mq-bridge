use crate::canonical_message::tracing_support::LazyMessageIds;
use crate::models::{MongoDbConfig, MongoDbFormat};
use crate::traits::{
    BatchCommitFunc, BoxFuture, ConsumerError, EndpointStatus, MessageConsumer, MessageDisposition,
    MessagePublisher, PublisherError, Received, ReceivedBatch, Sent, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::StreamExt;
use mongodb::{
    bson::{doc, to_bson, to_document, Bson, Document},
    change_stream::ChangeStream,
    error::ErrorKind,
    options::{
        FindOneAndUpdateOptions, FindOptions, FullDocumentType, ReturnDocument, UpdateOptions,
    },
};
use mongodb::{
    change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken},
    IndexModel,
};
use mongodb::{Client, Collection, Database};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tracing::{info, trace, warn};

/// A helper struct for deserialization that matches the BSON structure exactly.
/// The payload is read as a BSON Binary type, which we then manually convert.
#[derive(Serialize, Deserialize, Debug)]
struct MongoMessageRaw {
    #[serde(rename = "_id")]
    id: mongodb::bson::Uuid,
    payload: Bson,
    metadata: Option<Document>,
}

impl TryFrom<MongoMessageRaw> for CanonicalMessage {
    type Error = anyhow::Error;

    fn try_from(raw: MongoMessageRaw) -> Result<Self, Self::Error> {
        let metadata: HashMap<String, String> = raw
            .metadata
            .map(mongodb::bson::from_document)
            .transpose()
            .context("Failed to deserialize metadata from BSON document")?
            .unwrap_or_default();

        let message_id = u128::from_be_bytes(raw.id.bytes());

        let payload = match raw.payload {
            Bson::Binary(bin) => bin.bytes.into(),
            Bson::Document(doc) => {
                let json = serde_json::to_vec(&doc)?;
                json.into()
            }
            Bson::Array(arr) => {
                let json = serde_json::to_vec(&arr)?;
                json.into()
            }
            Bson::String(s) => {
                // Preserve the raw UTF-8 bytes of the string, not a JSON-encoded string
                s.into_bytes().into()
            }
            _ => {
                let json_val: serde_json::Value = mongodb::bson::from_bson(raw.payload)?;
                serde_json::to_vec(&json_val)?.into()
            }
        };

        Ok(CanonicalMessage {
            message_id,
            payload,
            metadata,
        })
    }
}

fn document_to_canonical(doc: Document) -> anyhow::Result<CanonicalMessage> {
    let payload = serde_json::to_vec(&doc)?;
    let mut msg = CanonicalMessage::new(payload, None);
    msg.metadata
        .insert("mq_bridge.original_format".to_string(), "raw".to_string());
    Ok(msg)
}

/// Payload field → `_id`. Fails fast if the payload isn't JSON, the field is absent, or it's null.
fn extract_id_bson(payload: &[u8], id_field: &str) -> anyhow::Result<Bson> {
    let json: serde_json::Value = serde_json::from_slice(payload)
        .with_context(|| format!("id_field '{}' requires a JSON payload", id_field))?;
    let value = json
        .get(id_field)
        .ok_or_else(|| anyhow!("id_field '{}' not found in payload", id_field))?;
    let bson = to_bson(value)
        .with_context(|| format!("id_field '{}' value is not valid BSON", id_field))?;
    if matches!(bson, Bson::Null) {
        return Err(anyhow!("id_field '{}' resolved to null", id_field));
    }
    // MongoDB forbids an array `_id`.
    if matches!(bson, Bson::Array(_)) {
        return Err(anyhow!(
            "id_field '{}' resolved to an array, which is not a valid _id",
            id_field
        ));
    }
    Ok(bson)
}

fn message_to_document(
    message: &CanonicalMessage,
    format: &MongoDbFormat,
    id_field: Option<&str>,
) -> anyhow::Result<Document> {
    // If request-reply metadata is present, we must use the wrapped format to preserve it,
    // regardless of whether the original format was raw.
    let force_wrapped = message.metadata.contains_key("correlation_id")
        || message.metadata.contains_key("reply_to");

    let explicit_id = match id_field {
        Some(field) => Some(extract_id_bson(&message.payload, field)?),
        None => None,
    };

    if !force_wrapped && matches!(format, MongoDbFormat::Raw) {
        if let Ok(mut doc) = serde_json::from_slice::<Document>(&message.payload) {
            if let Some(id) = &explicit_id {
                doc.insert("_id", id.clone());
            }
            return Ok(doc);
        }
        // If parsing fails, fall through to standard wrapping
    }

    let id_uuid = mongodb::bson::Uuid::from_bytes(message.message_id.to_be_bytes());

    let mut metadata = message.metadata.clone();
    // Source/provenance keys are per-hop context, not stored document fields.
    metadata.retain(|key, _| !crate::canonical_message::is_source_metadata_key(key));
    let payload_bson = if matches!(format, MongoDbFormat::Json) {
        if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(&message.payload) {
            if let Ok(bson_val) = mongodb::bson::to_bson(&json_val) {
                metadata.insert("type".to_string(), "json".to_string());
                bson_val
            } else {
                Bson::Binary(mongodb::bson::Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Generic,
                    bytes: message.payload.to_vec(),
                })
            }
        } else {
            // Fallback to binary if not valid JSON
            Bson::Binary(mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: message.payload.to_vec(),
            })
        }
    } else if matches!(format, MongoDbFormat::Text) {
        if let Ok(text) = std::str::from_utf8(&message.payload) {
            metadata.insert("type".to_string(), "text".to_string());
            Bson::String(text.to_string())
        } else {
            Bson::Binary(mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: message.payload.to_vec(),
            })
        }
    } else {
        Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: message.payload.to_vec(),
        })
    };

    let metadata_doc =
        to_document(&metadata).context("Failed to serialize metadata to BSON document")?;

    let id_bson = explicit_id.unwrap_or_else(|| Bson::from(id_uuid));

    Ok(doc! {
        "_id": id_bson,
        "payload": payload_bson,
        "metadata": metadata_doc,
        "locked_until": null,
        "created_at": mongodb::bson::DateTime::now()
    })
}

/// True when `doc` is a wrapped mq-bridge message whose fields are already known to
/// convert cleanly. That guarantee is what lets `parse_mongodb_document` take the fields
/// by value: the raw fallback needs the document intact, so it may only be bypassed when
/// the conversion cannot fail into it.
fn is_wrapped_message(doc: &Document) -> bool {
    if !doc.contains_key("payload") {
        return false;
    }
    if !matches!(doc.get("_id"), Some(Bson::Binary(b)) if b.to_uuid().is_ok()) {
        return false;
    }
    // Non-string metadata values fail `HashMap<String, String>` decoding, which today
    // falls back to the raw path — so those documents must keep taking it.
    match doc.get("metadata") {
        None | Some(Bson::Null) => true,
        Some(Bson::Document(m)) => m.values().all(|v| matches!(v, Bson::String(_))),
        Some(_) => false,
    }
}

fn parse_mongodb_document(mut doc: Document) -> anyhow::Result<CanonicalMessage> {
    // Move the three wrapped fields out of the document. `from_document` consumes what it
    // deserializes, so keeping the raw fallback alive used to cost a deep clone of every
    // document — including the ones that then went on to deserialize fine.
    if is_wrapped_message(&doc) {
        if let (Some(Bson::Binary(bin)), Some(payload)) = (doc.remove("_id"), doc.remove("payload"))
        {
            if let Ok(id) = bin.to_uuid() {
                let metadata = match doc.remove("metadata") {
                    Some(Bson::Document(m)) => Some(m),
                    _ => None,
                };
                return MongoMessageRaw {
                    id,
                    payload,
                    metadata,
                }
                .try_into();
            }
        }
    }
    document_to_canonical(doc)
}

/// Handle a reply to a MongoDB collection by inserting the response into the collection.
///
/// The reply will be inserted into the collection specified by the `reply_to` parameter.
/// If the `correlation_id` parameter is specified, it will be inserted into the reply document
/// as a field named `correlation_id` before insertion.
///
/// The function will log an error if the reply document cannot be serialized to BSON or if
/// the insertion into the collection fails.
async fn handle_reply(
    db: &Database,
    reply_to: Option<&String>,
    correlation_id: Option<&String>,
    response: CanonicalMessage,
) -> anyhow::Result<()> {
    if let Some(coll_name) = reply_to {
        let mut resp = response;
        if let Some(cid) = correlation_id {
            resp.metadata
                .insert("correlation_id".to_string(), cid.clone());
        }
        let doc = message_to_document(&resp, &MongoDbFormat::Normal, None).map_err(|e| {
            tracing::error!(collection = %coll_name, error = %e, "Failed to serialize MongoDB reply");
            anyhow!("Failed to serialize MongoDB reply: {}", e)
        })?;

        let reply_coll = db.collection::<Document>(coll_name);
        if let Err(e) = reply_coll.insert_one(doc).await {
            tracing::error!(collection = %coll_name, error = %e, "Failed to insert MongoDB reply");
            return Err(anyhow::anyhow!("Failed to insert MongoDB reply: {}", e,));
        }
    }
    Ok(())
}

/// A publisher that inserts messages into a MongoDB collection.
pub struct MongoDbPublisher {
    collection: Collection<Document>,
    meta_collection: Collection<Document>,
    db: Database,
    // Retains the shared registry entry so concurrent publishers reuse this client/pool.
    _shared_client: std::sync::Arc<Client>,
    collection_name: String,
    request_reply: bool,
    request_timeout: Duration,
    reply_polling_interval: Duration,
    format: MongoDbFormat,
    id_field: Option<String>,
    report_outcome: bool,
}

/// Metadata key carrying the insert outcome when `report_outcome` is enabled.
const OUTCOME_KEY: &str = "mongodb.outcome";
const OUTCOME_INSERTED: &str = "inserted";
const OUTCOME_EXISTED: &str = "existed";

fn mongodb_uses_sequencer(request_reply: bool, format: &MongoDbFormat) -> bool {
    !request_reply && !matches!(format, MongoDbFormat::Raw)
}

fn namespaced_sequencer_id(collection_name: &str) -> String {
    format!("{}:sequencer", collection_name)
}

fn namespaced_cursor_id(collection_name: &str, cursor_id: &str) -> String {
    format!("{}:cursor:{}", collection_name, cursor_id)
}

impl MongoDbPublisher {
    fn uses_sequencer(&self) -> bool {
        mongodb_uses_sequencer(self.request_reply, &self.format)
    }

    pub async fn new(config: &MongoDbConfig) -> anyhow::Result<Self> {
        let collection_name = config
            .collection
            .as_deref()
            .ok_or_else(|| anyhow!("Collection name is required for MongoDB publisher"))?;
        let shared_client = create_shared_client(config).await?;
        let client = (*shared_client).clone();
        let db = client.database(&config.database);

        if let Some(capped_size) = config.capped_size_bytes {
            let collections = db
                .list_collection_names()
                .filter(doc! { "name": collection_name })
                .await?;
            if collections.is_empty() {
                info!(collection = %collection_name, size = %capped_size, "Creating capped collection");
                db.create_collection(collection_name)
                    .capped(true)
                    .size(capped_size as u64)
                    .await?;
            }
        }

        let collection = db.collection(collection_name);
        let meta_collection_name = config
            .meta_collection
            .clone()
            .unwrap_or_else(|| collection_name.to_string());
        let meta_collection = db.collection(&meta_collection_name);

        if mongodb_uses_sequencer(config.request_reply, &config.format) {
            // Ensure unique index on seq. The sequencer doc has 'seq_counter', so it won't conflict.
            let index_options = mongodb::options::IndexOptions::builder()
                .unique(true)
                .sparse(true) // Only index documents that have the seq field
                .build();
            let index_model = IndexModel::builder()
                .keys(doc! { "seq": 1 })
                .options(index_options)
                .build();
            if let Err(e) = collection.create_index(index_model).await {
                warn!(
                    "Failed to create seq index on collection {}: {}",
                    collection_name, e
                );
            }
        }
        info!(database = %config.database, collection = %collection_name, request_reply = %config.request_reply, "MongoDB publisher connected");

        if let Some(ttl) = config.ttl_seconds {
            let options = mongodb::options::IndexOptions::builder()
                .expire_after(Duration::from_secs(ttl))
                .build();
            let model = IndexModel::builder()
                .keys(doc! { "created_at": 1 })
                .options(options)
                .build();
            if let Err(e) = collection.create_index(model).await {
                warn!(
                    "Failed to create TTL index on publisher collection {} : {}",
                    collection_name, e
                );
            }
        }

        if config.request_reply {
            let reply_collection_name = format!("{}_replies", collection_name);
            let reply_collection = db.collection::<Document>(&reply_collection_name);
            let index_model = IndexModel::builder()
                .keys(doc! { "metadata.correlation_id": 1 })
                .build();
            if let Err(e) = reply_collection.create_index(index_model).await {
                warn!(
                    "Failed to create correlation_id index on reply collection {} : {}",
                    reply_collection_name, e
                );
            }
            // Also apply TTL to the reply collection if configured, to clean up unconsumed replies.
            if let Some(ttl) = config.ttl_seconds {
                let options = mongodb::options::IndexOptions::builder()
                    .expire_after(Duration::from_secs(ttl))
                    .build();
                let model = IndexModel::builder()
                    .keys(doc! { "created_at": 1 })
                    .options(options)
                    .build();
                if let Err(e) = reply_collection.create_index(model).await {
                    warn!(
                        "Failed to create TTL index on reply collection {} : {}",
                        reply_collection_name, e
                    );
                }
            }
        }
        Ok(Self {
            collection,
            meta_collection,
            db,
            _shared_client: shared_client,
            collection_name: collection_name.to_string(),
            request_reply: config.request_reply,
            request_timeout: Duration::from_millis(config.request_timeout_ms.unwrap_or(30000)),
            reply_polling_interval: Duration::from_millis(config.reply_polling_ms.unwrap_or(50)),
            format: config.format.clone(),
            id_field: config.id_field.clone(),
            report_outcome: config.report_outcome,
        })
    }

    async fn recover_correlation_id_from_duplicate(
        &self,
        message: &mut CanonicalMessage,
    ) -> Result<(), PublisherError> {
        // Look up by the same `_id` message_to_document wrote: the id_field value when
        // configured, else the message_id UUID. Otherwise an explicit-id duplicate is
        // never found and the request retries forever.
        let id_bson = match self.id_field.as_deref() {
            Some(field) => {
                extract_id_bson(&message.payload, field).map_err(PublisherError::NonRetryable)?
            }
            None => Bson::from(mongodb::bson::Uuid::from_bytes(
                message.message_id.to_be_bytes(),
            )),
        };
        let filter = doc! { "_id": id_bson };
        match self.collection.find_one(filter).await {
            Ok(Some(existing_doc)) => {
                let existing_msg = parse_mongodb_document(existing_doc).map_err(|e| {
                    PublisherError::NonRetryable(anyhow::anyhow!(
                        "Failed to parse existing document: {}",
                        e
                    ))
                })?;

                if let Some(cid) = existing_msg.metadata.get("correlation_id") {
                    message
                        .metadata
                        .insert("correlation_id".to_string(), cid.clone());
                }
                if let Some(rt) = existing_msg.metadata.get("reply_to") {
                    message.metadata.insert("reply_to".to_string(), rt.clone());
                }
                Ok(())
            }
            Ok(None) => Err(PublisherError::Retryable(anyhow::anyhow!(
                "Duplicate key error but document not found"
            ))),
            Err(e) => Err(PublisherError::Retryable(anyhow::anyhow!(
                "Failed to fetch existing document: {}",
                e
            ))),
        }
    }

    fn outcome_or_ack(&self, message: CanonicalMessage, outcome: &str) -> Sent {
        tag_outcome(self.report_outcome, message, outcome)
    }
}

/// With `report_outcome`, tag the message with `mongodb.outcome` and return it as a
/// `Sent::Response` so a downstream `switch` can branch; otherwise a plain `Ack`.
fn tag_outcome(report_outcome: bool, mut message: CanonicalMessage, outcome: &str) -> Sent {
    if report_outcome {
        message
            .metadata
            .insert(OUTCOME_KEY.to_string(), outcome.to_string());
        Sent::Response(message)
    } else {
        Sent::Ack
    }
}

#[async_trait]
impl MessagePublisher for MongoDbPublisher {
    async fn send(&self, mut message: CanonicalMessage) -> Result<Sent, PublisherError> {
        if !self.request_reply {
            trace!(message_id = %format!("{:032x}", message.message_id), collection = %self.collection_name, uses_sequencer = self.uses_sequencer(), "Publishing document to MongoDB");
            let mut doc = message_to_document(&message, &self.format, self.id_field.as_deref())
                .map_err(PublisherError::NonRetryable)?;

            if self.uses_sequencer() {
                // Atomically increment a sequence counter. This is safe without a transaction for just getting a sequence number.
                // If the subsequent insert fails, a sequence number might be "lost", creating a gap.
                let filter = doc! {
                    "_id": namespaced_sequencer_id(&self.collection_name)
                };
                let update = doc! { "$inc": { "seq_counter": 1_i64 } };
                let options = FindOneAndUpdateOptions::builder()
                    .upsert(true)
                    .return_document(ReturnDocument::After)
                    .build();

                let counter_doc = self
                    .meta_collection
                    .find_one_and_update(filter, update)
                    .with_options(options)
                    .await
                    .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
                let seq = counter_doc
                    .ok_or_else(|| {
                        PublisherError::Retryable(anyhow!(
                            "Sequencer document not returned after upsert"
                        ))
                    })?
                    .get_i64("seq_counter")
                    .map_err(|e| {
                        PublisherError::Retryable(anyhow!(
                            "Invalid seq_counter in sequencer: {}",
                            e
                        ))
                    })?;
                doc.insert("seq", seq);
            }

            match self.collection.insert_one(doc).await {
                Ok(_) => {}
                Err(e) => {
                    if let ErrorKind::Write(mongodb::error::WriteFailure::WriteError(ref w)) =
                        *e.kind
                    {
                        if w.code == 11000 {
                            warn!(message_id = %format!("{:032x}", message.message_id), "Duplicate key error inserting into MongoDB. Treating as idempotent success.");
                            return Ok(self.outcome_or_ack(message, OUTCOME_EXISTED));
                        }
                    }
                    return Err(PublisherError::Retryable(
                        anyhow::anyhow!(e).context("Failed to insert document into MongoDB"),
                    ));
                }
            }

            return Ok(self.outcome_or_ack(message, OUTCOME_INSERTED));
        }

        // --- Request-Reply Logic ---
        let mut correlation_id = if let Some(cid) = message.metadata.get("correlation_id") {
            cid.clone()
        } else {
            fast_uuid_v7::gen_id_string()
        };
        // Convention: reply collection is named <request_collection>_replies
        let reply_collection_name = format!("{}_replies", self.collection_name);

        message
            .metadata
            .insert("correlation_id".to_string(), correlation_id.clone());
        message
            .metadata
            .insert("reply_to".to_string(), reply_collection_name.clone());

        trace!(message_id = %format!("{:032x}", message.message_id), correlation_id = %correlation_id, collection = %self.collection_name, "Publishing request document to MongoDB");
        let doc = message_to_document(&message, &self.format, self.id_field.as_deref())
            .map_err(PublisherError::NonRetryable)?;
        match self.collection.insert_one(doc).await {
            Ok(_) => {}
            Err(e) => {
                let is_duplicate = matches!(&*e.kind, ErrorKind::Write(mongodb::error::WriteFailure::WriteError(w)) if w.code == 11000);
                if is_duplicate {
                    warn!(message_id = %format!("{:032x}", message.message_id), "Duplicate key error inserting request into MongoDB. Treating as idempotent success.");
                    self.recover_correlation_id_from_duplicate(&mut message)
                        .await?;
                    if let Some(cid) = message.metadata.get("correlation_id") {
                        correlation_id = cid.clone();
                    }
                } else {
                    return Err(PublisherError::Retryable(
                        anyhow::anyhow!(e)
                            .context("Failed to insert request document into MongoDB"),
                    ));
                }
            }
        }

        // Now, wait for the response by polling the reply collection.
        let reply_collection = self.db.collection::<Document>(&reply_collection_name);
        let filter = doc! { "metadata.correlation_id": correlation_id.clone() };

        let timeout = self.request_timeout;
        let start = Instant::now();
        let mut current_sleep = self.reply_polling_interval;

        loop {
            if start.elapsed() > timeout {
                return Err(PublisherError::NonRetryable(anyhow!(
                    "Request timed out waiting for MongoDB response"
                )));
            }

            match reply_collection.find_one_and_delete(filter.clone()).await {
                Ok(Some(doc)) => {
                    trace!(correlation_id = %correlation_id, "Received MongoDB response");
                    let response_msg = parse_mongodb_document(doc).map_err(|e| {
                        PublisherError::NonRetryable(anyhow!("Failed to parse response: {}", e))
                    })?;
                    return Ok(Sent::Response(response_msg));
                }
                Ok(None) => {
                    tokio::time::sleep(current_sleep).await;
                    current_sleep = std::cmp::min(
                        current_sleep + current_sleep / 2,
                        Duration::from_millis(500),
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Error polling for MongoDB reply. Retrying...");
                    tokio::time::sleep(current_sleep).await;
                }
            }
        }
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }

        if self.request_reply || self.report_outcome {
            // report_outcome needs a per-message Response, so fan out through single send.
            return crate::traits::send_batch_helper(self, messages, |p, m| Box::pin(p.send(m)))
                .await;
        }

        trace!(count = messages.len(), collection = %self.collection_name, message_ids = ?LazyMessageIds(&messages), "Publishing batch of documents to MongoDB");
        let mut docs = Vec::with_capacity(messages.len());
        let mut failed_messages = Vec::new();
        let mut valid_messages = Vec::with_capacity(messages.len());

        for message in messages {
            match message_to_document(&message, &self.format, self.id_field.as_deref()) {
                Ok(doc) => {
                    docs.push(doc);
                    valid_messages.push(message);
                }
                Err(e) => {
                    failed_messages.push((message, PublisherError::NonRetryable(e)));
                }
            }
        }

        if docs.is_empty() {
            if failed_messages.is_empty() {
                return Ok(SentBatch::Ack);
            } else {
                return Ok(SentBatch::Partial {
                    responses: None,
                    failed: failed_messages,
                });
            }
        }

        if self.uses_sequencer() {
            // Atomically increment a sequence counter for the batch. This is safe without a transaction.
            // If the subsequent insert fails, sequence numbers might be "lost", creating gaps.
            let filter = doc! {
                "_id": namespaced_sequencer_id(&self.collection_name)
            };
            let update = doc! { "$inc": { "seq_counter": docs.len() as i64 } };
            let options = FindOneAndUpdateOptions::builder()
                .upsert(true)
                .return_document(ReturnDocument::After)
                .write_concern(
                    mongodb::options::WriteConcern::builder()
                        .w(mongodb::options::Acknowledgment::Majority)
                        .build(),
                )
                .build();
            let counter_doc = self
                .meta_collection
                .find_one_and_update(filter, update)
                .with_options(options)
                .await
                .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
            let end_seq = counter_doc
                .ok_or_else(|| {
                    PublisherError::Retryable(anyhow!(
                        "Sequencer document not returned after upsert"
                    ))
                })?
                .get_i64("seq_counter")
                .map_err(|e| {
                    PublisherError::Retryable(anyhow!("Invalid seq_counter in sequencer: {}", e))
                })?;
            let start_seq = end_seq - docs.len() as i64 + 1;

            for (i, doc) in docs.iter_mut().enumerate() {
                doc.insert("seq", start_seq + i as i64);
            }
        }

        match self.collection.insert_many(docs).await {
            Ok(_) => {
                if failed_messages.is_empty() {
                    Ok(SentBatch::Ack)
                } else {
                    Ok(SentBatch::Partial {
                        responses: None,
                        failed: failed_messages,
                    })
                }
            }
            Err(e) => {
                if let ErrorKind::InsertMany(ref err) = *e.kind {
                    let mut errors_by_index = HashMap::new();
                    if let Some(write_errors) = &err.write_errors {
                        for we in write_errors {
                            errors_by_index.insert(we.index, we);
                        }
                    }

                    // If we have a write concern error, assume all failed to be safe (potential rollback).
                    // Since we have unique indexes, retrying is idempotent.
                    if err.write_concern_error.is_some() {
                        warn!("MongoDB write concern error detected. Retrying entire batch.");
                        for msg in valid_messages {
                            failed_messages.push((
                                msg,
                                PublisherError::Retryable(anyhow::anyhow!(
                                    "MongoDB write concern error"
                                )),
                            ));
                        }
                        return Ok(SentBatch::Partial {
                            responses: None,
                            failed: failed_messages,
                        });
                    }

                    let mut stop_processing = false;

                    for (i, msg) in valid_messages.into_iter().enumerate() {
                        if stop_processing {
                            failed_messages.push((
                                msg,
                                PublisherError::Retryable(anyhow::anyhow!(
                                    "Message not inserted (skipped due to previous error)"
                                )),
                            ));
                            continue;
                        }

                        if let Some(w) = errors_by_index.get(&i) {
                            if w.code == 11000 {
                                // Duplicate key error. Treat as success (idempotent), but it stops execution in ordered mode.
                                stop_processing = true;
                            } else {
                                let error = PublisherError::Retryable(anyhow::anyhow!(
                                    "MongoDB write error: {:?}",
                                    w
                                ));
                                failed_messages.push((msg, error));
                                stop_processing = true;
                            }
                        }
                    }

                    Ok(SentBatch::Partial {
                        responses: None,
                        failed: failed_messages,
                    })
                } else {
                    Err(PublisherError::Retryable(anyhow!(e)))
                }
            }
        }
    }

    async fn status(&self) -> EndpointStatus {
        let (healthy, error) = match self.db.run_command(doc! { "ping": 1 }).await {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };
        EndpointStatus {
            healthy,
            target: self.collection_name.clone(),
            error,
            details: serde_json::json!({ "database": self.db.name(), "request_reply": self.request_reply }),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A consumer that receives messages from a MongoDB collection, treating it like a queue (locking).
pub struct MongoDbConsumer {
    collection: Collection<Document>,
    db: Database,
    change_stream: Option<tokio::sync::Mutex<ChangeStream<ChangeStreamEvent<Document>>>>,
    polling_interval: Duration,
    collection_name: String,
    receive_query: Option<Document>,
}

impl MongoDbConsumer {
    pub async fn new(config: &MongoDbConfig) -> anyhow::Result<Self> {
        let collection_name = config
            .collection
            .as_deref()
            .ok_or_else(|| anyhow!("Collection name is required for MongoDB consumer"))?;
        let client = create_client(config).await?;
        // The first operation will trigger connection and topology discovery.
        client.list_database_names().await?;

        let db = client.database(&config.database);
        let collection = db.collection(collection_name);

        // Create an index on `locked_until` to speed up finding available messages.
        // This is an idempotent operation, so it's safe to run on every startup.
        info!(collection = %collection_name, "Ensuring 'locked_until' index exists...");
        let index_model = IndexModel::builder()
            .keys(doc! { "locked_until": 1 })
            .build();
        collection.create_index(index_model).await?;

        // Attempt to create a change stream. If it fails because it's a standalone instance,
        // fall back to polling.
        let pipeline = [doc! { "$match": { "operationType": "insert" } }];
        let change_stream_result = collection.watch().pipeline(pipeline).await;

        let (change_stream, mode) = match change_stream_result {
            Ok(stream) => {
                info!("MongoDB is a replica set/sharded cluster. Using change stream.");
                (Some(tokio::sync::Mutex::new(stream)), "change_stream")
            }
            Err(e) if matches!(*e.kind, ErrorKind::Command(ref cmd_err) if cmd_err.code == 40573) =>
            {
                info!("MongoDB is a single instance (ChangeStream support check failed). Falling back to polling for consumer.");
                (None, "polling")
            }
            Err(e) => return Err(e.into()), // For any other error, we propagate it.
        };

        info!(database = %config.database, collection = %collection_name, mode = %mode, "MongoDB consumer connected");

        let receive_query = if let Some(q) = &config.receive_query {
            let doc: Document = serde_json::from_str(q)
                .context("Failed to parse 'receive_query' from configuration as a JSON document")?;
            Some(doc)
        } else {
            None
        };

        Ok(Self {
            collection,
            db,
            change_stream,
            polling_interval: Duration::from_millis(config.polling_interval_ms.unwrap_or(100)),
            collection_name: collection_name.to_string(),
            receive_query,
        })
    }
}

#[async_trait]
impl MessageConsumer for MongoDbConsumer {
    // MongoDB acks each document individually (update/delete by id), so commits
    // can run concurrently and out of order.
    fn commit_requires_order(&self) -> bool {
        false
    }
    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        let extra_filter = self.receive_query.clone().unwrap_or_default();
        loop {
            // Always try to poll for a single document first using the efficient atomic operation.
            if let Some(claimed) = self.try_claim_document(extra_filter.clone()).await? {
                return Ok(claimed);
            }

            // If no document found, wait.
            if let Some(stream_mutex) = &self.change_stream {
                // --- Change Stream Path ---
                // Wait for an event to wake us up.
                let mut stream = stream_mutex.lock().await;
                // Use a timeout to ensure we periodically check for documents even if stream is silent.
                match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
                    Ok(Some(Ok(_))) => continue, // Event received, loop back to try claiming documents.
                    Ok(Some(Err(e))) => return Err(ConsumerError::Connection(e.into())),
                    Ok(None) => {
                        return Err(anyhow!("MongoDB change stream ended unexpectedly").into())
                    }
                    Err(_) => continue, // Timeout, loop back to check for documents.
                }
            }

            // Standalone: Sleep for polling interval.
            tokio::time::sleep(self.polling_interval).await;
        }
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        let extra_filter = self.receive_query.clone().unwrap_or_default();
        loop {
            // Always try to poll for a batch first.
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .context("System time is before UNIX EPOCH")?
                .as_secs() as i64;
            let lock_duration_secs = 60;
            let locked_until = now + lock_duration_secs;

            let claimed_docs = self
                .find_and_claim_documents(extra_filter.clone(), max_messages, now, locked_until)
                .await?;

            if !claimed_docs.is_empty() {
                let (messages, commit) = self.process_claimed_documents(claimed_docs)?;
                return Ok(ReceivedBatch { messages, commit });
            }

            // Drained: wait for the next arrival, then surface an empty batch so the
            // route can pause (empty_batch_delay_ms) or, with exit_on_empty, terminate
            // gracefully. Blocking here indefinitely would make exit_on_empty unreachable.
            if let Some(stream_mutex) = &self.change_stream {
                // Replica set: wait briefly for an insert. On an event, loop back to
                // claim immediately (low latency); on timeout, return the empty batch
                // below so exit_on_empty can fire.
                let mut stream = stream_mutex.lock().await;
                match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
                    Ok(Some(Ok(_))) => continue, // Event received, loop back to claim.
                    Ok(Some(Err(e))) => return Err(ConsumerError::Connection(e.into())),
                    Ok(None) => {
                        return Err(anyhow!("MongoDB change stream ended unexpectedly").into())
                    }
                    Err(_) => {} // Timeout: fall through to return the empty batch.
                }
            } else {
                // Standalone: sleep the polling interval, then return the empty batch.
                tokio::time::sleep(self.polling_interval).await;
            }

            return Ok(ReceivedBatch {
                messages: Vec::new(),
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            });
        }
    }

    async fn status(&self) -> EndpointStatus {
        let mut error = None;
        let healthy = match self.db.run_command(doc! { "ping": 1 }).await {
            Ok(_) => true,
            Err(e) => {
                error = Some(e.to_string());
                false
            }
        };

        let pending = if healthy {
            let now = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let filter = if let Some(extra) = &self.receive_query {
                doc! { "$and": [Self::available_message_filter(now), extra.clone()] }
            } else {
                Self::available_message_filter(now)
            };
            match self.collection.count_documents(filter).await {
                Ok(c) => Some(c as usize),
                Err(e) => {
                    error = Some(format!("Failed to count pending documents: {}", e));
                    None
                }
            }
        } else {
            None
        };

        EndpointStatus {
            healthy,
            target: self.collection_name.clone(),
            pending,
            error,
            details: serde_json::json!({ "database": self.db.name(), "mode": if self.change_stream.is_some() { "change_stream" } else { "polling" } }),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl MongoDbConsumer {
    /// Creates a BSON document filter to find available (unlocked) messages.
    fn available_message_filter(now: i64) -> Document {
        doc! {
            "$and": [
                { "$or": [
                    { "locked_until": { "$exists": false } },
                    { "locked_until": null },
                    { "locked_until": { "$lt": now } }
                ] },
                { "seq_counter": { "$exists": false } },
                { "last_seq": { "$exists": false } }
            ]
        }
    }

    /// Atomically finds and claims one or more documents.
    async fn find_and_claim_documents(
        &self,
        extra_filter: Document,
        limit: usize,
        now: i64,
        locked_until: i64,
    ) -> anyhow::Result<Vec<Document>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let base_filter = if extra_filter.is_empty() {
            Self::available_message_filter(now)
        } else {
            doc! { "$and": [Self::available_message_filter(now), extra_filter] }
        };

        // 1. Find a batch of available documents.
        let mut cursor = self
            .collection
            .find(base_filter.clone())
            .limit(limit as i64)
            .projection(doc! { "_id": 1 })
            .sort(doc! { "_id": 1 })
            .await?;

        let mut ids_to_claim = Vec::new();
        while let Some(result) = cursor.next().await {
            if let Ok(doc) = result {
                if let Some(Bson::Binary(binary)) = doc.get("_id") {
                    if let Ok(uuid) = binary.to_uuid() {
                        ids_to_claim.push(uuid);
                    }
                }
            }
        }

        if ids_to_claim.is_empty() {
            return Ok(Vec::new());
        }

        // 2. Attempt to atomically claim the batch of documents.
        let mut update_filter = doc! { "_id": { "$in": &ids_to_claim } };
        update_filter.extend(base_filter);

        let update = doc! { "$set": { "locked_until": locked_until } };
        let update_result = self.collection.update_many(update_filter, update).await?;

        // 3. If we successfully modified any documents, retrieve their full content.
        if update_result.modified_count > 0 {
            self.get_documents_by_ids(&ids_to_claim).await
        } else {
            Ok(Vec::new())
        }
    }

    /// Atomically finds and locks a document matching the filter.
    async fn try_claim_document(&self, extra_filter: Document) -> anyhow::Result<Option<Received>> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs() as i64;
        let lock_duration_secs = 60;
        let locked_until = now + lock_duration_secs;

        let filter = if extra_filter.is_empty() {
            Self::available_message_filter(now)
        } else {
            doc! { "$and": [Self::available_message_filter(now), extra_filter] }
        };

        let update = doc! { "$set": { "locked_until": locked_until } };

        let options = FindOneAndUpdateOptions::builder()
            .projection(doc! { "_id": 1, "payload": 1, "metadata": 1 })
            .sort(doc! { "_id": 1 }) // Process oldest documents first (FIFO)
            .build();

        match self
            .collection
            .find_one_and_update(filter, update)
            .with_options(options)
            .await
        {
            Ok(Some(doc)) => {
                let id_val = doc
                    .get("_id")
                    .cloned()
                    .ok_or_else(|| anyhow!("Document missing _id"))?;

                let msg = parse_mongodb_document(doc)?;

                let reply_collection_name = msg.metadata.get("reply_to").cloned();
                let correlation_id = msg.metadata.get("correlation_id").cloned();
                let db = self.db.clone();
                let collection_clone = self.collection.clone();

                let commit = Box::new(move |disposition: MessageDisposition| {
                    Box::pin(async move {
                        match disposition {
                            MessageDisposition::Reply(resp) => {
                                handle_reply(
                                    &db,
                                    reply_collection_name.as_ref(),
                                    correlation_id.as_ref(),
                                    resp,
                                )
                                .await?;
                            }
                            MessageDisposition::Ack => {}
                            MessageDisposition::Nack => {
                                collection_clone
                                    .update_one(
                                        doc! { "_id": id_val.clone() },
                                        doc! { "$set": { "locked_until": null } },
                                    )
                                    .await
                                    .context("Failed to unlock Nacked message")?;
                                return Ok(());
                            }
                        }

                        match collection_clone
                            .delete_one(doc! { "_id": id_val.clone() })
                            .await
                        {
                            Ok(delete_result) => {
                                if delete_result.deleted_count == 1 {
                                    trace!(mongodb_id = %id_val, "MongoDB message acknowledged and deleted");
                                } else {
                                    warn!(mongodb_id = %id_val, "Attempted to ack/delete MongoDB message, but it was not found (already deleted?)");
                                }
                            }
                            Err(e) => {
                                tracing::error!(mongodb_id = %id_val, error = %e, "Failed to ack/delete MongoDB message");
                                return Err(anyhow::anyhow!(
                                    "Failed to ack/delete MongoDB message: {}",
                                    e
                                ));
                            }
                        }
                        Ok(())
                    }) as BoxFuture<'static, anyhow::Result<()>>
                });

                Ok(Some(Received {
                    message: msg,
                    commit,
                }))
            }
            Ok(None) => Ok(None), // No document found or claimed
            Err(e) => Err(e.into()),
        }
    }

    /// Retrieves documents by their IDs.
    async fn get_documents_by_ids(
        &self,
        claimed_ids: &[mongodb::bson::Uuid],
    ) -> anyhow::Result<Vec<Document>> {
        let filter = doc! { "_id": { "$in": claimed_ids } };
        let mut cursor = self
            .collection
            .find(filter)
            .projection(doc! { "_id": 1, "payload": 1, "metadata": 1 })
            .await?;

        let mut documents = Vec::new();
        while let Some(result) = cursor.next().await {
            documents.push(result?);
        }
        Ok(documents)
    }

    /// Processes a vector of claimed BSON documents into canonical messages and a single batch commit function.
    fn process_claimed_documents(
        &self,
        docs: Vec<Document>,
    ) -> anyhow::Result<(Vec<CanonicalMessage>, BatchCommitFunc)> {
        let mut messages = Vec::with_capacity(docs.len());
        let mut ids = Vec::with_capacity(docs.len());
        let mut reply_infos = Vec::with_capacity(docs.len());

        for doc in docs {
            let id_val = doc
                .get("_id")
                .cloned()
                .ok_or_else(|| anyhow!("Document missing _id"))?;

            let msg = parse_mongodb_document(doc)?;
            reply_infos.push((
                msg.metadata.get("reply_to").cloned(),
                msg.metadata.get("correlation_id").cloned(),
            ));
            messages.push(msg);

            ids.push(id_val);
        }

        trace!(count = messages.len(), collection = %self.collection_name, message_ids = ?LazyMessageIds(&messages), "Received batch of MongoDB documents");
        let collection_clone = self.collection.clone();
        let db = self.db.clone();

        let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
            Box::pin(async move {
                if dispositions.len() != reply_infos.len() {
                    tracing::warn!(
                        "Disposition count mismatch: expected {}, got {}",
                        reply_infos.len(),
                        dispositions.len()
                    );
                }
                process_mongodb_batch_commit(
                    &db,
                    &collection_clone,
                    &reply_infos,
                    &ids,
                    dispositions,
                )
                .await
            }) as BoxFuture<'static, anyhow::Result<()>>
        });

        Ok((messages, commit))
    }
}

async fn process_mongodb_batch_commit(
    db: &Database,
    collection: &Collection<Document>,
    reply_infos: &[(Option<String>, Option<String>)],
    ids: &[Bson],
    dispositions: Vec<MessageDisposition>,
) -> anyhow::Result<()> {
    let mut ids_to_delete = Vec::new();
    let mut ids_to_unlock = Vec::new();
    let mut errors = Vec::new();

    for (((reply_coll_opt, correlation_id_opt), disposition), id) in
        reply_infos.iter().zip(dispositions).zip(ids.iter())
    {
        // Only send a reply if the message has a 'reply_to' destination and the disposition is a Reply.
        // This allows for fire-and-forget patterns (no reply_to) or explicit replies.
        match disposition {
            MessageDisposition::Reply(resp) => match handle_reply(
                db,
                reply_coll_opt.as_ref(),
                correlation_id_opt.as_ref(),
                resp,
            )
            .await
            {
                Ok(_) => ids_to_delete.push(id.clone()),
                Err(e) => {
                    tracing::error!(id = %id, error = %e, "Failed to send reply");
                    errors.push(e);
                    ids_to_unlock.push(id.clone());
                }
            },
            MessageDisposition::Ack => {
                ids_to_delete.push(id.clone());
            }
            MessageDisposition::Nack => {
                ids_to_unlock.push(id.clone());
            }
        }
    }

    if !ids_to_unlock.is_empty() {
        let filter = doc! { "_id": { "$in": &ids_to_unlock } };
        let update = doc! { "$set": { "locked_until": null } };
        if let Err(e) = collection.update_many(filter, update).await {
            tracing::error!(error = %e, "Failed to unlock Nacked MongoDB messages");
            return Err(anyhow::anyhow!(
                "Failed to unlock Nacked MongoDB messages: {}",
                e
            ));
        }
    }

    if !ids_to_delete.is_empty() {
        let filter = doc! { "_id": { "$in": &ids_to_delete } };
        // Ack failure may result in redelivery. Enable deduplication middleware to handle duplicates.
        if let Err(e) = collection.delete_many(filter).await {
            tracing::error!(error = %e, "Failed to bulk-ack/delete MongoDB messages");
            return Err(anyhow::anyhow!(
                "Failed to bulk-ack/delete MongoDB messages: {}",
                e
            ));
        } else {
            trace!(
                count = ids_to_delete.len(),
                "MongoDB messages acknowledged and deleted"
            );
        }
    }

    if !errors.is_empty() {
        return Err(anyhow::anyhow!(
            "Errors occurred during commit: {:?}",
            errors
        ));
    }
    Ok(())
}

struct CachedCollStats {
    timestamp: Instant,
    stats: Document,
}

/// A subscriber that reads messages from a MongoDB collection using a monotonic sequence number.
/// This replaces the old EventStore-based implementation.
pub struct MongoDbSubscriber {
    collection: Collection<Document>,
    meta_collection: Collection<Document>,
    collection_name: String,
    polling_interval: Duration,
    db: Database,
    cursor_id: Option<String>,
    last_seq: Arc<AtomicI64>,
    cached_coll_stats: Mutex<Option<CachedCollStats>>,
    receive_query: Option<Document>,
}

impl MongoDbSubscriber {
    /// Creates a new MongoDB subscriber.
    ///
    /// The subscriber will watch for inserts to the specified collection and treat them as new events.
    /// If the MongoDB instance does not support ChangeStreams (i.e., a single instance), it will fall back to
    /// periodically polling the collection for new messages.
    ///
    /// Note that the subscriber will start consuming from the last inserted document if ChangeStreams are not
    /// supported. If the collection is empty, it will start consuming from the next inserted document.
    ///
    pub async fn new(config: &MongoDbConfig) -> anyhow::Result<Self> {
        if matches!(config.format, MongoDbFormat::Raw) {
            return Err(anyhow!(
                "MongoDB subscriber/change_stream mode requires wrapped documents with a seq ordering field; raw format is not supported"
            ));
        }
        let collection_name = config
            .collection
            .as_deref()
            .ok_or_else(|| anyhow!("Collection name is required for MongoDB subscriber"))?;
        let client = create_client(config).await?;
        let db = client.database(&config.database);
        let collection: Collection<Document> = db.collection(collection_name);

        let meta_collection_name = config
            .meta_collection
            .clone()
            .unwrap_or_else(|| collection_name.to_string());
        let meta_collection = db.collection::<Document>(&meta_collection_name);

        let missing_seq = collection
            .count_documents(doc! {
                "payload": { "$exists": true },
                "seq": { "$exists": false }
            })
            .limit(1)
            .await
            .with_context(|| {
                format!(
                    "Failed to count documents for collection '{}'",
                    collection_name
                )
            })?;

        if missing_seq > 0 {
            return Err(anyhow!(
                "MongoDB subscriber found documents with payload but no seq field in collection '{}'; use wrapped publisher format or disable subscriber/change_stream mode for raw collections",
                collection_name
            ));
        }

        let mut last_seq = 0;
        if let Some(cid) = &config.cursor_id {
            let cursor_doc_id = namespaced_cursor_id(collection_name, cid);
            if let Ok(Some(doc)) = meta_collection
                .find_one(doc! { "_id": cursor_doc_id })
                .await
            {
                last_seq = doc.get_i64("last_seq").unwrap_or(0);
            }
        } else {
            // Ephemeral mode: start from current sequencer value
            if let Ok(Some(doc)) = meta_collection
                .find_one(doc! { "_id": namespaced_sequencer_id(collection_name) })
                .await
            {
                last_seq = doc.get_i64("seq_counter").unwrap_or(0);
            }
        }
        info!(collection = %collection_name, cursor_id = ?config.cursor_id, start_seq = %last_seq, "MongoDB sequenced subscriber initialized");

        let receive_query = if let Some(q) = &config.receive_query {
            let doc: Document = serde_json::from_str(q)
                .context("Failed to parse 'receive_query' from configuration as a JSON document")?;
            Some(doc)
        } else {
            None
        };

        Ok(Self {
            collection,
            meta_collection,
            collection_name: collection_name.to_string(),
            polling_interval: Duration::from_millis(config.polling_interval_ms.unwrap_or(100)),
            db,
            cursor_id: config.cursor_id.clone(),
            last_seq: Arc::new(AtomicI64::new(last_seq)),
            cached_coll_stats: Mutex::new(None),
            receive_query,
        })
    }
}

#[async_trait]
impl MessageConsumer for MongoDbSubscriber {
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        // Filter for events with seq > last_seq.
        // Crucially, we must filter out the sequencer and cursor documents which might be in the same collection.
        // Events have a 'payload' field, while sequencer/cursors do not.
        let last_seq = self.last_seq.load(Ordering::Relaxed);
        let mut filter = doc! {
            "seq": { "$gt": last_seq },
            "payload": { "$exists": true }
        };
        if let Some(extra) = &self.receive_query {
            filter = doc! { "$and": [filter, extra.clone()] };
        }

        let find_options = FindOptions::builder()
            .sort(doc! { "seq": 1 })
            .limit(max_messages as i64)
            .build();

        let mut cursor = self
            .collection
            .find(filter)
            .with_options(find_options)
            .await
            .map_err(|e| ConsumerError::Connection(e.into()))?;

        let mut messages = Vec::new();
        let mut seqs = Vec::new();

        while let Some(result) = cursor.next().await {
            let doc = match result {
                Ok(doc) => doc,
                Err(e) => {
                    // Surface a read failure as a connection error while the batch is
                    // still empty (nothing consumed yet); once a partial batch exists,
                    // deliver it and skip the failed read.
                    if messages.is_empty() {
                        return Err(ConsumerError::Connection(e.into()));
                    }
                    warn!(
                        collection = %self.collection_name,
                        error = %e,
                        "Failed to read document from MongoDB cursor, skipping"
                    );
                    continue;
                }
            };
            let seq = match doc.get_i64("seq") {
                Ok(seq) => seq,
                Err(e) => {
                    warn!(
                        collection = %self.collection_name,
                        error = %e,
                        "Skipping document with missing or non-i64 'seq' field"
                    );
                    continue;
                }
            };
            match parse_mongodb_document(doc) {
                Ok(msg) => {
                    messages.push(msg);
                    seqs.push(seq);
                    // from here on, we will not received this seq anymore
                    self.last_seq.store(seq, Ordering::Relaxed);
                }
                Err(e) => {
                    warn!(
                        collection = %self.collection_name,
                        seq,
                        error = %e,
                        "Failed to parse MongoDB document, skipping"
                    );
                }
            }
        }

        if !messages.is_empty() {
            let meta_collection = self.meta_collection.clone();
            let collection_name = self.collection_name.clone();
            let cursor_id = self.cursor_id.clone();
            let last_seq_atomic = self.last_seq.clone();
            // Boundary before this batch, for rollback on a nack.
            let resume_from = last_seq;

            let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
                Box::pin(async move {
                    let mut acked = 0usize;
                    let mut highest_acked = 0;
                    for (disp, seq) in dispositions.iter().zip(seqs.iter()) {
                        if matches!(disp, MessageDisposition::Ack | MessageDisposition::Reply(_)) {
                            acked += 1;
                            highest_acked = *seq;
                        } else {
                            break; // Stop at first Nack
                        }
                    }

                    // Not fully acked: roll the in-memory cursor back to the acked
                    // boundary so unacked messages are redelivered instead of skipped.
                    if acked < seqs.len() {
                        let boundary = if acked == 0 {
                            resume_from
                        } else {
                            highest_acked
                        };
                        last_seq_atomic.store(boundary, Ordering::Relaxed);
                    }

                    if highest_acked > 0 {
                        // Only persist if we have a cursor_id
                        if let Some(cid) = cursor_id {
                            let cursor_doc_id = namespaced_cursor_id(&collection_name, &cid);
                            if let Err(e) = meta_collection
                                .update_one(
                                    doc! { "_id": cursor_doc_id },
                                    doc! { "$set": { "last_seq": highest_acked } },
                                )
                                .with_options(UpdateOptions::builder().upsert(true).build())
                                .await
                            {
                                tracing::warn!(cursor_id = %cid, error = %e, "Failed to persist cursor position. Messages may be reprocessed on restart.");
                            }
                        }
                    }
                    Ok(())
                }) as BoxFuture<'static, anyhow::Result<()>>
            });
            return Ok(ReceivedBatch { messages, commit });
        }

        // Drained: pace the poll, then surface an empty batch so the route can pause
        // (empty_batch_delay_ms) or, with exit_on_empty, terminate gracefully. Blocking
        // here indefinitely would make exit_on_empty unreachable.
        tokio::time::sleep(self.polling_interval).await;
        Ok(ReceivedBatch {
            messages: Vec::new(),
            commit: Box::new(|_| Box::pin(async { Ok(()) })),
        })
    }

    async fn status(&self) -> EndpointStatus {
        let mut error = None;
        let healthy = match self.db.run_command(doc! { "ping": 1 }).await {
            Ok(_) => true,
            Err(e) => {
                error = Some(e.to_string());
                false
            }
        };

        let pending = if healthy {
            let last_seq = self.last_seq.load(Ordering::Relaxed);
            let mut filter = doc! { "seq": { "$gt": last_seq }, "payload": { "$exists": true } };
            if let Some(extra) = &self.receive_query {
                filter = doc! { "$and": [filter, extra.clone()] };
            }

            match self.collection.count_documents(filter).await {
                Ok(c) => Some(c as usize),
                Err(e) => {
                    error = Some(format!("Failed to count pending: {}", e));
                    None
                }
            }
        } else {
            None
        };

        let (mut capacity, mut details) =
            (None, serde_json::json!({ "cursor_id": self.cursor_id }));

        if healthy {
            let mut stats_doc = {
                let cached_stats_guard = self.cached_coll_stats.lock().unwrap();
                cached_stats_guard
                    .as_ref()
                    .filter(|cached| cached.timestamp.elapsed() < Duration::from_secs(5))
                    .map(|cached| cached.stats.clone())
            };

            if stats_doc.is_none() {
                // Cache is stale or empty, fetch new stats
                match self
                    .db
                    .run_command(doc! { "collStats": self.collection.name() })
                    .await
                {
                    Ok(stats) => {
                        stats_doc = Some(stats.clone());
                        let mut cached_stats_guard = self.cached_coll_stats.lock().unwrap();
                        *cached_stats_guard = Some(CachedCollStats {
                            timestamp: Instant::now(),
                            stats,
                        });
                    }
                    Err(e) => {
                        warn!(
                            "Failed to get collStats for {}: {}",
                            self.collection.name(),
                            e
                        );
                        if error.is_none() {
                            // Only update error if no other error is present
                            error = Some(format!("Failed to get collStats: {}", e));
                        }
                    }
                }
            }

            if let Some(stats) = stats_doc {
                let is_capped = stats.get_bool("capped").unwrap_or(false);
                if is_capped {
                    if let Ok(max_size) = stats.get_i64("maxSize") {
                        details["capacity_bytes"] = serde_json::json!(max_size);
                    }
                    capacity = stats.get_i64("max").ok().map(|s| s as usize);
                }
                details = serde_json::json!({ "cursor_id": self.cursor_id });
                if is_capped {
                    details["capped"] = serde_json::Value::Bool(true);
                }
            }
        };

        EndpointStatus {
            healthy,
            target: self.collection.name().to_string(),
            pending,
            capacity,
            details,
            error,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// --- Non-destructive `_id`-cursor reader (arbitrary collections) ---

/// Encodes a BSON `_id` into a portable, tagged string for checkpoint persistence.
/// Supports the homogeneous `_id` types a real collection uses: ObjectId, BSON UUID
/// (subtype 4, as mq-bridge's own publisher writes), and integers. Returns `None` for
/// unsupported types (cursor is then not persisted).
fn encode_id(id: &Bson) -> Option<String> {
    match id {
        Bson::ObjectId(oid) => Some(format!("oid:{}", oid.to_hex())),
        Bson::Binary(bin)
            if bin.subtype == mongodb::bson::spec::BinarySubtype::Uuid
                || bin.subtype == mongodb::bson::spec::BinarySubtype::UuidOld =>
        {
            bin.to_uuid().ok().map(|u| format!("uuid:{}", u))
        }
        Bson::Int64(n) => Some(format!("int:{}", n)),
        Bson::Int32(n) => Some(format!("int:{}", n)),
        Bson::String(s) => Some(format!("str:{}", s)),
        _ => None,
    }
}

/// Decodes a tagged string produced by [`encode_id`] back into a BSON `_id` for the
/// `$gt` query. Returns `None` on a malformed/unknown value (reader then starts from the
/// beginning rather than silently skipping).
fn decode_id(s: &str) -> Option<Bson> {
    let (tag, val) = s.split_once(':')?;
    match tag {
        "oid" => mongodb::bson::oid::ObjectId::parse_str(val)
            .ok()
            .map(Bson::ObjectId),
        "uuid" => mongodb::bson::Uuid::parse_str(val).ok().map(Bson::from),
        "int" => val.parse::<i64>().ok().map(Bson::Int64),
        "str" => Some(Bson::String(val.to_string())),
        _ => None,
    }
}

/// Checkpoint store backed by a `mqb_cursors` collection in the source database.
struct MongoCollectionCheckpointStore {
    meta: Collection<Document>,
    doc_id: String,
}

#[async_trait]
impl crate::checkpoint::CheckpointStore for MongoCollectionCheckpointStore {
    async fn load(&self) -> anyhow::Result<Option<String>> {
        let doc = self
            .meta
            .find_one(doc! { "_id": self.doc_id.clone() })
            .await?;
        Ok(doc.and_then(|d| d.get_str("last_value").ok().map(|s| s.to_string())))
    }

    async fn save(&self, value: &str) -> anyhow::Result<()> {
        self.meta
            .update_one(
                doc! { "_id": self.doc_id.clone() },
                doc! { "$set": { "last_value": value } },
            )
            .with_options(UpdateOptions::builder().upsert(true).build())
            .await?;
        Ok(())
    }
}

/// Build a checkpoint store on an **external** MongoDB deployment (its own client), used when
/// `checkpoint_store` is a `mongodb://host/db[/collection]` URL.
pub(crate) async fn build_mongo_checkpoint_store(
    url: &str,
    database: &str,
    collection: Option<String>,
    source_name: &str,
    cursor_id: &str,
) -> anyhow::Result<Arc<dyn crate::checkpoint::CheckpointStore>> {
    let client = Client::with_uri_str(url)
        .await
        .with_context(|| format!("Failed to connect checkpoint store at '{}'", url))?;
    let db = client.database(database);
    let meta_name = collection.unwrap_or_else(|| crate::checkpoint::default_meta_name(source_name));
    Ok(Arc::new(MongoCollectionCheckpointStore {
        meta: db.collection::<Document>(&meta_name),
        doc_id: crate::checkpoint::checkpoint_key(source_name, cursor_id),
    }))
}

/// A non-destructive, resumable reader over an **arbitrary** MongoDB collection. Pages by
/// `_id` (`find({_id:{$gt:last}}).sort({_id:1})`), never mutates the source, and persists
/// the last successfully-sunk `_id` (keyed by `cursor_id`) to a pluggable checkpoint store
/// (a separate `mqb_cursors` collection by default, or a local file). At-least-once.
pub struct MongoDbIdReader {
    collection: Collection<Document>,
    db: Database,
    checkpoint: Option<Arc<dyn crate::checkpoint::CheckpointStore>>,
    cursor_id: Option<String>,
    last_id: Arc<Mutex<Option<Bson>>>,
    receive_query: Option<Document>,
}

impl MongoDbIdReader {
    pub async fn new(config: &MongoDbConfig) -> anyhow::Result<Self> {
        let collection_name = config
            .collection
            .as_deref()
            .ok_or_else(|| anyhow!("Collection name is required for MongoDB id-cursor reader"))?;
        let client = create_client(config).await?;
        let db = client.database(&config.database);
        let collection: Collection<Document> = db.collection(collection_name);

        let receive_query = if let Some(q) = &config.receive_query {
            let doc: Document = serde_json::from_str(q)
                .context("Failed to parse 'receive_query' from configuration as a JSON document")?;
            Some(doc)
        } else {
            None
        };

        let checkpoint: Option<Arc<dyn crate::checkpoint::CheckpointStore>> = if let Some(cid) =
            &config.cursor_id
        {
            use crate::checkpoint::CheckpointBackend;
            let backend = match &config.checkpoint_store {
                // Absent: a dedicated per-source collection so the source is never written.
                None => CheckpointBackend::Source {
                    name: crate::checkpoint::default_meta_name(collection_name),
                },
                Some(spec) => crate::checkpoint::parse_checkpoint_store(spec)?,
            };
            let store: Arc<dyn crate::checkpoint::CheckpointStore> = match backend {
                CheckpointBackend::Source { name } => Arc::new(MongoCollectionCheckpointStore {
                    meta: db.collection::<Document>(&name),
                    doc_id: crate::checkpoint::checkpoint_key(collection_name, cid),
                }),
                external => {
                    crate::checkpoint::build_external_store(external, collection_name, cid).await?
                }
            };
            Some(store)
        } else {
            warn!(
                collection = %collection_name,
                "MongoDB resumable reader has no cursor_id; resume is disabled and every restart re-copies from the beginning. Set cursor_id to persist progress."
            );
            None
        };

        let last_id = match &checkpoint {
            Some(cp) => cp.load().await?.and_then(|s| {
                let decoded = decode_id(&s);
                if decoded.is_none() {
                    warn!(value = %s, "Ignoring unparseable mongo id cursor; starting from beginning");
                }
                decoded
            }),
            None => None,
        };
        info!(collection = %collection_name, cursor_id = ?config.cursor_id, has_checkpoint = %last_id.is_some(), "MongoDB id-cursor reader initialized");

        Ok(Self {
            collection,
            db,
            checkpoint,
            cursor_id: config.cursor_id.clone(),
            last_id: Arc::new(Mutex::new(last_id)),
            receive_query,
        })
    }
}

#[async_trait]
impl MessageConsumer for MongoDbIdReader {
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        // `_id` before this batch, for rollback on nack (see the commit closure).
        let resume_from = self.last_id.lock().unwrap().clone();

        let mut messages = Vec::new();
        let mut ids: Vec<Bson> = Vec::new();

        // Page until we collect at least one message or a query returns no documents (truly
        // drained). This keeps an empty batch meaning "drained": a whole page of unreadable
        // docs is skipped-with-progress rather than stalling the reader or exiting early.
        loop {
            let last = self.last_id.lock().unwrap().clone();
            let mut filter = match &last {
                Some(v) => doc! { "_id": { "$gt": v.clone() } },
                None => doc! {},
            };
            if let Some(extra) = &self.receive_query {
                filter = if filter.is_empty() {
                    extra.clone()
                } else {
                    doc! { "$and": [filter, extra.clone()] }
                };
            }

            let find_options = FindOptions::builder()
                .sort(doc! { "_id": 1 })
                .limit(max_messages as i64)
                .build();

            let mut cursor = self
                .collection
                .find(filter)
                .with_options(find_options)
                .await
                .map_err(|e| ConsumerError::Connection(e.into()))?;

            let mut docs_in_page = 0usize;
            while let Some(result) = cursor.next().await {
                // A cursor error mid-page is a real failure; surface it instead of treating the
                // truncated page as "drained".
                let doc = result.map_err(|e| ConsumerError::Connection(e.into()))?;
                docs_in_page += 1;
                let Some(id) = doc.get("_id").cloned() else {
                    warn!("MongoDB document without an `_id`; skipping");
                    continue;
                };
                match parse_mongodb_document(doc) {
                    Ok(msg) => {
                        messages.push(msg);
                        ids.push(id.clone());
                    }
                    Err(e) => warn!(error = %e, "Skipping unparseable MongoDB document"),
                }
                // Advance past this `_id` whether or not it parsed, so a bad doc can't stall paging.
                *self.last_id.lock().unwrap() = Some(id);
            }

            // Got messages, or the collection is exhausted -> stop; otherwise the whole page was
            // skipped and more may follow, so page again.
            if !messages.is_empty() || docs_in_page == 0 {
                break;
            }
        }

        if messages.is_empty() {
            // Exhausted: surface an empty batch so the route can pause or terminate.
            return Ok(ReceivedBatch {
                messages: Vec::new(),
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            });
        }

        let checkpoint = self.checkpoint.clone();
        let last_id = self.last_id.clone();
        let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
            Box::pin(async move {
                // Highest `_id` of a contiguous run of Acks from the front (stop at first Nack).
                let mut acked = 0usize;
                for disp in dispositions.iter().take(ids.len()) {
                    if matches!(disp, MessageDisposition::Ack | MessageDisposition::Reply(_)) {
                        acked += 1;
                    } else {
                        break;
                    }
                }
                let boundary: Option<Bson> = if acked == 0 {
                    resume_from
                } else {
                    Some(ids[acked - 1].clone())
                };
                // If any doc was not acked, roll the in-memory read cursor back to the
                // committed boundary so nacked/unprocessed docs are re-read on the next
                // page (at-least-once) instead of being skipped until a restart.
                if acked < ids.len() {
                    *last_id.lock().unwrap() = boundary.clone();
                }
                if let (Some(id), Some(cp)) = (boundary, checkpoint) {
                    match encode_id(&id) {
                        Some(s) => {
                            if let Err(e) = cp.save(&s).await {
                                tracing::warn!(error = %e, "Failed to persist mongo id cursor. Messages may be reprocessed on restart.");
                            }
                        }
                        None => tracing::warn!(
                            "Unsupported _id type for cursor persistence; not checkpointing"
                        ),
                    }
                }
                Ok(())
            }) as BoxFuture<'static, anyhow::Result<()>>
        });

        Ok(ReceivedBatch { messages, commit })
    }

    async fn status(&self) -> EndpointStatus {
        let mut error = None;
        let healthy = match self.db.run_command(doc! { "ping": 1 }).await {
            Ok(_) => true,
            Err(e) => {
                error = Some(e.to_string());
                false
            }
        };
        let pending = if healthy {
            let last = self.last_id.lock().unwrap().clone();
            let filter = match &last {
                Some(v) => doc! { "_id": { "$gt": v.clone() } },
                None => doc! {},
            };
            match self.collection.count_documents(filter).await {
                Ok(c) => Some(c as usize),
                Err(e) => {
                    error = Some(format!("Failed to count pending: {}", e));
                    None
                }
            }
        } else {
            None
        };

        EndpointStatus {
            healthy,
            target: self.collection.name().to_string(),
            pending,
            capacity: None,
            details: serde_json::json!({ "cursor_id": self.cursor_id, "mode": "resumable" }),
            error,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Serializes a change-stream resume token to a canonical extended-JSON string for durable
/// checkpointing. Canonical extJSON preserves the token's BSON types (including any `_typeBits`
/// binary) so it round-trips exactly through [`decode_resume_token`].
fn encode_resume_token(token: &ResumeToken) -> anyhow::Result<String> {
    let doc = to_document(token).context("Failed to serialize resume token")?;
    let value = Bson::Document(doc).into_canonical_extjson();
    serde_json::to_string(&value).context("Failed to encode resume token")
}

/// Parses a resume token previously produced by [`encode_resume_token`]. Returns `None` on a
/// malformed value so the reader starts from the current stream position rather than failing.
fn decode_resume_token(s: &str) -> Option<ResumeToken> {
    let value: serde_json::Value = serde_json::from_str(s).ok()?;
    let bson = Bson::try_from(value).ok()?;
    mongodb::bson::from_bson::<ResumeToken>(bson).ok()
}

/// Opens a change stream on `collection` with an optional resume position, using `updateLookup`
/// so update/replace events carry the full post-image.
async fn open_change_stream(
    collection: &Collection<Document>,
    pipeline: &[Document],
    resume_after: Option<ResumeToken>,
) -> anyhow::Result<ChangeStream<ChangeStreamEvent<Document>>> {
    let mut watch = collection
        .watch()
        .pipeline(pipeline.to_vec())
        .full_document(FullDocumentType::UpdateLookup);
    if let Some(token) = resume_after {
        watch = watch.resume_after(token);
    }
    let name = collection.name().to_string();
    watch.await.map_err(|e| {
        // Preserve the source `mongodb::error::Error` (via `.context`, not stringified) so callers
        // can downcast it — `capture_all` only falls back to the `_id` reader for code 40573.
        anyhow::Error::new(e).context(format!("Failed to open MongoDB change stream for '{name}'"))
    })
}

/// True only for the MongoDB "change streams require a replica set" error (code 40573) — the one
/// case where `capture_all` may fall back to the insert-only `_id` reader. Auth, network, and
/// configuration failures return false so they propagate instead of being silently downgraded.
pub(crate) fn is_change_stream_unsupported(err: &anyhow::Error) -> bool {
    err.downcast_ref::<mongodb::error::Error>()
        .is_some_and(|e| matches!(&*e.kind, ErrorKind::Command(cmd) if cmd.code == 40573))
}

/// While idle (no matching changes), the CDC reader periodically advances its durable checkpoint to
/// the change stream's `postBatchResumeToken` so a long-idle stream's saved token can't age out of
/// the oplog window. This interval bounds how stale that saved position can get.
const IDLE_RESUME_REFRESH: Duration = Duration::from_secs(10);

/// A real change-data-capture reader over an arbitrary MongoDB collection. Tails the collection's
/// change stream (requires a replica set), emitting insert/update/replace/delete events with the
/// full post-image (`updateLookup`), and persists the resume token (keyed by `cursor_id`) to a
/// pluggable checkpoint store so a restart resumes exactly after the last acked change.
/// At-least-once. Backs the `capture_new`/`capture_all` modes; unlike the insert-only `_id` reader
/// it captures updates and deletes, not just appends.
pub struct MongoDbChangeStreamReader {
    collection: Collection<Document>,
    db: Database,
    collection_name: String,
    checkpoint: Option<Arc<dyn crate::checkpoint::CheckpointStore>>,
    cursor_id: Option<String>,
    receive_query: Option<Document>,
    pipeline: Vec<Document>,
    // Wrapped in a Mutex so the reader is `Sync` (a bare `ChangeStream` is `Send` but not `Sync`),
    // which the `MessageConsumer` trait's `&self` methods require. `None` while the initial
    // snapshot is draining; opened (at `pending_resume`) when the snapshot completes.
    stream: tokio::sync::Mutex<Option<ChangeStream<ChangeStreamEvent<Document>>>>,
    // Stream start position captured before the snapshot; the stream is opened here after it drains.
    pending_resume: Mutex<Option<ResumeToken>>,
    // Snapshot paging position (`_id > last`), shared with the commit closure for nack rollback.
    snapshot_last_id: Arc<Mutex<Option<Bson>>>,
    // Idle resume-token refresh state. `inflight` counts delivered-but-not-yet-committed batches;
    // `refresh_clean` is cleared for the session's remainder once a streaming batch is nacked (a
    // redelivery gap then exists). Idle refresh only persists the postBatchResumeToken when nothing
    // is in flight AND clean — so it can never advance past an un-acked change. `last_saved_token`
    // dedupes redundant writes when the token hasn't moved.
    inflight: Arc<AtomicUsize>,
    refresh_clean: Arc<AtomicBool>,
    last_saved_token: Arc<Mutex<Option<String>>>,
}

impl MongoDbChangeStreamReader {
    /// `snapshot` = read the existing documents before streaming changes (`capture_all`); when false
    /// only new changes are streamed (`capture_new`).
    pub async fn new(config: &MongoDbConfig, snapshot: bool) -> anyhow::Result<Self> {
        let collection_name = config
            .collection
            .as_deref()
            .ok_or_else(|| anyhow!("Collection name is required for MongoDB CDC reader"))?;
        let client = create_client(config).await?;
        let db = client.database(&config.database);
        let collection: Collection<Document> = db.collection(collection_name);

        // Optional filter: a `$match` stage on the change stream, and the equivalent `find` filter
        // for the snapshot phase.
        let receive_query = if let Some(q) = &config.receive_query {
            let doc: Document = serde_json::from_str(q)
                .context("Failed to parse 'receive_query' from configuration as a JSON document")?;
            Some(doc)
        } else {
            None
        };
        // A change stream sees event *envelopes*, not raw documents, so a `receive_query` on
        // document fields must target the `fullDocument` namespace or it would match nothing and
        // silently drop every event. The snapshot phase keeps the raw predicate (it queries the
        // collection directly). Note: delete events carry no `fullDocument`, so document-field
        // filters exclude deletes.
        let pipeline: Vec<Document> = receive_query
            .as_ref()
            .map(|q| vec![doc! { "$match": full_document_match(q) }])
            .unwrap_or_default();

        let checkpoint: Option<Arc<dyn crate::checkpoint::CheckpointStore>> = if let Some(cid) =
            &config.cursor_id
        {
            use crate::checkpoint::CheckpointBackend;
            let backend = match &config.checkpoint_store {
                None => CheckpointBackend::Source {
                    name: crate::checkpoint::default_meta_name(collection_name),
                },
                Some(spec) => crate::checkpoint::parse_checkpoint_store(spec)?,
            };
            let store: Arc<dyn crate::checkpoint::CheckpointStore> = match backend {
                CheckpointBackend::Source { name } => Arc::new(MongoCollectionCheckpointStore {
                    meta: db.collection::<Document>(&name),
                    doc_id: crate::checkpoint::checkpoint_key(collection_name, cid),
                }),
                external => {
                    crate::checkpoint::build_external_store(external, collection_name, cid).await?
                }
            };
            Some(store)
        } else {
            warn!(
                collection = %collection_name,
                "MongoDB CDC reader has no cursor_id; resume is disabled and every restart starts from the current stream position. Set cursor_id to persist progress."
            );
            None
        };

        let resume_token = match &checkpoint {
            Some(cp) => cp.load().await?.and_then(|s| {
                let decoded = decode_resume_token(&s);
                if decoded.is_none() {
                    warn!(value = %s, "Ignoring unparseable mongo resume token; starting from current stream position");
                }
                decoded
            }),
            None => None,
        };

        // Cold start with `capture_all`: capture the current stream position, then snapshot the
        // existing documents before streaming from that position (no gap; at-least-once). The
        // stream is opened later, when the snapshot drains, so no change-stream cursor is held open
        // during a potentially long snapshot.
        let take_snapshot = resume_token.is_none() && snapshot;
        let (stream, pending_resume) = if take_snapshot {
            let probe = open_change_stream(&collection, &pipeline, None).await?;
            match probe.resume_token() {
                Some(token) => {
                    info!(collection = %collection_name, "MongoDB CDC reader starting initial snapshot");
                    (None, Some(token))
                }
                None => {
                    warn!(collection = %collection_name, "Server did not provide a resume token; skipping snapshot and streaming new changes only");
                    (Some(probe), None)
                }
            }
        } else {
            (
                Some(open_change_stream(&collection, &pipeline, resume_token.clone()).await?),
                None,
            )
        };

        info!(collection = %collection_name, cursor_id = ?config.cursor_id, resumed = %resume_token.is_some(), snapshot = %pending_resume.is_some(), "MongoDB CDC reader initialized");

        Ok(Self {
            collection,
            db,
            collection_name: collection_name.to_string(),
            checkpoint,
            cursor_id: config.cursor_id.clone(),
            receive_query,
            pipeline,
            stream: tokio::sync::Mutex::new(stream),
            pending_resume: Mutex::new(pending_resume),
            snapshot_last_id: Arc::new(Mutex::new(None)),
            inflight: Arc::new(AtomicUsize::new(0)),
            refresh_clean: Arc::new(AtomicBool::new(true)),
            last_saved_token: Arc::new(Mutex::new(
                resume_token
                    .as_ref()
                    .and_then(|t| encode_resume_token(t).ok()),
            )),
        })
    }

    /// Pages the initial snapshot by `_id` (like the resumable reader), returning `None` once the
    /// collection is exhausted so the caller can hand off to the change stream.
    async fn snapshot_batch(
        &self,
        max_messages: usize,
    ) -> Result<Option<ReceivedBatch>, ConsumerError> {
        let resume_from = self.snapshot_last_id.lock().unwrap().clone();
        let last = resume_from.clone();
        // Never snapshot the bridge's own sequencer bookkeeping doc (see `available_message_filter`).
        let mut filter = match &last {
            Some(v) => doc! { "_id": { "$gt": v.clone() }, "seq_counter": { "$exists": false } },
            None => doc! { "seq_counter": { "$exists": false } },
        };
        if let Some(extra) = &self.receive_query {
            filter = doc! { "$and": [filter, extra.clone()] };
        }
        let find_options = FindOptions::builder()
            .sort(doc! { "_id": 1 })
            .limit(max_messages as i64)
            .build();
        let mut cursor = self
            .collection
            .find(filter)
            .with_options(find_options)
            .await
            .map_err(|e| ConsumerError::Connection(e.into()))?;

        let mut messages = Vec::new();
        let mut ids: Vec<Bson> = Vec::new();
        while let Some(result) = cursor.next().await {
            let doc = result.map_err(|e| ConsumerError::Connection(e.into()))?;
            let Some(id) = doc.get("_id").cloned() else {
                warn!("MongoDB snapshot document without an `_id`; skipping");
                continue;
            };
            match serde_json::to_vec(&doc) {
                Ok(payload) => {
                    let mut msg = CanonicalMessage::new(payload, None);
                    msg.metadata
                        .insert("mongodb.operation".to_string(), "insert".to_string());
                    msg.metadata
                        .insert("mongodb.snapshot".to_string(), "true".to_string());
                    if let Some(enc) = encode_id(&id) {
                        msg.metadata.insert("mongodb.document_id".to_string(), enc);
                    }
                    messages.push(msg);
                    ids.push(id.clone());
                }
                Err(e) => warn!(error = %e, "Skipping unserializable MongoDB snapshot document"),
            }
            *self.snapshot_last_id.lock().unwrap() = Some(id);
        }

        // Exhausted: no more snapshot documents. The caller hands off to the change stream.
        if messages.is_empty() {
            return Ok(None);
        }

        let last_id = self.snapshot_last_id.clone();
        // Gate idle refresh: an un-acked snapshot batch still in flight when streaming begins must
        // block the postBatchResumeToken from being persisted, or its docs would be lost on restart.
        let inflight = self.inflight.clone();
        let refresh_clean = self.refresh_clean.clone();
        let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
            Box::pin(async move {
                let mut acked = 0usize;
                for disp in dispositions.iter().take(ids.len()) {
                    if matches!(disp, MessageDisposition::Ack | MessageDisposition::Reply(_)) {
                        acked += 1;
                    } else {
                        break;
                    }
                }
                // Roll the snapshot cursor back to the last acked `_id` so nacked docs are re-read.
                if acked < ids.len() {
                    let boundary = if acked == 0 {
                        resume_from
                    } else {
                        Some(ids[acked - 1].clone())
                    };
                    *last_id.lock().unwrap() = boundary;
                    // Latch the gap: once the stream opens, snapshot docs can only be recovered by
                    // re-snapshotting from the start, so no resume token may be persisted this
                    // session. Blocks both idle refresh and later streaming-batch commits.
                    refresh_clean.store(false, Ordering::Release);
                }
                inflight.fetch_sub(1, Ordering::AcqRel);
                Ok(())
            }) as BoxFuture<'static, anyhow::Result<()>>
        });
        self.inflight.fetch_add(1, Ordering::AcqRel);
        Ok(Some(ReceivedBatch { messages, commit }))
    }

    /// Maps a change event into a canonical message, tagging the operation and document `_id`.
    /// Returns `None` for events carrying no usable payload (e.g. an update whose post-image was
    /// already deleted by the time of the lookup).
    fn event_to_message(event: &ChangeStreamEvent<Document>) -> Option<CanonicalMessage> {
        let (op, payload) = match event.operation_type {
            OperationType::Insert | OperationType::Update | OperationType::Replace => {
                let doc = event.full_document.as_ref()?;
                // Skip the bridge's own sequencer bookkeeping doc (its `$inc` updates and insert).
                if doc.contains_key("seq_counter") {
                    return None;
                }
                (op_str(&event.operation_type), serde_json::to_vec(doc).ok()?)
            }
            OperationType::Delete => {
                // No post-image on delete; carry the document key so the sink can act on the `_id`.
                let key = event.document_key.clone().unwrap_or_default();
                ("delete", serde_json::to_vec(&key).ok()?)
            }
            _ => return None, // drop/rename/invalidate/other: not row-level data changes
        };

        let mut msg = CanonicalMessage::new(payload, None);
        msg.metadata
            .insert("mongodb.operation".to_string(), op.to_string());
        if let Some(id) = event.document_key.as_ref().and_then(|k| k.get("_id")) {
            if let Some(enc) = encode_id(id) {
                msg.metadata.insert("mongodb.document_id".to_string(), enc);
            }
        }
        Some(msg)
    }

    /// Called while the stream is idle: persist the change stream's postBatchResumeToken so the
    /// durable checkpoint tracks the oplog even with no matching changes. Only advances when no
    /// batch is in flight (`inflight == 0`) and no un-acked gap exists (`refresh_clean`), so the
    /// persisted token is always a safe resume point that can't skip a delivered-but-un-acked
    /// change. During idle there are no matching changes, so the token only moves past irrelevant
    /// oplog entries — nothing is lost.
    /// `token` is the stream's postBatchResumeToken, extracted by the caller *before* any await (a
    /// shared `&ChangeStream` is not `Send`, so it can't be held across the checkpoint write).
    async fn refresh_idle_checkpoint(&self, token: Option<ResumeToken>) {
        let Some(cp) = &self.checkpoint else { return };
        if !self.refresh_clean.load(Ordering::Acquire) {
            return;
        }
        if self.inflight.load(Ordering::Acquire) != 0 {
            return;
        }
        let Some(token) = token else {
            return;
        };
        let encoded = match encode_resume_token(&token) {
            Ok(s) => s,
            Err(_) => return,
        };
        // Skip the write if the position hasn't moved since the last persist.
        if self.last_saved_token.lock().unwrap().as_deref() == Some(encoded.as_str()) {
            return;
        }
        if let Err(e) = cp.save(&encoded).await {
            tracing::warn!(error = %e, "Failed to persist idle mongo resume token");
            return;
        }
        *self.last_saved_token.lock().unwrap() = Some(encoded);
    }
}

/// Rewrite a document-field filter so it targets a change event's `fullDocument` namespace.
/// Field keys are prefixed with `fullDocument.`; top-level logical operators (`$and`/`$or`/`$nor`/
/// `$not`) are preserved and their nested sub-filters rewritten recursively. Field-level operators
/// (`$gt`, `$in`, …) inside a value are left untouched. Delete events have no `fullDocument`, so
/// such filters naturally exclude them.
fn full_document_match(query: &Document) -> Document {
    let mut out = Document::new();
    for (key, value) in query {
        if key.starts_with('$') {
            out.insert(key.clone(), rewrite_operator_value(value));
        } else {
            out.insert(format!("fullDocument.{key}"), value.clone());
        }
    }
    out
}

/// Recurse into the value of a logical operator: `$and`/`$or`/`$nor` take an array of sub-filters,
/// `$not` a single one. Nested document-field predicates are rewritten; everything else is copied.
fn rewrite_operator_value(value: &Bson) -> Bson {
    match value {
        Bson::Array(items) => Bson::Array(
            items
                .iter()
                .map(|item| match item {
                    Bson::Document(d) => Bson::Document(full_document_match(d)),
                    other => other.clone(),
                })
                .collect(),
        ),
        Bson::Document(d) => Bson::Document(full_document_match(d)),
        other => other.clone(),
    }
}

/// The change-event operation name stored in message metadata.
fn op_str(op: &OperationType) -> &'static str {
    match op {
        OperationType::Insert => "insert",
        OperationType::Update => "update",
        OperationType::Replace => "replace",
        OperationType::Delete => "delete",
        _ => "other",
    }
}

#[async_trait]
impl MessageConsumer for MongoDbChangeStreamReader {
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch {
                messages: Vec::new(),
                commit: Box::new(|_| Box::pin(async { Ok(()) })),
            });
        }

        let mut stream_guard = self.stream.lock().await;
        // Snapshot phase (opt-in cold start): drain existing documents, then open the stream at the
        // pre-snapshot position and fall through to streaming.
        if stream_guard.is_none() {
            if let Some(batch) = self.snapshot_batch(max_messages).await? {
                return Ok(batch);
            }
            let token = self.pending_resume.lock().unwrap().take();
            let opened = open_change_stream(&self.collection, &self.pipeline, token)
                .await
                .map_err(ConsumerError::Connection)?;
            info!(collection = %self.collection_name, "MongoDB CDC snapshot complete; streaming changes");
            *stream_guard = Some(opened);
        }
        let stream = stream_guard.as_mut().expect("stream opened above");

        let mut messages = Vec::new();
        // Per-message resume token: resuming `after` the last acked event's token gives
        // at-least-once (un-acked events are re-delivered on restart).
        let mut tokens: Vec<ResumeToken> = Vec::new();

        // Block for the first change (the route cancels this future on shutdown), then coalesce any
        // immediately-available events into the batch with a short timeout. While idle, periodically
        // advance the durable checkpoint to the stream's postBatchResumeToken so it can't age out of
        // the oplog (guarded so it never skips an un-acked change).
        loop {
            match tokio::time::timeout(IDLE_RESUME_REFRESH, stream.next()).await {
                Ok(Some(Ok(event))) => {
                    let token = event.id.clone();
                    if let Some(msg) = Self::event_to_message(&event) {
                        messages.push(msg);
                        tokens.push(token);
                    }
                    if !messages.is_empty() {
                        break;
                    }
                    // Event carried no payload (e.g. a drop); keep waiting for a real change.
                }
                Ok(Some(Err(e))) => return Err(ConsumerError::Connection(e.into())),
                Ok(None) => return Err(anyhow!("MongoDB change stream ended unexpectedly").into()),
                Err(_) => {
                    // Extract the token synchronously (stream ref isn't `Send`), then persist.
                    let token = stream.resume_token();
                    self.refresh_idle_checkpoint(token).await;
                }
            }
        }

        while messages.len() < max_messages {
            match tokio::time::timeout(Duration::from_millis(10), stream.next()).await {
                Ok(Some(Ok(event))) => {
                    let token = event.id.clone();
                    if let Some(msg) = Self::event_to_message(&event) {
                        messages.push(msg);
                        tokens.push(token);
                    }
                }
                Ok(Some(Err(e))) => return Err(ConsumerError::Connection(e.into())),
                Ok(None) => return Err(anyhow!("MongoDB change stream ended unexpectedly").into()),
                Err(_) => break, // no more events ready right now
            }
        }

        trace!(count = messages.len(), collection = %self.collection_name, "Received batch of MongoDB change events");

        let checkpoint = self.checkpoint.clone();
        let inflight = self.inflight.clone();
        let refresh_clean = self.refresh_clean.clone();
        let last_saved_token = self.last_saved_token.clone();
        let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
            Box::pin(async move {
                // Resume token of the last contiguous Ack from the front (stop at first Nack).
                let mut acked = 0usize;
                for disp in dispositions.iter().take(tokens.len()) {
                    if matches!(disp, MessageDisposition::Ack | MessageDisposition::Reply(_)) {
                        acked += 1;
                    } else {
                        break;
                    }
                }
                // An earlier batch (a snapshot batch, or a prior streaming batch) left an un-acked
                // redelivery gap and latched `refresh_clean` off. Commits run in delivery order
                // (ordered sequencer), so a token from this batch would sit past that gap: do not
                // persist it even when this batch is itself fully acked.
                let prior_gap = !refresh_clean.load(Ordering::Acquire);
                if acked > 0 && !prior_gap {
                    if let Some(cp) = checkpoint {
                        match encode_resume_token(&tokens[acked - 1]) {
                            Ok(s) => {
                                if let Err(e) = cp.save(&s).await {
                                    tracing::warn!(error = %e, "Failed to persist mongo resume token. Changes may be reprocessed on restart.");
                                } else {
                                    *last_saved_token.lock().unwrap() = Some(s);
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "Failed to encode mongo resume token; not checkpointing")
                            }
                        }
                    }
                }
                // This batch's own nack opens a gap (checkpoint deliberately behind delivered
                // events); latch idle refresh and all later commits off for the session so nothing
                // can skip past it.
                if acked < tokens.len() {
                    refresh_clean.store(false, Ordering::Release);
                }
                inflight.fetch_sub(1, Ordering::AcqRel);
                Ok(())
            }) as BoxFuture<'static, anyhow::Result<()>>
        });

        self.inflight.fetch_add(1, Ordering::AcqRel);
        Ok(ReceivedBatch { messages, commit })
    }

    async fn status(&self) -> EndpointStatus {
        let (healthy, error) = match self.db.run_command(doc! { "ping": 1 }).await {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };
        // "snapshot" until the initial snapshot drains and the change stream opens, then "streaming".
        let phase = match self.stream.try_lock() {
            Ok(g) if g.is_none() => "snapshot",
            Ok(_) => "streaming",
            Err(_) => "streaming", // stream in use by receive_batch → past the snapshot phase
        };
        let resume_token = self.last_saved_token.lock().unwrap().clone();
        EndpointStatus {
            healthy,
            target: self.collection_name.clone(),
            error,
            details: serde_json::json!({
                "cursor_id": self.cursor_id,
                "mode": "cdc",
                "phase": phase,
                "in_flight_batches": self.inflight.load(Ordering::Acquire),
                "resume_token": resume_token,
            }),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Returns a shared MongoDB client for this connection, building one on first use.
/// The collection/database are handles off the client, so a single client serves all.
async fn create_shared_client(config: &MongoDbConfig) -> anyhow::Result<std::sync::Arc<Client>> {
    let identity = crate::support::connection_registry::connection_identity((
        &config.url,
        &config.username,
        &config.password,
        config.tls.required,
        &config.tls.ca_file,
        &config.tls.cert_file,
        &config.tls.cert_password,
        config.tls.accept_invalid_certs,
    ));
    let config_clone = config.clone();
    crate::support::connection_registry::get_or_create(
        "mongodb-client",
        identity,
        config.shared.unwrap_or(true),
        move || async move { create_client(&config_clone).await },
    )
    .await
}

async fn create_client(config: &MongoDbConfig) -> anyhow::Result<Client> {
    let mut client_options = mongodb::options::ClientOptions::parse(&config.url).await?;
    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        client_options.credential = Some(
            mongodb::options::Credential::builder()
                .username(username.clone())
                .password(password.clone())
                .build(),
        );
    }

    if config.tls.required {
        let mut tls_options = mongodb::options::TlsOptions::builder().build();
        if let Some(ca_file) = &config.tls.ca_file {
            tls_options.ca_file_path = Some(std::path::PathBuf::from(ca_file));
        }
        if let Some(cert_file) = &config.tls.cert_file {
            tls_options.cert_key_file_path = Some(std::path::PathBuf::from(cert_file));
        }
        if config.tls.key_file.is_some() {
            tracing::warn!("MongoDB TLS configuration: 'key_file' is ignored. The private key must be included in the 'cert_file' (PEM format).");
        }
        if let Some(cert_password) = &config.tls.cert_password {
            tls_options.tls_certificate_key_file_password = Some(cert_password.as_bytes().to_vec());
        }
        if config.tls.accept_invalid_certs {
            tls_options.allow_invalid_certificates = Some(true);
        }
        client_options.tls = Some(mongodb::options::Tls::Enabled(tls_options));
    }
    Ok(Client::with_options(client_options)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CanonicalMessage;

    #[test]
    fn parse_document_takes_wrapped_fields_and_falls_back_otherwise() {
        let id = mongodb::bson::Uuid::new();
        // Wrapped: payload is unwrapped and metadata decoded.
        let msg = parse_mongodb_document(doc! {
            "_id": id, "payload": "hello", "metadata": { "kind": "greeting" }
        })
        .expect("wrapped document parses");
        assert_eq!(msg.payload.as_ref(), b"hello");
        assert_eq!(
            msg.metadata.get("kind").map(String::as_str),
            Some("greeting")
        );
        assert_eq!(msg.message_id, u128::from_be_bytes(id.bytes()));

        // Foreign document (no `payload`): serialized whole, marked raw.
        let raw = parse_mongodb_document(doc! { "_id": 7, "name": "ada" })
            .expect("foreign document parses");
        assert_eq!(
            raw.metadata
                .get("mq_bridge.original_format")
                .map(String::as_str),
            Some("raw")
        );
        assert!(
            serde_json::from_slice::<serde_json::Value>(&raw.payload).unwrap()["name"] == "ada"
        );

        // Non-string metadata values still take the raw path, document intact.
        let mixed =
            parse_mongodb_document(doc! { "_id": id, "payload": "x", "metadata": { "n": 1 } })
                .expect("mixed-metadata document parses");
        let body: serde_json::Value = serde_json::from_slice(&mixed.payload).unwrap();
        assert_eq!(body["payload"], "x");
    }

    #[test]
    fn resolved_consume_defaults_and_change_stream_alias() {
        use crate::models::{MongoConsume, MongoDbConfig};
        // Default: durable queue consumer.
        let cfg = MongoDbConfig::new("mongodb://localhost", "db");
        assert_eq!(cfg.resolved_consume(), MongoConsume::Consumer);
        // Deprecated `change_stream: true` (no `consume`) still maps to the subscriber mode.
        let mut legacy = MongoDbConfig::new("mongodb://localhost", "db");
        legacy.change_stream = true;
        assert_eq!(legacy.resolved_consume(), MongoConsume::Subscriber);
        // Explicit `consume` wins over the deprecated boolean.
        let mut explicit = MongoDbConfig::new("mongodb://localhost", "db");
        explicit.change_stream = true;
        explicit.consume = Some(MongoConsume::CaptureAll);
        assert_eq!(explicit.resolved_consume(), MongoConsume::CaptureAll);
    }

    #[test]
    fn full_document_match_prefixes_fields_and_preserves_operators() {
        // Plain field predicates (incl. field-level operators and dotted paths) get the
        // `fullDocument.` prefix; the operator value is left untouched.
        assert_eq!(
            full_document_match(&doc! { "type": "notification", "n": { "$gt": 5 } }),
            doc! { "fullDocument.type": "notification", "fullDocument.n": { "$gt": 5 } }
        );
        assert_eq!(
            full_document_match(&doc! { "address.city": "NYC" }),
            doc! { "fullDocument.address.city": "NYC" }
        );
        // Top-level logical operators are preserved and their nested predicates rewritten.
        assert_eq!(
            full_document_match(&doc! { "$or": [ { "a": 1 }, { "b": 2 } ] }),
            doc! { "$or": [ { "fullDocument.a": 1 }, { "fullDocument.b": 2 } ] }
        );
    }

    #[test]
    fn resumable_encode_decode_roundtrips_supported_types() {
        let oid = mongodb::bson::oid::ObjectId::new();
        let uuid = mongodb::bson::Uuid::new();
        let cases = [
            Bson::ObjectId(oid),
            Bson::from(uuid),
            Bson::Int64(123),
            Bson::String("k1".to_string()),
        ];
        for id in cases {
            let encoded = encode_id(&id).expect("supported type encodes");
            assert_eq!(decode_id(&encoded), Some(id), "roundtrip for {}", encoded);
        }
        // Int32 encodes as an int and decodes back as Int64 (BSON `$gt` compares numerically).
        assert_eq!(encode_id(&Bson::Int32(7)).as_deref(), Some("int:7"));
        // Unsupported types are not persisted.
        assert_eq!(encode_id(&Bson::Boolean(true)), None);
        assert_eq!(decode_id("bogus"), None);
    }

    #[test]
    fn resume_token_encode_decode_roundtrips() {
        // A resume token is an opaque `{ "_data": <hex string> }` document; build one directly.
        let token: ResumeToken =
            mongodb::bson::from_document(doc! { "_data": "826553F1A0000000012B02" })
                .expect("token deserializes");

        let encoded = encode_resume_token(&token).expect("token encodes");
        let decoded = decode_resume_token(&encoded).expect("token decodes");
        // Re-encoding the decoded token yields the same string (stable round-trip).
        assert_eq!(encode_resume_token(&decoded).unwrap(), encoded);
        // A malformed value decodes to None so the reader restarts cleanly instead of failing.
        assert!(decode_resume_token("not-json").is_none());
    }

    #[test]
    fn message_to_document_strips_source_metadata_but_keeps_user_keys() {
        let mut msg = CanonicalMessage::new(b"hello".to_vec(), None);
        msg.metadata.insert("kind".to_string(), "order".to_string());
        msg.metadata
            .insert("mqb.src.kafka_offset".to_string(), "42".to_string());

        let doc = message_to_document(&msg, &MongoDbFormat::Text, None).unwrap();
        let metadata = doc.get_document("metadata").unwrap();

        assert_eq!(metadata.get_str("kind").unwrap(), "order");
        assert!(
            !metadata.contains_key("mqb.src.kafka_offset"),
            "source/provenance keys must not be persisted to the document"
        );
    }

    #[test]
    fn message_to_document_id_field_sets_typed_id() {
        let msg = CanonicalMessage::new(br#"{"order_id":"A-1","qty":3}"#.to_vec(), None);
        let doc = message_to_document(&msg, &MongoDbFormat::Json, Some("order_id")).unwrap();
        assert_eq!(doc.get_str("_id").unwrap(), "A-1");

        // Numeric key keeps its BSON integer type.
        let msg = CanonicalMessage::new(br#"{"order_id":42}"#.to_vec(), None);
        let doc = message_to_document(&msg, &MongoDbFormat::Json, Some("order_id")).unwrap();
        assert_eq!(doc.get_i64("_id").unwrap(), 42);
    }

    #[test]
    fn message_to_document_id_field_overrides_raw_id() {
        // Raw inserts the payload verbatim; id_field still wins.
        let msg = CanonicalMessage::new(br#"{"order_id":"A-1","_id":"ignored"}"#.to_vec(), None);
        let doc = message_to_document(&msg, &MongoDbFormat::Raw, Some("order_id")).unwrap();
        assert_eq!(doc.get_str("_id").unwrap(), "A-1");
    }

    #[test]
    fn message_to_document_id_field_missing_or_non_json_errors() {
        for payload in [&br#"{"other":1}"#[..], b"not json", br#"{"order_id":null}"#] {
            let msg = CanonicalMessage::new(payload.to_vec(), None);
            assert!(message_to_document(&msg, &MongoDbFormat::Json, Some("order_id")).is_err());
        }
    }

    #[test]
    fn extract_id_bson_rejects_array_id() {
        // An array is not a valid MongoDB `_id`.
        assert!(extract_id_bson(br#"{"order_id":[1,2]}"#, "order_id").is_err());
    }

    #[test]
    fn tag_outcome_off_returns_ack() {
        let msg = CanonicalMessage::new(b"x".to_vec(), None);
        assert!(matches!(
            tag_outcome(false, msg, OUTCOME_INSERTED),
            Sent::Ack
        ));
    }

    #[test]
    fn tag_outcome_on_returns_tagged_response() {
        for outcome in [OUTCOME_INSERTED, OUTCOME_EXISTED] {
            let msg = CanonicalMessage::new(b"x".to_vec(), None);
            let id = msg.message_id;
            match tag_outcome(true, msg, outcome) {
                Sent::Response(m) => {
                    assert_eq!(m.message_id, id);
                    assert_eq!(
                        m.metadata.get(OUTCOME_KEY).map(String::as_str),
                        Some(outcome)
                    );
                }
                Sent::Ack => panic!("expected Response when report_outcome is on"),
            }
        }
    }
}
