//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use super::*;

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

        // Any `_id` type is claimable — ObjectId, string, integer, UUID binary. Restricting
        // this to UUIDs would silently skip documents `try_claim_document` picks up fine.
        let mut ids_to_claim: Vec<Bson> = Vec::new();
        while let Some(result) = cursor.next().await {
            if let Ok(doc) = result {
                if let Some(id) = doc.get("_id") {
                    ids_to_claim.push(id.clone());
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

        // If we successfully modified any documents, retrieve their full content.
        if update_result.modified_count > 0 {
            self.get_documents_by_ids(&ids_to_claim, locked_until).await
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

    /// Retrieves the documents this claim locked.
    ///
    /// The candidate ids were gathered before the update, so a concurrent consumer may
    /// have taken some of them. Matching on the exact `locked_until` we wrote narrows the
    /// result to the documents this call actually won.
    async fn get_documents_by_ids(
        &self,
        claimed_ids: &[Bson],
        locked_until: i64,
    ) -> anyhow::Result<Vec<Document>> {
        let filter = doc! { "_id": { "$in": claimed_ids }, "locked_until": locked_until };
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
